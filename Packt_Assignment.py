import requests
from datetime import date
from dateutil.relativedelta import relativedelta
from pyhive import hive

# Current Date
current_date = date.today()
print(current_date)
# Hive Format
formatted_date = current_date.strftime("%Y%m%d")
print(formatted_date)
# 1 Month Ago
new_date = current_date - relativedelta(months=1)
print(new_date)
partition_value = current_date


def hive_connet():
    connection = hive.Connection(
        host='host_name',
        port=10000,  # Default HiveServer2 port
        username='username',
        password='password',
        database='database'
    )
    cursor = connection.cursor()
    cursor.execute("show partitions tags")
    results = cursor.fetchall()
    partition_present = False

    for row in results:
        # Check if partition exists
        partition_values = row['result'].split('/')
        if formatted_date in partition_values:
            partition_present = True
    cursor.close()
    # if exists get the max value from there
    if partition_present:
        cursor.execute("select max(count) from table_name")
        max_count = cursor.fetchone()[0]
        cursor.execute(f"select * from table_name where count='{max_count}'")
        results = cursor.fetchall()
        for row in results:
            print(row)
    # if it doesn't exist fetch the max value from API
    else:
        get_api_data()


#

def get_api_data():
    # API endpoint for tags
    connection = hive.Connection(
        host='your_hive_host',
        port=10000,  # Default HiveServer2 port
        username='your_username',
        password='your_password',
        database='your_database'
    )

    # Create a cursor to execute Hive queries
    cursor = connection.cursor()

    url = "https://api.stackexchange.com/2.3/tags"
    # date=
    # Parameters
    params = {
        "site": "stackoverflow",
        "order": "desc",
        "fromdate": current_date,
        "todate": new_date,
        "pagesize": 100,  # Number of items per page
        "page": 1  # Number of tags to retrieve (adjust as needed)
    }
    all_data = []
    # Make the request
    while True:
        response = requests.get(url, params=params)
        data = response.json()
        items = data["items"]
        all_data.extend(items)
        has_more = data["has_more"]
        if not has_more:
            break
        params["page"] += 1

    for item in all_data:
        # Access relevant information from the item
        # Do something with the data item
        print(item)
        insert_statement = f"INSERT INTO TAGS PARTITION ('partitiondate'='{partition_value}') " \
                           f"(has_synonyms, is_moderator_only, is_required, count, name) " \
                           f"VALUES ({item['has_synonyms']}, {item['is_moderator_only']}, {item['is_required']}, " \
                           f"{item['count']}, '{item['name']}')"
        cursor.execute(insert_statement)
    hive_connet()


if __name__ == '__main__':
    hive_connet()
