import snowflake.connector
import requests
import os
import os
import time
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
SNOWFLAKE_TABLE = os.getenv('SNOWFLAKE_TABLE', 'TICKERS')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE', 'ALL_USERS_ROLE')

LIMIT = 1000
# 60 seconds / 5 requests = 12 seconds per request because of my API limit
REQUEST_DELAY = 12

# Example schema we want to match in the CSV
example_ticker = {
    "ticker": "ZTS",
    "name": "ZOETIS INC.",
    "market": "stocks",
    "locale": "us",
    "primary_exchange": "XNYS",
    "type": "CS",
    "active": True,
    "currency_name": "usd",
    "cik": "0001555280",
    "composite_figi": "BBG0039320N9",
    "share_class_figi": "BBG0039320P7",
    "last_updated_utc": "2025-12-06T07:06:03.841200256Z",
}

CSV_FIELDS = list(example_ticker.keys())


# Base URL for fetching tickers
def run_stock_job():
    """
    Fetch all stock tickers from the API (with pagination),
    respecting the rate limit, and write them to tickers.csv
    with the same schema as example_ticker.
    """

    if not POLYGON_API_KEY:
        print("ERROR: POLYGON_API_KEY is not set in your .env file.")
        return

    # Initial request to get the first page
    url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    tickers = []

    data = response.json()
    print(data.keys())
    for ticker in data['results']:
        tickers.append(ticker)

    request_count = 1

    # Handle pagination with delay to respect API rate limits
    while 'next_url' in data:
        print(f'requesting next page, waiting {REQUEST_DELAY} seconds...')
        time.sleep(REQUEST_DELAY)  # Wait before making next request

        response = requests.get(
            data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        print(data)
        request_count += 1

        for ticker in data['results']:
            tickers.append(ticker)

    print(
        f"Total tickers fetched: {len(tickers)} ({request_count} requests made)")

    # Snowflake Export
    if not (SNOWFLAKE_USER and SNOWFLAKE_PASSWORD and SNOWFLAKE_ACCOUNT):
        print("Snowflake credentials not fully set. Please set SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT in environment.")
        print(
            f"Fetched {len(tickers)} tickers locally; skipping Snowflake upload.")
        return

    print(f"Connecting to Snowflake account={SNOWFLAKE_ACCOUNT} ...")
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    cur = conn.cursor()
    try:
        cols_sql = ", ".join([f"{col.upper()} VARCHAR" for col in CSV_FIELDS])
        create_sql = f"CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} ({cols_sql})"
        cur.execute(create_sql)

        cols_list = ", ".join([col.upper() for col in CSV_FIELDS])
        placeholders = ", ".join(["%s"] * len(CSV_FIELDS))
        insert_sql = f"INSERT INTO {SNOWFLAKE_TABLE} ({cols_list}) VALUES ({placeholders})"

        rows = []
        for ticker in tickers:
            row = tuple(str(ticker.get(key, "")) for key in CSV_FIELDS)
            rows.append(row)

        if rows:
            cur.executemany(insert_sql, rows)
            conn.commit()
            print(
                f"Inserted {len(rows)} rows into {SNOWFLAKE_TABLE} (schema={SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA})")
        else:
            print("No rows to insert into Snowflake.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_stock_job()

"""
Zach's original code without delay and API limit handling:
while 'next_url' in data:
    print('requesting next page', data['next_url'])
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    data = response.json()
    print(data)
    for ticker in data['results']:
        tickers.append(ticker)

print(len(tickers))"""
