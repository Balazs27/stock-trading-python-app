import requests
import csv
import os
import time
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

LIMIT = 1000
REQUEST_DELAY = 12  # 60 seconds / 5 requests = 12 seconds per request because of my API limit

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
        
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        print(data)
        request_count += 1
        
        for ticker in data['results']:
            tickers.append(ticker)

    print(f"Total tickers fetched: {len(tickers)} ({request_count} requests made)")


    # CSV Export
    output_csv = "tickers.csv"
    print(f"Writing results to {output_csv} ...")

    with open(output_csv, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
        writer.writeheader()

        for ticker in tickers:
            # Only include the keys defined in example_ticker (ignore extra API fields)
            row = {key: ticker.get(key, "") for key in CSV_FIELDS}
            writer.writerow(row)

    print(f"Finished writing {len(tickers)} tickers to {output_csv}.")

if __name__ == "__main__":
    run_stock_job()

  
