import schedule
import time
from script import run_stock_job

from datetime import datetime

def basic_job():
    print(f"Job started at {datetime.now()}")

# Run the basic job every 3 minutes
schedule.every(3).minutes.do(basic_job)

# Run the stock job every 3 minutes
schedule.every(3).minutes.do(run_stock_job)


while True:
    schedule.run_pending()
    time.sleep(1)