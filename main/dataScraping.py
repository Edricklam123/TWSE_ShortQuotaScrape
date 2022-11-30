# Author: Edrick
# Date: 11/25/2022
import os
import sys
import time
import datetime

# Directory tuner to ensure the path are correctly fixed, you can re-organize again with __init__.py
MAIN_DIR = os.path.dirname(os.path.abspath('__file__'))
sys.path.append(os.path.join(MAIN_DIR, 'main'))

from eventHandler import PromptType
from dataScraper import ShortQuotaScraper

# Constant variables
trading_hour = {
    'start': datetime.time(8, 50),
    'end': datetime.time(17, 0)
}

if __name__ == '__main__':
    # Define the scraper
    db_path = os.path.join(MAIN_DIR, 'data', 'TWSE_SQ.db')
    tsqs = ShortQuotaScraper(db_path)

    # Assistant variables
    request_count = 0

    # Scraping data
    while True:
        now_time = datetime.datetime.now().time()
        try:
            if now_time >= trading_hour['start'] and now_time <= trading_hour['end']:
                print('-'*50)
                tsqs.scrape('twse_sq')
                request_count += 1
                time.sleep(6)
            else:
                print(f"{PromptType.SYS.value} Stopping scraper, total request sent is {request_count}...")
                break
        except Exception as err:
            print(f"{PromptType.ERROR.value} Encountered error: {err}, retry...")


if __name__ == '__debug__':
    import pandas as pd

    self = tsqs
    df = pd.read_sql('twse_sq', self.engine)
    df.query('stkno == "9933"')