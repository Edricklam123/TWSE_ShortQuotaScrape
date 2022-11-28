# Author: Edrick
# Date: 11/25/2022
import os
import time
import datetime

from main.eventHandler import PromptType
from main.dataScraper import ShortQuotaScraper


if __name__ == '__main__':
    tsqs = ShortQuotaScraper()
    trading_hour = {
        'start': datetime.time(8, 50),
         'end': datetime.time(17, 0)
    }

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
        self = tsqs
        df = pd.read_sql('twse_sq', self.engine)
        df.query('stkno == "9933"')