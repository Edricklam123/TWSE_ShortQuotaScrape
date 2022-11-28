import time

from twse_sq_scraper.main.dataScraper import ShortQuotaScraper


if __name__ == '__main__':
    tsqs = ShortQuotaScraper()

    while True:
        print('-'*50)
        tsqs.scrape('twse_sq')
        time.sleep(6)

if __name__ == '__debug__':
        self = tsqs
        df = pd.read_sql('twse_sq', self.engine)
        df.query('stkno == "9933"')