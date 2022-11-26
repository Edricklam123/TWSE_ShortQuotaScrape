import time

from twse_sq_scraper.main.dataScraper import ShortQuotaScraper


if __name__ == '__main__':
    tsqs = ShortQuotaScraper()

    while True:
        tsqs.scrape('twse_sq')
        time.sleep(5)