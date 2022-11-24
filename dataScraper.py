import json

import pandas as pd
import requests
import datetime
import pandas as np
import sqlalchemy

from Schonfeld_task.eventHandler import promptType

class ShortQuotaScraper:
    def __init__(self, db_path=r'sqlite:///Schonfeld_task/TWSE_SQ.db'):
        # XXX
        self.sqlDbPath = db_path
        self.sq_url = r'https://mis.twse.com.tw/stock/api/getStockSblsCap.jsp'


    def requestData(self):
        res = requests.get(self.sq_url)
        if res:
            print(f'{promptType.SYS.value} Successfully extracted data at ')
            res_text = res.text
            res_text = self.requestDataCleaner(res_text)
            self.createFrame(data=res_text)


    def pushFrame(self):
        pass

    def exportFrame(self):
        pass