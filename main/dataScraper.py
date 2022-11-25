import os
import json

import pandas as pd
import requests
import datetime

from Schonfeld_task.main.eventHandler import promptType
from Schonfeld_task.main.twseRequestHandler import twseResponse

class ShortQuotaScraper:
    def __init__(self, db_path=r'sqlite:///Schonfeld_task/TWSE_SQ.db'):
        # XXX
        self.sqldb_path = db_path
        self.sq_url = r'https://mis.twse.com.tw/stock/api/getStockSblsCap.jsp'
        self.js_control_path = r'./Schonfeld_task/data/control.json'
        self.js_control = None

        # void function to update
        self.readControlDict()

        # TODO Websocket implementation

    def scrape(self, data_key='twse_sq'):

        # Request Data
        res = self.requestData(data_key)
        # Check the requested Data
        if twseResponse.checkRequestStatus(res):
            # Create the data object
            twse_res = twseResponse(res)
            # Check update
            new_hash = twse_res.returnHash()
            if self.checkHashUpdate(data_key, new_hash):
                # If has new data, we push and save the hash
                self.pushFrame()


        # Pass request data



    def readControlDict(self):
        """
        void function to update self.js_control member
        :return:
        """
        with open(self.js_control_path, 'r') as f:
            js_control = json.loads(f.read())
        self.js_control = js_control

    def requestData(self, data_key):
        """

        :return: <requests.response>
        """
        res = requests.get(self.sq_url)
        date_time = datetime.datetime.now()
        date_time_str = date_time.strftime('%b %d, %T')

        if res:
            print(f'{promptType.SYS.value} Successfull extracted data at: {date_time_str}')

        return res

    def checkHashUpdate(self, data_key, new_hash):
        """

        :param data_key:
        :param new_hash:
        :return: <bool> if the hash is different -> return True, else False
        """
        self.readControlDict()
        return self.js_control[data_key]['hash'] != new_hash

    def saveHash(self, data_key, new_hash):
        self.readControlDict()
        self.js_control[data_key]['hash'] = new_hash
        with open(self.js_control_path, 'w') as f:
            f.write(json.dumps(self.js_control, indent=4))
        self.readControlDict()



    def pushFrame(self, df_to_push:pd.DataFrame, tb_name):

        data_tb_name = self.js_control
        df_to_push.to_sql()

    def exportFrame(self):
        pass