import os
import json

import pandas as pd
import requests
import datetime
from typing import Union

import sqlalchemy

from Schonfeld_task.main.eventHandler import promptType
from Schonfeld_task.main.twseRequestHandler import twseResponse

class ShortQuotaScraper:
    def __init__(self, db_path=r'sqlite:///Schonfeld_task/data/TWSE_SQ.db'):
        # XXX
        self.sqldb_path = db_path
        self.sq_url = r'https://mis.twse.com.tw/stock/api/getStockSblsCap.jsp'
        self.js_control_path = r'./Schonfeld_task/data/control.json'

        self.engine = None
        self.js_control = None

        # void function to update
        self.readControlDict()

        # TODO Websocket implementation


    def scrape(self, data_key='twse_sq'):

        # Request Data
        res = self.requestData(data_key)
        res_time = datetime.datetime.now()

        # Check the requested Data
        if twseResponse.checkRequestStatus(res):
            # Create the data object
            twse_res = twseResponse(res, res_time)
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

    def loadDbEngine(self):
        self.engine = sqlalchemy.create_engine(self.sqldb_path)

    def requestData(self, data_key):
        """

        :return: <requests.response>
        """
        self.readControlDict()
        res = requests.get(self.js_control[data_key]['url'])
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



    def pushFrame(self, df_to_push:pd.DataFrame, data_key, tb_type:str):
        """

        :param df_to_push:
        :param data_key:
        :param tb_type: <str> value in {'data', 'meta'}
        :return:
        """
        self.readControlDict()
        tb_name = self.js_control[data_key][tb_type]
        df_to_push.to_sql(tb_name, self.engine, if_exists='replace')

    def exportFrame(self):
        pass