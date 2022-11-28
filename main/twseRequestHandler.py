# Author: Edrick
# Date: 11/25/2022

# Import libraries
import json
import datetime
import hashlib
import pandas as pd
import requests

from main.eventHandler import PromptType

# Implementing class
class TwseResponse:
    """
    Class to handle the twse response object
    """

    def __init__(self, res, res_time):
        """
        :param res: <requests.response> the response that contains the TWSE short quota data request
        """
        self.res = res
        self.res_time = res_time # time when we receive the response (with slight slippery)
        self.data_key = 'msgArray' # note: only one key assumption
        self.meta_keys = ['userDelay', 'size', 'rtcode', 'queryTime', 'rtmessage']


    @staticmethod
    def checkRequestStatus(res):
        if res.status_code == 200:
            return True
        else:
            print(f'{PromptType.ERROR.value} Request Error with code <{res.status_code}>')
            print(f'Request url: {res.url}')
            return False

    @staticmethod
    def requestDataCleaner(res):
        if not isinstance(res, requests.models.Response):
            print(f'{PromptType.ERROR.value} Unexpected request data, unable to clean, returning False.')
            return False
        else:
            return res.text.strip()

    def createDataFrame(self):
        if self.checkRequestStatus(self.res):
            js_data = self.res.json()
            df_data = pd.DataFrame(js_data[self.data_key])

            # date strings
            request_time = self.res_time.time()
            request_date = self.res_time.date()

            # Formatting the dataframe
            df_data.insert(0, 'request_time', request_time)
            df_data.insert(0, 'request_date', request_date)
            # TODO: change the names after checking meaning!
            return df_data

    def createMetaDataFrame(self):
        if self.checkRequestStatus(self.res):
            js_data = self.res.json()
            df_meta = pd.DataFrame.from_dict({ k:js_data[k] for k in self.meta_keys}, orient='index')
            df_meta.columns = [self.res_time] # TODO: double check appropriate this column name or not
            df_meta = df_meta.T
            return df_meta

    def returnHash(self):
        """ Return the MD5 Hashed value for checking """
        js_data = self.res.json()
        data_str = str(js_data[self.data_key])
        return hashlib.md5(data_str.encode()).hexdigest()