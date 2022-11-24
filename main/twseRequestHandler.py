import datetime
import json
import pandas as pd

from Schonfeld_task.main.eventHandler import promptType

class twseResponse:
    def __init__(self, res):
        self.res = res
        self.data_key = 'msgArray' # note: only one key assumption
        self.meta_keys = ['userDelay', 'size', 'rtcode', 'queryTime', 'rtmessage']


    @staticmethod
    def checkRequestStatus(res):
        if res.status_code == 200:
            return True
        else:
            print(f'{promptType.ERROR.value} Request Error with code <{res.status_code}>')
            print(f'Request url: {res.url}')
            return False

    @staticmethod
    def requestDataCleaner(data: str):
        if not isinstance(data, str):
            print(f'{promptType.ERROR.value} Unexpected request data, unable to clean, returning False.')
            return False
        else:
            return data.strip()

    def createFrame(self, data):
        js_data = json.loads(data)
        df_data = pd.DataFrame(js_data[self.data_key])
        # TODO: change the names after checking meaning!


    def createMetaDataFrame(self, data):
        js_data = json.loads(data)
        df_meta = pd.DataFrame.from_dict({ k:js_data[k] for k in self.meta_keys}, orient='index')
        df_meta.columns = [datetime.datetime.now()] # TODO: double check appropriate this column name or not
        return df_meta
