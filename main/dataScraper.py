import os
import json

import pandas as pd
import requests
import datetime
from typing import Union
from dateutil.parser import parse

import sqlalchemy

from twse_sq_scraper.main.eventHandler import promptType
from twse_sq_scraper.main.twseRequestHandler import twseResponse

class ShortQuotaScraper:
    def __init__(self, db_path=r'sqlite:///twse_sq_scraper/data/TWSE_SQ.db'):
        # Paths
        self.sqldb_path = db_path
        self.sq_url = r'https://mis.twse.com.tw/stock/api/getStockSblsCap.jsp'
        self.js_control_path = r'./twse_sq_scraper/data/control.json'
        # Variables
        self.engine = None
        self.js_control = None
        # void function to load the variables
        self.readControlDict()
        self.loadDbEngine()

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
                df_to_push = twse_res.createDataFrame()
                self.pushFrame(df_to_push, data_key, 'data_tb')
                # Update the Hash
                self.js_control[data_key]['hash'] = new_hash
            else:
                print(f"{promptType.SYS.value} No new data from this request...")
        return None

        # Parse the request data



    def readControlDict(self):
        """
        void function to update self.js_control member
        :return:
        """
        with open(self.js_control_path, 'r') as f:
            js_control = json.loads(f.read())
        self.js_control = js_control

    def _exportControlDict(self):
        """
        """
        with open(self.js_control_path, 'w') as f:
            f.write(json.dumps(self.js_control, indent=4))


    def loadDbEngine(self):
        """ Load the sql engine into the object """
        self.engine = sqlalchemy.create_engine(self.sqldb_path)


    def requestData(self, data_key):
        """

        :return: <requests.response>
        """
        self.readControlDict()
        # Make request to the corresponding url according to the data_key
        res = requests.get(self.js_control[data_key]['url'])
        date_time = datetime.datetime.now()
        date_time_str = date_time.strftime('%b %d, %T')
        # Print out some debug message
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
        # Update the js_control
        self.readControlDict()
        tb_name = self.js_control[data_key][tb_type]
        # Take out the data that should be inserted into the data table
        df_to_push = self._query_new_data(df_to_push, data_key)
        # Insert the new data into the data table
        print(f'{promptType.SYS.value} Inserting {len(df_to_push)} rows of new data into table <{tb_name}>...')
        df_to_push.to_sql(tb_name, self.engine, if_exists='append', index=False)

        return None

    def _query_new_data(self, df_new_tb, data_key):
        # Create variables
        data_table_name = self.js_control[data_key]['data_tb']
        inspector = sqlalchemy.inspect(self.engine)

        # Check if the data table is created
        if data_table_name in inspector.get_table_names():
            # if data table is created, check the max date inside the data table
            max_date_query = f"SELECT MAX(request_date) FROM {data_table_name}"
            max_date_in_db = self.engine.execute(max_date_query).fetchone()[0]
            # if the max date in the data table is the same as the min the of the df_new_tb
            # this means we are doing the wihtin-day update, we need to compare the data
            # otherwise, it means this is the first append of the day, we can just return the
            # whole table to insert
            if parse(max_date_in_db).date() == df_new_tb['request_date'].min():
                # Create new table
                df_new_tb.to_sql('temp', self.engine, if_exists='replace', index=False)
                # Query the new data
                query_sql = f"SELECT * FROM temp " \
                            f"WHERE EXISTS " \
                            f"(SELECT * FROM {data_table_name} " \
                            f"WHERE {data_table_name}.stkno = temp.stkno " \
                            f"AND {data_table_name}.txtime != temp.txtime " \
                            f"AND {data_table_name}.request_date = temp.request_date)"
                df_to_insert = pd.DataFrame(self.engine.execute(query_sql))
                print(f'{promptType.SYS.value} Received {len(df_to_insert)} rows of new data...')
                return df_to_insert
        print(f'{promptType.SYS.value} Received completely new table with {len(df_new_tb)} rows of new data...')
        return df_new_tb
        # TODO: Delete the temp table? Actually not deleting is also fine




    def exportFrame(self):
        pass