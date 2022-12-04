# Author: Edrick
# Date: 11/25/2022

# Import libraries
import os
import json

import requests
import datetime
import sqlalchemy
import pandas as pd

from typing import Union
from dateutil.parser import parse

from eventHandler import PromptType
from twseRequestHandler import TwseResponse


class ShortQuotaScraper:
    """
    The main TWSE Short Quota Scraper
    """
    def __init__(self, db_path=r'.../data/TWSE_SQ.db'):
        # Paths
        self.sqldb_path = f'sqlite:///{db_path}'
        self.sq_url = r'https://mis.twse.com.tw/stock/api/getStockSblsCap.jsp'
        self.js_control_path = r'./data/control.json'
        # Variables
        self.engine = None
        self.js_control = None
        # void function to load the variables
        self.read_control_dict()
        self.load_db_engine()

    def scrape(self, data_key='twse_sq'):
        # Request Data
        res = self.request_data(data_key)
        res_time = datetime.datetime.now()

        # Check the requested Data
        if TwseResponse.check_response_status(res):
            # Create the data object
            twse_res = TwseResponse(res, res_time)
            # Check update
            new_hash = twse_res.get_hash()
            if self.check_hash_update(data_key, new_hash):
                # If has new data, we push and save the hash
                df_to_push = twse_res.create_dataframe()
                self.push_frame(df_to_push, data_key, 'data_tb')
                # Update the Hash
                self.save_hash(data_key, new_hash)
                self.check_duplicated()
                return True
            else:
                print(f"{PromptType.SYS.value} No new data from this request...")
                return False
        return None

        # Parse the request data

    def check_duplicated(self):
        """
        Void func
        :return:
        """
        df = pd.read_sql('twse_sq', self.engine)
        if any(df[['request_date', 'stkno', 'txtime']].duplicated()):
            print(df[df[['request_date', 'stkno', 'txtime']].duplicated()].sort_values('stkno'))
        else:
            print(f'{PromptType.SYS.value} == No duplicated data ==')

    def read_control_dict(self):
        """
        void function to update self.js_control member
        :return:
        """
        with open(self.js_control_path, 'r') as f:
            js_control = json.loads(f.read())
        self.js_control = js_control

    def _export_control_dict(self):
        """
        """
        with open(self.js_control_path, 'w') as f:
            f.write(json.dumps(self.js_control, indent=4))

    def load_db_engine(self):
        """ Load the sql engine into the object """
        self.engine = sqlalchemy.create_engine(self.sqldb_path)

    def request_data(self, data_key):
        """

        :return: <requests.response>
        """
        self.read_control_dict()
        # Make request to the corresponding url according to the data_key
        res = requests.get(self.js_control[data_key]['url'])
        date_time = datetime.datetime.now()
        date_time_str = date_time.strftime('%b %d, %T')
        # Print out some debug message
        if res:
            print(f'{PromptType.SYS.value} Successfull extracted data at: {date_time_str}')
        return res

    def check_hash_update(self, data_key, new_hash):
        """

        :param data_key:
        :param new_hash:
        :return: <bool> if the hash is different -> return True, else False
        """
        self.read_control_dict()
        return self.js_control[data_key]['hash'] != new_hash

    def save_hash(self, data_key, new_hash):
        self.read_control_dict()
        self.js_control[data_key]['hash'] = new_hash
        with open(self.js_control_path, 'w') as f:
            f.write(json.dumps(self.js_control, indent=4))
        self.read_control_dict()

    def push_frame(self, df_to_push:pd.DataFrame, data_key, tb_type:str):
        """

        :param df_to_push:
        :param data_key:
        :param tb_type: <str> value in {'data', 'meta'}
        :return:
        """
        # Update the js_control
        self.read_control_dict()
        tb_name = self.js_control[data_key][tb_type]
        # Take out the data that should be inserted into the data table
        df_to_insert = self._query_new_data(df_to_push, data_key)
        # Insert the new data into the data table
        print(f'{PromptType.SYS.value} Inserting {len(df_to_insert)} rows of new data into table <{tb_name}>...')
        df_to_insert.to_sql(tb_name, self.engine, if_exists='append', index=False)


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
                # Criteria -> drop those records that already have same date, same stk number and same sliblimit
                query_sql = f"SELECT * FROM temp " \
                            f"WHERE NOT EXISTS " \
                            f"(SELECT * FROM {data_table_name} " \
                            f"WHERE {data_table_name}.stkno = temp.stkno " \
                            f"AND {data_table_name}.txtime = temp.txtime " \
                            f"AND {data_table_name}.request_date = temp.request_date)"
                df_to_insert = pd.DataFrame(self.engine.execute(query_sql))
                print(f'{PromptType.SYS.value} Received {len(df_to_insert)} rows of new data...')
                return df_to_insert
        print(f'{PromptType.SYS.value} Received completely new table with {len(df_new_tb)} rows of new data...')
        return df_new_tb
        # TODO: Delete the temp table? Actually not deleting is also fine

    def export_frame(self):
        pass