# Author: Edrick
# Date: 11/25/2022

# Import libraries
import hashlib

import requests
import sqlalchemy
import pandas as pd

# Clumsy testing
url = r'https://mis.twse.com.tw/stock/api/getStockSblsCap.jsp'

res = requests.get(url)
res = res.json()

for k, v in res.items():
    print(k)
    # print(v)

"""
res.keys()
userDelay： 5000
size: 
rtcode：
queryTime： UTC ?
rtmessage: OK
"""
# Hashing
hashlib.md5(str(res['msgArray']).encode()).hexdigest()

df = pd.DataFrame(res['msgArray'])
df['txtime'].to_list()

df.query('txtime != ""')
pd.DataFrame(res)

engine = sqlalchemy.create_engine('sqlite:///twse_sq_scraper/data/TWSE_SQ.db')
engine = sqlalchemy.create_engine('sqlite:///data/TWSE_SQ.db')

df.to_sql('TWSE_sq', engine, if_exists='replace', index=False)
df_data.to_sql('TWSE_sq', engine, if_exists='replace', index=False)

pd.read_sql('twse_sq', engine)
pd.read_sql('TWSE_meta', engine)
pd.read_sql('TWSE_sq', engine)
pd.read_sql('TWSE_SQ', engine).query("stkno == '0050'")
engine.connect().execute('SELECT * FROM TWSE_sq')
pd.read_sql('twse_sq', engine).query('stkno == "6505"')

engine.execute("DROP TABLE IF EXISTS twse_sq;")

inspector = sqlalchemy.inspect(engine)
inspector.get_table_names()

t = pd.read_sql('TWSE_sq', engine)['txtime']
pd.to_datetime(t).max()

t = pd.read_sql('TWSE_sq_temp', engine)['txtime']
pd.to_datetime(t).max()

t_new = df['txtime']
pd.to_datetime(t_new).max()


# Testing insert into database
df.to_markdown()
print(df.to_markdown())
t = pd.read_sql('SELECT * FROM TWSE_sq WHERE stkno == "0050"', engine)

# How about insert into new table, then merge and remove duplicate records delete the temp table
df.to_sql('TWSE_sq_temp_2', engine, index=False)
df_to_push.to_sql('twse_sq', engine, index=False, if_exists='replace')
a = pd.read_sql('TWSE_sq', engine)
b = pd.read_sql('TWSE_sq_temp', engine)
pd.read_sql('temp', engine)
pd.read_sql('temp', self.engine)
a['txtime'].max()
b['txtime'].max()

sql_query_res = engine.execute("SELECT * FROM TWSE_sq_temp")
sql_query_res_1 = engine.execute("SELECT * FROM TWSE_sq")
df_new = pd.DataFrame(sql_query_res)
df_old = pd.DataFrame(sql_query_res_1)

df_new = pd.read_sql('TWSE_sq_temp', engine)
df_old = pd.read_sql('TWSE_sq', engine)
df_new[df_new['stkno'].isin(df_old['stkno']) & df_new['txtime'].isin(df_old['txtime'])]

# Testing the merge
# Only merge the record that is not duplicate in txtime and stkno
# this could extract all the new updates
query_sql = "SELECT * FROM TWSE_sq_temp " \
            "WHERE EXISTS " \
            "(SELECT * FROM TWSE_sq " \
            "WHERE TWSE_sq.stkno = TWSE_sq_temp.stkno " \
            "AND TWSE_sq.txtime != TWSE_sq_temp.txtime)"

pd.DataFrame(engine.execute(query_sql))

insert_sql = f"INSERT INTO TWSE_sq ({query_sql})"

cursor = engine.execute(query_sql)
cursor.fetchall()

sqlalchemy.dialects.sqlite.insert(df_new)
help(sqlalchemy.dialects.sqlite.insert)

engine.table_names()
engine.execute('TWSE_sq')
from sqlalchemy import select
statement = select(sqlalchemy.table('TWSE_sq').txtime)

# Testing insert rows into sql db
# Make a temp table
df.to_sql('TWSE_sq_temp', engine, index=False, if_exists='replace')
engine.table_names()

# Query the rows to insert
query_sql = "SELECT * FROM TWSE_sq_temp " \
            "WHERE EXISTS " \
            "(SELECT * FROM TWSE_sq " \
            "WHERE TWSE_sq.stkno = TWSE_sq_temp.stkno " \
            "AND TWSE_sq.txtime != TWSE_sq_temp.txtime)"
df_to_insert = pd.DataFrame(engine.execute(query_sql))

"""
There are 648 rows to insert, where 648 stocks have updated short quota
"""
insert_data = df_to_insert.values.tolist()
insert_sql = f"INSERT INTO TWSE_sq VALUES ({('?,'*df_to_insert.shape[1])[:-1]})"

engine.execute(insert_sql, insert_data)

pd.read_sql('TWSE_sq', engine)