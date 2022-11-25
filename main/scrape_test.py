import hashlib

import requests
import json
import pandas as pd
import sqlalchemy

url = r'https://mis.twse.com.tw/stock/api/getStockSblsCap.jsp'

res = requests.get(url)
res = res.text.strip()

res = json.loads(res)
for k, v in res.items():
    print(k)
    print(v)

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

engine = sqlalchemy.create_engine('sqlite:///Schonfeld_task/TWSE_SQ.db')

df.to_sql('TWSE_SQ', engine, if_exists='replace', index=False)

pd.read_sql('TWSE_SQ', engine)
pd.read_sql('TWSE_SQ', engine).query("stkno == '0050'")
engine.execute('SELECT *')