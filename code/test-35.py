import pandas as pd
import datetime
import pymysql
from tqdm import tqdm


# minute bar database
db = pymysql.connect(host='192.168.10.68',
                     port=3306,
                     user='nas',
                     password='Prism@123456')

sql = """SELECT symbol.szWindCode, transaction.nActionDay, transaction.nTime, transaction.nOpen, transaction.nHigh, transaction.nMatch, transaction.iVolume, transaction.iTurnover, transaction.barClose, transaction.S_DQ_PRECLOSE, 
transaction.S_DQ_ADJFACTOR, transaction.HighLimit, transaction.LowLimit FROM marketdata.transaction join marketdata.symbol where transaction.codeID = symbol.codeID and transaction.nTime = 92500000;"""

begin_time = datetime.datetime.now()
print(begin_time)
df = pd.read_sql(sql, db)
end_time = datetime.datetime.now()
print(end_time)
print(end_time - begin_time)
len(df)

import sys
sys.getsizeof(df)
sys.getsizeof(df)/1024/1024




