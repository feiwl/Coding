import pandas as pd
import datetime
import pymysql
from tqdm import tqdm


# minute bar database
db = pymysql.connect(host='192.168.10.68',
                     port=3306,
                     user='nas',
                     password='Prism@123456')

# sql = """SELECT Test_1.codeId,  Test_1.nActionDay, Test_1.nTime, Test_1.nOpen, Test_1.nHigh, Test_1.nMatch, Test_1.iVolume, Test_1.iTurnover, Test_1.barClose, Test_1.S_DQ_PRECLOSE,
# Test_1.S_DQ_ADJFACTOR, Test_1.HighLimit, Test_1.LowLimit FROM marketdata.Test_1 where  Test_1.nActionDay = '2020-06-24';"""

sql = """SELECT Test_1.szWindCode,  Test_1.nActionDay, Test_1.nTime, Test_1.nOpen, Test_1.nHigh, Test_1.nMatch, Test_1.iVolume, Test_1.iTurnover, Test_1.barClose, Test_1.S_DQ_PRECLOSE,
Test_1.S_DQ_ADJFACTOR, Test_1.HighLimit, Test_1.LowLimit FROM marketdata.Test_1 where Test_1.szWindCode='600000.SH' and Test_1.nTime = '92500000';"""

# sql = """SELECT transaction.codeId,  transaction.nActionDay, transaction.nTime, transaction.nOpen, transaction.nHigh, transaction.nMatch, transaction.iVolume, transaction.iTurnover, transaction.barClose, transaction.S_DQ_PRECLOSE,
# transaction.S_DQ_ADJFACTOR, transaction.HighLimit, transaction.LowLimit FROM marketdata.transaction where  transaction.nActionDay = '2019-06-24';"""

# sql = """SELECT symbol.szWindCode, transaction.nActionDay, transaction.nTime, transaction.nOpen, transaction.nHigh, transaction.nMatch, transaction.iVolume, transaction.iTurnover, transaction.barClose, transaction.S_DQ_PRECLOSE,
# transaction.S_DQ_ADJFACTOR, transaction.HighLimit, transaction.LowLimit FROM marketdata.transaction join marketdata.symbol where transaction.codeID = symbol.codeID and transaction.nActionDay = '20200615';"""

begin_time = datetime.datetime.now()
print(begin_time)

df = pd.read_sql(sql, db)

end_time = datetime.datetime.now()
print(end_time)
print(end_time - begin_time)
df.tail()




