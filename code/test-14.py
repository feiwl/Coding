import numpy as np
import dolphindb as ddb
import os
import pandas as pd
import datetime
import dolphindb.settings as keys

s = ddb.session()
s.connect("localhost", 8848)
dbPath = "/home/banruo/data"
# path="/home/banruo/600385.SH_20200623.hdf5"
path = "/home/banruo/symbol/20200612"

def generate_insert_args(filename):
    szWindCode=list()
    nActionDay=list()
    nTime=list()
    nOpen=list()
    nHigh=list()
    nLow=list()
    nMatch=list()
    iVolume=list()
    iTurnover=list()
    bar_close=list()
    S_DQ_PRECLOSE=list()
    S_DQ_ADJFACTOR=list()
    HighLimit=list()
    LowLimit=list()
    df = pd.read_hdf(filename)
    for index, row in df.iterrows():
        szWindCode.append(row['szWindCode'])
        nActionDay.append(row['nActionDay'])
        nTime.append(row['nTime'])
        nOpen.append(row['nOpen'])
        nHigh.append(row['nHigh'])
        nLow.append(row['nLow'])
        nMatch.append(row['nMatch'])
        iVolume.append(row['iVolume'])
        iTurnover.append(row['iTurnover'])
        bar_close.append(row['bar_close'])
        S_DQ_PRECLOSE.append(row['S_DQ_PRECLOSE'])
        S_DQ_ADJFACTOR.append(row['S_DQ_ADJFACTOR'])
        HighLimit.append(row['HighLimit'])
        LowLimit.append(row['LowLimit'])
    return  szWindCode,nActionDay,nTime,nOpen,nHigh,nLow,nMatch,iVolume,iTurnover,bar_close,S_DQ_PRECLOSE,S_DQ_ADJFACTOR,HighLimit,LowLimit

data = datetime.datetime.now()
print(data)

# script = """ dolphindb_table = table(10000:0,`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit,[SYMBOL,STRING,INT,INT,INT,INT,INT,INT,INT,BOOL,FLOAT,FLOAT,INT,INT])
# share dolphindb_table as dolphindb_share_table"""
# s.run(script)

# for file in os.listdir(path):
#         i = generate_insert_args(os.path.join(path, file))
#         s.upload({'szWindCode':i[0], "nActionDay":i[1], "nTime":i[2], "nOpen":i[3],
#                   "nHigh":i[4], "nLow":i[5], "nMatch":i[6], "iVolume":i[7], "iTurnover":i[8],
#                   "bar_close":i[9], "S_DQ_PRECLOSE":i[10], "S_DQ_ADJFACTOR":i[11], "HighLimit":i[12], "LowLimit":i[13]})
#
#         script = "insert into dolphindb_share_table values(szWindCode,nActionDay,nTime,nOpen,nHigh,nLow,nMatch,iVolume,iTurnover,bar_close,S_DQ_PRECLOSE,S_DQ_ADJFACTOR,HighLimit,LowLimit);"
#         s.run(script)


tableName = 'symbol_table'
script = """
db = database('{db}');
saveTable(db, dolphindb_share_table, `{tb});
""".format(db=dbPath, tb=tableName)
s.run(script)

# s.undef('dolphindb_share_table','SHARED')

# insert_dolphindb(path,dbPath)
# s.run('undef all')
# s.dropTable(tableName='symbol_table',dbPath=dbPath)
# s.run('clearAllCache')
# print(s.loadTable('dolphindb_share_table').toDF())
#
# s.clearAllCache()

# print(s.loadTable('symbol_table',dbPath).toDF())

print(datetime.datetime.now() - data)
s.close()