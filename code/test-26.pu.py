import dolphindb as ddb
import os
import pandas as pd
import time
import numpy as np
import argparse

def generate_insert_args(directorys):
    szWindCode = list()
    nActionDay = list()
    nTime = list()
    nOpen = list()
    nHigh = list()
    nLow = list()
    nMatch = list()
    iVolume = list()
    iTurnover = list()
    nNumTrades = list()
    bar_close = list()
    S_DQ_PRECLOSE = list()
    S_DQ_ADJFACTOR = list()
    HighLimit = list()
    LowLimit = list()
    for filename in os.listdir(directorys):
        df = pd.read_hdf(os.path.join(directorys,filename))
        for index, row in df.iterrows():
            szWindCode.append(row['szWindCode'])
            nActionDay.append(np.datetime64(time.strftime("%Y-%m-%d",time.strptime(row['nActionDay'], "%Y%m%d"))))
            nTime.append(row['nTime'])
            nOpen.append(row['nOpen'])
            nHigh.append(row['nHigh'])
            nLow.append(row['nLow'])
            nMatch.append(row['nMatch'])
            iVolume.append(row['iVolume'])
            iTurnover.append(row['iTurnover'])
            nNumTrades.append(row['nNumTrades'])
            bar_close.append(np.bool(row['bar_close']))
            S_DQ_PRECLOSE.append(row['S_DQ_PRECLOSE'])
            S_DQ_ADJFACTOR.append(row['S_DQ_ADJFACTOR'])
            HighLimit.append(row['HighLimit'])
            LowLimit.append(row['LowLimit'])
    return  szWindCode,nActionDay,nTime,nOpen,nHigh,nLow,nMatch,iVolume,iTurnover,nNumTrades,bar_close,S_DQ_PRECLOSE,S_DQ_ADJFACTOR,HighLimit,LowLimit

def insert_marketdatabase(path,connection,share_table,table_name):
    try:
        if connection.loadTable(share_table):
            i = generate_insert_args(path)
            connection.run("tableInsert{'share_table'}", i)
    except Exception as e:
        script = """ {table_name} = table(1000000:0,`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`nNumTrades`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit,[SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,INT,BOOL,INT,DOUBLE,INT,INT])
        share {table_name} as {share_table}""".format(table_name=table_name,share_table=share_table)
        connection.run(script)
        print(e)

# parser = argparse.ArgumentParser(description="Insert_Marketdata")
# parser.add_argument('--day-path', required=True, help='day path data')
# parser.add_argument('--share-table', required=True, help='memery share table')
# parser.add_argument('--table-name', required=True, help='table name')
# args = parser.parse_args()

s = ddb.session()
s.connect(host="localhost", port=8848, userid="admin", password="123456")

day_path='/home/banruo/symbol/20190624/'
share_table = 'share_table'
table_name = 'table_name'
insert_marketdatabase(day_path,s,share_table,table_name)


