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
    df = pd.read_hdf(os.path.join(directorys))
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

def create_makrtdatabase():
   s = ddb.session()
   s.connect(host="localhost", port=8848, userid="admin", password="123456")
   if not s.existsDatabase("dfs://marketdata"):
       s.run("valuep = database(, VALUE, 2017.01M..2025.12M)")
       s.run("""symbol = database("", HASH, [SYMBOL, 10])""")
       s.run("""r = [930, 1000, 1030, 1130, 1300, 1330, 1400, 1430, 1501] * 100000""")
       s.run("""nTime = database(, RANGE, r)""")
       s.run("""symbol_data = database("dfs://marketdata", COMPO, [symbol, valuep, nTime])""")

       columns = """`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`nNumTrades`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit"""
       types = """[SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,INT,BOOL,INT,DOUBLE,INT,INT]"""

       s.run("""table_new = symbol_data.createPartitionedTable(table(10:0, {cols}, {types}), `symbol_data,
                 `szWindCode`nActionDay`nTime)""".format(cols=columns, types=types))
       print("NEW TABLE CREATED....")

       return s
   else:
       return s
        # s.dropDatabase("dfs://marketdata")
        # print("Drop database ....")

def insert_marketdatabase(path,connection,share_table,table_name):
    script = """ {table_name} = table(1000000:0,`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`nNumTrades`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit,[SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,INT,BOOL,INT,DOUBLE,INT,INT])
    share {table_name} as {share_table}""".format(table_name=table_name,share_table=share_table)
    connection.run(script)

    i = generate_insert_args(path)
    connection.run("tableInsert{}".format(share_table), i)
    connection.undef('{share_table}'.format(share_table=share_table),'SHARED')
    connection.clearAllCache()

# parser = argparse.ArgumentParser(description="Insert_Marketdata")
# parser.add_argument('--day-path', required=True, help='day path data')
# parser.add_argument('--share-table', required=True, help='memery share table')
# parser.add_argument('--table-name', required=True, help='table name')
# args = parser.parse_args()

# s = ddb.session()
# s.connect(host="localhost", port=8848, userid="admin", password="123456")
# if not s.existsDatabase("dfs://marketdata"):
#    s.run("valuep = database(, VALUE, 2017.01M..2025.12M)")
#    s.run("""symbol = database("", HASH, [SYMBOL, 10])""")
#    s.run("""r = [930, 1000, 1030, 1130, 1300, 1330, 1400, 1430, 1501] * 100000""")
#    s.run("""nTime = database(, RANGE, r)""")
#    s.run("""symbol_data = database("dfs://marketdata", COMPO, [symbol, valuep, nTime])""")
#
#    columns = """`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`nNumTrades`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit"""
#    types = """[SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,INT,BOOL,INT,DOUBLE,INT,INT]"""
#
#    s.run("""table_new = symbol_data.createPartitionedTable(table(10:0, {cols}, {types}), `symbol_data,
#              `szWindCode`nActionDay`nTime)""".format(cols=columns, types=types))
#    print("NEW TABLE CREATED....")
# else:
#     s.dropDatabase("dfs://marketdata")
# insert_marketdatabase(args.day_path,conn,args.share_table,args.table_name)
#
s = ddb.session()
s.connect(host="localhost", port=8848, userid="admin", password="123456")
path='/home/banruo/symbol/20190624/603999.SH_20190624.hdf5'

# script = """ {table_name} = table(1000000:0,`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`nNumTrades`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit,[SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,INT,BOOL,INT,DOUBLE,INT,INT])
# share {table_name} as {share_table}""".format(table_name='table_name', share_table='share_table')
# s.run(script)

i = generate_insert_args(path)
s.run("tableInsert{share_table}".format(share_table='share_table'), i)
# print(s.loadTable('share_table').toDF())
# s.undef("share_table",'SHARED')

