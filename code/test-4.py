import dolphindb as ddb
import os
import pandas as pd
import time
import argparse

s = ddb.session()
s.connect(host="localhost", port=8848, userid="admin", password="123456")
#
if not s.existsDatabase("dfs://marketdata"):
    s.run("valuep = database(, VALUE, 2018.01.01..2024.12.31)")
    s.run("""symbol = database(, HASH, [SYMBOL, 20])""")
    s.run("""nTime = database(,HASH, [INT, 2])""")
    s.run("""symbol_data = database("dfs://marketdata", COMPO, [symbol, valuep, nTime])""")
    columns = """`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`nNumTrades`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit"""
    types = """[SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,INT,BOOL,INT,DOUBLE,INT,INT]"""

    s.run("""table_new = symbol_data.createPartitionedTable(table(10:0, {cols}, {types}), `symbol_data,
              `szWindCode`nActionDay`nTime)""".format(cols=columns, types=types))
    print("NEW TABLE CREATED....")
print(s.loadTable("symbol_data","dfs://marketdata").toDataFrame())
#
# s.dropDatabase("dfs://marketdata")
# s.undef("share_table","SHARED")
# s.clearAllCache()
# script = """ {table_name} = table(1000000:0,`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`nNumTrades`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit,[SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,INT,BOOL,INT,DOUBLE,INT,INT])
# share {table_name} as {share_table}""".format(table_name="table_name",share_table="share_table_name")
# s.run(script)

#
# script = "insert into share_table_name values('600520.SH',date(2019.06.24),92500000,134900,134900,134900,134900,66000,8903400000,46,bool(true),134900,2.341592,148400,121400);"
# s.run(script)
# data=s.loadTable("share_table_name").toDataFrame()
# print(data)

# s.run("append!{{loadTable('{db}', `{tb})}}".format(db='dfs://marketdata',tb='symbol_data'),data )
# print(s.loadTable("symbol_data","dfs://marketdata").toDataFrame())
#

# path='/home/banruo/symbol/20190624'
#
# def generate_insert_args(filename):
#     szWindCode=list()
#     nActionDay=list()
#     nTime=list()
#     nOpen=list()
#     nHigh=list()
#     nLow=list()
#     nMatch=list()
#     iVolume=list()
#     iTurnover=list()
#     nNumTrades=list()
#     bar_close=list()
#     S_DQ_PRECLOSE=list()
#     S_DQ_ADJFACTOR=list()
#     HighLimit=list()
#     LowLimit=list()
#     df = pd.read_hdf(filename)
#     for index, row in df.iterrows():
#         szWindCode.append(row['szWindCode'])
#         nActionDay.append(time.strftime("%Y.%m.%d",time.strptime(row['nActionDay'], "%Y%m%d")))
#         nTime.append(row['nTime'])
#         nOpen.append(row['nOpen'])
#         nHigh.append(row['nHigh'])
#         nLow.append(row['nLow'])
#         nMatch.append(row['nMatch'])
#         iVolume.append(row['iVolume'])
#         iTurnover.append(row['iTurnover'])
#         nNumTrades.append(row['nNumTrades'])
#         bar_close.append(str(row['bar_close']).replace("True",'true'))
#         S_DQ_PRECLOSE.append(row['S_DQ_PRECLOSE'])
#         S_DQ_ADJFACTOR.append(row['S_DQ_ADJFACTOR'])
#         HighLimit.append(row['HighLimit'])
#         LowLimit.append(row['LowLimit'])
#     return  szWindCode,nActionDay,nTime,nOpen,nHigh,nLow,nMatch,iVolume,iTurnover,nNumTrades,bar_close,S_DQ_PRECLOSE,S_DQ_ADJFACTOR,HighLimit,LowLimit
#
#
# for i in os.listdir(path):
#     data=generate_insert_args(os.path.join(path,i))
#     print(data)
