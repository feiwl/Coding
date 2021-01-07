import numpy as np
import dolphindb as ddb
import os
import pandas as pd
import datetime
import time
import dolphindb.settings as keys

s = ddb.session()
s.connect(host="localhost", port=8848, userid="admin", password="123456")
dbPath = "/home/banruo/data"
# path="/home/banruo/symbol/20200623/600385.SH_20200623.hdf5"
path = "/home/banruo/symbol/20200623"

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
        nActionDay.append(time.strftime("%Y.%m.%d",time.strptime(row['nActionDay'], "%Y%m%d")))
        nTime.append(row['nTime'])
        nOpen.append(row['nOpen'])
        nHigh.append(row['nHigh'])
        nLow.append(row['nLow'])
        nMatch.append(row['nMatch'])
        iVolume.append(row['iVolume'])
        iTurnover.append(row['iTurnover'])
        bar_close.append(str(row['bar_close']).replace("True",'true'))
        S_DQ_PRECLOSE.append(row['S_DQ_PRECLOSE'])
        S_DQ_ADJFACTOR.append(row['S_DQ_ADJFACTOR'])
        HighLimit.append(row['HighLimit'])
        LowLimit.append(row['LowLimit'])
    return  szWindCode,nActionDay,nTime,nOpen,nHigh,nLow,nMatch,iVolume,iTurnover,bar_close,S_DQ_PRECLOSE,S_DQ_ADJFACTOR,HighLimit,LowLimit

# def createDemoDict(path):
#     i=generate_insert_args(path)
#     return {'szWindCode':i[0],
#             'nActionDay':i[1],'nTime':i[2],'nOpen':i[3],'nHigh':i[4],'nLow':i[5],'nMatch':i[6],
#             'iVolume':i[7],'iTurnover':i[8],'bar_close':i[9],'S_DQ_PRECLOSE':i[10],'S_DQ_ADJFACTOR':i[11],'HighLimit':i[12],'LowLimit':i[13]}
#
script = """ dolphindb_table = table(1000000:0,`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit,[SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,BOOL,INT,DOUBLE,INT,INT])
share dolphindb_table as dolphindb_share_table"""
s.run(script)
# # # #
for file in os.listdir(path):
    i = generate_insert_args(os.path.join(path,file))
    s.upload({'szWindCode': i[0], "nActionDay":i[1], "nTime": i[2], "nOpen": i[3],
              "nHigh": i[4], "nLow": i[5], "nMatch": i[6], "iVolume": i[7], "iTurnover": i[8],
              "bar_close": i[9], "S_DQ_PRECLOSE": i[10], "S_DQ_ADJFACTOR": i[11], "HighLimit": i[12], "LowLimit": i[13]})

    script = "insert into dolphindb_share_table values(szWindCode,date(nActionDay),nTime,nOpen,nHigh,nLow,nMatch,iVolume,iTurnover,bool(bar_close),S_DQ_PRECLOSE,S_DQ_ADJFACTOR,HighLimit,LowLimit);"
    s.run(script)




data=s.loadTable("dolphindb_share_table").toDF()
s.run("append!{{loadTable('{db}', `{tb})}}".format(db='dfs://marketdata',tb='symbol_data'),data )

# print(data)
# # # data=createDemoDict(path)
# #
s.run("append!{{loadTable('{db}', `{tb})}}".format(db='dfs://marketdata',tb='symbol_data'),data )
# # #
print(s.loadTable('symbol_data','dfs://marketdata').toDF())



# print(s.loadTable("dolphindb_share_table").toDF())

# df = pd.read_hdf(path)
# for index,row in df.iterrows():
#     row[1]=time.strftime("%Y.%m.%d",time.strptime(row[1], "%Y%m%d"))
#     print(row[1])


# print(pd.DataFrame(df.))
# data=pd.DataFrame(df)

# s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db="dfs://marketdata",tb="symbol_data"), data)



# data = datetime.datetime.now()
# print(data)

# print(s.loadTable('dolphindb_share_table').toDF())
# script = """dolphindb_table = table(10:0,`szWindCode`nActionDay`bar_close,[SYMBOL,DATE,BOOL])
# share dolphindb_table as dolphindb_share_table"""
# s.run(script)
#
# script= """insert into dolphindb_share_table values ("60000.SH",date({}))""".format(np.array(['20200612'],dtype='datetime64'))
# script= """insert into dolphindb_share_table values ("60001.SH",date({}),92500000,169700,169700,169700,169700,41900,7110430000,bool('true'),172200,4.152541,189400,15000)""".format(time.strftime("%Y.%m.%d",time.strptime("20200622", "%Y%m%d")))
# s.run(script)

# s.loadTable('symbol_data','dfs://marketdata').toDF().append('dolphindb_share_table')

# print(type(np.array(['20200612'],dtype='datetime64')))
# print(s.run("date(2020.01.12)"))
# print(s.run("typestr",np.datetime64('2020-06-01')))
# print(s.loadTable('symbol_data','dfs://marketdata').toDF())
# s.undef('dolphindb_share_table','SHARED')
# s.clearAllCache()

# s.run("typestr",np.datetime64('20200612'))





# for file in os.listdir(path):
#         i = generate_insert_args(os.path.join(path, file))
#         i = generate_insert_args(path)
#         s.upload({'szWindCode':i[0], "nActionDay":i[1], "nTime":i[2], "nOpen":i[3],
#                   "nHigh":i[4], "nLow":i[5], "nMatch":i[6], "iVolume":i[7], "iTurnover":i[8],
#                   "bar_close":i[9], "S_DQ_PRECLOSE":i[10], "S_DQ_ADJFACTOR":i[11], "HighLimit":i[12], "LowLimit":i[13]})
#
#         script = "insert into dolphindb_share_table values(szWindCode,nActionDay,nTime,nOpen,nHigh,nLow,nMatch,iVolume,iTurnover,bar_close,S_DQ_PRECLOSE,S_DQ_ADJFACTOR,HighLimit,LowLimit);"
#         s.run(script)


# tableName = 'symbol_table'
# script = """
# db = database('{db}');
# saveTable(db, dolphindb_share_table, `{tb});
# """.format(db=dbPath, tb=tableName)
# s.run(script)

# symbol = s.loadTable('symbol_table',dbPath)
# symbol.append('dolphindb_share_table')


# s.undef('dolphindb_share_table','SHARED')

# insert_dolphindb(path,dbPath)
# s.run('undef all')
# s.dropTable(tableName='symbol_table',dbPath=dbPath)
# s.run('clearAllCache')
# print(s.loadTable('dolphindb_share_table').toDF())
#
# s.clearAllCache()

# print(s.loadTable('symbol_table',dbPath).toDF())
#
# print(datetime.datetime.now() - data)
# s.close()


# data=generate_insert_args(path)
# print(np.array(np.array(['{}'],dtype="datetime64[D]".format(data[1][1]))))


# print(np.array(['20200612'],dtype="datetime64"))

# print(time.strftime("%Y.%m.%d",time.strptime("20200612", "%Y%m%d")))




