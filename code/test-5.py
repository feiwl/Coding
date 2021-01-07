import dolphindb as ddb
import numpy as np
import pandas as pd
import dolphindb.settings as keys


dbPath = "/home/banruo/data"
s = ddb.session()
s.connect("localhost", 8848)

# script = """t = table(1:0,`id`date`ticker`price, [INT,DATE,STRING,DOUBLE]) share t as tglobal"""
# s.run(script)
#
# script = "insert into tglobal values(%s, date(%s), %s, %s);tglobal"% (1, np.datetime64("2019-01-01").astype(np.int64), '`AAPL', 5.6)
# s.run(script)

# rowNum = 5
# ids = np.arange(1, rowNum+1, 1, dtype=np.int32)
# dates = np.array(pd.date_range('4/1/2019', periods=rowNum), dtype='datetime64[D]')
# tickers = np.repeat("AA", rowNum)
# prices = np.arange(1, 0.6*(rowNum+1), 0.6, dtype=np.float64)
# s.upload({'ids':ids, "dates":dates, "tickers":tickers, "prices":prices})
# script = "insert into tglobal values(ids,date(dates),tickers,prices);"
# s.run(script)

# t = s.loadTableBySQL(tableName="tglobal", sql="select * from tglobal",dbPath="")
# print(t.toDF())

# args = [ids, dates, tickers, prices]
# s.run("tableInsert{tglobal}", args)
# s.run("tglobal")

# def createDemoDict():
#     return {'ID': [1,2,3,4],
#             'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
#             'ticker': ['APPL', 'AMZN', 'AMZN', 'A'],
#             'price': [22, 3.5, 21, 26]}

# # dt = s.table(data=createDemoDict(), tableAliasName="testDict")
# # print(s.loadTable("tglobal").toDF())
#
# tb=pd.DataFrame(createDemoDict())
# s.upload({'tb':tb})
# s.run("tableInsert(tglobal,(select id, date(date) as date, ticker, price from tb))")

# rowNum = 5
# ids = np.arange(1, rowNum+1, 1, dtype=np.int32)
# dates = np.array(pd.date_range('4/1/2019', periods=rowNum), dtype='datetime64[D]')
# tickers = np.repeat("AA", rowNum)
# prices = np.arange(1, 0.6*(rowNum+1), 0.6, dtype=np.float64)
# args = [ids, dates, tickers, prices]
#
# print(rowNum)
# print(ids)
# print(dates)
# print(tickers)
# print(prices)
# print(args)
#
# print(pd.DataFrame(createDemoDict()))

import dolphindb as ddb
# import numpy as np
#


# dbPath="/home/banruo/api_python3/data/test_1"
# path='/home/banruo/symbol/20200612/688399.SH_20200612.hdf5'
#
#
# tableName='tb'
# testDict=pd.DataFrame(pd.read_hdf(path))
# script="""
# dbPath='{db}'
# if(existsDatabase(dbPath))
#     dropDatabase(dbPath)
# db=database(dbPath, VALUE, ['688599.SH'])
# testDictSchema=table(10000:0, `szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit, [SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,BOOL,FLOAT,FLOAT,INT,INT])
# db.createPartitionedTable(testDictSchema, `{tb}, `szWindCode)""".format(db=dbPath,tb=tableName)
# s.run(script)
# # s.run("append!{{loadTable({db}, `{tb})}}".format(db=dbPath,tb=tableName),testDict)
# s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db=dbPath,tb=tableName),testDict)
# s.run("select * from loadTable('{db}', `{tb})".format(db=dbPath,tb=tableName))

print(s.loadTable('symbol_table',dbPath).toDF())
# print(s.loadTable('dolphindb_share_table').toDF())
# tableName = 'symbol_table'
# script = """
# saveTable('/home/banruo/data', dolphindb_share_table, `{tb});
# """.format(tb=tableName)
# s.run(script)
# data = s.loadTable('symbol_table',dbPath)
# s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db=dbPath,tb='symbol_table'), 'dolphindb_share_table')

# data = s.loadTable('symbol_table',dbPath, memoryMode=True)
# s.run("tableInsert{{'data'}}".format(data=data),'dolphindb_share_table')


# symbol = s.loadTable('symbol_table',dbPath)
# symbol.append()

# print(s.loadTable('symbol_table',dbPath).toDF())
# s.undef('dolphindb_share_table','SHARED')
# s.clearAllCache()
#
# print(s.close())





