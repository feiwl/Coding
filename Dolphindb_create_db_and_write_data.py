import dolphindb as ddb
import pymongo
import pandas as pd
import time
import numpy as np

def generate_columns():
    myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
    mydb = myclient["marketdata"]
    mycol = mydb["transaction"]
    all_symbol_name = mycol.distinct("szWindCode")
    all_symbol_name = list(map(lambda sym: sym.split('.')[1]+sym.split('.')[0] ,all_symbol_name))
    columns = "`nActionDay`nTime`" + "`".join(all_symbol_name)
    types = list(map(lambda x : "INT" , all_symbol_name))
    types.extend(['INT','DATE'])
    types.reverse()
    types = ",".join(types)

    return columns,types

def create_makrtdatabase():
   s = ddb.session()
   s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")

   if not s.existsDatabase("dfs://marketdata"):
        s.run("""db1 = database("", RANGE,date(datetimeAdd(2017.01M,0..4*12,'M')))""")
        s.run("""db2 = database("", VALUE,(900..11300 join 1400..1600) * 100000)""")
        s.run("""db = database("dfs://marketdata", COMPO, [db1, db2])""")
        s.run("""columns = `szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`bar_close""")
        s.run("""types = [SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,BOOL]""")

        s.run("""db.createPartitionedTable(table(100:0, columns,types),"symbol_data", `nActionDay`nTime)""")
        print("NEW TABLE CREATED....")

        return s
   else:
        s.dropDatabase("dfs://marketdata")
        print("Drop database ....")


# create_makrtdatabase()

s = ddb.session()
s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")


trade = s.loadTable('symbol_data', 'dfs://marketdata')
print(trade.select(['szWindCode','nActionDay','nOpen','bar_close']).where('nTime=133000000').toDF())




script="""
t=select szWindCode,nActionDay,nOpen as open,bar_close as close from loadTable('dfs://marketdata','symbol_data')  where nTime=153000000 
open, close = panel(t.nActionDay, t.szWindCode, [t.open, t.close])
open.setIndexedMatrix!()
close.setIndexedMatrix!()
close-open
"""




# myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
# mydb = myclient["marketdata"]
# mycol = mydb["transaction"]
#
# all_symbol_name = mycol.distinct("szWindCode")
# all_nActionDay = mycol.distinct("nActionDay")
# single_date_minute = mycol.distinct("nTime")


# for day in all_nActionDay:
#     nOpen_Dataframe = {"nTime":[], "nActionDay":[]}
#     all_symbol_pending = {}
#     all_sym_df = pd.DataFrame()
#     all_symbol_Processing_results = {}
#
#     nOpen_Dataframe["nTime"] = single_date_minute
#     nOpen_Dataframe["nActionDay"] = list(map(lambda DATE: np.datetime64(time.strftime("%Y-%m-%d",time.strptime(DATE, "%Y%m%d"))) ,([day] * len(nOpen_Dataframe["nTime"]))))
#
#     nOpen_value = [1100400] * len(nOpen_Dataframe["nTime"])
#
#     list(map(lambda x : all_symbol_pending.update({x:nOpen_value}), all_symbol_name))
#
#     for k,v in all_symbol_pending.items():
#         all_symbol_Processing_results[k.split('.')[1]+k.split('.')[0]] = v
#
#     all_symbol_Processing_results.update(nOpen_Dataframe)
#
#     for k,v in all_symbol_Processing_results.items():
#         all_sym_df = pd.concat([pd.DataFrame({k:v}), all_sym_df], axis=1)
#
#     s = ddb.session()
#     s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")
#
#     s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db="dfs://marketdata", tb="symbol_table"), all_sym_df)
#




# for day in mycol.distinct("nActionDay"):
#     myquery = {"nActionDay":"{}".format(day)}
#     nOpen_Dataframe = {"nTime":[], "nActionDay":[]}
#     all_symbol_name = mycol.distinct("szWindCode")
#     all_symbol_pending = {}
#     all_sym_df = pd.DataFrame()
#     all_symbol_Processing_results = {}
#
#     data = mycol.find(myquery, {'_id': 0, 'nOpen':1, 'szWindCode':1, 'nTime':1, 'nActionDay':1})
#
#     nOpen_Dataframe["nTime"] = data.distinct("nTime")
#     nOpen_Dataframe["nActionDay"] = list(map(lambda DATE: np.datetime64(time.strftime("%Y-%m-%d",time.strptime(DATE, "%Y%m%d"))) ,(data.distinct("nActionDay") * len(nOpen_Dataframe["nTime"]))))
#
#     nOpen_value = [1100400] * len(nOpen_Dataframe["nTime"])
#
#     list(map(lambda x : all_symbol_pending.update({x:nOpen_value}), all_symbol_name))
#
#     for k,v in all_symbol_pending.items():
#         all_symbol_Processing_results[k.split('.')[1]+k.split('.')[0]] = v
#
#     all_symbol_Processing_results.update(nOpen_Dataframe)
#
#     for k,v in all_symbol_Processing_results.items():
#         all_sym_df = pd.concat([pd.DataFrame({k:v}), all_sym_df], axis=1)
#
#     s = ddb.session()
#     s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")
#
#     s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db="dfs://marketdata", tb="symbol_table"), all_sym_df)












# create_makrtdatabase()

# myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
# mydb = myclient["marketdata"]
# mycol = mydb["transaction"]
# myquery = {"nActionDay":"20190102"}
#
# nOpen_Dataframe = {"nTime":[], "nActionDay":[]}
# all_symbol_name = mycol.distinct("szWindCode")
# all_symbol_pending = {}
# all_sym_df = pd.DataFrame()
# all_symbol_Processing_results = {}
#
# data = mycol.find(myquery, {'_id': 0, 'nOpen':1, 'szWindCode':1, 'nTime':1, 'nActionDay':1})
#
# nOpen_Dataframe["nTime"] = data.distinct("nTime")
#
# nOpen_Dataframe["nActionDay"] = list(map(lambda DATE: np.datetime64(time.strftime("%Y-%m-%d",time.strptime(DATE, "%Y%m%d"))) ,(data.distinct("nActionDay") * len(nOpen_Dataframe["nTime"]))))
#
#
# list(map(lambda x : all_symbol_pending.update({x:[]}), all_symbol_name))
# list(map(lambda i : all_symbol_pending[i['szWindCode']].append(i['nOpen']), data))
#
# for k,v in all_symbol_pending.items():
#     all_symbol_Processing_results[k.split('.')[1]+k.split('.')[0]] = v
#
# all_symbol_Processing_results.update(nOpen_Dataframe)
#
# for k,v in all_symbol_Processing_results.items():
#     all_sym_df = pd.concat([pd.DataFrame({k:v}), all_sym_df], axis=1)
#
#
# all_sym_df.to_hdf('/home/banruo/nOpen_hdf', 'data', mode='w')


# print(all_sym_df)
# s = ddb.session()
# s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")

# columns, types = generate_columns()

# s.run("tableInsert{dolphindb_share_table}",all_sym_df)

# script = """ dolphindb_table = table(100:0,{columns},[{types}])
#    share dolphindb_table as dolphindb_share_table""".format(columns=columns,types=types)
# s.run(script)
# s.undef('dolphindb_share_table','SHARED')

# s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db="dfs://marketdata",tb="symbol_table"), all_sym_df)




# import pymongo
# #%%
# myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
# mycollection = myclient["marketdata"]['transaction'].find({'nActionDay': {'$gte':'20201110'}}, {'_id':0, 'nActionDay':1, 'szWindCode':1, 'nTime': 1, 'nOpen':1})
# df = pd.DataFrame(mycollection)
# #%%
#
# df.head()
# #%%
# df.to_hdf('min_bar.hdf5', 'hello', mode = 'w')
#
# #%%
# table = pd.pivot_table(df, values='nOpen', index=['nActionDay', 'nTime'],
#                     columns=['szWindCode'])
# #%%
#
# table.head()
# #%%
# table.reset_index()
