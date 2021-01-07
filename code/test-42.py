import pymongo
from pymongo import ASCENDING, DESCENDING
from decimal import Decimal
import time
import pandas as pd
import numpy as np
import math
import datetime

myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")

mydb = myclient["marketdata"]
mycol = mydb["snap_to_close_index"]

# mycol.create_index([("nActionDay",1),("nTime",1)])

begin_time = datetime.datetime.now()
print(begin_time)

# na = list(map(lambda dt: nAction.append(dt) ,data))
# print(nAction)

def yield_rows(cursor, chunk_size):
    """
    Generator to yield chunks from cursor
    :param cursor:
    :param chunk_size:
    :return:
    """
    chunk = []
    for i, row in enumerate(cursor):
        if i % chunk_size == 0 and i > 0:
            yield chunk
            del chunk[:]
        chunk.append(row)
    yield chunk


# myquery = {"szWindCode":"000017.SZ"}
myquery = {"nActionDay":"20201229"}
# myquery = {"nTime": 92500000}
# myquery = {'nActionDay':{'$gte': '20200111','$lte': '20201125'}}
# myquery = {"nActionDay":"20200615","nTime": 92500000}
# myquery = {"szWindCode":"000017.SZ","nTime": 92500000}
# myquery = {'nActionDay':{'$gte': '20190101','$lte': '20201001'}, '$and':[{"nTime":1300e5}]}
# myquery = {'nActionDay':{'$gte': "20190101", '$lte': "20201001"}, "nTime":1400e5}
# mydoc = mycol.find().sort('_id',-1).limit(1)
# myquery = {'nActionDay':{'$gte': '20190801','$lte': '20200801'}, "szWindCode":'600000.SH'}



mydoc = mycol.find(myquery, {'_id':0, 'nTime': 0})
# print(mydoc.explain())

df = pd.DataFrame(mydoc)
print(df)



# df.to_hdf('/home/banruo/nOpen_hdf5', 'data', 'w')



# print(mydoc.explain()['executionStats']['executionStages'])

# mycollection = myclient["marketdata"]['transaction'].find(myquery, {'_id': 0, 'nTime': 0}).skip(0).limit(100000)
# mycollection_2 = myclient["marketdata"]['transaction'].find(myquery, {'_id': 0, 'nTime': 0}).skip(100000).limit(100000)
# print(pd.DataFrame(mycollection))
# print(pd.DataFrame(mycollection_2))

end_time = datetime.datetime.now()
print(end_time)
print(end_time - begin_time)


























# mycol.create_index([("szWindCode",DESCENDING),("nActionDay",ASCENDING)])

# mydict = {'szWindCode': '002902.SZ', 'nActionDay': '2019.06.24', 'nTime': 131600000, 'nOpen': 250400, 'nHigh': 250700, 'nLow': 250100, 'nMatch': 250500, 'iVolume': 68600, 'iTurnover': 17170700000, 'nNumTrades': 154, 'bar_close': 'true', 'S_DQ_PRECLOSE': 242800, 'S_DQ_ADJFACTOR': 1.508004, 'HighLimit': 267100, 'LowLimit': 218500}
# mylist = [
#   { "name": "Taobao", "alexa": "100", "url": "https://www.taobao.com" },
#   { "name": "QQ", "alexa": "101", "url": "https://www.qq.com" },
#   { "name": "Facebook", "alexa": "10", "url": "https://www.facebook.com" },
#   { "name": "知乎", "alexa": "103", "url": "https://www.zhihu.com" },
#   { "name": "Github", "alexa": "109", "url": "https://www.github.com" }
# ]

# # x = mycol.insert_many(mylist)
# # print(x.inserted_ids)
# for x in mycol.find({},{"_id": 0}):
#     print(x)

# def generate_insert_args(filename):
#     columns = list()
#     df = pd.read_hdf(filename)
#     for index, row in df.iterrows():
#         columns.append({'szWindCode': row['szWindCode'], 'nActionDay': time.strftime("%Y.%m.%d",time.strptime(row['nActionDay'], "%Y%m%d")), 'nTime': row['nTime'], 'nOpen': row['nOpen'],
#          'nHigh':row['nHigh'], 'nLow': row['nLow'], 'nMatch':row['nMatch'], 'iVolume': row['iVolume'], 'iTurnover':row['iTurnover'] , 'nNumTrades': row['nNumTrades'],
#          'bar_close': str(row['bar_close']).replace("True",'true'), 'S_DQ_PRECLOSE':row['S_DQ_PRECLOSE'] , 'S_DQ_ADJFACTOR':row['S_DQ_ADJFACTOR'], 'HighLimit':row['HighLimit'], 'LowLimit': row['LowLimit']})
#
#     return columns

# file_path = '/home/banruo/symbol/20190624'
# all_column_list=list()
# for i in os.listdir(file_path):
#     all_column_list.extend(generate_insert_args(os.path.join(file_path,i)))
#
# x = mycol.insert_many(all_column_list)
# print(x)

# for i in mycol.find({},{'_id':0}):
#     print(i)

# print(mycol.find())
# print(mycol.count())