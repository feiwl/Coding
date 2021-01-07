import pymongo
from pymongo import ASCENDING, DESCENDING
from decimal import Decimal
import time
import pandas as pd
import os
import datetime

myclient = pymongo.MongoClient("mongodb://192.168.10.102:40000")
# print(myclient.list_database_names())

# create database marketdata (use marketdata)
# mydb = myclient["marketdata"]
# create collection transaction (db.createCollection("mycol", {options}))
# mycol = mydb["sites"]
# show collections
# mydb.list_collection_names()

# mylist = [
#   { "name": "Taobao", "alexa": "100", "url": "https://www.taobao.com" },
#   { "name": "QQ", "alexa": "101", "url": "https://www.qq.com" },
#   { "name": "Facebook", "alexa": "10", "url": "https://www.facebook.com" },
#   { "name": "知乎", "alexa": "103", "url": "https://www.zhihu.com" },
#   { "name": "Github", "alexa": "109", "url": "https://www.github.com" }
# ]
mydb = myclient["marketdata"]
mycol = mydb["transaction"]
# mycol.create_index([("szWindCode",DESCENDING),("nActionDay",ASCENDING)])
# mycol.create_index([("szWindCode",1)])
# mycol.create_index([("nActionDay",1)])
# mycol.create_index([("nTime",1)])
# print(mycol.index_information())

# data = {'szWindCode': '000017.SZ', 'nActionDay': '2020.08.07', 'nTime': 145400000, 'nOpen': 26900, 'nHigh': 27000, 'nLow': 26900, 'nMatch': 26900, 'iVolume': 13700, 'iTurnover': 368640000, 'nNumTrades': 6, 'bar_close': True, 'S_DQ_PRECLOSE': 27800, 'S_DQ_ADJFACTOR': 2.682736, 'HighLimit': 29200, 'LowLimit': 26400}


# mycol.insert_one(data)

myquery = {"nActionDay": '2020.08.07'}
data = mycol.find(myquery).explain()
print(data)
# for i in data:
#     print(i)





