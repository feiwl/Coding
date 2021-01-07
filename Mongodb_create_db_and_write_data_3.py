from multiprocessing import Pool
import pandas as pd
import datetime
import pymongo
import math

class _DbConnector_mongo:
    def __init__(self, host, database, table):
        self._myclient = pymongo.MongoClient(host)
        self._mydb = self._myclient[database]
        self.mycol = self._mydb[table]

    def update(self, sql):
        """Parameter sql: {"": ""} and {"": "", "": "", ...}"""
        self.mycol.insert_many(sql)
        self.mycol.create_index([("nActionDay", 1), ("nTime", 1)])

    def query_and_fetch(self, sql):
        """Parameter sql: {"" : ""}"""
        mydoc = self.mycol.find_one(sql)
        return mydoc

def table_split_into_multiple_tables(begin_date, end_date, Field_table_mapping):

    myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
    mycollection = myclient["marketdata"]["transaction"]
    filter = {'nActionDay':{'$gte': '{}'.format(begin_date),'$lte': '{}'.format(end_date)}}
    projection = {'_id':0, 'nActionDay':1, 'szWindCode':1, 'nTime': 1, 'nOpen':1,'nHigh':1,'nLow':1, 'nMatch':1, 'iVolume':1, 'iTurnover':1, 'nNumTrades':1}

    mycollection = mycollection.find(filter, projection)

    for record in mycollection:


    nHigh_mongo_sqls = []

    conn = _DbConnector_mongo("mongodb://192.168.10.68:27017", "min_bar", "{}".format(table_name))
    conn.update(nHigh_mongo_sqls)

data = {'min_bar_nHigh':{'column':[1,2,3]}}


if __name__ == "__main__":

    Field_table_mapping = {
        "nOpen": "min_bar_open",
        "nHigh": "min_bar_nHigh",
        "nLow": "min_bar_nLow",
        "nMatch": "min_bar_nMatch",
        "iVolume": "min_bar_iVolume",
        "iTurnover": "min_bar_iTurnover",
        "nNumTrades": "min_bar_nNumTrades"
    }

    gte_date = '20190302'
    lte_date = '20190601'
    begin_time = datetime.datetime.now()
    print(begin_time)

    end_time = datetime.datetime.now()
    print(end_time)
    print(end_time - begin_time)
