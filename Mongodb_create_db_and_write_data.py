import pandas as pd
import pymongo

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

myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
mycollection = myclient["marketdata"]['transaction'].find({'nActionDay':{'$gte': '20190802','$lte': '20191101'}} ,{'_id':0, 'nActionDay':1, 'szWindCode':1, 'nTime': 1, 'nNumTrades':1})
df = pd.DataFrame(mycollection)

table = pd.pivot_table(df, values='nNumTrades', index=['nActionDay', 'nTime'],columns=['szWindCode'])

table.fillna(value=0, inplace=True)
table = table.astype("int")

table.reset_index(inplace=True)

nHigh_mongo_sqls = []
all_columns_name = table.columns

for _, row in table.iterrows():
    current_dict = {}
    new_sym_name = ''
    for sym_name in all_columns_name:

        if len(sym_name) == 9:
            new_sym_name = sym_name.split('.')[1]+sym_name.split('.')[0]
        else:
            new_sym_name = sym_name

        current_dict.update({new_sym_name:row[sym_name]})
    nHigh_mongo_sqls.append(current_dict)

conn = _DbConnector_mongo("mongodb://192.168.10.68:27017", "min_bar", "min_bar_nNumTrades")
conn.update(nHigh_mongo_sqls)
