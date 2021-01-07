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

def stock_snap_task(collection, filter, projection, skip, limit):
    myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
    mycollection = myclient["marketdata"][collection].find(filter, projection).skip(skip).limit(limit)
    result = pd.DataFrame(mycollection)
    print("skip: %d limit: %d count: %d" % (skip, limit, len(result)))
    myclient.close()
    return result

def stock_snap_from_open(begin_date: str, end_date: str):
    myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
    mycollection = myclient["marketdata"]["transaction"]
    filter = {'nActionDay':{'$gte': '{}'.format(begin_date),'$lte': '{}'.format(end_date)}}
    projection = {'_id':0, 'nActionDay':1, 'szWindCode':1, 'nTime': 1, 'nOpen':1,'nHigh':1,'nLow':1, 'nMatch':1, 'iVolume':1, 'iTurnover':1, 'nNumTrades':1}

    count = mycollection.find(filter,projection).count()
    limit = int(1e5)
    process_count = math.ceil(count/limit)
    print(process_count)

    pool = Pool()
    results = []
    for i in range(process_count):
        results.append(pool.apply_async(stock_snap_task, args=("transaction",filter, projection, i*limit, limit)))
    pool.close()
    pool.join()

    result = pd.DataFrame()
    for item in results:
        result = result.append(item.get())

    myclient.close()

    return result

def table_split_into_multiple_tables(transaction_df, table_name, field):

    table = pd.pivot_table(transaction_df, values='{}'.format(field), index=['nActionDay', 'nTime'],columns=['szWindCode'])

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

    conn = _DbConnector_mongo("mongodb://192.168.10.68:27017", "min_bar", "{}".format(table_name))
    conn.update(nHigh_mongo_sqls)

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
    transaction_df = stock_snap_from_open(gte_date, lte_date)

    pool = Pool()
    for field, table_name in Field_table_mapping.items():
        pool.apply_async(table_split_into_multiple_tables, args=(transaction_df, table_name, field))

    end_time = datetime.datetime.now()
    print(end_time)
    print(end_time - begin_time)
