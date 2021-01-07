import pymongo
import datetime
import pandas as pd
from concurrent import futures

class _DbConnector_Mongo:
    def __init__(self, host, database, table):
        self._myclient = pymongo.MongoClient(host)
        self._mydb = self._myclient[database]
        self.mycol = self._mydb[table]

    def update(self, sql):
        """Parameter sql: {"": ""} and {"": "", "": "", ...}"""
        self.mycol.insert_many(sql)

    def query_and_fetch(self, sql):
        """Parameter sql: {"" : ""}"""
        mydoc = self.mycol.find(sql, {'_id': 0, 'nTime':0})
        return mydoc

def computing_time(begin_date, end_date) -> list:
    days = None
    num_process = None
    current_date = None
    total_date = list()
    begin_date = datetime.datetime.strptime(begin_date, '%Y%m%d')
    end_date = datetime.datetime.strptime(end_date, '%Y%m%d')

    for days in [10, 15, 20, 25, 30]:
        num_process = int((end_date - begin_date).days // days)
        if num_process < 1:
            days = int((end_date - begin_date).days % days)
            num_process = 1
            break
        elif num_process <= 64:
            days = days
            break
    for _ in range(num_process):
        current_date = begin_date + datetime.timedelta(days)
        if current_date.strftime('%Y%m%d') >= end_date.strftime('%Y%m%d'):
            total_date.append((begin_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')))
            break
        total_date.append((begin_date.strftime('%Y%m%d'), current_date.strftime('%Y%m%d')))
        begin_date = current_date + datetime.timedelta(1)
    if current_date.strftime('%Y%m%d') < end_date.strftime('%Y%m%d'):
        total_date.append((begin_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')))

    return total_date

def worker(ntime, *args):
    conn = _DbConnector_Mongo("mongodb://192.168.10.68:27017", "marketdata", "transaction")
    begin_date, end_date = args
    myquery = {'nActionDay':{'$gte': begin_date,'$lte': end_date}, 'nTime': ntime}
    mydoc = conn.query_and_fetch(myquery)
    return  pd.DataFrame(mydoc)

if __name__ == "__main__":
    begin_time = datetime.datetime.now()
    print(begin_time)

    begin_date = '20190101'
    end_date = '20201201'
    ntime = 1400e5
    result_dataframe = pd.DataFrame()
    begin_end_date_lst = computing_time(begin_date, end_date)

    print(begin_end_date_lst)

    executor = futures.ProcessPoolExecutor(max_workers=len(begin_end_date_lst))
    fs = list()
    for begin_end in begin_end_date_lst:
        f = executor.submit(worker, ntime, *begin_end)
        fs.append(f)

    for f in fs:
        result_dataframe = result_dataframe.append(f.result(), ignore_index=True)

    print(result_dataframe)
    end_time = datetime.datetime.now()
    print(end_time)
    print(end_time - begin_time)

