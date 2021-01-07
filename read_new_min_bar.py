from concurrent import futures
import datetime
import pymongo
import pandas as pd

def query_min_bar_field(table, begin_date, end_date, target_time):
    myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
    mycollection = myclient["min_bar"]['{}'.format(table)].\
       find({'nActionDay':{'$gte': '{}'.format(begin_date),'$lte': '{}'.format(end_date)}
                                   ,"nTime": target_time}, {'_id':0, 'nTime': 0})
    if mycollection:
        df = pd.DataFrame(mycollection)
        return df

if __name__ == "__main__":

    begin_time = datetime.datetime.now()
    print(begin_time)

    min_bar_all_table = ['min_bar_nHigh', 'min_bar_open', 'min_bar_nLow', 'min_bar_nMatch', 'min_bar_iVolume','min_bar_iTurnover', 'min_bar_nNumTrades']
    # min_bar_all_table = ['min_bar_iTurnover']

    begin_date = 20190101
    end_date = 20201231
    target_time = 132000000

    fs = []
    executor = futures.ProcessPoolExecutor(len(min_bar_all_table))
    for table in min_bar_all_table:
        f = executor.submit(query_min_bar_field, table, begin_date, end_date, target_time)
        fs.append(f)

    for f in fs:
        print(f.result())

    end_time = datetime.datetime.now()
    print(end_time)
    print(end_time - begin_time)