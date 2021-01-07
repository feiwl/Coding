__author__ = 'ZhouTW'

import pandas as pd
import pymongo
import datetime
import os
import math
from multiprocessing import Process, Queue, Pool

pd.set_option('display.max_columns', 50)

def stock_snap_task(collection, filter, projection, skip, limit):
    # print(skip, limit)
    myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
    mycollection = myclient["marketdata"][collection].find(filter, projection).skip(skip).limit(limit)
    result = pd.DataFrame(mycollection)
    print("skip: %d limit: %d count: %d" % (skip, limit, len(result)))
    myclient.close()
    return result

def stock_snap_from_open(begin_date: str, end_date: str, target_time: int, l1):
    myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
    mycollection = myclient["marketdata"]["transaction"]
    # mycollection = myclient["marketdata"]["snap_from_open_stock"]
    # filter = {'nActionDay': {'$gte': begin_date, '$lte': end_date},
    #           'nTime': target_time}
    filter = {'nActionDay': {'$gte': begin_date, '$lte': end_date}, 'nTime': target_time}
    projection = {'_id': 0, 'szWindCode': 1,'nActionDay': 1,'nTime': 1,'nOpen': 1,'nHigh': 1,'nLow': 1,'nMatch': 1,'iVolume': 1,'iTurnover': 1,'bar_close': 1}

    begin_time = datetime.datetime.now()
    print('complete query stock to close at %s' % str(begin_time))

    count = mycollection.find(filter, projection).count()
    limit = int(1e5)
    process_count = math.ceil(count/limit)
    print(process_count)
    # print("count: %d process_count: %d" % (count, process_count))

    pool = Pool()
    results = []
    for i in range(process_count):
        results.append(pool.apply_async(stock_snap_task, args=("transaction",filter, projection, i*limit, limit)))
    pool.close()
    pool.join()

    result = pd.DataFrame()
    for item in results:
        result = result.append(item.get())

    end_time = datetime.datetime.now()
    print('complete transform from-open info to dataframe at %s' % str(end_time - end_time))

    # close the client
    myclient.close()
    # store the result to queue
    l1.put(result)
    return result


def stock_snap_to_close(begin_date: str, end_date: str, target_time: int, l2):

    myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
    mycollection = myclient["min_bar"]["min_bar_nLow"]
    filter = {'nActionDay': {'$gte': begin_date, '$lte': end_date},
              'nTime': target_time}
    projection = {'_id': 0, 'nTime': 0}

    begin_time = datetime.datetime.now()
    print('complete query stock to close at %s' % str(begin_time))

    count = mycollection.find(filter, projection).count()
    limit = int(1e5)
    process_count = math.ceil(count/limit)
    # print("count: %d process_count: %d" % (count, process_count))

    pool = Pool()
    results = []
    for i in range(process_count):
        results.append(pool.apply_async(stock_snap_task, args=("min_bar_nLow" ,filter, projection, i*limit, limit)))
    pool.close()
    pool.join()

    result = pd.DataFrame()
    for item in results:
        result = result.append(item.get())

    end_time = datetime.datetime.now()
    print('complete transform to-close info to dataframe at %s' % str(end_time - begin_time))
    # close the client
    myclient.close()
    # store the result to queue
    l2.put(result)
    return result


def new_stock_snap_A_B(begin_date: str, end_date: str, target_time: int):
    # find minute before the target time
    # min_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
    #                                         os.pardir, os.pardir,
    #                                         './config/minute_records/tran_stock_minute_list.txt'))
    # with open(min_path) as file:
    #     minute_list = file.readlines()
    #     minute_list = [int(item.rstrip('\n')) for item in minute_list]
    # one_minute_before = minute_list[minute_list.index(target_time) - 1]

    # start to get frame multiprocess
    fopen_q = Queue()
    fopen_p = Process(target=stock_snap_from_open, args=(begin_date, end_date, target_time, fopen_q, ))
    # tclose_q = Queue()
    # tclose_p = Process(target=stock_snap_to_close, args=(begin_date, end_date, target_time, tclose_q, ))
    fopen_p.start()
    # tclose_p.start()
    # return tclose_q.get(), fopen_q.get()
    return  fopen_q.get()


if __name__ == '__main__':
    begin_date = '20190101'
    end_date = '20201005'
    target_time = int(1400e5)
    begin_time_0 = datetime.datetime.now()
    B_frame = new_stock_snap_A_B(begin_date, end_date, target_time)
    print(datetime.datetime.now() - begin_time_0)
    print(B_frame)
