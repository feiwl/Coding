__author__ = 'ZhouTW'

import pandas as pd
import pymongo
import datetime
import os
from multiprocessing import Process, Queue
from concurrent import futures
pd.set_option('display.max_columns', 50)

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

def calculation(chunk):
    chunk = pd.DataFrame(chunk)
    return chunk

def stock_snap_from_open(begin_date: str, end_date: str, target_time: int, l1):
    # get database collection
    myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
    mydb = myclient["marketdata"]
    # query
    query_ = {'nActionDay': {'$gte': begin_date, '$lte': end_date},
              'nTime': target_time}
    db_query = mydb['snap_from_open_stock'].find(query_, {'_id': 0, 'nTime': 0})

    # chunks = yield_rows(db_query, 100000)
    # executor = futures.ProcessPoolExecutor()
    #
    # fs = list()
    # result_dataframe = pd.DataFrame()
    # for chunk in chunks:
    #     f = executor.submit(calculation, chunk)
    #     fs.append(f)
    #
    # for f in fs:
    #     result_dataframe = result_dataframe.append(f.result(), ignore_index=True)

    begin_time = datetime.datetime.now()
    print('complete query stock from open at %s' % str(begin_time))
    result = pd.DataFrame(db_query)
    end_time = datetime.datetime.now()
    print('complete transform from-open info to dataframe at %s' % str(end_time))
    # close the client
    myclient.close()
    # store the result to queue
    l1.put(result)
    return result


def stock_snap_to_close(begin_date: str, end_date: str, target_time: int, l2):
    # get database collection
    myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
    mydb = myclient["marketdata"]
    # query
    query_ = {'nActionDay': {'$gte': begin_date, '$lte': end_date},
              'nTime': target_time}
    db_query = mydb['snap_to_close_stock'].find(query_, {'_id': 0, 'nTime': 0})

    # chunks = yield_rows(db_query, 100000)
    # executor = futures.ProcessPoolExecutor(max_workers=10)
    #
    # fs = list()
    # result_dataframe = pd.DataFrame()
    # for chunk in chunks:
    #     f = executor.submit(calculation, chunk)
    #     fs.append(f)
    #
    # for f in fs:
    #     result_dataframe = result_dataframe.append(f.result(), ignore_index=True)

    begin_time = datetime.datetime.now()
    print('complete query stock to close at %s' % str(begin_time))
    result = pd.DataFrame(db_query)
    end_time = datetime.datetime.now()
    print('complete transform to-close info to dataframe at %s' % str(end_time))
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
    # fopen_p = Process(target=stock_snap_from_open, args=(begin_date, end_date, one_minute_before, fopen_q, ))
    fopen_p = Process(target=stock_snap_from_open, args=(begin_date, end_date, target_time, fopen_q,))
    tclose_q = Queue()
    tclose_p = Process(target=stock_snap_to_close, args=(begin_date, end_date, target_time, tclose_q, ))
    fopen_p.start()
    tclose_p.start()

    return tclose_q.get(), fopen_q.get()


if __name__ == '__main__':
    begin_date = '20190101'
    end_date = '20201005'
    target_time = int(1400e5)
    begin_time_0 = datetime.datetime.now()
    A_frame, B_frame = new_stock_snap_A_B(begin_date, end_date, target_time)
    print(datetime.datetime.now() - begin_time_0)
    print(A_frame)
    print(B_frame)
