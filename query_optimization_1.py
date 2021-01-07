import pymongo
import datetime
import pandas as pd
from concurrent import futures

myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")

mydb = myclient["marketdata"]
mycol = mydb["snap_to_close_stock"]


begin_time = datetime.datetime.now()
print(begin_time)

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

myquery = {'nActionDay':{'$gte': "20190101", '$lte': "20201001"}, "nTime":1400e5}
mydoc = mycol.find(myquery, {'_id':0, 'nTime':0}, batch_size = 50000)
chunks = yield_rows(mydoc, 50000)


executor = futures.ProcessPoolExecutor(max_workers=10)

fs = list()
result_dataframe = pd.DataFrame()
for chunk in chunks:
    f = executor.submit(calculation, chunk)
    fs.append(f)

for f in fs:
    result_dataframe = result_dataframe.append(f.result(), ignore_index=True)

print(result_dataframe)

end_time = datetime.datetime.now()
print(end_time)
print(end_time - begin_time)
