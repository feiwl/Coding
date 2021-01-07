import os
import pymongo
import pandas as pd
from watchdog.events import *
from watchdog.observers import Observer

myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")

class _DbConnector_mongo:
    def __init__(self, host, database, table):
        self._myclient = pymongo.MongoClient(host)
        self._mydb = self._myclient[database]
        self.mycol = self._mydb[table]

    def update(self, sql):
        """Parameter sql: {"": ""} and {"": "", "": "", ...}"""
        self.mycol.insert_many(sql)

    def query_and_fetch(self, sql):
        """Parameter sql: {"" : ""}"""
        mydoc = self.mycol.find(sql)
        return mydoc

# 实时监控目录
class FileMonitorHandler(FileSystemEventHandler):
    def __init__(self, conn, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        # 监控目录
        self.conn = conn

    # 重写文件改变函数，文件改变都会触发文件夹变
    def on_modified(self, event):
        if not event.is_directory:  # 文件改变都会触发文件夹变化
            print('File is move {}'.format(event))

class Insert_tran_mongo:
    def __init__(self, conn, date_file, tran_path):
        self._conn = conn
        self._date_file = date_file
        self._tran_path = tran_path

    def generate_insert_args(self, tran_path) -> list:
        sentences = list()
        if os.path.isdir(tran_path):
            for file in os.listdir(tran_path):
                df = pd.read_hdf(os.path.join(tran_path,file))
                for index, row in df.iterrows():
                    sentences.append({'nTime':row['nTime'], 'nOpen':row['nOpen'], 'nHigh':row['nHigh'], 'nLow':row['nLow'],
                                    'nMatch':row['nMatch'], 'iVolume':row['iVolume'], 'iTurnover':row['iTurnover'],
                                    'nNumTrades':row['nNumTrades'], 'bar_close':row['bar_close'], 'szWindCode':row['szWindCode'], 'nActionDay':row['nActionDay']})
        return sentences

    def get_latest_date(self, date_file, conn) -> list:
        dates = list()
        with open(date_file, 'r') as f:
            dl = sorted(list(map(lambda line: line.replace("\n", ""), f)),reverse=True)
        for i in dl:
            query_result = conn.query_and_fetch({"nActionDay": "{}".format(i)})
            if query_result:
                dates.append(i)
            else:
                return dates


# path="/home/banruo/T2"
# if __name__ == "__main__":
#     event_handler = FileMonitorHandler(conn)
#     observer = Observer()
#     observer.schedule(event_handler, path=path, recursive=True)  # recursive递归
#     observer.start()
#     observer.join()
