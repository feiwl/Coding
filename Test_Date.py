import pymysql
from queue import Queue
import threading

class ConnPool:
    def __init__(self, size, *args, **kwargs):
        self._size = size
        self._pool = Queue(size)
        self.local = threading.local()
        for i in range(size):
            conn = pymysql.connect(*args, **kwargs)
            self._pool.put(conn)

    def get_conn(self):
        return self._pool.get()

    def return_conn(self, conn:pymysql.connections.Connection):
        if isinstance(conn, pymysql.connections.Connection):
            self._pool.put(conn)

    @property
    def size(self):
        return self._pool.qsize()

    def __enter__(self):
        return self.get_conn()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.return_conn()

pool = ConnPool(5, '192.168.142.135', 'wayne', 'wayne', 'test')

conn = pool.get_conn()
print(conn)
print(pool.size)



