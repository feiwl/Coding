from queue import Queue
import pymysql
import threading


class ConnPool:
    def __init__(self, size, *args, **kwargs):
        if not isinstance(size, int) or size < 1:
            size = 8
        self._pool = Queue(size)
        for i in range(size):
            self._pool.put(pymysql.connect(*args, **kwargs))
        self.local = threading.local()

    def get_conn(self):
        return self._pool.get()  # 阻塞

    def return_conn(self, conn):
        self._pool.put(conn)

    def __enter__(self):
        # self.local.conn 在当前线程不存在抛属性异常
        if getattr(self.local, 'conn', None) is None:
            self.local.conn = self.get_conn()
        return self.local.conn.cursor()  # 返回一个游标

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.local.conn.rollback()
        else:
            self.local.conn.commit()
        self.return_conn(self.local_conn)
        self.local.conn = None


# 使用连接池
pool = ConnPool(4, '192.168.142.135', 'wayne', 'wayne', 'school')

with pool as cursor:  # 自动拿连接并归还, 还自动提交或回滚
    with cursor:
        sql = "select * from student"
        cursor.execute(sql)
        print(cursor.fetchone())

        sql = "SHOW PROCESSLIST"  # 观察连接, 权限小只能看自己的
        cursor.execute(sql)
        for x in cursor:
            print(x)