import pymysql
import argparse
import warnings
import pandas as pd
import time
import os

warnings.filterwarnings('ignore')

class _DbConnector(object):
    def __init__(self, host, user, password, database):
        self._conn = pymysql.connect(host, user, password, database)
        #self._conn = pymysql.connect(host, user, password, database)
        self._cursor = self._conn.cursor()

    def update(self, sqls):
        """Parameter sqls: [(sql statement, (value))]"""
        if not sqls:
            return
        try:
            for sql_values in sqls:
                self._cursor.execute(*sql_values)
            self._conn.commit()
        except:
            self._conn.rollback()
            raise

    def query_and_fetch(self, sql_values):
        """Parameter sql_values: (sql statement, (value))"""
        self._cursor.execute(*sql_values)
        return self._cursor.fetchall()

def update_symbol_tb(conn: _DbConnector ,refData):
    sqls = []
    symbol_df = pd.read_csv(refData)
    sql = "INSERT IGNORE INTO symbol(symbol, expireDate) VALUES (%s,%s)"
    for k, v in symbol_df.iterrows():
        symbol, expireDate = v['symbol'], v['expireDate']
        sqls.append((sql, (symbol, expireDate)))

    conn.update(sqls)

def update_virsym_tb(conn: _DbConnector, futures, today):
    sqls = []
    virsym_df = pd.read_csv(futures)
    ins_sql = "INSERT IGNORE INTO virsym(prod, symbol, date) VALUES (%s,%s,%s)"
    query_sql = "SELECT max(symbol) FROM virsym GROUP BY prod HAVING prod=%s"

    for k, v in virsym_df.iterrows():
        prod, symbol = v['prod'], v['symbol']
        db_max_symbol = conn.query_and_fetch((query_sql,prod))

        if db_max_symbol:
            if symbol > db_max_symbol[0][0]:
                sqls.append((ins_sql, (prod, symbol, today)))
        else:
            sqls.append((ins_sql, (prod, symbol, today)))

    conn.update(sqls)

parser = argparse.ArgumentParser(description="futures-vir.csv refData.csv")
parser.add_argument('--futures-path', required=True, help='futures-vir.csv')
parser.add_argument('--refData-path', required=True, help='refData.csv')
parser.add_argument('--date', default=time.strftime('%Y%m%d', time.localtime()), help='#: 20201228')
parser.add_argument('--db-host', default='192.168.10.101', help='db host name')
parser.add_argument('--db-name', default='ctp-vir-sym', help='db name')
parser.add_argument('--db-user', default='root', help='db user name')
# parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
parser.add_argument('--db-pw', default='123456', help='db password filename')
args = parser.parse_args()

conn = _DbConnector(host=args.db_host,user=args.db_user,password=args.db_pw, database=args.db_name)

futures = args.futures_path
refData = args.refData_path
today = args.date

if os.path.isfile(futures):
    update_virsym_tb(conn, futures, today)
else:
    raise FileNotFoundError('{} NotFoundError'.format(futures))

if os.path.isfile(refData):
    update_symbol_tb(conn, refData)
else:
    raise FileNotFoundError('{} NotFoundError'.format(refData))
