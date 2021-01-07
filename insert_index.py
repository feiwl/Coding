import time
import argparse
import os
import pandas as pd
import pymysql
import warnings

warnings.filterwarnings("ignore", category=pymysql.Warning)

class _DbConnector(object):
    def __init__(self, host, user, password, database):
        self._conn = pymysql.connect(host, user, password, database)
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

def generate_symbol_select(conn) -> list:
    sql = "SELECT szWindCode from symbol"
    return list(map(lambda field: field[0],conn.query_and_fetch([sql])))

def generate_symbols_args(conn,symbols):
    sym_sql = 'INSERT INTO symbol(szWindCode) VALUES (%s)'
    conn.update(map(lambda sym: (sym_sql, (sym)), symbols))

# 批次INSERT sql
def generate_records_args(conn, records):
    sym_sql = "INSERT INTO snap_min_index  (codeid, nActionDay, nTime,nOpenIndex,nHighIndex,nLowIndex,nLastIndex,iTotalVolume,iTurnover,nPreCloseIndex)"\
              "VALUES ((SELECT codeId FROM symbol WHERE szWindCode = %s),%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    conn.update(map(lambda sym: (sym_sql, (sym)), records))

# 获取hdf文件的单行列表及symbol
def generate_insert_args(filename):
    df = pd.read_hdf(filename)
    record = list()
    symbol = ''
    for index, row in df.iterrows():
        if type(row['szWindCode']) != float:
            if index == 0:
                symbol = row['szWindCode']
            record.append([row['szWindCode'],time.strftime("%Y.%m.%d",time.strptime(str(int(row['nActionDay'])), "%Y%m%d")), row['nTime'],row['nOpenIndex'],
            row['nHighIndex'],row['nLowIndex'],row['nLastIndex'],row['iTotalVolume'],row['iTurnover'],row['nPreCloseIndex']])
    return symbol, record

def record_director(path):
    symbols = []
    records = []
    for file in os.listdir(path):
        symbol, record = generate_insert_args(os.path.join(path, file))
        symbols.append(symbol)
        records.extend(record)
    return symbols,records

parser = argparse.ArgumentParser(description="Insert_TRANSACTION")
parser.add_argument('--db-host', default='localhost', help='db host name')
parser.add_argument('--db-name', required=True, help='db name')
parser.add_argument('--db-user', default='writer', help='db user name')
parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
parser.add_argument('--input', required=True, help='hdf5 directory')
args = parser.parse_args()

conn = _DbConnector(args.db_host, args.db_user, args.db_pw.readline().strip('\n'), args.db_name)
symbols,records = record_director(args.input)
DB_symbols = generate_symbol_select(conn)
symbols = list(set(symbols)-set(DB_symbols))
generate_symbols_args(conn,symbols)
generate_records_args(conn,records)