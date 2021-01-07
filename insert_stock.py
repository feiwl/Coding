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

def generate_symbols_args(conn,symbols):
    sym_sql = 'INSERT INTO symbol(szWindCode) VALUES (%s)'
    conn.update(map(lambda sym: (sym_sql, (sym)), symbols))

def generate_symbol_select(conn) -> list:
    sql = "SELECT szWindCode from symbol"
    return list(map(lambda field: field[0],conn.query_and_fetch([sql])))

# 批次 INSERT sql 字段
def generate_records_args(conn, records):
    sym_sql = "INSERT INTO snap_min_stock  (codeid, nActionDay, nTime, nPreClose, nOpen, nHigh,nLow, ap10, av10, ap9, av9, ap8, av8, ap7, av7," \
              "ap6,av6, ap5, av5, ap4, av4, ap3, av3, ap2, av2, ap1, av1, nMatch, bp1, bv1,bp2, bv2, bp3, bv3, bp4, bv4," \
              "bp5, bv5, bp6, bv6, bp7, bv7, bp8, bv8, bp9, bv9, bp10, bv10, nNumTrades, iVolume, iTurnover, nTotalBidVol," \
              "nTotalAskVol, nWeightedAvgBidPrice, nWeightedAvgAskPrice)"\
              "VALUES ((SELECT codeId FROM symbol WHERE szWindCode = %s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    conn.update(map(lambda sym: (sym_sql, (sym)), records))

# 获取hdf文件的单行列表及symbol
def generate_insert_args(filename) -> tuple:
    df = pd.read_hdf(filename)
    record = list()
    symbol = ''
    for index, row in df.iterrows():
        if type(row['szWindCode']) != float:
            if index == 0:
                symbol = row['szWindCode']
            record.append([row['szWindCode'],time.strftime("%Y.%m.%d",time.strptime(str(int(row['nActionDay'])), "%Y%m%d")), row['nTime'],row['nPreClose'],
             row['nOpen'], row['nHigh'],row['nLow'],row['ap10'],row['av10'],row['ap9'],row['av9'],
             row['ap8'], row['av8'] , row['ap7'], row['av7'], row['ap6'],  row['av6'], row['ap5'],row['av5'],row['ap4'],
            row['av4'],row['ap3'],row['av3'],row['ap2'],row['av2'],row['ap1'],row['av1'],row['nMatch'],row['bp1'],row['bv1'],row['bp2'],row['bv2'],
             row['bp3'],row['bv3'],row['bp4'],row['bv4'],row['bp5'],row['bv5'],row['bp6'],row['bv6'],row['bp7'],row['bv7'],row['bp8'],row['bv8'],row['bp9'],
           row['bv9'],row['bp10'],row['bv10'],row['nNumTrades'],row['iVolume'],row['iTurnover'],row['nTotalBidVol'],row['nTotalAskVol'],
           row['nWeightedAvgBidPrice'],row['nWeightedAvgAskPrice']])
    return symbol, record

def record_director(path) -> tuple:
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
