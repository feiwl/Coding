#!/usr/bin/env python3

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
    sym_sql = 'INSERT IGNORE INTO symbol(szWindCode) VALUES (%s)'
    conn.update(map(lambda sym: (sym_sql, (sym)), symbols))

def generate_records_args(conn, records):
    sym_sql = 'INSERT INTO transaction (codeid, nActionDay, nTime, nOpen, nHigh, nLow, nMatch, iVolume, iTurnover, nNumTrades, barClose, S_DQ_PRECLOSE, S_DQ_ADJFACTOR, HighLimit, LowLimit) ' \
              'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    conn.update(map(lambda sym: (sym_sql, (sym)), records))

def generate_insert_args(filename):
    df = pd.read_hdf(filename)
    record = list()
    for index, row in df.iterrows():
        if index == 0:
            symbol = row['szWindCode']
        record.append([row['szWindCode'], row['nActionDay'], row['nTime'], row['nOpen'], row['nHigh'], row['nLow'], row['nMatch'], row['iVolume'],row['iTurnover'],
                       row['nNumTrades'], row['bar_close'], row['S_DQ_PRECLOSE'], row['S_DQ_ADJFACTOR'], row['HighLimit'], row['LowLimit']])
    return symbol, record

def record_director(path):
    symbols = []
    records = []
    for file in os.listdir(path):
        symbol, record = generate_insert_args(os.path.join(path, file))
        symbols.append(symbol)
        records.extend(record)
    return symbols,records

#
parser = argparse.ArgumentParser(description="Insert_TRANSACTION")
parser.add_argument('--db-host', default='localhost', help='db host name')
parser.add_argument('--db-name', required=True, help='db name')
parser.add_argument('--db-user', default='writer', help='db user name')
parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
parser.add_argument('--input', required=True, help='hdf5 directory')
args = parser.parse_args()

symbols,records = record_director(args.input)
conn = _DbConnector(args.db_host, args.db_user, args.db_pw.readline().strip('\n'), args.db_name)
# generate_symbols_args(conn,symbols)
generate_records_args(conn,records)



# hdf_file='/home/banruo/symbol/20190624'
# symbols,records = record_director(hdf_file)
# conn = _DbConnector('192.168.10.101','root','123456','marketdata')
# generate_records_args(conn,records)
# # print(generate_records_args(conn,generate_insert_args(hdf_file)[1]))





