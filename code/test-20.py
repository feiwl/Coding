#!/usr/bin/env python3

import argparse
import pandas as pd
import pymysql
import os

def generate_symbols_args(conn,symbols):
    sym_sql = 'INSERT IGNORE INTO symbol(szWindCode) VALUES (%s)'
    conn.update(map(lambda sym: (sym_sql, (sym)), symbols))

def generate_records_args(conn, records):
    sym_sql = "INSERT INTO TRANSACTION (codeid, nActionDay, nTime, nOpen, nHigh, nMatch, iVolume, iTurnover, barClose, S_DQ_PRECLOSE, S_DQ_ADJFACTOR, HighLimit, LowLimit) " \
              "VALUES ((SELECT codeId FROM symbol WHERE szWindCode = %s), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    conn.update(map(lambda sym: (sym_sql, (sym)), records))

def generate_insert_args(filename):
    df = pd.read_hdf(filename)
    record = list()
    for index, row in df.iterrows():
        if index == 0:
            symbol = row['szWindCode']
        record.append([row['szWindCode'], row['nActionDay'], row['nTime'], row['nOpen'], row['nHigh'], row['nMatch'], row['iVolume'],row['iTurnover'], row['bar_close'], row['S_DQ_PRECLOSE'], row['S_DQ_ADJFACTOR'], row['HighLimit'], row['LowLimit']])
    return symbol, record

def record_director(path):
    symbols = []
    records = []
    symbol, record = generate_insert_args(path)
    symbols.append(symbol)
    records.extend(record)
    return symbols,records

path='/home/banruo/symbol/20200612/600408.SH_20200612.hdf5'

# symbols,records = record_director(path)
# generate_symbols_args(conn,symbols)
# generate_records_args(conn,records)

# print(symbols)
# print(records)

print(pd.DataFrame(pd.read_hdf(path)))