#!/usr/bin/env python3

import argparse
import json
import lzma
import re
import time
import warnings
import pymysql
from collections import defaultdict
from collections import Counter

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

def query_available_days(conn, now, offset):
    sql = "SELECT DATE_FORMAT(date, '%%Y%%m%%d') FROM tradingDay WHERE date <= %s ORDER BY date DESC LIMIT 1 OFFSET %s"
    result = conn.query_and_fetch((sql, (now, offset)))
    return next(iter([date for dates in result for date in dates]), None)

def query_eod(conn, table, date):
    sql = "SELECT userName,symbol,volume FROM " + table + " INNER JOIN user using(uid) where date = %s"
    query_result = conn.query_and_fetch((sql, (date,)))
    result = defaultdict(dict)
    list(map(lambda r : result[r[0]].update({r[1]: r[2]}), query_result))
    return dict(result)

def query_delta(conn, table, date):
    sql = "SELECT userName,symbol,volumeDelta FROM " + table + " INNER JOIN user using(uid) where date = %s"
    query_result = conn.query_and_fetch((sql, (date,)))
    result = defaultdict(dict)
    list(map(lambda r : result[r[0]].update({r[1]: r[2]}), query_result))
    return dict(result)

def update_table(date, table, result):
    sql = "INSERT INTO " + table + "(uid, date, symbol, volumeDelta) VALUES ((SELECT uid FROM user WHERE userName = %s), %s, %s, %s)"
    sqls = []
    for user in filter(lambda user: len(result[user]), result):
        sqls.extend(map(lambda sym: (sql, (user, date, sym, result[user][sym])),
                    filter(lambda sym: result[user][sym] != 0, result[user])))
    return sqls

def update_eod(date, table, result):
    sql = "INSERT INTO " + table + "(uid, date, symbol, volume) VALUES ((SELECT uid FROM user WHERE userName = %s), %s, %s, %s)"
    sqls = []
    for user in filter(lambda user: len(result[user]), result):
        sqls.extend(map(lambda sym: (sql, (user, date, sym, result[user][sym])),
                    filter(lambda sym: result[user][sym] != 0, result[user])))
    return sqls

def merge_result(pigeon, parrot):
    _pigeon = defaultdict(dict, pigeon)
    _parrot = defaultdict(dict, parrot)
    result = {}
    for user in set().union(_pigeon.keys(), _parrot.keys()):
        scapegoat = Counter(_pigeon[user]).copy()
        scapegoat.update(Counter(_parrot[user]))
        result.update({user: dict(scapegoat)})
    clean_up = lambda user_item : (user_item[0], dict(filter(lambda sym_item: sym_item[1] != 0, user_item[1].items())))
    return dict(map(clean_up, result.items()))

# return positive and negative value.
def separate_result(eod_pigeon_parrot):
    positive = lambda sym_item: sym_item[1] >= 200 if sym_item[0].startswith('688') else sym_item[1] >= 100
    negative = lambda sym_item: sym_item[1] < 200 if sym_item[0].startswith('688') else sym_item[1] < 100
    positive_result = lambda user_item : (user_item[0], dict(filter(positive, user_item[1].items())))
    negative_result = lambda user_item : (user_item[0], dict(filter(negative, user_item[1].items())))
    return dict(map(positive_result, eod_pigeon_parrot.items())), dict(map(negative_result, eod_pigeon_parrot.items()))

def validate_positive(result):
    positive_check = lambda sym_item : sym_item[1] >= 200 if sym_item[0].startswith('688') else sym_item[1] >= 100
    if not all(map(positive_check, [y for user in result.keys() for y in result[user].items()])):
        raise ValueError('Negative volume is impossible')

def validate_negative(result):
    negative_check = lambda sym_item : sym_item[1] < 200 if sym_item[0].startswith('688') else sym_item[1] < 100
    if not all(map(negative_check, [y for user in result.keys() for y in result[user].items()])):
        raise ValueError('Positive volume is impossible')

def update_eod_pigeon_ostrich(conn, now, offset_one, manual):
    eod_parrot = query_eod(conn, 'eod_parrot', offset_one)
    pigeon = query_delta(conn, 'pigeon', now)
    eod_pigeon_ostrich = merge_result(eod_parrot, pigeon)
    eod_pigeon, ostrich = separate_result(eod_pigeon_ostrich)
    validate_positive(eod_pigeon)
    validate_negative(ostrich)
    if manual:
        ostrich = merge_result(ostrich, manual)
    conn.update(update_eod(now, 'eod_pigeon', eod_pigeon))
    conn.update(update_table(now, 'ostrich', ostrich))

def update_eod_parrot(conn, now, offset_one):    # T0 eod_parrot
    eod_pigeon = query_eod(conn, 'eod_pigeon', offset_one)
    parrot = query_delta(conn, 'parrot', now)
    ostrich = query_delta(conn, 'ostrich', now)
    eod_temp = merge_result(eod_pigeon, parrot)
    eod_parrot = merge_result(eod_temp, ostrich)
    validate_positive(eod_parrot)
    conn.update(update_eod(now, 'eod_parrot', eod_parrot))

def convert_manual(str):
    convert = lambda sym_item: tuple(int(i) if i.isdigit() else i for i in sym_item.split('@'))
    def user_convert(user_item):
        user_sym_item = user_item.split(':')
        return (user_sym_item[0], dict(map(convert, user_sym_item[1].split(','))))
    return dict(map(user_convert, str.split('+')))

parser = argparse.ArgumentParser(description="Generate EOD for T2")
parser.add_argument('--date', default=time.strftime('%Y%m%d', time.localtime()), help='date')
parser.add_argument('--db-host', default='localhost', help='db host name')
parser.add_argument('--db-name', required=True, help='db name')
parser.add_argument('--db-user', default='writer', help='db user name')
parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
parser.add_argument('--manual', default=None, type=convert_manual, help='manual update format(user:sym@qty,sym@qty+...)')
args = parser.parse_args()

conn = _DbConnector(args.db_host, args.db_user, args.db_pw.readline().strip('\n'), args.db_name)
offset_one = query_available_days(conn, args.date, 1)
update_eod_pigeon_ostrich(conn, args.date, offset_one, args.manual)
update_eod_parrot(conn, args.date, offset_one)