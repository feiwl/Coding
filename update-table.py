#!/usr/bin/env python3

import argparse
import json
import lzma
import pymysql
import re
import time
import warnings

warnings.filterwarnings("ignore", category=pymysql.Warning)

TIME = '(?P<t>\d{2}:\d{2}:\d{2}.\d+)'
LH_OID = '(?P<oid>\d+)'
DETAIL = '(?P<qty>\d+)@(?P<px>\d+.\d+)'
UID = '(?P<uid>\d+)'
CHANNEL = '(?P<chl>\d+-\d+)'
SIDE = '(?P<side>Buy|Sell)'
SYMBOL = '(?P<sym>\d{6}.(SH|SZ))'
SESSION_ID = '(?P<session>\d+)'
ACCOUNT = '(?P<account>\d+)'
ADDED_ORDER = re.compile(TIME + ' Account ' + UID + ': added order ' + CHANNEL + ' lhOid ' + LH_OID + ' ' +
                         SIDE + ' ' + DETAIL + ' ' + SYMBOL)
FILLED = re.compile(TIME + ' onFilled: ' + DETAIL + ', lhOid ' + LH_OID)
ACCOUNTLINE = re.compile(TIME + ' Logged in account ' + UID + ' ' + ACCOUNT + ' with session ID ' + SESSION_ID)

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

class Capturlog(object):
    def __init__(self):
        self._result = {}
        self._account_map = {}
        self._oid_to_trade = {}

    @property
    def result(self):
        return self._result.copy()

    def extract_trading_detail(self, filename):
        self._account_map.clear()
        self._oid_to_trade.clear()
        for line in lzma.open(filename, 'rt'):
            account = ACCOUNTLINE.match(line)
            if account is not None:
                self._account_map[account.group('uid')] = account.group('account')
                self._result[account.group('account')] = {}
                continue

            order = ADDED_ORDER.match(line)
            if order is not None:
                self._oid_to_trade[order.group('oid')] = {
                    'account': self._account_map[order.group('uid')],
                    'symbol': order.group('sym'), 'side': order.group('side')}
                if order.group('sym') not in self._result[self._account_map[order.group('uid')]].keys():
                    self._result[self._account_map[order.group('uid')]].update({order.group('sym'): 0})
                continue

            onfilled = FILLED.match(line)
            if onfilled is not None:
                oid_detail = self._oid_to_trade[onfilled.group('oid')]
                if oid_detail['side'] == 'Buy':
                    self._result[oid_detail['account']][oid_detail['symbol']] += int(onfilled.group('qty'))
                elif oid_detail['side'] == 'Sell':
                    self._result[oid_detail['account']][oid_detail['symbol']] -= int(onfilled.group('qty'))

def update_user(result):
    user_sql = "INSERT IGNORE INTO user(userName) VALUES(%s)"
    return map(lambda user: (user_sql, (user,)), result)

def update_table(date, table, result):
    sql = "INSERT INTO " + table + "(uid, date, symbol, volumeDelta) VALUES ((SELECT uid FROM user WHERE userName = %s), %s, %s, %s)"
    sqls = []
    for user in filter(lambda user: len(result[user]), result):
        sqls.extend(map(lambda sym: (sql, (user, date, sym, result[user][sym])),
                    filter(lambda sym: result[user][sym] != 0, result[user])))
    return sqls

parser = argparse.ArgumentParser(description="Extract trading detail for T0")
parser.add_argument('--date', default=time.strftime("%Y%m%d", time.localtime()), help='date')
parser.add_argument('--tms-lh', required=True, nargs='+', help='tms lh filename(s)')
parser.add_argument('--db-host', default='localhost', help='db host name')
parser.add_argument('--db-name', required=True, help='db name')
parser.add_argument('--db-user', default='writer', help='db user name')
parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
parser.add_argument('--table', required=True, choices=('parrot', 'pigeon'), help='db table name')
args = parser.parse_args()

foo = Capturlog()
list(map(foo.extract_trading_detail, args.tms_lh))
conn = _DbConnector(args.db_host, args.db_user, args.db_pw.readline().strip('\n'), args.db_name)
conn.update(update_user(foo.result))
conn.update(update_table(args.date, args.table, foo.result))