#!/usr/bin/env python3

import argparse
import json
import time
import json
import pymysql
from collections import defaultdict

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

def query_pigeon_eod(conn, date):
    sql = "SELECT userName,symbol,volume FROM eod_pigeon INNER JOIN user using(uid) where date = %s"
    query_result = conn.query_and_fetch((sql, (date,)))
    result = defaultdict(dict)
    list(map(lambda r : result[r[0]].update({r[1]: r[2]}), query_result))
    return dict(result)

def query_parrot_eod(conn, date):
    sql = "SELECT userName,symbol,volume FROM eod_parrot INNER JOIN user using(uid) where date = %s"
    query_result = conn.query_and_fetch((sql, (date,)))
    result = defaultdict(dict)
    list(map(lambda r : result[r[0]].update({r[1]: r[2]}), query_result))
    return dict(result)

parser = argparse.ArgumentParser(description="Generate EOD for T2")
parser.add_argument('--date', default=time.strftime('%Y%m%d', time.localtime()), help='date')
parser.add_argument('--output', required=True, type=argparse.FileType('w'), help='output filename')
parser.add_argument('--db-host', default='localhost', help='db host name')
parser.add_argument('--db-name', required=True, help='db name')
parser.add_argument('--db-user', default='writer', help='db user name')
parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
args = parser.parse_args()

conn = _DbConnector(args.db_host, args.db_user, args.db_pw.readline().strip('\n'), args.db_name)
json_result = {
    'date': args.date,
    'parrot': query_pigeon_eod(conn, args.date),
    'pigeon': query_parrot_eod(conn, args.date)}
json.dump(json_result, args.output, indent=4, sort_keys=True)