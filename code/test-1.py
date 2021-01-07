import pymysql
import json
import argparse
import time
from collections import *

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

def query_sumdays_data(conn,today,knownday):
    pigeon_sql = "SELECT userName,symbol,sum(volume) FROM user LEFT JOIN pigeon USING(uid) WHERE ( date = %s or date = %s) GROUP BY userName,symbol;"
    query_pigeon = conn.query_and_fetch((pigeon_sql,(today,knownday)))
    #return [r for r in query_pigeon]
    result = defaultdict(dict)
    list(map(lambda r: result[r[0]].update({r[1]: int(r[2])}), query_pigeon))
    return dict(result)

def query_today_parrot(conn,today):
    parrot_sql = "SELECT userName,symbol,sum(volumeDelta) FROM user LEFT JOIN parrot USING(uid) WHERE ( date = %s ) GROUP BY userName,symbol;"
    query_parrot = conn.query_and_fetch((parrot_sql,(today)))
    #return [r for r in query_parrot]
    result = defaultdict(dict)
    list(map(lambda r: result[r[0]].update({r[1]: int(r[2])}), query_parrot))
    return dict(result)

def query_account(conn):
    sql = "select userName from user;"
    result = conn.query_and_fetch((sql,))
    return {date:{} for dates in result for date in dates}

def merge_result(pigeon, parrot):
    _pigeon = defaultdict(dict, pigeon)
    _parrot = defaultdict(dict, parrot)
    result = {}
    for user in set().union(_pigeon.keys(), _parrot.keys()):
        scapegoat = Counter(_pigeon[user]).copy()
        scapegoat.update(Counter(_parrot[user]))
        result.update({user: dict(scapegoat)})
    return result

# parser = argparse.ArgumentParser(description="Generate EOD for T2")
# parser.add_argument('--date', default=time.strftime('%Y%m%d', time.localtime()), help='date')
# parser.add_argument('--output', required=True, type=argparse.FileType('w'), help='output filename')
# parser.add_argument('--db-host', default='localhost', help='db host name')
# parser.add_argument('--db-name', required=True, help='db name')
# parser.add_argument('--db-user', default='writer', help='db user name')
# parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
# args = parser.parse_args()
#
# db_pw = args.db_pw.readline().strip('\n')
# conn = _DbConnector(args.db_host, args.db_user, db_pw, args.db_name)
# pigeon_day = query_available_days(conn, args.date, 1)

conn = _DbConnector('192.168.10.101','root','123456','users')
pigeon_day = query_available_days(conn, '20200327', 1)
date = time.strftime('%Y%m%d', time.localtime())
today_parrot = query_sumdays_data(conn,date,pigeon_day)
sumday_data= query_today_parrot(conn,date)
sumdict=query_account(conn)
print(sumday_data)
print(today_parrot)

print(merge_result(today_parrot,sumday_data))

