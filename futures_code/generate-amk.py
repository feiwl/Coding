import argparse
import pymysql
import json

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

parser = argparse.ArgumentParser(description="Data combination amk.json")
parser.add_argument('--prods', required=True, nargs='+',help='Multiple prod.')
parser.add_argument('--date', required=True, help='#: 20201228')
parser.add_argument('--output-filename', required=True, help=' output-filename ')
parser.add_argument('--maxOrderSize', required=True, help=' maxOrderSide ')
parser.add_argument('--maxPosition', required=True, help=' maxPosition ')
parser.add_argument('--placeThrough', required=True, help=' placeThrough ')
parser.add_argument('--db-host', default='192.168.10.101', help='db host name')
parser.add_argument('--db-name', default='ctp-vir-sym', help='db name')
parser.add_argument('--db-user', default='root', help='db user name')
# parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
parser.add_argument('--db-pw', default='123456', help='db password filename')
args = parser.parse_args()

conn = _DbConnector(host=args.db_host,user=args.db_user,password=args.db_pw, database=args.db_name)

contract_format = {
    "maxOrderSize": args.maxOrderSize,
    "maxPosition": args.maxPosition,
    "placeThrough": args.placeThrough
}

contracts = dict()
contract_sql = "SELECT prod,symbol,date FROM virsym WHERE (prod=%s) AND (date<=%s) ORDER BY date DESC LIMIT 1;"

for p in args.prods:
    data = conn.query_and_fetch((contract_sql, (p, args.date)))
    contract = next(iter(data))[1]
    contracts.update({contract:contract_format})

amk_result = {"strategyConfigs": {"amk": contracts}}

print(json.dumps(amk_result, indent=4, sort_keys=True))

with open(args.output_filename, 'w+' ) as f:
    json.dump(amk_result, f, indent=4, sort_keys=True)

