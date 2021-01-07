import pymysql
import argparse
import datetime
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

def get_contracts_from_db(conn:_DbConnector, prods, date, pre_date_count) -> dict:
    contracts = dict()
    contract_sql = "SELECT prod,symbol,date FROM virsym WHERE (prod=%s) AND (date<=%s) ORDER BY date DESC LIMIT 1;"
    for p in prods:
        data = conn.query_and_fetch((contract_sql, (p, date)))
        contract = next(iter(data))[1]
        contracts.update({contract:'base'})

    if pre_date_count:
        pre_date = datetime.datetime.strftime(
            (datetime.datetime.strptime(date, '%Y%m%d') - datetime.timedelta(days=int(pre_date_count))), '%Y%m%d')
        for p in prods:
            data = conn.query_and_fetch((contract_sql, (p, pre_date)))
            contract = next(iter(data))[1]
            contracts.update({contract: 'base'})
    return contracts

parser = argparse.ArgumentParser(description="Data combination amk.json")
parser.add_argument('--prods', required=True, nargs='+',help='Multiple prod.')
parser.add_argument('--date', required=True, help='#: 20201228')
parser.add_argument('--pre-date-count', required=False, help='#: pre-date-count lt date')
parser.add_argument('--output-filename', required=True, help=' output-filename ')
parser.add_argument('--maxPosition', required=True, type=int, help=' maxPosition ')
parser.add_argument('--reservedCancels', required=True, type=int, help=' reservedCancels ')
parser.add_argument('--maxOpenedLots', required=True, type=int, help=' maxOpenedLots ')
parser.add_argument('--maxOrderSize', required=True, type=int, help=' maxOpenedLots ')
parser.add_argument('--max3sNewOrders', required=True, type=int, help=' maxOpenedLots ')
parser.add_argument('--max30sAlternations', required=True, type=int, help=' maxOpenedLots ')
parser.add_argument('--maxOrdersPerSide', required=True, type=int, help=' maxOpenedLots ')
parser.add_argument('--db-host', default='192.168.10.101', help='db host name')
parser.add_argument('--db-name', default='ctp-vir-sym', help='db name')
parser.add_argument('--db-user', default='root', help='db user name')
# parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
parser.add_argument('--db-pw', default='123456', help='db password filename')
args = parser.parse_args()

conn = _DbConnector(host=args.db_host,user=args.db_user,password=args.db_pw, database=args.db_name)

contracts =  get_contracts_from_db(conn, args.prods, args.date, args.pre_date_count)

base_format = {
    "maxPosition": args.maxPosition,
    "reservedCancels": args.reservedCancels,
    "maxOpenedLots": args.maxOpenedLots,
    "maxOrderSize": args.maxOrderSize,
    "max3sNewOrders": args.max3sNewOrders,
    "max30sAlternations": args.max30sAlternations,
    "maxOrdersPerSide": args.maxOrdersPerSide
}

symbolLists = { "symbolLists": {"infra": list(contracts.keys())}}

riskCheckerConfigs = { "riskCheckerConfigs": {"base": base_format}}

riskConfigs = {"riskConfigs": {
        "rc1": {
            "min4ExchangeOrderIntervalMs": {
                "CFFEX": 1100,
                "CZCE": 1100,
                "DCE": 1100,
                "SHFE": 1100
            },
            "reservedAdds": {
                "CFFEX": 0,
                "CZCE": 0,
                "DCE": 0,
                "SHFE": 0
            },
            "reservedOpenedLots": {
            },
     		"monoUtilization": {
            }
        }
    }}

riskConfigs['riskConfigs']['rc1'].update(contracts)

risk_result = {'symbolLists': symbolLists, 'riskCheckerConfigs':riskCheckerConfigs, 'riskConfigs': riskConfigs}

print(json.dumps(risk_result, indent=4))

with open(args.output_filename, 'w+' ) as f:
    json.dump(risk_result, f, indent=4)
