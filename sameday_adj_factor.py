import pandas as pd
import pymysql
from decimal import Decimal
import os
import argparse
import warnings
warnings.filterwarnings('ignore',category=pd.io.pytables.PerformanceWarning)

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

def query_adj_close_factor(conn,datetime):
    Query_sql = "SELECT S_INFO_WINDCODE,S_DQ_CLOSE,S_DQ_ADJFACTOR,TRADE_DT FROM  wind.ASHAREEODPRICES WHERE TRADE_DT=(select max(TRADE_DT) FROM wind.ASHAREEODPRICES WHERE TRADE_DT < %s limit 1)"
    query_result=conn.query_and_fetch((Query_sql, datetime))
    adj_close_factor_result = {i[0]: i[1]*i[2] for i in query_result}
    return adj_close_factor_result

def handler_csv(path):
    data_csv=list()
    if os.path.isfile(path):
        with open(path,"r") as file:
            data_csv.append(list(filter(lambda line: line.split(',')[3] != '1' and line is not None ,file)))
            file.close()
        os.remove(path)
        with open(path,'a+') as file:
            for i in next(iter(data_csv)):
                file.write(i)
            file.close()

def generate_preclose(path):
    df = pd.DataFrame(pd.read_csv(path))
    sym_preclose_dict=dict()
    for index,value in df.iterrows():
        sym_preclose_dict.update({value['symbol']:Decimal(str(value['preClosePx']))})
    return sym_preclose_dict

def calculation_adj_factor(preclose,close_factor,datetime):
    adj_factor = {'datetime':datetime}
    for k in preclose.keys():
        if close_factor.get(k):
            adj_factor.update({k: [float(close_factor[k] / preclose[k])]})
        else:  # 昨天没有，今天有,标1
            adj_factor.update({k: [float(round(Decimal(str(1)), 6))]})
    return adj_factor

parser = argparse.ArgumentParser(description="Read Database to HDF5_FILE")
parser.add_argument('--same-date', default=True, help="hdf5 fromat datetime")
parser.add_argument('--db-host', default='localhost', help='db host name')
parser.add_argument('--db-name', required=True, help='db name')
parser.add_argument('--db-user', default='nas', help='db user name')
parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
parser.add_argument('--refData_csv',required=True,help='Today refData_csv_file')
parser.add_argument('--to-file-hdf5',required=True,help='Target to file hdf5 file')
args = parser.parse_args()

conn = _DbConnector(args.db_host,args.db_user,args.db_pw.readline().strip('\n'),args.db_name)
handler_csv(args.refData_csv)
preclose=generate_preclose(args.refData_csv)
close_factor=query_adj_close_factor(conn,args.same_date)

df = pd.DataFrame(calculation_adj_factor(preclose,close_factor,args.same_date)).set_index("datetime",drop=True)
h5_store = pd.HDFStore(args.to_file_hdf5, mode='a')
h5_store['data'] = df
h5_store.close()


