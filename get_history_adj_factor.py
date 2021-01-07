import pandas as pd
import pymysql
import argparse
import time
import warnings

#warnings.filterwarnings("PerformanceWarning")

class _DbConnector(object):
    def __init__(self, host, user, password, database):
        #self._conn = MySQLdb.connect(host, user, password, database)
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

def get_adj_factor(conn,datetime):
    Query_sql = "SELECT S_INFO_WINDCODE,S_DQ_ADJFACTOR  from wind.ASHAREEODPRICES WHERE TRADE_DT=%s"
    query_result=conn.query_and_fetch((Query_sql, datetime))
    return query_result,datetime

def handler_adj_factor(query_result):
    adj_result = {'datetime':query_result[1]}
    data = query_result
    for i in data[0]:
        adj_result[str(i[0])] = [float(i[1])]
    df = pd.DataFrame(adj_result).set_index("datetime",drop=True)
    return df

def insert_lowlimit_df(source_hdf,to_hdf,adj_factor):
    source_hdf_file = pd.DataFrame()
    if source_hdf:
        source_hdf_file = pd.read_hdf(source_hdf)
    current_result=source_hdf_file.append(adj_factor,sort=False)
    current_result = current_result.where((current_result.notna()), 'null')
    current_result = current_result[sorted(current_result.columns)]
    h5_store = pd.HDFStore(to_hdf, mode='a')
    h5_store['data'] = current_result
    h5_store.close()

# parser = argparse.ArgumentParser(description="Read Database to HDF5_FILE")
# parser.add_argument('--date', default=True, help="datetime=time.strftime())")
# parser.add_argument('--db-host', default='localhost', help='db host name')
# parser.add_argument('--db-name', required=True, help='db name')
# parser.add_argument('--db-user', default='nas', help='db user name')
# parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
# parser.add_argument('--adj-factor-source-hdf',required=False,help='History adj_factor_hdf5_file')
# parser.add_argument('--adj-factor-to-hdf',required=True,help='Target adj_factor_hdf5_path_file')
# args = parser.parse_args()

# conn = _DbConnector(args.db_host,args.db_user,args.db_pw.readline().strip('\n'),args.db_name)
conn = _DbConnector('192.168.1.225','wind_user','Q#wind2$%pvt','wind')
# insert_lowlimit_df(args.adj_factor_source_hdf,args.adj_factor_to_hdf,handler_adj_factor(get_adj_factor(conn,str(args.date))))
data = handler_adj_factor(get_adj_factor(conn,str(20190102)))
for k,v in data.iterrows():
    print(type(k))

