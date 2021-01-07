import pandas as pd
import argparse
import pymysql
import time
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

def query_marketdata(conn,datetime):
    HighLimit_dict = dict()
    LowLimit_dict = dict()
    query_symbols = ['SELECT * FROM symbol']
    all_symbols = conn.query_and_fetch(query_symbols)
    for symbol_codeId in all_symbols:
        query_trancation = ["SELECT HighLimit,LowLimit FROM transaction WHERE codeId=(SELECT codeId FROM symbol where szWindCode='{}') and nActionDay='{}' limit 1".format(symbol_codeId[0],datetime)]
        query_result=conn.query_and_fetch(query_trancation)
        if query_result:
            HighLimit_dict.update({str(symbol_codeId[0]):int(query_result[0][0])})
            LowLimit_dict.update({str(symbol_codeId[0]):int(query_result[0][1])})
    return HighLimit_dict,LowLimit_dict

def insert_data_df(source_hdf,data,datetime):
    source_hdf_file = pd.DataFrame()
    if source_hdf:
        source_hdf_file = pd.read_hdf(source_hdf)
    datetime_indexs = [ index for index,v in source_hdf_file.iterrows()]
    datetime_indexs.append(datetime)
    data_ss = pd.Series(data)
    data_column = set(data_ss.index) ^ set(source_hdf_file.columns)

    if len(data_column) > 0:
        for item in data_column:
            if item not in source_hdf_file:
                source_hdf_file[item] = None
            if item not in data_ss:
                data_ss[item] = None
        source_hdf_file = source_hdf_file.append(data_ss[source_hdf_file.columns], ignore_index=True)
    else:
        source_hdf_file = source_hdf_file.append(data_ss[source_hdf_file.columns], ignore_index=True)

    current_result = pd.DataFrame(index=datetime_indexs,data=source_hdf_file.values,columns=source_hdf_file.columns)
    current_result = current_result[sorted(current_result.columns)]
    h5_store = pd.HDFStore(source_hdf, mode='a')
    h5_store['data'] = current_result
    h5_store.close()

parser = argparse.ArgumentParser(description="Read Database to HDF5_FILE")
parser.add_argument('--date', default=True, help="File time example 2020-01-01")
parser.add_argument('--db-host', default='localhost', help='db host name')
parser.add_argument('--db-name', required=True, help='db name')
parser.add_argument('--db-user', default='nas', help='db user name')
parser.add_argument('--db-pw', required=True, type=argparse.FileType('r'), help='db password filename')
parser.add_argument('--highlimit_source_hdf',required=False,help='History highlimit_hdf5_file')
parser.add_argument('--lowlimit_source_hdf',required=False,help='History lowlimit_hdf5_file')
args = parser.parse_args()

conn = _DbConnector(args.db_host,args.db_user,args.db_pw.readline().strip('\n'),args.db_name)
symbol_result = query_marketdata(conn,time.strftime('%Y-%m-%d',time.strptime(args.date,'%Y%m%d')))
if symbol_result:
    insert_data_df(args.highlimit_source_hdf,symbol_result[0],args.date)
    insert_data_df(args.lowlimit_source_hdf,symbol_result[1],args.date)

# conn = _DbConnector('192.168.10.68','nas','Prism@123456','marketdata')
# symbol_result = query_marketdata(conn,'2020-07-23')
#
# insert_data_df('/home/banruo/test.hdf5','/home/banruo/test.hdf5',symbol_result[0],'20200723')
