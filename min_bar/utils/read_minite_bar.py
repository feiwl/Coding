import pandas as pd
import pymysql
from min_bar.utils.get_root_paths import get_root_path
from min_bar.utils.target_dates_stocks import target_dates
import multiprocessing as mp

class ReadMinBarDB:
    def __init__(self, date = None, minute = None, stock_id = None):
        self.date = date
        self.minute = minute
        self.stock_id = stock_id

        # database
        self.db = pymysql.connect(host='192.168.10.68',
                     port=3306,
                     user='nas',
                     password='Prism@123456')

    def read_one_stock(self):
        if self.stock_id is None:
            print('stock id is not specified to get date')
            raise ValueError('stock id not exist')
        sql = """SELECT symbol.szWindCode,  transaction.nActionDay, transaction.nTime, transaction.nOpen, 
        transaction.nHigh, transaction.nMatch, transaction.iVolume, transaction.iTurnover, transaction.barClose, 
        transaction.S_DQ_PRECLOSE, transaction.S_DQ_ADJFACTOR, transaction.HighLimit, transaction.LowLimit 
        FROM marketdata.transaction join marketdata.symbol where transaction.codeID = symbol.codeID 
        and symbol.szWindCode = %s;""" % self.stock_id

        return pd.read_sql(sql, self.db)

    def read_one_day(self):
        if self.stock_id is None:
            print('date is not specified to get date')
            raise ValueError('date not exist')
        sql = """SELECT symbol.szWindCode, transaction.nActionDay, transaction.nTime, transaction.nOpen, 
        transaction.nHigh, transaction.nMatch, transaction.iVolume, transaction.iTurnover, transaction.barClose, 
        transaction.S_DQ_PRECLOSE, transaction.S_DQ_ADJFACTOR, transaction.HighLimit, transaction.LowLimit FROM 
        marketdata.transaction join marketdata.symbol where transaction.codeID = symbol.codeID and 
        transaction.nActionDay = %s;""" % self.date

        return pd.read_sql(sql, self.db)

    def read_one_minute(self):
        pass

    def read_once(self, day, yesterday, szWindCode):
        file_name_tran = """SELECT symbol.szWindCode, transaction.nActionDay, transaction.nTime, transaction.nOpen,
         transaction.nHigh, transaction.nLow, transaction.nMatch, transaction.iVolume, transaction.iTurnover, 
         transaction.nNumTrades, transaction.barClose, transaction.S_DQ_PRECLOSE, transaction.S_DQ_ADJFACTOR, 
         transaction.HighLimit, transaction.LowLimit FROM marketdata.transaction join marketdata.symbol where 
         transaction.codeId = symbol.codeId and (nActionDay= '%s' or nActionDay= '%s') and symbol.szWindCode = '%s';""" \
                              % (day, yesterday, szWindCode)
        print(file_name_tran)
        
        return pd.read_sql(file_name_tran, self.db)

class ReadStockMinBarFiles():
    def __init__(self, begin_date = None, end_date = None, stock_id = None, type = None):

        self.__begin_date = begin_date
        self.__end_date = end_date
        self.__stock_id = stock_id
        self.__data_type = type
        self.__target_dates = target_dates(begin_date, end_date)

        _, file_dir_min_data, _ = get_root_path()
        if self.__data_type == 'snap':
            self.__root_path = file_dir_min_data + '%s_min/stock/'%self.__data_type
        else:
            self.__root_path = file_dir_min_data + '%s_min/'%self.__data_type

    def read_one_day(self, date):
        '''
        Read one day minute bar data for a certain stock
        '''
        try:
            daily_minute_bar = pd.read_hdf(self.__root_path + '%s/%s_%s.hdf5'%(date, self.__stock_id, date))
        except Exception as e:
            # print(e)
            # print('Fail to read the minute data for ticker %s on date %s'%(self.__stock_id, date))
            daily_minute_bar = pd.DataFrame()

        return daily_minute_bar

    def read_all_days(self):
        '''
        Read all days minute bar data for a certain stock
        '''

        minute_bar_record = []
        for date in self.__target_dates:
            minute_bar_record.append(self.read_one_day(date))

        result_df = pd.concat(minute_bar_record, axis=0, ignore_index=True)
        flag = True if len(minute_bar_record) > 0 else False

        return flag, result_df


class ReadIndexMinBarFiles():
    def __init__(self, begin_date = None, end_date = None, index_id = None):

        self.__begin_date = begin_date
        self.__end_date = end_date
        self.__index_id = index_id
        self.__target_dates = target_dates(begin_date, end_date)

        _, file_dir_min_data, _ = get_root_path()
        self.__root_path = file_dir_min_data + 'snap_min/index/'

    def read_one_day(self, date):
        '''
                Read one day minute bar data for a certain index
                '''
        try:
            daily_minute_bar = pd.read_hdf(self.__root_path + '%s_%s.hdf5' % (self.__index_id, date))
        except Exception as e:
            print(e)
            print('Fail to read the minute data for ticker %s on date %s' % (self.__index_id, date))
            daily_minute_bar = pd.DataFrame()

        return daily_minute_bar

    def read_all_days(self):
        '''
        Read all days minute bar data for a certain index
        '''

        minute_bar_record = []
        for date in self.__target_dates:
            minute_bar_record.append(self.read_one_day(date))

        result_df = pd.concat(minute_bar_record, axis=0, ignore_index=True)
        return result_df


if __name__ == '__main__':
    x = ReadStockMinBarFiles('20200812', '20200819', '000001.SZ', 'snap')
    y = x.read_all_days()

    print(y)
