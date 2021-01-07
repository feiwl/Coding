__author__ = 'ZhouTW'

import os
import datetime
import platform
import multiprocessing as mp
from min_bar.src_data_clean.tick_to_minute.transaction.one_stock_min_bar import stock_minute_bar
import warnings
from min_bar.utils.get_high_low_limit import compute_high_low_limit
from min_bar.utils.target_dates_stocks import *
warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)


class daily_min_bar:

    def __init__(self, date, data_dir, data_save_dir):

        ipo_df = available_stocks()
        self.stock_list = target_stocks(ipo_df, date)

        self.date = date
        self.data_dir = data_dir

        # build dir to stock bar
        self.target_store_dir = os.path.abspath(os.path.join(data_save_dir, 'tran_min/%s' % self.date))
        if not os.path.exists(self.target_store_dir):
            os.makedirs(self.target_store_dir)

        # some original data
        # minutes that need to be record
        self.__minute_record = None
        # EOD data daily
        self.df_EOD = None

        # get basis data for stocks
        self.get_daily_basis()

    def get_daily_basis(self):
        # minutes to be recorded
        with open('utils/files/minute_record.txt', 'r') as file:
            minute_record = file.readlines()
            self.__minute_record = [int(item.rstrip('\n')) for item in minute_record]

        # data source
        db = pymysql.connect(host='192.168.1.225',
                             port=3306,
                             user='wind_user',
                             password='Q#wind2$%pvt')

        # get EOD data
        sql = 'SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_ADJFACTOR FROM wind.ASHAREEODPRICES ' \
              'where TRADE_DT = %s' % self.date
        df_EOD = pd.read_sql(sql, db)

        # high and low limit price
        high_low_limit = compute_high_low_limit(self.date)
        df_EOD = df_EOD.join(high_low_limit.set_index('S_INFO_WINDCODE'), on='S_INFO_WINDCODE')
        df_EOD.rename(columns={'S_INFO_WINDCODE': 'szWindCode',
                               'TRADE_DT': 'nActionDay'},
                      inplace=True)
        df_EOD['S_DQ_PRECLOSE'] = (df_EOD['S_DQ_PRECLOSE'] * 1e4 + 1e-5).astype('int')

        self.df_EOD = df_EOD

    def get_min_bar_stock(self, stock):
        # construct basis dict
        stock_record = self.df_EOD[self.df_EOD['szWindCode'] == stock]
        try:
            stock_basis = {'S_DQ_PRECLOSE': int(stock_record['S_DQ_PRECLOSE']),
                           'S_DQ_ADJFACTOR': float(stock_record['S_DQ_ADJFACTOR']),
                           'HighLimit': int(stock_record['HighLimit']),
                           'LowLimit': int(stock_record['LowLimit'])}
        except:
            # no history record for this stock
            print('data for stock %s at date %s has no basis data' % (stock, self.date))
            return stock, None

        result = stock_minute_bar(stock, self.date, self.data_dir, stock_basis, self.__minute_record)
        code, result_df = result.get_min_bar_result()
        if result_df is not None:
            length = len(result_df)
            if length != 239:
                print('size warning for stock %s, the length is %d' % (code, length))

        return code, result_df

    def get_min_bar_day(self):
        begin_time = datetime.datetime.now()
        print('start time is %s' % begin_time.strftime("%H:%M:%S"))

        with mp.Pool(processes=32) as pool1:
            mp_result = [pool1.apply_async(self.get_min_bar_stock, (stock,)) for stock in self.stock_list]
            mp_result = [item.get() for item in mp_result]
            pool1.close()
            pool1.join()

        result_list = list(filter(lambda x: x[1] is not None, mp_result))
        end_time = datetime.datetime.now()
        print('end time is %s' % end_time.strftime("%H:%M:%S"))
        print('conputing time usage is %s' % str(end_time - begin_time))
        return result_list

    def save_result_once(self, one_item):
        frame = one_item[1]
        code = one_item[0]
        frame.to_hdf(self.target_store_dir + '/%s_%s.hdf5' % (code, self.date), 'hello', mode='w')

    def save_result(self, bar_list):
        begin_time = datetime.datetime.now()
        # method 1
        with mp.Pool(processes=32) as pool2:
            for item in bar_list:
                pool2.apply_async(self.save_result_once, (item,))
            pool2.close()
            pool2.join()

        print('saving time usage is %s' % (datetime.datetime.now() - begin_time))

def main(date):
    if platform.system() == 'Windows':
        print('platform is windows')
        file_dir_tick_data = r'\\192.168.10.235\data_home\stock_data\market_data\\'
        file_dir_save_data = os.getcwd() + '/data'
    else:
        print('platform is linux')
        file_dir_tick_data = '/mnt/data_home/stock_data/market_data//'
        file_dir_save_data = '/home/sharonyu/data/'

    result = daily_min_bar(date, file_dir_tick_data, file_dir_save_data)

    bar_list = result.get_min_bar_day()
    result.save_result(bar_list)

if __name__ == '__main__':
    date = '20200825'
    main(date)
