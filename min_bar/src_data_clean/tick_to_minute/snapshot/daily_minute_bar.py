__author__ = 'ZhouTW'

# coding = utf-8
import multiprocessing as mp
import datetime
import os
import platform
from min_bar.utils.target_dates_stocks import available_stocks, target_stocks
from min_bar.src_data_clean.tick_to_minute.snapshot.stock_minute_bar import StockMinuteSnap
from min_bar.src_data_clean.tick_to_minute.snapshot.index_minute_bar import IndexMinuteBarSnap

class DailyMinSnap:
    def __init__(self, date, data_source_dir, data_save_dir):
        # initial values
        self.date = date
        self.data_source_dir = data_source_dir
        self.data_save_dir = data_save_dir

        # return stock list
        ipo_df = available_stocks()
        self.__stock_list = target_stocks(ipo_df, date)

        # return index list
        self.__index_list = ['399905.SZ',           # zz500
                             '000001.SH',           # sh
                             '399001.SZ',           # sz
                             '000300.SH',           # hs300
                             ]

        # build dir to store snap data
        self.stock_target_store_dir = \
            os.path.abspath(os.path.join(self.data_save_dir, 'snap_min/stock/%s' % self.date))

        if not os.path.exists(self.stock_target_store_dir):
            os.makedirs(self.stock_target_store_dir)

        self.index_target_store_dir = \
            os.path.abspath(os.path.join(self.data_save_dir, 'snap_min/index'))

        if not os.path.exists(self.index_target_store_dir):
            os.makedirs(self.index_target_store_dir)

        # minute record
        self.__stock_minute_record = None
        self.__index_minute_record = None
        self.get_minute_record()

    def get_minute_record(self):

        with open('utils/files/stock_minute_list.txt', 'r') as file:
            minute_record = file.readlines()
            self.__stock_minute_record = [int(item.rstrip('\n')) for item in minute_record]

        with open('utils/files/index_minute_list.txt', 'r') as file:
            index_minute_record = file.readlines()
            self.__index_minute_record = [int(item.rstrip('\n')) for item in index_minute_record]

    def get_min_snap_stock(self, stock):
        try:
            result = StockMinuteSnap(stock, self.date, self.data_source_dir, self.__stock_minute_record)
            code, result_df = result.get_snap_min_bar_result()
            return code, result_df
        except Exception as e:
            print(e)
            print('fail to get minute record for stock %s at date %s' % (stock, self.date))
            return stock, None

    def get_min_snap_index(self, index):
        try:
            result = IndexMinuteBarSnap(index, self.date, self.data_source_dir, self.__index_minute_record)
            code, result_df = result.get_snap_min_bar_result()
            return code, result_df
        except Exception as e:
            print(e)
            print('fail to get minute record for index %s at date %s' % (index, self.date))
            return index, None

    def get_min_snap_day(self):
        begin_time = datetime.datetime.now()
        print('start time is %s' % begin_time.strftime("%H:%M:%S"))

        with mp.Pool(processes=32) as pool:
            mp_result = [pool.apply_async(self.get_min_snap_stock, (stock,)) for stock in self.__stock_list]
            mp_result.extend([pool.apply_async(self.get_min_snap_index, (index,)) for index in self.__index_list])
            # mp_result = [pool.apply_async(self.get_min_snap_index, (index,)) for index in self.__index_list]
            mp_result = [item.get() for item in mp_result]
            pool.close()
            pool.join()

        result_list = list(filter(lambda x: x[1] is not None, mp_result))
        end_time = datetime.datetime.now()
        print('end time is %s' % end_time.strftime("%H:%M:%S"))
        print('computing time usage is %s' % str(end_time - begin_time))

        return result_list

    def save_result_once(self, one_item):
        frame = one_item[1]
        code = one_item[0]
        if code in self.__index_list:
            frame.to_hdf(self.index_target_store_dir + '/%s_%s.hdf5' % (code, self.date), 'hello', mode='w')
        else:
            frame.to_hdf(self.stock_target_store_dir + '/%s_%s.hdf5' % (code, self.date), 'hello', mode='w')

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
    result = DailyMinSnap(date, file_dir_tick_data, file_dir_save_data)

    bar_list = result.get_min_snap_day()
    result.save_result(bar_list)

if __name__ == '__main__':
    main('20200825')
