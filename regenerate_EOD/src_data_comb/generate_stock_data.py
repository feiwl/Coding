__author__ = 'SharonYU'


import warnings
warnings.filterwarnings('ignore')

import multiprocessing as mp
from tqdm import tqdm
from regenerate_EOD.utils.target_dates_stocks import available_stocks

from regenerate_EOD.utils.read_minite_bar import ReadMinBarMongoDB
from regenerate_EOD.src_data_clean.tran_regenerate_EOD import new_daily_eod

class generate_stock_new_EOD():
    '''
        This class is used to call the regeneration of new stock EOD data
        Given begin date and end date, it will read data from mongodb first
        and then call the regenerate EOD function to regenerate the EOD data
        '''

    def __init__(self, begin_date = None, end_date = None, target_time = None):

        self.begin_date = begin_date
        self.end_date = end_date
        self.target_time = target_time


    def get_min_bar(self, stock_id):
        '''
        Read stock minute bar data from mongodb
        '''
        min_bar = ReadMinBarMongoDB(begin_date=self.begin_date,
                                    end_date=self.end_date,
                                    id=stock_id,
                                    type='tran')
        history_min_bar = min_bar.read_all_days()

        flag = True if history_min_bar.count() > 0 else False
        return flag, history_min_bar

    def get_new_EOD(self, stock_id):
        '''
        Regenerate EOD data for a certain stock
        '''
        flag, history_min_bar = self.get_min_bar(stock_id)
        if flag:
            new_EOD = new_daily_eod(data_cursor=history_min_bar,
                                    target_time=self.target_time)
        else:
            new_EOD = None

        return stock_id, new_EOD

if __name__ == '__main__':

    begin_date = '20190101'
    end_date = '20200101'
    target_time = 1400e5
    test = generate_stock_new_EOD(begin_date, end_date,target_time)

    ipo_df = available_stocks()
    all_stocks = ipo_df['S_INFO_WINDCODE'].tolist()
    delist_stocks = ipo_df[ipo_df['S_INFO_DELISTDATE'] <= begin_date]['S_INFO_WINDCODE'].tolist()
    notlist_stocks = ipo_df[ipo_df['S_INFO_LISTDATE'] > end_date]['S_INFO_WINDCODE'].tolist()
    target_stocks = list(set(all_stocks) - set(delist_stocks) - set(notlist_stocks))

    process_bar = tqdm(total=len(target_stocks))
    process_bar.set_description('Process for combining data')
    update = lambda *args: process_bar.update()

    with mp.Pool(processes=32) as pool:
        mp_result = [pool.apply_async(test.get_new_EOD, (stock,), callback=update) for stock in target_stocks]
        mp_result = [item.get() for item in mp_result]
        pool.close()
        pool.join()
