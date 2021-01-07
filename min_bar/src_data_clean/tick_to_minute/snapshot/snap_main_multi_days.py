__author__ = 'ZhouTW'
# coding = utf-8
from tqdm import tqdm
import platform
import os
from min_bar.src_data_clean.tick_to_minute.snapshot.daily_minute_bar import DailyMinSnap
from min_bar.utils.target_dates_stocks import target_dates

def main(begin_date, end_date):
    dates = target_dates(begin_date=begin_date, end_date=end_date)

    if platform.system() == 'Windows':
        print('platform is windows')
        file_dir_tick_data = r'\\192.168.10.235\data_home\stock_data\market_data\\'
        file_dir_save_data = os.getcwd() + '/data'
    else:
        print('platform is linux')
        file_dir_tick_data = '/mnt/data_home/stock_data/market_data//'
        file_dir_save_data = '/home/sharonyu/data/'

    for date in tqdm(dates):
        print('start for date %s' % date)
        result = DailyMinSnap(date, file_dir_tick_data, file_dir_save_data)
        bar_list = result.get_min_snap_day()
        result.save_result(bar_list)
        print('end for date %s' % date)

if __name__ == '__main__':
    main('20200825', '20200825')
