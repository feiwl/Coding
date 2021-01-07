__author__ = 'ZhouTW'


import pandas as pd
from tqdm import tqdm
from min_bar.src_data_clean.tick_to_minute.transaction.one_day_min_bar import daily_min_bar
import platform
import warnings
import pymysql
import os
from min_bar.utils.target_dates_stocks import target_dates
warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)

def available_stocks():

    """
    Find all stocks in the market
    :return:
    dataframe with stock_id, ipo_date, delist_date
    """

    db = pymysql.connect(host='192.168.1.225',
                         port=3306,
                         user='wind_user',
                         password='Q#wind2$%pvt')

    sql = """SELECT S_INFO_WINDCODE, S_INFO_LISTDATE, S_INFO_DELISTDATE FROM wind.ASHAREDESCRIPTION
            where s_info_listdate is not null
            and s_info_windcode not REGEXP '^[a-zA-Z]';"""

    df = pd.read_sql(sql, db)
    return df


def target_stocks(ipo_df, date_string):
    stock_id = []

    for i in range(len(ipo_df)):
        [stock_name, entry_date, remove_date] = list(ipo_df.iloc[i].values)
        if entry_date is not None:                  #如果没有entry date，代表上市失败，可不考虑
            if date_string >= entry_date:           #只考虑上市期间的日期
                if remove_date is None:             #如果还没退市，可以计算
                    stock_id.append(stock_name)
                elif remove_date >= date_string:    #如果已经退市，判断当前日期是否早于退市日期
                    stock_id.append(stock_name)

    return stock_id

if __name__ == '__main__':
    dates = target_dates(begin_date='20190101', end_date='20200825')

    ipo_df = available_stocks()

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
        available_stocks = target_stocks(ipo_df, date)
        result = daily_min_bar(date, file_dir_tick_data, file_dir_save_data)
        bar_list = result.get_min_bar_day()
        result.save_result(bar_list)
        print('end for date %s' % date)
