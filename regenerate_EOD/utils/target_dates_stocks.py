import pymysql
import pandas as pd
from regenerate_EOD.config.config import wind_db as db
# coding = utf-8

def target_dates(begin_date, end_date):
    db = pymysql.connect(host='192.168.1.225',
                         port=3306,
                         user='wind_user',
                         password='Q#wind2$%pvt')

    sql = """SELECT trade_days FROM wind.ASHARECALENDAR WHERE S_INFO_EXCHMARKET = 'SZSE';"""

    df = pd.read_sql(sql, db)

    df = df.sort_values('trade_days', ascending=True).reset_index(drop = True)

    dates = list(df.trade_days)

    dates = list(filter(lambda x: (x <= end_date) & (x >= begin_date), dates))
    dates = list(sorted(dates))

    return dates


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
        if entry_date is not None:
            if date_string >= entry_date:
                if remove_date is None:
                    stock_id.append(stock_name)
                elif remove_date >= date_string:
                    stock_id.append(stock_name)

    return stock_id

def get_st_stocks(target_date):

    sql = """SELECT S_INFO_WINDCODE, S_TYPE_ST, ENTRY_DT, REMOVE_DT, ANN_DT, REASON FROM wind.ASHAREST where 
    ENTRY_DT <= %s and (remove_dt > %s or remove_dt is null) and s_type_st in ('S', 'L');""" % (target_date, target_date)

    st_info = pd.read_sql(sql, db)
    remove_stocks = st_info[st_info['S_TYPE_ST'] == 'L']['S_INFO_WINDCODE'].to_list()
    st_info = st_info[~st_info['S_INFO_WINDCODE'].isin(remove_stocks)]
    st_stocks = st_info['S_INFO_WINDCODE'].to_list()
    return st_stocks


if __name__ == '__main__':
    df = available_stocks()
    df.to_csv('files/ipo_df.csv')