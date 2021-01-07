import pymysql
import pandas as pd
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