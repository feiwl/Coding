__author__ = 'SharonYU'

import pandas as pd
import pymysql
import datetime
import copy
import numpy as np
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)


def split_df(daily_minute_bar: pd.DataFrame, target_time: int) -> (pd.DataFrame, pd.DataFrame):
    # to identify the period inside a date, we denote A as the first half day, and B as the rest
    # which means in the afternoon is A, in the morning is B
    # init values
    columns = daily_minute_bar.columns.tolist()
    row_iterator = daily_minute_bar.itertuples(index=False)

    value_ = next(row_iterator)
    # get morning_dataframe
    # for the first record
    morning_record = pd.Series(dict(zip(columns, list(value_))))

    # for the rest records in the morning
    while getattr(value_, 'nTime') < target_time:
        # call the next iteration
        try:
            value_ = next(row_iterator)
            morning_record = pd.Series(dict(zip(columns, list(value_))))
        except:
            print('no record left')
            break

    # rename the morning dataframe
    morning_rename = {}
    for item in morning_record.index[1:]:
        morning_rename[item] = item + '_B'
    morning_record.rename(morning_rename, inplace=True)

    # get afternoon dataframe
    # for the first in the afternoon
    while getattr(value_, 'nTime') < 1500e5:
        value_ = next(row_iterator)

    afternoon_record = pd.Series(dict(zip(columns, list(value_))))

    # for the rest in the afternoon
    while True:
        # call iterate
        try:
            value_ = next(row_iterator)
        except:
            break

        afternoon_record = pd.Series(dict(zip(columns, list(value_))))

    # rename afternoon dataframe
    afternoon_rename = {}
    for item in afternoon_record.index[1:]:
        afternoon_rename[item] = item + '_A'
    afternoon_record.rename(afternoon_rename, inplace=True)

    return morning_record, afternoon_record


def get_stock_data(stock_id: str, begin_date: str, end_date: str) -> pd.DataFrame:
    """
    according to the begin_date and the end_date, retrieve all the transaction minute bar
    :param stock_id:
    :param begin_date:
    :param end_date:
    :return: pd.DataFrame, minute bar between the begin_date and the end_date
    """
    # minute bar
    target_column = ['szWindCode', 'nActionDay', 'nTime', 'nOpenIndex', 'nHighIndex',
                     'nLowIndex', 'nLastIndex', 'iTotalVolume', 'iTurnover', 'nPreCloseIndex']

    """ get data"""
    db = pymysql.connect(host='192.168.10.68',
                         port=3306,
                         user='nas',
                         password='Prism@123456')

    sql = """SELECT symbol.szWindCode, snap_min_index.* FROM marketdata.snap_min_index join marketdata.symbol where snap_min_index.codeID = symbol.codeID 
             and symbol.szWindCode = '%s' and snap_min_index.nActionDay >= %s and snap_min_index.nActionDay <= %s;""" \
          % (stock_id, begin_date, end_date)

    df = pd.read_sql(sql, db)
    df = df[target_column]
    return df


def new_daily_eod(history_min_bar: pd.DataFrame, target_min: int) -> pd.DataFrame:
    """
    given by the target time, and the minute bar records dataframe, generate the new EOD data
    :param history_min_bar:
    :param target_min:
    :return: new eod dataframe
    """
    target_column = ['szWindCode', 'nActionDay', 'nTime', 'nOpenIndex', 'nHighIndex',
                     'nLowIndex', 'nLastIndex', 'iTotalVolume', 'iTurnover', 'nPreCloseIndex']

    history_min_bar.rename(columns={'iTotalVolume': 'iTotalVolumeIndex',
                                    'iTurnover': 'iTurnoverIndex'},
                           inplace=True)
    dates = sorted(list(history_min_bar.nActionDay.unique()))
    # for the first record
    ds = history_min_bar[history_min_bar['nActionDay'] == dates[0]]

    _, yesterday_frame = split_df(ds, target_min)
    records = []
    del _

    for date in dates[1:]:
        ds = history_min_bar[history_min_bar['nActionDay'] == date]
        dataframe_a, dataframe_b = split_df(ds, target_min)
        records.append(yesterday_frame.append(dataframe_a.drop('szWindCode')))
        del dataframe_a
        yesterday_frame = copy.deepcopy(dataframe_b)

    # concat all result
    result = pd.concat(records, axis = 1, ignore_index=True).T
    result[result.columns.drop(['szWindCode', 'nActionDay_A', 'nActionDay_B'])] = \
        result[result.columns.drop(['szWindCode', 'nActionDay_A', 'nActionDay_B'])].astype('int64')

    result[['szWindCode', 'nActionDay_A', 'nActionDay_B']] = \
        result[['szWindCode', 'nActionDay_A', 'nActionDay_B']].astype(np.str)
    result = result[result.columns.drop(['nTime_A', 'nTime_B'])]
    return result


if __name__ == '__main__':
    begin_time = datetime.datetime.now()
    # get data
    target_time = 140000000
    stock_id = '000300.SH'
    begin_date = '20190601'
    end_date = '20200630'

    # retrieve data
    min_bar = get_stock_data(stock_id, begin_date, end_date)
    print(min_bar.info())
    new_EOD = new_daily_eod(min_bar, target_time)
    new_EOD.to_csv('EOD_index.csv', index = False)
    print(new_EOD.info())
    # print(new_EOD.head())
    print(datetime.datetime.now() - begin_time)
