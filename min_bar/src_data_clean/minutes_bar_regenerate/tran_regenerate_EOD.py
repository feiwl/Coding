__author__ = 'ZhouTw'

import pandas as pd
import pymysql
import datetime
import copy
import numpy as np

from min_bar.config.config import file_dir_new_EOD
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)


def split_df(daily_minute_bar: pd.DataFrame, target_time: int) -> (pd.DataFrame, pd.DataFrame):
    # to identify the period inside a date, we denote A as the first half day, and B as the rest
    # which means in the afternoon is A, in the morning is B
    # init values
    row_iterator = daily_minute_bar.iterrows()
    index_, value_ = next(row_iterator)
    # get morning_dataframe
    # for the first record
    while value_['nNumTrades'] == 0:
        index_, value_ = next(row_iterator)
    morning_record = value_
    morning_record.drop(labels=['barClose', 'S_DQ_PRECLOSE', 'nTime'], inplace=True)
    index_, value_ = next(row_iterator)

    # for the rest records in the morning
    while value_['nTime'] < target_time:
        if value_['nNumTrades'] == 0:
            pass
        else:
            # update the nMatch price
            morning_record['nMatch'] = value_['nMatch']

            # update the high low price
            morning_record['nHigh'] = max(morning_record['nHigh'], value_['nHigh'])
            morning_record['nLow'] = min(morning_record['nLow'], value_['nLow'])

            # update volume and amount
            morning_record['iVolume'] += value_['iVolume']
            morning_record['iTurnover'] += value_['iTurnover']

            # update size of trade
            morning_record['nNumTrades'] += value_['nNumTrades']

        # call the next iteration
        try:
            index_, value_ = next(row_iterator)
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
    while (value_['nNumTrades'] == 0) & (value_['nTime'] < 150000000):
        index_, value_ = next(row_iterator)

    afternoon_record = value_.copy()
    afternoon_record.drop(labels=['barClose', 'S_DQ_PRECLOSE', 'nTime'], inplace=True)

    # for the rest in the afternoon
    while True:
        # call iterate
        try:
            index_, value_ = next(row_iterator)
        except:
            break

        if value_['nNumTrades'] == 0:
            # if no trade happens in this minute
            pass
        else:
            # update the nMatch price
            afternoon_record['nMatch'] = value_['nMatch']

            # update the high low price
            afternoon_record['nHigh'] = max(afternoon_record['nHigh'], value_['nHigh'])
            afternoon_record['nLow'] = min(afternoon_record['nLow'], value_['nLow'])

            # update volume and amount
            afternoon_record['iVolume'] += value_['iVolume']
            afternoon_record['iTurnover'] += value_['iTurnover']

            # update size of trade
            afternoon_record['nNumTrades'] += value_['nNumTrades']

    # rename afternoon dataframe
    afternoon_rename = {}
    for item in afternoon_record.index[1:]:
        afternoon_rename[item] = item + '_A'
    afternoon_record.rename(afternoon_rename, inplace=True)

    assert afternoon_record['iVolume_A'] + morning_record['iVolume_B'] == daily_minute_bar['iVolume'].sum(),\
        "calculation error, sum unequal for date %s" % str(value_['nActionDay'])

    # change the type of the record
    return morning_record, afternoon_record


def split_df_tuple(daily_minute_bar: pd.DataFrame, target_time: int) -> (pd.DataFrame, pd.DataFrame):
    # to identify the period inside a date, we denote A as the first half day, and B as the rest
    # which means in the afternoon is A, in the morning is B
    # init values
    row_iterator = daily_minute_bar.itertuples()
    value_ = next(row_iterator)

    # get morning_dataframe
    columns = daily_minute_bar.columns.drop(['nTime'])
    morning_record = pd.Series(dict(zip(columns, np.repeat(None, len(columns)))))
    # for the first record
    while getattr(value_, 'nNumTrades') == 0:
        value_ = next(row_iterator)
    # update morning record
    for item in columns:
        morning_record[item] = getattr(value_, item)

    value_ = next(row_iterator)
    # for the rest records in the morning
    while getattr(value_, 'nTime') < target_time:
        if getattr(value_, 'nNumTrades') == 0:
            pass
        else:
            # update the nMatch price
            morning_record['nMatch'] = getattr(value_, 'nMatch')

            # update the high low price
            morning_record['nHigh'] = max(morning_record['nHigh'], getattr(value_, 'nHigh'))
            morning_record['nLow'] = min(morning_record['nLow'], getattr(value_, 'nLow'))

            # update volume and amount
            morning_record['iVolume'] += getattr(value_, 'iVolume')
            morning_record['iTurnover'] += getattr(value_, 'iTurnover')

            # update size of trade
            morning_record['nNumTrades'] += getattr(value_, 'nNumTrades')

        # call the next iteration
        try:
            value_ = next(row_iterator)
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
    while (getattr(value_, 'nNumTrades') == 0) & (getattr(value_, 'nTime') < 1500e5):
        value_ = next(row_iterator)

    afternoon_record = pd.Series(dict(zip(columns, np.repeat(None, len(columns)))))
    for item in columns:
        afternoon_record[item] = getattr(value_, item)

    # for the rest in the afternoon
    while True:
        # call iterate
        try:
            value_ = next(row_iterator)
        except:
            break

        if getattr(value_, 'nNumTrades') == 0:
            # if no trade happens in this minute
            pass
        else:
            # update the nMatch price
            afternoon_record['nMatch'] = getattr(value_, 'nMatch')

            # update the high low price
            afternoon_record['nHigh'] = max(afternoon_record['nHigh'], getattr(value_, 'nHigh'))
            afternoon_record['nLow'] = min(afternoon_record['nLow'], getattr(value_, 'nLow'))

            # update volume and amount
            afternoon_record['iVolume'] += getattr(value_, 'iVolume')
            afternoon_record['iTurnover'] += getattr(value_, 'iTurnover')

            # update size of trade
            afternoon_record['nNumTrades'] += getattr(value_, 'nNumTrades')

    # rename afternoon dataframe
    afternoon_rename = {}
    for item in afternoon_record.index[1:]:
        afternoon_rename[item] = item + '_A'
    afternoon_record.rename(afternoon_rename, inplace=True)

    assert afternoon_record['iVolume_A'] + morning_record['iVolume_B'] == daily_minute_bar['iVolume'].sum(),\
        "calculation error, sum unequal for date %s" % str(value_['nActionDay'])

    # change the type of the record
    return morning_record, afternoon_record


def get_stock_data(stock_id: str, begin_date: str, end_date: str) -> pd.DataFrame:
    """
    according to the begin_date and the end_date, retrieve all the transaction minute bar
    :param stock_id: str stock code
    :param begin_date: str begin date
    :param end_date: str end date
    :return: pd.DataFrame, minute bar between the begin_date and the end_date
    """
    # minute bar
    db = pymysql.connect(host='192.168.10.68',
                         port=3306,
                         user='nas',
                         password='Prism@123456')

    sql = """SELECT symbol.szWindCode,  transaction.nActionDay, transaction.nTime, transaction.nOpen, transaction.nHigh,
             transaction.nLow, transaction.nMatch, transaction.iVolume, transaction.iTurnover, transaction.barClose, 
             transaction.S_DQ_PRECLOSE, transaction.nNumTrades, transaction.S_DQ_ADJFACTOR, transaction.HighLimit, 
             transaction.LowLimit FROM marketdata.transaction join marketdata.symbol where 
             transaction.codeID = symbol.codeID and symbol.szWindCode = '%s' and transaction.nActionDay >= %s 
             and transaction.nActionDay <= %s;""" \
          % (stock_id, begin_date, end_date)

    df = pd.read_sql(sql, db)
    return df


def new_daily_eod(history_min_bar: pd.DataFrame, target_min: int) -> pd.DataFrame:
    """
    given by the target time, and the minute bar records dataframe, generate the new EOD data
    :param history_min_bar:
    :param target_min:
    :return: new eod dataframe
    """
    # stock_id = history_min_bar['szWindCode'].unique()
    # print(history_min_bar)
    target_columns = ['szWindCode', 'nActionDay', 'nTime', 'nOpen', 'nHigh',
                      'nLow', 'nMatch', 'iVolume', 'iTurnover','nNumTrades']

    history_min_bar = history_min_bar[target_columns]
    dates = sorted(list(history_min_bar.nActionDay.unique()))
    # for the first record
    ds = history_min_bar[history_min_bar['nActionDay'] == dates[0]]

    _, yesterday_frame = split_df_tuple(ds, target_min)
    records = []
    del _

    for date in dates[1:]:
        ds = history_min_bar[history_min_bar['nActionDay'] == date]
        dataframe_a, dataframe_b = split_df_tuple(ds, target_min)

        records.append(yesterday_frame.append(dataframe_a.drop('szWindCode')))
        del dataframe_a
        yesterday_frame = copy.deepcopy(dataframe_b)

    # concat all result
    result = pd.concat(records, axis = 1, ignore_index=True).T
    result[['nOpen_A', 'nHigh_A', 'nLow_A', 'nMatch_A', 'iVolume_A', 'iTurnover_A', 'nNumTrades_A',
            'nOpen_B', 'nHigh_B', 'nLow_B', 'nMatch_B', 'iVolume_B', 'iTurnover_B', 'nNumTrades_B']]\
        = result[['nOpen_A', 'nHigh_A', 'nLow_A', 'nMatch_A', 'iVolume_A', 'iTurnover_A', 'nNumTrades_A',
                  'nOpen_B', 'nHigh_B', 'nLow_B', 'nMatch_B', 'iVolume_B', 'iTurnover_B', 'nNumTrades_B']].astype('int64')

    result[['szWindCode', 'nActionDay_A', 'nActionDay_B']] = \
        result[['szWindCode', 'nActionDay_A', 'nActionDay_B']].astype(np.str)
    # result[['S_DQ_ADJFACTOR_A', 'S_DQ_ADJFACTOR_B']] = result[['S_DQ_ADJFACTOR_A', 'S_DQ_ADJFACTOR_B']].astype('float64')

    return result


if __name__ == '__main__':
    begin_time = datetime.datetime.now()
    # get data
    target_time = 140000000
    stock_id = '600539.SH'
    begin_date = '20190601'
    end_date = '20200630'

    # retrieve data
    min_bar = get_stock_data(stock_id, begin_date, end_date)
    print(min_bar.info())
    new_EOD = new_daily_eod(min_bar, target_time)
    new_EOD.to_csv('EOD.csv', index = False)
    print(new_EOD.info())
    # print(new_EOD.head())
    print(datetime.datetime.now() - begin_time)
