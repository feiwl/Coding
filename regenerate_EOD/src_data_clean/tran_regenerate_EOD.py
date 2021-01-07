__author__ = 'SharonYU'

import pandas as pd
import pymongo
import datetime
import copy
import numpy as np

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)

def new_daily_eod(data_cursor: pymongo.cursor, target_time: int) -> pd.DataFrame:
    '''
    This function aims to speed up the time cost of former split method which split the data day by day
    The main difference is that the function directly read data from mongodb and split the data for all days in one call
    params:
    data_cursor: pymongo.cursor contain all collection for a single stock from begin date to end
    target_time: int64, the target time we want to regenerate EOD
    '''

    # to identify the period inside a date, we denote A as the first half day, and B as the rest
    # which means in the afternoon is A, in the morning is B

    # init values and record the first day
    result_record = []
    try:
        value_ = data_cursor.next()
    except:
        return None
    first_day = value_['nActionDay']
    crt_day = first_day

    # record the current day
    while True:

        while value_['nNumTrades'] == 0:
            try:
                value_ = data_cursor.next()
            except:
                break
        # update morning record
        morning_record = copy.deepcopy(value_)
        try:
            value_ = data_cursor.next()
        except:
            break

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
                morning_record['nTime'] = value_['nTime']

            # call the next iteration
            try:
                value_ = data_cursor.next()
            except:
                # print('no record left')
                break

        # rename the morning dataframe
        for item in [key for key in morning_record.keys() if key != 'szWindCode']:
            morning_record[item + '_B'] = morning_record.pop(item)

        # get afternoon dataframe
        # for the first in the afternoon
        while (value_['nNumTrades'] == 0) & (value_['nTime'] < 1500e5):
            try:
                value_ = data_cursor.next()
            except:
                break

        afternoon_record = copy.deepcopy(value_)
        try:
            value_ = data_cursor.next()
        except:
            break
        # for the rest in the afternoon
        while value_['nActionDay'] == crt_day:

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
                afternoon_record['nTime'] = value_['nTime']

            # call iterate
            try:
                value_ = data_cursor.next()
            except:
                break

        # rename afternoon dataframe
        for item in [key for key in afternoon_record.keys() if key != 'szWindCode']:
            afternoon_record[item + '_A'] = afternoon_record.pop(item)

        # append the records
        # if this is the first day, we only keep the afternoon record for second day's use
        # else we keep both the morning and afternoon record and record as B and A date results
        if crt_day == first_day:
            yesterday_df = afternoon_record
            del morning_record
        else:
            del morning_record['szWindCode']
            result_record.append({**yesterday_df, **morning_record})
            del morning_record
            yesterday_df = copy.deepcopy(afternoon_record)

        # update current day
        crt_day = value_['nActionDay']

    result = pd.DataFrame(result_record)
    try:
        # adjust data types
        result[['nOpen_A', 'nHigh_A', 'nLow_A', 'nMatch_A', 'iVolume_A', 'iTurnover_A', 'nNumTrades_A',
                'nOpen_B', 'nHigh_B', 'nLow_B', 'nMatch_B', 'iVolume_B', 'iTurnover_B', 'nNumTrades_B']] \
            = result[['nOpen_A', 'nHigh_A', 'nLow_A', 'nMatch_A', 'iVolume_A', 'iTurnover_A', 'nNumTrades_A',
                      'nOpen_B', 'nHigh_B', 'nLow_B', 'nMatch_B', 'iVolume_B', 'iTurnover_B', 'nNumTrades_B']].astype(
            'int64')

        result[['szWindCode', 'nActionDay_A', 'nActionDay_B']] = \
            result[['szWindCode', 'nActionDay_A', 'nActionDay_B']].astype(np.str)

        result = result[result.columns.drop(['nTime_A', 'nTime_B', 'bar_close_A', 'bar_close_B'])]
    except:
        result = None

    return result

if __name__ == '__main__':

    # get data
    target_time = 140000000
    stock_id = '000017.SZ'
    begin_date = '20190101'
    end_date = '20200101'

    from regenerate_EOD.utils.read_minite_bar import ReadMinBarMongoDB

    begin_time = datetime.datetime.now()
    data = ReadMinBarMongoDB(begin_date=begin_date, end_date=end_date, id=stock_id, type='tran')
    cursor = data.read_all_days()

    df = new_daily_eod(cursor, target_time)

    print(datetime.datetime.now() - begin_time)
