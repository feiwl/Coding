__author__ = 'SharonYU'

import pandas as pd
import pymongo
import datetime
import copy
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)

def new_daily_eod(data_cursor: pymongo.cursor, target_time: int) -> pd.DataFrame:
    '''
    This function aims to speed up the time cost of former split method which split the data day by day
    The main difference is that the function directly read data from mongodb and split the data for all days in one call
    params:
    data_cursor: pymongo.cursor contain all collection for a single index from begin date to end
    target_time: int64, the target time we want to regenerate EOD
    '''

    # to identify the period inside a date, we denote A as the first half day, and B as the rest
    # which means in the afternoon is A, in the morning is B

    # init values and record the first day
    result_record = []
    value_ = data_cursor.next()
    first_day = value_['nActionDay']
    crt_day = first_day

    # record the current day
    while True:

        while value_['iVolume'] == 0:
            value_ = data_cursor.next()

        # update morning record
        morning_record = copy.deepcopy(value_)
        try:
            value_ = data_cursor.next()
        except:
            break

        # for the rest records in the morning
        while value_['nTime'] < target_time:
            if value_['iVolume'] == 0:
                pass
            else:
                # update the nMatch price
                morning_record['nLastIndex'] = value_['nLastIndex']

                # update the high low price
                morning_record['nHighIndex'] = max(morning_record['nHighIndex'], value_['nHighIndex'])
                morning_record['nLowIndex'] = min(morning_record['nLowIndex'], value_['nLowIndex'])

                # update volume and amount
                morning_record['iVolume'] += value_['iVolume']
                morning_record['iTurnover'] += value_['iTurnover']

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
        while (value_['iVolume'] == 0) & (value_['nTime'] < 1500e5):
            value_ = data_cursor.next()

        afternoon_record = copy.deepcopy(value_)
        value_ = data_cursor.next()
        # for the rest in the afternoon
        while value_['nActionDay'] == crt_day:

            if value_['iVolume'] == 0:
                # if no trade happens in this minute
                pass
            else:
                # update the nMatch price
                afternoon_record['nLastIndex'] = value_['nLastIndex']

                # update the high low price
                afternoon_record['nHighIndex'] = max(afternoon_record['nHighIndex'], value_['nHighIndex'])
                afternoon_record['nLowIndex'] = min(afternoon_record['nLowIndex'], value_['nLowIndex'])

                # update volume and amount
                afternoon_record['iVolume'] += value_['iVolume']
                afternoon_record['iTurnover'] += value_['iTurnover']

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

    # adjust data types
    result[result.columns.drop(['szWindCode', 'nActionDay_A', 'nActionDay_B'])] = \
        result[result.columns.drop(['szWindCode', 'nActionDay_A', 'nActionDay_B'])].astype('int64')

    result[['szWindCode', 'nActionDay_A', 'nActionDay_B']] = \
        result[['szWindCode', 'nActionDay_A', 'nActionDay_B']].astype(str)

    result = result[result.columns.drop(['nTime_A', 'nTime_B'])]

    # rename some columns to differ from those stock columns
    result.rename(columns={'iVolume_A': 'iVolumeIndex_A',
                           'iVolume_B': 'iVolumeIndex_B',
                           'iTurnover_A': 'iTurnoverIndex_A',
                           'iTurnover_B': 'iTurnoverIndex_B'},
                           inplace=True)

    return result

if __name__ == '__main__':
    begin_time = datetime.datetime.now()
    # get data
    target_time = 140000000
    stock_id = '000300.SH'
    begin_date = '20190601'
    end_date = '20200630'

    from regenerate_EOD.utils.read_minite_bar import ReadMinBarMongoDB
    data = ReadMinBarMongoDB(begin_date=begin_date, end_date=end_date, id=stock_id, type='snap_index')
    cursor = data.read_all_days()
    df = new_daily_eod(cursor, target_time)

    print(df.info())
    test_array = df.to_records(index=False)
    shape, dtype = test_array.shape, test_array.dtype
    print(shape, dtype)
    end_time = datetime.datetime.now()
    print(end_time - begin_time)
    # print(df)
