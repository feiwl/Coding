import pandas as pd
import pymysql
import datetime
import numpy as np


db = pymysql.connect(host='192.168.1.225',
                     port=3306,
                     user='wind_user',
                     password='Q#wind2$%pvt')


def get_float_volume_before_begin_date(begin_date: str) -> pd.DataFrame:
    # get data before the begin_date
    global db
    sql = """select f.WIND_CODE, f.CHANGE_DT, f.FLOAT_A_SHR
        from (
        	SELECT WIND_CODE, max(CHANGE_DT) as last_CHANGE_DT
        	FROM wind.ASHARECAPITALIZATION
        	where CHANGE_DT <= %s AND WIND_CODE NOT REGEXP '^[a-zA-Z]'
        	group by WIND_CODE
        ) as x inner join wind.ASHARECAPITALIZATION as f on f.WIND_CODE = x.WIND_CODE and f.CHANGE_DT = x.last_CHANGE_DT
        WHERE FLOAT_A_SHR > 0""" % (begin_date)

    return pd.read_sql(sql, db)


def get_volume_change_between_two_dates(begin_date: str, end_date: str) -> pd.DataFrame:
    # get data between the begin date and end date
    global db
    # %% 一年内的总股本
    sql = """select WIND_CODE, CHANGE_DT, FLOAT_A_SHR from wind.ASHARECAPITALIZATION
    where CHANGE_DT >%s and CHANGE_DT <= %s and FLOAT_A_SHR > 0 and WIND_CODE not REGEXP '^[a-zA-Z]';""" % (
    begin_date, end_date)

    return pd.read_sql(sql, db)


# get trade calender
def get_trade_dates(begin_date: str, end_date: str) -> list:
    global db
    sql = "SELECT * FROM wind.ASHARECALENDAR where TRADE_DAYS >= %s and trade_days <= %s " \
          "and S_INFO_EXCHMARKET = 'SZSE';" % (begin_date, end_date)

    df = pd.read_sql(sql, db)
    trade_days = df['TRADE_DAYS'].to_list()
    return sorted(trade_days)


def get_float_volume_between_begin_end_date(begin_date: str, end_date: str) -> pd.DataFrame:
    """
    return float volumes of stocks between the begin date and end date
    Notes:
    1. if begin date and end date are the same, result is a series. It doesn't matther whether the begin date or the end
    date is the trade date. If not trade date, return the volume of the last trade date
    2. if begin date and end date are not the same, it returns the volume between two trade dates. While, if there is no
    trade date between the begin and end date, Raise Error.
    :param begin_date:str
    :param end_date:str
    :return: get a pivot table of stock float a volume, index is date, column is stock
    """
    # get basis value
    if begin_date == end_date:
        result = get_float_volume_before_begin_date(begin_date)
        result['CHANGE_DT'] = begin_date
        result.rename(columns = {'WIND_CODE':'id',
                                 'CHANGE_DT':'date',
                                 'FLOAT_A_SHR':'float_volume'},
                      inplace = True)
        return result.pivot_table(columns='id', index='date', values='float_volume')
    else:
        # get basic data
        volume_a = get_float_volume_before_begin_date(begin_date)
        # print('a complete')
        volume_b = get_volume_change_between_two_dates(begin_date, end_date)
        # print('b complete')
        trade_days = get_trade_dates(begin_date, end_date)
        # print('dates complete')

        # append entries
        volume = volume_a.append(volume_b, ignore_index=True)
        volume.sort_values(['WIND_CODE', 'CHANGE_DT'], inplace=True)
        volume.reset_index(drop=True, inplace=True)

        # init values
        data_append = []

        # do the iteration over date
        for item in volume.WIND_CODE.unique():
            trade_dates_iter = np.array(trade_days.copy())
            #     print(item, datetime.datetime.now())
            ds = volume[volume['WIND_CODE'] == item]
            if len(ds) == 1:
                crt_date = ds['CHANGE_DT'].values[0]
                volume_crt = ds['FLOAT_A_SHR'].values[0]
                # find date need to append
                date_add = trade_dates_iter[trade_dates_iter > crt_date]
                data_append.extend([{'WIND_CODE': item,
                                     'CHANGE_DT': date_item,
                                     'FLOAT_A_SHR': volume_crt} for date_item in date_add])
            else:
                # print(ds)
                #         print('begin at %s' % str(datetime.datetime.now()))
                row_iterator = ds.iterrows()
                index_init, value_init = next(row_iterator)
                while True:
                    try:
                        index_new, value_new = next(row_iterator)
                    except:
                        break

                    volume_crt = value_init['FLOAT_A_SHR']

                    trade_dates_iter = trade_dates_iter[trade_dates_iter > value_init['CHANGE_DT']]
                    if len(trade_dates_iter) == 0:
                        break
                    date_add = trade_dates_iter[trade_dates_iter < value_new['CHANGE_DT']]
                    data_append.extend([{'WIND_CODE': item,
                                         'CHANGE_DT': date_item,
                                         'FLOAT_A_SHR': volume_crt} for date_item in date_add])
                    # update init value
                    value_init = value_new.copy()

                if len(trade_dates_iter) == 0:
                    pass
                else:
                    crt_date = value_init['CHANGE_DT']
                    volume_crt = value_init['FLOAT_A_SHR']
                    date_add = trade_dates_iter[trade_dates_iter > crt_date]
                    data_append.extend([{'WIND_CODE': item,
                                         'CHANGE_DT': date_item,
                                         'FLOAT_A_SHR': volume_crt} for date_item in date_add])
        #         print('end at %s' % str(datetime.datetime.now()))

        # combine the append records
        ds = pd.DataFrame.from_dict(data_append)
        # append this to the dataframe
        df = volume.append(ds, ignore_index=True)
        # sort value
        df.sort_values(['WIND_CODE', 'CHANGE_DT'], inplace=True)
        # only keep the trade days rows
        df = df[df['CHANGE_DT'].isin(trade_days)]
        # reset index
        df.reset_index(drop=True, inplace=True)

        df.rename(columns={'WIND_CODE': 'id',
                               'CHANGE_DT': 'date',
                               'FLOAT_A_SHR': 'float_volume'},
                      inplace=True)

        # reset to a pivot table
        df = df.pivot_table(columns='id', index='date', values='float_volume')
        # sort table
        df.sort_index(inplace = True)
        df = df[sorted(df.columns)]
        return df


if __name__ == '__main__':
    begin_date = '20190101'
    end_date = '20200719'
    df = get_float_volume_between_begin_end_date(begin_date, end_date)
    print(df)
    print(df.info())
    df.to_csv('volume.csv')

    df.to_hdf('volume_df_hist.hdf5', 'hello', mode = 'w')

