import pandas as pd
import pymysql
import datetime
import numpy as np
import argparse

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

def insert_float_volume_df(source_hdf,to_hdf,float_volume_data):
    if source_hdf:
        source_hdf_file = pd.read_hdf(source_hdf)
        source_hdf_file = source_hdf_file.append(float_volume_data, ignore_index=True)
    else:
        source_hdf_file = pd.DataFrame(float_volume_data)
    h5_store = pd.HDFStore(to_hdf, mode='a')
    h5_store['data'] = source_hdf_file
    h5_store.close()

# parser = argparse.ArgumentParser(description="Read Database to HDF5_FILE")
# parser.add_argument('--begin-date', default=True, help="begin_date")
# parser.add_argument('--end-date', default=True, help="end-date")
# parser.add_argument('--float_volume-source-hdf',required=False,help='History float_volume_hdf5_file')
# parser.add_argument('--float-volume-to-hdf',required=True,help='Target float_volume_hdf5_path_file')
# args = parser.parse_args()
#
# df = get_float_volume_between_begin_end_date(args.begin_date, args.end_date)
# insert_float_volume_df(args.float_volume_source_hdf,args.float_volume_to_hdf,df)
begin_date = '20200701'
end_date = '20200718'
df = get_float_volume_between_begin_end_date(begin_date, end_date)
insert_float_volume_df('/home/banruo/float_volume_between.hdf5','/home/banruo/float_volume_between.hdf5',df)
# h5_store = pd.HDFStore('/home/banruo/float_volume_between.hdf5', mode='a')
# h5_store['data'] = df
# h5_store.close()

