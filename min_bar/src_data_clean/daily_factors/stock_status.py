import pandas as pd
import pymysql
import datetime
import tqdm
import numpy as np


db = pymysql.connect(host='192.168.1.225',
                     port=3306,
                     user='wind_user',
                     password='Q#wind2$%pvt')

def available_stocks(begin_date, end_date):

    """
    Find all stocks in the market that have listed before the begin_date
    :return:
    dataframe with stock_id, ipo_date, delist_date
    """
    sql = """SELECT S_INFO_WINDCODE, S_INFO_LISTDATE, S_INFO_DELISTDATE FROM wind.ASHAREDESCRIPTION
            where s_info_listdate is not null
            and s_info_listdate <= %s and (s_info_delistdate >= %s or s_info_delistdate is null)
            and s_info_windcode not REGEXP '^[a-zA-Z]';""" % (end_date, begin_date)

    df = pd.read_sql(sql, db)
    return df


def get_st_stocks(begin_date, end_date):
    '''
    Find all ST info during the begin date and end date
    :param begin_date: str
    :param end_date: str
    :return:
    dataframe with stock_id, type of ST, entry date of ST and remove date
    '''
    global db
    sql = """SELECT S_INFO_WINDCODE, S_TYPE_ST, ENTRY_DT, REMOVE_DT FROM wind.ASHAREST
             where  entry_dt <= %s and (remove_dt >= %s or remove_dt is null)
             and s_type_st in ('S', 'L');""" % (end_date, begin_date)

    st_info = pd.read_sql(sql, db)
    return st_info

# get trade calender
def get_trade_dates(begin_date: str, end_date: str) -> list:
    global db
    sql = "SELECT * FROM wind.ASHARECALENDAR where trade_days >= %s and trade_days <= %s " \
          "and S_INFO_EXCHMARKET = 'SZSE';" % (begin_date, end_date)

    df = pd.read_sql(sql, db)
    trade_days = df['TRADE_DAYS'].to_list()

    if len(trade_days) == 0:
        raise ValueError('no trading days between %s and %s' %(begin_date, end_date))
    return sorted(trade_days)


def get_stock_status(begin_date: str, end_date: str) -> pd.DataFrame:
    """
    return trading status of stocks between the begin date and end date
    Notes:
    1. if begin date and end date are the same, result is a series. If this date is not a trading day, Raise Error
    2. if begin date and end date are not the same, it returns the volume between two trade dates. If there is no
    trade date between the begin and end date, Raise Error.
    :param begin_date:str
    :param end_date:str
    :return:
    get a pivot table of stock float a volume, index is date, column is stock
    status=0 means ST stock, status=1 means normal stock,
    status=-1 means not list yet status=2 means delist already
    """
    # get basis value
    ipo_df = available_stocks(begin_date, end_date)
    st_info_df = get_st_stocks(begin_date, end_date)
    trade_days = get_trade_dates(begin_date, end_date)

    trade_days = np.array(trade_days)

    st_info_df.sort_values(['S_INFO_WINDCODE', 'ENTRY_DT'], inplace=True)
    st_info_df.reset_index(drop=True, inplace=True)

    ipo_df.index = ipo_df['S_INFO_WINDCODE']
    # init values
    data_append = []
    # do the iteration over stocks
    for item in tqdm.tqdm(ipo_df.S_INFO_WINDCODE.unique()):

        list_date = ipo_df.loc[item, 'S_INFO_LISTDATE']
        delist_date = ipo_df.loc[item, 'S_INFO_DELISTDATE']

        # Find trading days between the list date end delist date of this stock
        date_notlist = list(trade_days[trade_days < list_date])

        target_days = trade_days[trade_days >= list_date]
        if delist_date:
            target_days = list(target_days[target_days < delist_date])
            date_delist = list(trade_days[trade_days >= delist_date])
        else:
            date_delist = []

        # Deal with stocks that have ST information
        trade_dates_iter = np.array(target_days.copy())
        day_iter = iter(trade_dates_iter)
        crt_day = next(day_iter)

        if item in st_info_df['S_INFO_WINDCODE'].tolist():

            ds = st_info_df[st_info_df['S_INFO_WINDCODE'] == item]

            # if this stock only have one ST record
            if len(ds) == 1:

                entry_dt = ds['ENTRY_DT'].values[0]
                remove_dt = ds['REMOVE_DT'].values[0]

                # find date need to append
                date_st = []
                date_normal = []

                # do the iteration over dates
                while True:

                    if crt_day >= entry_dt:
                        if remove_dt:
                            if crt_day < remove_dt:
                                date_st.append(crt_day)
                            else:
                                date_normal.append(crt_day)
                        else:
                            date_st.append(crt_day)
                    else:
                        date_normal.append(crt_day)

                    try:
                        crt_day = next(day_iter)
                    except:
                        break

                assert set(date_notlist + date_st + date_normal + date_delist) == \
                       set(trade_days), 'days are not paired'
                assert len(date_notlist + date_st + date_normal + date_delist) == \
                       len(trade_days), 'days length not right'

                data_append.extend([{'id': item,
                                     'date': date_item,
                                     'status': 0} for date_item in date_st])
                data_append.extend([{'id': item,
                                     'date': date_item,
                                     'status': 1} for date_item in date_normal])
                data_append.extend([{'id': item,
                                     'date': date_item,
                                     'status': -1} for date_item in date_notlist])
                data_append.extend([{'id': item,
                                     'date': date_item,
                                     'status': 2} for date_item in date_delist])

            # if this stock have several ST records, do the iteration over these records
            else:
                row_iterator = ds.itertuples(index = False)
                value_new = next(row_iterator)
                value_new = pd.Series(dict(zip(ds.columns, list(value_new))))

                date_st = []
                date_normal = []

                while True:

                    entry_dt = value_new['ENTRY_DT']
                    remove_dt = value_new['REMOVE_DT']

                    last_day = remove_dt if remove_dt else target_days[-1]

                    while crt_day <= last_day:
                        if crt_day >= entry_dt:
                            if remove_dt:
                                if crt_day < remove_dt:
                                    if crt_day not in date_st:
                                        date_st.append(crt_day)
                                else:
                                    date_normal.append(crt_day)
                            else:
                                date_st.append(crt_day)
                        else:
                            date_normal.append(crt_day)

                        try:
                            crt_day = next(day_iter)
                        except:
                            break

                    try:
                        value_new = next(row_iterator)
                        value_new = pd.Series(dict(zip(ds.columns, list(value_new))))
                    except:
                        break

                # date_st = list(set(date_st))
                # date_normal = list(set(date_normal))
                assert set(date_notlist + date_st + date_normal + date_delist) == \
                       set(trade_days), 'days are not paired'
                assert len(date_notlist + date_st + date_normal + date_delist) == \
                       len(trade_days), 'days length not right'

                data_append.extend([{'id': item,
                                     'date': date_item,
                                     'status': 0} for date_item in date_st])
                data_append.extend([{'id': item,
                                     'date': date_item,
                                     'status': 1} for date_item in date_normal])
                data_append.extend([{'id': item,
                                     'date': date_item,
                                     'status': -1} for date_item in date_notlist])
                data_append.extend([{'id': item,
                                     'date': date_item,
                                     'status': 2} for date_item in date_delist])

        # deal with stocks that don't have ST records, then all the trading status are normal
        else:

            data_append.extend([{'id': item,
                                 'date': date_item,
                                 'status': 1} for date_item in target_days])
            data_append.extend([{'id': item,
                                 'date': date_item,
                                 'status': -1} for date_item in date_notlist])
            data_append.extend([{'id': item,
                                 'date': date_item,
                                 'status': 2} for date_item in date_delist])


    # combine the append records
    ds = pd.DataFrame.from_dict(data_append)
    # sort value
    ds.sort_values(['id', 'date'], inplace=True)
    # only keep the trade days rows
    ds = ds[ds['date'].isin(trade_days)]
    # append this to the dataframe
    df = ds.pivot(columns='id', index='date', values='status')
    # sort table
    df.sort_index(inplace = True)
    df = df[sorted(df.columns)]
    return df


if __name__ == '__main__':
    begin_date = '20190101'
    end_date = '20200106'
    df = get_stock_status(begin_date, end_date)
    print(df)
    print(df.info())
    df.to_csv('status.csv')

    df.to_hdf('status_df_hist.hdf5', 'hello', mode = 'w')