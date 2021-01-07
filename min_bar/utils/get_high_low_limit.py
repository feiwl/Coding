import pandas as pd
import pymysql


def get_EOD_data(db, target_date):
    sql = "SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_HIGH, S_DQ_LOW FROM wind.ASHAREEODPRICES " \
          "WHERE TRADE_DT = %s;" % target_date

    return pd.read_sql(sql, db)


def get_st_stocks(db, target_date):
    sql = """SELECT S_INFO_WINDCODE, S_TYPE_ST, ENTRY_DT, REMOVE_DT, ANN_DT, REASON FROM wind.ASHAREST where 
    ENTRY_DT <= %s and (remove_dt > %s or remove_dt is null) and s_type_st in ('S', 'L');""" % (target_date, target_date
                                                                                                )

    st_info = pd.read_sql(sql, db)
    remove_stocks = st_info[st_info['S_TYPE_ST'] == 'L']['S_INFO_WINDCODE'].to_list()
    st_info = st_info[~st_info['S_INFO_WINDCODE'].isin(remove_stocks)]
    st_stocks = st_info['S_INFO_WINDCODE'].to_list()
    return st_stocks


def compute_high_low_limit(target_date):
    # database
    db = pymysql.connect(host='192.168.1.225',
                         port=3306,
                         user='wind_user',
                         password='Q#wind2$%pvt')

    EOD_dataframe = get_EOD_data(db, target_date)
    st_stocks = get_st_stocks(db, target_date)

    # for normal stocks
    EOD_dataframe['HighLimit'] = (EOD_dataframe['S_DQ_PRECLOSE'] * 1.1 * 1e4 + 1e-5).round(-2).astype(int)
    EOD_dataframe['LowLimit'] = (EOD_dataframe['S_DQ_PRECLOSE'] * .9 * 1e4 + 1e-5).round(-2).astype(int)

    # 科创板股票
    EOD_dataframe.loc[EOD_dataframe['S_INFO_WINDCODE'].str.startswith('688'), 'HighLimit'] = (
                EOD_dataframe.loc[EOD_dataframe['S_INFO_WINDCODE'].str.startswith('688'), 'S_DQ_PRECLOSE'] * 1.2 * 1e4
                + 1e-5).round(-2).astype(int)
    EOD_dataframe.loc[EOD_dataframe['S_INFO_WINDCODE'].str.startswith('688'), 'LowLimit'] = (
                EOD_dataframe.loc[EOD_dataframe['S_INFO_WINDCODE'].str.startswith('688'), 'S_DQ_PRECLOSE'] * .8 * 1e4
                + 1e-5).round(-2).astype(int)

    # st 股票
    EOD_dataframe.loc[EOD_dataframe['S_INFO_WINDCODE'].isin(st_stocks), 'HighLimit'] = (
                EOD_dataframe.loc[EOD_dataframe['S_INFO_WINDCODE'].isin(st_stocks), 'S_DQ_PRECLOSE'] * 1.05 * 1e4 +
                1e-5).round(-2).astype(int)
    EOD_dataframe.loc[EOD_dataframe['S_INFO_WINDCODE'].isin(st_stocks), 'LowLimit'] = (
                EOD_dataframe.loc[EOD_dataframe['S_INFO_WINDCODE'].isin(st_stocks), 'S_DQ_PRECLOSE'] * .95 * 1e4 +
                1e-5).round(-2).astype(int)

    # 未来创业板股票
    EOD_dataframe.loc[(EOD_dataframe['S_INFO_WINDCODE'].str.startswith('300')) &
                      (EOD_dataframe['TRADE_DT'] > '20250101'), 'HighLimit'] = \
        (EOD_dataframe.loc[(EOD_dataframe['S_INFO_WINDCODE'].str.startswith('300')) &
                           (EOD_dataframe['TRADE_DT'] > '20250101'), 'S_DQ_PRECLOSE'] * 1.2e4 + 1e-5).round(-2)\
            .astype(int)
    EOD_dataframe.loc[(EOD_dataframe['S_INFO_WINDCODE'].str.startswith('300')) &
                      (EOD_dataframe['TRADE_DT'] > '20250101'), 'LowLimit'] = \
        (EOD_dataframe.loc[(EOD_dataframe['S_INFO_WINDCODE'].str.startswith('300')) &
                           (EOD_dataframe['TRADE_DT'] > '20250101'), 'S_DQ_PRECLOSE'] * .8e4 + 1e-5).round(-2)\
            .astype(int)

    return EOD_dataframe[['S_INFO_WINDCODE', 'HighLimit', 'LowLimit']]