import pandas as pd
import pymysql
import tushare as ts
pro = ts.pro_api('3f158d816eb46498558d167cdad44afe09613c812f698d0429cd6c89')
pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 10)


def get_EOD_data(target_date):
    global db
    sql = "SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_HIGH, S_DQ_LOW FROM wind.ASHAREEODPRICES " \
          "WHERE TRADE_DT = %s;" % target_date

    return pd.read_sql(sql, db)


def get_st_stocks(target_date):
    global db
    sql = """SELECT S_INFO_WINDCODE, S_TYPE_ST, ENTRY_DT, REMOVE_DT, ANN_DT, REASON FROM wind.ASHAREST where 
    ENTRY_DT <= %s and (remove_dt > %s or remove_dt is null) and s_type_st in ('S', 'L');""" % (target_date, target_date)

    st_info = pd.read_sql(sql, db)
    remove_stocks = st_info[st_info['S_TYPE_ST'] == 'L']['S_INFO_WINDCODE'].to_list()
    st_info = st_info[~st_info['S_INFO_WINDCODE'].isin(remove_stocks)]
    st_stocks = st_info['S_INFO_WINDCODE'].to_list()
    return st_stocks


def compute_high_low_limit(EOD_dataframe, st_stocks):
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

    return EOD_dataframe


def print_result(eod_frame):
    df1 = eod_frame[eod_frame['S_DQ_HIGH'] > eod_frame['HighLimit']/1e4]
    df2 = eod_frame[eod_frame['S_DQ_LOW'] < eod_frame['LowLimit']/1e4]

    if len(df1) > 0:
        print('high over high_limit stocks:\n')
        print(df1)
    if len(df2) > 0:
        print('low over low limit stocks:\n')
        print(df2)


def get_dates(end_date):
    db_prism = pymysql.connect(host='192.168.10.66',
                               port=3306,
                               user='root',
                               password='prism123')
    sql = 'SELECT * FROM snapshot.tradeable_date where date > 20200101;'
    dates = pd.read_sql(sql, db_prism)
    dates = dates['date'].to_list()
    date_str = [item.strftime('%Y%m%d') for item in dates]
    date_str = list(filter(lambda x: x <= end_date, date_str))
    return date_str


def get_ts_high_low_limit(target_date):
    high_low_limit = pro.stk_limit(trade_date = target_date)
    high_low_limit.drop(columns = ['trade_date'], inplace = True)
    high_low_limit[['up_limit', 'down_limit']] = \
        (high_low_limit[['up_limit', 'down_limit']] * 1e4).round(-2).astype('int')
    return high_low_limit


if __name__ == '__main__':
    target_date = '20180827'
    # database connection
    db = pymysql.connect(host='192.168.1.225',
                         port=3306,
                         user='wind_user',
                         password='Q#wind2$%pvt')

    # multi days
    # date_str = get_dates(target_date)
    #
    # for date_item in date_str:
    #     print(date_item)
    #     eod_frame = get_EOD_data(date_item)
    #     st_stocks = get_st_stocks(date_item)
    #
    #     result = compute_high_low_limit(eod_frame, st_stocks)
    #     print_result(result)

    # one day
    print(target_date)
    eod_frame = get_EOD_data(target_date)
    st_stocks = get_st_stocks(target_date)

    result = compute_high_low_limit(eod_frame, st_stocks)
    print_result(result)
    # print(result.head())

    result_ts = get_ts_high_low_limit(target_date)
    # print(result_ts.head())

    df = result.join(result_ts.set_index('ts_code'), on = 'S_INFO_WINDCODE', how = 'left')

    high_limit_dif = df[df['HighLimit'] != df['up_limit']]
    low_limit_dif = df[df['LowLimit'] != df['down_limit']]

    print('high_limit dif')
    print(high_limit_dif)
    high_limit_dif.to_csv('high_limit_dif.csv', index = False)
    print('low limit dif')
    print(low_limit_dif)
    low_limit_dif.to_csv('low_limit_dif.csv', index = False)
