import pandas as pd
import pymysql
import copy
import tqdm


def generate_k_minute_bar_one_day(minute_bar_frame: pd.DataFrame, K: int) -> pd.DataFrame:
    """
    according to the one_minute_bar dataframe, generate the k minute bar
    since the transaction record do not include the post market record of the technology board stocks,
    the function does not consider the case of the technology board stocks
    :param minute_bar_frame: dataframe of the minute bar
    :param K: the target time range for a bar
    :return: dataframe of the k minute bar
    """
    # get the generator of the row iteration for the original one minute bar
    columns = minute_bar_frame.columns.tolist()
    row_iterator = minute_bar_frame.itertuples(index=False)

    # inital value of the iteration
    value_ = next(row_iterator)
    value_ = pd.Series(dict(zip(columns, list(value_))))

    # list to put the k minute record
    records = []

    """ morning pre market"""
    while value_['nTime'] == 925e5:
        records.append(value_)
        value_ = next(row_iterator)
        value_ = pd.Series(dict(zip(columns, list(value_))))

    """ morning continuous market"""
    # begin time
    while value_['nTime'] == 930e5:
        crt_bar = copy.deepcopy(value_)
        value_ = next(row_iterator)
        value_ = pd.Series(dict(zip(columns, list(value_))))
        iter_count = 1

    # start
    while (value_['nTime'] > 930e5) & (value_['nTime'] <= 1130e5):
        if iter_count == K:

            # reset the iter count to 0
            iter_count = 1

            # append a new minute bar to the minute_bar_record
            records.append(crt_bar)

            # generate new current bar
            crt_bar = copy.deepcopy(value_)
        else:
            # update high low
            crt_bar['nHigh'] = max(crt_bar['nHigh'], value_['nHigh'])
            crt_bar['nLow'] = min(crt_bar['nLow'], value_['nLow'])

            # update nMatch
            crt_bar['nMatch'] = value_['nMatch']

            # update some increment values
            crt_bar['iVolume'] = crt_bar['iVolume'] + value_['iVolume']
            crt_bar['iTurnover'] = crt_bar['iTurnover'] + value_['iTurnover']
            crt_bar['nNumTrades'] = crt_bar['nNumTrades'] + value_['nNumTrades']

            # add one the the iter count
            iter_count += 1

        # generate the next iterator
        value_ = next(row_iterator)
        value_ = pd.Series(dict(zip(columns, list(value_))))

    # put the last record in the morning to the records
    records.append(crt_bar)

    """ afternon continuous market"""
    # morning continuous close, start afternoon continuous
    while value_['nTime'] == 1300e5:
        crt_bar = copy.deepcopy(value_)
        value_ = next(row_iterator)
        value_ = pd.Series(dict(zip(columns, list(value_))))
        iter_count = 1

    # start
    while (value_['nTime'] > 1300e5) & (value_['nTime'] <= 1457e5):
        if iter_count == K:
            # reset the iter count to 0
            iter_count = 1

            # append a new minute bar to the minute_bar_record
            records.append(crt_bar)

            # generate new current bar
            crt_bar = copy.deepcopy(value_)
        else:
            # update high low
            crt_bar['nHigh'] = max(crt_bar['nHigh'], value_['nHigh'])
            crt_bar['nLow'] = min(crt_bar['nLow'], value_['nLow'])

            # update nMatch
            crt_bar['nMatch'] = value_['nMatch']

            # update some increment values
            crt_bar['iVolume'] = crt_bar['iVolume'] + value_['iVolume']
            crt_bar['iTurnover'] = crt_bar['iTurnover'] + value_['iTurnover']
            crt_bar['nNumTrades'] = crt_bar['nNumTrades'] + value_['nNumTrades']

            # add one the the iter count
            iter_count += 1

        # generate the next iterator
        value_ = next(row_iterator)
        value_ = pd.Series(dict(zip(columns, list(value_))))

    # put the last record in the morning to the records
    records.append(crt_bar)

    """ afternoon post market"""
    while value_['nTime'] == 1500e5:
        records.append(value_)
        try:
            value_ = next(row_iterator)
            value_ = pd.Series(dict(zip(columns, list(value_))))
            print('more rows for stock %s at date %s after the post market' %
                  (minute_bar_frame['szWindCode'][0], minute_bar_frame['nActionDay'][0]))
        except:
            break

    return pd.concat(records, axis=1, ignore_index=True).T


def get_stocks_k_minute_bar_history(begin_date: str, end_date: str, stock_id: str, K: int) -> pd.DataFrame:
    """
    get the history stock K_minute_bar
    :param begin_date: str, begin date
    :param end_date: str, end_date
    :param stock_id: str, target stock
    :param K: target bar length
    :return: pandas dataframe
    """
    """ get data"""
    db = pymysql.connect(host='192.168.10.68',
                         port=3306,
                         user='nas',
                         password='Prism@123456')

    sql = """SELECT symbol.szWindCode,  transaction.nActionDay, transaction.nTime, transaction.nOpen, transaction.nHigh,
     transaction.nLow, transaction.nMatch, transaction.iVolume, transaction.iTurnover, transaction.barClose, 
     transaction.S_DQ_PRECLOSE, transaction.nNumTrades, transaction.S_DQ_ADJFACTOR, transaction.HighLimit, 
     transaction.LowLimit FROM marketdata.transaction join marketdata.symbol where transaction.codeID = symbol.codeID 
     and symbol.szWindCode = '%s' and transaction.nActionDay >= %s and transaction.nActionDay <= %s;""" \
          % (stock_id, begin_date, end_date)

    minute_bar = pd.read_sql(sql, db)

    """ calculate k minute bar"""
    dates = minute_bar.nActionDay.unique()
    record = []

    for date in tqdm.tqdm(dates):
        df_sample = minute_bar[minute_bar['nActionDay'] == date]
        k_minute_bar = generate_k_minute_bar_one_day(df_sample, K)
        record.append(k_minute_bar)

    target_df = pd.concat(record, axis = 0, ignore_index=True)

    target_df[target_df.columns.drop(['szWindCode', 'nActionDay'])] = \
        target_df[target_df.columns.drop(['szWindCode', 'nActionDay'])].astype('int64')

    return target_df


if __name__ == '__main__':
    begin_date = '20200601'
    end_date = '20200630'
    stock_id = '688001.SH'
    time_range = 20

    df = get_stocks_k_minute_bar_history(begin_date, end_date, stock_id, time_range)
    df.to_csv('k_minute_bar_history.csv', index = False)
    print (df.info())
