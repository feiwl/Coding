__author__ = 'ZhouTW'
# coding = utf-8
import pandas as pd
import numpy as np
import os
import time
import copy
import warnings
import platform
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 50)


class StockMinuteSnap:
    """
    according by the daily snapshot, get the minute record
    notice that the record is at the end of every minute
    """
    def __init__(self, code, date, data_input_dir, minutes):
        """
        initial input
        :param code: str, stock_id
        :param date: str, target date
        :param data_input_dir: str, location of the data
        :param minutes: list, including all minutes
        """

        # set initial values
        self.code = code
        self.date = date
        self.data_source = data_input_dir + "%s/%s/%s_%s_SNAPSHOT.csv" % (code[-2:], code, code, date)
        self.minutes = minutes

        # target columns
        self.__target_column = ['szWindCode', 'nActionDay', 'nTime', 'nPreClose', 'nOpen', 'nHigh', 'nLow',
                                'ap10', 'av10', 'ap9', 'av9', 'ap8', 'av8', 'ap7', 'av7', 'ap6', 'av6', 'ap5', 'av5',
                                'ap4', 'av4', 'ap3', 'av3', 'ap2', 'av2', 'ap1', 'av1', 'nMatch', 'bp1', 'bv1', 'bp2',
                                'bv2', 'bp3', 'bv3', 'bp4', 'bv4', 'bp5', 'bv5', 'bp6', 'bv6', 'bp7', 'bv7', 'bp8',
                                'bv8', 'bp9', 'bv9', 'bp10', 'bv10', 'nNumTrades', 'iVolume', 'iTurnover',
                                'nTotalBidVol', 'nTotalAskVol', 'nWeightedAvgBidPrice', 'nWeightedAvgAskPrice']

        self.__time_stamp = {'morning_start': 915e5,
                             'morning_auction2': 925e5,
                             'morning_normal_start': 930e5,
                             'morning_normal_end': 1130e5,
                             'afternoon_cont_begin': 1300e5,
                             'afternoon_auction_end': 1500e5,
                             'market_close': 1504e5}

        # future attributes
        # dataframe for the minute record
        self.target_df = None

        # future attributes
        self.snap = None

        # read snapshot
        self.read_data()

    def read_data(self):
        """
        read stock transaction record
        :return: if file exists, pd.DataFrame, else, None
        """
        if os.path.exists(self.data_source):
            self.snap = pd.read_csv(self.data_source)
            # self.snap = self.read_single_csv()
            # keep the required columns
            self.snap = self.snap[self.__target_column]
            # 盘后数据不再需要
            self.snap = self.snap.loc[self.snap['nTime'] <= self.__time_stamp['market_close']]
        else:
            self.snap = None

    def check_file_validation(self):
        """ check file existence and whether the stock is traded """
        if self.snap is None:
            # file existence
            print('file for stock %s at date %s is not existed' % (self.code, self.date))
            return False
        elif self.snap.iloc[-1]['iTurnover'] == 0:
            # stock is traded or not
            print('stock %s has no trade record at date %s' % (self.code, self.date))
            return False
        else:
            return True

    def pandas_minute_record(self):
        if not self.check_file_validation():
            return None
        # transfer nTime to minute
        df = self.snap.copy()
        df['nTime_min'] = df['nTime'].apply(lambda x: int(x // 1e5 * 1e5))

        # latest record in every minute
        last_record = df.groupby('nTime_min').max()['nTime'].to_list()
        df = df[df['nTime'].isin(last_record)]

        df.drop(columns = ['nTime'], inplace = True)
        df.rename(columns = {'nTime_min' : 'nTime'}, inplace =True)
        df = df[self.__target_column]

        # drop rows which before the market open
        df = df[df['nTime'] >= self.__time_stamp['morning_start']]
        # start the iteration to pick right rows
        row_iterator = df.iterrows()
        row_, value_ = next(row_iterator)

        # list to store useful records
        target_record = []

        # before the end of the morning auction
        while value_['nTime'] < self.__time_stamp['morning_auction2']:   # 925前
            target_record.append(value_)
            row_, value_ = next(row_iterator)
        # deal with record after morning auction period.
        record_copy = value_.copy()             # in case of no record between 925 and 930

        while value_['nTime'] < self.__time_stamp['morning_normal_start']:
            record_copy = value_.copy()
            row_, value_ = next(row_iterator)
        record_copy['nTime'] = self.__time_stamp['morning_auction2']
        target_record.append(record_copy)

        # deal with record after morning market close
        while value_['nTime'] < self.__time_stamp['morning_normal_end']:
            target_record.append(value_)
            row_, value_ = next(row_iterator)
        record_copy = value_.copy()


        while value_['nTime'] < self.__time_stamp['afternoon_cont_begin']:
            record_copy = value_.copy()
            row_, value_ = next(row_iterator)
        record_copy['nTime'] = self.__time_stamp['morning_normal_end']
        target_record.append(record_copy)

        # deal with afternoon records
        while value_['nTime'] < self.__time_stamp['afternoon_auction_end']:
            target_record.append(value_)
            try:
                row_, value_ = next(row_iterator)
            except Exception as e:
                print(e)
                print('no close information for stock %s at date %s' % (self.code, self.date))
                break

        record_copy = value_.copy()     # in case of no record after 1500e5,
        while value_['nTime'] <= self.__time_stamp['market_close']:
            record_copy = value_.copy()
            try:
                row_, value_ = next(row_iterator)
            except:
                record_copy['nTime'] = self.__time_stamp['afternoon_auction_end']
                target_record.append(record_copy)
                break

        # save result
        self.target_df = pd.concat(target_record, axis=1, ignore_index=True).T

        # for those with missed minutes, insert rows
        missed_minutes = set(self.minutes) - set(self.target_df.nTime)
        if len(missed_minutes) == 0:
            pass
        else:
            missed_minutes = sorted(list(missed_minutes))
            # print('missed rows for stock %s at date %s' % (self.code, self.date))
            # print(missed_minutes)
            df_append = pd.DataFrame(columns = self.__target_column)
            df_append['nTime'] = missed_minutes
            self.target_df = self.target_df.append(df_append, ignore_index=True)
            self.target_df.sort_values('nTime', inplace = True)
            self.target_df.fillna(method='ffill', inplace = True)

        assert set(self.minutes) == set(self.target_df.nTime), 'minutes are not paired'

        # change dataframe type
        self.target_df = self.target_df.fillna(0)
        self.target_df[self.target_df.columns.drop('szWindCode')] = \
            self.target_df[self.target_df.columns.drop('szWindCode')].astype('int64')
        # print(target_df)
        # self.target_df.to_csv('last_record.csv', index=False)

    def get_minute_record(self):
        """
        minute record for every minute
        :return: pd.DataFrame if trading today, else: None
        """
        # check the validation of the data, whether it it traded or not
        if not self.check_file_validation():
            self.target_df_new = None
            return None

        target_record = []

        # if the data is correct, then retrieve the minute record
        row_iterator = self.snap.itertuples(index=False)
        crt_bar = 91500000

        """before the auction period"""
        while True:
            # ignore records before market start
            snap_record_ = next(row_iterator)
            snap_record_ = dict(zip(self.__target_column, list(snap_record_)))
            new_bar_record = copy.deepcopy(snap_record_)

            if snap_record_['nTime'] - crt_bar < 1e5:
                # still inside the bar
                # update the last record
                new_bar_record = copy.deepcopy(snap_record_)
                pass
            # After the morning start time
            if snap_record_['nTime'] >= self.__time_stamp['morning_start']:
                break

        """morning auction period"""
        while snap_record_['nTime'] < self.__time_stamp['morning_auction2']:
            nTime = snap_record_['nTime']
            if nTime - crt_bar < 1e5:
                # still inside the bar
                # update record
                new_bar_record = copy.deepcopy(snap_record_)
            else:
                # outside the bar
                # append the bar information
                new_bar_record['nTime'] = crt_bar
                target_record.append(pd.Series(new_bar_record))

                # repair bar record for minutes without snapshot record
                crt_bar_temp = int(nTime // 1e5 * 1e5)
                min_append = sorted(list(filter(lambda x: (x < crt_bar_temp) & (x > crt_bar), self.minutes)))

                for item in min_append:
                    new_bar_record['nTime'] = item
                    target_record.append(pd.Series(new_bar_record))

                # update time
                crt_bar = int(nTime // 1e5 * 1e5)
                # update bar record
                new_bar_record = copy.deepcopy(snap_record_)

            snap_record_ = next(row_iterator)
            snap_record_ = dict(zip(self.__target_column, list(snap_record_)))

        # record the last bar before morning auction period
        new_bar_record['nTime'] = crt_bar
        target_record.append(pd.Series(new_bar_record))

        # after the last record before morning auction period, if no record for the rest time before 925
        crt_bar_temp = int(snap_record_['nTime'] // 1e5 * 1e5)
        min_append = sorted(list(filter(lambda x: (x < 925e5) & (x > crt_bar), self.minutes)))

        for item in min_append:
            new_bar_record['nTime'] = item
            target_record.append(pd.Series(new_bar_record))

        # update current bar time
        crt_bar = int(new_bar_record['nTime'] // 1e5 * 1e5)
        new_bar_record['nTime'] = crt_bar

        new_bar_record = copy.deepcopy(snap_record_) # in case no data between 925 and 930

        """waiting for the open of the morning cont"""
        while snap_record_['nTime'] < self.__time_stamp['morning_normal_start']:
            new_bar_record = copy.deepcopy(snap_record_)
            snap_record_ = next(row_iterator)
            snap_record_ = dict(zip(self.__target_column, list(snap_record_)))

        # deal with record after morning auction period.
        new_bar_record['nTime'] = self.__time_stamp['morning_auction2']
        target_record.append(pd.Series(new_bar_record))

        # update current bar time
        crt_bar = (snap_record_['nTime'] // 1e5 * 1e5)

        """during the morning normal time"""
        while snap_record_['nTime'] < self.__time_stamp['morning_normal_end']:
            nTime = snap_record_['nTime']
            if nTime - crt_bar < 1e5:
                # still inside the bar
                # update record
                new_bar_record = copy.deepcopy(snap_record_)
            else:
                # outside the bar
                # append the bar record
                new_bar_record['nTime'] = crt_bar
                target_record.append(pd.Series(new_bar_record))

                # repair bar record for minutes without snapshot records
                crt_bar_temp = int(nTime // 1e5 * 1e5)
                min_append = sorted(list(filter(lambda x: (x < crt_bar_temp) & (x > crt_bar), self.minutes)))

                for item in min_append:
                    new_bar_record['nTime'] = item
                    target_record.append(pd.Series(new_bar_record))

                # update current bar time
                crt_bar = int(nTime // 1e5 * 1e5)
                # update current bar record
                new_bar_record = copy.deepcopy(snap_record_)

            snap_record_ = next(row_iterator)
            snap_record_ = dict(zip(self.__target_column, list(snap_record_)))

        # record the last minute record for morning trading period
        new_bar_record['nTime'] = crt_bar
        target_record.append(pd.Series(new_bar_record))

        # after the last record after morning trading period, if no record for the rest time before 1130
        crt_bar_temp = int(snap_record_['nTime'] // 1e5 * 1e5)
        min_append = sorted(list(filter(lambda x: (x < 1130e5) & (x > crt_bar), self.minutes)))

        for item in min_append:
            new_bar_record['nTime'] = item
            target_record.append(pd.Series(new_bar_record))

        crt_bar = int(new_bar_record['nTime'] // 1e5 * 1e5)
        new_bar_record['nTime'] = crt_bar

        """before the afternoon trading period"""
        while snap_record_['nTime'] < self.__time_stamp['afternoon_cont_begin']:
            new_bar_record = copy.deepcopy(snap_record_)
            snap_record_ = next(row_iterator)
            snap_record_ = dict(zip(self.__target_column, list(snap_record_)))

        new_bar_record['nTime'] = self.__time_stamp['morning_normal_end']
        target_record.append(pd.Series(new_bar_record))

        crt_bar = (snap_record_['nTime'] // 1e5 * 1e5)

        # deal with afternoon records
        while snap_record_['nTime'] < self.__time_stamp['afternoon_auction_end']:
            nTime = snap_record_['nTime']
            if nTime - crt_bar < 1e5:
                # still inside the bar
                # update record
                new_bar_record = copy.deepcopy(snap_record_)
            else:
                # outside the bar
                # append the record
                new_bar_record['nTime'] = crt_bar
                target_record.append(pd.Series(new_bar_record))

                # repair bar record for minutes without snapshot records
                crt_bar_temp = int(nTime // 1e5 * 1e5)
                min_append = sorted(list(filter(lambda x: (x < crt_bar_temp) & (x > crt_bar), self.minutes)))

                for item in min_append:
                    new_bar_record['nTime'] = item
                    target_record.append(pd.Series(new_bar_record))

                # update current bar time
                crt_bar = int(nTime // 1e5 * 1e5)
                # update current bar
                new_bar_record = copy.deepcopy(snap_record_)
            try:
                snap_record_ = next(row_iterator)
                snap_record_ = dict(zip(self.__target_column, list(snap_record_)))
            except Exception as e:
                print(e)
                print('no close information for stock %s at date %s' % (self.code, self.date))
                break

        # record the last minute in afternoon trading period
        new_bar_record['nTime'] = crt_bar
        target_record.append(pd.Series(new_bar_record))

        # after the last record after afternoon trading period, if no record for the rest time before 1500
        crt_bar_temp = int(snap_record_['nTime'] // 1e5 * 1e5)
        min_append = sorted(list(filter(lambda x: (x < crt_bar_temp) & (x > crt_bar), self.minutes)))

        for item in min_append:
            new_bar_record['nTime'] = item
            target_record.append(pd.Series(new_bar_record))

        # update current bar time
        crt_bar = int(new_bar_record['nTime'] // 1e5 * 1e5)
        new_bar_record['nTime'] = crt_bar

        new_bar_record = copy.deepcopy(snap_record_)  # in case of no record after 1500e5,
        while snap_record_['nTime'] <= self.__time_stamp['market_close']:
            new_bar_record = copy.deepcopy(snap_record_)
            try:
                snap_record_ = next(row_iterator)
                snap_record_ = dict(zip(self.__target_column, list(snap_record_)))
            except:
                new_bar_record['nTime'] = self.__time_stamp['afternoon_auction_end']
                target_record.append(pd.Series(new_bar_record))
                break

        # save result
        target_df = pd.concat(target_record, axis=1, ignore_index=True).T
        self.target_df_new = target_df
        assert set(self.minutes) == set(self.target_df_new.nTime), 'minutes are not paired: %s' % self.code

        # change dataframe type
        self.target_df_new[self.target_df_new.columns.drop(['szWindCode', 'nActionDay'])] = \
            self.target_df_new[self.target_df_new.columns.drop(['szWindCode', 'nActionDay'])].astype('int64')

        self.target_df_new[['szWindCode', 'nActionDay']] = \
            self.target_df_new[['szWindCode', 'nActionDay']].astype(np.str)
        return self.target_df_new

    def get_snap_min_bar_result(self):
        try:
            result_stock = self.get_minute_record()
            if result_stock is None:
                return self.code, None
            else:
                return self.code, result_stock
        except Exception as e:
            print(e)
            print('fail to get target for stock %s' % self.code)
            return self.code, None

if __name__ == '__main__':
    stock_id = '000001.SZ'
    date = '20190103'

    start = time.time()

    if platform.system() == 'Windows':
        input_dir_ = r"\\192.168.10.235\data_home\stock_data\market_data\\"
    else:
        input_dir_ = "/mnt/data_home/stock_data/market_data/"
    output_dir_ = os.path.abspath(os.path.join(os.getcwd(), os.path.pardir, "data/snap_min/stock/%s" % date))

    if not os.path.exists(output_dir_):
        os.makedirs(output_dir_)

    with open('stock_minute_list.txt', 'r') as file:
        minutes = file.readlines()
        minutes = [int(item.rstrip('\n')) for item in minutes]

    t1 = time.time()
    result = StockMinuteSnap(stock_id, date, input_dir_, minutes)
    # result.pandas_minute_record()
    between = time.time()
    result.get_minute_record()

    end = time.time()

    print ('Time cost before get_minute_record function: ', between - start)
    print ('Time cost for the get_minute_record function: ', end - between)



