import pandas as pd
import numpy as np
import os
import time
import warnings
# warnings.filterwarnings('ignore')
# coding = utf-8

class IndexMinuteBarSnap:
    def __init__(self, index_code: str, date: str, data_dir: str, minutes: list):
        """
        get index minute bar by snapshot
        :param index_code: index code
        :param day:
        :param data_dir:
        :param minutes:
        """

        # set initial values
        self.index_code = index_code
        self.date = date
        self.data_dir = data_dir
        self.minutes = minutes
        self.file_name_snap = "%s/%s/%s_%s_INDEX.csv" %\
                              (self.index_code[-2:], self.index_code, self.index_code, self.date)

        # target columns
        self.__target_columns = ['szWindCode', 'nActionDay', 'nTime', 'nOpenIndex', 'nHighIndex',
                                 'nLowIndex', 'nLastIndex', 'iTotalVolume', 'iTurnover', 'nPreCloseIndex']

        # target dataframe
        self.target_df = pd.DataFrame(columns = self.__target_columns)

        # value to store snapshot
        self.snap = None

        # read data
        self.read_index_snap()

    def read_index_snap(self):
        if os.path.exists(self.data_dir + self.file_name_snap):
            self.snap = pd.read_csv(self.data_dir + self.file_name_snap)
            # keep the required columns
            self.snap = self.snap[self.__target_columns]
        else:
            self.snap = None
            print('the snapshot does not exist for the index %s at date %s'% (self.index_code, self.date))

    def get_min_snap_pandas(self):
        if self.snap is None:
            return None

        # transfer nTime to minute
        df = self.snap.copy()

        # drop rows with turnover less than 0
        df = df[df['iTurnover'] > 0]

        df['nTime_min'] = df['nTime'].apply(lambda x: int(x // 1e5 * 1e5))

        # find the latest record in the minute
        last_record = df.groupby('nTime_min').max()['nTime'].to_list()
        df = df[df['nTime'].isin(last_record)]

        df.drop(columns = ['nTime'], inplace = True)
        df.rename(columns = {'nTime_min': 'nTime'}, inplace = True)
        df = df[self.__target_columns]

        # drop row after market close time
        df = df[(df['nTime'] <= 1500e5) & (df['nTime'] >= 925e5)]

        # for those with missed minutes, insert rows
        missed_minutes = set(self.minutes) - set(df.nTime)
        if len(missed_minutes) == 0:
            self.target_df = df.copy()
        else:
            print('missed rows for stock %s at date %s' % (self.index_code, self.date))
            missed_minutes = list(sorted(missed_minutes))
            print(missed_minutes)
            df_append = pd.DataFrame(columns=self.__target_columns)
            df_append['nTime'] = missed_minutes
            self.target_df = pd.concat([df, df_append], ignore_index=True)
            self.target_df.sort_values('nTime', inplace=True)
            self.target_df.fillna(method = 'ffill', inplace=True)

        if len(set(self.target_df.nTime)- set(self.minutes)) != 0:
            print('exist some extra time in the dataframe for index %s at date %s' % (self.index_code, self.date))
            print(sorted(list(set(self.target_df.nTime)- set(self.minutes))))

    def get_min_snap(self):
        # if no record was found the the index
        if self.snap is None:
            return None

        # set initial record
        crt_bar = 92500000
        market_open = False

        target_record = []
        "for the first trading record"
        row_it = self.snap.itertuples(index=False)

        while not market_open:
            new_bar_record = next(row_it)
            new_bar_record = dict(zip(self.__target_columns, list(new_bar_record)))
            # for the first record
            market_open = True

            # record exist now
            nTime = new_bar_record['nTime']
            if nTime - crt_bar < 1e5:
                pass
            # update the first record at 925
            else:
                # update time by minute
                crt_bar = int(nTime // 1e5 * 1e5)

                # repair bar data for missed minutes
                if crt_bar > 92500000:
                    min_append = sorted(list(filter(lambda x: x < crt_bar, self.minutes)))
                    for item in min_append:
                        new_bar_record['nTime'] = item
                        target_record.append(pd.Series(new_bar_record))

        record = new_bar_record

        for new_bar_record in row_it:
            new_bar_record = dict(zip(self.__target_columns, list(new_bar_record)))
            nTime = new_bar_record['nTime']

            # ignore records after market close
            if crt_bar > 1500e5:
                break

            if nTime - crt_bar < 1e5:
                # still inside the bar
                # update record
                record = new_bar_record
                pass
            else:
                # outside the bar
                # save records
                record['nTime'] = crt_bar
                target_record.append(pd.Series(record))

                # repair bar data for missed minutes
                crt_bar_temp = int(nTime // 1e5 * 1e5)
                min_append = sorted(list(filter(lambda x: (x < crt_bar_temp) & (x > crt_bar), self.minutes)))

                for item in min_append:
                    new_bar_record['nTime'] = item
                    target_record.append(pd.Series(new_bar_record))

                # update time
                crt_bar = int(nTime // 1e5 * 1e5)
                record = new_bar_record.copy()

        # after the last record, if no record for the rest of the day, repair data for rest minutes
        if target_record[-1]['nTime'] < 1500e5:
            min_append = sorted(list(filter(lambda x: x > target_record[-1]['nTime'], self.minutes)))
            for item in min_append:
                new_bar_record['nTime'] = item
                target_record.append(pd.Series(new_bar_record))

        self.target_df_new = pd.concat(target_record, axis=1, ignore_index=True).T

        # change dataframe type
        self.target_df_new[self.target_df_new.columns.drop(['szWindCode', 'nActionDay'])] = \
            self.target_df_new[self.target_df_new.columns.drop(['szWindCode', 'nActionDay'])].astype('int64')

        self.target_df_new[['szWindCode', 'nActionDay']] = \
            self.target_df_new[['szWindCode', 'nActionDay']].astype(np.str)

        return self.target_df_new

    def get_snap_min_bar_result(self):
        try:
            result_index = self.get_min_snap()
            if result_index is None:
                return self.index_code, None
            else:
                return self.index_code, result_index
        except Exception as e:
            print(e)
            print('fail to get target for index %s' % self.index_code)
            return self.index_code, None

if __name__ == '__main__':
    code = '000300.SH'
    day = '20200810'
    dir_ = r"\\192.168.10.235\data_home\stock_data\market_data\\"

    start = time.time()
    output_dir_ = os.path.abspath(os.path.join(os.getcwd(), os.path.pardir, "data\snap_min\index"))

    with open('index_minute_list.txt', 'r') as file:
        minutes = file.readlines()
        minutes = [int(item.rstrip('\n')) for item in minutes]

    result = IndexMinuteBarSnap(code, day, dir_, minutes)
    result.get_min_snap_pandas()
    result.get_min_snap()

    print(np.count_nonzero(result.target_df.drop_duplicates(subset=['nTime'], keep='last').values[:, 1:] - result.target_df_new.values[:, 1:]))

    end = time.time()
    print(end - start)






