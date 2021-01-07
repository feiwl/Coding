from regenerate_EOD.src_data_comb.generate_index_data import generate_index_new_EOD
from regenerate_EOD.src_data_comb.generate_stock_data import generate_stock_new_EOD
from regenerate_EOD.utils.target_dates_stocks import available_stocks
from regenerate_EOD.src_data_comb.data_preparation import data_preparation
import multiprocessing as mp
from tqdm import tqdm
import datetime
import pandas as pd
import pymongo
import copy
import numpy as np

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


class data_combination(data_preparation):
    '''
    This class is used to combine all data together to generate the new EOD data
    Including:
    Equity daily data: OHLC, turnover, volume, A/B dates separately
    Index daily data: OHLC, turnover, volume, A/B dates separately
    Equity daily factors: high/low limit, trading status, float volume, adjust factor

    Inputs:
    begin_date: str, start date
    end_date: str, end date
    target_time: int, target time to generate new EOD
    '''
    def __init__(self, begin_date = None, end_date = None, target_time = None):
        data_preparation.__init__(self, begin_date, end_date, target_time)

        self.check_data()
        self.prepare_index_EOD_data('399905.SZ')


    def check_data(self):
        '''
        check if all required data are ready
        if not, repair missing data
        '''
        if not self.check_db_minute_bar_status():
            raise ValueError('Minute bar data is not ready, repair them first!')
        if not self.check_daily_factor_status():
            raise ValueError('Daily factor is not ready, repair them first!')


    def prepare_index_EOD_data(self, index_id):
        '''
        prepare new EOD data for a certain index from begin_data to end_date
        '''
        index_new_EOD = generate_index_new_EOD(begin_date=self.begin_date,
                                               end_date=self.end_date,
                                               target_time=self.target_time,
                                               index_id=index_id)
        try:
            index_new_EOD_df = index_new_EOD.get_new_EOD()
            # separate index EOD df
            A_columns_index = [col for col in index_new_EOD_df.columns if '_A' in col]
            B_columns_index = [col for col in index_new_EOD_df.columns if '_B' in col]

            index_EOD_df_A = index_new_EOD_df[A_columns_index].copy()
            index_EOD_df_B = index_new_EOD_df[B_columns_index].copy()

            self.index_new_EOD_df = index_new_EOD_df
            self.index_EOD_df_A = index_EOD_df_A
            self.index_EOD_df_B = index_EOD_df_B
            return self.index_EOD_df_A, self.index_EOD_df_B

        except Exception as e:
            print(e)
            print('Fail to generate new EOD data for index %s'%index_id)
            self.index_new_EOD_df = None


    def multi_stocks_generate_EOD(self, stock_list = []):

        '''
        This function is used to generate new stock EOD results
        '''

        stock_new_EOD = generate_stock_new_EOD(begin_date=self.begin_date,
                                               end_date=self.end_date,
                                               target_time=self.target_time)

        # get target stocks, if user have target stock list, only run target stocks
        # if user doesn't have target stock list, run all stocks in the market
        if len(stock_list) > 0:
            target_stocks = stock_list
        else:
            ipo_df = available_stocks()
            all_stocks = ipo_df['S_INFO_WINDCODE'].tolist()

            # we only care about stocks that have trading data between begin and end dates
            # therefore we delete stocks that delist before begin date and ipo after end date
            delist_stocks = ipo_df[ipo_df['S_INFO_DELISTDATE'] <= self.begin_date]['S_INFO_WINDCODE'].tolist()
            notlist_stocks = ipo_df[ipo_df['S_INFO_LISTDATE'] > self.end_date]['S_INFO_WINDCODE'].tolist()
            target_stocks = list(set(all_stocks) - set(delist_stocks) - set(notlist_stocks))

        begin_time = datetime.datetime.now()
        print('start time is %s' % begin_time.strftime("%H:%M:%S"))

        # generate process bar
        process_bar = tqdm(total=len(target_stocks))
        process_bar.set_description('Process for combining data')
        update = lambda *args: process_bar.update()

        # regenerate stock new EOD data with multiprocessing
        with mp.Pool(processes=32) as pool:

            mp_result = [pool.apply_async(stock_new_EOD.get_new_EOD, (stock, ), callback=update) for stock in target_stocks]
            mp_result = [item.get() for item in mp_result]
            pool.close()
            pool.join()

        process_bar.close()
        # Filter None results
        result_list = list(filter(lambda x: x is not None, mp_result))
        result_list = [result[1] for result in result_list]
        end_time = datetime.datetime.now()
        print('end time is %s' % end_time.strftime("%H:%M:%S"))
        print('Regenerate stock EOD time usage is %s' % str(end_time - begin_time))

        return result_list

    def combination_process(self, result_list = []):
        '''
        This Function is used to generate final results by combining all dataframes
        including: stock EOD data, index EOD data and stock daily factors
        '''

        # concat all stock new EOD data
        all_stock_EOD_df = pd.concat(result_list, axis=0, ignore_index=True)

        # separate stock EOD df
        A_columns_stock = ['szWindCode'] + [col for col in all_stock_EOD_df.columns if '_A' in col]
        B_columns_stock = ['szWindCode'] + [col for col in all_stock_EOD_df.columns if '_B' in col]

        stock_EOD_df_A = all_stock_EOD_df[A_columns_stock].copy()
        stock_EOD_df_B = all_stock_EOD_df[B_columns_stock].copy()

        # combine stock and index EOD df for A and B
        combined_df_A = pd.merge(stock_EOD_df_A, self.index_EOD_df_A, how='inner', on=['nActionDay_A'])
        combined_df_B = pd.merge(stock_EOD_df_B, self.index_EOD_df_B, how='inner', on=['nActionDay_B'])

        # combine stock+index df and daily factors for A and B
        # Daily factors including high/low limit, adjust factor, float volume, trading status

        for key, df in self.daily_factor_dict.items():
            df_stacked = df.stack()
            df_stacked = df_stacked.reset_index()
            sub_df_A = df_stacked.copy()
            sub_df_B = df_stacked.copy()

            sub_df_A.columns = ['nActionDay_A', 'szWindCode', key + '_A']
            sub_df_B.columns = ['nActionDay_B', 'szWindCode', key + '_B']

            combined_df_A = pd.merge(combined_df_A, sub_df_A, how='inner', on=['nActionDay_A', 'szWindCode'])
            combined_df_B = pd.merge(combined_df_B, sub_df_B, how='inner', on=['nActionDay_B', 'szWindCode'])

        # sort values and reset index
        combined_df_A = combined_df_A.sort_values(ascending=True, by=['szWindCode', 'nActionDay_A'])
        combined_df_B = combined_df_B.sort_values(ascending=True, by=['szWindCode', 'nActionDay_B'])

        # merge A and B dataframe together
        combined_df_B = combined_df_B[combined_df_B.columns.drop('szWindCode')]
        combined_df = pd.merge(combined_df_A, combined_df_B, how='inner', left_index=True, right_index=True)

        # change data types
        combined_df[['HighLimit_A', 'LowLimit_A', 'HighLimit_B', 'LowLimit_B']] = \
            combined_df[['HighLimit_A', 'LowLimit_A', 'HighLimit_B', 'LowLimit_B']].astype('int64')

        combined_df[['nActionDay_A', 'nActionDay_B']] = \
            combined_df[['nActionDay_A', 'nActionDay_B']].astype(np.str)

        # print(combined_df.info())

        return combined_df


    def multi_stocks_combination_main(self, stock_list = [], concat_flag = True):
        '''
        This Function is the main function of generate new EOD data for multi stocks
        params:
        stock_list: cared stock list, if stock_list = [], will run all market stocks' result
        concat_flag: True if you want a whole dataframe, False if you want a list of dfs
        '''

        # get stock EOD data
        result_list = self.multi_stocks_generate_EOD(stock_list=stock_list)

        begin_time = datetime.datetime.now()
        # get final combined dataframe
        combined_df = self.combination_process(result_list)

        # return a large df or a list of dfs sliced by windcode
        if concat_flag:
            end_time_merge = datetime.datetime.now()
            print('merge time cost: %s' % str(end_time_merge - begin_time))
            # combined_df.to_csv('test_combined_df.csv')
            return combined_df
        else:
            df_list = []
            combined_df.index = combined_df['szWindCode'].values
            stock_list = combined_df['szWindCode'].unique()
            for id in stock_list:
                df = combined_df.loc[id, :].reset_index()
                if df.shape[0] < 20 or df.shape[1] < 20:
                    continue
                df = df[df.columns.drop('index')]
                df_list.append(df)

            end_time_merge = datetime.datetime.now()
            print('merge time cost: %s' % str(end_time_merge - begin_time))
            return df_list

    def single_stock_combination_main(self, stock_id=None):

        '''
        This function is used to generate the final results
        *** Aims for test one stock only ***

        params:
        begin_date: str, begin date
        end_date: str, end date
        target_time: int64: target time to regenerate EOD
        '''

        begin_time = datetime.datetime.now()
        print('start time is %s' % begin_time.strftime("%H:%M:%S"))

        stock_new_EOD = generate_stock_new_EOD(begin_date=self.begin_date,
                                               end_date=self.end_date,
                                               target_time=self.target_time)

        try:
            _, new_EOD_df = stock_new_EOD.get_new_EOD(stock_id)
        except:
            new_EOD_df = None

        if new_EOD_df is not None:
            result = self.combination_process([new_EOD_df])
        else:
            print('Fail to combine new EOD data for stock: %s'%stock_id)
            result = None

        end_time = datetime.datetime.now()
        print('end time is %s' % end_time.strftime("%H:%M:%S"))
        print('computing time usage is %s' % str(end_time - begin_time))

        return result





# myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
# mydb = myclient["marketdata"]
#
# target_collection = mydb['snap_min_index']
#
# begin_date = '20190101'
# end_date = '20201001'
#
# myquery = {
#     'szWindCode': '399905.SZ',
#     'nActionDay': {'$gte': begin_date, '$lte': end_date}
# }
#
# mydoc = target_collection.find(myquery, {'_id': 0}).sort([("nActionDay", 1), ("nTime" , 1)])
#
# index_new_EOD_df = new_daily_eod(mydoc, 92500000)
#
# A_columns_index = ['nOpenIndex_A', 'nHighIndex_A', 'nLowIndex_A', 'nLastIndex_A', 'iVolumeIndex_A', 'iTurnoverIndex_A', 'nActionDay_A']
# B_columns_index = ['nOpenIndex_B', 'nHighIndex_B', 'nLowIndex_B', 'nLastIndex_B', 'iVolumeIndex_B', 'iTurnoverIndex_B', 'nActionDay_B']
#
# index_EOD_df_A = index_new_EOD_df[A_columns_index].copy()
# index_EOD_df_B = index_new_EOD_df[B_columns_index].copy()
#
# print(index_EOD_df_A)


begin_date = '20190101'
end_date = '20200101'
target_time = 1400e5
#
# stock_id = '000001.SZ'
# concat_flag = True
#
test = data_combination(begin_date, end_date, target_time)
# # data = test.single_stock_combination_main(stock_id)
# data1 = test.multi_stocks_combination_main(['000001.SZ', '000002.SZ', '000004,SZ', '000017.SZ', '601399.SH'], True)
# print(data1)
data = test.prepare_index_EOD_data('399905.SZ')
for i in data:
    print(i)


