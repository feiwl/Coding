__author__ = 'SharonYU'

from tqdm import tqdm
import pandas as pd
import numpy as np
import datetime
import multiprocessing as mp
from min_bar.config.config import factor_mapping
from min_bar.utils.target_dates_stocks import available_stocks

from min_bar.src_data_comb.data_preparation import data_preparation
from min_bar.src_data_comb.generate_index_data import generate_index_new_EOD
from min_bar.src_data_comb.generate_stock_data import generate_stock_new_EOD

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)

class data_combination(data_preparation):

    def __init__(self, begin_date = None, end_date = None, target_time = None):
        data_preparation.__init__(self, begin_date, end_date, target_time)


    def check_data(self):
        '''
        check if all required data are ready
        if not, repair missing data
        '''
        minute_bar_status = self.prepare_minute_bar()
        daily_factor_status = self.check_daily_factor_status()

        status_flag = minute_bar_status & daily_factor_status
        return status_flag

    def prepare_daily_factors(self):
        '''
        Prepare all required daily data for the final combination
        Including: High/low limit, adjust factor, trading status, float volume
        '''
        daily_fac_dict = {}
        for dir in self.daily_factors_dir:
            factor = factor_mapping[dir]
            daily_fac_dict[factor] = pd.read_hdf(self.file_dir_daily_data + dir)

        self.daily_factor_dict = daily_fac_dict

    def prepare_stock_EOD_data(self, stock_id):
        '''
        prepare new EOD data for a certain stock from begin_data to end_date
        '''

        stock_new_EOD = generate_stock_new_EOD(begin_date=self.begin_date,
                                               end_date=self.end_date,
                                               target_time=self.target_time,
                                               stock_id=stock_id)
        try:
            stock_new_EOD_df = stock_new_EOD.get_new_EOD()
            flag = True
        except Exception as e:
            # print(e)
            # print('Fail to generate new EOD data for stock %s'%stock_id)
            stock_new_EOD_df = None
            flag = False

        return flag, stock_new_EOD_df


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

        except Exception as e:
            print(e)
            print('Fail to generate new EOD data for index %s'%index_id)
            index_new_EOD_df = None

        return index_new_EOD_df

    def combine_data_single_stock(self, stock_id):
        '''
        Combine all needed data for a single stock
        '''

        # get new EOD data for this stock
        # deal with A and B dates separately and combine them together later

        flag, new_EOD_df = self.prepare_stock_EOD_data(stock_id)

        if flag:
            print('combine data for stock: %s'%stock_id)
            # separate stock EOD df
            A_columns_stock = [col for col in new_EOD_df.columns if '_A' in col]
            B_columns_stock = [col for col in new_EOD_df.columns if '_B' in col]

            stock_EOD_df_A = new_EOD_df[A_columns_stock].copy()
            stock_EOD_df_B = new_EOD_df[B_columns_stock].copy()

            # combine stock and index EOD df for A and B
            combined_df_A = pd.merge(stock_EOD_df_A, self.index_EOD_df_A, how='left', on='nActionDay_A')
            combined_df_B = pd.merge(stock_EOD_df_B, self.index_EOD_df_B, how='left', on='nActionDay_B')

            # combine stock+index df and daily factors for A and B
            # Daily factors including high/low limit, adjust factor, float volume, trading status

            for key, df in self.daily_factor_dict.items():

                try:
                    sub_df_A = pd.DataFrame(df[stock_id])
                    sub_df_B = sub_df_A.copy()
                    sub_df_A.columns = [key + '_A']
                    sub_df_B.columns = [key + '_B']

                    sub_df_A['nActionDay_A'] = sub_df_A.index.values
                    sub_df_B['nActionDay_B'] = sub_df_B.index.values

                    combined_df_A = pd.merge(combined_df_A, sub_df_A, how='left', on='nActionDay_A')
                    combined_df_B = pd.merge(combined_df_B, sub_df_B, how='left', on='nActionDay_B')
                except:
                    print('Fail to combine new EOD data for stock: %s' % stock_id)
                    combined_df = None
                    return combined_df

            combined_df = pd.merge(combined_df_A, combined_df_B, how='left', left_index=True, right_index=True)
            combined_df[['HighLimit_A', 'LowLimit_A', 'HighLimit_B', 'LowLimit_B']] = \
                combined_df[['HighLimit_A', 'LowLimit_A', 'HighLimit_B', 'LowLimit_B']].astype('int64')

            combined_df[['nActionDay_A', 'nActionDay_B']] = \
                combined_df[['nActionDay_A', 'nActionDay_B']].astype(np.str)

            combined_df.index = [stock_id] * len(combined_df)
            combined_df.index.name = 'szWindCode'

        else:
            print('Fail to combine new EOD data for stock: %s'%stock_id)
            combined_df = None

        return combined_df

def main(begin_date = None, end_date = None, target_time = None, concat_flag = False):

    data_comb = data_combination(begin_date, end_date, target_time)
    data_comb.prepare_daily_factors()
    data_comb.prepare_index_EOD_data(index_id='399905.SZ')

    target_stocks = available_stocks()['S_INFO_WINDCODE'].tolist()
    # target_stocks = ['000001.SZ', '000002.SZ', '000004.SZ']
    begin_time = datetime.datetime.now()
    print('start time is %s' % begin_time.strftime("%H:%M:%S"))

    with mp.Pool(processes=32) as pool:

        mp_result = [pool.apply_async(data_comb.combine_data_single_stock, (stock, )) for stock in target_stocks]
        mp_result = [item.get() for item in mp_result]
        pool.close()
        pool.join()

    result_list = list(filter(lambda x: x is not None, mp_result))
    end_time = datetime.datetime.now()
    print('end time is %s' % end_time.strftime("%H:%M:%S"))
    print('computing time usage is %s' % str(end_time - begin_time))

    if concat_flag:
        result_df = pd.concat(result_list, axis=0, ignore_index=False)
        result_df.to_csv('history_regenerated_EOD.csv')
        result_df.to_hdf('history_regenerated_EOD.hdf', 'hello', mode = 'w')
        print(result_df.info())
        print(result_df)
        return result_df
    else:
        return result_list


if __name__ == '__main__':
    begin_date = '20190101'
    end_date = '20200825'
    target_time = 1400e5

    concat_flag = True

    main(begin_date, end_date, target_time, concat_flag)




