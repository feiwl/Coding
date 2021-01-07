import sys
sys.path.append('./')

from regenerate_EOD.utils.get_root_paths import get_root_path
from regenerate_EOD.utils.read_minite_bar import ReadMinBarMongoDB
from regenerate_EOD.utils.target_dates_stocks import *
from regenerate_EOD.config.config import factor_mapping

class data_preparation():
    '''
    This class is used to check if all needed data if ready, including:
    Minute bar data: including stock transaction, stock snapshot and index snapshot
    Daily factors: including high/low limit, float volume, trading status and adjust factor

    If all data are ready, we can move on to generate new EOD data
    If some minute bar data is not up-to-date, update some minute bar data first.
    '''
    def __init__(self, begin_date, end_date, target_time):

        self.begin_date = begin_date
        self.end_date = end_date
        self.target_time = target_time

        self.daily_factors_dir = ['history_float_volume.hdf5',
                                  'history_highlimit_data.hdf5',
                                  'history_lowlimit_data.hdf5',
                                  'history_adj_factor.hdf5',
                                  'status_df_hist.hdf5']

        self.target_dates = target_dates(begin_date, end_date)
        _, _, file_dir_daily_data = get_root_path()

        self.file_dir_daily_data = file_dir_daily_data

    def check_db_minute_bar_status(self):
        '''
        This function is used to check if minute bar data are up-to-date
        '''

        # check if stock minute transaction bar is ready
        print('Check if stock minute bar data is ready...')
        stock_check = ReadMinBarMongoDB(id=id, type='tran')
        stock_count = stock_check.read_one_day_allstock(self.target_dates[-1])

        stock_flag = True if stock_count > 0 else False

        if not stock_flag:
            print('stock minute bar data is not ready!')
            return False

        # check if index snapshot minute bar is ready
        print('Check if index minute bar data is ready...')
        index_check = ReadMinBarMongoDB(id='399905.SZ', type='snap_index')
        index_df = index_check.read_one_day(self.target_dates[-1])

        index_flag = True if len(index_df) > 0 else False

        if not index_flag:
            print('index minute bar data is not ready!')
            return False

        # return true if all minute bar data is ready
        if index_flag & stock_flag:
            print('All minute bar data are ready')
            return True


    def check_daily_factor_status(self):
        '''
        This function is used to check if the daily factors are ready
        '''
        daily_factor_dict = {}
        print('Check if daily factors data is ready...')
        for daily_fac in self.daily_factors_dir:
            factor = factor_mapping[daily_fac]
            check_df = pd.read_hdf(self.file_dir_daily_data + daily_fac)
            print(len(check_df.index.values))
            print(self.file_dir_daily_data + daily_fac)
            if check_df.index.values[-1] < self.end_date:
                print('%s file is not ready!' % daily_fac)
                return False
            daily_factor_dict[factor] = check_df

        self.daily_factor_dict = daily_factor_dict
        print('All daily factors data are ready')

        return True

if __name__ == '__main__':
    begin_date = '20190101'
    end_date = '20200101'
    target_time = 1400e5

    stock_id = '000001.SZ'
    concat_flag = True

    test = data_preparation(begin_date, end_date, target_time)
    test.check_daily_factor_status()
