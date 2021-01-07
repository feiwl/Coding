import os
import pandas as pd

from min_bar.update_daily_min_bar import main as update_min_bar
from min_bar.utils.get_root_paths import get_root_path
from min_bar.utils.target_dates_stocks import target_dates

class data_preparation():

    def __init__(self, begin_date, end_date, target_time):


        file_dir_tick_data, file_dir_min_data, file_dir_daily_data = get_root_path()
        self.begin_date = begin_date
        self.end_date = end_date
        self.target_dates = target_dates(begin_date, end_date)
        self.target_time = target_time

        self.file_dir_tick_data = file_dir_tick_data
        self.file_dir_min_data = file_dir_min_data
        self.file_dir_daily_data = file_dir_daily_data

        self.daily_factors_dir = ['history_float_volume.hdf5',
                                  'history_highlimit_data.hdf5',
                                  'history_lowlimit_data.hdf5',
                                  'history_adj_factor.hdf5',
                                  'status_df_hist.hdf5']

    def check_minute_bar_existence(self):
        '''
        This function is used to confirm that all minute bar exists
        If not all minute bar exists, return missing days and type for further use
        '''

        missing_dates = []

        # check file existence for each day and all required dates
        for date in self.target_dates:
            snap_stock_dir = self.file_dir_min_data + 'snap_min/stock/%s' % date
            snap_index_dir = self.file_dir_min_data + 'snap_min/index/%s_%s.hdf5' % ('399905.SZ', date) # we only care about zz500
            tran_stock_dir = self.file_dir_min_data + 'tran_min/%s' % date

            if os.path.exists(snap_stock_dir):
                if os.path.exists(snap_index_dir):
                    if os.path.exists(tran_stock_dir):
                        continue
                    else:
                        missing_dates.append({'type': 'tran',
                                              'date': date})
                else:
                    missing_dates.append({'type': 'snap_index',
                                          'date': date})
            else:
                missing_dates.append({'type': 'snap_stock',
                                      'date': date})

        flag = True if len(missing_dates) == 0 else False
        return flag, missing_dates

    def prepare_minute_bar(self):
        '''
        This function is used to generate missing minute bar data
        '''

        flag, missing_info = self.check_minute_bar_existence()
        if flag:
            print ('All minute bar data prepared')
            return True

        else:
            print('Not all minute bar data are prepared, repair data for these days')
            while len(missing_info) > 0:
                print ('missing_info: \n', missing_info)
                for item in missing_info:
                    # re-check the existence of certain file
                    # since snap_stock and snap_index will run together
                    missing_date = item['date']
                    missing_type = item['type']

                    # find the check dir for different type of data
                    if 'tran' in missing_type:
                        target_dir = self.file_dir_min_data + 'tran_min/%s' % missing_date
                    elif 'index' in missing_type:
                        target_dir = self.file_dir_min_data + 'snap_min/index/%s_%s.hdf5' % ('399905.SZ', missing_date)
                    else:
                        target_dir = self.file_dir_min_data + 'snap_min/stock/%s' % missing_date

                    # if the file exists already, remove the info from record and continue
                    if os.path.exists(target_dir):
                        missing_info.remove(item)
                        continue

                    # if the file doesn't exist, run the code for generating one day minute bar
                    else:
                        print ('fix %s data for date: %s' %(missing_type, missing_date))
                        snap_flag = False
                        tran_flag = False
                        if 'tran' in missing_type:
                            tran_flag = True
                        else:
                            snap_flag = True
                        update_min_bar(missing_date, snap_flag, tran_flag)

                    # re-check the existence of certain file
                    # if fix complete, remove it from the missing info record
                    if os.path.exists(target_dir):
                        missing_info.remove(item)
                        print ('fix complete for type %s on date: %s'%(missing_type, missing_date))
                        continue
                    else:
                        print('fix failed for type %s on date: %s'%(missing_type, missing_date))

            print('All minute bar data are prepared')
            return True

    def check_daily_factor_status(self):
        '''
        This function is used to check if the daily factors are ready
        '''
        repair_list = []
        for daily_fac in self.daily_factors_dir:
            check_df = pd.read_hdf(self.file_dir_daily_data + daily_fac)

            if check_df.index.values[-1] < self.end_date:
                print('%s file is not ready!'%daily_fac)
                repair_list.append(daily_fac)
            else:
                print('%s file is ready'%daily_fac)

        ready = True if len(repair_list) == 0 else False
        return ready