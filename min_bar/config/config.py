import os
import pymysql
import platform

wind_db = db = pymysql.connect(host='192.168.1.225',
                               port=3306,
                               user='wind_user',
                               password='Q#wind2$%pvt')

file_dir_data_valid = os.getcwd() + '/data/valid_result/'
file_dir_new_EOD = os.getcwd() + '/data/new_EOD/'

factor_mapping = {'history_float_volume.hdf5': 'FloatVolume',
                  'history_highlimit_data.hdf5': 'HighLimit',
                  'history_lowlimit_data.hdf5': 'LowLimit',
                  'history_adj_factor.hdf5': 'AdjFactor',
                  'status_df_hist.hdf5':'TradeStatus'}