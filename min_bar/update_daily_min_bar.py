import configparser
import os

from min_bar.config.config import file_dir_data_valid
from min_bar.utils.get_root_paths import get_root_path
import min_bar.src_data_valid.check_data.find_target_date as fd
from min_bar.src_data_valid.check_data.valid_data import main as valid_data_main
from min_bar.src_data_clean.tick_to_minute.snapshot.daily_minute_bar import main as snap_main
from min_bar.src_data_clean.tick_to_minute.transaction.one_day_min_bar import main as tran_main

def main(date, snap_flag, tran_flag):
    # do data validation first:

    print('tick data validation start')
    valid_data_main(date)

    target_file = file_dir_data_valid + '%s/%s.ini' % (date, date)

    cf = configparser.ConfigParser()
    cf.read(target_file)

    data_valid_result = cf.getboolean('res', 'result')
    # 如果tick data有问题，先不计算minute bar
    if not data_valid_result:
        print('tick data is not correct on date: %s' % date)
    # 如果tick data没有问题，开始计算snapshot(index, stock)和transaction(stock)的minute bar
    else:
        if snap_flag:
            print('generating minute snapshot data for date: %s' % date)
            snap_main(date)
        if tran_flag:
            print('generating minute transaction data for date: %s' % date)
            tran_main(date)

if __name__ == '__main__':
    date = fd.find_target_date()
    main(date, True, True)

