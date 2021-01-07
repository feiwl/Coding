__author__ = 'Iris'


import pandas as pd
from datetime import datetime
import datetime
import min_bar.src_data_valid.check_data.find_file_name as fn
import min_bar.src_data_valid.check_data.check_existence as ce
import min_bar.src_data_valid.check_data.read_snap_trans as read
from min_bar.config.config import file_dir_data_valid
import time
import os
import functools
print = functools.partial(print, flush=True)


def check_data(date):
    """PREPARING"""
    # Define the begin time, the end time, and the current time
    crt_time = datetime.datetime.now().time()
    begin_time = datetime.time(9, 30)
    end_time = datetime.time(23, 0)
    if crt_time <= begin_time:
        raise AttributeError('Current time is earlier than the begin time')
    elif crt_time >= end_time:
        raise AttributeError('Current time is later than the end time')

    # Print the introduction in console.
    print("Target date:", date)
    print("Current date:", datetime.datetime.now().strftime('%Y%m%d'))

    # Retrieve codes of all the trading stocks for the target day and sort in ascending order by "trading_code"
    raw_zz500 = fn.find_raw_trading_stock()
    zz500 = fn.add_stock_end_mark(raw_zz500)
    raw_suspend_codes = fn.find_suspend_stock(date)
    df, suspend_codes = fn.cleaned_trading_stock(zz500, raw_suspend_codes)          # 有重要参数可以打印: suspend_codes
    df = df.sort_values("trading_code", ascending=True)

    # Get the directory of the codes
    dir_df = pd.DataFrame()
    dir_df["snapshot"] = df["trading_code"].apply(lambda x: fn.file_directory(x, "snapshot", date))
    dir_df["transaction"] = df["trading_code"].apply(lambda x: fn.file_directory(x, "transaction", date))

    """START TO CHECK"""
    # Retrieve the signal showing if all the snap data are ready
    snap_signal = ce.check_all_snap_existence(begin_time, end_time, dir_df)

    # 如果检查10次，三者仍不相等则return出不相等stock的代码
    equal_signal = False
    times = 0
    while (times < 10) & ((not equal_signal) & ((datetime.datetime.now().time() > begin_time) &
                                                (datetime.datetime.now().time() < end_time))):
        # Check if all the wind data are ready
        wind_signal = False
        wind_df = pd.DataFrame()
        while (not wind_signal) & (snap_signal):
            wind_df = ce.read_wind_df(date, df)
            wind_signal = ce.check_wind_existence(wind_df, datetime.datetime.now().time())
        df["wind_volume"] = wind_df["wind_volume"]

        """READ SNAP AND TRANS FILES"""
        # Read snap files
        print("Current time:", datetime.datetime.now().time())
        print("Retrieving the snapshot data...")
        snap_eod_list = read.read_all_snap(dir_df)
        df["snap_volume"] = snap_eod_list

        # Read trans files
        print("Retrieving the transaction data...")
        trans_eod_list = read.read_all_trans(dir_df)
        df["trans_volume"] = trans_eod_list

        """COMPARE"""
        # Check if the three components are equal
        print("Checking if volume in these three files are equal...")
        df['Same'] = False
        df.loc[(df['snap_volume'] == df['wind_volume']) & (df['snap_volume'] == df['trans_volume']), 'same'] = True
        if df["same"].all():
            equal_signal = True
            print("Congrats! Data on %s are correct!!" % date)
            print()
            not_equal_stocks = None
        else:
            print("Oops, the following stocks are not equal: ")
            print()
            not_equal_stocks = df[df["Same"] == False]["trading_code"]
            print(not_equal_stocks)
            times += 1
            time.sleep(300)

    return equal_signal, not_equal_stocks


def write_result(date_str, signal, unequal_reason):
    # generate dir to store log

    target_dir = file_dir_data_valid + '%s/' % date_str
    try:
        os.mkdir(target_dir)
    except:
        pass

    # write log
    with open(target_dir + '%s.ini' % date_str, 'w') as file:
        if signal:
            file.write('[res]' + '\n' + "result = True")
        else:
            file.write('[res]' + '\n' + "result = False" + '\n')
            for item in unequal_reason:
                file.write(item + '\n')
                file.write('end')

    print('Result are saved')


def main(tdate):
    # find target date
    # get signal
    (equal_signal, invalid_reason) = check_data(tdate)

    # write result
    write_result(tdate, equal_signal, invalid_reason)


"""检查某一天的文件用这段code"""
if __name__ == "__main__":
    main(tdate='20200825')


# """检查之前某一时间段的文件用这段code"""
#
#
# if __name__ == "__main__":
#     start= datetime.datetime(2019,11,21).strftime("20%y%m%d")
#     end = datetime.datetime(2020,1,20).strftime("20%y%m%d")
#
#     db = pymysql.connect(host="192.168.1.225",
#                          port=3306,
#                          user="wind_user",
#                          passwd="Q#wind2$%pvt",
#                          db="wind",
#                          charset="utf8")
#     # Only select the data of zz500 from wind
#     sql = """SELECT TRADE_DT
#     FROM wind.ASHAREEODPRICES
#     WHERE %s < TRADE_DT AND TRADE_DT < %s;""" %(start, end)
#
#     date_df = pd.read_sql(sql, con=db)
#     date_df = date_df.sort_values("TRADE_DT", ascending=True, ignore_index=True)
#     dates = sorted(list(set(date_df["TRADE_DT"])))
#
#     file = open('E:/python/Assignment_1/check_stock_all_equal_2019.txt', 'w')
#     for date in dates:
#         result = main(date)
#         if result == True:
#             file.write("%s: True" % date)
#
#         else:
#             file.write("%s: False" % date, result)
#     file.close()
