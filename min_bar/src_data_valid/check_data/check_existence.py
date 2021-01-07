# check_existence.py
# Check if all the snap files exist.
#
#

import os
import time
import pandas as pd
import datetime
import pymysql
import numpy as np


"""Check if the file exists"""


def check_snap_file_existence(snap_directory):
    """
    :param snap_directory: directory, 单只股票的directory，可以直接用pd读取
    :return: bool，该股票所对应的文件存在为True, 不存在为False
    """
    if not os.path.exists(snap_directory):
        return False
    else:
        return True


"""Check the existence of all the snap files until they all exist"""


def check_all_snap_existence(begin_time, end_time, dir_df):
    """
    :param begin_time: timestamp
    :param end_time: timestamp
    :param dir_df: df, zz500中在target date当天交易未停盘的股票们的directory，可以直接用pd读取
    :return: ready: 如果全部snap文件存在则为True（有不存在的文件则等待5min后重新再检测）
    """
    ready = False
    crt_time = datetime.datetime.now().time()
    while (ready is False) & ((crt_time > begin_time) & (crt_time < end_time)):
        print("Current time:", crt_time)
        print("Check if all the snapshot files exist...")

        result = [item[-31: -22] for item in dir_df['snapshot'] if not check_snap_file_existence(item)]

        if len(result) == 0:
            ready = True
            print("All the snapshot files are ready.")
            print()
        else:
            print("The following %i snap files are not ready: " % len(result))
            print(result)
            print("Try again in 5 minutes...")
            print()
            time.sleep(300)
            crt_time = datetime.datetime.now().time()
    return ready


"""Read the data from wind to check if the data are ready"""


def read_wind_df(date, df):
    """
    :param date: str, target date
    :param df: df, 带后缀的codes for all the trading zz500 stocks
    :return wind_df: df，包含了wind里对应股票的volume（"wind_volume"）和带后缀的code（"trading_code"）
                    按"trading_code"这一列升序排列
    """
    # Get the wind data from SQL
    db = pymysql.connect(host="192.168.1.225",
                         port=3306,
                         user="wind_user",
                         passwd="Q#wind2$%pvt",
                         db="wind",
                         charset="utf8")
    # Only select the data of zz500 from wind
    code_str = ','.join(["'" + "%s" % x + "'" for x in df["trading_code"].tolist()])
    sql = """SELECT * FROM wind.ASHAREEODPRICES WHERE TRADE_DT = '%s' AND S_INFO_WINDCODE IN (%s);""" % (date, code_str)
    wind = pd.read_sql(sql, con=db)
    # Clean the data, and order by ascending
    wind_df = pd.DataFrame()
    wind_df["trading_code"] = wind["S_INFO_WINDCODE"]
    wind_df["wind_volume"] = wind["S_DQ_VOLUME"].apply(lambda x: int(np.round(x * 100)))
    wind_df = wind_df.sort_values("trading_code", ascending=True)
    df.reset_index(drop=True, inplace=True)
    return wind_df


"""Check if the data from wind are ready"""


def check_wind_existence(df, crt_time):
    """
    :param df： df, 从函数read_wind_df(date, df)得出，包含了数据volume和对应的code
    :return: bool, 若所读取的df为空则return False，否则为True
                    while loop在主文件内，因为每round都需要更新df的数值
    """
    print("Current time:", crt_time)
    print("Check if all the wind data exist...")
    # Check if wind data exist
    if df["wind_volume"].empty:
        ready = False
        print("Not all the wind data are ready")
        print("Try again in 5 minutes...")
        time.sleep(300)
    else:
        ready = True
        print("All the Wind data are ready.")
    print()
    return ready
