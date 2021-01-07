# find_file_name.py
# Transform the stock codes to the stock file names, ending with SH or SZ
#
#

import tushare as ts
import pandas as pd
import platform


"""Get the code of the trading stocks"""


def find_raw_trading_stock():
    """
    :return: zz500: df, 全部zz500的代码
    """
    zz500 = ts.get_zz500s()
    return zz500


"""Get the code of the suspend stocks"""


def find_suspend_stock(date):
    """
    :param date: str,设置好的时间target date
    :return: suspend_df 全部的停盘股票代码
    """
    pro = ts.pro_api('3f158d816eb46498558d167cdad44afe09613c812f698d0429cd6c89')
    suspend_raw = pro.suspend_d(suspend_type='S', trade_date='%s' % date)
    suspend_df = pd.DataFrame()
    suspend_df["suspend_code"] = suspend_raw["ts_code"]
    return suspend_df


"""Add the stock ending mark ---- .SH or .SZ to the stock codes"""


def add_stock_end_mark(zz500_df):
    """
    :param: zz500_df = find_raw_trading_stock(), df zz500里全部的股票信息
    :return: trading_code_df： df 只有一列 trading_code，有SH、SZ后缀
    """
    zz500_code_df = pd.DataFrame()
    zz500_code_df["trading_code"] = zz500_df["code"].apply(
        lambda x: "%s.SZ" % x.zfill(6) if int(x) < 400000 else "%s.SH" % x.zfill(6))
    return zz500_code_df


"""Get the code of the trading stocks, excluding the suspended stocks"""


def cleaned_trading_stock(zz500_df_marked, suspend_df):
    """
    :param zz500_df_marked = add_stock_end_mark(zz500_df) df, 含有后缀的、全部的zz500股票
    :param suspend_df = find_suspend_stock(date) df, 全部的停盘股票
    :return:
    cleaned_df：df, 有后缀，没有停盘股票
    suspend_zz500_df: df, 有后缀，zz500里面的停盘股票
    """
    cleaned_df = zz500_df_marked[~zz500_df_marked["trading_code"].isin(suspend_df["suspend_code"].values.tolist())]
    # cleaned_df = cleaned_df[~cleaned_df["trading_code"].isin(['002957.SZ', '603256.SH', '603983.SH'])]
    cleaned_df.reset_index(drop=True, inplace=True)
    suspend_zz500_df = suspend_df[suspend_df["suspend_code"].isin(zz500_df_marked["trading_code"].values.tolist())]
    return cleaned_df, suspend_zz500_df


"""Get the directory of the stocks"""


def file_directory(code, file_type, date):
    """
    :param code: str, 带后缀的zz500股票代码
    :param file_type: str, "snapshot" or "transaction"
    :param date: str, yyyymmdd
    :return: directory: directory, 单只股票的directory可以直接用pd读取
    """
    sys = platform.system()
    if sys == 'Windows':
        directory = r"\\192.168.1.225\Anonymous share\stockdata\%s\%s\%s_%s_%s.csv" \
                        % (code[-2:], code, code, date, file_type.upper())
    else:
        directory = '/mnt/stock_data_nas/stockdata/%s/%s/%s_%s_%s.csv' \
                        % (code[-2:], code, code, date, file_type.upper())
    return directory
