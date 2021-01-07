# read_snap_trans.py
# Read the data from snapshot and transaction files.
#
#

import pandas as pd
import multiprocessing as mp


"""Read the data from snapshot"""


def read_snap_volume(snap_dir):
    """
    :param snap_dir: directory，单个股票的directory，可以直接用pd读取
    :return snap_volume：该股票所对应的volume
    """
    snap_volume = None
    try:
        snap = pd.read_csv(snap_dir, dtype={'iVolume': "int"})
        snap_volume = snap.iloc[-1]["iVolume"]
        return snap_volume
    except Exception as e:
        print(e)
        print('Fail to read the snapshot files.')
        return snap_volume


"""Read data from snapshot files using multiprocessing"""


def read_all_snap(directory_df):
    """
    :param directory_df: df，当天全部正在交易的zz500股票的directory
    :return result1: list, snapshot文件里当天全部正在交易的zz500股票的volume
    """
    p1 = mp.Pool(processes = 5)
    result1 = [p1.apply_async(read_snap_volume, (snap_dir,)) for snap_dir in directory_df["snapshot"]]
    p1.close()
    p1.join()
    result1 = [_.get() for _ in result1]
    return result1


"""Read the data from transaction"""


def read_trans_volume(trans_dir):
    """
    :param trans_dir: directory，单个股票的directory，可以直接用pd读取
    :return trans_volume：该股票所对应的volume
    """
    trans_volume = None
    try:
        trans = pd.read_csv(trans_dir, dtype={'nVolume': "int", "chFunctionCode": "str"})
        clean_trans = trans[trans["chFunctionCode"] != "C"]["nVolume"]
        trans_volume = clean_trans.sum()
    except Exception as e:
        print(e)
        print('Fail to read the transaction files.')
        return trans_volume
    return trans_volume


"""Read data from snapshot files using multiprocessing"""


def read_all_trans(directory_df):
    """
    :param directory_df: df，当天全部正在交易的zz500股票的directory
    :return result2: list, transaction文件里当天全部正在交易的zz500股票的volume
    """
    p2 = mp.Pool(processes = 5)
    result2 = [p2.apply_async(read_trans_volume, (trans_dir,)) for trans_dir in directory_df["transaction"]]
    p2.close()
    p2.join()
    result2 = [x.get() for x in result2]
    return result2
