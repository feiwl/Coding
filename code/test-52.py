import os
import smtplib
import datetime
import pymongo
import pandas as pd
from concurrent import futures
from email.header import Header
from email.mime.text import MIMEText

class _DbConnector_mongo:
    def __init__(self, host, database, table):
        self._myclient = pymongo.MongoClient(host)
        self._mydb = self._myclient[database]
        self.mycol = self._mydb[table]

    def update(self, sql):
        """Parameter sql: {"": ""} and {"": "", "": "", ...}"""
        self.mycol.insert_many(sql)

    def query_and_fetch(self, sql):
        """Parameter sql: {"" : ""}"""
        mydoc = self.mycol.find_one(sql)
        return mydoc

def generate_transaction_insert_sql(tran_path) -> list:
    sqls = list()
    if os.path.isdir(tran_path):
        for file in os.listdir(tran_path):
            df = pd.read_hdf(os.path.join(tran_path,file))
            for index, row in df.iterrows():
                sqls.append({'nTime':row['nTime'], 'nOpen':row['nOpen'], 'nHigh':row['nHigh'],
                             'nLow':row['nLow'],'nMatch':row['nMatch'], 'iVolume':row['iVolume'],
                             'iTurnover':row['iTurnover'],'nNumTrades':row['nNumTrades'],
                             'bar_close':row['bar_close'], 'szWindCode':row['szWindCode'],
                             'nActionDay':row['nActionDay']})
    else:
        raise FileNotFoundError(tran_path)
    return sqls

def generate_snap_from_open_index_insert_sql(snap_from_open_index_path):
    sqls = list()
    if not os.path.isdir(snap_from_open_index_path):
        raise FileNotFoundError(snap_from_open_index_path)
    for file in os.listdir(snap_from_open_index_path):
        df = pd.read_hdf(os.path.join(snap_from_open_index_path, file))
        for index, row in df.iterrows():
            sqls.append({'iTurnover': row['iTurnover'], 'iVolume': row['iVolume'],
                            'nActionDay': row['nActionDay'], 'nHighIndex': row['nHighIndex'],
                            'nLastIndex': row['nLastIndex'], 'nLowIndex': row['nLowIndex'],
                            'nOpenIndex': row['nOpenIndex'], 'nTime': row['nTime'],
                            'szWindCode': row['szWindCode']})
    return sqls

def generate_snap_from_open_stock_insert_sql(snap_from_open_stock_path):
    sqls = list()
    if not os.path.isdir(snap_from_open_stock_path):
        raise FileNotFoundError(snap_from_open_stock_path)
    for file in os.listdir(snap_from_open_stock_path):
        df = pd.read_hdf(os.path.join(snap_from_open_stock_path, file))
        for index, row in df.iterrows():
            sqls.append({'iTurnover': row['iTurnover'], 'iVolume': row['iVolume'],
                            'nActionDay': row['nActionDay'], 'nHigh': row['nHigh'],
                            'nLow': row['nLow'], 'nMatch': row['nMatch'],
                            'nNumTrades': row['nNumTrades'], 'nOpen': row['nOpen'],
                            'nTime': row['nTime'], 'szWindCode': row['szWindCode']})
    return sqls

def generate_snap_min_index_insert_sql(snap_min_index_path):
    sqls = list()
    if not os.path.isdir(snap_min_index_path):
        raise FileNotFoundError(snap_min_index_path)
    for file in os.listdir(snap_min_index_path):
        df = pd.read_hdf(os.path.join(snap_min_index_path, file))
        for index, row in df.iterrows():
            sqls.append({'nTime': row['nTime'], 'nOpenIndex': row['nOpenIndex'], 'nHighIndex': row['nHighIndex'],
                            'nLowIndex': row['nLowIndex'], 'nLastIndex': row['nLastIndex'], 'iVolume': row['iVolume'],
                            'iTurnover': row['iTurnover'], 'szWindCode': row['szWindCode'],
                            'nActionDay': row['nActionDay']})
    return sqls

def generate_snap_min_stock_insert_sql(snap_min_stock_path):
    sqls = list()
    if not os.path.isdir(snap_min_stock_path):
        raise FileNotFoundError(snap_min_stock_path)
    for file in os.listdir(snap_min_stock_path):
        df = pd.read_hdf(os.path.join(snap_min_stock_path, file))
        for index, row in df.iterrows():
            sqls.append({'szWindCode': row['szWindCode'], 'nActionDay': row['nActionDay'], 'nTime': row['nTime'],
                            'nPreClose': row['nPreClose'],
                            'nOpen': row['nOpen'], 'nHigh': row['nHigh'], 'nLow': row['nLow'], 'ap10': row['ap10'],
                            'av10': row['av10'], 'ap9': row['ap9'], 'av9': row['av9'],
                            'ap8': row['ap8'], 'av8': row['av8'], 'ap7': row['ap7'], 'av7': row['av7'], 'ap6': row['ap6'],
                            'av6': row['av6'], 'ap5': row['ap5'], 'av5': row['av5'], 'ap4': row['ap4'],
                            'av4': row['av4'], 'ap3': row['ap3'], 'av3': row['av3'], 'ap2': row['ap2'], 'av2': row['av2'],
                            'ap1': row['ap1'], 'av1': row['av1'], 'nMatch': row['nMatch'], 'bp1': row['bp1'],
                            'bv1': row['bv1'], 'bp2': row['bp2'], 'bv2': row['bv2'],
                            'bp3': row['bp3'], 'bv3': row['bv3'], 'bp4': row['bp4'], 'bv4': row['bv4'], 'bp5': row['bp5'],
                            'bv5': row['bv5'], 'bp6': row['bp6'], 'bv6': row['bv6'], 'bp7': row['bp7'], 'bv7': row['bv7'],
                            'bp8': row['bp8'], 'bv8': row['bv8'], 'bp9': row['bp9'],
                            'bv9': row['bv9'], 'bp10': row['bp10'], 'bv10': row['bv10'], 'nNumTrades': row['nNumTrades'],
                            'iVolume': row['iVolume'], 'iTurnover': row['iTurnover'], 'nTotalBidVol': row['nTotalBidVol'],
                            'nTotalAskVol': row['nTotalAskVol'],
                            'nWeightedAvgBidPrice': row['nWeightedAvgBidPrice'],
                            'nWeightedAvgAskPrice': row['nWeightedAvgAskPrice']})
    return sqls

def generate_snap_to_close_index_sql(snap_to_close_index_path):
    sqls = list()
    if not os.path.isdir(snap_to_close_index_path):
        raise FileNotFoundError(snap_to_close_index_path)
    for file in os.listdir(snap_to_close_index_path):
        df = pd.read_hdf(os.path.join(snap_to_close_index_path, file))
        for index, row in df.iterrows():
            sqls.append({'iTurnover': row['iTurnover'], 'iVolume': row['iVolume'],
                            'nActionDay': row['nActionDay'], 'nHighIndex': row['nHighIndex'],
                            'nLastIndex': row['nLastIndex'], 'nLowIndex': row['nLowIndex'],
                            'nOpenIndex': row['nOpenIndex'], 'nTime': row['nTime'],
                            'szWindCode': row['szWindCode']})
    return sqls

def generate_snap_to_close_stock_sql(snap_to_close_stock_path):
    sqls = list()
    if not os.path.isdir(snap_to_close_stock_path):
        raise FileNotFoundError(snap_to_close_stock_path)
    for file in os.listdir(snap_to_close_stock_path):
        df = pd.read_hdf(os.path.join(snap_to_close_stock_path, file))
        for index, row in df.iterrows():
            sqls.append({'iTurnover': row['iTurnover'], 'iVolume': row['iVolume'],
                            'nActionDay': row['nActionDay'], 'nHigh': row['nHigh'],
                            'nLow': row['nLow'], 'nMatch': row['nMatch'],
                            'nNumTrades': row['nNumTrades'], 'nOpen': row['nOpen'],
                            'nTime': row['nTime'], 'szWindCode': row['szWindCode']})
    return sqls

def mail(flag,content):
    sender = '532706324@qq.com'
    receivers = 'fwl8378@163.com'

    message = MIMEText('{}'.format(content), 'plain', 'utf-8')
    message['Subject'] = Header(flag, 'utf-8')
    message['From'] = sender
    message['To'] = receivers

    smtpter = smtplib.SMTP_SSL('smtp.qq.com', 465)
    smtpter.set_debuglevel(1)
    smtpter.login(sender, 'hpaxpruupgnqbghd')
    smtpter.sendmail(sender, receivers, message.as_string())
    smtpter.quit()
    print('邮件发送完成')

def insert_data_to_database(sqls, table):
    """
    Write data to database
    :param sqls: FORMAT ({...},{...},{...})
    """
    conn = _DbConnector_mongo("mongodb://192.168.10.68:27017", "marketdata", table)
    try:
        for sql in sqls:
            conn.update(sql)
    except Exception as e:
        content_sub = "{} Faled...".format(table) + " " * 4 + " " * 4 + "邮件" + datetime.datetime.now().strftime("%H:%M:%S")
        mail(content_sub, e)

def main() -> dict:
    """
    :return: {tablename: sqls}
    """
    #to_day = datetime.datetime.now().strftime("%Y%m%d")
    to_day = '20201029'
    all_table_data = {
        "snap_from_open_index": "/data_zhoutw/min_data/snap_from_open/index/{}".format(to_day),
        "snap_from_open_stock": "/data_zhoutw/min_data/snap_from_open/stock/{}".format(to_day),
        "snap_min_index": "/data_zhoutw/min_data/min_bar/index/min_record/{}".format(to_day),
        "snap_min_stock": "/data_zhoutw/min_data/min_bar/stock/snap/{}".format(to_day),
        "snap_to_close_index": "/data_zhoutw/min_data/snap_to_close/index/{}".format(to_day),
        "snap_to_close_stock": "/data_zhoutw/min_data/snap_to_close/stock/{}".format(to_day),
        "transaction": "/data_zhoutw/min_data/min_bar/stock/min_record/{}".format(to_day)
    }

    executor = futures.ProcessPoolExecutor()

    for tablename, path in all_table_data.items():
        if tablename == "snap_from_open_index":
            f = executor.submit(generate_snap_from_open_index_insert_sql, path)
            all_table_data[tablename] = f
        elif tablename == "snap_from_open_stock":
            f = executor.submit(generate_snap_from_open_stock_insert_sql, path)
            all_table_data[tablename] = f
        elif tablename == "snap_min_index":
            f = executor.submit(generate_snap_min_index_insert_sql, path)
            all_table_data[tablename] = f
        elif tablename == "snap_min_stock":
            f = executor.submit(generate_snap_min_stock_insert_sql, path)
            all_table_data[tablename] = f
        elif tablename == "snap_to_close_index":
            f = executor.submit(generate_snap_to_close_index_sql, path)
            all_table_data[tablename] = f
        elif tablename == "snap_to_close_stock":
            f = executor.submit(generate_snap_to_close_stock_sql, path)
            all_table_data[tablename] = f
        elif tablename == "transaction":
            f = executor.submit(generate_transaction_insert_sql, path)
            all_table_data[tablename] = f

    for tablename, f in all_table_data.items():
        all_table_data[tablename] = f.result()

    return all_table_data

if __name__ == "__main__":
    begin_time = datetime.datetime.now()
    all_table_data = main()
    print(all_table_data.keys())
    end_time = datetime.datetime.now()
    print(end_time - begin_time)
    # executor = futures.ProcessPoolExecutor()
    # for tablename, sqls in all_table_data.items():
    #     f = executor.submit(insert_data_to_database, sqls, tablename)
