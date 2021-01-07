import pymongo
import pandas as pd
import os
import argparse

def result_dicts(path):
    all_column_list = list()
    if os.path.isdir(path):
        for file in os.listdir(path):
            all_column_list.extend(generate_insert_args(os.path.join(path, file)))
    return all_column_list

# 获取hdf文件的单行列表
def generate_insert_args(filename) -> list:
    columns = list()
    df = pd.read_hdf(filename)
    for index, row in df.iterrows():
        columns.append({'szWindCode': row['szWindCode'], 'nActionDay': row['nActionDay'], 'nTime': row['nTime'], 'nPreClose': row['nPreClose'],
         'nOpen':row['nOpen'], 'nHigh': row['nHigh'], 'nLow':row['nLow'], 'ap10': row['ap10'], 'av10':row['av10'] , 'ap9': row['ap9'],'av9': row['av9'],
         'ap8': row['ap8'], 'av8':row['av8'] , 'ap7':row['ap7'], 'av7':row['av7'], 'ap6': row['ap6'], 'av6': row['av6'], 'ap5': row['ap5'], 'av5':row['av5'],'ap4':row['ap4'],
        'av4':row['av4'],'ap3':row['ap3'],'av3':row['av3'],'ap2':row['ap2'],'av2':row['av2'],'ap1':row['ap1'],'av1':row['av1'],'nMatch':row['nMatch'],'bp1':row['bp1'],'bv1':row['bv1'],'bp2':row['bp2'],'bv2':row['bv2'],
         'bp3':row['bp3'],'bv3':row['bv3'],'bp4':row['bp4'],'bv4':row['bv4'],'bp5':row['bp5'],'bv5':row['bv5'],'bp6':row['bp6'],'bv6':row['bv6'],'bp7':row['bp7'],'bv7':row['bv7'],'bp8':row['bp8'],'bv8':row['bv8'],'bp9':row['bp9'],
        'bv9':row['bv9'],'bp10':row['bp10'],'bv10':row['bv10'],'nNumTrades':row['nNumTrades'],'iVolume':row['iVolume'],'iTurnover':row['iTurnover'],'nTotalBidVol':row['nTotalBidVol'],'nTotalAskVol':row['nTotalAskVol'],
        'nWeightedAvgBidPrice':row['nWeightedAvgBidPrice'],'nWeightedAvgAskPrice':row['nWeightedAvgAskPrice']})
    return columns

parse = argparse.ArgumentParser("Insert mongo_memory document")
parse.add_argument("--path",required=True,help='datetime path ')
args = parse.parse_args()

myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
mydb = myclient["marketdata"]
mycol = mydb["transaction"]
#mycol.create_index([("szWindCode",1),("nActionDay",1),("nTime",1)])
mycol.create_index([("szWindCode",1)])
mycol.create_index([("nActionDay",1)])
mycol.create_index([("nTime",1)])
x = mycol.insert_many(result_dicts(args.path))
print(x)