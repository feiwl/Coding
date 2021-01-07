import pymongo
import pandas as pd
import os
import argparse

def result_dicts(path) -> list:
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
        columns.append({'szWindCode':row['szWindCode'],'nActionDay':row['nActionDay'], 'nTime':row['nTime'],'nOpenIndex':row['nOpenIndex'],
            'nHighIndex':row['nHighIndex'],'nLowIndex':row['nLowIndex'],'nLastIndex':row['nLastIndex'],'iTotalVolume':row['iTotalVolume'],'iTurnover':row['iTurnover'],'nPreCloseIndex':row['nPreCloseIndex']})
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