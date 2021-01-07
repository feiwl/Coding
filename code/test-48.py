import pymongo
from pymongo import ASCENDING, DESCENDING
import time
import pandas as pd
import os
import argparse

def generate_insert_args(filename):
    columns = list()
    df = pd.read_hdf(filename)
    for index, row in df.iterrows():
        columns.append({'nTime':row['nTime'], 'nOpen':row['nOpen'], 'nHigh':row['nHigh'], 'nLow':row['nLow'],
                        'nMatch':row['nMatch'], 'iVolume':row['iVolume'], 'iTurnover':row['iTurnover'],
                        'nNumTrades':row['nNumTrades'], 'bar_close':row['bar_close'], 'szWindCode':row['szWindCode'], 'nActionDay':row['nActionDay']})
    return columns

def handle_file(path):
    all_column_list = list()
    if os.path.isdir(path):
        for file in os.listdir(path):
            all_column_list.extend(generate_insert_args(os.path.join(path, file)))
    return all_column_list

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
x = mycol.insert_many(handle_file(args.path))
print(x)