import pymongo
from pymongo import ASCENDING, DESCENDING
import time
import pandas as pd
import os
import argparse

def generate_insert_args(directory):
    if not os.path.isdir(directory):
        raise FileNotFoundError(directory)
    columns = list()
    for file in os.listdir(directory):
        df = pd.read_hdf(os.path.join(directory, file))
        for index, row in df.iterrows():
            columns.append({'iTurnover':row['iTurnover'], 'iVolume':row['iVolume'],
                            'nActionDay':row['nActionDay'], 'nHighIndex':row['nHighIndex'],
                            'nLastIndex':row['nLastIndex'], 'nLowIndex':row['nLowIndex'],
                            'nOpenIndex':row['nOpenIndex'],'nTime':row['nTime'],
                            'szWindCode':row['szWindCode']})
    return columns

parse = argparse.ArgumentParser("Insert mongo_memory document")
parse.add_argument("--path",required=True,help='datetime path ')
args = parse.parse_args()

myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
mydb = myclient["marketdata"]
mycol = mydb["snap_from_open_index"]
#mycol.create_index([("szWindCode",1),("nActionDay",1),("nTime",1)])
mycol.create_index([("szWindCode",1)])
mycol.create_index([("nActionDay",1)])
mycol.create_index([("nTime",1)])
x = mycol.insert_many(generate_insert_args(args.path))
print(x)
