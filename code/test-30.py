import pandas as pd
import time
import os
from decimal import Decimal
import argparse

def handler_file(path):
    data_csv=list()
    with open(path,"r") as file:
        data_csv.append(list(filter(lambda line: line.split(',')[3] != '1' and line is not None ,file)))
        file.close()
    if os.path.isfile(path):
        os.remove(path)
    with open(path,'a+') as file:
        for i in next(iter(data_csv)):
            file.write(i)
        file.close()

def dataframe_result(path,datetime):
    df = pd.DataFrame(pd.read_csv(path))
    symbol=list()
    upperLimitPx=list()
    lowerLimitPx=list()

    list(map(lambda sym: symbol.append(sym),df['symbol']))
    list(map(lambda ulp: upperLimitPx.append(ulp),df['upperLimitPx']))
    list(map(lambda llp: lowerLimitPx.append(llp),df['lowerLimitPx']))
    upperLimitPx=[ [f"{round(Decimal(str(round(i,4))) * 10000)}"] for i in upperLimitPx]
    lowerLimitPx=[ [f"{round(Decimal(str(round(i,4))) * 10000)}"] for i in lowerLimitPx]

    sym_upperlimit=dict(zip(symbol,upperLimitPx))
    sym_upperlimit=pd.DataFrame(sym_upperlimit)
    sym_upperlimit.insert(0,'datetime',datetime)

    sym_lowerlimit=dict(zip(symbol,lowerLimitPx))
    sym_lowerlimit=pd.DataFrame(sym_lowerlimit)
    sym_lowerlimit.insert(0,'datetime',datetime)

    return sym_upperlimit,sym_lowerlimit,upperLimitPx

def insert_highlimit_df(source_hdf,to_hdf,highlimit_data):
    source_hdf_file = pd.DataFrame()
    if source_hdf:
        source_hdf_file = pd.read_hdf(source_hdf)
    current_result=source_hdf_file.append(highlimit_data,ignore_index=True,sort=False)
    current_result = current_result.where((current_result.notna()), 'null')
    current_result = current_result[sorted(current_result.columns)]
    h5_store = pd.HDFStore(to_hdf, mode='a')
    h5_store['data'] = current_result
    h5_store.close()

def insert_lowlimit_df(source_hdf,to_hdf,lowlimit_data):
    source_hdf_file = pd.DataFrame()
    if source_hdf:
        source_hdf_file = pd.read_hdf(source_hdf)
    current_result=source_hdf_file.append(lowlimit_data,ignore_index=True,sort=False)
    current_result = current_result.where((current_result.notna()), 'null')
    current_result = current_result[sorted(current_result.columns)]
    h5_store = pd.HDFStore(to_hdf, mode='a')
    h5_store['data'] = current_result
    h5_store.close()

# parser = argparse.ArgumentParser(description="Read Database to HDF5_FILE")
# parser.add_argument('--date', default=True, help="File time example 20200101")
# parser.add_argument('--highlimit-source-hdf',required=False,help='History highlimit_hdf5_file')
# parser.add_argument('--highlimit-to-hdf',required=True,help='Target highlimit_hdf5_path_file')
# parser.add_argument('--lowlimit-source-hdf',required=False,help='History lowlimit_hdf5_file')
# parser.add_argument('--lowlimit-to-hdf',required=True,help='Target lowlimit_hdf5_path_file')
# parser.add_argument('--refData-csv',required=True,help='Today refData_csv_file')
# args = parser.parse_args()
#
# handler_file(args.refData_csv)
# sym_upperlimit,sym_lowerlimit = dataframe_result(args.refData_csv,time.strftime('%Y-%m-%d',time.strptime(args.date,'%Y%m%d')))
# insert_highlimit_df(args.highlimit_source_hdf,args.highlimit_to_hdf,sym_upperlimit)
# insert_lowlimit_df(args.lowlimit_source_hdf,args.lowlimit_to_hdf,sym_lowerlimit)

path='/home/banruo/20200701_refData.csv'
df = pd.read_csv(path)
for index,value in df.iterrows():
    print(value['symbol'],value['upperLimitPx'])
# data=dataframe_result(path,'2020-07-01')
# for index, value in data[0].iterrows():
#     print(value['688528.SH'])

