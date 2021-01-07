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

def dataframe_result(path):
    df = pd.DataFrame(pd.read_csv(path))
    symbol=list()
    upperLimitPx=list()
    lowerLimitPx=list()

    list(map(lambda sym: symbol.append(str(sym)),df['symbol']))
    list(map(lambda ulp: upperLimitPx.append(ulp),df['upperLimitPx']))
    list(map(lambda llp: lowerLimitPx.append(llp),df['lowerLimitPx']))
    upperLimitPx=[ int(f"{round(Decimal(str(round(i,4))) * 10000)}") for i in upperLimitPx]
    lowerLimitPx=[ int(f"{round(Decimal(str(round(i,4))) * 10000)}") for i in lowerLimitPx]

    sym_upperlimit=dict(zip(symbol,upperLimitPx))
    sym_lowerlimit=dict(zip(symbol,lowerLimitPx))
    return sym_upperlimit,sym_lowerlimit

def insert_data_df(source_hdf,to_hdf,data,datetime):
    source_hdf_file = pd.DataFrame()
    if source_hdf:
        source_hdf_file = pd.read_hdf(source_hdf)
    datetime_indexs = [ index for index,v in source_hdf_file.iterrows()]
    datetime_indexs.append(datetime)
    data_ss = pd.Series(data)
    data_column = set(data_ss.index) ^ set(source_hdf_file.columns)

    if len(data_column) > 0:
        for item in data_column:
            if item not in source_hdf_file:
                source_hdf_file[item] = None
            if item not in data_ss:
                data_ss[item] = None
        source_hdf_file = source_hdf_file.append(data_ss[source_hdf_file.columns], ignore_index=True)
    else:
        source_hdf_file = source_hdf_file.append(data_ss[source_hdf_file.columns], ignore_index=True)

    current_result = pd.DataFrame(index=datetime_indexs,data=source_hdf_file.values,columns=source_hdf_file.columns)
    current_result = current_result[sorted(source_hdf_file.columns)]
    h5_store = pd.HDFStore(to_hdf, mode='a')
    h5_store['data'] = current_result
    h5_store.close()

parser = argparse.ArgumentParser(description="Read Database to HDF5_FILE")
parser.add_argument('--date', default=True, help="File time example 20200101")
parser.add_argument('--highlimit-source-hdf',required=False,help='History highlimit_hdf5_file')
parser.add_argument('--highlimit-to-hdf',required=True,help='Target highlimit_hdf5_path_file')
parser.add_argument('--lowlimit-source-hdf',required=False,help='History lowlimit_hdf5_file')
parser.add_argument('--lowlimit-to-hdf',required=True,help='Target lowlimit_hdf5_path_file')
parser.add_argument('--refData-csv',required=True,help='Today refData_csv_file')
args = parser.parse_args()

handler_file(args.refData_csv)
sym_upperlimit,sym_lowerlimit = dataframe_result(args.refData_csv)
insert_data_df(args.highlimit_source_hdf,args.highlimit_to_hdf,sym_upperlimit,args.date)
insert_data_df(args.lowlimit_source_hdf,args.lowlimit_to_hdf,sym_lowerlimit,args.date)


