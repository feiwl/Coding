import csv
import pandas as pd
import json
import os
import time
import collections

path='/home/banruo/symbol/20190624/002184.SZ_20190624.hdf5'

def generate_insert_args(filename):
    list_data_result = []
    df = pd.read_hdf(filename)
    for index, row in df.iterrows():
        hdf_data_list = list()
        hdf_data_list.append(('szWindCode', row['szWindCode']))
        hdf_data_list.append(('nActionDay', time.strftime("%Y.%m.%d",time.strptime(row['nActionDay'], "%Y%m%d"))))
        hdf_data_list.append(('nTime', row['nTime']))
        hdf_data_list.append(('nTime', row['nOpen']))
        hdf_data_list.append(('nHigh', row['nHigh']))
        hdf_data_list.append(('nLow', row['nLow']))
        hdf_data_list.append(('nMatch', row['nMatch']))
        hdf_data_list.append(('iVolume', row['iVolume']))
        hdf_data_list.append(('iTurnover', row['iTurnover']))
        hdf_data_list.append(('nNumTrades', row['nNumTrades']))
        hdf_data_list.append(('bar_close', str(row['bar_close']).replace("True",'true')))
        hdf_data_list.append(('S_DQ_PRECLOSE', row['S_DQ_PRECLOSE']))
        hdf_data_list.append(('S_DQ_ADJFACTOR', row['S_DQ_ADJFACTOR']))
        hdf_data_list.append(('HighLimit', row['HighLimit']))
        hdf_data_list.append(('LowLimit', row['LowLimit']))
        list_data_result.append(tuple([hdf_data_list]))
    return  list_data_result

def bulk_import_lines(lines):
    for line in lines:
        yield json.dumps({'index': {'_index': 'elastic', '_type': 'type'}})
        yield json.dumps(line)

# def read_lines(path_csv_file):
#     field_name = ['szWindCode', 'nActionDay', 'nTime', 'nOpen', 'nHigh', 'nLow', 'nMatch',
#                   'iVolume', 'iTurnover', 'nNumTrades', 'bar_close', 'S_DQ_PRECLOSE',
#                   'S_DQ_ADJFACTOR', 'HighLimit', 'LowLimit']
#     skip = 0
#     OrderedDict_result = list()
#     with open(path_csv_file, 'r') as file:
#         symbols = csv.DictReader(file, fieldnames=field_name)
#         for line in symbols:
#             if skip == 0:
#                 skip +=1
#                 continue
#             OrderedDict_result.append(line)
#     return OrderedDict_result

# for i in bulk_import_lines(read_lines('/home/banruo/hdf5_csv.csv')):
#     print(i)

def read_lines(path_csv_file):
    with open(path_csv_file) as f:
        f.readline()
        field_name = ['szWindCode', 'nActionDay', 'nTime', 'nOpen', 'nHigh', 'nLow', 'nMatch',
                      'iVolume', 'iTurnover', 'nNumTrades', 'bar_close', 'S_DQ_PRECLOSE',
                      'S_DQ_ADJFACTOR', 'HighLimit', 'LowLimit']
        symbols = csv.DictReader(f, fieldnames=field_name)
        cnt = 0
        temp = []
        for symbol in symbols:
            symbol['bar_close'] = 'true'
            try:
                cnt = cnt+1
                temp.append(symbol)
            except:
                pass
            if(cnt%100000==0 and cnt!=0):
                print(cnt)
                yield temp
                temp = []
        if(len(temp) > 0):
            yield temp





# print(generate_insert_args('/home/banruo/symbol/20190624/600111.SH_20190624.hdf5'))
test = None
for i in read_lines('/home/banruo/hdf5_csv.csv'):
    print('2')
    data = '\n'.join(bulk_import_lines(i))
    data += '\n'
    test = data
print(test)
