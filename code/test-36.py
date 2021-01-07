import urllib3
import json
import pandas as pd
import time
import os
import csv

def main(path):
    if os.path.isdir(path):
        create_index_template()
        delete_index()
        create_index()
        http = urllib3.PoolManager()
        t1 = time.time()
        files = os.listdir(path)
        for file in files:
            for tmp in read_lines(file):
                # 分段生成小文件来加载
                data = '\n'.join(bulk_import_lines(tmp))
                data += '\n'
                response = http.request('PUT', 'http://localhost:9200/_bulk', body=data.encode('utf-8'), headers={'Content-Type': 'application/json'})
                print(response.status)
                print(response.data)
            t2 = time.time()
            print("导入数据耗时(ms):", (t2-t1)*1000)

def bulk_import_lines(lines):
    for line in lines:
        yield json.dumps({'index': {'_index': 'elastic', '_type': 'type'}})
        yield json.dumps(line)

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
            symbol.pop(None, None)
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

def create_index():
    http = urllib3.PoolManager()
    try:
        response = http.request('PUT', 'http://localhost:9200/elastic')
        print(response.status)
        print(response.data)
    except urllib3.exceptions:
        print('Connection failed.')

def delete_index():
    http = urllib3.PoolManager()
    try:
        response = http.request('DELETE', 'http://localhost:9200/elastic')
        print(response.status)
        print(response.data)
    except urllib3.exceptions:
        pass

def create_index_template():
    http = urllib3.PoolManager()
    data = json.dumps({
        'template': 'elastic',
        'settings': {
            'number_of_shards': 8,
            'number_of_replicas': 1,
            "index.refresh_interval": -1
        },
        'mappings': {
            '_source': {'enabled': True},
            'properties': {
                'szWindCode': {'type': 'keyword'},
                'nActionDay': {'type': 'date', "format": "yyyy.MM.dd"},
                'nTime': {'type': 'integer'},
                'nopen': {'type': 'integer'},
                'nhigh': {'type': 'integer'},
                'nlow': {'type': 'integer'},
                'nMatch': {'type': 'integer'},
                'iVolume': {'type': 'integer'},
                'iTurnover': {'type': 'short'},
                'nNumTrades': {'type': 'integer'},
                'bar_close': {'type': 'boolean'},
                'S_DQ_PRECLOSE': {'type': 'integer'},
                'S_DQ_ADJFACTOR': {'type': 'double'},
                'HighLimit': {'type': 'integer'},
                'LowLimit': {'type': 'integer'}
            }
        }
    }).encode('utf-8')
    r = http.request('PUT', 'http://localhost:9200/_template/elastic', body=data, headers={'Content-Type': 'application/json'})
    print(r.status)
    print(r.data)

def handle_hdf_to_csv(directory,directory_csv):
    if not os.path.isdir(directory_csv):
        os.mkdir(directory_csv)
    if os.path.isdir(directory):
        for file in os.listdir(directory):
            pd.DataFrame(pd.read_hdf(os.path.join(directory,file))).to_csv(os.path.join(directory_csv,file+'.csv'))
    return directory_csv

to_csv_directory = '/root/Data_csv'
path = '/root/20200630'

main(handle_hdf_to_csv(path,to_csv_directory))