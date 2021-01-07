from collections import Counter
import argparse
import json
import os
import re

'''
1. stock_data_addition: Add modules with the same account number in the JSON file and return the result
2. handling_csv_to_dict: Process the layout of the CSV file into the corresponding format
3. data_comparison: The processing results of CSV and JSON are compared to find out the difference
'''

def detect_file_existence(fn):
    def wrrper(*args,**kwargs):
        if not os.path.isfile(*args):
            raise FileNotFoundError(*args)
        x = fn(*args,**kwargs)
        return x
    return wrrper

@detect_file_existence
def stock_data_addition(sym_file) -> dict:
    account_dict = {}
    with open(sym_file, 'r') as f:
        module = Counter()
        for k,v in json.load(f).items():
            if k == 'position':
                for account, symbols_dict in v.items():
                    for _, sym_module_dict in symbols_dict.items():
                        counter_sym = dict()
                        for k,v in sym_module_dict.items():
                            counter_sym[re.search('\d+',k).group()] = v
                        counter_sym = Counter(counter_sym)
                        module += counter_sym
                    account_dict[account] = module
                    module = Counter()
    return account_dict

@detect_file_existence # handling_csv_to_dict = detect_file_existence(handling_csv_to_dict)
def handling_csv_to_dict(csv) -> dict:
    with open(csv, 'r') as file:
        test = list(map(lambda line: (''.join(line.split(',')[1].split()),
                                      ''.join(line.split(',')[3].split())), file))
        result = dict()
        test.pop(0)
        for k, v in test:
            if v != '0':
                result.update({k: int(v)})
        return result

def data_comparison(appcsv, outcsv):
    result_1=dict()
    result_2=dict()
    for k in appcsv.keys():
        if outcsv.get(k) != appcsv.get(k) and appcsv.get(k) != '0':
            result_1.update({(k, appcsv.get(k)):(k, outcsv.get(k))})

    for k in outcsv.keys():
        if outcsv.get(k) != appcsv.get(k) and appcsv.get(k) != '0':
            result_2.update({(k, appcsv.get(k)):(k, outcsv.get(k))})

    return result_1,result_2

parser = argparse.ArgumentParser(description="Data comparison")
parser.add_argument('--symbol-4096', required=True, help='Data csv file')
parser.add_argument('--symbol-4295', required=True, help='Data csv file')
parser.add_argument('--symbol-json', required=True, help='Data json file')
args = parser.parse_args()

calculation_results = stock_data_addition(args.symbol_json)

account_stock_data = {'105150004096':'', '105150004295':''}
account_stock_data['105150004096'] = handling_csv_to_dict(args.symbol_4096)
account_stock_data['105150004295'] = handling_csv_to_dict(args.symbol_4295)

results = list()
for k,v in calculation_results.items():
    result = data_comparison(account_stock_data[k],dict(v))
    results.append(result)


for i in results:
    print(i)
