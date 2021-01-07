import pandas as pd
import  json
import time
import os
import re
import argparse

datetime = time.strftime('%Y%m%d',time.localtime())

def json_handle(path):
    with open(path,"r",encoding="utf-8") as j_obj:
        json_data= json.load(j_obj)

    account_list = [ i for i in json_data]
    account_result = [ {i:json_data[i]['symbols']} for i in account_list]
    account_dict= { i:{'symbols':[],'position':[],'sellableQty':[]} for i in account_list}

    for i in account_result:
        for account_id, v in i.items():
            for symbol,v in v.items():
                account_dict[account_id]['symbols'].append(symbol)
                account_dict[account_id]['position'].append(v['position'])
                account_dict[account_id]['sellableQty'].append(v['sellableQty'])
    return account_dict

def to_csv(account_dict,source_path,to_path):
    for i in account_dict.keys():
        account = account_dict[i]
        symbol = account['symbols']
        position = account['position']
        sellableQty = account['sellableQty']

        symbol_se = pd.Series(symbol,name='symbol')
        position_se = pd.Series(position,name='position')
        sellableQty_se = pd.Series(sellableQty,name='sellableQty')
        result_df = pd.concat([symbol_se, position_se, sellableQty_se], axis=1)
        if not os.path.isdir(os.path.join(to_path,i)):
            os.mkdir(os.path.join(to_path,i))
        result_df.to_csv(os.path.join(to_path,i) + '/' + datetime + '_' + re.sub("\D", "", os.path.split(source_path)[1]) + '.csv',index=False)

# parser = argparse.ArgumentParser(description="Insert_Marketdata")
# parser.add_argument('--source-filepath', required=True, help='source data path')
# parser.add_argument('--tocsv-path', required=True, help='tocsv directory path')
# args = parser.parse_args()
#
# if not os.path.isdir(os.path.join(args.tocsv_path,datetime)):
#     os.mkdir(os.path.join(args.tocsv_path,datetime))
# to_csv(json_handle(args.source_filepath),args.source_path,args.tocsv_path+'/'+datetime)

source_path = '/home/banruo/as-091026.json'
for i in json_handle(source_path):
    print(i[-4:], i)

# print(os.path.split(source_path)[1])


