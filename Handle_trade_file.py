import os
import re
import datetime

trade_directory = '/home/wuwz/daily/trade_log/{datetime}/4096/'.format(datetime=datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d'))
# 处理trade_file column
for file in os.listdir(trade_directory):
    trade_150100 = re.match(r'\d{8}_\d{6}.*?.txt$', file)
    if trade_150100:
        trade_file = os.path.join(trade_directory,trade_150100.group())
        ticker_list = list()
        with open(trade_file,'r') as file:
            for line in file:
                ticker_list.append(line.split(',')[0])
        with open(trade_file,'w+') as file:
            for line in ticker_list:
                file.write(line+'\n')
