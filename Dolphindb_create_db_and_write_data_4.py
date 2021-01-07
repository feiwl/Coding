import dolphindb as ddb
import pandas as pd
import argparse
import datetime
import os

def generate_transaction_insert_sql(tran_path) -> list:
    sqls = list()
    if os.path.isdir(tran_path):
        for file in os.listdir(tran_path):
            df = pd.read_hdf(os.path.join(tran_path,file))
            for index, row in df.iterrows():
                sqls.append({'nTime':row['nTime'], 'nOpen':row['nOpen'], 'nHigh':row['nHigh'],
                             'nLow':row['nLow'],'nMatch':row['nMatch'], 'iVolume':row['iVolume'],
                             'iTurnover':row['iTurnover'],'nNumTrades':row['nNumTrades'],
                             'bar_close':row['bar_close'], 'szWindCode':row['szWindCode'],
                             'nActionDay':row['nActionDay']})
    else:
        raise FileNotFoundError(tran_path)
    return sqls

parse = argparse.ArgumentParser("Insert Dolphindb data")
parse.add_argument("--path",required=True,help='datetime path ')
args = parse.parse_args()


start_date = datetime.datetime.now()

df = generate_transaction_insert_sql(args.path)

df = pd.DataFrame(df)

df = df[['szWindCode', 'nActionDay', 'nTime', 'nOpen', 'nHigh', 'nLow', 'nMatch', 'iVolume', 'iTurnover', 'bar_close']]

s = ddb.session()
s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")
s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db="dfs://marketdata",tb="symbol_data"), df)

end_date = datetime.datetime.now()
print(end_date - start_date)