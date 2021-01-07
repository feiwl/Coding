import pymysql
import pandas as pd
import argparse

def contract_periodic_query(date:str, db:pymysql.connect) -> pd.DataFrame:
    sql = "select con.prod,con.symbol,con.date,symbol.expireDate from " \
          "(select prod,max(date) as date,max(symbol) as symbol from ctpVirSym.virsym where date<=%s group by prod) as con " \
          "left join ctpVirSym.symbol on  con.symbol = symbol.symbol;" %(date)
    df = pd.read_sql(sql, db)
    return df

parser = argparse.ArgumentParser(description="Query datetime")
parser.add_argument('--date', required=True, help='#: 20201228')
parser.add_argument('--to-csv', required=True, help='result to csv file...')
args = parser.parse_args()

db = pymysql.connect(host='192.168.10.68',
                     port=3306,
                     user='nas',
                     password='Prism@123456')

main_contract = contract_periodic_query(args.date, db)

main_contract.to_csv(args.to_csv, index=False)
