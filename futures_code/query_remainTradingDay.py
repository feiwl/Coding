import os
import pymysql
import pandas as pd
import datetime
import argparse

class Contract_Remaining:
    def __init__(self, date, holiday):
        self.db = pymysql.connect(host='192.168.10.68',
                     port=3306,
                     user='nas',
                     password='Prism@123456')
        self.contract_remain_dt = {'symbol': [], 'expireDate': [], 'remainTradingDay': []}
        self.date = date
        self.holiday = holiday

    def get_contract_expiration(self) -> pd.DataFrame:
        sql = "select con.prod,con.symbol,con.date,symbol.expireDate from " \
              "(select prod,max(date) as date,max(symbol) as symbol from ctpVirSym.virsym where date<=%s group by prod) as con " \
              "left join ctpVirSym.symbol on con.symbol = symbol.symbol;" % (self.date)

        df = pd.read_sql(sql, self.db)
        return df

    def get_remaining_maturity_date(self) -> pd.DataFrame:
        contract_data = self.get_contract_expiration()

        if os.path.isfile(self.holiday):
            HOLIDAY_CSV = pd.read_csv(self.holiday)

            # After going to all holiday days
            holidays = [datetime.datetime.strptime(v['Date'], '%Y-%m-%d') for k, v in HOLIDAY_CSV.iterrows()]
            TO_DAY = datetime.datetime.strptime(self.date, '%Y%m%d')
        else:
            raise FileNotFoundError(self.holiday)

        for _, row in contract_data.iterrows():
            expireDate = datetime.datetime.strptime(str(row['expireDate']), '%Y-%m-%d')

            # Filter out the remaining maturity date
            holiday_lst = list(filter(lambda day: (day < expireDate and day > TO_DAY), holidays))

            holiday_lst = list(map(lambda day: datetime.datetime.strftime(day, '%Y%m%d'), holiday_lst))
            remaining_date = (expireDate - TO_DAY).days - len(holiday_lst)

            if remaining_date < 10:
                self.contract_remain_dt['symbol'].append(row['symbol'])
                self.contract_remain_dt['expireDate'].append(row['expireDate'])
                self.contract_remain_dt['remainTradingDay'].append(remaining_date)
 
        return pd.DataFrame(self.contract_remain_dt)

parser = argparse.ArgumentParser(description="Query datetime")
parser.add_argument('--date', required=True, help='#: 20201228')
parser.add_argument('--to-csv', required=True, help='result to csv file...')
parser.add_argument('--holiday', required=True, help='holiday.csv')
args = parser.parse_args()

contract_remaining = Contract_Remaining(args.date, args.holiday)
df = contract_remaining.get_remaining_maturity_date()
df.to_csv(args.to_csv)