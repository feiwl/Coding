__author__ = 'ZhouTW'

""" import packages """
import pandas as pd
import os
import time
import warnings
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
warnings.filterwarnings("ignore")


class stock_minute_bar:
    def __init__(self, code, day, data_dir, basis_data, minutes):
        """:arg
        code: stock_id, str
        day: date, str
        data_dir: location for the date
        basis_data: dict, including preclose, high_limit, low_limit, adj_factor
        minute: list, including all minutes"""

        # set init values
        self.code = code
        self.day = day
        # target columns need to be retrieved
        self.target_columns = ['szWindCode', 'nActionDay', 'nTime', 'nOpen', 'nHigh', 'nLow', 'nMatch', 'iVolume',
                               'iTurnover', 'nNumTrades', 'bar_close']

        self.file_name_tran = "%s/%s/%s_%s_TRANSACTION.csv" % (code[-2:], code, code, day)

        self.data_dir = data_dir

        self.basis_data = basis_data
        self.minutes = minutes

        # read data
        self.read_data()

    def read_data(self):
        if os.path.exists(self.data_dir + self.file_name_tran):
            self.tran = pd.read_csv(self.data_dir + self.file_name_tran)
        else:
            print('data for stock %s at date %s is not existed' % (self.code, self.day))
            self.tran = None

    def get_min_bar(self):
        # if no record was found for the stock, return null value
        if self.tran is None:
            return None

        df = self.tran.copy()
        # if record exists
        "init values"
        # target_df = pd.DataFrame(columns=self.target_columns)
        target_record = []
        # set ini_record
        crt_bar = 92500000
        market_open = False

        "for the first trading record"
        tran_iter = iter(df.values)

        while not market_open:
            (systime, szWindCode, szCode, nActionDay, nTime, nIndex, nPrice, nVolume, nTurnover,
             nBSFlag, chOrderKind, chFunctionCode, nAskOrder, nBidOrder) = next(tran_iter)

            # 如果是撤单
            if chFunctionCode == 'C':
                continue

            # market is open now, first valid transaction record appears
            market_open = True

            # update crt bar time
            if nTime - crt_bar < 1e5:
                pass
            else:
                # 第一笔快照不在第一分钟
                # update time by minute
                crt_bar = int(nTime // 1e5 * 1e5)

                if crt_bar > 92500000:
                    min_append = sorted(list(filter(lambda x: x < crt_bar, self.minutes)))
                    preclose = self.basis_data['S_DQ_PRECLOSE']
                    for item in min_append:
                        target_record.append(pd.Series({'nTime': item,
                                                        'nOpen': preclose,
                                                        'nHigh': preclose,
                                                        'nLow': preclose,
                                                        'nMatch': preclose,
                                                        'iVolume': 0,
                                                        'iTurnover': 0,
                                                        'nNumTrades': 0,
                                                        'bar_close': True}))

            # generate new bar data
            new_bar_record = {'nTime': crt_bar,
                              'nOpen': nPrice,
                              'nHigh': nPrice,
                              'nLow': nPrice,
                              'nMatch': nPrice,
                              'iVolume': nVolume,
                              'iTurnover': nTurnover,
                              'nNumTrades': 1,
                              'bar_close': False}

        "after the first record"
        for (systime, szWindCode, szCode, nActionDay, nTime, nIndex, nPrice, nVolume, nTurnover, nBSFlag, chOrderKind,
             chFunctionCode, nAskOrder, nBidOrder) in tran_iter:
            if chFunctionCode == 'C':
                # if order cancelled
                continue

            if nTime - crt_bar < 1e5:
                # still inside the bar
                # update record
                # update high price
                if nPrice > new_bar_record['nHigh']:
                    new_bar_record['nHigh'] = nPrice
                elif nPrice < new_bar_record['nLow']:
                    new_bar_record['nLow'] = nPrice

                # update last price
                new_bar_record['nMatch'] = nPrice

                # update volume
                new_bar_record['iVolume'] += nVolume

                # update amount
                new_bar_record['iTurnover'] += nTurnover

                # num_trade increase
                new_bar_record['nNumTrades'] += 1

            else:
                # outside the bar
                # save records
                new_bar_record['bar_close'] = True
                target_record.append(pd.Series(new_bar_record))

                # 补齐没有记录的分钟bar
                crt_bar_temp = int(nTime // 1e5 * 1e5)
                min_append = sorted(list(filter(lambda x: (x < crt_bar_temp) & (x > crt_bar), self.minutes)))

                for item in min_append:
                    target_record.append(pd.Series({'nTime': item,
                                                    'nOpen': new_bar_record['nMatch'],
                                                    'nHigh': new_bar_record['nMatch'],
                                                    'nLow': new_bar_record['nMatch'],
                                                    'nMatch': new_bar_record['nMatch'],
                                                    'iVolume': 0,
                                                    'iTurnover': 0,
                                                    'nNumTrades': 0,
                                                    'bar_close': True}))

                # update time
                crt_bar = int(nTime // 1e5 * 1e5)
                # generate new bar data
                new_bar_record = {'nTime': crt_bar,
                                  'nOpen': nPrice,
                                  'nHigh': nPrice,
                                  'nLow': nPrice,
                                  'nMatch': nPrice,
                                  'iVolume': nVolume,
                                  'iTurnover': nTurnover,
                                  'nNumTrades': 1,
                                  'bar_close': False}

        new_bar_record['bar_close'] = True
        target_record.append(pd.Series(new_bar_record))

        # after the last record, if no record for the rest of the day
        if crt_bar < 150000000:
            min_append = sorted(list(filter(lambda x: x > crt_bar, self.minutes)))
            for item in min_append:
                target_record.append(pd.Series({'nTime': item,
                                                'nOpen': new_bar_record['nMatch'],
                                                'nHigh': new_bar_record['nMatch'],
                                                'nLow': new_bar_record['nMatch'],
                                                'nMatch': new_bar_record['nMatch'],
                                                'iVolume': 0,
                                                'iTurnover': 0,
                                                'nNumTrades': 0,
                                                'bar_close': True}))

        target_df = pd.concat(target_record, axis=1, ignore_index=True).T
        # modify data type
        target_df = target_df.astype({'nOpen': 'int64',
                                      'nHigh': 'int64',
                                      'nLow': 'int64',
                                      'nMatch': 'int64',
                                      'iVolume': 'int64',
                                      'iTurnover': 'int64',
                                      'nNumTrades' : 'int64',
                                      'bar_close': 'bool_'})

        # %% show result
        target_df['szWindCode'] = self.code
        target_df['nActionDay'] = self.day
        # target_df['S_DQ_PRECLOSE'] = self.basis_data['S_DQ_PRECLOSE']
        # target_df['S_DQ_ADJFACTOR'] = self.basis_data['S_DQ_ADJFACTOR']
        # target_df['HighLimit'] = self.basis_data['HighLimit']
        # target_df['LowLimit'] = self.basis_data['LowLimit']
        return target_df

    def get_min_bar_result(self):
        try:
            result_stock = self.get_min_bar()
            if result_stock is None:
                return self.code, None
            else:
                return self.code, result_stock
        except Exception as e:
            print(e)
            print('fail to get target for stock %s' % self.code)
            return self.code, None


if __name__ == '__main__':
    code = '000017.SZ'
    day = '20190108'
    dir_ = r'\\192.168.10.235\data_home\stock_data\market_data\\'

    start = time.time()
    with open('../../../utils/files/minute_record.txt', 'r') as file:
        minutes = file.readlines()
        minutes = [int(item.rstrip('\n')) for item in minutes]

    stock_basis = {'S_DQ_PRECLOSE': 100800,
                   'S_DQ_ADJFACTOR': 3.65,
                   'HighLimit': 110900,
                   'LowLimit': 90700}
    result = stock_minute_bar(code, day, dir_, stock_basis, minutes)

    between = time.time()
    code, result_df = result.get_min_bar_result()

    end = time.time()
    print('Time cost before get_min_bar function: ', between - start)
    print('Time cost for the get_min_bar function', end - between)
