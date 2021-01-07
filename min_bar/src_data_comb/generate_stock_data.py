__author__ = 'SharonYU'

from min_bar.utils.read_minite_bar import ReadStockMinBarFiles
from min_bar.src_data_clean.minutes_bar_regenerate.tran_regenerate_EOD import new_daily_eod

class generate_stock_new_EOD():
    def __init__(self, begin_date = None, end_date = None, target_time = None, stock_id = None):

        self.__begin_date = begin_date
        self.__end_date = end_date
        self.__target_time = target_time
        self.__stock_id = stock_id

    def get_min_bar(self):
        min_bar = ReadStockMinBarFiles(begin_date=self.__begin_date,
                                       end_date=self.__end_date,
                                       stock_id=self.__stock_id,
                                       type='tran')
        flag, history_min_bar = min_bar.read_all_days()

        return flag, history_min_bar

    def get_new_EOD(self):

        flag, history_min_bar = self.get_min_bar()
        if flag:
            new_EOD = new_daily_eod(history_min_bar=history_min_bar,
                                    target_min=self.__target_time)
        else:
            new_EOD = None
        return new_EOD