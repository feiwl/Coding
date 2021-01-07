__author__ = 'SharonYU'

from min_bar.utils.read_minite_bar import ReadIndexMinBarFiles
from min_bar.src_data_clean.minutes_bar_regenerate.snap_index_regenerate_EOD import new_daily_eod

class generate_index_new_EOD():
    def __init__(self, begin_date = None, end_date = None, target_time = None, index_id = None):

        self.__begin_date = begin_date
        self.__end_date = end_date
        self.__target_time = target_time
        self.__index_id = index_id

    def get_min_bar(self):
        min_bar = ReadIndexMinBarFiles(begin_date=self.__begin_date,
                                       end_date=self.__end_date,
                                       index_id=self.__index_id)
        history_min_bar = min_bar.read_all_days()

        return history_min_bar

    def get_new_EOD(self):
        print('Regenerating EOD data for index: %s' % self.__index_id)
        new_EOD = new_daily_eod(history_min_bar=self.get_min_bar(),
                                target_min=self.__target_time)
        return new_EOD