__author__ = 'SharonYU'

from regenerate_EOD.utils.read_minite_bar import ReadMinBarMongoDB
from regenerate_EOD.src_data_clean.snap_index_regenerate_EOD import new_daily_eod

class generate_index_new_EOD():

    '''
    This class is used to call the regeneration of new EOD data
    Given begin date and end date, it will read data from mongodb first
    and then call the regenerate EOD function to regenerate the EOD data
    '''
    def __init__(self, begin_date = None, end_date = None, target_time = None, index_id = None):

        self.__begin_date = begin_date
        self.__end_date = end_date
        self.__target_time = target_time
        self.__index_id = index_id

    def get_min_bar(self):
        '''
        Read minute bar data from mongodb
        '''
        min_bar = ReadMinBarMongoDB(begin_date=self.__begin_date,
                                    end_date=self.__end_date,
                                    id=self.__index_id,
                                    type='snap_index')
        history_min_bar = min_bar.read_all_days()

        return history_min_bar

    def get_new_EOD(self):
        '''
        Regenerate index EOD data
        '''
        print('Regenerating EOD data for index: %s' % self.__index_id)
        new_EOD = new_daily_eod(data_cursor=self.get_min_bar(),
                                target_time=self.__target_time)
        return new_EOD

if __name__ == '__main__':
    test = generate_index_new_EOD(begin_date='20190101', end_date='20200825',
                                  target_time=1400e5, index_id='399905.SZ')
    # y = test.get_min_bar()
    EOD = test.get_new_EOD()

    print()