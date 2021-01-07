import pymongo
import pandas as pd
from config.config import db_collection_mapping

class ReadMinBarMongoDB:
    '''
    This class is used to get minute bar data from mongodb
    given begin_date, end date and data type this class will help to
    find the dataframe containing minute bar data you need
    '''
    def __init__(self, begin_date = None, end_date = None, id = None, type = None):
        self.__begin_date = begin_date
        self.__end_date = end_date
        self.id = id

        myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
        mydb = myclient["marketdata"]

        self.target_collection = mydb[db_collection_mapping[type]]

    def read_one_day(self, date):

        '''
        Read all days' minute bar data for a certain stock/id
        Given begin_date, end_date, id and data type, return a dataframe
        '''

        myquery = {
            'szWindCode': self.id,
            'nActionDay': date
        }

        # Find needed minute bar data and change to dataframe
        mydoc = self.target_collection.find(myquery, {'_id': 0})
        result = pd.DataFrame(mydoc)

        return result

    def read_one_day_allstock(self, date):

        '''
        Read all days' minute bar data for a certain stock/id
        Given begin_date, end_date, id and data type, return a dataframe
        '''

        myquery = {
            'nActionDay': date
        }

        # Find needed minute bar data and change to dataframe
        mydoc = self.target_collection.find(myquery, {'_id': 0})
        # result = pd.DataFrame(mydoc)

        return mydoc.count()

    def read_all_days(self):

        '''
        Read all days' minute bar data for a certain stock/id
        Given begin_date, end_date, id and data type, return a dataframe
        '''

        myquery = {
            'szWindCode': self.id,
            'nActionDay': {'$gte': self.__begin_date, '$lte': self.__end_date}
        }

        # Find needed minute bar data and change to dataframe
        mydoc = self.target_collection.find(myquery, {'_id': 0}).sort([("nActionDay", 1), ("nTime" , 1)])
        # result = pd.DataFrame(mydoc)

        return mydoc


if __name__ == '__main__':
    x = ReadMinBarMongoDB('20200819', '20200819', '000001.SZ', 'tran')
    y = x.read_all_days()

    print(y)
