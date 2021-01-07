import configparser
import platform
import datetime
import os


def find_target_date():
    # read file
    pwd = os.getcwd()
    # father_path = os.path.abspath(os.path.dirname(pwd) + os.path.sep + ".")
    target_file = pwd + '/config/target_date.ini'

    cf = configparser.ConfigParser()
    cf.read(target_file)
    use_hist_date = cf.getboolean('set', 'calculate_hist')
    # 如果前一天没算，则用今天的日期
    if not use_hist_date:
        return datetime.datetime.now().strftime('%Y%m%d')
    # 如果前一天算了，则用target的日期
    else:
        result = cf.get('set', 'target_date')
        if len(result) == 8:
            return result
        else:
            raise Exception('Error Date Type')
