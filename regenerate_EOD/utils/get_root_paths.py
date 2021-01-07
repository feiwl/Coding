import platform

def get_root_path():
    if platform.system() == 'Windows':
        # print('platform is windows')
        file_dir_tick_data = r'\\192.168.10.235\data_home\stock_data\market_data\\'
        file_dir_min_data = r'F:\minute_bar_data\\'
        file_dir_daily_data = r'\\192.168.10.68\daily_factor\\'
    else:
        # print('platform is linux')
        file_dir_tick_data = '/mnt/data_home/stock_data/market_data//'
        file_dir_min_data = '/home/sharonyu/data/'
        # file_dir_daily_data = '/mnt/daily_factor/'
        file_dir_daily_data = '/home/banruo/daily_factor/'

    return file_dir_tick_data, file_dir_min_data, file_dir_daily_data