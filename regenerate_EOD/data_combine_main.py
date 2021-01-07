from src_data_comb.data_combination import data_combination

if __name__ == '__main__':
    begin_date = '20190101'
    end_date = '20200825'
    target_time = 1400e5

    stock_id = '000001.SZ'
    concat_flag = True

    combine_data = data_combination(begin_date=begin_date, end_date=end_date, target_time=target_time)

    # Combine one stock EOD data
    result = combine_data.single_stock_combination_main(stock_id)

    # Combine a list of stocks' EOD data
    # result = combine_data.multi_stocks_combination_main([], concat_flag)
