from read_stock_new_snap_1 import new_stock_snap_A_B
import datetime
import numpy as np
import dolphindb as ddb

begin_date = '20190101'
end_date = '20190102'
target_time = int(1400e5)
begin_time_0 = datetime.datetime.now()
B_frame = new_stock_snap_A_B(begin_date, end_date, target_time)

print(B_frame)

# B_frame['nActionDay'] = B_frame['nActionDay'].astype(np.datetime64)
# B_frame['bar_close'] = B_frame['bar_close'].astype(np.bool)
# B_frame['szWindCode'] = B_frame['szWindCode'].astype(np.object)
# B_frame = B_frame[['szWindCode','nActionDay','nTime','nOpen','nHigh','nLow','nMatch','iVolume','iTurnover','bar_close']]
# print(B_frame)
#
# s = ddb.session()
# s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")
# s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db="dfs://marketdata",tb="symbol_data"), B_frame)
