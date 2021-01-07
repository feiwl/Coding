import datetime as dt
import dolphindb as ddb
import datetime

begin_time = datetime.datetime.now()
print(begin_time)

s = ddb.session()
s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")
data=s.loadTable("share_table")
# s = ddb.session()
# s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")
# data = s.loadTable("symbol_data", "dfs://marketdata", memoryMode=True)

# print(data.select(['szWindCode','nActionDay','nTime','nOpen',
#                    'nHigh','nLow','nMatch','iVolume','iTurnover',
#                    'bar_close','S_DQ_PRECLOSE','S_DQ_ADJFACTOR',
#                    'HighLimit','LowLimit']).where("nActionDay=2020.06.19").toDF())  # 0.42  0.02

# print(data.select(['szWindCode','nActionDay','nTime','nOpen',
#                    'nHigh','nLow','nMatch','iVolume','iTurnover',
#                    'bar_close','S_DQ_PRECLOSE','S_DQ_ADJFACTOR',
#                    'HighLimit','LowLimit']).where("szWindCode=`000016.SZ").toDF())    # 0.50 0.098

# print(data.select(['szWindCode','nActionDay','nTime','nOpen',
#                    'nHigh','nLow','nMatch','iVolume','iTurnover',
#                    'bar_close','S_DQ_PRECLOSE','S_DQ_ADJFACTOR',
#                    'HighLimit','LowLimit']).where("nTime=93000000").toDF())  # 1.34 0.6

print(data.select(['szWindCode','nActionDay','nTime','nOpen',
                   'nHigh','nLow','nMatch','iVolume','iTurnover',
                   'bar_close','S_DQ_PRECLOSE','S_DQ_ADJFACTOR',
                   'HighLimit','LowLimit']).where("szWindCode=`000017.SZ").where("nTime=93000000").toDF())  # 0.05

end_time = datetime.datetime.now()
print(end_time)
print(end_time - begin_time)





