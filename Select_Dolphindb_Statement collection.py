import dolphindb as ddb
import pymongo
import datetime

s = ddb.session()
s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")
myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
mydb = myclient["marketdata"]
mycol = mydb["transaction"]
all_symbol_name = mycol.distinct("szWindCode")
all_symbol_name = list(map(lambda sym: sym.split('.')[1]+sym.split('.')[0] ,all_symbol_name))
all_symbol_name.extend(["nActionDay"])
all_symbol_name.reverse()

all_symbol_name.remove('SZ003004')

delta = datetime.datetime.now()
print(delta)
trade = s.loadTable("symbol_table", "dfs://marketdata")

df = trade.select(all_symbol_name).where("nTime=132300000")

print('import done')
df = df.toDF()

print(df)

end_date = datetime.datetime.now()
print(end_date - delta)
