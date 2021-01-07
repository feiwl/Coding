import pandas as pd
import numpy as np
import pymongo
import dolphindb as ddb

# parser = argparse.ArgumentParser(description="start_date end_date")
# parser.add_argument('--start-date', default=True, help="start date")
# parser.add_argument('--end-date',required=False,help='end date')
# args = parser.parse_args()

myclient = pymongo.MongoClient("mongodb://192.168.10.68:27017")
mycollection = myclient["marketdata"]['transaction'].find({'nActionDay':{'$gte': '20201123','$lte': '20201126'}} ,{'_id':0, 'nActionDay':1, 'szWindCode':1, 'nTime': 1, 'nOpen':1})
df = pd.DataFrame(mycollection)

table = pd.pivot_table(df, values='nOpen', index=['nActionDay', 'nTime'],columns=['szWindCode'])

table.fillna(value=0, inplace=True)
table = table.astype("int")

table.reset_index(inplace=True)

table['nActionDay'] = table['nActionDay'].astype(np.datetime64)
print(table)

# s = ddb.session()
# s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")
# s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db="dfs://marketdata",tb="symbol_table"), table)
