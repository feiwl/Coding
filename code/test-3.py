import dolphindb as ddb
import os
import pandas as pd
import time
import argparse

def generate_insert_args(filename):
    szWindCode=list()
    nActionDay=list()
    nTime=list()
    nOpen=list()
    nHigh=list()
    nLow=list()
    nMatch=list()
    iVolume=list()
    iTurnover=list()
    bar_close=list()
    S_DQ_PRECLOSE=list()
    S_DQ_ADJFACTOR=list()
    HighLimit=list()
    LowLimit=list()
    df = pd.read_hdf(filename)
    for index, row in df.iterrows():
        szWindCode.append(row['szWindCode'])
        nActionDay.append(time.strftime("%Y.%m.%d",time.strptime(row['nActionDay'], "%Y%m%d")))
        nTime.append(row['nTime'])
        nOpen.append(row['nOpen'])
        nHigh.append(row['nHigh'])
        nLow.append(row['nLow'])
        nMatch.append(row['nMatch'])
        iVolume.append(row['iVolume'])
        iTurnover.append(row['iTurnover'])
        bar_close.append(str(row['bar_close']).replace("True",'true'))
        S_DQ_PRECLOSE.append(row['S_DQ_PRECLOSE'])
        S_DQ_ADJFACTOR.append(row['S_DQ_ADJFACTOR'])
        HighLimit.append(row['HighLimit'])
        LowLimit.append(row['LowLimit'])
    return  szWindCode,nActionDay,nTime,nOpen,nHigh,nLow,nMatch,iVolume,iTurnover,bar_close,S_DQ_PRECLOSE,S_DQ_ADJFACTOR,HighLimit,LowLimit

def create_makrtdatabase():
   s = ddb.session()
   s.connect(host="localhost", port=8848, userid="admin", password="123456")
   if not s.existsDatabase("dfs://marketdata"):
        s.run("valuep = database(, VALUE, 2016.01.01..2023.12.31)")
        s.run("""symbol = database(, HASH, [SYMBOL, 20])""")
        s.run("""symbol_data = database("dfs://marketdata", COMPO, [valuep, symbol])""")
        columns = """`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit"""
        types = """[SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,BOOL,INT,DOUBLE,INT,INT]"""

        s.run("""table_new = symbol_data.createPartitionedTable(table(10:0, {cols}, {types}), `symbol_data,
                  `nActionDay`szWindCode)""".format(cols=columns, types=types))
        print("NEW TABLE CREATED....")

        return s
   else:
        return s
        # s.dropDatabase("dfs://marketdata")
        # print("Drop database ....")

def insert_marketdatabase(path,connection,share_table,table_name):
    script = """ {table_name} = table(1000000:0,`szWindCode`nActionDay`nTime`nOpen`nHigh`nLow`nMatch`iVolume`iTurnover`bar_close`S_DQ_PRECLOSE`S_DQ_ADJFACTOR`HighLimit`LowLimit,[SYMBOL,DATE,INT,INT,INT,INT,INT,INT,INT,BOOL,INT,DOUBLE,INT,INT])
    share {table_name} as {share_table}}""".format(table_name=table_name,share_table=share_table)
    connection.run(script)

    for file in os.listdir(path):
        i = generate_insert_args(os.path.join(path,file))
        connection.upload({'szWindCode': i[0], "nActionDay":i[1], "nTime": i[2], "nOpen": i[3],
                  "nHigh": i[4], "nLow": i[5], "nMatch": i[6], "iVolume": i[7], "iTurnover": i[8],
                  "bar_close": i[9], "S_DQ_PRECLOSE": i[10], "S_DQ_ADJFACTOR": i[11], "HighLimit": i[12], "LowLimit": i[13]})

        script = "insert into {share_table} values(szWindCode,date(nActionDay),nTime,nOpen,nHigh,nLow,nMatch,iVolume,iTurnover,bool(bar_close),S_DQ_PRECLOSE,S_DQ_ADJFACTOR,HighLimit,LowLimit);".format(share_table=share_table)
        connection.run(script)

    data=connection.loadTable("{share_table}".format(share_table=share_table)).toDF()
    connection.run("append!{{loadTable('{db}', `{tb})}}".format(db='dfs://marketdata',tb='symbol_data'),data )
    connection.undef('{share_table}'.format(share_table=share_table),'SHARED')
    connection.clearAllCache()

parser = argparse.ArgumentParser(description="Insert_Marketdata")
parser.add_argument('--day-path', required=True, help='day path data')
parser.add_argument('--share-table', required=True, help='memery share table')
parser.add_argument('--table-name', required=True, help='table name')
args = parser.parse_args()

conn = create_makrtdatabase()
insert_marketdatabase(args.day_path,conn,args.share_table,args.table_name)



