#!/usr/bin/env python
# coding=utf-8
import dolphindb as ddb
import datetime
import timeit

begin_time = datetime.datetime.now()

s = ddb.session()
s.connect(host="192.168.10.68", port=8848, userid="admin", password="123456")


# read by stock
# script = "select szWindCode, nActionDay, nTime, nOpen, nHigh, nLow, " \
#          "nMatch, iVolume, iTurnover, bar_close from " \
#          "loadTable('dfs://marketdata','symbol_data') where szWindCode = '000017.SZ'"

# read by date
# script = "select szWindCode, nActionDay, nTime, nOpen, nHigh, nLow, " \
#          "nMatch, iVolume, iTurnover, bar_close from " \
#          "loadTable('dfs://marketdata','symbol_data') where nActionDay  = 2020.06.15"

# read by time
# script = "select szWindCode, nActionDay, nTime, nOpen, nHigh, nLow, " \
#          "nMatch, iVolume, iTurnover, bar_close from " \
#          "loadTable('dfs://marketdata','symbol_data') where nTime   = 92500000"

# read by date and time
# script = "select szWindCode, nActionDay, nTime, nOpen, nHigh, nLow, " \
#          "nMatch, iVolume, iTurnover, bar_close from " \
#              "loadTable('dfs://marketdata','symbol_data') where nActionDay = 2020.07.15 and nTime = 92500000 "

# read by stock and time
# script = "select szWindCode, nActionDay, nTime, nOpen, nHigh, nLow, " \
#          "nMatch, iVolume, iTurnover, bar_close from " \
#              "loadTable('dfs://marketdata','symbol_data') where szWindCode = '600000.SH' and nTime = 92500000 "

# s.run("t=select szWindCode,nActionDay,nOpen from loadTable('dfs://marketdata','symbol_data') where nTime=103000000; open = panel(t.nActionDay, t.szWindCode, [t.nOpen])")
# print(s.run('open'))


# script="""
# t=select szWindCode,nActionDay,nOpen as open,bar_close as close from loadTable('dfs://marketdata','symbol_data')  where nTime=133000000
# open, close = panel(t.nActionDay, t.szWindCode, [t.open, t.close])
# open.setIndexedMatrix!()
# close.setIndexedMatrix!()
# close-open
# """
# re3 = s.run(script)
# print(re3)


# ref1 = s.run(script)
# print(ref1)

print(datetime.datetime.now() - begin_time)
