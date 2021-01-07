import re
#
# TIME = '(?P<t>\d+)'
# SEQ = '(?P<seq>\d+)'
# SET = '(?P<set>asset)'
# BUYING = '(?P<buying>\d+)'
# FBA = '(?P<fba>\d+)'
# FBF = '(?P<fbf>\d+)'
# FSA = '(?P<fsa>\d+)'
# FSF = '(?P<fsf>\d+)'
# WDA = '(?P<wda>\d+)'
# TCK = '(?P<tck>\d+)'
# MARK = '(?P<mark>sh|sz)'
# TOTAL = '(?P<total>\d+)'
# SAQ = '(?P<saq>\d+)'
# APC = '(?P<apc>\d+)'
# YDP = '(?P<ydp>\d+)'
# AAT = '(?P<aat>\d+)'
# RAT = '(?P<rat>\d+)'
# UQD = '(?P<uqd>\d+)'
# PIE = '(?P<pie>\d+)'
# VLE = '(?P<vle>\d+)'
# DCN = '(?P<vle>\d+)'
# TDP = '(?P<vle>\d+)'
# TVE = '(?P<vle>\d+)'
#
# #time:105240616,sequenceId:1,query:asset,buyingPower:306807615,fundBuyAmount:0,fundBuyFee:0,fundSellAmount:244639300,fundSellFee:318031,withholdingAmount:0;
# QUERY_ASSET = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',query:' + SET +
#                          ',buyingPower:' + BUYING + ',fundBuyAmount:' + FBA + ',fundBuyFee:'
#                          + FBF + ',fundSellAmount:' + FSA + ',fundSellFee:' + FSF + ',withholdingAmount:' + WDA + ';')
#
# #time:105240616,sequenceId:2,query:position,offset:start;
# QUERY_POSITION_END = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',query:position,offset:end;')
#
# #time:105240616,sequenceId:3,query:position,ticker:600000,market:sh,totalQty:12000,sellableQty:12000,avgPrice:125000,yesterdayPosition:12000;
# QUERY_POSITION = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',query:position' + ',ticker:' +
#                             TCK + ',market:' + MARK + ',totalQty:' + TOTAL + ',sellableQty:' + SAQ + ',avgPrice:'
#                             + APC + ',yesterdayPosition:' + YDP + ';')
#
# #time:105240616,sequenceId:6,addAmount:10000000;
# ADDAMOUNT = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',addAmount:' + AAT + ';')
#
# #time:105240616,sequenceId:7,reduceAmount:2000000;
# REDUCEAMOUNT = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',reduceAmount:' + RAT + ';')
#
# #time:105240616,sequenceId:8,order:accepted,uniqueId:150212155112,ticker:600000,market:sh,price:56500,volume:100,direction:1;
# ORDERACCEPTED = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',order:accepted' + ',uniqueId:' + UQD + ',ticker:' + TCK + ',market:' + MARK + ',price:' + PIE + ',volume:' + VLE + ',direction:' + DCN + ';')
#
# #time:105240616,sequenceId:9,order:rejected,uniqueId:150212155113,ticker:600000,market:sh,price:0,volume:100,direction:1;
# ORDERREJECTED = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',order:rejected' + ',uniqueId:' + UQD + ',ticker:' + TCK + ',market:' + MARK + ',price:' + PIE + ',volume:' + VLE + ',direction:' + DCN + ';')
#
# #time:105240616,sequenceId:10,order:cancelAccepted,uniqueId:150212155113;
# ORDERCANCELACCEPTED = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',order:cancelAccepted,' + 'uniqueId:' + UQD + ';')
#
# #time:105240616,sequenceId:11,order:cancelRejected,uniqueId:150212155113;
# ORDERCANCELREJECTED = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',order:cancelRejected,' + 'uniqueId:' + UQD + ';')
#
# #time:105240616,sequenceId:12,order:canceled,uniqueId:150212155113;
# ORDERCAMCLED = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',order:canceled,' + 'uniqueId:' + UQD + ';')
#
# #time:105240616,sequenceId:13,order:traded,uniqueId:150212155113,ticker:300050,market:sz,tradePrice:56500,tradeVolume:100,direction:0;
# ORDERTRADED = re.compile('time:' + TIME + ',sequenceId:' + SEQ + ',order:traded,' + 'uniqueId:' + UQD + ',ticker:' + TCK + ',market:' + MARK + ',tradePrice:' + TDP + ',tradeVolume:' + TVE + ',direction:' + DCN + ';')


import socket
import datetime
from concurrent import futures

# def blocking_way():
#     sock = socket.socket()
#     sock.connect(('example.com', 80))
#     request = 'GET / HTTP/1.0\r\nHost: example.com\r\n\r\n'
#     sock.send(request.encode('ascii'))
#     response = b''
#     chunk = sock.recv(4096)
#     while chunk:
#         response += chunk
#         chunk = sock.recv(4096)
#     print(response)
#     return response
#
# def sync_way():
#     start = datetime.datetime.now()
#     res = []
#     for i in range(10):
#         res.append(blocking_way())
#     duration = datetime.datetime.now() - start
#     return len(res),duration.total_seconds()
#
# print(sync_way())

def blocking_way():
    sock = socket.socket()
    sock.connect(('example.com', 80))
    request = 'GET / HTTP/1.0\r\nHost: example.com\r\n\r\n'
    sock.send(request.encode('ascii'))
    response = b''
    chunk = sock.recv(4096)
    while chunk:
        response += chunk
        chunk = sock.recv(4096)
    print(response)
    return response

# def process_way():
#     workers = 10
#     start = datetime.datetime.now()
#     with futures.ProcessPoolExecutor(workers) as executor:
#         futs = {executor.submit(blocking_way) for i in range(10)}
#         print(futs)
#     duration = (datetime.datetime.now() - start).total_seconds()
#     return len([fut.result() for fut in futs]),duration
# print(process_way())

def thread_way():
    workers = 10
    start = datetime.datetime.now()
    with futures.ThreadPoolExecutor(workers) as executor:
        futs = {executor.submit(blocking_way) for i in range(10)}
    duration = (datetime.datetime.now() - start).total_seconds()
    return len([fut.result() for fut in futs]),duration
print(thread_way())