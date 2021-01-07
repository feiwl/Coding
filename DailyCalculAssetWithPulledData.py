# coding: utf-8
# author: wuweizuo
# 盘后的一些日志的归类整理

import os
import datetime
import pexpect
import time
import re
import IndexReptiler

yyyymmdd = "{0:0>4}{1:0>2}{2:0>2}".format(datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day)
#yyyymmdd = "20200108"

# 输出所用的结构中的价格都是还原了的浮点数
class AssetDataOutput(object):
    def __init__(self):
        self.account = ""
        self.all_asset = 0.0
        self.all_pos_asset = 0.0
        self.self_all_useable_asset = 0.0
        self.fund_buy_amount = 0.0
        self.fund_buy_fee = 0.0
        self.fund_sell_amount = 0.0
        self.fund_sell_fee = 0.0
        self.ror = 0.0
        self.weekROR = 0.0
        self.tingpaiAsset = list()
        self.noMarketTicks = list()
        self.tingpaiAndDonotCalculedInPosAsset = list()

# 来自本地文件的数据中的所有涉及到价格的字段都是*100后的字符串类型
class AssetData(object):
    def __init__(self):
        self.tradingDay = ""
        self.ampm = ""
        self.all_asset = ""
        self.all_pos_asset = ""
        self.self_all_useable_asset = ""
        self.fund_buy_amount = ""
        self.fund_buy_fee = ""
        self.fund_sell_amount = ""
        self.fund_sell_fee = ""
        self.ror = ""
        self.tingpai = ""
        self.nomarket = ""
    def init(self, itemlist):
        self.tradingDay = itemlist[0]
        self.ampm = itemlist[1]
        self.all_asset = itemlist[2]
        self.all_pos_asset = itemlist[3]
        self.self_all_useable_asset = itemlist[4]
        self.fund_buy_amount = itemlist[5]
        self.fund_buy_fee = itemlist[6]
        self.fund_sell_amount = itemlist[7]
        self.fund_sell_fee = itemlist[8]
        self.ror = itemlist[9]
        self.tingpai = itemlist[10]
        self.nomarket = itemlist[11]

class DealedAssetData(object):
    def __init__(self):
        self.account = ""
        self.tradingDay = ""
        self.ampm = ""
        self.all_asset = 0.0
        self.ic = 0.0
        self.ih = 0.0

class PositionData(object):
    def __init__(self):
        self.tradingDay = ""
        self.ticker = ""
        self.exchange_id = ""
        self.total_qty = ""
        self.sellable_qty = ""
        self.avg_price = ""
        self.yesterday_position = ""
        self.last_price = ""
    def init(self, itemlist):
        self.tradingDay = itemlist[0]
        self.ticker = itemlist[1]
        self.exchange_id = itemlist[2]
        self.total_qty = itemlist[3]
        self.sellable_qty = itemlist[4]
        self.avg_price = itemlist[5]
        self.yesterday_position = itemlist[6]
        self.last_price = itemlist[7]

class TickData(object):
    def __init__(self):
        self.ticker = ""
        self.exchange_id = ""
        self.last_price = ""
        self.pre_close_price = ""
        self.open_price = ""
        self.high_price = ""
        self.low_price = ""
        self.close_price = ""
        self.pre_settl_price = ""
        self.settl_price = ""
        self.upper_limit_price = ""
        self.lower_limit_price = ""
        self.qty = ""
        self.turnover = ""
        self.bid0 = ""
        self.ask0 = ""
        self.bid_qty0 = ""
        self.ask_qty0 = ""
        self.trades_count = ""
        self.ticker_status = ""
    def init(self, itemlist):
        self.ticker = itemlist[0]
        self.exchange_id = itemlist[1]
        self.last_price = itemlist[2]
        self.pre_close_price = itemlist[3]
        self.open_price = itemlist[4]
        self.high_price = itemlist[5]
        self.low_price = itemlist[6]
        self.close_price = itemlist[7]
        self.pre_settl_price = itemlist[8]
        self.settl_price = itemlist[9]
        self.upper_limit_price = itemlist[10]
        self.lower_limit_price = itemlist[11]
        self.qty = itemlist[12]
        self.turnover = itemlist[13]
        self.bid0 = itemlist[14]
        self.ask0 = itemlist[15]
        self.bid_qty0 = itemlist[16]
        self.ask_qty0 = itemlist[17]
        self.trades_count = itemlist[18]
        self.ticker_status = itemlist[19]


class TingPaiInfo(object):
    def __init__(self):
        self.ticker = ""
        self.position = 0
        self.last_price = 0.0

def GetPosInfo(path):
    posDataDict = dict()
    with open(path, "r") as file:
        skipNum = 0
        for line in file:
            if 0 == skipNum:
                skipNum = skipNum + 1
                continue
            subItems = line.strip(" ").split(",")
            if 8 == len(subItems):
                posData = PositionData()
                posData.init(subItems)
                posDataDict[posData.ticker] = posData
    return posDataDict

def GetAssetInfo(path):
    assetData = AssetData()
    with open(path, "r") as file:
        skipNum = 0
        for line in file:
            if 0 == skipNum:
                skipNum = skipNum + 1
                continue
            subItems = line.strip(" ").split(",")
            if 12 == len(subItems):
                assetData.init(subItems)
                return assetData
    return assetData

def GetMarketInfo(path):
    tickDataDict = dict()
    with open(path, "r") as file:
        skipNum = 0
        for line in file:
            if 0 == skipNum:
                skipNum = skipNum + 1
                continue
            subItems = line.strip(" ").split(",")
            tickData = TickData()
            tickData.init(subItems)
            #print(tickData.ticker)
            tickDataDict[tickData.ticker] = tickData
    return tickDataDict

def GetDealedAssetFileInfo(path):
    dealedAssetDataDict = dict()
    with open(path, "r", encoding="utf-8") as file:
        tradingDay = ""
        ampm = ""
        IC = ""
        IH = ""
        regexMatchTradingDayAmPm = re.compile(r'(\d+)_(.*):.*?')
        regexMatchAccount = re.compile(r'(\d+?):.*?')
        regexMatchAllAsset = re.compile(r'allAsset=(.*?),\s*allPosAsset=(.*?),\s*selfAllUseableAsset=(.*?).*?')
        ICMATCH = re.compile(r'中证: (\d+.\d+).*?')
        IHMATCH = re.compile(r'上证: (\d+.\d+).*?')
        for line in file:
            # 20191220_am:
            reRes = regexMatchTradingDayAmPm.match(line)
            if reRes:
                tradingDay = reRes.groups()[0]
                ampm = reRes.groups()[1]
                continue

            # '中证: 6643.94, '上证: 3380.68
            ICvalue = ICMATCH.match(line)
            if ICvalue:
                IC = ICvalue.groups()[0]

            IHvalue = IHMATCH.match(line)
            if IHvalue:
                IH = IHvalue.groups()[0]

            # 105150004096:
            reRes = regexMatchAccount.match(line)
            if reRes:
                newAccount = DealedAssetData()
                newAccount.tradingDay = tradingDay
                newAccount.ic = IC
                newAccount.ih = IH
                newAccount.ampm = ampm
                newAccount.account = reRes.groups()[0]
                continue
            # allAsset=36657454.7, allPosAsset=35803131.0, selfAllUseableAsset=296371.7
            reRes = regexMatchAllAsset.match(line)
            if reRes:
                newAccount.all_asset = float(reRes.groups()[0])
                dealedAssetDataDict[newAccount.account] = newAccount

    return dealedAssetDataDict

def FindTingpaiLastPrice(ticker):
    subTickFiles = os.listdir("/home/prism/daily/marketdata")
    subTickFiles.sort()
    subTickFiles.reverse()
    lastTickFileName = ""
    findValidTingpaiLastPriceSign = False
    for tickFile in subTickFiles:
        #print("market=" + tickFile)
        currTickFilePath = "/home/prism/daily/marketdata/{}".format(tickFile)
        tickDataDict = GetMarketInfo(currTickFilePath)
        if tickDataDict is None:
            print("tickDataDict is None")
        if ticker in tickDataDict.keys():
            #print("ticker {} is in tickDataDict.keys".format(ticker))
            #print("tickDataDict[ticker].last_price)={}".format(tickDataDict[ticker].last_price))
            if int(tickDataDict[ticker].last_price) > 0:
                #print("tickDataDict[ticker].last_price)={}".format(tickDataDict[ticker].last_price))
                return (currTickFilePath, tickFile, tickDataDict)
    return (None, None, None)

def DailyCalculAmPmAssetWithPulledDataEachUser(userid, ampm):
    print('-----------------------------------------------------------------------')
    print("DailyCalculMorningAssetWithPulledDataEachUser deal {}".format(userid))
    srcDir = "/home/prism/daily"
    assetSrcDir = "/home/prism/daily/assetdata"
    assetSrcCurrDir = "/home/prism/daily/assetdata/{}".format(yyyymmdd)
    marketSrcDir = "/home/prism/daily/marketdata"
    positionSrcDir = "/home/prism/daily/positiondata"
    positionSrcCurrDir = "/home/prism/daily/positiondata/{}".format(yyyymmdd)
    outputDir = "/home/prism/daily/python/output"
    if "am" == ampm:
        assetFilePath = "{}/{}/assetdata_{}_{}_am.txt".format(assetSrcDir, yyyymmdd, userid, yyyymmdd)
    else:
        assetFilePath = "{}/{}/assetdata_{}_{}_pm.txt".format(assetSrcDir, yyyymmdd, userid, yyyymmdd)
    posFilePath = "{}/{}/positiondata_{}_{}.txt".format(positionSrcCurrDir, yyyymmdd, userid, yyyymmdd)

    # 找昨天盘后的原始资金信息，找到其中的持仓金额
    print("find last pm asset info start")
    subAssetDirs = os.listdir("{}".format(assetSrcDir))
    subAssetDirs.sort()
    subAssetDirs.reverse()
    lastPmAssetDirName = ""
    for assetDir in subAssetDirs:
        print("assetDir=" + assetDir)
        if int(assetDir) < int(yyyymmdd):
            lastPmAssetDirName = assetDir
            break
    lastDownedPmAssetFileName = "assetdata_{}_{}_pm.txt".format(userid, lastPmAssetDirName)
    lastDownedPmAssetFilePath = "/home/prism/daily/assetdata/{}/{}".format(lastPmAssetDirName, lastDownedPmAssetFileName)
    print("find last pm asset info end, lastDownedPmAssetFileName=" + lastDownedPmAssetFileName)
    # 读取上一天的盘后的资金文件
    lastDownedPmAssetData = GetAssetInfo(lastDownedPmAssetFilePath)

    # 读取今天的资金文件
    currDownedAmAssetData = GetAssetInfo(assetFilePath)     # 读今天拿到的资金文件
    print("tingpailist: {}".format(currDownedAmAssetData.tingpai))

    # 找昨天盘后的持仓信息，早晨的资金计算需要
    print("find last pm pos info start")
    subPosDirs = os.listdir("{}".format(positionSrcDir))
    subPosDirs.sort()
    subPosDirs.reverse()
    lastPosDirName = ""
    for posDir in subPosDirs:
        print("dir=" + posDir)
        if int(posDir) < int(yyyymmdd):
            lastPosDirName = posDir
            break
    if "" == lastPosDirName:
        print("cannot find last pos dir, return")
        return
    lastDownedPosDirPath = "{}/{}".format(positionSrcDir, lastPosDirName)
    lastDownedPosFilePath = "{}/positiondata_{}_{}.txt".format(lastDownedPosDirPath, userid, lastPosDirName)
    print("find last pm pos info end, lastDownedPosDirPath=" + lastDownedPosDirPath + ", lastDownedPosFilePath=" + lastDownedPosFilePath)
    # 计算停牌的股票需要用昨天收盘后的持仓*最后有效的lastprice
    # 读昨天的持仓文件
    lastDownedPosFileDataDict = GetPosInfo(lastDownedPosFilePath)

    # 早上的计算中，资金 = 昨天持仓金额 + 今天查到的可用资金 + 停牌的资金
    if "am" == ampm:
        print('am')
        # 上一天盘后的资金数据，转为为输出所用的价格类型
        lastCalculedPmDataOutput = AssetDataOutput()
        lastCalculedPmDataOutput.all_pos_asset = 0
        noMarketTicks = list()
        if True:  # 计算all_pos_asset
            # 早晨的统计中，和今天停牌的股票没有关系，只是输出用来提醒
            # 统计昨天收盘后的持仓资金，这里面要考虑停牌的是否有价格
            donotCalculedInPosAssetGupiaoList = list()
            for key in lastDownedPosFileDataDict.keys():
                if int(lastDownedPosFileDataDict[key].last_price) <= 0:
                    donotCalculedInPosAssetGupiaoList.append(key)
                else:
                    realLastPrice = round(float(lastDownedPosFileDataDict[key].last_price) / 100, 2)
                    lastCalculedPmDataOutput.all_pos_asset = lastCalculedPmDataOutput.all_pos_asset + realLastPrice * int(lastDownedPosFileDataDict[key].total_qty)
            # 计算昨天收盘后停牌的那些票的持仓资金，价格为0的股票认为是停牌
            for singleTingpaiGu in donotCalculedInPosAssetGupiaoList:
                # 找停牌股票最后一次出现的lastprice
                (currTickFilePath, tickFile, tickDataDict) = FindTingpaiLastPrice(singleTingpaiGu)
                print("tingpai: " + singleTingpaiGu)
                if tickDataDict:
                    print(tickDataDict[singleTingpaiGu].last_price)
                    realLastPrice = round(float(tickDataDict[singleTingpaiGu].last_price) / 100, 2)
                    lastCalculedPmDataOutput.all_pos_asset = lastCalculedPmDataOutput.all_pos_asset + realLastPrice * int(lastDownedPosFileDataDict[singleTingpaiGu].total_qty)
                else:
                    noMarketTicks.append(singleTingpaiGu)

        # 检查今天的持仓中有没有停牌的股票，检查到了后，对于早盘的输出来说，只是输出提醒
        print("check whether has tingpai start")
        currDayTingPaiInfoList = list()
        if "" != currDownedAmAssetData.tingpai:
            tingpaiGus = currDownedAmAssetData.tingpai.strip(" ").split("|")
            for singleTingpaiGu in tingpaiGus:
                # 找停牌股票最后一次出现的lastprice
                (currTickFilePath, tickFile, tickDataDict) = FindTingpaiLastPrice(singleTingpaiGu)
                print("tingpai: " + singleTingpaiGu)
                print(tickDataDict[singleTingpaiGu].last_price)

                tingpai = TingPaiInfo()
                tingpai.ticker = singleTingpaiGu
                tingpai.position = int(lastDownedPosFileDataDict[singleTingpaiGu].total_qty)
                tingpai.last_price = float(tickDataDict[singleTingpaiGu].last_price)
                currDayTingPaiInfoList.append(tingpai)
        print("check whether has tingpai end")

        amAssetDataOutput = AssetDataOutput()
        amAssetDataOutput.account = userid
        amAssetDataOutput.all_pos_asset = round(lastCalculedPmDataOutput.all_pos_asset, 1)
        amAssetDataOutput.self_all_useable_asset = round(float(currDownedAmAssetData.self_all_useable_asset) / 100, 1)
        amAssetDataOutput.fund_buy_amount = round(float(currDownedAmAssetData.fund_buy_amount) / 100, 1)
        amAssetDataOutput.fund_buy_fee = round(float(currDownedAmAssetData.fund_buy_fee) / 100, 1)
        amAssetDataOutput.fund_sell_amount = round(float(currDownedAmAssetData.fund_sell_amount) / 100, 1)
        amAssetDataOutput.fund_sell_fee = round(float(currDownedAmAssetData.fund_sell_fee) / 100, 1)
        amAssetDataOutput.ror = 0
        amAssetDataOutput.tingpaiAsset = currDayTingPaiInfoList
        amAssetDataOutput.noMarketTicks = noMarketTicks
        amAssetDataOutput.all_asset = round(float(amAssetDataOutput.all_pos_asset) + float(amAssetDataOutput.self_all_useable_asset), 1)
        return amAssetDataOutput
    else:
        print('pm')
        # 判断今天的持仓中有没有停牌的股票
        print("check whether has tingpai start")
        print("currDownedAmAssetData.tingpai={}".format(currDownedAmAssetData.tingpai))
        tingPaiInfoList = list()
        if "" != currDownedAmAssetData.tingpai:
            tingpaiGus = currDownedAmAssetData.tingpai.strip(" ").split("|")
            for singleTingpaiGu in tingpaiGus:
                # 找停牌股票最后一次出现的lastprice
                (currTickFilePath, tickFile, tickDataDict) = FindTingpaiLastPrice(singleTingpaiGu)
                print("tingpai: " + singleTingpaiGu)
                print(tickDataDict[singleTingpaiGu].last_price)

                tingpai = TingPaiInfo()
                tingpai.ticker = singleTingpaiGu
                tingpai.position = int(lastDownedPosFileDataDict[singleTingpaiGu].total_qty)
                tingpai.last_price = float(tickDataDict[singleTingpaiGu].last_price)
                tingPaiInfoList.append(tingpai)

        time.sleep(1)
        print("show tingPaiInfoList start")
        print(tingPaiInfoList)
        print("show tingPaiInfoList end")
        time.sleep(1)

        # 盘后的计算中，资金 = 昨天持仓金额 + 今天查到的可用资金 + 停牌的资金
        currCalculedPmDataOutput = AssetDataOutput()
        currCalculedPmDataOutput.account = userid
        currCalculedPmDataOutput.all_asset = 0
        currCalculedPmDataOutput.all_pos_asset = 0
        currCalculedPmDataOutput.self_all_useable_asset = round(float(currDownedAmAssetData.self_all_useable_asset) / 100, 1)
        currCalculedPmDataOutput.fund_buy_amount = round(float(currDownedAmAssetData.fund_buy_amount) / 100, 1)
        currCalculedPmDataOutput.fund_buy_fee = round(float(currDownedAmAssetData.fund_buy_fee) / 100, 1)
        currCalculedPmDataOutput.fund_sell_amount = round(float(currDownedAmAssetData.fund_sell_amount) / 100, 1)
        currCalculedPmDataOutput.fund_sell_fee = round(float(currDownedAmAssetData.fund_sell_fee) / 100, 1)
        currCalculedPmDataOutput.tingpaiAsset = tingPaiInfoList
        noMarketTicks = list()
        if True: # 计算all_pos_asset
            # 读今天的持仓文件
            currDownedPosFilePath = "{}/{}/positiondata_{}_{}.txt".format(positionSrcDir, yyyymmdd, userid, yyyymmdd)
            currDownedPosFileDataDict = GetPosInfo(currDownedPosFilePath)

            donotCalculedInPosAssetGupiaoList = list()
            for key in currDownedPosFileDataDict.keys():
                if int(currDownedPosFileDataDict[key].last_price) <= 0:
                    donotCalculedInPosAssetGupiaoList.append(key)
                else:
                    realLastPrice = round(float(currDownedPosFileDataDict[key].last_price) / 100, 2)
                    currCalculedPmDataOutput.all_pos_asset = currCalculedPmDataOutput.all_pos_asset + realLastPrice * int(currDownedPosFileDataDict[key].total_qty)
            print("currCalculedPmDataOutput.all_pos_asset before add tingpai asset is={}".format(currCalculedPmDataOutput.all_pos_asset))
            tingpaiPosAsset = 0
            for tingpai in donotCalculedInPosAssetGupiaoList:
                findTingPaiSign = False
                for item in tingPaiInfoList:
                    if tingpai == item.ticker:
                        print("tingpai == item.ticker, tingpai={}, item.ticker={}".format(tingpai, item.ticker))
                        findTingPaiSign = True
                        realLastPrice = round(float(item.last_price) / 100, 2)
                        tingpaiPosAsset = tingpaiPosAsset + realLastPrice * item.position
                if not findTingPaiSign:
                    '''
                    # 没有在停牌的股票列表中找到，再从行情落地列表文件中依次查找该股票行情
                    (currTickFilePath, tickFile, tickDataDict) = FindTingpaiLastPrice(tingpai)
                    print("canot find gupiao in tingPaiInfoList: " + tingpai)
                    if tickDataDict:
                        print(tickDataDict[tingpai].last_price)
                        realLastPrice = round(float(tickDataDict[tingpai].last_price) / 100, 2)
                        lastCalculedPmDataOutput.all_pos_asset = lastCalculedPmDataOutput.all_pos_asset + realLastPrice * int(
                            lastDownedPosFileDataDict[singleTingpaiGu].total_qty)
                    else:
                        noMarketTicks.append(singleTingpaiGu)
                    '''
                    print("error: cannot find tingpai info: ticker={}".format(tingpai))
            currCalculedPmDataOutput.all_pos_asset = currCalculedPmDataOutput.all_pos_asset + tingpaiPosAsset
            print("currCalculedPmDataOutput.all_pos_asset after add tingpai asset is={}".format(currCalculedPmDataOutput.all_pos_asset))
        # 计算总资金
        currCalculedPmDataOutput.all_pos_asset = round(currCalculedPmDataOutput.all_pos_asset, 1)
        currCalculedPmDataOutput.self_all_useable_asset = round(currCalculedPmDataOutput.self_all_useable_asset, 1)
        currCalculedPmDataOutput.all_asset = currCalculedPmDataOutput.all_pos_asset + currCalculedPmDataOutput.self_all_useable_asset
        currCalculedPmDataOutput.all_asset = round(currCalculedPmDataOutput.all_asset, 1)

        # 查找昨天盘后的处理后的资金数据
        lastCalculedPmAssetDataOutput = None
        if True: # 计算昨天计算后的资金lastCalculedPmAssetDataOutput
            print("find last dealed pm asset info start")
            pythonOutputDirList = os.listdir("/home/prism/daily/python/output")
            pythonOutputDirList.sort()
            pythonOutputDirList.reverse()
            lastCalculedPmAssetFileName = ""
            for dealFile in pythonOutputDirList:
                print("dealFile=" + dealFile)
                reRes = re.match(r'(.*?)_pm.*?', dealFile)
                if reRes:
                    if reRes.groups()[0] < yyyymmdd:
                        lastCalculedPmAssetFileName = dealFile
                        break
            lastCalculedPmAssetFilePath = "/home/prism/daily/python/output/{}".format(lastCalculedPmAssetFileName)
            print("find last pm asset info end, lastCalculedPmAssetFilePath=" + lastCalculedPmAssetFilePath)
            lastCalculedPmAssetDataOutput = GetDealedAssetFileInfo(lastCalculedPmAssetFilePath)

        # 查找上周最后工作日的资金数据
        if True:
            path = '/home/prism/daily/python/output'
            path_files = os.listdir(path)
            path_files.sort()
            path_files.reverse()
            Last_working_day_of_last_week_file = ""
            calculation_time = int((datetime.datetime.now().date() - datetime.timedelta(
                days=int(datetime.date.strftime(datetime.datetime.now(), "%w")))).strftime("%Y%m%d"))
            for file in path_files:
                Rematch = re.match("\d+", file)
                if Rematch:
                    if int(Rematch.group()) < calculation_time:
                        Last_working_day_of_last_week_file = file
                        break
            Last_working_day_of_last_week_FilePath = "/home/prism/daily/python/output/{}".format(Last_working_day_of_last_week_file)
            Last_working_day_of_last_week_Output = GetDealedAssetFileInfo(Last_working_day_of_last_week_FilePath)

        print("all_asset={}, all_pos_asset={}, lastCalculedPmAssetDataOutput[{}].all_asset={}".format(currCalculedPmDataOutput.all_asset, currCalculedPmDataOutput.all_pos_asset, userid, lastCalculedPmAssetDataOutput[userid].all_asset))
        currCalculedPmDataOutput.ror = round((currCalculedPmDataOutput.all_asset / lastCalculedPmAssetDataOutput[userid].all_asset - 1) * 100, 3)
        currCalculedPmDataOutput.weekROR = round((currCalculedPmDataOutput.all_asset / Last_working_day_of_last_week_Output[userid].all_asset - 1) * 100, 3)
        currCalculedPmDataOutput.weekIC = Last_working_day_of_last_week_Output[userid].ic
        currCalculedPmDataOutput.weekIH = Last_working_day_of_last_week_Output[userid].ih

        print(currCalculedPmDataOutput.ror)
        print(currCalculedPmDataOutput.weekROR)
        return currCalculedPmDataOutput

def CalculEntry():
    useridList = ["109292000175", "109292000176", "105150004096", "205150000346", "105150004289", "105150004295", "105150004427", "105150004617", "105150004296"]
    allAccountAssetDataOutput = list()
    if time.localtime().tm_hour <= 11:
        for userid in useridList:
            assetOutput = DailyCalculAmPmAssetWithPulledDataEachUser(userid, "am")
            allAccountAssetDataOutput.append(assetOutput)

        outputList = list()
        outputList.append("{}_am:".format(yyyymmdd))
        isFirstItem = True
        for account in allAccountAssetDataOutput:
            if isFirstItem:
                isFirstItem = False
            else:
                outputList.append("")
            outputList.append("{}:".format(account.account))
            outputList.append(
                "allAsset={}, allPosAsset={}, selfAllUseableAsset={}".format(account.all_asset, account.all_pos_asset,
                                                                             account.self_all_useable_asset))
            # outputList.append("fund_buy_amount:{}, fund_buy_fee={}, fund_sell_amount:{}, fund_sell_fee={}".format(account.fund_buy_amount, account.fund_buy_fee, account.fund_sell_amount, account.fund_sell_fee))
            if len(account.tingpaiAsset) > 0:
                tingpaiLine = "停牌:"
                for tingpai in account.tingpaiAsset:
                    tingpaiLine = tingpaiLine + " " + tingpai.ticker
                outputList.append(tingpaiLine)

        for line in outputList:
            print(line)
        outputFilePath = "/home/prism/daily/python/output/{}_am.txt".format(yyyymmdd)
        with open(outputFilePath, "w") as file:
            file.writelines([line + '\n' for line in outputList])


    else:
        for userid in useridList:
            assetOutput = DailyCalculAmPmAssetWithPulledDataEachUser(userid, "pm")
            allAccountAssetDataOutput.append(assetOutput)

        rate399905 = IndexReptiler.Get399905Rate()[1]
        rate000001 = IndexReptiler.Get000001Rate()[1]

        ihValue = IndexReptiler.Get399905Rate()[0]
        icValue = IndexReptiler.Get000001Rate()[0]

        outputList = list()
        outputList.append("{}_pm:".format(yyyymmdd))
        outputList.append("中证: {}, {}".format(ihValue,rate399905))
        outputList.append("上证: {}, {}".format(icValue,rate000001))
        isFirstItem = True
        for account in allAccountAssetDataOutput:
            if isFirstItem:
                isFirstItem = False
            else:
                outputList.append("")
            # carIC 中证超额收益  carIH 上证超额收益
            carIC = f"{round(account.ror - float(rate399905.replace('%','')),3)}%"
            carIH = f"{round(account.ror - float(rate000001.replace('%','')),3)}%"
            #carIC = rate399905
            #carIH = account.ror           

            weekIC = round((float(icValue) / float(account.weekIC) - 1) * 100, 3)
            weekIH = round((float(ihValue) / float(account.weekIH) - 1) * 100, 3)
            weekCarIC = account.weekROR - weekIC
            weekCarIH = account.weekROR - weekIH

            outputList.append("{}:".format(account.account))
            outputList.append(
                "allAsset={}, allPosAsset={}, selfAllUseableAsset={}".format(account.all_asset, account.all_pos_asset,
                                                                             account.self_all_useable_asset))
            outputList.append("fund_buy_amount:{}, fund_buy_fee={}, fund_sell_amount:{}, fund_sell_fee={}".format(
                account.fund_buy_amount, account.fund_buy_fee, account.fund_sell_amount, account.fund_sell_fee))
            outputList.append("RoR: {}%".format(account.ror))
            outputList.append("carIC: {}, carIH: {}".format(carIC,carIH))
            outputList.append("weekRoR: {}%, weekCarIC: {}%, weekCarIH: {}%".format(account.weekROR,weekCarIC, weekCarIH))
            if len(account.tingpaiAsset) > 0:
                tingpaiLine = "停牌:"
                for tingpai in account.tingpaiAsset:
                    tingpaiLine = tingpaiLine + " " + tingpai.ticker
                outputList.append(tingpaiLine)

        for line in outputList:
            print(line)
        outputFilePath = "/home/prism/daily/python/output/{}_pm.txt".format(yyyymmdd)
        with open(outputFilePath, "w") as file:
            file.writelines([line + '\n' for line in outputList])

if __name__ == "__main__":
    CalculEntry()
