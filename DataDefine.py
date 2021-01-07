# coding: utf-8
# author: wuweizuo
# 数据采集程序输出要用的通讯接口

import os

class QueryAsset(object):
    def __init__(self):
        self.userId = ""
        self.sequenceId = 0
        self.buyingPower = 0
        self.fundBuyAmount = 0
        self.fundBuyFee = 0
        self.fundSellAmount = 0
        self.fundSellFee = 0
        self.withholdingAmount = 0

    def Print(self):
        print("QueryAsset----------------")
        print("userId: {}".format(self.userId))
        print("sequenceId: {}".format(self.sequenceId))
        print("buyingPower: {}".format(self.buyingPower))
        print("fundBuyAmount: {}".format(self.fundBuyAmount))
        print("fundBuyFee: {}".format(self.fundBuyFee))
        print("fundSellAmount: {}".format(self.fundSellAmount))
        print("fundSellFee: {}".format(self.fundSellFee))
        print("withholdingAmount: {}".format(self.withholdingAmount))
        print("QueryAsset================")


class QueryPosition(object):
    def __init__(self):
        self.sequenceId = 0
        self.ticker = ""
        self.market = ""
        self.totalQty = 0
        self.sellableQty = 0
        self.avgPrice = 0
        self.yesterdayPosition = 0

    def Print(self):
        print("QueryPosition----------------")
        print("sequenceId: {}".format(self.sequenceId))
        print("ticker: {}".format(self.ticker))
        print("market: {}".format(self.market))
        print("totalQty: {}".format(self.totalQty))
        print("sellableQty: {}".format(self.sellableQty))
        print("avgPrice: {}".format(self.avgPrice))
        print("yesterdayPosition: {}".format(self.yesterdayPosition))
        print("QueryPosition================")

class QueryPositions(object):
    def __init__(self):
        self.userId = ""
        self.positions = list()

    def Print(self):
        print("QueryPositions----------------")
        print("userId: {}".format(self.userId))
        for pos in self.positions:
            pos.Print()
        print("QueryPositions================")

class OrderAccepted(object):
    def __init__(self):
        self.userId = ""
        self.sequenceId = 0
        self.uniqueId = ""
        self.ticker = ""
        self.market = ""
        self.price = 0
        self.volume = 0
        self.direction = 0

    def Print(self):
        print("OrderAccepted----------------")
        print("userId: {}".format(self.userId))
        print("sequenceId: {}".format(self.sequenceId))
        print("uniqueId: {}".format(self.uniqueId))
        print("ticker: {}".format(self.ticker))
        print("market: {}".format(self.market))
        print("price: {}".format(self.price))
        print("volume: {}".format(self.volume))
        print("direction: {}".format(self.direction))
        print("OrderAccepted================")

class OrderRejected(object):
    def __init__(self):
        self.userId = ""
        self.sequenceId = 0
        self.uniqueId = ""
        self.ticker = ""
        self.market = ""
        self.price = 0
        self.volume = 0
        self.direction = 0

    def Print(self):
        print("OrderRejected----------------")
        print("userId: {}".format(self.userId))
        print("sequenceId: {}".format(self.sequenceId))
        print("uniqueId: {}".format(self.uniqueId))
        print("ticker: {}".format(self.ticker))
        print("market: {}".format(self.market))
        print("price: {}".format(self.price))
        print("volume: {}".format(self.volume))
        print("direction: {}".format(self.direction))
        print("OrderRejected================")

class OrderCancelAccepted(object):
    def __init__(self):
        self.userId = ""
        self.sequenceId = 0
        self.uniqueId = ""

    def Print(self):
        print("OrderCancelAccepted----------------")
        print("userId: {}".format(self.userId))
        print("sequenceId: {}".format(self.sequenceId))
        print("uniqueId: {}".format(self.uniqueId))
        print("OrderCancelAccepted================")

class OrderCancelRejected(object):
    def __init__(self):
        self.userId = ""
        self.sequenceId = 0
        self.uniqueId = ""

    def Print(self):
        print("OrderCancelRejected----------------")
        print("userId: {}".format(self.userId))
        print("sequenceId: {}".format(self.sequenceId))
        print("uniqueId: {}".format(self.uniqueId))
        print("OrderCancelRejected================")

class OrderCanceled(object):
    def __init__(self):
        self.userId = ""
        self.sequenceId = 0
        self.uniqueId = ""

    def Print(self):
        print("OrderCanceled----------------")
        print("userId: {}".format(self.userId))
        print("sequenceId: {}".format(self.sequenceId))
        print("uniqueId: {}".format(self.uniqueId))
        print("OrderCanceled================")

class OrderTraded(object):
    def __init__(self):
        self.userId = ""
        self.sequenceId = 0
        self.uniqueId = ""
        self.ticker = ""
        self.market = ""
        self.tradePrice = 0
        self.tradeVolume = 0
        self.direction = 0

    def Print(self):
        print("OrderTraded----------------")
        print("userId: {}".format(self.userId))
        print("sequenceId: {}".format(self.sequenceId))
        print("uniqueId: {}".format(self.uniqueId))
        print("ticker: {}".format(self.ticker))
        print("market: {}".format(self.market))
        print("tradePrice: {}".format(self.tradePrice))
        print("tradeVolume: {}".format(self.tradeVolume))
        print("direction: {}".format(self.direction))
        print("OrderTraded================")

class AddAmount(object):
    def __init__(self):
        self.userId = ""
        self.sequenceId = 0
        self.amount = 0

    def Print(self):
        print("AddAmount----------------")
        print("userId: {}".format(self.userId))
        print("sequenceId: {}".format(self.sequenceId))
        print("amount: {}".format(self.amount))
        print("AddAmount================")

class ReduceAmount(object):
    def __init__(self):
        self.userId = ""
        self.sequenceId = 0
        self.amount = 0

    def Print(self):
        print("ReduceAmount----------------")
        print("userId: {}".format(self.userId))
        print("sequenceId: {}".format(self.sequenceId))
        print("amount: {}".format(self.amount))
        print("ReduceAmount================")

class Tick(object):
    def __init__(self):
        self.ticker = ""
        self.market = ""
        self.lastPrice = 0
        self.preClosePrice = 0
        self.openPrice = 0
        self.highPrice = 0
        self.lowPrice = 0
        self.closePrice = 0
        self.upperLimitPrice = 0
        self.lowerLimitPrice = 0
        self.dataTime = 0
        self.qty = 0
        self.turnover = 0
        self.avgPrice = 0
        self.bids = list()
        self.asks = list()
        self.bidQtys = list()
        self.askQtys = list()
        self.tradesCount = 0
        self.tickerStatus = ""

    def Print(self):
        print("Tick----------------")
        print("ticker: {}".format(self.ticker))
        print("market: {}".format(self.market))
        print("lastPrice: {}".format(self.lastPrice))
        print("preClosePrice: {}".format(self.preClosePrice))
        print("openPrice: {}".format(self.openPrice))
        print("highPrice: {}".format(self.highPrice))
        print("lowPrice: {}".format(self.lowPrice))
        print("closePrice: {}".format(self.closePrice))
        print("upperLimitPrice: {}".format(self.upperLimitPrice))
        print("lowerLimitPrice: {}".format(self.lowerLimitPrice))
        print("dataTime: {}".format(self.dataTime))
        print("qty: {}".format(self.qty))
        print("turnover: {}".format(self.turnover))
        print("avgPrice: {}".format(self.avgPrice))
        print("bids size: {}".format(len(self.bids)))
        print("asks size: {}".format(len(self.asks)))
        print("bidQtys size: {}".format(len(self.bidQtys)))
        print("askQtys size: {}".format(len(self.askQtys)))
        print("tradesCount: {}".format(self.tradesCount))
        print("tickerStatus: {}".format(self.tickerStatus))
        print("Tick================")

class Ticks(object):
    def __init__(self):
        self.ticks = list()

    def Print(self):
        print("Ticks----------------")
        for tick in self.ticks:
            tick.Print()
        print("Ticks================")