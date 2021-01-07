# coding: utf-8
# 数据采集格式

import sys
sys.path.append('../')

import re
# from protocol import DataDefine
import DataDefine

class Specific_log:

    # Class QueryPosition 具体持仓
    def querypositionlist(self,listline,line,userDict):
        tickDataDict = userDict.currPositions
        if len(listline) >= 8:
            if re.match('sequenceId', listline[1].strip(":")) \
                    and re.match('query:position', listline[2].strip(":")) \
                    and re.match('market:sh', listline[4].strip(":")) \
                    or re.match('market:sz', listline[4].strip(":")):
                query_position = DataDefine.QueryPosition()
                query_position.sequenceId = int(listline[1][11:])
                query_position.ticker = str(listline[3][7:])
                query_position.market = str(listline[4][7:])
                query_position.totalQty = int(listline[5][9:])
                query_position.sellableQty = int(listline[6][12:])
                query_position.avgPrice = int(listline[7][9:])
                query_position.yesterdayPosition = int(re.findall(r'\b\d+\b', listline[8])[0])
                tickDataDict.append(query_position)

        elif re.search('offset:end',line):
            userDict.currPositions = []
            return tickDataDict

    # Class OrderAccepted 委托被接受日志
    def orderaccepted(self,listline,userName):
      if len(listline) >=8:
          if re.match('sequenceId', listline[1].strip(":")) \
                  and re.match('order:accepted', listline[2].strip(":")) \
                  and re.match('uniqueId', listline[3].strip(":")):
              order_accepted = DataDefine.OrderAccepted()
              order_accepted.userId= str(userName)
              order_accepted.sequenceId= int(listline[1][11:])
              order_accepted.uniqueId= int(listline[3][9:])
              order_accepted.ticker= str(listline[4][7:])
              order_accepted.market= str(listline[5][7:])
              order_accepted.price= int(listline[6][6:])
              order_accepted.volume= int(listline[7][7:])
              order_accepted.direction= int(re.findall(r'\b\d+\b', listline[8])[0])
              return order_accepted

    # Class OrderRejected 委托被拒绝日志
    def orderrejected(self,listline,userName):
      if len(listline) >= 8:
          if re.match('sequenceId', listline[1].strip(":")) \
                  and re.match('order:rejected', listline[2].strip(":")) \
                  and re.match('uniqueId', listline[3].strip(":")):
              order_rejected = DataDefine.OrderRejected()
              order_rejected.userId=str(userName)
              order_rejected.sequenceId= int(listline[1][11:])
              order_rejected.uniqueId= int(listline[3][9:])
              order_rejected.ticker= str(listline[4][7:])
              order_rejected.market= str(listline[5][7:])
              order_rejected.price= int(listline[6][6:])
              order_rejected.volume= int(listline[7][7:])
              order_rejected.direction= int(re.findall(r'\b\d+\b', listline[8])[0])
              return order_rejected

    # Class OrderTraded 成交日志
    def ordertraded(self,listline,userName):
      if len(listline) >= 8:
          if re.match('sequenceId', listline[1].strip(":")) \
                  and re.match('order:traded', listline[2].strip(":")) \
                  and re.match('market:sz', listline[5].strip(":")) \
                  or re.match('market:sh', listline[5].strip(":")) \
                  and re.match('tradePrice', listline[6].strip(":")):
              order_traded = DataDefine.OrderTraded()
              order_traded.userId= str(userName)
              order_traded.sequenceId= int(listline[1][11:])
              order_traded.uniqueId= int(listline[3][9:])
              order_traded.ticker= str(listline[4][7:])
              order_traded.market= str(listline[5][7:])
              order_traded.tradePrice= int(listline[6][11:])
              order_traded.tradeVolume= int(listline[7][12:])
              order_traded.direction= int(re.findall(r'\b\d+\b', listline[8])[0])
              return order_traded

    # Class QueryAsset  资金日志
    def queryasset(self,listline,userName):
      if len(listline) >= 8:
          if re.match('sequenceId', listline[1].strip(":")) \
                  and re.match('query:asset', listline[2].strip(":")) \
                  and re.match('withholdingAmount', listline[8].strip(":")):
              query_asset = DataDefine.QueryAsset()
              query_asset.userId=str(userName)
              query_asset.sequenceId= int(listline[1][11:])
              query_asset.buyingPower= int(listline[3][12:])
              query_asset.fundBuyAmount= int(listline[4][14:])
              query_asset.fundBuyFee= int(listline[5][11:])
              query_asset.fundSellAmount= int(listline[6][15:])
              query_asset.fundSellFee=int(listline[7][12:])
              query_asset.withholdingAmount=int(re.findall(r'\b\d+\b', listline[8])[0])
              return query_asset
    #Class OrderCancelAccepted 委托撤单被接收日志
    def ordercancelaccepted(self,listline,userName):
      if len(listline) <=5:
          if re.match('sequenceId', listline[1].strip(":")) \
               and re.match('order:cancelAccepted', listline[2].strip(":")) \
               and re.match('uniqueId', listline[3].strip(":")):
              order_cancelaccepted = DataDefine.OrderCancelAccepted()
              order_cancelaccepted.userId=str(userName)
              order_cancelaccepted.sequenceId=int(listline[1][11:])
              order_cancelaccepted.uniqueId=int(re.findall(r'\b\d+\b', listline[3])[0])
              return order_cancelaccepted

    # #Class OrderCancelRejected 委托撤单被拒绝日志
    def ordercancelrejected(self,listline,userName):
      if len(listline) <= 5:
          if re.match('sequenceId', listline[1].strip(":")) \
               and re.match('order:cancelRejected', listline[2].strip(":")) \
               and re.match('uniqueId', listline[3].strip(":")):
              order_cancelrejected = DataDefine.OrderCancelRejected()
              order_cancelrejected.userId=str(userName)
              order_cancelrejected.sequenceId=int(listline[1][11:])
              order_cancelrejected.uniqueId=int(re.findall(r'\b\d+\b', listline[3])[0])
              return order_cancelrejected

    # # Class OrderCanceled 委托撤单成功日志
    def ordercanceled(self,listline,userName):
       if len(listline) <= 5:
           if re.match('sequenceId', listline[1].strip(":")) \
                and re.match('order:canceled', listline[2].strip(":")) \
                and re.match('uniqueId', listline[3].strip(":")):
               order_canceled = DataDefine.OrderCanceled()
               order_canceled.userId=str(userName)
               order_canceled.sequenceId=int(listline[1][11:])
               order_canceled.uniqueId=int(re.findall(r'\b\d+\b', listline[3])[0])
               return order_canceled

    # Class AddAmount 增加资金日志
    def addamount(self,listline,userName):
      if len(listline) <= 3:
           if re.match('sequenceId', listline[1].strip(":")) \
                and re.match('addAmount', listline[2].strip(":")) :
               add_amount=DataDefine.AddAmount()
               add_amount.userId=str(userName)
               add_amount.sequenceId=int(listline[1][11:])
               add_amount.amount=int(re.findall(r'\b\d+\b|\b\S+\b', listline[2].split(':')[1])[0])
               return add_amount

    # Class ReduceAmount 减少资金日志
    def reduceamount(self,listline,userName):
      if len(listline) <= 3:
           if re.match('sequenceId', listline[1].strip(":")) \
                and re.match('reduceAmount', listline[2].strip(":")) :
               reduce_amount=DataDefine.ReduceAmount()
               reduce_amount.userId=str(userName)
               reduce_amount.sequenceId=int(listline[1][11:])
               reduce_amount.amount=int(re.findall(r'\b\d+\b|\b\S+\b', listline[2].split(':')[1])[0])
               return reduce_amount

