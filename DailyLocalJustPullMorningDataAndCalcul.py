# coding: utf-8
# author: wuweizuo
# 盘后的一些日志的归类整理

import os
import datetime
import pexpect
import time
import DailyLocalJustPullMorningData
import DailyCalculAssetWithPulledData

if __name__ == "__main__":
    DailyLocalJustPullMorningData.DailyDbPullAssetData()
    DailyCalculAssetWithPulledData.CalculEntry()
