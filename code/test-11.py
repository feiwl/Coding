# coding: utf-8
# author: wuweizuo
# 盘后的一些日志的归类整理

# scp 阿里云 各个账号的 am.txt 文件


import os
import datetime
import pexpect
import time

yyyymmdd = "{0:0>4}{1:0>2}{2:0>2}".format(datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day)

def DailyDbPullAssetData():
    aimDir = "/home/prism/daily"
    os.chdir(aimDir)
    srcDir = "/home/wuwz/daily/account"
    os.system("mkdir -p {}/assetdata/{}".format(aimDir, yyyymmdd))

    # copy asset
    assetFilePath0175 = "{}/assetdata/assetdata_109292000175_{}_am.txt".format(srcDir, yyyymmdd)
    assetFilePath0176 = "{}/assetdata/assetdata_109292000176_{}_am.txt".format(srcDir, yyyymmdd)
    assetFilePath4096 = "{}/assetdata/assetdata_105150004096_{}_am.txt".format(srcDir, yyyymmdd)
    assetFilePath0346 = "{}/assetdata/assetdata_205150000346_{}_am.txt".format(srcDir, yyyymmdd)
    assetFilePath4289 = "{}/assetdata/assetdata_105150004289_{}_am.txt".format(srcDir, yyyymmdd)
    assetFilePath4295 = "{}/assetdata/assetdata_105150004295_{}_am.txt".format(srcDir, yyyymmdd)
    assetFilePath4427 = "{}/assetdata/assetdata_105150004427_{}_am.txt".format(srcDir, yyyymmdd)
    assetFilePath4617 = "{}/assetdata/assetdata_105150004617_{}_am.txt".format(srcDir, yyyymmdd)
    assetFilePaths = [assetFilePath0175, assetFilePath0176, assetFilePath4096, assetFilePath0346, assetFilePath4289, assetFilePath4295, assetFilePath4427, assetFilePath4617]
    for assetFile in assetFilePaths:
        task = pexpect.spawn(command="scp wuwz@139.196.125.29:{} {}/assetdata/{}".format(assetFile, aimDir, yyyymmdd), timeout=None)
        task.expect("wuwz@139.196.125.29's password:", timeout=None)
        task.sendline("prism123")
        task.read()
        task.expect(pexpect.EOF, timeout=None)
        task.close()
        print("after scp asset: {}".format(assetFile))
    print("after scp asset")

    subFiles = os.listdir("/home/prism/daily/assetdata/{}".format(yyyymmdd))
    if len(subFiles) >= 6:
        print("pull assetdata data ok")
    else:
        print("pull assetdata data failed, sleep 10s and retry")
        time.sleep(10)
        DailyDbPullAssetData()

if __name__ == "__main__":
    DailyDbPullAssetData()
