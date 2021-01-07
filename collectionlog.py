# 日志收集程序

import sys

sys.path.append('../')
#from data_communicate import PackagePush
from watchdog.events import *
from watchdog.observers import Observer
import DataDefine
import time
import re
import specificlog
#from data_recorder import specificlog


# 记录用户数据
class UserData(object):
    def __init__(self):
        self.readedLine = 1
        self.currPositions = []
        self.allset = set()


g_userDataDict = dict()

# test todo ####################################################################
g_userDataDict['105150004096'] = UserData()
g_userDataDict['105150004617'] = UserData()
#g_userDataDict['105150004427'] = UserData()

path_4096 = '/home/banruo/Test/4096'
path_4617 = '/home/banruo/Test/4617'
#path_4427 = '/home/prism/program/XtpMonitorDataDown_4427/MonitorOutput'
# test todo ####################################################################


g_pathList = [path_4617, path_4096]
g_dataSender = None
allsetdata = set()


# 实时监控目录
class FileMonitorHandler(FileSystemEventHandler):
    def __init__(self, userBypath, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)

        self.userBypath = userBypath

    # 重写文件改变函数，文件改变都会触发文件夹变
    def on_modified(self, event):
        if not event.is_directory:  # 文件改变都会触发文件夹变化
            if getUserByPath(self.userBypath) != None:
                main(getUserByPath(self.userBypath))


def Alldirectory():
    success = []
    overtime = []

    for path in g_pathList:
        if getUserByPath(path) != None:
            success.append(path)
        else:
            overtime.append(path)

    return success, overtime


def output_log(listline, line, userName, userDict):
    Specificlog = specificlog.Specific_log()

    query_position_log = Specificlog.querypositionlist(listline, line, userDict)

    if query_position_log != None:
        positions = DataDefine.QueryPositions()
        positions.positions = query_position_log
        positions.userId = userName
        positions.Print()
        if g_dataSender:
            g_dataSender.pushPackage(positions)

    if len(listline) >= 8:
        query_asset_log = Specificlog.queryasset(listline, userName)
        if query_asset_log != None:
            data = query_asset_log
            data.Print()
            if g_dataSender:
                g_dataSender.pushPackage(data)

        order_accepted_log = Specificlog.orderaccepted(listline, userName)
        if order_accepted_log != None:
            data = order_accepted_log
            data.Print()
            if g_dataSender:
                g_dataSender.pushPackage(data)

        order_rejected_log = Specificlog.orderrejected(listline, userName)
        if order_rejected_log != None:
            data = order_rejected_log
            data.Print()
            if g_dataSender:
                g_dataSender.pushPackage(data)

        order_traded_log = Specificlog.ordertraded(listline, userName)
        if order_traded_log != None:
            data = order_traded_log
            data.Print()
            if g_dataSender:
                g_dataSender.pushPackage(data)

    if len(listline) <= 5:
        order_cancel_accepted_log = Specificlog.ordercancelaccepted(listline, userName)
        if order_cancel_accepted_log != None:
            data = order_cancel_accepted_log
            data.Print()
            if g_dataSender:
                g_dataSender.pushPackage(data)

        order_cancel_rejected_log = Specificlog.ordercancelrejected(listline, userName)
        if order_cancel_rejected_log != None:
            data = order_cancel_rejected_log
            data.Print()
            if g_dataSender:
                g_dataSender.pushPackage(data)

        order_canceled_log = Specificlog.ordercanceled(listline, userName)
        if order_canceled_log != None:
            data = order_canceled_log
            data.Print()
            if g_dataSender:
                g_dataSender.pushPackage(data)

        add_mount_log = Specificlog.addamount(listline, userName)
        if add_mount_log != None:
            data = add_mount_log
            data.Print()
            if g_dataSender:
                g_dataSender.pushPackage(data)

        reduce_amount_log = Specificlog.reduceamount(listline, userName)
        if reduce_amount_log != None:
            data = reduce_amount_log
            data.Print()
            if g_dataSender:
                g_dataSender.pushPackage(data)


def getUserByPath(path):
    numsfile = os.listdir(path)
    for i in numsfile:
        if re.search(".swp", i):
            continue

        # test todo ####################################################################
        if re.search(time.strftime("%Y%m%d", time.localtime()), i):
            return i.split("_")[0], path + "/" + i
        '''
        if re.search("20200410", i):
            return i.split("_")[0], path + "/" + i
        '''


def main(userByPath):
    global g_userDataDict
    userName = userByPath[0]
    userPath = userByPath[1]
    userDict = g_userDataDict[userName]
    with open(userPath, 'r') as con:
        num = 0
        for line in con:
            time.sleep(0.001)
            if not re.search(';', line):
                continue
            elif not line.strip():
                continue
            num += 1
            if num == userDict.readedLine:
                userDict.readedLine += 1
                listline = line.strip(" ").split(",")
                output_log(listline, line, userName, userDict)

    time.sleep(0.01)


def MainEntry(dataSender):
    global g_dataSender
    g_dataSender = dataSender

    monitorList = []
    skipNum = 0
    if 0 == skipNum:
        skipNum = skipNum + 1
        (success, overtime) = Alldirectory()

        for succdog in success:
            main(getUserByPath(succdog))
            event_handler = FileMonitorHandler(succdog)
            observer = Observer()
            observer.schedule(event_handler, path=succdog, recursive=True)  # recursive递归
            observer.start()
            monitorList.append(observer)

        for overdog in overtime:
            event_handler = FileMonitorHandler(overdog)
            observer = Observer()
            observer.schedule(event_handler, path=overdog, recursive=True)  # recursive递归
            observer.start()
            monitorList.append(observer)

        for i in monitorList:
            i.join()


def GetAllUsedTicks():
    global allsetdata
    for i in g_userDataDict:
        setdata = g_userDataDict[i]
        allsetdata.update(setdata.allset)
    return allsetdata


if __name__ == '__main__':
    monitorList = []
    skipNum = 0
    if 0 == skipNum:
        skipNum = skipNum + 1
        (success, overtime) = Alldirectory()
        for succdog in success:
            main(getUserByPath(succdog))
            event_handler = FileMonitorHandler(succdog)
            observer = Observer()
            observer.schedule(event_handler, path=succdog, recursive=True)  # recursive递归
            observer.start()
            monitorList.append(observer)

        for overdog in overtime:
            event_handler = FileMonitorHandler(overdog)
            observer = Observer()
            observer.schedule(event_handler, path=overdog, recursive=True)  # recursive递归
            observer.start()
            monitorList.append(observer)

        for i in monitorList:
            i.join()


