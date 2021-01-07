import smtplib
import datetime
import pymongo
from concurrent import futures
from email.header import Header
from email.mime.text import MIMEText

class _DbConnector_mongo:

    """Mongodb database connection initialization..."""

    def __init__(self, host, database, table):
        self._myclient = pymongo.MongoClient(host)
        self._mydb = self._myclient[database]
        self.mycol = self._mydb[table]

    def update(self, sql):
        """Parameter sql: {"": ""} and {"": "", "": "", ...}"""
        self.mycol.insert_many(sql)

    def query_and_fetch(self, sql):
        """Parameter sql: {"" : ""}"""
        mydoc = self.mycol.find_one(sql)
        return mydoc

def mail(flag,content):
    receivers = ['fwl8378@163.com', '532706324@qq.com']
    for receiver in receivers:
        sender = '532706324@qq.com'
        message = MIMEText('{}'.format(content), 'plain', 'utf-8')
        message['Subject'] = Header(flag, 'utf-8')
        message['From'] = sender
        message['To'] = receiver

        smtpter = smtplib.SMTP_SSL('smtp.qq.com', 465)
        smtpter.set_debuglevel(1)
        smtpter.login(sender, 'hpaxpruupgnqbghd')
        smtpter.sendmail(sender, receiver, message.as_string())
        smtpter.quit()
        print('邮件发送完成')

def check_whether_the_data_is_complete(tables):
    """Check all tables and send email notification ..."""
    insert_success_lst = []
    insert_failed_lst = []
    to_day = datetime.datetime.now().strftime("%Y%m%d")
    for tablename in tables:
        conn = _DbConnector_mongo("mongodb://192.168.10.68:27017", "marketdata", tablename)
        query_result = conn.query_and_fetch({"nActionDay": "{}".format(to_day)})
        if query_result:
            insert_success_lst.append(tablename)
        else:
            insert_failed_lst.append(tablename)
    if insert_success_lst:
        flag = "Tables inserted successfully     " + datetime.datetime.now().strftime("%H:%M:%S")
        mail(flag, insert_success_lst)
    if insert_failed_lst:
        flag = "Insert failed tables     " + datetime.datetime.now().strftime("%H:%M:%S")
        mail(flag, insert_failed_lst)

if __name__ == "__main__":
    mongo_tables = ['snap_from_open_index',
                    'snap_from_open_stock',
                    'snap_min_index',
                    'snap_min_stock',
                    'snap_to_close_index',
                    'snap_to_close_stock',
                    'transaction']
    check_whether_the_data_is_complete(mongo_tables)
