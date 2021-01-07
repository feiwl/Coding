#coding:utf8
import socket
import time
import os
import threading
import argparse
import smtplib
from email.header import Header
from email.mime.text import MIMEText

MAX_BYTES = 1024
is_alive = 0

def mail(to_mail):
    sender = '532706324@qq.com'
    receivers = to_mail

    message = MIMEText('客户端心跳停止', 'plain', 'utf-8')
    message['Subject'] = Header('monitor检测内网失败', 'utf-8')
    message['From'] = sender
    message['To'] = receivers

    smtpter = smtplib.SMTP_SSL('smtp.qq.com', 465)
    smtpter.set_debuglevel(1)
    smtpter.login(sender, 'hpaxpruupgnqbghd')
    smtpter.sendmail(sender, receivers, message.as_string())
    smtpter.quit()
    print('邮件发送完成')

def server(host,port,delay,to_mail):
    if not isinstance(host,str):
        raise KeyError("The host must be a string like \'127.0.0.1\'")
    if not isinstance(port,int):
        raise KeyError('The port must be a integer')
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock.bind((host,port))
    def recv():
        global is_alive
        while True:
            #print('test')
            data,addr = sock.recvfrom(MAX_BYTES)
            print(data)
            is_alive += 1
            if is_alive >= 10000:
                is_alive = 0
    client = threading.Thread(target=recv)
    client.setDaemon(True)
    client.start()
    IS_ALIVE = True
    while IS_ALIVE:
        before = is_alive
        time.sleep(int(delay))
        if before is is_alive:
            #result = os.popen('python test.py')
            list(map(lambda to: mail(to), to_mail))
            #print(result)
            IS_ALIVE = False
    sock.close()

def main():
    server('192.168.10.101', 5000, 60,['fwl8378@163.com'])
    # parse = argparse.ArgumentParser(description='Listen to a port and excute a file')
    # parse.add_argument('-T',required=True, nargs='+', help='to mail monitor')
    # parse.add_argument('-H',nargs='?',default='127.0.0.1',const='127.0.0.1')
    # parse.add_argument('-P',nargs='?',default=2333,const=2333,type=int)
    # parse.add_argument('-D',nargs='?',default=5,const=5)
    # result = parse.parse_args()
    # print(result.H,result.P,result.D,result.T)
    # server(result.H,result.P,result.D,result.T)

if __name__=='__main__':
    main()
