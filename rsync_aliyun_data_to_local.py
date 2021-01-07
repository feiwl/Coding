from concurrent import futures
import pexpect

def pex_connection(command, expect, info):
    task = pexpect.spawn(command=command,timeout=None)
    task.expect(expect, timeout=None)
    task.sendline(info)
    task.read()
    task.expect(pexpect.EOF, timeout=None)
    task.close()
    task.

expect = "root@139.196.125.29's password:"
info = "fish.Ops86"


# command_trade_log ="rsync -ruvz root@139.196.125.29:/home/wuwz/daily/trade_log/ " \
#          "/home/prism/daily/trade_log/ --info=progress2 --info=name0 " \
#          "--partial > /home/feiwl/script/log/trade_log.txt"
#
# command_predict_multi_log = "rsync -ruvz root@139.196.125.29:/predict/predict_log/ " \
#          "/home/prism/daily/predict_multi_log/ --info=progress2 --info=name0 " \
#          "--partial > /home/feiwl/script/log/predict_multi_log.txt"
#
# command_predict_output = "rsync -ruvz root@139.196.125.29:/predict/predict_result/ " \
#          "/home/prism/daily/predict_multi_output/ --info=progress2 --info=name0 " \
#          "--partial > /home/feiwl/script/log/predict_multi_output.txt"

# commands = [command_trade_log, command_predict_output, command_predict_multi_log]
#
# fs = []
# executor = futures.ProcessPoolExecutor()
# for com in commands:
#     f = executor.submit(pex_connection, com, expect, info)
#     fs.append(f)
# for f in fs:
#     print(f.result())
