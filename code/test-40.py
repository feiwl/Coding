import subprocess
import time

datetime = time.strftime("%Y%m%d",time.localtime())

# Copy same day refData.csv to /home/feiwl/script/given_hdf5_format/history_high_low_limit/refData/...
copy_refData = "cp /md/md/durian/md/{datetime}/refData.csv /home/feiwl/script/given_hdf5_format/handler_data_hdf5/refData/{datetime}_refData.csv".format(datetime=datetime)

# implement get_refData_high_low_limit script same day low_high_limit
Insert_history_adj_factor = "/root/anaconda3/bin/python3 /home/feiwl/script/given_hdf5_format/get_history_adj_factor.py " \
                             "--date={datetime} --db-host='192.168.1.225' --db-name='wind' --db-user='wind_user' " \
                             "--db-pw=/home/feiwl/script/given_hdf5_format/pw_1 " \
                             "--adj-factor-source-hdf=/home/feiwl/script/given_hdf5_format/handler_data_hdf5/history_adj_factor.hdf5 " \
                             " --adj-factor-to-hdf=/home/feiwl/script/given_hdf5_format/handler_data_hdf5/history_adj_factor.hdf5".format(datetime=datetime)

def while_script():
    (refData_status,uploadres) = subprocess.getstatusoutput(cmd=copy_refData)
    if refData_status != 0:
        print(uploadres)
        time.sleep(10)
        while_script()

while_script()

(high_history_adj_factor_status,high_history_adj_factor) = subprocess.getstatusoutput(cmd=Insert_history_adj_factor)
if high_history_adj_factor_status !=0:
    print(high_history_adj_factor)
