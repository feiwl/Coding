import subprocess
import time

datetime = time.strftime("%Y%m%d",time.localtime())

# Copy same day refData.csv to /home/feiwl/script/given_hdf5_format/history_high_low_limit/refData/...
copy_refData = "cp /md/md/durian/md/{datetime}/refData.csv /home/feiwl/script/given_hdf5_format/handler_data_hdf5/refData/{datetime}_refData.csv".format(datetime=datetime)

# implement get_refData_high_low_limit script same day low_high_limit
get_refData_high_low_limit = "python3 /home/feiwl/script/given_hdf5_format/get_refData_high_low_limit.py --date={datetime} " \
                             " --highlimit-source-hdf=/home/feiwl/script/given_hdf5_format/handler_data_hdf5/history_highlimit_data.hdf5" \
                             " --highlimit-to-hdf=/home/feiwl/script/given_hdf5_format/handler_data_hdf5/history_highlimit_data.hdf5 " \
                             "--lowlimit-source-hdf=/home/feiwl/script/given_hdf5_format/handler_data_hdf5/history_lowlimit_data.hdf5 " \
                             "--lowlimit-to-hdf=/home/feiwl/script/given_hdf5_format/handler_data_hdf5/history_lowlimit_data.hdf5 " \
                             "--refData-csv=/home/feiwl/script/given_hdf5_format/history_high_low_limit/refData/{ref_datetime}_refData.csv".format(datetime=datetime,ref_datetime=datetime)

# implement sameday_adj_factor script same day dj_factor
sameday_adj_factor = "python3  /home/feiwl/script/given_hdf5_format/sameday_adj_factor.py --same-date={same_date}" \
                     "--db-host='192.168.1.225' --db-name='wind'" \
                     " --db-user='wind_user' " \
                     "--db-pw=/home/feiwl/script/given_hdf5_format/pw_1  " \
                     "--refData_csv=/home/feiwl/script/given_hdf5_format/history_high_low_limit/refData/{ref_datetime}_refData.csv " \
                     "--to-file-hdf5=/home/feiwl/script/given_hdf5_format/handler_data_hdf5/sameday_adj_factor.hdf5".format(same_date=datetime,ref_datetime=datetime)

# implement insert_transaction script all day volume
insert_float_volume_df = "python3  /home/feiwl/script/given_hdf5_format/insert_float_volume_df.py " \
                     "--begin-date=20190101 " \
                     "--end-date={same_day} " \
                     "--float-volume-to-hdf=/home/feiwl/script/given_hdf5_format/handler_data_hdf5/history_float_volume.hdf5".format(same_day=datetime)

(refData_status,uploadres) = subprocess.getstatusoutput(cmd=copy_refData)
if refData_status != 0:
    raise FileNotFoundError(uploadres)

(high_low_limit_status,high_low_limit_information) = subprocess.getstatusoutput(cmd=get_refData_high_low_limit)
if high_low_limit_status !=0:
    print(high_low_limit_information)

(adj_factor_limit_status,adj_factor_information) = subprocess.getstatusoutput(cmd=sameday_adj_factor)
if adj_factor_limit_status !=0:
    print(high_low_limit_information)

(float_volume_status,float_volume_information) = subprocess.getstatusoutput(cmd=insert_float_volume_df)
if float_volume_status != 0:
    print(float_volume_information)


