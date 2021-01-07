import obtion_system_state as sys_state
import threading
import argparse
import openpyxl

parser = argparse.ArgumentParser(description='Information file...')
parser.add_argument('--information', required=True, help='Information file')
parser.add_argument('--outfile', required=True, help='out file')
args = parser.parse_args()

information_file = args.information
config_infor = sys_state.InForMation_File_Handler()
config_infor.infor_match_proc(information_file)

computer_lists = config_infor.computer_infors
global_infor = config_infor.global_config
watch_date = global_infor['watch_date']
date_frame = global_infor['date_frame']

computer_state_dict = dict()
for computer in computer_lists:
    conn = sys_state._DConnection(computer['ip'], computer['port'], computer['username'], computer['password'])
    system_state = sys_state.Get_SystemState()
    thread = threading.Thread(target=system_state.obtain_states, args=(conn, date_frame), daemon=True).start()
    computer_state_dict[computer['ip']] = system_state

sys_state.time_control(watch_date)

for ip, computer in computer_state_dict.items():
    computer_date = computer.state
    # infor = "IP <{}> CPU_MAX_VALUE <{}>," \
    #         " MEMORY_MAX_VALUE <{}>, " \
    #         "DISK_CURRENT_VALUE <{}>".format(ip, str(float(computer_date['cpu'])*100)+'%',
    #                                                            str(float(computer_date['memory'])*100)+'%')
    # with open(args.outfile, 'a+') as file:
    #     file.write(infor+'\n')
