import os
import re
import shutil
from concurrent import futures

class Executor:
    def __init__(self):
        self._results = list()
        self.count = 0

    @property
    def result(self):
        return self._results

    def dir_exec(self, dirs,path):
        directory = list(filter(lambda file: os.path.isdir(path+'/'+file), dirs))
        for file in directory:
            self.path_list(path+'/'+file)

    def file_exec(self, file,path):
        file_list = list(map(lambda file : path+'/'+file,filter(lambda file: re.search('\.md$', path+'/'+file), file)))
        return file_list

    def path_list(self, path):
        file_path = os.listdir(path)
        if file_path:
            self.dir_exec(file_path, path)
            file_list = self.file_exec(file_path, path)
            if file_list:
            #     self._results.append(file_list)
            #     self.result.append(list(filter(lambda file: re.search('ZJ_L1_CFFEX_\w+', file), file_list)))
                  self.result.append(list(filter(lambda file: re.search('ZJIN_L2_CFFEX_\w+', file), file_list)))

def delete_file(path, name):
    if name == "IF":
        shutil.rmtree(path+'/'+name)
    elif name == "IH":
        shutil.rmtree(path+'/'+name)
    elif name == "T":
        shutil.rmtree(path+'/'+name)
    elif name == "TF":
        shutil.rmtree(path+'/'+name)
    elif name == "TS":
        shutil.rmtree(path + '/' + name)
    elif name == "TEFP":
        shutil.rmtree(path + '/' + name)
    elif name == "IOC":
        shutil.rmtree(path + '/' + name)
    elif name == "IOP":
        shutil.rmtree(path + '/' + name)
#IC  IF  IH  T  TF

def target_path(src_path,do_path):
    num = re.search('\d{8}',src_path[0])
    for_mat = list(map(lambda file: re.search('[A-Z]{4}_(L1|L2)_[A-Z0-9]+', file).group() ,src_path))
    path_do = do_path + '/' + num.group() + '/' + for_mat[0]
    if not os.path.exists(path_do):
        os.makedirs(path_do)
    return path_do

def write_main(list_md, tar_path):
    for i in list_md:
        path = target_path([i], tar_path)
        os.system('{} {} {}'.format(executable, i, path + '/'))

def delte_main(list_md, tar_path):
    for i in list_md:
        path = target_path([i], tar_path)
        for i in os.listdir(path+'/'):
            delete_file(path,i)

path='/media/banruo/My Passport/bindata/bindata201912'
#path = '/home/banruo/TestMD/20170928'
executable = '/home/banruo/TestMD/read_md_tool.tool'

tar_path = '/media/banruo/MyPassport/bindata201912'
result = Executor()
result.path_list(path)


# delete direcotry
# for list_md in result.result:
#     delte_main(list_md, tar_path)

Process_Poll = futures.ProcessPoolExecutor(max_workers=3)
for list_md in result.result:
    Process_Poll.submit(write_main,list_md,tar_path)
Process_Poll.shutdown()





#os.system('/home/banruo/TestMD/read_md_tool.tool /home/banruo/TestMD/20170928/DL_L2_DCE_20170928.md /home/banruo/TestMDOUTPUT/', )