import os
import re

file = '/home/banruo/futures.csv'



with open(file, 'r')as f:
    for i in f:
        code = re.sub("[A-Za-z]", "" ,i.split(',')[0])
        print(code)




