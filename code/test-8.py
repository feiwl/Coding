




import pandas as pd
import numpy as np
#
# date1 = {'20201002': {'600000':50, '600001': 60, '600002': 70, '600003': 80}}
# date2 = {'20201002': {'600000': None, '600001': None,'600002':None}}
#
#
# date3 = {'20201003': {'600000': 10, '600001': 20,'600002':30}}
# date4 = {'20201003': {'600000': None, '600001': None,'600002':None}}
# df1 = pd.DataFrame(date1).T
# df2 = pd.DataFrame(date2).T
# df3 = pd.DataFrame(date3).T
# df4 = pd.DataFrame(date4).T
# index = date1.keys()
#
#
# Differences = set=(df1.columns) ^ set(df2.columns)
# print(Differences)
# for symbol in Differences:
#     if symbol not in df1:
#         df1[symbol] = None
#     if symbol not in df2:
#         df2[symbol] = None
#
# data = df1.append(df2,ignore_index=True)
# print(data)
#
# Template = {'20201002': {'600000': None, '600001': None, '600002': None, '600003': None}}
#
# date1 = {'20201002': {'600000':50, '600001': None, '600002': 70, '600003': 80}}
# date2 = {'20201003': {'600000': None, '600001': 90,'600002':None}}
#
#
# date3 = {'20201003': {'600000': 10, '600001': 20,'600002':30}}
# date4 = {'20201003': {'600000': None, '600001': None,'600002':None}}
# df1 = pd.DataFrame(date1).T
# df2 = pd.DataFrame(date2).T
# df3 = pd.DataFrame(date3).T
# df4 = pd.DataFrame(date4).T
# index = date1.keys()
# temp = pd.DataFrame(Template).T





date1 = {'20201002': {'600000':50, '600001': None, '600002': 70, '600003': 80}}
df1 = pd.array(date1, pd.Int64Dtype)
print(df1)

