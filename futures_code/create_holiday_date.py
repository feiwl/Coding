# import pandas as pd
# import requests
# from lxml import etree
#
# class WanNianRiLi(object):
#     """万年日历接口数据抓取
#     Params:year 四位数年份字符串
#     """
#
#     def __init__(self, year):
#         self.year = year
#         data = self.parseHTML()
#         self.exportCSV(data)
#
#     def parseHTML(self):
#         """页面解析"""
#         result = []
#         # 生成月份列表
#         dateList = [self.year + '-' + '%02d' % i for i in range(1, 13)]
#         for year_month in dateList:
#             s = requests.session()
#             url = 'https://wannianrili.51240.com/ajax/'
#             payload = {'q': year_month}
#             response = s.get(url, params=payload)
#             element = etree.HTML(response.text)
#             html = element.xpath('//div[@class="wnrl_riqi"]')
#             print('In Working:', year_month)
#             for _element in html:
#                 # 获取节点属性
#                 item = _element.xpath('./a')[0].attrib
#                 if 'class' in item:
#                     if item['class'] == 'wnrl_riqi_xiu':
#                         tag = '休假'
#                     elif item['class'] == 'wnrl_riqi_ban':
#                         tag = '补班'
#                     else:
#                         pass
#                     _span = _element.xpath('.//text()')
#                     result.append({'Date': year_month + '-' + _span[0], 'Holiday': _span[1], 'Tag': tag})
#         print(result)
#         return result
#
#     def exportCSV(self, data):
#         df = pd.DataFrame(data)
#         df.to_csv(self.year + 'Holiday.csv', index=False)
#         print(df)
#
# if __name__ == '__main__':
#     rili = WanNianRiLi('2021')
