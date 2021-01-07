# coding: utf-8
# author: wuweizuo
# 爬虫，爬取399905和000001的涨跌幅

import time
from bs4 import BeautifulSoup
from selenium import webdriver
from decimal import Decimal

def Get399905Rate():
    url = 'http://stockpage.10jqka.com.cn/realHead_v2.html#32_399905'
    option = webdriver.ChromeOptions()
    option.add_argument('--headless')
    option.add_argument('--no-sandbox')
    browser = webdriver.Chrome(options=option,keep_alive=False)
    browser.get(url)
    browser.implicitly_wait(60)
    browser.set_page_load_timeout(60)
    while True:
        time.sleep(1)
        valueElement = browser.find_element_by_id('hexm_float_rate')
        valuePrice = browser.find_element_by_id('hexm_curPrice')
        if valueElement is None:
            continue
        rate = valueElement.get_attribute('textContent')
        ihValue = valuePrice.get_attribute('textContent')
        if rate == '--':
            continue
        else:
            break
    browser.close()
    return ihValue, rate

def Get000001Rate():
    url = 'http://stockpage.10jqka.com.cn/realHead_v2.html#16_1A0001'
    option = webdriver.ChromeOptions()
    option.add_argument('--headless')
    option.add_argument('--no-sandbox')
    browser = webdriver.Chrome(options=option,keep_alive=False)
    browser.get(url)
    browser.implicitly_wait(60)
    browser.set_page_load_timeout(60)

    while True:
        time.sleep(1)
        valueElement = browser.find_element_by_id('hexm_float_rate')
        valuePrice = browser.find_element_by_id('hexm_curPrice')
        if valueElement is None:
            continue
        rate = valueElement.get_attribute('textContent')
        icValue = valuePrice.get_attribute('textContent')
        if rate == '--':
            continue
        else:
            break
    browser.close()
    return icValue, rate

if __name__ == "__main__":
    rate399905 = Get399905Rate()[0]
    time.sleep(1)
    rate000001 = Get000001Rate()[0]
    print(rate000001)
    print(rate399905)
    # print(f"{round(0.71 - float(rate399905.replace('%','')),3)}%")
    # print("rate399905={}".format(rate399905[1]))
    # print("rate000001={}".format(rate000001[1]))