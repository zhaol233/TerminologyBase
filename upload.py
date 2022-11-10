# -*- coding: utf-8 -*-
# @Time    : 2022/11/9 17:36
# @Author  : zhaoliang
# @Description: TODO

# http://10.20.2.235/login

from selenium import webdriver
import time
# from pywinauto import Desktop

# from pywinauto.keyboard import send_keys

browser  = webdriver.Chrome()
browser.implicitly_wait(5)  # 设置隐性等待,等待10S加载出相关控件再执行之后的操作

url = r"http://10.20.2.235/login"
browser .get(url)
time.sleep(2)
username = browser.find_element_by_xpath('//*[@id="txtUserName"]')
username.clear()
username.send_keys('*******')
print('username input success')
# 输入密码
browser.find_element_by_xpath('//*[@id="txtPassword"]').send_keys('******')
print('password input success')

