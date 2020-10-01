# -*- coding:utf-8  -*-
"""
time: 2020-09-28 22:19 
"""
import os

DEBUG = True

# 项目根路径
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

REQUEST_RETRY = 3
RETRY_INTERVAL = 20

# 任务并发设置
DL_CONCURRENCY = 3
PAGE_CONCURRENCY = 1

# 下载路径
DL_PATH = "D:/new_mm"

SITE_BASE_URL = "https://www.mzitu.com"
