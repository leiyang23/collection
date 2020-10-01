# -*- coding:utf-8  -*-
"""
time: 2020-09-27 22:40
妹子图（https://www.mzitu.com)美女图片下载
特点：增量更新
"""
import sys
import warnings

from mzitu.spider import Spider

python_version = sys.version_info
if python_version.major < 3 and python_version.minor < 8:
    raise RuntimeError("不支持Python3.8 以下的版本")

try:
    import lxml, aiohttp, sqlalchemy
except ImportError:
    raise RuntimeError("请先安装依赖：lxml、aiohttp、sqlalchemy")

warnings.warn("仅供学习使用，请勿用于商业行为或影响到网站正常运行！！！")


def start():
    """`妹子图`"""
    Spider().start()


if __name__ == '__main__':
    start()
