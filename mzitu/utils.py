# -*- coding:utf-8  -*-
"""
time: 2020-09-27 23:51 
"""
import asyncio
import os
import random
import logging
import logging.handlers

import aiohttp

from mzitu.settings import DEBUG, BASE_PATH, REQUEST_RETRY, RETRY_INTERVAL

invalid_chars_in_path = ['*', '|', ':', '：', '?', '/', '<', '>', '"', '\\']

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0",
    "Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)"
]


def dl_header() -> dict:
    """ 随机返回图片下载请求头 """
    header = {
        "user-agent": random.choice(user_agents),
        "Referer": "https://www.mzitu.com/"
    }
    return header


def page_header() -> dict:
    """ 随机返回主站网页请求头 """
    header = {
        "user-agent": random.choice(user_agents),
        "Host": "www.mzitu.com",
    }
    return header


class Logger:
    def __init__(self):
        self.logger = logging.getLogger("spider")
        self.logger.setLevel(logging.DEBUG)

        simple_fmt = logging.Formatter("%(pathname)s - %(lineno)d - %(levelname)s - %(message)s")
        stand_fmt = logging.Formatter("%(asctime)s - %(filename)s - %(levelname)s - %(lineno)d - %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")

        if DEBUG:
            # 输出到 控制台
            debug_handler = logging.StreamHandler()
            debug_handler.setLevel(logging.DEBUG)
            debug_handler.setFormatter(simple_fmt)
            debug_handler.name = "debug_handler"
            self.logger.addHandler(debug_handler)
        else:
            # 输出到文件：每天一个日志，保留最近七天。
            log_file = os.path.join(BASE_PATH, "app.log")
            file_handler = logging.handlers.TimedRotatingFileHandler(log_file, when="d", interval=1, backupCount=7,
                                                                     encoding="utf-8")
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(stand_fmt)
            file_handler.name = "file_handler"
            self.logger.addHandler(file_handler)

        self.logger.info(f"日志初始化完毕，名称：{self.logger.name}，是否调试：{'YES' if DEBUG else 'NO'}")

    def __call__(self, *args, **kwargs):
        return self.logger


get_logger = Logger()


class HttpSession:
    def __init__(self, header_gen=None):
        self.log = get_logger()
        self.header_gen = header_gen
        self.log.info(f"HTTP请求Session初始化完毕，请求头生成函数：{header_gen.__name__}")

    async def get(self, url, file_path=None):
        self.log.debug(f"请求地址：{url}")
        await asyncio.sleep(random.uniform(.5, 1.5))
        resp = None
        timeout = aiohttp.ClientTimeout(total=60, connect=30)
        connector = aiohttp.TCPConnector(limit=5)
        session = aiohttp.ClientSession(connector=connector, headers=self.header_gen(), timeout=timeout)

        for i in range(REQUEST_RETRY):
            try:
                resp = await session.get(url)
                if resp.status == 429:
                    self.log.warning(f"请求{url}触发网站反爬机制，睡眠，重试-{i + 1}")
                    await session.close()
                    await asyncio.sleep(RETRY_INTERVAL)
                    continue
                if resp.status != 200:
                    self.log.warning(f"请求 {url} 失败：{resp.status}")
                    raise ConnectionError

                if file_path:
                    with open(file_path, 'wb') as fd:
                        while True:
                            chunk = await resp.content.read(1024)
                            if not chunk:
                                break
                            fd.write(chunk)

                text = await resp.text()
                return text

            except asyncio.TimeoutError:
                self.log.error("timeout-1")
                session = aiohttp.ClientSession(connector=connector, headers=self.header_gen(), timeout=timeout)
                continue
            except asyncio.CancelledError:
                self.log.error("timeout-2")
                continue
            finally:
                if resp:
                    resp.close()
                await session.close()
        else:
            raise ConnectionError


page_session = HttpSession(header_gen=page_header)
dl_session = HttpSession(header_gen=dl_header)
