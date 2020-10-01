# -*- coding:utf-8  -*-
"""
time: 2020-09-29 22:48 
"""
import os
import asyncio

from mzitu.core.base import DB
from mzitu.settings import DL_PATH, DEBUG, PAGE_CONCURRENCY, DL_CONCURRENCY
from mzitu.core.tasks import collect_tag_detail_url, collect_info, collect_number, downloader
from mzitu.utils import get_logger

logger = get_logger()


class Spider:
    def __init__(self):
        self.db = DB()
        self.page_currency = PAGE_CONCURRENCY
        self.dl_currency = DL_CONCURRENCY

    def start(self):
        if not os.path.exists(DL_PATH):
            os.makedirs(DL_PATH, exist_ok=True)

        asyncio.run(self.run(), debug=DEBUG)

    async def run(self):
        tag_detail_url_queue = asyncio.Queue()
        number_queue = asyncio.Queue()
        info_queue = asyncio.Queue()

        # 初始化队列，从数据库读取数据
        for number in self.db.get_not_info_collection():
            number_queue.put_nowait(number)

        for info in self.db.get_not_dl_collection():
            info_queue.put_nowait(info)

        logger.debug(f"数据库遗留任务：")
        logger.debug(f"》》 编号队列，共计：{number_queue.qsize()} 条")
        logger.debug(f"》》 元数据队列，共计：{info_queue.qsize()} 条")

        tasks = []

        # 如果当前任务较少就阻塞式地从网站更新数据，否则就放到异步执行收集标签详情页的任务
        all_collection_numbers = self.db.get_all_collection_numbers()
        if len(all_collection_numbers) < 1000 and info_queue.qsize() < 10:
            logger.info("正在从网站更新数据...")
            await collect_tag_detail_url(tag_detail_url_queue)
            logger.info("从网站更新数据完毕！")
        else:
            for _ in range(self.page_currency):
                task = asyncio.create_task(collect_tag_detail_url(tag_detail_url_queue), name="tag-detail")
                tasks.append(task)

        for i in range(self.page_currency):
            task = asyncio.create_task(collect_number(self.db, tag_detail_url_queue, number_queue), name=f"number-{i}")
            tasks.append(task)

        for j in range(self.page_currency):
            task = asyncio.create_task(collect_info(self.db, number_queue, info_queue), name=f"info-{j}")
            tasks.append(task)

        for k in range(self.dl_currency):
            task = asyncio.create_task(downloader(self.db, info_queue), name=f"dl-{k}")
            tasks.append(task)

        logger.info(f"任务列表：{[t.get_name() for t in asyncio.all_tasks()]}")

        await tag_detail_url_queue.join()
        await number_queue.join()
        await info_queue.join()
        logger.info("任务完成！")

        for t in tasks:
            t.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    def reset_dl(self):
        """重置下载记录"""
        self.db.reset_dl()
        logger.info("已重置下载记录")

    def report(self):
        total_nums, info_nums, dl_nums, picture_nums = self.db.report()
        logger.info(f"共有合集：{total_nums} 个，包含图片：{picture_nums} 张，"
                    f"已完成合集元数据获取：{info_nums} 个，已完成图片下载：{dl_nums}个")
