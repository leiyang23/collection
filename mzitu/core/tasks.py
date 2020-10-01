# -*- coding:utf-8  -*-
"""
time: 2020-09-28 22:12 
"""
import os
import random
import asyncio
from asyncio import Queue, sleep

from mzitu.core.base import DB
from mzitu.core.coroutines import extract_number_in_tag, get_all_tag, get_max_pages_in_tag, extract_info_from_number
from mzitu.settings import SITE_BASE_URL, DL_PATH
from mzitu.utils import invalid_chars_in_path, get_logger, dl_session

logger = get_logger()


async def collect_tag_detail_url(tag_detail_url_queue: Queue):
    """收集所有的tag详情页网址"""
    logger.info(f"任务：{asyncio.current_task().get_name()} 启动")
    tags = await get_all_tag()
    for tag in tags:
        try:
            max_pages = await get_max_pages_in_tag(tag)
        except (ConnectionError, IndexError):
            continue

        # 根据分页总数构造出该标签下所有的分页地址
        for i in range(1, int(max_pages) + 1):
            url = f"{SITE_BASE_URL}/{tag}/page/{i}"
            await tag_detail_url_queue.put(url)

        # todo：最后一个标签
        if tag == "cosplay":
            break


async def collect_number(db: DB, tag_detail_url_queue: Queue, number_queue: Queue):
    """从tag详情页中提取合集编号，并将未记录的编号入库、入队"""
    logger.info(f"任务：{asyncio.current_task().get_name()} 启动")
    db_numbers = db.get_all_collection_numbers()
    while True:
        url = await tag_detail_url_queue.get()

        numbers = await extract_number_in_tag(url)
        new_numbers = set(numbers) - set(db_numbers)
        if not new_numbers:
            tag_detail_url_queue.task_done()
            continue

        for number in new_numbers:
            await number_queue.put(number)
        db.batch_add_collection_number(new_numbers)

        tag_detail_url_queue.task_done()
        logger.debug(f"新入库、入队 {len(new_numbers)} 条编号")


async def collect_info(db: DB, number_queue: Queue, info_queue: Queue):
    """收集合集信息"""
    logger.info(f"任务：{asyncio.current_task().get_name()} 启动")
    while True:
        number = await number_queue.get()
        info = await extract_info_from_number(number)

        db.add_collection_info(info)

        picture_url_list = list()
        for i in range(1, int(info['total_num']) + 1):
            num = "0" + str(i) if i < 10 else str(i)
            url = info['url_prefix'] + num + info['url_suffix']
            picture_url_list.append(url)
        await info_queue.put((number, info['name'], picture_url_list))

        number_queue.task_done()
        logger.info(f"合集：{number} 元数据已入库、入队")


async def downloader(db: DB, info_queue: Queue):
    """合集图片下载器"""
    logger.info(f"任务：{asyncio.current_task().get_name()} 启动")
    while True:
        collection_number, collection_name, url_list = await info_queue.get()
        logger.info(f"开始下载合集：{collection_name}，共有{len(url_list)}张图片")

        # 此合集下载失败的图片数量，若大于 10，返回None 合集下载失败
        fail_count = 0
        for img_url in url_list:
            await sleep(random.uniform(.5, 2.5))

            file_name = img_url.split("/")[-1]

            # 检查文件夹命名的格式，删除命名中的非法字符
            for char in invalid_chars_in_path:
                if char in collection_name:
                    collection_name = collection_name.replace(char, "")
            dir_path = os.path.join(DL_PATH, collection_name)

            if not os.path.exists(dir_path):
                try:
                    os.mkdir(dir_path)
                except NotADirectoryError:
                    os.mkdir(os.path.join(DL_PATH, "unknown"))

            file_path = os.path.join(dir_path, file_name)

            # 如果已经下载就跳过
            if os.path.exists(file_path):
                continue

            try:
                await dl_session.get(img_url, file_path=file_path)
                logger.debug(f"{file_path} 下载完毕")

            except ConnectionError:
                fail_count += 1

        if fail_count < 10:
            db.update_picture_status(collection_number, 1)
            info_queue.task_done()
        else:
            logger.warning(f"合集：{collection_name} 由于图片失败太多导致下载失败")
