# -*- coding:utf-8  -*-
"""
time: 2020-09-29 1:38 
"""
import os

from lxml import etree

from mzitu.settings import SITE_BASE_URL
from mzitu.utils import page_session, get_logger

logger = get_logger()


async def get_all_tag():
    """从`专题页`获取所有的标签"""
    url = f"{SITE_BASE_URL}/zhuanti/"
    page = await page_session.get(url)
    html = etree.HTML(page)
    one_level_urls = html.xpath("//dl[@class='tags']/dd/a/@href")
    return [url.replace("https://www.mzitu.com/tag/", "").strip("/") for url in one_level_urls]


async def get_max_pages_in_tag(tag):
    """获取该`标签`的分页总数，进而可以构造出所有标签页地址"""
    url = f"{SITE_BASE_URL}/tag/{tag}/"
    page = await page_session.get(url)
    html = etree.HTML(page)
    max_pages = int(html.xpath("//div[@class='nav-links']/a[last()-1]/text()")[0])
    return max_pages


async def extract_number_in_tag(tag_detail_url):
    """从`标签分页`抽取合集编号"""
    text = await page_session.get(tag_detail_url)
    html = etree.HTML(text)
    hrefs = html.xpath("//ul[@id='pins']/li/a/@href")
    numbers = [href.strip(f"{SITE_BASE_URL}/") for href in hrefs]
    return numbers


async def extract_info_from_number(collection_num):
    """根据合集编号，构造出合集首页地址，进而抽取元数据"""
    url = f"{SITE_BASE_URL}/{collection_num}"
    text = await page_session.get(url)
    html = etree.HTML(text)
    tag_names = html.xpath("//div[@class='main-tags']/a/text()")
    total_num = html.xpath("//div[@class='pagenavi']/a[last()-1]/span/text()")[0]
    name = html.xpath("//h2[@class='main-title']/text()")[0]

    img_first_url = html.xpath("//div[@class='main-image']/p/a/img/@src")[0]

    splits_1 = os.path.split(img_first_url)
    url_prefix = splits_1[0] + "/" + splits_1[1][:3]
    url_suffix = splits_1[1][5:]

    splits_2 = img_first_url.split("/")
    year = splits_2[3]
    month = splits_2[4]
    day = splits_2[5][:2]

    res = {
        "collection_num": collection_num,
        "name": name,
        "total_num": total_num,
        "year": year,
        "month": month,
        "day": day,
        "url_prefix": url_prefix,
        "url_suffix": url_suffix,
        "tag_names": tag_names,
    }
    return res