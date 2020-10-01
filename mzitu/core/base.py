# -*- coding:utf-8  -*-
"""
time: 2020-09-27 22:59 
"""
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from mzitu.core.model import base, DownloadRecord, Collection, Tag
from mzitu.utils import get_logger

logger = get_logger()


class DB:
    def __init__(self, db_engine=None):
        self.db_engine = db_engine or "sqlite:///mm.db"
        logger.debug(f"db_engine：{self.db_engine}")
        try:
            engine = create_engine(self.db_engine, echo=False)
            db_session = sessionmaker(bind=engine)
            self.session = db_session()
            # 创建表（如果表已经存在，则不会创建）
            base.metadata.create_all(engine)
            logger.info("数据库已连接")
        except ImportError as e:
            if e.name == '_sqlite3':
                logger.error(f"执行 yum install sqlite-devel，之后重新编译安装Python")
            else:
                logger.error(f"请检查是否安装此模块：{e.name}")
            exit()

    def reset_dl(self):
        """重置下载记录"""
        self.session.query(DownloadRecord).all().update({DownloadRecord.dl_status: 0})
        self.session.commit()

    def report(self):
        """返回数据库中的数据统计"""
        # 合集总数
        total_nums = self.session.query(func.count(DownloadRecord.collection_num)).scalar()
        # 已获取的合集元数据数
        info_nums = self.session.query(func.count(Collection.collection_num)).scalar()
        # 已完成下载图片的合集数
        dl_nums = self.session.query(func.count(DownloadRecord.dl_status)).filter(
            DownloadRecord.dl_status == 1).scalar()
        # 合集中包含的图片总数（理论）
        picture_nums = self.session.query(func.sum(Collection.total_num)).filter().scalar()

        return total_nums, info_nums, dl_nums, picture_nums

    def get_all_collection_numbers(self):
        """返回所有记录的合集编号"""
        return [i.collection_num for i in self.session.query(DownloadRecord.collection_num).all()]

    def get_not_info_collection(self):
        """返回还未获取合集元数据的合集编号"""
        res = []
        records = self.session.query(DownloadRecord).filter_by(status=0)
        for record in records:
            res.append(record.collection_num)

        return set(res)

    def get_not_dl_collection(self):
        """返回还未下载图片的合集信息"""
        res = []
        records = self.session.query(Collection.collection_num, Collection.name, Collection.total_num,
                                     Collection.url_prefix, Collection.url_suffix) \
            .outerjoin(DownloadRecord, Collection.collection_num == DownloadRecord.collection_num) \
            .filter(DownloadRecord.status == 1, DownloadRecord.dl_status == 0)

        for record in records:
            picture_url_list = list()
            for i in range(1, int(record.total_num) + 1):
                num = "0" + str(i) if i < 10 else str(i)
                url = record.url_prefix + num + record.url_suffix
                picture_url_list.append(url)
            res.append((record.collection_num, record.name, picture_url_list))

        return res

    def add_collection_number(self, number: str):
        """单条添加合集编号"""
        self.session.add(DownloadRecord(collection_num=str(number)))
        self.session.commit()
        logger.info(f"添加一条合集编号：{number}")

    def batch_add_collection_number(self, numbers):
        """批量添加合集编号"""
        count = 0
        for number in numbers:
            # 格式校验
            if not number.isdigit():
                continue

            self.session.add(DownloadRecord(collection_num=str(number)))
            count += 1
            if count % 100 == 0:  # 对于sqlite，单次提交不能超过99条
                self.session.flush()

        self.session.commit()
        logger.info(f"添加{count}条编号")

    def add_collection_info(self, entry: dict):
        """添加合集元数据"""
        tags = []
        for tag_name in entry['tag_names']:
            t = self._get_tag(tag_name)
            if t.count() == 0:  # tag不存在
                self._add_tag(tag_name)
                tags.append(self.session.query(Tag).filter_by(tag_name=tag_name).first())
            else:  # tag 存在
                tags.append(t.first())

        del entry['tag_names']
        entry['tags'] = tags
        collection = Collection(**entry)
        self.session.add(collection)

        self._update_info_status(entry["collection_num"], 1)
        self.session.commit()

    def _add_tag(self, tag: str):
        self.session.add(Tag(tag_name=tag))
        self.session.commit()

    def _get_tag(self, tag):
        return self.session.query(Tag).filter_by(tag_name=tag)

    def get_numbers_of_not_info(self):
        """获取还未获取元数据的合集"""
        return self.session.query(DownloadRecord).filter_by(status=0)

    def get_numbers_of_not_picture(self):
        """获取还未下载图片的合集"""
        collections = []
        dl_records = self.session.query(DownloadRecord).filter_by(dl_status=0)
        for dl_record in dl_records:
            collections.extend(self.session.query(Collection).filter_by(collection_num=dl_record.collection_num).all())

        return collections

    def _update_info_status(self, collection_num, status):
        """更新合集元信息获取状态"""
        self.session.query(DownloadRecord) \
            .filter(DownloadRecord.collection_num == collection_num) \
            .update({"status": status})
        self.session.commit()
        logger.info(f"合集编号：{collection_num} 元数据入库完毕")

    def update_picture_status(self, collection_num, status):
        """更新合集图片下载状态"""
        self.session.query(DownloadRecord) \
            .filter(DownloadRecord.collection_num == collection_num) \
            .update({"dl_status": status})

        self.session.commit()
        logger.info(f"合集编号：{collection_num} 图片下载完毕")
