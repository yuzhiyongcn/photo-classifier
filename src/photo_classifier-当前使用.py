"""
根据读取的照片信息分类照片
分类:
    目录名:2020\01
    文件名:2020-01-时间戳
"""

import os
import sys
from posixpath import abspath
import exifread
import time
import shutil
import hashlib
import sqlite3
import datetime
import pytz
from win32com.propsys import propsys, pscon

# 使用SQLite数据库


class Classifier:
    mode = "prod"  # 开发模式(dev)还是产品模式(prod)
    IMAGE_EXTENTIONS = ["jpg", "jpeg", "bmp", "png", "tif", "gif", "heic"]
    VIDEO_EXTENTIONS = ["mp4", "avi", "rmvb", "mkv", "mov", "amr", "mpg"]
    TEST_TABLE = "TEST_PHOTO"
    TABLE = "PHOTO"
    PHOTO_NO_DATE_KEYS = ["EXIF ExifVersion"]
    PHOTO_DATE_KEYS = ["Image DateTime", "EXIF DateTimeOriginal"]
    PHOTO_EXIF_KEYS = PHOTO_NO_DATE_KEYS + PHOTO_DATE_KEYS
    SKIP_FOLDERS = ["System Volume Information", "$RECYCLE.BIN", ".stfolder"]

    def __init__(self, input_folder, photo_output, video_output, image_output):
        self.input = input_folder
        self.photo_output = photo_output
        self.video_output = video_output
        self.image_output = image_output
        self.processed_count = 0
        self.table = self.TEST_TABLE if self.mode == "dev" else self.TABLE
        # Ensure database directory exists
        self.db_dir = "database"
        if not os.path.exists(self.db_dir):
            os.makedirs(self.db_dir)
        self.db_path = os.path.join(self.db_dir, "photo_classifier.db")
        pass

    def connect_database(self):
        # Connect to SQLite database
        self.db = sqlite3.connect(self.db_path)

    def close_database(self):
        self.db.close()

    def create_table(self):
        self.connect_database()
        cursor = self.db.cursor()

        sql = "DROP TABLE IF EXISTS {}".format(self.table)
        cursor.execute(sql)
        print("删除表 {}".format(self.table))

        # SQLite syntax for table creation
        sql = """CREATE TABLE {} (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            MD5 TEXT NOT NULL UNIQUE
        )""".format(self.table)
        cursor.execute(sql)
        print("创建表 {}".format(self.table))

        self.db.commit()
        self.close_database()

    def start(self):
        self.connect_database()
        self.process_folder(self.input)
        self.delete_folders(self.input)
        self.close_database()

    def get_file_count(self, folder):
        count = 0
        for _, _, _files in os.walk(folder):
            count += len(_files)
        return count

    def delete_folders(self, folder):
        for root, dirs, files in os.walk(folder):
            for dir in dirs:
                if dir in self.SKIP_FOLDERS:
                    continue
                abs_path = os.path.join(root, dir)
                if os.path.isdir(abs_path):
                    if self.get_file_count(abs_path) == 0:
                        shutil.rmtree(abs_path)
                        print("删除目录: {}".format(abs_path))

    def is_photo(self, file_name):
        return self.is_image(file_name) and self.contains_exif(file_name)

    def is_video(self, file_name):
        for ext in self.VIDEO_EXTENTIONS:
            if file_name.lower().endswith(ext):
                return True
        return False

    def is_image(self, file_name):
        for ext in self.IMAGE_EXTENTIONS:
            if file_name.lower().endswith(ext):
                return True
        return False

    def contains_exif(self, file_name):
        with open(file_name, "rb") as reader:
            tags = exifread.process_file(reader)
            keys = [key for key in self.PHOTO_EXIF_KEYS if key in tags]
            return len(keys) > 0

    def process_folder(self, folder):
        for root, dirs, files in os.walk(folder):
            for file in files:
                self.process_file(root, file)

    def get_md5(self, file):
        with open(file, "rb") as reader:
            return hashlib.md5(reader.read()).hexdigest()

    def process_file(self, root, file):
        file_path = os.path.join(root, file)
        if self.is_image(file_path) or self.is_video(file_path):
            md5 = self.get_md5(file_path)
            try:
                self.validate(file_path, md5)
                year, month, day = self.read_date(file_path)
                new_name = self.rename_move(file_path, year, month, day, md5)
                self.add_record(md5)
                self.processed_count += 1
                print(
                    "已处理 {}: {} --> {}".format(self.processed_count, file, new_name)
                )
            except Exception as e:
                print(str(e))
        else:
            print("非图片或视频, 忽略文件: {}".format(file_path))

    def add_record(self, md5):
        try:
            cursor = self.db.cursor()
            # Use parameterized query for SQLite
            sql = "INSERT INTO {}(MD5) VALUES(?)".format(self.table)
            cursor.execute(sql, (md5,))
            self.db.commit()
        except Exception as e:
            print("插入记录 {} 到数据库photo_classifier失败: {}".format(md5, str(e)))
            self.db.rollback()
            raise e

    def validate(self, file_path, md5):
        # check if the md5 of the photo exists in database
        try:
            cursor = self.db.cursor()
            # Use parameterized query for SQLite
            sql = "SELECT MD5 FROM {} WHERE MD5=?".format(self.table)
            cursor.execute(sql, (md5,))
            record = cursor.fetchone()
            if record is not None:
                os.remove(file_path)
                raise Exception("重复文件 {} --> 删除".format(file_path))
        except Exception as e:
            raise e

        if (not self.is_image(file_path)) and (not self.is_video(file_path)):
            raise Exception("非图片或视频: {} --> 跳过".format(file_path))

    def get_photo_create_date(self, file):
        with open(file, "rb") as reader:
            tags = exifread.process_file(reader)
            keys = [key for key in self.PHOTO_DATE_KEYS if key in tags]
            if len(keys) > 0:
                key = keys[0]
                origin_date = tags[key]
                time_str = str(origin_date)
                _date = time_str[:10].split(":")
                year = _date[0]
                month = _date[1]
                day = _date[2]
                return (year, month, day)
        return None

    def get_video_create_date(self, file):
        try:
            properties = propsys.SHGetPropertyStoreFromParsingName(file)
            dt = properties.GetValue(pscon.PKEY_Media_DateEncoded).GetValue()
            time_str = str(dt.astimezone(pytz.timezone("Asia/Shanghai")))
            _date = time_str[:10].split("-")
            year = _date[0]
            month = _date[1]
            day = _date[2]
            return (year, month, day)
        except:
            return None

    def read_date(self, file):
        file = file.replace("/", "\\")
        date = None
        if self.is_photo(file):
            date = self.get_photo_create_date(file)  # 照片可能没有EXIF日期
        elif self.is_video(file):
            date = self.get_video_create_date(file)  # 视频可能没有媒体创建日期

        if not date:  # 获取文件上次修改日期
            time_str = os.path.getmtime(file)
            time_str = str(datetime.datetime.fromtimestamp(time_str))
            _date = time_str[:10].split("-")
            year = _date[0]
            month = _date[1]
            day = _date[2]
            date = (year, month, day)
        return date

    def rename_move(self, file_path, year, month, day, md5):
        if self.is_image(file_path):
            if self.is_photo(file_path):
                output = self.photo_output
            else:
                output = self.image_output
        elif self.is_video(file_path):
            output = self.video_output
        else:
            raise Exception("移动文件失败, 非图片或视频: {}".format(file_path))

        new_path = os.path.join(output, year, month, day)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        file_name, file_ext = os.path.splitext(file_path)
        new_name = year + "-" + month + "-" + day + "-" + md5 + file_ext
        shutil.move(file_path, os.path.join(new_path, new_name))
        return new_name


cf = Classifier(
    input_folder="D:/待分类照片视频",
    # input_folder="D:/down/需整理"
    # input_folder="D:/总仓库-照片视频-bak",
    photo_output="D:/总仓库-照片视频/总照片备份",
    video_output="D:/总仓库-照片视频/总视频备份",
    image_output="D:/总仓库-照片视频/总图片备份",
)

cf.start()
# cf.create_table()
