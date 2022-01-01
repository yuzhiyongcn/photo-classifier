'''
根据读取的照片信息分类照片
分类:
    目录名:2020\01
    文件名:2020-01-时间戳
'''

import os
from posixpath import abspath
import exifread
import time
import shutil
import hashlib
import pymysql
import datetime
import pytz
from win32com.propsys import propsys, pscon


class Classifier():
    mode = 'prod'  # 开发模式(dev)还是产品模式(prod)
    IMAGE_EXTENTIONS = ['jpg', 'jpeg', 'bmp', 'png', 'tif', 'gif']
    VIDEO_EXTENTIONS = ['mp4', 'avi', 'rmvb', 'mkv', 'mov', 'ppt', 'amr', 'mpg']
    TEST_TABLE = 'TEST_PHOTO'
    TABLE = 'PHOTO'
    PHOTO_NO_DATE_KEYS = ['EXIF ExifVersion']
    PHOTO_DATE_KEYS = ['Image DateTime', 'EXIF DateTimeOriginal']
    PHOTO_EXIF_KEYS = PHOTO_NO_DATE_KEYS + PHOTO_DATE_KEYS

    def __init__(self, input_folder, photo_output, video_output, image_output):
        self.input = input_folder
        self.photo_output = photo_output
        self.video_output = video_output
        self.image_output = image_output
        self.processed_count = 0
        self.table = self.TEST_TABLE if self.mode == 'dev' else self.TABLE
        pass

    def connect_database(self):
        self.db = pymysql.connect(host='bt.biggerfish.tech', user='admin', password='zhiyong214', database='photo_classifier')

    def close_database(self):
        self.db.close()

    def create_table(self):
        self.connect_database()
        cursor = self.db.cursor()

        sql = 'DROP TABLE IF EXISTS {}'.format(self.table)
        cursor.execute(sql)
        print('删除表 {}'.format(self.table))

        sql = '''CREATE TABLE {} (
            ID INT NOT NULL AUTO_INCREMENT ,
            MD5 VARCHAR(255) NOT NULL ,
            PRIMARY KEY (ID), UNIQUE (MD5))
            ENGINE = InnoDB;'''.format(self.table)
        cursor.execute(sql)
        print('创建表 {}'.format(self.table))

        self.close_database()

    def start(self):
        self.connect_database()
        self.process_folder(self.input)
        self.delete_folders(self.input)
        self.close_database()

    def get_file_count(self, folder):
        count = 0
        for (_, _, _files) in os.walk(folder):
            count += len(_files)
        return count

    def delete_folders(self, folder):
        for (root, dirs, files) in os.walk(folder):
            for dir in dirs:
                abs_path = os.path.join(root, dir)
                if os.path.isdir(abs_path):
                    if self.get_file_count(abs_path) == 0:
                        shutil.rmtree(abs_path)
                        print('删除目录: {}'.format(abs_path))

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
        with open(file_name, 'rb') as reader:
            tags = exifread.process_file(reader)
            keys = [key for key in self.PHOTO_EXIF_KEYS if key in tags]
            return len(keys) > 0

    def process_folder(self, folder):
        for (root, dirs, files) in os.walk(folder):
            for file in files:
                self.process_file(root, file)

    def get_md5(self, file):
        with open(file, 'rb') as reader:
            return hashlib.md5(reader.read()).hexdigest()

    def process_file(self, root, file):
        file_path = os.path.join(root, file)
        md5 = self.get_md5(file_path)
        try:
            self.validate(file_path, md5)
            year, month = self.read_date(file_path)
            new_name = self.rename_move(file_path, year, month)
            self.add_record(md5)
            self.processed_count += 1
            print('已处理 {}: {} --> {}'.format(self.processed_count, file, new_name))
        except Exception as e:
            print(str(e))

    def add_record(self, md5):
        try:
            cursor = self.db.cursor()
            sql = "INSERT INTO {}(MD5) VALUES('{}')".format(self.table, md5)
            cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print('插入记录 {} 到数据库photo_classifier失败: {}'.format(md5, str(e)))
            self.db.rollback()
            raise e

    def validate(self, file_path, md5):
        # check if the md5 of the photo exists in database
        try:
            cursor = self.db.cursor()
            sql = "SELECT MD5 FROM {} WHERE MD5='{}'".format(self.table, md5)
            cursor.execute(sql)
            record = cursor.fetchone()
            if str(record) != 'None':
                os.remove(file_path)
                raise Exception('重复文件 {} --> 删除'.format(file_path))
        except Exception as e:
            raise e

        if (not self.is_image(file_path)) and (not self.is_video(file_path)):
            raise Exception('非图片或视频: {} --> 跳过'.format(file_path))

    def get_photo_create_date(self, file):
        with open(file, 'rb') as reader:
            tags = exifread.process_file(reader)
            keys = [key for key in self.PHOTO_DATE_KEYS if key in tags]
            if len(keys) > 0:
                key = keys[0]
                origin_date = tags[key]
                time_str = str(origin_date)
                _date = time_str[:7].split(':')
                year = _date[0]
                month = _date[1]
                return (year, month)
        return None

    def get_video_create_date(self, file):
        try:
            properties = propsys.SHGetPropertyStoreFromParsingName(file)
            dt = properties.GetValue(pscon.PKEY_Media_DateEncoded).GetValue()
            time_str = str(dt.astimezone(pytz.timezone('Asia/Shanghai')))
            _date = time_str[:7].split('-')
            year = _date[0]
            month = _date[1]
            return (year, month)
        except:
            return None

    def read_date(self, file):
        file = file.replace('/', '\\')
        date = None
        if self.is_photo(file):
            date = self.get_photo_create_date(file)  # 照片可能没有EXIF日期
        elif self.is_video(file):
            date = self.get_video_create_date(file)  # 视频可能没有媒体创建日期

        if not date:  # 获取文件上次修改日期
            time_str = os.path.getmtime(file)
            time_str = str(datetime.datetime.fromtimestamp(time_str))
            _date = time_str[:7].split('-')
            year = _date[0]
            month = _date[1]
            date = (year, month)
        return date

    def rename_move(self, file_path, year, month):
        if self.is_image(file_path):
            if self.is_photo(file_path):
                output = self.photo_output
            else:
                output = self.image_output
        elif self.is_video(file_path):
            output = self.video_output
        else:
            raise Exception('移动文件失败, 非图片或视频: {}'.format(file_path))

        new_path = os.path.join(output, year, month)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        file_name, file_ext = os.path.splitext(file_path)
        new_name = year + '-' + month + '-' + str(time.time()) + file_ext
        shutil.move(file_path, os.path.join(new_path, new_name))
        return new_name


cf = Classifier(input_folder='D:/temp/相册',
                photo_output='D:/总仓库-照片视频/总照片备份',
                video_output='D:/总仓库-照片视频/总视频备份',
                image_output='D:/总仓库-照片视频/总图片备份')

# cf.create_table()
cf.start()