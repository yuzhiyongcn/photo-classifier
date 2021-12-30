'''
根据读取的照片信息分类照片
分类:
    目录名:2020\01
    文件名:2020-01-时间戳
处理过的文件名存为json文件: processed_files.json
如果照片经过修改, 丢失原始EXIF信息, 将会被跳过
'''

import os
from posixpath import abspath
import exifread
import time
import shutil
import hashlib
import pymysql
import datetime


class Classifier():

    def __init__(self, input_folder, output_folder):
        self.input = input_folder
        self.output = output_folder
        self.processed_count = 0
        self.db = pymysql.connect(host='bt.biggerfish.tech', user='admin', password='zhiyong214', database='photo_classifier')
        self.photo_info_keys = ['Image DateTime', 'EXIF DateTimeOriginal', 'EXIF ExifVersion']
        self.key_without_date = 'EXIF ExifVersion'
        pass

    def start(self):
        self.process_folder(self.input)
        self.db.close()
        self.delete_folders(self.input)
        
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
        for ext in ['jpg', 'jpeg', 'bmp', 'png']:
            if file_name.lower().endswith(ext):
                return True
        return False

    def process_folder(self, folder):
        for (root, dirs, files) in os.walk(folder):
            for file in files:
                if self.is_photo(file):
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
            print('已处理照片 {}: {} --> {}'.format(self.processed_count, file, new_name))
        except Exception as e:
            print(str(e))
            
    def add_record(self, md5):
        try:
            cursor = self.db.cursor()
            sql = "INSERT INTO photo_md5(MD5) VALUES('{}')".format(md5)
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
            sql = "SELECT MD5 FROM photo_md5 WHERE MD5='{}'".format(md5)
            cursor.execute(sql)
            record = cursor.fetchone()
            if str(record) != 'None':
                os.remove(file_path)
                raise Exception('重复照片 {} --> 删除'.format(file_path))
        except Exception as e:
            raise e
        
        # check if image is photo or not
        with open(file_path, 'rb') as reader:
            tags = exifread.process_file(reader)
            keys = [key for key in self.photo_info_keys if key in tags]
            if len(keys) == 0:
                raise Exception('图片不是照片: {} --> 跳过'.format(file_path))
        

    def read_date(self, file):
        with open(file, 'rb') as reader:
            tags = exifread.process_file(reader)
            keys = [key for key in self.photo_info_keys if key in tags]
            if len(keys) > 0:
                key = keys[0]
                if key != self.key_without_date:
                    origin_date = tags[key]
                    time_str = str(origin_date)
                    _date = time_str[:7].split(':')
                    year = _date[0]
                    month = _date[1]
                    return (year, month)
                else: # 是拍摄照片, 但没有拍摄日期
                    time_str = os.path.getmtime(file)
                    time_str = str(datetime.datetime.fromtimestamp(time_str))
                    _date = time_str[:7].split('-')
                    year = _date[0]
                    month = _date[1]
                    return (year, month)
                    
        

    def rename_move(self, file_path, year, month):
        new_path = os.path.join(self.output, year, month)
        if not os.path.exists(new_path):
            os.makedirs(new_path)
        file_name, file_ext = os.path.splitext(file_path)
        new_name = year + '-' + month + '-' + str(time.time()) + file_ext
        shutil.move(file_path, os.path.join(new_path, new_name))
        return new_name

cf = Classifier('D:/temp/相册', 'D:/自动同步/总相册')
cf.start()