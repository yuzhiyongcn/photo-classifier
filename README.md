# photo-classifier
1. 对指定目录及子目录下的照片进行分类, 先按 年/月 分目录, 文件名重命名为"年-月-时间戳"
2. 每个照片文件的md5校验码存储到oracle新加坡云的mysql数据库'photo_classifier', 用于检查照片是否重复, 重复的会跳过
3. 如果照片经过修改, 丢失EXIF信息, 将被跳过
4. 整理后的照片存放到"自动备份", 被备份到多个云盘