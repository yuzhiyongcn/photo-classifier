# 照片分类器-python3
1. 对指定目录及子目录下的照片进行分类, 先按 年/月 分目录, 文件名重命名为"年-月-时间戳"
2. 每个照片文件的md5校验码存储到oracle新加坡云的mysql数据库'photo_classifier', 表名'photo', 用于检查照片是否重复, 重复的会跳过
3. 可以处理视频, 照片, 非照片的图片
4. 整理后的照片存放到"总仓库-照片视频", 被备份到多个云盘