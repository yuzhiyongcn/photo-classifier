import os
from PIL import Image
import pillow_heif

# 输入 HEIC 文件夹路径
input_folder = r"D:\待分类照片视频"
# 输出 JPG 文件夹路径
output_folder = r"D:\待分类照片视频"

os.makedirs(output_folder, exist_ok=True)

# 遍历文件夹
for filename in os.listdir(input_folder):
    if filename.lower().endswith(".heic"):
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(
            output_folder, os.path.splitext(filename)[0] + ".jpg"
        )

        try:
            # 读取 HEIC 并转成 PIL Image
            heif_file = pillow_heif.read_heif(input_path)
            image = Image.frombytes(
                heif_file.mode, heif_file.size, heif_file.data, "raw"
            )

            # 保存为 JPG
            image.save(output_path, "JPEG", quality=95)
            print(f"转换成功: {filename} -> {output_path}")
        except Exception as e:
            print(f"转换失败: {filename}, 错误: {e}")
