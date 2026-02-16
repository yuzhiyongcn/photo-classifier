import os
from PIL import Image
import pillow_heif

# 注册 HEIF 支持（关键！！！）
pillow_heif.register_heif_opener()

input_folder = r"D:\temp"
output_folder = r"D:\temp"

os.makedirs(output_folder, exist_ok=True)

for filename in os.listdir(input_folder):
    if filename.lower().endswith(".heic"):
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(
            output_folder, os.path.splitext(filename)[0] + ".jpg"
        )

        try:
            # 直接用 PIL 打开（自动支持 HEIC）
            with Image.open(input_path) as img:
                # 转 RGB（防止 RGBA / P 模式导致 JPG 保存失败）
                img = img.convert("RGB")

                img.save(output_path, "JPEG", quality=95)

            print(f"转换成功: {filename} -> {output_path}")

        except Exception as e:
            print(f"转换失败: {filename}, 错误: {e}")
