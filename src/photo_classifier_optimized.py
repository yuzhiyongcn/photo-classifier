"""
Photo Classifier - Optimized Version
Automatically classifies and organizes photos and videos based on creation date
with enhanced error handling, logging, and configuration management.
"""

import os
import sys
import json
import argparse
import logging
import exifread
import time
import shutil
import hashlib
import sqlite3
import datetime
import pytz
from pathlib import Path
from typing import Tuple, Optional, Dict, Any, List
from win32com.propsys import propsys, pscon


class ConfigManager:
    """Configuration management class for loading and validating settings"""
    
    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def _validate_config(self) -> None:
        """Validate required configuration keys"""
        required_keys = ['paths', 'supported_formats', 'database']
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required configuration key: {key}")
    
    def get(self, key_path: str, default=None):
        """Get configuration value by dot-separated key path"""
        keys = key_path.split('.')
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value


class PhotoClassifierOptimized:
    """Optimized photo classifier with enhanced error handling and logging"""
    
    def __init__(self, config_path: str = "config.json"):
        # Load configuration
        self.config = ConfigManager(config_path)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize paths from config
        self.input_folder = self.config.get('paths.input_folder')
        self.photo_output = self.config.get('paths.photo_output')
        self.video_output = self.config.get('paths.video_output')
        self.image_output = self.config.get('paths.image_output')
        
        # Initialize file extensions
        self.image_extensions = self.config.get('supported_formats.image_extensions', [])
        self.video_extensions = self.config.get('supported_formats.video_extensions', [])
        
        # Initialize database settings
        self.table_name = self.config.get('database.table_name', 'PHOTO')
        self.db_dir = self.config.get('paths.database_dir', 'database')
        self.db_file = self.config.get('paths.database_file', 'photo_classifier.db')
        
        # Initialize EXIF keys
        self.photo_no_date_keys = self.config.get('exif.photo_no_date_keys', [])
        self.photo_date_keys = self.config.get('exif.photo_date_keys', [])
        self.photo_exif_keys = self.photo_no_date_keys + self.photo_date_keys
        
        # Other settings
        self.skip_folders = self.config.get('skip_folders', [])
        self.timezone = self.config.get('timezone', 'Asia/Shanghai')
        self.min_file_size = self.config.get('performance.min_file_size', 1024)
        
        # Initialize counters
        self.processed_count = 0
        self.error_count = 0
        self.duplicate_count = 0
        
        # Database connection
        self.db = None
        self.db_path = os.path.join(self.db_dir, self.db_file)
        
        # Validate configuration
        self._validate_paths()
        
        self.logger.info("照片分类器初始化成功")
    
    def _setup_logging(self) -> None:
        """Setup logging configuration"""
        log_level = self.config.get('logging.level', 'INFO')
        log_format = self.config.get('logging.format', 
                                   '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_file = self.config.get('logging.file', 'photo_classifier.log')
        
        # Create logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(log_format)
        
        # File handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def _validate_paths(self) -> None:
        """Validate that required paths exist or can be created"""
        paths_to_check = [
            ('input_folder', self.input_folder, True),  # Must exist
            ('photo_output', self.photo_output, False),  # Can be created
            ('video_output', self.video_output, False),  # Can be created
            ('image_output', self.image_output, False),  # Can be created
        ]
        
        for path_name, path_value, must_exist in paths_to_check:
            if not path_value:
                raise ValueError(f"Path not configured: {path_name}")
            
            if must_exist and not os.path.exists(path_value):
                raise FileNotFoundError(f"Required path does not exist: {path_value}")
            
            if not must_exist:
                try:
                    os.makedirs(path_value, exist_ok=True)
                    self.logger.info(f"创建输出目录: {path_value}")
                except OSError as e:
                    raise OSError(f"Cannot create directory {path_value}: {e}")
    
    def connect_database(self) -> None:
        """Connect to SQLite database with error handling"""
        try:
            # Ensure database directory exists
            os.makedirs(self.db_dir, exist_ok=True)
            
            self.db = sqlite3.connect(self.db_path)
            self.db.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints
            self.logger.info(f"连接到数据库: {self.db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"连接数据库失败: {e}")
            raise
    
    def close_database(self) -> None:
        """Close database connection safely"""
        if self.db:
            try:
                self.db.close()
                self.logger.info("数据库连接已关闭")
            except sqlite3.Error as e:
                self.logger.error(f"关闭数据库错误: {e}")
    
    def create_table(self) -> None:
        """Create database table with enhanced error handling"""
        try:
            self.connect_database()
            cursor = self.db.cursor()
            
            # Drop existing table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            self.logger.info(f"删除现有表: {self.table_name}")
            
            # Create new table with additional metadata
            sql = f"""CREATE TABLE {self.table_name} (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                MD5 TEXT NOT NULL UNIQUE,
                ORIGINAL_PATH TEXT,
                NEW_PATH TEXT,
                FILE_SIZE INTEGER,
                CREATED_DATE TEXT,
                PROCESSED_DATE TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
            cursor.execute(sql)
            
            # Create index for better performance
            cursor.execute(f"CREATE INDEX idx_md5 ON {self.table_name}(MD5)")
            
            self.db.commit()
            self.logger.info(f"创建表: {self.table_name}")
            
        except sqlite3.Error as e:
            self.logger.error(f"创建表失败: {e}")
            if self.db:
                self.db.rollback()
            raise
        finally:
            self.close_database()
    
    def _drop_table(self) -> None:
        """Drop database table"""
        try:
            self.connect_database()
            cursor = self.db.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            self.db.commit()
            self.logger.info(f"删除表: {self.table_name}")
        except sqlite3.Error as e:
            self.logger.error(f"删除表失败: {e}")
            raise
        finally:
            self.close_database()
    
    def _show_db_info(self) -> None:
        """Show database information"""
        try:
            self.connect_database()
            cursor = self.db.cursor()
            
            print("=" * 50)
            print("数据库信息")  
            print("=" * 50)
            print(f"📍 数据库文件: {self.db_path}")
            print(f"📋 表名: {self.table_name}")
            
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.table_name,))
            if cursor.fetchone():
                # Get record count
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                count = cursor.fetchone()[0]
                print(f"📊 记录数量: {count}")
                
                # Get table schema
                cursor.execute(f"PRAGMA table_info({self.table_name})")
                columns = cursor.fetchall()
                print("🏗️  表结构:")
                for col in columns:
                    print(f"   {col[1]} ({col[2]})")
            else:
                print("❌ 表不存在")
            print("=" * 50)
            
        except sqlite3.Error as e:
            print(f"❌ 数据库错误: {e}")
        finally:
            self.close_database()
    
    def _list_records(self, limit: int = 10) -> None:
        """List recent records from database"""
        try:
            self.connect_database()
            cursor = self.db.cursor()
            
            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.table_name,))
            if not cursor.fetchone():
                print("❌ 数据库表不存在，请先运行 --create-table")
                return
            
            # Get recent records
            cursor.execute(f"SELECT MD5, ORIGINAL_PATH, CREATED_DATE, PROCESSED_DATE FROM {self.table_name} ORDER BY ID DESC LIMIT ?", (limit,))
            records = cursor.fetchall()
            
            print("=" * 80)
            print(f"最近 {len(records)} 条记录")
            print("=" * 80)
            
            if records:
                print(f"{'MD5':<32} {'创建日期':<12} {'处理时间':<20} {'原始路径'}")
                print("-" * 80)
                for record in records:
                    md5, original_path, created_date, processed_date = record
                    # Truncate long paths
                    display_path = (original_path[:30] + "...") if len(original_path) > 33 else original_path
                    print(f"{md5[:8]:<8}...{md5[-8:]} {created_date:<12} {processed_date[:19]:<20} {display_path}")
            else:
                print("📭 数据库为空")
            print("=" * 80)
            
        except sqlite3.Error as e:
            print(f"❌ 数据库错误: {e}")
        finally:
            self.close_database()
    
    def is_valid_file_size(self, file_path: str) -> bool:
        """Check if file size meets minimum requirements"""
        try:
            file_size = os.path.getsize(file_path)
            return file_size >= self.min_file_size
        except OSError:
            return False
    
    def is_photo(self, file_path: str) -> bool:
        """Check if file is a photo with EXIF data"""
        return self.is_image(file_path) and self.contains_exif(file_path)
    
    def is_video(self, file_path: str) -> bool:
        """Check if file is a video"""
        return any(file_path.lower().endswith(ext) for ext in self.video_extensions)
    
    def is_image(self, file_path: str) -> bool:
        """Check if file is an image"""
        return any(file_path.lower().endswith(ext) for ext in self.image_extensions)
    
    def contains_exif(self, file_path: str) -> bool:
        """Check if image contains EXIF data"""
        try:
            with open(file_path, "rb") as reader:
                tags = exifread.process_file(reader)
                return any(key in tags for key in self.photo_exif_keys)
        except (IOError, OSError) as e:
            self.logger.warning(f"无法从 {file_path} 读取EXIF: {e}")
            return False
    
    def get_md5(self, file_path: str) -> str:
        """Calculate MD5 hash of file with error handling"""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except (IOError, OSError) as e:
            self.logger.error(f"计算 {file_path} 的MD5失败: {e}")
            raise
    
    def validate_file(self, file_path: str, md5: str) -> None:
        """Validate file and check for duplicates"""
        try:
            cursor = self.db.cursor()
            cursor.execute(f"SELECT MD5, ORIGINAL_PATH FROM {self.table_name} WHERE MD5=?", (md5,))
            record = cursor.fetchone()
            
            if record:
                original_path = record[1] if record[1] else "Unknown"
                self.logger.warning(f"发现重复文件: {file_path} (原始: {original_path})")
                os.remove(file_path)
                self.duplicate_count += 1
                raise ValueError(f"Duplicate file removed: {file_path}")
                
        except sqlite3.Error as e:
            self.logger.error(f"验证期间数据库错误: {e}")
            raise
        except OSError as e:
            self.logger.error(f"删除重复文件 {file_path} 失败: {e}")
            raise
    
    def get_photo_create_date(self, file_path: str) -> Optional[Tuple[str, str, str]]:
        """Extract creation date from photo EXIF data"""
        try:
            with open(file_path, "rb") as reader:
                tags = exifread.process_file(reader)
                for key in self.photo_date_keys:
                    if key in tags:
                        time_str = str(tags[key])
                        if ':' in time_str[:10]:
                            date_parts = time_str[:10].split(":")
                            if len(date_parts) == 3:
                                return tuple(date_parts)
            return None
        except (IOError, OSError) as e:
            self.logger.warning(f"无法从 {file_path} 读取照片日期: {e}")
            return None
    
    def get_video_create_date(self, file_path: str) -> Optional[Tuple[str, str, str]]:
        """Extract creation date from video metadata"""
        try:
            properties = propsys.SHGetPropertyStoreFromParsingName(file_path)
            dt = properties.GetValue(pscon.PKEY_Media_DateEncoded).GetValue()
            if dt:
                time_str = str(dt.astimezone(pytz.timezone(self.timezone)))
                date_parts = time_str[:10].split("-")
                if len(date_parts) == 3:
                    return tuple(date_parts)
            return None
        except Exception as e:
            self.logger.warning(f"无法从 {file_path} 读取视频日期: {e}")
            return None
    
    def read_date(self, file_path: str) -> Tuple[str, str, str]:
        """Read creation date from file with fallback to modification time"""
        file_path = file_path.replace("/", "\\")
        date = None
        
        if self.is_photo(file_path):
            date = self.get_photo_create_date(file_path)
        elif self.is_video(file_path):
            date = self.get_video_create_date(file_path)
        
        if not date:
            # Fallback to file modification time
            try:
                mtime = os.path.getmtime(file_path)
                dt = datetime.datetime.fromtimestamp(mtime)
                date = (str(dt.year), f"{dt.month:02d}", f"{dt.day:02d}")
                self.logger.info(f"使用文件 {file_path} 的修改日期")
            except OSError as e:
                self.logger.error(f"无法获取文件 {file_path} 的修改时间: {e}")
                # Use current date as last resort
                now = datetime.datetime.now()
                date = (str(now.year), f"{now.month:02d}", f"{now.day:02d}")
        
        return date
    
    def rename_move(self, file_path: str, year: str, month: str, day: str, md5: str) -> str:
        """Rename and move file to appropriate directory"""
        # Determine output directory
        if self.is_image(file_path):
            output_dir = self.photo_output if self.is_photo(file_path) else self.image_output
        elif self.is_video(file_path):
            output_dir = self.video_output
        else:
            raise ValueError(f"Unsupported file type: {file_path}")
        
        # Create target directory
        target_dir = os.path.join(output_dir, year, month, day)
        os.makedirs(target_dir, exist_ok=True)
        
        # Generate new filename
        _, file_ext = os.path.splitext(file_path)
        new_name = f"{year}-{month}-{day}-{md5}{file_ext}"
        target_path = os.path.join(target_dir, new_name)
        
        # Handle filename conflicts
        counter = 1
        while os.path.exists(target_path):
            base_name = f"{year}-{month}-{day}-{md5}_{counter}{file_ext}"
            target_path = os.path.join(target_dir, base_name)
            counter += 1
        
        # Move file
        try:
            shutil.move(file_path, target_path)
            self.logger.info(f"移动: {file_path} -> {target_path}")
            return os.path.basename(target_path)
        except (IOError, OSError) as e:
            self.logger.error(f"移动文件 {file_path} 到 {target_path} 失败: {e}")
            raise
    
    def add_record(self, md5: str, original_path: str, new_path: str, file_size: int, created_date: str) -> None:
        """Add file record to database with enhanced metadata"""
        try:
            cursor = self.db.cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name}(MD5, ORIGINAL_PATH, NEW_PATH, FILE_SIZE, CREATED_DATE) VALUES(?,?,?,?,?)",
                (md5, original_path, new_path, file_size, created_date)
            )
            self.db.commit()
        except sqlite3.Error as e:
            self.logger.error(f"添加记录 {md5} 失败: {e}")
            self.db.rollback()
            raise
    
    def process_file(self, root: str, filename: str) -> None:
        """Process a single file with comprehensive error handling"""
        file_path = os.path.join(root, filename)
        
        try:
            # Check file size
            if not self.is_valid_file_size(file_path):
                self.logger.debug(f"文件太小，跳过: {file_path}")
                return
            
            # Check if it's a supported file type
            if not (self.is_image(file_path) or self.is_video(file_path)):
                self.logger.debug(f"不支持的文件类型，跳过: {file_path}")
                return
            
            # Calculate MD5
            md5 = self.get_md5(file_path)
            file_size = os.path.getsize(file_path)
            
            # Validate (check for duplicates)
            self.validate_file(file_path, md5)
            
            # Extract date
            year, month, day = self.read_date(file_path)
            created_date = f"{year}-{month}-{day}"
            
            # Move and rename file
            new_name = self.rename_move(file_path, year, month, day, md5)
            new_path = os.path.join(
                self.photo_output if self.is_photo(file_path) else 
                (self.video_output if self.is_video(file_path) else self.image_output),
                year, month, day, new_name
            )
            
            # Add record to database
            self.add_record(md5, file_path, new_path, file_size, created_date)
            
            self.processed_count += 1
            self.logger.info(f"已处理 ({self.processed_count}): {filename} -> {new_name}")
            
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"处理 {file_path} 错误: {e}")
    
    def process_folder(self, folder: str) -> None:
        """Process all files in folder recursively"""
        self.logger.info(f"开始处理文件夹: {folder}")
        
        for root, dirs, files in os.walk(folder):
            # Skip system folders
            dirs[:] = [d for d in dirs if d not in self.skip_folders]
            
            for filename in files:
                self.process_file(root, filename)
    
    def delete_empty_folders(self, folder: str) -> None:
        """Delete empty folders after processing"""
        deleted_count = 0
        for root, dirs, files in os.walk(folder, topdown=False):
            for dir_name in dirs:
                if dir_name in self.skip_folders:
                    continue
                    
                dir_path = os.path.join(root, dir_name)
                try:
                    if not os.listdir(dir_path):  # Check if directory is empty
                        os.rmdir(dir_path)
                        deleted_count += 1
                        self.logger.info(f"删除空目录: {dir_path}")
                except OSError as e:
                    self.logger.warning(f"无法删除目录 {dir_path}: {e}")
        
        self.logger.info(f"删除了 {deleted_count} 个空目录")
    
    def generate_report(self) -> None:
        """Generate processing report"""
        self.logger.info("=" * 50)
        self.logger.info("处理报告")
        self.logger.info("=" * 50)
        self.logger.info(f"已处理文件: {self.processed_count}")
        self.logger.info(f"发现重复: {self.duplicate_count}")
        self.logger.info(f"遇到错误: {self.error_count}")
        self.logger.info("=" * 50)
    
    def start(self) -> None:
        """Start the classification process"""
        start_time = time.time()
        self.logger.info("开始照片分类处理")
        
        try:
            self.connect_database()
            self.process_folder(self.input_folder)
            self.delete_empty_folders(self.input_folder)
            
        except Exception as e:
            self.logger.error(f"处理期间致命错误: {e}")
            raise
        finally:
            self.close_database()
            
        end_time = time.time()
        duration = end_time - start_time
        self.logger.info(f"处理完成，用时 {duration:.2f} 秒")
        self.generate_report()



def main():
    """Main function with command line argument support"""
    parser = argparse.ArgumentParser(description="Photo Classifier - Optimized Version")
    parser.add_argument("--config", default="config.json", help="Configuration file path")
    parser.add_argument("--create-table", action="store_true", help="Create/recreate database table")
    parser.add_argument("--drop-table", action="store_true", help="Drop existing database table")
    parser.add_argument("--list-records", action="store_true", help="List all records in database")
    parser.add_argument("--db-info", action="store_true", help="Show database information")
    parser.add_argument("--input", help="Override input folder from config")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    try:
        # Initialize classifier
        classifier = PhotoClassifierOptimized(args.config)
        
        # Override input folder if provided
        if args.input:
            classifier.input_folder = args.input
            classifier.logger.info(f"输入文件夹已覆盖为: {args.input}")
        
        # Set verbose logging if requested
        if args.verbose:
            classifier.logger.setLevel(logging.DEBUG)
        
        # Handle database operations
        if args.create_table:
            print("正在创建/重建数据库表...")
            classifier.create_table()
            print("✅ 数据库表创建完成")
            print(f"📍 数据库位置: {classifier.db_path}")
            print(f"📋 表名: {classifier.table_name}")
            return
        
        if args.drop_table:
            print("正在删除数据库表...")
            classifier._drop_table()
            print("✅ 数据库表删除完成")
            return
        
        if args.db_info:
            classifier._show_db_info()
            return
        
        if args.list_records:
            classifier._list_records()
            return
        
        # Start processing
        classifier.start()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # 🎯 IDE开发模式：直接在代码中设置参数，无需命令行输入
    import sys
    
    # 💡 取消注释你需要的功能（只能同时启用一个）：
    
    # sys.argv = ["script_name", "--create-table"]              # 创建数据库表
    # sys.argv = ["script_name", "--db-info"]                   # 查看数据库信息  
    # sys.argv = ["script_name", "--list-records"]              # 查看最近记录
    # sys.argv = ["script_name", "--drop-table"]                # 删除数据库表
    # sys.argv = ["script_name", "--verbose"]                   # 启用详细日志
    # sys.argv = ["script_name", "--input", "D:\\test\\input"]  # 自定义输入目录
    # sys.argv = ["script_name", "--config", "my_config.json"]  # 自定义配置文件
    
    # 默认运行：照片分类处理（注释掉上面所有选项时使用）
    # sys.argv = ["script_name"]  
    
    main() 