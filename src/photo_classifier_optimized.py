"""
Photo Classifier - Multithreaded Optimized Version with Fast Pre-check
自动根据创建日期分类和整理照片视频，支持多线程处理和快速预检查避免重复计算hash

Features:
- 🚀 Multi-threaded processing for improved performance
- ⚡ Fast pre-check using file size + date to skip duplicate MD5 calculations
- 🔒 Thread-safe database operations with WAL mode
- 📊 Detailed statistics and progress reporting
- 🛡️  Comprehensive error handling and logging
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
import threading
import queue
from pathlib import Path
from typing import Tuple, Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from win32com.propsys import propsys, pscon


@dataclass
class FileProcessResult:
    """文件处理结果"""

    md5: str
    file_size: int
    file_type: str
    created_date: str
    original_path: str
    new_path: str
    success: bool
    error_message: Optional[str] = None


class ThreadSafeDatabase:
    """线程安全的数据库管理器"""

    def __init__(self, db_path: str, table_name: str):
        self.db_path = db_path
        self.table_name = table_name
        self._local = threading.local()
        self._write_lock = threading.Lock()  # 写操作锁
        self._setup_database()

    def _setup_database(self):
        """设置数据库（启用WAL模式提升并发性能）"""
        try:
            conn = sqlite3.connect(self.db_path)
            # Enable WAL mode for better concurrent access
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.close()
        except sqlite3.Error as e:
            print(f"数据库设置失败: {e}")

    def get_connection(self):
        """获取线程本地数据库连接"""
        if not hasattr(self._local, "connection"):
            self._local.connection = sqlite3.connect(
                self.db_path, check_same_thread=False, timeout=30.0
            )
            self._local.connection.execute("PRAGMA journal_mode=WAL")
        return self._local.connection

    def check_file_exists(self, file_size: int, created_date: str) -> bool:
        """线程安全的快速预检查"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT MD5 FROM {self.table_name} WHERE FILE_SIZE=? AND CREATED_DATE=?",
                (file_size, created_date),
            )
            return cursor.fetchone() is not None
        except sqlite3.Error:
            return False

    def check_duplicate(self, md5: str) -> bool:
        """检查重复文件（线程安全读操作）"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT MD5 FROM {self.table_name} WHERE MD5=?", (md5,))
            return cursor.fetchone() is not None
        except sqlite3.Error:
            return False

    def batch_add_records(self, results: List[FileProcessResult]) -> int:
        """批量添加记录（线程安全写操作）"""
        with self._write_lock:  # 串行化写操作
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                added_count = 0

                conn.execute("BEGIN TRANSACTION")
                for result in results:
                    if result.success:
                        try:
                            cursor.execute(
                                f"INSERT INTO {self.table_name}(MD5, FILE_SIZE, FILE_TYPE, CREATED_DATE) VALUES(?,?,?,?)",
                                (
                                    result.md5,
                                    result.file_size,
                                    result.file_type,
                                    result.created_date,
                                ),
                            )
                            added_count += 1
                        except sqlite3.IntegrityError:
                            continue  # Skip duplicates
                conn.commit()
                return added_count
            except sqlite3.Error as e:
                conn.rollback()
                raise e


class ConfigManager:
    """Configuration management class for loading and validating settings"""

    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self.config = self._load_config()
        self._validate_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")

    def _validate_config(self) -> None:
        """Validate required configuration keys"""
        required_keys = ["paths", "supported_formats", "database"]
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required configuration key: {key}")

    def get(self, key_path: str, default=None):
        """Get configuration value by dot-separated key path"""
        keys = key_path.split(".")
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value


class PhotoClassifierOptimized:
    """Optimized photo classifier with fast pre-check and enhanced error handling"""

    def __init__(self, config_path: str = "config.json"):
        # Load configuration
        self.config = ConfigManager(config_path)

        # Setup logging
        self._setup_logging()

        # Initialize paths from config
        self.input_folder = self.config.get("paths.input_folder")
        self.photo_output = self.config.get("paths.photo_output")
        self.video_output = self.config.get("paths.video_output")
        self.image_output = self.config.get("paths.image_output")

        # Initialize file extensions
        self.image_extensions = self.config.get(
            "supported_formats.image_extensions", []
        )
        self.video_extensions = self.config.get(
            "supported_formats.video_extensions", []
        )

        # Initialize database settings
        self.table_name = self.config.get("database.table_name", "PHOTO")
        self.db_dir = self.config.get("paths.database_dir", "database")
        self.db_file = self.config.get("paths.database_file", "photo_classifier.db")

        # Initialize EXIF keys
        self.photo_no_date_keys = self.config.get("exif.photo_no_date_keys", [])
        self.photo_date_keys = self.config.get("exif.photo_date_keys", [])
        self.photo_exif_keys = self.photo_no_date_keys + self.photo_date_keys

        # Other settings
        self.skip_folders = self.config.get("skip_folders", [])
        self.timezone = self.config.get("timezone", "Asia/Shanghai")
        self.min_file_size = self.config.get("performance.min_file_size", 1024)

        # Initialize counters (thread-safe)
        self._counter_lock = threading.Lock()
        self.processed_count = 0
        self.error_count = 0
        self.duplicate_count = 0
        self.skipped_count = 0  # 快速跳过的文件数

        # Multithreading settings
        self.enable_multithreading = self.config.get(
            "performance.enable_multithreading", True
        )
        self.max_workers = self.config.get("performance.max_workers", os.cpu_count())
        self.batch_size = self.config.get("performance.batch_size", 50)

        # Database connections
        self.db = None  # Main database connection
        self.db_path = os.path.join(self.db_dir, self.db_file)
        self.thread_safe_db = None  # Will be initialized when needed

        # Validate configuration
        self._validate_paths()

        mode_desc = "多线程" if self.enable_multithreading else "单线程"
        self.logger.info(f"照片分类器初始化成功 - 支持快速预检查 ({mode_desc})")

    def _increment_counter(self, counter_name: str) -> None:
        """线程安全的计数器增加"""
        with self._counter_lock:
            current_value = getattr(self, counter_name)
            setattr(self, counter_name, current_value + 1)

    def _setup_logging(self) -> None:
        """Setup logging configuration"""
        log_level = self.config.get("logging.level", "INFO")
        log_format = self.config.get(
            "logging.format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        log_file = self.config.get("logging.file", "photo_classifier.log")

        # Create logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(getattr(logging, log_level.upper()))

        # Clear existing handlers
        self.logger.handlers.clear()

        # Create formatter
        formatter = logging.Formatter(log_format)

        # File handler
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _validate_paths(self) -> None:
        """Validate that required paths exist or can be created"""
        paths_to_check = [
            ("input_folder", self.input_folder, True),  # Must exist
            ("photo_output", self.photo_output, False),  # Can be created
            ("video_output", self.video_output, False),  # Can be created
            ("image_output", self.image_output, False),  # Can be created
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
            self.db.execute(
                "PRAGMA foreign_keys = ON"
            )  # Enable foreign key constraints
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
        """Create optimized database table structure with statistics"""
        try:
            self.connect_database()
            cursor = self.db.cursor()

            # Drop existing tables if exists
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            cursor.execute("DROP TABLE IF EXISTS STATISTICS")
            self.logger.info(f"删除现有表: {self.table_name}, STATISTICS")

            # Create new optimized table structure (removed ORIGINAL_PATH and NEW_PATH)
            sql = f"""CREATE TABLE {self.table_name} (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                MD5 TEXT NOT NULL UNIQUE,
                FILE_SIZE INTEGER NOT NULL,
                FILE_TYPE TEXT NOT NULL,  -- 'photo', 'image', 'video'
                CREATED_DATE TEXT,
                PROCESSED_DATE TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
            cursor.execute(sql)

            # Create simple statistics table
            stats_sql = """CREATE TABLE STATISTICS (
                ID INTEGER PRIMARY KEY DEFAULT 1,
                PHOTO_COUNT INTEGER DEFAULT 0,     -- 照片数量（有EXIF）
                IMAGE_COUNT INTEGER DEFAULT 0,     -- 图片数量（无EXIF）
                VIDEO_COUNT INTEGER DEFAULT 0,     -- 视频数量
                UPDATED_DATE TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
            cursor.execute(stats_sql)

            # Create indexes for better performance
            cursor.execute(f"CREATE INDEX idx_md5 ON {self.table_name}(MD5)")
            cursor.execute(
                f"CREATE INDEX idx_size_date ON {self.table_name}(FILE_SIZE, CREATED_DATE)"
            )
            cursor.execute(
                f"CREATE INDEX idx_file_type ON {self.table_name}(FILE_TYPE)"
            )

            # Initialize statistics record
            cursor.execute(
                "INSERT INTO STATISTICS (ID, PHOTO_COUNT, IMAGE_COUNT, VIDEO_COUNT) VALUES (1, 0, 0, 0)"
            )

            self.db.commit()
            self.logger.info(
                f"创建优化后的表: {self.table_name}, STATISTICS (支持快速预检查和分类统计)"
            )

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
            cursor.execute("DROP TABLE IF EXISTS STATISTICS")
            self.db.commit()
            self.logger.info(f"删除表: {self.table_name}, STATISTICS")
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

            print("=" * 60)
            print("数据库信息")
            print("=" * 60)
            print(f"📍 数据库文件: {self.db_path}")
            print(f"📋 表名: {self.table_name}")

            # Check if table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (self.table_name,),
            )
            if cursor.fetchone():
                # Get record count
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                count = cursor.fetchone()[0]
                print(f"📊 总记录数量: {count}")

                # Show statistics if available
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='STATISTICS'"
                )
                if cursor.fetchone():
                    cursor.execute(
                        "SELECT PHOTO_COUNT, IMAGE_COUNT, VIDEO_COUNT, UPDATED_DATE FROM STATISTICS WHERE ID=1"
                    )
                    stats = cursor.fetchone()
                    if stats:
                        photo_count, image_count, video_count, updated_date = stats
                        print()
                        print("📈 分类统计:")
                        print(f"   📸 照片数量（有EXIF）: {photo_count}")
                        print(f"   🖼️  图片数量（无EXIF）: {image_count}")
                        print(f"   🎬 视频数量: {video_count}")
                        print(f"   🕒 更新时间: {updated_date}")

                print()
                # Get table schema
                cursor.execute(f"PRAGMA table_info({self.table_name})")
                columns = cursor.fetchall()
                print("🏗️  表结构:")
                for col in columns:
                    print(f"   {col[1]} ({col[2]})")

                # Show indexes
                cursor.execute(f"PRAGMA index_list({self.table_name})")
                indexes = cursor.fetchall()
                print("🔍 索引:")
                for idx in indexes:
                    print(f"   {idx[1]}")
            else:
                print("❌ 表不存在")
            print("=" * 60)

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
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (self.table_name,),
            )
            if not cursor.fetchone():
                print("❌ 数据库表不存在，请先运行 --create-table")
                return

            # Get recent records (updated for new schema)
            cursor.execute(
                f"SELECT MD5, FILE_SIZE, FILE_TYPE, CREATED_DATE, PROCESSED_DATE FROM {self.table_name} ORDER BY ID DESC LIMIT ?",
                (limit,),
            )
            records = cursor.fetchall()

            print("=" * 80)
            print(f"最近 {len(records)} 条记录")
            print("=" * 80)

            if records:
                print(
                    f"{'MD5':<20} {'大小':<8} {'类型':<6} {'创建日期':<12} {'处理时间'}"
                )
                print("-" * 80)
                for record in records:
                    md5, file_size, file_type, created_date, processed_date = record
                    # Format file size
                    if file_size > 1024 * 1024:
                        size_str = f"{file_size//1024//1024}MB"
                    elif file_size > 1024:
                        size_str = f"{file_size//1024}KB"
                    else:
                        size_str = f"{file_size}B"

                    # Format file type display
                    type_display = {
                        "photo": "📸照片",
                        "image": "🖼️图片",
                        "video": "🎬视频",
                    }.get(file_type, file_type)

                    print(
                        f"{md5[:8]}...{md5[-8:]} {size_str:<8} {type_display:<6} {created_date:<12} {processed_date[:19]}"
                    )
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

    def should_process_file(self, file_path: str) -> bool:
        """🚀 Fast pre-check to determine if file needs processing"""
        try:
            # Get file size
            file_size = os.path.getsize(file_path)

            # Extract creation date (this is the main cost, but still faster than MD5)
            year, month, day = self.read_date(file_path)
            created_date = f"{year}-{month}-{day}"

            # Check if file already exists with same size and creation date
            cursor = self.db.cursor()
            cursor.execute(
                f"SELECT MD5 FROM {self.table_name} WHERE FILE_SIZE=? AND CREATED_DATE=?",
                (file_size, created_date),
            )

            if cursor.fetchone():
                self.logger.debug(f"文件已存在（大小+日期匹配），跳过: {file_path}")
                self.skipped_count += 1
                return False

            return True

        except (OSError, sqlite3.Error) as e:
            self.logger.warning(f"预检查 {file_path} 失败: {e}")
            return True  # 出错时仍然处理，让后续流程处理

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
            cursor.execute(f"SELECT MD5 FROM {self.table_name} WHERE MD5=?", (md5,))
            record = cursor.fetchone()

            if record:
                self.logger.warning(f"发现重复文件（MD5相同）: {file_path}")
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
                        if ":" in time_str[:10]:
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

    def rename_move(
        self, file_path: str, year: str, month: str, day: str, md5: str
    ) -> str:
        """Rename and move file to appropriate directory"""
        # Determine output directory
        if self.is_image(file_path):
            output_dir = (
                self.photo_output if self.is_photo(file_path) else self.image_output
            )
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

    def add_record(
        self, md5: str, file_size: int, file_type: str, created_date: str
    ) -> None:
        """Add file record to database with optimized metadata (removed path fields)"""
        try:
            cursor = self.db.cursor()
            cursor.execute(
                f"INSERT INTO {self.table_name}(MD5, FILE_SIZE, FILE_TYPE, CREATED_DATE) VALUES(?,?,?,?)",
                (md5, file_size, file_type, created_date),
            )
            self.db.commit()
        except sqlite3.Error as e:
            self.logger.error(f"添加记录 {md5} 失败: {e}")
            self.db.rollback()
            raise

    def process_file_single(self, file_path: str) -> FileProcessResult:
        """处理单个文件（多线程安全版本）"""
        try:
            # Check file size
            if not self.is_valid_file_size(file_path):
                self.logger.debug(f"文件太小，跳过: {file_path}")
                return None

            # Check if it's a supported file type
            if not (self.is_image(file_path) or self.is_video(file_path)):
                self.logger.debug(f"不支持的文件类型，跳过: {file_path}")
                return None

            # Get file metadata for fast pre-check
            file_size = os.path.getsize(file_path)
            year, month, day = self.read_date(file_path)
            created_date = f"{year}-{month}-{day}"

            # 🚀 Fast pre-check using thread-safe database
            if self.thread_safe_db.check_file_exists(file_size, created_date):
                self.logger.debug(f"文件已存在（大小+日期匹配），跳过: {file_path}")
                self._increment_counter("skipped_count")
                return None

            # Calculate MD5 (this is the expensive operation)
            self.logger.debug(f"开始计算MD5: {file_path}")
            md5 = self.get_md5(file_path)

            # Check for MD5 duplicates
            if self.thread_safe_db.check_duplicate(md5):
                self.logger.warning(f"发现重复文件（MD5相同）: {file_path}")
                try:
                    os.remove(file_path)
                    self._increment_counter("duplicate_count")
                except OSError as e:
                    self.logger.error(f"删除重复文件失败: {e}")
                return None

            # Determine file type
            if self.is_photo(file_path):
                file_type = "photo"
            elif self.is_video(file_path):
                file_type = "video"
            else:
                file_type = "image"  # Image without EXIF

            # Move and rename file
            new_name = self.rename_move(file_path, year, month, day, md5)
            new_path = os.path.join(
                (
                    self.photo_output
                    if file_type == "photo"
                    else (
                        self.video_output if file_type == "video" else self.image_output
                    )
                ),
                year,
                month,
                day,
                new_name,
            )

            self._increment_counter("processed_count")
            self.logger.info(
                f"已处理 ({self.processed_count}): {os.path.basename(file_path)} -> {new_name}"
            )

            return FileProcessResult(
                md5=md5,
                file_size=file_size,
                file_type=file_type,
                created_date=created_date,
                original_path=file_path,
                new_path=new_path,
                success=True,
            )

        except Exception as e:
            self._increment_counter("error_count")
            self.logger.error(f"处理 {file_path} 错误: {e}")
            return FileProcessResult(
                md5="",
                file_size=0,
                file_type="",
                created_date="",
                original_path=file_path,
                new_path="",
                success=False,
                error_message=str(e),
            )

    def process_file(self, root: str, filename: str) -> None:
        """Process a single file with fast pre-check and comprehensive error handling (legacy method)"""
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

            # 🚀 Fast pre-check: Skip if file already processed and unchanged
            if not self.should_process_file(file_path):
                return

            # Calculate MD5 only if needed
            self.logger.debug(f"开始计算MD5: {file_path}")
            md5 = self.get_md5(file_path)
            file_size = os.path.getsize(file_path)

            # Validate (check for duplicates)
            self.validate_file(file_path, md5)

            # Determine file type
            if self.is_photo(file_path):
                file_type = "photo"
            elif self.is_video(file_path):
                file_type = "video"
            else:
                file_type = "image"  # Image without EXIF

            # Extract date
            year, month, day = self.read_date(file_path)
            created_date = f"{year}-{month}-{day}"

            # Move and rename file
            new_name = self.rename_move(file_path, year, month, day, md5)

            # Add record to database (without path information)
            self.add_record(md5, file_size, file_type, created_date)

            self.processed_count += 1
            self.logger.info(
                f"已处理 ({self.processed_count}): {filename} -> {new_name}"
            )

        except Exception as e:
            self.error_count += 1
            self.logger.error(f"处理 {file_path} 错误: {e}")

    def collect_files(self, folder: str) -> List[str]:
        """收集所有需要处理的文件"""
        file_paths = []
        self.logger.info(f"收集文件: {folder}")

        for root, dirs, files in os.walk(folder):
            # Skip system folders
            dirs[:] = [d for d in dirs if d not in self.skip_folders]

            for filename in files:
                file_path = os.path.join(root, filename)
                if (
                    self.is_image(file_path) or self.is_video(file_path)
                ) and self.is_valid_file_size(file_path):
                    file_paths.append(file_path)

        self.logger.info(f"发现 {len(file_paths)} 个符合条件的文件")
        return file_paths

    def process_folder_multithreaded(self, folder: str) -> None:
        """多线程处理文件夹"""
        self.logger.info(
            f"开始多线程处理文件夹: {folder} (工作线程: {self.max_workers})"
        )

        # Initialize thread-safe database
        self.thread_safe_db = ThreadSafeDatabase(self.db_path, self.table_name)

        # Collect all files first
        file_paths = self.collect_files(folder)
        if not file_paths:
            self.logger.info("没有找到需要处理的文件")
            return

        # Process files in batches using thread pool
        total_files = len(file_paths)
        processed_batches = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Process files in batches
            for i in range(0, total_files, self.batch_size):
                batch = file_paths[i : i + self.batch_size]
                batch_results = []

                # Submit batch to thread pool
                future_to_file = {
                    executor.submit(self.process_file_single, file_path): file_path
                    for file_path in batch
                }

                # Collect results
                for future in as_completed(future_to_file):
                    result = future.result()
                    if result is not None:
                        batch_results.append(result)

                # Batch write to database
                if batch_results:
                    try:
                        added_count = self.thread_safe_db.batch_add_records(
                            batch_results
                        )
                        processed_batches += 1
                        self.logger.info(
                            f"批次 {processed_batches} 完成，添加了 {added_count} 条记录"
                        )
                    except Exception as e:
                        self.logger.error(f"批量写入数据库失败: {e}")

        self.logger.info(f"多线程处理完成，总共处理了 {processed_batches} 个批次")

    def process_folder(self, folder: str) -> None:
        """Process all files in folder recursively"""
        if self.enable_multithreading:
            self.process_folder_multithreaded(folder)
        else:
            self.logger.info(f"开始单线程处理文件夹: {folder}")
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

    def update_statistics(self) -> None:
        """Update statistics table with current data"""
        try:
            cursor = self.db.cursor()

            # Count files by type
            cursor.execute(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE FILE_TYPE='photo'"
            )
            photo_count = cursor.fetchone()[0]

            cursor.execute(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE FILE_TYPE='image'"
            )
            image_count = cursor.fetchone()[0]

            cursor.execute(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE FILE_TYPE='video'"
            )
            video_count = cursor.fetchone()[0]

            # Update statistics table
            cursor.execute(
                """
                UPDATE STATISTICS SET 
                    PHOTO_COUNT = ?, 
                    IMAGE_COUNT = ?, 
                    VIDEO_COUNT = ?, 
                    UPDATED_DATE = CURRENT_TIMESTAMP 
                WHERE ID = 1
            """,
                (photo_count, image_count, video_count),
            )

            self.db.commit()
            self.logger.info(
                f"更新统计数据: 照片{photo_count}张, 图片{image_count}张, 视频{video_count}个"
            )

        except sqlite3.Error as e:
            self.logger.error(f"更新统计数据失败: {e}")

    def generate_report(self) -> None:
        """Generate processing report with optimization and multithreading statistics"""
        self.logger.info("=" * 60)
        self.logger.info("📊 处理报告")
        self.logger.info("=" * 60)
        mode = "多线程" if self.enable_multithreading else "单线程"
        if self.enable_multithreading:
            self.logger.info(
                f"🔧 处理模式: {mode} (工作线程: {self.max_workers}, 批量大小: {self.batch_size})"
            )
        else:
            self.logger.info(f"🔧 处理模式: {mode}")
        self.logger.info(f"✅ 已处理文件: {self.processed_count}")
        self.logger.info(f"⚡ 快速跳过: {self.skipped_count}")
        self.logger.info(f"🔄 发现重复: {self.duplicate_count}")
        self.logger.info(f"❌ 遇到错误: {self.error_count}")
        total_checked = (
            self.processed_count
            + self.skipped_count
            + self.duplicate_count
            + self.error_count
        )
        if total_checked > 0:
            skip_ratio = (self.skipped_count / total_checked) * 100
            self.logger.info(f"🚀 预检查优化率: {skip_ratio:.1f}%")
        self.logger.info("=" * 60)

    def start(self) -> None:
        """Start the classification process"""
        start_time = time.time()
        self.logger.info("开始照片分类处理 - 支持快速预检查")

        try:
            self.connect_database()
            self.process_folder(self.input_folder)
            self.delete_empty_folders(self.input_folder)

            # Update statistics after processing
            self.update_statistics()

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
    parser = argparse.ArgumentParser(
        description="Photo Classifier - Multithreaded Fast Pre-check Optimized Version"
    )
    parser.add_argument(
        "--config", default="config.json", help="Configuration file path"
    )
    parser.add_argument(
        "--create-table", action="store_true", help="Create/recreate database table"
    )
    parser.add_argument(
        "--drop-table", action="store_true", help="Drop existing database table"
    )
    parser.add_argument(
        "--list-records", action="store_true", help="List all records in database"
    )
    parser.add_argument(
        "--db-info", action="store_true", help="Show database information"
    )
    parser.add_argument(
        "--stats", action="store_true", help="Update and show statistics"
    )
    parser.add_argument("--input", help="Override input folder from config")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "--single-thread", action="store_true", help="Force single-threaded mode"
    )
    parser.add_argument(
        "--max-workers", type=int, help="Maximum number of worker threads"
    )
    parser.add_argument(
        "--batch-size", type=int, help="Batch size for processing files"
    )

    args = parser.parse_args()

    try:
        # Initialize classifier
        classifier = PhotoClassifierOptimized(args.config)

        # Override input folder if provided
        if args.input:
            classifier.input_folder = args.input
            classifier.logger.info(f"输入文件夹已覆盖为: {args.input}")

        # Set multithreading options
        if args.single_thread:
            classifier.enable_multithreading = False
            classifier.logger.info("强制使用单线程模式")

        if args.max_workers:
            classifier.max_workers = args.max_workers
            classifier.logger.info(f"最大工作线程数设置为: {args.max_workers}")

        if args.batch_size:
            classifier.batch_size = args.batch_size
            classifier.logger.info(f"批量处理大小设置为: {args.batch_size}")

        # Set verbose logging if requested
        if args.verbose:
            classifier.logger.setLevel(logging.DEBUG)

        # Handle database operations
        if args.create_table:
            print("正在创建/重建数据库表...")
            classifier.create_table()
            print("✅ 数据库表创建完成（支持快速预检查和统计）")
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

        if args.stats:
            print("正在更新统计数据...")
            classifier.connect_database()
            classifier.update_statistics()
            classifier.close_database()
            print("✅ 统计数据更新完成")
            classifier._show_db_info()
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

    # === 数据库操作 ===
    # sys.argv = ["script_name", "--create-table"]  # 创建数据库表
    # sys.argv = ["script_name", "--db-info"]                   # 查看数据库信息
    # sys.argv = ["script_name", "--list-records"]              # 查看最近记录
    # sys.argv = ["script_name", "--stats"]                     # 更新并显示统计数据
    # sys.argv = ["script_name", "--drop-table"]                # 删除数据库表

    # === 处理模式 ===
    # sys.argv = ["script_name", "--verbose"]                   # 启用详细日志
    # sys.argv = ["script_name", "--single-thread"]             # 强制单线程模式
    # sys.argv = ["script_name", "--max-workers", "8"]          # 设置最大线程数
    # sys.argv = ["script_name", "--batch-size", "100"]         # 设置批处理大小

    # === 自定义配置 ===
    # sys.argv = ["script_name", "--input", "D:\\test\\input"]  # 自定义输入目录
    # sys.argv = ["script_name", "--config", "my_config.json"]  # 自定义配置文件

    # === 组合使用示例 ===
    # sys.argv = ["script_name", "--verbose", "--max-workers", "6", "--batch-size", "50"]  # 多线程详细模式

    # 默认运行：多线程照片分类处理（注释掉上面所有选项时使用）
    # sys.argv = ["script_name"]

    main()
