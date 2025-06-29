"""
Photo Classifier - Multithreaded Version
针对SQLite优化的多线程照片分类器
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Tuple, Optional, Dict, Any, List
from pathlib import Path
from win32com.propsys import propsys, pscon


@dataclass
class FileProcessResult:
    """文件处理结果"""
    md5: str
    original_path: str
    new_path: str
    file_size: int
    created_date: str
    success: bool
    error_message: Optional[str] = None


class DatabaseManager:
    """线程安全的数据库管理器"""
    
    def __init__(self, db_path: str, table_name: str):
        self.db_path = db_path
        self.table_name = table_name
        self._local = threading.local()
        self._write_lock = threading.Lock()  # 写操作锁
        self._setup_database()
    
    def _setup_database(self):
        """设置数据库（启用WAL模式提升并发性能）"""
        conn = sqlite3.connect(self.db_path)
        try:
            # Enable WAL mode for better concurrent access
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            
            # Create table if not exists
            sql = f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                MD5 TEXT NOT NULL UNIQUE,
                ORIGINAL_PATH TEXT,
                NEW_PATH TEXT,
                FILE_SIZE INTEGER,
                CREATED_DATE TEXT,
                PROCESSED_DATE TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
            conn.execute(sql)
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_md5 ON {self.table_name}(MD5)")
            conn.commit()
        finally:
            conn.close()
    
    def get_connection(self):
        """获取线程本地数据库连接"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.connect(
                self.db_path, 
                check_same_thread=False,
                timeout=30.0
            )
            self._local.connection.execute("PRAGMA journal_mode=WAL")
        return self._local.connection
    
    def check_duplicate(self, md5: str) -> Optional[str]:
        """检查重复文件（线程安全读操作）"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT ORIGINAL_PATH FROM {self.table_name} WHERE MD5=?", (md5,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def batch_add_records(self, results: List[FileProcessResult]) -> int:
        """批量添加记录（线程安全写操作）"""
        with self._write_lock:  # 串行化写操作
            conn = self.get_connection()
            cursor = conn.cursor()
            added_count = 0
            
            try:
                conn.execute("BEGIN TRANSACTION")
                for result in results:
                    try:
                        cursor.execute(
                            f"INSERT INTO {self.table_name}(MD5, ORIGINAL_PATH, NEW_PATH, FILE_SIZE, CREATED_DATE) VALUES(?,?,?,?,?)",
                            (result.md5, result.original_path, result.new_path, result.file_size, result.created_date)
                        )
                        added_count += 1
                    except sqlite3.IntegrityError:
                        continue  # Skip duplicates
                conn.commit()
                return added_count
            except sqlite3.Error:
                conn.rollback()
                raise


class PhotoClassifierMultithreaded:
    """Multithreaded photo classifier optimized for SQLite"""
    
    def __init__(self, config_path: str = "config.json"):
        # Load configuration (reuse from optimized version)
        from photo_classifier_optimized import ConfigManager
        self.config = ConfigManager(config_path)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize paths and settings
        self._init_settings()
        
        # Initialize threading components
        self.max_workers = self.config.get('performance.max_workers', os.cpu_count())
        self.batch_size = self.config.get('performance.batch_size', 100)
        
        # Initialize database manager
        self.db_manager = DatabaseManager(self.db_path, self.table_name)
        
        # Processing queues
        self.file_queue = queue.Queue()
        self.result_queue = queue.Queue()
        
        # Statistics
        self.processed_count = 0
        self.error_count = 0
        self.duplicate_count = 0
        self.skipped_count = 0
        
        self.logger.info(f"Multithreaded Photo Classifier initialized with {self.max_workers} workers")
    
    def _setup_logging(self):
        """Setup logging (same as optimized version)"""
        log_level = self.config.get('logging.level', 'INFO')
        log_format = self.config.get('logging.format', 
                                   '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_file = self.config.get('logging.file', 'photo_classifier_mt.log')
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        self.logger.handlers.clear()
        
        formatter = logging.Formatter(log_format)
        
        # File handler
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def _init_settings(self):
        """Initialize settings from config"""
        self.input_folder = self.config.get('paths.input_folder')
        self.photo_output = self.config.get('paths.photo_output')
        self.video_output = self.config.get('paths.video_output')
        self.image_output = self.config.get('paths.image_output')
        
        self.image_extensions = self.config.get('supported_formats.image_extensions', [])
        self.video_extensions = self.config.get('supported_formats.video_extensions', [])
        
        self.table_name = self.config.get('database.table_name', 'PHOTO')
        self.db_dir = self.config.get('paths.database_dir', 'database')
        self.db_file = self.config.get('paths.database_file', 'photo_classifier.db')
        self.db_path = os.path.join(self.db_dir, self.db_file)
        
        self.photo_no_date_keys = self.config.get('exif.photo_no_date_keys', [])
        self.photo_date_keys = self.config.get('exif.photo_date_keys', [])
        self.photo_exif_keys = self.photo_no_date_keys + self.photo_date_keys
        
        self.skip_folders = self.config.get('skip_folders', [])
        self.timezone = self.config.get('timezone', 'Asia/Shanghai')
        self.min_file_size = self.config.get('performance.min_file_size', 1024)
        
        # Create output directories
        os.makedirs(self.db_dir, exist_ok=True)
        for path in [self.photo_output, self.video_output, self.image_output]:
            os.makedirs(path, exist_ok=True)
    
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
        except (IOError, OSError):
            return False
    
    def get_md5(self, file_path: str) -> str:
        """Calculate MD5 hash of file"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
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
        except (IOError, OSError):
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
        except Exception:
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
            try:
                mtime = os.path.getmtime(file_path)
                dt = datetime.datetime.fromtimestamp(mtime)
                date = (str(dt.year), f"{dt.month:02d}", f"{dt.day:02d}")
            except OSError:
                now = datetime.datetime.now()
                date = (str(now.year), f"{now.month:02d}", f"{now.day:02d}")
        
        return date
    
    def rename_move(self, file_path: str, year: str, month: str, day: str, md5: str) -> str:
        """Rename and move file to appropriate directory"""
        if self.is_image(file_path):
            output_dir = self.photo_output if self.is_photo(file_path) else self.image_output
        elif self.is_video(file_path):
            output_dir = self.video_output
        else:
            raise ValueError(f"Unsupported file type: {file_path}")
        
        target_dir = os.path.join(output_dir, year, month, day)
        os.makedirs(target_dir, exist_ok=True)
        
        _, file_ext = os.path.splitext(file_path)
        new_name = f"{year}-{month}-{day}-{md5}{file_ext}"
        target_path = os.path.join(target_dir, new_name)
        
        # Handle filename conflicts
        counter = 1
        while os.path.exists(target_path):
            base_name = f"{year}-{month}-{day}-{md5}_{counter}{file_ext}"
            target_path = os.path.join(target_dir, base_name)
            counter += 1
        
        shutil.move(file_path, target_path)
        return target_path
    
    def process_single_file(self, file_path: str) -> FileProcessResult:
        """Process a single file (worker thread function)"""
        try:
            # Quick checks first
            if not self.is_valid_file_size(file_path):
                return FileProcessResult("", file_path, "", 0, "", False, "File too small")
            
            if not (self.is_image(file_path) or self.is_video(file_path)):
                return FileProcessResult("", file_path, "", 0, "", False, "Unsupported file type")
            
            # Calculate MD5 (CPU intensive)
            md5 = self.get_md5(file_path)
            file_size = os.path.getsize(file_path)
            
            # Check for duplicates (database read)
            existing_path = self.db_manager.check_duplicate(md5)
            if existing_path:
                try:
                    os.remove(file_path)
                    return FileProcessResult(md5, file_path, "", file_size, "", False, f"Duplicate file (original: {existing_path})")
                except OSError as e:
                    return FileProcessResult(md5, file_path, "", file_size, "", False, f"Failed to remove duplicate: {e}")
            
            # Extract date (IO intensive)
            year, month, day = self.read_date(file_path)
            created_date = f"{year}-{month}-{day}"
            
            # Move file (IO intensive)
            new_path = self.rename_move(file_path, year, month, day, md5)
            
            return FileProcessResult(md5, file_path, new_path, file_size, created_date, True)
            
        except Exception as e:
            return FileProcessResult("", file_path, "", 0, "", False, str(e))
    
    def collect_files(self) -> List[str]:
        """Collect all files to process"""
        files = []
        for root, dirs, filenames in os.walk(self.input_folder):
            # Skip system folders
            dirs[:] = [d for d in dirs if d not in self.skip_folders]
            
            for filename in filenames:
                file_path = os.path.join(root, filename)
                files.append(file_path)
        
        self.logger.info(f"Collected {len(files)} files for processing")
        return files
    
    def start(self):
        """Start the multithreaded classification process"""
        start_time = time.time()
        self.logger.info("Starting multithreaded photo classification process")
        
        try:
            # Collect all files
            files = self.collect_files()
            if not files:
                self.logger.info("No files to process")
                return
            
            # Process files with thread pool
            batch_results = []
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_file = {executor.submit(self.process_single_file, file_path): file_path 
                                for file_path in files}
                
                # Process results as they complete
                for future in as_completed(future_to_file):
                    result = future.result()
                    
                    if result.success:
                        batch_results.append(result)
                        self.processed_count += 1
                        
                        if len(batch_results) >= self.batch_size:
                            # Batch write to database
                            added = self.db_manager.batch_add_records(batch_results)
                            self.logger.info(f"Batch processed {len(batch_results)} files, {added} added to database")
                            batch_results.clear()
                    else:
                        if "Duplicate" in result.error_message:
                            self.duplicate_count += 1
                        elif "too small" in result.error_message or "Unsupported" in result.error_message:
                            self.skipped_count += 1
                        else:
                            self.error_count += 1
                        
                        self.logger.debug(f"Skipped {result.original_path}: {result.error_message}")
                    
                    # Progress reporting
                    total_processed = self.processed_count + self.duplicate_count + self.skipped_count + self.error_count
                    if total_processed % 100 == 0:
                        self.logger.info(f"Progress: {total_processed}/{len(files)} files processed")
            
            # Process remaining batch
            if batch_results:
                added = self.db_manager.batch_add_records(batch_results)
                self.logger.info(f"Final batch: {len(batch_results)} files, {added} added to database")
            
            # Clean up empty folders
            self._delete_empty_folders(self.input_folder)
            
        except Exception as e:
            self.logger.error(f"Fatal error during processing: {e}")
            raise
        finally:
            # Close all database connections
            self.db_manager.close_connection()
        
        end_time = time.time()
        duration = end_time - start_time
        self._generate_report(duration)
    
    def _delete_empty_folders(self, folder: str):
        """Delete empty folders after processing"""
        deleted_count = 0
        for root, dirs, files in os.walk(folder, topdown=False):
            for dir_name in dirs:
                if dir_name in self.skip_folders:
                    continue
                    
                dir_path = os.path.join(root, dir_name)
                try:
                    if not os.listdir(dir_path):
                        os.rmdir(dir_path)
                        deleted_count += 1
                except OSError:
                    pass
        
        self.logger.info(f"Deleted {deleted_count} empty directories")
    
    def _generate_report(self, duration: float):
        """Generate processing report"""
        total_files = self.processed_count + self.duplicate_count + self.skipped_count + self.error_count
        
        self.logger.info("=" * 60)
        self.logger.info("MULTITHREADED PROCESSING REPORT")
        self.logger.info("=" * 60)
        self.logger.info(f"Total files processed: {total_files}")
        self.logger.info(f"Successfully processed: {self.processed_count}")
        self.logger.info(f"Duplicates removed: {self.duplicate_count}")
        self.logger.info(f"Files skipped: {self.skipped_count}")
        self.logger.info(f"Errors encountered: {self.error_count}")
        self.logger.info(f"Processing time: {duration:.2f} seconds")
        self.logger.info(f"Average speed: {total_files/duration:.2f} files/second")
        self.logger.info(f"Worker threads: {self.max_workers}")
        self.logger.info("=" * 60)


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Photo Classifier - Multithreaded Version")
    parser.add_argument("--config", default="config.json", help="Configuration file path")
    parser.add_argument("--workers", type=int, help="Number of worker threads")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    try:
        classifier = PhotoClassifierMultithreaded(args.config)
        
        if args.workers:
            classifier.max_workers = args.workers
            classifier.logger.info(f"Using {args.workers} worker threads")
        
        if args.verbose:
            classifier.logger.setLevel(logging.DEBUG)
        
        classifier.start()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 