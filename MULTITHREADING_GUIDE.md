# 多线程照片分类器使用指南

## 功能特点

### 🚀 多线程处理
- **并行计算MD5**：多个线程同时计算不同文件的MD5值
- **线程安全数据库**：使用WAL模式和线程锁确保数据安全
- **批量处理**：分批处理文件并批量写入数据库
- **智能预检查**：通过文件大小+创建日期快速跳过已处理文件

### ⚡ 性能优化
- **WAL模式**：SQLite启用WAL模式提升并发读写性能
- **连接池**：每个线程使用独立数据库连接
- **批量写入**：减少数据库写入次数
- **快速预检查**：避免重复MD5计算

## 配置选项

### config.json 配置
```json
{
    "performance": {
        "enable_multithreading": true,  // 是否启用多线程
        "max_workers": 4,              // 最大工作线程数
        "batch_size": 50,              // 批处理大小
        "min_file_size": 1024          // 最小文件大小
    }
}
```

### 命令行参数
```bash
# 基本使用
python src/photo_classifier_optimized.py

# 自定义线程数
python src/photo_classifier_optimized.py --max-workers 8

# 自定义批处理大小
python src/photo_classifier_optimized.py --batch-size 100

# 强制单线程模式
python src/photo_classifier_optimized.py --single-thread

# 组合使用
python src/photo_classifier_optimized.py --verbose --max-workers 6 --batch-size 30
```

## 性能建议

### 线程数设置
- **CPU密集型任务**：设置为CPU核心数
- **I/O密集型任务**：可设置为CPU核心数的2-4倍
- **建议值**：4-8个线程适合大多数情况

### 批处理大小
- **小文件多**：增加批处理大小 (100-200)
- **大文件少**：减少批处理大小 (20-50)
- **默认值**：50个文件一批

### 内存使用
- 每个线程会占用一定内存
- 批处理越大内存使用越多
- 建议监控内存使用情况

## 使用示例

### 1. 数据库操作
```bash
# 创建优化的数据库表
python src/photo_classifier_optimized.py --create-table

# 查看数据库信息
python src/photo_classifier_optimized.py --db-info

# 查看处理统计
python src/photo_classifier_optimized.py --stats
```

### 2. 处理模式
```bash
# 默认多线程处理
python src/photo_classifier_optimized.py

# 单线程处理（调试用）
python src/photo_classifier_optimized.py --single-thread

# 高性能多线程处理
python src/photo_classifier_optimized.py --max-workers 8 --batch-size 100

# 详细日志模式
python src/photo_classifier_optimized.py --verbose
```

### 3. 自定义输入
```bash
# 自定义输入目录
python src/photo_classifier_optimized.py --input "D:\MyPhotos"

# 使用自定义配置文件
python src/photo_classifier_optimized.py --config "my_config.json"
```

## 处理流程

### 多线程处理步骤
1. **文件发现**：单线程扫描并收集所有符合条件的文件
2. **分批处理**：将文件分成批次
3. **并行处理**：多线程并行处理每个批次
   - 快速预检查（文件大小+日期）
   - MD5计算
   - 重复检查
   - 文件移动和重命名
4. **批量写入**：每个批次处理完成后批量写入数据库
5. **统计更新**：处理完成后更新统计数据

### 线程安全保证
- **数据库写入**：使用线程锁串行化写操作
- **计数器**：使用线程锁保护共享计数器
- **文件操作**：每个文件只被一个线程处理
- **WAL模式**：启用SQLite WAL模式支持并发读取

## 故障排除

### 常见问题
1. **内存不足**：减少 `max_workers` 或 `batch_size`
2. **处理过慢**：增加 `max_workers`，检查磁盘I/O性能
3. **数据库锁定**：确保没有其他程序占用数据库文件
4. **线程错误**：使用 `--single-thread` 模式调试

### 性能监控
```bash
# 详细日志查看处理进度
python src/photo_classifier_optimized.py --verbose

# 监控统计数据
python src/photo_classifier_optimized.py --stats
```

## 版本特性

### v2.0 多线程版本
- ✅ 多线程并行处理
- ✅ 线程安全数据库操作
- ✅ 智能批量处理
- ✅ WAL模式数据库优化
- ✅ 详细性能统计
- ✅ 灵活的配置选项

### 兼容性
- 向下兼容单线程模式
- 配置文件向下兼容
- 数据库结构保持一致
- 支持原有的所有功能 