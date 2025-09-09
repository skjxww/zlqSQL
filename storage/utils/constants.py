"""
存储系统常量定义
统一管理所有配置参数和常量
"""

import os

# ==================== 页管理相关常量 ====================
PAGE_SIZE = 4096  # 页大小：4KB
PAGE_HEADER_SIZE = 16  # 页头大小：16字节
MAX_PAGES = 1000000  # 最大页数限制
DEFAULT_PAGE_ALLOCATION = 10  # 默认预分配页数

# ==================== 缓存相关常量 ====================
BUFFER_SIZE = 100  # 缓存池大小：最多缓存100页
DEFAULT_CACHE_SIZE = 50  # 默认缓存大小
MAX_CACHE_SIZE = 1000  # 最大缓存大小
MIN_CACHE_SIZE = 10  # 最小缓存大小

# 缓存替换策略
CACHE_POLICY_LRU = "LRU"  # 最近最少使用
CACHE_POLICY_FIFO = "FIFO"  # 先进先出
CACHE_POLICY_LFU = "LFU"  # 最少使用频率

# ==================== 文件路径常量 ====================
# 数据目录
DATA_DIR = "data"
LOG_DIR = "logs"
BACKUP_DIR = "backup"

# 主要文件路径
DATA_FILE = os.path.join(DATA_DIR, "database.db")  # 数据文件
META_FILE = os.path.join(DATA_DIR, "metadata.json")  # 页元数据文件
CATALOG_FILE = os.path.join(DATA_DIR, "system_catalog.json")  # 系统目录文件

# 日志文件路径
STORAGE_LOG = os.path.join(LOG_DIR, "storage.log")
PAGE_LOG = os.path.join(LOG_DIR, "page_manager.log")
BUFFER_LOG = os.path.join(LOG_DIR, "buffer_pool.log")
TABLE_LOG = os.path.join(LOG_DIR, "table_manager.log")

# ==================== 记录和数据类型常量 ====================
# 支持的数据类型
SUPPORTED_DATA_TYPES = {
    'INT': {'size': 4, 'format': 'i'},
    'FLOAT': {'size': 4, 'format': 'f'},
    'BOOLEAN': {'size': 1, 'format': '?'},
    'DATE': {'size': 8, 'format': 'Q'},
    'VARCHAR': {'size': 'variable', 'format': 'varchar'}
}

# 记录相关常量
MAX_RECORD_SIZE = 2048  # 最大记录大小：2KB
MAX_VARCHAR_LENGTH = 1024  # VARCHAR最大长度
DEFAULT_VARCHAR_LENGTH = 255  # VARCHAR默认长度
RECORD_HEADER_SIZE = 1  # 记录头大小：1字节

# 记录状态标志
RECORD_STATUS_NORMAL = 0  # 正常记录
RECORD_STATUS_DELETED = 1  # 已删除记录

# ==================== 表管理常量 ====================
MAX_TABLE_NAME_LENGTH = 64  # 表名最大长度
MAX_COLUMN_NAME_LENGTH = 64  # 列名最大长度
MAX_COLUMNS_PER_TABLE = 32  # 每表最大列数
MAX_TABLES = 1000  # 最大表数量

# 系统表名（保留）
SYSTEM_TABLES = {
    'pg_catalog',
    'sqlite_master',
    'system_tables',
    'system_columns'
}

# ==================== 性能和限制常量 ====================
# 性能相关
DEFAULT_SCAN_BATCH_SIZE = 100  # 默认扫描批次大小
MAX_CONCURRENT_OPERATIONS = 50  # 最大并发操作数
FLUSH_INTERVAL_SECONDS = 30  # 自动刷盘间隔

# 内存限制
MAX_MEMORY_USAGE_MB = 512  # 最大内存使用：512MB
MEMORY_WARNING_THRESHOLD = 0.8  # 内存警告阈值：80%

# ==================== 日志配置常量 ====================
# 日志级别
LOG_LEVEL_DEBUG = "DEBUG"
LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_WARNING = "WARNING"
LOG_LEVEL_ERROR = "ERROR"
LOG_LEVEL_CRITICAL = "CRITICAL"

# 默认日志配置
DEFAULT_LOG_LEVEL = LOG_LEVEL_INFO
LOG_FILE_MAX_SIZE_MB = 10  # 日志文件最大大小：10MB
LOG_BACKUP_COUNT = 5  # 日志备份文件数量

# 性能监控配置
ENABLE_PERFORMANCE_MONITORING = True  # 启用性能监控
PERFORMANCE_LOG_INTERVAL = 100  # 性能日志记录间隔

# ==================== 错误码常量 ====================
# 通用错误码
ERROR_CODE_SUCCESS = 0
ERROR_CODE_GENERAL_ERROR = 1000
ERROR_CODE_INVALID_PARAMETER = 1001
ERROR_CODE_PERMISSION_DENIED = 1002

# 存储错误码
ERROR_CODE_DISK_IO = 2000
ERROR_CODE_PAGE_NOT_FOUND = 2001
ERROR_CODE_PAGE_ALLOCATION_FAILED = 2002
ERROR_CODE_BUFFER_FULL = 2003

# 表管理错误码
ERROR_CODE_TABLE_NOT_FOUND = 3000
ERROR_CODE_TABLE_ALREADY_EXISTS = 3001
ERROR_CODE_INVALID_SCHEMA = 3002
ERROR_CODE_RECORD_TOO_LARGE = 3003

# ==================== 系统配置常量 ====================
# 版本信息
STORAGE_SYSTEM_VERSION = "1.0.0"
CATALOG_VERSION = "1.0"
METADATA_VERSION = "1.0"

# 系统限制
MAX_TRANSACTION_SIZE = 1000  # 最大事务大小
DEFAULT_TIMEOUT_SECONDS = 30  # 默认超时时间

# 兼容性配置
STRICT_MODE = True  # 严格模式
ALLOW_EMPTY_TABLES = True  # 允许空表
AUTO_CREATE_DIRECTORIES = True  # 自动创建目录

# ==================== 开发和调试常量 ====================
# 调试模式
DEBUG_MODE = False  # 调试模式
VERBOSE_LOGGING = False  # 详细日志
ENABLE_ASSERTIONS = True  # 启用断言

# 测试配置
TEST_DATA_DIR = "test_data"
TEST_LOG_DIR = "test_logs"
ENABLE_TEST_MODE = False  # 测试模式

# 性能测试配置
BENCHMARK_MODE = False  # 基准测试模式
BENCHMARK_ITERATIONS = 1000  # 基准测试迭代次数


# ==================== 工具函数 ====================

def get_data_file_path(filename: str = None) -> str:
    """获取数据文件的完整路径"""
    if filename is None:
        return DATA_FILE
    return os.path.join(DATA_DIR, filename)


def get_log_file_path(component: str) -> str:
    """获取指定组件的日志文件路径"""
    log_files = {
        'storage': STORAGE_LOG,
        'page': PAGE_LOG,
        'buffer': BUFFER_LOG,
        'table': TABLE_LOG
    }
    return log_files.get(component, STORAGE_LOG)


def ensure_directories():
    """确保必要的目录存在"""
    directories = [DATA_DIR, LOG_DIR]

    if ENABLE_TEST_MODE:
        directories.extend([TEST_DATA_DIR, TEST_LOG_DIR])

    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)


def validate_page_size(size: int) -> bool:
    """验证页大小是否有效"""
    return size > 0 and size % 512 == 0  # 必须是512的倍数


def validate_buffer_size(size: int) -> bool:
    """验证缓存大小是否有效"""
    return MIN_CACHE_SIZE <= size <= MAX_CACHE_SIZE


def get_system_info() -> dict:
    """获取系统配置信息"""
    return {
        'version': STORAGE_SYSTEM_VERSION,
        'page_size': PAGE_SIZE,
        'buffer_size': BUFFER_SIZE,
        'data_dir': DATA_DIR,
        'log_dir': LOG_DIR,
        'debug_mode': DEBUG_MODE,
        'strict_mode': STRICT_MODE
    }


# ==================== 初始化检查 ====================

# 确保目录存在
if AUTO_CREATE_DIRECTORIES:
    ensure_directories()

# 验证关键配置
assert validate_page_size(PAGE_SIZE), f"Invalid page size: {PAGE_SIZE}"
assert validate_buffer_size(BUFFER_SIZE), f"Invalid buffer size: {BUFFER_SIZE}"
assert MAX_RECORD_SIZE < PAGE_SIZE, "Max record size must be less than page size"