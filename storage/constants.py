# constants.py
"""
存储系统常量定义
"""

# 页相关常量
PAGE_SIZE = 4096          # 页大小：4KB
BUFFER_SIZE = 100         # 缓存池大小：最多缓存100页

# 文件路径常量
DATA_FILE = "database.db"    # 数据文件名
META_FILE = "metadata.json"  # 元数据文件（记录页分配信息）

# 缓存相关常量
DEFAULT_CACHE_SIZE = 50   # 默认缓存大小
MAX_CACHE_SIZE = 200      # 最大缓存大小

# 日志级别
LOG_LEVEL = "INFO"