"""
存储系统工具模块
提供异常处理、日志记录、序列化等工具功能
"""

# 异常类
from .exceptions import (
    StorageException, PageException, InvalidPageIdException, PageNotAllocatedException,
    DiskIOException, BufferPoolException, BufferFullException, TableException,
    TableNotFoundException, TableAlreadyExistsException, SchemaException,
    InvalidSchemaException, RecordException, RecordTooLargeException,
    SerializationException, SystemShutdownException, ExceptionReporter,
    handle_storage_exceptions
)

# 日志功能
from .logger import (
    LogLevel, StorageLogger, PerformanceTimer, LoggerFactory,
    get_logger, performance_monitor
)

# 序列化工具
from .serializer import (
    DataType, RecordSerializer, PageSerializer, SchemaSerializer
)

# 常量
from .constants import (
    PAGE_SIZE, BUFFER_SIZE, DATA_FILE, META_FILE, CATALOG_FILE,
    MAX_RECORD_SIZE, MAX_VARCHAR_LENGTH, SUPPORTED_DATA_TYPES,
    ensure_directories, get_system_info
)

__all__ = [
    # 异常处理
    'StorageException',
    'PageException',
    'InvalidPageIdException',
    'PageNotAllocatedException',
    'DiskIOException',
    'BufferPoolException',
    'BufferFullException',
    'TableException',
    'TableNotFoundException',
    'TableAlreadyExistsException',
    'SchemaException',
    'InvalidSchemaException',
    'RecordException',
    'RecordTooLargeException',
    'SerializationException',
    'SystemShutdownException',
    'ExceptionReporter',
    'handle_storage_exceptions',

    # 日志
    'LogLevel',
    'StorageLogger',
    'PerformanceTimer',
    'LoggerFactory',
    'get_logger',
    'performance_monitor',

    # 序列化
    'DataType',
    'RecordSerializer',
    'PageSerializer',
    'SchemaSerializer',

    # 常量
    'PAGE_SIZE',
    'BUFFER_SIZE',
    'DATA_FILE',
    'META_FILE',
    'CATALOG_FILE',
    'MAX_RECORD_SIZE',
    'MAX_VARCHAR_LENGTH',
    'SUPPORTED_DATA_TYPES',
    'ensure_directories',
    'get_system_info'
]