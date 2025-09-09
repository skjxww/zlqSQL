"""
存储系统异常类定义
提供详细的错误分类和错误信息
"""


class StorageException(Exception):
    """存储系统基础异常类"""

    def __init__(self, message: str, error_code: str = None, details: dict = None):
        """
        初始化存储异常

        Args:
            message: 错误信息
            error_code: 错误代码
            details: 错误详细信息
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code or "STORAGE_ERROR"
        self.details = details or {}

    def __str__(self):
        """返回格式化的错误信息"""
        base_msg = f"[{self.error_code}] {self.message}"
        if self.details:
            detail_str = ", ".join([f"{k}={v}" for k, v in self.details.items()])
            return f"{base_msg} (Details: {detail_str})"
        return base_msg

    def to_dict(self):
        """转换为字典格式，便于日志记录"""
        return {
            "error_type": self.__class__.__name__,
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details
        }


class PageException(StorageException):
    """页管理相关异常"""

    def __init__(self, message: str, page_id: int = None, **kwargs):
        super().__init__(message, error_code="PAGE_ERROR", **kwargs)
        if page_id is not None:
            self.details['page_id'] = page_id


class InvalidPageIdException(PageException):
    """无效页号异常"""

    def __init__(self, page_id: int):
        super().__init__(
            f"Invalid page ID: {page_id}",
            page_id=page_id,
            error_code="INVALID_PAGE_ID"
        )


class PageNotAllocatedException(PageException):
    """页未分配异常"""

    def __init__(self, page_id: int):
        super().__init__(
            f"Page {page_id} is not allocated",
            page_id=page_id,
            error_code="PAGE_NOT_ALLOCATED"
        )


class DiskIOException(StorageException):
    """磁盘I/O异常"""

    def __init__(self, message: str, file_path: str = None, operation: str = None):
        super().__init__(message, error_code="DISK_IO_ERROR")
        if file_path:
            self.details['file_path'] = file_path
        if operation:
            self.details['operation'] = operation


class BufferPoolException(StorageException):
    """缓存池异常"""

    def __init__(self, message: str, **kwargs):
        super().__init__(message, error_code="BUFFER_POOL_ERROR", **kwargs)


class BufferFullException(BufferPoolException):
    """缓存池已满异常"""

    def __init__(self, capacity: int):
        super().__init__(
            f"Buffer pool is full (capacity: {capacity})",
            error_code="BUFFER_FULL"
        )
        self.details['capacity'] = capacity


class TableException(StorageException):
    """表管理相关异常"""

    def __init__(self, message: str, table_name: str = None, **kwargs):
        super().__init__(message, error_code="TABLE_ERROR", **kwargs)
        if table_name:
            self.details['table_name'] = table_name


class TableNotFoundException(TableException):
    """表不存在异常"""

    def __init__(self, table_name: str):
        super().__init__(
            f"Table '{table_name}' not found",
            table_name=table_name,
            error_code="TABLE_NOT_FOUND"
        )


class TableAlreadyExistsException(TableException):
    """表已存在异常"""

    def __init__(self, table_name: str):
        super().__init__(
            f"Table '{table_name}' already exists",
            table_name=table_name,
            error_code="TABLE_ALREADY_EXISTS"
        )


class SchemaException(StorageException):
    """模式相关异常"""

    def __init__(self, message: str, **kwargs):
        super().__init__(message, error_code="SCHEMA_ERROR", **kwargs)


class InvalidSchemaException(SchemaException):
    """无效模式异常"""

    def __init__(self, schema_info: str):
        super().__init__(
            f"Invalid schema: {schema_info}",
            error_code="INVALID_SCHEMA"
        )
        self.details['schema_info'] = schema_info


class RecordException(StorageException):
    """记录相关异常"""

    def __init__(self, message: str, **kwargs):
        super().__init__(message, error_code="RECORD_ERROR", **kwargs)


class RecordTooLargeException(RecordException):
    """记录过大异常"""

    def __init__(self, record_size: int, max_size: int):
        super().__init__(
            f"Record size {record_size} exceeds maximum size {max_size}",
            error_code="RECORD_TOO_LARGE"
        )
        self.details.update({
            'record_size': record_size,
            'max_size': max_size
        })


class SerializationException(StorageException):
    """序列化异常"""

    def __init__(self, message: str, data_type: str = None, **kwargs):
        super().__init__(message, error_code="SERIALIZATION_ERROR", **kwargs)
        if data_type:
            self.details['data_type'] = data_type


class SystemShutdownException(StorageException):
    """系统关闭异常"""

    def __init__(self):
        super().__init__(
            "Storage system is shutdown",
            error_code="SYSTEM_SHUTDOWN"
        )


# 异常处理装饰器
def handle_storage_exceptions(func):
    """存储异常处理装饰器"""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except StorageException:
            # 重新抛出存储异常
            raise
        except FileNotFoundError as e:
            raise DiskIOException(f"File not found: {e}", operation="file_access")
        except PermissionError as e:
            raise DiskIOException(f"Permission denied: {e}", operation="file_access")
        except OSError as e:
            raise DiskIOException(f"OS error: {e}", operation="file_access")
        except ValueError as e:
            raise SerializationException(f"Value error: {e}")
        except Exception as e:
            raise StorageException(f"Unexpected error: {e}", error_code="UNKNOWN_ERROR")

    return wrapper


# 异常报告工具
class ExceptionReporter:
    """异常报告工具类"""

    @staticmethod
    def format_exception(exception: StorageException) -> str:
        """格式化异常信息"""
        return f"Storage Exception Report:\n{exception.to_dict()}"

    @staticmethod
    def log_exception(exception: StorageException, logger=None):
        """记录异常到日志"""
        if logger:
            logger.error(ExceptionReporter.format_exception(exception))
        else:
            print(f"ERROR: {exception}")