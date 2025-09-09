"""
存储系统日志管理
提供统一的日志记录接口和格式化
"""

import logging
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum


class LogLevel(Enum):
    """日志级别枚举"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class StorageLogger:
    """存储系统专用日志器"""

    def __init__(self, name: str = "storage", log_file: str = None, level: str = "INFO"):
        """
        初始化日志器

        Args:
            name: 日志器名称
            log_file: 日志文件路径，None表示只输出到控制台
            level: 日志级别
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))

        # 清除已存在的处理器
        self.logger.handlers.clear()

        # 设置格式器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # 文件处理器（如果指定了文件路径）
        if log_file:
            # 确保日志目录存在
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)

            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        # 性能统计
        self.performance_stats = {
            'operation_count': 0,
            'total_time': 0.0,
            'average_time': 0.0
        }

    def debug(self, message: str, **kwargs):
        """记录调试信息"""
        self._log_with_context(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs):
        """记录一般信息"""
        self._log_with_context(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs):
        """记录警告信息"""
        self._log_with_context(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs):
        """记录错误信息"""
        self._log_with_context(logging.ERROR, message, **kwargs)

    def critical(self, message: str, **kwargs):
        """记录严重错误信息"""
        self._log_with_context(logging.CRITICAL, message, **kwargs)

    def _log_with_context(self, level: int, message: str, **kwargs):
        """带上下文信息的日志记录"""
        if kwargs:
            context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            full_message = f"{message} | Context: {context}"
        else:
            full_message = message

        self.logger.log(level, full_message)

    def log_operation(self, operation: str, **details):
        """记录操作日志"""
        self.info(f"Operation: {operation}", **details)

    def log_performance(self, operation: str, duration: float, **details):
        """记录性能信息"""
        self.performance_stats['operation_count'] += 1
        self.performance_stats['total_time'] += duration
        self.performance_stats['average_time'] = (
                self.performance_stats['total_time'] / self.performance_stats['operation_count']
        )

        self.info(
            f"Performance: {operation} completed in {duration:.4f}s",
            avg_time=f"{self.performance_stats['average_time']:.4f}s",
            **details
        )

    def log_cache_stats(self, stats: Dict[str, Any]):
        """记录缓存统计信息"""
        self.info("Cache Statistics", **stats)

    def log_page_operation(self, operation: str, page_id: int, **details):
        """记录页操作"""
        self.debug(f"Page {operation}: {page_id}", page_id=page_id, **details)

    def log_table_operation(self, operation: str, table_name: str, **details):
        """记录表操作"""
        self.info(f"Table {operation}: {table_name}", table_name=table_name, **details)

    def log_error_with_exception(self, message: str, exception: Exception, **details):
        """记录异常错误"""
        self.error(
            f"{message}: {str(exception)}",
            exception_type=type(exception).__name__,
            **details
        )

    def get_performance_summary(self) -> Dict[str, Any]:
        """获取性能统计摘要"""
        return self.performance_stats.copy()


class PerformanceTimer:
    """性能计时器上下文管理器"""

    def __init__(self, logger: StorageLogger, operation: str, **details):
        """
        初始化计时器

        Args:
            logger: 日志器实例
            operation: 操作名称
            **details: 额外的详细信息
        """
        self.logger = logger
        self.operation = operation
        self.details = details
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        """进入上下文，开始计时"""
        self.start_time = time.time()
        self.logger.debug(f"Starting {self.operation}", **self.details)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文，结束计时并记录"""
        self.end_time = time.time()
        duration = self.end_time - self.start_time

        if exc_type is None:
            # 操作成功完成
            self.logger.log_performance(self.operation, duration, **self.details)
        else:
            # 操作出现异常
            self.logger.error(
                f"Operation {self.operation} failed after {duration:.4f}s",
                exception=str(exc_val),
                **self.details
            )

    def get_duration(self) -> Optional[float]:
        """获取操作持续时间"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


class LoggerFactory:
    """日志器工厂类"""

    _loggers = {}

    @classmethod
    def get_logger(cls, name: str, log_file: str = None, level: str = "INFO") -> StorageLogger:
        """
        获取或创建日志器实例

        Args:
            name: 日志器名称
            log_file: 日志文件路径
            level: 日志级别

        Returns:
            StorageLogger: 日志器实例
        """
        key = f"{name}_{log_file}_{level}"

        if key not in cls._loggers:
            cls._loggers[key] = StorageLogger(name, log_file, level)

        return cls._loggers[key]

    @classmethod
    def get_storage_logger(cls) -> StorageLogger:
        """获取存储系统主日志器"""
        return cls.get_logger("storage", "logs/storage.log", "INFO")

    @classmethod
    def get_page_logger(cls) -> StorageLogger:
        """获取页管理日志器"""
        return cls.get_logger("page_manager", "logs/page_manager.log", "DEBUG")

    @classmethod
    def get_buffer_logger(cls) -> StorageLogger:
        """获取缓存日志器"""
        return cls.get_logger("buffer_pool", "logs/buffer_pool.log", "DEBUG")

    @classmethod
    def get_table_logger(cls) -> StorageLogger:
        """获取表管理日志器"""
        return cls.get_logger("table_manager", "logs/table_manager.log", "INFO")


# 便捷函数
def get_logger(component: str = "storage") -> StorageLogger:
    """
    获取指定组件的日志器

    Args:
        component: 组件名称 (storage, page, buffer, table)

    Returns:
        StorageLogger: 对应的日志器实例
    """
    logger_map = {
        'storage': LoggerFactory.get_storage_logger,
        'page': LoggerFactory.get_page_logger,
        'buffer': LoggerFactory.get_buffer_logger,
        'table': LoggerFactory.get_table_logger
    }

    getter = logger_map.get(component, LoggerFactory.get_storage_logger)
    return getter()


def performance_monitor(operation: str, logger: StorageLogger = None, **details):
    """
    性能监控装饰器

    Args:
        operation: 操作名称
        logger: 日志器实例
        **details: 额外的详细信息
    """
    if logger is None:
        logger = get_logger("storage")

    def decorator(func):
        def wrapper(*args, **kwargs):
            with PerformanceTimer(logger, f"{operation}:{func.__name__}", **details):
                return func(*args, **kwargs)

        return wrapper

    return decorator