"""
存储管理器：对外统一接口，整合页管理器和缓存池（重构版）
增加了完善的异常处理、日志记录和性能监控
"""

import time
import threading
from typing import Optional, Dict, List, Any, Tuple
from contextlib import contextmanager

from .page_manager import PageManager
from .buffer_pool import BufferPool
from ..utils.constants import BUFFER_SIZE, DATA_FILE, META_FILE, FLUSH_INTERVAL_SECONDS
from ..utils.exceptions import (
    StorageException, SystemShutdownException, PageException,
    handle_storage_exceptions
)
from ..utils.logger import get_logger, PerformanceTimer, performance_monitor


class StorageManager:
    """存储管理器类 - 提供统一的存储访问接口（增强版）"""

    def __init__(self, buffer_size: int = BUFFER_SIZE,
                 data_file: str = DATA_FILE,
                 meta_file: str = META_FILE,
                 auto_flush_interval: int = FLUSH_INTERVAL_SECONDS):
        """
        初始化存储管理器

        Args:
            buffer_size: 缓存池大小
            data_file: 数据文件路径
            meta_file: 元数据文件路径
            auto_flush_interval: 自动刷盘间隔（秒）

        Raises:
            StorageException: 初始化失败
        """
        try:
            self.page_manager = PageManager(data_file, meta_file)
            self.buffer_pool = BufferPool(buffer_size)
            self.auto_flush_interval = auto_flush_interval

            # 状态管理
            self.is_shutdown = False
            self._lock = threading.RLock()

            # 统计信息
            self.start_time = time.time()
            self.last_flush_time = time.time()
            self.operation_count = 0
            self.flush_count = 0

            # 日志器
            self.logger = get_logger("storage")

            # 自动刷盘定时器（可选）
            self._flush_timer = None
            if auto_flush_interval > 0:
                self._start_auto_flush()

            self.logger.info("StorageManager initialized successfully",
                             buffer_size=buffer_size,
                             data_file=data_file,
                             meta_file=meta_file,
                             auto_flush_interval=auto_flush_interval)

        except Exception as e:
            self.logger.error(f"Failed to initialize StorageManager: {e}")
            raise StorageException(f"StorageManager initialization failed: {e}")

    def _start_auto_flush(self):
        """启动自动刷盘定时器"""

        def auto_flush():
            if not self.is_shutdown:
                try:
                    self._auto_flush_if_needed()
                except Exception as e:
                    self.logger.error(f"Auto flush failed: {e}")
                finally:
                    # 重新调度下次刷盘
                    if not self.is_shutdown:
                        self._flush_timer = threading.Timer(self.auto_flush_interval, auto_flush)
                        self._flush_timer.daemon = True
                        self._flush_timer.start()

        self._flush_timer = threading.Timer(self.auto_flush_interval, auto_flush)
        self._flush_timer.daemon = True
        self._flush_timer.start()

        self.logger.debug(f"Auto flush timer started with {self.auto_flush_interval}s interval")

    def _auto_flush_if_needed(self):
        """根据条件自动刷盘"""
        current_time = time.time()
        time_since_last_flush = current_time - self.last_flush_time

        # 检查是否需要刷盘
        dirty_pages = self.buffer_pool.get_dirty_pages()

        if dirty_pages and time_since_last_flush >= self.auto_flush_interval:
            with PerformanceTimer(self.logger, "auto_flush"):
                self.flush_all_pages()
                self.logger.info(f"Auto flush completed, {len(dirty_pages)} pages flushed")

    def _check_shutdown(self):
        """检查系统是否已关闭"""
        if self.is_shutdown:
            raise SystemShutdownException()

    @handle_storage_exceptions
    @performance_monitor("read_page")
    def read_page(self, page_id: int) -> bytes:
        """
        读取页数据（优先从缓存读取）

        Args:
            page_id: 页号

        Returns:
            bytes: 页数据

        Raises:
            SystemShutdownException: 系统已关闭
            PageException: 页操作错误
        """
        self._check_shutdown()

        with self._lock:
            self.operation_count += 1

            # 首先从缓存中尝试获取
            data = self.buffer_pool.get(page_id)

            if data is not None:
                # 缓存命中
                self.logger.debug(f"Cache hit for page {page_id}")
                return data
            else:
                # 缓存未命中，从磁盘读取
                self.logger.debug(f"Cache miss for page {page_id}, reading from disk")

                data = self.page_manager.read_page_from_disk(page_id)

                # 将数据放入缓存
                self.buffer_pool.put(page_id, data, is_dirty=False)

                return data

    @handle_storage_exceptions
    @performance_monitor("write_page")
    def write_page(self, page_id: int, data: bytes):
        """
        写入页数据（写入缓存，标记为脏页）

        Args:
            page_id: 页号
            data: 要写入的数据

        Raises:
            SystemShutdownException: 系统已关闭
            PageException: 页操作错误
        """
        self._check_shutdown()

        if not isinstance(data, bytes):
            raise PageException(f"Data must be bytes, got {type(data)}", page_id)

        with self._lock:
            self.operation_count += 1

            # 将数据写入缓存并标记为脏页
            self.buffer_pool.put(page_id, data, is_dirty=True)

            self.logger.debug(f"Page {page_id} written to cache and marked dirty")

    @handle_storage_exceptions
    @performance_monitor("allocate_page")
    def allocate_page(self) -> int:
        """
        分配一个新页

        Returns:
            int: 新分配的页号

        Raises:
            SystemShutdownException: 系统已关闭
            PageException: 页分配失败
        """
        self._check_shutdown()

        with self._lock:
            page_id = self.page_manager.allocate_page()
            self.logger.info(f"Allocated new page {page_id}")
            return page_id

    @handle_storage_exceptions
    @performance_monitor("deallocate_page")
    def deallocate_page(self, page_id: int):
        """
        释放一个页

        Args:
            page_id: 要释放的页号

        Raises:
            SystemShutdownException: 系统已关闭
            PageException: 页操作错误
        """
        self._check_shutdown()

        with self._lock:
            # 从缓存中移除
            removed = self.buffer_pool.remove(page_id)
            if removed:
                data, is_dirty = removed
                if is_dirty:
                    # 如果是脏页，先写入磁盘
                    self.page_manager.write_page_to_disk(page_id, data)
                    self.logger.debug(f"Flushed dirty page {page_id} before deallocation")

            # 从页管理器中释放
            self.page_manager.deallocate_page(page_id)

            self.logger.info(f"Deallocated page {page_id}")

    @handle_storage_exceptions
    @performance_monitor("flush_page")
    def flush_page(self, page_id: int) -> bool:
        """
        刷新指定页到磁盘

        Args:
            page_id: 页号

        Returns:
            bool: 是否实际执行了刷盘操作

        Raises:
            SystemShutdownException: 系统已关闭
        """
        self._check_shutdown()

        with self._lock:
            # 检查页是否在缓存中且为脏页
            if page_id in self.buffer_pool.cache:
                data, is_dirty, _ = self.buffer_pool.cache[page_id]
                if is_dirty:
                    # 写入磁盘
                    self.page_manager.write_page_to_disk(page_id, data)
                    # 清除脏标记
                    self.buffer_pool.clear_dirty_flag(page_id)
                    self.logger.debug(f"Flushed page {page_id} to disk")
                    return True
                else:
                    self.logger.debug(f"Page {page_id} is not dirty, no flush needed")
                    return False
            else:
                self.logger.debug(f"Page {page_id} not in cache")
                return False

    @handle_storage_exceptions
    @performance_monitor("flush_all_pages")
    def flush_all_pages(self) -> int:
        """
        刷新所有脏页到磁盘

        Returns:
            int: 刷新的页数
        """
        self._check_shutdown()

        with self._lock:
            dirty_pages = self.buffer_pool.flush_all()

            for page_id, data in dirty_pages.items():
                self.page_manager.write_page_to_disk(page_id, data)

            self.flush_count += 1
            self.last_flush_time = time.time()

            self.logger.info(f"Flushed all dirty pages",
                             pages_flushed=len(dirty_pages),
                             flush_count=self.flush_count)

            return len(dirty_pages)

    def get_cache_stats(self) -> dict:
        """
        获取缓存统计信息

        Returns:
            dict: 缓存统计信息
        """
        return self.buffer_pool.get_statistics()

    def get_page_stats(self) -> dict:
        """
        获取页管理统计信息

        Returns:
            dict: 页管理统计信息
        """
        return self.page_manager.get_statistics()

    def get_storage_info(self) -> dict:
        """
        获取存储系统信息

        Returns:
            dict: 存储系统信息
        """
        cache_stats = self.buffer_pool.get_statistics()
        page_stats = self.page_manager.get_statistics()
        uptime = time.time() - self.start_time

        return {
            "system_status": "running" if not self.is_shutdown else "shutdown",
            "uptime_seconds": round(uptime, 2),
            "operation_count": self.operation_count,
            "flush_count": self.flush_count,
            "last_flush_time": self.last_flush_time,
            "auto_flush_interval": self.auto_flush_interval,
            "cache_statistics": cache_stats,
            "page_statistics": page_stats,
            "performance_metrics": self.get_performance_metrics()
        }

    def get_performance_metrics(self) -> dict:
        """
        获取性能指标

        Returns:
            dict: 性能指标
        """
        uptime = time.time() - self.start_time
        cache_stats = self.buffer_pool.get_statistics()

        return {
            "operations_per_second": round(self.operation_count / max(uptime, 1), 2),
            "cache_hit_rate": cache_stats.get("hit_rate", 0),
            "cache_usage": cache_stats.get("cache_usage", 0),
            "average_flush_interval": round(uptime / max(self.flush_count, 1), 2),
            "dirty_pages_ratio": round(
                cache_stats.get("dirty_pages", 0) / max(cache_stats.get("cache_size", 1), 1) * 100, 2
            )
        }

    @contextmanager
    def transaction(self):
        """
        事务上下文管理器（简单实现）
        在事务结束时自动刷盘
        """
        transaction_start = time.time()
        initial_operation_count = self.operation_count

        try:
            self.logger.debug("Transaction started")
            yield self

            # 事务成功，刷新所有脏页
            flushed_pages = self.flush_all_pages()

            transaction_time = time.time() - transaction_start
            operations_in_transaction = self.operation_count - initial_operation_count

            self.logger.info("Transaction committed successfully",
                             duration=round(transaction_time, 3),
                             operations=operations_in_transaction,
                             pages_flushed=flushed_pages)

        except Exception as e:
            self.logger.error(f"Transaction failed: {e}")
            # 简单的错误处理：记录错误但不回滚（这里可以扩展实现真正的事务回滚）
            raise

    def force_eviction(self) -> Optional[Tuple[int, bool]]:
        """
        强制执行一次缓存淘汰（用于测试和内存管理）

        Returns:
            Optional[Tuple[int, bool]]: (被淘汰的页号, 是否为脏页) 或 None
        """
        self._check_shutdown()

        if self.buffer_pool.cache:
            evicted = self.buffer_pool._evict_lru()
            if evicted:
                page_id, data, is_dirty = evicted
                if is_dirty:
                    self.page_manager.write_page_to_disk(page_id, data)
                    self.logger.debug(f"Force evicted dirty page {page_id} and wrote to disk")
                else:
                    self.logger.debug(f"Force evicted clean page {page_id}")
                return page_id, is_dirty

        return None

    def optimize_cache(self):
        """
        优化缓存性能
        包括：压缩空闲页列表、验证元数据等
        """
        self._check_shutdown()

        with PerformanceTimer(self.logger, "cache_optimization"):
            # 压缩页管理器的空闲页列表
            self.page_manager.compact_free_pages()

            # 获取优化前的统计信息
            cache_stats_before = self.buffer_pool.get_statistics()

            # 这里可以添加更多优化逻辑，比如：
            # 1. 预加载热点页
            # 2. 调整缓存大小
            # 3. 清理无效缓存项

            cache_stats_after = self.buffer_pool.get_statistics()

            self.logger.info("Cache optimization completed",
                             hit_rate_before=cache_stats_before.get("hit_rate", 0),
                             hit_rate_after=cache_stats_after.get("hit_rate", 0))

    def validate_system(self) -> Dict[str, Any]:
        """
        验证存储系统的完整性

        Returns:
            Dict[str, Any]: 验证结果
        """
        self.logger.info("Starting system validation")

        validation_results = {
            "timestamp": time.time(),
            "cache_validation": {},
            "page_validation": {},
            "overall_status": "unknown"
        }

        try:
            # 验证页管理器
            page_validation = self.page_manager.validate_metadata()
            validation_results["page_validation"] = page_validation

            # 验证缓存一致性
            cache_info = self.buffer_pool.get_cache_info()
            cache_validation = {
                "cache_size_valid": len(cache_info["cache_details"]) == cache_info["capacity_info"]["current"],
                "no_null_data": all(
                    details["data_size"] > 0 for details in cache_info["cache_details"].values()
                )
            }
            validation_results["cache_validation"] = cache_validation

            # 整体状态评估
            all_checks_passed = (
                    all(page_validation.values()) and
                    all(cache_validation.values())
            )
            validation_results["overall_status"] = "passed" if all_checks_passed else "failed"

            self.logger.info("System validation completed",
                             status=validation_results["overall_status"])

        except Exception as e:
            validation_results["error"] = str(e)
            validation_results["overall_status"] = "error"
            self.logger.error(f"System validation failed: {e}")

        return validation_results

    @handle_storage_exceptions
    def shutdown(self):
        """
        关闭存储管理器，确保所有数据持久化
        """
        if self.is_shutdown:
            self.logger.warning("StorageManager already shutdown")
            return

        self.logger.info("Starting StorageManager shutdown")

        try:
            with self._lock:
                # 停止自动刷盘定时器
                if self._flush_timer:
                    self._flush_timer.cancel()
                    self.logger.debug("Auto flush timer stopped")

                # 刷新所有脏页
                flushed_pages = self.flush_all_pages()

                # 获取最终统计信息
                final_cache_stats = self.get_cache_stats()
                final_page_stats = self.get_page_stats()
                final_storage_info = self.get_storage_info()

                # 清理页管理器
                self.page_manager.cleanup()

                # 标记为已关闭
                self.is_shutdown = True

                shutdown_summary = {
                    "pages_flushed": flushed_pages,
                    "total_operations": self.operation_count,
                    "total_flushes": self.flush_count,
                    "final_cache_hit_rate": final_cache_stats.get("hit_rate", 0),
                    "uptime_seconds": round(time.time() - self.start_time, 2)
                }

                self.logger.info("StorageManager shutdown completed successfully",
                                 **shutdown_summary)

        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
            self.is_shutdown = True  # 即使出错也要标记为关闭
            raise StorageException(f"Shutdown failed: {e}")

    def __enter__(self):
        """支持上下文管理器"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出时自动关闭"""
        self.shutdown()

    def __str__(self) -> str:
        """字符串表示"""
        stats = self.get_storage_info()
        return (f"StorageManager(status={stats['system_status']}, "
                f"operations={stats['operation_count']}, "
                f"cache_hit_rate={stats['cache_statistics'].get('hit_rate', 0)}%)")

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()


# 便捷函数
def create_storage_manager(buffer_size: int = BUFFER_SIZE,
                           data_dir: str = None,
                           auto_flush: bool = True) -> StorageManager:
    """
    创建存储管理器实例

    Args:
        buffer_size: 缓存池大小
        data_dir: 数据目录路径
        auto_flush: 是否启用自动刷盘

    Returns:
        StorageManager: 存储管理器实例
    """
    if data_dir:
        import os
        data_file = os.path.join(data_dir, "database.db")
        meta_file = os.path.join(data_dir, "metadata.json")
    else:
        data_file = DATA_FILE
        meta_file = META_FILE

    auto_flush_interval = FLUSH_INTERVAL_SECONDS if auto_flush else 0

    return StorageManager(
        buffer_size=buffer_size,
        data_file=data_file,
        meta_file=meta_file,
        auto_flush_interval=auto_flush_interval
    )