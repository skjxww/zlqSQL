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
from .transaction_manager import TransactionManager, IsolationLevel, TransactionException
from storage.core.lock_manager import SimpleLockManager, LockType
from .preread import PrereadManager, PrereadConfig, PrereadMode


class StorageManager:
    """存储管理器类 - 提供统一的存储访问接口（增强版）"""

    def __init__(self, buffer_size: int = BUFFER_SIZE,
                 data_file: str = DATA_FILE,
                 meta_file: str = META_FILE,
                 auto_flush_interval: int = FLUSH_INTERVAL_SECONDS,
                 enable_extent_management: bool = True,
                 enable_wal: bool = True,
                 enable_concurrency: bool = True):
        """
        初始化存储管理器

        Args:
            buffer_size: 缓存池大小
            data_file: 数据文件路径
            meta_file: 元数据文件路径
            auto_flush_interval: 自动刷盘间隔（秒）
            enable_extent_management: 是否启用区管理功能（实验性）

        Raises:
            StorageException: 初始化失败
        """
        # 日志器
        self.logger = get_logger("storage")

        # 先初始化WAL相关属性，避免属性不存在错误
        self.wal_enabled = enable_wal
        self.wal_manager = None  # 先设为None

        try:
            # 初始化表空间管理器
            from .tablespace_manager import TablespaceManager
            # 从data_file路径中提取目录
            import os
            # 添加索引管理器
            from .index_manager import IndexManager
            self.index_manager = IndexManager(self)
            data_dir = os.path.dirname(data_file) if os.path.dirname(data_file) else "data"
            self.tablespace_manager = TablespaceManager(data_dir)
            # 将表空间管理器传递给页管理器
            self.page_manager = PageManager(data_file, meta_file, tablespace_manager=self.tablespace_manager)
            # 新增：设置文件映射更新回调
            self.tablespace_manager._notify_file_mapping_update = self._update_page_manager_files
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
            # 添加这两行
            self.read_count = 0
            self.write_count = 0

            # 新增：ExtentManager集成
            self.enable_extent_management = enable_extent_management
            if enable_extent_management:
                from .extent_manager import ExtentManager
                self.extent_manager = ExtentManager(self.page_manager, extent_size=64)
                self.logger.info("ExtentManager enabled (experimental feature)")
            else:
                self.extent_manager = None
                self.logger.info("ExtentManager disabled, using direct page allocation")

            # 新增：表上下文管理
            self._current_table_context = None
            self._context_lock = threading.Lock()  # 线程安全

            # 自动刷盘定时器（可选）
            self._flush_timer = None
            if auto_flush_interval > 0:
                self._start_auto_flush()

            # 事务管理器（在WAL之后初始化）
            self.transaction_manager = TransactionManager(self, wal_enabled=enable_wal)
            self.logger.info("Transaction support enabled")

            # 当前默认事务（用于非事务操作的兼容性）
            self._default_txn_id = None

            # 预读系统集成（新增）
            self.enable_preread = True  # 可以通过参数控制
            if self.enable_preread:
                try:
                    # 创建预读配置
                    preread_config = PrereadConfig()
                    preread_config.enabled = True
                    preread_config.mode = PrereadMode.ADAPTIVE
                    preread_config.max_preread_pages = 6

                    # 初始化预读管理器
                    self.preread_manager = PrereadManager(self, preread_config)
                    self.logger.info("Preread system enabled")
                except Exception as e:
                    self.logger.error(f"Failed to initialize preread system: {e}")
                    self.preread_manager = None
                    self.enable_preread = False
            else:
                self.preread_manager = None
                self.logger.info("Preread system disabled")

            # 并发控制（新增）
            self.enable_concurrency = enable_concurrency
            if enable_concurrency:
                self.lock_manager = SimpleLockManager(timeout=5.0)
                self.logger.info("Concurrency control enabled with lock manager")
            else:
                self.lock_manager = None
                self.logger.info("Concurrency control disabled")

            # WAL集成（在所有其他组件初始化之后）
            self.wal_enabled = enable_wal
            if enable_wal:
                from .wal import WALManager
                self.wal_manager = WALManager(
                    storage_manager=self,
                    wal_dir=os.path.join(os.path.dirname(data_file), "wal"),
                    enable_wal=True,
                    sync_mode="fsync",
                    checkpoint_interval=1000,
                    enable_compression=False,
                    enable_auto_recovery=True
                )
                self.logger.info("WAL enabled for enhanced durability")

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

    def _update_page_manager_files(self):
        """更新页管理器的表空间文件映射"""
        try:
            # 获取最新的表空间文件映射
            updated_files = self.tablespace_manager.get_all_tablespace_files()

            # 更新页管理器的文件映射
            self.page_manager.tablespace_files.update(updated_files)

            self.logger.debug(f"Updated PageManager file mapping with {len(updated_files)} tablespaces")

        except Exception as e:
            self.logger.error(f"Failed to update PageManager file mapping: {e}")

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
            self.read_count += 1

            # 首先从缓存中尝试获取
            data = self.buffer_pool.get(page_id)

            if data is not None:
                # 缓存命中
                self.logger.debug(f"Cache hit for page {page_id}")
            else:
                # 缓存未命中，从磁盘读取
                self.logger.debug(f"Cache miss for page {page_id}, reading from disk")

                data = self.page_manager.read_page_from_disk(page_id)

                # 将数据放入缓存
                self.buffer_pool.put(page_id, data, is_dirty=False)

            # 预读系统：记录页面访问（新增）
            if self.enable_preread and self.preread_manager:
                try:
                    # 获取当前表上下文
                    current_table = self.get_current_table_context()
                    # 通知预读管理器
                    self.preread_manager.on_page_access(page_id, current_table, "read")
                except Exception as e:
                    self.logger.debug(f"Preread system error: {e}")

            return data

    @handle_storage_exceptions
    @performance_monitor("write_page")
    def write_page(self, page_id: int, data: bytes):
        """写入页数据（写入缓存，标记为脏页）"""
        self._check_shutdown()

        if not isinstance(data, bytes):
            raise PageException(f"Data must be bytes, got {type(data)}", page_id)

        with self._lock:
            self.operation_count += 1
            self.write_count += 1

            # WAL: 先写日志（添加安全检查）
            if self.wal_enabled and hasattr(self, 'wal_manager') and self.wal_manager:
                self.wal_manager.write_page(page_id, data)

            # 确保数据填充到PAGE_SIZE
            from ..utils.constants import PAGE_SIZE
            if len(data) < PAGE_SIZE:
                data = data + b'\x00' * (PAGE_SIZE - len(data))
            elif len(data) > PAGE_SIZE:
                data = data[:PAGE_SIZE]

            # 写入缓存并标记为脏页
            self.buffer_pool.put(page_id, data, is_dirty=True)

            self.logger.debug(f"Page {page_id} written to cache and marked dirty")

    def set_table_context(self, table_name: str):
        """
        设置当前表上下文，后续的allocate_page调用将使用此表名进行智能分配

        Args:
            table_name: 表名
        """
        with self._context_lock:
            self._current_table_context = table_name
            self.logger.debug(f"Set table context to '{table_name}'")

    def clear_table_context(self):
        """清除表上下文"""
        with self._context_lock:
            old_context = self._current_table_context
            self._current_table_context = None
            if old_context:
                self.logger.debug(f"Cleared table context (was '{old_context}')")

    def get_current_table_context(self) -> Optional[str]:
        """获取当前表上下文"""
        with self._context_lock:
            return self._current_table_context

    @handle_storage_exceptions
    @performance_monitor("allocate_page")
    def allocate_page(self, tablespace_name: str = None, table_name: str = None) -> int:
        """
        分配一个新页 - 增强版，完全向后兼容

        Args:
            tablespace_name: 指定的表空间名称，如果为None则使用默认表空间
            table_name: 表名，用于智能分配。如果不指定，会尝试使用表上下文

        Returns:
            int: 新分配的页号

        Raises:
            SystemShutdownException: 系统已关闭
            PageException: 页分配失败
        """
        self._check_shutdown()

        with self._lock:
            if tablespace_name is None:
                tablespace_name = "default"

            # 智能决策表名：优先级 = 显式参数 > 上下文 > "unknown"
            effective_table_name = table_name
            if effective_table_name is None:
                with self._context_lock:
                    effective_table_name = self._current_table_context
            if effective_table_name is None:
                effective_table_name = "unknown"

            # 使用统一的智能分配逻辑
            if self.extent_manager:
                page_id = self.extent_manager.allocate_page_smart(effective_table_name, tablespace_name)
            else:
                page_id = self.page_manager.allocate_page(tablespace_name)

            self.logger.info(
                f"Allocated page {page_id} for table '{effective_table_name}' in tablespace '{tablespace_name}'")
            return page_id

    @handle_storage_exceptions
    @performance_monitor("deallocate_page")
    def deallocate_page(self, page_id: int):
        """
        释放一个页 - 现在支持智能释放
        """
        self._check_shutdown()

        with self._lock:
            # 从缓存中移除
            removed = self.buffer_pool.remove(page_id)
            if removed:
                data, is_dirty = removed
                if is_dirty:
                    self.page_manager.write_page_to_disk(page_id, data)
                    self.logger.debug(f"Flushed dirty page {page_id} before deallocation")

            # 如果启用了区管理，使用智能释放
            if self.extent_manager:
                self.extent_manager.deallocate_page_smart(page_id)
            else:
                self.page_manager.deallocate_page(page_id)

            self.logger.info(f"Deallocated page {page_id}")

    def allocate_page_for_table(self, table_name: str) -> int:
        """
        为指定表分配页 - 现在是convenience wrapper

        Args:
            table_name: 表名

        Returns:
            int: 新分配的页号
        """
        self._check_shutdown()

        # 通过表空间管理器选择合适的表空间
        tablespace_name = self.tablespace_manager.allocate_tablespace_for_table(table_name)

        # 直接调用增强版的allocate_page
        return self.allocate_page(tablespace_name=tablespace_name, table_name=table_name)

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

        # 关闭预读系统
        if self.preread_manager:
            self.preread_manager.shutdown()
            self.logger.info("Preread system shutdown")

        # 回滚所有活跃事务
        if hasattr(self, 'transaction_manager'):
            self.logger.info("Aborting all active transactions...")
            self.transaction_manager.abort_all_transactions()

        # 关闭WAL
        if self.wal_enabled and self.wal_manager:
            self.wal_manager.shutdown()

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

    def configure_preread(self, enabled: bool = None, mode: str = None,
                          max_pages: int = None) -> bool:
        """
        配置预读系统

        Args:
            enabled: 是否启用预读
            mode: 预读模式 ("sequential", "table_aware", "extent_based", "adaptive")
            max_pages: 最大预读页数

        Returns:
            bool: 配置是否成功
        """
        if not self.preread_manager:
            self.logger.warning("Preread system not initialized")
            return False

        try:
            config = self.preread_manager.config

            if enabled is not None:
                config.enabled = enabled
                self.enable_preread = enabled

            if mode is not None:
                mode_map = {
                    "sequential": PrereadMode.SEQUENTIAL,
                    "table_aware": PrereadMode.TABLE_AWARE,
                    "extent_based": PrereadMode.EXTENT_BASED,
                    "adaptive": PrereadMode.ADAPTIVE,
                    "disabled": PrereadMode.DISABLED
                }
                if mode in mode_map:
                    config.mode = mode_map[mode]
                else:
                    self.logger.warning(f"Invalid preread mode: {mode}")
                    return False

            if max_pages is not None:
                config.max_preread_pages = max_pages

            # 应用新配置
            self.preread_manager.set_config(config)

            self.logger.info(f"Preread configured: enabled={config.enabled}, mode={config.mode.value}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to configure preread: {e}")
            return False

    def get_preread_statistics(self) -> Optional[Dict]:
        """获取预读系统统计信息"""
        if not self.preread_manager:
            return None

        try:
            return self.preread_manager.get_statistics()
        except Exception as e:
            self.logger.error(f"Failed to get preread statistics: {e}")
            return None

    def force_preread_pages(self, page_ids: List[int]) -> bool:
        """
        强制预读指定页面（用于测试）

        Args:
            page_ids: 要预读的页面ID列表

        Returns:
            bool: 是否成功
        """
        if not self.preread_manager or not self.enable_preread:
            return False

        try:
            return self.preread_manager.force_preread(page_ids)
        except Exception as e:
            self.logger.error(f"Failed to force preread: {e}")
            return False

    def optimize_preread_for_table(self, table_name: str, aggressiveness: float = 0.7):
        """
        为特定表优化预读设置

        Args:
            table_name: 表名
            aggressiveness: 预读激进程度 (0.0-1.0)
        """
        if not self.preread_manager:
            return

        try:
            config = self.preread_manager.config
            config.set_table_config(table_name, {
                'preread_size': min(8, int(4 + aggressiveness * 4)),
                'aggressiveness': aggressiveness
            })

            self.logger.info(f"Optimized preread for table '{table_name}' (aggressiveness={aggressiveness})")

        except Exception as e:
            self.logger.error(f"Failed to optimize preread for table '{table_name}': {e}")

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()

    def create_tablespace(self, name: str, file_path: str = None, size_mb: int = 100) -> bool:
        """
        创建新的表空间

        Args:
            name: 表空间名称
            file_path: 文件路径，如果为None则自动生成
            size_mb: 表空间大小（MB）

        Returns:
            bool: 创建是否成功
        """
        try:
            result = self.tablespace_manager.create_tablespace(name, file_path, size_mb)
            if result:
                # 更新页管理器的表空间文件映射
                self.page_manager.tablespace_files = self.tablespace_manager.get_all_tablespace_files()
                self.logger.info(f"Created tablespace '{name}' successfully")
            return result
        except Exception as e:
            self.logger.error(f"Failed to create tablespace '{name}': {e}")
            return False

    def list_tablespaces(self) -> List[dict]:
        """列出所有表空间"""
        return self.tablespace_manager.list_tablespaces()

    def get_tablespace_info(self, name: str) -> Optional[dict]:
        """获取指定表空间的信息"""
        return self.tablespace_manager.get_tablespace_info(name)

    def get_table_tablespace(self, table_name: str) -> str:
        """获取表所在的表空间"""
        return self.tablespace_manager.get_tablespace_for_table(table_name)

    def get_storage_summary(self) -> dict:
        """
        获取存储系统的完整摘要信息

        Returns:
            dict: 包含缓存、页管理、表空间的完整信息
        """
        storage_info = self.get_storage_info()
        tablespace_list = self.list_tablespaces()

        # 新增：区管理统计信息
        extent_info = {}
        if self.extent_manager:
            extent_info = {
                "enabled": True,
                "stats": self.extent_manager.get_stats(),
                "extents": self.extent_manager.list_extents()
            }
        else:
            extent_info = {
                "enabled": False,
                "message": "Extent management is disabled"
            }

        return {
            **storage_info,
            "tablespaces": {
                "count": len(tablespace_list),
                "list": tablespace_list
            },
            "extent_management": extent_info,  # 新增这部分
            "feature_status": {
                "tablespace_support": True,
                "multi_file_support": True,
                "cache_strategies": True
            }
        }

    def table_context(self, table_name: str):
        """
        返回表上下文管理器，支持 with 语句

        Args:
            table_name: 表名

        Returns:
            TableContext: 上下文管理器

        Example:
            with storage.table_context("user_profiles"):
                page1 = storage.allocate_page()  # 自动使用区分配
                page2 = storage.allocate_page()  # 自动使用区分配
        """
        return TableContext(self, table_name)

    def begin_transaction(self, isolation_level: str = "READ_COMMITTED") -> int:
        """
        开始一个新事务

        Args:
            isolation_level: 隔离级别 ("READ_UNCOMMITTED", "READ_COMMITTED",
                           "REPEATABLE_READ", "SERIALIZABLE")

        Returns:
            int: 事务ID
        """
        # 转换隔离级别字符串到枚举
        level_map = {
            "READ_UNCOMMITTED": IsolationLevel.READ_UNCOMMITTED,
            "READ_COMMITTED": IsolationLevel.READ_COMMITTED,
            "REPEATABLE_READ": IsolationLevel.REPEATABLE_READ,
            "SERIALIZABLE": IsolationLevel.SERIALIZABLE
        }
        isolation = level_map.get(isolation_level.upper(), IsolationLevel.READ_COMMITTED)

        txn_id = self.transaction_manager.begin_transaction(isolation)
        self.logger.info(f"Started transaction {txn_id} with isolation {isolation_level}")
        return txn_id

    def commit_transaction(self, txn_id: int):
        """
        提交事务

        Args:
            txn_id: 事务ID
        """
        self.transaction_manager.commit(txn_id)
        self.logger.info(f"Committed transaction {txn_id}")

    def rollback_transaction(self, txn_id: int):
        """
        回滚事务

        Args:
            txn_id: 事务ID
        """
        self.transaction_manager.rollback(txn_id)
        self.logger.info(f"Rolled back transaction {txn_id}")

    # 修改 read_page_transactional 方法
    def read_page_transactional(self, page_id: int, txn_id: int = None) -> bytes:
        """
        事务性读取页 - 自动处理锁
        """
        if txn_id is None:
            return self.read_page(page_id)

        # 自动获取共享锁（新增）
        if self.lock_manager:
            if not self.lock_manager.acquire_lock(txn_id, page_id, LockType.SHARED):
                raise StorageException(f"Failed to acquire read lock on page {page_id} for transaction {txn_id}")
            self.logger.debug(f"Acquired read lock on page {page_id} for transaction {txn_id}")

        # 记录读操作
        txn = self.transaction_manager.get_transaction(txn_id)
        if txn:
            txn.add_read_record(page_id)

        # 检查是否有事务可见的版本
        visible_data = self.transaction_manager.get_visible_data(txn_id, page_id)
        if visible_data is not None:
            self.logger.debug(f"Transaction {txn_id} reading versioned data for page {page_id}")
            return visible_data

        # 读取物理存储的版本
        return self.read_page(page_id)

    # 修改 write_page_transactional 方法
    def write_page_transactional(self, page_id: int, data: bytes, txn_id: int = None):
        """
        事务性写入页 - 自动处理锁
        """
        if txn_id is None:
            self.write_page(page_id, data)
            return

        # 自动获取排他锁（新增）
        if self.lock_manager:
            if not self.lock_manager.acquire_lock(txn_id, page_id, LockType.EXCLUSIVE):
                raise StorageException(f"Failed to acquire write lock on page {page_id} for transaction {txn_id}")
            self.logger.debug(f"Acquired write lock on page {page_id} for transaction {txn_id}")

        # 获取事务对象
        txn = self.transaction_manager.get_transaction(txn_id)
        if txn:
            # 如果是第一次修改这个页，保存原始数据
            if page_id not in txn.modified_pages:
                original_data = self.read_page(page_id)
                txn.add_undo_record(page_id, original_data)

        # 执行写入
        self.write_page(page_id, data)

        # 记录redo信息
        self.transaction_manager.record_write(txn_id, page_id, data)

        self.logger.debug(f"Transaction {txn_id} wrote page {page_id}")

    def get_concurrency_status(self) -> Dict:
        """获取并发控制状态信息"""
        if not self.lock_manager:
            return {"enabled": False}

        stats = self.lock_manager.get_statistics()
        stats["enabled"] = True
        return stats

    def allocate_page_transactional(self, tablespace_name: str = None,
                                    table_name: str = None, txn_id: int = None) -> int:
        """
        事务性分配页

        Args:
            tablespace_name: 表空间名称
            table_name: 表名
            txn_id: 事务ID

        Returns:
            int: 新分配的页号
        """
        page_id = self.allocate_page(tablespace_name, table_name)

        if txn_id is not None:
            # 记录到事务的修改集合
            txn = self.transaction_manager.get_transaction(txn_id)
            if txn:
                txn.modified_pages.add(page_id)

        return page_id

    def get_active_transactions(self) -> List[int]:
        """获取所有活跃事务ID列表"""
        return self.transaction_manager.get_active_transactions()

    def get_transaction_info(self, txn_id: int = None) -> dict:
        """
        获取事务信息

        Args:
            txn_id: 事务ID，如果为None则返回所有事务统计

        Returns:
            dict: 事务信息
        """
        if txn_id is not None:
            txn = self.transaction_manager.get_transaction(txn_id)
            if txn:
                return txn.to_dict()
            else:
                return {"error": f"Transaction {txn_id} not found"}
        else:
            return self.transaction_manager.get_statistics()


class TableContext:
    """表上下文管理器"""

    def __init__(self, storage_manager: StorageManager, table_name: str):
        self.storage_manager = storage_manager
        self.table_name = table_name
        self.previous_context = None

    def __enter__(self):
        # 保存之前的上下文
        self.previous_context = self.storage_manager.get_current_table_context()
        # 设置新的上下文
        self.storage_manager.set_table_context(self.table_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 恢复之前的上下文
        if self.previous_context is not None:
            self.storage_manager.set_table_context(self.previous_context)
        else:
            self.storage_manager.clear_table_context()

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