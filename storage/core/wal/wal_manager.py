"""
WAL管理器 - 核心组件
整合所有WAL功能，提供统一接口
"""

import os
import time
import threading
from typing import Optional, Dict, Any, List
from pathlib import Path
from contextlib import contextmanager

from .log_record import LogRecord, LogRecordType
from .log_writer import LogWriter, SyncMode
from .log_reader import LogReader
from .checkpoint import CheckpointManager
from .recovery import RecoveryManager
from .wal_stats import WALStatistics
from ...utils.logger import get_logger
from ...utils.exceptions import StorageException


class WALManager:
    """
    WAL管理器 - 提供完整的预写日志功能

    特性：
    - 自动恢复
    - 检查点管理
    - 性能监控
    - 事务支持
    - 批量优化
    """

    def __init__(self,
                 storage_manager,
                 wal_dir: str = "data/wal",
                 enable_wal: bool = True,
                 sync_mode: str = "fsync",
                 checkpoint_interval: int = 1000,
                 enable_compression: bool = False,
                 enable_auto_recovery: bool = True):
        """
        初始化WAL管理器

        Args:
            storage_manager: 存储管理器实例
            wal_dir: WAL目录
            enable_wal: 是否启用WAL
            sync_mode: 同步模式 (none/flush/fsync/fdatasync)
            checkpoint_interval: 检查点间隔
            enable_compression: 是否启用压缩
            enable_auto_recovery: 是否自动恢复
        """
        self.storage_manager = storage_manager
        self.wal_dir = Path(wal_dir)
        self.enable_wal = enable_wal
        self.enable_compression = enable_compression

        # 创建WAL目录
        self.wal_dir.mkdir(parents=True, exist_ok=True)

        # LSN管理
        self.current_lsn = 0
        self.lsn_lock = threading.Lock()

        # 事务管理
        self.next_transaction_id = 1
        self.active_transactions: Dict[int, dict] = {}
        self.transaction_lock = threading.Lock()

        # 日志器
        self.logger = get_logger("wal_manager")

        if not enable_wal:
            self.logger.warning("WAL is disabled - data durability not guaranteed!")
            self.writer = None
            self.checkpoint_manager = None
            self.statistics = None
            return

        # 初始化各组件
        try:
            # 统计组件
            self.statistics = WALStatistics(str(self.wal_dir / "wal_stats.json"))

            # 写入器
            sync_mode_map = {
                'none': SyncMode.NONE,
                'flush': SyncMode.FLUSH,
                'fsync': SyncMode.FSYNC,
                'fdatasync': SyncMode.FDATASYNC
            }
            self.writer = LogWriter(
                str(self.wal_dir),
                sync_mode=sync_mode_map.get(sync_mode, SyncMode.FSYNC),
                enable_compression=enable_compression
            )

            # 检查点管理器
            self.checkpoint_manager = CheckpointManager(
                self.writer,
                str(self.wal_dir),
                checkpoint_interval=checkpoint_interval
            )

            # 性能计时器
            self.operation_timers = {}

            # 自动恢复
            if enable_auto_recovery:
                self._perform_recovery()

            self.logger.info(f"WAL Manager initialized",
                             wal_dir=str(self.wal_dir),
                             sync_mode=sync_mode,
                             checkpoint_interval=checkpoint_interval,
                             compression=enable_compression)

        except Exception as e:
            self.logger.error(f"Failed to initialize WAL Manager: {e}")
            raise StorageException(f"WAL initialization failed: {e}")

    def _perform_recovery(self):
        """执行恢复流程"""
        self.logger.info("Checking for recovery...")

        # 检查是否需要恢复
        recovery_info = self.checkpoint_manager.get_recovery_info()

        if recovery_info:
            self.logger.info("Recovery needed, starting recovery process...")

            recovery_manager = RecoveryManager(self.storage_manager, str(self.wal_dir))
            recovery_start = time.time()

            try:
                stats = recovery_manager.recover()
                recovery_time = (time.time() - recovery_start) * 1000

                # 记录恢复统计
                self.statistics.record_recovery(
                    recovery_time,
                    stats['pages_recovered'],
                    stats['transactions_rolled_back']
                )

                # 更新LSN
                if recovery_info['end_lsn']:
                    self.current_lsn = recovery_info['end_lsn'] + 1

                self.logger.info(f"Recovery completed successfully",
                                 duration_ms=recovery_time,
                                 **stats)

            except Exception as e:
                self.logger.error(f"Recovery failed: {e}")
                raise StorageException(f"Recovery failed: {e}")
        else:
            self.logger.info("No recovery needed")

    def write_page(self, page_id: int, data: bytes, transaction_id: Optional[int] = None):
        """
        记录页面写入操作

        Args:
            page_id: 页号
            data: 页数据
            transaction_id: 事务ID（可选）
        """
        if not self.enable_wal:
            return

        start_time = time.time()

        try:
            # 生成LSN
            lsn = self._get_next_lsn()

            # 创建日志记录
            record = LogRecord(
                lsn=lsn,
                record_type=LogRecordType.PAGE_WRITE,
                page_id=page_id,
                transaction_id=transaction_id,
                data=data
            )

            # 写入日志
            bytes_written = self.writer.write(record)

            # 更新检查点管理器
            self.checkpoint_manager.record_write(lsn, page_id, transaction_id)

            # 记录统计
            latency_ms = (time.time() - start_time) * 1000
            self.statistics.record_write(bytes_written, latency_ms)

            self.logger.debug(f"WAL: Logged page write",
                              lsn=lsn,
                              page_id=page_id,
                              size=len(data),
                              latency_ms=round(latency_ms, 2))

        except Exception as e:
            self.statistics.record_error('write')
            self.logger.error(f"Failed to log page write: {e}")
            raise StorageException(f"WAL write failed: {e}")

    def write_page_update(self, page_id: int, offset: int, data: bytes,
                          transaction_id: Optional[int] = None):
        """
        记录页面部分更新

        Args:
            page_id: 页号
            offset: 更新偏移
            data: 更新数据
            transaction_id: 事务ID
        """
        if not self.enable_wal:
            return

        try:
            lsn = self._get_next_lsn()

            record = LogRecord(
                lsn=lsn,
                record_type=LogRecordType.PAGE_UPDATE,
                page_id=page_id,
                transaction_id=transaction_id,
                data=data,
                metadata={'offset': offset}
            )

            self.writer.write(record)
            self.checkpoint_manager.record_write(lsn, page_id, transaction_id)

        except Exception as e:
            self.logger.error(f"Failed to log page update: {e}")
            raise

    def begin_transaction(self) -> int:
        """
        开始新事务

        Returns:
            int: 事务ID
        """
        if not self.enable_wal:
            return 0

        with self.transaction_lock:
            transaction_id = self.next_transaction_id
            self.next_transaction_id += 1

            lsn = self._get_next_lsn()

            # 记录事务开始
            record = LogRecord(
                lsn=lsn,
                record_type=LogRecordType.TRANSACTION_BEGIN,
                transaction_id=transaction_id
            )

            self.writer.write(record)

            # 更新事务表
            self.active_transactions[transaction_id] = {
                'start_lsn': lsn,
                'status': 'active',
                'start_time': time.time()
            }

            # 通知检查点管理器
            self.checkpoint_manager.begin_transaction(transaction_id, lsn)

            self.logger.debug(f"Transaction {transaction_id} started")

            return transaction_id

    def commit_transaction(self, transaction_id: int):
        """提交事务"""
        if not self.enable_wal or transaction_id == 0:
            return

        with self.transaction_lock:
            if transaction_id not in self.active_transactions:
                raise StorageException(f"Transaction {transaction_id} not found")

            lsn = self._get_next_lsn()

            # 记录事务提交
            record = LogRecord(
                lsn=lsn,
                record_type=LogRecordType.TRANSACTION_COMMIT,
                transaction_id=transaction_id
            )

            # 强制同步，确保事务持久化
            self.writer.write(record, force_sync=True)

            # 更新事务状态
            self.active_transactions[transaction_id]['status'] = 'committed'
            self.active_transactions[transaction_id]['end_lsn'] = lsn

            # 通知检查点管理器
            self.checkpoint_manager.end_transaction(transaction_id, commit=True)

            # 清理事务
            del self.active_transactions[transaction_id]

            self.logger.debug(f"Transaction {transaction_id} committed")

    def abort_transaction(self, transaction_id: int):
        """回滚事务"""
        if not self.enable_wal or transaction_id == 0:
            return

        with self.transaction_lock:
            if transaction_id not in self.active_transactions:
                raise StorageException(f"Transaction {transaction_id} not found")

            lsn = self._get_next_lsn()

            # 记录事务中止
            record = LogRecord(
                lsn=lsn,
                record_type=LogRecordType.TRANSACTION_ABORT,
                transaction_id=transaction_id
            )

            self.writer.write(record, force_sync=True)

            # 更新事务状态
            self.active_transactions[transaction_id]['status'] = 'aborted'

            # 通知检查点管理器
            self.checkpoint_manager.end_transaction(transaction_id, commit=False)

            # 清理事务
            del self.active_transactions[transaction_id]

            self.logger.info(f"Transaction {transaction_id} aborted")

    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        txn_id = self.begin_transaction()

        try:
            yield txn_id
            self.commit_transaction(txn_id)
        except Exception as e:
            self.abort_transaction(txn_id)
            raise

    def create_checkpoint(self, force: bool = False):
        """创建检查点"""
        if not self.enable_wal:
            return

        self.logger.info("Creating checkpoint...")

        # 先刷新所有待写入的日志
        self.writer.flush()

        # 创建检查点
        metadata = self.checkpoint_manager.create_checkpoint(force)

        # 记录统计
        if metadata:
            self.statistics.record_checkpoint(
                (time.time() - metadata.checkpoint_time) * 1000,
                len(metadata.dirty_pages)
            )

    def flush(self):
        """强制刷新所有待写入的日志"""
        if self.enable_wal and self.writer:
            bytes_flushed = self.writer.flush()
            self.logger.debug(f"Flushed {bytes_flushed} bytes to disk")

    def _get_next_lsn(self) -> int:
        """获取下一个LSN"""
        with self.lsn_lock:
            self.current_lsn += 1
            return self.current_lsn

    def get_statistics(self) -> dict:
        """获取WAL统计信息"""
        if not self.enable_wal:
            return {'enabled': False}

        stats = {
            'enabled': True,
            'current_lsn': self.current_lsn,
            'active_transactions': len(self.active_transactions),
            'writer_stats': self.writer.get_statistics() if self.writer else {},
            'checkpoint_stats': self.checkpoint_manager.get_statistics() if self.checkpoint_manager else {},
            'performance_stats': self.statistics.get_summary() if self.statistics else {}
        }

        return stats

    def get_health_report(self) -> dict:
        """获取健康报告"""
        if not self.enable_wal:
            return {'status': 'disabled'}

        return self.statistics.get_health_status() if self.statistics else {}

    def get_detailed_report(self) -> dict:
        """获取详细报告"""
        if not self.enable_wal:
            return {'wal_enabled': False}

        return {
            'wal_enabled': True,
            'statistics': self.get_statistics(),
            'health': self.get_health_report(),
            'performance': self.statistics.get_detailed_report() if self.statistics else {}
        }

    def shutdown(self):
        """关闭WAL管理器"""
        if not self.enable_wal:
            return

        self.logger.info("Shutting down WAL Manager...")

        try:
            # 停止检查点管理器
            if self.checkpoint_manager:
                self.checkpoint_manager.stop()

            # 关闭写入器
            if self.writer:
                self.writer.close()

            # 保存统计
            if self.statistics:
                self.statistics.save_stats()

            self.logger.info("WAL Manager shutdown completed")

        except Exception as e:
            self.logger.error(f"Error during WAL shutdown: {e}")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.shutdown()