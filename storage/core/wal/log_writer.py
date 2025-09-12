"""
WAL日志写入器
负责将日志记录写入磁盘，支持批量写入和同步模式
"""

import os
import threading
import time
from typing import Optional, List
from pathlib import Path
from enum import Enum

from .log_record import LogRecord, LogRecordBatch, LogRecordType
from ...utils.logger import get_logger
from ...utils.exceptions import StorageException


class SyncMode(Enum):
    """同步模式"""
    NONE = 0  # 不同步（最快，但不安全）
    FLUSH = 1  # 调用flush（中等）
    FSYNC = 2  # 调用fsync（最安全）
    FDATASYNC = 3  # 调用fdatasync（安全且较快）


class LogWriter:
    """
    WAL日志写入器

    特性：
    - 顺序写入优化
    - 批量写入支持
    - 多种同步模式
    - 自动轮转
    - 性能监控
    """

    def __init__(self,
                 wal_dir: str = "data/wal",
                 file_size_limit: int = 16 * 1024 * 1024,  # 16MB
                 sync_mode: SyncMode = SyncMode.FSYNC,
                 batch_size: int = 65536,  # 64KB
                 enable_compression: bool = False):
        """
        初始化日志写入器

        Args:
            wal_dir: WAL目录
            file_size_limit: 单个日志文件大小限制
            sync_mode: 同步模式
            batch_size: 批次大小
            enable_compression: 是否启用压缩
        """
        self.wal_dir = Path(wal_dir)
        self.file_size_limit = file_size_limit
        self.sync_mode = sync_mode
        self.batch_size = batch_size
        self.enable_compression = enable_compression

        # 创建WAL目录
        self.wal_dir.mkdir(parents=True, exist_ok=True)

        # 当前日志文件
        self.current_file = None
        self.current_file_path = None
        self.current_file_size = 0
        self.current_file_number = 0

        # 批量写入缓冲
        self.batch = LogRecordBatch(batch_size)
        self.batch_lock = threading.Lock()

        # 统计信息
        self.total_records_written = 0
        self.total_bytes_written = 0
        self.total_syncs = 0
        self.total_rotations = 0

        # 日志器
        self.logger = get_logger("wal_writer")

        # 初始化第一个日志文件
        self._open_next_file()

        self.logger.info(f"WAL Writer initialized",
                         wal_dir=str(self.wal_dir),
                         sync_mode=sync_mode.name,
                         batch_size=batch_size)

    def write(self, record: LogRecord, force_sync: bool = False) -> int:
        """
        写入日志记录

        Args:
            record: 日志记录
            force_sync: 是否强制同步

        Returns:
            int: 写入的字节数
        """
        with self.batch_lock:
            # 尝试添加到批次
            if not self.batch.add(record):
                # 批次已满，先刷新
                self._flush_batch()
                # 再次尝试添加
                if not self.batch.add(record):
                    # 单条记录太大，直接写入
                    return self._write_single(record, force_sync)

            # 如果需要强制同步或者是重要的记录类型，立即刷新
            if force_sync or self._should_immediate_flush(record):
                return self._flush_batch()

            # 如果批次快满了，也刷新
            if self.batch.is_full():
                return self._flush_batch()

            return 0  # 还在批次中，尚未写入

    def _should_immediate_flush(self, record: LogRecord) -> bool:
        """判断是否需要立即刷新"""
        # 事务提交和检查点需要立即刷新
        return record.record_type in [
            LogRecordType.TRANSACTION_COMMIT,
            LogRecordType.CHECKPOINT_END,
            LogRecordType.TABLE_CREATE,
            LogRecordType.TABLE_DROP
        ]

    def _write_single(self, record: LogRecord, force_sync: bool) -> int:
        """写入单条记录"""
        data = record.serialize(self.enable_compression)
        bytes_written = self._write_to_file(data)

        if force_sync or self.sync_mode != SyncMode.NONE:
            self._sync_file()

        self.total_records_written += 1
        self.total_bytes_written += bytes_written

        return bytes_written

    def _flush_batch(self) -> int:
        """刷新批次到磁盘"""
        if self.batch.is_empty():
            return 0

        # 序列化批次
        data = self.batch.serialize(self.enable_compression)

        # 写入文件
        bytes_written = self._write_to_file(data)

        # 更新统计
        self.total_records_written += len(self.batch.records)
        self.total_bytes_written += bytes_written

        # 清空批次
        self.batch.clear()

        # 根据同步模式决定是否同步
        if self.sync_mode != SyncMode.NONE:
            self._sync_file()

        return bytes_written

    def _write_to_file(self, data: bytes) -> int:
        """
        写入数据到文件

        Args:
            data: 要写入的数据

        Returns:
            int: 写入的字节数
        """
        if self.current_file is None:
            self._open_next_file()

        # 检查是否需要轮转
        if self.current_file_size + len(data) > self.file_size_limit:
            self._rotate_file()

        # 写入数据
        try:
            self.current_file.write(data)
            self.current_file_size += len(data)

            self.logger.debug(f"Wrote {len(data)} bytes to WAL file",
                              file_number=self.current_file_number,
                              file_size=self.current_file_size)

            return len(data)

        except Exception as e:
            self.logger.error(f"Failed to write to WAL file: {e}")
            raise StorageException(f"WAL write failed: {e}")

    def _sync_file(self):
        """同步文件到磁盘"""
        if self.current_file is None:
            return

        try:
            if self.sync_mode == SyncMode.FLUSH:
                self.current_file.flush()
            elif self.sync_mode == SyncMode.FSYNC:
                self.current_file.flush()
                os.fsync(self.current_file.fileno())
            elif self.sync_mode == SyncMode.FDATASYNC:
                self.current_file.flush()
                # Python没有fdatasync，使用fsync代替
                os.fsync(self.current_file.fileno())

            self.total_syncs += 1

        except Exception as e:
            self.logger.error(f"Failed to sync WAL file: {e}")
            raise StorageException(f"WAL sync failed: {e}")

    def _open_next_file(self):
        """打开下一个日志文件"""
        # 找到下一个可用的文件号
        self.current_file_number = self._find_next_file_number()

        # 构造文件路径
        self.current_file_path = self.wal_dir / f"wal_{self.current_file_number:08d}.log"

        # 打开文件
        try:
            self.current_file = open(self.current_file_path, 'ab')
            self.current_file_size = self.current_file_path.stat().st_size

            self.logger.info(f"Opened WAL file",
                             path=str(self.current_file_path),
                             number=self.current_file_number,
                             size=self.current_file_size)

        except Exception as e:
            self.logger.error(f"Failed to open WAL file: {e}")
            raise StorageException(f"Cannot open WAL file: {e}")

    def _find_next_file_number(self) -> int:
        """查找下一个可用的文件号"""
        existing_files = list(self.wal_dir.glob("wal_*.log"))
        if not existing_files:
            return 1

        # 解析文件号
        numbers = []
        for f in existing_files:
            try:
                num = int(f.stem.split('_')[1])
                numbers.append(num)
            except:
                continue

        return max(numbers) + 1 if numbers else 1

    def _rotate_file(self):
        """轮转日志文件"""
        self.logger.info(f"Rotating WAL file",
                         current_file=self.current_file_number,
                         size=self.current_file_size)

        # 关闭当前文件
        if self.current_file:
            self._sync_file()
            self.current_file.close()

        # 打开新文件
        self._open_next_file()
        self.total_rotations += 1

    def flush(self) -> int:
        """强制刷新所有待写入的数据"""
        with self.batch_lock:
            bytes_written = self._flush_batch()
            self._sync_file()
            return bytes_written

    def close(self):
        """关闭写入器"""
        try:
            # 刷新剩余数据
            self.flush()

            # 关闭文件
            if self.current_file:
                self.current_file.close()
                self.current_file = None

            self.logger.info(f"WAL Writer closed",
                             total_records=self.total_records_written,
                             total_bytes=self.total_bytes_written,
                             total_syncs=self.total_syncs,
                             total_rotations=self.total_rotations)

        except Exception as e:
            self.logger.error(f"Error closing WAL writer: {e}")

    def get_current_position(self) -> tuple:
        """
        获取当前写入位置

        Returns:
            tuple: (文件号, 文件内偏移)
        """
        return (self.current_file_number, self.current_file_size)

    def get_statistics(self) -> dict:
        """获取统计信息"""
        return {
            'current_file_number': self.current_file_number,
            'current_file_size': self.current_file_size,
            'total_records_written': self.total_records_written,
            'total_bytes_written': self.total_bytes_written,
            'total_syncs': self.total_syncs,
            'total_rotations': self.total_rotations,
            'batch_size': self.batch_size,
            'sync_mode': self.sync_mode.name,
            'compression_enabled': self.enable_compression
        }

    def set_sync_mode(self, mode: SyncMode):
        """动态调整同步模式"""
        old_mode = self.sync_mode
        self.sync_mode = mode
        self.logger.info(f"Changed sync mode from {old_mode.name} to {mode.name}")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()