"""
WAL检查点管理器
负责创建检查点、管理检查点元数据、清理旧日志
"""

import os
import json
import time
import threading
from pathlib import Path
from typing import Optional, Dict, List, Set
from dataclasses import dataclass, asdict

from .log_record import LogRecord, LogRecordType
from .log_writer import LogWriter
from .log_reader import LogReader
from ...utils.logger import get_logger
from ...utils.exceptions import StorageException


@dataclass
class CheckpointMetadata:
    """检查点元数据"""
    checkpoint_lsn: int  # 检查点的LSN
    checkpoint_time: float  # 检查点创建时间
    start_lsn: int  # 检查点开始时的LSN
    end_lsn: int  # 检查点结束时的LSN
    dirty_pages: Dict[int, int]  # 脏页映射 {page_id: recovery_lsn}
    active_transactions: List[int]  # 活跃事务列表
    file_number: int  # 检查点所在的文件号
    file_offset: int  # 检查点在文件中的偏移

    def to_dict(self) -> dict:
        """转换为字典"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'CheckpointMetadata':
        """从字典创建"""
        return cls(**data)


class CheckpointManager:
    """
    检查点管理器

    特性：
    - 定期检查点
    - 增量检查点
    - 模糊检查点
    - 自动日志清理
    - 检查点恢复
    """

    def __init__(self,
                 writer: LogWriter,
                 wal_dir: str = "data/wal",
                 checkpoint_interval: int = 1000,  # 每1000条记录做一次检查点
                 checkpoint_timeout: int = 300,  # 每5分钟强制检查点
                 enable_auto_checkpoint: bool = True):
        """
        初始化检查点管理器

        Args:
            writer: 日志写入器
            wal_dir: WAL目录
            checkpoint_interval: 检查点间隔（记录数）
            checkpoint_timeout: 检查点超时（秒）
            enable_auto_checkpoint: 是否启用自动检查点
        """
        self.writer = writer
        self.wal_dir = Path(wal_dir)
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_timeout = checkpoint_timeout
        self.enable_auto_checkpoint = enable_auto_checkpoint

        # 检查点元数据文件
        self.metadata_file = self.wal_dir / "checkpoint.json"

        # 当前检查点信息
        self.last_checkpoint: Optional[CheckpointMetadata] = None
        self.records_since_checkpoint = 0
        self.last_checkpoint_time = time.time()

        # 活跃事务跟踪
        self.active_transactions: Set[int] = set()
        self.transaction_start_lsn: Dict[int, int] = {}

        # 脏页跟踪
        self.dirty_pages: Dict[int, int] = {}  # {page_id: first_dirty_lsn}

        # 锁
        self.checkpoint_lock = threading.Lock()

        # 统计信息
        self.total_checkpoints = 0
        self.total_log_cleanups = 0

        # 日志器
        self.logger = get_logger("checkpoint")

        # 加载最后的检查点
        self._load_last_checkpoint()

        # 自动检查点定时器
        self.checkpoint_timer = None
        if enable_auto_checkpoint:
            self._start_auto_checkpoint()

        self.logger.info(f"Checkpoint Manager initialized",
                         checkpoint_interval=checkpoint_interval,
                         checkpoint_timeout=checkpoint_timeout,
                         auto_checkpoint=enable_auto_checkpoint)

    def _load_last_checkpoint(self):
        """加载最后的检查点元数据"""
        if not self.metadata_file.exists():
            self.logger.info("No previous checkpoint found")
            return

        try:
            with open(self.metadata_file, 'r') as f:
                data = json.load(f)
                self.last_checkpoint = CheckpointMetadata.from_dict(data)
                self.logger.info(f"Loaded last checkpoint",
                                 lsn=self.last_checkpoint.checkpoint_lsn,
                                 time=self.last_checkpoint.checkpoint_time)
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint metadata: {e}")

    def _save_checkpoint_metadata(self, metadata: CheckpointMetadata):
        """保存检查点元数据"""
        try:
            # 原子写入
            temp_file = self.metadata_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(metadata.to_dict(), f, indent=2)
            temp_file.replace(self.metadata_file)

            self.logger.debug("Checkpoint metadata saved")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint metadata: {e}")
            raise StorageException(f"Cannot save checkpoint metadata: {e}")

    def _start_auto_checkpoint(self):
        """启动自动检查点定时器"""

        def auto_checkpoint():
            try:
                # 检查是否需要检查点
                if self._should_checkpoint():
                    self.create_checkpoint()
            except Exception as e:
                self.logger.error(f"Auto checkpoint failed: {e}")
            finally:
                # 重新调度
                if self.enable_auto_checkpoint:
                    self.checkpoint_timer = threading.Timer(60, auto_checkpoint)  # 每分钟检查一次
                    self.checkpoint_timer.daemon = True
                    self.checkpoint_timer.start()

        # 启动定时器
        self.checkpoint_timer = threading.Timer(60, auto_checkpoint)
        self.checkpoint_timer.daemon = True
        self.checkpoint_timer.start()

        self.logger.debug("Auto checkpoint timer started")

    def _should_checkpoint(self) -> bool:
        """判断是否应该创建检查点"""
        # 基于记录数
        if self.records_since_checkpoint >= self.checkpoint_interval:
            self.logger.debug(f"Checkpoint needed: {self.records_since_checkpoint} records")
            return True

        # 基于时间
        time_since_last = time.time() - self.last_checkpoint_time
        if time_since_last >= self.checkpoint_timeout:
            self.logger.debug(f"Checkpoint needed: {time_since_last:.1f} seconds elapsed")
            return True

        return False

    def record_write(self, lsn: int, page_id: Optional[int] = None,
                     transaction_id: Optional[int] = None):
        """
        记录写操作（由WAL管理器调用）

        Args:
            lsn: 日志序列号
            page_id: 页号
            transaction_id: 事务ID
        """
        with self.checkpoint_lock:
            self.records_since_checkpoint += 1

            # 更新脏页信息
            if page_id is not None and page_id not in self.dirty_pages:
                self.dirty_pages[page_id] = lsn

            # 检查是否需要自动检查点
            if self.enable_auto_checkpoint and self._should_checkpoint():
                # 异步创建检查点，避免阻塞写入
                threading.Thread(target=self.create_checkpoint, daemon=True).start()

    def begin_transaction(self, transaction_id: int, lsn: int):
        """
        记录事务开始

        Args:
            transaction_id: 事务ID
            lsn: 开始LSN
        """
        with self.checkpoint_lock:
            self.active_transactions.add(transaction_id)
            self.transaction_start_lsn[transaction_id] = lsn
            self.logger.debug(f"Transaction {transaction_id} started at LSN {lsn}")

    def end_transaction(self, transaction_id: int, commit: bool = True):
        """
        记录事务结束

        Args:
            transaction_id: 事务ID
            commit: 是否提交（False表示回滚）
        """
        with self.checkpoint_lock:
            if transaction_id in self.active_transactions:
                self.active_transactions.remove(transaction_id)
            if transaction_id in self.transaction_start_lsn:
                del self.transaction_start_lsn[transaction_id]

            status = "committed" if commit else "aborted"
            self.logger.debug(f"Transaction {transaction_id} {status}")

    def create_checkpoint(self, force: bool = False) -> CheckpointMetadata:
        """
        创建检查点

        Args:
            force: 是否强制创建（忽略间隔限制）

        Returns:
            CheckpointMetadata: 检查点元数据
        """
        with self.checkpoint_lock:
            # 检查是否需要检查点
            if not force and not self._should_checkpoint():
                return self.last_checkpoint

            self.logger.info("Creating checkpoint")
            start_time = time.time()

            # 获取当前LSN
            current_lsn = self.writer.total_records_written

            # 写入检查点开始记录
            begin_record = LogRecord(
                lsn=current_lsn,
                record_type=LogRecordType.CHECKPOINT_BEGIN,
                metadata={
                    'dirty_page_count': len(self.dirty_pages),
                    'active_transaction_count': len(self.active_transactions)
                }
            )
            self.writer.write(begin_record, force_sync=True)

            # 创建检查点元数据
            file_number, file_offset = self.writer.get_current_position()
            metadata = CheckpointMetadata(
                checkpoint_lsn=current_lsn + 1,
                checkpoint_time=time.time(),
                start_lsn=self.last_checkpoint.end_lsn if self.last_checkpoint else 0,
                end_lsn=current_lsn,
                dirty_pages=self.dirty_pages.copy(),
                active_transactions=list(self.active_transactions),
                file_number=file_number,
                file_offset=file_offset
            )

            # 写入检查点结束记录
            end_record = LogRecord(
                lsn=current_lsn + 2,
                record_type=LogRecordType.CHECKPOINT_END,
                data=json.dumps(metadata.to_dict()).encode('utf-8')
            )
            self.writer.write(end_record, force_sync=True)

            # 保存检查点元数据
            self._save_checkpoint_metadata(metadata)

            # 更新状态
            self.last_checkpoint = metadata
            self.last_checkpoint_time = time.time()
            self.records_since_checkpoint = 0
            self.total_checkpoints += 1

            # 清理旧日志（可选）
            if self.total_checkpoints % 10 == 0:  # 每10个检查点清理一次
                self._cleanup_old_logs()

            elapsed = time.time() - start_time
            self.logger.info(f"Checkpoint created",
                             lsn=metadata.checkpoint_lsn,
                             dirty_pages=len(metadata.dirty_pages),
                             active_transactions=len(metadata.active_transactions),
                             elapsed_ms=int(elapsed * 1000))

            return metadata

    def _cleanup_old_logs(self):
        """清理旧的日志文件"""
        if not self.last_checkpoint:
            return

        try:
            # 获取所有日志文件
            log_files = sorted(self.wal_dir.glob("wal_*.log"))

            # 保留最近的几个文件
            min_file_to_keep = max(0, self.last_checkpoint.file_number - 2)

            cleaned = 0
            for log_file in log_files:
                try:
                    file_number = int(log_file.stem.split('_')[1])
                    if file_number < min_file_to_keep:
                        log_file.unlink()
                        cleaned += 1
                        self.logger.debug(f"Cleaned old log file: {log_file.name}")
                except:
                    continue

            if cleaned > 0:
                self.total_log_cleanups += 1
                self.logger.info(f"Cleaned {cleaned} old log files")

        except Exception as e:
            self.logger.error(f"Failed to cleanup old logs: {e}")

    def get_recovery_info(self) -> Optional[Dict]:
        """
        获取恢复所需的信息

        Returns:
            dict: 恢复信息
        """
        if not self.last_checkpoint:
            return None

        return {
            'checkpoint_lsn': self.last_checkpoint.checkpoint_lsn,
            'start_lsn': self.last_checkpoint.start_lsn,
            'end_lsn': self.last_checkpoint.end_lsn,
            'dirty_pages': self.last_checkpoint.dirty_pages,
            'active_transactions': self.last_checkpoint.active_transactions,
            'file_number': self.last_checkpoint.file_number,
            'file_offset': self.last_checkpoint.file_offset
        }

    def get_statistics(self) -> dict:
        """获取统计信息"""
        return {
            'total_checkpoints': self.total_checkpoints,
            'total_log_cleanups': self.total_log_cleanups,
            'records_since_checkpoint': self.records_since_checkpoint,
            'time_since_checkpoint': time.time() - self.last_checkpoint_time,
            'active_transactions': len(self.active_transactions),
            'dirty_pages': len(self.dirty_pages),
            'last_checkpoint_lsn': self.last_checkpoint.checkpoint_lsn if self.last_checkpoint else None
        }

    def stop(self):
        """停止检查点管理器"""
        # 停止自动检查点
        if self.checkpoint_timer:
            self.checkpoint_timer.cancel()

        # 创建最终检查点
        self.create_checkpoint(force=True)

        self.logger.info("Checkpoint Manager stopped",
                         total_checkpoints=self.total_checkpoints,
                         total_cleanups=self.total_log_cleanups)