"""
WAL恢复管理器
负责系统崩溃后的数据恢复，实现ARIES算法的简化版本
"""

import time
from typing import Dict, List, Set, Optional, Tuple
from pathlib import Path

from .log_record import LogRecord, LogRecordType
from .log_reader import LogReader
from .checkpoint import CheckpointMetadata
from ...utils.logger import get_logger
from ...utils.exceptions import StorageException


class RecoveryManager:
    """
    恢复管理器

    实现三阶段恢复：
    1. 分析阶段（Analysis）：确定需要恢复的内容
    2. 重做阶段（Redo）：重放所有操作
    3. 回滚阶段（Undo）：回滚未提交事务
    """

    def __init__(self,
                 storage_manager,
                 wal_dir: str = "data/wal"):
        """
        初始化恢复管理器

        Args:
            storage_manager: 存储管理器实例
            wal_dir: WAL目录
        """
        self.storage_manager = storage_manager
        self.wal_dir = Path(wal_dir)

        # 恢复状态
        self.dirty_pages: Dict[int, int] = {}  # {page_id: recovery_lsn}
        self.active_transactions: Set[int] = set()
        self.transaction_table: Dict[int, dict] = {}  # 事务表
        self.redo_lsn = 0  # 重做起始LSN
        self.checkpoint_metadata: Optional[CheckpointMetadata] = None

        # 统计信息
        self.pages_recovered = 0
        self.transactions_rolled_back = 0
        self.logs_processed = 0
        self.recovery_time = 0

        # 日志器
        self.logger = get_logger("recovery")

    def recover(self) -> dict:
        """
        执行完整的恢复流程

        Returns:
            dict: 恢复统计信息
        """
        self.logger.info("Starting recovery process")
        start_time = time.time()

        try:
            # 1. 分析阶段
            self.logger.info("Phase 1: Analysis")
            self._analysis_phase()

            # 2. 重做阶段
            self.logger.info("Phase 2: Redo")
            self._redo_phase()

            # 3. 回滚阶段
            self.logger.info("Phase 3: Undo")
            self._undo_phase()

            self.recovery_time = time.time() - start_time

            stats = self.get_statistics()
            self.logger.info(f"Recovery completed successfully",
                             pages_recovered=self.pages_recovered,
                             transactions_rolled_back=self.transactions_rolled_back,
                             logs_processed=self.logs_processed,
                             elapsed_seconds=round(self.recovery_time, 2))

            return stats

        except Exception as e:
            self.logger.error(f"Recovery failed: {e}")
            raise StorageException(f"Recovery failed: {e}")

    def _analysis_phase(self):
        """
        分析阶段：扫描日志，重建崩溃时的状态

        目标：
        1. 确定重做起始点
        2. 识别脏页
        3. 识别活跃事务
        """
        self.logger.debug("Starting analysis phase")

        # 加载最后的检查点
        checkpoint = self._load_last_checkpoint()

        if checkpoint:
            # 从检查点恢复状态
            self.checkpoint_metadata = checkpoint
            self.dirty_pages = checkpoint.dirty_pages.copy()
            self.active_transactions = set(checkpoint.active_transactions)
            self.redo_lsn = checkpoint.start_lsn

            self.logger.info(f"Found checkpoint at LSN {checkpoint.checkpoint_lsn}",
                             dirty_pages=len(self.dirty_pages),
                             active_transactions=len(self.active_transactions))

            # 从检查点后开始扫描
            start_lsn = checkpoint.checkpoint_lsn + 1
        else:
            # 没有检查点，从头开始
            self.logger.warning("No checkpoint found, scanning from beginning")
            start_lsn = 0
            self.redo_lsn = 0

        # 扫描日志，更新状态
        reader = LogReader(self.wal_dir)

        for record in reader.read_from_lsn(start_lsn):
            self._analyze_log_record(record)
            self.logs_processed += 1

        self.logger.info(f"Analysis phase completed",
                         redo_lsn=self.redo_lsn,
                         dirty_pages=len(self.dirty_pages),
                         active_transactions=len(self.active_transactions),
                         logs_analyzed=self.logs_processed)

    def _analyze_log_record(self, record: LogRecord):
        """分析单条日志记录"""
        # 更新事务表
        if record.transaction_id:
            if record.record_type == LogRecordType.TRANSACTION_BEGIN:
                self.active_transactions.add(record.transaction_id)
                self.transaction_table[record.transaction_id] = {
                    'status': 'active',
                    'first_lsn': record.lsn,
                    'last_lsn': record.lsn,
                    'undo_next_lsn': None
                }
            elif record.record_type == LogRecordType.TRANSACTION_COMMIT:
                if record.transaction_id in self.active_transactions:
                    self.active_transactions.remove(record.transaction_id)
                if record.transaction_id in self.transaction_table:
                    self.transaction_table[record.transaction_id]['status'] = 'committed'
            elif record.record_type == LogRecordType.TRANSACTION_ABORT:
                if record.transaction_id in self.active_transactions:
                    self.active_transactions.remove(record.transaction_id)
                if record.transaction_id in self.transaction_table:
                    self.transaction_table[record.transaction_id]['status'] = 'aborted'

            # 更新最后LSN
            if record.transaction_id in self.transaction_table:
                self.transaction_table[record.transaction_id]['last_lsn'] = record.lsn

        # 更新脏页表
        if record.is_page_related() and record.page_id:
            if record.page_id not in self.dirty_pages:
                self.dirty_pages[record.page_id] = record.lsn

    def _redo_phase(self):
        """
        重做阶段：重放所有需要重做的操作

        从redo_lsn开始，重做所有操作直到日志末尾
        """
        self.logger.debug(f"Starting redo phase from LSN {self.redo_lsn}")

        if self.redo_lsn == 0 and len(self.dirty_pages) == 0:
            self.logger.info("No operations to redo")
            return

        reader = LogReader(self.wal_dir)
        redo_count = 0
        skip_count = 0

        for record in reader.read_from_lsn(self.redo_lsn):
            if self._should_redo(record):
                self._redo_operation(record)
                redo_count += 1
            else:
                skip_count += 1

            self.logs_processed += 1

            # 定期报告进度
            if (redo_count + skip_count) % 1000 == 0:
                self.logger.debug(f"Redo progress: {redo_count} redone, {skip_count} skipped")

        # 刷新所有脏页
        self.storage_manager.flush_all_pages()

        self.logger.info(f"Redo phase completed",
                         operations_redone=redo_count,
                         operations_skipped=skip_count)

    def _should_redo(self, record: LogRecord) -> bool:
        """
        判断是否需要重做操作

        条件：
        1. 是页面相关操作
        2. 页面在脏页表中
        3. 记录的LSN >= 页面的recovery_lsn
        """
        if not record.is_page_related() or not record.page_id:
            return False

        if record.page_id not in self.dirty_pages:
            return False

        recovery_lsn = self.dirty_pages[record.page_id]
        return record.lsn >= recovery_lsn

    def _redo_operation(self, record: LogRecord):
        """重做单个操作"""
        try:
            if record.record_type == LogRecordType.PAGE_WRITE:
                # 重做页面写入
                self.storage_manager.write_page(record.page_id, record.data)
                self.pages_recovered += 1
                self.logger.debug(f"Redone page write for page {record.page_id}")

            elif record.record_type == LogRecordType.PAGE_UPDATE:
                # 重做页面更新（部分更新）
                # 先读取页面
                current_data = self.storage_manager.read_page(record.page_id)

                # 应用更新（这里简化处理，实际应该有更复杂的合并逻辑）
                if record.metadata and 'offset' in record.metadata:
                    offset = record.metadata['offset']
                    updated_data = bytearray(current_data)
                    update_len = min(len(record.data), len(updated_data) - offset)
                    updated_data[offset:offset + update_len] = record.data[:update_len]
                    self.storage_manager.write_page(record.page_id, bytes(updated_data))
                else:
                    # 没有偏移信息，执行完整写入
                    self.storage_manager.write_page(record.page_id, record.data)

                self.pages_recovered += 1
                self.logger.debug(f"Redone page update for page {record.page_id}")

        except Exception as e:
            self.logger.error(f"Failed to redo operation: {e}")
            # 继续恢复，不因单个操作失败而中断

    def _undo_phase(self):
        """
        回滚阶段：回滚所有未提交的事务

        从活跃事务的最后LSN开始，反向回滚
        """
        self.logger.debug(f"Starting undo phase with {len(self.active_transactions)} active transactions")

        if not self.active_transactions:
            self.logger.info("No transactions to rollback")
            return

        # 构建需要回滚的LSN列表
        undo_list = []
        for txn_id in self.active_transactions:
            if txn_id in self.transaction_table:
                last_lsn = self.transaction_table[txn_id].get('last_lsn')
                if last_lsn:
                    undo_list.append((last_lsn, txn_id))

        if not undo_list:
            self.logger.info("No operations to undo")
            return

        # 按LSN降序排序
        undo_list.sort(reverse=True)

        # 读取所有日志记录到内存（用于反向遍历）
        reader = LogReader(self.wal_dir)
        all_records = list(reader.read_all())
        record_by_lsn = {r.lsn: r for r in all_records}

        # 执行回滚
        rolled_back_txns = set()
        undo_count = 0

        while undo_list:
            lsn, txn_id = undo_list.pop(0)

            if lsn not in record_by_lsn:
                continue

            record = record_by_lsn[lsn]

            # 只回滚属于活跃事务的操作
            if record.transaction_id != txn_id:
                continue

            # 回滚操作
            if record.is_page_related() and record.page_id:
                self._undo_operation(record)
                undo_count += 1

            # 标记事务已回滚
            rolled_back_txns.add(txn_id)

            # 查找该事务的前一个操作
            # 简化处理：扫描更早的日志
            for prev_lsn in range(lsn - 1, 0, -1):
                if prev_lsn in record_by_lsn:
                    prev_record = record_by_lsn[prev_lsn]
                    if prev_record.transaction_id == txn_id:
                        undo_list.append((prev_lsn, txn_id))
                        break

        self.transactions_rolled_back = len(rolled_back_txns)

        self.logger.info(f"Undo phase completed",
                         transactions_rolled_back=self.transactions_rolled_back,
                         operations_undone=undo_count)

    def _undo_operation(self, record: LogRecord):
        """回滚单个操作"""
        try:
            # 简化处理：对于页面操作，我们假设有before image
            # 实际系统中，应该记录before image或使用补偿日志记录

            if record.metadata and 'before_image' in record.metadata:
                # 如果有before image，恢复它
                before_data = record.metadata['before_image']
                self.storage_manager.write_page(record.page_id, before_data)
                self.logger.debug(f"Undone operation for page {record.page_id}")
            else:
                # 没有before image，标记页面需要重建
                self.logger.warning(f"No before image for page {record.page_id}, marking for rebuild")

        except Exception as e:
            self.logger.error(f"Failed to undo operation: {e}")

    def _load_last_checkpoint(self) -> Optional[CheckpointMetadata]:
        """加载最后的检查点"""
        checkpoint_file = self.wal_dir / "checkpoint.json"

        if not checkpoint_file.exists():
            return None

        try:
            import json
            with open(checkpoint_file, 'r') as f:
                data = json.load(f)
                return CheckpointMetadata.from_dict(data)
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            return None

    def get_statistics(self) -> dict:
        """获取恢复统计信息"""
        return {
            'recovery_time': round(self.recovery_time, 2),
            'pages_recovered': self.pages_recovered,
            'transactions_rolled_back': self.transactions_rolled_back,
            'logs_processed': self.logs_processed,
            'dirty_pages_found': len(self.dirty_pages),
            'active_transactions_found': len(self.active_transactions),
            'redo_start_lsn': self.redo_lsn
        }