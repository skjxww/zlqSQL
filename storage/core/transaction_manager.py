"""
事务管理器 - 提供ACID事务支持
"""

import time
import threading
import enum
from typing import Dict, List, Optional, Set, Tuple, Any
from collections import defaultdict
import json
import os

from ..utils.exceptions import StorageException, TransactionException
from ..utils.logger import get_logger
from ..utils.constants import PAGE_SIZE


class TransactionState(enum.Enum):
    """事务状态枚举"""
    ACTIVE = "ACTIVE"  # 活跃中
    COMMITTED = "COMMITTED"  # 已提交
    ABORTED = "ABORTED"  # 已回滚
    PREPARING = "PREPARING"  # 准备提交中（2PC用）


class IsolationLevel(enum.Enum):
    """事务隔离级别"""
    READ_UNCOMMITTED = 0  # 读未提交
    READ_COMMITTED = 1  # 读已提交
    REPEATABLE_READ = 2  # 可重复读
    SERIALIZABLE = 3  # 串行化


class Transaction:
    """事务对象"""

    def __init__(self, txn_id: int, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        self.txn_id = txn_id
        self.state = TransactionState.ACTIVE
        self.isolation_level = isolation_level

        # 时间戳
        self.start_time = time.time()
        self.end_time = None

        # 事务修改记录
        self.modified_pages: Set[int] = set()  # 修改过的页号
        self.undo_log: List[Tuple[int, bytes]] = []  # (page_id, old_data)
        self.redo_log: List[Tuple[int, bytes]] = []  # (page_id, new_data)

        # 读写集合（用于冲突检测）
        self.read_set: Set[int] = set()  # 读过的页号
        self.write_set: Set[int] = set()  # 写过的页号

        # 锁信息（简单实现）
        self.held_locks: Dict[int, str] = {}  # {page_id: lock_type}
        self.wal_txn_id = None  # WAL事务ID

    def add_undo_record(self, page_id: int, old_data: bytes):
        """添加undo记录"""
        self.undo_log.append((page_id, old_data))
        self.modified_pages.add(page_id)
        self.write_set.add(page_id)

    def add_redo_record(self, page_id: int, new_data: bytes):
        """添加redo记录"""
        self.redo_log.append((page_id, new_data))

    def add_read_record(self, page_id: int):
        """记录读操作"""
        self.read_set.add(page_id)

    def to_dict(self) -> dict:
        """转换为字典（用于持久化）"""
        return {
            'txn_id': self.txn_id,
            'state': self.state.value,
            'isolation_level': self.isolation_level.value,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'modified_pages': list(self.modified_pages),
            'read_set': list(self.read_set),
            'write_set': list(self.write_set)
        }


class TransactionManager:
    """事务管理器"""

    def __init__(self, storage_manager, wal_enabled: bool = True):
        """
        初始化事务管理器

        Args:
            storage_manager: 存储管理器引用
            wal_enabled: 是否启用WAL
        """
        self.storage_manager = storage_manager
        self.wal_enabled = wal_enabled
        self.logger = get_logger("transaction")

        # 事务管理
        self.transactions: Dict[int, Transaction] = {}  # 活跃事务
        self.next_txn_id = 1
        self.txn_counter_lock = threading.Lock()

        # 全局锁表（简单的页级锁）
        self.lock_table: Dict[int, Dict[str, Set[int]]] = defaultdict(lambda: {'S': set(), 'X': set()})
        self.lock_table_lock = threading.Lock()

        # 事务历史（用于恢复和审计）
        self.txn_history: List[Dict] = []
        self.history_file = os.path.join(
            os.path.dirname(storage_manager.page_manager.meta_file),
            "transaction_history.json"
        )

        # 加载历史事务信息
        self._load_history()

        # 版本管理（简单的MVCC）
        self.page_versions: Dict[int, List[Tuple[int, bytes, int]]] = defaultdict(list)
        # 格式: {page_id: [(txn_id, data, timestamp), ...]}

        self.logger.info(f"TransactionManager initialized (WAL={'enabled' if wal_enabled else 'disabled'})")

    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> int:
        """开始一个新事务"""
        with self.txn_counter_lock:
            txn_id = self.next_txn_id
            self.next_txn_id += 1

        # 创建事务对象
        txn = Transaction(txn_id, isolation_level)

        # 如果启用WAL，记录事务开始
        if self.wal_enabled and hasattr(self.storage_manager, 'wal_manager'):
            try:
                # 使用WAL的事务ID（如果WAL管理自己的ID）
                wal_txn_id = self.storage_manager.wal_manager.begin_transaction()
                txn.wal_txn_id = wal_txn_id
                self.logger.debug(f"Transaction {txn_id} mapped to WAL transaction {wal_txn_id}")
            except Exception as e:
                self.logger.warning(f"WAL transaction begin failed: {e}, continuing without WAL")
                txn.wal_txn_id = None

        self.transactions[txn_id] = txn

        self.logger.info(f"Transaction {txn_id} started with isolation level {isolation_level.name}")
        return txn_id

    def get_transaction(self, txn_id: int) -> Optional[Transaction]:
        """获取事务对象"""
        return self.transactions.get(txn_id)

    def prepare_write(self, txn_id: int, page_id: int) -> bool:
        """
        准备写操作（获取写锁，保存undo信息）

        Args:
            txn_id: 事务ID
            page_id: 页号

        Returns:
            bool: 是否成功
        """
        txn = self.get_transaction(txn_id)
        if not txn or txn.state != TransactionState.ACTIVE:
            raise TransactionException(f"Transaction {txn_id} is not active")

        # 获取写锁（简化版：不等待，直接失败）
        if not self._acquire_lock(txn_id, page_id, 'X'):
            raise TransactionException(f"Failed to acquire write lock on page {page_id}")

        # 如果是第一次修改这个页，保存原始数据
        if page_id not in txn.modified_pages:
            try:
                # 读取当前数据作为undo信息
                original_data = self.storage_manager.read_page(page_id)
                txn.add_undo_record(page_id, original_data)

                self.logger.debug(f"Transaction {txn_id} saved undo record for page {page_id}")
            except Exception as e:
                # 释放锁
                self._release_lock(txn_id, page_id)
                raise TransactionException(f"Failed to prepare write: {e}")

        return True

    def record_write(self, txn_id: int, page_id: int, new_data: bytes):
        """
        记录写操作（用于redo）

        Args:
            txn_id: 事务ID
            page_id: 页号
            new_data: 新数据
        """
        txn = self.get_transaction(txn_id)
        if not txn:
            return

        txn.add_redo_record(page_id, new_data)

        # 保存版本信息（MVCC）
        timestamp = int(time.time() * 1000000)  # 微秒时间戳
        self.page_versions[page_id].append((txn_id, new_data, timestamp))

        # 限制版本数量（保留最近10个版本）
        if len(self.page_versions[page_id]) > 10:
            self.page_versions[page_id] = self.page_versions[page_id][-10:]

    def prepare_read(self, txn_id: int, page_id: int) -> bool:
        """
        准备读操作（获取读锁）

        Args:
            txn_id: 事务ID
            page_id: 页号

        Returns:
            bool: 是否成功
        """
        txn = self.get_transaction(txn_id)
        if not txn or txn.state != TransactionState.ACTIVE:
            raise TransactionException(f"Transaction {txn_id} is not active")

        # 根据隔离级别处理
        if txn.isolation_level == IsolationLevel.READ_UNCOMMITTED:
            # 读未提交：不需要锁
            pass
        else:
            # 其他级别：需要读锁
            if not self._acquire_lock(txn_id, page_id, 'S'):
                raise TransactionException(f"Failed to acquire read lock on page {page_id}")

        txn.add_read_record(page_id)
        return True

    def get_visible_data(self, txn_id: int, page_id: int) -> Optional[bytes]:
        """获取事务可见的数据版本（MVCC）"""
        txn = self.get_transaction(txn_id)
        if not txn:
            return None

        # 根据隔离级别返回合适的版本
        if txn.isolation_level == IsolationLevel.READ_UNCOMMITTED:
            # 读最新版本（包括未提交的）
            if page_id in self.page_versions and self.page_versions[page_id]:
                return self.page_versions[page_id][-1][1]

        elif txn.isolation_level == IsolationLevel.READ_COMMITTED:
            # 读最新已提交版本
            for txn_id_v, data, _ in reversed(self.page_versions.get(page_id, [])):
                if txn_id_v == txn_id:  # 自己的修改
                    return data
                if txn_id_v not in self.transactions:  # 已提交的事务
                    return data

        # 修改这里：使用.value进行比较
        elif txn.isolation_level.value >= IsolationLevel.REPEATABLE_READ.value:
            # 读事务开始时的版本（快照隔离）
            txn_start_time = txn.start_time * 1000000  # 转换为微秒
            for txn_id_v, data, timestamp in reversed(self.page_versions.get(page_id, [])):
                if timestamp <= txn_start_time:
                    return data

        return None  # 使用物理存储的版本

    def commit(self, txn_id: int):
        """提交事务"""
        print(f"DEBUG: Starting commit for transaction {txn_id}")

        txn = self.get_transaction(txn_id)
        if not txn:
            raise TransactionException(f"Transaction {txn_id} not found")

        if txn.state != TransactionState.ACTIVE:
            raise TransactionException(f"Transaction {txn_id} is not active")

        try:
            print(f"DEBUG: Setting transaction {txn_id} to PREPARING")
            txn.state = TransactionState.PREPARING

            print(f"DEBUG: Flushing modified pages: {txn.modified_pages}")
            # 将所有修改刷到磁盘
            for page_id in txn.modified_pages:
                self.storage_manager.flush_page(page_id)

            print(f"DEBUG: All pages flushed")

            print(f"DEBUG: Marking transaction as COMMITTED")
            # 标记为已提交
            txn.state = TransactionState.COMMITTED
            txn.end_time = time.time()

            # 暂时跳过锁释放，因为我们没有实际使用锁
            # print(f"DEBUG: Releasing locks")
            # self._release_all_locks(txn_id)

            print(f"DEBUG: Adding to history")
            # 记录到历史
            self._add_to_history(txn)

            print(f"DEBUG: Removing from active transactions")
            # 从活跃事务中移除
            del self.transactions[txn_id]

            self.logger.info(f"Transaction {txn_id} committed successfully")
            print(f"DEBUG: Commit completed for transaction {txn_id}")

        except Exception as e:
            print(f"DEBUG: Commit failed with error: {e}")
            import traceback
            traceback.print_exc()
            # 提交失败，执行回滚
            self.logger.error(f"Failed to commit transaction {txn_id}: {e}")
            self.rollback(txn_id)
            raise TransactionException(f"Commit failed: {e}")

    def rollback(self, txn_id: int):
        """
        回滚事务

        Args:
            txn_id: 事务ID
        """
        txn = self.get_transaction(txn_id)
        if not txn:
            self.logger.warning(f"Transaction {txn_id} not found for rollback")
            return

        if txn.state == TransactionState.COMMITTED:
            raise TransactionException(f"Cannot rollback committed transaction {txn_id}")

        try:
            self.logger.info(f"Rolling back transaction {txn_id}")

            # 标记为回滚中
            txn.state = TransactionState.ABORTED

            # 使用undo log恢复数据
            for page_id, old_data in reversed(txn.undo_log):
                try:
                    self.storage_manager.write_page(page_id, old_data)
                    self.logger.debug(f"Restored page {page_id} for transaction {txn_id}")
                except Exception as e:
                    self.logger.error(f"Failed to restore page {page_id}: {e}")

            # 清理版本信息
            for page_id in txn.modified_pages:
                if page_id in self.page_versions:
                    self.page_versions[page_id] = [
                        (t_id, data, ts) for t_id, data, ts in self.page_versions[page_id]
                        if t_id != txn_id
                    ]

            # 改为：
            if self.wal_enabled and hasattr(self.storage_manager, 'wal_manager'):
                try:
                    if hasattr(txn, 'wal_txn_id') and txn.wal_txn_id is not None:
                        self.storage_manager.wal_manager.abort_transaction(txn.wal_txn_id)
                        self.logger.debug(f"WAL transaction {txn.wal_txn_id} aborted")
                except Exception as e:
                    self.logger.warning(f"WAL abort failed: {e}, continuing anyway")

            # 释放所有锁
            self._release_all_locks(txn_id)

            # 记录到历史
            txn.end_time = time.time()
            self._add_to_history(txn)

            # 从活跃事务中移除
            del self.transactions[txn_id]

            self.logger.info(f"Transaction {txn_id} rolled back successfully")

        except Exception as e:
            self.logger.error(f"Error during rollback of transaction {txn_id}: {e}")
            raise TransactionException(f"Rollback failed: {e}")

    def _acquire_lock(self, txn_id: int, page_id: int, lock_type: str) -> bool:
        """
        获取锁（简化版本，不支持锁升级）

        Args:
            txn_id: 事务ID
            page_id: 页号
            lock_type: 锁类型 ('S' or 'X')

        Returns:
            bool: 是否成功
        """
        with self.lock_table_lock:
            page_locks = self.lock_table[page_id]

            if lock_type == 'S':  # 读锁
                # 检查是否有其他事务持有写锁
                if page_locks['X'] and txn_id not in page_locks['X']:
                    return False
                # 添加读锁
                page_locks['S'].add(txn_id)

            elif lock_type == 'X':  # 写锁
                # 检查是否有其他事务持有任何锁
                other_readers = page_locks['S'] - {txn_id}
                other_writers = page_locks['X'] - {txn_id}
                if other_readers or other_writers:
                    return False
                # 添加写锁
                page_locks['X'].add(txn_id)
                # 如果有读锁，升级为写锁
                page_locks['S'].discard(txn_id)

            # 记录到事务对象
            txn = self.get_transaction(txn_id)
            if txn:
                txn.held_locks[page_id] = lock_type

            return True

    def _release_lock(self, txn_id: int, page_id: int):
        """释放指定页的锁"""
        with self.lock_table_lock:
            page_locks = self.lock_table[page_id]
            page_locks['S'].discard(txn_id)
            page_locks['X'].discard(txn_id)

            # 如果页上没有任何锁，清理锁表
            if not page_locks['S'] and not page_locks['X']:
                del self.lock_table[page_id]

    def _release_all_locks(self, txn_id: int):
        """释放事务持有的所有锁"""
        txn = self.get_transaction(txn_id)
        if not txn:
            return

        with self.lock_table_lock:
            for page_id in list(txn.held_locks.keys()):
                self._release_lock(txn_id, page_id)
            txn.held_locks.clear()

    def _add_to_history(self, txn: Transaction):
        """添加事务到历史记录"""
        self.txn_history.append(txn.to_dict())

        # 限制历史记录数量
        if len(self.txn_history) > 1000:
            self.txn_history = self.txn_history[-1000:]

        # 异步保存历史
        self._save_history()

    def _save_history(self):
        """保存事务历史到文件"""
        try:
            with open(self.history_file, 'w') as f:
                json.dump(self.txn_history, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save transaction history: {e}")

    def _load_history(self):
        """加载事务历史"""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r') as f:
                    self.txn_history = json.load(f)

                # 恢复next_txn_id
                if self.txn_history:
                    max_id = max(h['txn_id'] for h in self.txn_history)
                    self.next_txn_id = max_id + 1

            except Exception as e:
                self.logger.error(f"Failed to load transaction history: {e}")

    def get_active_transactions(self) -> List[int]:
        """获取所有活跃事务ID"""
        return list(self.transactions.keys())

    def abort_all_transactions(self):
        """中止所有活跃事务（用于关闭时）"""
        for txn_id in list(self.transactions.keys()):
            try:
                self.rollback(txn_id)
            except Exception as e:
                self.logger.error(f"Failed to abort transaction {txn_id}: {e}")

    def get_statistics(self) -> dict:
        """获取事务管理器统计信息"""
        return {
            'active_transactions': len(self.transactions),
            'next_txn_id': self.next_txn_id,
            'total_commits': sum(1 for h in self.txn_history if h['state'] == 'COMMITTED'),
            'total_rollbacks': sum(1 for h in self.txn_history if h['state'] == 'ABORTED'),
            'lock_table_size': len(self.lock_table),
            'version_count': sum(len(v) for v in self.page_versions.values())
        }


# 异常类
class TransactionException(StorageException):
    """事务相关异常"""
    pass