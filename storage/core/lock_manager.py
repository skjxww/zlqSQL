"""
锁管理器 - 实现并发控制
Author: Your Name
Date: 2024-01-13
"""

from enum import Enum
from threading import RLock
import time
from typing import Dict, Set, Optional
import logging
from storage.utils.exceptions import StorageException


class LockType(Enum):
    """锁类型"""
    SHARED = "S"  # 共享锁（读锁）
    EXCLUSIVE = "X"  # 排他锁（写锁）


class SimpleLockManager:
    """
    简化的锁管理器
    - 页级锁
    - 超时机制防止死锁
    - 自动锁管理
    """

    def __init__(self, timeout: float = 5.0):
        """
        初始化锁管理器

        Args:
            timeout: 锁等待超时时间（秒）
        """
        # 锁表：page_id -> lock_info
        self.locks = {}

        # 事务持有的锁：txn_id -> set of (page_id, lock_type)
        self.txn_locks = {}

        # 全局互斥锁
        self.mutex = RLock()

        # 配置
        self.timeout = timeout

        # 日志
        self.logger = logging.getLogger(__name__)

        # 统计信息
        self.stats = {
            'locks_granted': 0,
            'locks_waited': 0,
            'locks_timeout': 0,
            'deadlocks_prevented': 0
        }

    def acquire_lock(self, txn_id: int, page_id: int, lock_type: LockType) -> bool:
        """
        获取锁

        Args:
            txn_id: 事务ID
            page_id: 页ID
            lock_type: 锁类型

        Returns:
            bool: 是否成功获取锁
        """
        start_time = time.time()
        wait_count = 0

        with self.mutex:
            # 初始化锁表项
            if page_id not in self.locks:
                self.locks[page_id] = {
                    'S_holders': set(),  # 共享锁持有者
                    'X_holder': None,  # 排他锁持有者
                    'waiters': []  # 等待队列
                }

            # 初始化事务锁集合
            if txn_id not in self.txn_locks:
                self.txn_locks[txn_id] = set()

            # 检查是否已持有兼容的锁
            if self._already_holds_lock(txn_id, page_id, lock_type):
                return True

            # 尝试获取锁
            while True:
                if self._can_grant_lock(txn_id, page_id, lock_type):
                    self._grant_lock(txn_id, page_id, lock_type)

                    if wait_count > 0:
                        self.stats['locks_waited'] += 1
                        self.logger.debug(f"Lock acquired after {wait_count} waits: "
                                          f"txn={txn_id}, page={page_id}, type={lock_type.value}")
                    else:
                        self.stats['locks_granted'] += 1

                    return True

                # 检查超时
                elapsed = time.time() - start_time
                if elapsed > self.timeout:
                    self.stats['locks_timeout'] += 1
                    self.stats['deadlocks_prevented'] += 1
                    self.logger.warning(f"Lock timeout (possible deadlock prevented): "
                                        f"txn={txn_id}, page={page_id}, type={lock_type.value}, "
                                        f"elapsed={elapsed:.2f}s")
                    return False

                wait_count += 1

        # 在锁外等待，避免长时间持有互斥锁
        time.sleep(0.01)  # 10ms

    def _already_holds_lock(self, txn_id: int, page_id: int, lock_type: LockType) -> bool:
        """检查事务是否已持有兼容的锁"""
        for held_page, held_type in self.txn_locks.get(txn_id, set()):
            if held_page == page_id:
                # 如果已持有X锁，则兼容任何锁请求
                if held_type == LockType.EXCLUSIVE:
                    return True
                # 如果已持有S锁，只兼容S锁请求
                if held_type == LockType.SHARED and lock_type == LockType.SHARED:
                    return True
        return False

    def _can_grant_lock(self, txn_id: int, page_id: int, lock_type: LockType) -> bool:
        """
        检查是否可以授予锁

        锁兼容性矩阵：
                 | S | X |
        ---------+---+---+
        S        | Y | N |
        X        | N | N |
        """
        lock_info = self.locks[page_id]

        if lock_type == LockType.SHARED:
            # 请求S锁：检查是否有其他事务的X锁
            return lock_info['X_holder'] is None or lock_info['X_holder'] == txn_id

        else:  # EXCLUSIVE
            # 请求X锁：检查是否有其他事务的任何锁
            other_s_holders = lock_info['S_holders'] - {txn_id}
            no_other_shared = len(other_s_holders) == 0
            no_other_exclusive = lock_info['X_holder'] is None or lock_info['X_holder'] == txn_id
            return no_other_shared and no_other_exclusive

    def _grant_lock(self, txn_id: int, page_id: int, lock_type: LockType):
        """授予锁"""
        lock_info = self.locks[page_id]

        if lock_type == LockType.SHARED:
            lock_info['S_holders'].add(txn_id)
        else:  # EXCLUSIVE
            lock_info['X_holder'] = txn_id
            # 锁升级：如果之前有S锁，移除
            lock_info['S_holders'].discard(txn_id)

        # 记录事务持有的锁
        self.txn_locks[txn_id].add((page_id, lock_type))

        self.logger.debug(f"Lock granted: txn={txn_id}, page={page_id}, type={lock_type.value}")

    def release_transaction_locks(self, txn_id: int):
        """
        释放事务的所有锁

        Args:
            txn_id: 事务ID
        """
        with self.mutex:
            if txn_id not in self.txn_locks:
                return

            released_locks = []

            # 释放所有页面的锁
            for page_id, lock_type in self.txn_locks[txn_id]:
                if page_id in self.locks:
                    lock_info = self.locks[page_id]

                    if lock_type == LockType.SHARED:
                        lock_info['S_holders'].discard(txn_id)
                    elif lock_info['X_holder'] == txn_id:
                        lock_info['X_holder'] = None

                    released_locks.append((page_id, lock_type.value))

                    # 清理空的锁表项
                    if not lock_info['S_holders'] and lock_info['X_holder'] is None:
                        del self.locks[page_id]

            # 清理事务记录
            del self.txn_locks[txn_id]

            if released_locks:
                self.logger.debug(f"Released {len(released_locks)} locks for txn={txn_id}: {released_locks}")

    def get_lock_info(self, page_id: int) -> Optional[Dict]:
        """获取页面的锁信息（用于调试和监控）"""
        with self.mutex:
            if page_id in self.locks:
                info = self.locks[page_id].copy()
                info['S_holders'] = list(info['S_holders'])
                return info
            return None

    def get_transaction_locks(self, txn_id: int) -> Set:
        """获取事务持有的所有锁"""
        with self.mutex:
            return self.txn_locks.get(txn_id, set()).copy()

    def get_statistics(self) -> Dict:
        """获取统计信息"""
        with self.mutex:
            stats = self.stats.copy()
            stats['active_locks'] = sum(
                len(info['S_holders']) + (1 if info['X_holder'] else 0)
                for info in self.locks.values()
            )
            stats['active_transactions'] = len(self.txn_locks)
            return stats

    def clear_all_locks(self):
        """清除所有锁（仅用于测试）"""
        with self.mutex:
            self.locks.clear()
            self.txn_locks.clear()
            self.logger.info("All locks cleared")