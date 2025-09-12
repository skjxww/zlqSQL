"""
WAL (Write-Ahead Logging) 模块
提供完整的预写日志功能，确保数据持久性和崩溃恢复
"""

from .log_record import LogRecord, LogRecordType, LogRecordBatch
from .log_writer import LogWriter, SyncMode
from .log_reader import LogReader
from .checkpoint import CheckpointManager, CheckpointMetadata
from .recovery import RecoveryManager
from .wal_stats import WALStatistics, WALMetrics
from .wal_manager import WALManager

__all__ = [
    # 核心管理器
    'WALManager',

    # 日志记录
    'LogRecord',
    'LogRecordType',
    'LogRecordBatch',

    # 读写组件
    'LogWriter',
    'LogReader',
    'SyncMode',

    # 检查点
    'CheckpointManager',
    'CheckpointMetadata',

    # 恢复
    'RecoveryManager',

    # 统计
    'WALStatistics',
    'WALMetrics',
]

# 版本信息
__version__ = '1.0.0'
__author__ = 'Storage Team'

# 模块描述
__doc__ = """
WAL模块实现了完整的预写日志机制，包括：

核心功能：
- 日志记录和持久化
- 崩溃恢复（ARIES算法简化版）
- 检查点管理
- 事务支持

高级特性：
- 批量写入优化
- 日志压缩
- 自适应同步策略
- 性能监控和统计
- 健康检查

使用示例：
    from storage.core.wal import WALManager

    # 创建WAL管理器
    wal = WALManager(
        storage_manager=storage,
        enable_wal=True,
        sync_mode='fsync'
    )

    # 在事务中使用
    with wal.transaction() as txn_id:
        wal.write_page(page_id, data, txn_id)
        # 自动提交或回滚
"""