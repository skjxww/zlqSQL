"""
WAL日志记录类型定义和序列化
支持多种日志类型，使用二进制格式提高效率
"""

import struct
import time
import zlib
from enum import IntEnum
from typing import Optional, Dict, Any, Tuple
import json

from ...utils.constants import PAGE_SIZE
from ...utils.exceptions import StorageException


class LogRecordType(IntEnum):
    """日志记录类型枚举"""
    PAGE_WRITE = 1  # 页面完整写入
    PAGE_UPDATE = 2  # 页面部分更新
    TRANSACTION_BEGIN = 3  # 事务开始
    TRANSACTION_COMMIT = 4  # 事务提交
    TRANSACTION_ABORT = 5  # 事务中止
    CHECKPOINT_BEGIN = 6  # 检查点开始
    CHECKPOINT_END = 7  # 检查点结束
    TABLE_CREATE = 8  # 表创建
    TABLE_DROP = 9  # 表删除
    INDEX_CREATE = 10  # 索引创建
    INDEX_DROP = 11  # 索引删除
    SYSTEM_INIT = 12  # 系统初始化


class LogRecord:
    """
    WAL日志记录

    二进制格式：
    [4字节 魔数][4字节 LSN][4字节 类型][8字节 时间戳][4字节 事务ID]
    [4字节 页号][4字节 数据长度][N字节 数据][4字节 CRC32校验]

    总头部大小：32字节
    """

    MAGIC_NUMBER = 0x57414C31  # 'WAL1' in hex
    HEADER_SIZE = 32

    def __init__(self,
                 lsn: int,
                 record_type: LogRecordType,
                 page_id: Optional[int] = None,
                 transaction_id: Optional[int] = None,
                 data: Optional[bytes] = None,
                 metadata: Optional[Dict[str, Any]] = None):
        """
        初始化日志记录

        Args:
            lsn: 日志序列号
            record_type: 记录类型
            page_id: 相关页号（可选）
            transaction_id: 事务ID（可选）
            data: 数据载荷（可选）
            metadata: 额外元数据（可选）
        """
        self.lsn = lsn
        self.record_type = record_type
        self.timestamp = time.time()
        self.page_id = page_id if page_id is not None else 0
        self.transaction_id = transaction_id if transaction_id is not None else 0
        self.data = data if data is not None else b''
        self.metadata = metadata if metadata is not None else {}

        # 计算属性
        self.data_length = len(self.data)
        self.crc32 = 0  # 将在序列化时计算

    def serialize(self, compress: bool = False) -> bytes:
        """
        将日志记录序列化为二进制格式

        Args:
            compress: 是否压缩数据部分

        Returns:
            bytes: 序列化后的二进制数据
        """
        # 准备数据
        actual_data = self.data
        if compress and len(actual_data) > 512:  # 只压缩较大的数据
            actual_data = zlib.compress(actual_data, level=6)
            # 在元数据中标记已压缩
            self.metadata['compressed'] = True

        # 如果有元数据，将其JSON序列化后附加到数据中
        if self.metadata:
            metadata_json = json.dumps(self.metadata).encode('utf-8')
            metadata_length = len(metadata_json)
            # 数据格式：[4字节 元数据长度][元数据JSON][原始数据]
            actual_data = struct.pack('<I', metadata_length) + metadata_json + actual_data

        # 打包头部
        header = struct.pack(
            '<IIII d II',  # 格式：<小端 4个int 1个double 2个int
            self.MAGIC_NUMBER,  # 魔数
            self.lsn,  # LSN
            int(self.record_type),  # 类型
            0,  # 预留字段（对齐用）
            self.timestamp,  # 时间戳（double）
            self.transaction_id,  # 事务ID
            self.page_id  # 页号
        )

        # 打包数据长度和数据
        data_part = struct.pack('<I', len(actual_data)) + actual_data

        # 计算CRC32（不包括CRC字段本身）
        full_data = header + data_part
        self.crc32 = zlib.crc32(full_data) & 0xffffffff

        # 添加CRC32到末尾
        return full_data + struct.pack('<I', self.crc32)

    @classmethod
    def deserialize(cls, data: bytes) -> 'LogRecord':
        """
        从二进制数据反序列化日志记录

        Args:
            data: 二进制数据

        Returns:
            LogRecord: 反序列化的日志记录

        Raises:
            StorageException: 数据格式错误或校验失败
        """
        if len(data) < cls.HEADER_SIZE + 8:  # 最小大小：头部+数据长度+CRC
            raise StorageException(f"Invalid log record size: {len(data)}")

        # 解析头部
        magic, lsn, record_type, reserved, timestamp, txn_id, page_id = struct.unpack(
            '<IIII d II', data[:cls.HEADER_SIZE]
        )

        # 验证魔数
        if magic != cls.MAGIC_NUMBER:
            raise StorageException(f"Invalid magic number: {hex(magic)}")

        # 解析数据长度
        data_length = struct.unpack('<I', data[cls.HEADER_SIZE:cls.HEADER_SIZE + 4])[0]

        # 验证总长度
        expected_length = cls.HEADER_SIZE + 4 + data_length + 4  # 头部+长度+数据+CRC
        if len(data) != expected_length:
            raise StorageException(
                f"Size mismatch: expected {expected_length}, got {len(data)}"
            )

        # 提取数据和CRC
        actual_data = data[cls.HEADER_SIZE + 4:cls.HEADER_SIZE + 4 + data_length]
        stored_crc = struct.unpack('<I', data[-4:])[0]

        # 验证CRC
        computed_crc = zlib.crc32(data[:-4]) & 0xffffffff
        if stored_crc != computed_crc:
            raise StorageException(
                f"CRC mismatch: stored={stored_crc}, computed={computed_crc}"
            )

        # 解析元数据（如果存在）
        metadata = {}
        if actual_data and len(actual_data) > 4:
            # 尝试解析元数据
            try:
                metadata_length = struct.unpack('<I', actual_data[:4])[0]
                if metadata_length > 0 and metadata_length < len(actual_data):
                    metadata_json = actual_data[4:4 + metadata_length]
                    metadata = json.loads(metadata_json.decode('utf-8'))
                    actual_data = actual_data[4 + metadata_length:]

                    # 如果数据被压缩，解压
                    if metadata.get('compressed'):
                        actual_data = zlib.decompress(actual_data)
                        del metadata['compressed']
            except:
                # 如果解析失败，认为没有元数据
                pass

        # 创建日志记录对象
        record = cls(
            lsn=lsn,
            record_type=LogRecordType(record_type),
            page_id=page_id if page_id != 0 else None,
            transaction_id=txn_id if txn_id != 0 else None,
            data=actual_data,
            metadata=metadata
        )
        record.timestamp = timestamp
        record.crc32 = stored_crc

        return record

    def get_size(self) -> int:
        """获取序列化后的大小"""
        metadata_size = 0
        if self.metadata:
            metadata_json = json.dumps(self.metadata).encode('utf-8')
            metadata_size = 4 + len(metadata_json)

        return self.HEADER_SIZE + 4 + metadata_size + len(self.data) + 4

    def is_page_related(self) -> bool:
        """判断是否是页面相关的日志"""
        return self.record_type in [
            LogRecordType.PAGE_WRITE,
            LogRecordType.PAGE_UPDATE
        ]

    def is_transaction_related(self) -> bool:
        """判断是否是事务相关的日志"""
        return self.record_type in [
            LogRecordType.TRANSACTION_BEGIN,
            LogRecordType.TRANSACTION_COMMIT,
            LogRecordType.TRANSACTION_ABORT
        ]

    def is_checkpoint(self) -> bool:
        """判断是否是检查点日志"""
        return self.record_type in [
            LogRecordType.CHECKPOINT_BEGIN,
            LogRecordType.CHECKPOINT_END
        ]

    def __str__(self) -> str:
        """字符串表示"""
        return (f"LogRecord(LSN={self.lsn}, type={self.record_type.name}, "
                f"page={self.page_id}, txn={self.transaction_id}, "
                f"data_len={self.data_length})")

    def __repr__(self) -> str:
        """详细字符串表示"""
        return (f"LogRecord(lsn={self.lsn}, type={self.record_type.name}, "
                f"timestamp={self.timestamp:.2f}, page_id={self.page_id}, "
                f"transaction_id={self.transaction_id}, data_length={self.data_length}, "
                f"metadata={self.metadata})")


class LogRecordBatch:
    """
    日志记录批次 - 用于批量写入优化
    """

    def __init__(self, max_size: int = 65536):  # 64KB
        """
        初始化日志批次

        Args:
            max_size: 批次最大大小（字节）
        """
        self.records = []
        self.total_size = 0
        self.max_size = max_size

    def add(self, record: LogRecord) -> bool:
        """
        添加记录到批次

        Args:
            record: 日志记录

        Returns:
            bool: 是否成功添加（false表示批次已满）
        """
        record_size = record.get_size()
        if self.total_size + record_size > self.max_size and self.records:
            return False

        self.records.append(record)
        self.total_size += record_size
        return True

    def is_empty(self) -> bool:
        """批次是否为空"""
        return len(self.records) == 0

    def is_full(self) -> bool:
        """批次是否已满"""
        return self.total_size >= self.max_size * 0.9  # 90%时认为满

    def clear(self):
        """清空批次"""
        self.records.clear()
        self.total_size = 0

    def serialize(self, compress: bool = False) -> bytes:
        """
        序列化整个批次

        Returns:
            bytes: 序列化的数据
        """
        result = bytearray()
        for record in self.records:
            result.extend(record.serialize(compress))
        return bytes(result)