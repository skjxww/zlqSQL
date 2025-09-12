"""
WAL日志读取器
负责从磁盘读取和解析日志记录，支持顺序读取和定位
"""

import os
from pathlib import Path
from typing import Optional, List, Generator, Tuple
import struct

from .log_record import LogRecord, LogRecordType
from ...utils.logger import get_logger
from ...utils.exceptions import StorageException


class LogReader:
    """
    WAL日志读取器

    特性：
    - 顺序读取
    - 随机定位
    - 多文件支持
    - 错误恢复
    - 过滤功能
    """

    def __init__(self, wal_dir: str = "data/wal"):
        """
        初始化日志读取器

        Args:
            wal_dir: WAL目录
        """
        self.wal_dir = Path(wal_dir)
        self.current_file = None
        self.current_file_path = None
        self.current_file_number = 0
        self.current_position = 0

        # 统计信息
        self.total_records_read = 0
        self.total_bytes_read = 0
        self.corrupted_records = 0

        # 日志器
        self.logger = get_logger("wal_reader")

        # 获取所有WAL文件
        self.wal_files = self._scan_wal_files()

        self.logger.info(f"WAL Reader initialized",
                         wal_dir=str(self.wal_dir),
                         file_count=len(self.wal_files))

    def _scan_wal_files(self) -> List[Path]:
        """扫描所有WAL文件"""
        if not self.wal_dir.exists():
            return []

        files = list(self.wal_dir.glob("wal_*.log"))
        # 按文件号排序
        files.sort(key=lambda f: self._extract_file_number(f))
        return files

    def _extract_file_number(self, file_path: Path) -> int:
        """从文件名提取文件号"""
        try:
            return int(file_path.stem.split('_')[1])
        except:
            return 0

    def read_all(self) -> Generator[LogRecord, None, None]:
        """
        读取所有日志记录

        Yields:
            LogRecord: 日志记录
        """
        for file_path in self.wal_files:
            yield from self._read_file(file_path)

    def read_from_lsn(self, start_lsn: int) -> Generator[LogRecord, None, None]:
        """
        从指定LSN开始读取

        Args:
            start_lsn: 起始LSN

        Yields:
            LogRecord: LSN >= start_lsn的日志记录
        """
        for record in self.read_all():
            if record.lsn >= start_lsn:
                yield record

    def read_range(self, start_lsn: int, end_lsn: int) -> Generator[LogRecord, None, None]:
        """
        读取LSN范围内的记录

        Args:
            start_lsn: 起始LSN
            end_lsn: 结束LSN

        Yields:
            LogRecord: 在指定范围内的日志记录
        """
        for record in self.read_all():
            if record.lsn > end_lsn:
                break
            if record.lsn >= start_lsn:
                yield record

    def read_by_type(self, record_type: LogRecordType) -> Generator[LogRecord, None, None]:
        """
        读取特定类型的记录

        Args:
            record_type: 记录类型

        Yields:
            LogRecord: 指定类型的日志记录
        """
        for record in self.read_all():
            if record.record_type == record_type:
                yield record

    def read_by_page(self, page_id: int) -> Generator[LogRecord, None, None]:
        """
        读取特定页面相关的记录

        Args:
            page_id: 页面ID

        Yields:
            LogRecord: 与指定页面相关的日志记录
        """
        for record in self.read_all():
            if record.page_id == page_id:
                yield record

    def read_by_transaction(self, transaction_id: int) -> Generator[LogRecord, None, None]:
        """
        读取特定事务的记录

        Args:
            transaction_id: 事务ID

        Yields:
            LogRecord: 属于指定事务的日志记录
        """
        for record in self.read_all():
            if record.transaction_id == transaction_id:
                yield record

    def _read_file(self, file_path: Path) -> Generator[LogRecord, None, None]:
        """
        读取单个文件的所有记录

        Args:
            file_path: 文件路径

        Yields:
            LogRecord: 日志记录
        """
        if not file_path.exists():
            self.logger.warning(f"WAL file not found: {file_path}")
            return

        self.logger.debug(f"Reading WAL file: {file_path}")

        try:
            with open(file_path, 'rb') as f:
                file_size = file_path.stat().st_size
                position = 0

                while position < file_size:
                    # 保存当前位置
                    f.seek(position)

                    # 尝试读取记录
                    record = self._read_next_record(f, position, file_size)

                    if record is None:
                        # 无法读取更多记录
                        break

                    # 更新位置
                    position = f.tell()

                    # 更新统计
                    self.total_records_read += 1
                    self.total_bytes_read += record.get_size()

                    yield record

        except Exception as e:
            self.logger.error(f"Error reading WAL file {file_path}: {e}")
            raise StorageException(f"Failed to read WAL file: {e}")

    def _read_next_record(self, file, position: int, file_size: int) -> Optional[LogRecord]:
        """
        读取下一条记录

        Args:
            file: 文件对象
            position: 当前位置
            file_size: 文件大小

        Returns:
            LogRecord或None
        """
        # 检查是否有足够的空间存放最小记录
        min_record_size = LogRecord.HEADER_SIZE + 8  # 头部 + 数据长度 + CRC
        if position + min_record_size > file_size:
            return None

        try:
            # 读取魔数检查是否是有效记录
            magic_bytes = file.read(4)
            if len(magic_bytes) < 4:
                return None

            magic = struct.unpack('<I', magic_bytes)[0]
            if magic != LogRecord.MAGIC_NUMBER:
                # 尝试恢复：向前扫描寻找下一个有效记录
                self.logger.warning(f"Invalid magic number at position {position}")
                self.corrupted_records += 1
                return self._scan_for_next_record(file, position + 1, file_size)

            # 回到记录开始位置
            file.seek(position)

            # 读取完整的头部
            header = file.read(LogRecord.HEADER_SIZE)
            if len(header) < LogRecord.HEADER_SIZE:
                return None

            # 解析头部获取数据长度
            # 跳过前面的字段，直接获取数据长度
            # 头部格式：魔数(4) + LSN(4) + 类型(4) + 保留(4) + 时间戳(8) + 事务ID(4) + 页号(4)
            # 数据长度在头部之后
            data_length_bytes = file.read(4)
            if len(data_length_bytes) < 4:
                return None

            data_length = struct.unpack('<I', data_length_bytes)[0]

            # 检查数据长度是否合理
            if data_length > 10 * 1024 * 1024:  # 10MB上限
                self.logger.warning(f"Suspicious data length {data_length} at position {position}")
                self.corrupted_records += 1
                return self._scan_for_next_record(file, position + 1, file_size)

            # 读取完整记录
            total_size = LogRecord.HEADER_SIZE + 4 + data_length + 4  # 头部+长度+数据+CRC
            file.seek(position)
            record_data = file.read(total_size)

            if len(record_data) < total_size:
                # 文件末尾不完整的记录
                self.logger.warning(f"Incomplete record at position {position}")
                return None

            # 反序列化记录
            record = LogRecord.deserialize(record_data)

            return record

        except Exception as e:
            self.logger.warning(f"Failed to read record at position {position}: {e}")
            self.corrupted_records += 1
            # 尝试跳过损坏的记录
            return self._scan_for_next_record(file, position + 1, file_size)

    def _scan_for_next_record(self, file, start_position: int, file_size: int) -> Optional[LogRecord]:
        """
        扫描寻找下一个有效记录

        Args:
            file: 文件对象
            start_position: 开始扫描的位置
            file_size: 文件大小

        Returns:
            LogRecord或None
        """
        position = start_position

        while position < file_size - LogRecord.HEADER_SIZE:
            file.seek(position)

            # 读取4字节检查是否是魔数
            magic_bytes = file.read(4)
            if len(magic_bytes) < 4:
                break

            magic = struct.unpack('<I', magic_bytes)[0]
            if magic == LogRecord.MAGIC_NUMBER:
                # 找到可能的记录开始
                file.seek(position)
                record = self._read_next_record(file, position, file_size)
                if record:
                    self.logger.info(f"Recovered from corruption, found valid record at position {position}")
                    return record

            position += 1

        return None

    def find_last_checkpoint(self) -> Optional[Tuple[LogRecord, LogRecord]]:
        """
        查找最后一个完整的检查点

        Returns:
            tuple: (检查点开始记录, 检查点结束记录) 或 None
        """
        checkpoints = []

        for record in self.read_all():
            if record.record_type == LogRecordType.CHECKPOINT_BEGIN:
                # 开始新的检查点
                checkpoints.append([record, None])
            elif record.record_type == LogRecordType.CHECKPOINT_END:
                # 结束当前检查点
                if checkpoints and checkpoints[-1][1] is None:
                    checkpoints[-1][1] = record

        # 返回最后一个完整的检查点
        for checkpoint in reversed(checkpoints):
            if checkpoint[0] and checkpoint[1]:
                return tuple(checkpoint)

        return None

    def get_statistics(self) -> dict:
        """获取统计信息"""
        return {
            'total_files': len(self.wal_files),
            'total_records_read': self.total_records_read,
            'total_bytes_read': self.total_bytes_read,
            'corrupted_records': self.corrupted_records,
            'current_file': str(self.current_file_path) if self.current_file_path else None,
            'current_position': self.current_position
        }

    def validate_files(self) -> List[dict]:
        """
        验证所有WAL文件的完整性

        Returns:
            list: 验证结果列表
        """
        results = []

        for file_path in self.wal_files:
            result = {
                'file': str(file_path),
                'size': file_path.stat().st_size,
                'valid_records': 0,
                'corrupted_records': 0,
                'last_lsn': None
            }

            try:
                for record in self._read_file(file_path):
                    result['valid_records'] += 1
                    result['last_lsn'] = record.lsn
            except Exception as e:
                result['error'] = str(e)

            result['corrupted_records'] = self.corrupted_records
            results.append(result)

        return results

    def close(self):
        """关闭读取器"""
        if self.current_file:
            self.current_file.close()
            self.current_file = None

        self.logger.info(f"WAL Reader closed",
                         total_records=self.total_records_read,
                         total_bytes=self.total_bytes_read,
                         corrupted_records=self.corrupted_records)