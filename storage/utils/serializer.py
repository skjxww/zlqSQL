"""
数据序列化工具 - 完整实现
包含页序列化器和记录序列化器
"""

import struct
import json
from typing import List, Tuple, Any, Dict, Optional
from enum import Enum
from .exceptions import SerializationException


class DataType(Enum):
    """支持的数据类型"""
    INT = "INT"
    VARCHAR = "VARCHAR"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"


class RecordSerializer:
    """记录序列化器 - 处理记录级别的序列化和反序列化"""

    # 数据类型格式映射 (struct format, fixed_size)
    TYPE_FORMATS = {
        DataType.INT: ('i', 4),  # 32位整数
        DataType.FLOAT: ('f', 4),  # 32位浮点数
        DataType.BOOLEAN: ('?', 1),  # 布尔值
        DataType.DATE: ('Q', 8),  # 64位无符号整数(时间戳)
    }

    @staticmethod
    def calculate_record_size(schema: List[Tuple[str, str, Optional[int]]]) -> int:
        """
        计算记录的固定大小

        Args:
            schema: 表模式 [(column_name, data_type, length), ...]

        Returns:
            int: 记录的字节大小
        """
        total_size = 0

        for col_name, data_type, length in schema:
            try:
                dtype = DataType(data_type.upper())

                if dtype == DataType.VARCHAR:
                    # VARCHAR: 2字节长度 + 实际字符串长度(最大)
                    max_length = length if length else 255
                    total_size += 2 + max_length  # 2字节存储实际长度
                else:
                    # 固定长度类型
                    _, size = RecordSerializer.TYPE_FORMATS[dtype]
                    total_size += size

            except (ValueError, KeyError):
                raise SerializationException(f"Unsupported data type: {data_type}")

        # 添加记录头信息：1字节状态标志
        return total_size + 1

    @staticmethod
    def serialize_record(record: Dict[str, Any], schema: List[Tuple[str, str, Optional[int]]]) -> bytes:
        """
        序列化记录为字节流

        Args:
            record: 记录数据字典
            schema: 表模式

        Returns:
            bytes: 序列化后的字节流
        """
        try:
            buffer = bytearray()

            # 记录状态标志：0=正常，1=已删除
            buffer.append(0)

            for col_name, data_type, length in schema:
                value = record.get(col_name)
                dtype = DataType(data_type.upper())

                if value is None:
                    # NULL值处理
                    if dtype == DataType.VARCHAR:
                        buffer.extend(struct.pack('<H', 0))  # 长度为0表示NULL
                        if length:  # 如果是定长VARCHAR，还需要填充
                            buffer.extend(b'\x00' * length)
                    else:
                        # 固定长度类型的NULL值用0填充
                        _, size = RecordSerializer.TYPE_FORMATS[dtype]
                        buffer.extend(b'\x00' * size)
                else:
                    # 序列化非NULL值
                    if dtype == DataType.VARCHAR:
                        # VARCHAR处理
                        str_value = str(value)
                        str_bytes = str_value.encode('utf-8')

                        max_length = length if length else 255
                        if len(str_bytes) > max_length:
                            str_bytes = str_bytes[:max_length]

                        # 2字节长度 + 字符串内容
                        buffer.extend(struct.pack('<H', len(str_bytes)))
                        buffer.extend(str_bytes)

                        # 如果有最大长度限制，用0填充到固定大小
                        if length:
                            padding = max_length - len(str_bytes)
                            buffer.extend(b'\x00' * padding)

                    elif dtype == DataType.INT:
                        buffer.extend(struct.pack('<i', int(value)))
                    elif dtype == DataType.FLOAT:
                        buffer.extend(struct.pack('<f', float(value)))
                    elif dtype == DataType.BOOLEAN:
                        buffer.extend(struct.pack('<?', bool(value)))
                    elif dtype == DataType.DATE:
                        # 假设传入的是时间戳
                        buffer.extend(struct.pack('<Q', int(value)))

            return bytes(buffer)

        except Exception as e:
            raise SerializationException(f"Failed to serialize record: {e}", data_type="record")

    @staticmethod
    def deserialize_record(data: bytes, schema: List[Tuple[str, str, Optional[int]]]) -> Optional[Dict[str, Any]]:
        """
        反序列化字节流为记录

        Args:
            data: 字节流数据
            schema: 表模式

        Returns:
            Dict[str, Any]: 记录数据字典，如果记录已删除返回None
        """
        try:
            if len(data) == 0:
                return None

            offset = 0

            # 读取记录状态标志
            if offset >= len(data):
                return None
            status = data[offset]
            offset += 1

            if status == 1:  # 已删除记录
                return None

            record = {}

            for col_name, data_type, length in schema:
                if offset >= len(data):
                    break

                dtype = DataType(data_type.upper())

                if dtype == DataType.VARCHAR:
                    # 读取VARCHAR
                    if offset + 2 > len(data):
                        record[col_name] = None
                        break

                    str_length = struct.unpack('<H', data[offset:offset + 2])[0]
                    offset += 2

                    if str_length == 0:
                        record[col_name] = None
                        if length:  # 跳过填充
                            offset += length
                    else:
                        if length:
                            # 定长VARCHAR：读取实际字符串，然后跳过剩余的填充空间
                            if offset + str_length > len(data):
                                record[col_name] = None
                                break
                            str_bytes = data[offset:offset + str_length]
                            try:
                                record[col_name] = str_bytes.decode('utf-8')
                            except UnicodeDecodeError:
                                record[col_name] = str_bytes.decode('utf-8', errors='replace')
                            offset += length  # 跳过整个定长区域
                        else:
                            # 变长VARCHAR：只读取实际字符串长度
                            if offset + str_length > len(data):
                                record[col_name] = None
                                break
                            str_bytes = data[offset:offset + str_length]
                            try:
                                record[col_name] = str_bytes.decode('utf-8')
                            except UnicodeDecodeError:
                                record[col_name] = str_bytes.decode('utf-8', errors='replace')
                            offset += str_length

                else:
                    # 固定长度类型
                    format_char, size = RecordSerializer.TYPE_FORMATS[dtype]

                    if offset + size > len(data):
                        record[col_name] = None
                        break

                    value_bytes = data[offset:offset + size]
                    offset += size

                    if value_bytes == b'\x00' * size:
                        record[col_name] = None
                    else:
                        try:
                            value = struct.unpack('<' + format_char, value_bytes)[0]

                            if dtype == DataType.DATE:
                                record[col_name] = value  # 保持时间戳格式
                            else:
                                record[col_name] = value
                        except struct.error:
                            record[col_name] = None

            return record

        except Exception as e:
            raise SerializationException(f"Failed to deserialize record: {e}", data_type="record")


class PageSerializer:
    """页序列化器 - 负责页级数据的组织和管理"""

    PAGE_HEADER_SIZE = 16  # 页头大小：记录数(4) + 空闲空间起始(4) + 下一页ID(4) + 保留(4)

    @staticmethod
    def create_empty_page() -> bytes:
        """创建空页"""
        from .constants import PAGE_SIZE

        # 页头：记录数(0) + 空闲空间起始位置(PAGE_HEADER_SIZE) + 下一页ID(0) + 保留(0)
        header = struct.pack('<IIII', 0, PageSerializer.PAGE_HEADER_SIZE, 0, 0)

        # 填充剩余空间为0
        remaining = PAGE_SIZE - len(header)
        return header + b'\x00' * remaining

    @staticmethod
    def get_page_info(page_data: bytes) -> Dict[str, int]:
        """获取页的基本信息"""
        if len(page_data) < PageSerializer.PAGE_HEADER_SIZE:
            raise SerializationException("Invalid page data: too short")

        record_count, free_space_start, next_page_id, reserved = struct.unpack('<IIII', page_data[:16])

        return {
            'record_count': record_count,           # 页中记录数量
            'free_space_start': free_space_start,   # 空闲空间起始位置
            'next_page_id': next_page_id,           # 链表中的下一页ID
            'reserved': reserved,                   # 保留字段
            'free_space_size': len(page_data) - free_space_start  # 可用空闲空间大小
        }

    @staticmethod
    def add_data_to_page(page_data: bytes, data_block: bytes) -> Tuple[bytes, bool]:
        """向页中添加一个数据块"""
        from .constants import PAGE_SIZE

        try:
            page_info = PageSerializer.get_page_info(page_data)

            # 计算空间需求
            current_offset_table_size = page_info['record_count'] * 4
            new_offset_table_size = (page_info['record_count'] + 1) * 4

            # 计算新数据块的位置
            current_data_start = PageSerializer.PAGE_HEADER_SIZE + current_offset_table_size
            current_data_end = page_info['free_space_start']
            current_data_size = current_data_end - current_data_start

            new_data_start = PageSerializer.PAGE_HEADER_SIZE + new_offset_table_size
            new_record_offset = new_data_start + current_data_size

            # 检查空间是否足够
            total_needed = new_offset_table_size + current_data_size + len(data_block)
            available_space = PAGE_SIZE - PageSerializer.PAGE_HEADER_SIZE

            if total_needed > available_space:
                return page_data, False  # 空间不足

            # 构建新页
            new_page = bytearray(PAGE_SIZE)

            # 1. 复制并更新页头
            new_record_count = page_info['record_count'] + 1
            new_free_space_start = new_record_offset + len(data_block)

            struct.pack_into('<IIII', new_page, 0,
                           new_record_count,
                           new_free_space_start,
                           page_info['next_page_id'],
                           page_info['reserved'])

            # 2. 重建偏移表
            offset_adjustment = new_data_start - current_data_start

            # 复制现有偏移并调整
            for i in range(page_info['record_count']):
                old_offset_pos = PageSerializer.PAGE_HEADER_SIZE + (i * 4)
                old_offset = struct.unpack('<I', page_data[old_offset_pos:old_offset_pos + 4])[0]
                new_offset = old_offset + offset_adjustment
                struct.pack_into('<I', new_page, old_offset_pos, new_offset)

            # 添加新记录的偏移
            new_offset_pos = PageSerializer.PAGE_HEADER_SIZE + (page_info['record_count'] * 4)
            struct.pack_into('<I', new_page, new_offset_pos, new_record_offset)

            # 3. 复制现有数据
            if current_data_size > 0:
                old_data = page_data[current_data_start:current_data_end]
                new_page[new_data_start:new_data_start + current_data_size] = old_data

            # 4. 添加新数据块
            new_page[new_record_offset:new_record_offset + len(data_block)] = data_block

            return bytes(new_page), True

        except Exception as e:
            raise SerializationException(f"Failed to add data to page: {e}")

    @staticmethod
    def get_data_blocks_from_page(page_data: bytes) -> List[bytes]:
        """从页中提取所有数据块"""
        try:
            page_info = PageSerializer.get_page_info(page_data)
            data_blocks = []

            # 读取每个数据块的偏移量和内容
            for i in range(page_info['record_count']):
                offset_position = PageSerializer.PAGE_HEADER_SIZE + (i * 4)

                if offset_position + 4 > len(page_data):
                    break

                data_offset = struct.unpack('<I', page_data[offset_position:offset_position + 4])[0]

                # 计算数据块大小
                if i + 1 < page_info['record_count']:
                    # 不是最后一个，大小 = 下一个偏移 - 当前偏移
                    next_offset_position = PageSerializer.PAGE_HEADER_SIZE + ((i + 1) * 4)
                    next_data_offset = struct.unpack('<I', page_data[next_offset_position:next_offset_position + 4])[0]
                    data_size = next_data_offset - data_offset
                else:
                    # 最后一个，大小 = 空闲空间开始 - 当前偏移
                    data_size = page_info['free_space_start'] - data_offset

                # 提取数据块
                if data_offset + data_size <= len(page_data) and data_size > 0:
                    data_block = page_data[data_offset:data_offset + data_size]
                    data_blocks.append(data_block)

            return data_blocks

        except Exception as e:
            raise SerializationException(f"Failed to get data blocks from page: {e}")

    @staticmethod
    def get_records_from_page(page_data: bytes, schema: List[Tuple[str, str, Optional[int]]]) -> List[Dict[str, Any]]:
        """
        从页中获取所有记录（结合记录反序列化）

        Args:
            page_data: 页数据
            schema: 表模式

        Returns:
            List[Dict[str, Any]]: 记录列表
        """
        try:
            data_blocks = PageSerializer.get_data_blocks_from_page(page_data)
            records = []

            for data_block in data_blocks:
                record = RecordSerializer.deserialize_record(data_block, schema)
                if record is not None:  # 跳过已删除的记录
                    records.append(record)

            return records

        except Exception as e:
            raise SerializationException(f"Failed to get records from page: {e}")

    @staticmethod
    def add_record_to_page(page_data: bytes, record_data: bytes) -> Tuple[bytes, bool]:
        """向页中添加记录（兼容接口）"""
        return PageSerializer.add_data_to_page(page_data, record_data)

    @staticmethod
    def remove_data_from_page(page_data: bytes, block_index: int) -> Tuple[bytes, bool]:
        """从页中移除指定索引的数据块"""
        try:
            page_info = PageSerializer.get_page_info(page_data)

            if block_index >= page_info['record_count'] or block_index < 0:
                return page_data, False

            # 获取所有数据块
            data_blocks = PageSerializer.get_data_blocks_from_page(page_data)

            # 移除指定索引的数据块
            if block_index < len(data_blocks):
                data_blocks.pop(block_index)

            # 重建页面
            return PageSerializer._rebuild_page_with_blocks(data_blocks, page_info['next_page_id'])

        except Exception as e:
            raise SerializationException(f"Failed to remove data from page: {e}")

    @staticmethod
    def get_page_utilization(page_data: bytes) -> Dict[str, float]:
        """获取页面空间利用率统计"""
        from .constants import PAGE_SIZE

        page_info = PageSerializer.get_page_info(page_data)

        header_size = PageSerializer.PAGE_HEADER_SIZE
        offset_table_size = page_info['record_count'] * 4
        data_size = page_info['free_space_start'] - (header_size + offset_table_size)
        free_space = PAGE_SIZE - page_info['free_space_start']

        return {
            'total_size': PAGE_SIZE,
            'header_size': header_size,
            'offset_table_size': offset_table_size,
            'data_size': data_size,
            'free_space': free_space,
            'utilization_ratio': (PAGE_SIZE - free_space) / PAGE_SIZE,
            'data_ratio': data_size / PAGE_SIZE,
            'overhead_ratio': (header_size + offset_table_size) / PAGE_SIZE
        }

    @staticmethod
    def _rebuild_page_with_blocks(data_blocks: List[bytes], next_page_id: int = 0) -> Tuple[bytes, bool]:
        """使用给定的数据块重建页面"""
        from .constants import PAGE_SIZE

        # 从空页开始
        new_page = PageSerializer.create_empty_page()

        # 设置next_page_id
        if next_page_id != 0:
            struct.pack_into('<I', new_page, 8, next_page_id)  # 第3个32位字段

        # 逐个添加数据块
        current_page = new_page
        for data_block in data_blocks:
            current_page, success = PageSerializer.add_data_to_page(current_page, data_block)
            if not success:
                # 页空间不足，无法添加所有数据块
                return current_page, False

        return current_page, True


class SchemaSerializer:
    """模式序列化器"""

    @staticmethod
    def serialize_schema(schema: List[Tuple[str, str, Optional[int]]]) -> str:
        """序列化表模式为JSON字符串"""
        schema_list = []
        for col_name, data_type, length in schema:
            col_info = {
                'name': col_name,
                'type': data_type,
                'length': length
            }
            schema_list.append(col_info)

        return json.dumps(schema_list, ensure_ascii=False)

    @staticmethod
    def deserialize_schema(schema_json: str) -> List[Tuple[str, str, Optional[int]]]:
        """从JSON字符串反序列化表模式"""
        try:
            schema_list = json.loads(schema_json)
            schema = []

            for col_info in schema_list:
                schema.append((
                    col_info['name'],
                    col_info['type'],
                    col_info.get('length')
                ))

            return schema

        except Exception as e:
            raise SerializationException(f"Failed to deserialize schema: {e}")

    @staticmethod
    def validate_schema(schema: List[Tuple[str, str, Optional[int]]]) -> bool:
        """验证表模式的有效性"""
        if not schema:
            return False

        column_names = set()

        for col_name, data_type, length in schema:
            # 检查列名重复
            if col_name in column_names:
                return False
            column_names.add(col_name)

            # 检查数据类型有效性
            try:
                DataType(data_type.upper())
            except ValueError:
                return False

            # 检查VARCHAR长度
            if data_type.upper() == 'VARCHAR' and length is not None and length <= 0:
                return False

        return True