# engine/storage_engine.py
from typing import List, Dict, Any, Optional
from storage.core.storage_manager import StorageManager
from storage.core.table_storage import TableStorage
from storage.utils.serializer import RecordSerializer, PageSerializer
from storage.utils.exceptions import StorageException, TableNotFoundException
from storage.utils.logger import get_logger


class StorageEngine:
    def __init__(self, storage_manager: StorageManager, table_storage: TableStorage, catalog_manager=None):
        self.storage_manager = storage_manager
        self.table_storage = table_storage
        self.catalog_manager = catalog_manager
        self.logger = get_logger("storage_engine")

    def create_table(self, table_name: str, columns: List[Dict]) -> None:
        """为表分配初始存储空间"""
        try:
            # 计算预估记录大小
            schema = self._convert_to_schema_format(columns)
            estimated_size = RecordSerializer.calculate_record_size(schema)

            # 通过TableStorage创建表存储空间
            success = self.table_storage.create_table_storage(table_name, estimated_size)
            if not success:
                raise StorageException(f"Failed to create storage for table '{table_name}'")

            self.logger.info(f"Created table storage for '{table_name}'")

        except Exception as e:
            self.logger.error(f"Error creating table '{table_name}': {e}")
            raise

    def serialize_row(self, row_data: Dict, schema: List[Dict]) -> bytes:
        """将一行数据序列化为二进制格式"""
        try:
            # 转换schema格式
            schema_format = self._convert_to_schema_format(schema)

            # 使用RecordSerializer进行序列化
            return RecordSerializer.serialize_record(row_data, schema_format)

        except Exception as e:
            self.logger.error(f"Error serializing row: {e}")
            raise

    def deserialize_row(self, binary_data: bytes, schema: List[Dict]) -> Dict:
        """从二进制数据反序列化一行"""
        try:
            # 转换schema格式
            schema_format = self._convert_to_schema_format(schema)

            # 使用RecordSerializer进行反序列化
            record = RecordSerializer.deserialize_record(binary_data, schema_format)
            if record is None:
                raise StorageException("Failed to deserialize record")

            return record

        except Exception as e:
            self.logger.error(f"Error deserializing row: {e}")
            raise

    # engine/storage_engine.py
    def insert_row(self, table_name: str, row_data: List[Any]) -> None:
        """插入一行数据"""
        try:
            # 获取表schema - 从catalog获取真实schema
            schema = self._get_table_schema(table_name)
            if not schema:
                raise StorageException(f"Schema not found for table '{table_name}'")

            # 将值列表转换为字典格式（与schema列顺序匹配）
            column_names = [col['name'] for col in schema]
            if len(row_data) != len(column_names):
                raise StorageException(
                    f"Number of values ({len(row_data)}) doesn't match number of columns ({len(column_names)})")

            # 创建字典格式的行数据
            row_dict = {}
            for i, col_name in enumerate(column_names):
                if i < len(row_data):
                    row_dict[col_name] = row_data[i]
                else:
                    row_dict[col_name] = None  # 设置默认值

            # 添加调试信息
            print(f"DEBUG: Inserting row data: {row_dict}")
            print(f"DEBUG: Schema: {schema}")

            # 序列化记录
            binary_row = self.serialize_row(row_dict, schema)
            print(f"DEBUG: Serialized binary data length: {len(binary_row)}")
            print(f"DEBUG: Serialized data (first 50 bytes): {binary_row[:50]}")

            # 检查表是否存在
            if not self.table_storage.table_exists(table_name):
                raise TableNotFoundException(table_name)

            # 获取表的所有页
            page_ids = self.table_storage.get_table_pages(table_name)

            # 尝试在现有页中插入记录
            for page_index, page_id in enumerate(page_ids):
                # 读取页数据
                page_data = self.table_storage.read_table_page(table_name, page_index)

                # 尝试将记录添加到页
                new_page_data, success = PageSerializer.add_record_to_page(page_data, binary_row)

                if success:
                    # 成功添加到页，写入更新后的页
                    self.table_storage.write_table_page(table_name, page_index, new_page_data)
                    self.logger.debug(f"Inserted row into table '{table_name}', page {page_id}")
                    return

            # 如果没有现有页有足够空间，分配新页
            new_page_id = self.table_storage.allocate_table_page(table_name)
            page_index = len(page_ids)  # 新页的索引

            # 创建空页并添加记录
            empty_page = PageSerializer.create_empty_page()
            new_page_data, success = PageSerializer.add_record_to_page(empty_page, binary_row)

            if not success:
                raise StorageException(f"Failed to add record to new page {new_page_id}")

            # 写入新页
            self.table_storage.write_table_page(table_name, page_index, new_page_data)
            self.logger.debug(f"Inserted row into new page {new_page_id} for table '{table_name}'")

        except Exception as e:
            self.logger.error(f"Error inserting row into table '{table_name}': {e}")
            raise

    def get_all_rows(self, table_name: str) -> List[Dict]:
        """获取表中的所有行（用于SeqScan）"""
        try:
            # 获取表schema - 从catalog获取真实schema
            schema = self._get_table_schema(table_name)
            if not schema:
                raise StorageException(f"Schema not found for table '{table_name}'")

            all_rows = []

            # 获取表的所有页
            page_count = self.table_storage.get_table_page_count(table_name)
            print(f"DEBUG: Table {table_name} has {page_count} pages")

            # 遍历所有页提取记录
            for page_index in range(page_count):
                # 读取页数据
                page_data = self.table_storage.read_table_page(table_name, page_index)
                print(f"DEBUG: Page {page_index} data length: {len(page_data)}")

                # 从页中提取所有记录
                schema_format = self._convert_to_schema_format(schema)
                records = PageSerializer.get_records_from_page(page_data, schema_format)

                # 添加调试信息
                print(f"DEBUG: Page {page_index} contains {len(records)} records")
                for i, record in enumerate(records):
                    print(f"DEBUG: Record {i}: {record}")

                all_rows.extend(records)

            return all_rows

        except Exception as e:
            self.logger.error(f"Error getting all rows from table '{table_name}': {e}")
            raise

    # engine/storage_engine.py
    def _convert_to_schema_format(self, columns: List[Dict]) -> List[tuple]:
        """将列定义转换为RecordSerializer需要的格式"""
        schema = []
        for col in columns:
            col_name = col['name']
            col_type = col['type']
            length = col.get('length')

            # 处理带括号的类型定义，提取基本类型和长度
            if '(' in col_type and ')' in col_type:
                # 提取类型名称和长度，如 VARCHAR(50) -> VARCHAR 和 50
                import re
                match = re.match(r'(\w+)\((\d+)\)', col_type)
                if match:
                    col_type = match.group(1).upper()  # 提取基本类型名称并转为大写
                    # 如果schema中没有指定length，使用括号中的值
                    if length is None:
                        length = int(match.group(2))
            else:
                col_type = col_type.upper()  # 确保类型名称大写

            # 对于VARCHAR类型，确保有长度限制
            if col_type == 'VARCHAR' and length is None:
                length = 255  # 默认长度

            schema.append((col_name, col_type, length))
        return schema

    # storage_engine.py 中的 _get_table_schema 方法
    # storage_engine.py 中的 _get_table_schema 方法
    def _get_table_schema(self, table_name: str) -> List[Dict]:
        """获取表schema - 从catalog获取真实schema"""
        try:
            # 优先使用catalog manager实例
            if self.catalog_manager:
                table_info = self.catalog_manager.get_table(table_name)
                if table_info:
                    columns = table_info.get("columns", [])
                    print(f"DEBUG: Got table info from catalog for '{table_name}': {table_info}")

                    # 验证 schema 的正确性
                    valid_schema = []
                    valid_types = ['INT', 'VARCHAR', 'CHAR', 'TEXT', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'DATE', 'DATETIME']

                    for col in columns:
                        col_name = col.get('name', '')
                        col_type = col.get('type', '').upper()

                        # 验证类型是否有效
                        if col_type not in valid_types:
                            # 尝试从带括号的类型中提取基本类型
                            import re
                            match = re.match(r'(\w+)\(.*\)', col_type)
                            if match:
                                base_type = match.group(1).upper()
                                if base_type in valid_types:
                                    col_type = base_type
                                else:
                                    print(f"WARNING: Invalid column type '{col_type}' in table '{table_name}'")
                                    continue
                            else:
                                print(f"WARNING: Invalid column type '{col_type}' in table '{table_name}'")
                                continue

                        valid_schema.append({
                            'name': col_name,
                            'type': col_type,
                            'length': self._extract_length_from_type(col.get('type', ''))
                        })

                    print(f"DEBUG: Converted schema for storage: {valid_schema}")
                    return valid_schema

            # 如果从catalog获取失败，返回空schema
            print(f"WARNING: Could not get schema from catalog for table '{table_name}'")
            return []

        except Exception as e:
            self.logger.error(f"Error getting schema from catalog for table '{table_name}': {e}")
            return []

    # storage_engine.py 中添加这个方法
    def _extract_length_from_type(self, type_str: str) -> Optional[int]:
        """从类型字符串中提取长度信息"""
        if not type_str:
            return None

        import re
        # 匹配 VARCHAR(50) 或 CHAR(10) 这样的格式
        match = re.match(r'^\w+\((\d+)\)$', type_str)
        if match:
            return int(match.group(1))

        # 如果是基本类型，返回默认长度
        type_upper = type_str.upper()
        if type_upper == 'VARCHAR':
            return 255
        elif type_upper == 'CHAR':
            return 50
        else:
            return None

    def shutdown(self):
        """关闭存储引擎"""
        try:
            # 刷盘确保数据持久化
            self.storage_manager.flush_all_pages()
            self.logger.info("Storage engine shutdown completed")
        except Exception as e:
            self.logger.error(f"Error during storage engine shutdown: {e}")
            raise

    def update_row(self, table_name: str, old_row: Dict, new_data: Dict) -> None:
        """更新表中的一行数据"""
        try:
            # 获取表schema
            schema = self._get_table_schema(table_name)
            if not schema:
                raise StorageException(f"Schema not found for table '{table_name}'")

            # 获取表的所有页
            page_count = self.table_storage.get_table_page_count(table_name)

            # 遍历所有页查找要更新的行
            for page_index in range(page_count):
                # 读取页数据
                page_data = self.table_storage.read_table_page(table_name, page_index)

                # 从页中提取所有记录
                schema_format = self._convert_to_schema_format(schema)
                records = PageSerializer.get_records_from_page(page_data, schema_format)

                # 查找要更新的记录（基于所有字段的精确匹配）
                for i, record in enumerate(records):
                    # 检查是否是要更新的行（比较所有字段）
                    if self._rows_match(record, old_row):
                        # 创建更新后的行数据
                        updated_row = record.copy()  # 使用当前记录而不是old_row
                        updated_row.update(new_data)

                        # 序列化更新后的记录
                        binary_updated_row = self.serialize_row(updated_row, schema)

                        # 更新页中的记录
                        # 首先需要移除旧记录，然后添加新记录

                        # 移除旧记录
                        page_data_after_remove, success = PageSerializer.remove_data_from_page(page_data, i)
                        if not success:
                            raise StorageException("Failed to remove old record from page")

                        # 添加新记录
                        updated_page_data, success = PageSerializer.add_record_to_page(page_data_after_remove,
                                                                                       binary_updated_row)
                        if not success:
                            raise StorageException("Failed to add updated record to page")

                        # 写入更新后的页
                        self.table_storage.write_table_page(table_name, page_index, updated_page_data)
                        self.logger.debug(f"Updated row in table '{table_name}', page {page_index}")
                        return

            raise StorageException(f"Row not found in table '{table_name}' for update")

        except Exception as e:
            self.logger.error(f"Error updating row in table '{table_name}': {e}")
            raise

    def _rows_match(self, row1: Dict, row2: Dict) -> bool:
        """比较两行数据是否匹配"""
        if set(row1.keys()) != set(row2.keys()):
            return False

        for key in row1.keys():
            if row1.get(key) != row2.get(key):
                return False

        return True

    def delete_row(self, table_name: str, row: Dict) -> None:
        """删除表中的一行数据"""
        try:
            # 获取表schema
            schema = self._get_table_schema(table_name)
            if not schema:
                raise StorageException(f"Schema not found for table '{table_name}'")

            # 获取表的所有页
            page_count = self.table_storage.get_table_page_count(table_name)

            # 遍历所有页查找要删除的行
            for page_index in range(page_count):
                # 读取页数据
                page_data = self.table_storage.read_table_page(table_name, page_index)

                # 从页中提取所有记录
                schema_format = self._convert_to_schema_format(schema)
                records = PageSerializer.get_records_from_page(page_data, schema_format)

                # 查找要删除的记录（基于所有字段的精确匹配）
                for i, record in enumerate(records):
                    # 检查是否是要删除的行（比较所有字段）
                    if self._rows_match(record, row):
                        # 从页中移除记录
                        updated_page_data, success = PageSerializer.remove_data_from_page(page_data, i)

                        if success:
                            # 写入更新后的页
                            self.table_storage.write_table_page(table_name, page_index, updated_page_data)
                            self.logger.debug(f"Deleted row from table '{table_name}', page {page_index}")
                            return
                        else:
                            raise StorageException("Failed to remove record from page")

            raise StorageException(f"Row not found in table '{table_name}' for deletion")

        except Exception as e:
            self.logger.error(f"Error deleting row from table '{table_name}': {e}")
            raise