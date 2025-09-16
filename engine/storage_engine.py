# engine/storage_engine.py
# engine/storage_engine.py
import os
import json
from typing import List, Dict, Any, Optional
from storage.core.storage_manager import StorageManager
from storage.core.table_storage import TableStorage
from storage.utils.serializer import RecordSerializer, PageSerializer
from storage.utils.exceptions import StorageException, TableNotFoundException
from storage.utils.logger import get_logger
from sql_compiler.btree.BPlusTreeIndex import BPlusTreeIndex  # 导入B+树索引
from storage.core.transaction_manager import TransactionManager, IsolationLevel, TransactionState  # 添加TransactionState导入

class StorageEngine:
    def __init__(self, storage_manager: StorageManager, table_storage: TableStorage, catalog_manager=None):
        self.storage_manager = storage_manager
        self.table_storage = table_storage
        self.catalog_manager = catalog_manager
        self.logger = get_logger("storage_engine")

        # 表空间和区管理相关属性
        self.current_table_context = None
        self.table_tablespace_mapping = {}  # 表名到表空间的映射
        self.table_indexes: Dict[str, Dict[str, BPlusTreeIndex]] = {}  # 表名 -> {索引名 -> BPlusTreeIndex实例}

        # 添加事务管理器
        self.transaction_manager = TransactionManager(storage_manager)

        # 添加视图存储
        self.views = {}  # 视图名 -> 视图定义

        # 从持久化存储加载视图
        self.load_views()

        # 确保系统表空间存在
        try:
            if not self.storage_manager.tablespace_manager.tablespace_exists("system"):
                self.storage_manager.tablespace_manager.create_tablespace("system")
        except Exception as e:
            self.logger.warning(f"Could not initialize system tablespace: {e}")

    # 添加事务操作方法
    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> int:
        """开始一个新事务"""
        return self.transaction_manager.begin_transaction(isolation_level)

    def commit_transaction(self, txn_id: int) -> bool:
        """提交事务"""
        try:
            self.transaction_manager.commit(txn_id)
            return True
        except Exception as e:
            self.logger.error(f"Failed to commit transaction {txn_id}: {e}")
            return False

    # storage_engine.py 中的修改

    def rollback_transaction(self, txn_id: int) -> bool:
        """回滚指定事务"""
        print(f"DEBUG: Attempting rollback for transaction ID: {txn_id}")

        if txn_id is None:
            print("WARNING: No transaction ID provided for rollback")
            return True

        if self.transaction_manager is None:
            print("ERROR: Transaction manager not initialized")
            return False

        try:
            # 检查事务是否存在
            txn = self.transaction_manager.get_transaction(txn_id)
            if txn is None:
                print(f"WARNING: Transaction {txn_id} not found - may have been already rolled back")
                return True

            # 确保事务状态正确
            if txn.state == TransactionState.COMMITTED:
                print(f"WARNING: Transaction {txn_id} is already committed")
                return True

            # 执行回滚
            success = self.transaction_manager.rollback(txn_id)
            if success:
                print(f"DEBUG: Successfully rolled back transaction {txn_id}")
            else:
                print(f"ERROR: Failed to rollback transaction {txn_id}")
            return success
        except Exception as e:
            print(f"ERROR: Exception during rollback: {str(e)}")
            # 尝试强制清理事务
            try:
                self.transaction_manager.force_cleanup_transaction(txn_id)
            except Exception as cleanup_error:
                print(f"ERROR: Failed to cleanup transaction during rollback: {cleanup_error}")
            return False

    def get_transaction_status(self, txn_id: int) -> Dict[str, Any]:
        """获取事务状态"""
        txn = self.transaction_manager.get_transaction(txn_id)
        if txn:
            return {
                "transaction_id": txn_id,
                "state": txn.state.name,
                "isolation_level": txn.isolation_level.name,
                "start_time": txn.start_time,
                "modified_pages": list(txn.modified_pages)
            }
        else:
            return {"error": f"Transaction {txn_id} not found"}

    # 添加视图操作方法
    # storage_engine.py 修改部分

    def create_view(self, view_name: str, definition: str, columns: Optional[List[str]] = None,
                    materialized: bool = False, with_check_option: bool = False) -> bool:
        """创建视图并持久化存储 - 修复版，确保与catalog同步"""
        try:
            # 存储到内存
            self.views[view_name] = {
                'definition': definition,
                'columns': columns,
                'materialized': materialized,
                'with_check_option': with_check_option
            }

            # 简化持久化：只使用文件存储
            views_dir = "system_views"
            if not os.path.exists(views_dir):
                os.makedirs(views_dir)

            view_file = os.path.join(views_dir, f"{view_name}.json")
            view_data = {
                'name': view_name,
                'definition': definition,  # 保存SQL定义
                'columns': columns,
                'materialized': materialized,
                'with_check_option': with_check_option,
                'type': 'VIEW'
            }

            with open(view_file, 'w', encoding='utf-8') as f:
                json.dump(view_data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"View '{view_name}' created and persisted successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error creating view '{view_name}': {e}")
            # 即使持久化失败，也保持在内存中可用
            return True

    def drop_view(self, view_name: str) -> bool:
        """删除视图及其持久化存储 - 修复版"""
        try:
            # 从内存中删除
            if view_name in self.views:
                del self.views[view_name]

            # 删除持久化文件
            views_dir = "system_views"
            view_file = os.path.join(views_dir, f"{view_name}.json")

            if os.path.exists(view_file):
                os.remove(view_file)
                self.logger.info(f"View file '{view_file}' deleted")

            self.logger.info(f"View '{view_name}' dropped successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error dropping view '{view_name}': {e}")
            return False

    def load_views(self) -> None:
        """从持久化存储加载所有视图 - 修复版"""
        try:
            views_dir = "system_views"
            if os.path.exists(views_dir):
                for view_file in os.listdir(views_dir):
                    if view_file.endswith('.json'):
                        try:
                            with open(os.path.join(views_dir, view_file), 'r', encoding='utf-8') as f:
                                view_data = json.load(f)
                            view_name = view_data['name']
                            self.views[view_name] = {
                                'definition': view_data['definition'],  # 加载SQL定义
                                'columns': view_data['columns'],
                                'materialized': view_data['materialized'],
                                'with_check_option': view_data['with_check_option']
                            }
                        except Exception as e:
                            self.logger.error(f"Error loading view from {view_file}: {e}")
                            continue
        except Exception as e:
            self.logger.error(f"Error loading views: {e}")

    def get_view(self, view_name: str) -> Optional[Dict]:
        """获取视图定义"""
        return self.views.get(view_name)

    def get_all_views(self) -> Dict[str, Dict]:
        """获取所有视图"""
        return self.views.copy()

    # 添加事务性数据操作方法
    def insert_row_transactional(self, table_name: str, row_data: List[Any], txn_id: int) -> bool:
        """在事务中插入一行数据"""
        try:
            # 添加调试信息
            print(f"DEBUG: Inserting into table '{table_name}' in transaction {txn_id}")
            print(f"DEBUG: Row data: {row_data}")

            # 检查表是否存在
            if not self.table_storage.table_exists(table_name):
                print(f"ERROR: Table '{table_name}' does not exist")
                return False

            # 获取表schema
            schema = self._get_table_schema(table_name)
            if not schema:
                print(f"ERROR: Schema not found for table '{table_name}'")
                return False

            print(f"DEBUG: Table schema: {schema}")

            # 将值列表转换为字典格式
            column_names = [col['name'] for col in schema]
            if len(row_data) != len(column_names):
                raise StorageException(
                    f"Number of values ({len(row_data)}) doesn't match number of columns ({len(column_names)})")

            row_dict = {}
            for i, col_name in enumerate(column_names):
                if i < len(row_data):
                    row_dict[col_name] = row_data[i]
                else:
                    row_dict[col_name] = None

            # 序列化记录
            binary_row = self.serialize_row(row_dict, schema)
            print(f"DEBUG: Serialized binary data length: {len(binary_row)}")

            # 获取表的所有页
            page_ids = self.table_storage.get_table_pages(table_name)
            print(f"DEBUG: Table has {len(page_ids)} pages")

            # 尝试在现有页中插入记录
            for page_index, page_id in enumerate(page_ids):
                # 准备写操作（获取锁，保存undo信息）
                if not self.transaction_manager.prepare_write(txn_id, page_id):
                    raise StorageException(f"Failed to acquire write lock on page {page_id}")

                # 读取页数据
                page_data = self.table_storage.read_table_page(table_name, page_index)
                print(f"DEBUG: Page {page_index} data length: {len(page_data)}")

                # 尝试将记录添加到页
                new_page_data, success = PageSerializer.add_record_to_page(page_data, binary_row)

                if success:
                    # 记录redo日志
                    self.transaction_manager.record_write(txn_id, page_id, new_page_data)

                    # 写入更新后的页
                    self.table_storage.write_table_page(table_name, page_index, new_page_data)
                    self.logger.debug(
                        f"Inserted row into table '{table_name}', page {page_id} in transaction {txn_id}")

                    # 维护所有索引
                    if table_name in self.table_indexes:
                        for index_name, index in self.table_indexes[table_name].items():
                            col_name = index_name.split('_')[-1]
                            key = row_dict.get(col_name)
                            if key is not None:
                                index.insert(key, row_dict)

                    return True
                else:
                    print(f"DEBUG: Page {page_id} does not have enough space")

            # 如果没有现有页有足够空间，分配新页
            new_page_id = self.table_storage.allocate_table_page(table_name)
            page_index = len(page_ids)
            print(f"DEBUG: Allocated new page {new_page_id}")

            # 准备写操作（获取锁，保存undo信息）
            if not self.transaction_manager.prepare_write(txn_id, new_page_id):
                raise StorageException(f"Failed to acquire write lock on new page {new_page_id}")

            # 创建空页并添加记录
            empty_page = PageSerializer.create_empty_page()
            new_page_data, success = PageSerializer.add_record_to_page(empty_page, binary_row)

            if not success:
                raise StorageException(f"Failed to add record to new page {new_page_id}")

            # 记录redo日志
            self.transaction_manager.record_write(txn_id, new_page_id, new_page_data)

            # 写入新页
            self.table_storage.write_table_page(table_name, page_index, new_page_data)

            # 维护所有索引
            if table_name in self.table_indexes:
                for index_name, index in self.table_indexes[table_name].items():
                    col_name = index_name.split('_')[-1]
                    key = row_dict.get(col_name)
                    if key is not None:
                        index.insert(key, row_dict)

            self.logger.debug(
                f"Inserted row into new page {new_page_id} for table '{table_name}' in transaction {txn_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error inserting row into table '{table_name}' in transaction {txn_id}: {e}")
            # 添加详细的异常信息
            import traceback
            traceback.print_exc()
            return False

    def update_row_transactional(self, table_name: str, old_row: Dict, new_data: Dict, txn_id: int) -> bool:
        """在事务中更新一行数据"""
        try:
            # 添加事务状态检查 - 使用已有的方法
            if not self.transaction_manager:
                self.logger.error(f"Transaction manager not available")
                return False

            txn = self.transaction_manager.get_transaction(txn_id)
            if not txn or txn.state != TransactionState.ACTIVE:
                self.logger.error(f"Transaction {txn_id} is not active or doesn't exist")
                return False

            # 获取表schema
            schema = self._get_table_schema(table_name)
            if not schema:
                raise StorageException(f"Schema not found for table '{table_name}'")

            # 获取表的所有页
            page_count = self.table_storage.get_table_page_count(table_name)
            self.logger.debug(f"Searching {page_count} pages for row to update")

            # 遍历所有页查找要更新的行
            for page_index in range(page_count):
                # 准备读操作（获取锁）
                # 使用已有的方法获取页面ID列表，然后通过索引获取
                page_ids = self.table_storage.get_table_pages(table_name)
                if page_index >= len(page_ids):
                    raise StorageException(f"Page index {page_index} out of range for table '{table_name}'")
                page_id = page_ids[page_index]
                if not self.transaction_manager.prepare_read(txn_id, page_id):
                    self.logger.error(f"Failed to acquire read lock on page {page_id}")
                    continue

                # 读取页数据
                page_data = self.table_storage.read_table_page(table_name, page_index)

                # 从页中提取所有记录
                schema_format = self._convert_to_schema_format(schema)
                records = PageSerializer.get_records_from_page(page_data, schema_format)

                # 查找要更新的记录
                for i, record in enumerate(records):
                    if self._rows_match(record, old_row):
                        # 准备写操作（获取锁，保存undo信息）
                        if not self.transaction_manager.prepare_write(txn_id, page_id):
                            self.logger.error(f"Failed to acquire write lock on page {page_id}")
                            return False

                        # 创建更新后的行数据
                        updated_row = record.copy()
                        updated_row.update(new_data)

                        # 序列化更新后的记录
                        binary_updated_row = self.serialize_row(updated_row, schema)

                        # 移除旧记录
                        page_data_after_remove, success = PageSerializer.remove_data_from_page(page_data, i)
                        if not success:
                            raise StorageException("Failed to remove old record from page")

                        # 添加新记录
                        updated_page_data, success = PageSerializer.add_record_to_page(page_data_after_remove,
                                                                                       binary_updated_row)
                        if not success:
                            raise StorageException("Failed to add updated record to page")

                        # 记录redo日志
                        self.transaction_manager.record_write(txn_id, page_id, updated_page_data)

                        # 写入更新后的页
                        self.table_storage.write_table_page(table_name, page_index, updated_page_data)
                        self.logger.debug(
                            f"Updated row in table '{table_name}', page {page_index} in transaction {txn_id}")
                        return True

            self.logger.error(f"Row not found in table '{table_name}' for update: {old_row}")
            return False

        except Exception as e:
            self.logger.error(f"Error updating row in table '{table_name}' in transaction {txn_id}: {e}")
            return False

    def delete_row_transactional(self, table_name: str, row: Dict, txn_id: int) -> bool:
        """在事务中删除一行数据"""
        try:
            # 获取表schema
            schema = self._get_table_schema(table_name)
            if not schema:
                raise StorageException(f"Schema not found for table '{table_name}'")

            # 获取表的所有页
            page_count = self.table_storage.get_table_page_count(table_name)

            # 遍历所有页查找要删除的行
            for page_index in range(page_count):
                # 准备读操作（获取锁）
                page_ids = self.table_storage.get_table_pages(table_name)
                if page_index >= len(page_ids):
                    raise StorageException(f"Page index {page_index} out of range for table '{table_name}'")
                page_id = page_ids[page_index]
                if not self.transaction_manager.prepare_read(txn_id, page_id):
                    raise StorageException(f"Failed to acquire read lock on page {page_id}")

                # 读取页数据
                page_data = self.table_storage.read_table_page(table_name, page_index)

                # 从页中提取所有记录
                schema_format = self._convert_to_schema_format(schema)
                records = PageSerializer.get_records_from_page(page_data, schema_format)

                # 查找要删除的记录
                for i, record in enumerate(records):
                    if self._rows_match(record, row):
                        # 准备写操作（获取锁，保存undo信息）
                        if not self.transaction_manager.prepare_write(txn_id, page_id):
                            raise StorageException(f"Failed to acquire write lock on page {page_id}")

                        # 从页中移除记录
                        updated_page_data, success = PageSerializer.remove_data_from_page(page_data, i)
                        if not success:
                            raise StorageException("Failed to remove record from page")

                        # 记录redo日志
                        self.transaction_manager.record_write(txn_id, page_id, updated_page_data)

                        # 写入更新后的页
                        self.table_storage.write_table_page(table_name, page_index, updated_page_data)
                        self.logger.debug(
                            f"Deleted row from table '{table_name}', page {page_index} in transaction {txn_id}")

                        # 维护所有索引
                        if table_name in self.table_indexes:
                            for index_name, index in self.table_indexes[table_name].items():
                                col_name = index_name.split('_')[-1]
                                key = row.get(col_name)
                                if key is not None:
                                    index.delete(key)

                        return True

            raise StorageException(f"Row not found in table '{table_name}' for deletion")
        except Exception as e:
            self.logger.error(f"Error deleting row from table '{table_name}' in transaction {txn_id}: {e}")
            return False

    def create_table(self, table_name: str, columns: List[Dict]) -> None:
        """为表分配初始存储空间 - 增强版，支持表空间和区管理"""
        try:
            # 计算预估记录大小
            schema = self._convert_to_schema_format(columns)
            estimated_size = RecordSerializer.calculate_record_size(schema)

            # 智能选择表空间 - 强制使用表空间管理器
            if self.storage_manager and hasattr(self.storage_manager, 'tablespace_manager'):
                tablespace_name = self.storage_manager.tablespace_manager.allocate_tablespace_for_table(table_name)
            else:
                # 回退策略
                tablespace_name = self._choose_tablespace_for_table(table_name)

            self.table_tablespace_mapping[table_name] = tablespace_name

            # 设置表上下文，启用区分配
            self.storage_manager.set_table_context(table_name)

            # 通过TableStorage创建表存储空间
            success = self.table_storage.create_table_storage(
                table_name,
                estimated_size,
                tablespace_name=tablespace_name  # 明确指定表空间
            )

            if not success:
                raise StorageException(f"Failed to create storage for table '{table_name}'")

            self.logger.info(f"Created table storage for '{table_name}' in tablespace '{tablespace_name}'")

        except Exception as e:
            self.logger.error(f"Error creating table '{table_name}': {e}")
            raise

    def _choose_tablespace_for_table(self, table_name: str) -> str:
        """为表智能选择表空间"""
        try:
            # 优先使用存储管理器的表空间管理器
            if (self.storage_manager and
                    hasattr(self.storage_manager, 'tablespace_manager')):
                return self.storage_manager.tablespace_manager.allocate_tablespace_for_table(table_name)

            # 回退策略：基于表名特征选择
            table_lower = table_name.lower()
            if any(table_lower.startswith(prefix) for prefix in ['sys_', 'pg_', 'system_', 'catalog_']):
                return "system"
            elif any(table_lower.startswith(prefix) for prefix in ['temp_', 'tmp_', 'sort_']):
                return "temp"
            elif any(table_lower.startswith(prefix) for prefix in ['log_', 'audit_', 'history_']):
                return "log"
            elif any(table_lower.startswith(prefix) for prefix in ['user_', 'data_', 'main_', 'large_', 'big_']):
                return "user_data"  # 确保用户表使用 user_data 表空间
            else:
                # 默认情况下也使用 user_data，而不是 default
                return "user_data"

        except Exception as e:
            self.logger.warning(f"Failed to choose tablespace for table '{table_name}': {e}, using user_data")
            return "user_data"  # 错误时也使用 user_data

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

    # def insert_row(self, table_name: str, row_data: List[Any]) -> None:
    #     """插入一行数据 - 增强版，支持表空间和区管理"""
    #     try:
    #         # 获取表schema - 从catalog获取真实schema
    #         schema = self._get_table_schema(table_name)
    #         if not schema:
    #             raise StorageException(f"Schema not found for table '{table_name}'")
    #
    #         # 将值列表转换为字典格式（与schema列顺序匹配）
    #         column_names = [col['name'] for col in schema]
    #         if len(row_data) != len(column_names):
    #             raise StorageException(
    #                 f"Number of values ({len(row_data)}) doesn't match number of columns ({len(column_names)})")
    #
    #         # 创建字典格式的行数据
    #         row_dict = {}
    #         for i, col_name in enumerate(column_names):
    #             if i < len(row_data):
    #                 row_dict[col_name] = row_data[i]
    #             else:
    #                 row_dict[col_name] = None  # 设置默认值
    #
    #         # 添加调试信息
    #         self.logger.debug(f"Inserting row data: {row_dict}")
    #         self.logger.debug(f"Schema: {schema}")
    #
    #         # 设置表上下文，启用区分配优化
    #         self.storage_manager.set_table_context(table_name)
    #
    #         # 序列化记录
    #         binary_row = self.serialize_row(row_dict, schema)
    #         self.logger.debug(f"Serialized binary data length: {len(binary_row)}")
    #
    #         # 检查表是否存在
    #         if not self.table_storage.table_exists(table_name):
    #             raise TableNotFoundException(table_name)
    #
    #         # 获取表的所有页
    #         page_ids = self.table_storage.get_table_pages(table_name)
    #
    #         # 尝试在现有页中插入记录
    #         for page_index, page_id in enumerate(page_ids):
    #             # 读取页数据
    #             page_data = self.table_storage.read_table_page(table_name, page_index)
    #
    #             # 尝试将记录添加到页
    #             new_page_data, success = PageSerializer.add_record_to_page(page_data, binary_row)
    #
    #             if success:
    #                 # 成功添加到页，写入更新后的页
    #                 self.table_storage.write_table_page(table_name, page_index, new_page_data)
    #                 self.logger.debug(f"Inserted row into table '{table_name}', page {page_id}")
    #
    #                 # 维护所有索引
    #                 if table_name in self.table_indexes:
    #                     for index_name, index in self.table_indexes[table_name].items():
    #                         # 获取索引对应的列名（需从catalog或索引元数据中获取，这里简化处理）
    #                         col_name = index_name.split('_')[-1]  # 假设索引名为 idx_表名_列名
    #                         key = row_dict.get(col_name)
    #                         if key is not None:
    #                             index.insert(key, row_dict)
    #
    #                 # 清除表上下文
    #                 self.storage_manager.clear_table_context()
    #                 return
    #
    #         # 如果没有现有页有足够空间，分配新页（使用智能区分配）
    #         new_page_id = self.table_storage.allocate_table_page(table_name)
    #         page_index = len(page_ids)  # 新页的索引
    #
    #         # 创建空页并添加记录
    #         empty_page = PageSerializer.create_empty_page()
    #         new_page_data, success = PageSerializer.add_record_to_page(empty_page, binary_row)
    #
    #         if not success:
    #             raise StorageException(f"Failed to add record to new page {new_page_id}")
    #
    #         # 写入新页
    #         self.table_storage.write_table_page(table_name, page_index, new_page_data)
    #
    #         # 维护所有索引
    #         if table_name in self.table_indexes:
    #             for index_name, index in self.table_indexes[table_name].items():
    #                 col_name = index_name.split('_')[-1]
    #                 key = row_dict.get(col_name)
    #                 if key is not None:
    #                     index.insert(key, row_dict)
    #
    #         # 清除表上下文
    #         self.storage_manager.clear_table_context()
    #
    #         self.logger.debug(f"Inserted row into new page {new_page_id} for table '{table_name}'")
    #
    #     except Exception as e:
    #         # 确保在异常时也清除上下文
    #         self.storage_manager.clear_table_context()
    #         self.logger.error(f"Error inserting row into table '{table_name}': {e}")
    #         raise

    # 修改原有的非事务方法，使其在需要时自动使用事务
    def insert_row(self, table_name: str, row_data: List[Any]) -> None:
        """插入一行数据 - 非事务版本"""
        # 对于非事务操作，自动开始并提交一个事务
        txn_id = self.begin_transaction()
        try:
            success = self.insert_row_transactional(table_name, row_data, txn_id)
            if not success:
                raise StorageException("Failed to insert row")
            self.commit_transaction(txn_id)
        except Exception as e:
            self.rollback_transaction(txn_id)
            raise

    def get_all_rows(self, table_name: str) -> List[Dict]:
        """获取表中的所有行（用于SeqScan）"""
        try:
            # 首先检查是否是视图
            if self.view_exists(table_name):
                return []

            # 获取表schema - 从catalog获取真实schema
            schema = self._get_table_schema(table_name)
            if not schema:
                raise StorageException(f"Schema not found for table '{table_name}'")

            all_rows = []

            # 获取表的所有页
            page_count = self.table_storage.get_table_page_count(table_name)
            self.logger.debug(f"Table {table_name} has {page_count} pages")

            # 遍历所有页提取记录
            for page_index in range(page_count):
                # 读取页数据
                page_data = self.table_storage.read_table_page(table_name, page_index)
                self.logger.debug(f"Page {page_index} data length: {len(page_data)}")

                # 从页中提取所有记录
                schema_format = self._convert_to_schema_format(schema)
                records = PageSerializer.get_records_from_page(page_data, schema_format)

                # 添加调试信息
                self.logger.debug(f"Page {page_index} contains {len(records)} records")
                for i, record in enumerate(records):
                    self.logger.debug(f"Record {i}: {record}")

                all_rows.extend(records)

            return all_rows

        except Exception as e:
            self.logger.error(f"Error getting all rows from table '{table_name}': {e}")
            raise

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

    def _get_table_schema(self, table_name: str) -> List[Dict]:
        """获取表schema - 从catalog获取真实schema，添加回退机制"""
        try:
            # 优先使用catalog manager实例
            if self.catalog_manager:
                table_info = self.catalog_manager.get_table(table_name)
                if table_info:
                    columns = table_info.get("columns", [])
                    self.logger.debug(f"Got table info from catalog for '{table_name}': {table_info}")

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
                                    self.logger.warning(f"Invalid column type '{col_type}' in table '{table_name}'")
                                    continue
                            else:
                                self.logger.warning(f"Invalid column type '{col_type}' in table '{table_name}'")
                                continue

                        valid_schema.append({
                            'name': col_name,
                            'type': col_type,
                            'length': self._extract_length_from_type(col.get('type', ''))
                        })

                    self.logger.debug(f"Converted schema for storage: {valid_schema}")
                    return valid_schema

            # 如果从catalog获取失败，尝试使用符号表作为回退
            if hasattr(self, 'execution_engine') and hasattr(self.execution_engine, 'symbol_table'):
                try:
                    table_info = self.execution_engine.symbol_table.get_table(table_name)
                    if table_info:
                        self.logger.info(f"Using symbol table as fallback for table '{table_name}'")
                        # 转换符号表格式为存储引擎需要的格式
                        schema = []
                        for col_name, col_type in table_info:
                            schema.append({
                                'name': col_name,
                                'type': col_type.upper() if col_type else 'VARCHAR',
                                'length': self._extract_length_from_type(col_type)
                            })
                        return schema
                except Exception as e:
                    self.logger.warning(f"Failed to get schema from symbol table: {e}")

            # 如果还是失败，尝试从系统表中查找
            if table_name in self.table_tablespace_mapping:
                self.logger.warning(
                    f"Table '{table_name}' exists in tablespace mapping but schema not found in catalog")
                # 返回一个默认schema（可能需要根据实际情况调整）
                return [{'name': 'id', 'type': 'INT'}, {'name': 'name', 'type': 'VARCHAR', 'length': 255}]

            # 最终回退：检查是否是视图
            if self.view_exists(table_name):
                self.logger.info(f"Table '{table_name}' is actually a view, returning empty schema")
                return []

            self.logger.error(f"Schema not found for table '{table_name}' in catalog, symbol table, or as view")
            raise StorageException(f"Schema not found for table '{table_name}'")

        except Exception as e:
            self.logger.error(f"Error getting schema from catalog for table '{table_name}': {e}")
            raise StorageException(f"Schema not found for table '{table_name}': {str(e)}")

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

    # def update_row(self, table_name: str, old_row: Dict, new_data: Dict) -> None:
    #     """更新一行数据 - 非事务版本"""
    #     # 对于非事务操作，自动开始并提交一个事务
    #     txn_id = self.begin_transaction()
    #     try:
    #         success = self.update_row_transactional(table_name, old_row, new_data, txn_id)
    #         if not success:
    #             raise StorageException("Failed to update row")
    #         self.commit_transaction(txn_id)
    #     except Exception as e:
    #         self.rollback_transaction(txn_id)
    #         raise

    def _rows_match(self, row1: Dict, row2: Dict) -> bool:
        """比较两行数据是否匹配"""
        # 添加调试信息
        self.logger.debug(f"Comparing rows:\nRow1: {row1}\nRow2: {row2}")

        if set(row1.keys()) != set(row2.keys()):
            self.logger.debug("Row keys don't match")
            return False

        for key in row1.keys():
            if row1.get(key) != row2.get(key):
                self.logger.debug(f"Value mismatch for key '{key}': {row1.get(key)} != {row2.get(key)}")
                return False

        self.logger.debug("Rows match")
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

                            # 维护所有索引
                            if table_name in self.table_indexes:
                                for index_name, index in self.table_indexes[table_name].items():
                                    col_name = index_name.split('_')[-1]
                                    key = row.get(col_name)
                                    if key is not None:
                                        index.delete(key)

                            return
                        else:
                            raise StorageException("Failed to remove record from page")

            raise StorageException(f"Row not found in table '{table_name}' for deletion")

        except Exception as e:
            self.logger.error(f"Error deleting row from table '{table_name}': {e}")
            raise

    # def delete_row(self, table_name: str, row: Dict) -> None:
    #     """删除一行数据 - 非事务版本"""
    #     # 对于非事务操作，自动开始并提交一个事务
    #     txn_id = self.begin_transaction()
    #     try:
    #         success = self.delete_row_transactional(table_name, row, txn_id)
    #         if not success:
    #             raise StorageException("Failed to delete row")
    #         self.commit_transaction(txn_id)
    #     except Exception as e:
    #         self.rollback_transaction(txn_id)
    #         raise

    def get_table_tablespace(self, table_name: str) -> str:
        """获取表所在的表空间"""
        return self.table_tablespace_mapping.get(table_name, "default")

    def get_storage_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        try:
            # 获取存储管理器的统计信息
            storage_info = self.storage_manager.get_storage_summary()

            # 添加表空间信息
            tablespace_info = {}
            for table_name, tablespace in self.table_tablespace_mapping.items():
                if tablespace not in tablespace_info:
                    tablespace_info[tablespace] = []
                tablespace_info[tablespace].append(table_name)

            return {
                "storage_manager": storage_info,
                "tablespaces": tablespace_info,
                "total_tables": len(self.table_tablespace_mapping)
            }
        except Exception as e:
            self.logger.error(f"Error getting storage stats: {e}")
            return {"error": str(e)}

    def optimize_storage(self, table_name: str = None) -> None:
        """优化存储空间"""
        try:
            if table_name:
                # 优化特定表
                self.storage_manager.defragment_table(table_name)
                self.logger.info(f"Optimized storage for table '{table_name}'")
            else:
                # 优化所有表
                for table in self.table_tablespace_mapping.keys():
                    self.storage_manager.defragment_table(table)
                self.logger.info("Optimized storage for all tables")
        except Exception as e:
            self.logger.error(f"Error optimizing storage: {e}")
            raise

    def verify_tablespace_allocation(self) -> Dict[str, List[str]]:
        """
        验证表空间分配情况

        Returns:
            每个表空间中的表列表
        """
        tablespace_tables = {}

        for table_name, tablespace in self.table_tablespace_mapping.items():
            if tablespace not in tablespace_tables:
                tablespace_tables[tablespace] = []
            tablespace_tables[tablespace].append(table_name)

        # 记录验证结果
        for tablespace, tables in tablespace_tables.items():
            self.logger.info(f"Tablespace '{tablespace}' contains {len(tables)} tables: {tables}")

        return tablespace_tables

    def create_index(self, table_name: str, index_name: str, column_name: str) -> bool:
        """为表创建索引"""
        try:
            # 首先确保表存在
            if not self.table_storage.table_exists(table_name):
                raise StorageException(f"Table '{table_name}' does not exist")

            if table_name not in self.table_indexes:
                self.table_indexes[table_name] = {}

            # 创建B+树索引实例
            index = BPlusTreeIndex(index_name)
            self.table_indexes[table_name][index_name] = index

            # 获取表schema - 从catalog获取真实schema
            schema = self._get_table_schema(table_name)
            if not schema:
                # 如果从catalog获取失败，尝试使用默认schema
                self.logger.warning(f"Could not get schema from catalog for table '{table_name}', using default schema")
                # 这里需要根据实际情况提供默认schema，或者抛出异常
                raise StorageException(f"Schema not found for table '{table_name}'")

            # 为现有数据构建索引
            rows = self.get_all_rows(table_name)
            for row in rows:
                key = row.get(column_name)
                if key is not None:
                    index.insert(key, row)  # 存储整个行数据（简化实现）

            # 更新catalog信息
            success = True
            if self.catalog_manager:
                self.catalog_manager.create_index(index_name, table_name, [column_name], False, "BTREE")

            return success

        except Exception as e:
            self.logger.error(f"Error creating index '{index_name}' for table '{table_name}': {e}")
            return False

    def drop_index(self, table_name: str, index_name: str) -> bool:
        """删除索引"""
        try:
            if table_name in self.table_indexes and index_name in self.table_indexes[table_name]:
                del self.table_indexes[table_name][index_name]
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error dropping index '{index_name}' from table '{table_name}': {e}")
            return False

    def get_rows_by_index(self, table_name: str, index_name: str, key: Any) -> List[Dict]:
        """通过索引键查询行"""
        try:
            if table_name in self.table_indexes and index_name in self.table_indexes[table_name]:
                index = self.table_indexes[table_name][index_name]
                result = index.search(key)
                return [result] if result else []
            return []
        except Exception as e:
            self.logger.error(f"Error querying index '{index_name}' for key '{key}': {e}")
            return []

    def get_view_definition(self, view_name: str) -> Optional[Dict]:
        """获取视图定义"""
        return self.views.get(view_name)

    def view_exists(self, view_name: str) -> bool:
        """检查视图是否存在"""
        return view_name in self.views