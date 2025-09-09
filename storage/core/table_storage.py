# storage/core/table_storage.py
"""
表存储管理器 - 专注于表到页的映射关系
这是存储层的职责，不涉及具体的记录格式和模式管理
"""

import json
import os
import time
from typing import Dict, List, Optional
from ..utils.exceptions import StorageException, TableNotFoundException
from ..utils.logger import get_logger


class TableStorageMetadata:
    """表存储元数据 - 只关注存储层信息"""

    def __init__(self, table_name: str, estimated_record_size: int):
        self.table_name = table_name
        self.pages = []  # 表占用的页号列表
        self.estimated_record_size = estimated_record_size
        self.created_time = time.time()
        self.last_modified = time.time()

        # 存储层统计
        self.total_page_allocations = 0
        self.total_page_reads = 0
        self.total_page_writes = 0

    def add_page(self, page_id: int):
        """添加页"""
        if page_id not in self.pages:
            self.pages.append(page_id)
            self.total_page_allocations += 1
            self.last_modified = time.time()

    def remove_page(self, page_id: int):
        """移除页"""
        if page_id in self.pages:
            self.pages.remove(page_id)
            self.last_modified = time.time()

    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'table_name': self.table_name,
            'pages': self.pages,
            'estimated_record_size': self.estimated_record_size,
            'created_time': self.created_time,
            'last_modified': self.last_modified,
            'total_page_allocations': self.total_page_allocations,
            'total_page_reads': self.total_page_reads,
            'total_page_writes': self.total_page_writes
        }

    @classmethod
    def from_dict(cls, data: dict):
        """从字典创建实例"""
        metadata = cls(data['table_name'], data.get('estimated_record_size', 1024))
        metadata.pages = data.get('pages', [])
        metadata.created_time = data.get('created_time', time.time())
        metadata.last_modified = data.get('last_modified', time.time())
        metadata.total_page_allocations = data.get('total_page_allocations', 0)
        metadata.total_page_reads = data.get('total_page_reads', 0)
        metadata.total_page_writes = data.get('total_page_writes', 0)
        return metadata


class TableStorage:
    """
    表存储管理器 - 负责表到页的映射
    这是存储层的职责，专注于页级管理，不涉及记录格式
    """

    def __init__(self, storage_manager, catalog_file="data/table_storage_catalog.json"):
        self.storage_manager = storage_manager
        self.catalog_file = catalog_file
        self.tables: Dict[str, TableStorageMetadata] = {}
        self.logger = get_logger("table_storage")

        # 确保目录存在
        os.makedirs(os.path.dirname(catalog_file), exist_ok=True)

        # 加载现有的表存储信息
        self._load_catalog()

        self.logger.info(f"TableStorage initialized with {len(self.tables)} tables")

    def create_table_storage(self, table_name: str, estimated_record_size: int = 1024) -> bool:
        """
        为表创建存储空间

        Args:
            table_name: 表名
            estimated_record_size: 估算的记录大小（用于优化页分配）
        """
        if table_name in self.tables:
            self.logger.warning(f"Table storage for '{table_name}' already exists")
            return False

        try:
            # 分配初始页
            initial_page = self.storage_manager.allocate_page()

            # 创建表存储元数据
            metadata = TableStorageMetadata(table_name, estimated_record_size)
            metadata.add_page(initial_page)

            # 初始化页内容（空页）
            from ..utils.serializer import PageSerializer
            empty_page = PageSerializer.create_empty_page()
            self.storage_manager.write_page(initial_page, empty_page)

            self.tables[table_name] = metadata
            self._save_catalog()

            self.logger.info(f"Created storage for table '{table_name}' with initial page {initial_page}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create storage for table '{table_name}': {e}")
            return False

    def drop_table_storage(self, table_name: str) -> bool:
        """删除表的存储空间"""
        if table_name not in self.tables:
            raise TableNotFoundException(table_name)

        try:
            metadata = self.tables[table_name]

            # 释放所有页
            for page_id in metadata.pages:
                self.storage_manager.deallocate_page(page_id)

            # 从目录中移除
            del self.tables[table_name]
            self._save_catalog()

            self.logger.info(f"Dropped storage for table '{table_name}', freed {len(metadata.pages)} pages")
            return True

        except Exception as e:
            self.logger.error(f"Failed to drop storage for table '{table_name}': {e}")
            return False

    def get_table_pages(self, table_name: str) -> List[int]:
        """获取表占用的页号列表"""
        if table_name not in self.tables:
            raise TableNotFoundException(table_name)
        return self.tables[table_name].pages.copy()

    def allocate_table_page(self, table_name: str) -> int:
        """为表分配新页"""
        if table_name not in self.tables:
            raise TableNotFoundException(table_name)

        try:
            new_page = self.storage_manager.allocate_page()

            # 初始化新页
            from ..utils.serializer import PageSerializer
            empty_page = PageSerializer.create_empty_page()
            self.storage_manager.write_page(new_page, empty_page)

            # 添加到表的页列表
            self.tables[table_name].add_page(new_page)
            self._save_catalog()

            self.logger.debug(f"Allocated new page {new_page} for table '{table_name}'")
            return new_page

        except Exception as e:
            self.logger.error(f"Failed to allocate page for table '{table_name}': {e}")
            raise StorageException(f"Page allocation failed: {e}")

    def read_table_page(self, table_name: str, page_index: int) -> bytes:
        """
        读取表的指定页

        Args:
            table_name: 表名
            page_index: 页在表中的索引（非页号）
        """
        if table_name not in self.tables:
            raise TableNotFoundException(table_name)

        pages = self.tables[table_name].pages
        if page_index >= len(pages):
            raise StorageException(f"Page index {page_index} out of range for table '{table_name}'")

        page_id = pages[page_index]
        self.tables[table_name].total_page_reads += 1

        return self.storage_manager.read_page(page_id)

    def write_table_page(self, table_name: str, page_index: int, data: bytes):
        """
        写入表的指定页

        Args:
            table_name: 表名
            page_index: 页在表中的索引（非页号）
            data: 页数据
        """
        if table_name not in self.tables:
            raise TableNotFoundException(table_name)

        pages = self.tables[table_name].pages
        if page_index >= len(pages):
            raise StorageException(f"Page index {page_index} out of range for table '{table_name}'")

        page_id = pages[page_index]
        self.tables[table_name].total_page_writes += 1
        self.tables[table_name].last_modified = time.time()

        self.storage_manager.write_page(page_id, data)

    def get_table_page_count(self, table_name: str) -> int:
        """获取表的页数量"""
        if table_name not in self.tables:
            raise TableNotFoundException(table_name)
        return len(self.tables[table_name].pages)

    def table_exists(self, table_name: str) -> bool:
        """检查表存储是否存在"""
        return table_name in self.tables

    def list_tables(self) -> List[str]:
        """列出所有表"""
        return list(self.tables.keys())

    def get_storage_info(self, table_name: str = None) -> dict:
        """获取存储信息"""
        if table_name:
            if table_name not in self.tables:
                raise TableNotFoundException(table_name)
            return self.tables[table_name].to_dict()
        else:
            # 返回所有表的存储信息
            return {
                'total_tables': len(self.tables),
                'total_pages': sum(len(meta.pages) for meta in self.tables.values()),
                'tables': {name: meta.to_dict() for name, meta in self.tables.items()}
            }

    def optimize_table_storage(self, table_name: str) -> dict:
        """
        优化表存储（简化版本）
        主要是整理页的分配，提供统计信息
        """
        if table_name not in self.tables:
            raise TableNotFoundException(table_name)

        metadata = self.tables[table_name]

        # 简单的优化：确保所有页都是已分配的
        allocated_pages = set(self.storage_manager.page_manager.get_allocated_pages())
        valid_pages = [p for p in metadata.pages if p in allocated_pages]

        removed_pages = len(metadata.pages) - len(valid_pages)
        metadata.pages = valid_pages

        if removed_pages > 0:
            self.logger.info(f"Cleaned up {removed_pages} invalid pages for table '{table_name}'")
            self._save_catalog()

        return {
            'table_name': table_name,
            'pages_cleaned': removed_pages,
            'total_pages': len(valid_pages),
            'estimated_size_mb': len(valid_pages) * 4096 / (1024 * 1024)
        }

    def _load_catalog(self):
        """加载表存储目录"""
        if not os.path.exists(self.catalog_file):
            self.logger.info("No existing table storage catalog found")
            return

        try:
            with open(self.catalog_file, 'r', encoding='utf-8') as f:
                catalog_data = json.load(f)

            for table_data in catalog_data.get('tables', []):
                metadata = TableStorageMetadata.from_dict(table_data)
                self.tables[metadata.table_name] = metadata

            self.logger.info(f"Loaded {len(self.tables)} table storage entries")

        except Exception as e:
            self.logger.error(f"Failed to load table storage catalog: {e}")

    def _save_catalog(self):
        """保存表存储目录"""
        try:
            catalog_data = {
                'version': '1.0',
                'created_time': time.time(),
                'table_count': len(self.tables),
                'tables': [meta.to_dict() for meta in self.tables.values()]
            }

            # 原子写入
            temp_file = self.catalog_file + '.tmp'
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(catalog_data, f, indent=2, ensure_ascii=False)

            os.replace(temp_file, self.catalog_file)

        except Exception as e:
            self.logger.error(f"Failed to save table storage catalog: {e}")

    def shutdown(self):
        """关闭表存储管理器"""
        try:
            self._save_catalog()
            self.logger.info("TableStorage shutdown completed")
        except Exception as e:
            self.logger.error(f"Error during TableStorage shutdown: {e}")