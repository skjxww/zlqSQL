"""
索引管理器 - 提供给数据库引擎层的接口
"""
from typing import Optional, Dict, List, Tuple
from .btree.btree import BPlusTree
import json
import os


class IndexManager:
    """索引管理器 - 管理所有B+树索引"""

    def __init__(self, storage_manager, catalog_file="data/indexes.json"):
        self.storage = storage_manager
        self.catalog_file = catalog_file
        self.indexes: Dict[str, BPlusTree] = {}  # 已加载的索引
        self.index_metadata = {}  # 索引元数据

        # 确保目录存在
        os.makedirs(os.path.dirname(catalog_file), exist_ok=True)

        # 加载索引目录
        self._load_catalog()

    def create_index(self, index_name: str, table_name: str,
                     column_name: str) -> bool:
        """
        创建索引

        Args:
            index_name: 索引名称
            table_name: 表名
            column_name: 列名

        Returns:
            bool: 创建是否成功
        """
        if index_name in self.index_metadata:
            return False

        # 创建B+树
        btree = BPlusTree(self.storage, index_name)
        self.indexes[index_name] = btree

        # 保存元数据
        self.index_metadata[index_name] = {
            'index_name': index_name,
            'table_name': table_name,
            'column_name': column_name,
            'root_page_id': btree.root_page_id,
            'index_type': 'btree'
        }

        self._save_catalog()
        return True

    def get_index(self, table_name: str, column_name: str) -> Optional[BPlusTree]:
        """
        获取指定表和列的索引

        Args:
            table_name: 表名
            column_name: 列名

        Returns:
            BPlusTree or None
        """
        # 查找索引
        for index_name, metadata in self.index_metadata.items():
            if (metadata['table_name'] == table_name and
                    metadata['column_name'] == column_name):

                # 如果索引未加载，加载它
                if index_name not in self.indexes:
                    btree = BPlusTree(self.storage, index_name)
                    btree.root_page_id = metadata['root_page_id']
                    self.indexes[index_name] = btree

                return self.indexes[index_name]

        return None

    def insert_into_index(self, table_name: str, column_name: str,
                          key: int, page_id: int, slot_id: int) -> bool:
        """
        向索引插入数据

        Args:
            table_name: 表名
            column_name: 列名
            key: 索引键值
            page_id: 记录所在页
            slot_id: 记录在页内的槽位

        Returns:
            bool: 插入是否成功
        """
        btree = self.get_index(table_name, column_name)
        if btree:
            return btree.insert(key, (page_id, slot_id))
        return False

    def search_index(self, table_name: str, column_name: str,
                     key: int) -> Optional[Tuple[int, int]]:
        """
        使用索引查找

        Args:
            table_name: 表名
            column_name: 列名
            key: 查找的键值

        Returns:
            (page_id, slot_id) or None
        """
        btree = self.get_index(table_name, column_name)
        if btree:
            return btree.search(key)
        return None

    def range_search_index(self, table_name: str, column_name: str,
                           start_key: int, end_key: int) -> List[Tuple[int, Tuple[int, int]]]:
        """
        使用索引进行范围查询

        Args:
            table_name: 表名
            column_name: 列名
            start_key: 起始键
            end_key: 结束键

        Returns:
            [(key, (page_id, slot_id)), ...]
        """
        btree = self.get_index(table_name, column_name)
        if btree:
            return btree.range_search(start_key, end_key)
        return []

    def drop_index(self, index_name: str) -> bool:
        """删除索引"""
        if index_name not in self.index_metadata:
            return False

        # 从内存中移除
        if index_name in self.indexes:
            del self.indexes[index_name]

        # 从元数据中移除
        del self.index_metadata[index_name]

        self._save_catalog()
        return True

    def list_indexes(self) -> List[dict]:
        """列出所有索引"""
        return list(self.index_metadata.values())

    def _load_catalog(self):
        """加载索引目录"""
        if os.path.exists(self.catalog_file):
            try:
                with open(self.catalog_file, 'r') as f:
                    data = json.load(f)
                    self.index_metadata = data.get('indexes', {})
            except:
                self.index_metadata = {}

    def _save_catalog(self):
        """保存索引目录"""
        data = {
            'version': '1.0',
            'indexes': self.index_metadata
        }
        with open(self.catalog_file, 'w') as f:
            json.dump(data, f, indent=2)