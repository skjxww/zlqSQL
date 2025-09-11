"""
表存储管理器测试
测试表到页的映射关系和表级存储管理
"""

import os
import tempfile
import unittest
import sys
import shutil

# 导入待测试的模块
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from storage.core.storage_manager import StorageManager
from storage.core.table_storage import TableStorage
from storage.utils.exceptions import (
    TableNotFoundException, StorageException
)
from storage.utils.constants import PAGE_SIZE


class TestTableStorage(unittest.TestCase):
    """表存储管理器测试类"""

    def setUp(self):
        """测试前准备"""
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp()
        self.data_file = os.path.join(self.temp_dir, "test_table_storage.db")
        self.meta_file = os.path.join(self.temp_dir, "test_table_storage_meta.json")
        self.catalog_file = os.path.join(self.temp_dir, "test_table_catalog.json")

        # 创建存储管理器
        self.storage_manager = StorageManager(
            buffer_size=10,
            data_file=self.data_file,
            meta_file=self.meta_file,
            auto_flush_interval=0
        )

        # 创建表存储管理器
        self.table_storage = TableStorage(
            self.storage_manager,
            self.catalog_file
        )

    def tearDown(self):
        """测试后清理"""
        # 关闭表存储管理器
        if hasattr(self, 'table_storage'):
            self.table_storage.shutdown()

        # 关闭存储管理器
        if hasattr(self, 'storage_manager') and not self.storage_manager.is_shutdown:
            self.storage_manager.shutdown()

        # 删除临时文件
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_01_initialization(self):
        """测试初始化"""
        print("测试1: 表存储管理器初始化")

        # 检查初始状态
        self.assertEqual(len(self.table_storage.list_tables()), 0)
        self.assertFalse(self.table_storage.table_exists("nonexistent"))

        # 检查存储信息
        storage_info = self.table_storage.get_storage_info()
        self.assertEqual(storage_info['total_tables'], 0)
        self.assertEqual(storage_info['total_pages'], 0)

        print("✓ 初始化正常")

    def test_02_create_table_storage(self):
        """测试创建表存储"""
        print("测试2: 创建表存储")

        # 创建表存储
        result = self.table_storage.create_table_storage("students", 512)
        self.assertTrue(result)

        # 检查表是否存在
        self.assertTrue(self.table_storage.table_exists("students"))
        self.assertIn("students", self.table_storage.list_tables())

        # 检查表的页数
        self.assertEqual(self.table_storage.get_table_page_count("students"), 1)

        # 检查存储信息
        storage_info = self.table_storage.get_storage_info()
        self.assertEqual(storage_info['total_tables'], 1)
        self.assertEqual(storage_info['total_pages'], 1)

        # 重复创建应该返回False
        result = self.table_storage.create_table_storage("students", 512)
        self.assertFalse(result)

        print("✓ 创建表存储正常")

    def test_03_table_page_operations(self):
        """测试表页操作"""
        print("测试3: 表页操作")

        # 创建表
        self.table_storage.create_table_storage("test_table", 1024)

        # 分配额外的页
        new_page_id = self.table_storage.allocate_table_page("test_table")
        self.assertIsNotNone(new_page_id)

        # 检查页数增加
        self.assertEqual(self.table_storage.get_table_page_count("test_table"), 2)

        # 准备测试数据
        test_data = b"Test page data for table" + b"\x00" * (PAGE_SIZE - 25)

        # 写入页数据
        self.table_storage.write_table_page("test_table", 0, test_data)

        # 读取页数据
        read_data = self.table_storage.read_table_page("test_table", 0)
        self.assertEqual(read_data, test_data)

        print("✓ 表页操作正常")

    def test_04_drop_table_storage(self):
        """测试删除表存储"""
        print("测试4: 删除表存储")

        # 创建表并添加页
        self.table_storage.create_table_storage("temp_table", 512)
        self.table_storage.allocate_table_page("temp_table")

        # 检查表存在且有2页
        self.assertTrue(self.table_storage.table_exists("temp_table"))
        self.assertEqual(self.table_storage.get_table_page_count("temp_table"), 2)

        # 删除表
        result = self.table_storage.drop_table_storage("temp_table")
        self.assertTrue(result)

        # 检查表已不存在
        self.assertFalse(self.table_storage.table_exists("temp_table"))
        self.assertNotIn("temp_table", self.table_storage.list_tables())

        # 重复删除应该抛出异常
        with self.assertRaises(TableNotFoundException):
            self.table_storage.drop_table_storage("temp_table")

        print("✓ 删除表存储正常")

    def test_05_multiple_tables(self):
        """测试多表管理"""
        print("测试5: 多表管理")

        # 创建多个表
        tables = ["users", "orders", "products", "categories"]
        for table_name in tables:
            result = self.table_storage.create_table_storage(table_name, 256)
            self.assertTrue(result)

            # 为每个表分配不同数量的页
            pages_to_add = tables.index(table_name) + 1
            for _ in range(pages_to_add):
                self.table_storage.allocate_table_page(table_name)

        # 检查所有表都存在
        created_tables = self.table_storage.list_tables()
        self.assertEqual(len(created_tables), 4)
        for table_name in tables:
            self.assertIn(table_name, created_tables)

        # 检查每个表的页数
        for i, table_name in enumerate(tables):
            expected_pages = i + 2  # 初始页 + 额外分配的页
            self.assertEqual(self.table_storage.get_table_page_count(table_name), expected_pages)

        # 检查总体存储信息
        storage_info = self.table_storage.get_storage_info()
        self.assertEqual(storage_info['total_tables'], 4)
        expected_total_pages = sum(i + 2 for i in range(4))  # 1+2+3+4 = 10
        self.assertEqual(storage_info['total_pages'], expected_total_pages)

        print("✓ 多表管理正常")

    def test_06_table_data_operations(self):
        """测试表数据操作"""
        print("测试6: 表数据操作")

        # 创建表并分配多个页
        self.table_storage.create_table_storage("data_table", 1024)
        self.table_storage.allocate_table_page("data_table")
        self.table_storage.allocate_table_page("data_table")

        # 准备不同的测试数据
        test_data_pages = []
        for i in range(3):
            data = f"Page {i} content - test data".encode() + b"\x00" * (PAGE_SIZE - 30)
            test_data_pages.append(data)

        # 写入不同页的数据
        for i, data in enumerate(test_data_pages):
            self.table_storage.write_table_page("data_table", i, data)

        # 读取并验证数据
        for i, expected_data in enumerate(test_data_pages):
            read_data = self.table_storage.read_table_page("data_table", i)
            self.assertEqual(read_data, expected_data)

        # 修改数据并验证
        modified_data = b"Modified page content" + b"\x00" * (PAGE_SIZE - 21)
        self.table_storage.write_table_page("data_table", 1, modified_data)

        read_modified = self.table_storage.read_table_page("data_table", 1)
        self.assertEqual(read_modified, modified_data)

        # 确保其他页没有受到影响
        self.assertEqual(self.table_storage.read_table_page("data_table", 0), test_data_pages[0])
        self.assertEqual(self.table_storage.read_table_page("data_table", 2), test_data_pages[2])

        print("✓ 表数据操作正常")

    def test_07_error_handling(self):
        """测试错误处理"""
        print("测试7: 错误处理")

        # 测试对不存在表的操作
        with self.assertRaises(TableNotFoundException):
            self.table_storage.get_table_pages("nonexistent")

        with self.assertRaises(TableNotFoundException):
            self.table_storage.allocate_table_page("nonexistent")

        with self.assertRaises(TableNotFoundException):
            self.table_storage.read_table_page("nonexistent", 0)

        with self.assertRaises(TableNotFoundException):
            self.table_storage.write_table_page("nonexistent", 0, b"data")

        # 创建表用于测试页索引越界
        self.table_storage.create_table_storage("test_table", 512)

        # 测试页索引越界
        with self.assertRaises(StorageException):
            self.table_storage.read_table_page("test_table", 999)  # 不存在的页索引

        with self.assertRaises(StorageException):
            self.table_storage.write_table_page("test_table", 999, b"data")

        print("✓ 错误处理正常")

    def test_08_storage_persistence(self):
        """测试存储持久化"""
        print("测试8: 存储持久化")

        # 创建表并写入数据
        self.table_storage.create_table_storage("persistent_table", 1024)
        test_data = b"Persistent test data" + b"\x00" * (PAGE_SIZE - 20)
        self.table_storage.write_table_page("persistent_table", 0, test_data)

        # 强制刷新数据
        self.storage_manager.flush_all_pages()

        # 关闭表存储管理器
        self.table_storage.shutdown()
        self.storage_manager.shutdown()

        # 重新创建存储管理器和表存储管理器
        new_storage_manager = StorageManager(
            buffer_size=10,
            data_file=self.data_file,
            meta_file=self.meta_file,
            auto_flush_interval=0
        )

        new_table_storage = TableStorage(
            new_storage_manager,
            self.catalog_file
        )

        try:
            # 验证表仍然存在
            self.assertTrue(new_table_storage.table_exists("persistent_table"))

            # 验证数据仍然可以读取
            read_data = new_table_storage.read_table_page("persistent_table", 0)
            self.assertEqual(read_data, test_data)

            print("✓ 存储持久化正常")

        finally:
            # 清理新的管理器
            new_table_storage.shutdown()
            new_storage_manager.shutdown()


if __name__ == "__main__":
    unittest.main()