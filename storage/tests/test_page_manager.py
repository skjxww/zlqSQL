"""
页管理器测试
测试页的分配、释放、读写等基本功能
"""

import os
import tempfile
import unittest
from pathlib import Path

# 导入待测试的模块
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from storage.core.page_manager import PageManager
from storage.utils.exceptions import InvalidPageIdException, PageNotAllocatedException
from storage.utils.constants import PAGE_SIZE


class TestPageManager(unittest.TestCase):
    """页管理器测试类"""

    def setUp(self):
        """测试前准备"""
        # 创建临时目录和文件
        self.temp_dir = tempfile.mkdtemp()
        self.data_file = os.path.join(self.temp_dir, "test_data.db")
        self.meta_file = os.path.join(self.temp_dir, "test_metadata.json")

        # 创建页管理器实例
        self.page_manager = PageManager(self.data_file, self.meta_file)

    def tearDown(self):
        """测试后清理"""
        # 清理页管理器
        if hasattr(self, 'page_manager'):
            self.page_manager.cleanup()

        # 删除临时文件
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_01_initial_state(self):
        """测试初始状态"""
        print("测试1: 页管理器初始状态")

        # 检查初始状态
        self.assertEqual(self.page_manager.get_page_count(), 0)
        self.assertEqual(self.page_manager.get_free_page_count(), 0)
        self.assertTrue(os.path.exists(self.data_file))
        # 移除这行：self.assertTrue(os.path.exists(self.meta_file))
        # 因为元数据文件只有在有页操作时才会创建

        print("✓ 初始状态正常")

    def test_02_allocate_single_page(self):
        """测试分配单个页"""
        print("测试2: 分配单个页")

        page_id = self.page_manager.allocate_page()

        # 验证分配结果
        self.assertEqual(page_id, 1)  # 第一个页应该是1
        self.assertEqual(self.page_manager.get_page_count(), 1)
        self.assertTrue(self.page_manager.is_page_allocated(page_id))

        print(f"✓ 成功分配页 {page_id}")

    def test_03_write_and_read_page(self):
        """测试页的写入和读取"""
        print("测试3: 页的写入和读取")

        # 分配一个页
        page_id = self.page_manager.allocate_page()

        # 准备测试数据 - 修改这里
        test_message = b"Hello, Page Manager!"
        test_data = test_message + b"\x00" * (PAGE_SIZE - len(test_message))

        # 写入数据
        self.page_manager.write_page_to_disk(page_id, test_data)

        # 读取数据
        read_data = self.page_manager.read_page_from_disk(page_id)

        # 验证数据一致性 - 修改比较方式
        self.assertEqual(len(read_data), PAGE_SIZE)
        self.assertEqual(read_data[:len(test_message)], test_message)  # 只比较有效数据部分
        self.assertEqual(read_data, test_data)  # 验证完整数据一致性

        print(f"✓ 页 {page_id} 写入和读取成功")

    def test_04_deallocate_page(self):
        """测试页的释放和重用"""
        print("测试4: 页的释放和重用")

        # 分配两个页
        page1 = self.page_manager.allocate_page()
        page2 = self.page_manager.allocate_page()
        self.assertEqual(self.page_manager.get_page_count(), 2)

        # 释放第一个页
        self.page_manager.deallocate_page(page1)
        self.assertEqual(self.page_manager.get_page_count(), 1)
        self.assertEqual(self.page_manager.get_free_page_count(), 1)

        # 重新分配页，应该重用被释放的页
        page3 = self.page_manager.allocate_page()
        self.assertEqual(page3, page1)  # 应该重用page1
        self.assertEqual(self.page_manager.get_free_page_count(), 0)

        print(f"✓ 页释放和重用功能正常")

    def test_05_invalid_page_operations(self):
        """测试无效页操作的异常处理"""
        print("测试5: 无效页操作异常处理")

        # 测试无效页号
        with self.assertRaises(InvalidPageIdException):
            self.page_manager.read_page_from_disk(0)

        with self.assertRaises(InvalidPageIdException):
            self.page_manager.read_page_from_disk(-1)

        with self.assertRaises(InvalidPageIdException):
            self.page_manager.write_page_to_disk(0, b"test")

        # 测试释放未分配的页
        with self.assertRaises(PageNotAllocatedException):
            self.page_manager.deallocate_page(999)

        print("✓ 异常处理正常")

    def test_06_metadata_persistence(self):
        """测试元数据持久化"""
        print("测试6: 元数据持久化")

        # 分配几个页
        page1 = self.page_manager.allocate_page()
        page2 = self.page_manager.allocate_page()
        page3 = self.page_manager.allocate_page()

        # 释放一个页
        self.page_manager.deallocate_page(page2)

        # 记录当前状态
        allocated_pages = self.page_manager.get_allocated_pages()
        free_pages = self.page_manager.get_free_pages()

        # 关闭并重新创建页管理器
        self.page_manager.cleanup()
        new_page_manager = PageManager(self.data_file, self.meta_file)

        # 验证状态恢复
        self.assertEqual(new_page_manager.get_allocated_pages(), allocated_pages)
        self.assertEqual(new_page_manager.get_free_pages(), free_pages)

        # 清理新的页管理器
        new_page_manager.cleanup()

        print("✓ 元数据持久化正常")


if __name__ == "__main__":
    unittest.main()