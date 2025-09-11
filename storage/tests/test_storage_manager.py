"""
存储管理器测试
测试页管理器和缓存池的集成功能
"""

import os
import tempfile
import unittest
import sys
import shutil

# 导入待测试的模块
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from storage.core.storage_manager import StorageManager
from storage.utils.exceptions import (
    SystemShutdownException, InvalidPageIdException,
    PageNotAllocatedException
)
from storage.utils.constants import PAGE_SIZE

from storage.utils.exceptions import (
    SystemShutdownException, InvalidPageIdException,
    PageNotAllocatedException, BufferPoolException  # 添加这个
)


class TestStorageManager(unittest.TestCase):
    """存储管理器测试类"""

    def setUp(self):
        """测试前准备"""
        # 创建临时目录和文件
        self.temp_dir = tempfile.mkdtemp()
        self.data_file = os.path.join(self.temp_dir, "test_storage.db")
        self.meta_file = os.path.join(self.temp_dir, "test_storage_meta.json")

        # 创建存储管理器实例（小缓存便于测试）
        self.storage_manager = StorageManager(
            buffer_size=10,  # 最小允许缓存大小
            data_file=self.data_file,
            meta_file=self.meta_file,
            auto_flush_interval=0  # 禁用自动刷新
        )

        # 准备测试数据
        self.test_data_1 = b"Storage Test Data 1" + b"\x00" * (PAGE_SIZE - 19)
        self.test_data_2 = b"Storage Test Data 2" + b"\x00" * (PAGE_SIZE - 19)
        self.test_data_3 = b"Storage Test Data 3" + b"\x00" * (PAGE_SIZE - 19)

    def tearDown(self):
        """测试后清理"""
        # 关闭存储管理器（检查是否已关闭）
        if hasattr(self, 'storage_manager') and not self.storage_manager.is_shutdown:
            self.storage_manager.shutdown()

        # 删除临时文件
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_01_initialization(self):
        """测试初始化"""
        print("测试1: 存储管理器初始化")

        # 检查初始状态
        storage_info = self.storage_manager.get_storage_info()

        self.assertEqual(storage_info['system_status'], 'running')
        self.assertEqual(storage_info['operation_count'], 0)
        self.assertEqual(storage_info['flush_count'], 0)

        # 检查缓存统计
        cache_stats = self.storage_manager.get_cache_stats()
        self.assertEqual(cache_stats['cache_size'], 0)
        self.assertEqual(cache_stats['cache_capacity'], 10)

        # 检查页统计
        page_stats = self.storage_manager.get_page_stats()
        self.assertEqual(page_stats['pages']['allocated'], 0)

        print("✓ 初始化正常")

    def test_02_page_allocation_and_deallocation(self):
        """测试页分配和释放"""
        print("测试2: 页分配和释放")

        # 分配页
        page_id = self.storage_manager.allocate_page()
        self.assertEqual(page_id, 1)

        # 检查统计
        page_stats = self.storage_manager.get_page_stats()
        self.assertEqual(page_stats['pages']['allocated'], 1)

        # 释放页
        self.storage_manager.deallocate_page(page_id)
        page_stats = self.storage_manager.get_page_stats()
        self.assertEqual(page_stats['pages']['allocated'], 0)
        self.assertEqual(page_stats['pages']['free'], 1)

        print("✓ 页分配和释放正常")

    def test_03_read_write_with_caching(self):
        """测试读写操作与缓存集成"""
        print("测试3: 读写操作与缓存集成")

        # 分配页
        page_id = self.storage_manager.allocate_page()

        # 写入数据（应该进入缓存）
        self.storage_manager.write_page(page_id, self.test_data_1)

        # 读取数据（应该从缓存命中）
        read_data = self.storage_manager.read_page(page_id)
        self.assertEqual(read_data, self.test_data_1)

        # 检查缓存统计
        cache_stats = self.storage_manager.get_cache_stats()
        self.assertEqual(cache_stats['cache_size'], 1)
        self.assertEqual(cache_stats['hit_count'], 1)  # 读取时缓存命中
        self.assertEqual(cache_stats['dirty_pages'], 1)  # 写入的页是脏页

        print("✓ 读写操作与缓存集成正常")

    def test_04_cache_eviction_with_disk_write(self):
        """测试缓存满时的淘汰和磁盘写入"""
        print("测试4: 缓存淘汰和磁盘写入")

        # 分配并填满缓存（10个页）
        page_ids = []
        for i in range(10):
            page_id = self.storage_manager.allocate_page()
            page_ids.append(page_id)
            test_data = f"Test data for page {i + 1}".encode() + b"\x00" * (
                        PAGE_SIZE - len(f"Test data for page {i + 1}"))
            self.storage_manager.write_page(page_id, test_data)

        # 检查缓存已满
        cache_stats = self.storage_manager.get_cache_stats()
        self.assertEqual(cache_stats['cache_size'], 10)
        self.assertEqual(cache_stats['dirty_pages'], 10)

        # 访问前几个页，改变LRU顺序
        self.storage_manager.read_page(page_ids[0])
        self.storage_manager.read_page(page_ids[1])

        # 分配新页，触发缓存淘汰
        new_page_id = self.storage_manager.allocate_page()
        new_data = b"New page data" + b"\x00" * (PAGE_SIZE - 13)
        self.storage_manager.write_page(new_page_id, new_data)

        # 检查缓存统计
        cache_stats = self.storage_manager.get_cache_stats()
        self.assertEqual(cache_stats['cache_size'], 10)  # 仍然满的
        self.assertGreaterEqual(cache_stats['eviction_count'], 1)  # 发生了淘汰

        # 验证最近访问的页仍在缓存中
        data = self.storage_manager.read_page(page_ids[0])
        self.assertIsNotNone(data)

        print("✓ 缓存淘汰和磁盘写入正常")

    def test_05_flush_operations(self):
        """测试各种刷新操作"""
        print("测试5: 刷新操作")

        # 创建一些脏页
        page_ids = []
        for i in range(3):
            page_id = self.storage_manager.allocate_page()
            page_ids.append(page_id)
            test_data = f"Flush test data {i + 1}".encode() + b"\x00" * (PAGE_SIZE - len(f"Flush test data {i + 1}"))
            self.storage_manager.write_page(page_id, test_data)

        # 检查脏页数量
        cache_stats = self.storage_manager.get_cache_stats()
        self.assertEqual(cache_stats['dirty_pages'], 3)

        # 刷新单个页
        flushed = self.storage_manager.flush_page(page_ids[0])
        self.assertTrue(flushed)

        # 检查脏页数量减少
        cache_stats = self.storage_manager.get_cache_stats()
        self.assertEqual(cache_stats['dirty_pages'], 2)

        # 刷新所有页
        flushed_count = self.storage_manager.flush_all_pages()
        self.assertEqual(flushed_count, 2)

        # 检查没有脏页了
        cache_stats = self.storage_manager.get_cache_stats()
        self.assertEqual(cache_stats['dirty_pages'], 0)

        print("✓ 刷新操作正常")

    def test_06_data_persistence(self):
        """测试数据持久化"""
        print("测试6: 数据持久化")

        # 写入一些数据
        page_id = self.storage_manager.allocate_page()
        original_data = b"Persistence test data" + b"\x00" * (PAGE_SIZE - 21)
        self.storage_manager.write_page(page_id, original_data)

        # 强制刷新到磁盘
        self.storage_manager.flush_all_pages()

        # 关闭存储管理器
        self.storage_manager.shutdown()

        # 重新创建存储管理器（使用相同的文件）
        new_storage_manager = StorageManager(
            buffer_size=10,
            data_file=self.data_file,
            meta_file=self.meta_file,
            auto_flush_interval=0
        )

        try:
            # 读取数据，应该从磁盘加载
            read_data = new_storage_manager.read_page(page_id)
            self.assertEqual(read_data, original_data)

            print("✓ 数据持久化正常")

        finally:
            # 清理新的存储管理器
            new_storage_manager.shutdown()

    def test_07_transaction_context(self):
        """测试事务上下文管理器"""
        print("测试7: 事务上下文管理器")

        # 使用事务上下文
        with self.storage_manager.transaction():
            # 在事务中执行操作
            page_id = self.storage_manager.allocate_page()
            test_data = b"Transaction test data" + b"\x00" * (PAGE_SIZE - 21)
            self.storage_manager.write_page(page_id, test_data)

            # 检查脏页存在
            cache_stats = self.storage_manager.get_cache_stats()
            self.assertEqual(cache_stats['dirty_pages'], 1)

        # 事务结束后，脏页应该被自动刷新
        cache_stats = self.storage_manager.get_cache_stats()
        self.assertEqual(cache_stats['dirty_pages'], 0)

        # 验证数据仍然可以读取
        read_data = self.storage_manager.read_page(page_id)
        self.assertEqual(read_data, test_data)

        print("✓ 事务上下文管理器正常")

    def test_08_error_handling(self):
        """测试错误处理"""
        print("测试8: 错误处理")

        # 测试无效页号读取
        with self.assertRaises(BufferPoolException):
            self.storage_manager.read_page(-1)

        # 测试无效页号写入 - 使用更明显无效的页号
        with self.assertRaises(BufferPoolException):
            self.storage_manager.write_page(-1, b"invalid")

        # 测试释放未分配的页
        with self.assertRaises(PageNotAllocatedException):
            self.storage_manager.deallocate_page(999)

        print("✓ 错误处理正常")

    def test_09_shutdown_behavior(self):
        """单独测试关闭行为"""
        print("测试9: 关闭行为")

        # 执行一些正常操作
        page_id = self.storage_manager.allocate_page()
        test_data = b"Normal data" + b"\x00" * (PAGE_SIZE - 11)
        self.storage_manager.write_page(page_id, test_data)

        # 关闭存储管理器
        self.storage_manager.shutdown()

        # 关闭后的操作应该抛出异常
        with self.assertRaises(SystemShutdownException):
            self.storage_manager.allocate_page()

        with self.assertRaises(SystemShutdownException):
            self.storage_manager.read_page(1)

        print("✓ 关闭行为正常")

if __name__ == "__main__":
    unittest.main()