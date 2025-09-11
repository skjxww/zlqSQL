"""
缓存池测试
测试LRU缓存机制、脏页管理等功能
"""

import os
import tempfile
import unittest
import sys

# 导入待测试的模块
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from storage.core.buffer_pool import BufferPool
from storage.utils.exceptions import BufferPoolException
from storage.utils.constants import PAGE_SIZE


class TestBufferPool(unittest.TestCase):
    """缓存池测试类"""

    def setUp(self):
        """测试前准备"""
        # 创建小容量的缓存池便于测试（最小容量是10）
        self.buffer_pool = BufferPool(capacity=10)

        # 准备测试数据
        self.test_data_1 = b"Page 1 data" + b"\x00" * (PAGE_SIZE - 11)
        self.test_data_2 = b"Page 2 data" + b"\x00" * (PAGE_SIZE - 11)
        self.test_data_3 = b"Page 3 data" + b"\x00" * (PAGE_SIZE - 11)
        self.test_data_4 = b"Page 4 data" + b"\x00" * (PAGE_SIZE - 11)

    def test_01_initial_state(self):
        """测试初始状态"""
        print("测试1: 缓存池初始状态")

        stats = self.buffer_pool.get_statistics()

        self.assertEqual(stats['cache_size'], 0)
        self.assertEqual(stats['cache_capacity'], 10)  # 改为10
        self.assertEqual(stats['hit_rate'], 0)
        self.assertEqual(stats['dirty_pages'], 0)

        print("✓ 初始状态正常")

    def test_02_put_and_get(self):
        """测试基本的放入和获取功能"""
        print("测试2: 基本的放入和获取")

        # 放入数据
        self.buffer_pool.put(1, self.test_data_1, is_dirty=False)

        # 获取数据
        retrieved_data = self.buffer_pool.get(1)

        self.assertIsNotNone(retrieved_data)
        self.assertEqual(retrieved_data, self.test_data_1)

        # 检查统计信息
        stats = self.buffer_pool.get_statistics()
        self.assertEqual(stats['cache_size'], 1)
        self.assertEqual(stats['hit_count'], 1)

        print("✓ 基本放入和获取功能正常")

    def test_03_cache_miss(self):
        """测试缓存未命中"""
        print("测试3: 缓存未命中")

        # 尝试获取不存在的页
        data = self.buffer_pool.get(999)

        self.assertIsNone(data)

        # 检查统计信息
        stats = self.buffer_pool.get_statistics()
        self.assertEqual(stats['miss_count'], 1)
        self.assertEqual(stats['hit_rate'], 0)

        print("✓ 缓存未命中处理正常")

    def test_04_dirty_page_management(self):
        """测试脏页管理"""
        print("测试4: 脏页管理")

        # 添加干净页
        self.buffer_pool.put(1, self.test_data_1, is_dirty=False)

        # 添加脏页
        self.buffer_pool.put(2, self.test_data_2, is_dirty=True)

        # 标记页为脏
        self.buffer_pool.mark_dirty(1)

        # 检查脏页
        dirty_pages = self.buffer_pool.get_dirty_pages()
        self.assertEqual(len(dirty_pages), 2)
        self.assertIn(1, dirty_pages)
        self.assertIn(2, dirty_pages)

        # 清除脏标记
        self.buffer_pool.clear_dirty_flag(1)
        dirty_pages = self.buffer_pool.get_dirty_pages()
        self.assertEqual(len(dirty_pages), 1)
        self.assertIn(2, dirty_pages)

        print("✓ 脏页管理功能正常")

    def test_05_lru_eviction_simple(self):
        """测试简单的LRU淘汰"""
        print("测试5: 简单LRU淘汰")

        # 创建容量为3的小缓存池
        small_buffer = BufferPool(capacity=10)  # 使用最小允许容量

        # 填满缓存
        for i in range(10):
            data = f"Page {i} data".encode() + b"\x00" * (PAGE_SIZE - len(f"Page {i} data"))
            small_buffer.put(i + 1, data, is_dirty=False)

        # 访问一些页，改变LRU顺序
        small_buffer.get(1)  # page 1 成为最近使用的
        small_buffer.get(5)  # page 5 成为最近使用的

        # 再添加一页，应该淘汰最久未使用的页
        new_data = b"New page data" + b"\x00" * (PAGE_SIZE - 13)
        small_buffer.put(11, new_data, is_dirty=False)

        # 检查统计
        stats = small_buffer.get_statistics()
        self.assertEqual(stats['cache_size'], 10)  # 仍然是满的
        self.assertEqual(stats['eviction_count'], 1)  # 发生了一次淘汰

        # 确保最近使用的页仍在缓存中
        self.assertIsNotNone(small_buffer.get(1))
        self.assertIsNotNone(small_buffer.get(5))
        self.assertIsNotNone(small_buffer.get(11))

        print("✓ LRU淘汰功能正常")

    def test_06_flush_operations(self):
        """测试刷新操作"""
        print("测试6: 刷新操作")

        # 添加一些脏页
        self.buffer_pool.put(1, self.test_data_1, is_dirty=True)
        self.buffer_pool.put(2, self.test_data_2, is_dirty=True)
        self.buffer_pool.put(3, self.test_data_3, is_dirty=False)

        # 检查初始脏页数量
        stats = self.buffer_pool.get_statistics()
        self.assertEqual(stats['dirty_pages'], 2)

        # 执行全量刷新
        flushed_pages = self.buffer_pool.flush_all()
        self.assertEqual(len(flushed_pages), 2)
        self.assertIn(1, flushed_pages)
        self.assertIn(2, flushed_pages)

        # 检查刷新后脏页数量
        stats = self.buffer_pool.get_statistics()
        self.assertEqual(stats['dirty_pages'], 0)

        print("✓ 刷新操作正常")


if __name__ == "__main__":
    unittest.main()