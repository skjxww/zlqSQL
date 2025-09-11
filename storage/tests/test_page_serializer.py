"""
页序列化器测试
测试页内数据布局管理和空间分配
"""

import unittest
import sys
import os

# 导入待测试的模块
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from storage.utils.serializer import PageSerializer, RecordSerializer
from storage.utils.exceptions import SerializationException
from storage.utils.constants import PAGE_SIZE


class TestPageSerializer(unittest.TestCase):
    """页序列化器测试类"""

    def setUp(self):
        """测试前准备"""
        # 准备测试数据块
        self.test_data_1 = b"First data block for testing page serialization"
        self.test_data_2 = b"Second block with different content length here"
        self.test_data_3 = b"Third block - shorter"

        # 准备用于记录测试的模式
        self.test_schema = [
            ("id", "INT", None),
            ("name", "VARCHAR", 20),
            ("value", "INT", None)
        ]

    def test_01_create_empty_page(self):
        """测试创建空页"""
        print("测试1: 创建空页")

        # 创建空页
        empty_page = PageSerializer.create_empty_page()

        # 检查页大小
        self.assertEqual(len(empty_page), PAGE_SIZE)

        # 检查页信息
        page_info = PageSerializer.get_page_info(empty_page)
        self.assertEqual(page_info['record_count'], 0)
        self.assertEqual(page_info['free_space_start'], PageSerializer.PAGE_HEADER_SIZE)
        self.assertEqual(page_info['next_page_id'], 0)
        self.assertEqual(page_info['free_space_size'], PAGE_SIZE - PageSerializer.PAGE_HEADER_SIZE)

        print(f"✓ 空页创建正常，可用空间: {page_info['free_space_size']} 字节")

    def test_02_add_single_data_block(self):
        """测试添加单个数据块"""
        print("测试2: 添加单个数据块")

        # 创建空页
        page = PageSerializer.create_empty_page()

        # 添加数据块
        new_page, success = PageSerializer.add_data_to_page(page, self.test_data_1)
        self.assertTrue(success)

        # 检查页信息
        page_info = PageSerializer.get_page_info(new_page)
        self.assertEqual(page_info['record_count'], 1)

        # 计算预期的空闲空间起始位置
        expected_start = (PageSerializer.PAGE_HEADER_SIZE +
                          4 +  # 偏移表 (1个记录 * 4字节)
                          len(self.test_data_1))  # 数据块大小
        self.assertEqual(page_info['free_space_start'], expected_start)

        print(f"✓ 数据块添加成功，记录数: {page_info['record_count']}")

    def test_03_add_multiple_data_blocks(self):
        """测试添加多个数据块"""
        print("测试3: 添加多个数据块")

        page = PageSerializer.create_empty_page()
        test_blocks = [self.test_data_1, self.test_data_2, self.test_data_3]

        # 依次添加数据块
        for i, data_block in enumerate(test_blocks):
            page, success = PageSerializer.add_data_to_page(page, data_block)
            self.assertTrue(success, f"Failed to add block {i}")

            # 检查记录数
            page_info = PageSerializer.get_page_info(page)
            self.assertEqual(page_info['record_count'], i + 1)

        # 最终检查
        final_info = PageSerializer.get_page_info(page)
        self.assertEqual(final_info['record_count'], 3)

        print(f"✓ 成功添加 {len(test_blocks)} 个数据块")

    def test_04_retrieve_data_blocks(self):
        """测试提取数据块"""
        print("测试4: 提取数据块")

        # 构建包含多个数据块的页
        page = PageSerializer.create_empty_page()
        original_blocks = [self.test_data_1, self.test_data_2, self.test_data_3]

        for data_block in original_blocks:
            page, success = PageSerializer.add_data_to_page(page, data_block)
            self.assertTrue(success)

        # 提取数据块
        retrieved_blocks = PageSerializer.get_data_blocks_from_page(page)

        # 验证数量和内容
        self.assertEqual(len(retrieved_blocks), len(original_blocks))

        for i, (original, retrieved) in enumerate(zip(original_blocks, retrieved_blocks)):
            self.assertEqual(original, retrieved, f"Block {i} mismatch")

        print(f"✓ 成功提取 {len(retrieved_blocks)} 个数据块")

    def test_05_page_space_management(self):
        """测试页面空间管理"""
        print("测试5: 页面空间管理")

        page = PageSerializer.create_empty_page()

        # 记录初始可用空间
        initial_info = PageSerializer.get_page_info(page)
        initial_free_space = initial_info['free_space_size']

        # 添加数据块
        data_block = b"Space management test data"
        page, success = PageSerializer.add_data_to_page(page, data_block)
        self.assertTrue(success)

        # 检查空间使用
        after_info = PageSerializer.get_page_info(page)
        used_space = (4 +  # 偏移表条目
                      len(data_block))  # 数据块大小

        expected_free_space = initial_free_space - used_space
        self.assertEqual(after_info['free_space_size'], expected_free_space)

        # 获取页面利用率统计
        utilization = PageSerializer.get_page_utilization(page)
        self.assertGreater(utilization['utilization_ratio'], 0)
        self.assertLess(utilization['utilization_ratio'], 1)

        print(f"✓ 空间管理正常，利用率: {utilization['utilization_ratio']:.2%}")

    def test_06_page_full_handling(self):
        """测试页满处理"""
        print("测试6: 页满处理")

        page = PageSerializer.create_empty_page()

        # 创建大数据块填满页面
        # 计算能放入的最大数据块大小
        initial_info = PageSerializer.get_page_info(page)
        available_space = initial_info['free_space_size']

        # 尝试添加一个几乎填满页面的数据块
        large_data = b"X" * (available_space - 100)  # 留100字节给偏移表
        page, success = PageSerializer.add_data_to_page(page, large_data)
        self.assertTrue(success)

        # 现在尝试添加另一个数据块，应该失败（空间不足）
        another_data = b"This should not fit" * 10  # 故意很大的数据
        page, success = PageSerializer.add_data_to_page(page, another_data)
        self.assertFalse(success)  # 应该失败

        # 页面记录数应该仍然是1
        page_info = PageSerializer.get_page_info(page)
        self.assertEqual(page_info['record_count'], 1)

        print("✓ 页满处理正常")

    def test_07_remove_data_from_page(self):
        """测试从页面移除数据"""
        print("测试7: 从页面移除数据")

        # 构建包含多个数据块的页
        page = PageSerializer.create_empty_page()
        test_blocks = [self.test_data_1, self.test_data_2, self.test_data_3]

        for data_block in test_blocks:
            page, success = PageSerializer.add_data_to_page(page, data_block)
            self.assertTrue(success)

        # 移除中间的数据块（索引1）
        page, success = PageSerializer.remove_data_from_page(page, 1)
        self.assertTrue(success)

        # 检查记录数减少
        page_info = PageSerializer.get_page_info(page)
        self.assertEqual(page_info['record_count'], 2)

        # 检查剩余的数据块
        remaining_blocks = PageSerializer.get_data_blocks_from_page(page)
        self.assertEqual(len(remaining_blocks), 2)
        self.assertEqual(remaining_blocks[0], self.test_data_1)
        self.assertEqual(remaining_blocks[1], self.test_data_3)  # 第二个数据块被移除了

        print("✓ 数据移除功能正常")

    def test_08_record_level_operations(self):
        """测试记录级操作"""
        print("测试8: 记录级操作")

        # 准备测试记录
        test_records = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 300}
        ]

        page = PageSerializer.create_empty_page()

        # 序列化记录并添加到页面
        for record in test_records:
            serialized_record = RecordSerializer.serialize_record(record, self.test_schema)
            page, success = PageSerializer.add_record_to_page(page, serialized_record)
            self.assertTrue(success, f"Failed to add record: {record}")

        # 从页面获取所有记录
        retrieved_records = PageSerializer.get_records_from_page(page, self.test_schema)

        # 验证记录数量和内容
        self.assertEqual(len(retrieved_records), len(test_records))

        for original, retrieved in zip(test_records, retrieved_records):
            self.assertEqual(retrieved["id"], original["id"])
            self.assertEqual(retrieved["name"], original["name"])
            self.assertEqual(retrieved["value"], original["value"])

        print(f"✓ 记录级操作正常，处理了 {len(test_records)} 条记录")

    def test_09_page_operations_integration(self):
        """测试页面操作集成功能"""
        print("测试9: 页面操作集成")

        # 创建页面并添加数据
        page = PageSerializer.create_empty_page()
        original_blocks = [self.test_data_1, self.test_data_2, self.test_data_3]

        # 添加所有数据块
        for data_block in original_blocks:
            page, success = PageSerializer.add_data_to_page(page, data_block)
            self.assertTrue(success)

        # 获取页面利用率信息
        utilization = PageSerializer.get_page_utilization(page)

        # 验证利用率信息的合理性
        self.assertGreater(utilization['utilization_ratio'], 0)
        self.assertLess(utilization['utilization_ratio'], 1)
        self.assertEqual(utilization['total_size'], PAGE_SIZE)
        self.assertGreater(utilization['data_size'], 0)
        self.assertGreater(utilization['offset_table_size'], 0)

        # 验证各部分大小加起来等于总大小
        total_used = (utilization['header_size'] +
                      utilization['offset_table_size'] +
                      utilization['data_size'] +
                      utilization['free_space'])
        self.assertEqual(total_used, PAGE_SIZE)

        # 测试页面信息的一致性
        page_info = PageSerializer.get_page_info(page)
        self.assertEqual(page_info['record_count'], len(original_blocks))

        # 验证数据完整性
        retrieved_blocks = PageSerializer.get_data_blocks_from_page(page)
        self.assertEqual(len(retrieved_blocks), len(original_blocks))

        for original, retrieved in zip(original_blocks, retrieved_blocks):
            self.assertEqual(original, retrieved)

        print(f"✓ 页面集成操作正常，利用率: {utilization['utilization_ratio']:.2%}")


    def test_10_edge_cases_and_errors(self):
        """测试边界情况和错误处理"""
        print("测试10: 边界情况和错误处理")

        # 测试空数据块
        page = PageSerializer.create_empty_page()
        empty_data = b""
        page, success = PageSerializer.add_data_to_page(page, empty_data)
        self.assertTrue(success)  # 空数据应该能添加

        # 测试非常大的数据块
        huge_data = b"X" * (PAGE_SIZE + 1000)  # 超过页面大小
        page, success = PageSerializer.add_data_to_page(page, huge_data)
        self.assertFalse(success)  # 应该失败

        # 测试无效页数据
        invalid_page_data = b"invalid"
        with self.assertRaises(SerializationException):
            PageSerializer.get_page_info(invalid_page_data)

        # 测试移除不存在的数据块索引
        page = PageSerializer.create_empty_page()
        page, success = PageSerializer.add_data_to_page(page, self.test_data_1)

        # 尝试移除不存在的索引
        page, success = PageSerializer.remove_data_from_page(page, 999)
        self.assertFalse(success)

        print("✓ 边界情况和错误处理正常")


if __name__ == "__main__":
    unittest.main()