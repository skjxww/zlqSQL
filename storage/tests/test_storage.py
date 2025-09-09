"""
存储系统综合测试
包含所有组件的单元测试和集成测试
"""

import unittest
import tempfile
import shutil
import os

# 导入要测试的模块
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.core.buffer_pool import BufferPool
from storage.core.page_manager import PageManager
from storage.core.storage_manager import StorageManager
from storage.core.table_storage import TableManager
from storage.utils.exceptions import *
from storage.utils.serializer import RecordSerializer
from storage.utils.constants import PAGE_SIZE


class TestBufferPool(unittest.TestCase):
    """缓存池测试"""

    def setUp(self):
        """测试准备"""
        self.buffer_pool = BufferPool(capacity=3)

    def test_basic_operations(self):
        """测试基本操作"""
        # 测试缓存未命中
        self.assertIsNone(self.buffer_pool.get(1))

        # 测试写入和读取
        test_data = b"test data"
        self.buffer_pool.put(1, test_data)
        self.assertEqual(self.buffer_pool.get(1), test_data)

        # 测试统计信息
        stats = self.buffer_pool.get_statistics()
        self.assertEqual(stats["total_requests"], 2)  # 一次miss，一次hit
        self.assertEqual(stats["hit_count"], 1)
        self.assertEqual(stats["cache_size"], 1)

    def test_lru_eviction(self):
        """测试LRU淘汰"""
        # 填满缓存
        for i in range(3):
            self.buffer_pool.put(i, f"data_{i}".encode())

        # 访问页0，使其成为最近使用的
        self.buffer_pool.get(0)

        # 添加新页，应该淘汰页1（最久未使用）
        self.buffer_pool.put(3, b"data_3")

        # 页0和2应该还在，页1应该被淘汰
        self.assertIsNotNone(self.buffer_pool.get(0))
        self.assertIsNone(self.buffer_pool.get(1))  # 被淘汰
        self.assertIsNotNone(self.buffer_pool.get(2))
        self.assertIsNotNone(self.buffer_pool.get(3))

    def test_dirty_pages(self):
        """测试脏页管理"""
        # 添加脏页
        self.buffer_pool.put(1, b"dirty_data", is_dirty=True)

        # 检查脏页
        dirty_pages = self.buffer_pool.get_dirty_pages()
        self.assertEqual(len(dirty_pages), 1)
        self.assertIn(1, dirty_pages)

        # 清除脏标记
        self.buffer_pool.clear_dirty_flag(1)
        dirty_pages = self.buffer_pool.get_dirty_pages()
        self.assertEqual(len(dirty_pages), 0)


class TestPageManager(unittest.TestCase):
    """页管理器测试"""

    def setUp(self):
        """测试准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.data_file = os.path.join(self.temp_dir, "test.db")
        self.meta_file = os.path.join(self.temp_dir, "test_meta.json")
        self.page_manager = PageManager(self.data_file, self.meta_file)

    def tearDown(self):
        """清理"""
        self.page_manager.cleanup()
        shutil.rmtree(self.temp_dir)

    def test_page_allocation(self):
        """测试页分配"""
        # 分配页
        page_id1 = self.page_manager.allocate_page()
        page_id2 = self.page_manager.allocate_page()

        self.assertEqual(page_id1, 1)
        self.assertEqual(page_id2, 2)

        # 检查页是否已分配
        self.assertTrue(self.page_manager.is_page_allocated(page_id1))
        self.assertTrue(self.page_manager.is_page_allocated(page_id2))

    def test_page_deallocation(self):
        """测试页释放"""
        # 分配并释放页
        page_id = self.page_manager.allocate_page()
        self.page_manager.deallocate_page(page_id)

        # 检查页是否已释放
        self.assertFalse(self.page_manager.is_page_allocated(page_id))

        # 再次分配应该重用该页号
        new_page_id = self.page_manager.allocate_page()
        self.assertEqual(new_page_id, page_id)

    def test_page_io(self):
        """测试页读写"""
        page_id = self.page_manager.allocate_page()
        test_data = b"x" * PAGE_SIZE

        # 写入页
        self.page_manager.write_page_to_disk(page_id, test_data)

        # 读取页
        read_data = self.page_manager.read_page_from_disk(page_id)
        self.assertEqual(read_data, test_data)

    def test_invalid_operations(self):
        """测试无效操作"""
        # 测试释放未分配的页
        with self.assertRaises(PageNotAllocatedException):
            self.page_manager.deallocate_page(999)

        # 测试无效页号
        with self.assertRaises(InvalidPageIdException):
            self.page_manager.read_page_from_disk(-1)


class TestStorageManager(unittest.TestCase):
    """存储管理器测试"""

    def setUp(self):
        """测试准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.data_file = os.path.join(self.temp_dir, "test.db")
        self.meta_file = os.path.join(self.temp_dir, "test_meta.json")
        self.storage_manager = StorageManager(
            buffer_size=5,
            data_file=self.data_file,
            meta_file=self.meta_file,
            auto_flush_interval=0  # 禁用自动刷盘
        )

    def tearDown(self):
        """清理"""
        self.storage_manager.shutdown()
        shutil.rmtree(self.temp_dir)

    def test_basic_operations(self):
        """测试基本操作"""
        # 分配页
        page_id = self.storage_manager.allocate_page()
        self.assertIsInstance(page_id, int)
        self.assertGreater(page_id, 0)

        # 写入数据
        test_data = b"test data" + b"\x00" * (PAGE_SIZE - 9)
        self.storage_manager.write_page(page_id, test_data)

        # 读取数据
        read_data = self.storage_manager.read_page(page_id)
        self.assertEqual(read_data, test_data)

    def test_cache_integration(self):
        """测试缓存集成"""
        page_id = self.storage_manager.allocate_page()
        test_data = b"x" * PAGE_SIZE

        # 写入页（应该在缓存中）
        self.storage_manager.write_page(page_id, test_data)

        # 第一次读取（缓存命中）
        read_data1 = self.storage_manager.read_page(page_id)
        self.assertEqual(read_data1, test_data)

        # 检查缓存统计
        stats = self.storage_manager.get_cache_stats()
        self.assertGreater(stats["hit_rate"], 0)

    def test_transaction_context(self):
        """测试事务上下文"""
        page_id = self.storage_manager.allocate_page()
        test_data = b"x" * PAGE_SIZE

        with self.storage_manager.transaction():
            self.storage_manager.write_page(page_id, test_data)
            # 事务结束时应该自动刷盘

        # 验证数据已持久化
        read_data = self.storage_manager.read_page(page_id)
        self.assertEqual(read_data, test_data)


class TestRecordSerializer(unittest.TestCase):
    """记录序列化测试"""

    def setUp(self):
        """测试准备"""
        self.schema = [
            ("id", "INT", None),
            ("name", "VARCHAR", 50),
            ("age", "INT", None),
            ("active", "BOOLEAN", None)
        ]

    def test_serialize_deserialize(self):
        """测试序列化和反序列化"""
        record = {
            "id": 1,
            "name": "Alice",
            "age": 25,
            "active": True
        }

        # 序列化
        serialized = RecordSerializer.serialize_record(record, self.schema)
        self.assertIsInstance(serialized, bytes)

        # 反序列化
        deserialized = RecordSerializer.deserialize_record(serialized, self.schema)
        self.assertEqual(deserialized["id"], 1)
        self.assertEqual(deserialized["name"], "Alice")
        self.assertEqual(deserialized["age"], 25)
        self.assertEqual(deserialized["active"], True)

    def test_null_values(self):
        """测试NULL值处理"""
        record = {
            "id": 1,
            "name": None,
            "age": None,
            "active": False
        }

        serialized = RecordSerializer.serialize_record(record, self.schema)
        deserialized = RecordSerializer.deserialize_record(serialized, self.schema)

        self.assertEqual(deserialized["id"], 1)
        self.assertIsNone(deserialized["name"])
        self.assertIsNone(deserialized["age"])
        self.assertEqual(deserialized["active"], False)


class TestTableManager(unittest.TestCase):
    """表管理器测试"""

    def setUp(self):
        """测试准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.data_file = os.path.join(self.temp_dir, "test.db")
        self.meta_file = os.path.join(self.temp_dir, "test_meta.json")
        self.catalog_file = os.path.join(self.temp_dir, "test_catalog.json")

        self.storage_manager = StorageManager(
            buffer_size=10,
            data_file=self.data_file,
            meta_file=self.meta_file,
            auto_flush_interval=0
        )
        self.table_manager = TableManager(self.storage_manager, self.catalog_file)

    def tearDown(self):
        """清理"""
        self.table_manager.shutdown()
        self.storage_manager.shutdown()
        shutil.rmtree(self.temp_dir)

    def test_create_table(self):
        """测试创建表"""
        schema = [
            ("id", "INT", None),
            ("name", "VARCHAR", 50),
            ("age", "INT", None)
        ]

        # 创建表
        success = self.table_manager.create_table("users", schema)
        self.assertTrue(success)

        # 检查表是否存在
        self.assertTrue(self.table_manager.table_exists("users"))

        # 获取表信息
        table_info = self.table_manager.get_table_info("users")
        self.assertEqual(table_info["name"], "users")
        self.assertEqual(len(table_info["schema"]), 3)

    def test_duplicate_table(self):
        """测试重复创建表"""
        schema = [("id", "INT", None)]

        # 创建表
        self.table_manager.create_table("test", schema)

        # 尝试重复创建
        with self.assertRaises(TableAlreadyExistsException):
            self.table_manager.create_table("test", schema)

    def test_insert_record(self):
        """测试插入记录"""
        schema = [
            ("id", "INT", None),
            ("name", "VARCHAR", 50)
        ]

        self.table_manager.create_table("test", schema)

        # 插入记录
        record = {"id": 1, "name": "Alice"}
        success = self.table_manager.insert_record("test", record)
        self.assertTrue(success)

        # 验证记录
        records = list(self.table_manager.scan_table("test"))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 1)
        self.assertEqual(records[0]["name"], "Alice")

    def test_scan_with_condition(self):
        """测试条件查询"""
        schema = [
            ("id", "INT", None),
            ("age", "INT", None)
        ]

        self.table_manager.create_table("test", schema)

        # 插入多条记录
        records = [
            {"id": 1, "age": 20},
            {"id": 2, "age": 25},
            {"id": 3, "age": 30}
        ]

        for record in records:
            self.table_manager.insert_record("test", record)

        # 条件查询
        def age_filter(record):
            return record.get("age", 0) > 22

        filtered_records = list(self.table_manager.scan_table("test", age_filter))
        self.assertEqual(len(filtered_records), 2)  # age > 22

    def test_delete_records(self):
        """测试删除记录"""
        schema = [("id", "INT", None), ("name", "VARCHAR", 20)]
        self.table_manager.create_table("test", schema)

        # 插入记录
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"}
        ]

        for record in records:
            self.table_manager.insert_record("test", record)

        # 删除记录
        def delete_condition(record):
            return record.get("id") == 2

        deleted_count = self.table_manager.delete_records("test", delete_condition)
        self.assertEqual(deleted_count, 1)

        # 验证删除结果
        remaining_records = list(self.table_manager.scan_table("test"))
        self.assertEqual(len(remaining_records), 2)

        # 确认被删除的记录不存在
        ids = [record["id"] for record in remaining_records]
        self.assertNotIn(2, ids)

    def test_drop_table(self):
        """测试删除表"""
        schema = [("id", "INT", None)]

        # 创建并删除表
        self.table_manager.create_table("temp", schema)
        self.assertTrue(self.table_manager.table_exists("temp"))

        success = self.table_manager.drop_table("temp")
        self.assertTrue(success)
        self.assertFalse(self.table_manager.table_exists("temp"))


class TestSchemaManager(unittest.TestCase):
    """模式管理器测试"""

    def setUp(self):
        """测试准备"""
        self.schema_manager = SchemaManager()

    def test_validate_table_name(self):
        """测试表名验证"""
        # 有效表名
        valid, errors = self.schema_manager.validate_table_name("users")
        self.assertTrue(valid)
        self.assertEqual(len(errors), 0)

        # 无效表名
        invalid_names = ["", "123invalid", "user-name", "select"]

        for name in invalid_names:
            valid, errors = self.schema_manager.validate_table_name(name)
            self.assertFalse(valid)
            self.assertGreater(len(errors), 0)

    def test_validate_schema(self):
        """测试模式验证"""
        # 有效模式
        valid_schema = [
            ("id", "INT", None),
            ("name", "VARCHAR", 50),
            ("age", "INT", None)
        ]

        valid, errors = self.schema_manager.validate_schema(valid_schema)
        self.assertTrue(valid)
        self.assertEqual(len(errors), 0)

        # 无效模式：重复列名
        invalid_schema = [
            ("id", "INT", None),
            ("id", "VARCHAR", 50)  # 重复列名
        ]

        valid, errors = self.schema_manager.validate_schema(invalid_schema)
        self.assertFalse(valid)
        self.assertGreater(len(errors), 0)

    def test_compare_schemas(self):
        """测试模式比较"""
        schema1 = [
            ("id", "INT", None),
            ("name", "VARCHAR", 50)
        ]

        schema2 = [
            ("id", "INT", None),
            ("name", "VARCHAR", 100),  # 长度改变
            ("email", "VARCHAR", 255)  # 新增列
        ]

        result = self.schema_manager.compare_schemas(schema1, schema2)

        self.assertFalse(result["are_identical"])
        self.assertEqual(len(result["added_columns"]), 1)
        self.assertEqual(len(result["modified_columns"]), 1)
        self.assertEqual(result["added_columns"][0]["name"], "email")


class TestIntegration(unittest.TestCase):
    """集成测试"""

    def setUp(self):
        """测试准备"""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """清理"""
        shutil.rmtree(self.temp_dir)

    def test_complete_workflow(self):
        """测试完整工作流程"""
        # 创建存储系统
        data_file = os.path.join(self.temp_dir, "integration.db")
        meta_file = os.path.join(self.temp_dir, "integration_meta.json")
        catalog_file = os.path.join(self.temp_dir, "integration_catalog.json")

        storage_manager = StorageManager(
            buffer_size=20,
            data_file=data_file,
            meta_file=meta_file,
            auto_flush_interval=0
        )
        table_manager = TableManager(storage_manager, catalog_file)

        try:
            # 1. 创建表
            student_schema = [
                ("id", "INT", None),
                ("name", "VARCHAR", 50),
                ("age", "INT", None),
                ("gpa", "FLOAT", None),
                ("active", "BOOLEAN", None)
            ]

            success = table_manager.create_table("students", student_schema)
            self.assertTrue(success)

            # 2. 插入测试数据
            students = [
                {"id": 1, "name": "Alice", "age": 20, "gpa": 3.8, "active": True},
                {"id": 2, "name": "Bob", "age": 22, "gpa": 3.2, "active": True},
                {"id": 3, "name": "Charlie", "age": 19, "gpa": 3.9, "active": False},
                {"id": 4, "name": "Diana", "age": 21, "gpa": 3.5, "active": True},
                {"id": 5, "name": "Eve", "age": 20, "gpa": 3.7, "active": True}
            ]

            for student in students:
                success = table_manager.insert_record("students", student)
                self.assertTrue(success)

            # 3. 查询所有记录
            all_students = list(table_manager.scan_table("students"))
            self.assertEqual(len(all_students), 5)

            # 4. 条件查询
            # 查询GPA > 3.5的学生
            high_gpa_students = list(table_manager.scan_table(
                "students",
                lambda r: r.get("gpa", 0) > 3.5
            ))
            self.assertEqual(len(high_gpa_students), 3)

            # 查询活跃学生
            active_students = list(table_manager.scan_table(
                "students",
                lambda r: r.get("active", False) == True
            ))
            self.assertEqual(len(active_students), 4)

            # 5. 删除记录
            # 删除非活跃学生
            deleted_count = table_manager.delete_records(
                "students",
                lambda r: r.get("active", False) == False
            )
            self.assertEqual(deleted_count, 1)

            # 验证删除结果
            remaining_students = list(table_manager.scan_table("students"))
            self.assertEqual(len(remaining_students), 4)

            # 6. 获取统计信息
            table_stats = table_manager.get_statistics()
            self.assertEqual(table_stats["table_count"], 1)
            self.assertEqual(table_stats["tables"]["students"]["record_count"], 4)

            cache_stats = storage_manager.get_cache_stats()
            self.assertGreater(cache_stats["total_requests"], 0)

            # 7. 测试持久化
            # 刷新所有数据到磁盘
            flushed_pages = storage_manager.flush_all_pages()
            self.assertGreaterEqual(flushed_pages, 0)

        finally:
            # 清理
            table_manager.shutdown()
            storage_manager.shutdown()

    def test_stress_operations(self):
        """压力测试"""
        data_file = os.path.join(self.temp_dir, "stress.db")
        meta_file = os.path.join(self.temp_dir, "stress_meta.json")
        catalog_file = os.path.join(self.temp_dir, "stress_catalog.json")

        storage_manager = StorageManager(
            buffer_size=50,
            data_file=data_file,
            meta_file=meta_file,
            auto_flush_interval=0
        )
        table_manager = TableManager(storage_manager, catalog_file)

        try:
            # 创建表
            schema = [
                ("id", "INT", None),
                ("data", "VARCHAR", 100)
            ]

            table_manager.create_table("stress_test", schema)

            # 插入大量记录
            record_count = 1000
            for i in range(record_count):
                record = {
                    "id": i,
                    "data": f"test_data_{i:04d}"
                }
                success = table_manager.insert_record("stress_test", record)
                self.assertTrue(success)

            # 验证记录数
            all_records = list(table_manager.scan_table("stress_test"))
            self.assertEqual(len(all_records), record_count)

            # 随机查询测试
            import random
            random.seed(42)

            for _ in range(100):
                target_id = random.randint(0, record_count - 1)
                found_records = list(table_manager.scan_table(
                    "stress_test",
                    lambda r: r.get("id") == target_id
                ))
                self.assertEqual(len(found_records), 1)
                self.assertEqual(found_records[0]["id"], target_id)

        finally:
            table_manager.shutdown()
            storage_manager.shutdown()


def run_all_tests():
    """运行所有测试"""
    # 创建测试套件
    test_suite = unittest.TestSuite()

    # 添加测试类
    test_classes = [
        TestBufferPool,
        TestPageManager,
        TestStorageManager,
        TestRecordSerializer,
        TestTableManager,
        TestSchemaManager,
        TestIntegration
    ]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # 输出结果摘要
    print(f"\n{'=' * 50}")
    print(f"测试结果摘要:")
    print(f"总测试数: {result.testsRun}")
    print(f"失败数: {len(result.failures)}")
    print(f"错误数: {len(result.errors)}")
    print(f"跳过数: {len(result.skipped)}")
    print(f"成功率: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    print(f"{'=' * 50}")

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)