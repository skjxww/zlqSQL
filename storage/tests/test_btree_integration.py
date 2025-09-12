"""
B+树索引集成测试
测试索引管理器和B+树的完整功能
"""

import sys
import os
import time
import random
import shutil

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from storage.core.storage_manager import StorageManager
from storage.core.index_manager import IndexManager


class TestBTreeIntegration:
    """B+树集成测试类"""

    def __init__(self):
        self.test_dir = "test_data"
        self.storage = None
        self.index_mgr = None

    def setup(self):
        """测试前准备"""
        # 清理旧的测试数据 - 更安全的清理方式
        if os.path.exists(self.test_dir):
            try:
                # 先尝试删除
                shutil.rmtree(self.test_dir)
            except Exception as e:
                print(f"警告: 无法完全清理测试目录: {e}")
                # 至少创建目录
                os.makedirs(self.test_dir, exist_ok=True)
        else:
            os.makedirs(self.test_dir)

        # 初始化存储管理器 - 禁用自动创建额外表空间
        self.storage = StorageManager(
            buffer_size=100,
            data_file=f"{self.test_dir}/test.db",
            meta_file=f"{self.test_dir}/test_meta.json",
            auto_flush_interval=0,  # 关闭自动刷盘
            enable_extent_management=False  # 关闭区管理以简化测试
        )

        # 创建索引管理器
        self.index_mgr = IndexManager(
            self.storage,
            catalog_file=f"{self.test_dir}/indexes.json"
        )

        print("✅ 测试环境准备完成")

    def teardown(self):
        """测试后清理"""
        try:
            if self.storage:
                # 确保刷新所有脏页
                self.storage.flush_all_pages()
                # 关闭存储管理器
                self.storage.shutdown()
                self.storage = None

            # 给Windows一点时间释放文件句柄
            time.sleep(0.1)

            # 尝试清理测试目录
            if os.path.exists(self.test_dir):
                try:
                    shutil.rmtree(self.test_dir)
                except Exception as e:
                    print(f"警告: 清理测试目录时出错: {e}")

        except Exception as e:
            print(f"清理时出错: {e}")
        finally:
            print("✅ 测试环境清理完成")

    def test_create_index(self):
        """测试创建索引"""
        print("\n=== 测试创建索引 ===")

        # 创建第一个索引
        success = self.index_mgr.create_index(
            index_name="idx_users_id",
            table_name="users",
            column_name="id"
        )
        assert success, "创建索引失败"
        print("✓ 创建索引 idx_users_id")

        # 尝试创建重复索引
        success = self.index_mgr.create_index(
            index_name="idx_users_id",
            table_name="users",
            column_name="id"
        )
        assert not success, "不应该允许创建重复索引"
        print("✓ 正确拒绝重复索引")

        # 创建另一个索引
        success = self.index_mgr.create_index(
            index_name="idx_users_name",
            table_name="users",
            column_name="name"
        )
        assert success, "创建第二个索引失败"
        print("✓ 创建索引 idx_users_name")

        # 列出所有索引
        indexes = self.index_mgr.list_indexes()
        assert len(indexes) == 2, f"应该有2个索引，实际有{len(indexes)}个"
        print(f"✓ 成功创建 {len(indexes)} 个索引")

        for idx in indexes:
            print(f"  - {idx['index_name']}: {idx['table_name']}.{idx['column_name']}")

    def test_insert_and_search(self):
        """测试插入和查询"""
        print("\n=== 测试插入和查询 ===")

        # 创建索引
        self.index_mgr.create_index("idx_test_id", "test_table", "id")

        # 插入测试数据
        test_data = [
            (1, 10, 0),
            (5, 11, 0),
            (3, 12, 0),
            (7, 13, 0),
            (2, 14, 0),
            (9, 15, 0),
            (4, 16, 0),
            (6, 17, 0),
            (8, 18, 0),
            (10, 19, 0),
        ]

        print("插入测试数据...")
        for key, page_id, slot_id in test_data:
            success = self.index_mgr.insert_into_index(
                table_name="test_table",
                column_name="id",
                key=key,
                page_id=page_id,
                slot_id=slot_id
            )
            assert success, f"插入键 {key} 失败"
        print(f"✓ 成功插入 {len(test_data)} 条数据")

        # 查询测试
        print("查询测试...")
        for key, expected_page, expected_slot in test_data:
            result = self.index_mgr.search_index(
                table_name="test_table",
                column_name="id",
                key=key
            )
            assert result is not None, f"未找到键 {key}"
            page_id, slot_id = result
            assert page_id == expected_page and slot_id == expected_slot, \
                f"键 {key} 的值不匹配：期望({expected_page},{expected_slot})，实际({page_id},{slot_id})"
        print("✓ 所有查询都返回正确结果")

        # 查询不存在的键
        result = self.index_mgr.search_index(
            table_name="test_table",
            column_name="id",
            key=999
        )
        assert result is None, "不应该找到不存在的键"
        print("✓ 正确处理不存在的键")

    def test_range_search(self):
        """测试范围查询"""
        print("\n=== 测试范围查询 ===")

        # 创建索引并插入数据
        self.index_mgr.create_index("idx_range_test", "range_table", "value")

        # 插入连续数据
        for i in range(1, 21):
            self.index_mgr.insert_into_index(
                table_name="range_table",
                column_name="value",
                key=i,
                page_id=100 + i,
                slot_id=i % 10
            )

        # 测试不同的范围查询
        test_cases = [
            (5, 10, 6),   # [5, 10] 应该有6个结果
            (1, 5, 5),    # [1, 5] 应该有5个结果
            (15, 20, 6),  # [15, 20] 应该有6个结果
            (10, 10, 1),  # [10, 10] 应该有1个结果
            (25, 30, 0),  # [25, 30] 应该有0个结果
        ]

        for start, end, expected_count in test_cases:
            results = self.index_mgr.range_search_index(
                table_name="range_table",
                column_name="value",
                start_key=start,
                end_key=end
            )
            assert len(results) == expected_count, \
                f"范围[{start},{end}]应该有{expected_count}个结果，实际有{len(results)}个"

            # 验证结果的正确性
            for key, (page_id, slot_id) in results:
                assert start <= key <= end, f"键 {key} 不在范围 [{start},{end}] 内"

            print(f"✓ 范围查询 [{start},{end}] 返回 {len(results)} 条记录")

    def test_performance(self):
        """性能测试"""
        print("\n=== 性能测试 ===")

        # 创建索引
        self.index_mgr.create_index("idx_perf", "perf_table", "id")

        # 批量插入测试
        n = 5000
        print(f"插入 {n} 条数据...")

        start_time = time.time()
        for i in range(n):
            self.index_mgr.insert_into_index(
                table_name="perf_table",
                column_name="id",
                key=i,
                page_id=i // 100,
                slot_id=i % 100
            )
        insert_time = time.time() - start_time

        print(f"✓ 插入耗时: {insert_time:.2f}秒")
        print(f"  平均每条: {insert_time/n*1000:.3f}毫秒")

        # 随机查询测试
        query_count = 1000
        print(f"执行 {query_count} 次随机查询...")

        start_time = time.time()
        for _ in range(query_count):
            key = random.randint(0, n-1)
            result = self.index_mgr.search_index(
                table_name="perf_table",
                column_name="id",
                key=key
            )
            assert result is not None, f"未找到键 {key}"
        query_time = time.time() - start_time

        print(f"✓ 查询耗时: {query_time:.2f}秒")
        print(f"  平均每次: {query_time/query_count*1000:.3f}毫秒")

        # 范围查询测试
        print("执行范围查询...")

        start_time = time.time()
        results = self.index_mgr.range_search_index(
            table_name="perf_table",
            column_name="id",
            start_key=1000,
            end_key=2000
        )
        range_time = time.time() - start_time

        print(f"✓ 范围查询 [1000,2000] 耗时: {range_time*1000:.2f}毫秒")
        print(f"  返回 {len(results)} 条记录")

        # 获取缓存统计
        cache_stats = self.storage.get_cache_stats()
        print(f"\n缓存统计:")
        print(f"  命中率: {cache_stats['hit_rate']}%")
        print(f"  缓存大小: {cache_stats['cache_size']}/{cache_stats['cache_capacity']}")
        print(f"  脏页数: {cache_stats['dirty_pages']}")

    def test_persistence(self):
        """测试持久化"""
        print("\n=== 测试持久化 ===")

        # 创建索引并插入数据
        self.index_mgr.create_index("idx_persist", "persist_table", "id")

        test_data = [(i, i*10, i%5) for i in range(10)]
        for key, page_id, slot_id in test_data:
            self.index_mgr.insert_into_index(
                table_name="persist_table",
                column_name="id",
                key=key,
                page_id=page_id,
                slot_id=slot_id
            )
        print(f"✓ 插入 {len(test_data)} 条测试数据")

        # 关闭存储管理器
        self.storage.shutdown()
        self.storage = None  # 确保释放引用
        time.sleep(0.1)  # 给Windows时间释放文件
        print("✓ 关闭存储管理器")

        # 重新打开
        self.storage = StorageManager(
            buffer_size=100,
            data_file=f"{self.test_dir}/test.db",
            meta_file=f"{self.test_dir}/test_meta.json",
            auto_flush_interval=0,
            enable_extent_management=False
        )
        self.index_mgr = IndexManager(
            self.storage,
            catalog_file=f"{self.test_dir}/indexes.json"
        )
        print("✓ 重新打开存储管理器")

        # 验证索引还在
        indexes = self.index_mgr.list_indexes()
        assert any(idx['index_name'] == 'idx_persist' for idx in indexes), \
            "索引元数据丢失"
        print("✓ 索引元数据正确持久化")

        # 验证数据还在
        for key, expected_page, expected_slot in test_data:
            result = self.index_mgr.search_index(
                table_name="persist_table",
                column_name="id",
                key=key
            )
            assert result is not None, f"键 {key} 丢失"
            page_id, slot_id = result
            assert page_id == expected_page and slot_id == expected_slot, \
                f"键 {key} 的值不正确"
        print("✓ 索引数据正确持久化")

    def run_all_tests(self):
        """运行所有测试"""
        print("\n" + "="*50)
        print("B+树索引集成测试")
        print("="*50)

        test_methods = [
            self.test_create_index,
            self.test_insert_and_search,
            self.test_range_search,
            self.test_performance,
            self.test_persistence,
        ]

        passed = 0
        failed = 0

        for test_method in test_methods:
            try:
                self.setup()
                test_method()
                passed += 1
            except AssertionError as e:
                print(f"❌ 测试失败: {e}")
                failed += 1
            except Exception as e:
                print(f"❌ 测试出错: {e}")
                import traceback
                traceback.print_exc()
                failed += 1
            finally:
                self.teardown()

        print("\n" + "="*50)
        print(f"测试完成: {passed} 通过, {failed} 失败")
        print("="*50)

        return failed == 0


if __name__ == "__main__":
    # 确保从干净状态开始
    if os.path.exists("test_data"):
        try:
            shutil.rmtree("test_data")
        except:
            pass

    tester = TestBTreeIntegration()
    success = tester.run_all_tests()

    # 最终清理
    if os.path.exists("test_data"):
        try:
            shutil.rmtree("test_data")
        except:
            pass

    if success:
        print("\n🎉 所有测试通过！")
        sys.exit(0)
    else:
        print("\n❌ 有测试失败")
        sys.exit(1)