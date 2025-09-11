"""
修复版集成测试 - 根据实际实现调整参数
"""

import os
import sys
import time
import shutil
import random
import threading
from typing import List, Dict, Any
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from storage.core.storage_manager import StorageManager, create_storage_manager
from storage.core.table_storage import TableStorage
from storage.core.page_manager import PageManager
from storage.core.buffer_pool import BufferPool
from storage.utils.constants import PAGE_SIZE, BUFFER_SIZE, MIN_CACHE_SIZE, MAX_CACHE_SIZE
from storage.utils.serializer import RecordSerializer, PageSerializer, SchemaSerializer
from storage.utils.exceptions import *
from storage.utils.logger import get_logger


class FixedStorageSystemTester:
    """修复版存储系统测试器"""

    def __init__(self, test_data_dir="fixed_test_data"):
        self.test_data_dir = Path(test_data_dir)
        self.logger = get_logger("test")
        self.test_results = {
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'test_details': []
        }

        self._setup_test_environment()

    def _setup_test_environment(self):
        if self.test_data_dir.exists():
            shutil.rmtree(self.test_data_dir)
        self.test_data_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Test environment setup completed at {self.test_data_dir}")

    def run_test(self, test_name: str, test_func):
        self.test_results['total_tests'] += 1

        print(f"\n{'=' * 60}")
        print(f"运行测试: {test_name}")
        print(f"{'=' * 60}")

        start_time = time.time()

        try:
            test_func()
            duration = time.time() - start_time
            self.test_results['passed_tests'] += 1
            status = "PASSED"
            error_msg = None

            print(f"✓ 测试通过 ({duration:.3f}s)")
            self.logger.info(f"Test '{test_name}' PASSED in {duration:.3f}s")

        except Exception as e:
            duration = time.time() - start_time
            self.test_results['failed_tests'] += 1
            status = "FAILED"
            error_msg = str(e)

            print(f"✗ 测试失败: {e}")
            self.logger.error(f"Test '{test_name}' FAILED: {e}")

        self.test_results['test_details'].append({
            'name': test_name,
            'status': status,
            'duration': duration,
            'error': error_msg
        })

    def test_01_page_manager_basic(self):
        """测试页管理器基本功能 - 修复版"""
        print("测试页管理器的基本功能...")

        data_file = self.test_data_dir / "test_pages.db"
        meta_file = self.test_data_dir / "test_meta.json"

        with PageManager(str(data_file), str(meta_file)) as pm:
            # 测试页分配
            page1 = pm.allocate_page()
            page2 = pm.allocate_page()
            assert page1 == 1, f"Expected page1=1, got {page1}"
            assert page2 == 2, f"Expected page2=2, got {page2}"
            print(f"  ✓ 分配页: {page1}, {page2}")

            # 测试页写入和读取
            test_data = b"Hello, Storage!" + b"\x00" * (PAGE_SIZE - 15)
            pm.write_page_to_disk(page1, test_data)

            read_data = pm.read_page_from_disk(page1)
            assert read_data == test_data, "写入和读取的数据不匹配"
            print(f"  ✓ 页读写功能正常")

            # 测试页释放
            pm.deallocate_page(page1)
            assert not pm.is_page_allocated(page1), "页释放失败"
            print(f"  ✓ 页释放功能正常")

            # 测试元数据 - 修复：使用正确的字段名
            metadata = pm.get_metadata_info()
            assert metadata['total_allocated'] == 1, f"元数据不正确: {metadata['total_allocated']}"
            print(f"  ✓ 元数据管理正常")

    def test_02_buffer_pool_basic(self):
        """测试缓存池基本功能 - 修复版"""
        print("测试缓存池的基本功能...")

        # 修复：使用符合最小容量要求的值
        bp = BufferPool(capacity=15)  # 使用15而不是3

        # 测试缓存放入和获取
        test_data1 = b"page1_data" + b"\x00" * (PAGE_SIZE - 10)
        test_data2 = b"page2_data" + b"\x00" * (PAGE_SIZE - 10)

        bp.put(1, test_data1)
        bp.put(2, test_data2)

        assert bp.get(1) == test_data1, "缓存获取失败"
        assert bp.get(2) == test_data2, "缓存获取失败"
        print(f"  ✓ 缓存读写功能正常")

        # 测试缓存命中率
        stats = bp.get_statistics()
        assert stats['hit_rate'] > 0, "命中率计算异常"
        print(f"  ✓ 缓存命中率: {stats['hit_rate']}%")

        # 测试LRU淘汰 - 修复：调整容量以适应MIN_CACHE_SIZE
        bp_small = BufferPool(capacity=MIN_CACHE_SIZE)  # 使用最小容量

        # 填满缓存
        for i in range(MIN_CACHE_SIZE):
            test_data = f"page{i}".encode() + b"\x00" * (PAGE_SIZE - 10)
            bp_small.put(i, test_data)

        # 添加一个新页，应该淘汰最老的
        bp_small.put(MIN_CACHE_SIZE, b"new_page" + b"\x00" * (PAGE_SIZE - 8))

        # 检查最老的页是否被淘汰
        assert bp_small.get(0) is None, "LRU淘汰策略失效"
        print(f"  ✓ LRU淘汰策略正常")

        # 测试脏页管理
        bp.put(5, b"dirty_page" + b"\x00" * (PAGE_SIZE - 10), is_dirty=True)
        dirty_pages = bp.get_dirty_pages()
        assert 5 in dirty_pages, "脏页管理失效"
        print(f"  ✓ 脏页管理功能正常")

    def test_03_storage_manager_integration(self):
        """测试存储管理器集成功能 - 修复版"""
        print("测试存储管理器的集成功能...")

        data_file = self.test_data_dir / "integration_test.db"
        meta_file = self.test_data_dir / "integration_meta.json"

        # 修复：使用符合最小容量要求的缓存大小
        with StorageManager(
                buffer_size=20,  # 使用20而不是5
                data_file=str(data_file),
                meta_file=str(meta_file),
                auto_flush_interval=0
        ) as sm:
            # 测试页分配和访问
            page_id = sm.allocate_page()
            assert page_id > 0, "页分配失败"
            print(f"  ✓ 分配页: {page_id}")

            # 测试写入数据（写入缓存）
            test_data = b"integration_test_data" + b"\x00" * (PAGE_SIZE - 21)
            sm.write_page(page_id, test_data)
            print(f"  ✓ 数据写入缓存")

            # 测试从缓存读取
            read_data = sm.read_page(page_id)
            assert read_data == test_data, "缓存读取失败"
            print(f"  ✓ 从缓存读取数据成功")

            # 测试刷盘
            flushed_count = sm.flush_all_pages()
            assert flushed_count > 0, "刷盘失败"
            print(f"  ✓ 刷盘成功，刷新了 {flushed_count} 页")

            # 清空缓存后重新读取（从磁盘）
            sm.buffer_pool.clear()
            read_data_from_disk = sm.read_page(page_id)
            assert read_data_from_disk == test_data, "磁盘数据不一致"
            print(f"  ✓ 数据持久化成功")

            # 测试统计信息
            storage_info = sm.get_storage_info()
            assert storage_info['system_status'] == 'running', "系统状态异常"
            print(f"  ✓ 系统状态正常")

    def test_04_performance_and_stress(self):
        """测试性能和压力 - 修复版"""
        print("测试系统性能和压力...")

        # 修复：使用合适的缓存大小
        sm = create_storage_manager(
            buffer_size=50,  # 使用50而不是20
            data_dir=str(self.test_data_dir / "stress_test")
        )

        try:
            # 批量分配页
            allocated_pages = []
            start_time = time.time()

            for i in range(100):
                page_id = sm.allocate_page()
                allocated_pages.append(page_id)

                # 写入测试数据
                test_data = f"stress_test_page_{i}".encode() + b"\x00" * (PAGE_SIZE - 20)
                sm.write_page(page_id, test_data)

            allocation_time = time.time() - start_time
            print(f"  ✓ 分配并写入100页耗时: {allocation_time:.3f}s")

            # 随机读取测试
            start_time = time.time()
            for _ in range(200):
                random_page = random.choice(allocated_pages)
                data = sm.read_page(random_page)
                assert len(data) == PAGE_SIZE, "读取数据长度错误"

            read_time = time.time() - start_time
            print(f"  ✓ 随机读取200次耗时: {read_time:.3f}s")

            # 缓存性能统计
            cache_stats = sm.get_cache_stats()
            print(f"  ✓ 缓存命中率: {cache_stats['hit_rate']}%")
            print(f"  ✓ 缓存使用率: {cache_stats['cache_usage']}%")

            # 刷盘性能
            start_time = time.time()
            flushed = sm.flush_all_pages()
            flush_time = time.time() - start_time
            print(f"  ✓ 刷盘 {flushed} 页耗时: {flush_time:.3f}s")

        finally:
            sm.shutdown()

    def run_all_tests(self):
        """运行所有修复版测试"""
        print(f"\n{'=' * 80}")
        print(f"开始执行修复版磁盘存储系统集成测试")
        print(f"参数信息: MIN_CACHE_SIZE={MIN_CACHE_SIZE}, MAX_CACHE_SIZE={MAX_CACHE_SIZE}")
        print(f"{'=' * 80}")

        start_time = time.time()

        # 修复版测试列表
        tests = [
            ("页管理器基础功能", self.test_01_page_manager_basic),
            ("缓存池基础功能", self.test_02_buffer_pool_basic),
            ("存储管理器集成", self.test_03_storage_manager_integration),
            ("性能压力测试", self.test_04_performance_and_stress),
        ]

        # 执行所有测试
        for test_name, test_func in tests:
            self.run_test(test_name, test_func)

        total_time = time.time() - start_time

        # 输出测试结果总结
        self.print_test_summary(total_time)

    def print_test_summary(self, total_time):
        print(f"\n{'=' * 80}")
        print(f"修复版测试结果总结")
        print(f"{'=' * 80}")

        print(f"总测试时间: {total_time:.3f}s")
        print(f"总测试数量: {self.test_results['total_tests']}")
        print(f"通过测试: {self.test_results['passed_tests']}")
        print(f"失败测试: {self.test_results['failed_tests']}")

        success_rate = (self.test_results['passed_tests'] /
                        max(self.test_results['total_tests'], 1)) * 100
        print(f"成功率: {success_rate:.1f}%")

        if self.test_results['failed_tests'] == 0:
            print(f"\n🎉 所有测试通过！存储系统功能完整且运行正常。")
        else:
            print(f"\n⚠️  仍有 {self.test_results['failed_tests']} 个测试失败，需要进一步调试。")


def main():
    print("修复版存储系统集成测试")

    tester = FixedStorageSystemTester()

    try:
        tester.run_all_tests()
    except KeyboardInterrupt:
        print("\n测试被用户中断")
    except Exception as e:
        print(f"\n测试执行出现异常: {e}")
        import traceback
        traceback.print_exc()

    print("\n测试程序结束")


if __name__ == "__main__":
    main()