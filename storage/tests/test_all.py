#!/usr/bin/env python3
"""
存储系统全面测试方案
测试你负责的存储层功能：页式存储、缓存管理、表到页映射

测试重点：
1. 构造模拟数据表，执行插入、查询、删除操作
2. 验证页分配与释放是否正确
3. 验证缓存命中率与替换策略效果
4. 输出日志与统计信息
"""

import os
import sys
import time
import random
import tempfile
import shutil
from typing import List, Dict, Any

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import create_storage_system
from storage.utils.serializer import PageSerializer


class StorageSystemTester:
    """存储系统测试器"""

    def __init__(self, test_dir: str = None):
        """初始化测试环境"""
        self.test_dir = test_dir or tempfile.mkdtemp()
        self.test_results = []
        self.current_test = ""

        print("=" * 70)
        print("存储系统全面测试")
        print("=" * 70)
        print(f"测试目录: {self.test_dir}")
        print()

    def log_test_result(self, test_name: str, success: bool, details: str = ""):
        """记录测试结果"""
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status} {test_name}")
        if details:
            print(f"    {details}")

        self.test_results.append({
            'test_name': test_name,
            'success': success,
            'details': details,
            'timestamp': time.time()
        })

    def test_basic_page_operations(self):
        """测试1: 基础页操作"""
        print("\n1. 基础页操作测试")
        print("-" * 50)

        with create_storage_system(buffer_size=20, data_dir=f"{self.test_dir}/test1") as storage:

            # 1.1 页分配测试
            try:
                pages = []
                for i in range(10):
                    page_id = storage.allocate_page()
                    pages.append(page_id)

                # 验证页号的唯一性
                unique_pages = set(pages)
                success = len(unique_pages) == len(pages)
                self.log_test_result("页分配唯一性", success, f"分配了{len(pages)}个页，{len(unique_pages)}个唯一")

            except Exception as e:
                self.log_test_result("页分配", False, f"异常: {e}")
                return

            # 1.2 页读写测试
            try:
                test_data = {}
                for i, page_id in enumerate(pages[:5]):
                    data = f"测试数据页{page_id}_内容{i}".encode('utf-8')
                    # 填充到4KB
                    data += b'\x00' * (4096 - len(data))
                    storage.write_page(page_id, data)
                    test_data[page_id] = data

                # 验证读取
                read_success = 0
                for page_id, expected_data in test_data.items():
                    read_data = storage.read_page(page_id)
                    if read_data == expected_data:
                        read_success += 1

                success = read_success == len(test_data)
                self.log_test_result("页读写一致性", success, f"{read_success}/{len(test_data)}页读写正确")

            except Exception as e:
                self.log_test_result("页读写", False, f"异常: {e}")

            # 1.3 页释放测试
            try:
                initial_allocated = len(storage.storage_manager.page_manager.get_allocated_pages())

                # 释放部分页
                for page_id in pages[:3]:
                    storage.deallocate_page(page_id)

                final_allocated = len(storage.storage_manager.page_manager.get_allocated_pages())
                freed_count = initial_allocated - final_allocated

                success = freed_count == 3
                self.log_test_result("页释放", success, f"释放了{freed_count}个页")

            except Exception as e:
                self.log_test_result("页释放", False, f"异常: {e}")

    def test_cache_mechanism(self):
        """测试2: 缓存机制测试"""
        print("\n2. 缓存机制测试")
        print("-" * 50)

        # 使用小缓存来测试LRU行为
        with create_storage_system(buffer_size=5, data_dir=f"{self.test_dir}/test2") as storage:

            try:
                # 分配页面
                pages = [storage.allocate_page() for _ in range(10)]

                # 2.1 测试缓存填充
                print("  写入页面以填充缓存...")
                for i, page_id in enumerate(pages):
                    data = f"缓存测试页{page_id}".encode('utf-8').ljust(4096, b'\0')
                    storage.write_page(page_id, data)

                    cache_stats = storage.get_cache_stats()
                    print(f"    写入页{page_id}: 缓存大小{cache_stats['cache_size']}, "
                          f"命中率{cache_stats['hit_rate']}%")

                # 2.2 测试缓存命中
                print("  重复读取前5个页面测试命中...")
                initial_hits = storage.get_cache_stats()['hit_count']

                for _ in range(3):  # 重复读取3次
                    for page_id in pages[:5]:
                        storage.read_page(page_id)

                final_stats = storage.get_cache_stats()
                hit_improvement = final_stats['hit_count'] - initial_hits

                success = hit_improvement > 10  # 应该有明显的命中增加
                self.log_test_result("缓存命中测试", success,
                                     f"命中增加{hit_improvement}次，最终命中率{final_stats['hit_rate']}%")

                # 2.3 测试LRU淘汰
                cache_capacity = final_stats.get('cache_capacity', 5)
                eviction_count = final_stats.get('eviction_count', 0)

                success = eviction_count > 0  # 应该有淘汰发生
                self.log_test_result("LRU淘汰机制", success,
                                     f"缓存容量{cache_capacity}，淘汰次数{eviction_count}")

                # 2.4 缓存统计完整性
                required_stats = ['hit_rate', 'cache_size', 'total_requests', 'cache_usage']
                missing_stats = [stat for stat in required_stats if stat not in final_stats]

                success = len(missing_stats) == 0
                self.log_test_result("缓存统计完整性", success,
                                     f"缺失统计项: {missing_stats}" if missing_stats else "所有统计项完整")

            except Exception as e:
                self.log_test_result("缓存机制测试", False, f"异常: {e}")

    def test_table_storage_management(self):
        """测试3: 表存储管理测试"""
        print("\n3. 表存储管理测试")
        print("-" * 50)

        with create_storage_system(buffer_size=30, data_dir=f"{self.test_dir}/test3") as storage:

            # 3.1 表存储创建测试
            try:
                tables = ["students", "courses", "enrollments", "grades"]
                created_tables = []

                for table_name in tables:
                    success = storage.create_table_storage(table_name, estimated_record_size=512)
                    if success:
                        created_tables.append(table_name)

                success = len(created_tables) == len(tables)
                self.log_test_result("表存储创建", success, f"成功创建{len(created_tables)}/{len(tables)}个表")

            except Exception as e:
                self.log_test_result("表存储创建", False, f"异常: {e}")
                return

            # 3.2 表页分配测试
            try:
                table_page_counts = {}

                for table_name in created_tables:
                    initial_pages = len(storage.get_table_pages(table_name))

                    # 为每个表分配额外的页
                    for _ in range(random.randint(2, 5)):
                        storage.allocate_table_page(table_name)

                    final_pages = len(storage.get_table_pages(table_name))
                    table_page_counts[table_name] = final_pages - initial_pages

                total_allocated = sum(table_page_counts.values())
                success = total_allocated > 0

                self.log_test_result("表页分配", success, f"总共为表分配了{total_allocated}个额外页")

                for table_name, count in table_page_counts.items():
                    print(f"    表'{table_name}': +{count}页")

            except Exception as e:
                self.log_test_result("表页分配", False, f"异常: {e}")

            # 3.3 表页读写测试
            try:
                test_table = created_tables[0]
                pages = storage.get_table_pages(test_table)

                # 向表的页中写入测试数据
                test_data = {}
                for i, page_index in enumerate(range(min(3, len(pages)))):
                    data = f"表{test_table}页{page_index}测试数据".encode('utf-8')
                    data += b'\x00' * (4096 - len(data))

                    storage.write_table_page(test_table, page_index, data)
                    test_data[page_index] = data

                # 验证读取
                read_correct = 0
                for page_index, expected_data in test_data.items():
                    read_data = storage.read_table_page(test_table, page_index)
                    if read_data == expected_data:
                        read_correct += 1

                success = read_correct == len(test_data)
                self.log_test_result("表页读写", success, f"{read_correct}/{len(test_data)}页读写正确")

            except Exception as e:
                self.log_test_result("表页读写", False, f"异常: {e}")

            # 3.4 表存储信息测试
            try:
                overall_info = storage.get_table_storage_info()

                required_fields = ['total_tables', 'total_pages']
                has_required = all(field in overall_info for field in required_fields)

                tables_match = overall_info['total_tables'] == len(created_tables)

                success = has_required and tables_match
                self.log_test_result("表存储信息", success,
                                     f"统计信息完整性: {has_required}, 表数匹配: {tables_match}")

                print(f"    总表数: {overall_info['total_tables']}")
                print(f"    总页数: {overall_info['total_pages']}")

            except Exception as e:
                self.log_test_result("表存储信息", False, f"异常: {e}")

    def test_simulated_database_operations(self):
        """测试4: 模拟数据库操作"""
        print("\n4. 模拟数据库操作测试")
        print("-" * 50)

        with create_storage_system(buffer_size=50, data_dir=f"{self.test_dir}/test4") as storage:

            # 4.1 模拟创建学生表
            try:
                table_name = "students"
                success = storage.create_table_storage(table_name, estimated_record_size=256)
                self.log_test_result("模拟建表", success)

                if not success:
                    return

                # 4.2 模拟插入操作（存储二进制数据块）
                print("  模拟插入学生记录...")
                inserted_records = []

                for i in range(100):  # 插入100条模拟记录
                    # 模拟记录的二进制数据（实际应该由记录序列化器生成）
                    mock_record_data = f"RECORD|ID:{i}|NAME:学生{i}|AGE:{18 + i % 10}".encode('utf-8')
                    mock_record_data += b'\x00' * (200 - len(mock_record_data))  # 填充到固定大小

                    # 获取表的页，尝试插入
                    pages = storage.get_table_pages(table_name)
                    inserted = False

                    # 尝试在现有页中插入
                    for page_idx in range(len(pages)):
                        current_page_data = storage.read_table_page(table_name, page_idx)
                        new_page_data, success = PageSerializer.add_data_to_page(current_page_data, mock_record_data)

                        if success:
                            storage.write_table_page(table_name, page_idx, new_page_data)
                            inserted_records.append((page_idx, i))
                            inserted = True
                            break

                    # 如果现有页都满了，分配新页
                    if not inserted:
                        new_page_id = storage.allocate_table_page(table_name)
                        new_pages = storage.get_table_pages(table_name)
                        new_page_idx = len(new_pages) - 1

                        empty_page = PageSerializer.create_empty_page()
                        new_page_data, success = PageSerializer.add_data_to_page(empty_page, mock_record_data)

                        if success:
                            storage.write_table_page(table_name, new_page_idx, new_page_data)
                            inserted_records.append((new_page_idx, i))

                insert_success = len(inserted_records) == 100
                self.log_test_result("模拟插入操作", insert_success,
                                     f"成功插入{len(inserted_records)}/100条记录")

                if insert_success:
                    final_pages = storage.get_table_pages(table_name)
                    print(f"    表最终使用{len(final_pages)}个页")

            except Exception as e:
                self.log_test_result("模拟插入操作", False, f"异常: {e}")
                return

            # 4.3 模拟查询操作（读取所有数据块）
            try:
                print("  模拟查询所有记录...")
                all_records = []
                pages = storage.get_table_pages(table_name)

                for page_idx in range(len(pages)):
                    page_data = storage.read_table_page(table_name, page_idx)
                    data_blocks = PageSerializer.get_data_blocks_from_page(page_data)
                    all_records.extend(data_blocks)

                query_success = len(all_records) == len(inserted_records)
                self.log_test_result("模拟查询操作", query_success,
                                     f"查询到{len(all_records)}条记录")

            except Exception as e:
                self.log_test_result("模拟查询操作", False, f"异常: {e}")

            # 4.4 模拟删除操作（删除部分记录）
            try:
                print("  模拟删除操作...")
                pages = storage.get_table_pages(table_name)
                deleted_count = 0

                # 从第一个页删除一些记录
                if pages:
                    page_data = storage.read_table_page(table_name, 0)
                    data_blocks = PageSerializer.get_data_blocks_from_page(page_data)

                    if len(data_blocks) > 2:
                        # 删除前两条记录
                        new_page_data, success1 = PageSerializer.remove_data_from_page(page_data, 0)
                        if success1:
                            new_page_data, success2 = PageSerializer.remove_data_from_page(new_page_data, 0)
                            if success2:
                                storage.write_table_page(table_name, 0, new_page_data)
                                deleted_count = 2

                delete_success = deleted_count > 0
                self.log_test_result("模拟删除操作", delete_success,
                                     f"删除了{deleted_count}条记录")

            except Exception as e:
                self.log_test_result("模拟删除操作", False, f"异常: {e}")

    def test_performance_and_statistics(self):
        """测试5: 性能和统计测试"""
        print("\n5. 性能和统计测试")
        print("-" * 50)

        with create_storage_system(buffer_size=100, data_dir=f"{self.test_dir}/test5") as storage:

            # 5.1 大量页操作性能测试
            try:
                print("  大量页操作性能测试...")
                start_time = time.time()

                # 分配大量页
                pages = []
                for i in range(1000):
                    page_id = storage.allocate_page()
                    pages.append(page_id)

                allocation_time = time.time() - start_time

                # 写入操作
                start_time = time.time()
                test_data = b"性能测试数据" + b'\x00' * (4096 - 16)
                for page_id in pages[:500]:  # 写入前500页
                    storage.write_page(page_id, test_data)

                write_time = time.time() - start_time

                # 读取操作
                start_time = time.time()
                for page_id in pages[:500]:
                    storage.read_page(page_id)

                read_time = time.time() - start_time

                # 性能统计
                alloc_rate = len(pages) / allocation_time
                write_rate = 500 / write_time
                read_rate = 500 / read_time

                print(f"    页分配速率: {alloc_rate:.1f} 页/秒")
                print(f"    页写入速率: {write_rate:.1f} 页/秒")
                print(f"    页读取速率: {read_rate:.1f} 页/秒")

                success = alloc_rate > 100 and write_rate > 50 and read_rate > 100
                self.log_test_result("大量页操作性能", success,
                                     f"分配{alloc_rate:.1f}, 写入{write_rate:.1f}, 读取{read_rate:.1f} 页/秒")

            except Exception as e:
                self.log_test_result("性能测试", False, f"异常: {e}")

            # 5.2 统计信息完整性测试
            try:
                cache_stats = storage.get_cache_stats()
                storage_stats = storage.get_storage_stats()

                # 检查关键统计字段
                required_cache_fields = ['hit_rate', 'total_requests', 'cache_size', 'cache_usage']
                required_storage_fields = ['system_status', 'uptime_seconds', 'operation_count']

                cache_complete = all(field in cache_stats for field in required_cache_fields)
                storage_complete = all(field in storage_stats for field in required_storage_fields)

                print(f"    缓存统计完整性: {cache_complete}")
                print(f"    存储统计完整性: {storage_complete}")

                if cache_complete:
                    print(f"      缓存命中率: {cache_stats['hit_rate']}%")
                    print(f"      总请求数: {cache_stats['total_requests']}")
                    print(f"      缓存使用率: {cache_stats['cache_usage']}%")

                if storage_complete:
                    print(f"      系统状态: {storage_stats['system_status']}")
                    print(f"      运行时间: {storage_stats['uptime_seconds']:.2f}秒")
                    print(f"      操作次数: {storage_stats['operation_count']}")

                success = cache_complete and storage_complete
                self.log_test_result("统计信息完整性", success)

            except Exception as e:
                self.log_test_result("统计信息测试", False, f"异常: {e}")

    def test_error_handling_and_edge_cases(self):
        """测试6: 异常处理和边界情况"""
        print("\n6. 异常处理和边界情况测试")
        print("-" * 50)

        with create_storage_system(buffer_size=10, data_dir=f"{self.test_dir}/test6") as storage:

            # 6.1 无效页操作测试
            try:
                # 尝试读取不存在的页
                try:
                    storage.read_page(99999)
                    invalid_read_handled = False
                except:
                    invalid_read_handled = True

                # 尝试写入无效数据
                try:
                    storage.write_page(1, b"too_short")  # 数据过短
                    invalid_write_handled = False
                except:
                    invalid_write_handled = True

                success = invalid_read_handled or invalid_write_handled  # 至少一个应该被处理
                self.log_test_result("无效页操作处理", success)

            except Exception as e:
                self.log_test_result("无效页操作处理", False, f"异常: {e}")

            # 6.2 表存储边界测试
            try:
                # 创建重复表名
                storage.create_table_storage("test_table", 1024)
                duplicate_result = storage.create_table_storage("test_table", 1024)

                # 访问不存在的表
                try:
                    storage.get_table_pages("nonexistent_table")
                    nonexistent_handled = False
                except:
                    nonexistent_handled = True

                success = not duplicate_result and nonexistent_handled
                self.log_test_result("表存储边界处理", success)

            except Exception as e:
                self.log_test_result("表存储边界处理", False, f"异常: {e}")

    def generate_test_report(self):
        """生成测试报告"""
        print("\n" + "=" * 70)
        print("测试报告")
        print("=" * 70)

        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['success'])
        failed_tests = total_tests - passed_tests

        print(f"总测试数: {total_tests}")
        print(f"通过: {passed_tests}")
        print(f"失败: {failed_tests}")
        print(f"通过率: {passed_tests / total_tests * 100:.1f}%")

        if failed_tests > 0:
            print("\n失败的测试:")
            for result in self.test_results:
                if not result['success']:
                    print(f"  ✗ {result['test_name']}: {result['details']}")

        print("\n详细统计:")
        test_categories = {}
        for result in self.test_results:
            category = result['test_name'].split()[0] if ' ' in result['test_name'] else result['test_name']
            if category not in test_categories:
                test_categories[category] = {'total': 0, 'passed': 0}
            test_categories[category]['total'] += 1
            if result['success']:
                test_categories[category]['passed'] += 1

        for category, stats in test_categories.items():
            pass_rate = stats['passed'] / stats['total'] * 100
            print(f"  {category}: {stats['passed']}/{stats['total']} ({pass_rate:.1f}%)")

        return passed_tests == total_tests

    def cleanup(self):
        """清理测试环境"""
        try:
            shutil.rmtree(self.test_dir)
            print(f"\n测试环境清理完成: {self.test_dir}")
        except Exception as e:
            print(f"\n清理失败: {e}")


def main():
    """主测试函数"""
    tester = StorageSystemTester()

    try:
        # 执行所有测试
        tester.test_basic_page_operations()
        tester.test_cache_mechanism()
        tester.test_table_storage_management()
        tester.test_simulated_database_operations()
        tester.test_performance_and_statistics()
        tester.test_error_handling_and_edge_cases()

        # 生成报告
        all_passed = tester.generate_test_report()

        if all_passed:
            print("\n🎉 所有测试通过！你的存储系统实现正确。")
        else:
            print("\n⚠️  部分测试失败，请检查相关功能。")

    except KeyboardInterrupt:
        print("\n测试被用户中断")
    except Exception as e:
        print(f"\n测试过程中发生严重错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        tester.cleanup()


if __name__ == "__main__":
    main()