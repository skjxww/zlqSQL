import os
import shutil
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor


def test_comprehensive_storage_debug():
    """综合测试（调试版）- 逐步执行并定位问题"""
    print("\n=== 综合存储系统测试（调试版）===")
    test_dir = "test_comprehensive_debug"

    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    try:
        from storage.core.storage_manager import StorageManager
        from storage.core.table_storage import TableStorage
        from storage.utils.serializer import PageSerializer, RecordSerializer
        from storage.utils.constants import PAGE_SIZE

        print("\n步骤1: 初始化存储系统")
        try:
            storage = StorageManager(
                buffer_size=20,
                data_file=f"{test_dir}/test.db",
                meta_file=f"{test_dir}/meta.json",
                auto_flush_interval=5,
                enable_extent_management=True,  # 这里可能有问题
                enable_wal=False,
                enable_concurrency=False
            )
            print("  ✓ StorageManager初始化成功")
        except Exception as e:
            print(f"  ✗ StorageManager初始化失败: {e}")
            raise

        try:
            table_storage = TableStorage(storage, f"{test_dir}/catalog.json")
            print("  ✓ TableStorage初始化成功")
        except Exception as e:
            print(f"  ✗ TableStorage初始化失败: {e}")
            raise

        print("\n步骤2: 测试多表操作")
        tables = ["users", "orders", "products"]
        for table_name in tables:
            try:
                success = table_storage.create_table_storage(table_name, 256)
                if not success:
                    print(f"  ✗ 创建表{table_name}返回False")
                    raise Exception(f"创建表{table_name}失败")
                print(f"  ✓ 创建表: {table_name}")
            except Exception as e:
                print(f"  ✗ 创建表{table_name}异常: {e}")
                raise

        print("\n步骤3: 测试数据插入")
        user_schema = [
            ("id", "INT", None),
            ("name", "VARCHAR", 50),
            ("age", "INT", None),
            ("email", "VARCHAR", 100)
        ]

        # 只插入10条测试
        print("  插入测试记录...")
        for i in range(10):
            try:
                record = {
                    "id": i + 1,
                    "name": f"User_{i:04d}",
                    "age": 20 + (i % 50),
                    "email": f"user{i}@example.com"
                }

                # 序列化记录
                record_bytes = RecordSerializer.serialize_record(record, user_schema)

                # 获取当前页
                pages = table_storage.get_table_pages("users")
                last_page_index = len(pages) - 1
                page_data = table_storage.read_table_page("users", last_page_index)

                # 尝试添加到当前页
                new_page_data, success = PageSerializer.add_record_to_page(page_data, record_bytes)

                if success:
                    table_storage.write_table_page("users", last_page_index, new_page_data)
                else:
                    # 分配新页
                    new_page_id = table_storage.allocate_table_page("users")
                    empty_page = PageSerializer.create_empty_page()
                    new_page_data, _ = PageSerializer.add_record_to_page(empty_page, record_bytes)
                    table_storage.write_table_page("users", len(pages), new_page_data)

                if i == 0:
                    print(f"    第一条记录插入成功")

            except Exception as e:
                print(f"  ✗ 插入第{i + 1}条记录失败: {e}")
                import traceback
                traceback.print_exc()
                raise

        print(f"  ✓ 成功插入10条测试记录")

        print("\n步骤4: 测试数据读取")
        try:
            pages = table_storage.get_table_pages("users")
            print(f"  表'users'使用了 {len(pages)} 个页")

            all_records = []
            for page_index in range(len(pages)):
                page_data = table_storage.read_table_page("users", page_index)
                records = PageSerializer.get_records_from_page(page_data, user_schema)
                all_records.extend(records)

            print(f"  ✓ 读取到 {len(all_records)} 条记录")
        except Exception as e:
            print(f"  ✗ 读取数据失败: {e}")
            raise

        print("\n步骤5: 测试持久化")
        try:
            storage.flush_all_pages()
            storage.shutdown()
            print("  ✓ 第一次关闭成功")

            # 重新打开
            storage2 = StorageManager(
                buffer_size=20,
                data_file=f"{test_dir}/test.db",
                meta_file=f"{test_dir}/meta.json",
                auto_flush_interval=0,
                enable_extent_management=True,
                enable_wal=False,
                enable_concurrency=False
            )

            table_storage2 = TableStorage(storage2, f"{test_dir}/catalog.json")

            # 验证表
            if not table_storage2.table_exists("users"):
                raise Exception("表'users'丢失")

            print("  ✓ 持久化验证通过")
            storage2.shutdown()

        except Exception as e:
            print(f"  ✗ 持久化测试失败: {e}")
            raise

        print("\n✓ 调试版综合测试通过！")
        return True

    except Exception as e:
        print(f"\n✗ 综合测试在某个步骤失败")
        return False
    finally:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)


# 运行调试版本
if __name__ == "__main__":
    test_comprehensive_storage_debug()