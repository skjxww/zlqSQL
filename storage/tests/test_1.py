import os
import shutil
import json
from pathlib import Path


def setup_test_env(test_name):
    """创建干净的测试环境"""
    test_dir = f"test_{test_name}"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)
    return test_dir


def cleanup_test_env(test_dir):
    """清理测试环境"""
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


# 测试1: 基础页管理测试
def test_basic_page_operations():
    """测试页的分配、读写、释放"""
    print("\n=== 测试1: 基础页管理 ===")
    test_dir = setup_test_env("basic_page")

    try:
        from storage.core.storage_manager import StorageManager
        from storage.utils.constants import PAGE_SIZE

        storage = StorageManager(
            buffer_size=5,
            data_file=f"{test_dir}/test.db",
            meta_file=f"{test_dir}/test_meta.json",
            auto_flush_interval=0,
            enable_extent_management=False,
            enable_wal=False,
            enable_concurrency=False
        )

        page_id = storage.allocate_page()
        print(f"分配的页号: {page_id}")
        assert page_id > 0, "页号应该大于0"

        test_data = b"Hello Storage!" + b'\x00' * (PAGE_SIZE - 14)
        storage.write_page(page_id, test_data)
        print(f"写入{len(test_data)}字节数据到页{page_id}")

        read_data = storage.read_page(page_id)
        assert read_data == test_data, "读取的数据应该与写入的一致"
        print("缓存读取验证: 通过")

        storage.flush_page(page_id)
        print("页已刷盘")

        storage.buffer_pool.clear()
        print("缓存已清空")

        disk_data = storage.read_page(page_id)
        assert disk_data == test_data, "磁盘数据应该与原始数据一致"
        print("磁盘读取验证: 通过")

        storage.deallocate_page(page_id)
        print(f"页{page_id}已释放")

        stats = storage.get_storage_info()
        print(f"缓存命中率: {stats['cache_statistics']['hit_rate']}%")

        storage.shutdown()
        print("测试1: ✓ 通过")
        return True

    except Exception as e:
        print(f"测试1失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cleanup_test_env(test_dir)


# 测试2: 页序列化器测试
def test_page_serializer():
    """测试页内数据组织"""
    print("\n=== 测试2: 页序列化器 ===")

    try:
        from storage.utils.serializer import PageSerializer, RecordSerializer
        from storage.utils.constants import PAGE_SIZE

        empty_page = PageSerializer.create_empty_page()
        assert len(empty_page) == PAGE_SIZE, f"空页大小应该是{PAGE_SIZE}"
        print(f"空页创建: 大小={len(empty_page)}")

        page_info = PageSerializer.get_page_info(empty_page)
        assert page_info['record_count'] == 0, "空页应该有0条记录"
        assert page_info['free_space_start'] == 16, "空闲空间应该从16开始"
        print(f"空页信息: {page_info}")

        test_record1 = b"Record_001" + b'\x00' * 50
        page, success = PageSerializer.add_data_to_page(empty_page, test_record1)
        assert success, "添加第一条记录应该成功"

        page_info = PageSerializer.get_page_info(page)
        assert page_info['record_count'] == 1, "应该有1条记录"
        print(f"添加1条记录后: record_count={page_info['record_count']}")

        test_record2 = b"Record_002" + b'\x00' * 50
        page, success = PageSerializer.add_data_to_page(page, test_record2)
        assert success, "添加第二条记录应该成功"

        page_info = PageSerializer.get_page_info(page)
        assert page_info['record_count'] == 2, "应该有2条记录"
        print(f"添加2条记录后: record_count={page_info['record_count']}")

        data_blocks = PageSerializer.get_data_blocks_from_page(page)
        assert len(data_blocks) == 2, "应该读出2个数据块"
        assert data_blocks[0] == test_record1, "第一个数据块应该匹配"
        assert data_blocks[1] == test_record2, "第二个数据块应该匹配"
        print("数据块读取验证: 通过")

        utilization = PageSerializer.get_page_utilization(page)
        print(f"页面利用率: {utilization['utilization_ratio'] * 100:.2f}%")

        print("测试2: ✓ 通过")
        return True

    except Exception as e:
        print(f"测试2失败: {e}")
        import traceback
        traceback.print_exc()
        return False


# 测试3: 表存储测试
def test_table_storage():
    """测试表存储管理"""
    print("\n=== 测试3: 表存储管理 ===")
    test_dir = setup_test_env("table_storage")

    try:
        from storage.core.storage_manager import StorageManager
        from storage.core.table_storage import TableStorage
        from storage.utils.serializer import RecordSerializer, PageSerializer

        storage = StorageManager(
            buffer_size=10,
            data_file=f"{test_dir}/test.db",
            meta_file=f"{test_dir}/test_meta.json",
            auto_flush_interval=0,
            enable_extent_management=False,
            enable_wal=False,
            enable_concurrency=False
        )

        table_storage = TableStorage(storage, f"{test_dir}/table_catalog.json")

        success = table_storage.create_table_storage("test_table", 256)
        assert success, "创建表存储应该成功"
        print("表存储创建: 成功")

        pages = table_storage.get_table_pages("test_table")
        assert len(pages) == 1, "新表应该有1个初始页"
        print(f"初始页列表: {pages}")

        page_data = table_storage.read_table_page("test_table", 0)
        assert len(page_data) == 4096, "页大小应该是4096"

        page_info = PageSerializer.get_page_info(page_data)
        assert page_info['record_count'] == 0, "初始页应该是空的"
        print("初始页验证: 是空页")

        schema = [("id", "INT", None), ("name", "VARCHAR", 50)]
        record = {"id": 1, "name": "Test"}
        record_bytes = RecordSerializer.serialize_record(record, schema)

        page_data, success = PageSerializer.add_record_to_page(page_data, record_bytes)
        assert success, "添加记录应该成功"

        table_storage.write_table_page("test_table", 0, page_data)
        print("写入测试记录: 成功")

        storage.flush_all_pages()

        read_page = table_storage.read_table_page("test_table", 0)
        records = PageSerializer.get_records_from_page(read_page, schema)
        assert len(records) == 1, "应该读出1条记录"
        assert records[0]["id"] == 1, "ID应该是1"
        assert records[0]["name"] == "Test", "名称应该是Test"
        print(f"记录读取验证: {records[0]}")

        new_page_id = table_storage.allocate_table_page("test_table")
        pages = table_storage.get_table_pages("test_table")
        assert len(pages) == 2, "应该有2个页"
        print(f"分配新页后: {pages}")

        storage.shutdown()
        print("测试3: ✓ 通过")
        return True

    except Exception as e:
        print(f"测试3失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cleanup_test_env(test_dir)


# 测试4: 持久性测试（修复版）
def test_persistence():
    """测试数据持久性"""
    print("\n=== 测试4: 数据持久性 ===")
    test_dir = setup_test_env("persistence")

    try:
        from storage.core.storage_manager import StorageManager
        from storage.utils.constants import PAGE_SIZE

        # 阶段1: 写入数据
        print("阶段1: 写入数据")
        storage1 = StorageManager(
            buffer_size=10,
            data_file=f"{test_dir}/persist.db",
            meta_file=f"{test_dir}/persist_meta.json",
            auto_flush_interval=0,
            enable_extent_management=False,
            enable_wal=False,
            enable_concurrency=False
        )

        # 分配3个页并写入不同数据
        page_data_map = {}
        for i in range(3):
            page_id = storage1.allocate_page()
            # 修复：正确计算数据长度
            text = f"Page_{i}_Data"
            data = text.encode() + b'\x00' * (PAGE_SIZE - len(text.encode()))
            storage1.write_page(page_id, data)
            page_data_map[page_id] = data
            print(f"  写入页{page_id}")

        # 确保刷盘
        flushed = storage1.flush_all_pages()
        print(f"  刷盘{flushed}个脏页")

        # 正常关闭
        storage1.shutdown()
        print("  存储管理器已关闭")

        # 阶段2: 重新打开并验证
        print("阶段2: 重新打开并验证")
        storage2 = StorageManager(
            buffer_size=10,
            data_file=f"{test_dir}/persist.db",
            meta_file=f"{test_dir}/persist_meta.json",
            auto_flush_interval=0,
            enable_extent_management=False,
            enable_wal=False,
            enable_concurrency=False
        )

        # 验证每个页的数据
        all_correct = True
        for page_id, expected_data in page_data_map.items():
            actual_data = storage2.read_page(page_id)
            if actual_data != expected_data:
                print(f"  页{page_id}数据不匹配!")
                all_correct = False
            else:
                print(f"  页{page_id}验证通过")

        assert all_correct, "所有页数据应该正确"

        storage2.shutdown()
        print("测试4: ✓ 通过")
        return True

    except Exception as e:
        print(f"测试4失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cleanup_test_env(test_dir)


# 运行所有测试
def run_all_tests():
    """运行所有测试"""
    print("=" * 50)
    print("存储层功能测试")
    print("=" * 50)

    tests = [
        test_basic_page_operations,
        test_page_serializer,
        test_table_storage,
        test_persistence
    ]

    results = []
    for test in tests:
        result = test()
        results.append(result)

    print("\n" + "=" * 50)
    print("测试总结:")
    passed = sum(results)
    total = len(results)
    print(f"通过: {passed}/{total}")

    if passed == total:
        print("✓ 所有测试通过！存储层功能正常")
    else:
        print("✗ 有测试失败，需要进一步排查")


if __name__ == "__main__":
    run_all_tests()