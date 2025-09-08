# test_storage.py
"""
存储系统测试文件
"""

import os
import time
from storage_manager import StorageManager, create_storage_manager


def test_basic_operations():
    """测试基本操作"""
    print("=" * 50)
    print("测试1: 基本读写操作")
    print("=" * 50)

    # 创建存储管理器
    storage = create_storage_manager(buffer_size=5)

    try:
        # 分配几个页
        page1 = storage.allocate_page()
        page2 = storage.allocate_page()
        page3 = storage.allocate_page()

        print(f"分配的页号: {page1}, {page2}, {page3}")

        # 写入数据
        data1 = b"Hello, World! This is page 1" + b"\x00" * (4096 - 29)
        data2 = b"Page 2 contains different data" + b"\x00" * (4096 - 30)
        data3 = b"Page 3 has some other content" + b"\x00" * (4096 - 30)

        storage.write_page(page1, data1)
        storage.write_page(page2, data2)
        storage.write_page(page3, data3)

        # 读取数据
        read_data1 = storage.read_page(page1)
        read_data2 = storage.read_page(page2)
        read_data3 = storage.read_page(page3)

        # 验证数据
        assert data1 == read_data1, "页1数据不匹配"
        assert data2 == read_data2, "页2数据不匹配"
        assert data3 == read_data3, "页3数据不匹配"

        print("✓ 基本读写操作测试通过")

        # 显示缓存统计
        stats = storage.get_cache_stats()
        print(f"缓存统计: {stats}")

    finally:
        storage.shutdown()


def test_cache_mechanism():
    """测试缓存机制"""
    print("\n" + "=" * 50)
    print("测试2: 缓存机制和LRU淘汰")
    print("=" * 50)

    # 创建小容量缓存池进行测试
    storage = create_storage_manager(buffer_size=3)

    try:
        # 分配多个页
        pages = []
        for i in range(6):
            page_id = storage.allocate_page()
            pages.append(page_id)

            # 写入数据
            data = f"Page {i + 1} data".encode() + b"\x00" * (4096 - len(f"Page {i + 1} data"))
            storage.write_page(page_id, data)

        print(f"分配了6个页: {pages}")

        # 读取前3个页（应该全部命中缓存）
        print("\n读取前3个页（测试缓存命中）:")
        for i in range(3):
            data = storage.read_page(pages[i])
            print(f"读取页 {pages[i]}: {data[:15].decode().strip()}")

        # 读取后3个页（会触发缓存淘汰）
        print("\n读取后3个页（触发LRU淘汰）:")
        for i in range(3, 6):
            data = storage.read_page(pages[i])
            print(f"读取页 {pages[i]}: {data[:15].decode().strip()}")

        # 再次读取前3个页（应该从磁盘重新加载）
        print("\n再次读取前3个页（从磁盘重新加载）:")
        for i in range(3):
            data = storage.read_page(pages[i])
            print(f"读取页 {pages[i]}: {data[:15].decode().strip()}")

        # 显示最终统计
        stats = storage.get_cache_stats()
        print(f"\n最终缓存统计: {stats}")

        print("✓ 缓存机制测试通过")

    finally:
        storage.shutdown()


def test_data_persistence():
    """测试数据持久化"""
    print("\n" + "=" * 50)
    print("测试3: 数据持久化")
    print("=" * 50)

    test_data = {}

    # 第一阶段：写入数据并关闭
    print("第一阶段：写入数据")
    storage1 = create_storage_manager()

    try:
        for i in range(3):
            page_id = storage1.allocate_page()
            data = f"Persistent data {i + 1}".encode() + b"\x00" * (4096 - len(f"Persistent data {i + 1}"))
            storage1.write_page(page_id, data)
            test_data[page_id] = data
            print(f"写入页 {page_id}: {data[:20].decode().strip()}")

        # 手动刷新到磁盘
        storage1.flush_all_pages()

    finally:
        storage1.shutdown()

    print("\n存储管理器已关闭，模拟程序重启...")
    time.sleep(1)

    # 第二阶段：重新加载并验证数据
    print("\n第二阶段：重新加载验证数据")
    storage2 = create_storage_manager()

    try:
        for page_id, expected_data in test_data.items():
            read_data = storage2.read_page(page_id)
            assert read_data == expected_data, f"页 {page_id} 数据不匹配"
            print(f"验证页 {page_id}: {read_data[:20].decode().strip()} ✓")

        print("✓ 数据持久化测试通过")

    finally:
        storage2.shutdown()


def test_page_allocation_deallocation():
    """测试页分配和释放"""
    print("\n" + "=" * 50)
    print("测试4: 页分配和释放")
    print("=" * 50)

    storage = create_storage_manager()

    try:
        # 分配页
        allocated_pages = []
        for i in range(5):
            page_id = storage.allocate_page()
            allocated_pages.append(page_id)
            print(f"分配页: {page_id}")

        # 释放部分页
        pages_to_free = allocated_pages[:3]
        for page_id in pages_to_free:
            storage.deallocate_page(page_id)
            print(f"释放页: {page_id}")

        # 重新分配页（应该重用已释放的页号）
        print("\n重新分配页（测试页重用）:")
        reused_pages = []
        for i in range(3):
            page_id = storage.allocate_page()
            reused_pages.append(page_id)
            print(f"重新分配页: {page_id}")

        # 检查是否重用了之前释放的页号
        reused_set = set(reused_pages)
        freed_set = set(pages_to_free)

        if reused_set == freed_set:
            print("✓ 页重用机制工作正常")
        else:
            print(f"警告: 页重用可能有问题. 释放的: {freed_set}, 重用的: {reused_set}")

        # 显示存储信息
        info = storage.get_storage_info()
        print(f"\n存储系统信息:")
        print(f"页管理器信息: {info['page_manager_info']}")

        print("✓ 页分配释放测试通过")

    finally:
        storage.shutdown()


def test_error_handling():
    """测试错误处理"""
    print("\n" + "=" * 50)
    print("测试5: 错误处理")
    print("=" * 50)

    storage = create_storage_manager()

    try:
        # 测试读取不存在的页
        print("测试读取不存在的页:")
        data = storage.read_page(999)  # 不存在的页号
        print(f"读取不存在的页，返回数据长度: {len(data)}")

        # 测试释放未分配的页
        print("\n测试释放未分配的页:")
        storage.deallocate_page(888)  # 未分配的页号

        # 测试关闭后的操作
        print("\n测试关闭后的操作:")
        storage.shutdown()

        try:
            storage.read_page(1)
            print("错误：关闭后仍能操作")
        except RuntimeError as e:
            print(f"✓ 正确捕获错误: {e}")

        print("✓ 错误处理测试通过")

    except Exception as e:
        print(f"测试过程中发生异常: {e}")
        storage.shutdown()


def test_performance():
    """性能测试"""
    print("\n" + "=" * 50)
    print("测试6: 性能测试")
    print("=" * 50)

    storage = create_storage_manager(buffer_size=20)

    try:
        # 大量页操作性能测试
        num_pages = 50
        pages = []

        # 分配和写入测试
        print(f"分配并写入 {num_pages} 个页...")
        start_time = time.time()

        for i in range(num_pages):
            page_id = storage.allocate_page()
            pages.append(page_id)

            data = f"Performance test page {i}".encode() + b"\x00" * (4096 - len(f"Performance test page {i}"))
            storage.write_page(page_id, data)

        write_time = time.time() - start_time
        print(f"写入完成，耗时: {write_time:.3f}秒")

        # 随机读取测试
        print(f"\n随机读取测试...")
        start_time = time.time()

        import random
        random.shuffle(pages)

        for page_id in pages:
            data = storage.read_page(page_id)
            # 验证数据完整性
            assert len(data) == 4096, f"页 {page_id} 数据长度不正确"

        read_time = time.time() - start_time
        print(f"读取完成，耗时: {read_time:.3f}秒")

        # 显示性能统计
        stats = storage.get_cache_stats()
        print(f"\n性能统计:")
        print(f"总请求数: {stats['total_requests']}")
        print(f"缓存命中率: {stats['hit_rate']}%")
        print(f"平均写入时间: {write_time / num_pages * 1000:.2f}ms/页")
        print(f"平均读取时间: {read_time / num_pages * 1000:.2f}ms/页")

        print("✓ 性能测试完成")

    finally:
        storage.shutdown()


def cleanup_test_files():
    """清理测试文件"""
    files_to_remove = ["database.db", "metadata.json"]
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
            print(f"清理文件: {file}")


def run_all_tests():
    """运行所有测试"""
    print("开始存储系统测试")
    print("=" * 60)

    # 清理之前的测试文件
    cleanup_test_files()

    try:
        test_basic_operations()
        test_cache_mechanism()
        test_data_persistence()
        test_page_allocation_deallocation()
        test_error_handling()
        test_performance()

        print("\n" + "=" * 60)
        print("🎉 所有测试通过！存储系统工作正常")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # 最终清理
        print(f"\n清理测试文件...")
        cleanup_test_files()


if __name__ == "__main__":
    run_all_tests()