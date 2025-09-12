"""
WAL功能测试
"""

from storage.core.storage_manager import StorageManager
import time


def test_wal_basic():
    """测试基本WAL功能"""
    print("测试WAL基本功能...")

    # 创建启用WAL的存储管理器
    storage = StorageManager(
        buffer_size=10,
        data_file="data/test_wal.db",
        meta_file="data/test_wal_meta.json",
        enable_wal=True  # 启用WAL
    )

    # 测试页面写入
    print("写入测试页面...")
    page1 = storage.allocate_page()
    storage.write_page(page1, b"Hello WAL!" + b'\x00' * 4086)

    page2 = storage.allocate_page()
    storage.write_page(page2, b"Test Data" + b'\x00' * 4087)

    # 测试事务
    print("测试事务...")
    if storage.wal_manager:
        with storage.wal_manager.transaction() as txn_id:
            print(f"事务 {txn_id} 开始")
            page3 = storage.allocate_page()
            storage.write_page(page3, b"Transaction Data" + b'\x00' * 4080)
            print(f"事务 {txn_id} 提交")

    # 创建检查点
    print("创建检查点...")
    if storage.wal_manager:
        storage.wal_manager.create_checkpoint()

    # 获取统计信息
    print("\nWAL统计信息:")
    if storage.wal_manager:
        stats = storage.wal_manager.get_statistics()
        print(f"  当前LSN: {stats['current_lsn']}")
        print(f"  活跃事务: {stats['active_transactions']}")

        health = storage.wal_manager.get_health_report()
        print(f"  健康状态: {health.get('status', 'unknown')}")

    # 关闭
    storage.shutdown()
    print("测试完成!")


def test_wal_recovery():
    """测试WAL恢复功能"""
    print("\n测试WAL恢复功能...")

    # 第一阶段：写入数据
    print("阶段1: 写入数据...")
    storage1 = StorageManager(
        buffer_size=10,
        data_file="data/test_recovery.db",
        meta_file="data/test_recovery_meta.json",
        enable_wal=True
    )

    pages = []
    for i in range(5):
        page = storage1.allocate_page()
        pages.append(page)
        # 修改这里：使用页号而不是索引
        data = f"Page {page} data".encode() + b'\x00' * (4096 - 15)
        storage1.write_page(page, data)
        print(f"  写入页面 {page}")

    # 强制刷新WAL（但不刷缓存到磁盘）
    if storage1.wal_manager:
        storage1.wal_manager.flush()
        print("  WAL已刷新到磁盘")

    # 不调用shutdown，模拟崩溃
    print("模拟崩溃（不正常关闭）...")
    # 注意：这里故意不调用 storage1.shutdown()

    # 第二阶段：恢复
    print("\n阶段2: 重启并恢复...")
    storage2 = StorageManager(
        buffer_size=10,  # 改为10
        data_file="data/test_recovery.db",
        meta_file="data/test_recovery_meta.json",
        enable_wal=True  # 启用WAL会自动触发恢复
    )

    # 验证数据
    print("验证恢复的数据...")
    for page in pages:
        data = storage2.read_page(page)
        # 修改验证逻辑
        expected_prefix = f"Page {page} data".encode()
        if data[:len(expected_prefix)] == expected_prefix:
            print(f"  ✓ 页面 {page} 恢复成功")
        else:
            print(f"  ✗ 页面 {page} 恢复失败!")
            print(f"    期望: {expected_prefix}")
            print(f"    实际: {data[:20]}")

    storage2.shutdown()
    print("恢复测试完成!")


if __name__ == "__main__":
    test_wal_basic()
    test_wal_recovery()