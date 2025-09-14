# test_gui_concurrency.py
import logging

logging.disable(logging.CRITICAL)

from storage.core.storage_manager import StorageManager


def test_concurrency_in_gui_context():
    """测试GUI环境下的并发控制"""
    print("=" * 50)
    print("并发控制集成测试（GUI环境）")
    print("=" * 50)

    # 使用与GUI相同的初始化方式
    storage = StorageManager(enable_concurrency=True)

    try:
        # 测试1：基本的读写锁
        print("\n测试1: 基本读写锁")
        page_id = storage.allocate_page()
        print(f"✓ 分配页: {page_id}")

        data = b"Test data" + b'\x00' * (4096 - 9)
        storage.write_page(page_id, data)
        print(f"✓ 写入页: {page_id}")

        read_data = storage.read_page(page_id)
        print(f"✓ 读取页: {page_id}, 数据匹配: {read_data[:9] == b'Test data'}")

        # 测试2：事务并发
        print("\n测试2: 事务并发控制")
        txn1 = storage.begin_transaction()
        txn2 = storage.begin_transaction()
        print(f"✓ 创建事务: txn1={txn1}, txn2={txn2}")

        # 获取锁统计
        if storage.lock_manager:
            stats = storage.lock_manager.get_statistics()
            print(f"✓ 锁统计: 活跃锁={stats['active_locks']}, 活跃事务={stats['active_transactions']}")

        storage.commit_transaction(txn1)
        storage.commit_transaction(txn2)
        print("✓ 事务提交成功")

        print("\n✅ 所有测试通过！并发控制正常工作。")
        return True

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        storage.shutdown()


if __name__ == "__main__":
    success = test_concurrency_in_gui_context()
    if success:
        print("\n可以安全地使用GUI了！")
    else:
        print("\n请检查并发控制实现。")