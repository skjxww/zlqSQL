#!/usr/bin/env python3
"""
验证表空间文件映射修复测试
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.core.storage_manager import StorageManager
from storage.core.table_storage import TableStorage


def test_tablespace_mapping_fix():
    """测试表空间文件映射修复"""

    print("=== 表空间文件映射修复测试 ===")

    # 1. 创建存储管理器
    print("1. 初始化存储管理器...")
    storage = StorageManager(buffer_size=50, data_file="test_data/mapping_test.db")

    # 2. 检查初始文件映射
    print("2. 检查初始文件映射...")
    initial_files = storage.page_manager.tablespace_files.copy()
    print(f"   初始文件映射: {list(initial_files.keys())}")

    # 3. 创建表存储管理器
    print("3. 创建表存储管理器...")
    table_storage = TableStorage(storage, "test_data/mapping_table_catalog.json")

    # 4. 创建一个用户表，触发表空间创建
    print("4. 创建用户表，触发表空间自动创建...")
    success = table_storage.create_table_storage("test_user", 1024)
    print(f"   创建表结果: {'成功' if success else '失败'}")

    # 5. 检查更新后的文件映射
    print("5. 检查更新后的文件映射...")
    updated_files = storage.page_manager.tablespace_files.copy()
    print(f"   更新后映射: {list(updated_files.keys())}")

    # 6. 验证新表空间是否在映射中
    expected_tablespaces = ['default', 'system', 'user_data', 'temp', 'log']
    missing_tablespaces = []
    for ts in expected_tablespaces:
        if ts not in updated_files:
            missing_tablespaces.append(ts)
        else:
            print(f"   ✓ {ts}: {updated_files[ts]}")

    if missing_tablespaces:
        print(f"   ✗ 缺失的表空间: {missing_tablespaces}")

    # 7. 测试实际的页写入路由
    print("7. 测试页写入路由...")

    # 分配一页给用户表
    page_id = table_storage.allocate_table_page("test_user")
    print(f"   分配的页号: {page_id}")

    # 检查页的表空间归属
    page_tablespace = storage.page_manager.metadata.page_tablespaces.get(str(page_id), "unknown")
    print(f"   页的表空间归属: {page_tablespace}")

    # 写入测试数据
    test_data = b"Test mapping fix!" + b"\x00" * (4096 - 17)
    storage.write_page(page_id, test_data)

    # 读取并验证
    read_data = storage.read_page(page_id)
    write_success = read_data[:17] == b'Test mapping fix!'
    print(f"   页读写测试: {'成功' if write_success else '失败'}")

    # 8. 检查文件实际内容
    print("8. 检查实际写入的文件...")
    target_file = updated_files.get(page_tablespace, "unknown")
    if target_file != "unknown" and os.path.exists(target_file):
        file_size = os.path.getsize(target_file)
        print(f"   目标文件: {target_file}")
        print(f"   文件大小: {file_size} 字节")

        # 检查是否真的写入了目标文件
        if file_size > 0:
            print("   ✓ 数据确实写入了正确的表空间文件")
        else:
            print("   ✗ 目标文件为空，可能写入了错误的文件")
    else:
        print(f"   ✗ 目标文件不存在: {target_file}")

    # 9. 清理
    print("9. 清理资源...")
    storage.shutdown()

    print("=== 测试完成 ===")

    # 返回测试结果
    return (
            len(missing_tablespaces) == 0 and
            write_success and
            page_tablespace != "unknown"
    )


if __name__ == "__main__":
    # 确保测试目录存在
    os.makedirs("test_data", exist_ok=True)

    try:
        success = test_tablespace_mapping_fix()
        if success:
            print("\n✅ 文件映射修复成功！表空间功能完全正常。")
        else:
            print("\n⚠️  部分功能可能需要进一步调试")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()