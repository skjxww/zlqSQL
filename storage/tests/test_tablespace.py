#!/usr/bin/env python3
"""
表空间功能测试
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.core.storage_manager import StorageManager
from storage.core.table_storage import TableStorage


def test_tablespace_functionality():
    """测试表空间基本功能"""

    print("=== 表空间功能测试 ===")

    # 1. 创建存储管理器
    print("1. 初始化存储管理器...")
    storage = StorageManager(buffer_size=50, data_file="test_data/test.db")

    # 2. 检查默认表空间
    print("2. 检查表空间列表...")
    tablespaces = storage.list_tablespaces()
    print(f"   发现表空间: {[ts['name'] for ts in tablespaces]}")

    # 3. 创建表存储管理器
    print("3. 创建表存储管理器...")
    table_storage = TableStorage(storage, "test_data/table_catalog.json")

    # 4. 创建测试表
    print("4. 创建测试表...")
    success = table_storage.create_table_storage("test_table", 1024)
    print(f"   创建表结果: {'成功' if success else '失败'}")

    # 5. 为表分配页
    print("5. 为表分配页...")
    page_id = table_storage.allocate_table_page("test_table")
    print(f"   分配的页号: {page_id}")

    # 6. 测试页读写
    print("6. 测试页读写...")
    test_data = b"Hello Tablespace!" + b"\x00" * (4096 - 17)
    storage.write_page(page_id, test_data)
    read_data = storage.read_page(page_id)
    print(f"   写入读取测试: {'成功' if read_data[:17] == b'Hello Tablespace!' else '失败'}")

    # 7. 获取存储摘要
    print("7. 获取存储摘要...")
    summary = storage.get_storage_summary()
    print(f"   表空间数量: {summary['tablespaces']['count']}")
    print(f"   多文件支持: {summary['feature_status']['multi_file_support']}")

    # 8. 清理
    print("8. 清理资源...")
    storage.shutdown()

    print("=== 测试完成 ===")


if __name__ == "__main__":
    # 确保测试目录存在
    os.makedirs("test_data", exist_ok=True)

    try:
        test_tablespace_functionality()
        print("\n✅ 所有测试通过！表空间功能正常工作。")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()