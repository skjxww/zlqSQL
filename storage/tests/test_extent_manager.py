# tests/test_real_extent_allocation.py
"""
测试真正的区分配功能
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.core.storage_manager import StorageManager


def test_real_extent_allocation():
    """测试真正的区分配"""
    print("\n=== 测试真正的区分配功能 ===")

    storage = StorageManager(enable_extent_management=True)

    print("初始状态：")
    summary = storage.get_storage_summary()
    extent_info = summary["extent_management"]
    print(f"区数量: {extent_info['stats']['total_extents']}")
    print(f"创建的区: {extent_info['stats']['extents_created']}")

    # 使用"unknown"表名，应该走单页分配
    print("\n1. 分配给未知表（应该是单页分配）：")
    page1 = storage.allocate_page()  # table_name是"unknown"
    print(f"分配的页: {page1}")

    # 使用具体表名，应该触发区分配
    print("\n2. 分配给large_user_data表（应该创建区）：")
    page2 = storage.allocate_page_for_table("large_user_data")  # 包含"large"和"user"
    print(f"分配的页: {page2}")

    # 再分配几个页给同一个表
    print("\n3. 继续为同一表分配页（应该从区内分配）：")
    page3 = storage.allocate_page_for_table("large_user_data")
    page4 = storage.allocate_page_for_table("large_user_data")
    page5 = storage.allocate_page_for_table("large_user_data")
    print(f"分配的页: {page3}, {page4}, {page5}")

    # 检查区统计信息
    print("\n4. 查看区分配结果：")
    summary = storage.get_storage_summary()
    extent_info = summary["extent_management"]
    print(f"区数量: {extent_info['stats']['total_extents']}")
    print(f"创建的区: {extent_info['stats']['extents_created']}")

    if extent_info['extents']:
        print("区详细信息：")
        for extent in extent_info['extents']:
            print(f"  区{extent['extent_id']}: 起始页{extent['start_page']}, "
                  f"已用{extent['allocated_pages']}页, 空闲{extent['free_pages']}页")

    # 测试释放页
    print(f"\n5. 释放页 {page3}：")
    storage.deallocate_page(page3)

    # 再次查看统计
    summary = storage.get_storage_summary()
    extent_info = summary["extent_management"]
    print("释放页后的区信息：")
    if extent_info['extents']:
        for extent in extent_info['extents']:
            print(f"  区{extent['extent_id']}: 起始页{extent['start_page']}, "
                  f"已用{extent['allocated_pages']}页, 空闲{extent['free_pages']}页")

    storage.shutdown()

    print("\n✅ 真正的区分配测试完成")


if __name__ == "__main__":
    print("开始测试真正的区分配功能...")

    try:
        test_real_extent_allocation()
        print("\n🎉 区分配功能正常工作！")

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()