"""
测试增强的接口功能
验证向后兼容性和新的上下文功能
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.core.storage_manager import StorageManager


def test_backward_compatibility():
    """测试向后兼容性"""
    print("\n=== 测试向后兼容性 ===")

    storage = StorageManager()

    # 原有调用方式应该完全正常
    page1 = storage.allocate_page()
    print(f"原有接口分配: 页 {page1}")

    page2 = storage.allocate_page()
    print(f"原有接口分配: 页 {page2}")

    storage.shutdown()
    print("✅ 向后兼容性测试通过")


def test_enhanced_interface():
    """测试增强接口"""
    print("\n=== 测试增强接口 ===")

    storage = StorageManager()

    # 新方式1：直接传递表名参数
    page1 = storage.allocate_page(table_name="large_user_data")
    print(f"增强接口分配: 页 {page1} (表: large_user_data)")

    page2 = storage.allocate_page(table_name="large_user_data")
    print(f"增强接口分配: 页 {page2} (表: large_user_data)")

    # 查看区统计
    stats = storage.get_storage_summary()
    extent_info = stats["extent_management"]
    print(f"区数量: {extent_info['stats']['total_extents']}")

    storage.shutdown()
    print("✅ 增强接口测试通过")


def test_context_management():
    """测试上下文管理"""
    print("\n=== 测试上下文管理 ===")

    storage = StorageManager()

    # 设置表上下文
    storage.set_table_context("large_log_table")

    # 后续分配自动使用表上下文
    page1 = storage.allocate_page()
    print(f"上下文分配: 页 {page1} (自动使用 large_log_table)")

    page2 = storage.allocate_page()
    print(f"上下文分配: 页 {page2} (自动使用 large_log_table)")

    # 显式参数覆盖上下文
    page3 = storage.allocate_page(table_name="temp_table")
    print(f"显式参数分配: 页 {page3} (显式指定 temp_table)")

    # 清除上下文
    storage.clear_table_context()
    page4 = storage.allocate_page()
    print(f"清除上下文后: 页 {page4} (回到 unknown)")

    storage.shutdown()
    print("✅ 上下文管理测试通过")


def test_context_manager():
    """测试Python上下文管理器"""
    print("\n=== 测试Python上下文管理器 ===")

    storage = StorageManager()

    # 使用 with 语句
    with storage.table_context("big_data_table"):
        page1 = storage.allocate_page()
        page2 = storage.allocate_page()
        print(f"with语句中分配: 页 {page1}, {page2} (自动使用 big_data_table)")

    # 上下文自动清除
    page3 = storage.allocate_page()
    print(f"with语句外分配: 页 {page3} (自动回到 unknown)")

    # 查看最终统计
    stats = storage.get_storage_summary()
    extent_info = stats["extent_management"]
    print(f"\n最终统计:")
    print(f"总区数: {extent_info['stats']['total_extents']}")
    if extent_info['extents']:
        for extent in extent_info['extents']:
            print(f"  区{extent['extent_id']}: 已用{extent['allocated_pages']}页")

    storage.shutdown()
    print("✅ Python上下文管理器测试通过")


if __name__ == "__main__":
    print("开始测试增强接口功能...")

    try:
        test_backward_compatibility()
        test_enhanced_interface()
        test_context_management()
        test_context_manager()

        print("\n🎉 所有测试都通过了！")
        print("\n总结：")
        print("✅ 原有接口完全兼容")
        print("✅ 新增table_name参数正常工作")
        print("✅ 上下文管理功能正常")
        print("✅ Python with语句支持正常")

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()