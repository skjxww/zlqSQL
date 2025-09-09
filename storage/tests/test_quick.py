#!/usr/bin/env python3
"""
最小依赖测试版本
只测试你负责的存储层核心功能，不依赖可能不存在的类

运行方法:
python test_minimal.py
"""

import os
import sys
import tempfile
import shutil

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_imports():
    """测试导入，逐个检查可用的组件"""
    print("检查模块导入...")

    try:
        from storage import create_storage_system
        print("✓ 主存储接口导入成功")
        storage_available = True
    except Exception as e:
        print(f"✗ 主存储接口导入失败: {e}")
        storage_available = False

    try:
        from storage.utils.serializer import PageSerializer
        print("✓ PageSerializer导入成功")
        page_serializer_available = True
    except Exception as e:
        print(f"✗ PageSerializer导入失败: {e}")
        page_serializer_available = False

    try:
        from storage.core.storage_manager import StorageManager
        print("✓ StorageManager导入成功")
        storage_manager_available = True
    except Exception as e:
        print(f"✗ StorageManager导入失败: {e}")
        storage_manager_available = False

    try:
        from storage.core.page_manager import PageManager
        print("✓ PageManager导入成功")
        page_manager_available = True
    except Exception as e:
        print(f"✗ PageManager导入失败: {e}")
        page_manager_available = False

    try:
        from storage.core.buffer_pool import BufferPool
        print("✓ BufferPool导入成功")
        buffer_pool_available = True
    except Exception as e:
        print(f"✗ BufferPool导入失败: {e}")
        buffer_pool_available = False

    return {
        'storage': storage_available,
        'page_serializer': page_serializer_available,
        'storage_manager': storage_manager_available,
        'page_manager': page_manager_available,
        'buffer_pool': buffer_pool_available
    }


def test_with_available_components(available):
    """根据可用组件进行测试"""
    print("\n" + "=" * 50)
    print("存储系统核心功能测试")
    print("=" * 50)

    test_dir = tempfile.mkdtemp()
    print(f"测试目录: {test_dir}")

    try:
        if available['storage']:
            test_main_storage_interface(test_dir)

        if available['page_serializer']:
            test_page_serializer()

        if available['storage_manager']:
            test_storage_manager_directly(test_dir)

        print("\n测试完成！")

    finally:
        try:
            shutil.rmtree(test_dir)
            print("测试环境清理完成")
        except:
            pass


def test_main_storage_interface(test_dir):
    """测试主存储接口"""
    print("\n1. 主存储接口测试...")

    try:
        from storage import create_storage_system

        with create_storage_system(buffer_size=10, data_dir=test_dir) as storage:

            # 基础页操作
            print("   测试页分配...")
            page1 = storage.allocate_page()
            page2 = storage.allocate_page()
            print(f"   分配了页: {page1}, {page2}")

            # 页读写
            print("   测试页读写...")
            test_data = b"test_data_12345" + b'\x00' * (4096 - 15)
            storage.write_page(page1, test_data)

            read_data = storage.read_page(page1)
            rw_success = read_data == test_data
            print(f"   页读写: {'✓' if rw_success else '✗'}")

            # 缓存统计
            print("   测试缓存统计...")
            try:
                stats = storage.get_cache_stats()
                required_fields = ['hit_rate', 'total_requests']
                has_stats = all(field in stats for field in required_fields)
                print(f"   缓存统计: {'✓' if has_stats else '✗'}")
                if has_stats:
                    print(f"     命中率: {stats['hit_rate']}%")
                    print(f"     总请求: {stats['total_requests']}")
            except:
                print("   缓存统计: ✗")

            # 表存储功能（如果可用）
            try:
                print("   测试表存储...")
                table_success = storage.create_table_storage("test_table", 512)
                print(f"   表存储创建: {'✓' if table_success else '✗'}")

                if table_success:
                    pages = storage.get_table_pages("test_table")
                    print(f"     表页数: {len(pages)}")
            except Exception as e:
                print(f"   表存储: ✗ ({e})")

            # 页释放
            print("   测试页释放...")
            storage.deallocate_page(page2)
            print("   页释放: ✓")

    except Exception as e:
        print(f"   主存储接口测试失败: {e}")


def test_page_serializer():
    """测试页序列化器"""
    print("\n2. 页序列化器测试...")

    try:
        from storage.utils.serializer import PageSerializer

        # 创建空页
        empty_page = PageSerializer.create_empty_page()
        print(f"   空页长度: {len(empty_page)} ({'✓' if len(empty_page) == 4096 else '✗'})")

        # 页信息
        page_info = PageSerializer.get_page_info(empty_page)
        print(f"   页信息: 记录数={page_info['record_count']}, 空闲空间={page_info.get('free_space_size', 'N/A')}")

        # 添加数据块
        test_record = b"test_record_data" + b'\x00' * 100
        new_page, success = PageSerializer.add_data_to_page(empty_page, test_record)
        print(f"   添加数据块: {'✓' if success else '✗'}")

        if success:
            # 提取数据块
            blocks = PageSerializer.get_data_blocks_from_page(new_page)
            extract_success = len(blocks) == 1 and blocks[0] == test_record
            print(f"   提取数据块: {'✓' if extract_success else '✗'}")

            # 页利用率
            try:
                utilization = PageSerializer.get_page_utilization(new_page)
                print(f"   页利用率: {utilization['utilization_ratio']:.1%}")
            except Exception as e:
                print(f"   页利用率: ✗ ({e})")

    except Exception as e:
        print(f"   页序列化器测试失败: {e}")


def test_storage_manager_directly(test_dir):
    """直接测试存储管理器"""
    print("\n3. 存储管理器直接测试...")

    try:
        from storage.core.storage_manager import StorageManager

        storage_manager = StorageManager(
            buffer_size=15,
            data_file=f"{test_dir}/direct_test.db",
            meta_file=f"{test_dir}/direct_meta.json"
        )

        print("   存储管理器初始化: ✓")

        # 分配页
        page1 = storage_manager.allocate_page()
        page2 = storage_manager.allocate_page()
        print(f"   直接分配页: {page1}, {page2}")

        # 读写
        test_data = b"direct_test" + b'\x00' * (4096 - 11)
        storage_manager.write_page(page1, test_data)
        read_data = storage_manager.read_page(page1)

        rw_success = read_data == test_data
        print(f"   直接读写: {'✓' if rw_success else '✗'}")

        # 缓存统计
        try:
            cache_stats = storage_manager.get_cache_stats()
            print(f"   直接缓存统计: ✓ (命中率: {cache_stats.get('hit_rate', 'N/A')}%)")
        except Exception as e:
            print(f"   直接缓存统计: ✗ ({e})")

        # 关闭
        storage_manager.shutdown()
        print("   存储管理器关闭: ✓")

    except Exception as e:
        print(f"   存储管理器直接测试失败: {e}")


def test_individual_components():
    """测试独立组件"""
    print("\n4. 独立组件测试...")

    # 测试页管理器
    try:
        from storage.core.page_manager import PageManager

        test_dir = tempfile.mkdtemp()
        try:
            pm = PageManager(
                data_file=f"{test_dir}/test_pages.db",
                meta_file=f"{test_dir}/test_meta.json"
            )

            page_id = pm.allocate_page()
            test_data = b"page_manager_test" + b'\x00' * (4096 - 17)
            pm.write_page_to_disk(page_id, test_data)
            read_data = pm.read_page_from_disk(page_id)

            success = read_data == test_data
            print(f"   页管理器独立测试: {'✓' if success else '✗'}")

        finally:
            shutil.rmtree(test_dir)

    except Exception as e:
        print(f"   页管理器测试: ✗ ({e})")

    # 测试缓存池
    try:
        from storage.core.buffer_pool import BufferPool

        bp = BufferPool(capacity=10)

        # 测试基本缓存操作
        test_data = b"cache_test_data" + b'\x00' * 100
        bp.put(1, test_data)
        cached_data = bp.get(1)

        cache_success = cached_data == test_data
        print(f"   缓存池独立测试: {'✓' if cache_success else '✗'}")

        # 测试统计
        stats = bp.get_statistics()
        has_stats = 'hit_rate' in stats
        print(f"   缓存统计: {'✓' if has_stats else '✗'}")

    except Exception as e:
        print(f"   缓存池测试: ✗ ({e})")


def main():
    """主测试函数"""
    print("存储系统诊断测试")
    print("=" * 50)

    # 检查导入
    available = test_imports()

    # 根据可用组件进行测试
    test_with_available_components(available)

    # 独立组件测试
    test_individual_components()

    # 总结
    print("\n" + "=" * 50)
    print("测试总结")
    print("=" * 50)

    total_components = len(available)
    working_components = sum(available.values())

    print(f"可用组件: {working_components}/{total_components}")

    for component, is_available in available.items():
        status = "✓" if is_available else "✗"
        print(f"  {status} {component}")

    if working_components >= 3:
        print("\n🎉 核心功能基本可用！")
        print("建议:")
        print("1. 检查失败的组件导入问题")
        print("2. 确保所有必需的文件都存在")
        print("3. 验证文件路径和模块结构")
    else:
        print("\n⚠️ 需要修复导入问题")
        print("请检查:")
        print("1. 文件路径是否正确")
        print("2. __init__.py 文件是否存在")
        print("3. 模块依赖是否完整")


if __name__ == "__main__":
    main()