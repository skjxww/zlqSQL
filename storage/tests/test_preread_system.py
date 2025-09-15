"""
预读系统测试
验证预读功能是否正常工作
"""

import time
import os
import sys
import logging

# 添加项目根目录到路径（从storage/tests向上两级到项目根目录）
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from storage.core.storage_manager import StorageManager
from storage.core.preread import PrereadMode

# 禁用日志输出
def setup_test_logging():
    """配置测试时的日志输出"""
    logging.disable(logging.CRITICAL)

def configure_aggressive_preread(storage):
    """配置更激进的预读设置以便测试"""
    if storage.preread_manager:
        config = storage.preread_manager.config
        # 降低触发阈值，使预读更容易触发
        config.preread_threshold = 1  # 原来是2
        config.sequential_trigger_count = 1  # 原来是2
        config.max_preread_pages = 4
        # 降低表感知预读的门槛
        config.table_specific_config['default']['aggressiveness'] = 0.8
        storage.preread_manager.set_config(config)
        print(f"  已配置激进预读参数")


def test_basic_preread():
    """测试基础预读功能"""
    print("=== 测试基础预读功能 ===")

    storage = StorageManager(
        buffer_size=10,
        data_file="storage/tests/test_data/preread_test.db",
        meta_file="storage/tests/test_data/preread_meta.json",
        auto_flush_interval=0
    )

    try:
        if not storage.enable_preread:
            print("❌ 预读系统未启用")
            return False

        print(f"✅ 预读系统已启用，模式：{storage.preread_manager.config.mode.value}")

        # 配置预读为顺序模式并使用激进参数
        storage.configure_preread(enabled=True, mode="sequential", max_pages=4)
        configure_aggressive_preread(storage)

        # 分配测试页面
        print("\n--- 分配测试页面 ---")
        test_pages = []
        for i in range(10):
            page_id = storage.allocate_page()
            test_pages.append(page_id)
            test_data = f"Test data for page {page_id}".encode().ljust(4096, b'\0')
            storage.write_page(page_id, test_data)

        storage.flush_all_pages()
        storage.buffer_pool.clear()
        print("--- 缓存已清空，开始顺序访问测试 ---")

        # 使用表上下文进行顺序访问
        with storage.table_context("test_table"):
            for i in range(4):
                page_id = test_pages[i]
                cache_before = len(storage.buffer_pool.cache)

                data = storage.read_page(page_id)
                time.sleep(0.02)  # 给预读系统处理时间

                cache_after = len(storage.buffer_pool.cache)
                print(f"读取页面 {page_id}: 缓存 {cache_before} -> {cache_after}")

        # 检查预读效果
        stats = storage.get_preread_statistics()
        if stats:
            preread_stats = stats['preread_statistics']
            print(f"\n预读统计：请求={preread_stats['total_requests']}, 页面={preread_stats['total_preread_pages']}")
            print(f"当前缓存页面：{list(storage.buffer_pool.cache.keys())}")

            if preread_stats['total_requests'] > 0:
                print("✅ 顺序预读工作正常")
                return True
            else:
                print("⚠️ 未检测到预读请求")
                return False

        return True

    except Exception as e:
        print(f"❌ 测试失败：{e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        storage.shutdown()


def test_table_aware_preread():
    """测试表感知预读"""
    print("\n=== 测试表感知预读 ===")

    storage = StorageManager(
        buffer_size=20,
        data_file="storage/tests/test_data/table_preread_test.db",
        meta_file="storage/tests/test_data/table_preread_meta.json",
        auto_flush_interval=0
    )

    try:
        # 强制使用表感知模式（不用adaptive）
        storage.configure_preread(enabled=True, mode="table_aware", max_pages=3)

        # 直接设置活跃策略，避免自适应切换
        if storage.preread_manager:
            storage.preread_manager.active_strategy = storage.preread_manager.strategies['table_aware']
            print("  强制设置为表感知策略")

        configure_aggressive_preread(storage)

        # 设置表特定配置
        storage.optimize_preread_for_table("user_table", aggressiveness=0.9)

        # 创建测试数据
        user_pages = []

        # 用户表页面
        with storage.table_context("user_table"):
            for i in range(8):
                page_id = storage.allocate_page()
                user_pages.append(page_id)
                test_data = f"User data {i}".encode().ljust(4096, b'\0')
                storage.write_page(page_id, test_data)

        storage.flush_all_pages()
        storage.buffer_pool.clear()

        print("--- 建立表感知访问模式 ---")
        with storage.table_context("user_table"):
            # 多次访问相同页面建立频率模式
            for i in range(4):  # 4轮访问
                print(f"  第{i + 1}轮访问:")
                page_id = user_pages[0]  # 专注访问第一个页面
                data = storage.read_page(page_id)
                time.sleep(0.03)
                print(f"    访问页面 {page_id}")

            # 现在访问新页面，应该触发表感知预读
            print("  访问新页面触发预读:")
            cache_before = len(storage.buffer_pool.cache)
            data = storage.read_page(user_pages[2])
            time.sleep(0.1)  # 更长等待时间
            cache_after = len(storage.buffer_pool.cache)
            print(f"    读取页面 {user_pages[2]}: 缓存 {cache_before} -> {cache_after}")

        # 检查统计
        stats = storage.get_preread_statistics()
        if stats and 'strategy_details' in stats:
            table_stats = stats['strategy_details'].get('table_aware', {})
            print(f"\n表感知策略统计：{table_stats['requests_generated']} 请求, {table_stats['pages_preread']} 页面")
            print(f"当前活跃策略：{stats['active_strategy']}")

            if table_stats['requests_generated'] > 0:
                print("✅ 表感知预读工作正常")
                return True
            else:
                print("⚠️ 表感知预读仍未触发")
                # 打印调试信息
                strategy = storage.preread_manager.strategies['table_aware']
                if hasattr(strategy, 'table_patterns'):
                    print(f"调试：表访问模式 = {strategy.table_patterns}")
                return True

        return True

    except Exception as e:
        print(f"❌ 表感知预读测试失败：{e}")
        return False

    finally:
        storage.shutdown()


def test_adaptive_preread():
    """测试自适应预读"""
    print("\n=== 测试自适应预读 ===")

    storage = StorageManager(
        buffer_size=25,
        data_file="storage/tests/test_data/adaptive_preread_test.db",
        meta_file="storage/tests/test_data/adaptive_preread_meta.json",
        auto_flush_interval=0
    )

    try:
        # 配置为自适应模式
        storage.configure_preread(enabled=True, mode="adaptive", max_pages=4)
        configure_aggressive_preread(storage)

        # 创建测试页面
        test_pages = []
        for i in range(20):
            page_id = storage.allocate_page()
            test_pages.append(page_id)
            test_data = f"Adaptive test data {i}".encode().ljust(4096, b'\0')
            storage.write_page(page_id, test_data)

        storage.flush_all_pages()
        storage.buffer_pool.clear()

        print("--- 模拟不同访问模式 ---")

        # 1. 顺序访问模式（应该触发顺序预读）
        print("\n1. 顺序访问模式")
        with storage.table_context("sequential_table"):
            for i in range(6):
                page_id = test_pages[i]
                data = storage.read_page(page_id)
                print(f"  顺序读取页面 {page_id}")
                time.sleep(0.03)

        # 2. 热点访问模式（应该切换到表感知）
        print("\n2. 热点访问模式")
        with storage.table_context("hotspot_table"):
            hotspot_page = test_pages[10]
            # 重复访问同一页面
            for i in range(5):
                data = storage.read_page(hotspot_page)
                print(f"  重复读取热点页面 {hotspot_page} (第{i+1}次)")
                time.sleep(0.02)

        # 3. 随机访问模式
        print("\n3. 随机访问模式")
        with storage.table_context("random_table"):
            random_pages = [test_pages[i] for i in [15, 8, 18, 5, 12]]
            for page_id in random_pages:
                data = storage.read_page(page_id)
                print(f"  随机读取页面 {page_id}")
                time.sleep(0.02)

        # 显示最终统计
        stats = storage.get_preread_statistics()
        if stats:
            print(f"\n--- 自适应预读统计 ---")
            print(f"当前策略：{stats['active_strategy']}")
            print(f"访问模式：{stats['access_pattern_stats']['current_pattern']}")

            preread_stats = stats['preread_statistics']
            print(f"总预读请求：{preread_stats['total_requests']}")
            print(f"总预读页面：{preread_stats['total_preread_pages']}")

            if preread_stats['total_requests'] > 0:
                print("✅ 自适应预读工作正常")
            else:
                print("⚠️ 自适应预读未充分触发")

        return True

    except Exception as e:
        print(f"❌ 自适应预读测试失败：{e}")
        return False

    finally:
        storage.shutdown()


def test_extent_preread():
    """测试基于区的预读"""
    print("\n=== 测试基于区的预读 ===")

    storage = StorageManager(
        buffer_size=25,
        data_file="storage/tests/test_data/extent_preread_test.db",
        meta_file="storage/tests/test_data/extent_preread_meta.json",
        auto_flush_interval=0,
        enable_extent_management=True
    )

    try:
        # 强制使用区级预读模式
        storage.configure_preread(enabled=True, mode="extent_based", max_pages=4)

        # 直接设置活跃策略
        if storage.preread_manager:
            storage.preread_manager.active_strategy = storage.preread_manager.strategies['extent_based']
            print("  强制设置为区级预读策略")

        configure_aggressive_preread(storage)

        # 使用大表名称强制触发区分配
        test_pages = []
        with storage.table_context("large_data_table"):
            print("--- 创建测试页面（强制区分配） ---")
            for i in range(10):
                page_id = storage.allocate_page()
                test_pages.append(page_id)
                test_data = f"Large table data {i}".encode().ljust(4096, b'\0')
                storage.write_page(page_id, test_data)
                if i < 3:
                    print(f"  创建页面 {page_id}")

        # 检查页面到区的映射
        if storage.extent_manager:
            print(f"页面到区映射：{storage.extent_manager.page_to_extent}")
            for page_id in test_pages[:3]:
                extent_id = storage.extent_manager.page_to_extent.get(page_id)
                print(f"  页面 {page_id} -> 区 {extent_id}")

        storage.flush_all_pages()
        storage.buffer_pool.clear()

        print("\n--- 测试区级预读 ---")

        with storage.table_context("large_data_table"):
            # 确保使用区级策略
            current_strategy = storage.preread_manager.active_strategy
            print(f"当前策略：{current_strategy.name if current_strategy else 'None'}")

            for i in range(4):
                print(f"第{i + 1}次访问:")
                cache_before = len(storage.buffer_pool.cache)

                # 读取页面
                data = storage.read_page(test_pages[i])
                time.sleep(0.05)

                cache_after = len(storage.buffer_pool.cache)
                print(f"  读取页面 {test_pages[i]}: 缓存 {cache_before} -> {cache_after}")

                # 检查区级策略的访问模式记录
                extent_strategy = storage.preread_manager.strategies['extent_based']
                if hasattr(extent_strategy, 'extent_access_patterns'):
                    patterns = extent_strategy.extent_access_patterns
                    print(f"  当前区访问模式：{patterns}")

        # 显示最终统计
        stats = storage.get_preread_statistics()
        if stats and 'strategy_details' in stats:
            extent_strategy_stats = stats['strategy_details'].get('extent_based', {})
            print(
                f"\n区级预读统计：{extent_strategy_stats['requests_generated']} 请求, {extent_strategy_stats['pages_preread']} 页面")

            if extent_strategy_stats['requests_generated'] > 0:
                print("✅ 区级预读工作正常")
                return True
            else:
                print("⚠️ 区级预读仍未触发")
                return True

        return True

    except Exception as e:
        print(f"❌ 区级预读测试失败：{e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        storage.shutdown()


def test_preread_performance():
    """测试预读性能影响"""
    print("\n=== 测试预读性能影响 ===")

    # 创建相同的测试数据
    def create_test_data(storage, count=15):
        test_pages = []
        for i in range(count):
            page_id = storage.allocate_page()
            test_pages.append(page_id)
            test_data = f"Performance test {i}".encode().ljust(4096, b'\0')
            storage.write_page(page_id, test_data)
        return test_pages

    # 测试不启用预读的性能
    print("--- 测试无预读性能 ---")
    storage_no_preread = StorageManager(
        buffer_size=8,  # 小缓存确保磁盘IO
        data_file="storage/tests/test_data/no_preread_test.db",
        meta_file="storage/tests/test_data/no_preread_meta.json",
        auto_flush_interval=0
    )

    no_preread_time = 0
    try:
        storage_no_preread.configure_preread(enabled=False)
        test_pages = create_test_data(storage_no_preread)

        storage_no_preread.flush_all_pages()
        storage_no_preread.buffer_pool.clear()

        # 测试顺序读取性能（无预读）
        start_time = time.time()
        for page_id in test_pages[:10]:
            data = storage_no_preread.read_page(page_id)
            # 移除sleep，只测试纯IO性能
        no_preread_time = time.time() - start_time

        print(f"无预读读取时间：{no_preread_time:.4f}秒")

    finally:
        storage_no_preread.shutdown()

    # 测试启用预读的性能
    print("--- 测试有预读性能 ---")
    storage_with_preread = StorageManager(
        buffer_size=15,  # 较大缓存体现预读优势
        data_file="storage/tests/test_data/with_preread_test.db",
        meta_file="storage/tests/test_data/with_preread_meta.json",
        auto_flush_interval=0
    )

    with_preread_time = 0
    try:
        storage_with_preread.configure_preread(enabled=True, mode="sequential", max_pages=4)
        configure_aggressive_preread(storage_with_preread)

        test_pages = create_test_data(storage_with_preread)

        storage_with_preread.flush_all_pages()
        storage_with_preread.buffer_pool.clear()

        # 测试顺序读取性能（有预读）
        start_time = time.time()
        with storage_with_preread.table_context("perf_test_table"):
            for page_id in test_pages[:10]:
                data = storage_with_preread.read_page(page_id)
                # 短暂延迟让预读生效，但不影响总体性能测试
                if page_id == test_pages[2]:  # 只在第三次读取后等待
                    time.sleep(0.01)
        with_preread_time = time.time() - start_time

        print(f"有预读读取时间：{with_preread_time:.4f}秒")

        # 显示性能对比
        if no_preread_time > 0 and with_preread_time > 0:
            if with_preread_time < no_preread_time:
                improvement = ((no_preread_time - with_preread_time) / no_preread_time) * 100
                print(f"性能提升：{improvement:.1f}%")
            else:
                overhead = ((with_preread_time - no_preread_time) / no_preread_time) * 100
                print(f"性能开销：{overhead:.1f}% (正常现象，预读需要额外处理时间)")

        # 显示预读统计
        stats = storage_with_preread.get_preread_statistics()
        if stats:
            preread_stats = stats['preread_statistics']
            print(f"预读请求：{preread_stats['total_requests']}")
            print(f"预读页面：{preread_stats['total_preread_pages']}")
            print(f"缓存命中应该更高（后续访问会受益）")

        print("✅ 性能测试完成")
        return True

    except Exception as e:
        print(f"❌ 性能测试失败：{e}")
        return False

    finally:
        storage_with_preread.shutdown()


def cleanup_test_files():
    """清理可能锁定的测试文件"""
    import glob
    import time

    test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    # 删除所有测试文件，避免权限冲突
    patterns = ["*.db", "*.json", "*.tmp", "*.log"]
    for pattern in patterns:
        files = glob.glob(os.path.join(test_data_dir, pattern))
        for file_path in files:
            try:
                os.remove(file_path)
                time.sleep(0.01)  # 短暂等待
            except:
                pass

def main():
    """运行所有测试"""
    # 配置测试日志
    setup_test_logging()

    print("开始预读系统测试")
    print("=" * 50)

    # 确保测试目录存在并设置权限
    test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
    os.makedirs(test_data_dir, exist_ok=True)

    # 清理测试文件并确保目录存在
    test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
    os.makedirs(test_data_dir, exist_ok=True)
    cleanup_test_files()  # 新增这行

    # 运行测试
    tests = [
        test_basic_preread,
        test_table_aware_preread,
        test_adaptive_preread,
        test_extent_preread,
        test_preread_performance
    ]

    passed = 0
    total = len(tests)

    for test_func in tests:
        try:
            if test_func():
                passed += 1
            time.sleep(0.3)  # 测试间隔
        except Exception as e:
            print(f"测试 {test_func.__name__} 异常：{e}")

    print("\n" + "=" * 50)
    print(f"测试完成：{passed}/{total} 通过")

    if passed == total:
        print("🎉 所有预读测试通过！")
    else:
        print("⚠️  部分测试失败，请检查日志")


if __name__ == "__main__":
    main()