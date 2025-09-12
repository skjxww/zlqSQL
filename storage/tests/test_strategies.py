# test_cache_strategy_comprehensive.py
"""
缓存策略综合测试
==================

测试内容：
1. 基础策略功能测试（LRU、FIFO）
2. BufferPool策略集成测试
3. 缓存管理功能完整性测试
4. 策略行为差异对比测试
5. 自适应策略切换验证测试

作者：[你的姓名]
日期：2025年9月
课程：大型平台软件设计实习
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.core.buffer_pool import BufferPool
from storage.core.cache_strategies import LRUStrategy, FIFOStrategy, AdaptiveStrategy


class CacheStrategyTester:
    """缓存策略测试类"""

    def __init__(self):
        self.test_results = []
        self.current_test = ""

    def log_result(self, test_name: str, passed: bool, details: str = ""):
        """记录测试结果"""
        status = "✓ PASS" if passed else "✗ FAIL"
        result = f"{status} | {test_name}"
        if details:
            result += f" | {details}"
        self.test_results.append(result)
        print(result)

    def print_separator(self, title: str):
        """打印分隔线和标题"""
        print(f"\n{'=' * 60}")
        print(f"  {title}")
        print(f"{'=' * 60}")

    def test_basic_strategies(self):
        """测试1：基础策略功能"""
        self.print_separator("测试1：基础策略功能验证")

        # 测试LRU策略
        print("\n1.1 测试LRU策略基本功能")
        try:
            lru = LRUStrategy(3)

            # 添加数据
            lru.put(1, (b'data1', False, 1.0))
            lru.put(2, (b'data2', False, 2.0))
            lru.put(3, (b'data3', False, 3.0))

            # 访问页1（使其成为最近使用）
            result = lru.get(1)

            # 添加页4，应该淘汰页2（最久未使用）
            lru.put(4, (b'data4', False, 4.0))
            evicted = lru.evict()

            # 验证淘汰的是页2
            lru_passed = evicted and evicted[0] == 2
            self.log_result("LRU策略基本功能", lru_passed,
                            f"淘汰页面：{evicted[0] if evicted else 'None'}，期望：2")

        except Exception as e:
            self.log_result("LRU策略基本功能", False, f"异常：{e}")

        # 测试FIFO策略
        print("\n1.2 测试FIFO策略基本功能")
        try:
            fifo = FIFOStrategy(3)

            # 添加数据
            fifo.put(1, (b'data1', False, 1.0))
            fifo.put(2, (b'data2', False, 2.0))
            fifo.put(3, (b'data3', False, 3.0))

            # 访问页1（FIFO中不影响顺序）
            result = fifo.get(1)

            # 添加页4，应该淘汰页1（最先进入）
            fifo.put(4, (b'data4', False, 4.0))
            evicted = fifo.evict()

            # 验证淘汰的是页1
            fifo_passed = evicted and evicted[0] == 1
            self.log_result("FIFO策略基本功能", fifo_passed,
                            f"淘汰页面：{evicted[0] if evicted else 'None'}，期望：1")

        except Exception as e:
            self.log_result("FIFO策略基本功能", False, f"异常：{e}")

    def test_buffer_integration(self):
        """测试2：BufferPool策略集成"""
        self.print_separator("测试2：BufferPool策略集成验证")

        try:
            buffer = BufferPool(capacity=15)

            # 验证策略模式启用
            strategy_enabled = hasattr(buffer, '_strategy') and buffer._strategy is not None
            self.log_result("策略模式启用", strategy_enabled,
                            f"策略类型：{type(buffer._strategy).__name__}")

            # 测试基本操作
            buffer.put(1, b'data1' * 100, is_dirty=False)
            buffer.put(2, b'data2' * 100, is_dirty=True)
            data1 = buffer.get(1)

            # 验证策略缓存和普通缓存同步
            cache_sync = len(buffer.cache) == len(buffer._strategy)
            self.log_result("缓存同步", cache_sync,
                            f"普通缓存：{len(buffer.cache)}，策略缓存：{len(buffer._strategy)}")

            # 验证数据正确性
            data_correct = data1 is not None and len(data1) > 0
            self.log_result("数据正确性", data_correct,
                            f"获取数据长度：{len(data1) if data1 else 0}")

        except Exception as e:
            self.log_result("BufferPool集成", False, f"异常：{e}")

    def test_cache_management(self):
        """测试3：缓存管理功能完整性"""
        self.print_separator("测试3：缓存管理功能完整性验证")

        try:
            buffer = BufferPool(capacity=12)

            # 添加测试数据
            buffer.put(1, b'data1' * 100, is_dirty=False)
            buffer.put(2, b'data2' * 100, is_dirty=True)

            # 测试mark_dirty
            buffer.mark_dirty(1)
            dirty_pages = buffer.get_dirty_pages()
            mark_dirty_ok = 1 in dirty_pages and 2 in dirty_pages
            self.log_result("mark_dirty功能", mark_dirty_ok,
                            f"脏页数量：{len(dirty_pages)}")

            # 测试clear_dirty_flag
            buffer.clear_dirty_flag(1)
            dirty_pages = buffer.get_dirty_pages()
            clear_dirty_ok = 1 not in dirty_pages and 2 in dirty_pages
            self.log_result("clear_dirty_flag功能", clear_dirty_ok,
                            f"清除后脏页数量：{len(dirty_pages)}")

            # 测试remove
            removed = buffer.remove(2)
            remove_ok = removed is not None and len(buffer.cache) == 1
            self.log_result("remove功能", remove_ok,
                            f"移除后缓存大小：{len(buffer.cache)}")

            # 测试统计信息
            stats = buffer.get_statistics()
            stats_ok = 'hit_rate' in stats and 'total_requests' in stats
            self.log_result("统计信息", stats_ok,
                            f"命中率：{stats.get('hit_rate', 0)}%")

        except Exception as e:
            self.log_result("缓存管理功能", False, f"异常：{e}")

    def test_strategy_behavior_differences(self):
        """测试4：策略行为差异对比"""
        self.print_separator("测试4：LRU vs FIFO策略行为差异验证")

        try:
            buffer = BufferPool(capacity=12)

            # 建立测试场景：重复访问模式（应该有利于LRU）
            for i in range(1, 8):
                buffer.put(i, f'data{i}'.encode() * 50, is_dirty=False)

            # 访问页1，使其成为最近使用
            data1 = buffer.get(1)

            # 添加更多页面，触发淘汰
            for i in range(8, 15):
                buffer.put(i, f'data{i}'.encode() * 50, is_dirty=False)

            # 检查页1是否仍在缓存（LRU应该保留最近访问的页面）
            data1_final = buffer.get(1)
            lru_behavior_ok = data1_final is not None

            self.log_result("LRU行为验证", lru_behavior_ok,
                            "LRU正确保留了最近访问的页面" if lru_behavior_ok else "LRU未保留最近访问的页面")

            # 获取当前策略统计
            if hasattr(buffer._strategy, 'get_strategy_stats'):
                stats = buffer._strategy.get_strategy_stats()
                pattern_stats = stats.get('pattern_stats', {})
                repeat_rate = pattern_stats.get('repeat_rate', 0)

                pattern_detection_ok = repeat_rate > 0
                self.log_result("访问模式检测", pattern_detection_ok,
                                f"重复访问率：{repeat_rate:.3f}")

        except Exception as e:
            self.log_result("策略行为差异", False, f"异常：{e}")

    def test_adaptive_strategy_switching(self):
        """测试5：自适应策略切换"""
        self.print_separator("测试5：自适应策略切换验证")

        try:
            buffer = BufferPool(capacity=12)

            # 记录初始策略
            initial_strategy = buffer._strategy.get_current_strategy()

            # 创建明确的顺序访问模式（有利于FIFO）
            for i in range(1, 301):  # 300次纯顺序访问
                buffer.put(i, f'data{i}'.encode() * 50, is_dirty=False)

            # 检查最终策略
            final_strategy = buffer._strategy.get_current_strategy()
            strategy_switched = initial_strategy != final_strategy

            self.log_result("策略切换功能", strategy_switched,
                            f"从 {initial_strategy} 切换到 {final_strategy}")

            # 验证访问模式检测
            if hasattr(buffer._strategy, 'get_strategy_stats'):
                stats = buffer._strategy.get_strategy_stats()
                pattern_stats = stats.get('pattern_stats', {})
                sequential_rate = pattern_stats.get('sequential_rate', 0)

                pattern_detection_ok = sequential_rate > 0.8  # 应该检测到高顺序访问率
                self.log_result("顺序访问检测", pattern_detection_ok,
                                f"顺序访问率：{sequential_rate:.3f}")

                # 验证决策机制
                decisions = stats.get('consecutive_decisions', [])
                decision_mechanism_ok = len(decisions) > 0
                self.log_result("决策机制", decision_mechanism_ok,
                                f"连续决策：{decisions}")

        except Exception as e:
            self.log_result("自适应策略切换", False, f"异常：{e}")

    def test_performance_and_statistics(self):
        """测试6：性能和统计信息"""
        self.print_separator("测试6：性能和统计信息验证")

        try:
            buffer = BufferPool(capacity=20)

            # 执行大量操作
            for i in range(1, 51):
                buffer.put(i, f'data{i}'.encode() * 100, is_dirty=i % 3 == 0)

            # 随机访问一些页面
            for i in [1, 5, 10, 15, 20, 1, 5, 10]:
                buffer.get(i)

            # 获取详细统计信息
            stats = buffer.get_statistics()

            # 验证统计信息完整性
            required_stats = ['total_requests', 'hit_count', 'hit_rate', 'eviction_count']
            stats_complete = all(key in stats for key in required_stats)
            self.log_result("统计信息完整性", stats_complete,
                            f"包含所需字段：{required_stats}")

            # 验证命中率计算
            hit_rate_valid = 0 <= stats['hit_rate'] <= 100
            self.log_result("命中率计算", hit_rate_valid,
                            f"命中率：{stats['hit_rate']}%")

            # 验证性能指标
            perf_metrics = buffer.get_performance_metrics()
            perf_valid = 'cache_hit_rate' in perf_metrics
            self.log_result("性能指标", perf_valid,
                            f"缓存效率：{perf_metrics.get('cache_efficiency', 0)}")

        except Exception as e:
            self.log_result("性能统计", False, f"异常：{e}")

    def run_all_tests(self):
        """运行所有测试"""
        print("缓存策略综合测试开始")
        print(f"测试时间：{__import__('time').strftime('%Y-%m-%d %H:%M:%S')}")

        # 执行所有测试
        self.test_basic_strategies()
        self.test_buffer_integration()
        self.test_cache_management()
        self.test_strategy_behavior_differences()
        self.test_adaptive_strategy_switching()
        self.test_performance_and_statistics()

        # 生成测试报告
        self.generate_test_report()

    def generate_test_report(self):
        """生成测试报告"""
        self.print_separator("测试报告")

        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if "✓ PASS" in result)
        failed_tests = total_tests - passed_tests
        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0

        print(f"\n测试总结：")
        print(f"  总测试数：{total_tests}")
        print(f"  通过测试：{passed_tests}")
        print(f"  失败测试：{failed_tests}")
        print(f"  成功率：{success_rate:.1f}%")

        print(f"\n详细结果：")
        for result in self.test_results:
            print(f"  {result}")

        if success_rate == 100:
            print(f"\n🎉 所有测试通过！缓存策略系统实现完整且功能正常。")
        else:
            print(f"\n⚠️  部分测试失败，请检查失败的测试项。")


def main():
    """主测试函数"""
    tester = CacheStrategyTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()