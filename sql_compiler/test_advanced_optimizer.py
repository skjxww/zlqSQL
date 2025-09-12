"""
修复版优化器测试 - 兼容当前SQL解析器
"""

import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer
from sql_compiler.parser.syntax_analyzer import SyntaxAnalyzer
from sql_compiler.semantic.semantic_analyzer import SemanticAnalyzer
from catalog.catalog_manager import CatalogManager
from sql_compiler.codegen.plan_generator import PlanGenerator
from sql_compiler.codegen.operators import *

# 尝试导入优化器
try:
    from sql_compiler.optimizer.statistics import StatisticsManager
    from sql_compiler.optimizer.advanced_optimizer import AdvancedQueryOptimizer, QueryOptimizationPipeline

    ADVANCED_OPTIMIZER_AVAILABLE = True
except ImportError:
    try:
        from sql_compiler.optimizer.simple_optimizer import SimpleQueryOptimizer

        ADVANCED_OPTIMIZER_AVAILABLE = False
        print("⚠️ 高级优化器不可用，使用简化优化器")
    except ImportError:
        ADVANCED_OPTIMIZER_AVAILABLE = False
        print("⚠️ 优化器模块不可用")


class FixedOptimizerTestSuite:
    """修复版优化器测试套件"""

    def __init__(self):
        self.catalog = CatalogManager()
        self.setup_test_environment()
        self.setup_optimizer()

        self.test_results = []
        self.successful_tests = 0
        self.failed_tests = 0

    def setup_test_environment(self):
        """设置测试环境"""
        print("🔧 设置测试环境...")

        test_tables = [
            ("customers", [
                ("id", "INT", ["PRIMARY KEY"]),
                ("name", "VARCHAR(100)", []),
                ("city", "VARCHAR(50)", []),
                ("country", "VARCHAR(50)", []),
                ("age", "INT", [])
            ]),
            ("orders", [
                ("id", "INT", ["PRIMARY KEY"]),
                ("customer_id", "INT", ["FOREIGN KEY"]),
                ("product", "VARCHAR(100)", []),
                ("amount", "DECIMAL(10,2)", []),
                ("order_date", "DATE", [])
            ]),
            ("products", [
                ("id", "INT", ["PRIMARY KEY"]),
                ("name", "VARCHAR(100)", []),
                ("category", "VARCHAR(50)", []),
                ("price", "DECIMAL(10,2)", [])
            ])
        ]

        for table_name, columns in test_tables:
            try:
                self.catalog.create_table(table_name, columns)
                print(f"   ✅ 创建表: {table_name}")
            except Exception as e:
                print(f"   ⚠️ 表 {table_name} 可能已存在")

    def setup_optimizer(self):
        """设置优化器"""
        print("🔧 设置优化器...")

        if ADVANCED_OPTIMIZER_AVAILABLE:
            try:
                self.stats_manager = StatisticsManager()
                self.optimization_pipeline = QueryOptimizationPipeline(self.catalog)
                self.optimizer_type = "advanced"
                print("   ✅ 高级优化器初始化成功")

                # 生成统计信息
                self._generate_mock_statistics()

            except Exception as e:
                print(f"   ⚠️ 高级优化器初始化失败: {e}")
                self.setup_fallback_optimizer()
        else:
            self.setup_fallback_optimizer()

    def setup_fallback_optimizer(self):
        """设置回退优化器"""
        try:
            from sql_compiler.optimizer.simple_optimizer import SimpleQueryOptimizer
            self.simple_optimizer = SimpleQueryOptimizer(silent_mode=True)
            self.optimizer_type = "simple"
            self.stats_manager = None
            print("   ✅ 简单优化器初始化成功")
        except ImportError:
            self.optimizer_type = "none"
            self.simple_optimizer = None
            self.stats_manager = None
            print("   ⚠️ 没有可用的优化器")

    def _generate_mock_statistics(self):
        """生成模拟统计信息"""
        if not self.stats_manager:
            return

        print("📊 生成模拟统计信息...")

        sample_data = {
            "customers": [
                {"id": i, "name": f"Customer{i}", "city": f"City{i % 10}",
                 "country": f"Country{i % 5}", "age": 20 + (i % 60)}
                for i in range(100)
            ],
            "orders": [
                {"id": i, "customer_id": i % 100, "product": f"Product{i % 50}",
                 "amount": 100.0 + (i % 1000), "order_date": "2023-01-01"}
                for i in range(200)
            ],
            "products": [
                {"id": i, "name": f"Product{i}", "category": f"Cat{i % 10}",
                 "price": 10.0 + (i % 100)}
                for i in range(50)
            ]
        }

        for table_name, data in sample_data.items():
            try:
                self.stats_manager.analyze_table(table_name, data)
                print(f"   ✅ 统计信息: {table_name}")
            except Exception as e:
                print(f"   ⚠️ 统计信息生成失败 {table_name}: {e}")

    def test_query(self, name: str, sql: str, description: str = ""):
        """测试单个查询"""
        print(f"\n📝 {name}")
        if description:
            print(f"   描述: {description}")
        print(f"   SQL: {sql}")

        try:
            # 编译查询
            start_compile = time.time()

            lexer = LexicalAnalyzer(sql)
            tokens = lexer.tokenize()

            parser = SyntaxAnalyzer(tokens)
            ast = parser.parse()

            semantic = SemanticAnalyzer(self.catalog)
            semantic.analyze(ast)

            plan_generator = PlanGenerator(
                enable_optimization=False,
                silent_mode=True,
                catalog_manager=self.catalog
            )

            original_plan = plan_generator.generate(ast)
            compile_time = time.time() - start_compile

            print(f"   ✅ 编译成功 ({compile_time * 1000:.2f}ms)")

            # 优化查询
            start_optimize = time.time()

            if self.optimizer_type == "advanced":
                optimized_plan = self.optimization_pipeline.optimize(original_plan)
            elif self.optimizer_type == "simple":
                optimized_plan = self.simple_optimizer.optimize(original_plan)
            else:
                optimized_plan = original_plan

            optimize_time = time.time() - start_optimize

            print(f"   🎯 优化完成 ({optimize_time * 1000:.2f}ms)")

            # 分析结果
            original_ops = self._count_operators(original_plan)
            optimized_ops = self._count_operators(optimized_plan)

            print(f"   📊 操作符数量: {len(original_ops)} -> {len(optimized_ops)}")

            # 显示计划变化
            if set(original_ops.keys()) != set(optimized_ops.keys()):
                print("   🔄 执行计划已优化")
                changed_ops = set(optimized_ops.keys()) - set(original_ops.keys())
                if changed_ops:
                    print(f"   ➕ 新增操作符: {', '.join(changed_ops)}")
            else:
                print("   ➡️  执行计划未变化")

            # 成本分析
            if (self.optimizer_type == "advanced" and
                    hasattr(self, 'optimization_pipeline')):
                try:
                    cost_model = self.optimization_pipeline.optimizer.cost_model
                    original_cost = cost_model.calculate_cost(original_plan)
                    optimized_cost = cost_model.calculate_cost(optimized_plan)

                    print(f"   💰 原始成本: {original_cost['total_cost']:.2f}")
                    print(f"   💰 优化成本: {optimized_cost['total_cost']:.2f}")

                    if original_cost['total_cost'] > 0:
                        improvement = ((original_cost['total_cost'] - optimized_cost['total_cost']) /
                                       original_cost['total_cost'] * 100)
                        print(f"   📈 性能提升: {improvement:.1f}%")

                except Exception as e:
                    print(f"   ⚠️ 成本计算失败: {e}")

            self.successful_tests += 1

        except Exception as e:
            print(f"   ❌ 测试失败: {e}")
            self.failed_tests += 1

    def _count_operators(self, plan: Operator) -> dict:
        """统计操作符类型"""
        counts = {}

        def count_recursive(op):
            op_type = type(op).__name__
            counts[op_type] = counts.get(op_type, 0) + 1
            for child in op.children:
                count_recursive(child)

        count_recursive(plan)
        return counts

    def run_basic_tests(self):
        """运行基础测试"""
        print(f"\n🎯 基础优化测试（{self.optimizer_type}优化器）")
        print("=" * 50)

        # 简单查询（不使用AS别名）
        self.test_query(
            "简单投影查询",
            "SELECT name FROM customers;",
            "测试列投影优化"
        )

        self.test_query(
            "带条件查询",
            "SELECT * FROM customers WHERE age > 30;",
            "测试谓词下推优化"
        )

        self.test_query(
            "组合查询",
            "SELECT name, city FROM customers WHERE country = 'USA';",
            "测试投影+谓词下推"
        )

        # 连接查询（不使用AS别名）
        self.test_query(
            "简单连接",
            "SELECT customers.name, orders.amount FROM customers JOIN orders ON customers.id = orders.customer_id;",
            "测试两表连接优化"
        )

        self.test_query(
            "带过滤的连接",
            "SELECT customers.name FROM customers JOIN orders ON customers.id = orders.customer_id WHERE customers.age > 25;",
            "测试连接+过滤优化"
        )

        # 聚合查询
        self.test_query(
            "简单聚合",
            "SELECT city, COUNT(*) FROM customers GROUP BY city;",
            "测试分组聚合优化"
        )

        self.test_query(
            "带条件聚合",
            "SELECT city, COUNT(*) FROM customers WHERE country = 'USA' GROUP BY city;",
            "测试聚合+过滤优化"
        )

        # 排序查询
        self.test_query(
            "排序查询",
            "SELECT name, age FROM customers ORDER BY age DESC;",
            "测试排序优化"
        )

    def run_performance_tests(self):
        """运行性能测试"""
        print(f"\n⚡ 性能测试（{self.optimizer_type}优化器）")
        print("=" * 50)

        queries = [
            ("简单扫描", "SELECT * FROM customers;"),
            ("索引查找", "SELECT * FROM customers WHERE id = 1;"),
            ("范围查询", "SELECT * FROM customers WHERE age BETWEEN 25 AND 35;"),
            ("连接查询",
             "SELECT customers.name, orders.amount FROM customers JOIN orders ON customers.id = orders.customer_id;"),
            ("复杂查询",
             "SELECT customers.city, COUNT(*) FROM customers JOIN orders ON customers.id = orders.customer_id WHERE customers.country = 'USA' GROUP BY customers.city ORDER BY COUNT(*) DESC;")
        ]

        total_time = 0
        for name, sql in queries:
            start_time = time.time()
            try:
                self.test_query(f"性能测试: {name}", sql)
                end_time = time.time()
                query_time = end_time - start_time
                total_time += query_time
                print(f"   ⏱️  查询处理时间: {query_time * 1000:.2f}ms")
            except Exception as e:
                print(f"   ❌ 性能测试失败: {e}")

        if len(queries) > 0:
            avg_time = total_time / len(queries)
            print(f"\n📊 性能总结:")
            print(f"   平均处理时间: {avg_time * 1000:.2f}ms")
            print(f"   总处理时间: {total_time * 1000:.2f}ms")
            print(f"   查询吞吐量: {len(queries) / total_time:.1f} 查询/秒")

    def generate_report(self):
        """生成测试报告"""
        print("\n" + "=" * 60)
        print("📋 优化器测试报告")
        print("=" * 60)

        total_tests = self.successful_tests + self.failed_tests
        success_rate = (self.successful_tests / total_tests * 100) if total_tests > 0 else 0

        print(f"\n📊 测试总览:")
        print(f"   优化器类型: {self.optimizer_type}")
        print(f"   总测试数: {total_tests}")
        print(f"   成功测试: {self.successful_tests}")
        print(f"   失败测试: {self.failed_tests}")
        print(f"   成功率: {success_rate:.1f}%")

        # 根据成功率给出评价
        if success_rate >= 90:
            print("   评价: 🎉 优秀")
        elif success_rate >= 70:
            print("   评价: 👍 良好")
        elif success_rate >= 50:
            print("   评价: ⚠️ 一般")
        else:
            print("   评价: ❌ 需要改进")

        # 优化器特定统计
        if (self.optimizer_type == "advanced" and
                hasattr(self, 'optimization_pipeline')):
            try:
                pipeline_stats = self.optimization_pipeline.get_optimization_statistics()
                if pipeline_stats:
                    print(f"\n🔄 优化流水线统计:")
                    print(f"   优化次数: {pipeline_stats.get('total_optimizations', 0)}")
                    print(f"   优化成功率: {pipeline_stats.get('success_rate', 0) * 100:.1f}%")
                    avg_improvement = pipeline_stats.get('avg_improvement_percent', 0)
                    if avg_improvement > 0:
                        print(f"   平均性能提升: {avg_improvement:.1f}%")
            except Exception as e:
                print(f"   流水线统计获取失败: {e}")

        print(f"\n🎯 测试完成！")

    def run_all_tests(self):
        """运行所有测试"""
        print("🚀 开始修复版优化器测试")
        print("=" * 60)

        start_time = time.time()

        try:
            self.run_basic_tests()
            self.run_performance_tests()

        except KeyboardInterrupt:
            print("\n⏹️ 测试被用户中断")
        except Exception as e:
            print(f"\n💥 测试过程中发生错误: {e}")
            import traceback
            traceback.print_exc()

        total_time = time.time() - start_time
        print(f"\n⏱️ 总测试时间: {total_time:.2f}秒")

        self.generate_report()


def main():
    """主函数"""
    print("🧪 修复版优化器测试套件")
    print("=" * 60)

    try:
        test_suite = FixedOptimizerTestSuite()
        test_suite.run_all_tests()

    except Exception as e:
        print(f"💥 测试初始化失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()