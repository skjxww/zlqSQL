from typing import List, Dict, Any, Optional
from sql_compiler.optimizer.statistics import StatisticsManager
from sql_compiler.optimizer.cost_model import CostModel, SystemParameters
from sql_compiler.optimizer.plan_enumerator import AdvancedPlanEnumerator, PlanSpace
from sql_compiler.optimizer.simple_optimizer import SimpleQueryOptimizer
from sql_compiler.codegen.operators import *
from sql_compiler.parser.ast_nodes import *


class AdvancedQueryOptimizer:
    """高级查询优化器"""

    def __init__(self, stats_manager: StatisticsManager = None, silent_mode: bool = False):
        self.stats_manager = stats_manager or StatisticsManager()
        self.silent_mode = silent_mode

        # 成本模型
        self.system_params = SystemParameters()
        self.cost_model = CostModel(self.stats_manager, self.system_params)

        # 计划枚举器
        self.plan_enumerator = AdvancedPlanEnumerator(self.cost_model)

        # 简单优化器（用于基础规则优化）
        self.rule_optimizer = SimpleQueryOptimizer(silent_mode)

        # 优化开关
        self.enable_cost_based_optimization = True
        self.enable_advanced_enumeration = True
        self.enable_statistics = True

    def optimize(self, plan: Operator) -> Operator:
        """主优化入口"""
        if not self.silent_mode:
            print("\n🚀 高级查询优化器启动")

        try:
            # 第一阶段：基于规则的逻辑优化
            if not self.silent_mode:
                print("📋 阶段1: 逻辑优化（基于规则）")

            logical_optimized = self.rule_optimizer.optimize(plan)

            # 第二阶段：基于成本的优化（仅对复杂查询）
            if (self.enable_cost_based_optimization and
                    self._is_complex_query(logical_optimized)):

                if not self.silent_mode:
                    print("💰 阶段2: 物理优化（基于成本）")

                cost_optimized = self._cost_based_optimization(logical_optimized)
            else:
                cost_optimized = logical_optimized

            # 第三阶段：最终优化调整
            if not self.silent_mode:
                print("🔧 阶段3: 最终优化调整")

            final_optimized = self._final_optimization(cost_optimized)

            # 输出优化统计
            if not self.silent_mode:
                self._print_optimization_summary(plan, final_optimized)

            return final_optimized

        except Exception as e:
            if not self.silent_mode:
                print(f"⚠️ 高级优化失败: {e}, 回退到规则优化")
            return self.rule_optimizer.optimize(plan)

    def _is_complex_query(self, plan: Operator) -> bool:
        """判断是否为复杂查询（加权评分系统）"""
        complexity_score = 0

        # 1. 连接操作（每个连接+2分）
        join_count = self._count_joins(plan)
        complexity_score += join_count * 2

        # 2. 表数量（第3个表开始+1分）
        table_count = len(self._extract_tables(plan))
        if table_count > 2:
            complexity_score += (table_count - 2)

        # 3. 聚合操作（+3分）
        if self._has_aggregation(plan):
            complexity_score += 3

        # 4. 子查询（+4分）
        if self._has_subquery(plan):
            complexity_score += 4

        # 5. 排序/分组（+2分）
        if self._has_order_by(plan) or self._has_group_by(plan):
            complexity_score += 2

        # 6. 复杂条件（OR条件、函数调用等，+2分）
        if self._has_complex_conditions(plan):
            complexity_score += 2

        # 阈值判断
        return complexity_score >= 5

    def _has_subquery(self, plan: Operator) -> bool:
        """检查执行计划中是否包含子查询"""
        try:
            # 检查当前操作符是否是子查询相关
            if isinstance(plan, (SubqueryOp, InOp)):
                return True

            # 递归检查子节点
            for child in plan.children:
                if self._has_subquery(child):
                    return True

            return False
        except Exception:
            return False

    def _has_order_by(self, plan: Operator) -> bool:
        """检查执行计划中是否包含排序操作"""
        try:
            # 检查当前操作符是否是排序相关
            if isinstance(plan, (OrderByOp, QuickSortOp, ExternalSortOp)):
                return True

            # 递归检查子节点
            for child in plan.children:
                if self._has_order_by(child):
                    return True

            return False
        except Exception:
            return False

    def _has_group_by(self, plan: Operator) -> bool:
        """检查执行计划中是否包含分组操作"""
        try:
            # 检查当前操作符是否是分组相关
            if isinstance(plan, (GroupByOp, HashAggregateOp, SortAggregateOp)):
                return True

            # 递归检查子节点
            for child in plan.children:
                if self._has_group_by(child):
                    return True

            return False
        except Exception:
            return False

    def _has_complex_conditions(self, plan: Operator) -> bool:
        """检查执行计划中是否包含复杂条件"""
        try:
            # 检查过滤操作符的复杂性
            if isinstance(plan, FilterOp):
                return self._is_complex_condition(plan.condition)

            # 检查连接操作符的复杂性
            if isinstance(plan, (JoinOp, NestedLoopJoinOp, HashJoinOp, SortMergeJoinOp)):
                if hasattr(plan, 'on_condition') and plan.on_condition:
                    return self._is_complex_condition(plan.on_condition)

            # 递归检查子节点
            for child in plan.children:
                if self._has_complex_conditions(child):
                    return True

            return False
        except Exception:
            return False

    def _is_complex_condition(self, condition) -> bool:
        """判断条件是否复杂"""
        try:
            if not condition:
                return False

            # 检查是否是二元表达式
            if hasattr(condition, 'operator'):
                # OR 条件被认为是复杂的
                if condition.operator.upper() == 'OR':
                    return True

                # 嵌套的 AND 条件也可能复杂
                if condition.operator.upper() == 'AND':
                    # 递归检查左右操作数
                    left_complex = self._is_complex_condition(getattr(condition, 'left', None))
                    right_complex = self._is_complex_condition(getattr(condition, 'right', None))
                    return left_complex or right_complex

            # 检查是否包含函数调用
            if hasattr(condition, 'function_name'):
                return True

            # 检查是否是 IN 操作（通常较复杂）
            if hasattr(condition, 'operator') and condition.operator.upper() in ['IN', 'NOT IN']:
                return True

            # 检查是否包含子查询
            if hasattr(condition, 'subquery'):
                return True

            # LIKE 操作（特别是通配符较多时）
            if hasattr(condition, 'operator') and condition.operator.upper() in ['LIKE', 'NOT LIKE']:
                return True

            return False
        except Exception:
            return False

    def _has_aggregation(self, plan: Operator) -> bool:
        """检查执行计划中是否包含聚合操作"""
        try:
            # 检查当前操作符是否是聚合相关
            if isinstance(plan, (GroupByOp, HashAggregateOp, SortAggregateOp)):
                return True

            # 检查投影操作中是否有聚合函数
            if isinstance(plan, ProjectOp):
                for column in plan.columns:
                    if self._is_aggregate_column(column):
                        return True

            # 递归检查子节点
            for child in plan.children:
                if self._has_aggregation(child):
                    return True

            return False
        except Exception:
            return False

    def _is_aggregate_column(self, column_expr) -> bool:
        """检查列表达式是否是聚合函数"""
        try:
            # 如果是字符串形式的聚合函数
            if isinstance(column_expr, str):
                aggregate_patterns = ['COUNT(', 'SUM(', 'AVG(', 'MIN(', 'MAX(', 'GROUP_CONCAT(']
                column_upper = column_expr.upper()
                return any(pattern in column_upper for pattern in aggregate_patterns)

            # 如果是表达式对象
            if hasattr(column_expr, 'function_name'):
                aggregate_functions = {'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT'}
                return column_expr.function_name.upper() in aggregate_functions

            return False
        except Exception:
            return False

    def _count_joins(self, plan: Operator) -> int:
        """统计计划中的连接数量"""
        if isinstance(plan, (JoinOp, NestedLoopJoinOp, HashJoinOp, SortMergeJoinOp)):
            count = 1
        else:
            count = 0

        for child in plan.children:
            count += self._count_joins(child)

        return count

    def _extract_tables(self, plan: Operator) -> set:
        """提取计划中涉及的所有表"""
        tables = set()

        if hasattr(plan, 'table_name') and plan.table_name:
            tables.add(plan.table_name)

        for child in plan.children:
            tables.update(self._extract_tables(child))

        return tables

    def _cost_based_optimization(self, plan: Operator) -> Operator:
        """基于成本的优化"""
        try:
            # 提取查询信息
            plan_space = self._extract_plan_space(plan)

            # 枚举所有可能的计划
            if not self.silent_mode:
                print(f"   🔍 枚举执行计划（{len(plan_space.tables)}个表）...")

            all_plans = self.plan_enumerator.enumerate_plans(plan_space)

            if not all_plans:
                if not self.silent_mode:
                    print("   ⚠️ 未找到可行的执行计划，使用原计划")
                return plan

            # 选择成本最低的计划
            best_plan, best_cost = min(all_plans, key=lambda x: x[1])

            if not self.silent_mode:
                original_cost = self.cost_model.calculate_cost(plan)['total_cost']
                improvement = ((original_cost - best_cost) / original_cost * 100) if original_cost > 0 else 0
                print(f"   ✅ 找到{len(all_plans)}个候选计划，最优成本: {best_cost:.2f}")
                print(f"   📈 性能提升: {improvement:.1f}%")

            return best_plan

        except Exception as e:
            if not self.silent_mode:
                print(f"   ⚠️ 基于成本的优化失败: {e}")
            return plan

    def _extract_plan_space(self, plan: Operator) -> PlanSpace:
        """从执行计划中提取查询空间信息"""
        tables = list(self._extract_tables(plan))

        # 简化：提取连接条件和过滤条件
        join_conditions = self._extract_join_conditions(plan)
        filters = self._extract_filters(plan)
        projections = self._extract_projections(plan)

        return PlanSpace(tables, join_conditions, filters, projections)

    def _extract_join_conditions(self, plan: Operator) -> List[Any]:
        """提取连接条件"""
        conditions = []

        if isinstance(plan, (JoinOp, NestedLoopJoinOp, HashJoinOp, SortMergeJoinOp)):
            if hasattr(plan, 'on_condition') and plan.on_condition:
                conditions.append(plan.on_condition)
            elif hasattr(plan, 'condition') and plan.condition:
                conditions.append(plan.condition)

        for child in plan.children:
            conditions.extend(self._extract_join_conditions(child))

        return conditions

    def _extract_filters(self, plan: Operator) -> List[Any]:
        """提取过滤条件"""
        filters = []

        if isinstance(plan, FilterOp) and plan.condition:
            filters.append(plan.condition)

        for child in plan.children:
            filters.extend(self._extract_filters(child))

        return filters

    def _extract_projections(self, plan: Operator) -> List[str]:
        """提取投影列"""
        if isinstance(plan, ProjectOp):
            return plan.columns

        for child in plan.children:
            projections = self._extract_projections(child)
            if projections:
                return projections

        return ['*']

    def _final_optimization(self, plan: Operator) -> Operator:
        """最终优化调整"""
        optimized = plan

        # 应用一些最终的优化规则
        optimized = self._apply_predicate_merge(optimized)
        optimized = self._apply_projection_elimination(optimized)
        optimized = self._apply_redundancy_elimination(optimized)

        return optimized

    def _apply_predicate_merge(self, plan: Operator) -> Operator:
        """合并相邻的谓词"""
        if isinstance(plan, FilterOp):
            if (len(plan.children) == 1 and
                    isinstance(plan.children[0], FilterOp)):
                # 合并两个相邻的Filter
                inner_filter = plan.children[0]
                merged_condition = BinaryExpr(plan.condition, 'AND', inner_filter.condition)
                return FilterOp(merged_condition, inner_filter.children)

        # 递归处理子节点
        new_children = []
        for child in plan.children:
            new_children.append(self._apply_predicate_merge(child))

        if new_children != plan.children:
            return self._clone_operator(plan, new_children)

        return plan

    def _apply_projection_elimination(self, plan: Operator) -> Operator:
        """消除不必要的投影操作"""
        if isinstance(plan, ProjectOp):
            if len(plan.children) == 1:
                child = plan.children[0]

                # 如果子节点也是投影，合并它们
                if isinstance(child, ProjectOp):
                    # 检查是否可以合并投影
                    if self._can_merge_projections(plan.columns, child.columns):
                        return ProjectOp(plan.columns, child.children)

                # 如果投影是SELECT *且子节点输出相同，则消除
                if (plan.columns == ['*'] and
                        isinstance(child, (SeqScanOp, IndexScanOp))):
                    return child

        # 递归处理子节点
        new_children = []
        for child in plan.children:
            new_children.append(self._apply_projection_elimination(child))

        if new_children != plan.children:
            return self._clone_operator(plan, new_children)

        return plan

    def _apply_redundancy_elimination(self, plan: Operator) -> Operator:
        """消除冗余操作"""
        # 消除不必要的排序
        if isinstance(plan, OrderByOp):
            if (len(plan.children) == 1 and
                    isinstance(plan.children[0], OrderByOp)):
                # 两个相邻的排序，保留外层的
                inner_sort = plan.children[0]
                return OrderByOp(plan.order_columns, inner_sort.children)

        # 递归处理子节点
        new_children = []
        for child in plan.children:
            new_children.append(self._apply_redundancy_elimination(child))

        if new_children != plan.children:
            return self._clone_operator(plan, new_children)

        return plan

    def _can_merge_projections(self, outer_columns: List[str], inner_columns: List[str]) -> bool:
        """检查是否可以合并两个投影"""
        # 简化实现：如果外层投影的列都在内层投影中，则可以合并
        if outer_columns == ['*']:
            return True

        if inner_columns == ['*']:
            return True

        return set(outer_columns).issubset(set(inner_columns))

    def _clone_operator(self, original: Operator, new_children: List[Operator]) -> Operator:
        """克隆操作符并使用新的子节点"""
        if isinstance(original, FilterOp):
            return FilterOp(original.condition, new_children)
        elif isinstance(original, ProjectOp):
            return ProjectOp(original.columns, new_children)
        elif isinstance(original, JoinOp):
            return JoinOp(original.join_type, original.on_condition, new_children)
        elif isinstance(original, NestedLoopJoinOp):
            return NestedLoopJoinOp(original.join_type, original.on_condition, new_children)
        elif isinstance(original, HashJoinOp):
            return HashJoinOp(original.join_type, original.on_condition, new_children)
        elif isinstance(original, SortMergeJoinOp):
            return SortMergeJoinOp(original.join_type, original.on_condition, new_children)
        elif isinstance(original, GroupByOp):
            return GroupByOp(original.group_columns, original.having_condition, new_children)
        elif isinstance(original, OrderByOp):
            return OrderByOp(original.order_columns, new_children)
        else:
            return original

    def _print_optimization_summary(self, original_plan: Operator, optimized_plan: Operator):
        """打印优化总结"""
        print("\n📊 优化总结:")

        # 计算成本对比
        try:
            original_cost = self.cost_model.calculate_cost(original_plan)
            optimized_cost = self.cost_model.calculate_cost(optimized_plan)

            print(f"   原始计划成本: {original_cost['total_cost']:.2f}")
            print(f"   优化后成本: {optimized_cost['total_cost']:.2f}")

            if original_cost['total_cost'] > 0:
                improvement = ((original_cost['total_cost'] - optimized_cost['total_cost']) /
                               original_cost['total_cost'] * 100)
                print(f"   性能提升: {improvement:.1f}%")

            print(f"   预估处理行数: {optimized_cost['rows']:.0f}")
            print(f"   预估行宽度: {optimized_cost['width']:.0f} 字节")

        except Exception as e:
            print(f"   成本计算失败: {e}")

        # 分析执行计划结构
        original_ops = self._count_operators(original_plan)
        optimized_ops = self._count_operators(optimized_plan)

        print(f"   原始计划操作数: {sum(original_ops.values())}")
        print(f"   优化后操作数: {sum(optimized_ops.values())}")

        # 显示主要优化技术
        optimizations = []
        if 'OptimizedSeqScanOp' in [type(op).__name__ for op in self._get_all_operators(optimized_plan)]:
            optimizations.append("投影下推")
        if 'FilteredSeqScanOp' in [type(op).__name__ for op in self._get_all_operators(optimized_plan)]:
            optimizations.append("谓词下推")
        if 'HashJoinOp' in [type(op).__name__ for op in self._get_all_operators(optimized_plan)]:
            optimizations.append("哈希连接")
        if 'IndexScanOp' in [type(op).__name__ for op in self._get_all_operators(optimized_plan)]:
            optimizations.append("索引访问")

        if optimizations:
            print(f"   应用的优化技术: {', '.join(optimizations)}")

        print("🎯 优化完成\n")

    def _count_operators(self, plan: Operator) -> Dict[str, int]:
        """统计操作符数量"""
        counts = {}
        op_type = type(plan).__name__
        counts[op_type] = counts.get(op_type, 0) + 1

        for child in plan.children:
            child_counts = self._count_operators(child)
            for op, count in child_counts.items():
                counts[op] = counts.get(op, 0) + count

        return counts

    def _get_all_operators(self, plan: Operator) -> List[Operator]:
        """获取所有操作符"""
        operators = [plan]
        for child in plan.children:
            operators.extend(self._get_all_operators(child))
        return operators

    # 高级优化特性
    def enable_adaptive_optimization(self, enable: bool = True):
        """启用自适应优化"""
        self.adaptive_optimization = enable

    def enable_parallel_optimization(self, enable: bool = True):
        """启用并行优化"""
        self.parallel_optimization = enable

    def set_optimization_timeout(self, timeout_ms: int):
        """设置优化超时"""
        self.optimization_timeout = timeout_ms

    def add_custom_optimization_rule(self, rule_name: str, rule_func):
        """添加自定义优化规则"""
        if not hasattr(self, 'custom_rules'):
            self.custom_rules = {}
        self.custom_rules[rule_name] = rule_func


class QueryOptimizationPipeline:
    """查询优化流水线 - 管理整个优化流程"""

    def __init__(self, catalog_manager=None):
        self.catalog_manager = catalog_manager

        # 初始化统计信息管理器
        self.stats_manager = StatisticsManager()

        # 自动收集统计信息
        if catalog_manager:
            self._collect_initial_statistics()

        # 创建高级优化器
        self.optimizer = AdvancedQueryOptimizer(self.stats_manager)

        # 优化历史
        self.optimization_history = []

    def optimize(self, plan: Operator, query_context: Dict[str, Any] = None) -> Operator:
        """优化执行计划"""
        import time

        start_time = time.time()

        # 记录优化前状态
        original_cost = self._estimate_plan_cost(plan)

        # 执行优化
        try:
            optimized_plan = self.optimizer.optimize(plan)
            optimization_success = True
            error_message = None
        except Exception as e:
            optimized_plan = plan
            optimization_success = False
            error_message = str(e)

        # 记录优化历史
        optimization_time = time.time() - start_time
        optimized_cost = self._estimate_plan_cost(optimized_plan)

        history_entry = {
            'timestamp': time.time(),
            'original_cost': original_cost,
            'optimized_cost': optimized_cost,
            'optimization_time': optimization_time,
            'success': optimization_success,
            'error': error_message,
            'query_context': query_context or {}
        }

        self.optimization_history.append(history_entry)

        # 限制历史记录大小
        if len(self.optimization_history) > 100:
            self.optimization_history = self.optimization_history[-100:]

        return optimized_plan

    def _collect_initial_statistics(self):
        """收集初始统计信息"""
        try:
            all_tables = self.catalog_manager.get_all_tables()

            for table_name in all_tables:
                # 生成模拟统计信息
                self.stats_manager.analyze_table(table_name)

        except Exception as e:
            print(f"收集统计信息失败: {e}")

    def _estimate_plan_cost(self, plan: Operator) -> float:
        """估算执行计划成本"""
        try:
            cost_info = self.optimizer.cost_model.calculate_cost(plan)
            return cost_info['total_cost']
        except:
            return 1000.0  # 默认成本

    def get_optimization_statistics(self) -> Dict[str, Any]:
        """获取优化统计信息"""
        if not self.optimization_history:
            return {}

        successful_optimizations = [h for h in self.optimization_history if h['success']]

        if not successful_optimizations:
            return {'success_rate': 0.0}

        avg_improvement = sum(
            (h['original_cost'] - h['optimized_cost']) / h['original_cost']
            for h in successful_optimizations
            if h['original_cost'] > 0
        ) / len(successful_optimizations)

        avg_time = sum(h['optimization_time'] for h in successful_optimizations) / len(successful_optimizations)

        return {
            'total_optimizations': len(self.optimization_history),
            'success_rate': len(successful_optimizations) / len(self.optimization_history),
            'avg_improvement_percent': avg_improvement * 100,
            'avg_optimization_time_ms': avg_time * 1000,
            'recent_errors': [h['error'] for h in self.optimization_history[-10:] if not h['success']]
        }

    def reset_statistics(self):
        """重置统计信息"""
        self.optimization_history = []
        self.stats_manager = StatisticsManager()

