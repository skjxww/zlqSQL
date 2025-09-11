from typing import List, Dict, Any, Optional
from sql_compiler.optimizer.statistics import StatisticsManager
from sql_compiler.optimizer.cost_model import CostModel, SystemParameters
from sql_compiler.optimizer.plan_enumerator import AdvancedPlanEnumerator, PlanSpace
from sql_compiler.optimizer.simple_optimizer import SimpleQueryOptimizer
from sql_compiler.codegen.operators import *
from sql_compiler.parser.ast_nodes import *


class AdvancedQueryOptimizer:
    """高级查询优化器 - 智能分层保护策略"""

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
        """主优化入口 - 智能分层保护"""
        if not self.silent_mode:
            print("\n🚀 高级查询优化器启动")

        try:
            # 分析查询特征
            query_analysis = self._analyze_query_characteristics(plan)

            if not self.silent_mode:
                print(f"📊 查询分析:")
                print(f"   安全级别: {query_analysis['optimization_safety_level']}")
                print(f"   聚合复杂度: {query_analysis['aggregation_complexity']}")
                print(f"   复杂度评分: {query_analysis['complexity_score']}")

            # 第一阶段：安全的逻辑优化（对所有查询都适用）
            if not self.silent_mode:
                print("📋 阶段1: 安全逻辑优化")

            logical_optimized = self._apply_safe_logical_optimizations(plan, query_analysis)

            # 第二阶段：有选择性的物理优化
            if self.enable_cost_based_optimization:
                if not self.silent_mode:
                    print("💰 阶段2: 智能物理优化")

                cost_optimized = self._apply_selective_physical_optimizations(logical_optimized, query_analysis)
            else:
                cost_optimized = logical_optimized

            # 第三阶段：最终调优（根据查询类型选择策略）
            if not self.silent_mode:
                print("🔧 阶段3: 自适应最终优化")

            final_optimized = self._apply_adaptive_final_optimizations(cost_optimized, query_analysis)

            # 最终验证
            final_optimized = self._final_safety_check(plan, final_optimized, query_analysis)

            # 输出优化统计
            if not self.silent_mode:
                self._print_optimization_summary(plan, final_optimized)

            return final_optimized

        except Exception as e:
            if not self.silent_mode:
                print(f"⚠️ 高级优化失败: {e}, 回退到安全优化")
            return self._apply_safe_fallback_optimization(plan)

    def _analyze_query_characteristics(self, plan: Operator) -> Dict[str, Any]:
        """分析查询特征，制定优化策略"""
        analysis = {
            'has_aggregation': self._has_aggregation(plan),
            'has_group_by': self._contains_group_by_operator(plan),
            'has_having': self._has_having_clause(plan),
            'has_joins': self._has_joins_in_plan(plan),
            'has_subqueries': self._has_subquery(plan),
            'table_count': len(self._extract_tables(plan)),
            'complexity_score': self._calculate_complexity_score(plan),
            'aggregation_complexity': self._analyze_aggregation_complexity(plan),
            'optimization_safety_level': 'HIGH'  # 默认高安全级别
        }

        # 确定优化安全级别
        analysis['optimization_safety_level'] = self._determine_safety_level(analysis)

        return analysis

    def _determine_safety_level(self, analysis: Dict[str, Any]) -> str:
        """确定优化安全级别"""
        if analysis['has_having'] and analysis['has_aggregation']:
            return 'VERY_HIGH'  # 非常高：有HAVING的聚合查询
        elif analysis['has_aggregation'] and analysis['complexity_score'] > 8:
            return 'HIGH'  # 高：复杂聚合查询
        elif analysis['has_aggregation']:
            return 'MEDIUM'  # 中：简单聚合查询
        elif analysis['has_joins'] and analysis['table_count'] > 3:
            return 'MEDIUM'  # 中：复杂连接查询
        else:
            return 'LOW'  # 低：简单查询

    def _has_having_clause(self, plan: Operator) -> bool:
        """检查是否包含HAVING子句"""
        if isinstance(plan, GroupByOp) and plan.having_condition:
            return True

        for child in plan.children:
            if self._has_having_clause(child):
                return True

        return False

    def _has_joins_in_plan(self, plan: Operator) -> bool:
        """检查计划中是否有连接"""
        return self._count_joins(plan) > 0

    def _analyze_aggregation_complexity(self, plan: Operator) -> str:
        """分析聚合复杂度"""
        if not self._has_aggregation(plan):
            return 'NONE'

        has_having = self._has_having_clause(plan)
        has_multiple_groups = self._has_multiple_group_columns(plan)

        if has_having and has_multiple_groups:
            return 'COMPLEX'
        elif has_having or has_multiple_groups:
            return 'MODERATE'
        else:
            return 'SIMPLE'

    def _has_multiple_group_columns(self, plan: Operator) -> bool:
        """检查是否有多个分组列"""
        if isinstance(plan, GroupByOp):
            return len(plan.group_columns) > 1

        for child in plan.children:
            if self._has_multiple_group_columns(child):
                return True

        return False

    def _calculate_complexity_score(self, plan: Operator) -> int:
        """计算查询复杂度分数"""
        score = 0

        # 基础分数
        score += len(self._extract_tables(plan))

        # 连接复杂度
        score += self._count_joins(plan) * 2

        # 聚合复杂度
        if self._has_aggregation(plan):
            score += 3
            if self._has_having_clause(plan):
                score += 2
            if self._has_multiple_group_columns(plan):
                score += 1

        # 子查询复杂度
        if self._has_subquery(plan):
            score += 4

        return score

    def _apply_safe_logical_optimizations(self, plan: Operator, analysis: Dict[str, Any]) -> Operator:
        """应用安全的逻辑优化"""
        optimized = plan

        safety_level = analysis['optimization_safety_level']

        if not self.silent_mode:
            print(f"   🛡️ 安全级别: {safety_level}")

        # 1. 常量折叠（对所有查询都安全）
        optimized = self._apply_constant_folding(optimized)

        # 2. 基础规则优化（根据安全级别决定）
        if safety_level in ['LOW', 'MEDIUM']:
            # 应用完整的规则优化
            optimized = self.rule_optimizer.optimize(optimized)

            # 验证聚合查询是否被破坏
            if analysis['has_group_by'] and not self._contains_group_by_operator(optimized):
                if not self.silent_mode:
                    print("   ⚠️ 规则优化移除了GROUP BY，恢复原计划")
                optimized = plan

        elif safety_level == 'HIGH':
            # 应用保守的规则优化
            optimized = self._apply_conservative_rule_optimization(optimized, analysis)

        # VERY_HIGH 级别只做最基本的优化
        elif safety_level == 'VERY_HIGH':
            if not self.silent_mode:
                print("   🔒 极度保守模式：跳过规则优化")

        return optimized

    def _apply_conservative_rule_optimization(self, plan: Operator, analysis: Dict[str, Any]) -> Operator:
        """保守的规则优化"""
        # 只应用不会影响聚合结果的优化
        optimized = plan

        # 1. 谓词合并
        optimized = self._apply_predicate_merge(optimized)

        # 2. 常量折叠
        optimized = self._apply_constant_folding(optimized)

        return optimized

    def _apply_selective_physical_optimizations(self, plan: Operator, analysis: Dict[str, Any]) -> Operator:
        """有选择性地应用物理优化"""
        safety_level = analysis['optimization_safety_level']

        if safety_level == 'VERY_HIGH':
            if not self.silent_mode:
                print("   🚫 跳过物理优化（VERY_HIGH安全级别）")
            return plan

        if safety_level == 'HIGH':
            if not self.silent_mode:
                print("   ⚠️ 保守物理优化（HIGH安全级别）")
            return self._apply_conservative_physical_optimization(plan, analysis)

        # MEDIUM 和 LOW 级别可以应用更多优化
        if not self.silent_mode:
            print("   🚀 标准物理优化")

        # 但仍要检查复杂度
        if self._is_complex_query(plan):
            return self._cost_based_optimization(plan)
        else:
            if not self.silent_mode:
                print("   ⏭️ 查询过于简单，跳过成本优化")
            return plan

    def _apply_conservative_physical_optimization(self, plan: Operator, analysis: Dict[str, Any]) -> Operator:
        """保守的物理优化"""
        # 对于有聚合但不太复杂的查询，只做基本的物理优化
        return plan

    def _apply_adaptive_final_optimizations(self, plan: Operator, analysis: Dict[str, Any]) -> Operator:
        """自适应最终优化"""
        safety_level = analysis['optimization_safety_level']

        if not self.silent_mode:
            print(f"   🎯 自适应优化（安全级别: {safety_level}）")

        if safety_level == 'VERY_HIGH':
            # 最保守的优化
            return self._apply_minimal_final_optimization(plan)
        elif safety_level == 'HIGH':
            # 保守优化
            return self._apply_conservative_final_optimization(plan, analysis)
        else:
            # 标准优化
            return self._apply_standard_final_optimization(plan, analysis)

    def _apply_minimal_final_optimization(self, plan: Operator) -> Operator:
        """最小化最终优化"""
        # 只做最基本的清理
        if not self.silent_mode:
            print("   🔒 最小化优化：只做基本清理")
        return self._apply_redundancy_elimination(plan)

    def _apply_conservative_final_optimization(self, plan: Operator, analysis: Dict[str, Any]) -> Operator:
        """保守的最终优化"""
        if not self.silent_mode:
            print("   🛡️ 保守优化：保护聚合结构")

        optimized = plan

        # 1. 谓词合并（安全）
        optimized = self._apply_predicate_merge(optimized)

        # 2. 非常保守的投影消除
        if analysis['has_group_by']:
            optimized = self._apply_safe_projection_elimination(optimized)

        # 3. 冗余消除
        optimized = self._apply_redundancy_elimination(optimized)

        return optimized

    def _apply_standard_final_optimization(self, plan: Operator, analysis: Dict[str, Any]) -> Operator:
        """标准最终优化"""
        if not self.silent_mode:
            print("   🚀 标准优化：完整优化流程")

        return self._final_optimization(plan, preserve_groupby=analysis['has_group_by'])

    def _final_safety_check(self, original_plan: Operator, optimized_plan: Operator,
                            analysis: Dict[str, Any]) -> Operator:
        """最终安全检查"""
        # 确保关键结构没有被破坏
        if analysis['has_group_by'] and not self._contains_group_by_operator(optimized_plan):
            if not self.silent_mode:
                print("🚨 最终安全检查失败：GROUP BY丢失，恢复原计划")
            return original_plan

        if analysis['has_having'] and not self._has_having_clause(optimized_plan):
            if not self.silent_mode:
                print("🚨 最终安全检查失败：HAVING丢失，恢复原计划")
            return original_plan

        return optimized_plan

    def _apply_safe_fallback_optimization(self, plan: Operator) -> Operator:
        """安全的回退优化"""
        try:
            # 只应用最安全的优化
            if not self.silent_mode:
                print("   🆘 回退到最安全的优化")
            optimized = self._apply_constant_folding(plan)
            optimized = self._apply_redundancy_elimination(optimized)
            return optimized
        except:
            return plan

    def _apply_constant_folding(self, plan: Operator) -> Operator:
        """常量折叠优化"""
        # 简化实现：在实际项目中需要遍历表达式树进行常量计算
        return plan

    # === 保持原有的所有其他方法不变 ===
    def _is_aggregation_query(self, plan: Operator) -> bool:
        """检查是否是聚合查询"""
        return self._contains_group_by_operator(plan) or self._contains_aggregate_functions(plan)

    def _contains_group_by_operator(self, plan: Operator) -> bool:
        """检查是否包含GroupByOp操作符"""
        if isinstance(plan, GroupByOp):
            return True

        for child in plan.children:
            if self._contains_group_by_operator(child):
                return True

        return False

    def _contains_aggregate_functions(self, plan: Operator) -> bool:
        """检查是否包含聚合函数"""
        if isinstance(plan, ProjectOp):
            for column in plan.columns:
                if isinstance(column, str) and self._is_aggregate_column(column):
                    return True

        for child in plan.children:
            if self._contains_aggregate_functions(child):
                return True

        return False

    def _final_optimization(self, plan: Operator, preserve_groupby: bool = False) -> Operator:
        """最终优化调整 - 增加GROUP BY保护"""
        if preserve_groupby and not self.silent_mode:
            print("   🛡️ GROUP BY保护模式启用")

        optimized = plan

        # 应用最终的优化规则（但要保护GROUP BY）
        optimized = self._apply_predicate_merge(optimized)

        # 谨慎应用投影消除
        if preserve_groupby:
            # 对聚合查询使用保护性的投影消除
            optimized = self._apply_safe_projection_elimination(optimized)
        else:
            optimized = self._apply_projection_elimination(optimized)

        optimized = self._apply_redundancy_elimination(optimized)

        return optimized

    def _apply_safe_projection_elimination(self, plan: Operator) -> Operator:
        """安全的投影消除 - 保护包含HAVING的GROUP BY"""
        try:
            if isinstance(plan, ProjectOp):
                if len(plan.children) == 1:
                    child = plan.children[0]

                    # 🔑 特别保护包含HAVING条件的GroupByOp
                    if isinstance(child, GroupByOp):
                        # 如果GroupByOp包含HAVING条件，绝对不要修改其结构
                        if child.having_condition:
                            if not self.silent_mode:
                                print("   🛡️ 保护包含HAVING的GROUP BY操作")
                            # 只递归处理更深层的子节点
                            fixed_children = []
                            for grandchild in child.children:
                                fixed_children.append(self._apply_safe_projection_elimination(grandchild))

                            if fixed_children != child.children:
                                new_group_by = GroupByOp(child.group_columns, child.having_condition, fixed_children)
                                return ProjectOp(plan.columns, [new_group_by])

                            return plan  # 保持完整结构
                        else:
                            # 没有HAVING条件的GroupByOp处理
                            # 检查投影列是否与分组列匹配
                            if set(plan.columns) == set(child.group_columns):
                                # 投影列与分组列完全匹配，可以消除投影
                                if not self.silent_mode:
                                    print("   ✅ 消除冗余投影（GROUP BY列匹配）")
                                # 递归处理子节点后返回子节点
                                fixed_children = []
                                for grandchild in child.children:
                                    fixed_children.append(self._apply_safe_projection_elimination(grandchild))

                                if fixed_children != child.children:
                                    return GroupByOp(child.group_columns, child.having_condition, fixed_children)
                                return child
                            else:
                                # 投影列包含聚合函数或其他列，保持投影
                                if not self.silent_mode:
                                    print("   ℹ️ 保持投影（包含聚合函数或额外列）")
                                # 递归处理子节点
                                fixed_children = []
                                for grandchild in child.children:
                                    fixed_children.append(self._apply_safe_projection_elimination(grandchild))

                                if fixed_children != child.children:
                                    new_group_by = GroupByOp(child.group_columns, child.having_condition,
                                                             fixed_children)
                                    return ProjectOp(plan.columns, [new_group_by])
                                return plan

            # 递归处理其他情况
            new_children = []
            changed = False
            for child in plan.children:
                new_child = self._apply_safe_projection_elimination(child)
                new_children.append(new_child)
                if new_child != child:
                    changed = True

            if changed:
                return self._clone_operator(plan, new_children)

        except Exception:
            pass

        return plan

    def _apply_projection_elimination(self, plan: Operator) -> Operator:
        """消除不必要的投影操作 - 增强GROUP BY保护"""
        try:
            if isinstance(plan, ProjectOp):
                if len(plan.children) == 1:
                    child = plan.children[0]

                    # 强化GROUP BY保护
                    if isinstance(child, (GroupByOp, HashAggregateOp, SortAggregateOp)):
                        # 聚合操作后的投影通常是必要的，不要消除
                        # 只递归优化更深层的节点
                        fixed_children = []
                        for grandchild in child.children:
                            fixed_children.append(self._apply_projection_elimination(grandchild))

                        if fixed_children != child.children:
                            # 重建聚合操作
                            if isinstance(child, GroupByOp):
                                new_agg_op = GroupByOp(child.group_columns, child.having_condition, fixed_children)
                            elif isinstance(child, HashAggregateOp):
                                new_agg_op = HashAggregateOp(child.group_columns, child.agg_functions,
                                                             child.having_condition, fixed_children)
                            else:  # SortAggregateOp
                                new_agg_op = SortAggregateOp(child.group_columns, child.agg_functions,
                                                             child.having_condition, fixed_children)

                            return ProjectOp(plan.columns, [new_agg_op])

                        return plan  # 保持原结构

                    # 处理其他情况的投影合并
                    if isinstance(child, ProjectOp):
                        if self._can_merge_projections(plan.columns, child.columns):
                            return ProjectOp(plan.columns, child.children)

                    # SELECT * 的优化
                    if (plan.columns == ['*'] and
                            isinstance(child, (SeqScanOp, IndexScanOp))):
                        return child

            # 递归处理子节点
            new_children = []
            for child in plan.children:
                new_children.append(self._apply_projection_elimination(child))

            if new_children != plan.children:
                return self._clone_operator(plan, new_children)

        except Exception as e:
            if not self.silent_mode:
                print(f"   ⚠️ 投影消除出错: {e}")

        return plan

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
        """克隆操作符并使用新的子节点 - 增强GROUP BY支持"""
        try:
            if isinstance(original, FilterOp):
                return FilterOp(original.condition, new_children)
            elif isinstance(original, ProjectOp):
                return ProjectOp(original.columns, new_children)
            elif isinstance(original, GroupByOp):
                return GroupByOp(
                    group_columns=original.group_columns,
                    having_condition=original.having_condition,
                    children=new_children,
                    aggregate_functions=original.aggregate_functions
                )
            elif isinstance(original, JoinOp):
                return JoinOp(original.join_type, original.on_condition, new_children)
            elif isinstance(original, NestedLoopJoinOp):
                return NestedLoopJoinOp(original.join_type, original.on_condition, new_children)
            elif isinstance(original, HashJoinOp):
                return HashJoinOp(original.join_type, original.on_condition, new_children)
            elif isinstance(original, SortMergeJoinOp):
                return SortMergeJoinOp(original.join_type, original.on_condition, new_children)
            elif isinstance(original, OrderByOp):
                return OrderByOp(original.order_columns, new_children)
            else:
                # 对于未知类型，尝试保持原有属性
                new_op = type(original)(new_children)
                # 复制关键属性
                for attr in ['group_columns', 'having_condition', 'order_columns', 'table_name', 'columns',
                             'condition']:
                    if hasattr(original, attr):
                        setattr(new_op, attr, getattr(original, attr))
                return new_op
        except Exception:
            pass

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