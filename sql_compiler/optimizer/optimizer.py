from typing import List, Dict, Any
from sql_compiler.parser.ast_nodes import *
from sql_compiler.codegen.operators import *


class QueryOptimizer:
    """查询优化器"""

    def __init__(self):
        self.optimization_rules = [
            PredicatePushdownRule(),
            ProjectionPushdownRule(),
            JoinReorderRule(),
            ConstantFoldingRule(),
            RedundantOperatorEliminationRule()
        ]

    def optimize(self, plan: Operator) -> Operator:
        """对执行计划进行优化"""
        print("\n=== 查询优化 ===")
        original_plan = plan

        # 应用所有优化规则
        optimized_plan = plan
        total_optimizations = 0

        for rule in self.optimization_rules:
            print(f"应用优化规则: {rule.__class__.__name__}")
            new_plan = rule.apply(optimized_plan)

            if new_plan != optimized_plan:
                optimizations = rule.get_optimization_count()
                print(f"  ✅ 应用了 {optimizations} 个优化")
                total_optimizations += optimizations
                optimized_plan = new_plan
            else:
                print(f"  ⏭️  无优化机会")

        print(f"总共应用了 {total_optimizations} 个优化")

        if total_optimizations > 0:
            print("优化前后对比:")
            print("原始计划复杂度:", self._calculate_complexity(original_plan))
            print("优化后复杂度:", self._calculate_complexity(optimized_plan))

        return optimized_plan

    def _calculate_complexity(self, plan: Operator) -> int:
        """计算执行计划的复杂度（简化版本）"""
        complexity = 1
        for child in plan.children:
            complexity += self._calculate_complexity(child)

        # 不同操作符有不同的复杂度权重
        if isinstance(plan, JoinOp):
            complexity *= 3  # JOIN 操作复杂度高
        elif isinstance(plan, FilterOp):
            complexity *= 1.5
        elif isinstance(plan, SeqScanOp):
            complexity *= 2

        return int(complexity)


class OptimizationRule:
    """优化规则基类"""

    def __init__(self):
        self.optimization_count = 0

    def apply(self, plan: Operator) -> Operator:
        """应用优化规则"""
        self.optimization_count = 0
        return self._apply_recursive(plan)

    def _apply_recursive(self, plan: Operator) -> Operator:
        """递归应用优化规则"""
        # 先优化子节点
        optimized_children = []
        for child in plan.children:
            optimized_child = self._apply_recursive(child)
            optimized_children.append(optimized_child)

        # 更新子节点
        plan.children = optimized_children

        # 应用当前规则
        return self._apply_rule(plan)

    def _apply_rule(self, plan: Operator) -> Operator:
        """应用具体的优化规则（子类实现）"""
        return plan

    def get_optimization_count(self) -> int:
        """获取优化次数"""
        return self.optimization_count


class PredicatePushdownRule(OptimizationRule):
    """谓词下推优化规则"""

    def _apply_rule(self, plan: Operator) -> Operator:
        """应用谓词下推规则"""
        if isinstance(plan, JoinOp):
            return self._pushdown_join_predicates(plan)
        elif isinstance(plan, FilterOp):
            return self._pushdown_filter_predicates(plan)
        return plan

    def _pushdown_join_predicates(self, join_op: JoinOp) -> Operator:
        """将JOIN条件中的谓词下推"""
        if not hasattr(join_op, 'condition') or not join_op.condition:
            return join_op

        # 分析JOIN条件，尝试将单表谓词下推到表扫描
        pushdown_conditions = self._extract_pushdown_conditions(join_op.condition)

        if pushdown_conditions:
            self.optimization_count += len(pushdown_conditions)
            print(f"    🎯 谓词下推: 将 {len(pushdown_conditions)} 个条件下推到表扫描")

            # 重构执行计划
            return self._reconstruct_with_pushdown(join_op, pushdown_conditions)

        return join_op

    def _pushdown_filter_predicates(self, filter_op: FilterOp) -> Operator:
        """将Filter条件下推到子操作"""
        if len(filter_op.children) == 1 and isinstance(filter_op.children[0], JoinOp):
            join_child = filter_op.children[0]

            # 尝试将Filter条件合并到JOIN条件中
            if self._can_merge_conditions(filter_op.condition, join_child):
                self.optimization_count += 1
                print(f"    🎯 谓词下推: 将Filter条件合并到JOIN中")
                return self._merge_filter_to_join(filter_op, join_child)

        return filter_op

    def _extract_pushdown_conditions(self, condition: Expression) -> List[Dict]:
        """从JOIN条件中提取可以下推的谓词"""
        # 简化实现：识别单表条件
        pushdown_conditions = []

        if isinstance(condition, BinaryExpr):
            if condition.operator in ['AND']:
                # 递归处理AND条件的左右子树
                pushdown_conditions.extend(self._extract_pushdown_conditions(condition.left))
                pushdown_conditions.extend(self._extract_pushdown_conditions(condition.right))
            elif condition.operator in ['=', '<', '>', '<=', '>=', '<>']:
                # 检查是否是单表谓词
                if self._is_single_table_predicate(condition):
                    pushdown_conditions.append({
                        'condition': condition,
                        'table': self._get_predicate_table(condition)
                    })

        return pushdown_conditions

    def _is_single_table_predicate(self, condition: BinaryExpr) -> bool:
        """检查是否是单表谓词"""
        # 简化实现：检查左右操作数是否都来自同一个表
        left_tables = self._get_expression_tables(condition.left)
        right_tables = self._get_expression_tables(condition.right)

        # 如果右边是字面量，左边是单表列，则可以下推
        if len(right_tables) == 0 and len(left_tables) == 1:
            return True

        return False

    def _get_expression_tables(self, expr: Expression) -> set:
        """获取表达式中涉及的表"""
        tables = set()

        if isinstance(expr, IdentifierExpr):
            if expr.table_name:
                tables.add(expr.table_name)
        elif isinstance(expr, BinaryExpr):
            tables.update(self._get_expression_tables(expr.left))
            tables.update(self._get_expression_tables(expr.right))

        return tables

    def _get_predicate_table(self, condition: BinaryExpr) -> str:
        """获取谓词涉及的表名"""
        if isinstance(condition.left, IdentifierExpr) and condition.left.table_name:
            return condition.left.table_name
        return "unknown"

    def _can_merge_conditions(self, filter_condition: Expression, join_op: JoinOp) -> bool:
        """检查Filter条件是否可以合并到JOIN中"""
        # 简化实现
        return True

    def _merge_filter_to_join(self, filter_op: FilterOp, join_op: JoinOp) -> Operator:
        """将Filter条件合并到JOIN中"""
        # 创建新的JOIN条件
        if hasattr(join_op, 'condition') and join_op.condition:
            # 合并条件
            new_condition = BinaryExpr(join_op.condition, 'AND', filter_op.condition)
        else:
            new_condition = filter_op.condition

        # 创建新的JOIN操作
        new_join = JoinOp(join_op.join_type, join_op.children)
        new_join.condition = new_condition

        return new_join

    def _reconstruct_with_pushdown(self, join_op: JoinOp, pushdown_conditions: List[Dict]) -> Operator:
        """重构带有谓词下推的执行计划"""
        # 简化实现：为每个子表添加Filter操作
        new_children = []

        for child in join_op.children:
            child_conditions = [pc['condition'] for pc in pushdown_conditions
                                if pc['table'] == self._get_child_table_name(child)]

            if child_conditions:
                # 添加Filter操作
                if len(child_conditions) == 1:
                    filter_condition = child_conditions[0]
                else:
                    # 多个条件用AND连接
                    filter_condition = child_conditions[0]
                    for cond in child_conditions[1:]:
                        filter_condition = BinaryExpr(filter_condition, 'AND', cond)

                filter_op = FilterOp(filter_condition, [child])
                new_children.append(filter_op)
            else:
                new_children.append(child)

        # 创建新的JOIN操作，移除已下推的条件
        new_join = JoinOp(join_op.join_type, new_children)
        remaining_condition = self._remove_pushed_conditions(join_op.condition, pushdown_conditions)
        if remaining_condition:
            new_join.condition = remaining_condition

        return new_join

    def _get_child_table_name(self, child: Operator) -> str:
        """获取子操作对应的表名"""
        if isinstance(child, SeqScanOp):
            return child.table_name
        return "unknown"

    def _remove_pushed_conditions(self, original_condition: Expression, pushed_conditions: List[Dict]) -> Expression:
        """从原始条件中移除已下推的条件"""
        # 简化实现：返回原始条件
        return original_condition


class ProjectionPushdownRule(OptimizationRule):
    """投影下推优化规则"""

    def _apply_rule(self, plan: Operator) -> Operator:
        """应用投影下推规则"""
        if isinstance(plan, ProjectOp):
            return self._pushdown_projection(plan)
        return plan

    def _pushdown_projection(self, project_op: ProjectOp) -> Operator:
        """下推投影操作"""
        if len(project_op.children) == 1:
            child = project_op.children[0]

            # 如果子操作是表扫描，可以直接在扫描时只读取需要的列
            if isinstance(child, SeqScanOp):
                self.optimization_count += 1
                print(f"    🎯 投影下推: 在表扫描时只读取需要的列")

                # 创建优化的表扫描操作
                optimized_scan = OptimizedSeqScanOp(
                    child.table_name,
                    selected_columns=project_op.columns
                )
                return optimized_scan

        return project_op


class JoinReorderRule(OptimizationRule):
    """JOIN重排序优化规则"""

    def _apply_rule(self, plan: Operator) -> Operator:
        """应用JOIN重排序规则"""
        if isinstance(plan, JoinOp) and len(plan.children) >= 2:
            return self._reorder_joins(plan)
        return plan

    def _reorder_joins(self, join_op: JoinOp) -> Operator:
        """重排序JOIN操作"""
        # 简化实现：基于表大小估计重排序
        children = join_op.children

        # 估计每个子操作的大小
        child_sizes = [(child, self._estimate_size(child)) for child in children]

        # 按大小排序，小表在前
        sorted_children = sorted(child_sizes, key=lambda x: x[1])

        if [child for child, _ in sorted_children] != children:
            self.optimization_count += 1
            print(f"    🎯 JOIN重排序: 将小表移到前面")

            new_join = JoinOp(join_op.join_type, [child for child, _ in sorted_children])
            if hasattr(join_op, 'condition'):
                new_join.condition = join_op.condition
            return new_join

        return join_op

    def _estimate_size(self, operator: Operator) -> int:
        """估计操作结果的大小"""
        if isinstance(operator, SeqScanOp):
            # 基于表名的简单估计
            return hash(operator.table_name) % 1000  # 简化的大小估计
        elif isinstance(operator, FilterOp):
            return self._estimate_size(operator.children[0]) // 2  # 假设过滤掉一半
        else:
            return 100  # 默认大小


class ConstantFoldingRule(OptimizationRule):
    """常量折叠优化规则"""

    def _apply_rule(self, plan: Operator) -> Operator:
        """应用常量折叠规则"""
        if isinstance(plan, FilterOp):
            return self._fold_filter_constants(plan)
        return plan

    def _fold_filter_constants(self, filter_op: FilterOp) -> Operator:
        """折叠Filter中的常量表达式"""
        if filter_op.condition:
            folded_condition = self._fold_expression_constants(filter_op.condition)
            if folded_condition != filter_op.condition:
                self.optimization_count += 1
                print(f"    🎯 常量折叠: 预计算常量表达式")

                new_filter = FilterOp(folded_condition, filter_op.children)
                return new_filter

        return filter_op

    def _fold_expression_constants(self, expr: Expression) -> Expression:
        """折叠表达式中的常量"""
        if isinstance(expr, BinaryExpr):
            left = self._fold_expression_constants(expr.left)
            right = self._fold_expression_constants(expr.right)

            # 如果左右都是字面量，尝试计算结果
            if isinstance(left, LiteralExpr) and isinstance(right, LiteralExpr):
                result = self._evaluate_constant_expression(left, expr.operator, right)
                if result is not None:
                    return LiteralExpr(result)

            if left != expr.left or right != expr.right:
                return BinaryExpr(left, expr.operator, right)

        return expr

    def _evaluate_constant_expression(self, left: LiteralExpr, operator: str, right: LiteralExpr):
        """计算常量表达式"""
        try:
            if operator == '+':
                return left.value + right.value
            elif operator == '-':
                return left.value - right.value
            elif operator == '*':
                return left.value * right.value
            elif operator == '/':
                if right.value != 0:
                    return left.value / right.value
            elif operator == '=' and left.value == right.value:
                return True
            elif operator == '<>' and left.value != right.value:
                return True
        except:
            pass

        return None


class RedundantOperatorEliminationRule(OptimizationRule):
    """冗余操作符消除规则"""

    def _apply_rule(self, plan: Operator) -> Operator:
        """应用冗余操作符消除规则"""
        if isinstance(plan, FilterOp):
            return self._eliminate_redundant_filters(plan)
        elif isinstance(plan, ProjectOp):
            return self._eliminate_redundant_projections(plan)
        return plan

    def _eliminate_redundant_filters(self, filter_op: FilterOp) -> Operator:
        """消除冗余的Filter操作"""
        # 如果Filter条件总是为真，可以消除该Filter
        if self._is_always_true(filter_op.condition):
            self.optimization_count += 1
            print(f"    🎯 冗余消除: 移除总是为真的Filter")
            return filter_op.children[0] if filter_op.children else filter_op

        return filter_op

    def _eliminate_redundant_projections(self, project_op: ProjectOp) -> Operator:
        """消除冗余的Projection操作"""
        # 如果投影包含所有列，且子操作也是投影，可以合并
        if len(project_op.children) == 1 and isinstance(project_op.children[0], ProjectOp):
            child_project = project_op.children[0]

            # 检查是否可以合并投影
            if self._can_merge_projections(project_op, child_project):
                self.optimization_count += 1
                print(f"    🎯 冗余消除: 合并连续的投影操作")

                merged_columns = self._merge_projection_columns(project_op.columns, child_project.columns)
                new_project = ProjectOp(merged_columns, child_project.children)
                return new_project

        return project_op

    def _is_always_true(self, condition: Expression) -> bool:
        """检查条件是否总是为真"""
        if isinstance(condition, LiteralExpr):
            return bool(condition.value)
        return False

    def _can_merge_projections(self, parent: ProjectOp, child: ProjectOp) -> bool:
        """检查是否可以合并两个投影操作"""
        # 简化实现
        return True

    def _merge_projection_columns(self, parent_columns: List[str], child_columns: List[str]) -> List[str]:
        """合并投影列"""
        # 简化实现：返回父投影的列
        return parent_columns


# 新的优化操作符
class OptimizedSeqScanOp(Operator):
    """优化的表扫描操作符（支持列投影）"""

    def __init__(self, table_name: str, selected_columns: List[str] = None):
        super().__init__([])
        self.table_name = table_name
        self.selected_columns = selected_columns or ["*"]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "OptimizedSeqScanOp",
            "table_name": self.table_name,
            "selected_columns": self.selected_columns,
            "optimization": "projection_pushdown"
        }

    def execute(self):
        # 模拟优化的表扫描
        print(f"优化的表扫描: {self.table_name}, 只读取列: {self.selected_columns}")
        return iter([])