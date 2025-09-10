from typing import List, Dict, Any, Optional
from sql_compiler.parser.ast_nodes import *
from sql_compiler.codegen.operators import *


class SimpleQueryOptimizer:
    """简化的查询优化器"""

    def __init__(self, silent_mode=False):
        self.silent_mode = silent_mode

    def optimize(self, plan: Operator) -> Operator:
        """优化执行计划"""
        if not self.silent_mode:
            print("\n=== 查询优化 ===")

        optimized_plan = plan
        total_optimizations = 0

        try:
            # 1. 应用投影下推
            result = self._apply_projection_pushdown(optimized_plan)
            if result is not None and len(result) == 2:
                new_plan, count1 = result
                if count1 > 0:
                    if not self.silent_mode:
                        print(f"✅ 投影下推: 应用了 {count1} 个优化")
                    total_optimizations += count1
                    optimized_plan = new_plan
                else:
                    if not self.silent_mode:
                        print("⏭️  投影下推: 无优化机会")
            else:
                if not self.silent_mode:
                    print("⏭️  投影下推: 无优化机会")

            # 2. 应用谓词下推
            result = self._apply_predicate_pushdown(optimized_plan)
            if result is not None and len(result) == 2:
                new_plan, count2 = result
                if count2 > 0:
                    if not self.silent_mode:
                        print(f"✅ 谓词下推: 应用了 {count2} 个优化")
                    total_optimizations += count2
                    optimized_plan = new_plan
                else:
                    if not self.silent_mode:
                        print("⏭️  谓词下推: 无优化机会")
            else:
                if not self.silent_mode:
                    print("⏭️  谓词下推: 无优化机会")

            # 3. 应用JOIN重排序
            result = self._apply_join_reorder(optimized_plan)
            if result is not None and len(result) == 2:
                new_plan, count3 = result
                if count3 > 0:
                    if not self.silent_mode:
                        print(f"✅ JOIN重排序: 应用了 {count3} 个优化")
                    total_optimizations += count3
                    optimized_plan = new_plan
                else:
                    if not self.silent_mode:
                        print("⏭️  JOIN重排序: 无优化机会")
            else:
                if not self.silent_mode:
                    print("⏭️  JOIN重排序: 无优化机会")

            # 4. 应用常量折叠
            result = self._apply_constant_folding(optimized_plan)
            if result is not None and len(result) == 2:
                new_plan, count4 = result
                if count4 > 0:
                    if not self.silent_mode:
                        print(f"✅ 常量折叠: 应用了 {count4} 个优化")
                    total_optimizations += count4
                    optimized_plan = new_plan
                else:
                    if not self.silent_mode:
                        print("⏭️  常量折叠: 无优化机会")
            else:
                if not self.silent_mode:
                    print("⏭️  常量折叠: 无优化机会")

            if not self.silent_mode:
                print(f"总共应用了 {total_optimizations} 个优化")
                if total_optimizations > 0:
                    print("优化后的执行计划已生成")

            return optimized_plan

        except Exception as e:
            if not self.silent_mode:
                print(f"⚠️ 优化过程中出现错误: {e}")
            return plan

    def _apply_projection_pushdown(self, plan: Operator) -> tuple:
        """应用投影下推优化"""
        try:
            optimizations = 0

            if isinstance(plan, ProjectOp):
                # 情况1: Project -> SeqScan
                if (len(plan.children) == 1 and
                        isinstance(plan.children[0], SeqScanOp) and
                        plan.columns != ["*"] and
                        len(plan.columns) > 0):

                    scan_op = plan.children[0]
                    optimized_scan = OptimizedSeqScanOp(
                        scan_op.table_name,
                        selected_columns=plan.columns
                    )
                    optimizations += 1
                    return optimized_scan, optimizations

                # 情况2: Project -> Filter -> SeqScan
                elif (len(plan.children) == 1 and
                      isinstance(plan.children[0], FilterOp) and
                      len(plan.children[0].children) == 1 and
                      isinstance(plan.children[0].children[0], SeqScanOp) and
                      plan.columns != ["*"] and
                      len(plan.columns) > 0):

                    filter_op = plan.children[0]
                    scan_op = filter_op.children[0]

                    # 创建优化的扫描操作，包含投影信息
                    optimized_scan = OptimizedSeqScanOp(
                        scan_op.table_name,
                        selected_columns=plan.columns
                    )

                    # 重新构建Filter操作
                    new_filter = FilterOp(filter_op.condition, [optimized_scan])
                    optimizations += 1
                    return new_filter, optimizations

            # 递归处理子节点
            new_children = []
            children_changed = False

            for child in plan.children:
                result = self._apply_projection_pushdown(child)
                if result is not None and len(result) == 2:
                    new_child, child_opts = result
                    new_children.append(new_child)
                    optimizations += child_opts
                    if new_child is not child:
                        children_changed = True
                else:
                    new_children.append(child)

            # 如果子节点发生了变化，创建新的操作符
            if children_changed:
                new_plan = self._clone_operator(plan, new_children)
                return new_plan, optimizations

            return plan, optimizations

        except Exception:
            return plan, 0

    def _apply_predicate_pushdown(self, plan: Operator) -> tuple:
        """应用谓词下推优化"""
        try:
            optimizations = 0

            # 情况1: Filter -> SeqScan 合并为带条件的扫描
            if isinstance(plan, FilterOp):
                if (len(plan.children) == 1 and
                        isinstance(plan.children[0], SeqScanOp)):

                    scan_op = plan.children[0]
                    # 创建带过滤条件的扫描操作
                    optimized_scan = FilteredSeqScanOp(
                        scan_op.table_name,
                        condition=plan.condition
                    )
                    optimizations += 1
                    return optimized_scan, optimizations

                # 情况2: Filter -> Join，尝试将条件合并到JOIN中
                elif (len(plan.children) == 1 and
                      isinstance(plan.children[0], JoinOp)):

                    join_op = plan.children[0]
                    # 合并条件到JOIN
                    combined_condition = self._combine_conditions(
                        join_op.on_condition, plan.condition
                    )

                    new_join = JoinOp(
                        join_op.join_type,
                        combined_condition,
                        join_op.children
                    )
                    optimizations += 1
                    return new_join, optimizations

            # 递归处理子节点
            new_children = []
            children_changed = False

            for child in plan.children:
                result = self._apply_predicate_pushdown(child)
                if result is not None and len(result) == 2:
                    new_child, child_opts = result
                    new_children.append(new_child)
                    optimizations += child_opts
                    if new_child is not child:
                        children_changed = True
                else:
                    new_children.append(child)

            if children_changed:
                new_plan = self._clone_operator(plan, new_children)
                return new_plan, optimizations

            return plan, optimizations

        except Exception:
            return plan, 0

    def _apply_join_reorder(self, plan: Operator) -> tuple:
        """应用JOIN重排序优化"""
        try:
            optimizations = 0

            if isinstance(plan, JoinOp):
                # 检查是否有多个表的JOIN，可以重排序
                if len(plan.children) >= 2:
                    # 简单的启发式：将小表放在前面
                    left_child = plan.children[0]
                    right_child = plan.children[1]

                    left_cost = self._estimate_table_size(left_child)
                    right_cost = self._estimate_table_size(right_child)

                    # 如果右边的表更小，交换顺序
                    if right_cost < left_cost:
                        new_children = [right_child, left_child]
                        new_join = JoinOp(
                            plan.join_type,
                            plan.on_condition,
                            new_children
                        )
                        optimizations += 1
                        return new_join, optimizations

            # 递归处理子节点
            new_children = []
            children_changed = False

            for child in plan.children:
                result = self._apply_join_reorder(child)
                if result is not None and len(result) == 2:
                    new_child, child_opts = result
                    new_children.append(new_child)
                    optimizations += child_opts
                    if new_child is not child:
                        children_changed = True
                else:
                    new_children.append(child)

            if children_changed:
                new_plan = self._clone_operator(plan, new_children)
                return new_plan, optimizations

            return plan, optimizations

        except Exception:
            return plan, 0

    def _apply_constant_folding(self, plan: Operator) -> tuple:
        """应用常量折叠优化"""
        try:
            optimizations = 0

            # 在Filter条件中查找可以折叠的常量表达式
            if isinstance(plan, FilterOp) and plan.condition:
                result = self._fold_expression(plan.condition)
                if result is not None and len(result) == 2:
                    folded_condition, was_folded = result
                    if was_folded:
                        new_filter = FilterOp(folded_condition, plan.children)
                        optimizations += 1
                        return new_filter, optimizations

            # 在JOIN条件中也进行常量折叠
            elif isinstance(plan, JoinOp) and plan.on_condition:
                result = self._fold_expression(plan.on_condition)
                if result is not None and len(result) == 2:
                    folded_condition, was_folded = result
                    if was_folded:
                        new_join = JoinOp(
                            plan.join_type,
                            folded_condition,
                            plan.children
                        )
                        optimizations += 1
                        return new_join, optimizations

            # 递归处理子节点
            new_children = []
            children_changed = False

            for child in plan.children:
                result = self._apply_constant_folding(child)
                if result is not None and len(result) == 2:
                    new_child, child_opts = result
                    new_children.append(new_child)
                    optimizations += child_opts
                    if new_child is not child:
                        children_changed = True
                else:
                    new_children.append(child)

            if children_changed:
                new_plan = self._clone_operator(plan, new_children)
                return new_plan, optimizations

            return plan, optimizations

        except Exception:
            return plan, 0

    def _combine_conditions(self, existing_condition: Optional[Expression],
                            new_condition: Expression) -> Expression:
        """合并两个条件"""
        if existing_condition is None:
            return new_condition

        # 用AND连接两个条件
        return BinaryExpr(existing_condition, 'AND', new_condition)

    def _estimate_table_size(self, operator: Operator) -> int:
        """估算表大小（用于JOIN重排序）"""
        try:
            if isinstance(operator, SeqScanOp):
                # 基于表名进行简单的大小估算
                return hash(operator.table_name) % 1000
            elif isinstance(operator, FilterOp):
                # 过滤后的表通常更小
                return self._estimate_table_size(operator.children[0]) // 2
            else:
                return 100  # 默认大小
        except Exception:
            return 100

    def _fold_expression(self, expr: Expression) -> tuple:
        """折叠表达式中的常量"""
        try:
            if isinstance(expr, BinaryExpr):
                # 递归折叠左右子表达式
                left_result = self._fold_expression(expr.left)
                right_result = self._fold_expression(expr.right)

                if (left_result is not None and len(left_result) == 2 and
                        right_result is not None and len(right_result) == 2):

                    left, left_folded = left_result
                    right, right_folded = right_result

                    # 如果左右都是字面量，尝试计算结果
                    if isinstance(left, LiteralExpr) and isinstance(right, LiteralExpr):
                        result = self._evaluate_constants(left.value, expr.operator, right.value)
                        if result is not None:
                            return LiteralExpr(result), True

                    # 如果子表达式被折叠了，创建新的表达式
                    if left_folded or right_folded:
                        return BinaryExpr(left, expr.operator, right), True

            return expr, False

        except Exception:
            return expr, False

    def _evaluate_constants(self, left_val, operator: str, right_val):
        """计算常量表达式"""
        try:
            if operator == '+':
                return left_val + right_val
            elif operator == '-':
                return left_val - right_val
            elif operator == '*':
                return left_val * right_val
            elif operator == '/':
                if right_val != 0:
                    return left_val / right_val
            elif operator == '=' or operator == '==':
                return left_val == right_val
            elif operator == '<>' or operator == '!=':
                return left_val != right_val
            elif operator == '<':
                return left_val < right_val
            elif operator == '>':
                return left_val > right_val
            elif operator == '<=':
                return left_val <= right_val
            elif operator == '>=':
                return left_val >= right_val
        except Exception:
            pass
        return None

    def _clone_operator(self, original: Operator, new_children: List[Operator]) -> Operator:
        """克隆操作符并使用新的子节点"""
        try:
            if isinstance(original, FilterOp):
                return FilterOp(original.condition, new_children)
            elif isinstance(original, ProjectOp):
                return ProjectOp(original.columns, new_children)
            elif isinstance(original, JoinOp):
                return JoinOp(original.join_type, original.on_condition, new_children)
            elif isinstance(original, GroupByOp):
                return GroupByOp(original.group_columns, original.having_condition, new_children)
            elif isinstance(original, OrderByOp):
                return OrderByOp(original.order_columns, new_children)
            elif isinstance(original, UpdateOp):
                return UpdateOp(original.table_name, original.assignments, new_children)
            elif isinstance(original, DeleteOp):
                return DeleteOp(original.table_name, new_children)
            else:
                # 对于其他类型，尝试保持原样
                return original
        except Exception:
            return original