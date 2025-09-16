from abc import ABC, abstractmethod
from datetime import time
from typing import List, Any, Dict, Optional, Iterator, Tuple
from sql_compiler.parser.ast_nodes import *
from typing import List, Any, Dict, Optional, Iterator, Tuple
from sql_compiler.parser.ast_nodes import Expression
import time


# 添加事务相关的枚举和类
class IsolationLevel:
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"


class TransactionMode:
    READ_WRITE = "READ WRITE"
    READ_ONLY = "READ ONLY"


class Operator(ABC):
    """操作符基类"""

    def __init__(self, children: Optional[List['Operator']] = None):
        self.children = children or []

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典表示"""
        pass

    @abstractmethod
    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行操作"""
        pass

class CommitTransactionOp(Operator):
    """COMMIT TRANSACTION 操作符"""

    def __init__(self, work: bool = False):
        super().__init__()
        self.work = work

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CommitTransactionOp",
            "work": self.work
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行提交事务操作"""
        yield {
            "operation": "commit_transaction",
            "work": self.work,
            "status": "ready_for_execution",
            "message": "事务提交操作已准备就绪"
        }


class RollbackTransactionOp(Operator):
    """ROLLBACK TRANSACTION 操作符"""

    def __init__(self, work: bool = False, to_savepoint: Optional[str] = None):
        super().__init__()
        self.work = work
        self.to_savepoint = to_savepoint

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "RollbackTransactionOp",
            "work": self.work,
            "to_savepoint": self.to_savepoint
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行回滚事务操作"""
        yield {
            "operation": "rollback_transaction",
            "work": self.work,
            "to_savepoint": self.to_savepoint,
            "status": "ready_for_execution",
            "message": "事务回滚操作已准备就绪"
        }


class SavepointOp(Operator):
    """SAVEPOINT 操作符"""

    def __init__(self, savepoint_name: str):
        super().__init__()
        self.savepoint_name = savepoint_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SavepointOp",
            "savepoint_name": self.savepoint_name
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行保存点操作"""
        yield {
            "operation": "create_savepoint",
            "savepoint_name": self.savepoint_name,
            "status": "ready_for_execution"
        }


class ReleaseSavepointOp(Operator):
    """RELEASE SAVEPOINT 操作符"""

    def __init__(self, savepoint_name: str):
        super().__init__()
        self.savepoint_name = savepoint_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ReleaseSavepointOp",
            "savepoint_name": self.savepoint_name
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {
            "operation": "release_savepoint",
            "savepoint_name": self.savepoint_name,
            "status": "ready_for_execution"
        }

# ==================== DDL操作符 ====================

class CreateTableOp(Operator):
    """CREATE TABLE操作符"""

    def __init__(self, table_name: str, columns: List[tuple]):
        super().__init__()
        self.table_name = table_name
        self.columns = columns

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CreateTableOp",
            "table_name": self.table_name,
            "columns": [
                {"name": col[0], "type": col[1], "constraints": col[2]}
                for col in self.columns
            ]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 实际的表创建逻辑会在存储层实现
        yield {"operation": "create_table", "table": self.table_name, "status": "success"}

# ==================== 查询操作符 ====================

class FilterOp(Operator):
    """过滤操作符"""

    def __init__(self, condition: Expression, children: List[Operator]):
        super().__init__(children)
        self.condition = condition

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "FilterOp",
            "condition": self.condition.to_dict(),
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        for child in self.children:
            for row in child.execute():
                # 模拟条件过滤
                yield row


class ProjectOp(Operator):
    """投影操作符"""

    def __init__(self, columns: List[str], children: List[Operator]):
        super().__init__(children)
        self.columns = columns

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ProjectOp",
            "columns": self.columns,
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        for child in self.children:
            for row in child.execute():
                # 模拟列投影
                yield row


class JoinOp(Operator):
    """连接操作符"""

    def __init__(self, join_type: str, on_condition: Optional[Expression], children: List[Operator]):
        super().__init__(children)
        self.join_type = join_type
        self.on_condition = on_condition

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "JoinOp",
            "join_type": self.join_type,
            "on_condition": self.on_condition.to_dict() if self.on_condition else None,
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 模拟连接操作
        for i, child in enumerate(self.children):
            for row in child.execute():
                yield {**row, "join_side": i}


class GroupByOp(Operator):
    """分组操作符"""

    def __init__(self, group_columns: List[str], having_condition: Optional[Expression] = None,
                 children: List[Operator] = None, aggregate_functions: List[tuple] = None):
        super().__init__(children or [])
        self.group_columns = group_columns
        self.having_condition = having_condition
        self.aggregate_functions = aggregate_functions or []  # 🔑 添加聚合函数属性

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "GroupByOp",
            "group_columns": self.group_columns,
            "having_condition": self.having_condition.to_dict() if self.having_condition else None,
            "aggregate_functions": self.aggregate_functions,  # 🔑 包含聚合函数信息
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行分组操作 - 改进版本"""
        # 收集所有子结果
        child_results = []
        for child in self.children:
            child_results.extend(list(child.execute()))

        # 如果没有聚合函数但有GROUP BY，添加默认的COUNT(*)
        aggregate_functions = self.aggregate_functions
        if not aggregate_functions and self.group_columns:
            aggregate_functions = [('COUNT', '*')]

        # 分组操作
        groups = {}
        for row in child_results:
            # 构建分组键
            group_key = tuple(row.get(col, None) for col in self.group_columns)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)

        # 对每个分组计算聚合结果
        for group_key, group_rows in groups.items():
            result_row = {}

            # 添加分组列
            for i, col in enumerate(self.group_columns):
                result_row[col] = group_key[i]

            # 计算聚合函数
            for func_name, column_name in aggregate_functions:
                agg_result = self._calculate_aggregate(func_name, column_name, group_rows)
                # 构建聚合列名
                if column_name == '*':
                    agg_column_name = f"{func_name.upper()}(*)"
                else:
                    agg_column_name = f"{func_name.upper()}({column_name})"
                result_row[agg_column_name] = agg_result

            # 应用HAVING条件
            if self.having_condition:
                if self._evaluate_having_condition(result_row):
                    yield result_row
            else:
                yield result_row

    def _calculate_aggregate(self, func_name: str, column_name: str, group_rows: List[Dict]) -> Any:
        """计算聚合函数值"""
        func_name = func_name.upper()

        if func_name == 'COUNT':
            if column_name == '*':
                return len(group_rows)
            else:
                # COUNT(column) - 计算非空值数量
                return len([row for row in group_rows if row.get(column_name) is not None])

        elif func_name == 'SUM':
            values = [row.get(column_name) for row in group_rows
                      if row.get(column_name) is not None and isinstance(row.get(column_name), (int, float))]
            return sum(values) if values else 0

        elif func_name == 'AVG':
            values = [row.get(column_name) for row in group_rows
                      if row.get(column_name) is not None and isinstance(row.get(column_name), (int, float))]
            return sum(values) / len(values) if values else None

        elif func_name == 'MAX':
            values = [row.get(column_name) for row in group_rows
                      if row.get(column_name) is not None]
            return max(values) if values else None

        elif func_name == 'MIN':
            values = [row.get(column_name) for row in group_rows
                      if row.get(column_name) is not None]
            return min(values) if values else None

        else:
            raise ValueError(f"不支持的聚合函数: {func_name}")

    def _evaluate_having_condition(self, result_row: Dict[str, Any]) -> bool:
        """评估HAVING条件 - 简化实现"""
        try:
            if not self.having_condition:
                return True

            # 这里需要根据你的Expression类型来实现条件评估
            # 简化实现 - 假设是二元比较表达式
            if hasattr(self.having_condition, 'left') and hasattr(self.having_condition, 'operator'):
                left_value = self._evaluate_expression(self.having_condition.left, result_row)
                right_value = self._evaluate_expression(self.having_condition.right, result_row)
                operator = self.having_condition.operator

                if operator == '>':
                    return left_value > right_value
                elif operator == '>=':
                    return left_value >= right_value
                elif operator == '<':
                    return left_value < right_value
                elif operator == '<=':
                    return left_value <= right_value
                elif operator == '=':
                    return left_value == right_value
                elif operator == '!=':
                    return left_value != right_value

            return True
        except Exception:
            return True

    def _evaluate_expression(self, expr, result_row: Dict[str, Any]):
        """评估表达式值"""
        # 如果是函数表达式（如COUNT(*)）
        if hasattr(expr, 'function_name'):
            func_name = expr.function_name.upper()
            if func_name == 'COUNT' and hasattr(expr, 'arguments'):
                if expr.arguments and hasattr(expr.arguments[0], 'value') and expr.arguments[0].value == '*':
                    return result_row.get('COUNT(*)', 0)
                else:
                    # COUNT(column)的情况
                    column = expr.arguments[0].value if expr.arguments else '*'
                    return result_row.get(f'COUNT({column})', 0)

        # 如果是字面值
        elif hasattr(expr, 'value'):
            return expr.value

        # 如果是列引用
        elif hasattr(expr, 'column_name'):
            return result_row.get(expr.column_name)

        return 0


class OrderByOp(Operator):
    """排序操作符"""

    def __init__(self, order_columns: List[Tuple[str, str]], children: List[Operator]):
        super().__init__(children)
        self.order_columns = order_columns  # [(column, direction), ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "OrderByOp",
            "order_columns": [
                {"column": col, "direction": direction}
                for col, direction in self.order_columns
            ],
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 收集所有行然后排序
        all_rows = []
        for child in self.children:
            all_rows.extend(child.execute())

        # 模拟排序（实际实现会更复杂）
        for row in sorted(all_rows, key=lambda x: str(x)):
            yield row


class InOp(Operator):
    """IN操作符"""

    def __init__(self, left_expr: Expression, right_expr: Expression, is_not: bool, children: List[Operator]):
        super().__init__(children)
        self.left_expr = left_expr
        self.right_expr = right_expr
        self.is_not = is_not

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "InOp",
            "left_expr": self.left_expr.to_dict(),
            "right_expr": self.right_expr.to_dict(),
            "is_not": self.is_not,
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 模拟IN操作
        for child in self.children:
            for row in child.execute():
                yield row


class SubqueryOp(Operator):
    """子查询操作符"""

    def __init__(self, select_plan: Operator):
        super().__init__([select_plan])
        self.select_plan = select_plan

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SubqueryOp",
            "select_plan": self.select_plan.to_dict(),
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 执行子查询
        for row in self.select_plan.execute():
            yield row

class FilteredSeqScanOp(Operator):
    """带过滤条件的表扫描操作符"""

    def __init__(self, table_name: str, condition: Optional[Expression] = None):
        super().__init__([])
        self.table_name = table_name
        self.condition = condition

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "FilteredSeqScanOp",
            "table_name": self.table_name,
            "condition": self.condition.to_dict() if self.condition else None,
            "optimization": "predicate_pushdown"
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {
            "table": self.table_name,
            "operation": "filtered_scan",
            "condition": "applied"
        }


# ==================== 高级连接操作符 ====================

class NestedLoopJoinOp(JoinOp):
    """嵌套循环连接操作符"""

    def __init__(self, join_type: str, on_condition: Optional[Expression], children: List[Operator]):
        super().__init__(join_type, on_condition, children)

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["type"] = "NestedLoopJoinOp"
        result["algorithm"] = "nested_loop"
        result["cost_model"] = "O(M*N)"
        return result

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 嵌套循环连接的执行逻辑
        if len(self.children) < 2:
            return

        outer_child = self.children[0]
        inner_child = self.children[1]

        for outer_row in outer_child.execute():
            for inner_row in inner_child.execute():
                # 简化的连接逻辑
                joined_row = {**outer_row, **inner_row, "join_algorithm": "nested_loop"}
                yield joined_row


class HashJoinOp(JoinOp):
    """哈希连接操作符"""

    def __init__(self, join_type: str, on_condition: Optional[Expression], children: List[Operator]):
        super().__init__(join_type, on_condition, children)

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["type"] = "HashJoinOp"
        result["algorithm"] = "hash_join"
        result["cost_model"] = "O(M+N)"
        result["memory_intensive"] = True
        return result

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 哈希连接的执行逻辑
        if len(self.children) < 2:
            return

        build_child = self.children[0]  # 构建表（通常是较小的表）
        probe_child = self.children[1]  # 探测表

        # 构建哈希表
        hash_table = {}
        for build_row in build_child.execute():
            # 简化的哈希键生成
            hash_key = str(build_row)
            if hash_key not in hash_table:
                hash_table[hash_key] = []
            hash_table[hash_key].append(build_row)

        # 探测哈希表
        for probe_row in probe_child.execute():
            probe_key = str(probe_row)
            if probe_key in hash_table:
                for build_row in hash_table[probe_key]:
                    joined_row = {**build_row, **probe_row, "join_algorithm": "hash"}
                    yield joined_row


class SortMergeJoinOp(JoinOp):
    """排序合并连接操作符"""

    def __init__(self, join_type: str, on_condition: Optional[Expression], children: List[Operator]):
        super().__init__(join_type, on_condition, children)

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["type"] = "SortMergeJoinOp"
        result["algorithm"] = "sort_merge"
        result["cost_model"] = "O(M*logM + N*logN)"
        result["requires_sorting"] = True
        return result

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 排序合并连接的执行逻辑
        if len(self.children) < 2:
            return

        left_child = self.children[0]
        right_child = self.children[1]

        # 收集并排序左表数据
        left_rows = sorted(list(left_child.execute()), key=lambda x: str(x))

        # 收集并排序右表数据
        right_rows = sorted(list(right_child.execute()), key=lambda x: str(x))

        # 合并排序后的数据
        for left_row in left_rows:
            for right_row in right_rows:
                joined_row = {**left_row, **right_row, "join_algorithm": "sort_merge"}
                yield joined_row


# ==================== 索引操作符 ====================

class IndexScanOp(Operator):
    """索引扫描操作符"""

    def __init__(self, table_name: str, index_name: Optional[str] = None,
                 scan_condition: Optional[Expression] = None):
        super().__init__([])
        self.table_name = table_name
        self.index_name = index_name or f"idx_{table_name}_auto"
        self.scan_condition = scan_condition

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "IndexScanOp",
            "table_name": self.table_name,
            "index_name": self.index_name,
            "scan_condition": self.scan_condition.to_dict() if self.scan_condition else None,
            "optimization": "index_access"
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {
            "table": self.table_name,
            "operation": "index_scan",
            "index": self.index_name,
            "condition": "indexed_lookup"
        }


class IndexOnlyScanOp(Operator):
    """仅索引扫描操作符（覆盖索引）"""

    def __init__(self, table_name: str, index_name: str,
                 columns: List[str], condition: Optional[Expression] = None):
        super().__init__([])
        self.table_name = table_name
        self.index_name = index_name
        self.columns = columns
        self.condition = condition

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "IndexOnlyScanOp",
            "table_name": self.table_name,
            "index_name": self.index_name,
            "columns": self.columns,
            "condition": self.condition.to_dict() if self.condition else None,
            "optimization": "covering_index"
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {
            "table": self.table_name,
            "operation": "index_only_scan",
            "index": self.index_name,
            "no_table_access": True
        }


class BitmapIndexScanOp(Operator):
    """位图索引扫描操作符"""

    def __init__(self, table_name: str, index_conditions: List[Tuple[str, Expression]]):
        super().__init__([])
        self.table_name = table_name
        self.index_conditions = index_conditions  # [(index_name, condition), ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "BitmapIndexScanOp",
            "table_name": self.table_name,
            "index_conditions": [
                {"index_name": idx, "condition": cond.to_dict()}
                for idx, cond in self.index_conditions
            ],
            "optimization": "bitmap_scan"
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {
            "table": self.table_name,
            "operation": "bitmap_scan",
            "indexes_used": len(self.index_conditions)
        }


# ==================== 聚合操作符 ====================

class HashAggregateOp(Operator):
    """哈希聚合操作符"""

    def __init__(self, group_columns: List[str], agg_functions: List[Dict[str, Any]],
                 having_condition: Optional[Expression], children: List[Operator]):
        super().__init__(children)
        self.group_columns = group_columns
        self.agg_functions = agg_functions  # [{"func": "COUNT", "column": "*", "alias": "cnt"}, ...]
        self.having_condition = having_condition

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "HashAggregateOp",
            "group_columns": self.group_columns,
            "aggregate_functions": self.agg_functions,
            "having_condition": self.having_condition.to_dict() if self.having_condition else None,
            "algorithm": "hash_aggregation",
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 哈希聚合的执行逻辑
        hash_groups = {}

        for child in self.children:
            for row in child.execute():
                # 生成分组键
                group_key = tuple(row.get(col, None) for col in self.group_columns)

                if group_key not in hash_groups:
                    hash_groups[group_key] = {"rows": [], "aggregates": {}}

                hash_groups[group_key]["rows"].append(row)

        # 计算聚合函数
        for group_key, group_data in hash_groups.items():
            result = dict(zip(self.group_columns, group_key))

            for agg_func in self.agg_functions:
                func_name = agg_func["func"]
                column = agg_func["column"]
                alias = agg_func.get("alias", f"{func_name}({column})")

                if func_name == "COUNT":
                    result[alias] = len(group_data["rows"])
                elif func_name == "SUM":
                    result[alias] = sum(row.get(column, 0) for row in group_data["rows"])
                elif func_name == "AVG":
                    values = [row.get(column, 0) for row in group_data["rows"]]
                    result[alias] = sum(values) / len(values) if values else 0
                elif func_name == "MIN":
                    values = [row.get(column) for row in group_data["rows"] if row.get(column) is not None]
                    result[alias] = min(values) if values else None
                elif func_name == "MAX":
                    values = [row.get(column) for row in group_data["rows"] if row.get(column) is not None]
                    result[alias] = max(values) if values else None

            yield result


class SortAggregateOp(Operator):
    """排序聚合操作符"""

    def __init__(self, group_columns: List[str], agg_functions: List[Dict[str, Any]],
                 having_condition: Optional[Expression], children: List[Operator]):
        super().__init__(children)
        self.group_columns = group_columns
        self.agg_functions = agg_functions
        self.having_condition = having_condition

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SortAggregateOp",
            "group_columns": self.group_columns,
            "aggregate_functions": self.agg_functions,
            "having_condition": self.having_condition.to_dict() if self.having_condition else None,
            "algorithm": "sort_aggregation",
            "requires_sorting": True,
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 排序聚合：先排序再分组聚合
        all_rows = []
        for child in self.children:
            all_rows.extend(child.execute())

        # 按分组列排序
        def sort_key(row):
            return tuple(str(row.get(col, '')) for col in self.group_columns)

        sorted_rows = sorted(all_rows, key=sort_key)

        # 分组聚合
        current_group_key = None
        current_group_rows = []

        for row in sorted_rows:
            group_key = tuple(row.get(col, None) for col in self.group_columns)

            if current_group_key != group_key:
                if current_group_rows:
                    # 输出前一组的结果
                    yield self._compute_group_aggregate(current_group_key, current_group_rows)

                current_group_key = group_key
                current_group_rows = [row]
            else:
                current_group_rows.append(row)

        # 输出最后一组
        if current_group_rows:
            yield self._compute_group_aggregate(current_group_key, current_group_rows)

    def _compute_group_aggregate(self, group_key: tuple, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算分组聚合结果"""
        result = dict(zip(self.group_columns, group_key))

        for agg_func in self.agg_functions:
            func_name = agg_func["func"]
            column = agg_func["column"]
            alias = agg_func.get("alias", f"{func_name}({column})")

            if func_name == "COUNT":
                result[alias] = len(rows)
            elif func_name == "SUM":
                result[alias] = sum(row.get(column, 0) for row in rows)
            # 其他聚合函数类似...

        return result


# ==================== 排序操作符 ====================

class QuickSortOp(OrderByOp):
    """快速排序操作符"""

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["type"] = "QuickSortOp"
        result["algorithm"] = "quicksort"
        result["avg_complexity"] = "O(n log n)"
        result["worst_complexity"] = "O(n²)"
        return result


class ExternalSortOp(OrderByOp):
    """外部排序操作符（用于大数据量）"""

    def __init__(self, order_columns: List[Tuple[str, str]], children: List[Operator],
                 memory_limit: int = 1024 * 1024):  # 1MB
        super().__init__(order_columns, children)
        self.memory_limit = memory_limit

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["type"] = "ExternalSortOp"
        result["algorithm"] = "external_sort"
        result["memory_limit"] = self.memory_limit
        result["disk_based"] = True
        return result

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 外部排序的执行逻辑（分批排序后合并）
        all_rows = []
        for child in self.children:
            all_rows.extend(child.execute())

        # 简化实现：直接内存排序
        sorted_rows = sorted(all_rows, key=self._sort_key)

        for row in sorted_rows:
            yield row

    def _sort_key(self, row: Dict[str, Any]):
        """生成排序键"""
        key_values = []
        for column, direction in self.order_columns:
            value = row.get(column, '')
            if direction.upper() == 'DESC':
                # 反转排序（简化实现）
                if isinstance(value, (int, float)):
                    value = -value
                else:
                    value = str(value)
            key_values.append(value)
        return tuple(key_values)


# ==================== 特殊操作符 ====================

class MaterializeOp(Operator):
    """物化操作符（将结果存储在内存中）"""

    def __init__(self, children: List[Operator]):
        super().__init__(children)
        self._materialized_data = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "MaterializeOp",
            "purpose": "cache_intermediate_results",
            "memory_usage": "high",
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        if self._materialized_data is None:
            # 第一次执行：物化数据
            self._materialized_data = []
            for child in self.children:
                for row in child.execute():
                    self._materialized_data.append(row)

        # 返回物化的数据
        for row in self._materialized_data:
            yield row


class UnionOp(Operator):
    """UNION操作符"""

    def __init__(self, union_type: str, children: List[Operator]):  # union_type: "UNION" or "UNION ALL"
        super().__init__(children)
        self.union_type = union_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "UnionOp",
            "union_type": self.union_type,
            "removes_duplicates": self.union_type == "UNION",
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        seen_rows = set() if self.union_type == "UNION" else None

        for child in self.children:
            for row in child.execute():
                if self.union_type == "UNION":
                    row_key = str(sorted(row.items()))
                    if row_key not in seen_rows:
                        seen_rows.add(row_key)
                        yield row
                else:  # UNION ALL
                    yield row


class IntersectOp(Operator):
    """INTERSECT操作符"""

    def __init__(self, children: List[Operator]):
        super().__init__(children)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "IntersectOp",
            "operation": "set_intersection",
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        if len(self.children) < 2:
            return

        # 收集第一个子查询的结果
        first_child_rows = set()
        for row in self.children[0].execute():
            first_child_rows.add(str(sorted(row.items())))

        # 检查其他子查询的交集
        for i in range(1, len(self.children)):
            current_child_rows = set()
            for row in self.children[i].execute():
                row_key = str(sorted(row.items()))
                if row_key in first_child_rows:
                    current_child_rows.add(row_key)

            first_child_rows = current_child_rows

        # 输出交集结果（简化实现）
        for row_key in first_child_rows:
            yield {"intersect_result": row_key}


class ExceptOp(Operator):
    """EXCEPT操作符"""

    def __init__(self, children: List[Operator]):
        super().__init__(children)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ExceptOp",
            "operation": "set_difference",
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        if len(self.children) < 2:
            return

        # 收集第一个子查询的结果
        first_child_rows = {}
        for row in self.children[0].execute():
            row_key = str(sorted(row.items()))
            first_child_rows[row_key] = row

        # 移除在其他子查询中出现的行
        for i in range(1, len(self.children)):
            for row in self.children[i].execute():
                row_key = str(sorted(row.items()))
                first_child_rows.pop(row_key, None)

        # 输出差集结果
        for row in first_child_rows.values():
            yield row


# ==================== 窗口函数操作符 ====================

class WindowFunctionOp(Operator):
    """窗口函数操作符"""

    def __init__(self, window_functions: List[Dict[str, Any]],
                 partition_by: List[str], order_by: List[Tuple[str, str]],
                 children: List[Operator]):
        super().__init__(children)
        self.window_functions = window_functions  # [{"func": "ROW_NUMBER", "alias": "rn"}, ...]
        self.partition_by = partition_by
        self.order_by = order_by

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "WindowFunctionOp",
            "window_functions": self.window_functions,
            "partition_by": self.partition_by,
            "order_by": [{"column": col, "direction": dir} for col, dir in self.order_by],
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 收集所有行
        all_rows = []
        for child in self.children:
            all_rows.extend(child.execute())

        # 按分区分组
        partitions = {}
        for row in all_rows:
            partition_key = tuple(row.get(col, None) for col in self.partition_by)
            if partition_key not in partitions:
                partitions[partition_key] = []
            partitions[partition_key].append(row)

        # 为每个分区计算窗口函数
        for partition_key, partition_rows in partitions.items():
            # 在分区内排序
            if self.order_by:
                def sort_key(row):
                    return tuple(row.get(col, '') for col, _ in self.order_by)

                partition_rows.sort(key=sort_key)

            # 计算窗口函数
            for i, row in enumerate(partition_rows):
                result_row = row.copy()

                for win_func in self.window_functions:
                    func_name = win_func["func"]
                    alias = win_func["alias"]

                    if func_name == "ROW_NUMBER":
                        result_row[alias] = i + 1
                    elif func_name == "RANK":
                        # 简化实现
                        result_row[alias] = i + 1
                    elif func_name == "DENSE_RANK":
                        # 简化实现
                        result_row[alias] = i + 1

                yield result_row



class GatherOp(Operator):
    """收集并行结果操作符"""

    def __init__(self, children: List[Operator]):
        super().__init__(children)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "GatherOp",
            "purpose": "collect_parallel_results",
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 收集所有并行子操作的结果
        for child in self.children:
            for row in child.execute():
                yield row



class AliasAwareJoinOp(JoinOp):
    """支持别名的连接操作符"""

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()

        # 增强连接条件显示，保留别名信息
        if self.on_condition:
            result["on_condition_formatted"] = self._format_condition_display()

        return result

    def _format_condition_display(self) -> str:
        """格式化条件显示，保留别名"""
        if not self.on_condition:
            return ""

        try:
            if hasattr(self.on_condition, 'left') and hasattr(self.on_condition, 'right'):
                left_display = self._format_identifier_display(self.on_condition.left)
                right_display = self._format_identifier_display(self.on_condition.right)
                operator = getattr(self.on_condition, 'operator', '=')
                return f"{left_display} {operator} {right_display}"
        except Exception:
            pass

        return str(self.on_condition)

    def _format_identifier_display(self, expr) -> str:
        """格式化标识符显示"""
        if hasattr(expr, 'table_name') and hasattr(expr, 'name'):
            table_part = expr.table_name if expr.table_name else ""
            return f"{table_part}.{expr.name}" if table_part else expr.name
        return str(expr)


# 创建一个工厂函数来创建合适的扫描操作符
def create_scan_op(table_name: str, table_alias: Optional[str] = None) -> Operator:
    """创建扫描操作符的工厂函数"""
    if table_alias:
        return AliasAwareSeqScanOp(table_name, table_alias)
    else:
        return SeqScanOp(table_name)


def create_join_op(join_type: str, on_condition: Optional[Expression],
                   children: List[Operator], preserve_aliases: bool = True) -> Operator:
    """创建连接操作符的工厂函数"""
    if preserve_aliases:
        return AliasAwareJoinOp(join_type, on_condition, children)
    else:
        return JoinOp(join_type, on_condition, children)


# 添加索引相关操作符
class CreateIndexOp(Operator):
    """创建索引操作符"""

    def __init__(self, index_name: str, table_name: str, columns: List[str],
                 unique: bool = False, index_type: str = "BTREE"):
        super().__init__([])
        self.index_name = index_name
        self.table_name = table_name
        self.columns = columns
        self.unique = unique
        self.index_type = index_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CreateIndexOp",
            "index_name": self.index_name,
            "table_name": self.table_name,
            "columns": self.columns,
            "unique": self.unique,
            "index_type": self.index_type
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {
            "operation": "create_index",
            "index_name": self.index_name,
            "table_name": self.table_name,
            "status": "success"
        }


class BTreeIndexScanOp(Operator):
    """B+树索引扫描操作符"""

    def __init__(self, table_name: str, index_name: str,
                 scan_condition: Optional[Expression] = None,
                 is_covering_index: bool = False):
        super().__init__([])
        self.table_name = table_name
        self.index_name = index_name
        self.scan_condition = scan_condition
        self.is_covering_index = is_covering_index

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "BTreeIndexScanOp",
            "table_name": self.table_name,
            "index_name": self.index_name,
            "scan_condition": self.scan_condition.to_dict() if self.scan_condition else None,
            "is_covering_index": self.is_covering_index,
            "algorithm": "btree_traversal"
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {
            "table": self.table_name,
            "operation": "btree_index_scan",
            "index": self.index_name,
            "covering": self.is_covering_index
        }


class IndexNestedLoopJoinOp(Operator):
    """索引嵌套循环连接操作符"""

    def __init__(self, join_type: str, join_condition: Expression,
                 outer_child: Operator, inner_index_scan: BTreeIndexScanOp):
        super().__init__([outer_child])
        self.join_type = join_type
        self.join_condition = join_condition
        self.inner_index_scan = inner_index_scan

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "IndexNestedLoopJoinOp",
            "join_type": self.join_type,
            "join_condition": self.join_condition.to_dict(),
            "outer_child": self.children[0].to_dict(),
            "inner_index_scan": self.inner_index_scan.to_dict(),
            "algorithm": "index_nested_loop"
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        for outer_row in self.children[0].execute():
            # 模拟通过索引查找匹配的内表行
            yield {
                **outer_row,
                "join_method": "index_nested_loop",
                "index_used": self.inner_index_scan.index_name
            }

class DropIndexOp(Operator):
    """删除索引操作符"""

    def __init__(self, index_name: str):
        super().__init__([])
        self.index_name = index_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DropIndexOp",
            "index_name": self.index_name
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {
            "operation": "drop_index",
            "index_name": self.index_name,
            "status": "success",
            "message": f"索引 '{self.index_name}' 已删除"
        }


class ShowIndexesOp(Operator):
    """显示索引操作符"""

    def __init__(self, table_name: Optional[str] = None):
        super().__init__([])
        self.table_name = table_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ShowIndexesOp",
            "table_name": self.table_name
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 模拟返回索引信息
        if self.table_name:
            yield {
                "operation": "show_indexes",
                "table_name": self.table_name,
                "indexes": [
                    {
                        "index_name": f"idx_{self.table_name}_id",
                        "columns": ["id"],
                        "unique": True,
                        "type": "BTREE"
                    }
                ]
            }
        else:
            yield {
                "operation": "show_all_indexes",
                "indexes": [
                    {
                        "table_name": "users",
                        "index_name": "idx_users_id",
                        "columns": ["id"],
                        "unique": True,
                        "type": "BTREE"
                    }
                ]
            }


class SortOp(Operator):
    """排序操作符"""

    def __init__(self, order_by: List[Tuple[str, str]], children: List[Operator]):
        """
        :param order_by: 排序列表，每个元素为 (column, direction)，如 [('name', 'ASC'), ('age', 'DESC')]
        :param children: 子操作符
        """
        super().__init__(children)
        self.order_by = order_by  # [(column, direction), ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SortOp",
            "order_by": [{"column": col, "direction": direction}
                         for col, direction in self.order_by],
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行排序操作"""
        # 收集所有子结果
        child_results = []
        for child in self.children:
            child_results.extend(list(child.execute()))

        # 执行排序
        def sort_key(row):
            key_values = []
            for column, direction in self.order_by:
                value = row.get(column, 0)
                # 处理不同数据类型的排序
                if isinstance(value, str):
                    sort_value = value.lower()
                else:
                    sort_value = value if value is not None else 0

                # 降序需要反转
                if direction.upper() == 'DESC':
                    if isinstance(sort_value, (int, float)):
                        sort_value = -sort_value
                    elif isinstance(sort_value, str):
                        # 字符串降序比较复杂，这里简化处理
                        sort_value = ''.join(chr(255 - ord(c)) for c in sort_value[:10])

                key_values.append(sort_value)

            return tuple(key_values)

        # 排序并返回结果
        sorted_results = sorted(child_results, key=sort_key)
        for row in sorted_results:
            yield row



class TransactionOp(Operator):
    """事务操作基类"""

    def __init__(self):
        super().__init__([])  # 事务操作通常没有子节点
        self.execution_time: Optional[float] = None


class BeginTransactionOp(TransactionOp):
    """BEGIN TRANSACTION 操作符"""

    def __init__(self, isolation_level: Optional[IsolationLevel] = None,
                 transaction_mode: Optional[TransactionMode] = None):
        super().__init__()
        self.isolation_level = isolation_level
        self.transaction_mode = transaction_mode

    def to_dict(self) -> dict:
        return {
            "type": "BeginTransactionOp",
            "isolation_level": self.isolation_level.value if self.isolation_level else None,
            "transaction_mode": self.transaction_mode.value if self.transaction_mode else None
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行开始事务操作"""
        start_time = time.time()

        try:
            yield {
                "operation": "begin_transaction",
                "isolation_level": self.isolation_level.value if self.isolation_level else "READ_COMMITTED",
                "transaction_mode": self.transaction_mode.value if self.transaction_mode else "READ_WRITE",
                "status": "ready_for_execution",
                "message": "事务开始操作已准备就绪，等待执行引擎调用存储层接口"
            }

        finally:
            self.execution_time = time.time() - start_time


class CommitTransactionOp(TransactionOp):
    """COMMIT TRANSACTION 操作符"""

    def __init__(self, work: bool = False):
        super().__init__()
        self.work = work

    def to_dict(self) -> dict:
        return {
            "type": "CommitTransactionOp",
            "work": self.work
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行提交事务操作"""
        start_time = time.time()

        try:
            yield {
                "operation": "commit_transaction",
                "work": self.work,
                "status": "ready_for_execution",
                "message": "事务提交操作已准备就绪，等待执行引擎调用存储层接口"
            }

        finally:
            self.execution_time = time.time() - start_time


class RollbackTransactionOp(TransactionOp):
    """ROLLBACK TRANSACTION 操作符"""

    def __init__(self, work: bool = False, to_savepoint: Optional[str] = None):
        super().__init__()
        self.work = work
        self.to_savepoint = to_savepoint

    def to_dict(self) -> dict:
        return {
            "type": "RollbackTransactionOp",
            "work": self.work,
            "to_savepoint": self.to_savepoint
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行回滚事务操作"""
        start_time = time.time()

        try:
            yield {
                "operation": "rollback_transaction",
                "work": self.work,
                "to_savepoint": self.to_savepoint,
                "status": "ready_for_execution",
                "message": "事务回滚操作已准备就绪，等待执行引擎调用存储层接口"
            }

        finally:
            self.execution_time = time.time() - start_time


class SavepointOp(TransactionOp):
    """SAVEPOINT 操作符"""

    def __init__(self, savepoint_name: str):
        super().__init__()
        self.savepoint_name = savepoint_name

    def to_dict(self) -> dict:
        return {
            "type": "SavepointOp",
            "savepoint_name": self.savepoint_name
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行保存点操作"""
        start_time = time.time()

        try:
            yield {
                "operation": "create_savepoint",
                "savepoint_name": self.savepoint_name,
                "status": "ready_for_execution",
                "message": f"保存点 {self.savepoint_name} 创建操作已准备就绪"
            }

        finally:
            self.execution_time = time.time() - start_time


class ReleaseSavepointOp(TransactionOp):
    """RELEASE SAVEPOINT 操作符"""

    def __init__(self, savepoint_name: str):
        super().__init__()
        self.savepoint_name = savepoint_name

    def to_dict(self) -> dict:
        return {
            "type": "ReleaseSavepointOp",
            "savepoint_name": self.savepoint_name
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行释放保存点操作"""
        start_time = time.time()

        try:
            yield {
                "operation": "release_savepoint",
                "savepoint_name": self.savepoint_name,
                "status": "ready_for_execution",
                "message": f"保存点 {self.savepoint_name} 释放操作已准备就绪"
            }

        finally:
            self.execution_time = time.time() - start_time


class TransactionAwareOp(Operator):
    """支持事务的操作符基类"""

    def __init__(self, children: List[Operator]):
        super().__init__(children)
        self.transaction_id: Optional[str] = None
        self.requires_transaction: bool = True  # 是否需要在事务中执行

    def set_transaction_context(self, transaction_id: Optional[str]):
        """设置事务上下文"""
        self.transaction_id = transaction_id
        # 递归设置子操作符的事务上下文
        for child in self.children:
            if isinstance(child, TransactionAwareOp):
                child.set_transaction_context(transaction_id)


class SeqScanOp(TransactionAwareOp):
    """顺序扫描操作符 - 支持事务（合并版）"""

    def __init__(self, table_name: str):
        super().__init__([])
        self.table_name = table_name
        self.requires_transaction = False  # 读操作可以在事务外执行

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SeqScanOp",
            "table_name": self.table_name,
            "transaction_id": self.transaction_id,
            "requires_transaction": self.requires_transaction
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行顺序扫描 - 支持事务"""
        yield {
            "operation": "seq_scan",
            "table": self.table_name,
            "transaction_id": self.transaction_id,
            "status": "ready_for_execution",
            "message": f"顺序扫描表 {self.table_name}，事务ID: {self.transaction_id or 'None'}"
        }


class InsertOp(Operator):
    """INSERT操作符 - 支持事务"""

    def __init__(self, table_name: str, columns: Optional[List[str]], values: List[Expression]):
        super().__init__()
        self.table_name = table_name
        self.columns = columns  # 保持原有的columns属性
        self.values = values

        # 添加事务支持属性
        self.transaction_id: Optional[str] = None
        self.requires_transaction: bool = True

    def set_transaction_context(self, transaction_id: Optional[str]):
        """设置事务上下文"""
        self.transaction_id = transaction_id

    def to_dict(self) -> Dict[str, Any]:
        # 处理values中的表达式对象，防止JSON序列化错误
        serializable_values = []
        for value in self.values:
            if hasattr(value, 'to_dict'):
                serializable_values.append(value.to_dict())
            elif hasattr(value, 'value'):  # LiteralExpr对象
                serializable_values.append(value.value)
            else:
                serializable_values.append(str(value))

        return {
            "type": "InsertOp",
            "table_name": self.table_name,
            "columns": self.columns,  # 保持原有结构
            "values": serializable_values,  # 使用处理后的values
            "transaction_id": self.transaction_id,
            "requires_transaction": self.requires_transaction
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 提取实际值用于执行
        actual_values = []
        for value in self.values:
            if hasattr(value, 'value'):
                actual_values.append(value.value)
            else:
                actual_values.append(value)

        yield {
            "operation": "insert",
            "table": self.table_name,
            "columns": self.columns,
            "values": actual_values,
            "transaction_id": self.transaction_id,
            "requires_transaction": self.requires_transaction,
            "rows_affected": 1
        }


class UpdateOp(Operator):
    """UPDATE操作符 - 支持事务"""

    def __init__(self, table_name: str, assignments: List[tuple], children: List[Operator]):
        super().__init__(children)
        self.table_name = table_name
        self.assignments = assignments  # 保持原有结构 [(column, expression), ...]

        # 添加事务支持
        self.transaction_id: Optional[str] = None
        self.requires_transaction: bool = True

    def set_transaction_context(self, transaction_id: Optional[str]):
        """设置事务上下文"""
        self.transaction_id = transaction_id

    def to_dict(self) -> Dict[str, Any]:
        # 处理assignments中的表达式
        serializable_assignments = []
        for col, expr in self.assignments:
            if hasattr(expr, 'to_dict'):
                serializable_assignments.append({"column": col, "expression": expr.to_dict()})
            elif hasattr(expr, 'value'):
                serializable_assignments.append({"column": col, "expression": expr.value})
            else:
                serializable_assignments.append({"column": col, "expression": str(expr)})

        return {
            "type": "UpdateOp",
            "table_name": self.table_name,
            "assignments": serializable_assignments,
            "transaction_id": self.transaction_id,
            "requires_transaction": self.requires_transaction,
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 从子操作符获取要更新的行
        rows_affected = 0
        for child in self.children:
            for row in child.execute():
                rows_affected += 1

        # 处理实际的赋值
        actual_assignments = []
        for col, expr in self.assignments:
            if hasattr(expr, 'value'):
                actual_assignments.append((col, expr.value))
            else:
                actual_assignments.append((col, expr))

        yield {
            "operation": "update",
            "table": self.table_name,
            "assignments": actual_assignments,
            "transaction_id": self.transaction_id,
            "requires_transaction": self.requires_transaction,
            "rows_affected": rows_affected
        }


class DeleteOp(Operator):
    """DELETE操作符 - 支持事务"""

    def __init__(self, table_name: str, children: List[Operator]):
        super().__init__(children)
        self.table_name = table_name

        # 添加事务支持
        self.transaction_id: Optional[str] = None
        self.requires_transaction: bool = True

    def set_transaction_context(self, transaction_id: Optional[str]):
        """设置事务上下文"""
        self.transaction_id = transaction_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DeleteOp",
            "table_name": self.table_name,
            "transaction_id": self.transaction_id,
            "requires_transaction": self.requires_transaction,
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        rows_affected = 0
        for child in self.children:
            for row in child.execute():
                rows_affected += 1

        yield {
            "operation": "delete",
            "table": self.table_name,
            "transaction_id": self.transaction_id,
            "requires_transaction": self.requires_transaction,
            "rows_affected": rows_affected
        }

class OptimizedSeqScanOp(TransactionAwareOp):
    """优化的表扫描操作符（支持列投影和事务）"""

    def __init__(self, table_name: str, selected_columns: Optional[List[str]] = None):
        super().__init__([])
        self.table_name = table_name
        self.selected_columns = selected_columns or ["*"]
        self.requires_transaction = False  # 读操作可以在事务外执行

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "OptimizedSeqScanOp",
            "table_name": self.table_name,
            "selected_columns": self.selected_columns,
            "transaction_id": self.transaction_id,
            "requires_transaction": self.requires_transaction,
            "optimization": "projection_pushdown"
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行优化扫描"""
        yield {
            "operation": "optimized_seq_scan",
            "table": self.table_name,
            "selected_columns": self.selected_columns,
            "transaction_id": self.transaction_id,
            "optimization": "projection_pushdown",
            "status": "ready_for_execution"
        }


class ParallelSeqScanOp(TransactionAwareOp):
    """并行顺序扫描操作符 - 支持事务"""

    def __init__(self, table_name: str, worker_count: int = 4):
        super().__init__([])
        self.table_name = table_name
        self.worker_count = worker_count
        self.requires_transaction = False  # 读操作可以在事务外执行

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ParallelSeqScanOp",
            "table_name": self.table_name,
            "worker_count": self.worker_count,
            "transaction_id": self.transaction_id,
            "requires_transaction": self.requires_transaction,
            "parallelism": True
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行并行扫描"""
        # 模拟并行扫描
        for worker_id in range(self.worker_count):
            yield {
                "table": self.table_name,
                "operation": "parallel_scan",
                "worker_id": worker_id,
                "worker_count": self.worker_count,
                "transaction_id": self.transaction_id,
                "status": "ready_for_execution"
            }

class AliasAwareSeqScanOp(SeqScanOp):
    """支持别名的顺序扫描操作符"""

    def __init__(self, table_name: str, table_alias: Optional[str] = None):
        super().__init__(table_name)
        self.table_alias = table_alias
        self.real_table_name = table_name

    def get_effective_name(self) -> str:
        """获取有效的表名（优先使用别名）"""
        return self.table_alias if self.table_alias else self.real_table_name

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "SeqScanOp",
            "table_name": self.real_table_name
        }

        if self.table_alias:
            result["table_alias"] = self.table_alias
            result["display_name"] = self.table_alias
        else:
            result["display_name"] = self.real_table_name

        return result

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {
            "table": self.real_table_name,
            "alias": self.table_alias,
            "operation": "scan",
            "display_name": self.get_effective_name()
        }

    def __repr__(self):
        if self.table_alias:
            return f"SeqScanOp({self.real_table_name} AS {self.table_alias})"
        return f"SeqScanOp({self.real_table_name})"


def set_transaction_context_for_plan(plan: Operator, transaction_id: Optional[str]):
    """为执行计划树设置事务上下文"""
    # 为当前节点设置事务上下文
    if hasattr(plan, 'set_transaction_context'):
        plan.set_transaction_context(transaction_id)

    # 递归处理子节点
    for child in plan.children:
        set_transaction_context_for_plan(child, transaction_id)


def requires_transaction(plan: Operator) -> bool:
    """判断执行计划是否需要事务支持"""
    # 检查当前节点
    if hasattr(plan, 'requires_transaction') and plan.requires_transaction:
        return True

    # 检查子节点
    for child in plan.children:
        if requires_transaction(child):
            return True

    return False


def is_transaction_statement(plan: Operator) -> bool:
    """判断是否为事务控制语句"""
    return isinstance(plan, (
        BeginTransactionOp,
        CommitTransactionOp,
        RollbackTransactionOp,
        SavepointOp,
        ReleaseSavepointOp
    ))

class CreateViewOp(Operator):
    """CREATE VIEW 操作符"""

    def __init__(self, view_name: str, select_plan: Operator,
                 columns: Optional[List[str]] = None, or_replace: bool = False,
                 materialized: bool = False, with_check_option: bool = False,
                 catalog=None):  # 添加catalog参数
        super().__init__([select_plan])
        self.view_name = view_name
        self.select_plan = select_plan
        self.columns = columns
        self.or_replace = or_replace
        self.materialized = materialized
        self.with_check_option = with_check_option
        self.catalog = catalog  # 保存catalog引用

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CreateViewOp",
            "view_name": self.view_name,
            "columns": self.columns,
            "or_replace": self.or_replace,
            "materialized": self.materialized,
            "with_check_option": self.with_check_option,
            "select_plan": self.select_plan.to_dict()
        }

    def _get_view_definition(self, view_name: str):
        """获取视图定义"""
        if hasattr(self.catalog, 'get_view_definition'):
            definition = self.catalog.get_view_definition(view_name)
            # 这里需要重新解析视图的SELECT语句
            from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer
            from sql_compiler.parser.syntax_analyzer import SyntaxAnalyzer

            lexer = LexicalAnalyzer(definition)
            tokens = lexer.tokenize()
            parser = SyntaxAnalyzer(tokens)
            return parser.parse()

        # 简化实现
        from sql_compiler.parser.ast_nodes import SelectStmt, TableRef
        return SelectStmt(columns=["*"], from_clause=TableRef("dummy"))

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行创建视图操作"""
        try:
            # 从select_plan构造视图定义字符串
            definition = self._construct_view_definition_from_plan()

            # 实际调用catalog的create_view方法
            success = self.catalog.create_view(
                view_name=self.view_name,
                definition=definition,
                columns=self.columns,
                is_materialized=self.materialized,
                or_replace=self.or_replace,
                with_check_option=self.with_check_option
            )

            if success:
                yield {
                    "operation": "create_view",
                    "view_name": self.view_name,
                    "materialized": self.materialized,
                    "or_replace": self.or_replace,
                    "status": "success",
                    "message": f"视图 {self.view_name} 创建成功"
                }
            else:
                yield {
                    "operation": "create_view",
                    "view_name": self.view_name,
                    "status": "failed",
                    "message": f"视图 {self.view_name} 创建失败"
                }

        except Exception as e:
            yield {
                "operation": "create_view",
                "view_name": self.view_name,
                "status": "error",
                "message": f"视图 {self.view_name} 创建错误: {str(e)}"
            }

    def _construct_view_definition_from_plan(self) -> str:
        """从执行计划构造视图定义字符串"""
        try:
            # 从执行计划中重构SELECT语句
            return self._reconstruct_select_sql(self.select_plan)
        except Exception as e:
            # 如果重构失败，使用简化版本
            return self._simple_reconstruct()

    def _reconstruct_select_sql(self, plan) -> str:
        """递归重构SELECT语句"""
        if hasattr(plan, 'type'):
            plan_type = getattr(plan, 'type', plan.__class__.__name__)
        else:
            plan_type = plan.__class__.__name__

        if plan_type == "ProjectOp":
            # 处理SELECT子句
            columns = getattr(plan, 'columns', ['*'])
            columns_str = ', '.join(columns)

            # 递归处理子计划
            if hasattr(plan, 'children') and plan.children:
                from_part = self._reconstruct_from_clause(plan.children[0])
                return f"SELECT {columns_str} FROM {from_part}"
            else:
                return f"SELECT {columns_str}"

        elif plan_type == "FilterOp":
            # 处理WHERE子句
            base_sql = self._reconstruct_select_sql(plan.children[0]) if plan.children else "SELECT *"
            where_clause = self._reconstruct_expression(getattr(plan, 'condition', None))
            return f"{base_sql} WHERE {where_clause}"

        elif plan_type == "GroupByOp":
            # 处理GROUP BY子句
            base_sql = self._reconstruct_select_sql(plan.children[0]) if plan.children else "SELECT *"
            group_columns = getattr(plan, 'group_columns', [])
            if group_columns:
                group_by_str = ', '.join(group_columns)
                base_sql += f" GROUP BY {group_by_str}"

            # 处理HAVING子句
            having_condition = getattr(plan, 'having_condition', None)
            if having_condition:
                having_clause = self._reconstruct_expression(having_condition)
                base_sql += f" HAVING {having_clause}"

            return base_sql

        elif plan_type == "SeqScanOp":
            # 基础表扫描
            table_name = getattr(plan, 'table_name', 'unknown_table')
            return table_name

        else:
            # 其他类型的计划，返回简化版本
            return self._simple_reconstruct()

    def _reconstruct_from_clause(self, plan) -> str:
        """重构FROM子句"""
        plan_type = getattr(plan, 'type', plan.__class__.__name__)

        if plan_type == "SeqScanOp":
            return getattr(plan, 'table_name', 'unknown_table')
        elif plan_type == "JoinOp":
            # 处理JOIN
            left_table = self._reconstruct_from_clause(plan.left_child) if hasattr(plan, 'left_child') else 'table1'
            right_table = self._reconstruct_from_clause(plan.right_child) if hasattr(plan, 'right_child') else 'table2'
            join_type = getattr(plan, 'join_type', 'INNER')

            result = f"{left_table} {join_type} JOIN {right_table}"

            # 添加ON条件
            on_condition = getattr(plan, 'on_condition', None)
            if on_condition:
                on_clause = self._reconstruct_expression(on_condition)
                result += f" ON {on_clause}"

            return result
        else:
            return 'unknown_table'

    def _reconstruct_expression(self, expr) -> str:
        """重构表达式"""
        if expr is None:
            return "TRUE"

        # 如果是字典形式的表达式
        if isinstance(expr, dict):
            expr_type = expr.get('type', '')

            if expr_type == "BinaryExpr":
                left = self._reconstruct_expression(expr.get('left'))
                operator = expr.get('operator', '=')
                right = self._reconstruct_expression(expr.get('right'))
                return f"{left} {operator} {right}"

            elif expr_type == "IdentifierExpr":
                table_name = expr.get('table_name')
                column_name = expr.get('name', 'unknown')
                if table_name:
                    return f"{table_name}.{column_name}"
                return column_name

            elif expr_type == "LiteralExpr":
                value = expr.get('value')
                if isinstance(value, str):
                    return f"'{value}'"
                return str(value)

            elif expr_type == "FunctionExpr":
                func_name = expr.get('function_name', 'UNKNOWN')
                args = expr.get('arguments', [])
                args_str = ', '.join([self._reconstruct_expression(arg) for arg in args])
                return f"{func_name}({args_str})"

        # 如果是对象形式的表达式
        elif hasattr(expr, 'to_dict'):
            return self._reconstruct_expression(expr.to_dict())

        # 如果是简单的字符串或数值
        else:
            return str(expr)

    def _simple_reconstruct(self) -> str:
        """简化版本的SQL重构"""
        # 尝试从select_plan获取基本信息
        table_name = "unknown_table"
        columns = "*"

        if hasattr(self.select_plan, 'children') and self.select_plan.children:
            first_child = self.select_plan.children[0]
            if hasattr(first_child, 'table_name'):
                table_name = first_child.table_name

        if hasattr(self.select_plan, 'columns'):
            columns = ', '.join(self.select_plan.columns)

        return f"SELECT {columns} FROM {table_name}"


class DropViewOp(Operator):
    def __init__(self, view_names: List[str], if_exists: bool = False,
                 cascade: bool = False, materialized: bool = False, catalog=None):
        super().__init__()
        self.view_names = view_names
        self.if_exists = if_exists
        self.cascade = cascade
        self.materialized = materialized
        self.catalog = catalog

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行删除视图操作"""
        for view_name in self.view_names:
            try:
                success = self.catalog.drop_view(
                    view_name=view_name,
                    if_exists=self.if_exists,
                    cascade=self.cascade
                )

                if success:
                    yield {
                        "operation": "drop_view",
                        "view_name": view_name,
                        "status": "success",
                        "message": f"视图 {view_name} 删除成功"
                    }
                else:
                    yield {
                        "operation": "drop_view",
                        "view_name": view_name,
                        "status": "failed",
                        "message": f"视图 {view_name} 删除失败"
                    }
            except Exception as e:
                yield {
                    "operation": "drop_view",
                    "view_name": view_name,
                    "status": "error",
                    "message": f"视图 {view_name} 删除错误: {str(e)}"
                }


class ShowViewsOp(Operator):
    """SHOW VIEWS 操作符"""

    def __init__(self, pattern: Optional[str] = None, database: Optional[str] = None):
        super().__init__()
        self.pattern = pattern
        self.database = database

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ShowViewsOp",
            "pattern": self.pattern,
            "database": self.database
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行显示视图操作"""
        yield {
            "operation": "show_views",
            "pattern": self.pattern,
            "database": self.database,
            "status": "success",
            "message": "显示视图列表"
        }


class DescribeViewOp(Operator):
    """DESCRIBE VIEW 操作符"""

    def __init__(self, view_name: str):
        super().__init__()
        self.view_name = view_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DescribeViewOp",
            "view_name": self.view_name
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行描述视图操作"""
        yield {
            "operation": "describe_view",
            "view_name": self.view_name,
            "status": "success",
            "message": f"视图 {self.view_name} 描述信息"
        }


class ViewScanOp(Operator):
    """视图扫描操作符 - 将视图查询转换为底层表查询"""

    def __init__(self, view_name: str, underlying_plan: Operator):
        super().__init__([underlying_plan])
        self.view_name = view_name
        self.underlying_plan = underlying_plan

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ViewScanOp",
            "view_name": self.view_name,
            "underlying_plan": self.underlying_plan.to_dict()
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行视图扫描 - 实际执行底层查询"""
        for result in self.underlying_plan.execute():
            # 为结果添加视图信息
            result["_view_source"] = self.view_name
            yield result

