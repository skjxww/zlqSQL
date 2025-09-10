from abc import ABC, abstractmethod
from typing import List, Any, Dict, Optional, Iterator, Tuple
from sql_compiler.parser.ast_nodes import Expression


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


# ==================== DML操作符 ====================

class InsertOp(Operator):
    """INSERT操作符"""

    def __init__(self, table_name: str, columns: Optional[List[str]], values: List[Expression]):
        super().__init__()
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "InsertOp",
            "table_name": self.table_name,
            "columns": self.columns,
            "values": [v.to_dict() for v in self.values]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {"operation": "insert", "table": self.table_name, "rows_affected": 1}


class UpdateOp(Operator):
    """UPDATE操作符"""

    def __init__(self, table_name: str, assignments: List[tuple], children: List[Operator]):
        super().__init__(children)
        self.table_name = table_name
        self.assignments = assignments  # [(column, expression), ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "UpdateOp",
            "table_name": self.table_name,
            "assignments": [
                {"column": col, "expression": expr.to_dict()}
                for col, expr in self.assignments
            ],
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 从子操作符获取要更新的行
        rows_affected = 0
        for child in self.children:
            for row in child.execute():
                # 应用赋值操作
                rows_affected += 1

        yield {"operation": "update", "table": self.table_name, "rows_affected": rows_affected}


class DeleteOp(Operator):
    """DELETE操作符"""

    def __init__(self, table_name: str, children: List[Operator]):
        super().__init__(children)
        self.table_name = table_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DeleteOp",
            "table_name": self.table_name,
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        rows_affected = 0
        for child in self.children:
            for row in child.execute():
                rows_affected += 1

        yield {"operation": "delete", "table": self.table_name, "rows_affected": rows_affected}


# ==================== 查询操作符 ====================

class SeqScanOp(Operator):
    """顺序扫描操作符"""

    def __init__(self, table_name: str):
        super().__init__()
        self.table_name = table_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SeqScanOp",
            "table_name": self.table_name
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 模拟从表中扫描数据
        yield {"table": self.table_name, "operation": "scan"}


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

    def __init__(self, group_columns: List[str], having_condition: Optional[Expression], children: List[Operator]):
        super().__init__(children)
        self.group_columns = group_columns
        self.having_condition = having_condition

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "GroupByOp",
            "group_columns": self.group_columns,
            "having_condition": self.having_condition.to_dict() if self.having_condition else None,
            "children": [child.to_dict() for child in self.children]
        }

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 模拟分组操作
        groups = {}
        for child in self.children:
            for row in child.execute():
                # 简化的分组逻辑
                key = tuple(row.get(col, None) for col in self.group_columns)
                if key not in groups:
                    groups[key] = []
                groups[key].append(row)

        for group_key, group_rows in groups.items():
            yield {"group_key": group_key, "count": len(group_rows)}


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


# ==================== 优化操作符 ====================

class OptimizedSeqScanOp(Operator):
    """优化的表扫描操作符（支持列投影）"""

    def __init__(self, table_name: str, selected_columns: Optional[List[str]] = None):
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

    def execute(self) -> Iterator[Dict[str, Any]]:
        yield {
            "table": self.table_name,
            "operation": "optimized_scan",
            "columns": self.selected_columns
        }


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


# ==================== 并行操作符 ====================

class ParallelSeqScanOp(SeqScanOp):
    """并行顺序扫描操作符"""

    def __init__(self, table_name: str, worker_count: int = 4):
        super().__init__(table_name)
        self.worker_count = worker_count

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["type"] = "ParallelSeqScanOp"
        result["worker_count"] = self.worker_count
        result["parallelism"] = True
        return result

    def execute(self) -> Iterator[Dict[str, Any]]:
        # 模拟并行扫描
        for worker_id in range(self.worker_count):
            yield {
                "table": self.table_name,
                "operation": "parallel_scan",
                "worker_id": worker_id,
                "total_workers": self.worker_count
            }


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