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