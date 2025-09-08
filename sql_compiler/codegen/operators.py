from abc import ABC, abstractmethod
from typing import List, Any, Dict, Optional, Iterator
from sql_compiler.parser.ast_nodes import Expression


class Operator(ABC):
    """执行算子基类"""

    def __init__(self):
        self.children: List[Operator] = []

    @abstractmethod
    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行算子，返回结果迭代器"""
        pass

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        pass

    def add_child(self, child: 'Operator'):
        """添加子算子"""
        self.children.append(child)


class CreateTableOp(Operator):
    """CREATE TABLE算子"""

    def __init__(self, table_name: str, columns: List[tuple]):
        super().__init__()
        self.table_name = table_name
        self.columns = columns  # [(name, type, constraints), ...]

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行创建表操作"""
        # 这里应该调用存储引擎创建表
        yield {
            "operation": "CREATE_TABLE",
            "table_name": self.table_name,
            "status": "SUCCESS",
            "message": f"Table '{self.table_name}' created successfully"
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CreateTableOp",
            "table_name": self.table_name,
            "columns": self.columns
        }


class InsertOp(Operator):
    """INSERT算子"""

    def __init__(self, table_name: str, columns: Optional[List[str]], values: List[Any]):
        super().__init__()
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行插入操作"""
        # 这里应该调用存储引擎插入数据
        yield {
            "operation": "INSERT",
            "table_name": self.table_name,
            "rows_affected": 1,
            "status": "SUCCESS"
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "InsertOp",
            "table_name": self.table_name,
            "columns": self.columns,
            "values": self.values
        }


class SeqScanOp(Operator):
    """顺序扫描算子"""

    def __init__(self, table_name: str):
        super().__init__()
        self.table_name = table_name

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行顺序扫描"""
        # 这里应该调用存储引擎扫描表
        # 模拟返回数据
        sample_rows = [
            {"id": 1, "name": "Alice", "age": 20},
            {"id": 2, "name": "Bob", "age": 22}
        ]

        for row in sample_rows:
            yield row

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SeqScanOp",
            "table_name": self.table_name
        }


class FilterOp(Operator):
    """过滤算子"""

    def __init__(self, condition: Expression):
        super().__init__()
        self.condition = condition

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行过滤操作"""
        if not self.children:
            return

        child_op = self.children[0]
        for row in child_op.execute():
            if self._evaluate_condition(row, self.condition):
                yield row

    def _evaluate_condition(self, row: Dict[str, Any], condition: Expression) -> bool:
        """评估条件表达式"""
        from ..parser.ast_nodes import BinaryExpr, IdentifierExpr, LiteralExpr

        if isinstance(condition, BinaryExpr):
            left_val = self._evaluate_expression(row, condition.left)
            right_val = self._evaluate_expression(row, condition.right)

            if condition.operator == '=':
                return left_val == right_val
            elif condition.operator == '<>':
                return left_val != right_val
            elif condition.operator == '<':
                return left_val < right_val
            elif condition.operator == '>':
                return left_val > right_val
            elif condition.operator == '<=':
                return left_val <= right_val
            elif condition.operator == '>=':
                return left_val >= right_val
            elif condition.operator == 'AND':
                return left_val and right_val
            elif condition.operator == 'OR':
                return left_val or right_val

        return True  # 默认通过

    def _evaluate_expression(self, row: Dict[str, Any], expr: Expression) -> Any:
        """评估表达式值"""
        from ..parser.ast_nodes import BinaryExpr, IdentifierExpr, LiteralExpr

        if isinstance(expr, LiteralExpr):
            return expr.value
        elif isinstance(expr, IdentifierExpr):
            return row.get(expr.name)
        elif isinstance(expr, BinaryExpr):
            return self._evaluate_condition(row, expr)

        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "FilterOp",
            "condition": self.condition.to_dict(),
            "children": [child.to_dict() for child in self.children]
        }


class ProjectOp(Operator):
    """投影算子"""

    def __init__(self, columns: List[str]):
        super().__init__()
        self.columns = columns

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行投影操作"""
        if not self.children:
            return

        child_op = self.children[0]
        for row in child_op.execute():
            if self.columns == ["*"]:
                yield row
            else:
                projected_row = {
                    col: row.get(col)
                    for col in self.columns
                    if col in row
                }
                yield projected_row

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ProjectOp",
            "columns": self.columns,
            "children": [child.to_dict() for child in self.children]
        }


class DeleteOp(Operator):
    """DELETE算子"""

    def __init__(self, table_name: str, condition: Optional[Expression] = None):
        super().__init__()
        self.table_name = table_name
        self.condition = condition

    def execute(self) -> Iterator[Dict[str, Any]]:
        """执行删除操作"""
        # 这里应该调用存储引擎删除数据
        yield {
            "operation": "DELETE",
            "table_name": self.table_name,
            "rows_affected": 1,  # 模拟删除了1行
            "status": "SUCCESS"
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DeleteOp",
            "table_name": self.table_name,
            "condition": self.condition.to_dict() if self.condition else None
        }