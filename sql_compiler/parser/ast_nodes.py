from abc import ABC, abstractmethod
from typing import List, Optional, Any, Dict


class ASTNode(ABC):
    """AST节点基类"""

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典表示"""
        pass


# ==================== 语句节点 ====================

class Statement(ASTNode):
    """语句基类"""
    pass


class CreateTableStmt(Statement):
    """CREATE TABLE语句"""

    def __init__(self, table_name: str, columns: List[tuple]):
        self.table_name = table_name
        self.columns = columns  # [(name, type, constraints), ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CreateTableStmt",
            "table_name": self.table_name,
            "columns": [
                {"name": col[0], "type": col[1], "constraints": col[2]}
                for col in self.columns
            ]
        }


class InsertStmt(Statement):
    """INSERT语句"""

    def __init__(self, table_name: str, columns: Optional[List[str]], values: List['Expression']):
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "InsertStmt",
            "table_name": self.table_name,
            "columns": self.columns,
            "values": [v.to_dict() for v in self.values]
        }


class SelectStmt(Statement):
    """SELECT语句"""

    def __init__(self, columns: List[str], from_clause: 'FromClause',
                 where_clause: Optional['Expression'] = None,
                 group_by: Optional[List[str]] = None,
                 having_clause: Optional['Expression'] = None,
                 order_by: Optional[List[tuple]] = None):  # [(column, direction), ...]
        self.columns = columns
        self.from_clause = from_clause
        self.where_clause = where_clause
        self.group_by = group_by
        self.having_clause = having_clause
        self.order_by = order_by

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "SelectStmt",
            "columns": self.columns,
            "from_clause": self.from_clause.to_dict() if self.from_clause else None,
            "where_clause": self.where_clause.to_dict() if self.where_clause else None,
        }

        if self.group_by:
            result["group_by"] = self.group_by

        if self.having_clause:
            result["having_clause"] = self.having_clause.to_dict()

        if self.order_by:
            result["order_by"] = [{"column": col, "direction": direction}
                                  for col, direction in self.order_by]

        return result


class UpdateStmt(Statement):
    """UPDATE语句"""

    def __init__(self, table_name: str, assignments: List[tuple],
                 where_clause: Optional['Expression'] = None):
        self.table_name = table_name
        self.assignments = assignments  # [(column, expression), ...]
        self.where_clause = where_clause

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "UpdateStmt",
            "table_name": self.table_name,
            "assignments": [
                {"column": col, "expression": expr.to_dict()}
                for col, expr in self.assignments
            ],
            "where_clause": self.where_clause.to_dict() if self.where_clause else None
        }


class DeleteStmt(Statement):
    """DELETE语句"""

    def __init__(self, table_name: str, where_clause: Optional['Expression'] = None):
        self.table_name = table_name
        self.where_clause = where_clause

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DeleteStmt",
            "table_name": self.table_name,
            "where_clause": self.where_clause.to_dict() if self.where_clause else None
        }


# ==================== FROM子句节点 ====================

class FromClause(ASTNode):
    """FROM子句基类"""
    pass


class TableRef(FromClause):
    """表引用"""

    def __init__(self, table_name: str, alias: Optional[str] = None):
        self.table_name = table_name
        self.alias = alias

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "TableRef",
            "table_name": self.table_name
        }
        if self.alias:
            result["alias"] = self.alias
        return result


class JoinExpr(FromClause):
    """JOIN表达式"""

    def __init__(self, join_type: str, left: FromClause, right: FromClause,
                 on_condition: Optional['Expression'] = None):
        self.join_type = join_type  # "INNER", "LEFT", "RIGHT"
        self.left = left
        self.right = right
        self.on_condition = on_condition

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "JoinExpr",
            "join_type": self.join_type,
            "left": self.left.to_dict(),
            "right": self.right.to_dict(),
            "on_condition": self.on_condition.to_dict() if self.on_condition else None
        }


# ==================== 表达式节点 ====================

class Expression(ASTNode):
    """表达式基类"""
    pass


class BinaryExpr(Expression):
    """二元表达式"""

    def __init__(self, left: Expression, operator: str, right: Expression):
        self.left = left
        self.operator = operator
        self.right = right

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "BinaryExpr",
            "left": self.left.to_dict(),
            "operator": self.operator,
            "right": self.right.to_dict()
        }


class IdentifierExpr(Expression):
    """标识符表达式"""

    def __init__(self, name: str, table_name: Optional[str] = None):
        self.name = name
        self.table_name = table_name  # 用于 table.column 格式

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "IdentifierExpr",
            "name": self.name
        }
        if self.table_name:
            result["table_name"] = self.table_name
        return result


class LiteralExpr(Expression):
    """字面量表达式"""

    def __init__(self, value: Any):
        self.value = value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "LiteralExpr",
            "value": self.value
        }


class FunctionExpr(Expression):
    """函数表达式"""

    def __init__(self, function_name: str, arguments: List[Expression]):
        self.function_name = function_name
        self.arguments = arguments

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "FunctionExpr",
            "function_name": self.function_name,
            "arguments": [arg.to_dict() for arg in self.arguments]
        }