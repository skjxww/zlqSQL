from abc import ABC, abstractmethod
from typing import List, Any, Optional


class ASTNode(ABC):
    """AST节点基类"""

    @abstractmethod
    def accept(self, visitor):
        pass

    @abstractmethod
    def to_dict(self):
        """转换为字典格式，便于序列化"""
        pass


class Expression(ASTNode):
    """表达式基类"""
    pass


class Statement(ASTNode):
    """语句基类"""
    pass


# 表达式节点
class IdentifierExpr(Expression):
    def __init__(self, name: str):
        self.name = name

    def accept(self, visitor):
        return visitor.visit_identifier_expr(self)

    def to_dict(self):
        return {"type": "IdentifierExpr", "name": self.name}


class LiteralExpr(Expression):
    def __init__(self, value: Any):
        self.value = value

    def accept(self, visitor):
        return visitor.visit_literal_expr(self)

    def to_dict(self):
        return {"type": "LiteralExpr", "value": self.value}


class BinaryExpr(Expression):
    def __init__(self, left: Expression, operator: str, right: Expression):
        self.left = left
        self.operator = operator
        self.right = right

    def accept(self, visitor):
        return visitor.visit_binary_expr(self)

    def to_dict(self):
        return {
            "type": "BinaryExpr",
            "left": self.left.to_dict(),
            "operator": self.operator,
            "right": self.right.to_dict()
        }


# 语句节点
class CreateTableStmt(Statement):
    def __init__(self, table_name: str, columns: List[tuple]):
        self.table_name = table_name
        self.columns = columns  # [(column_name, column_type, constraints), ...]

    def accept(self, visitor):
        return visitor.visit_create_table_stmt(self)

    def to_dict(self):
        return {
            "type": "CreateTableStmt",
            "table_name": self.table_name,
            "columns": self.columns
        }


class InsertStmt(Statement):
    def __init__(self, table_name: str, columns: Optional[List[str]], values: List[Expression]):
        self.table_name = table_name
        self.columns = columns
        self.values = values

    def accept(self, visitor):
        return visitor.visit_insert_stmt(self)

    def to_dict(self):
        return {
            "type": "InsertStmt",
            "table_name": self.table_name,
            "columns": self.columns,
            "values": [v.to_dict() for v in self.values]
        }


class SelectStmt(Statement):
    def __init__(self, columns: List[str], table_name: str, where_clause: Optional[Expression] = None):
        self.columns = columns  # ['*'] 或 ['col1', 'col2']
        self.table_name = table_name
        self.where_clause = where_clause

    def accept(self, visitor):
        return visitor.visit_select_stmt(self)

    def to_dict(self):
        return {
            "type": "SelectStmt",
            "columns": self.columns,
            "table_name": self.table_name,
            "where_clause": self.where_clause.to_dict() if self.where_clause else None
        }


class DeleteStmt(Statement):
    def __init__(self, table_name: str, where_clause: Optional[Expression] = None):
        self.table_name = table_name
        self.where_clause = where_clause

    def accept(self, visitor):
        return visitor.visit_delete_stmt(self)

    def to_dict(self):
        return {
            "type": "DeleteStmt",
            "table_name": self.table_name,
            "where_clause": self.where_clause.to_dict() if self.where_clause else None
        }