from abc import ABC, abstractmethod
from typing import List, Optional, Any, Dict
from enum import Enum
from typing import Optional, List

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

class TransactionAwareStmt(Statement):
    """支持事务的语句基类"""

    def __init__(self):
        super().__init__()
        self.transaction_id: Optional[str] = None  # 执行时由引擎设置
        self.in_transaction: bool = False

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


class InsertStmt(TransactionAwareStmt):
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


class SelectStmt(TransactionAwareStmt):
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
        self.in_transaction = False
        self.transaction_id = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "SelectStmt",
            "columns": self.columns,
            "from_clause": self.from_clause.to_dict() if self.from_clause else None,
            "where_clause": self.where_clause.to_dict() if self.where_clause else None,
            "transaction_id": self.transaction_id,
            "in_transaction": self.in_transaction
        }

        if self.group_by:
            result["group_by"] = self.group_by

        if self.having_clause:
            result["having_clause"] = self.having_clause.to_dict()

        if self.order_by:
            result["order_by"] = [{"column": col, "direction": direction}
                                  for col, direction in self.order_by]

        return result


class UpdateStmt(TransactionAwareStmt):
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


class DeleteStmt(TransactionAwareStmt):
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


class InExpr(Expression):
    """IN表达式节点"""

    def __init__(self, left_expr: Expression, right_expr: Expression, is_not: bool = False):
        self.left_expr = left_expr  # 左侧表达式
        self.right_expr = right_expr  # 右侧表达式（可能是子查询或值列表）
        self.is_not = is_not  # 是否是 NOT IN

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "InExpr",
            "left_expr": self.left_expr.to_dict(),
            "right_expr": self.right_expr.to_dict(),
            "is_not": self.is_not
        }


class SubqueryExpr(Expression):
    """子查询表达式节点"""

    def __init__(self, select_stmt: 'SelectStmt'):
        self.select_stmt = select_stmt

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SubqueryExpr",
            "select_stmt": self.select_stmt.to_dict()
        }


class ValueListExpr(Expression):
    """值列表表达式节点（用于 IN (1, 2, 3) 这种形式）"""

    def __init__(self, values: List[Expression]):
        self.values = values

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ValueListExpr",
            "values": [value.to_dict() for value in self.values]
        }

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

    def __init__(self, value: Any, data_type: Optional[str] = None):
        self.value = value
        self.data_type = data_type or self._infer_type(value)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "LiteralExpr",
            "value": self.value,
            "data_type": self.data_type
        }

    def _infer_type(self, value: Any) -> str:
        """推断数据类型"""
        if isinstance(value, int):
            return "INT"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, str):
            return "STRING"
        elif isinstance(value, bool):
            return "BOOLEAN"
        else:
            return "UNKNOWN"

    def __str__(self) -> str:
        if isinstance(self.value, str):
            return f"'{self.value}'"
        return str(self.value)


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


# 添加索引相关的AST节点
class CreateIndexStmt(Statement):
    """CREATE INDEX语句"""

    def __init__(self, index_name: str, table_name: str, columns: List[str],
                 unique: bool = False, index_type: str = "BTREE"):
        self.index_name = index_name
        self.table_name = table_name
        self.columns = columns
        self.unique = unique
        self.index_type = index_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CreateIndexStmt",
            "index_name": self.index_name,
            "table_name": self.table_name,
            "columns": self.columns,
            "unique": self.unique,
            "index_type": self.index_type
        }


class DropIndexStmt(Statement):
    """DROP INDEX语句"""

    def __init__(self, index_name: str):
        self.index_name = index_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DropIndexStmt",
            "index_name": self.index_name
        }


class ShowIndexesStmt(Statement):
    """SHOW INDEXES语句"""

    def __init__(self, table_name: Optional[str] = None):
        self.table_name = table_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ShowIndexesStmt",
            "table_name": self.table_name
        }


class ColumnRef(Expression):
    """列引用表达式"""

    def __init__(self, column: str, table_alias: Optional[str] = None):
        """
        :param column: 列名
        :param table_alias: 表别名（可选）
        """
        self.column = column
        self.table_alias = table_alias

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ColumnRef",
            "column": self.column,
            "table_alias": self.table_alias
        }

    @property
    def full_name(self) -> str:
        """获取完整的列名"""
        if self.table_alias:
            return f"{self.table_alias}.{self.column}"
        return self.column


class OrderByExpr(ASTNode):
    """ORDER BY表达式"""

    def __init__(self, column_ref: ColumnRef, direction: str = "ASC"):
        """
        :param column_ref: 列引用
        :param direction: 排序方向 ASC/DESC
        """
        self.column_ref = column_ref
        self.direction = direction.upper()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "OrderByExpr",
            "column_ref": self.column_ref.to_dict(),
            "direction": self.direction
        }

class IsolationLevel(Enum):
    """隔离级别"""
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"


class TransactionMode(Enum):
    """事务模式"""
    READ_WRITE = "READ WRITE"
    READ_ONLY = "READ ONLY"


# 事务控制语句基类
class TransactionStmt(Statement):
    """事务控制语句基类"""
    pass


class BeginTransactionStmt(TransactionStmt):
    """BEGIN TRANSACTION语句"""

    def __init__(self, isolation_level: Optional[IsolationLevel] = None,
                 transaction_mode: Optional[TransactionMode] = None):
        """
        BEGIN [TRANSACTION]
        [ISOLATION LEVEL isolation_level]
        [READ WRITE | READ ONLY]
        """
        self.isolation_level = isolation_level
        self.transaction_mode = transaction_mode

    def to_dict(self) -> dict:
        return {
            "type": "BeginTransactionStmt",
            "isolation_level": self.isolation_level.value if self.isolation_level else None,
            "transaction_mode": self.transaction_mode.value if self.transaction_mode else None
        }

    def __str__(self) -> str:
        parts = ["BEGIN TRANSACTION"]
        if self.isolation_level:
            parts.append(f"ISOLATION LEVEL {self.isolation_level.value}")
        if self.transaction_mode:
            parts.append(self.transaction_mode.value)
        return " ".join(parts)


class CommitStmt(TransactionStmt):
    """COMMIT语句"""

    def __init__(self, work: bool = False):
        """
        COMMIT [WORK]
        """
        self.work = work

    def to_dict(self) -> dict:
        return {
            "type": "CommitStmt",
            "work": self.work
        }

    def __str__(self) -> str:
        return "COMMIT" + (" WORK" if self.work else "")


class RollbackStmt(TransactionStmt):
    """ROLLBACK语句"""

    def __init__(self, work: bool = False, to_savepoint: Optional[str] = None):
        """
        ROLLBACK [WORK] [TO [SAVEPOINT] savepoint_name]
        """
        self.work = work
        self.to_savepoint = to_savepoint

    def to_dict(self) -> dict:
        return {
            "type": "RollbackStmt",
            "work": self.work,
            "to_savepoint": self.to_savepoint
        }

    def __str__(self) -> str:
        result = "ROLLBACK"
        if self.work:
            result += " WORK"
        if self.to_savepoint:
            result += f" TO SAVEPOINT {self.to_savepoint}"
        return result


class SavepointStmt(TransactionStmt):
    """SAVEPOINT语句"""

    def __init__(self, savepoint_name: str):
        """
        SAVEPOINT savepoint_name
        """
        self.savepoint_name = savepoint_name

    def to_dict(self) -> dict:
        return {
            "type": "SavepointStmt",
            "savepoint_name": self.savepoint_name
        }

    def __str__(self) -> str:
        return f"SAVEPOINT {self.savepoint_name}"


class ReleaseSavepointStmt(TransactionStmt):
    """RELEASE SAVEPOINT语句"""

    def __init__(self, savepoint_name: str):
        """
        RELEASE SAVEPOINT savepoint_name
        """
        self.savepoint_name = savepoint_name

    def to_dict(self) -> dict:
        return {
            "type": "ReleaseSavepointStmt",
            "savepoint_name": self.savepoint_name
        }

    def __str__(self) -> str:
        return f"RELEASE SAVEPOINT {self.savepoint_name}"


class ViewStmt(Statement):
    """视图语句基类"""
    pass


class CreateViewStmt(ViewStmt):
    """CREATE VIEW语句"""

    def __init__(self, view_name: str, select_stmt: SelectStmt,
                 columns: Optional[List[str]] = None, or_replace: bool = False,
                 materialized: bool = False, with_check_option: bool = False):
        self.view_name = view_name
        self.select_stmt = select_stmt
        self.columns = columns
        self.or_replace = or_replace
        self.materialized = materialized
        self.with_check_option = with_check_option

    def to_dict(self) -> dict:
        return {
            "type": "CreateViewStmt",
            "view_name": self.view_name,
            "select_stmt": self.select_stmt.to_dict(),
            "columns": self.columns,
            "or_replace": self.or_replace,
            "materialized": self.materialized,
            "with_check_option": self.with_check_option
        }


class DropViewStmt(ViewStmt):
    """DROP VIEW语句"""

    def __init__(self, view_names: List[str], if_exists: bool = False,
                 cascade: bool = False, materialized: bool = False):
        self.view_names = view_names
        self.if_exists = if_exists
        self.cascade = cascade
        self.materialized = materialized

    def to_dict(self) -> dict:
        return {
            "type": "DropViewStmt",
            "view_names": self.view_names,
            "if_exists": self.if_exists,
            "cascade": self.cascade,
            "materialized": self.materialized
        }


class ShowViewsStmt(ViewStmt):
    """SHOW VIEWS语句"""

    def __init__(self, pattern: Optional[str] = None, database: Optional[str] = None):
        self.pattern = pattern
        self.database = database

    def to_dict(self) -> dict:
        return {
            "type": "ShowViewsStmt",
            "pattern": self.pattern,
            "database": self.database
        }


class DescribeViewStmt(ViewStmt):
    """DESCRIBE VIEW语句"""

    def __init__(self, view_name: str):
        self.view_name = view_name

    def to_dict(self) -> dict:
        return {
            "type": "DescribeViewStmt",
            "view_name": self.view_name
        }