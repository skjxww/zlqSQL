from typing import List, Any, Dict
from sql_compiler.parser.ast_nodes import *
from sql_compiler.semantic.symbol_table import SymbolTable
from sql_compiler.catalog.catalog_manager import CatalogManager
from sql_compiler.exceptions.compiler_errors import SemanticError


class SemanticAnalyzer:
    """语义分析器 - 扩展支持新语法"""

    def __init__(self, catalog: CatalogManager):
        self.catalog = catalog
        self.symbol_table = SymbolTable()

    def analyze(self, stmt: Statement):
        """分析语句"""
        if isinstance(stmt, CreateTableStmt):
            self._analyze_create_table(stmt)
        elif isinstance(stmt, InsertStmt):
            self._analyze_insert(stmt)
        elif isinstance(stmt, SelectStmt):
            self._analyze_select(stmt)
        elif isinstance(stmt, UpdateStmt):
            self._analyze_update(stmt)
        elif isinstance(stmt, DeleteStmt):
            self._analyze_delete(stmt)
        else:
            raise SemanticError(f"不支持的语句类型: {type(stmt).__name__}")

    def _analyze_create_table(self, stmt: CreateTableStmt):
        """分析CREATE TABLE语句"""
        # 检查表是否已存在
        if self.catalog.table_exists(stmt.table_name):
            raise SemanticError(f"表 '{stmt.table_name}' 已存在")

        # 检查列名是否重复
        column_names = [col[0] for col in stmt.columns]
        if len(column_names) != len(set(column_names)):
            raise SemanticError("表定义中存在重复的列名")

        # 验证数据类型
        for column_name, column_type, constraints in stmt.columns:
            if not self._is_valid_data_type(column_type):
                raise SemanticError(f"无效的数据类型: {column_type}")

        # 添加到目录
        success = self.catalog.create_table(stmt.table_name, stmt.columns)
        if not success:
            raise SemanticError(f"创建表 '{stmt.table_name}' 失败")

    def _analyze_insert(self, stmt: InsertStmt):
        """分析INSERT语句"""
        # 检查表是否存在
        if not self.catalog.table_exists(stmt.table_name):
            raise SemanticError(f"表 '{stmt.table_name}' 不存在")

        table_info = self.catalog.get_table(stmt.table_name)
        if not table_info:
            raise SemanticError(f"无法获取表 '{stmt.table_name}' 的信息")

        table_columns = [col["name"] for col in table_info["columns"]]

        # 检查列名
        if stmt.columns:
            for col in stmt.columns:
                if col not in table_columns:
                    raise SemanticError(f"表 '{stmt.table_name}' 中不存在列 '{col}'")
            target_columns = stmt.columns
        else:
            target_columns = table_columns

        # 检查值的数量
        if len(stmt.values) != len(target_columns):
            raise SemanticError(f"值的数量({len(stmt.values)})与列的数量({len(target_columns)})不匹配")

        # 分析每个值表达式
        available_tables = {stmt.table_name: table_columns}
        for value in stmt.values:
            self._analyze_expression(value, available_tables)

    def _analyze_select(self, stmt: SelectStmt):
        """分析SELECT语句"""
        # 分析FROM子句
        available_tables = self._analyze_from_clause(stmt.from_clause)

        # 分析选择列表
        if stmt.columns != ["*"]:
            for col in stmt.columns:
                if not self._is_valid_column_reference(col, available_tables):
                    raise SemanticError(f"无效的列引用: {col}")

        # 分析WHERE子句
        if stmt.where_clause:
            self._analyze_expression(stmt.where_clause, available_tables)

        # 分析GROUP BY子句
        if stmt.group_by:
            for col in stmt.group_by:
                if not self._is_valid_column_reference(col, available_tables):
                    raise SemanticError(f"GROUP BY中的无效列引用: {col}")

        # 分析HAVING子句
        if stmt.having_clause:
            if not stmt.group_by:
                raise SemanticError("HAVING子句只能与GROUP BY一起使用")
            self._analyze_expression(stmt.having_clause, available_tables)

        # 分析ORDER BY子句
        if stmt.order_by:
            for col, direction in stmt.order_by:
                if not self._is_valid_column_reference(col, available_tables):
                    raise SemanticError(f"ORDER BY中的无效列引用: {col}")
                if direction not in ["ASC", "DESC"]:
                    raise SemanticError(f"无效的排序方向: {direction}")

    def _analyze_update(self, stmt: UpdateStmt):
        """分析UPDATE语句"""
        # 检查表是否存在
        if not self.catalog.table_exists(stmt.table_name):
            raise SemanticError(f"表 '{stmt.table_name}' 不存在")

        table_info = self.catalog.get_table(stmt.table_name)
        if not table_info:
            raise SemanticError(f"无法获取表 '{stmt.table_name}' 的信息")

        table_columns = [col["name"] for col in table_info["columns"]]
        available_tables = {stmt.table_name: table_columns}

        # 分析赋值语句
        for column, expression in stmt.assignments:
            if column not in table_columns:
                raise SemanticError(f"表 '{stmt.table_name}' 中不存在列 '{column}'")
            self._analyze_expression(expression, available_tables)

        # 分析WHERE子句
        if stmt.where_clause:
            self._analyze_expression(stmt.where_clause, available_tables)

    def _analyze_delete(self, stmt: DeleteStmt):
        """分析DELETE语句"""
        # 检查表是否存在
        if not self.catalog.table_exists(stmt.table_name):
            raise SemanticError(f"表 '{stmt.table_name}' 不存在")

        if stmt.where_clause:
            table_info = self.catalog.get_table(stmt.table_name)
            if not table_info:
                raise SemanticError(f"无法获取表 '{stmt.table_name}' 的信息")

            table_columns = [col["name"] for col in table_info["columns"]]
            available_tables = {stmt.table_name: table_columns}
            self._analyze_expression(stmt.where_clause, available_tables)

    def _analyze_from_clause(self, from_clause: FromClause) -> Dict[str, List[str]]:
        """分析FROM子句，返回可用的表和列"""
        if isinstance(from_clause, TableRef):
            if not self.catalog.table_exists(from_clause.table_name):
                raise SemanticError(f"表 '{from_clause.table_name}' 不存在")

            table_info = self.catalog.get_table(from_clause.table_name)
            if not table_info:
                raise SemanticError(f"无法获取表 '{from_clause.table_name}' 的信息")

            table_columns = [col["name"] for col in table_info["columns"]]

            # 使用别名（如果有）作为键，否则使用表名
            key = from_clause.alias if from_clause.alias else from_clause.table_name
            return {key: table_columns}

        elif isinstance(from_clause, JoinExpr):
            # 递归分析JOIN的左右两边
            left_tables = self._analyze_from_clause(from_clause.left)
            right_tables = self._analyze_from_clause(from_clause.right)

            # 合并可用的表
            available_tables = {**left_tables, **right_tables}

            # 分析ON条件
            if from_clause.on_condition:
                self._analyze_expression(from_clause.on_condition, available_tables)

            return available_tables

        else:
            raise SemanticError(f"不支持的FROM子句类型: {type(from_clause).__name__}")

    def _analyze_expression(self, expr: Expression, available_tables: Dict[str, List[str]]):
        """分析表达式"""
        if isinstance(expr, LiteralExpr):
            # 字面量总是有效的
            pass
        elif isinstance(expr, IdentifierExpr):
            # 检查标识符引用是否有效
            if not self._is_valid_identifier(expr, available_tables):
                if expr.table_name:
                    raise SemanticError(f"无效的列引用: {expr.table_name}.{expr.name}")
                else:
                    raise SemanticError(f"无效的列引用: {expr.name}")
        elif isinstance(expr, BinaryExpr):
            # 递归分析左右操作数
            self._analyze_expression(expr.left, available_tables)
            self._analyze_expression(expr.right, available_tables)
        elif isinstance(expr, FunctionExpr):
            # 分析函数参数
            for arg in expr.arguments:
                if not isinstance(arg, LiteralExpr) or arg.value != "*":
                    self._analyze_expression(arg, available_tables)
        else:
            raise SemanticError(f"不支持的表达式类型: {type(expr).__name__}")

    def _is_valid_column_reference(self, col_ref: str, available_tables: Dict[str, List[str]]) -> bool:
        """检查列引用是否有效"""
        if "." in col_ref:
            # table.column 格式
            parts = col_ref.split(".", 1)
            if len(parts) != 2:
                return False
            table_name, column_name = parts
            return (table_name in available_tables and
                    column_name in available_tables[table_name])
        else:
            # 简单列名，在所有可用表中查找
            for columns in available_tables.values():
                if col_ref in columns:
                    return True

            # 检查是否是聚合函数
            if any(func in col_ref.upper() for func in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("]):
                return True

            return False

    def _is_valid_identifier(self, expr: IdentifierExpr, available_tables: Dict[str, List[str]]) -> bool:
        """检查标识符是否有效"""
        if expr.table_name:
            # table.column 格式
            return (expr.table_name in available_tables and
                    expr.name in available_tables[expr.table_name])
        else:
            # 简单列名
            for columns in available_tables.values():
                if expr.name in columns:
                    return True
            return False

    def _is_valid_data_type(self, data_type: str) -> bool:
        """检查数据类型是否有效"""
        base_types = ["INT", "VARCHAR", "CHAR"]

        for base_type in base_types:
            if data_type == base_type or data_type.startswith(f"{base_type}("):
                return True

        return False