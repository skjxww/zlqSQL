from typing import List, Any
from sql_compiler.parser.ast_nodes import *
from sql_compiler.semantic.symbol_table import SymbolTable
from sql_compiler.catalog.catalog_manager import CatalogManager
from sql_compiler.exceptions.compiler_errors import SemanticError


class SemanticAnalyzer:
    def __init__(self, catalog: CatalogManager):
        self.catalog = catalog
        self.symbol_table = SymbolTable()
        # 加载已有的表信息到符号表
        self._load_existing_tables()

    def _load_existing_tables(self):
        """从catalog加载已存在的表信息"""
        for table_name in self.catalog.get_all_tables():
            columns = self.catalog.get_table_schema(table_name)
            if columns:
                self.symbol_table.add_table(table_name, columns)

    def analyze(self, ast: Statement):
        """语义分析入口"""
        return ast.accept(self)

    def visit_create_table_stmt(self, stmt: CreateTableStmt):
        """分析CREATE TABLE语句"""
        # 检查表是否已存在
        if self.symbol_table.table_exists(stmt.table_name):
            raise SemanticError(f"表 '{stmt.table_name}' 已存在", 0, 0)

        # 检查列名重复
        column_names = [col[0] for col in stmt.columns]
        if len(column_names) != len(set(column_names)):
            raise SemanticError(f"表 '{stmt.table_name}' 中存在重复的列名", 0, 0)

        # 验证列类型
        for column_name, column_type, constraints in stmt.columns:
            if not self._is_valid_column_type(column_type):
                raise SemanticError(f"无效的列类型: {column_type}", 0, 0)

        # 添加到符号表
        success = self.symbol_table.add_table(stmt.table_name, stmt.columns)
        if not success:
            raise SemanticError(f"无法创建表 '{stmt.table_name}'", 0, 0)

        # 添加到catalog
        self.catalog.create_table(stmt.table_name, stmt.columns)

    def visit_insert_stmt(self, stmt: InsertStmt):
        """分析INSERT语句"""
        # 检查表是否存在
        if not self.symbol_table.table_exists(stmt.table_name):
            raise SemanticError(f"表 '{stmt.table_name}' 不存在", 0, 0)

        table_columns = self.symbol_table.get_table_columns(stmt.table_name)

        # 如果指定了列名
        if stmt.columns:
            # 检查列是否存在
            for column_name in stmt.columns:
                if not self.symbol_table.column_exists(stmt.table_name, column_name):
                    raise SemanticError(f"列 '{column_name}' 在表 '{stmt.table_name}' 中不存在", 0, 0)

            # 检查列数和值数是否匹配
            if len(stmt.columns) != len(stmt.values):
                raise SemanticError(f"列数({len(stmt.columns)})与值数({len(stmt.values)})不匹配", 0, 0)

            # 类型检查
            for i, column_name in enumerate(stmt.columns):
                column_type = self.symbol_table.get_column_type(stmt.table_name, column_name)
                value_type = self._get_expression_type(stmt.values[i])

                if not self._types_compatible(column_type, value_type):
                    raise SemanticError(f"列 '{column_name}' 类型({column_type})与值类型({value_type})不兼容", 0, 0)
        else:
            # 未指定列名，使用表的所有列
            if len(table_columns) != len(stmt.values):
                raise SemanticError(f"表列数({len(table_columns)})与值数({len(stmt.values)})不匹配", 0, 0)

            # 类型检查
            for i, column in enumerate(table_columns):
                column_type = column.data_type
                value_type = self._get_expression_type(stmt.values[i])

                if not self._types_compatible(column_type, value_type):
                    raise SemanticError(f"列 '{column.name}' 类型({column_type})与值类型({value_type})不兼容", 0, 0)

        # 验证表达式
        for value_expr in stmt.values:
            self._validate_expression(value_expr, stmt.table_name)

    def visit_select_stmt(self, stmt: SelectStmt):
        """分析SELECT语句"""
        # 检查表是否存在
        if not self.symbol_table.table_exists(stmt.table_name):
            raise SemanticError(f"表 '{stmt.table_name}' 不存在", 0, 0)

        # 检查选择的列
        if stmt.columns != ["*"]:
            for column_name in stmt.columns:
                if not self.symbol_table.column_exists(stmt.table_name, column_name):
                    raise SemanticError(f"列 '{column_name}' 在表 '{stmt.table_name}' 中不存在", 0, 0)

        # 验证WHERE子句
        if stmt.where_clause:
            self._validate_expression(stmt.where_clause, stmt.table_name)

    def visit_delete_stmt(self, stmt: DeleteStmt):
        """分析DELETE语句"""
        # 检查表是否存在
        if not self.symbol_table.table_exists(stmt.table_name):
            raise SemanticError(f"表 '{stmt.table_name}' 不存在", 0, 0)

        # 验证WHERE子句
        if stmt.where_clause:
            self._validate_expression(stmt.where_clause, stmt.table_name)

    def visit_binary_expr(self, expr: BinaryExpr):
        """分析二元表达式"""
        left_type = expr.left.accept(self)
        right_type = expr.right.accept(self)

        # 类型兼容性检查
        if not self._types_compatible(left_type, right_type):
            raise SemanticError(f"二元表达式两侧类型不兼容: {left_type} {expr.operator} {right_type}", 0, 0)

        # 返回结果类型
        if expr.operator in ['=', '<>', '<', '>', '<=', '>=', 'AND', 'OR']:
            return 'BOOLEAN'
        else:
            return left_type

    def visit_identifier_expr(self, expr: IdentifierExpr):
        """分析标识符表达式"""
        return 'IDENTIFIER'  # 具体类型需要结合上下文确定

    def visit_literal_expr(self, expr: LiteralExpr):
        """分析字面量表达式"""
        if isinstance(expr.value, int):
            return 'INT'
        elif isinstance(expr.value, str):
            return 'VARCHAR'
        else:
            return 'UNKNOWN'

    def _validate_expression(self, expr: Expression, table_name: str):
        """验证表达式中的标识符"""
        if isinstance(expr, IdentifierExpr):
            if not self.symbol_table.column_exists(table_name, expr.name):
                raise SemanticError(f"列 '{expr.name}' 在表 '{table_name}' 中不存在", 0, 0)
        elif isinstance(expr, BinaryExpr):
            self._validate_expression(expr.left, table_name)
            self._validate_expression(expr.right, table_name)

    def _get_expression_type(self, expr: Expression) -> str:
        """获取表达式类型"""
        if isinstance(expr, LiteralExpr):
            if isinstance(expr.value, int):
                return 'INT'
            elif isinstance(expr.value, str):
                return 'VARCHAR'
        elif isinstance(expr, IdentifierExpr):
            return 'IDENTIFIER'  # 需要结合上下文
        elif isinstance(expr, BinaryExpr):
            return 'BOOLEAN'  # 假设二元表达式返回布尔值

        return 'UNKNOWN'

    def _is_valid_column_type(self, column_type: str) -> bool:
        """验证列类型是否有效"""
        base_types = ['INT', 'VARCHAR', 'CHAR']
        if column_type in base_types:
            return True

        # 检查带长度的类型
        for base_type in ['VARCHAR', 'CHAR']:
            if column_type.startswith(f"{base_type}(") and column_type.endswith(")"):
                try:
                    size_str = column_type[len(base_type) + 1:-1]
                    size = int(size_str)
                    return size > 0
                except ValueError:
                    return False

        return False

    def _types_compatible(self, type1: str, type2: str) -> bool:
        """检查类型兼容性"""
        if type1 == type2:
            return True

        # VARCHAR和CHAR兼容
        if (type1.startswith('VARCHAR') or type1.startswith('CHAR')) and \
                (type2.startswith('VARCHAR') or type2.startswith('CHAR')):
            return True

        # 字符串字面量与字符串类型兼容
        if type1 == 'VARCHAR' and type2 in ['VARCHAR', 'CHAR']:
            return True
        if type2 == 'VARCHAR' and type1 in ['VARCHAR', 'CHAR']:
            return True

        return False