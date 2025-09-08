from typing import Dict, Any, Optional
from sql_compiler.parser.ast_nodes import Expression, BinaryExpr, IdentifierExpr, LiteralExpr
from sql_compiler.semantic.symbol_table import SymbolTable


class TypeChecker:
    """类型检查器"""

    def __init__(self, symbol_table: SymbolTable):
        self.symbol_table = symbol_table
        self.current_table = None

    def set_context_table(self, table_name: str):
        """设置当前上下文表"""
        self.current_table = table_name

    def check_expression_type(self, expr: Expression) -> str:
        """检查表达式类型"""
        if isinstance(expr, LiteralExpr):
            return self._get_literal_type(expr.value)
        elif isinstance(expr, IdentifierExpr):
            return self._get_identifier_type(expr.name)
        elif isinstance(expr, BinaryExpr):
            return self._check_binary_expr_type(expr)
        else:
            return 'UNKNOWN'

    def _get_literal_type(self, value: Any) -> str:
        """获取字面量类型"""
        if isinstance(value, int):
            return 'INT'
        elif isinstance(value, str):
            return 'VARCHAR'
        elif isinstance(value, float):
            return 'FLOAT'
        elif isinstance(value, bool):
            return 'BOOLEAN'
        else:
            return 'UNKNOWN'

    def _get_identifier_type(self, identifier: str) -> str:
        """获取标识符类型"""
        if self.current_table:
            column_type = self.symbol_table.get_column_type(self.current_table, identifier)
            if column_type:
                return column_type

        return 'UNKNOWN'

    def _check_binary_expr_type(self, expr: BinaryExpr) -> str:
        """检查二元表达式类型"""
        left_type = self.check_expression_type(expr.left)
        right_type = self.check_expression_type(expr.right)

        # 逻辑运算符返回布尔值
        if expr.operator in ['AND', 'OR']:
            return 'BOOLEAN'

        # 比较运算符返回布尔值
        if expr.operator in ['=', '<>', '<', '>', '<=', '>=']:
            return 'BOOLEAN'

        # 算术运算符返回数值类型
        if expr.operator in ['+', '-', '*', '/']:
            if left_type == 'INT' and right_type == 'INT':
                return 'INT'
            else:
                return 'FLOAT'

        return 'UNKNOWN'

    def types_compatible(self, type1: str, type2: str) -> bool:
        """检查类型兼容性"""
        if type1 == type2:
            return True

        # 数值类型兼容性
        numeric_types = {'INT', 'FLOAT'}
        if type1 in numeric_types and type2 in numeric_types:
            return True

        # 字符串类型兼容性
        if type1.startswith('VARCHAR') and type2.startswith('VARCHAR'):
            return True
        if type1.startswith('CHAR') and type2.startswith('CHAR'):
            return True
        if (type1.startswith('VARCHAR') and type2.startswith('CHAR')) or \
                (type1.startswith('CHAR') and type2.startswith('VARCHAR')):
            return True

        return False