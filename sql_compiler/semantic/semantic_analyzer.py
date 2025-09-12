from sql_compiler.parser.ast_nodes import *
from sql_compiler.semantic.symbol_table import SymbolTable
from catalog.catalog_manager import CatalogManager
from sql_compiler.exceptions.compiler_errors import SemanticError


class SemanticAnalyzer:
    """语义分析器 - 扩展支持新语法和类型检查"""

    def __init__(self, catalog: CatalogManager):
        self.catalog = catalog
        self.symbol_table = SymbolTable()
        # 添加别名追踪
        self.current_aliases = {}  # 当前查询中的别名映射
        self.alias_to_real = {}  # 别名 -> 真实表名

    def analyze(self, stmt: Statement):
        """分析语句"""
        # 重置别名信息
        self.current_aliases = {}
        self.alias_to_real = {}

        if isinstance(stmt, CreateIndexStmt):
            self._analyze_create_index(stmt)
        elif isinstance(stmt, DropIndexStmt):
            self._analyze_drop_index(stmt)
        elif isinstance(stmt, ShowIndexesStmt):
            self._analyze_show_indexes(stmt)
        elif isinstance(stmt, CreateTableStmt):
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

    def _analyze_create_index(self, stmt: CreateIndexStmt):
        """分析CREATE INDEX语句"""
        # 检查表是否存在
        if not self.catalog.table_exists(stmt.table_name):
            raise SemanticError(f"表 '{stmt.table_name}' 不存在")

        # 检查列是否存在
        table_columns = [col["name"] for col in self.catalog.get_table(stmt.table_name)["columns"]]
        for col in stmt.columns:
            if col not in table_columns:
                raise SemanticError(f"表 '{stmt.table_name}' 中不存在列 '{col}'")

        # 检查索引名是否重复
        if stmt.index_name in self.catalog.indexes:
            raise SemanticError(f"索引 '{stmt.index_name}' 已存在")

    def _analyze_drop_index(self, stmt: DropIndexStmt):
        """分析DROP INDEX语句"""
        # 检查索引是否存在
        if stmt.index_name not in self.catalog.indexes:
            raise SemanticError(f"索引 '{stmt.index_name}' 不存在")

        # 检查是否有权限删除索引（可选）
        index_info = self.catalog.indexes[stmt.index_name]
        if index_info.get('system_index', False):
            raise SemanticError(f"无法删除系统索引 '{stmt.index_name}'")

        # 检查是否有依赖的约束（如主键、唯一约束等）
        if self._has_constraint_dependency(stmt.index_name):
            raise SemanticError(f"索引 '{stmt.index_name}' 被约束依赖，无法删除")

    def _analyze_show_indexes(self, stmt: ShowIndexesStmt):
        """分析SHOW INDEXES语句"""
        if stmt.table_name:
            # 检查表是否存在
            if not self.catalog.table_exists(stmt.table_name):
                raise SemanticError(f"表 '{stmt.table_name}' 不存在")

        # SHOW INDEXES语句不需要特殊的语义检查，主要是权限检查
        # 这里可以添加权限检查逻辑
        if not self._has_show_privilege():
            raise SemanticError("没有查看索引信息的权限")

    def _has_constraint_dependency(self, index_name: str) -> bool:
        """检查索引是否被约束依赖"""
        # 简化实现 - 实际应该检查主键、外键、唯一约束等
        index_info = self.catalog.indexes.get(index_name)
        if index_info:
            return index_info.get('unique', False) or 'primary' in index_name.lower()
        return False

    def _has_show_privilege(self) -> bool:
        """检查是否有显示权限"""
        # 简化实现 - 实际应该检查用户权限
        return True

    def _analyze_from_clause(self, from_clause: FromClause) -> Dict[str, List[str]]:
        """分析FROM子句，返回可用的表和列 - 增强别名支持"""
        if isinstance(from_clause, TableRef):
            if not self.catalog.table_exists(from_clause.table_name):
                raise SemanticError(f"表 '{from_clause.table_name}' 不存在")

            table_info = self.catalog.get_table(from_clause.table_name)
            if not table_info:
                raise SemanticError(f"无法获取表 '{from_clause.table_name}' 的信息")

            table_columns = [col["name"] for col in table_info["columns"]]
            real_table_name = from_clause.table_name

            # 构建结果字典
            result = {}

            # 处理别名
            if from_clause.alias:
                table_alias = from_clause.alias

                # 建立别名映射
                self.alias_to_real[table_alias] = real_table_name
                self.current_aliases[table_alias] = table_columns

                # 在结果中主要使用别名
                result[table_alias] = table_columns
                # 也保留真实表名，用于兼容性
                result[real_table_name] = table_columns
            else:
                # 没有别名
                self.current_aliases[real_table_name] = table_columns
                result[real_table_name] = table_columns

            return result

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

    def get_current_aliases(self) -> Dict[str, str]:
        """获取当前的别名映射"""
        return self.alias_to_real.copy()

    def resolve_table_reference(self, table_ref: str) -> str:
        """解析表引用，返回真实表名"""
        return self.alias_to_real.get(table_ref, table_ref)

    def get_real_table_name(self, table_identifier: str) -> str:
        """获取表的真实名称（处理别名）"""
        if table_identifier in self.alias_to_real:
            return self.alias_to_real[table_identifier]
        return table_identifier

    def get_table_alias(self, real_table_name: str) -> str:
        """获取表的别名（如果有的话）"""
        return self.real_to_alias.get(real_table_name, real_table_name)

    def get_alias_info(self) -> Dict[str, Any]:
        """获取别名信息"""
        return {
            'alias_to_real': self.alias_to_real,
            'real_to_alias': self.real_to_alias,
            'all_aliases': list(self.table_aliases.keys())
        }

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
        column_types = self.catalog.get_table_column_types(stmt.table_name)

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

        # 检查类型匹配
        for i, (column, value_expr) in enumerate(zip(target_columns, stmt.values)):
            expected_type = column_types.get(column)
            if expected_type:
                value_type = self._get_expression_type(value_expr, {stmt.table_name: table_columns})
                if not self._is_type_compatible(value_type, expected_type):
                    raise SemanticError(f"列 '{column}' 期望类型 '{expected_type}'，但得到 '{value_type}'")

        # 分析每个值表达式
        available_tables = {stmt.table_name: table_columns}
        for value in stmt.values:
            self._analyze_expression(value, available_tables)

    def _analyze_select(self, stmt: SelectStmt):
        """分析SELECT语句 - 增强列引用验证"""
        # 分析FROM子句
        available_tables = self._analyze_from_clause(stmt.from_clause)

        # 分析选择列表
        if stmt.columns != ["*"]:
            for col in stmt.columns:
                if not self._is_valid_column_reference(col, available_tables):
                    # 提供更具体的错误信息
                    if self._is_aggregate_function_call(col):
                        func_name, args_str = self._parse_function_call(col)
                        args = self._parse_function_arguments(args_str)
                        for arg in args:
                            arg = arg.strip()
                            if arg != "*" and not arg.isdigit() and not (arg.startswith("'") and arg.endswith("'")):
                                if not self._is_valid_simple_column_reference(arg, available_tables):
                                    raise SemanticError(f"聚合函数 {func_name} 中的列引用无效: {arg}")
                        raise SemanticError(f"聚合函数调用无效: {col}")
                    else:
                        raise SemanticError(f"无效的列引用: {col}")

            # 验证GROUP BY规则
            self._validate_group_by_rules(stmt, available_tables)

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

    def _validate_group_by_rules(self, stmt: SelectStmt, available_tables: Dict[str, List[str]]):
        """验证GROUP BY规则"""
        if stmt.columns == ["*"]:
            # SELECT * 的情况特殊处理
            if stmt.group_by:
                # SELECT * 不能和 GROUP BY 一起使用
                raise SemanticError("SELECT * 不能与GROUP BY一起使用")
            return

        # 分析选择列表中的列类型
        aggregate_columns = []
        non_aggregate_columns = []

        for col in stmt.columns:
            if self._is_aggregate_function_call(col):
                aggregate_columns.append(col)
            else:
                # 检查是否是简单的列引用
                if self._is_simple_column_reference(col, available_tables):
                    non_aggregate_columns.append(col)
                # 其他情况（如计算表达式）暂时允许

        # 如果既有聚合列又有非聚合列
        if aggregate_columns and non_aggregate_columns:
            if not stmt.group_by:
                # 没有GROUP BY，但有混合列类型
                non_agg_list = ', '.join(non_aggregate_columns)
                raise SemanticError(f"查询包含聚合函数和非聚合列，必须使用GROUP BY。非聚合列: {non_agg_list}")
            else:
                # 有GROUP BY，检查所有非聚合列是否都在GROUP BY中
                for col in non_aggregate_columns:
                    if col not in stmt.group_by:
                        raise SemanticError(f"列 '{col}' 必须出现在GROUP BY子句中，或者在聚合函数中使用")

    def _is_aggregate_function_call(self, column_ref: str) -> bool:
        """检查是否是聚合函数调用"""
        if not isinstance(column_ref, str):
            return False

        upper_col = column_ref.upper().strip()
        aggregate_functions = ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("]
        return any(upper_col.startswith(func) or func in upper_col for func in aggregate_functions)

    def _is_simple_column_reference(self, column_ref: str, available_tables: Dict[str, List[str]]) -> bool:
        """检查是否是简单的列引用（非聚合函数）"""
        if not isinstance(column_ref, str):
            return False

        # 如果包含聚合函数，就不是简单列引用
        if self._is_aggregate_function_call(column_ref):
            return False

        # 检查是否包含复杂表达式（简化版本）
        # 如果包含运算符，认为是复杂表达式
        if any(op in column_ref for op in ['+', '-', '*', '/', '(', ')']):
            return False

        # 检查是否是有效的列引用
        return self._is_valid_column_reference(column_ref, available_tables)

    def _analyze_update(self, stmt: UpdateStmt):
        """分析UPDATE语句 - 增强类型检查"""
        # 检查表是否存在
        if not self.catalog.table_exists(stmt.table_name):
            raise SemanticError(f"表 '{stmt.table_name}' 不存在")

        table_info = self.catalog.get_table(stmt.table_name)
        if not table_info:
            raise SemanticError(f"无法获取表 '{stmt.table_name}' 的信息")

        table_columns = [col["name"] for col in table_info["columns"]]
        column_types = self.catalog.get_table_column_types(stmt.table_name)
        available_tables = {stmt.table_name: table_columns}

        # 分析赋值语句，包括类型检查
        for column, expression in stmt.assignments:
            if column not in table_columns:
                raise SemanticError(f"表 '{stmt.table_name}' 中不存在列 '{column}'")

            # 类型检查
            expected_type = column_types.get(column)
            if expected_type:
                expression_type = self._get_expression_type(expression, available_tables)
                if not self._is_type_compatible(expression_type, expected_type):
                    raise SemanticError(
                        f"列 '{column}' 期望类型 '{expected_type}'，但表达式返回类型 '{expression_type}'")

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

    def _get_expression_type(self, expr: Expression, available_tables: Dict[str, List[str]]) -> str:
        """获取表达式的类型"""
        if isinstance(expr, LiteralExpr):
            if isinstance(expr.value, int):
                return "INT"
            elif isinstance(expr.value, str):
                return "VARCHAR"
            elif isinstance(expr.value, float):
                return "FLOAT"  # 如果支持浮点数
            else:
                return "UNKNOWN"

        elif isinstance(expr, IdentifierExpr):
            # 查找列的类型
            for table_name, columns in available_tables.items():
                if expr.name in columns:
                    # 获取该列的实际类型
                    column_type = self.catalog.get_column_type(table_name, expr.name)
                    return column_type if column_type else "UNKNOWN"
            return "UNKNOWN"

        elif isinstance(expr, BinaryExpr):
            # 二元表达式的类型推断
            left_type = self._get_expression_type(expr.left, available_tables)
            right_type = self._get_expression_type(expr.right, available_tables)

            # 算术运算结果通常是数值类型
            if expr.operator in ['+', '-', '*', '/']:
                if left_type == "INT" and right_type == "INT":
                    return "INT"
                else:
                    return "NUMERIC"  # 通用数值类型

            # 比较运算结果是布尔类型
            elif expr.operator in ['=', '<>', '<', '>', '<=', '>=']:
                return "BOOLEAN"

            # 逻辑运算结果是布尔类型
            elif expr.operator in ['AND', 'OR']:
                return "BOOLEAN"

            return "UNKNOWN"

        elif isinstance(expr, FunctionExpr):
            # 聚合函数的类型推断
            if expr.function_name.upper() == "COUNT":
                return "INT"
            elif expr.function_name.upper() in ["SUM", "AVG"]:
                return "NUMERIC"
            elif expr.function_name.upper() in ["MAX", "MIN"]:
                # MAX/MIN 的类型取决于参数类型
                if expr.arguments:
                    return self._get_expression_type(expr.arguments[0], available_tables)
                return "UNKNOWN"
            return "UNKNOWN"

        elif isinstance(expr, InExpr):
            return "BOOLEAN"

        elif isinstance(expr, SubqueryExpr):
            # 子查询的类型取决于其选择的列
            return "UNKNOWN"  # 简化处理

        elif isinstance(expr, ValueListExpr):
            # 值列表的类型取决于第一个值的类型
            if expr.values:
                return self._get_expression_type(expr.values[0], available_tables)
            return "UNKNOWN"

        return "UNKNOWN"

    def _is_type_compatible(self, actual_type: str, expected_type: str) -> bool:
        """检查类型是否兼容"""
        if actual_type == "UNKNOWN" or expected_type == "UNKNOWN":
            return True  # 未知类型暂时允许

        # 完全匹配
        if actual_type == expected_type:
            return True

        # VARCHAR 和 CHAR 兼容
        if (actual_type.startswith("VARCHAR") and expected_type.startswith("CHAR")) or \
                (actual_type.startswith("CHAR") and expected_type.startswith("VARCHAR")):
            return True

        # 字符串字面量可以赋值给任何字符串类型
        if actual_type == "VARCHAR" and (expected_type.startswith("VARCHAR") or expected_type.startswith("CHAR")):
            return True

        # 数值类型兼容
        numeric_types = {"INT", "NUMERIC", "FLOAT"}
        if actual_type in numeric_types and expected_type in numeric_types:
            return True

        return False

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
        elif isinstance(expr, InExpr):
            # 分析 IN 表达式
            self._analyze_expression(expr.left_expr, available_tables)
            self._analyze_expression(expr.right_expr, available_tables)
        elif isinstance(expr, SubqueryExpr):
            # 分析子查询
            self.analyze(expr.select_stmt)
        elif isinstance(expr, ValueListExpr):
            # 分析值列表
            for value in expr.values:
                self._analyze_expression(value, available_tables)
        else:
            raise SemanticError(f"不支持的表达式类型: {type(expr).__name__}")

    def _is_valid_column_reference(self, col_ref: str, available_tables: Dict[str, List[str]]) -> bool:
        """检查列引用是否有效 - 增强聚合函数参数验证"""

        # 检查是否是聚合函数调用
        if self._is_aggregate_function_call(col_ref):
            return self._validate_aggregate_function_arguments(col_ref, available_tables)

        if "." in col_ref:
            # table.column 格式
            parts = col_ref.split(".", 1)
            if len(parts) != 2:
                return False
            table_name, column_name = parts

            # 检查表名（可能是别名）是否存在
            if table_name not in available_tables:
                return False

            # 检查列名是否在该表中
            return column_name in available_tables[table_name]
        else:
            # 简单列名，在所有可用表中查找
            for columns in available_tables.values():
                if col_ref in columns:
                    return True
            return False

    def _validate_aggregate_function_arguments(self, func_call: str, available_tables: Dict[str, List[str]]) -> bool:
        """验证聚合函数的参数"""
        try:
            # 解析函数调用
            func_name, args_str = self._parse_function_call(func_call)

            if not func_name:
                return False

            # 解析参数
            args = self._parse_function_arguments(args_str)

            # 验证每个参数
            for arg in args:
                arg = arg.strip()
                if arg == "*":
                    # COUNT(*) 总是有效的
                    continue
                elif arg.isdigit() or (arg.startswith("'") and arg.endswith("'")):
                    # 字面量总是有效的
                    continue
                else:
                    # 检查列引用是否存在
                    if not self._is_valid_simple_column_reference(arg, available_tables):
                        return False

            return True

        except Exception:
            # 解析失败，认为无效
            return False

    def _parse_function_call(self, func_call: str) -> tuple:
        """解析函数调用，返回(函数名, 参数字符串)"""
        func_call = func_call.strip()

        # 查找左括号
        paren_pos = func_call.find('(')
        if paren_pos == -1:
            return None, None

        func_name = func_call[:paren_pos].strip().upper()

        # 查找匹配的右括号
        paren_count = 0
        start_pos = paren_pos + 1
        end_pos = len(func_call) - 1

        for i in range(paren_pos, len(func_call)):
            if func_call[i] == '(':
                paren_count += 1
            elif func_call[i] == ')':
                paren_count -= 1
                if paren_count == 0:
                    end_pos = i
                    break

        args_str = func_call[start_pos:end_pos].strip()
        return func_name, args_str

    def _parse_function_arguments(self, args_str: str) -> list:
        """解析函数参数列表"""
        if not args_str:
            return []

        # 简单的参数分割（不处理嵌套函数）
        args = []
        current_arg = ""
        paren_count = 0

        for char in args_str:
            if char == ',' and paren_count == 0:
                args.append(current_arg.strip())
                current_arg = ""
            else:
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                current_arg += char

        if current_arg.strip():
            args.append(current_arg.strip())

        return args

    def _is_valid_simple_column_reference(self, col_ref: str, available_tables: Dict[str, List[str]]) -> bool:
        """检查简单的列引用是否有效（不包含聚合函数）"""
        col_ref = col_ref.strip()

        if "." in col_ref:
            # table.column 格式
            parts = col_ref.split(".", 1)
            if len(parts) != 2:
                return False
            table_name, column_name = parts

            # 检查表名（可能是别名）是否存在
            if table_name not in available_tables:
                return False

            # 检查列名是否在该表中
            return column_name in available_tables[table_name]
        else:
            # 简单列名，在所有可用表中查找
            for columns in available_tables.values():
                if col_ref in columns:
                    return True
            return False

    def _is_valid_identifier(self, expr: IdentifierExpr, available_tables: Dict[str, List[str]]) -> bool:
        """检查标识符是否有效"""
        if expr.table_name:
            # table.column 格式
            if expr.table_name not in available_tables:
                return False
            return expr.name in available_tables[expr.table_name]
        else:
            # 简单列名，在所有可用表中查找
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