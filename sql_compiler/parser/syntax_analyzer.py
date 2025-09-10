from typing import List, Optional
from sql_compiler.lexer.token import Token, TokenType
from sql_compiler.parser.ast_nodes import *
from sql_compiler.exceptions.compiler_errors import SyntaxError as SyntaxErr


class SyntaxAnalyzer:
    """语法分析器 - 支持扩展的SQL语法"""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.current = 0

    def parse(self) -> Statement:
        """解析入口"""
        if self._is_at_end() or (len(self.tokens) == 1 and self.tokens[0].type == TokenType.EOF):
            raise SyntaxErr("空的SQL语句", 1, 1, "SQL语句")

        # 跳过前导空白token
        while self._check(TokenType.NEWLINE):
            self._advance()

        if self._is_at_end():
            raise SyntaxErr("空的SQL语句", 1, 1, "SQL语句")

        stmt = self._parse_statement()

        # 检查分号
        if not self._check(TokenType.SEMICOLON):
            current_token = self._current_token()
            if current_token.type == TokenType.EOF:
                raise SyntaxErr("SQL语句必须以分号结尾",
                                current_token.line, current_token.column, "';'")
            else:
                raise SyntaxErr(f"期望分号，但遇到 '{current_token.lexeme}'",
                                current_token.line, current_token.column, "';'")

        self._advance()  # 消费分号

        # 检查多余token
        while self._check(TokenType.NEWLINE):
            self._advance()

        if not self._is_at_end():
            extra_token = self._current_token()
            if extra_token.type != TokenType.EOF:
                raise SyntaxErr(f"SQL语句结束后不应该有额外的内容: '{extra_token.lexeme}'",
                                extra_token.line, extra_token.column)

        return stmt

    def _parse_statement(self) -> Statement:
        """解析语句"""
        current_token = self._current_token()

        if self._match(TokenType.CREATE):
            return self._parse_create_table()
        elif self._match(TokenType.INSERT):
            return self._parse_insert()
        elif self._match(TokenType.SELECT):
            return self._parse_select()  # SELECT token已经被消费
        elif self._match(TokenType.UPDATE):
            return self._parse_update()
        elif self._match(TokenType.DELETE):
            return self._parse_delete()
        else:
            raise SyntaxErr(f"无效的语句开头: '{current_token.lexeme}'",
                            current_token.line, current_token.column,
                            "CREATE, INSERT, SELECT, UPDATE, DELETE")
    # ==================== CREATE TABLE 解析 ====================

    def _parse_create_table(self) -> CreateTableStmt:
        """解析CREATE TABLE语句"""
        self._expect(TokenType.TABLE)
        table_name = self._expect(TokenType.IDENTIFIER).lexeme

        self._expect(TokenType.LEFT_PAREN)

        if self._check(TokenType.RIGHT_PAREN):
            raise SyntaxErr("表定义不能为空",
                            self._current_token().line,
                            self._current_token().column, "列定义")

        columns = []
        columns.append(self._parse_column_definition())

        while self._match(TokenType.COMMA):
            if self._check(TokenType.RIGHT_PAREN):
                raise SyntaxErr("逗号后不能直接是右括号",
                                self._current_token().line,
                                self._current_token().column, "列定义")
            columns.append(self._parse_column_definition())

        self._expect(TokenType.RIGHT_PAREN)
        return CreateTableStmt(table_name, columns)

    def _parse_column_definition(self) -> tuple:
        """解析列定义"""
        column_name = self._expect(TokenType.IDENTIFIER).lexeme
        column_type = self._parse_column_type()
        return (column_name, column_type, None)

    def _parse_column_type(self) -> str:
        """解析列类型"""
        current_token = self._current_token()

        if self._match(TokenType.INT):
            return "INT"
        elif self._match(TokenType.VARCHAR):
            type_str = "VARCHAR"
            if self._match(TokenType.LEFT_PAREN):
                size = self._expect(TokenType.INTEGER_LITERAL).value
                if size <= 0:
                    raise SyntaxErr("VARCHAR长度必须大于0",
                                    self._previous().line, self._previous().column)
                self._expect(TokenType.RIGHT_PAREN)
                type_str += f"({size})"
            return type_str
        elif self._match(TokenType.CHAR):
            type_str = "CHAR"
            if self._match(TokenType.LEFT_PAREN):
                size = self._expect(TokenType.INTEGER_LITERAL).value
                if size <= 0:
                    raise SyntaxErr("CHAR长度必须大于0",
                                    self._previous().line, self._previous().column)
                self._expect(TokenType.RIGHT_PAREN)
                type_str += f"({size})"
            return type_str
        else:
            raise SyntaxErr(f"无效的列类型: '{current_token.lexeme}'",
                            current_token.line, current_token.column,
                            "INT, VARCHAR, CHAR")

    # ==================== INSERT 解析 ====================

    def _parse_insert(self) -> InsertStmt:
        """解析INSERT语句"""
        self._expect(TokenType.INTO)
        table_name = self._expect(TokenType.IDENTIFIER).lexeme

        columns = None
        if self._match(TokenType.LEFT_PAREN):
            columns = []
            columns.append(self._expect(TokenType.IDENTIFIER).lexeme)

            while self._match(TokenType.COMMA):
                if self._check(TokenType.RIGHT_PAREN):
                    raise SyntaxErr("逗号后不能直接是右括号",
                                    self._current_token().line,
                                    self._current_token().column, "列名")
                columns.append(self._expect(TokenType.IDENTIFIER).lexeme)

            self._expect(TokenType.RIGHT_PAREN)

        self._expect(TokenType.VALUES)
        self._expect(TokenType.LEFT_PAREN)

        if self._check(TokenType.RIGHT_PAREN):
            raise SyntaxErr("VALUES子句不能为空",
                            self._current_token().line,
                            self._current_token().column, "值")

        values = []
        values.append(self._parse_expression())

        while self._match(TokenType.COMMA):
            if self._check(TokenType.RIGHT_PAREN):
                raise SyntaxErr("逗号后不能直接是右括号",
                                self._current_token().line,
                                self._current_token().column, "值")
            values.append(self._parse_expression())

        self._expect(TokenType.RIGHT_PAREN)
        return InsertStmt(table_name, columns, values)

    # ==================== SELECT 解析 ====================

    def _parse_select(self) -> SelectStmt:
        """解析SELECT语句（SELECT token已经被消费）"""
        # 解析选择列表
        columns = self._parse_select_list()

        # 其余代码保持不变...
        # 解析FROM子句
        self._expect(TokenType.FROM)
        from_clause = self._parse_from_clause()

        # 可选的WHERE子句
        where_clause = None
        if self._match(TokenType.WHERE):
            where_clause = self._parse_expression()

        # 可选的GROUP BY子句
        group_by = None
        if self._match(TokenType.GROUP):
            self._expect(TokenType.BY)
            group_by = []
            group_by.append(self._parse_column_reference())

            while self._match(TokenType.COMMA):
                group_by.append(self._parse_column_reference())

        # 可选的HAVING子句
        having_clause = None
        if self._match(TokenType.HAVING):
            having_clause = self._parse_expression()

        # 可选的ORDER BY子句
        order_by = None
        if self._match(TokenType.ORDER):
            self._expect(TokenType.BY)
            order_by = []

            column = self._parse_order_by_item()
            direction = "ASC"  # 默认升序
            if self._match(TokenType.ASC):
                direction = "ASC"
            elif self._match(TokenType.DESC):
                direction = "DESC"
            order_by.append((column, direction))

            while self._match(TokenType.COMMA):
                column = self._parse_order_by_item()
                direction = "ASC"
                if self._match(TokenType.ASC):
                    direction = "ASC"
                elif self._match(TokenType.DESC):
                    direction = "DESC"
                order_by.append((column, direction))

        return SelectStmt(columns, from_clause, where_clause, group_by, having_clause, order_by)

    def _parse_select_list(self) -> List[str]:
        """解析选择列表"""
        columns = []

        if self._match(TokenType.ASTERISK):
            return ["*"]

        # 解析第一个选择项
        columns.append(self._parse_select_item())

        while self._match(TokenType.COMMA):
            columns.append(self._parse_select_item())

        return columns

    def _parse_select_item(self) -> str:
        """解析选择项"""
        # 检查是否是聚合函数
        if self._check_aggregate_function():
            func_name = self._advance().lexeme
            self._expect(TokenType.LEFT_PAREN)

            if self._match(TokenType.ASTERISK):
                # COUNT(*)
                self._expect(TokenType.RIGHT_PAREN)
                return f"{func_name}(*)"
            else:
                # COUNT(column), SUM(column) 等
                column = self._parse_column_reference()
                self._expect(TokenType.RIGHT_PAREN)
                return f"{func_name}({column})"

        # 普通列引用
        return self._parse_column_reference()

    def _parse_order_by_item(self) -> str:
        """解析ORDER BY项"""
        # 可能是聚合函数
        if self._check_aggregate_function():
            func_name = self._advance().lexeme
            self._expect(TokenType.LEFT_PAREN)

            if self._match(TokenType.ASTERISK):
                self._expect(TokenType.RIGHT_PAREN)
                return f"{func_name}(*)"
            else:
                column = self._parse_column_reference()
                self._expect(TokenType.RIGHT_PAREN)
                return f"{func_name}({column})"

        # 普通列引用
        return self._parse_column_reference()

    def _check_aggregate_function(self) -> bool:
        """检查是否是聚合函数"""
        return self._check(TokenType.COUNT, TokenType.SUM, TokenType.AVG,
                           TokenType.MAX, TokenType.MIN)

    def _parse_column_reference(self) -> str:
        """解析列引用 (可能包含表名)"""
        first_part = self._expect(TokenType.IDENTIFIER).lexeme

        if self._match(TokenType.DOT):
            # table.column 格式
            second_part = self._expect(TokenType.IDENTIFIER).lexeme
            return f"{first_part}.{second_part}"

        return first_part

    def _parse_from_clause(self) -> FromClause:
        """解析FROM子句 - 修复版本"""
        # 解析第一个表引用
        from_clause = self._parse_table_reference()

        # 解析可能的JOIN链
        while self._is_join_keyword():
            join_type = self._parse_join_type()
            self._expect(TokenType.JOIN)

            # 解析右表引用
            right_ref = self._parse_table_reference()

            # 解析ON条件
            on_condition = None
            if self._match(TokenType.ON):
                on_condition = self._parse_expression()

            from_clause = JoinExpr(join_type, from_clause, right_ref, on_condition)

        return from_clause

    def _parse_table_reference(self) -> TableRef:
        """解析表引用（表名 + 可选别名）"""
        table_name = self._expect(TokenType.IDENTIFIER).lexeme

        # 检查是否有别名
        alias = None

        # 处理 AS 关键字别名
        if self._match(TokenType.AS):
            # AS 后面必须是标识符
            alias = self._expect(TokenType.IDENTIFIER).lexeme
        # 处理直接标识符别名（如果没有AS关键字）
        elif (self._check(TokenType.IDENTIFIER) and
              not self._is_sql_keyword(self._current_token())):
            alias = self._advance().lexeme

        return TableRef(table_name, alias)

    def _is_join_keyword(self) -> bool:
        """检查是否是JOIN相关关键字"""
        return (self._check(TokenType.JOIN) or
                self._check(TokenType.INNER) or
                self._check(TokenType.LEFT) or
                self._check(TokenType.RIGHT))

    def _parse_join_type(self) -> str:
        """解析JOIN类型"""
        if self._match(TokenType.INNER):
            return "INNER"
        elif self._match(TokenType.LEFT):
            return "LEFT"
        elif self._match(TokenType.RIGHT):
            return "RIGHT"
        else:
            return "INNER"  # 默认INNER JOIN

    def _is_sql_keyword(self, token: Token) -> bool:
        """检查token是否是SQL关键字"""
        sql_keywords = {
            # 基本查询关键字
            TokenType.SELECT, TokenType.FROM, TokenType.WHERE,

            # 分组和排序
            TokenType.GROUP, TokenType.BY, TokenType.ORDER, TokenType.HAVING,
            TokenType.ASC, TokenType.DESC,

            # JOIN相关
            TokenType.JOIN, TokenType.INNER, TokenType.LEFT, TokenType.RIGHT, TokenType.ON,

            # 逻辑运算符
            TokenType.AND, TokenType.OR, TokenType.NOT, TokenType.IN,  # 添加了 IN

            # DML关键字
            TokenType.INSERT, TokenType.INTO, TokenType.VALUES,
            TokenType.UPDATE, TokenType.SET,
            TokenType.DELETE,

            # DDL关键字
            TokenType.CREATE, TokenType.TABLE,

            # 聚合函数
            TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MAX, TokenType.MIN
        }
        return token.type in sql_keywords

    # ==================== UPDATE 解析 ====================

    def _parse_update(self) -> UpdateStmt:
        """解析UPDATE语句"""
        table_name = self._expect(TokenType.IDENTIFIER).lexeme

        self._expect(TokenType.SET)

        # 解析赋值列表
        assignments = []

        # 第一个赋值
        column = self._expect(TokenType.IDENTIFIER).lexeme
        self._expect(TokenType.EQUALS)
        expression = self._parse_expression()  # 这里会正确解析 age + 1
        assignments.append((column, expression))

        # 后续赋值
        while self._match(TokenType.COMMA):
            column = self._expect(TokenType.IDENTIFIER).lexeme
            self._expect(TokenType.EQUALS)
            expression = self._parse_expression()
            assignments.append((column, expression))

        # 可选的WHERE子句
        where_clause = None
        if self._match(TokenType.WHERE):
            where_clause = self._parse_expression()  # 这里会正确解析 customer_id IN (...)

        return UpdateStmt(table_name, assignments, where_clause)

    # ==================== DELETE 解析 ====================

    def _parse_delete(self) -> DeleteStmt:
        """解析DELETE语句"""
        self._expect(TokenType.FROM)
        table_name = self._expect(TokenType.IDENTIFIER).lexeme

        where_clause = None
        if self._match(TokenType.WHERE):
            where_clause = self._parse_expression()

        return DeleteStmt(table_name, where_clause)

    # ==================== 表达式解析 ====================

    def _parse_expression(self) -> Expression:
        """解析表达式"""
        return self._parse_or()

    def _parse_or(self) -> Expression:
        """解析OR表达式"""
        expr = self._parse_and()

        while self._match(TokenType.OR):
            operator = self._previous().lexeme
            right = self._parse_and()
            expr = BinaryExpr(expr, operator, right)

        return expr

    def _parse_and(self) -> Expression:
        """解析AND表达式"""
        expr = self._parse_equality()

        while self._match(TokenType.AND):
            operator = self._previous().lexeme
            right = self._parse_equality()
            expr = BinaryExpr(expr, operator, right)

        return expr

    def _parse_equality(self) -> Expression:
        """解析相等性表达式"""
        expr = self._parse_comparison()

        while self._match(TokenType.EQUALS, TokenType.NOT_EQUALS):
            operator = self._previous().lexeme
            right = self._parse_comparison()
            expr = BinaryExpr(expr, operator, right)

        return expr

    def _parse_comparison(self) -> Expression:
        """解析比较表达式"""
        expr = self._parse_arithmetic()

        while True:
            if self._match(TokenType.GREATER_THAN, TokenType.GREATER_EQUAL,
                           TokenType.LESS_THAN, TokenType.LESS_EQUAL):
                operator = self._previous().lexeme
                right = self._parse_arithmetic()
                expr = BinaryExpr(expr, operator, right)
            elif self._match(TokenType.IN):
                # 处理 IN 表达式
                right_expr = self._parse_in_expression()
                expr = InExpr(expr, right_expr, False)
            elif self._check(TokenType.NOT) and self._peek_next_token().type == TokenType.IN:
                # 处理 NOT IN 表达式
                self._advance()  # 消费 NOT
                self._advance()  # 消费 IN
                right_expr = self._parse_in_expression()
                expr = InExpr(expr, right_expr, True)
            else:
                break

        return expr

    def _parse_subquery(self) -> SelectStmt:
        """解析子查询（括号内的SELECT语句）"""
        # 消费 SELECT token
        self._expect(TokenType.SELECT)

        # 解析选择列表
        columns = self._parse_select_list()

        # 解析FROM子句
        self._expect(TokenType.FROM)
        from_clause = self._parse_from_clause()

        # 可选的WHERE子句
        where_clause = None
        if self._match(TokenType.WHERE):
            where_clause = self._parse_expression()

        # 可选的GROUP BY子句
        group_by = None
        if self._match(TokenType.GROUP):
            self._expect(TokenType.BY)
            group_by = []
            group_by.append(self._parse_column_reference())

            while self._match(TokenType.COMMA):
                group_by.append(self._parse_column_reference())

        # 可选的HAVING子句
        having_clause = None
        if self._match(TokenType.HAVING):
            having_clause = self._parse_expression()

        # 可选的ORDER BY子句
        order_by = None
        if self._match(TokenType.ORDER):
            self._expect(TokenType.BY)
            order_by = []

            column = self._parse_order_by_item()
            direction = "ASC"
            if self._match(TokenType.ASC):
                direction = "ASC"
            elif self._match(TokenType.DESC):
                direction = "DESC"
            order_by.append((column, direction))

            while self._match(TokenType.COMMA):
                column = self._parse_order_by_item()
                direction = "ASC"
                if self._match(TokenType.ASC):
                    direction = "ASC"
                elif self._match(TokenType.DESC):
                    direction = "DESC"
                order_by.append((column, direction))

        return SelectStmt(columns, from_clause, where_clause, group_by, having_clause, order_by)

    def _parse_in_expression(self) -> Expression:
        """解析 IN 表达式的右侧部分"""
        self._expect(TokenType.LEFT_PAREN)

        # 检查是否是子查询
        if self._check(TokenType.SELECT):
            # 解析子查询
            subquery = self._parse_subquery()
            self._expect(TokenType.RIGHT_PAREN)
            return SubqueryExpr(subquery)
        else:
            # 解析值列表
            values = []
            if not self._check(TokenType.RIGHT_PAREN):  # 避免空列表
                values.append(self._parse_expression())
                while self._match(TokenType.COMMA):
                    values.append(self._parse_expression())

            self._expect(TokenType.RIGHT_PAREN)
            return ValueListExpr(values)

    def _peek_next_token(self) -> Token:
        """查看下一个token但不消费"""
        if self.current + 1 < len(self.tokens):
            return self.tokens[self.current + 1]
        return Token(TokenType.EOF, '', 1, 1)

    def _parse_arithmetic(self) -> Expression:
        """解析算术表达式（加法和减法）"""
        expr = self._parse_term()

        while self._match(TokenType.PLUS, TokenType.MINUS):
            operator = self._previous().lexeme
            right = self._parse_term()
            expr = BinaryExpr(expr, operator, right)

        return expr

    def _parse_term(self) -> Expression:
        """解析乘法和除法表达式"""
        expr = self._parse_primary()

        while self._match(TokenType.MULTIPLY, TokenType.DIVIDE, TokenType.ASTERISK):
            operator = self._previous().lexeme
            # 将 * 符号统一处理为乘法
            if operator == '*':
                operator = '*'
            right = self._parse_primary()
            expr = BinaryExpr(expr, operator, right)

        return expr

    def _parse_primary(self) -> Expression:
        """解析基础表达式"""
        if self._match(TokenType.INTEGER_LITERAL):
            return LiteralExpr(self._previous().value)

        if self._match(TokenType.STRING_LITERAL):
            return LiteralExpr(self._previous().value)

        # 检查聚合函数
        if self._check_aggregate_function():
            func_name = self._advance().lexeme
            self._expect(TokenType.LEFT_PAREN)

            arguments = []
            if self._match(TokenType.ASTERISK):
                arguments.append(LiteralExpr("*"))
            else:
                if not self._check(TokenType.RIGHT_PAREN):  # 避免空参数列表
                    arguments.append(self._parse_expression())
                    while self._match(TokenType.COMMA):
                        arguments.append(self._parse_expression())

            self._expect(TokenType.RIGHT_PAREN)
            return FunctionExpr(func_name, arguments)

        if self._match(TokenType.IDENTIFIER):
            identifier = self._previous().lexeme

            # 检查是否是 table.column 格式
            if self._match(TokenType.DOT):
                column = self._expect(TokenType.IDENTIFIER).lexeme
                return IdentifierExpr(column, identifier)

            return IdentifierExpr(identifier)

        if self._match(TokenType.LEFT_PAREN):
            expr = self._parse_expression()
            self._expect(TokenType.RIGHT_PAREN)
            return expr

        current_token = self._current_token()
        if current_token.type == TokenType.EOF:
            raise SyntaxErr("表达式不完整",
                            current_token.line, current_token.column,
                            "标识符、数字、字符串或括号表达式")
        else:
            raise SyntaxErr(f"无效的表达式: '{current_token.lexeme}'",
                            current_token.line, current_token.column,
                            "标识符、数字、字符串或括号表达式")

    # ==================== 辅助方法 ====================

    def _match(self, *token_types: TokenType) -> bool:
        """匹配指定类型的token"""
        for token_type in token_types:
            if self._check(token_type):
                self._advance()
                return True
        return False

    def _check(self, *token_types: TokenType) -> bool:
        """检查当前token类型"""
        if self._is_at_end():
            return TokenType.EOF in token_types
        return self._current_token().type in token_types

    def _advance(self) -> Token:
        """前进到下一个token"""
        if not self._is_at_end():
            self.current += 1
        return self._previous()

    def _is_at_end(self) -> bool:
        """是否到达结尾"""
        return (self.current >= len(self.tokens) or
                self._current_token().type == TokenType.EOF)

    def _current_token(self) -> Token:
        """获取当前token"""
        if self.current < len(self.tokens):
            return self.tokens[self.current]
        return Token(TokenType.EOF, '', 1, 1)

    def _previous(self) -> Token:
        """获取前一个token"""
        if self.current > 0:
            return self.tokens[self.current - 1]
        return self.tokens[0] if self.tokens else Token(TokenType.EOF, '', 1, 1)

    def _expect(self, token_type: TokenType) -> Token:
        """期望特定类型的token"""
        if self._check(token_type):
            return self._advance()

        current = self._current_token()

        if current.type == TokenType.EOF:
            raise SyntaxErr(f"意外的语句结束，期望 '{token_type.value}'",
                            current.line, current.column, token_type.value)
        else:
            raise SyntaxErr(f"期望 '{token_type.value}'，但遇到 '{current.lexeme}'",
                            current.line, current.column, token_type.value)