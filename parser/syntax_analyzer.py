from typing import List, Optional
from sql_compiler.lexer.token import Token, TokenType
from sql_compiler.parser.ast_nodes import *
from sql_compiler.exceptions.compiler_errors import SyntaxError as SyntaxErr


class SyntaxAnalyzer:
    """语法分析器 - 递归下降解析器"""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.current = 0

    def parse(self) -> Statement:
        """解析入口 - 解析一个完整的SQL语句"""
        if self._is_at_end() or (len(self.tokens) == 1 and self.tokens[0].type == TokenType.EOF):
            raise SyntaxErr("空的SQL语句", 1, 1, "SQL语句")

        # 跳过前导空白token（如果有的话）
        while self._check(TokenType.NEWLINE):
            self._advance()

        if self._is_at_end():
            raise SyntaxErr("空的SQL语句", 1, 1, "SQL语句")

        # 解析语句
        stmt = self._parse_statement()

        # 严格检查分号
        if not self._check(TokenType.SEMICOLON):
            current_token = self._current_token()
            if current_token.type == TokenType.EOF:
                raise SyntaxErr("SQL语句必须以分号结尾",
                                current_token.line,
                                current_token.column,
                                "';'")
            else:
                raise SyntaxErr(f"期望分号，但遇到 '{current_token.lexeme}'",
                                current_token.line,
                                current_token.column,
                                "';'")

        self._advance()  # 消费分号

        # 检查是否还有多余的token（除了EOF）
        while self._check(TokenType.NEWLINE):
            self._advance()

        if not self._is_at_end():
            extra_token = self._current_token()
            if extra_token.type != TokenType.EOF:
                raise SyntaxErr(f"SQL语句结束后不应该有额外的内容: '{extra_token.lexeme}'",
                                extra_token.line,
                                extra_token.column)

        return stmt

    def _parse_statement(self) -> Statement:
        """解析语句"""
        current_token = self._current_token()

        if self._match(TokenType.CREATE):
            return self._parse_create_table()
        elif self._match(TokenType.INSERT):
            return self._parse_insert()
        elif self._match(TokenType.SELECT):
            return self._parse_select()
        elif self._match(TokenType.DELETE):
            return self._parse_delete()
        else:
            raise SyntaxErr(f"无效的语句开头: '{current_token.lexeme}'",
                            current_token.line,
                            current_token.column,
                            "CREATE, INSERT, SELECT, DELETE")

    def _parse_create_table(self) -> CreateTableStmt:
        """解析CREATE TABLE语句"""
        if not self._check(TokenType.TABLE):
            current_token = self._current_token()
            if current_token.type == TokenType.EOF:
                raise SyntaxErr("CREATE语句不完整，期望TABLE关键字",
                                current_token.line, current_token.column, "TABLE")
            else:
                raise SyntaxErr(f"CREATE语句后应该跟TABLE，但遇到 '{current_token.lexeme}'",
                                current_token.line, current_token.column, "TABLE")

        self._advance()  # 消费TABLE

        if not self._check(TokenType.IDENTIFIER):
            current_token = self._current_token()
            if current_token.type == TokenType.EOF:
                raise SyntaxErr("TABLE关键字后缺少表名",
                                current_token.line, current_token.column, "表名")
            else:
                raise SyntaxErr(f"TABLE后应该是表名，但遇到 '{current_token.lexeme}'",
                                current_token.line, current_token.column, "表名")

        table_name = self._advance().lexeme

        self._expect(TokenType.LEFT_PAREN)
        columns = []

        # 检查空列定义
        if self._check(TokenType.RIGHT_PAREN):
            raise SyntaxErr("表定义不能为空",
                            self._current_token().line,
                            self._current_token().column,
                            "列定义")

        # 解析第一个列定义
        columns.append(self._parse_column_definition())

        # 解析后续列定义
        while self._match(TokenType.COMMA):
            # 检查逗号后是否直接遇到右括号
            if self._check(TokenType.RIGHT_PAREN):
                raise SyntaxErr("逗号后不能直接是右括号",
                                self._current_token().line,
                                self._current_token().column,
                                "列定义")

            columns.append(self._parse_column_definition())

        self._expect(TokenType.RIGHT_PAREN)
        return CreateTableStmt(table_name, columns)

    def _parse_column_definition(self) -> tuple:
        """解析列定义 - 返回 (column_name, column_type, constraints)"""
        column_name = self._expect(TokenType.IDENTIFIER).lexeme
        column_type = self._parse_column_type()
        return (column_name, column_type, None)  # 暂不支持约束

    def _parse_column_type(self) -> str:
        """解析列类型"""
        current_token = self._current_token()

        if self._match(TokenType.INT):
            return "INT"
        elif self._match(TokenType.VARCHAR):
            type_str = "VARCHAR"
            if self._match(TokenType.LEFT_PAREN):
                if not self._check(TokenType.INTEGER_LITERAL):
                    current = self._current_token()
                    if current.type == TokenType.EOF:
                        raise SyntaxErr("VARCHAR类型定义不完整，缺少长度",
                                        current.line, current.column, "数字")
                    else:
                        raise SyntaxErr(f"VARCHAR长度必须是数字，但遇到 '{current.lexeme}'",
                                        current.line, current.column, "数字")

                size = self._advance().value
                if size <= 0:
                    raise SyntaxErr("VARCHAR长度必须大于0",
                                    self._previous().line,
                                    self._previous().column)

                self._expect(TokenType.RIGHT_PAREN)
                type_str += f"({size})"
            return type_str
        elif self._match(TokenType.CHAR):
            type_str = "CHAR"
            if self._match(TokenType.LEFT_PAREN):
                if not self._check(TokenType.INTEGER_LITERAL):
                    current = self._current_token()
                    if current.type == TokenType.EOF:
                        raise SyntaxErr("CHAR类型定义不完整，缺少长度",
                                        current.line, current.column, "数字")
                    else:
                        raise SyntaxErr(f"CHAR长度必须是数字，但遇到 '{current.lexeme}'",
                                        current.line, current.column, "数字")

                size = self._advance().value
                if size <= 0:
                    raise SyntaxErr("CHAR长度必须大于0",
                                    self._previous().line,
                                    self._previous().column)

                self._expect(TokenType.RIGHT_PAREN)
                type_str += f"({size})"
            return type_str
        else:
            if current_token.type == TokenType.EOF:
                raise SyntaxErr("列定义不完整，缺少数据类型",
                                current_token.line,
                                current_token.column,
                                "INT, VARCHAR, CHAR")
            else:
                raise SyntaxErr(f"无效的列类型: '{current_token.lexeme}'",
                                current_token.line,
                                current_token.column,
                                "INT, VARCHAR, CHAR")

    def _parse_insert(self) -> InsertStmt:
        """解析INSERT语句"""
        if not self._check(TokenType.INTO):
            current_token = self._current_token()
            if current_token.type == TokenType.EOF:
                raise SyntaxErr("INSERT语句不完整，期望INTO关键字",
                                current_token.line, current_token.column, "INTO")
            else:
                raise SyntaxErr(f"INSERT后应该跟INTO，但遇到 '{current_token.lexeme}'",
                                current_token.line, current_token.column, "INTO")

        self._advance()  # 消费INTO

        table_name = self._expect(TokenType.IDENTIFIER).lexeme

        # 可选的列名列表
        columns = None
        if self._match(TokenType.LEFT_PAREN):
            columns = []
            columns.append(self._expect(TokenType.IDENTIFIER).lexeme)

            while self._match(TokenType.COMMA):
                if self._check(TokenType.RIGHT_PAREN):
                    raise SyntaxErr("逗号后不能直接是右括号",
                                    self._current_token().line,
                                    self._current_token().column,
                                    "列名")
                columns.append(self._expect(TokenType.IDENTIFIER).lexeme)

            self._expect(TokenType.RIGHT_PAREN)

        # VALUES关键字
        if not self._check(TokenType.VALUES):
            current_token = self._current_token()
            if current_token.type == TokenType.EOF:
                raise SyntaxErr("INSERT语句不完整，期望VALUES关键字",
                                current_token.line, current_token.column, "VALUES")
            else:
                raise SyntaxErr(f"期望VALUES关键字，但遇到 '{current_token.lexeme}'",
                                current_token.line, current_token.column, "VALUES")

        self._advance()  # 消费VALUES

        self._expect(TokenType.LEFT_PAREN)

        # 解析值列表
        if self._check(TokenType.RIGHT_PAREN):
            raise SyntaxErr("VALUES子句不能为空",
                            self._current_token().line,
                            self._current_token().column,
                            "值")

        values = []
        values.append(self._parse_expression())

        while self._match(TokenType.COMMA):
            if self._check(TokenType.RIGHT_PAREN):
                raise SyntaxErr("逗号后不能直接是右括号",
                                self._current_token().line,
                                self._current_token().column,
                                "值")
            values.append(self._parse_expression())

        self._expect(TokenType.RIGHT_PAREN)
        return InsertStmt(table_name, columns, values)

    def _parse_select(self) -> SelectStmt:
        """解析SELECT语句"""
        # 解析选择列
        columns = []

        # 支持 * 通配符
        if self._match(TokenType.ASTERISK):
            columns = ["*"]
        else:
            if not self._check(TokenType.IDENTIFIER):
                current_token = self._current_token()
                if current_token.type == TokenType.EOF:
                    raise SyntaxErr("SELECT语句不完整，缺少列名",
                                    current_token.line, current_token.column,
                                    "列名或*")
                else:
                    raise SyntaxErr(f"SELECT后应该是列名或*，但遇到 '{current_token.lexeme}'",
                                    current_token.line, current_token.column,
                                    "列名或*")

            columns.append(self._advance().lexeme)
            while self._match(TokenType.COMMA):
                if not self._check(TokenType.IDENTIFIER):
                    current_token = self._current_token()
                    if current_token.type == TokenType.EOF:
                        raise SyntaxErr("逗号后缺少列名",
                                        current_token.line, current_token.column,
                                        "列名")
                    else:
                        raise SyntaxErr(f"逗号后应该是列名，但遇到 '{current_token.lexeme}'",
                                        current_token.line, current_token.column,
                                        "列名")
                columns.append(self._advance().lexeme)

        # FROM关键字
        if not self._check(TokenType.FROM):
            current_token = self._current_token()
            if current_token.type == TokenType.EOF:
                raise SyntaxErr("SELECT语句缺少FROM子句",
                                current_token.line, current_token.column,
                                "FROM")
            else:
                raise SyntaxErr(f"SELECT语句缺少FROM子句，遇到 '{current_token.lexeme}'",
                                current_token.line, current_token.column,
                                "FROM")

        self._advance()  # 消费FROM

        # 表名
        if not self._check(TokenType.IDENTIFIER):
            current_token = self._current_token()
            if current_token.type == TokenType.EOF:
                raise SyntaxErr("FROM后缺少表名",
                                current_token.line, current_token.column,
                                "表名")
            else:
                raise SyntaxErr(f"FROM后应该是表名，但遇到 '{current_token.lexeme}'",
                                current_token.line, current_token.column,
                                "表名")

        table_name = self._advance().lexeme

        # 可选的WHERE子句
        where_clause = None
        if self._match(TokenType.WHERE):
            where_clause = self._parse_expression()

        return SelectStmt(columns, table_name, where_clause)

    def _parse_delete(self) -> DeleteStmt:
        """解析DELETE语句"""
        if not self._check(TokenType.FROM):
            current_token = self._current_token()
            if current_token.type == TokenType.EOF:
                raise SyntaxErr("DELETE语句不完整，期望FROM关键字",
                                current_token.line, current_token.column, "FROM")
            else:
                raise SyntaxErr(f"DELETE后应该跟FROM，但遇到 '{current_token.lexeme}'",
                                current_token.line, current_token.column, "FROM")

        self._advance()  # 消费FROM

        table_name = self._expect(TokenType.IDENTIFIER).lexeme

        # 可选的WHERE子句
        where_clause = None
        if self._match(TokenType.WHERE):
            where_clause = self._parse_expression()

        return DeleteStmt(table_name, where_clause)

    # ==================== 表达式解析 ====================

    def _parse_expression(self) -> Expression:
        """解析表达式（处理OR）"""
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
        expr = self._parse_primary()

        while self._match(TokenType.GREATER_THAN, TokenType.GREATER_EQUAL,
                          TokenType.LESS_THAN, TokenType.LESS_EQUAL):
            operator = self._previous().lexeme
            right = self._parse_primary()
            expr = BinaryExpr(expr, operator, right)

        return expr

    def _parse_primary(self) -> Expression:
        """解析基础表达式"""
        if self._match(TokenType.INTEGER_LITERAL):
            return LiteralExpr(self._previous().value)

        if self._match(TokenType.STRING_LITERAL):
            return LiteralExpr(self._previous().value)

        if self._match(TokenType.IDENTIFIER):
            return IdentifierExpr(self._previous().lexeme)

        if self._match(TokenType.LEFT_PAREN):
            expr = self._parse_expression()
            self._expect(TokenType.RIGHT_PAREN)
            return expr

        current_token = self._current_token()
        if current_token.type == TokenType.EOF:
            raise SyntaxErr("表达式不完整",
                            current_token.line,
                            current_token.column,
                            "标识符、数字、字符串或括号表达式")
        else:
            raise SyntaxErr(f"无效的表达式: '{current_token.lexeme}'",
                            current_token.line,
                            current_token.column,
                            "标识符、数字、字符串或括号表达式")

    # ==================== 辅助方法 ====================

    def _match(self, *token_types: TokenType) -> bool:
        """匹配指定类型的token"""
        for token_type in token_types:
            if self._check(token_type):
                self._advance()
                return True
        return False

    def _check(self, token_type: TokenType) -> bool:
        """检查当前token类型"""
        if self._is_at_end():
            return token_type == TokenType.EOF
        return self._current_token().type == token_type

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
        # 如果超出范围，返回一个虚拟的EOF token
        return Token(TokenType.EOF, '', 1, 1)

    def _previous(self) -> Token:
        """获取前一个token"""
        if self.current > 0:
            return self.tokens[self.current - 1]
        return self.tokens[0] if self.tokens else Token(TokenType.EOF, '', 1, 1)

    def _expect(self, token_type: TokenType) -> Token:
        """期望特定类型的token，改进错误信息"""
        if self._check(token_type):
            return self._advance()

        current = self._current_token()

        # 特殊处理EOF情况
        if current.type == TokenType.EOF:
            raise SyntaxErr(f"意外的语句结束，期望 '{token_type.value}'",
                            current.line, current.column, token_type.value)
        else:
            raise SyntaxErr(f"期望 '{token_type.value}'，但遇到 '{current.lexeme}'",
                            current.line, current.column, token_type.value)

    def get_position_info(self) -> str:
        """获取当前位置信息（用于调试）"""
        current = self._current_token()
        return f"位置: [行 {current.line}, 列 {current.column}], 当前token: '{current.lexeme}' ({current.type.value})"