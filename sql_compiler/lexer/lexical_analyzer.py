import re
from typing import List, Optional
from sql_compiler.lexer.token import Token, TokenType
from sql_compiler.lexer.keywords import get_keyword_type, SYMBOLS
from sql_compiler.exceptions.compiler_errors import LexicalError


class LexicalAnalyzer:
    def __init__(self, source_code: str):
        self.source = source_code
        self.tokens = []
        self.current_pos = 0
        self.line = 1
        self.column = 1

    def tokenize(self) -> List[Token]:
        """词法分析主函数"""
        while self.current_pos < len(self.source):
            self._skip_whitespace()

            if self.current_pos >= len(self.source):
                break

            # 处理注释
            if self._match_comment():
                continue

            # 处理字符串字面量
            if self._current_char() in ['"', "'"]:
                self._tokenize_string()
                continue

            # 处理数字
            if self._current_char().isdigit():
                self._tokenize_number()
                continue

            # 处理标识符和关键字
            if self._current_char().isalpha() or self._current_char() == '_':
                self._tokenize_identifier()
                continue

            # 处理运算符和分隔符
            if self._tokenize_operator():
                continue

            # 未识别字符
            raise LexicalError(f"未识别的字符: '{self._current_char()}'",
                               self.line, self.column)

        # 添加EOF标记
        self.tokens.append(Token(TokenType.EOF, '', self.line, self.column))
        return self.tokens

    def _current_char(self) -> str:
        """获取当前字符"""
        if self.current_pos >= len(self.source):
            return '\0'
        return self.source[self.current_pos]

    def _peek_char(self, offset: int = 1) -> str:
        """预读字符"""
        pos = self.current_pos + offset
        if pos >= len(self.source):
            return '\0'
        return self.source[pos]

    def _advance(self) -> str:
        """前进一个字符"""
        if self.current_pos < len(self.source):
            char = self.source[self.current_pos]
            self.current_pos += 1
            if char == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            return char
        return '\0'

    def _skip_whitespace(self):
        """跳过空白字符"""
        while self._current_char().isspace():
            self._advance()

    def _match_comment(self) -> bool:
        """处理注释"""
        if self._current_char() == '-' and self._peek_char() == '-':
            # 单行注释
            while self._current_char() != '\n' and self._current_char() != '\0':
                self._advance()
            return True
        return False

    def _tokenize_string(self):
        """处理字符串字面量"""
        quote_char = self._current_char()
        start_column = self.column
        self._advance()  # 跳过开始引号

        value = ''
        while self._current_char() != quote_char and self._current_char() != '\0':
            if self._current_char() == '\\':
                self._advance()
                if self._current_char() != '\0':
                    value += self._current_char()
                    self._advance()
            else:
                value += self._current_char()
                self._advance()

        if self._current_char() != quote_char:
            raise LexicalError("字符串未正确闭合", self.line, start_column)

        self._advance()  # 跳过结束引号

        lexeme = f"{quote_char}{value}{quote_char}"
        token = Token(TokenType.STRING_LITERAL, lexeme, self.line, start_column, value)
        self.tokens.append(token)

    def _tokenize_number(self):
        """处理数字字面量"""
        start_column = self.column
        value = ''

        while self._current_char().isdigit():
            value += self._current_char()
            self._advance()

        token = Token(TokenType.INTEGER_LITERAL, value, self.line, start_column, int(value))
        self.tokens.append(token)

    def _tokenize_identifier(self):
        """处理标识符和关键字"""
        start_column = self.column
        value = ''

        while (self._current_char().isalnum() or self._current_char() == '_'):
            value += self._current_char()
            self._advance()

        token_type = get_keyword_type(value)
        token = Token(token_type, value.upper() if token_type != TokenType.IDENTIFIER else value,
                      self.line, start_column)
        self.tokens.append(token)

    def _tokenize_operator(self) -> bool:
        """处理运算符和分隔符"""
        start_column = self.column

        # 处理双字符运算符
        two_char = self._current_char() + self._peek_char()
        if two_char in SYMBOLS:
            self._advance()
            self._advance()
            token = Token(SYMBOLS[two_char], two_char, self.line, start_column)
            self.tokens.append(token)
            return True

        # 处理单字符运算符
        one_char = self._current_char()
        if one_char in SYMBOLS:
            self._advance()
            token = Token(SYMBOLS[one_char], one_char, self.line, start_column)
            self.tokens.append(token)
            return True

        return False