from enum import Enum
from typing import Any, Optional


class TokenType(Enum):
    # 关键字
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    CREATE = "CREATE"
    TABLE = "TABLE"
    INSERT = "INSERT"
    INTO = "INTO"
    VALUES = "VALUES"
    DELETE = "DELETE"

    # 数据类型
    INT = "INT"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"

    # 标识符和字面量
    IDENTIFIER = "IDENTIFIER"
    INTEGER_LITERAL = "INTEGER_LITERAL"
    STRING_LITERAL = "STRING_LITERAL"

    # 运算符
    EQUALS = "="
    NOT_EQUALS = "<>"
    LESS_THAN = "<"
    GREATER_THAN = ">"
    LESS_EQUAL = "<="
    GREATER_EQUAL = ">="

    # 分隔符
    SEMICOLON = ";"
    COMMA = ","
    LEFT_PAREN = "("
    RIGHT_PAREN = ")"

    # 特殊符号
    ASTERISK = "*"

    # 逻辑运算符
    AND = "AND"
    OR = "OR"
    NOT = "NOT"

    # 特殊
    EOF = "EOF"
    NEWLINE = "NEWLINE"


class Token:
    def __init__(self, token_type: TokenType, lexeme: str, line: int, column: int, value: Any = None):
        self.type = token_type
        self.lexeme = lexeme  # 词素值
        self.line = line  # 行号
        self.column = column  # 列号
        self.value = value  # 实际值（用于数字、字符串等）

    def __repr__(self):
        return f"[{self.type.value}, {self.lexeme}, {self.line}, {self.column}]"

    def __eq__(self, other):
        if isinstance(other, Token):
            return (self.type == other.type and
                    self.lexeme == other.lexeme and
                    self.line == other.line and
                    self.column == other.column)
        return False