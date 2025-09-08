from sql_compiler.lexer.token import TokenType

# 关键字映射表
KEYWORDS = {
    'SELECT': TokenType.SELECT,
    'FROM': TokenType.FROM,
    'WHERE': TokenType.WHERE,
    'CREATE': TokenType.CREATE,
    'TABLE': TokenType.TABLE,
    'INSERT': TokenType.INSERT,
    'INTO': TokenType.INTO,
    'VALUES': TokenType.VALUES,
    'DELETE': TokenType.DELETE,
    'INT': TokenType.INT,
    'VARCHAR': TokenType.VARCHAR,
    'CHAR': TokenType.CHAR,
    'AND': TokenType.AND,
    'OR': TokenType.OR,
    'NOT': TokenType.NOT,
}

# 符号映射表
SYMBOLS = {
    ';': TokenType.SEMICOLON,
    ',': TokenType.COMMA,
    '(': TokenType.LEFT_PAREN,
    ')': TokenType.RIGHT_PAREN,
    '=': TokenType.EQUALS,
    '<>': TokenType.NOT_EQUALS,
    '<': TokenType.LESS_THAN,
    '>': TokenType.GREATER_THAN,
    '<=': TokenType.LESS_EQUAL,
    '>=': TokenType.GREATER_EQUAL,
    '*': TokenType.ASTERISK,
}

def get_keyword_type(word: str) -> TokenType:
    """获取关键字类型，如果不是关键字则返回IDENTIFIER"""
    return KEYWORDS.get(word.upper(), TokenType.IDENTIFIER)