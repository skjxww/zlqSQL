from sql_compiler.lexer.token import TokenType

# 关键字映射表
KEYWORDS = {
    # 基本SQL关键字
    'SELECT': TokenType.SELECT,
    'FROM': TokenType.FROM,
    'WHERE': TokenType.WHERE,
    'CREATE': TokenType.CREATE,
    'TABLE': TokenType.TABLE,
    'INSERT': TokenType.INSERT,
    'INTO': TokenType.INTO,
    'VALUES': TokenType.VALUES,
    'DELETE': TokenType.DELETE,
    'UPDATE': TokenType.UPDATE,
    'SET': TokenType.SET,

    # JOIN相关
    'JOIN': TokenType.JOIN,
    'INNER': TokenType.INNER,
    'LEFT': TokenType.LEFT,
    'RIGHT': TokenType.RIGHT,
    'ON': TokenType.ON,

    # 排序和分组
    'ORDER': TokenType.ORDER,
    'BY': TokenType.BY,
    'GROUP': TokenType.GROUP,
    'HAVING': TokenType.HAVING,
    'ASC': TokenType.ASC,
    'DESC': TokenType.DESC,

    # 聚合函数
    'COUNT': TokenType.COUNT,
    'SUM': TokenType.SUM,
    'AVG': TokenType.AVG,
    'MAX': TokenType.MAX,
    'MIN': TokenType.MIN,

    # 数据类型
    'INT': TokenType.INT,
    'VARCHAR': TokenType.VARCHAR,
    'CHAR': TokenType.CHAR,

    # 逻辑运算符
    'AND': TokenType.AND,
    'OR': TokenType.OR,
    'NOT': TokenType.NOT,
    'IN': TokenType.IN,  # 添加 IN
}

# 符号映射表 - 更新
SYMBOLS = {
    ';': TokenType.SEMICOLON,
    ',': TokenType.COMMA,
    '(': TokenType.LEFT_PAREN,
    ')': TokenType.RIGHT_PAREN,
    '.': TokenType.DOT,

    # 比较运算符
    '=': TokenType.EQUALS,
    '<>': TokenType.NOT_EQUALS,
    '<=': TokenType.LESS_EQUAL,
    '>=': TokenType.GREATER_EQUAL,
    '<': TokenType.LESS_THAN,
    '>': TokenType.GREATER_THAN,

    # 算术运算符
    '+': TokenType.PLUS,
    '-': TokenType.MINUS,
    '*': TokenType.MULTIPLY,  # 注意：这里用MULTIPLY，ASTERISK保留给SELECT *
    '/': TokenType.DIVIDE,
}


def get_keyword_type(word: str) -> TokenType:
    """获取关键字类型，如果不是关键字则返回IDENTIFIER"""
    return KEYWORDS.get(word.upper(), TokenType.IDENTIFIER)