import json
from typing import Any, Dict, List


def format_json(obj: Any, indent: int = 2) -> str:
    """格式化JSON输出"""
    return json.dumps(obj, indent=indent, ensure_ascii=False)


def print_tokens(tokens: List) -> None:
    """打印Token列表"""
    print("词法分析结果:")
    for i, token in enumerate(tokens):
        if token.type.value != 'EOF':
            print(f"{i:2d}: {token}")


def print_ast(ast: Any, indent: int = 0) -> None:
    """打印AST树"""
    if hasattr(ast, 'to_dict'):
        print_dict(ast.to_dict(), indent)
    else:
        print("  " * indent + str(ast))


def print_dict(d: Dict[str, Any], indent: int = 0) -> None:
    """递归打印字典"""
    for key, value in d.items():
        if isinstance(value, dict):
            print("  " * indent + f"{key}:")
            print_dict(value, indent + 1)
        elif isinstance(value, list):
            print("  " * indent + f"{key}: [")
            for item in value:
                if isinstance(item, dict):
                    print_dict(item, indent + 1)
                else:
                    print("  " * (indent + 1) + str(item))
            print("  " * indent + "]")
        else:
            print("  " * indent + f"{key}: {value}")


def validate_sql_syntax(sql: str) -> bool:
    """简单的SQL语法预验证"""
    sql = sql.strip()
    if not sql:
        return False
    if not sql.endswith(';'):
        return False

    keywords = ['CREATE', 'INSERT', 'SELECT', 'DELETE']
    first_word = sql.split()[0].upper()
    return first_word in keywords


def print_usage_help():
    """打印使用帮助"""
    help_text = """
SQL编译器使用说明:
==================

支持的SQL语句:
- CREATE TABLE table_name (column_name data_type, ...);
- INSERT INTO table_name [(columns)] VALUES (values);
- SELECT columns FROM table_name [WHERE condition];
- DELETE FROM table_name [WHERE condition];

注意事项:
- 所有SQL语句必须以分号(;)结尾
- 关键字不区分大小写
- 标识符区分大小写
- 字符串使用单引号包围

支持的数据类型:
- INT: 整数类型
- VARCHAR(n): 变长字符串，n为最大长度
- CHAR(n): 定长字符串，n为长度

示例:
CREATE TABLE users (id INT, name VARCHAR(50), age INT);
INSERT INTO users VALUES (1, 'Alice', 25);
SELECT * FROM users WHERE age > 20;
DELETE FROM users WHERE id = 1;
"""
    print(help_text)


def escape_sql_string(value: str) -> str:
    """转义SQL字符串"""
    return value.replace("'", "''").replace('"', '""')


def parse_column_type(type_str: str) -> tuple:
    """解析列类型定义"""
    if '(' in type_str and ')' in type_str:
        base_type = type_str[:type_str.index('(')]
        size_str = type_str[type_str.index('(') + 1:type_str.rindex(')')]
        try:
            size = int(size_str)
            return (base_type, size)
        except ValueError:
            return (type_str, None)
    else:
        return (type_str, None)