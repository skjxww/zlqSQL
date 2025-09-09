"""
智能错误诊断测试
"""

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from sql_compiler.main import SQLCompiler


def test_error_diagnostics():
    """测试智能错误诊断功能"""
    compiler = SQLCompiler(test_mode=False, enable_diagnostics=True)

    setup_queries = [
        "CREATE TABLE users (id INT, name VARCHAR(50), email VARCHAR(100));",
        "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');",
    ]

    print("🔧 设置测试环境...")
    for sql in setup_queries:
        compiler.compile(sql)

    # 错误诊断测试用例
    diagnostic_test_cases = [
        # 词法错误
        ("SELECT * FROM users；", "中文分号错误"),
        ("SELECT * FROM users WHERE name = \"Alice\";", "双引号错误"),
        ("SELECT * FROM users，", "中文逗号错误"),

        # 语法错误
        ("SELECT * FROM", "缺少表名"),
        ("SELECT * FROM users WHERE", "WHERE子句不完整"),
        ("UPDATE users SET", "SET子句不完整"),
        ("INSERT INTO users VALUES", "VALUES子句不完整"),
        ("SELCT * FROM users;", "关键字拼写错误"),

        # 语义错误 - 表不存在
        ("SELECT * FROM user;", "表名拼写错误"),
        ("SELECT * FROM customers;", "表不存在"),

        # 语义错误 - 列不存在
        ("SELECT nam FROM users;", "列名拼写错误"),
        ("SELECT id, fullname FROM users;", "列不存在"),

        # 类型错误
        ("UPDATE users SET id = 'string';", "类型不匹配"),
        ("INSERT INTO users VALUES ('one', 'Alice', 'alice@example.com');", "类型不匹配"),

        # GROUP BY错误
        ("SELECT name, COUNT(*) FROM users;", "缺少GROUP BY"),
        ("SELECT name, id, COUNT(*) FROM users GROUP BY name;", "列不在GROUP BY中"),

        # 聚合函数错误
        ("SELECT COUNT(fullname) FROM users;", "聚合函数参数列不存在"),
    ]

    print(f"\n🔍 测试智能错误诊断 ({len(diagnostic_test_cases)} 个测试用例)...")

    for i, (sql, description) in enumerate(diagnostic_test_cases, 1):
        print(f"\n{'🧪' * 20} 诊断测试 {i}/{len(diagnostic_test_cases)} {'🧪' * 20}")
        print(f"测试用例: {description}")
        print(f"SQL: {sql}")
        print("🧪" * 60)

        # 执行SQL（预期会失败并显示诊断信息）
        result = compiler.compile(sql)

        if result is None:
            print("✅ 成功检测到错误并提供了诊断信息")
        else:
            print("❌ 意外成功（应该失败）")

        print("🧪" * 60)

    print(f"\n🎯 错误诊断测试完成")
    return True


if __name__ == "__main__":
    test_error_diagnostics()