"""
查询优化器测试
"""

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from sql_compiler.main import SQLCompiler


def test_query_optimization():
    """测试查询优化功能"""
    compiler = SQLCompiler(test_mode=False, enable_diagnostics=True)

    # 设置测试环境
    setup_queries = [
        "CREATE TABLE customers (id INT, name VARCHAR(50), city VARCHAR(30), age INT);",
        "CREATE TABLE orders (order_id INT, customer_id INT, amount INT, status VARCHAR(20));",
        "CREATE TABLE products (product_id INT, name VARCHAR(50), price INT);",

        "INSERT INTO customers VALUES (1, 'Alice', 'New York', 30);",
        "INSERT INTO customers VALUES (2, 'Bob', 'Chicago', 25);",
        "INSERT INTO customers VALUES (3, 'Carol', 'New York', 35);",

        "INSERT INTO orders VALUES (101, 1, 500, 'completed');",
        "INSERT INTO orders VALUES (102, 2, 300, 'pending');",
        "INSERT INTO orders VALUES (103, 1, 200, 'completed');",
    ]

    print("🔧 设置测试环境...")
    for sql in setup_queries:
        compiler.compile(sql)

    # 测试优化的查询
    optimization_test_queries = [
        # 谓词下推测试
        "SELECT * FROM customers WHERE age > 30;",

        # JOIN + 谓词下推
        "SELECT c.name, o.amount FROM customers c INNER JOIN orders o ON c.id = o.customer_id WHERE c.city = 'New York';",

        # 投影下推
        "SELECT name FROM customers WHERE age > 25;",

        # 复杂优化
        "SELECT c.city, COUNT(*), AVG(o.amount) FROM customers c LEFT JOIN orders o ON c.id = o.customer_id WHERE c.age > 25 GROUP BY c.city ORDER BY COUNT(*) DESC;",
    ]

    print("\n🚀 测试查询优化...")

    success_count = 0
    for i, sql in enumerate(optimization_test_queries, 1):
        print(f"\n{'=' * 60}")
        print(f"优化测试 {i}: {sql}")
        print("=" * 60)

        result = compiler.compile(sql)
        if result:
            success_count += 1
            print("✅ 优化成功")
        else:
            print("❌ 优化失败")

    print(f"\n🎯 优化测试总结: {success_count}/{len(optimization_test_queries)} 成功")
    return success_count == len(optimization_test_queries)


if __name__ == "__main__":
    test_query_optimization()