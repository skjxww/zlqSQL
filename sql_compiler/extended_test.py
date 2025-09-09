import sys
from sql_compiler.main import SQLCompiler


def test_extended_sql():
    """测试扩展的SQL功能"""
    compiler = SQLCompiler()

    extended_test_cases = [
        # 创建测试表
        "CREATE TABLE employees (id INT, name VARCHAR(50), salary INT, department VARCHAR(30));",
        "CREATE TABLE departments (dept_id INT, dept_name VARCHAR(30), manager VARCHAR(50));",
        "CREATE TABLE projects (proj_id INT, proj_name VARCHAR(100), dept_id INT);",

        # 插入测试数据
        "INSERT INTO employees VALUES (1, 'Alice', 5000, 'Engineering');",
        "INSERT INTO employees VALUES (2, 'Bob', 4500, 'Marketing');",
        "INSERT INTO employees VALUES (3, 'Charlie', 6000, 'Engineering');",
        "INSERT INTO employees VALUES (4, 'Diana', 5500, 'HR');",
        "INSERT INTO employees VALUES (5, 'Eve', 4000, 'Marketing');",

        "INSERT INTO departments VALUES (1, 'Engineering', 'Alice');",
        "INSERT INTO departments VALUES (2, 'Marketing', 'Bob');",
        "INSERT INTO departments VALUES (3, 'HR', 'Diana');",

        "INSERT INTO projects VALUES (101, 'Web Platform', 1);",
        "INSERT INTO projects VALUES (102, 'Mobile App', 1);",
        "INSERT INTO projects VALUES (103, 'Marketing Campaign', 2);",
        "INSERT INTO projects VALUES (104, 'HR System', 3);",

        # UPDATE语句测试
        "UPDATE employees SET salary = 5200 WHERE name = 'Alice';",
        "UPDATE employees SET department = 'Senior Engineering' WHERE salary > 5500;",
        "UPDATE employees SET salary = salary + 500 WHERE department = 'Marketing';",

        # SELECT with ORDER BY
        "SELECT * FROM employees ORDER BY salary DESC;",
        "SELECT name, salary FROM employees ORDER BY salary ASC, name DESC;",

        # SELECT with GROUP BY
        "SELECT department, COUNT(*) FROM employees GROUP BY department;",
        "SELECT department, AVG(salary) FROM employees GROUP BY department;",
        "SELECT department, MAX(salary), MIN(salary) FROM employees GROUP BY department;",

        # SELECT with HAVING
        "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 4500;",
        "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 1;",

        # JOIN查询
        "SELECT e.name, d.dept_name FROM employees e INNER JOIN departments d ON e.department = d.dept_name;",
        "SELECT e.name, e.salary, d.manager FROM employees e LEFT JOIN departments d ON e.department = d.dept_name;",
        "SELECT p.proj_name, d.dept_name FROM projects p RIGHT JOIN departments d ON p.dept_id = d.dept_id;",

        # 复合查询
        "SELECT e.name, e.salary, d.dept_name FROM employees e INNER JOIN departments d ON e.department = d.dept_name ORDER BY e.salary DESC;",
        "SELECT d.dept_name, COUNT(e.id) FROM departments d LEFT JOIN employees e ON d.dept_name = e.department GROUP BY d.dept_name;",
        "SELECT e.department, AVG(e.salary) FROM employees e WHERE e.salary > 4000 GROUP BY e.department HAVING AVG(e.salary) > 4500 ORDER BY AVG(e.salary) DESC;",

        # 聚合函数测试
        "SELECT COUNT(*) FROM employees;",
        "SELECT SUM(salary) FROM employees WHERE department = 'Engineering';",
        "SELECT department, COUNT(id), SUM(salary), AVG(salary) FROM employees GROUP BY department;",

        # 表别名测试
        "SELECT e.name, e.salary FROM employees e WHERE e.salary > 5000;",

        # 删除测试
        "DELETE FROM employees WHERE salary < 4200;",
        "DELETE FROM projects WHERE dept_id NOT IN (SELECT dept_id FROM departments);",

        # 最终查询验证
        "SELECT * FROM employees ORDER BY id;",
        "SELECT * FROM departments;",
        "SELECT * FROM projects;",
    ]

    print("=" * 80)
    print("扩展SQL功能测试")
    print("=" * 80)

    compiler.compile_multiple(extended_test_cases)


def test_complex_queries():
    """测试复杂查询"""
    compiler = SQLCompiler()

    # 先建立测试环境
    setup_queries = [
        "CREATE TABLE customers (customer_id INT, name VARCHAR(100), city VARCHAR(50), age INT);",
        "CREATE TABLE orders (order_id INT, customer_id INT, order_date VARCHAR(20), total_amount INT);",
        "CREATE TABLE order_items (item_id INT, order_id INT, product_name VARCHAR(100), quantity INT, price INT);",

        "INSERT INTO customers VALUES (1, 'John Doe', 'New York', 30);",
        "INSERT INTO customers VALUES (2, 'Jane Smith', 'Los Angeles', 25);",
        "INSERT INTO customers VALUES (3, 'Bob Johnson', 'Chicago', 35);",
        "INSERT INTO customers VALUES (4, 'Alice Brown', 'New York', 28);",
        "INSERT INTO customers VALUES (5, 'Charlie Wilson', 'Boston', 32);",

        "INSERT INTO orders VALUES (1001, 1, '2024-01-15', 150);",
        "INSERT INTO orders VALUES (1002, 2, '2024-01-16', 200);",
        "INSERT INTO orders VALUES (1003, 1, '2024-01-17', 75);",
        "INSERT INTO orders VALUES (1004, 3, '2024-01-18', 300);",
        "INSERT INTO orders VALUES (1005, 4, '2024-01-19', 120);",
        "INSERT INTO orders VALUES (1006, 2, '2024-01-20', 180);",

        "INSERT INTO order_items VALUES (1, 1001, 'Laptop', 1, 100);",
        "INSERT INTO order_items VALUES (2, 1001, 'Mouse', 2, 25);",
        "INSERT INTO order_items VALUES (3, 1002, 'Keyboard', 1, 75);",
        "INSERT INTO order_items VALUES (4, 1002, 'Monitor', 1, 125);",
        "INSERT INTO order_items VALUES (5, 1003, 'Headphones', 1, 75);",
        "INSERT INTO order_items VALUES (6, 1004, 'Tablet', 2, 150);",
        "INSERT INTO order_items VALUES (7, 1005, 'Phone Case', 3, 40);",
        "INSERT INTO order_items VALUES (8, 1006, 'Charger', 2, 90);",
    ]

    complex_queries = [
        # 复杂的统计查询
        "SELECT c.city, COUNT(o.order_id), AVG(o.total_amount) FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.city ORDER BY AVG(o.total_amount) DESC;",

        # 多表连接
        "SELECT c.name, o.order_id, oi.product_name, oi.quantity FROM customers c INNER JOIN orders o ON c.customer_id = o.customer_id INNER JOIN order_items oi ON o.order_id = oi.order_id ORDER BY c.name, o.order_id;",

        # 复杂的WHERE条件
        "SELECT * FROM customers WHERE age > 25 AND city = 'New York' OR age < 30 AND city = 'Los Angeles';",

        # GROUP BY with HAVING和ORDER BY
        "SELECT c.city, COUNT(c.customer_id), AVG(c.age) FROM customers c GROUP BY c.city HAVING COUNT(c.customer_id) > 1 ORDER BY AVG(c.age) DESC;",

        # 更新复杂条件
        "UPDATE customers SET age = age + 1 WHERE customer_id IN (SELECT customer_id FROM orders WHERE total_amount > 150);",

        # 聚合函数组合
        "SELECT product_name, SUM(quantity), COUNT(item_id), AVG(price) FROM order_items GROUP BY product_name HAVING SUM(quantity) > 1;",
    ]

    print("=" * 80)
    print("复杂查询测试")
    print("=" * 80)

    # 执行设置查询
    print("设置测试环境...")
    compiler.compile_multiple(setup_queries)

    print("\n" + "=" * 80)
    print("执行复杂查询...")
    print("=" * 80)

    # 执行复杂查询
    compiler.compile_multiple(complex_queries)


def test_error_cases_extended():
    """测试扩展语法的错误情况"""
    compiler = SQLCompiler()

    # 先创建一些表用于测试
    setup = [
        "CREATE TABLE test_table (id INT, name VARCHAR(50), value INT);",
        "INSERT INTO test_table VALUES (1, 'test', 100);",
    ]

    for sql in setup:
        compiler.compile(sql)

    error_cases = [
        # UPDATE错误
        ("UPDATE nonexistent_table SET value = 1;", "表不存在"),
        ("UPDATE test_table SET nonexistent_col = 1;", "列不存在"),
        ("UPDATE test_table SET id = 'string' WHERE id = 1;", "类型不匹配"),

        # JOIN错误
        ("SELECT * FROM test_table t1 INNER JOIN nonexistent_table t2 ON t1.id = t2.id;", "表不存在"),
        ("SELECT * FROM test_table t1 INNER JOIN test_table t2 ON t1.nonexistent = t2.id;", "列不存在"),

        # GROUP BY错误
        ("SELECT name FROM test_table GROUP BY nonexistent_col;", "列不存在"),
        ("SELECT * FROM test_table HAVING value > 50;", "HAVING without GROUP BY"),

        # ORDER BY错误
        ("SELECT * FROM test_table ORDER BY nonexistent_col;", "列不存在"),
        ("SELECT * FROM test_table ORDER BY name INVALID_DIRECTION;", "无效排序方向"),

        # 聚合函数错误
        ("SELECT COUNT(nonexistent_col) FROM test_table;", "列不存在"),
        ("SELECT id, COUNT(*) FROM test_table;", "SELECT list不一致"),

        # 语法错误
        ("UPDATE test_table SET;", "SET语句不完整"),
        ("SELECT * FROM test_table ORDER BY;", "ORDER BY不完整"),
        ("SELECT * FROM test_table GROUP BY;", "GROUP BY不完整"),
        ("SELECT * FROM test_table t1 JOIN;", "JOIN语句不完整"),
    ]

    print("=" * 80)
    print("扩展语法错误测试")
    print("=" * 80)

    success_count = 0
    total_tests = len(error_cases)

    for i, (sql, expected_error) in enumerate(error_cases, 1):
        print(f"\n错误测试 {i:2d}/{total_tests}: {sql}")
        print(f"预期错误: {expected_error}")

        result = compiler.compile(sql)
        if result is None:
            print("✅ 正确检测到错误")
            success_count += 1
        else:
            print("❌ 应该失败但成功了")

        print("-" * 60)

    print(f"\n扩展语法错误测试总结: {success_count}/{total_tests} 测试通过")


if __name__ == "__main__":
    if "--extended" in sys.argv:
        test_extended_sql()
    elif "--complex" in sys.argv:
        test_complex_queries()
    elif "--error-extended" in sys.argv:
        test_error_cases_extended()
    elif "--all-extended" in sys.argv:
        print("执行所有扩展测试...")
        test_extended_sql()
        print("\n" + "=" * 100 + "\n")
        test_complex_queries()
        print("\n" + "=" * 100 + "\n")
        test_error_cases_extended()
    else:
        print("扩展SQL测试选项:")
        print("--extended: 基本扩展功能测试")
        print("--complex: 复杂查询测试")
        print("--error-extended: 扩展语法错误测试")
        print("--all-extended: 所有扩展测试")