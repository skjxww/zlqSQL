import sys
import os
import time

from sql_compiler.main import SQLCompiler


class TestSuite:
    """测试套件管理器"""

    def __init__(self):
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = 0
        self.test_results = []

    def run_test(self, test_name: str, test_func):
        """运行单个测试"""
        print(f"\n{'=' * 80}")
        print(f"🧪 运行测试: {test_name}")
        print("=" * 80)

        start_time = time.time()

        try:
            result = test_func()
            end_time = time.time()

            if result:
                print(f"✅ {test_name} - 通过 ({end_time - start_time:.2f}s)")
                self.passed_tests += 1
            else:
                print(f"❌ {test_name} - 失败 ({end_time - start_time:.2f}s)")
                self.failed_tests += 1

            self.test_results.append({
                'name': test_name,
                'passed': result,
                'duration': end_time - start_time
            })

        except Exception as e:
            end_time = time.time()
            print(f"💥 {test_name} - 异常: {e} ({end_time - start_time:.2f}s)")
            self.failed_tests += 1
            self.test_results.append({
                'name': test_name,
                'passed': False,
                'duration': end_time - start_time,
                'error': str(e)
            })

        self.total_tests += 1

    def print_summary(self):
        """打印测试总结"""
        print(f"\n{'🎯' * 30}")
        print("测试总结")
        print("🎯" * 30)
        print(f"总测试数: {self.total_tests}")
        print(f"通过: {self.passed_tests} ✅")
        print(f"失败: {self.failed_tests} ❌")
        print(f"成功率: {self.passed_tests / self.total_tests * 100:.1f}%")

        total_time = sum(result['duration'] for result in self.test_results)
        print(f"总耗时: {total_time:.2f}s")

        if self.failed_tests > 0:
            print(f"\n❌ 失败的测试:")
            for result in self.test_results:
                if not result['passed']:
                    error_info = f" - {result.get('error', '')}" if 'error' in result else ""
                    print(f"  • {result['name']}{error_info}")


def test_basic_ddl():
    """测试基本DDL功能"""
    compiler = SQLCompiler()

    ddl_statements = [
        "CREATE TABLE users (id INT, name VARCHAR(50), email VARCHAR(100));",
        "CREATE TABLE products (product_id INT, name VARCHAR(100), price INT, category VARCHAR(50));",
        "CREATE TABLE orders (order_id INT, user_id INT, total INT, status VARCHAR(20));",
    ]

    results = compiler.compile_multiple(ddl_statements)
    success_count = sum(1 for r in results if r is not None)

    print(f"DDL测试: {success_count}/{len(ddl_statements)} 成功")
    return success_count == len(ddl_statements)


def test_basic_dml():
    """测试基本DML功能"""
    compiler = SQLCompiler()

    # 先创建表
    setup = [
        "CREATE TABLE test_dml (id INT, name VARCHAR(50), value INT);",
    ]

    dml_statements = [
        "INSERT INTO test_dml VALUES (1, 'Alice', 100);",
        "INSERT INTO test_dml VALUES (2, 'Bob', 200);",
        "INSERT INTO test_dml VALUES (3, 'Charlie', 150);",
        "SELECT * FROM test_dml;",
        "SELECT name, value FROM test_dml WHERE value > 120;",
        "UPDATE test_dml SET value = value + 10 WHERE name = 'Alice';",
        "DELETE FROM test_dml WHERE value < 110;",
        "SELECT * FROM test_dml;",
    ]

    # 执行设置
    for sql in setup:
        compiler.compile(sql)

    results = compiler.compile_multiple(dml_statements)
    success_count = sum(1 for r in results if r is not None)

    print(f"DML测试: {success_count}/{len(dml_statements)} 成功")
    return success_count == len(dml_statements)


def test_complex_queries():
    """测试复杂查询功能"""
    compiler = SQLCompiler()

    # 设置复杂的测试环境
    setup = [
        "CREATE TABLE customers (customer_id INT, name VARCHAR(50), city VARCHAR(30), age INT);",
        "CREATE TABLE orders (order_id INT, customer_id INT, amount INT, order_date VARCHAR(20));",
        "CREATE TABLE products (product_id INT, name VARCHAR(50), category VARCHAR(30), price INT);",

        "INSERT INTO customers VALUES (1, 'John', 'New York', 30);",
        "INSERT INTO customers VALUES (2, 'Jane', 'Los Angeles', 25);",
        "INSERT INTO customers VALUES (3, 'Bob', 'Chicago', 35);",

        "INSERT INTO orders VALUES (101, 1, 500, '2024-01-01');",
        "INSERT INTO orders VALUES (102, 2, 300, '2024-01-02');",
        "INSERT INTO orders VALUES (103, 1, 200, '2024-01-03');",
        "INSERT INTO orders VALUES (104, 3, 400, '2024-01-04');",

        "INSERT INTO products VALUES (1, 'Laptop', 'Electronics', 1000);",
        "INSERT INTO products VALUES (2, 'Phone', 'Electronics', 600);",
        "INSERT INTO products VALUES (3, 'Book', 'Education', 30);",
    ]

    complex_queries = [
        # GROUP BY 和聚合函数
        "",
        "SELECT city, AVG(age) FROM customers GROUP BY city;",
        "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;",

        # ORDER BY
        "SELECT * FROM customers ORDER BY age DESC;",
        "SELECT * FROM products ORDER BY price ASC, name DESC;",

        # HAVING
        "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id HAVING COUNT(*) > 1;",
        "SELECT city, AVG(age) FROM customers GROUP BY city HAVING AVG(age) > 30;",

        # JOIN
        "SELECT c.name, o.amount FROM customers c INNER JOIN orders o ON c.customer_id = o.customer_id;",
        "SELECT c.name, o.amount FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id;",

        # 复合查询
        "SELECT c.city, COUNT(o.order_id), AVG(o.amount) FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.city ORDER BY AVG(o.amount) DESC;",

        # 子查询和IN
        "SELECT * FROM customers WHERE customer_id IN (SELECT customer_id FROM orders WHERE amount > 300);",
    ]

    # 执行设置
    for sql in setup:
        result = compiler.compile(sql)
        if result is None:
            print(f"设置失败: {sql}")
            return False

    results = compiler.compile_multiple(complex_queries)
    success_count = sum(1 for r in results if r is not None)

    print(f"复杂查询测试: {success_count}/{len(complex_queries)} 成功")
    return success_count >= len(complex_queries) * 0.8  # 80%成功率即通过


def test_error_handling():
    """测试错误处理"""
    compiler = SQLCompiler()

    # 先创建测试表
    setup = [
        "CREATE TABLE error_test (id INT, name VARCHAR(50), value INT);",
        "INSERT INTO error_test VALUES (1, 'test', 100);",
    ]

    for sql in setup:
        compiler.compile(sql)

    # 应该失败的测试用例
    error_cases = [
        # 表相关错误
        "SELECT * FROM nonexistent_table;",
        "INSERT INTO fake_table VALUES (1, 'test');",
        "CREATE TABLE error_test (id INT);",  # 表已存在

        # 列相关错误
        "SELECT nonexistent_col FROM error_test;",
        "INSERT INTO error_test (fake_col) VALUES (1);",
        "UPDATE error_test SET fake_col = 1;",

        # 类型错误
        "UPDATE error_test SET id = 'string' WHERE id = 1;",
        "INSERT INTO error_test VALUES ('string', 'name', 100);",

        # GROUP BY错误
        "SELECT id, COUNT(*) FROM error_test;",
        "SELECT * FROM error_test HAVING value > 50;",

        # 聚合函数参数错误
        "SELECT COUNT(nonexistent_col) FROM error_test;",

        # 语法错误
        "SELECT * FROM;",
        "UPDATE error_test SET;",
        "INSERT INTO error_test VALUES;",
    ]

    failed_as_expected = 0
    for sql in error_cases:
        result = compiler.compile(sql)
        if result is None:
            failed_as_expected += 1
        else:
            print(f"应该失败但成功了: {sql}")

    print(f"错误处理测试: {failed_as_expected}/{len(error_cases)} 正确识别错误")
    return failed_as_expected >= len(error_cases) * 0.8  # 80%识别率即通过


def test_advanced_features():
    """测试高级功能"""
    compiler = SQLCompiler()

    # 高级功能设置
    setup = [
        "CREATE TABLE students (id INT, name VARCHAR(50), major VARCHAR(30), gpa INT);",
        "CREATE TABLE courses (course_id INT, course_name VARCHAR(50), credits INT);",
        "CREATE TABLE enrollments (student_id INT, course_id INT, grade VARCHAR(2));",

        "INSERT INTO students VALUES (1, 'Alice', 'CS', 385);",
        "INSERT INTO students VALUES (2, 'Bob', 'Math', 350);",
        "INSERT INTO students VALUES (3, 'Carol', 'CS', 395);",

        "INSERT INTO courses VALUES (101, 'Database Systems', 3);",
        "INSERT INTO courses VALUES (102, 'Algorithms', 4);",
        "INSERT INTO courses VALUES (201, 'Linear Algebra', 3);",

        "INSERT INTO enrollments VALUES (1, 101, 'A');",
        "INSERT INTO enrollments VALUES (1, 102, 'B');",
        "INSERT INTO enrollments VALUES (2, 201, 'A');",
        "INSERT INTO enrollments VALUES (3, 101, 'A');",
    ]

    advanced_queries = [
        # 多表JOIN
        "SELECT s.name, c.course_name, e.grade FROM students s INNER JOIN enrollments e ON s.id = e.student_id INNER JOIN courses c ON e.course_id = c.course_id;",

        # 复杂的WHERE条件
        "SELECT * FROM students WHERE gpa > 360 AND major = 'CS' OR gpa > 380;",

        # 嵌套聚合
        "SELECT major, COUNT(*), AVG(gpa) FROM students GROUP BY major HAVING COUNT(*) > 1 ORDER BY AVG(gpa) DESC;",

        # 算术表达式
        "UPDATE students SET gpa = gpa + 5 WHERE major = 'Math';",

        # 复杂子查询
        "SELECT * FROM students WHERE id IN (SELECT student_id FROM enrollments WHERE grade = 'A');",
    ]

    # 执行设置
    for sql in setup:
        result = compiler.compile(sql)
        if result is None:
            print(f"高级功能设置失败: {sql}")
            return False

    results = compiler.compile_multiple(advanced_queries)
    success_count = sum(1 for r in results if r is not None)

    print(f"高级功能测试: {success_count}/{len(advanced_queries)} 成功")
    return success_count >= len(advanced_queries) * 0.7  # 70%成功率即通过


def test_business_scenarios():
    """测试真实业务场景"""
    compiler = SQLCompiler()

    # 电商系统场景
    ecommerce_scenario = [
        # 创建电商系统表
        "CREATE TABLE users (user_id INT, username VARCHAR(50), email VARCHAR(100), created_at VARCHAR(20));",
        "CREATE TABLE categories (category_id INT, name VARCHAR(50), description VARCHAR(200));",
        "CREATE TABLE products (product_id INT, name VARCHAR(100), category_id INT, price INT, stock INT);",
        "CREATE TABLE orders (order_id INT, user_id INT, total_amount INT, status VARCHAR(20), order_date VARCHAR(20));",
        "CREATE TABLE order_items (item_id INT, order_id INT, product_id INT, quantity INT, price INT);",

        # 插入测试数据
        "INSERT INTO users VALUES (1, 'alice', 'alice@email.com', '2024-01-01');",
        "INSERT INTO users VALUES (2, 'bob', 'bob@email.com', '2024-01-02');",

        "INSERT INTO categories VALUES (1, 'Electronics', 'Electronic devices');",
        "INSERT INTO categories VALUES (2, 'Books', 'Physical and digital books');",

        "INSERT INTO products VALUES (101, 'Laptop', 1, 1200, 10);",
        "INSERT INTO products VALUES (102, 'Phone', 1, 800, 20);",
        "INSERT INTO products VALUES (103, 'Python Guide', 2, 50, 100);",

        "INSERT INTO orders VALUES (1001, 1, 1250, 'completed', '2024-01-15');",
        "INSERT INTO orders VALUES (1002, 2, 850, 'pending', '2024-01-16');",

        "INSERT INTO order_items VALUES (1, 1001, 101, 1, 1200);",
        "INSERT INTO order_items VALUES (2, 1001, 103, 1, 50);",
        "INSERT INTO order_items VALUES (3, 1002, 102, 1, 800);",
        "INSERT INTO order_items VALUES (4, 1002, 103, 1, 50);",

        # 业务查询
        "SELECT u.username, COUNT(o.order_id), SUM(o.total_amount) FROM users u LEFT JOIN orders o ON u.user_id = o.user_id GROUP BY u.username;",
        "SELECT p.name, SUM(oi.quantity), SUM(oi.quantity * oi.price) FROM products p INNER JOIN order_items oi ON p.product_id = oi.product_id GROUP BY p.name;",
        "SELECT c.name, AVG(p.price) FROM categories c INNER JOIN products p ON c.category_id = p.category_id GROUP BY c.name;",
        "UPDATE products SET stock = stock - 1 WHERE product_id IN (SELECT product_id FROM order_items WHERE order_id = 1001);",
    ]

    results = compiler.compile_multiple(ecommerce_scenario)
    success_count = sum(1 for r in results if r is not None)

    print(f"业务场景测试: {success_count}/{len(ecommerce_scenario)} 成功")
    return success_count >= len(ecommerce_scenario) * 0.8


def run_performance_test():
    """性能测试"""
    compiler = SQLCompiler()

    print("🚀 运行性能测试...")

    # 创建大量数据的测试
    setup = ["CREATE TABLE perf_test (id INT, value INT, name VARCHAR(50));"]

    # 批量插入测试
    insert_statements = []
    for i in range(50):  # 创建50条INSERT语句
        insert_statements.append(f"INSERT INTO perf_test VALUES ({i}, {i * 10}, 'user{i}');")

    # 复杂查询测试
    complex_queries = [
        "SELECT COUNT(*) FROM perf_test;",
        "SELECT AVG(value) FROM perf_test;",
        "SELECT * FROM perf_test WHERE value > 200 ORDER BY value DESC;",
        "SELECT name, value FROM perf_test WHERE id > 10 AND value < 300;",
    ]

    start_time = time.time()

    # 执行设置
    for sql in setup:
        compiler.compile(sql)

    # 执行批量插入
    insert_results = compiler.compile_multiple(insert_statements)

    # 执行复杂查询
    query_results = compiler.compile_multiple(complex_queries)

    end_time = time.time()

    insert_success = sum(1 for r in insert_results if r is not None)
    query_success = sum(1 for r in query_results if r is not None)

    print(f"性能测试结果:")
    print(f"  插入测试: {insert_success}/{len(insert_statements)} 成功")
    print(f"  查询测试: {query_success}/{len(complex_queries)} 成功")
    print(f"  总耗时: {end_time - start_time:.2f}s")
    print(f"  平均每条语句: {(end_time - start_time) / (len(insert_statements) + len(complex_queries)):.3f}s")

    return insert_success >= len(insert_statements) * 0.9 and query_success == len(complex_queries)


def main():
    """主测试函数"""
    print("🎯" * 50)
    print("SQL编译器完整测试套件")
    print("🎯" * 50)

    suite = TestSuite()

    # 定义所有测试
    tests = [
        ("基本DDL功能", test_basic_ddl),
        ("基本DML功能", test_basic_dml),
        ("复杂查询功能", test_complex_queries),
        ("错误处理机制", test_error_handling),
        ("高级SQL功能", test_advanced_features),
        ("业务场景测试", test_business_scenarios),
        ("性能测试", run_performance_test),
    ]

    # 运行所有测试
    for test_name, test_func in tests:
        suite.run_test(test_name, test_func)

    # 打印总结
    suite.print_summary()

    # 返回测试是否整体成功
    return suite.passed_tests >= suite.total_tests * 0.7  # 70%通过率认为成功


if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_name = sys.argv[1].lower()

        # 支持运行单个测试
        test_map = {
            'ddl': test_basic_ddl,
            'dml': test_basic_dml,
            'complex': test_complex_queries,
            'error': test_error_handling,
            'advanced': test_advanced_features,
            'business': test_business_scenarios,
            'performance': run_performance_test,
        }

        if test_name in test_map:
            print(f"运行单个测试: {test_name}")
            result = test_map[test_name]()
            print(f"测试结果: {'通过' if result else '失败'}")
        else:
            print(f"未知测试: {test_name}")
            print(f"可用测试: {', '.join(test_map.keys())}")
    else:
        # 运行所有测试
        success = main()
        sys.exit(0 if success else 1)