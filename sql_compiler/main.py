import sys
from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer
from sql_compiler.parser.syntax_analyzer import SyntaxAnalyzer
from sql_compiler.semantic.semantic_analyzer import SemanticAnalyzer
from sql_compiler.codegen.plan_generator import PlanGenerator
from sql_compiler.catalog.catalog_manager import CatalogManager
from sql_compiler.exceptions.compiler_errors import CompilerError
from sql_compiler.utils.helpers import print_tokens, format_json

class SQLCompiler:
    def __init__(self):
        self.catalog = CatalogManager()
        print(f"系统目录已加载，当前表数: {len(self.catalog.get_all_tables())}")

    def compile(self, sql_text: str):
        """编译SQL语句"""
        try:
            print("=== 开始SQL编译 ===")
            print(f"输入SQL: {sql_text.strip()}")

            # 词法分析
            print("\n=== 词法分析 ===")
            lexer = LexicalAnalyzer(sql_text)
            tokens = lexer.tokenize()
            print_tokens(tokens)

            # 语法分析
            print("\n=== 语法分析 ===")
            parser = SyntaxAnalyzer(tokens)
            ast = parser.parse()
            print("AST结构:")
            print(format_json(ast.to_dict()))

            # 语义分析
            print("\n=== 语义分析 ===")
            semantic = SemanticAnalyzer(self.catalog)
            semantic.analyze(ast)
            print("✓ 语义分析通过")

            # 执行计划生成
            print("\n=== 执行计划生成 ===")
            codegen = PlanGenerator()
            plan = codegen.generate(ast)
            print("执行计划:")
            print(format_json(plan.to_dict()))

            print("\n=== 编译成功 ===")
            return plan

        except CompilerError as e:
            print(f"\n❌ 编译失败: {e}")
            return None
        except Exception as e:
            print(f"\n❌ 系统错误: {e}")
            return None

    def compile_multiple(self, sql_statements: list):
        """编译多条SQL语句"""
        results = []
        success_count = 0

        print("=" * 70)
        print("开始批量SQL编译")
        print(f"共 {len(sql_statements)} 条语句")
        print("=" * 70)

        for i, sql in enumerate(sql_statements, 1):
            print(f"\n{'█' * 20} 语句 {i}/{len(sql_statements)} {'█' * 20}")
            result = self.compile(sql)
            results.append(result)

            if result is not None:
                success_count += 1
                print(f"✅ 语句 {i} 执行成功")
            else:
                print(f"❌ 语句 {i} 执行失败")

            print("█" * 60)

        # 输出总结
        print(f"\n{'=' * 70}")
        print("批量编译总结")
        print("=" * 70)
        print(f"总语句数: {len(sql_statements)}")
        print(f"成功数量: {success_count}")
        print(f"失败数量: {len(sql_statements) - success_count}")
        print(f"成功率: {success_count / len(sql_statements) * 100:.1f}%")
        print("=" * 70)

        return results

    def interactive_mode(self):
        """交互模式"""
        print("SQL编译器交互模式")
        print("输入SQL语句（以分号结尾），输入'quit'退出")
        print("支持多行输入：输入'multi'开始多行模式")
        print("-" * 50)

        while True:
            try:
                sql_input = input("SQL> ").strip()

                if sql_input.lower() in ['quit', 'exit', 'q']:
                    print("再见！")
                    break

                if not sql_input:
                    continue

                # 多行模式
                if sql_input.lower() == 'multi':
                    self._multi_line_mode()
                    continue

                self.compile(sql_input)
                print("-" * 50)

            except KeyboardInterrupt:
                print("\n再见！")
                break
            except EOFError:
                print("\n再见！")
                break

    def _multi_line_mode(self):
        """多行输入模式"""
        print("\n进入多行输入模式")
        print("每行输入一条SQL语句（以分号结尾）")
        print("输入'END'结束多行输入")
        print("输入'CLEAR'清空当前输入")
        print("-" * 50)

        statements = []

        while True:
            try:
                line = input("MULTI> ").strip()

                if line.upper() == 'END':
                    break
                elif line.upper() == 'CLEAR':
                    statements = []
                    print("已清空所有输入")
                    continue
                elif not line:
                    continue

                statements.append(line)
                print(f"已添加语句 {len(statements)}: {line}")

            except KeyboardInterrupt:
                print("\n多行输入被中断")
                return
            except EOFError:
                print("\n多行输入结束")
                break

        if statements:
            print(f"\n准备执行 {len(statements)} 条语句:")
            for i, stmt in enumerate(statements, 1):
                print(f"{i:2d}. {stmt}")

            confirm = input("\n是否执行这些语句？(y/n): ").strip().lower()
            if confirm in ['y', 'yes', '是']:
                self.compile_multiple(statements)
            else:
                print("已取消执行")
        else:
            print("没有输入任何语句")


def main():
    compiler = SQLCompiler()

    if len(sys.argv) > 1:
        # 文件模式
        filename = sys.argv[1]
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            # 分割多条SQL语句
            statements = []
            current_statement = ""

            for line in sql_content.split('\n'):
                line = line.strip()
                # 跳过注释和空行
                if not line or line.startswith('--'):
                    continue

                current_statement += " " + line if current_statement else line

                # 如果行以分号结尾，这是一个完整语句
                if line.endswith(';'):
                    statements.append(current_statement.strip())
                    current_statement = ""

            # 处理最后一个可能没有分号的语句
            if current_statement.strip():
                statements.append(current_statement.strip())

            print(f"从文件 '{filename}' 中解析出 {len(statements)} 条SQL语句")

            if statements:
                compiler.compile_multiple(statements)

        except FileNotFoundError:
            print(f"文件未找到: {filename}")
        except Exception as e:
            print(f"读取文件错误: {e}")
    else:
        # 交互模式
        compiler.interactive_mode()


# 测试用例
def run_tests():
    compiler = SQLCompiler()

    test_cases = [
        "CREATE TABLE student (id INT, name VARCHAR(50), age INT);",
        "INSERT INTO student (id, name, age) VALUES (1, 'Alice', 20);",
        "INSERT INTO student VALUES (2, 'Bob', 22);",
        "SELECT * FROM student;",
        "SELECT id, name FROM student WHERE age > 18;",
        "DELETE FROM student WHERE id = 1;",

        # 错误测试用例
        "CREATE TABLE student (id INT, name VARCHAR(50));",  # 表已存在
        "INSERT INTO nonexistent VALUES (1, 'test');",  # 表不存在
        "SELECT unknown_col FROM student;",  # 列不存在
        "INSERT INTO student VALUES (1);",  # 值数量不匹配
    ]

    print("执行基础测试用例...")
    compiler.compile_multiple(test_cases)


def run_multi_line_tests():
    """多行测试用例"""
    compiler = SQLCompiler()

    # 完整业务场景测试
    business_test = [
        "CREATE TABLE users (user_id INT, username VARCHAR(50), email VARCHAR(100));",
        "CREATE TABLE products (product_id INT, name VARCHAR(100), price INT);",
        "CREATE TABLE orders (order_id INT, user_id INT, product_id INT, quantity INT);",

        "INSERT INTO users VALUES (1, 'alice', 'alice@example.com');",
        "INSERT INTO users VALUES (2, 'bob', 'bob@example.com');",
        "INSERT INTO users VALUES (3, 'charlie', 'charlie@example.com');",

        "INSERT INTO products VALUES (101, 'Laptop', 1200);",
        "INSERT INTO products VALUES (102, 'Mouse', 25);",
        "INSERT INTO products VALUES (103, 'Keyboard', 75);",
        "INSERT INTO products VALUES (104, 'Monitor', 300);",

        "INSERT INTO orders VALUES (1001, 1, 101, 1);",
        "INSERT INTO orders VALUES (1002, 1, 102, 2);",
        "INSERT INTO orders VALUES (1003, 2, 103, 1);",
        "INSERT INTO orders VALUES (1004, 3, 101, 1);",
        "INSERT INTO orders VALUES (1005, 2, 104, 1);",

        "SELECT * FROM users;",
        "SELECT * FROM products;",
        "SELECT * FROM orders;",

        "SELECT username FROM users WHERE user_id = 1;",
        "SELECT name, price FROM products WHERE price > 50;",
        "SELECT * FROM orders WHERE quantity > 1;",

        "SELECT * FROM users WHERE username = 'alice' OR username = 'bob';",
        "SELECT * FROM products WHERE price >= 100 AND price <= 500;",

        "DELETE FROM orders WHERE quantity = 1 AND user_id = 3;",
        "SELECT * FROM orders;",
    ]

    print("执行完整业务场景测试...")
    compiler.compile_multiple(business_test)


def run_error_tests():
    """错误处理测试"""
    compiler = SQLCompiler()

    error_test_cases = [
        # 语法错误
        "CREATE TABLE test (id INT)",  # 缺少分号
        "SELECT * FROM",  # 缺少表名
        "INSERT INTO test VALUES",  # 缺少值
        "DELETE FROM",  # 缺少表名

        # 语义错误（需要先有一些表）
        "CREATE TABLE test_table (id INT, name VARCHAR(50));",
        "INSERT INTO nonexistent_table VALUES (1, 'test');",  # 表不存在
        "SELECT nonexistent_col FROM test_table;",  # 列不存在
        "INSERT INTO test_table VALUES (1);",  # 值数量不匹配
        "CREATE TABLE test_table (id INT);",  # 表已存在

        # 类型错误
        "INSERT INTO test_table VALUES ('not_number', 'name');",  # 类型不匹配
    ]

    print("执行错误处理测试...")
    compiler.compile_multiple(error_test_cases)


def run_extended_tests():
    """运行扩展功能测试"""
    from sql_compiler.extended_test import test_extended_sql, test_complex_queries, test_error_cases_extended

    print("开始执行扩展功能测试...")
    test_extended_sql()

    print("\n" + "=" * 100 + "\n")
    test_complex_queries()

    print("\n" + "=" * 100 + "\n")
    test_error_cases_extended()

if __name__ == "__main__":
    if "--test" in sys.argv:
        run_tests()
    elif "--multi-test" in sys.argv:
        run_multi_line_tests()
    elif "--error-test" in sys.argv:
        run_error_tests()
    elif "--extended-test" in sys.argv:
        run_extended_tests()
    elif "--all-test" in sys.argv:
        print("执行所有测试...")
        run_tests()
        print("\n" + "=" * 80 + "\n")
        run_multi_line_tests()
        print("\n" + "=" * 80 + "\n")
        run_error_tests()
    else:
        main()