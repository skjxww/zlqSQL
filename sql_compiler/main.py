import sys
from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer
from sql_compiler.parser.syntax_analyzer import SyntaxAnalyzer
from sql_compiler.semantic.semantic_analyzer import SemanticAnalyzer
from sql_compiler.codegen.plan_generator import PlanGenerator
from sql_compiler.catalog.catalog_manager import CatalogManager
from sql_compiler.exceptions.compiler_errors import CompilerError
from sql_compiler.utils.helpers import print_tokens, format_json
from sql_compiler.diagnostics.error_analyzer import SmartErrorReporter


class SQLCompiler:
    def __init__(self, test_mode=False, enable_diagnostics=True):
        self.catalog = CatalogManager()
        self.test_mode = test_mode
        self.enable_diagnostics = enable_diagnostics

        # 初始化智能错误报告器
        if self.enable_diagnostics:
            self.error_reporter = SmartErrorReporter(self.catalog)

        if not test_mode:
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

            # 执行计划生成（包含优化）
            print("\n=== 执行计划生成 ===")
            codegen = PlanGenerator(enable_optimization=True)
            plan = codegen.generate(ast)
            print("执行计划:")
            print(format_json(plan.to_dict()))

            print("\n=== 编译成功 ===")
            return plan

        except CompilerError as e:
            if self.enable_diagnostics and not self.test_mode:
                # 使用智能错误诊断
                self.error_reporter.report_error(e, sql_text)
            else:
                print(f"\n❌ 编译失败: {e}")
            return None
        except Exception as e:
            print(f"\n❌ 系统错误: {e}")
            return None

    def compile_silent(self, sql_text: str):
        """静默编译（不打印详细信息，用于生产环境）"""
        try:
            # 词法分析
            lexer = LexicalAnalyzer(sql_text)
            tokens = lexer.tokenize()

            # 语法分析
            parser = SyntaxAnalyzer(tokens)
            ast = parser.parse()

            # 语义分析
            semantic = SemanticAnalyzer(self.catalog)
            semantic.analyze(ast)

            # 执行计划生成（静默模式）
            codegen = PlanGenerator(enable_optimization=True, silent_mode=True)
            plan = codegen.generate(ast)

            return plan

        except CompilerError:
            return None
        except Exception:
            return None

    def compile_multiple_silent(self, sql_statements: list):
        """静默批量编译（用于生产环境）"""
        results = []
        for sql in sql_statements:
            result = self.compile_silent(sql)
            results.append(result)
        return results

    def reset_database(self):
        """重置数据库状态（用于测试）"""
        if hasattr(self.catalog, 'reset_for_testing'):
            self.catalog.reset_for_testing()
        else:
            # 如果没有 reset_for_testing 方法，使用 clear_all_tables
            if hasattr(self.catalog, 'clear_all_tables'):
                self.catalog.clear_all_tables()

    def validate_sql(self, sql: str) -> bool:
        """验证SQL是否有效"""
        result = self.compile_silent(sql)
        return result is not None

    def get_table_info(self, table_name: str):
        """获取表信息"""
        return self.catalog.get_table(table_name)

    def get_all_tables_info(self):
        """获取所有表信息"""
        return self.catalog.get_all_tables()

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

    def load_from_file(self, filename: str):
        """从文件加载SQL语句"""
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
                return self.compile_multiple(statements)
            else:
                print("文件中没有找到有效的SQL语句")
                return []

        except FileNotFoundError:
            print(f"文件未找到: {filename}")
            return None
        except Exception as e:
            print(f"读取文件错误: {e}")
            return None


def main():
    """主函数 - 简化版本"""
    compiler = SQLCompiler()

    if len(sys.argv) > 1:
        # 文件模式
        filename = sys.argv[1]
        compiler.load_from_file(filename)
    else:
        # 交互模式
        compiler.interactive_mode()


if __name__ == "__main__":
    main()