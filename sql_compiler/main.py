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
            codegen = PlanGenerator(enable_optimization=True)
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