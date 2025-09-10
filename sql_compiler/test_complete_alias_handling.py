"""
完整的别名处理测试
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer
from sql_compiler.parser.syntax_analyzer import SyntaxAnalyzer
from sql_compiler.semantic.semantic_analyzer import SemanticAnalyzer
from sql_compiler.catalog.catalog_manager import CatalogManager
from sql_compiler.codegen.plan_generator import PlanGenerator
import json


def test_complete_alias_handling():
    print("🔧 完整别名处理测试")
    print("=" * 60)

    # 设置测试环境
    catalog = CatalogManager()

    # 创建测试表
    test_tables = [
        ("students", [("id", "INT", []), ("name", "VARCHAR(100)", []), ("age", "INT", [])]),
        ("enrollments", [("student_id", "INT", []), ("course_id", "INT", []), ("grade", "VARCHAR(2)", [])]),
        ("courses", [("course_id", "INT", []), ("course_name", "VARCHAR(100)", []), ("credits", "INT", [])])
    ]

    for table_name, columns in test_tables:
        try:
            catalog.create_table(table_name, columns)
            print(f"✅ 创建表: {table_name}")
        except Exception as e:
            print(f"⚠️ 表 {table_name}: {e}")

    # 测试用例
    test_cases = [
        {
            "name": "单表别名查询",
            "sql": "SELECT s.name, s.age FROM students s WHERE s.age > 20;",
            "expected_aliases": {"s": "students"}
        },
        {
            "name": "两表连接别名",
            "sql": "SELECT s.name, e.grade FROM students s JOIN enrollments e ON s.id = e.student_id;",
            "expected_aliases": {"s": "students", "e": "enrollments"}
        },
        {
            "name": "三表连接别名",
            "sql": "SELECT s.name, c.course_name, e.grade FROM students s INNER JOIN enrollments e ON s.id = e.student_id INNER JOIN courses c ON e.course_id = c.course_id;",
            "expected_aliases": {"s": "students", "e": "enrollments", "c": "courses"}
        },
        {
            "name": "混合别名和无别名",
            "sql": "SELECT s.name FROM students s JOIN enrollments ON s.id = enrollments.student_id;",
            "expected_aliases": {"s": "students"}
        }
    ]

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📝 测试 {i}: {test_case['name']}")
        print(f"   SQL: {test_case['sql']}")

        try:
            # 编译查询
            result = compile_with_alias_tracking(test_case['sql'], catalog)

            if result:
                plan, semantic_aliases, plan_aliases = result

                # 验证语义分析器的别名
                print(f"   🏷️  语义别名: {semantic_aliases}")
                print(f"   🏷️  计划别名: {plan_aliases}")

                # 验证期望的别名
                expected = test_case['expected_aliases']
                if semantic_aliases == expected:
                    print("   ✅ 别名映射正确")
                else:
                    print(f"   ❌ 别名映射错误，期望: {expected}")

                # 显示执行计划
                plan_dict = plan.to_dict()
                print("   📋 执行计划:")
                print_plan_summary(plan_dict, indent=6)

                # 验证执行计划中的别名
                if verify_plan_aliases(plan_dict, expected):
                    print("   ✅ 执行计划别名正确")
                else:
                    print("   ⚠️ 执行计划别名可能有问题")
            else:
                print("   ❌ 编译失败")

        except Exception as e:
            print(f"   ❌ 测试失败: {e}")
            import traceback
            traceback.print_exc()


def compile_with_alias_tracking(sql: str, catalog: CatalogManager):
    """编译SQL并跟踪别名"""
    try:
        # 词法分析
        lexer = LexicalAnalyzer(sql)
        tokens = lexer.tokenize()

        # 语法分析
        parser = SyntaxAnalyzer(tokens)
        ast = parser.parse()

        # 语义分析
        semantic = SemanticAnalyzer(catalog)
        semantic.analyze(ast)

        # 获取语义分析器的别名信息
        semantic_aliases = semantic.get_current_aliases()

        # 生成执行计划
        plan_generator = PlanGenerator(
            enable_optimization=False,
            silent_mode=True,
            catalog_manager=catalog
        )

        plan = plan_generator.generate(ast)

        # 获取计划生成器的别名信息
        plan_aliases = plan_generator.get_alias_info()['alias_to_real']

        return plan, semantic_aliases, plan_aliases

    except Exception as e:
        print(f"编译失败: {e}")
        return None


def print_plan_summary(plan_dict: dict, indent: int = 0):
    """打印执行计划摘要"""
    spaces = " " * indent

    plan_type = plan_dict.get('type', 'Unknown')

    if plan_type == 'SeqScanOp':
        table_name = plan_dict.get('table_name', 'unknown')
        alias = plan_dict.get('table_alias')
        display = plan_dict.get('display_name', table_name)

        if alias:
            print(f"{spaces}├─ {plan_type}: {table_name} AS {alias}")
        else:
            print(f"{spaces}├─ {plan_type}: {table_name}")

    elif 'Join' in plan_type:
        print(f"{spaces}├─ {plan_type}")
        condition_display = plan_dict.get('on_condition_formatted')
        if condition_display:
            print(f"{spaces}│  └─ ON: {condition_display}")
    else:
        print(f"{spaces}├─ {plan_type}")

    # 递归打印子节点
    children = plan_dict.get('children', [])
    for i, child in enumerate(children):
        print_plan_summary(child, indent + 2)


def verify_plan_aliases(plan_dict: dict, expected_aliases: dict) -> bool:
    """验证执行计划中的别名是否正确"""
    try:
        scan_ops = extract_scan_operations(plan_dict)

        for scan_op in scan_ops:
            table_name = scan_op.get('table_name')
            table_alias = scan_op.get('table_alias')

            if table_alias:
                if table_alias not in expected_aliases:
                    print(f"     ❌ 意外的别名: {table_alias}")
                    return False
                if expected_aliases[table_alias] != table_name:
                    print(f"     ❌ 别名映射错误: {table_alias} -> {table_name}, 期望: {expected_aliases[table_alias]}")
                    return False

        return True

    except Exception as e:
        print(f"     ⚠️ 别名验证失败: {e}")
        return False


def extract_scan_operations(plan_dict: dict) -> list:
    """提取所有扫描操作"""
    scan_ops = []

    if isinstance(plan_dict, dict):
        if plan_dict.get('type') == 'SeqScanOp':
            scan_ops.append(plan_dict)

        children = plan_dict.get('children', [])
        for child in children:
            scan_ops.extend(extract_scan_operations(child))

    return scan_ops


if __name__ == "__main__":
    test_complete_alias_handling()