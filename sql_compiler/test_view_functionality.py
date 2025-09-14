def test_view_functionality():
    """测试视图功能"""

    from catalog.catalog_manager import CatalogManager
    from sql_compiler.main import SQLCompiler

    print("=== 视图功能测试 ===")

    # 初始化
    catalog = CatalogManager()
    compiler = SQLCompiler(catalog)

    # 创建基础表
    catalog.create_table("employees", [
        ("id", "INT", "PRIMARY KEY"),
        ("name", "VARCHAR(50)", None),
        ("department", "VARCHAR(30)", None),
        ("salary", "DECIMAL(10,2)", None)
    ])

    catalog.create_table("departments", [
        ("id", "INT", "PRIMARY KEY"),
        ("name", "VARCHAR(30)", None),
        ("manager_id", "INT", None)
    ])

    # 测试视图SQL
    view_test_cases = [
        "CREATE VIEW employee_summary AS SELECT name, department, salary FROM employees;",
        "CREATE VIEW emp_info (emp_name, dept, pay) AS SELECT name, department, salary FROM employees;",
        "CREATE OR REPLACE VIEW employee_summary AS SELECT name, department, salary FROM employees WHERE salary > 50000;",
        "CREATE MATERIALIZED VIEW dept_stats AS SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary FROM employees GROUP BY department;",
        "SELECT * FROM employee_summary;",
        "SELECT emp_name, pay FROM emp_info WHERE pay > 60000;",
        "SHOW VIEWS;",
        "SHOW VIEWS LIKE 'emp%';",
        "DESCRIBE VIEW employee_summary;",
        "DROP VIEW IF EXISTS temp_view;",
        "DROP VIEW employee_summary;",
    ]

    print("\n视图操作测试:")
    for i, sql in enumerate(view_test_cases, 1):
        try:
            plans = compiler.compile(sql)
            if plans is None:
                print(f"❌ 测试 {i}: 编译返回None")
                continue

            print(f"✅ 测试 {i}: {sql.split()[0]} {sql.split()[1] if len(sql.split()) > 1 else ''} 编译成功")

            # 执行计划 - 修复迭代问题
            if hasattr(plans, '__iter__') and not isinstance(plans, str):
                # 如果plans是可迭代的
                for plan in plans:
                    if hasattr(plan, 'execute'):
                        results = list(plan.execute())
                        if results:
                            print(f"   执行结果: {results[0].get('message', results[0])}")
            else:
                # 如果plans是单个对象
                if hasattr(plans, 'execute'):
                    results = list(plans.execute())
                    if results:
                        print(f"   执行结果: {results[0].get('message', results[0])}")

        except Exception as e:
            print(f"❌ 测试 {i}: 失败 - {e}")

    print(f"\n=== 视图功能测试完成 ===")


if __name__ == "__main__":
    test_view_functionality()