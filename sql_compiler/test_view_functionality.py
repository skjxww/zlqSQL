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

    # 测试CREATE VIEW
    sql = "CREATE VIEW employee_summary AS SELECT name, department, salary FROM employees;"

    try:
        plans = compiler.compile(sql)
        print(f"✅ CREATE VIEW 编译成功")

        # 执行计划
        if plans and hasattr(plans, 'execute'):
            results = list(plans.execute())
            print(f"   执行结果: {results}")

        # 关键：检查视图是否真的被创建
        print(f"📋 视图是否存在: {catalog.view_exists('employee_summary')}")
        print(f"📋 所有视图: {catalog.get_all_views()}")

        if catalog.view_exists('employee_summary'):
            print(f"📋 视图列信息: {catalog.get_view_columns('employee_summary')}")

    except Exception as e:
        print(f"❌ 测试失败: {e}")

    # 现在测试查询视图
    if catalog.view_exists('employee_summary'):
        try:
            query_sql = "SELECT * FROM employee_summary;"
            query_plans = compiler.compile(query_sql)
            print(f"✅ 查询视图编译成功")
        except Exception as e:
            print(f"❌ 查询视图失败: {e}")
    else:
        print("❌ 视图未创建，无法测试查询")


if __name__ == "__main__":
    test_view_functionality()