"""
使用现有SQLCompiler的简单集成测试
"""

from sql_compiler.main import SQLCompiler


def test_simple_integration():
    """测试简单集成"""
    # 创建编译器
    compiler = SQLCompiler(test_mode=True, enable_diagnostics=False)

    # 重置数据库状态
    compiler.reset_database()

    # 测试SQL
    test_cases = [
        "CREATE TABLE users (id INT, name VARCHAR(50));",
        "INSERT INTO users VALUES (1, 'Alice');",
        "SELECT * FROM users;",
        "SELECT name FROM users WHERE id = 1;",
    ]

    print("🧪 测试简单集成...")

    success_count = 0
    for i, sql in enumerate(test_cases, 1):
        print(f"\n测试 {i}: {sql}")

        # 使用静默编译
        plan = compiler.compile_silent(sql)

        if plan:
            print(f"✅ 编译成功: {type(plan).__name__}")
            success_count += 1
        else:
            print("❌ 编译失败")

    print(f"\n🎯 测试结果: {success_count}/{len(test_cases)} 成功")

    # 测试表信息获取
    print(f"\n📊 数据库状态:")
    tables = compiler.get_all_tables_info()

    # 现在应该返回字典
    if isinstance(tables, dict):
        print(f"   当前表数: {len(tables)}")
        for name, info in tables.items():
            columns_count = len(info.get('columns', []))
            print(f"   📋 {name}: {columns_count} 列")
    else:
        print(f"   获取表信息失败: {type(tables)}")


def test_catalog_operations():
    """测试目录操作"""
    compiler = SQLCompiler(test_mode=True, enable_diagnostics=False)
    compiler.reset_database()

    print("\n🗂️  测试目录操作...")

    # 测试创建表
    create_result = compiler.compile_silent("CREATE TABLE test_catalog (id INT, data VARCHAR(100));")
    print(f"创建表: {'成功' if create_result else '失败'}")

    # 测试表是否存在
    exists = compiler.catalog.table_exists('test_catalog')
    print(f"表存在检查: {'通过' if exists else '失败'}")

    # 测试获取列信息
    columns = compiler.catalog.get_table_columns('test_catalog')
    print(f"获取列信息: {columns}")

    # 测试获取列类型
    column_types = compiler.catalog.get_table_column_types('test_catalog')
    print(f"获取列类型: {column_types}")

    # 测试目录统计
    stats = compiler.catalog.get_catalog_stats()
    print(f"目录统计: {stats}")

    # 测试导出结构
    schema_sql = compiler.catalog.export_schema()
    print(f"导出结构:\n{schema_sql}")


if __name__ == "__main__":
    test_simple_integration()
    test_catalog_operations()