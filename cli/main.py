# cli/main.py
from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer  # 添加lexer导入
from sql_compiler.parser.syntax_analyzer import SyntaxAnalyzer
from sql_compiler.codegen.plan_generator import PlanGenerator
from storage.core.page_manager import PageManager
from storage.core.buffer_pool import BufferPool
from storage.core.storage_manager import StorageManager
from catalog.catalog_manager import CatalogManager
from engine.storage_engine import StorageEngine
from engine.execution_engine import ExecutionEngine


# main.py
class SimpleDB:
    def __init__(self, data_dir='data/'):
        # 初始化存储组件
        self.page_manager = PageManager()
        self.buffer_pool = BufferPool()
        self.storage_manager = StorageManager()

        # 初始化 TableStorage
        from storage.core.table_storage import TableStorage
        self.table_storage = TableStorage(self.storage_manager)

        # 初始化数据库引擎组件
        self.catalog_manager = CatalogManager()
        self.storage_engine = StorageEngine(storage_manager=self.storage_manager,
            table_storage=self.table_storage, catalog_manager=self.catalog_manager)  # 传入实际的 TableStorage 实例
        self.execution_engine = ExecutionEngine(storage_engine=self.storage_engine,
                                                catalog_manager=self.catalog_manager)

        # 初始化SQL编译器组件
        self.lexer = LexicalAnalyzer

    def execute(self, sql: str):
        """执行SQL语句"""
        try:
            # 1. 词法分析
            lexer = self.lexer(sql)
            tokens = lexer.tokenize()

            # 2. 语法分析
            parser = SyntaxAnalyzer(tokens)
            ast = parser.parse()

            # 3. 生成执行计划
            planner = PlanGenerator(catalog_manager=self.catalog_manager)
            plan = planner.generate(ast)  # 返回操作符对象

            # 4. 执行计划
            return self.execution_engine.execute_plan(plan)

        except Exception as e:
            return f"Error executing SQL: {str(e)}"

    def shutdown(self):
        """关闭数据库，确保所有数据持久化"""
        self.storage_manager.shutdown()


def main():
    db = SimpleDB()
    print("SimpleDB started. Type 'exit;' to quit.")

    while True:
        try:
            sql = input("SQL> ").strip()
            if sql.lower() == 'exit;':
                db.shutdown()
                print("Goodbye!")
                break

            if sql.endswith(';'):
                result = db.execute(sql)
                if isinstance(result, list):
                    for row in result:
                        print(row)
                else:
                    print(result)
            else:
                print("SQL statements must end with a semicolon (;)")

        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()