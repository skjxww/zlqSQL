import json
from storage.core.page_manager import PageManager
from storage.core.buffer_pool import BufferPool
from storage.core.storage_manager import StorageManager
from storage.core.table_storage import TableStorage
from catalog.catalog_manager import CatalogManager
from engine.storage_engine import StorageEngine
from engine.execution_engine import ExecutionEngine
from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer
from sql_compiler.parser.syntax_analyzer import SyntaxAnalyzer
from sql_compiler.codegen.plan_generator import PlanGenerator
from sql_compiler.diagnostics.error_analyzer import SmartSQLCorrector


class DatabaseManager:
    def __init__(self):
        self._init_database()

    def _init_database(self):
        """初始化数据库组件"""
        try:
            # 初始化存储组件
            self.page_manager = PageManager()
            self.buffer_pool = BufferPool()
            self.storage_manager = StorageManager()
            self.table_storage = TableStorage(self.storage_manager)

            # 初始化数据库引擎组件
            self.catalog_manager = CatalogManager()
            self.storage_engine = StorageEngine(
                storage_manager=self.storage_manager,
                table_storage=self.table_storage,
                catalog_manager=self.catalog_manager
            )
            self.execution_engine = ExecutionEngine(
                storage_engine=self.storage_engine,
                catalog_manager=self.catalog_manager
            )

            # 设置事务管理器
            if hasattr(self.storage_engine, 'transaction_manager'):
                self.execution_engine.set_transaction_manager(self.storage_engine.transaction_manager)
            else:
                from storage.core.transaction_manager import TransactionManager
                transaction_manager = TransactionManager(self.storage_manager)
                self.execution_engine.set_transaction_manager(transaction_manager)

            # 初始化SQL编译器组件
            self.lexer = LexicalAnalyzer
            self.sql_corrector = SmartSQLCorrector(self.catalog_manager)

        except Exception as e:
            raise Exception(f"数据库初始化失败: {str(e)}")

    def execute_query(self, sql):
        """执行SQL查询"""
        try:
            # 词法分析
            lexer = self.lexer(sql)
            tokens = lexer.tokenize()

            # 语法分析
            parser = SyntaxAnalyzer(tokens)
            ast = parser.parse()

            # 生成执行计划
            planner = PlanGenerator(
                enable_optimization=True,
                silent_mode=True,
                catalog_manager=self.catalog_manager
            )
            plan = planner.generate(ast)

            # 执行计划
            result = self.execution_engine.execute_plan(plan)
            return result

        except Exception as e:
            raise e

    def get_tables(self):
        """获取所有表信息"""
        try:
            return self.catalog_manager.get_all_table_names()
        except:
            return {}

    def shutdown(self):
        """关闭数据库连接"""
        try:
            self.storage_manager.shutdown()
        except:
            pass

    def refresh_info(self):
        """刷新数据库信息"""
        return self.get_tables()

    def get_execution_plan(self, sql):
        try:
            # 词法分析
            lexer = self.lexer(sql)
            tokens = lexer.tokenize()

            # 语法分析
            parser = SyntaxAnalyzer(tokens)
            ast = parser.parse()

            # 生成执行计划
            planner = PlanGenerator(
                enable_optimization=True,
                silent_mode=True,
                catalog_manager=self.catalog_manager
            )
            plan = planner.generate(ast)

            # 返回执行计划的字典表示
            return plan.to_dict()

        except Exception as e:
            # 返回错误信息
            return {
                'error': str(e),
                'error_type': type(e).__name__
            }
