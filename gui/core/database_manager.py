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

    def get_all_tables(self):
        """获取所有表信息（与GUI接口匹配）"""
        try:
            print("=== DatabaseManager.get_all_tables() 开始执行 ===")

            # 首先检查各个组件是否正常
            self._debug_components()

            # 尝试获取表名
            table_names = self._get_table_names()
            print(f"获取到的表名: {table_names}")

            # 构建表信息字典
            tables_dict = {}

            if isinstance(table_names, dict):
                # 如果返回的已经是字典格式
                tables_dict = table_names
                print("使用原有字典格式")
            elif isinstance(table_names, (list, tuple)):
                # 如果返回的是列表，为每个表构建详细信息
                print("构建表详细信息...")
                for table_name in table_names:
                    try:
                        print(f"正在处理表: {table_name}")
                        table_info = self._get_detailed_table_info(table_name)
                        tables_dict[table_name] = table_info
                        print(f"表 {table_name} 信息: {table_info}")
                    except Exception as e:
                        print(f"获取表 {table_name} 详细信息失败: {e}")
                        # 提供基本信息
                        tables_dict[table_name] = {
                            'columns': [],
                            'rows': 0,
                            'created_at': '未知',
                            'updated_at': '未知',
                            'indexes': []
                        }

            print(f"最终返回的表字典: {tables_dict}")
            print("=== get_all_tables() 执行完成 ===")
            return tables_dict

        except Exception as e:
            print(f"DatabaseManager.get_all_tables() 出错: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def _debug_components(self):
        """调试各个组件的状态"""
        print("=== 组件状态检查 ===")

        components = [
            ('catalog_manager', self.catalog_manager),
            ('storage_engine', self.storage_engine),
            ('storage_manager', self.storage_manager),
            ('table_storage', self.table_storage)
        ]

        for name, component in components:
            if component:
                print(f"{name}: 存在 ({type(component).__name__})")
                # 列出可用方法
                methods = [method for method in dir(component)
                           if not method.startswith('_') and callable(getattr(component, method))]
                print(f"  可用方法: {methods[:5]}...")  # 只显示前5个方法
            else:
                print(f"{name}: 不存在")

    def _get_table_names(self):
        """尝试多种方式获取表名"""
        table_names = []

        # 方法1: catalog_manager.get_all_table_names()
        try:
            if hasattr(self.catalog_manager, 'get_all_table_names'):
                result = self.catalog_manager.get_all_table_names()
                print(f"方法1 - get_all_table_names(): {result}")
                if result:
                    return result
        except Exception as e:
            print(f"方法1失败: {e}")

        # 方法2: catalog_manager.list_tables()
        try:
            if hasattr(self.catalog_manager, 'list_tables'):
                result = self.catalog_manager.list_tables()
                print(f"方法2 - list_tables(): {result}")
                if result:
                    return result
        except Exception as e:
            print(f"方法2失败: {e}")

        # 方法3: catalog_manager.tables 属性
        try:
            if hasattr(self.catalog_manager, 'tables'):
                result = self.catalog_manager.tables
                print(f"方法3 - tables属性: {result}")
                if result:
                    if isinstance(result, dict):
                        return list(result.keys())
                    return result
        except Exception as e:
            print(f"方法3失败: {e}")

        # 方法4: storage_engine
        try:
            if hasattr(self.storage_engine, 'get_all_tables'):
                result = self.storage_engine.get_all_tables()
                print(f"方法4 - storage_engine.get_all_tables(): {result}")
                if result:
                    return result
        except Exception as e:
            print(f"方法4失败: {e}")

        # 方法5: 直接查询catalog表
        try:
            result = self._query_system_catalog()
            print(f"方法5 - 查询系统目录: {result}")
            if result:
                return result
        except Exception as e:
            print(f"方法5失败: {e}")

        print("所有方法都失败了，返回空列表")
        return []

    def _query_system_catalog(self):
        """直接查询系统目录表获取表名"""
        try:
            # 这里你可能需要根据你的系统目录结构来调整
            # 通常系统目录会有一个存储所有表信息的地方
            if hasattr(self.catalog_manager, 'system_catalog'):
                catalog = self.catalog_manager.system_catalog
                if hasattr(catalog, 'tables'):
                    return list(catalog.tables.keys())

            # 或者尝试其他方式
            if hasattr(self.catalog_manager, '_tables'):
                return list(self.catalog_manager._tables.keys())

        except Exception as e:
            print(f"查询系统目录失败: {e}")

        return []

    def _get_detailed_table_info(self, table_name):
        """获取表的详细信息"""
        print(f"=== 获取表 {table_name} 的详细信息 ===")

        table_info = {
            'columns': [],
            'rows': 0,
            'created_at': '未知',
            'updated_at': '未知',
            'indexes': []
        }

        # 获取列信息
        columns = self._get_table_columns(table_name)
        table_info['columns'] = columns
        print(f"列信息: {columns}")

        # 获取行数
        row_count = self._get_table_row_count(table_name)
        table_info['rows'] = row_count
        print(f"行数: {row_count}")

        # 获取索引信息
        indexes = self._get_table_indexes(table_name)
        table_info['indexes'] = indexes
        print(f"索引信息: {indexes}")

        return table_info

    def _get_table_columns(self, table_name):
        """获取表的列信息"""
        try:
            # 方法1: catalog_manager.get_table_schema()
            if hasattr(self.catalog_manager, 'get_table_schema'):
                schema = self.catalog_manager.get_table_schema(table_name)
                if schema and 'columns' in schema:
                    return schema['columns']

            # 方法2: catalog_manager.get_columns()
            if hasattr(self.catalog_manager, 'get_columns'):
                columns = self.catalog_manager.get_columns(table_name)
                if columns:
                    return columns

            # 方法3: 通过table storage
            if hasattr(self.table_storage, 'get_table_schema'):
                schema = self.table_storage.get_table_schema(table_name)
                if schema:
                    return schema.get('columns', [])

            print(f"无法获取表 {table_name} 的列信息")

        except Exception as e:
            print(f"获取表 {table_name} 列信息失败: {e}")

        return []

    def _get_table_row_count(self, table_name):
        """获取表的行数"""
        try:
            # 方法1: storage_engine.get_table_row_count()
            if hasattr(self.storage_engine, 'get_table_row_count'):
                count = self.storage_engine.get_table_row_count(table_name)
                if count is not None:
                    return count

            # 方法2: table_storage.count_rows()
            if hasattr(self.table_storage, 'count_rows'):
                count = self.table_storage.count_rows(table_name)
                if count is not None:
                    return count

            # 方法3: 执行SELECT COUNT(*)
            try:
                count_sql = f"SELECT COUNT(*) FROM {table_name}"
                result, _ = self.execute_query(count_sql)
                if result and len(result) > 0 and len(result[0]) > 0:
                    return result[0][0]
            except:
                pass

            print(f"无法获取表 {table_name} 的行数")

        except Exception as e:
            print(f"获取表 {table_name} 行数失败: {e}")

        return 0

    def _get_table_indexes(self, table_name):
        """获取表的索引信息"""
        try:
            # 方法1: catalog_manager.get_table_indexes()
            if hasattr(self.catalog_manager, 'get_table_indexes'):
                indexes = self.catalog_manager.get_table_indexes(table_name)
                if indexes:
                    return indexes

            # 方法2: catalog_manager.get_indexes()
            if hasattr(self.catalog_manager, 'get_indexes'):
                indexes = self.catalog_manager.get_indexes(table_name)
                if indexes:
                    return indexes

            print(f"无法获取表 {table_name} 的索引信息")

        except Exception as e:
            print(f"获取表 {table_name} 索引信息失败: {e}")

        return []

    def shutdown(self):
        """关闭数据库连接"""
        try:
            self.storage_manager.shutdown()
        except:
            pass

    def refresh_info(self):
        """刷新数据库信息"""
        return self.get_all_tables()

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
