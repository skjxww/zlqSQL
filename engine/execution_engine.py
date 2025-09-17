# engine/execution_engine.py
from typing import List, Dict, Any, Optional, Tuple
from engine.storage_engine import StorageEngine
from catalog.catalog_manager import CatalogManager
from sql_compiler.codegen.operators import (Operator, CreateTableOp, InsertOp, SeqScanOp, FilterOp, ProjectOp, UpdateOp, \
    DeleteOp, OptimizedSeqScanOp, GroupByOp, OrderByOp, JoinOp, FilteredSeqScanOp, IndexScanOp, IndexOnlyScanOp, CreateIndexOp,
    DropIndexOp, BeginTransactionOp, CommitTransactionOp, RollbackTransactionOp, CreateViewOp, DropViewOp, ShowViewsOp,
    DescribeViewOp, ViewScanOp, ShowIndexesOp)
from sql_compiler.exceptions.compiler_errors import SemanticError
from sql_compiler.semantic.symbol_table import SymbolTable
from sql_compiler.semantic.type_checker import TypeChecker
from storage.core.transaction_manager import TransactionManager, IsolationLevel  # 添加事务管理器导入
from sql_compiler.parser.ast_nodes import TableRef
import logging

# 添加日志记录器
logger = logging.getLogger("execution_engine")

class ExecutionEngine:
    def __init__(self, storage_engine: StorageEngine, catalog_manager: CatalogManager):
        self.storage_engine = storage_engine
        self.catalog = catalog_manager
        self.symbol_table = SymbolTable()
        self.type_checker = TypeChecker(self.symbol_table)

        # 添加事务相关属性
        self.transaction_manager = None
        self.current_transaction_id = None
        self.transaction_mode = None
        self.isolation_level = IsolationLevel.READ_COMMITTED

        # 添加视图相关属性
        self.views = {}  # 存储视图定义

        # 添加日志记录器实例
        self.logger = logging.getLogger("execution_engine")

    def set_transaction_manager(self, transaction_manager: TransactionManager):
        """设置事务管理器"""
        self.transaction_manager = transaction_manager

    def begin_transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> int:
        """开始一个新事务"""
        if self.transaction_manager is None:
            raise SemanticError("Transaction manager not initialized")

        self.current_transaction_id = self.transaction_manager.begin_transaction(isolation_level)
        self.isolation_level = isolation_level
        return self.current_transaction_id

    def commit_transaction(self) -> bool:
        """提交当前事务"""
        if self.current_transaction_id is None:
            raise SemanticError("No active transaction to commit")

        if self.transaction_manager is None:
            raise SemanticError("Transaction manager not initialized")

        try:
            self.transaction_manager.commit(self.current_transaction_id)
            self.current_transaction_id = None
            return True
        except Exception as e:
            raise SemanticError(f"Failed to commit transaction: {str(e)}")

    # execution_engine.py 中的修改

    def rollback_transaction(self) -> bool:
        """回滚当前事务"""
        if self.current_transaction_id is None:
            # 改为返回成功而不是抛出错误，因为ROLLBACK在没有事务时是允许的
            print("WARNING: No active transaction to rollback - this is allowed in SQL")
            return True

        if self.transaction_manager is None:
            raise SemanticError("Transaction manager not initialized")

        try:
            # 确保传递事务ID给存储引擎
            success = self.storage_engine.rollback_transaction(self.current_transaction_id)
            if success:
                self.current_transaction_id = None
            return success
        except Exception as e:
            # 添加详细错误日志
            print(f"ERROR: Failed to rollback transaction {self.current_transaction_id}: {str(e)}")
            # 尝试强制清理事务
            try:
                if self.transaction_manager:
                    self.transaction_manager.force_cleanup_transaction(self.current_transaction_id)
            except Exception as cleanup_error:
                print(f"ERROR: Failed to cleanup transaction during rollback: {cleanup_error}")
            self.current_transaction_id = None
            raise SemanticError(f"Failed to rollback transaction: {str(e)}")

    def get_transaction_status(self) -> Dict[str, Any]:
        """获取当前事务状态"""
        if self.current_transaction_id is None:
            return {"status": "NO_ACTIVE_TRANSACTION"}

        if self.transaction_manager is None:
            return {"status": "TRANSACTION_MANAGER_NOT_INITIALIZED"}

        txn = self.transaction_manager.get_transaction(self.current_transaction_id)
        if txn:
            return {
                "status": "ACTIVE",
                "transaction_id": self.current_transaction_id,
                "isolation_level": txn.isolation_level.name,
                "state": txn.state.name
            }
        else:
            return {"status": "TRANSACTION_NOT_FOUND"}

    # 在 execute_plan 方法中添加这个分支
    def execute_plan(self, plan: Operator) -> Any:
        """执行查询计划 - 修复GroupBy执行"""
        try:
            # 检查是否为视图扫描
            if isinstance(plan, SeqScanOp) and plan.table_name in self.views:
                # 这是视图，执行视图定义
                view_info = self.views[plan.table_name]
                if 'plan' in view_info:
                    # 执行视图的SELECT计划
                    return self.execute_plan(view_info['plan'])
                else:
                    # 回退到直接使用视图定义（如果有）
                    return view_info.get('definition', [])

            # 检查是否为事务操作
            if hasattr(plan, 'operation_type'):
                if plan.operation_type == 'BEGIN_TRANSACTION':
                    isolation_level = getattr(plan, 'isolation_level', IsolationLevel.READ_COMMITTED)
                    txn_id = self.begin_transaction(isolation_level)
                    return f"Transaction {txn_id} started with isolation level {isolation_level.name}"

                elif plan.operation_type == 'COMMIT_TRANSACTION':
                    self.commit_transaction()
                    return "Transaction committed successfully"

                elif plan.operation_type == 'ROLLBACK_TRANSACTION':
                    self.rollback_transaction()
                    return "Transaction rolled back successfully"

            # 检查当前是否有活跃事务
            if self.current_transaction_id is None and self._requires_transaction(plan):
                # 自动开始一个事务
                self.begin_transaction()
                print(f"Auto-started transaction {self.current_transaction_id} for operation")

            # 添加对事务操作符的直接类型检查
            if isinstance(plan, BeginTransactionOp):
                isolation_level = getattr(plan, 'isolation_level', None)
                if isolation_level is None:
                    isolation_level = IsolationLevel.READ_COMMITTED
                elif isinstance(isolation_level, str):
                    # 将字符串转换为 IsolationLevel 枚举
                    try:
                        isolation_level = IsolationLevel[isolation_level.upper().replace(' ', '_')]
                    except KeyError:
                        isolation_level = IsolationLevel.READ_COMMITTED

                txn_id = self.begin_transaction(isolation_level)
                return f"Transaction {txn_id} started with isolation level {isolation_level.name}"

            elif isinstance(plan, CommitTransactionOp):
                self.commit_transaction()
                return "Transaction committed successfully"

            elif isinstance(plan, RollbackTransactionOp):
                self.rollback_transaction()
                return "Transaction rolled back successfully"

            # 添加视图操作符处理
            elif isinstance(plan, CreateViewOp):
                return self.execute_create_view(
                    plan.view_name,
                    plan.select_plan,
                    plan.columns,
                    plan.or_replace,
                    plan.materialized,
                    plan.with_check_option
                )
            elif isinstance(plan, DropViewOp):
                return self.execute_drop_view(
                    plan.view_names,
                    plan.if_exists,
                    plan.cascade,
                    plan.materialized
                )
            elif isinstance(plan, ShowViewsOp):
                return self.execute_show_views(plan.pattern, plan.database)
            elif isinstance(plan, DescribeViewOp):
                return self.execute_describe_view(plan.view_name)
            elif isinstance(plan, ViewScanOp):
                return self.execute_view_scan(plan.underlying_plan)

            if isinstance(plan, CreateTableOp):
                return self.execute_create_table(plan.table_name, plan.columns)
            elif isinstance(plan, InsertOp):
                return self.execute_insert(plan.table_name, plan.columns, plan.values)
            # 在 execute_plan 方法中添加对 SeqScanOp 的特殊处理
            elif isinstance(plan, SeqScanOp):
                # 检查是否是视图
                if plan.table_name in self.views:
                    view_info = self.views[plan.table_name]
                    if 'plan' in view_info:
                        # 执行视图的SELECT计划
                        return self.execute_plan(view_info['plan'])
                    else:
                        # 回退到直接使用视图定义
                        return view_info.get('definition', [])
                else:
                    return self.execute_seq_scan(plan.table_name)
            elif isinstance(plan, OptimizedSeqScanOp):
                return self.execute_optimized_seq_scan(plan.table_name, plan.selected_columns)
            elif isinstance(plan, FilterOp):
                # 处理过滤操作
                child_results = self.execute_plan(plan.children[0])
                return [row for row in child_results if self.evaluate_condition(row, plan.condition)]
            elif isinstance(plan, GroupByOp):
                # 🔑 使用独立的execute_group_by方法，传入正确的参数
                return self.execute_group_by(
                    group_columns=plan.group_columns,
                    having_condition=plan.having_condition,
                    child_plan=plan.children[0],  # 传入子计划而不是执行结果
                    aggregate_functions=plan.aggregate_functions
                )
            elif isinstance(plan, ProjectOp):
                return self.execute_project(plan.columns, plan.children[0])
            elif isinstance(plan, UpdateOp):
                return self.execute_update(plan.table_name, plan.assignments, plan.children[0])
            elif isinstance(plan, DeleteOp):
                return self.execute_delete(plan.table_name, plan.children[0])
            elif isinstance(plan, OrderByOp):
                return self.execute_order_by(plan.order_columns, plan.children[0])
            elif isinstance(plan, JoinOp):
                return self.execute_join(plan.join_type, plan.on_condition, plan.children)
            elif isinstance(plan, FilteredSeqScanOp):
                return self.execute_filtered_seq_scan(plan.table_name, plan.condition)
            elif isinstance(plan, IndexScanOp):
                return self.execute_index_scan(plan.table_name, plan.index_name, plan.scan_condition)
            elif isinstance(plan, IndexOnlyScanOp):
                return self.execute_index_only_scan(plan.table_name, plan.index_name, plan.scan_condition)
            elif isinstance(plan, CreateIndexOp):
                return self.execute_create_index(plan.index_name, plan.table_name, plan.columns, plan.unique,
                                                 plan.index_type)
            elif isinstance(plan, DropIndexOp):
                return self.execute_drop_index(plan.index_name)
            elif isinstance(plan, ShowIndexesOp):
                return self.execute_show_indexes(plan.table_name)
            else:
                raise SemanticError(f"不支持的执行计划类型: {type(plan).__name__}")
        except Exception as e:
            # 发生错误时自动回滚事务
            if self.current_transaction_id is not None:
                try:
                    self.rollback_transaction()
                    print(f"Auto-rolled back transaction due to error: {str(e)}")
                except:
                    pass  # 忽略回滚过程中的错误
            raise SemanticError(f"执行错误: {str(e)}")

    # 修改execute_drop_view方法
    # execution_engine.py 修改部分

    def execute_create_view(self, view_name: str, select_plan: Operator, columns: Optional[List[str]] = None,
                            or_replace: bool = False, materialized: bool = False,
                            with_check_option: bool = False) -> str:
        """执行CREATE VIEW语句 - 修复版本，确保持久化到catalog"""
        try:
            # 检查视图是否已存在
            if view_name in self.views and not or_replace:
                raise SemanticError(f"View '{view_name}' already exists")

            # 从SELECT计划构建视图定义SQL
            view_definition = self._construct_view_definition(
                select_plan, view_name, columns, or_replace, materialized, with_check_option
            )

            # 调用catalog manager创建视图（这会持久化到system_catalog.json）
            success = self.catalog.create_view(
                view_name=view_name,
                definition=view_definition,
                columns=columns,
                is_materialized=materialized,
                or_replace=or_replace,
                with_check_option=with_check_option
            )

            if not success:
                raise SemanticError(f"Failed to create view '{view_name}' in catalog")

            # 存储视图定义到内存
            self.views[view_name] = {
                'plan': select_plan,  # 保存执行计划
                'columns': columns,  # 保存视图列名
                'materialized': materialized,
                'with_check_option': with_check_option,
                'definition': view_definition  # 保存SQL定义
            }

            # 尝试存储到持久化存储（可选）
            try:
                self.storage_engine.create_view(
                    view_name,
                    view_definition,  # 使用SQL定义而不是执行计划
                    columns,
                    materialized,
                    with_check_option
                )
            except Exception as e:
                self.logger.warning(f"Could not persist view '{view_name}' to storage: {e}")

            return f"View '{view_name}' created successfully"
        except Exception as e:
            # 如果创建失败，确保从catalog中删除视图（如果已创建）
            if self.catalog.view_exists(view_name):
                self.catalog.drop_view(view_name, if_exists=True)
            raise SemanticError(f"创建视图错误: {str(e)}")

    def execute_drop_view(self, view_names: List[str], if_exists: bool = False,
                          cascade: bool = False, materialized: bool = False) -> str:
        """执行DROP VIEW语句 - 确保从catalog中删除"""
        try:
            dropped_count = 0
            for view_name in view_names:
                # 首先从catalog中删除视图（这会持久化到system_catalog.json）
                catalog_success = self.catalog.drop_view(
                    view_name=view_name,
                    if_exists=if_exists,
                    cascade=cascade
                )

                if not catalog_success and not if_exists:
                    raise SemanticError(f"Failed to drop view '{view_name}' from catalog")

                # 然后从内存中删除视图
                if view_name in self.views:
                    del self.views[view_name]
                    dropped_count += 1

                # 从存储引擎删除视图
                try:
                    storage_success = self.storage_engine.drop_view(view_name)
                    if not storage_success and not if_exists:
                        self.logger.warning(f"Failed to drop view '{view_name}' from storage")
                except Exception as e:
                    self.logger.warning(f"Error dropping view '{view_name}' from storage: {e}")

            return f"view '{view_name}' dropped successfully"
        except Exception as e:
            raise SemanticError(f"删除视图错误: {str(e)}")

    def _construct_view_definition(self, select_plan: Operator, view_name: str, columns: Optional[List[str]] = None,
                                   or_replace: bool = False, materialized: bool = False,
                                   with_check_option: bool = False) -> str:
        """从SELECT计划构建视图定义SQL"""

        def reconstruct_sql(operator: Operator) -> str:
            """递归重构SQL语句"""
            if not operator:
                return ""

            operator_type = operator.__class__.__name__

            # 处理投影操作 (SELECT子句)
            if operator_type == "ProjectOp":
                columns = getattr(operator, 'columns', ['*'])
                columns_str = ', '.join(columns)

                # 处理FROM子句
                from_clause = ""
                if operator.children:
                    from_clause = reconstruct_sql(operator.children[0])

                return f"SELECT {columns_str} FROM {from_clause}"

            # 处理过滤操作 (WHERE子句)
            elif operator_type == "FilterOp":
                condition = getattr(operator, 'condition', None)
                condition_str = reconstruct_expression(condition) if condition else "1=1"

                child_sql = reconstruct_sql(operator.children[0]) if operator.children else "SELECT *"

                # 检查child_sql是否已经有WHERE子句
                if "WHERE" in child_sql.upper():
                    # 如果已经有WHERE，使用AND连接
                    return f"{child_sql} AND {condition_str}"
                else:
                    return f"{child_sql} WHERE {condition_str}"

            # 处理连接操作
            elif operator_type == "JoinOp":
                join_type = getattr(operator, 'join_type', 'INNER').upper()
                on_condition = getattr(operator, 'on_condition', None)

                if len(operator.children) >= 2:
                    left_sql = reconstruct_sql(operator.children[0])
                    right_sql = reconstruct_sql(operator.children[1])

                    join_clause = f"{left_sql} {join_type} JOIN {right_sql}"

                    if on_condition:
                        on_str = reconstruct_expression(on_condition)
                        join_clause += f" ON {on_str}"

                    return join_clause
                return ""

            # 处理分组操作 (GROUP BY子句)
            elif operator_type == "GroupByOp":
                group_columns = getattr(operator, 'group_columns', [])
                having_condition = getattr(operator, 'having_condition', None)
                aggregate_functions = getattr(operator, 'aggregate_functions', [])

                # 构建SELECT子句（包含聚合函数）
                select_parts = []

                # 添加分组列
                select_parts.extend(group_columns)

                # 添加聚合函数
                for func_name, column_name in aggregate_functions:
                    if column_name == '*':
                        select_parts.append(f"{func_name.upper()}(*)")
                    else:
                        select_parts.append(f"{func_name.upper()}({column_name})")

                select_clause = ', '.join(select_parts) if select_parts else '*'

                # 构建基础SQL
                base_sql = reconstruct_sql(operator.children[0]) if operator.children else "SELECT *"

                # 替换SELECT子句
                if base_sql.upper().startswith('SELECT'):
                    base_sql = f"SELECT {select_clause}" + base_sql[6:]  # 移除原来的SELECT部分

                # 添加GROUP BY
                if group_columns:
                    group_by_str = ', '.join(group_columns)
                    base_sql += f" GROUP BY {group_by_str}"

                # 添加HAVING
                if having_condition:
                    having_str = reconstruct_expression(having_condition)
                    base_sql += f" HAVING {having_str}"

                return base_sql

            # 处理排序操作 (ORDER BY子句)
            elif operator_type == "OrderByOp" or operator_type == "SortOp":
                order_columns = getattr(operator, 'order_columns', [])
                order_by_parts = []

                for col_info in order_columns:
                    if isinstance(col_info, tuple):
                        column, direction = col_info
                        order_by_parts.append(f"{column} {direction.upper()}")
                    elif isinstance(col_info, dict):
                        column = col_info.get('column', '')
                        direction = col_info.get('direction', 'ASC')
                        order_by_parts.append(f"{column} {direction.upper()}")

                order_by_str = ', '.join(order_by_parts)

                child_sql = reconstruct_sql(operator.children[0]) if operator.children else "SELECT *"

                return f"{child_sql} ORDER BY {order_by_str}"

            # 处理表扫描操作
            elif operator_type == "SeqScanOp" or operator_type == "FilteredSeqScanOp":
                table_name = getattr(operator, 'table_name', 'unknown_table')
                table_alias = getattr(operator, 'table_alias', None)

                if table_alias and table_alias != table_name:
                    return f"{table_name} AS {table_alias}"
                return table_name

            # 处理子查询
            elif operator_type == "SubqueryOp":
                subquery_sql = reconstruct_sql(operator.select_plan)
                return f"({subquery_sql})"

            # 处理联合操作
            elif operator_type == "UnionOp":
                union_type = getattr(operator, 'union_type', 'UNION')
                child_sqls = [reconstruct_sql(child) for child in operator.children]
                return f" {union_type} ".join([f"({sql})" for sql in child_sqls])

            # 处理哈希聚合
            elif operator_type == "HashAggregateOp":
                group_columns = getattr(operator, 'group_columns', [])
                agg_functions = getattr(operator, 'agg_functions', [])
                having_condition = getattr(operator, 'having_condition', None)

                # 构建SELECT子句
                select_parts = []
                select_parts.extend(group_columns)

                for agg_func in agg_functions:
                    func_name = agg_func.get('func', '')
                    column = agg_func.get('column', '')
                    alias = agg_func.get('alias', '')

                    if column == '*':
                        agg_expr = f"{func_name.upper()}(*)"
                    else:
                        agg_expr = f"{func_name.upper()}({column})"

                    if alias:
                        select_parts.append(f"{agg_expr} AS {alias}")
                    else:
                        select_parts.append(agg_expr)

                select_clause = ', '.join(select_parts)

                # 构建基础SQL
                base_sql = reconstruct_sql(operator.children[0]) if operator.children else "SELECT *"

                # 替换SELECT子句
                if base_sql.upper().startswith('SELECT'):
                    base_sql = f"SELECT {select_clause}" + base_sql[6:]

                # 添加GROUP BY
                if group_columns:
                    group_by_str = ', '.join(group_columns)
                    base_sql += f" GROUP BY {group_by_str}"

                # 添加HAVING
                if having_condition:
                    having_str = reconstruct_expression(having_condition)
                    base_sql += f" HAVING {having_str}"

                return base_sql

            # 默认情况：递归处理子节点
            elif operator.children:
                child_sqls = [reconstruct_sql(child) for child in operator.children]
                return ' '.join([sql for sql in child_sqls if sql])

            return "SELECT *"

        def reconstruct_expression(expr) -> str:
            """重构表达式"""
            if expr is None:
                return ""

            # 如果是字典形式的表达式（来自to_dict()）
            if isinstance(expr, dict):
                expr_type = expr.get('type', '')

                if expr_type == "BinaryExpr":
                    left = reconstruct_expression(expr.get('left'))
                    operator = expr.get('operator', '=')
                    right = reconstruct_expression(expr.get('right'))
                    return f"{left} {operator} {right}"

                elif expr_type == "IdentifierExpr":
                    table_name = expr.get('table_name')
                    column_name = expr.get('name', 'unknown')
                    if table_name:
                        return f"{table_name}.{column_name}"
                    return column_name

                elif expr_type == "LiteralExpr":
                    value = expr.get('value')
                    if isinstance(value, str):
                        return f"'{value}'"
                    return str(value)

                elif expr_type == "FunctionExpr":
                    func_name = expr.get('function_name', 'UNKNOWN')
                    args = expr.get('arguments', [])
                    args_str = ', '.join([reconstruct_expression(arg) for arg in args])
                    return f"{func_name}({args_str})"

                elif expr_type == "BetweenExpr":
                    expr_val = reconstruct_expression(expr.get('expr'))
                    lower = reconstruct_expression(expr.get('lower'))
                    upper = reconstruct_expression(expr.get('upper'))
                    return f"{expr_val} BETWEEN {lower} AND {upper}"

            # 如果是AST节点对象
            elif hasattr(expr, 'to_dict'):
                return reconstruct_expression(expr.to_dict())

            # 如果是简单的值
            else:
                return str(expr)

        # 构建完整的CREATE VIEW语句
        select_statement = reconstruct_sql(select_plan)

        # 添加视图选项
        view_options = []
        if or_replace:
            view_options.append("OR REPLACE")
        if materialized:
            view_options.append("MATERIALIZED")
        if with_check_option:
            view_options.append("WITH CHECK OPTION")

        options_str = ' '.join(view_options)

        # 添加列定义
        columns_str = ""
        if columns:
            columns_str = f" ({', '.join(columns)})"

        return f"CREATE {options_str} VIEW {view_name}{columns_str} AS {select_statement}"

    def execute_describe_view(self, view_name: str) -> Dict[str, Any]:
        """执行DESCRIBE VIEW语句"""
        try:
            if view_name not in self.views:
                raise SemanticError(f"View '{view_name}' does not exist")

            view_info = self.views[view_name]
            return {
                'name': view_name,
                'columns': view_info['columns'],
                'materialized': view_info['materialized'],
                'with_check_option': view_info['with_check_option'],
                'definition': view_info['definition']
            }
        except Exception as e:
            raise SemanticError(f"描述视图错误: {str(e)}")

    def execute_view_scan(self, underlying_plan: Operator) -> List[Dict]:
        """执行视图扫描操作"""
        try:
            # 执行底层查询计划
            return self.execute_plan(underlying_plan)
        except Exception as e:
            raise SemanticError(f"视图扫描错误: {str(e)}")

    def _pattern_match(self, name: str, pattern: str) -> bool:
        """简单的模式匹配函数"""
        # 将SQL模式转换为正则表达式
        import re
        regex_pattern = pattern.replace('%', '.*').replace('_', '.')
        return re.match(regex_pattern, name) is not None

    def _requires_transaction(self, plan: Operator) -> bool:
        """检查执行计划是否需要事务支持"""
        # DML操作需要事务支持
        return isinstance(plan, (InsertOp, UpdateOp, DeleteOp))

    # execution_engine.py 中的 execute_create_table 方法
    def execute_create_table(self, table_name: str, columns: List[tuple]) -> str:
        """执行CREATE TABLE语句"""
        try:
            # 添加调试信息
            print(f"DEBUG: Creating table {table_name}")
            print(f"DEBUG: Columns received: {columns}")

            # 检查columns参数的结构
            for i, col_tuple in enumerate(columns):
                print(f"DEBUG: Column {i}: {col_tuple}, type: {type(col_tuple)}, length: {len(col_tuple)}")

            # 添加到符号表
            self.symbol_table.add_table(table_name, columns)

            # 添加到catalog - 确保传递正确的参数格式（元组列表）
            success = self.catalog.create_table(table_name, columns)
            print(f"DEBUG: Catalog create_table result: {success}")

            # 将columns格式从tuple列表转换为dict列表（用于存储引擎）
            column_dicts = []
            for col_tuple in columns:
                if len(col_tuple) >= 2:
                    col_dict = {
                        'name': col_tuple[0],
                        'type': col_tuple[1],
                        'constraints': col_tuple[2] if len(col_tuple) > 2 else None
                    }
                    column_dicts.append(col_dict)
                    print(f"DEBUG: Column tuple: {col_tuple} -> Column dict: {col_dict}")

            # 添加到存储引擎
            self.storage_engine.create_table(table_name, column_dicts)
            return f"Table {table_name} created successfully"
        except Exception as e:
            raise SemanticError(f"创建表错误: {str(e)}")

    # execution_engine.py 中的 execute_insert 方法
    def execute_insert(self, table_name: str, columns: List[str], values: List[Any]) -> str:
        """执行INSERT语句"""
        try:
            # 获取表schema
            schema = self.catalog.get_table_schema(table_name)
            if schema is None:
                raise SemanticError(f"Table '{table_name}' schema not found")

            # 调试信息：打印schema信息
            print(f"DEBUG: Table schema from catalog: {schema}")

            # 构建行数据字典
            row_data = {}
            column_names = [col_info[0] for col_info in schema]  # 从 tuple 中提取列名

            if columns:  # 指定了列名
                for i, col_name in enumerate(columns):
                    if i < len(values):
                        value = self._extract_value(values[i], table_name)
                        # 从catalog获取类型信息
                        col_info = self.catalog.get_column_info(table_name, col_name)
                        print(f"DEBUG: Column info for '{col_name}': {col_info}")

                        if col_info:
                            expected_type = col_info.get('type')
                            print(
                                f"DEBUG: Column '{col_name}' - expected: {expected_type}, actual: {type(value).__name__}")

                            # 进行类型检查
                            if expected_type and not self._is_type_compatible(type(value).__name__, expected_type):
                                # 添加更详细的错误信息
                                raise SemanticError(
                                    f"列 '{col_name}' 类型不兼容: 期望 {expected_type}, 得到 {type(value).__name__}。"
                                    f"值: {value} (类型: {type(value).__name__})")

                        row_data[col_name] = value
            else:  # 未指定列名，按顺序插入所有值
                for i, value_expr in enumerate(values):
                    if i < len(column_names):
                        col_name = column_names[i]
                        value = self._extract_value(value_expr, table_name)
                        # 从catalog获取类型信息
                        col_info = self.catalog.get_column_info(table_name, col_name)
                        if col_info:
                            expected_type = col_info.get('type')
                            print(
                                f"DEBUG: Column '{col_name}' (index {i}) - expected: {expected_type}, actual: {type(value).__name__}")

                            # 进行类型检查
                            if expected_type and not self._is_type_compatible(type(value).__name__, expected_type):
                                # 添加更详细的错误信息
                                raise SemanticError(
                                    f"列 '{col_name}' 类型不兼容: 期望 {expected_type}, 得到 {type(value).__name__}。"
                                    f"值: {value} (类型: {type(value).__name__})")

                        row_data[col_name] = value

            # 确保所有字段都有值，缺失的字段设为None
            values_list = []
            for col_name in column_names:
                if col_name in row_data:
                    values_list.append(row_data[col_name])
                else:
                    values_list.append(None)  # 设置默认值

            # 添加调试信息
            print(f"DEBUG: Final values list for insertion: {values_list}")
            print(f"DEBUG: Column names: {column_names}")

            # 添加事务支持
            if self.current_transaction_id is not None and self.transaction_manager is not None:
                # 在事务中执行插入
                success = self.storage_engine.insert_row_transactional(
                    table_name, values_list, self.current_transaction_id
                )
                if not success:
                    raise SemanticError("Failed to insert row in transaction")
            else:
                # 非事务插入
                self.storage_engine.insert_row(table_name, values_list)

            return "1 row inserted"
        except Exception as e:
            raise SemanticError(f"插入行错误: {str(e)}")

    # execution_engine.py 中的 execute_seq_scan 方法
    def execute_seq_scan(self, table_name: str) -> List[Dict]:
        """执行顺序扫描 - 添加视图支持"""
        try:
            # 首先检查是否是视图
            if table_name in self.views:
                # 这是视图，执行视图定义
                view_info = self.views[table_name]
                if 'plan' in view_info:
                    # 执行视图的SELECT计划
                    results = list(self.execute_plan(view_info['plan']))
                    # 处理列映射（如果有）
                    if view_info.get('columns'):
                        mapped_results = []
                        for row in results:
                            mapped_row = {}
                            for i, col_name in enumerate(view_info['columns']):
                                if i < len(row):
                                    original_keys = list(row.keys())
                                    if i < len(original_keys):
                                        mapped_row[col_name] = row[original_keys[i]]
                            mapped_results.append(mapped_row)
                        return mapped_results
                    return results
                else:
                    # 回退到直接使用视图定义（如果有）
                    return view_info.get('definition', [])
            else:
                # 普通表，从存储引擎获取数据
                try:
                    return self.storage_engine.get_all_rows(table_name)
                except Exception as e:
                    # 如果表不存在，检查是否是大小写问题
                    if "not found" in str(e).lower():
                        # 尝试查找可能的大小写变体
                        all_tables = self.catalog.get_all_tables()
                        matching_tables = [t for t in all_tables if t.lower() == table_name.lower()]

                        if matching_tables:
                            # 使用正确大小写的表名重试
                            correct_name = matching_tables[0]
                            self.logger.warning(f"Table '{table_name}' not found, using '{correct_name}' instead")
                            return self.storage_engine.get_all_rows(correct_name)

                    # 如果还是失败，重新抛出异常
                    raise SemanticError(f"扫描表 {table_name} 错误: {str(e)}")
        except Exception as e:
            raise SemanticError(f"扫描表/视图 {table_name} 错误: {str(e)}")

    def execute_filter(self, condition: Any, child_plan: Operator) -> List[Dict]:
        """执行过滤操作"""
        try:
            # 先执行子计划
            child_results = list(self.execute_plan(child_plan))

            # 应用过滤条件
            filtered_results = []
            for row in child_results:
                if self.evaluate_condition(row, condition):
                    filtered_results.append(row)

            return filtered_results
        except Exception as e:
            raise SemanticError(f"应用过滤条件错误: {str(e)}")

    def execute_project(self, columns: List[str], child_plan: Operator) -> List[Dict]:
        """执行投影操作"""
        try:
            # 先执行子计划
            child_results = list(self.execute_plan(child_plan))
            print(f"DEBUG: Project - Input rows: {len(child_results)}")
            print(f"DEBUG: Project - Columns to select: {columns}")

            if child_results:
                print(f"DEBUG: Project - First input row keys: {list(child_results[0].keys())}")
                print(f"DEBUG: Project - First input row values: {child_results[0]}")

            # 应用投影
            projected_results = []
            for row in child_results:
                projected_row = {}
                for col in columns:
                    # 处理聚合函数列（如 COUNT(*), SUM(age) 等）
                    if '(' in col and ')' in col:
                        # 这是聚合函数列，应该已经在分组阶段计算好了
                        # 直接从结果行中获取
                        if col in row:
                            projected_row[col] = row[col]
                        else:
                            # 尝试查找类似的聚合列（处理大小写或格式差异）
                            matching_keys = [key for key in row.keys() if key.upper() == col.upper()]
                            if matching_keys:
                                projected_row[col] = row[matching_keys[0]]
                            else:
                                # 如果还是找不到，保持原样（可能是普通列名包含括号）
                                projected_row[col] = row.get(col, None)

                    # 处理普通列名
                    elif col in row:
                        projected_row[col] = row[col]
                    elif col == '*':  # 选择所有列
                        projected_row = row.copy()
                        break
                    else:
                        # 处理带表别名的列名
                        if '.' in col:
                            table_alias, column_name = col.split('.', 1)
                            # 查找匹配的列
                            found = False
                            for key in row.keys():
                                if key == col:  # 完全匹配 table.column
                                    projected_row[col] = row[key]
                                    found = True
                                    break
                                elif key.endswith('.' + column_name):  # 部分匹配
                                    projected_row[col] = row[key]
                                    found = True
                                    break
                            if not found:
                                projected_row[col] = None
                        else:
                            projected_row[col] = None

                projected_results.append(projected_row)
                print(f"DEBUG: Project - Output row: {projected_row}")

            print(f"DEBUG: Project - Final results: {len(projected_results)} rows")
            return projected_results

        except Exception as e:
            print(f"DEBUG: Project - Error: {e}")
            raise SemanticError(f"应用投影错误: {str(e)}")

    def execute_update(self, table_name: str, assignments: List[tuple], child_plan: Operator) -> str:
        """执行UPDATE语句"""
        try:
            # 检查事务状态
            if self.current_transaction_id is None:
                self.logger.warning("No active transaction for UPDATE operation, auto-starting one")
                self.begin_transaction()

            txn_status = self.get_transaction_status()
            self.logger.debug(f"Transaction status before UPDATE: {txn_status}")

            # 设置类型检查器的上下文表
            self.type_checker.set_context_table(table_name)

            # 先执行子计划获取要更新的行
            rows_to_update = list(self.execute_plan(child_plan))
            self.logger.debug(f"Found {len(rows_to_update)} rows to update")

            # 应用更新操作
            updated_count = 0
            for i, row in enumerate(rows_to_update):
                self.logger.debug(f"Processing row {i}: {row}")

                # 构建更新数据
                update_data = {}
                for col_name, value_expr in assignments:
                    # 计算表达式的值（需要传入当前行的上下文）
                    value = self._evaluate_expression(value_expr, row, table_name)
                    self.logger.debug(f"Assignment {col_name} = {value} (from expression {value_expr})")

                    # 类型检查
                    expected_type = self.symbol_table.get_column_type(table_name, col_name)
                    if expected_type and not self._is_type_compatible(type(value).__name__.upper(), expected_type):
                        raise SemanticError(
                            f"列 '{col_name}' 类型不兼容: 期望 {expected_type}, 得到 {type(value).__name__}")
                    update_data[col_name] = value

                self.logger.debug(f"Update data: {update_data}")

                # 添加事务支持
                if self.current_transaction_id is not None and self.transaction_manager is not None:
                    # 在事务中执行更新
                    success = self.storage_engine.update_row_transactional(
                        table_name, row, update_data, self.current_transaction_id
                    )
                    if not success:
                        self.logger.error(f"Failed to update row in transaction {self.current_transaction_id}")
                        # 尝试获取更多错误信息
                        txn_status = self.get_transaction_status()
                        self.logger.error(f"Transaction status after failure: {txn_status}")
                        raise SemanticError("Failed to update row in transaction")
                else:
                    # 非事务更新
                    self.storage_engine.update_row(table_name, row, update_data)
                updated_count += 1

            return f"{updated_count} rows updated"
        except Exception as e:
            self.logger.error(f"更新数据错误: {str(e)}")
            raise SemanticError(f"更新数据错误: {str(e)}")

    def execute_delete(self, table_name: str, child_plan: Operator) -> str:
        """执行DELETE语句"""
        try:
            # 先执行子计划获取要删除的行
            rows_to_delete = list(self.execute_plan(child_plan))

            # 执行删除操作
            deleted_count = 0
            for row in rows_to_delete:
                # 添加事务支持
                if self.current_transaction_id is not None and self.transaction_manager is not None:
                    # 在事务中执行删除
                    success = self.storage_engine.delete_row_transactional(
                        table_name, row, self.current_transaction_id
                    )
                    if not success:
                        raise SemanticError("Failed to delete row in transaction")
                else:
                    # 非事务删除
                    self.storage_engine.delete_row(table_name, row)
                deleted_count += 1

            return f"{deleted_count} rows deleted"
        except Exception as e:
            raise SemanticError(f"删除数据错误: {str(e)}")

    def evaluate_condition(self, row: Dict, condition: Any) -> bool:
        """评估WHERE条件"""
        # 添加调试信息
        print(f"DEBUG: Evaluating condition: {condition} on row: {row}")

        # 对于HAVING条件，需要特殊处理聚合函数
        if hasattr(condition, 'to_dict'):
            condition_dict = condition.to_dict()
            print(f"DEBUG: Condition dict: {condition_dict}")

            # 检查是否是HAVING条件（包含聚合函数）
            if self._is_having_condition(condition_dict):
                result = self._evaluate_having_condition(row, condition_dict)
                print(f"DEBUG: HAVING condition evaluation result: {result}")
                return result
            else:
                result = self._evaluate_condition_from_dict(row, condition_dict)
                print(f"DEBUG: Condition evaluation result: {result}")
                return result
        elif isinstance(condition, dict):
            print(f"DEBUG: Condition dict: {condition}")
            if self._is_having_condition(condition):
                result = self._evaluate_having_condition(row, condition)
                print(f"DEBUG: HAVING condition evaluation result: {result}")
                return result
            else:
                result = self._evaluate_condition_from_dict(row, condition)
                print(f"DEBUG: Condition evaluation result: {result}")
                return result
        else:
            # 默认返回True，实际应该根据条件类型进行解析
            print(f"DEBUG: Unknown condition type, returning True")
            return True

    def _is_having_condition(self, condition_dict: Dict) -> bool:
        """检查条件是否包含聚合函数（HAVING条件）"""
        if not condition_dict:
            return False

        # 检查是否包含聚合函数
        def _contains_aggregate(expr):
            if not isinstance(expr, dict):
                return False

            if expr.get('type') == 'FunctionExpr':
                func_name = expr.get('function_name', '').upper()
                if func_name in ['AVG', 'SUM', 'COUNT', 'MAX', 'MIN']:
                    return True

            # 递归检查子表达式
            for key, value in expr.items():
                if isinstance(value, dict):
                    if _contains_aggregate(value):
                        return True
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict) and _contains_aggregate(item):
                            return True

            return False

        return _contains_aggregate(condition_dict)

    def _evaluate_having_condition(self, row: Dict, condition: Dict) -> bool:
        """专门处理HAVING条件（包含聚合函数）"""
        # HAVING条件应该在分组结果中直接使用已经计算好的聚合值
        # 而不是重新计算聚合函数

        condition_type = condition.get('type')

        if condition_type == 'BinaryExpr':
            left = condition.get('left', {})
            right = condition.get('right', {})
            operator = condition.get('operator', '')

            # 对于HAVING条件，直接获取左右表达式的值（而不是重新计算）
            left_value = self._get_value_for_having(row, left)
            right_value = self._get_value_for_having(row, right)

            # 处理None值
            if left_value is None or right_value is None:
                return False

            # 添加调试信息
            print(
                f"DEBUG: HAVING BinaryExpr - left_value: {left_value}, right_value: {right_value}, operator: {operator}")

            if operator == '=':
                return left_value == right_value
            elif operator == '!=' or operator == '<>':
                return left_value != right_value
            elif operator == '>':
                return left_value > right_value
            elif operator == '>=':
                return left_value >= right_value
            elif operator == '<':
                return left_value < right_value
            elif operator == '<=':
                return left_value <= right_value

        # 对于其他类型的条件，使用普通的评估逻辑
        return self._evaluate_condition_from_dict(row, condition)

    def _get_value_for_having(self, row: Dict, expr: Dict) -> Any:
        """为HAVING条件获取值（优先使用分组结果中的值）"""
        expr_type = expr.get('type')

        if expr_type == 'FunctionExpr':
            # 对于聚合函数，尝试从结果行中查找对应的列
            func_name = expr.get('function_name', '').upper()
            args = expr.get('arguments', [])

            # 构建聚合函数列名（与分组结果中的列名格式一致）
            if args and len(args) == 1 and args[0].get('type') == 'LiteralExpr' and args[0].get('value') == '*':
                column_name = f"{func_name}(*)"
            elif args:
                # 处理有参数的函数
                arg_values = []
                for arg in args:
                    if arg.get('type') == 'IdentifierExpr':
                        arg_values.append(arg.get('name'))
                    elif arg.get('type') == 'LiteralExpr':
                        arg_values.append(str(arg.get('value')))
                    else:
                        arg_values.append('?')
                column_name = f"{func_name}({', '.join(arg_values)})"
            else:
                column_name = f"{func_name}()"

            # 首先尝试从结果行中获取已经计算好的聚合值
            if column_name in row:
                return row[column_name]

            # 如果找不到，尝试其他可能的列名格式
            possible_keys = [key for key in row.keys() if key.startswith(func_name + '(')]
            if possible_keys:
                return row[possible_keys[0]]

            # 如果还是找不到，回退到普通计算（但这种情况不应该发生）
            return self._evaluate_function(func_name, args, row)

        elif expr_type == 'LiteralExpr':
            return expr.get('value')

        elif expr_type == 'IdentifierExpr':
            column_name = expr.get('name')
            # 直接使用分组结果中的值
            return row.get(column_name)

        # 其他类型的表达式使用普通逻辑
        return self._get_value_from_condition(row, expr)

    def _evaluate_condition_from_dict(self, row: Dict, condition: Dict) -> bool:
        """从字典形式评估条件"""
        condition_type = condition.get('type')

        if condition_type == 'BinaryExpr':
            left = condition.get('left', {})
            right = condition.get('right', {})
            operator = condition.get('operator', '')

            if operator in ['AND', 'OR']:
                left_result = self._evaluate_condition_from_dict(row, left)
                right_result = self._evaluate_condition_from_dict(row, right)

                if operator == 'AND':
                    return left_result and right_result
                elif operator == 'OR':
                    return left_result or right_result
            else:
                # 对于比较操作符，需要获取实际值进行比较
                left_value = self._get_value_from_condition(row, left)
                right_value = self._get_value_from_condition(row, right)

                # 处理None值
                if left_value is None or right_value is None:
                    return False

                # 添加调试信息
                print(f"DEBUG: BinaryExpr - left_value: {left_value}, right_value: {right_value}, operator: {operator}")

                if operator == '=':
                    return left_value == right_value
                elif operator == '!=' or operator == '<>':
                    return left_value != right_value
                elif operator == '>':
                    return left_value > right_value
                elif operator == '>=':
                    return left_value >= right_value
                elif operator == '<':
                    return left_value < right_value
                elif operator == '<=':
                    return left_value <= right_value

        elif condition_type == 'IdentifierExpr':
            column_name = condition.get('name')
            table_name = condition.get('table_name')

            # 处理聚合函数列名（如 "AVG(age)"）
            if '(' in column_name and ')' in column_name:
                # 这是聚合函数列，直接从结果行中获取
                if column_name in row:
                    return bool(row[column_name])
                return False

            # 如果指定了表名，检查格式是否为 table.column
            if table_name:
                full_column_name = f"{table_name}.{column_name}"
                if full_column_name in row:
                    return bool(row[full_column_name])
                return False

            # 直接使用列名
            if column_name in row:
                return bool(row[column_name])
            return False

        elif condition_type == 'LiteralExpr':
            value = condition.get('value')
            return bool(value)

        elif condition_type == 'UnaryExpr':
            operator = condition.get('operator')
            operand = condition.get('operand', {})
            operand_value = self._evaluate_condition_from_dict(row, operand)

            if operator == 'NOT':
                return not operand_value

        elif condition_type == 'FunctionExpr':
            # 处理函数表达式
            func_name = condition.get('function_name', '').upper()
            args = condition.get('arguments', [])
            return self._evaluate_function(func_name, args, row)

        elif condition_type == 'InExpr':
            # 处理IN表达式
            left_expr = condition.get('left_expr', {})
            right_expr = condition.get('right_expr', {})
            is_not = condition.get('is_not', False)

            left_value = self._get_value_from_condition(row, left_expr)
            right_value = self._get_value_from_condition(row, right_expr)

            # 处理子查询结果（应该是列表）
            if isinstance(right_value, list):
                result = left_value in right_value
                return not result if is_not else result
            elif isinstance(right_value, (str, int, float, bool)):
                # 处理字面值比较
                result = left_value == right_value
                return not result if is_not else result
            return False

        elif condition_type == 'SubqueryExpr':
            # 处理子查询表达式
            subquery_result = self._evaluate_subquery_expression(row, condition)
            # 子查询应该返回一个值列表，用于IN操作
            return bool(subquery_result)

        return True

    def _evaluate_function(self, func_name: str, args: List[Dict], row: Dict) -> Any:
        """评估函数调用"""
        # 提取参数值
        arg_values = [self._get_value_from_condition(row, arg) for arg in args]

        if func_name == 'COUNT':
            return len([v for v in arg_values if v is not None])
        elif func_name == 'SUM':
            return sum([v for v in arg_values if isinstance(v, (int, float))])
        elif func_name == 'AVG':
            values = [v for v in arg_values if isinstance(v, (int, float))]
            return sum(values) / len(values) if values else 0
        elif func_name == 'MAX':
            values = [v for v in arg_values if isinstance(v, (int, float))]
            return max(values) if values else None
        elif func_name == 'MIN':
            values = [v for v in arg_values if isinstance(v, (int, float))]
            return min(values) if values else None
        else:
            return None

    def _get_value_from_condition(self, row: Dict, condition: Dict) -> Any:
        """从条件字典中获取实际值"""
        condition_type = condition.get('type')

        if condition_type == 'IdentifierExpr':
            column_name = condition.get('name')
            table_name = condition.get('table_name')

            # 处理带表别名的情况
            if table_name:
                full_column_name = f"{table_name}.{column_name}"

                # 首先尝试查找带表别名的列
                if full_column_name in row:
                    return row[full_column_name]

                # 如果找不到带表别名的列，尝试只用列名
                # 这在JOIN后的结果中是常见的，因为列名可能会被合并
                if column_name in row:
                    return row[column_name]

                return None

            # 直接使用列名（没有表别名的情况）
            return row.get(column_name)

        elif condition_type == 'LiteralExpr':
            return condition.get('value')

        elif condition_type == 'BinaryExpr':
            # 对于二元表达式，递归计算值
            left = self._get_value_from_condition(row, condition.get('left', {}))
            right = self._get_value_from_condition(row, condition.get('right', {}))
            operator = condition.get('operator', '')

            # 更精确的 NULL 值处理
            # 只有当一个操作数是 NULL 时才返回 NULL
            if left is None or right is None:
                # 对于算术运算，如果任一操作数为 NULL，结果为 NULL
                if operator in ['+', '-', '*', '/']:
                    return None
                # 对于比较运算，需要特殊处理（但这里主要是处理算术运算）
                return None

            if operator == '+':
                return left + right
            elif operator == '-':
                return left - right
            elif operator == '*':
                return left * right
            elif operator == '/':
                return left / right if right != 0 else None  # 除零也返回 None

        elif condition_type == 'FunctionExpr':
            # 处理函数表达式
            func_name = condition.get('function_name', '').upper()
            args = condition.get('arguments', [])
            return self._evaluate_function(func_name, args, row)

        elif condition_type == 'ValueListExpr':
            # 处理值列表
            values = condition.get('values', [])
            return [self._get_value_from_condition(row, val) for val in values]

        elif condition_type == 'SubqueryExpr':
            # 处理子查询表达式
            return self._evaluate_subquery_expression(row, condition)

        # 默认返回None
        return None

    # execution_engine.py 中的 _extract_value 方法
    def _extract_value(self, value_expr: Any, context_table: str = None) -> Any:
        """从表达式节点中提取值"""
        # 设置类型检查器的上下文表
        if context_table:
            self.type_checker.set_context_table(context_table)

        # 添加详细的调试信息
        print(f"DEBUG: Extracting value from: {value_expr}, type: {type(value_expr)}")

        # 检查是否是字面量表达式节点
        if hasattr(value_expr, 'to_dict'):
            expr_dict = value_expr.to_dict()
            print(f"DEBUG: Expression dict: {expr_dict}")

            # 如果是字面量表达式，直接返回value
            if expr_dict.get('type') == 'LiteralExpr':
                value = expr_dict.get('value')
                print(f"DEBUG: Extracted literal value: {value}, type: {type(value).__name__}")
                return value

        # 如果是基本数据类型，直接返回
        elif isinstance(value_expr, (int, float, str, bool)):
            print(f"DEBUG: Extracted basic value: {value_expr}, type: {type(value_expr).__name__}")
            return value_expr

        # 添加更多调试信息
        elif hasattr(value_expr, 'value'):
            value = value_expr.value
            print(f"DEBUG: Extracted value from .value attribute: {value}, type: {type(value).__name__}")
            return value

        else:
            print(f"DEBUG: Unknown value_expr type: {type(value_expr)}, repr: {repr(value_expr)}")
            # 尝试直接访问可能的值属性
            for attr in ['value', 'val', 'data']:
                if hasattr(value_expr, attr):
                    value = getattr(value_expr, attr)
                    print(f"DEBUG: Found value in {attr}: {value}")
                    return value

        return None

    def _extract_value_from_dict(self, expr_dict: Dict) -> Any:
        """从表达式字典中提取值"""
        expr_type = expr_dict.get('type')

        if expr_type == 'LiteralExpr':
            return expr_dict.get('value')
        elif expr_type == 'IdentifierExpr':
            # 列引用，无法直接获取值
            return None
        elif expr_type == 'BinaryExpr':
            # 对于二元表达式，递归计算值
            left = self._extract_value_from_dict(expr_dict.get('left', {}))
            right = self._extract_value_from_dict(expr_dict.get('right', {}))
            operator = expr_dict.get('operator', '')

            if operator == '+':
                return left + right
            elif operator == '-':
                return left - right
            elif operator == '*':
                return left * right
            elif operator == '/':
                return left / right if right != 0 else 0
        elif expr_type == 'FunctionExpr':
            # 处理函数表达式
            func_name = expr_dict.get('function_name', '').upper()
            args = expr_dict.get('arguments', [])
            arg_values = [self._extract_value_from_dict(arg) for arg in args]

            if func_name == 'COUNT':
                return len([v for v in arg_values if v is not None])
            elif func_name == 'SUM':
                return sum([v for v in arg_values if isinstance(v, (int, float))])
            elif func_name == 'AVG':
                values = [v for v in arg_values if isinstance(v, (int, float))]
                return sum(values) / len(values) if values else 0
            elif func_name == 'MAX':
                values = [v for v in arg_values if isinstance(v, (int, float))]
                return max(values) if values else None
            elif func_name == 'MIN':
                values = [v for v in arg_values if isinstance(v, (int, float))]
                return min(values) if values else None

        return None

    def execute_optimized_seq_scan(self, table_name: str, selected_columns: List[str]) -> List[Dict]:
        """执行优化的顺序扫描（包含投影下推）- 添加视图支持"""
        try:
            # 首先检查是否是视图
            if table_name in self.views:
                # 这是视图，执行视图定义
                view_info = self.views[table_name]
                if 'plan' in view_info:
                    # 执行视图的SELECT计划
                    view_results = list(self.execute_plan(view_info['plan']))

                    # 应用投影：只选择指定的列
                    projected_rows = []
                    for row in view_results:
                        projected_row = {}
                        for col in selected_columns:
                            if col in row:
                                projected_row[col] = row[col]
                            # 处理通配符 *
                            elif col == '*':
                                projected_row = row.copy()
                                break
                        projected_rows.append(projected_row)

                    return projected_rows
                else:
                    # 回退到直接使用视图定义（如果有）
                    view_data = view_info.get('definition', [])

                    # 应用投影
                    projected_rows = []
                    for row in view_data:
                        projected_row = {}
                        for col in selected_columns:
                            if col in row:
                                projected_row[col] = row[col]
                            elif col == '*':
                                projected_row = row.copy()
                                break
                        projected_rows.append(projected_row)

                    return projected_rows
            else:
                # 普通表，从存储引擎获取数据
                all_rows = self.storage_engine.get_all_rows(table_name)

                # 应用投影：只选择指定的列
                projected_rows = []
                for row in all_rows:
                    projected_row = {}
                    for col in selected_columns:
                        if col in row:
                            projected_row[col] = row[col]
                        # 处理通配符 *
                        elif col == '*':
                            projected_row = row.copy()
                            break
                    projected_rows.append(projected_row)

                return projected_rows
        except Exception as e:
            raise SemanticError(f"扫描表/视图 {table_name} 错误: {str(e)}")

    def execute_delete(self, table_name: str, child_plan: Operator) -> str:
        """执行DELETE语句"""
        try:
            # 先执行子计划获取要删除的行
            rows_to_delete = list(self.execute_plan(child_plan))

            # 执行删除操作
            deleted_count = 0
            for row in rows_to_delete:
                self.storage_engine.delete_row(table_name, row)
                deleted_count += 1

            return f"{deleted_count} rows deleted"
        except Exception as e:
            raise SemanticError(f"删除数据错误: {str(e)}")

    def _is_type_compatible(self, actual_python_type: str, expected_sql_type: str) -> bool:
        """检查类型是否兼容"""
        # 首先清理 expected_sql_type，移除长度信息
        # 例如：VARCHAR(50) -> VARCHAR, INT -> INT
        import re

        # 清理期望的SQL类型（移除括号和长度信息）
        cleaned_expected_type = re.sub(r'\(.*\)', '', expected_sql_type).upper()

        # Python 类型到 SQL 类型的映射
        type_mapping = {
            'int': 'INT',
            'str': 'VARCHAR',
            'float': 'FLOAT',
            'bool': 'BOOLEAN'
        }

        # 将 Python 类型转换为对应的 SQL 类型
        actual_sql_type = type_mapping.get(actual_python_type.lower(), 'UNKNOWN')

        if actual_sql_type == "UNKNOWN" or cleaned_expected_type == "UNKNOWN":
            return True  # 未知类型暂时允许

        # 完全匹配
        if actual_sql_type == cleaned_expected_type:
            return True

        # VARCHAR 和 CHAR 兼容
        if (actual_sql_type == "VARCHAR" and cleaned_expected_type == "CHAR") or \
                (actual_sql_type == "CHAR" and cleaned_expected_type == "VARCHAR"):
            return True

        # 数值类型兼容
        numeric_types = {"INT", "NUMERIC", "FLOAT", "DOUBLE", "DECIMAL"}
        if actual_sql_type in numeric_types and cleaned_expected_type in numeric_types:
            return True

        # 字符串类型兼容（任何Python字符串都可以赋值给VARCHAR或CHAR）
        if actual_sql_type == "VARCHAR" and cleaned_expected_type in ["VARCHAR", "CHAR", "TEXT"]:
            return True

        return False

    def _evaluate_expression(self, expr: Any, row: Dict, context_table: str = None) -> Any:
        """计算表达式的值（考虑当前行的上下文）"""
        # 设置类型检查器的上下文表
        if context_table:
            self.type_checker.set_context_table(context_table)

        print(f"DEBUG: _evaluate_expression - expr: {expr}, row: {row}")

        # 如果表达式有 to_dict 方法，转换为字典形式
        if hasattr(expr, 'to_dict'):
            expr_dict = expr.to_dict()
            result = self._get_value_from_condition(row, expr_dict)
            print(f"DEBUG: _evaluate_expression result: {result}")
            return result

        # 如果是字典形式，直接使用
        elif isinstance(expr, dict):
            result = self._get_value_from_condition(row, expr)
            print(f"DEBUG: _evaluate_expression result: {result}")
            return result

        # 如果是基本数据类型，直接返回
        elif isinstance(expr, (int, float, str, bool)):
            print(f"DEBUG: _evaluate_expression result: {expr}")
            return expr

        # 其他情况尝试提取值
        else:
            result = self._extract_value(expr, context_table)
            print(f"DEBUG: _evaluate_expression result: {result}")
            return result

    def execute_order_by(self, order_columns: List[Tuple[str, str]], child_plan: Operator) -> List[Dict]:
        """执行排序操作"""
        try:
            # 先执行子计划获取数据
            child_results = list(self.execute_plan(child_plan))

            # 如果没有结果或没有排序条件，直接返回
            if not child_results or not order_columns:
                return child_results

            # 构建排序键函数
            def get_sort_key(row):
                sort_key = []
                for column, direction in order_columns:
                    # 处理可能的 table.column 格式
                    if '.' in column:
                        # 如果列名包含表名，尝试直接查找
                        if column in row:
                            value = row[column]
                        else:
                            # 尝试分割表名和列名
                            table_name, col_name = column.split('.', 1)
                            full_key = f"{table_name}.{col_name}"
                            value = row.get(full_key, row.get(col_name, None))
                    else:
                        value = row.get(column, None)

                    # 处理排序方向
                    sort_key.append((value, direction.lower() == 'desc'))
                return sort_key

            # 排序函数
            def sort_rows(row):
                sort_key = get_sort_key(row)
                key_values = []
                for value, reverse in sort_key:
                    # 处理None值，将其放在最后（无论升序降序）
                    if value is None:
                        # 对于降序，None值应该在最前面，但我们统一放在最后
                        key_value = (0 if reverse else 1, '')  # 使用特殊值确保None在最后
                    else:
                        # 对于降序，使用负值或反转比较
                        if reverse:
                            # 对于数字，使用负值；对于其他类型，使用特殊处理
                            if isinstance(value, (int, float)):
                                key_value = (-value, '')
                            else:
                                key_value = (1, str(value))  # 降序时字符串排序需要特殊处理
                        else:
                            key_value = (0, value) if isinstance(value, (int, float)) else (1, str(value))
                    key_values.append(key_value)
                return tuple(key_values)

            # 执行排序
            sorted_results = sorted(child_results, key=sort_rows)

            return sorted_results

        except Exception as e:
            raise SemanticError(f"排序操作错误: {str(e)}")

    def execute_group_by(self, group_columns: List[str], having_condition: Optional[Any],
                         child_plan: Operator, aggregate_functions: List[tuple]) -> List[Dict]:
        """执行分组操作"""
        try:
            # 先执行子计划获取数据
            child_results = list(self.execute_plan(child_plan))
            print(f"DEBUG: GroupBy - Child results: {child_results}")
            print(f"DEBUG: GroupBy - Group columns: {group_columns}")
            print(f"DEBUG: GroupBy - Aggregate functions: {aggregate_functions}")

            # 如果没有聚合函数但有GROUP BY，需要添加COUNT(*)
            if not aggregate_functions and group_columns:
                aggregate_functions = [('COUNT', '*')]
                print(f"DEBUG: GroupBy - Added default COUNT(*) aggregation")

            # 分组操作
            groups = {}
            for row in child_results:
                # 构建分组键
                group_key_values = []
                for col in group_columns:
                    group_key_values.append(row.get(col, None))

                group_key = tuple(group_key_values)
                print(f"DEBUG: GroupBy - Row: {row}, Group key: {group_key}")

                if group_key not in groups:
                    groups[group_key] = []
                groups[group_key].append(row)

            print(f"DEBUG: GroupBy - Groups formed: {groups}")

            # 应用聚合函数并构建结果
            result_rows = []
            for group_key, group_rows in groups.items():
                print(f"DEBUG: GroupBy - Processing group: {group_key}, rows: {len(group_rows)}")

                # 构建分组结果行
                result_row = {}

                # 添加分组列
                for i, col in enumerate(group_columns):
                    result_row[col] = group_key[i]

                # 计算所有聚合函数
                for func_name, column_name in aggregate_functions:
                    print(f"DEBUG: GroupBy - Calculating {func_name}({column_name})")

                    # 在 execute_group_by 方法中，修改聚合列名的创建方式
                    if func_name.upper() == 'COUNT':
                        if column_name == '*':
                            # COUNT(*) - 计算所有行数
                            result_row["COUNT(*)"] = len(group_rows)  # 使用 "COUNT(*)" 而不是 "COUNT(*)"
                            print(f"DEBUG: GroupBy - COUNT(*): {len(group_rows)}")
                        else:
                            # COUNT(column) - 计算非空值数量
                            values = [row.get(column_name) for row in group_rows if row.get(column_name) is not None]
                            result_row[f"COUNT({column_name})"] = len(values)  # 使用 "COUNT(column)" 格式
                            print(f"DEBUG: GroupBy - COUNT({column_name}): {len(values)}")

                    elif func_name.upper() == 'SUM':
                        # 计算总和
                        values = [row.get(column_name) for row in group_rows
                                  if
                                  row.get(column_name) is not None and isinstance(row.get(column_name), (int, float))]
                        sum_value = sum(values) if values else 0
                        result_row[f"SUM({column_name})"] = sum_value
                        print(f"DEBUG: GroupBy - SUM({column_name}): {sum_value}")

                    elif func_name.upper() == 'AVG':
                        # 计算平均值
                        values = [row.get(column_name) for row in group_rows
                                  if
                                  row.get(column_name) is not None and isinstance(row.get(column_name), (int, float))]
                        print(f"DEBUG: GroupBy - AVG values: {values}")
                        if values:
                            avg_value = sum(values) / len(values)
                            result_row[f"AVG({column_name})"] = avg_value
                            print(f"DEBUG: GroupBy - AVG({column_name}): {avg_value}")
                        else:
                            result_row[f"AVG({column_name})"] = None
                            print(f"DEBUG: GroupBy - AVG({column_name}): None")

                    elif func_name.upper() == 'MAX':
                        # 计算最大值
                        values = [row.get(column_name) for row in group_rows
                                  if
                                  row.get(column_name) is not None and isinstance(row.get(column_name), (int, float))]
                        max_value = max(values) if values else None
                        result_row[f"MAX({column_name})"] = max_value
                        print(f"DEBUG: GroupBy - MAX({column_name}): {max_value}")

                    elif func_name.upper() == 'MIN':
                        # 计算最小值
                        values = [row.get(column_name) for row in group_rows
                                  if
                                  row.get(column_name) is not None and isinstance(row.get(column_name), (int, float))]
                        min_value = min(values) if values else None
                        result_row[f"MIN({column_name})"] = min_value
                        print(f"DEBUG: GroupBy - MIN({column_name}): {min_value}")

                print(f"DEBUG: GroupBy - Result row before HAVING: {result_row}")

                # 应用 HAVING 条件（如果有）
                if having_condition:
                    condition_result = self.evaluate_condition(result_row, having_condition)
                    print(f"DEBUG: GroupBy - HAVING condition result: {condition_result}")

                    if not condition_result:
                        print(f"DEBUG: GroupBy - Group filtered out by HAVING")
                        continue  # 跳过不满足 HAVING 条件的分组

                result_rows.append(result_row)
                print(f"DEBUG: GroupBy - Added result row: {result_row}")

            print(f"DEBUG: GroupBy - Final result rows: {result_rows}")
            return result_rows

        except Exception as e:
            print(f"DEBUG: GroupBy - Error: {e}")
            raise SemanticError(f"分组操作错误: {str(e)}")

    def _evaluate_subquery_expression(self, row: Dict, subquery_expr: Dict) -> Any:
        """评估子查询表达式"""
        try:
            # 获取子查询的SELECT语句
            select_stmt_dict = subquery_expr.get('select_stmt', {})

            # 需要将字典形式的SELECT语句转换回AST节点
            from sql_compiler.parser.ast_nodes import SelectStmt, TableRef, IdentifierExpr, LiteralExpr, BinaryExpr
            from sql_compiler.codegen.plan_generator import PlanGenerator

            # 构建SELECT语句AST节点
            columns = select_stmt_dict.get('columns', [])
            from_clause_dict = select_stmt_dict.get('from_clause', {})
            where_clause_dict = select_stmt_dict.get('where_clause', {})

            # 构建FROM子句
            if from_clause_dict.get('type') == 'TableRef':
                from_clause = TableRef(
                    from_clause_dict.get('table_name'),
                    from_clause_dict.get('alias')
                )
            else:
                # 简化处理，只处理单表
                from_clause = TableRef(from_clause_dict.get('table_name', ''))

            # 构建WHERE子句
            where_clause = None
            if where_clause_dict:
                if where_clause_dict.get('type') == 'BinaryExpr':
                    left_dict = where_clause_dict.get('left', {})
                    right_dict = where_clause_dict.get('right', {})

                    left_expr = IdentifierExpr(left_dict.get('name', '')) if left_dict.get(
                        'type') == 'IdentifierExpr' else None
                    right_expr = LiteralExpr(right_dict.get('value')) if right_dict.get(
                        'type') == 'LiteralExpr' else None

                    if left_expr and right_expr:
                        where_clause = BinaryExpr(
                            left_expr,
                            where_clause_dict.get('operator', '='),
                            right_expr
                        )

            # 创建SELECT语句
            select_stmt = SelectStmt(columns, from_clause)
            select_stmt.where_clause = where_clause

            # 生成执行计划并执行子查询
            plan_generator = PlanGenerator(enable_optimization=False, silent_mode=True)
            subquery_plan = plan_generator.generate(select_stmt)


            # 执行子查询
            subquery_results = list(self.execute_plan(subquery_plan))

            # 提取子查询结果的第一列值
            result_values = []
            for result_row in subquery_results:
                # 获取第一列的值（假设子查询只返回一列）
                if result_row:
                    first_key = list(result_row.keys())[0]
                    result_values.append(result_row[first_key])

            return result_values

        except Exception as e:
            print(f"DEBUG: Error evaluating subquery: {e}")
            return []

    def execute_filtered_seq_scan(self, table_name: str, condition: Any) -> List[Dict]:
        """执行带过滤条件的顺序扫描（谓词下推优化）"""
        try:
            # 获取所有行数据
            all_rows = self.storage_engine.get_all_rows(table_name)

            # 应用过滤条件
            filtered_rows = []
            for row in all_rows:
                if self.evaluate_condition(row, condition):
                    filtered_rows.append(row)

            return filtered_rows
        except Exception as e:
            raise SemanticError(f"扫描表 {table_name} 错误: {str(e)}")

    def execute_join(self, join_type: str, on_condition: Any, children: List[Operator]) -> List[Dict]:
        """执行JOIN操作"""
        try:
            # 先执行左右子计划
            left_results = list(self.execute_plan(children[0]))
            right_results = list(self.execute_plan(children[1]))

            print(f"DEBUG: JOIN - Left results columns: {list(left_results[0].keys()) if left_results else 'None'}")
            print(f"DEBUG: JOIN - Right results columns: {list(right_results[0].keys()) if right_results else 'None'}")

            # 获取左右表的真实别名（从操作符中获取）
            left_alias = getattr(children[0], 'table_alias', None) or "left_table"
            right_alias = getattr(children[1], 'table_alias', None) or "right_table"

            print(f"DEBUG: JOIN - Left alias: {left_alias}, Right alias: {right_alias}")

            joined_results = []

            # 处理不同的JOIN类型
            if join_type.upper() == 'INNER':
                for left_row in left_results:
                    for right_row in right_results:
                        # 创建合并的行，使用完整的限定列名
                        merged_row = {}

                        # 添加左表列，加上表别名前缀
                        for key, value in left_row.items():
                            merged_row[f"{left_alias}.{key}"] = value

                        # 添加右表列，加上表别名前缀
                        for key, value in right_row.items():
                            merged_row[f"{right_alias}.{key}"] = value

                        # 评估ON条件
                        if self.evaluate_condition(merged_row, on_condition):
                            joined_results.append(merged_row)

            elif join_type.upper() == 'LEFT':
                for left_row in left_results:
                    matched = False
                    for right_row in right_results:
                        # 创建合并的行，使用完整的限定列名
                        merged_row = {}

                        # 添加左表列，加上表别名前缀
                        for key, value in left_row.items():
                            merged_row[f"{left_alias}.{key}"] = value

                        # 添加右表列，加上表别名前缀
                        for key, value in right_row.items():
                            merged_row[f"{right_alias}.{key}"] = value

                        if self.evaluate_condition(merged_row, on_condition):
                            joined_results.append(merged_row)
                            matched = True

                    # 如果没有匹配，添加左表行，右表列为NULL
                    if not matched:
                        null_row = {}
                        # 添加左表列
                        for key, value in left_row.items():
                            null_row[f"{left_alias}.{key}"] = value
                        # 添加右表NULL值
                        for key in right_results[0].keys():
                            null_row[f"{right_alias}.{key}"] = None
                        joined_results.append(null_row)

            elif join_type.upper() == 'RIGHT':
                for right_row in right_results:
                    matched = False
                    for left_row in left_results:
                        # 创建合并的行，使用完整的限定列名
                        merged_row = {}

                        # 添加左表列，加上表别名前缀
                        for key, value in left_row.items():
                            merged_row[f"{left_alias}.{key}"] = value

                        # 添加右表列，加上表别名前缀
                        for key, value in right_row.items():
                            merged_row[f"{right_alias}.{key}"] = value

                        if self.evaluate_condition(merged_row, on_condition):
                            joined_results.append(merged_row)
                            matched = True

                    # 如果没有匹配，添加右表行，左表列为NULL
                    if not matched:
                        null_row = {}
                        # 添加左表NULL值
                        for key in left_results[0].keys():
                            null_row[f"{left_alias}.{key}"] = None
                        # 添加右表列
                        for key, value in right_row.items():
                            null_row[f"{right_alias}.{key}"] = value
                        joined_results.append(null_row)

            else:
                raise SemanticError(f"不支持的JOIN类型: {join_type}")

            print(f"DEBUG: JOIN - Result: {len(joined_results)} rows")
            if joined_results:
                print(f"DEBUG: JOIN - First result row keys: {list(joined_results[0].keys())}")
            return joined_results

        except Exception as e:
            raise SemanticError(f"JOIN操作错误: {str(e)}")

    def execute_index_scan(self, table_name: str, index_name: str, scan_condition: Any) -> List[Dict]:
        """执行索引扫描"""
        try:
            # 从条件中提取键值（简化处理，仅支持等值查询）
            key = self._extract_key_from_condition(scan_condition)
            if key is None:
                raise SemanticError("Unsupported index condition")

            # 调用存储引擎的索引查询方法
            rows = self.storage_engine.get_rows_by_index(table_name, index_name, key)
            return rows
        except Exception as e:
            raise SemanticError(f"Index scan error: {str(e)}")

    def _extract_key_from_condition(self, condition: Any) -> Any:
        """从条件中提取等值查询的键值"""
        if hasattr(condition, 'to_dict'):
            cond_dict = condition.to_dict()
        else:
            cond_dict = condition

        if cond_dict.get('type') == 'BinaryExpr' and cond_dict.get('operator') == '=':
            left = cond_dict.get('left', {})
            right = cond_dict.get('right', {})
            if left.get('type') == 'IdentifierExpr' and right.get('type') == 'LiteralExpr':
                return right.get('value')
        return None

    def execute_create_index(self, index_name: str, table_name: str, columns: List[str], unique: bool,
                             index_type: str) -> str:
        """执行创建索引操作"""
        try:
            # 首先检查表是否存在
            if not self.catalog.table_exists(table_name):
                raise SemanticError(f"Table '{table_name}' does not exist")

            # 检查列是否存在 - 通过获取表信息来验证列
            table_info = self.catalog.get_table(table_name)
            if table_info is None:
                raise SemanticError(f"Table '{table_name}' does not exist")

            # 获取表的列名列表
            table_columns = [col['name'] for col in table_info['columns']]
            for column in columns:
                if column not in table_columns:
                    raise SemanticError(f"Column '{column}' does not exist in table '{table_name}'")

            # 调用存储引擎创建索引
            success = self.storage_engine.create_index(table_name, index_name, columns[0])  # 仅支持单列索引
            if not success:
                raise SemanticError("Failed to create index")

            # 更新系统目录
            self.catalog.create_index(index_name, table_name, columns, unique, index_type)

            return f"Index '{index_name}' created successfully"
        except Exception as e:
            raise SemanticError(f"Create index error: {str(e)}")

    def _extract_index_range(self, condition: Any, index_columns: List[str]) -> Tuple[Any, Any]:
        """从条件中提取索引键范围"""
        # 简化实现：假设是等值查询或范围查询
        # 实际应根据条件表达式解析出范围
        if hasattr(condition, 'to_dict'):
            condition_dict = condition.to_dict()
            if condition_dict.get('type') == 'BinaryExpr':
                left = condition_dict.get('left', {})
                right = condition_dict.get('right', {})
                operator = condition_dict.get('operator', '')

                if (left.get('type') == 'IdentifierExpr' and
                        left.get('name') in index_columns and
                        right.get('type') == 'LiteralExpr'):

                    value = right.get('value')
                    if operator == '=':
                        return value, value
                    elif operator == '>':
                        return value, None
                    elif operator == '>=':
                        return value, None
                    elif operator == '<':
                        return None, value
                    elif operator == '<=':
                        return None, value
                    elif operator == 'BETWEEN':
                        # 处理BETWEEN操作符
                        if (isinstance(value, list) and len(value) == 2):
                            return value[0], value[1]

        # 默认返回全范围
        return None, None

    def execute_drop_index(self, index_name: str) -> str:
        """执行删除索引操作"""
        try:
            # 从 catalog 获取所有索引信息
            all_indexes = self.catalog.get_all_indexes()

            if index_name not in all_indexes:
                return f"Index '{index_name}' does not exist"

            # 获取索引信息
            index_info = all_indexes[index_name]
            table_name = index_info["table"]

            # 调用存储引擎删除索引
            success = self.storage_engine.drop_index(table_name, index_name)
            if not success:
                raise SemanticError(f"Failed to drop index '{index_name}'")

            # 更新 catalog
            self.catalog.drop_index(index_name)

            return f"Index '{index_name}' dropped successfully"
        except Exception as e:
            raise SemanticError(f"Drop index error: {str(e)}")

    # 在计划生成器中添加视图识别逻辑
    def _visit_table_ref(self, node: TableRef):
        table_name = node.table_name

        # 检查是否是视图
        if table_name in self.execution_engine.views:
            view_info = self.execution_engine.views[table_name]
            if 'plan' in view_info:
                # 返回视图扫描操作符
                return ViewScanOp(table_name, view_info['plan'])

        # 普通表，返回顺序扫描
        return SeqScanOp(table_name)

    def execute_show_indexes(self, table_name: Optional[str] = None) -> List[Dict]:
        """执行SHOW INDEXES语句"""
        try:
            # 从catalog获取索引信息
            if table_name:
                # 获取指定表的索引
                indexes = self.catalog.get_table_indexes(table_name)  # 使用正确的方法名
                return [{
                    "Table": table_name,
                    "Index_name": idx["name"],  # 使用正确的键名
                    "Columns": ', '.join(idx["columns"]),
                    "Unique": idx["unique"],  # 使用正确的键名
                    "Index_type": idx["type"]  # 使用正确的键名
                } for idx in indexes]
            else:
                # 获取所有索引
                all_indexes = self.catalog.get_all_indexes()  # 使用正确的方法名
                result = []
                for idx_name, idx_info in all_indexes.items():
                    result.append({
                        "Table": idx_info["table"],
                        "Index_name": idx_name,  # 直接使用索引名
                        "Columns": ', '.join(idx_info["columns"]),
                        "Unique": idx_info["unique"],
                        "Index_type": idx_info["type"]
                    })
                return result
        except Exception as e:
            raise SemanticError(f"显示索引错误: {str(e)}")