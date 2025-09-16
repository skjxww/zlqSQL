from sql_compiler.codegen.operators import *
from sql_compiler.exceptions.compiler_errors import SemanticError

# 导入高级优化器
try:
    from sql_compiler.optimizer.advanced_optimizer import AdvancedQueryOptimizer, QueryOptimizationPipeline

    ADVANCED_OPTIMIZER_AVAILABLE = True
except ImportError:
    try:
        from sql_compiler.optimizer.simple_optimizer import SimpleQueryOptimizer

        ADVANCED_OPTIMIZER_AVAILABLE = False
    except ImportError:
        ADVANCED_OPTIMIZER_AVAILABLE = False


class PlanGenerator:
    """执行计划生成器 - 支持高级优化和别名处理"""

    def __init__(self, enable_optimization=True, silent_mode=False, catalog_manager=None):
        """初始化计划生成器"""
        self.enable_optimization = enable_optimization and ADVANCED_OPTIMIZER_AVAILABLE
        self.silent_mode = silent_mode
        self.catalog_manager = catalog_manager

        # 添加别名追踪
        self.table_aliases = {}  # 别名 -> 真实表名
        self.real_to_alias = {}  # 真实表名 -> 别名

        if self.enable_optimization:
            if ADVANCED_OPTIMIZER_AVAILABLE:
                # 使用高级优化流水线
                self.optimization_pipeline = QueryOptimizationPipeline(catalog_manager)
                self.optimizer = self.optimization_pipeline.optimizer
                self.optimizer.silent_mode = silent_mode
            else:
                # 回退到简单优化器
                from sql_compiler.optimizer.simple_optimizer import SimpleQueryOptimizer
                self.optimizer = SimpleQueryOptimizer(silent_mode)
        else:
            self.optimizer = None

    def generate(self, stmt: Statement) -> Operator:
        """生成执行计划"""
        # 重置别名映射
        self._reset_alias_mappings()

        # 预处理：收集别名信息
        if isinstance(stmt, SelectStmt):
            self._collect_aliases_from_select(stmt)

        # 生成执行计划
        if isinstance(stmt, BeginTransactionStmt):
            return self._generate_begin_transaction_plan(stmt)
        elif isinstance(stmt, CommitStmt):
            return self._generate_commit_plan(stmt)
        elif isinstance(stmt, RollbackStmt):
            return self._generate_rollback_plan(stmt)
        elif isinstance(stmt, SavepointStmt):
            return self._generate_savepoint_plan(stmt)
        elif isinstance(stmt, ReleaseSavepointStmt):
            return self._generate_release_savepoint_plan(stmt)
        elif isinstance(stmt, CreateTableStmt):
            plan = self._generate_create_table_plan(stmt)
        elif isinstance(stmt, InsertStmt):
            plan = self._generate_insert_plan(stmt)
        elif isinstance(stmt, SelectStmt):
            plan = self._generate_select_plan(stmt)
        elif isinstance(stmt, UpdateStmt):
            plan = self._generate_update_plan(stmt)
        elif isinstance(stmt, DeleteStmt):
            plan = self._generate_delete_plan(stmt)
        elif isinstance(stmt, CreateIndexStmt):
            return self._generate_create_index_plan(stmt)
        elif isinstance(stmt, DropIndexStmt):
            return self._generate_drop_index_plan(stmt)
        elif isinstance(stmt, ShowIndexesStmt):
            return self._generate_show_indexes_plan(stmt)
        elif isinstance(stmt, CreateViewStmt):
            return self._generate_create_view_plan(stmt)
        elif isinstance(stmt, DropViewStmt):
            return self._generate_drop_view_plan(stmt)
        elif isinstance(stmt, ShowViewsStmt):
            return self._generate_show_views_plan(stmt)
        elif isinstance(stmt, DescribeViewStmt):
            return self._generate_describe_view_plan(stmt)
        else:
            raise SemanticError(f"不支持的语句类型: {type(stmt).__name__}")

        # 只对SELECT语句应用优化
        if (self.optimizer and
                self.enable_optimization and
                isinstance(stmt, SelectStmt)):
            try:
                if hasattr(self, 'optimization_pipeline'):
                    # 使用高级优化流水线
                    query_context = {
                        'statement_type': 'SELECT',
                        'table_count': len(self._extract_tables_from_stmt(stmt)),
                        'has_joins': self._has_joins(stmt),
                        'has_aggregation': self._has_aggregation(stmt),
                        'has_subqueries': self._has_subqueries(stmt),
                        'table_aliases': self.table_aliases.copy()
                    }
                    optimized_plan = self.optimization_pipeline.optimize(plan, query_context)
                else:
                    # 使用简单优化器
                    optimized_plan = self.optimizer.optimize(plan)

                return optimized_plan

            except Exception as e:
                if not self.silent_mode:
                    print(f"⚠️ 查询优化失败: {e}，使用原始计划")

        return plan

    def _generate_begin_transaction_plan(self, stmt: BeginTransactionStmt) -> BeginTransactionOp:
        """生成开始事务的执行计划"""
        if not self.silent_mode:
            print(f"   🔄 生成BEGIN TRANSACTION计划")
            if stmt.isolation_level:
                print(f"     隔离级别: {stmt.isolation_level.value}")
            if stmt.transaction_mode:
                print(f"     事务模式: {stmt.transaction_mode.value}")

        return BeginTransactionOp(
            isolation_level=stmt.isolation_level,
            transaction_mode=stmt.transaction_mode
        )

    def _generate_commit_plan(self, stmt: CommitStmt) -> CommitTransactionOp:
        """生成提交事务的执行计划"""
        if not self.silent_mode:
            print(f"   ✅ 生成COMMIT计划")
            if stmt.work:
                print(f"     包含WORK关键字")

        return CommitTransactionOp(work=stmt.work)

    def _generate_rollback_plan(self, stmt: RollbackStmt) -> RollbackTransactionOp:
        """生成回滚事务的执行计划"""
        if not self.silent_mode:
            print(f"   ↩️ 生成ROLLBACK计划")
            if stmt.work:
                print(f"     包含WORK关键字")
            if stmt.to_savepoint:
                print(f"     回滚到保存点: {stmt.to_savepoint}")

        return RollbackTransactionOp(
            work=stmt.work,
            to_savepoint=stmt.to_savepoint
        )

    def _generate_savepoint_plan(self, stmt: SavepointStmt) -> SavepointOp:
        """生成保存点的执行计划"""
        if not self.silent_mode:
            print(f"   💾 生成SAVEPOINT计划: {stmt.savepoint_name}")

        return SavepointOp(savepoint_name=stmt.savepoint_name)

    def _generate_release_savepoint_plan(self, stmt: ReleaseSavepointStmt) -> ReleaseSavepointOp:
        """生成释放保存点的执行计划"""
        if not self.silent_mode:
            print(f"   🗑️ 生成RELEASE SAVEPOINT计划: {stmt.savepoint_name}")

        return ReleaseSavepointOp(savepoint_name=stmt.savepoint_name)

    def _generate_insert_plan(self, stmt: InsertStmt) -> InsertOp:
        """生成插入计划 - 支持事务"""
        if not self.silent_mode:
            print(f"   ➕ 生成INSERT计划: {stmt.table_name}")
            if hasattr(stmt, 'transaction_id') and stmt.transaction_id:
                print(f"     事务ID: {stmt.transaction_id}")

        # 验证表是否存在
        if not self.catalog_manager.table_exists(stmt.table_name):
            raise ValueError(f"表不存在: {stmt.table_name}")

        # 创建插入操作符
        insert_op = InsertOp(stmt.table_name,stmt.columns, stmt.values)

        # 设置事务上下文（如果语句包含事务信息）
        if hasattr(stmt, 'transaction_id'):
            insert_op.set_transaction_context(stmt.transaction_id)

        return insert_op

    def _generate_create_index_plan(self, stmt: CreateIndexStmt) -> CreateIndexOp:
        """生成创建索引计划"""
        return CreateIndexOp(
            stmt.index_name,
            stmt.table_name,
            stmt.columns,
            stmt.unique,
            stmt.index_type
        )

    def _generate_drop_index_plan(self, stmt: DropIndexStmt) -> 'DropIndexOp':
        """生成删除索引计划"""
        return DropIndexOp(stmt.index_name)

    def _generate_show_indexes_plan(self, stmt: ShowIndexesStmt) -> 'ShowIndexesOp':
        """生成显示索引计划"""
        return ShowIndexesOp(stmt.table_name)

    def _reset_alias_mappings(self):
        """重置别名映射"""
        self.table_aliases = {}
        self.real_to_alias = {}

    def _collect_aliases_from_select(self, stmt: SelectStmt):
        """从SELECT语句中收集别名信息"""
        if stmt.from_clause:
            self._collect_aliases_from_from_clause(stmt.from_clause)

    def _collect_aliases_from_from_clause(self, from_clause: FromClause):
        """从FROM子句中收集别名"""
        if isinstance(from_clause, TableRef):
            real_name = from_clause.table_name
            alias = from_clause.alias

            if alias:
                self.table_aliases[alias] = real_name
                self.real_to_alias[real_name] = alias

        elif isinstance(from_clause, JoinExpr):
            # 递归收集左右两边的别名
            self._collect_aliases_from_from_clause(from_clause.left)
            self._collect_aliases_from_from_clause(from_clause.right)

    def _generate_create_table_plan(self, stmt: CreateTableStmt) -> Operator:
        """生成CREATE TABLE执行计划"""
        return CreateTableOp(stmt.table_name, stmt.columns)

    def _set_transaction_context_for_plan(self, plan: Operator, transaction_id: Optional[str]):
        """为执行计划树设置事务上下文"""
        if isinstance(plan, TransactionAwareOp):
            plan.set_transaction_context(transaction_id)

        # 递归处理子节点
        for child in plan.children:
            self._set_transaction_context_for_plan(child, transaction_id)

    def _generate_select_plan(self, stmt: SelectStmt) -> Operator:
        """生成SELECT执行计划"""
        if not self.silent_mode:
            print(f"   📋 生成SELECT计划")
            # 修复：检查属性是否存在
            if hasattr(stmt, 'transaction_id'):
                print(f"     事务ID: {stmt.transaction_id}")

        # 生成基本的查询计划
        plan = self._generate_basic_select_plan(stmt)

        # 修复：为计划树中的所有事务感知操作符设置事务上下文
        if hasattr(stmt, 'transaction_id'):
            self._set_transaction_context_for_plan(plan, stmt.transaction_id)

        return plan

    def _generate_basic_select_plan(self, stmt: SelectStmt) -> Operator:
        """生成SELECT执行计划 - 修复聚合函数传递"""

        if not self.silent_mode:
            print(f"\n🔧 生成SELECT执行计划")
            print(f"   选择列: {stmt.columns}")
            print(f"   GROUP BY: {stmt.group_by}")
            print(f"   HAVING: {'有' if stmt.having_clause else '无'}")

        # 从FROM子句开始构建计划
        plan = self._generate_from_plan(stmt.from_clause)

        # 添加WHERE过滤
        if stmt.where_clause:
            plan = FilterOp(stmt.where_clause, [plan])
            if not self.silent_mode:
                print(f"   ✅ 添加WHERE过滤")

        # 添加GROUP BY（包含HAVING条件和聚合函数）
        if stmt.group_by and len(stmt.group_by) > 0:
            # 🔑 解析聚合函数
            aggregate_functions = self._extract_aggregate_functions(stmt.columns)

            # 创建GroupByOp，传递聚合函数
            plan = GroupByOp(
                group_columns=stmt.group_by,
                having_condition=stmt.having_clause,
                children=[plan],
                aggregate_functions=aggregate_functions  # 🔑 传递聚合函数
            )

            if not self.silent_mode:
                print(f"   ✅ 添加GROUP BY，分组列: {stmt.group_by}")
                print(f"   ✅ 聚合函数: {aggregate_functions}")
                if stmt.having_clause:
                    print(f"   ✅ 包含HAVING条件")

        # 添加投影
        if stmt.columns != ["*"]:
            plan = ProjectOp(stmt.columns, [plan])
            if not self.silent_mode:
                print(f"   ✅ 添加投影，列: {stmt.columns}")

        # 添加ORDER BY
        if stmt.order_by:
            plan = OrderByOp(stmt.order_by, [plan])
            if not self.silent_mode:
                print(f"   ✅ 添加ORDER BY")

        if not self.silent_mode:
            print(f"   🎯 最终计划: {type(plan).__name__}")

        return plan

    def _extract_aggregate_functions(self, columns: List[str]) -> List[tuple]:
        """从选择列中提取聚合函数"""
        aggregate_functions = []

        for column in columns:
            if isinstance(column, str):
                # 检查是否是聚合函数
                column_upper = column.upper()

                if 'COUNT(' in column_upper:
                    if 'COUNT(*)' in column_upper:
                        aggregate_functions.append(('COUNT', '*'))
                    else:
                        # 提取列名 COUNT(column_name)
                        start = column_upper.find('COUNT(') + 6
                        end = column_upper.find(')', start)
                        if end > start:
                            col_name = column[start:end].strip()
                            aggregate_functions.append(('COUNT', col_name))

                elif 'SUM(' in column_upper:
                    start = column_upper.find('SUM(') + 4
                    end = column_upper.find(')', start)
                    if end > start:
                        col_name = column[start:end].strip()
                        aggregate_functions.append(('SUM', col_name))

                elif 'AVG(' in column_upper:
                    start = column_upper.find('AVG(') + 4
                    end = column_upper.find(')', start)
                    if end > start:
                        col_name = column[start:end].strip()
                        aggregate_functions.append(('AVG', col_name))

                elif 'MAX(' in column_upper:
                    start = column_upper.find('MAX(') + 4
                    end = column_upper.find(')', start)
                    if end > start:
                        col_name = column[start:end].strip()
                        aggregate_functions.append(('MAX', col_name))

                elif 'MIN(' in column_upper:
                    start = column_upper.find('MIN(') + 4
                    end = column_upper.find(')', start)
                    if end > start:
                        col_name = column[start:end].strip()
                        aggregate_functions.append(('MIN', col_name))

        return aggregate_functions

    def _generate_update_plan(self, stmt: UpdateStmt) -> UpdateOp:
        """生成更新计划 - 支持事务"""

        # 先扫描表
        scan_plan = SeqScanOp(stmt.table_name)

        # 如果有WHERE条件，添加过滤
        if stmt.where_clause:
            scan_plan = FilterOp(stmt.where_clause, [scan_plan])

        scan_plan = SeqScanOp(stmt.table_name)
        # 创建更新操作符
        update_op = UpdateOp(
            stmt.table_name,
            stmt.assignments,
            [scan_plan]
        )

        # 设置事务上下文
        if hasattr(stmt, 'transaction_id'):
            update_op.set_transaction_context(stmt.transaction_id)

        return update_op

    def _generate_delete_plan(self, stmt: DeleteStmt) -> DeleteOp:
        """生成删除计划 - 支持事务"""
        # 先扫描表
        scan_plan = SeqScanOp(stmt.table_name)

        # 如果有WHERE条件，添加过滤
        if stmt.where_clause:
            scan_plan = FilterOp(stmt.where_clause, [scan_plan])

        # 创建删除操作符
        delete_op = DeleteOp(stmt.table_name, [scan_plan])

        # 设置事务上下文
        if hasattr(stmt, 'transaction_id'):
            delete_op.set_transaction_context(stmt.transaction_id)

        return delete_op

    # 在 PlanGenerator 类中添加以下缺失的方法：

    def _extract_condition_columns(self, condition) -> List[str]:
        """从条件表达式中提取涉及的列名"""
        if not condition:
            return []

        columns = []

        try:
            # 如果是二元表达式（如 a = b, a > 10）
            if hasattr(condition, 'left') and hasattr(condition, 'right'):
                # 处理左操作数
                if hasattr(condition.left, 'name'):
                    columns.append(condition.left.name)
                elif hasattr(condition.left, 'column_name'):
                    columns.append(condition.left.column_name)

                # 处理右操作数
                if hasattr(condition.right, 'name'):
                    columns.append(condition.right.name)
                elif hasattr(condition.right, 'column_name'):
                    columns.append(condition.right.column_name)

            # 如果是单个列引用
            elif hasattr(condition, 'name'):
                columns.append(condition.name)
            elif hasattr(condition, 'column_name'):
                columns.append(condition.column_name)

            # 如果是复合条件（AND, OR）
            elif hasattr(condition, 'conditions'):
                for sub_condition in condition.conditions:
                    columns.extend(self._extract_condition_columns(sub_condition))

        except Exception as e:
            if not self.silent_mode:
                print(f"⚠️ 提取条件列失败: {e}")

        return list(set(columns))  # 去重

    def _optimize_with_indexes(self, plan: Operator, stmt: SelectStmt) -> Operator:
        """使用索引优化查询计划"""
        try:
            if not isinstance(stmt.from_clause, TableRef):
                return plan

            table_name = stmt.from_clause.table_name

            # 分析WHERE条件中的列
            condition_columns = self._extract_condition_columns(stmt.where_clause)

            if not condition_columns:
                return plan

            # 寻找最佳索引
            if hasattr(self.catalog_manager, 'find_best_index'):
                best_index = self.catalog_manager.find_best_index(table_name, condition_columns)

                if best_index:
                    if not self.silent_mode:
                        print(f"   🔍 使用索引优化: {best_index}")
                    # 用索引扫描替换表扫描
                    return BTreeIndexScanOp(table_name, best_index, stmt.where_clause)

            return plan

        except Exception as e:
            if not self.silent_mode:
                print(f"⚠️ 索引优化失败: {e}")
            return plan

    def _extract_tables_from_stmt(self, stmt: SelectStmt) -> List[str]:
        """从SELECT语句中提取表名"""
        tables = []

        try:
            if hasattr(stmt, 'from_clause') and stmt.from_clause:
                tables.extend(self._extract_tables_from_from_clause(stmt.from_clause))
        except Exception as e:
            if not self.silent_mode:
                print(f"⚠️ 提取表名失败: {e}")

        return tables

    def _extract_tables_from_from_clause(self, from_clause) -> List[str]:
        """从FROM子句中提取表名"""
        tables = []

        try:
            if isinstance(from_clause, TableRef):
                tables.append(from_clause.table_name)

            elif isinstance(from_clause, JoinExpr):
                # 递归提取左右两边的表名
                tables.extend(self._extract_tables_from_from_clause(from_clause.left))
                tables.extend(self._extract_tables_from_from_clause(from_clause.right))

            # 处理其他类型的FROM子句
            elif hasattr(from_clause, 'table_name'):
                tables.append(from_clause.table_name)

        except Exception as e:
            if not self.silent_mode:
                print(f"⚠️ 从FROM子句提取表名失败: {e}")

        return tables

    def _has_joins(self, stmt: SelectStmt) -> bool:
        """检查是否包含JOIN"""
        try:
            if hasattr(stmt, 'from_clause') and stmt.from_clause:
                return self._check_joins_in_from_clause(stmt.from_clause)
            return False
        except Exception:
            return False

    def _check_joins_in_from_clause(self, from_clause) -> bool:
        """检查FROM子句中是否包含JOIN"""
        try:
            if isinstance(from_clause, JoinExpr):
                return True
            elif hasattr(from_clause, 'join_type'):
                return True
            else:
                return False
        except Exception:
            return False

    def _has_aggregation(self, stmt: SelectStmt) -> bool:
        """检查是否包含聚合"""
        try:
            # 检查GROUP BY
            if hasattr(stmt, 'group_by') and stmt.group_by:
                return True

            # 检查SELECT列中是否有聚合函数
            if hasattr(stmt, 'columns'):
                for column in stmt.columns:
                    if isinstance(column, str):
                        column_upper = column.upper()
                        if any(func in column_upper for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']):
                            return True

            return False
        except Exception:
            return False

    def _has_subqueries(self, stmt: SelectStmt) -> bool:
        """检查是否包含子查询"""
        try:
            # 简化实现：检查WHERE子句中的子查询
            if hasattr(stmt, 'where_clause') and stmt.where_clause:
                return self._check_subqueries_in_expression(stmt.where_clause)

            # 检查SELECT列表中的子查询
            if hasattr(stmt, 'columns'):
                for column in stmt.columns:
                    if hasattr(column, 'subquery') or (isinstance(column, str) and 'SELECT' in column.upper()):
                        return True

            return False
        except Exception:
            return False

    def _check_subqueries_in_expression(self, expr) -> bool:
        """检查表达式中是否包含子查询"""
        try:
            # 检查是否是子查询表达式
            if hasattr(expr, 'subquery') or hasattr(expr, 'select_stmt'):
                return True

            # 递归检查复合表达式
            if hasattr(expr, 'left'):
                if self._check_subqueries_in_expression(expr.left):
                    return True

            if hasattr(expr, 'right'):
                if self._check_subqueries_in_expression(expr.right):
                    return True

            return False
        except Exception:
            return False

    def get_optimization_statistics(self) -> Dict[str, Any]:
        """获取优化统计信息"""
        try:
            if hasattr(self, 'optimization_pipeline'):
                return self.optimization_pipeline.get_optimization_statistics()
            elif hasattr(self.optimizer, 'get_statistics'):
                return self.optimizer.get_statistics()
            else:
                return {
                    "optimizer_type": "basic",
                    "optimizations_applied": 0,
                    "optimization_enabled": self.enable_optimization
                }
        except Exception as e:
            return {
                "error": str(e),
                "optimizer_type": "unknown",
                "optimization_enabled": self.enable_optimization
            }

    def get_alias_info(self) -> Dict[str, Any]:
        """获取当前的别名信息"""
        return {
            'alias_to_real': self.table_aliases.copy(),
            'real_to_alias': self.real_to_alias.copy(),
            'total_aliases': len(self.table_aliases)
        }

    def _generate_create_view_plan(self, stmt: CreateViewStmt) -> 'CreateViewOp':
        """生成创建视图的执行计划"""
        if not self.silent_mode:
            print(f"   🏗️ 生成CREATE VIEW计划: {stmt.view_name}")
            if stmt.materialized:
                print(f"     类型: 物化视图")
            if stmt.or_replace:
                print(f"     模式: OR REPLACE")

        # 生成SELECT语句的执行计划
        select_plan = self._generate_basic_select_plan(stmt.select_stmt)

        return CreateViewOp(
            view_name=stmt.view_name,
            select_plan=select_plan,
            columns=stmt.columns,
            or_replace=stmt.or_replace,
            materialized=stmt.materialized,
            with_check_option=stmt.with_check_option,
            catalog=self.catalog_manager
        )

    def _generate_drop_view_plan(self, stmt: DropViewStmt) -> 'DropViewOp':
        """生成删除视图的执行计划"""
        if not self.silent_mode:
            print(f"   🗑️ 生成DROP VIEW计划: {stmt.view_names}")
            if stmt.materialized:
                print(f"     类型: 物化视图")
            if stmt.cascade:
                print(f"     模式: CASCADE")

        return DropViewOp(
            view_names=stmt.view_names,
            if_exists=stmt.if_exists,
            cascade=stmt.cascade,
            materialized=stmt.materialized,
            catalog=self.catalog_manager
        )

    def _generate_show_views_plan(self, stmt: ShowViewsStmt) -> 'ShowViewsOp':
        """生成显示视图的执行计划"""
        if not self.silent_mode:
            print(f"   📋 生成SHOW VIEWS计划")
            if stmt.database:
                print(f"     数据库: {stmt.database}")
            if stmt.pattern:
                print(f"     模式: {stmt.pattern}")

        return ShowViewsOp(
            pattern=stmt.pattern,
            database=stmt.database
        )

    def _generate_describe_view_plan(self, stmt: DescribeViewStmt) -> 'DescribeViewOp':
        """生成描述视图的执行计划"""
        if not self.silent_mode:
            print(f"   📝 生成DESCRIBE VIEW计划: {stmt.view_name}")

        return DescribeViewOp(view_name=stmt.view_name)

    def _generate_from_plan(self, from_clause) -> Operator:
        """生成FROM子句的执行计划 - 支持视图"""
        if isinstance(from_clause, TableRef):
            real_table_name = from_clause.table_name
            table_alias = getattr(from_clause, 'alias', None)

            # 检查是否是视图
            if self._is_view(real_table_name):
                # 获取视图定义并展开
                view_definition = self._get_view_definition(real_table_name)
                underlying_plan = self._generate_basic_select_plan(view_definition)

                return ViewScanOp(real_table_name, underlying_plan)
            else:
                # 普通表
                if table_alias:
                    return AliasAwareSeqScanOp(real_table_name, table_alias)
                else:
                    return SeqScanOp(real_table_name)

        elif isinstance(from_clause, JoinExpr):
            left_plan = self._generate_from_plan(from_clause.left)
            right_plan = self._generate_from_plan(from_clause.right)

            return AliasAwareJoinOp(
                from_clause.join_type,
                getattr(from_clause, 'on_condition', None),
                [left_plan, right_plan]
            )

        else:
            raise SemanticError(f"不支持的FROM子句类型: {type(from_clause).__name__}")

    def _is_view(self, name: str) -> bool:
        """检查是否是视图"""
        if hasattr(self.catalog_manager, 'is_view'):
            return self.catalog_manager.is_view(name)
        return False

    def _get_view_definition(self, view_name: str):
        """获取视图定义"""
        if hasattr(self.catalog_manager, 'get_view_definition'):
            definition = self.catalog_manager.get_view_definition(view_name)
            # 这里需要重新解析视图的SELECT语句
            from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer
            from sql_compiler.parser.syntax_analyzer import SyntaxAnalyzer

            lexer = LexicalAnalyzer(definition)
            tokens = lexer.tokenize()
            parser = SyntaxAnalyzer(tokens)
            return parser.parse()

        # 简化实现
        from sql_compiler.parser.ast_nodes import SelectStmt, TableRef
        return SelectStmt(columns=["*"], from_clause=TableRef("dummy"))