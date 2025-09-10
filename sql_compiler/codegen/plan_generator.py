from typing import List, Any, Dict
from sql_compiler.parser.ast_nodes import *
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

        # 生成基础执行计划
        if isinstance(stmt, CreateTableStmt):
            plan = self._generate_create_table_plan(stmt)
        elif isinstance(stmt, InsertStmt):
            plan = self._generate_insert_plan(stmt)
        elif isinstance(stmt, SelectStmt):
            plan = self._generate_select_plan(stmt)
        elif isinstance(stmt, UpdateStmt):
            plan = self._generate_update_plan(stmt)
        elif isinstance(stmt, DeleteStmt):
            plan = self._generate_delete_plan(stmt)
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

    def _generate_from_plan(self, from_clause: FromClause) -> Operator:
        """生成FROM子句的执行计划 - 增强别名支持"""
        if isinstance(from_clause, TableRef):
            real_table_name = from_clause.table_name
            table_alias = from_clause.alias

            # 使用别名感知的扫描操作符
            if table_alias:
                return AliasAwareSeqScanOp(real_table_name, table_alias)
            else:
                return SeqScanOp(real_table_name)

        elif isinstance(from_clause, JoinExpr):
            left_plan = self._generate_from_plan(from_clause.left)
            right_plan = self._generate_from_plan(from_clause.right)

            # 使用别名感知的连接操作符
            return AliasAwareJoinOp(
                from_clause.join_type,
                from_clause.on_condition,
                [left_plan, right_plan]
            )
        else:
            raise SemanticError(f"不支持的FROM子句类型: {type(from_clause).__name__}")

    def get_alias_info(self) -> Dict[str, Any]:
        """获取当前的别名信息"""
        return {
            'alias_to_real': self.table_aliases.copy(),
            'real_to_alias': self.real_to_alias.copy()
        }


    def _extract_tables_from_stmt(self, stmt: SelectStmt) -> List[str]:
        """从SELECT语句中提取表名"""
        tables = []
        if hasattr(stmt, 'from_clause') and stmt.from_clause:
            if hasattr(stmt.from_clause, 'table_name'):
                tables.append(stmt.from_clause.table_name)
            # 处理JOIN的情况
            # 这里需要根据你的AST结构来实现
        return tables

    def _has_joins(self, stmt: SelectStmt) -> bool:
        """检查是否包含JOIN"""
        return hasattr(stmt, 'from_clause') and hasattr(stmt.from_clause, 'join_type')

    def _has_aggregation(self, stmt: SelectStmt) -> bool:
        """检查是否包含聚合"""
        return hasattr(stmt, 'group_by') and stmt.group_by is not None

    def _has_subqueries(self, stmt: SelectStmt) -> bool:
        """检查是否包含子查询"""
        # 简化实现，实际需要遍历AST
        return False

    def get_optimization_statistics(self) -> Dict[str, Any]:
        """获取优化统计信息"""
        if hasattr(self, 'optimization_pipeline'):
            return self.optimization_pipeline.get_optimization_statistics()
        else:
            return {}

    def _generate_create_table_plan(self, stmt: CreateTableStmt) -> Operator:
        """生成CREATE TABLE执行计划"""
        return CreateTableOp(stmt.table_name, stmt.columns)

    def _generate_insert_plan(self, stmt: InsertStmt) -> Operator:
        """生成INSERT执行计划"""
        return InsertOp(stmt.table_name, stmt.columns, stmt.values)

    def _generate_select_plan(self, stmt: SelectStmt) -> Operator:
        """生成SELECT执行计划"""
        # 从FROM子句开始构建计划
        plan = self._generate_from_plan(stmt.from_clause)

        # 添加WHERE过滤
        if stmt.where_clause:
            plan = FilterOp(stmt.where_clause, [plan])

        # 添加GROUP BY
        if stmt.group_by:
            plan = GroupByOp(stmt.group_by, stmt.having_clause, [plan])

        # 添加投影
        if stmt.columns != ["*"]:
            plan = ProjectOp(stmt.columns, [plan])

        # 添加ORDER BY
        if stmt.order_by:
            plan = OrderByOp(stmt.order_by, [plan])

        return plan

    def _generate_update_plan(self, stmt: UpdateStmt) -> Operator:
        """生成UPDATE执行计划"""
        # 先扫描表
        scan_plan = SeqScanOp(stmt.table_name)

        # 如果有WHERE条件，添加过滤
        if stmt.where_clause:
            scan_plan = FilterOp(stmt.where_clause, [scan_plan])

        # 添加UPDATE操作
        return UpdateOp(stmt.table_name, stmt.assignments, [scan_plan])

    def _generate_delete_plan(self, stmt: DeleteStmt) -> Operator:
        """生成DELETE执行计划"""
        # 先扫描表
        scan_plan = SeqScanOp(stmt.table_name)

        # 如果有WHERE条件，添加过滤
        if stmt.where_clause:
            scan_plan = FilterOp(stmt.where_clause, [scan_plan])

        # 添加DELETE操作
        return DeleteOp(stmt.table_name, [scan_plan])