from typing import List, Any, Dict
from sql_compiler.parser.ast_nodes import *
from sql_compiler.codegen.operators import *
from sql_compiler.exceptions.compiler_errors import SemanticError


class PlanGenerator:
    """执行计划生成器 """

    def generate(self, stmt: Statement) -> Operator:
        """生成执行计划"""
        if isinstance(stmt, CreateTableStmt):
            return self._generate_create_table_plan(stmt)
        elif isinstance(stmt, InsertStmt):
            return self._generate_insert_plan(stmt)
        elif isinstance(stmt, SelectStmt):
            return self._generate_select_plan(stmt)
        elif isinstance(stmt, UpdateStmt):
            return self._generate_update_plan(stmt)
        elif isinstance(stmt, DeleteStmt):
            return self._generate_delete_plan(stmt)
        else:
            raise SemanticError(f"不支持的语句类型: {type(stmt).__name__}")

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

    def _generate_from_plan(self, from_clause: FromClause) -> Operator:
        """生成FROM子句的执行计划"""
        if isinstance(from_clause, TableRef):
            return SeqScanOp(from_clause.table_name)
        elif isinstance(from_clause, JoinExpr):
            left_plan = self._generate_from_plan(from_clause.left)
            right_plan = self._generate_from_plan(from_clause.right)
            return JoinOp(from_clause.join_type, from_clause.on_condition,
                          [left_plan, right_plan])
        else:
            raise SemanticError(f"不支持的FROM子句类型: {type(from_clause).__name__}")

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