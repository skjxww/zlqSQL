from typing import List, Any, Dict
from sql_compiler.parser.ast_nodes import *
from sql_compiler.codegen.operators import *
from sql_compiler.exceptions.compiler_errors import SemanticError


class PlanGenerator:
    """执行计划生成器"""

    def __init__(self):
        pass

    def generate(self, ast: Statement) -> Operator:
        """生成执行计划"""
        return ast.accept(self)

    def visit_create_table_stmt(self, stmt: CreateTableStmt) -> CreateTableOp:
        """生成CREATE TABLE执行计划"""
        return CreateTableOp(stmt.table_name, stmt.columns)

    def visit_insert_stmt(self, stmt: InsertStmt) -> InsertOp:
        """生成INSERT执行计划"""
        # 评估值表达式
        values = []
        for value_expr in stmt.values:
            if isinstance(value_expr, LiteralExpr):
                values.append(value_expr.value)
            else:
                # 对于更复杂的表达式，这里需要进一步处理
                values.append(str(value_expr.to_dict()))

        return InsertOp(stmt.table_name, stmt.columns, values)

    def visit_select_stmt(self, stmt: SelectStmt) -> Operator:
        """生成SELECT执行计划"""
        # 构建执行计划树：Project -> Filter -> SeqScan

        # 1. 基础扫描算子
        scan_op = SeqScanOp(stmt.table_name)

        # 2. 如果有WHERE条件，添加过滤算子
        current_op = scan_op
        if stmt.where_clause:
            filter_op = FilterOp(stmt.where_clause)
            filter_op.add_child(current_op)
            current_op = filter_op

        # 3. 添加投影算子
        project_op = ProjectOp(stmt.columns)
        project_op.add_child(current_op)

        return project_op

    def visit_delete_stmt(self, stmt: DeleteStmt) -> DeleteOp:
        """生成DELETE执行计划"""
        return DeleteOp(stmt.table_name, stmt.where_clause)

    def visit_binary_expr(self, expr: BinaryExpr):
        """处理二元表达式"""
        # 在执行计划生成阶段，表达式通常作为算子的参数
        return expr

    def visit_identifier_expr(self, expr: IdentifierExpr):
        """处理标识符表达式"""
        return expr

    def visit_literal_expr(self, expr: LiteralExpr):
        """处理字面量表达式"""
        return expr