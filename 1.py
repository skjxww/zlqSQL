# 数据库系统中的使用示例
from sql_compiler.main import SQLCompiler


class DatabaseEngine:
    def __init__(self):
        # 创建SQL编译器实例（生产模式）
        self.sql_compiler = SQLCompiler(
            test_mode=True,  # 减少输出
            enable_diagnostics=False  # 生产环境关闭诊断
        )

    def execute_sql(self, sql: str):
        """执行SQL的主要接口"""
        # 1. 编译SQL
        execution_plan = self.sql_compiler.compile_silent(sql)

        if execution_plan is None:
            return {"success": False, "error": "SQL编译失败"}

        # 2. 执行计划
        try:
            result = self.execute_plan(execution_plan)
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def execute_plan(self, plan):
        """执行执行计划"""
        plan_type = type(plan).__name__

        if plan_type == "SeqScanOp":
            return self.scan_table(plan.table_name)
        elif plan_type == "FilterOp":
            child_result = self.execute_plan(plan.children[0])
            return self.apply_filter(child_result, plan.condition)
        elif plan_type == "ProjectOp":
            child_result = self.execute_plan(plan.children[0])
            return self.apply_projection(child_result, plan.columns)
        elif plan_type == "InsertOp":
            return self.insert_data(plan.table_name, plan.columns, plan.values)
        # ... 其他操作类型

    def scan_table(self, table_name):
        """扫描表数据 - 对接你们的存储引擎"""
        # 调用存储引擎的扫描方法
        pass

    def apply_filter(self, data, condition):
        """应用过滤条件 - 实现WHERE逻辑"""
        # 根据条件过滤数据
        pass

    def apply_projection(self, data, columns):
        """应用投影 - 实现SELECT列选择"""
        # 选择指定的列
        pass

    def insert_data(self, table_name, columns, values):
        """插入数据 - 对接你们的存储引擎"""
        # 调用存储引擎的插入方法
        pass

    def execute_sql_batch(self, sql_list: list):
        """批量执行SQL"""
        results = []

        for sql in sql_list:
            execution_plan = self.sql_compiler.compile_silent(sql)
            if execution_plan:
                try:
                    result = self.execute_plan(execution_plan)
                    results.append({"sql": sql, "success": True, "result": result})
                except Exception as e:
                    results.append({"sql": sql, "success": False, "error": str(e)})
            else:
                results.append({"sql": sql, "success": False, "error": "编译失败"})

        return results

    def execute_transaction(self, sql_list: list):
        """执行事务（所有SQL要么全部成功，要么全部失败）"""
        # 1. 编译所有SQL
        plans = []
        for sql in sql_list:
            plan = self.sql_compiler.compile_silent(sql)
            if plan is None:
                return {"success": False, "error": f"SQL编译失败: {sql}"}
            plans.append(plan)

        # 2. 开始事务
        self.begin_transaction()

        try:
            # 3. 执行所有计划
            results = []
            for plan in plans:
                result = self.execute_plan(plan)
                results.append(result)

            # 4. 提交事务
            self.commit_transaction()
            return {"success": True, "results": results}

        except Exception as e:
            # 5. 回滚事务
            self.rollback_transaction()
            return {"success": False, "error": str(e)}