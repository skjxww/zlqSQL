import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
from datetime import datetime
import time
from gui.utils.syntax_highlight import SyntaxHighlighter
from extensions.smart_completion import CompletionUI

class SQLQueryTab:
    def __init__(self, parent, db_manager, ai_manager, result_display):
        self.db_manager = db_manager
        self.ai_manager = ai_manager
        self.result_display = result_display
        self.frame = ttk.Frame(parent)

        # 获取根窗口引用
        self.root = self._get_root_window(parent)

        self._create_widgets()
        self._setup_bindings()

        # 当前错误分析
        self.current_error_analysis = None

        # 设置结果显示的回调和纠错器
        if result_display:
            result_display.set_sql_corrector(db_manager.sql_corrector)
            result_display.set_sql_query_callback(self._handle_result_display_callback)

    def _get_root_window(self, widget):
        """获取根窗口"""
        try:
            # 尝试获取根窗口
            root = widget
            while hasattr(root, 'master') and root.master:
                root = root.master
            return root
        except:
            # 如果失败，返回widget本身
            return widget

    def _create_widgets(self):
        """创建SQL查询标签页组件"""
        # SQL输入区域
        sql_input_frame = ttk.LabelFrame(self.frame, text="SQL输入", padding="5")
        sql_input_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.sql_text = scrolledtext.ScrolledText(
            sql_input_frame,
            height=8,
            font=("Consolas", 11),
            wrap=tk.WORD
        )
        self.sql_text.pack(fill=tk.BOTH, expand=True)

        # 设置代码补全和语法高亮
        print(f"AI manager completion engine: {self.ai_manager.completion_engine}")  # 调试
        if self.ai_manager.completion_engine:
            print("正在初始化CompletionUI...")  # 调试
            from extensions.smart_completion import CompletionUI
            self.completion = CompletionUI(self.sql_text, self.ai_manager.completion_engine)
            print("CompletionUI初始化完成")  # 调试
        else:
            print("补全引擎未初始化!")  # 调试

        self.highlighter = SyntaxHighlighter(self.sql_text)

        # 按钮框架
        button_frame = ttk.Frame(sql_input_frame)
        button_frame.pack(fill=tk.X, pady=5)

        self.execute_btn = ttk.Button(button_frame, text="🚀 执行SQL", command=self._execute_sql)
        self.execute_btn.pack(side=tk.LEFT, padx=(0, 5))

        ttk.Button(button_frame, text="🔍 智能检查", command=self._smart_check).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="📋 格式化", command=self._format_sql).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="🗑️ 清空", command=self._clear_sql).pack(side=tk.LEFT)

        # 示例SQL按钮
        self._create_example_buttons(sql_input_frame)

    def _create_example_buttons(self, parent):
        """创建示例SQL按钮"""
        example_frame = ttk.LabelFrame(parent, text="示例SQL", padding="5")
        example_frame.pack(fill=tk.X, pady=(5, 0))

        examples = [
            ("创建表", "CREATE TABLE users (id INT, name VARCHAR(50), email VARCHAR(100));"),
            ("插入数据", "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');"),
            ("查询数据", "SELECT * FROM users;"),
            ("聚合查询", "SELECT city, COUNT(*) FROM customers GROUP BY city HAVING COUNT(*) > 1;"),
        ]

        for i, (name, sql) in enumerate(examples):
            btn = ttk.Button(
                example_frame,
                text=name,
                command=lambda s=sql: self._insert_example_sql(s),
                width=15
            )
            btn.grid(row=i // 2, column=i % 2, padx=2, pady=2, sticky=(tk.W, tk.E))

        example_frame.columnconfigure(0, weight=1)
        example_frame.columnconfigure(1, weight=1)

    def _handle_result_display_callback(self, action, data):
        """处理来自结果显示的回调"""
        if action == 'apply_correction':
            # 应用修正的SQL
            self.sql_text.delete(1.0, tk.END)
            self.sql_text.insert(1.0, data)
            messagebox.showinfo("修正应用", "SQL已更新，您可以重新执行")

        elif action == 'recheck':
            # 重新检查当前SQL
            self._smart_check()

    def _setup_bindings(self):
        """设置事件绑定"""
        self.sql_text.bind('<Control-Return>', lambda e: self._execute_sql())

    def _execute_sql(self):
        """执行SQL"""
        sql_content = self.sql_text.get(1.0, tk.END).strip()
        if not sql_content:
            messagebox.showwarning("警告", "请输入SQL语句")
            return

        # 解析多个SQL语句
        sql_statements = self._parse_sql_statements(sql_content)

        if not sql_statements:
            messagebox.showwarning("警告", "未找到有效的SQL语句")
            return

        # 禁用执行按钮
        self.execute_btn.configure(state=tk.DISABLED, text="执行中...")

        # 在单独线程中执行
        thread = threading.Thread(target=self._execute_in_thread, args=(sql_statements,))
        thread.daemon = True
        thread.start()

    def _smart_check(self):
        """智能检查SQL"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showinfo("提示", "请先输入SQL语句")
            return

        # 进行智能分析
        try:
            analysis = self.db_manager.sql_corrector.analyze_and_suggest(sql)
            self.result_display.update_smart_analysis(analysis, success=True if not analysis.get('suggestions') else None,
                                                      original_sql=sql)

            # 切换到智能分析标签页
            self.result_display.result_notebook.select(2)

        except Exception as e:
            messagebox.showerror("分析错误", f"智能分析失败: {str(e)}")

    def _parse_sql_statements(self, sql_content):
        """解析多个SQL语句"""
        import re

        # 移除注释（简单处理）
        lines = sql_content.split('\n')
        cleaned_lines = []
        for line in lines:
            # 移除行注释
            if '--' in line:
                line = line[:line.index('--')]
            cleaned_lines.append(line)

        cleaned_content = '\n'.join(cleaned_lines)

        # 按分号分割SQL语句
        statements = []
        current_statement = ""

        for char in cleaned_content:
            current_statement += char
            if char == ';':
                # 找到一个完整的SQL语句
                stmt = current_statement.strip()
                if stmt and stmt != ';':
                    statements.append(stmt)
                current_statement = ""

        # 处理最后一个可能没有分号的语句
        if current_statement.strip():
            final_stmt = current_statement.strip()
            if not final_stmt.endswith(';'):
                final_stmt += ';'
            statements.append(final_stmt)

        return statements

    def _execute_in_thread(self, sql_statements):
        """在线程中执行SQL"""
        try:
            if len(sql_statements) == 1:
                # 单个SQL语句
                self._execute_single_sql(sql_statements[0])
            else:
                # 多个SQL语句
                self._execute_batch_sql(sql_statements)
        except Exception as e:
            # 确保在发生未捕获异常时也能重新启用按钮
            print(f"❌ 执行线程异常: {e}")  # 添加调试信息
            self._safe_ui_update(lambda: self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL"))
            self.result_display.log(f"执行过程中发生未预期错误: {str(e)}")

    def _safe_ui_update(self, update_func):
        """安全的UI更新方法"""
        try:
            if self.root and hasattr(self.root, 'after'):
                self.root.after(0, update_func)
            elif self.frame and hasattr(self.frame, 'after'):
                self.frame.after(0, update_func)
            else:
                # 直接调用（可能在主线程中）
                update_func()
        except Exception as e:
            print(f"UI更新失败: {e}")

    def _execute_single_sql(self, sql):
        """执行单个SQL语句（修改了错误处理）"""
        start_time = time.time()

        try:
            # 执行SQL
            result = self.db_manager.execute_query(sql)
            execution_time = time.time() - start_time

            # 在主线程中更新UI
            self._safe_ui_update(lambda: self._update_success_ui(sql, result, execution_time))

        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = str(e)
            print(f"❌ SQL执行失败: {error_msg}")  # 添加调试信息

            # 在主线程中更新错误UI（包含智能分析）
            self._safe_ui_update(lambda: self._update_error_ui_with_smart_analysis(sql, error_msg, execution_time))

    def _update_error_ui_with_smart_analysis(self, sql, error_msg, execution_time):
        """更新错误UI并进行智能分析"""
        print(f"❌ 更新错误UI: {error_msg}")

        try:
            # 重新启用执行按钮
            self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

            # 使用ResultDisplay的智能分析方法
            self.result_display.display_error_with_analysis(error_msg, sql)

            # 记录执行时间
            self.result_display.log(f"⏱️ 执行耗时: {execution_time:.3f}s")

            # 添加到历史
            self.result_display.add_to_history(sql, execution_time, False, error_msg)

        except Exception as e:
            print(f"❌ 更新错误UI失败: {e}")
            # 至少显示基本错误信息
            messagebox.showerror("执行错误", error_msg)

    def _execute_batch_sql(self, sql_statements):
        """执行批量SQL语句"""
        print(f"📋 开始执行批量SQL，共 {len(sql_statements)} 条语句")

        all_results = []
        successful_count = 0
        failed_count = 0
        start_time = time.time()

        for i, sql in enumerate(sql_statements, 1):
            print(f"🔄 执行第 {i} 条语句...")

            try:
                result = self.db_manager.execute_query(sql)

                # 获取执行计划（如果支持）
                plan = None
                try:
                    if hasattr(self.db_manager, 'get_execution_plan'):
                        plan = self.db_manager.get_execution_plan(sql)
                except:
                    pass

                all_results.append({
                    'index': i,
                    'sql': sql,
                    'result': result,
                    'plan': plan,
                    'success': True,
                    'error': None
                })
                successful_count += 1
                print(f"✅ 第 {i} 条语句执行成功")

            except Exception as e:
                error_msg = str(e)
                all_results.append({
                    'index': i,
                    'sql': sql,
                    'result': None,
                    'plan': None,
                    'success': False,
                    'error': error_msg
                })
                failed_count += 1
                print(f"❌ 第 {i} 条语句执行失败: {error_msg}")

        execution_time = time.time() - start_time
        execution_summary = {
            'total': len(sql_statements),
            'successful': successful_count,
            'failed': failed_count
        }

        print(f"📊 批量执行完成: 成功 {successful_count}/{len(sql_statements)}")

        # 在主线程中更新UI
        self._safe_ui_update(lambda: self._update_batch_result_ui(all_results, execution_summary, execution_time))

    def _update_success_ui(self, sql, result, execution_time):
        """更新成功结果UI"""
        print(f"✅ 更新成功UI，执行时间: {execution_time:.3f}s")

        try:
            # 重新启用执行按钮
            self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

            # 更新结果显示
            self.result_display.display_result(result)

            # 获取执行计划
            try:
                if hasattr(self.db_manager, 'get_execution_plan'):
                    plan = self.db_manager.get_execution_plan(sql)
                    self.result_display.update_execution_plan(plan)
            except Exception as e:
                print(f"⚠️ 获取执行计划失败: {e}")

            # 进行性能分析
            try:
                if hasattr(self.db_manager, 'sql_corrector'):
                    improvement_analysis = self.db_manager.sql_corrector.analyze_and_suggest(sql)
                    self.result_display.update_smart_analysis(improvement_analysis, success=True)
            except Exception as e:
                print(f"⚠️ 智能分析失败: {e}")

            # 添加到历史
            self.result_display.add_to_history(sql, execution_time, True)

            # 记录成功日志
            self.result_display.log(f"✅ 执行成功，耗时: {execution_time:.3f}s")

        except Exception as e:
            print(f"❌ 更新成功UI失败: {e}")
            self.result_display.log(f"UI更新错误: {str(e)}")

    def _update_batch_result_ui(self, all_results, execution_summary, execution_time):
        """更新批量执行结果UI"""
        try:
            # 显示执行摘要
            summary_msg = (f"批量执行完成！\n"
                           f"总计: {execution_summary['total']} 条语句\n"
                           f"成功: {execution_summary['successful']} 条\n"
                           f"失败: {execution_summary['failed']} 条\n"
                           f"总耗时: {execution_time:.3f}s")

            # 记录日志
            self.result_display.log(summary_msg)

            # 显示批量执行结果
            self._show_batch_results_dialog(all_results, execution_summary, execution_time)

            # 更新结果显示 - 显示最后一个成功的结果
            last_successful_result = None
            for result_info in reversed(all_results):
                if result_info['success'] and result_info['result'] is not None:
                    last_successful_result = result_info
                    break

            if last_successful_result:
                self.result_display.display_result(last_successful_result['result'])
                self.result_display.update_execution_plan(last_successful_result['plan'])

            # 添加到历史记录
            all_sql = '\n'.join([r['sql'] for r in all_results])
            success = execution_summary['failed'] == 0
            error_msg = None if success else f"{execution_summary['failed']} 条语句执行失败"
            self.result_display.add_to_history(all_sql, execution_time, success, error_msg)

            # 如果有失败的语句，显示智能分析
            if execution_summary['failed'] > 0:
                self._analyze_failed_statements(all_results)

        except Exception as e:
            self.result_display.log(f"UI更新错误: {str(e)}")
        finally:
            # 重新启用执行按钮
            self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _update_result_ui(self, result, sql, execution_time, improvement_analysis, plan):
        """更新成功结果UI"""
        try:
            # 更新结果显示
            self.result_display.display_result(result)

            # 更新执行计划
            self.result_display.update_execution_plan(plan)

            # 更新智能分析
            self.result_display.update_smart_analysis(improvement_analysis, success=True)

            # 添加到历史
            self.result_display.add_to_history(sql, execution_time, True)

            # 记录成功日志
            self.result_display.log(f"执行成功，耗时: {execution_time:.3f}s")

        except Exception as e:
            self.result_display.log(f"UI更新错误: {str(e)}")
        finally:
            # 重新启用执行按钮
            self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _update_error_ui_with_analysis(self, error, analysis):
        """更新错误UI并显示智能分析"""
        # 更新状态
        self.result_display.log(f"执行失败: {str(error)}")

        # 添加到历史
        sql = self.sql_text.get(1.0, tk.END).strip()
        self.result_display.add_to_history(sql, 0, False, str(error))

        # 更新智能分析
        self.result_display.update_smart_analysis(analysis, success=False)

        # 显示错误分析对话框
        if analysis.get('suggestions') or analysis.get('corrected_sql_options'):
            self.frame.after(100, lambda: self._show_error_analysis_dialog(analysis))
        else:
            messagebox.showerror("执行错误", str(error))

        # 重新启用执行按钮
        self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _show_batch_results_dialog(self, all_results, execution_summary, execution_time):
        """显示批量执行结果对话框"""
        dialog = tk.Toplevel(self.root)  # 使用 self.root
        dialog.title("📊 批量执行结果")
        dialog.geometry("900x600")
        dialog.transient(self.root)

        main_frame = ttk.Frame(dialog, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 摘要信息
        summary_frame = ttk.LabelFrame(main_frame, text="执行摘要", padding="10")
        summary_frame.pack(fill=tk.X, pady=(0, 10))

        summary_text = (f"📈 总计: {execution_summary['total']} 条语句  |  "
                        f"✅ 成功: {execution_summary['successful']} 条  |  "
                        f"❌ 失败: {execution_summary['failed']} 条  |  "
                        f"⏱️ 耗时: {execution_time:.3f}s")

        ttk.Label(summary_frame, text=summary_text, font=("Arial", 10)).pack()

        # 创建Notebook来分类显示结果
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # 成功的语句标签页
        success_frame = ttk.Frame(notebook)
        notebook.add(success_frame, text=f"✅ 成功 ({execution_summary['successful']})")

        success_tree = self._create_result_treeview(success_frame, "success")

        # 失败的语句标签页
        if execution_summary['failed'] > 0:
            fail_frame = ttk.Frame(notebook)
            notebook.add(fail_frame, text=f"❌ 失败 ({execution_summary['failed']})")

            fail_tree = self._create_result_treeview(fail_frame, "failure")

        # 填充数据
        for result_info in all_results:
            if result_info['success']:
                # 添加到成功列表
                row_count = "多行" if isinstance(result_info['result'], list) and len(
                    result_info['result']) > 1 else "1行"
                if isinstance(result_info['result'], list):
                    row_count = f"{len(result_info['result'])}行"

                success_tree.insert("", tk.END, values=(
                    result_info['index'],
                    result_info['sql'][:60] + "..." if len(result_info['sql']) > 60 else result_info['sql'],
                    row_count,
                    "✅ 成功"
                ))
            else:
                # 添加到失败列表
                if execution_summary['failed'] > 0:
                    fail_tree.insert("", tk.END, values=(
                        result_info['index'],
                        result_info['sql'][:60] + "..." if len(result_info['sql']) > 60 else result_info['sql'],
                        result_info['error'][:40] + "..." if len(result_info['error']) > 40 else result_info['error'],
                        "❌ 失败"
                    ))

        # 按钮框架
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        if execution_summary['failed'] > 0:
            ttk.Button(
                button_frame,
                text="🔧 智能修复失败语句",
                command=lambda: self._batch_fix_failed_statements(all_results, dialog)
            ).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Button(
            button_frame,
            text="📋 导出结果",
            command=lambda: self._export_batch_results(all_results)
        ).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Button(button_frame, text="关闭", command=dialog.destroy).pack(side=tk.RIGHT)

    def _create_result_treeview(self, parent, result_type):
        """创建结果树视图"""
        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        if result_type == "success":
            columns = ("序号", "SQL语句", "影响行数", "状态")
        else:
            columns = ("序号", "SQL语句", "错误信息", "状态")

        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=15)

        # 设置列标题和宽度
        tree.heading("序号", text="序号")
        tree.heading("SQL语句", text="SQL语句")
        tree.heading(columns[2], text=columns[2])
        tree.heading("状态", text="状态")

        tree.column("序号", width=50, minwidth=50)
        tree.column("SQL语句", width=400, minwidth=200)
        tree.column(columns[2], width=200, minwidth=100)
        tree.column("状态", width=80, minwidth=80)

        # 添加滚动条
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)

        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        return tree



    def _update_error_ui(self, error_msg):
        """更新错误UI（简单版本）"""
        messagebox.showerror("执行错误", error_msg)
        self.result_display.log(error_msg)
        self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _format_sql(self):
        """格式化SQL"""
        sql_content = self.sql_text.get(1.0, tk.END).strip()
        if sql_content:
            formatted = self._simple_format_sql(sql_content)
            self.sql_text.delete(1.0, tk.END)
            self.sql_text.insert(1.0, formatted)

    def _simple_format_sql(self, sql):
        """简单的SQL格式化"""
        import re
        sql = re.sub(r'\bSELECT\b', '\nSELECT', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bFROM\b', '\nFROM', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bWHERE\b', '\nWHERE', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bGROUP BY\b', '\nGROUP BY', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bORDER BY\b', '\nORDER BY', sql, flags=re.IGNORECASE)
        return sql.strip()

    def _clear_sql(self):
        """清空SQL"""
        self.sql_text.delete(1.0, tk.END)
        self.current_error_analysis = None
        self.result_display.clear_analysis()

    def _insert_example_sql(self, sql):
        """插入示例SQL"""
        self.sql_text.delete(1.0, tk.END)
        self.sql_text.insert(1.0, sql)

    def _show_error_analysis_dialog(self, analysis):
        """显示错误分析对话框"""
        dialog = tk.Toplevel(self.root)
        dialog.title("🔍 SQL 智能错误分析")
        dialog.geometry("700x500")
        dialog.transient(self.root)
        dialog.grab_set()

        # 创建主框架
        main_frame = ttk.Frame(dialog, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 创建文本框显示分析结果
        text_frame = ttk.Frame(main_frame)
        text_frame.pack(fill=tk.BOTH, expand=True)

        analysis_text = scrolledtext.ScrolledText(
            text_frame,
            wrap=tk.WORD,
            font=("Consolas", 10),
            height=20
        )
        analysis_text.pack(fill=tk.BOTH, expand=True)

        # 格式化分析结果
        content = self._format_error_analysis(analysis)
        analysis_text.insert(1.0, content)
        analysis_text.configure(state=tk.DISABLED)

        # 按钮框架
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))

        # 如果有修正建议，显示修正按钮
        if analysis.get('corrected_sql_options'):
            ttk.Button(
                button_frame,
                text="🔧 应用修正",
                command=lambda: self._show_correction_options(dialog, analysis)
            ).pack(side=tk.LEFT)

        ttk.Button(button_frame, text="关闭", command=dialog.destroy).pack(side=tk.RIGHT)

    def _analyze_failed_statements(self, all_results):
        """分析失败的SQL语句"""
        failed_results = [r for r in all_results if not r['success']]

        if not failed_results:
            return

        try:
            # 对第一个失败的语句进行智能分析
            first_failed = failed_results[0]
            error = Exception(first_failed['error'])
            analysis = self.db_manager.sql_corrector.analyze_and_suggest(first_failed['sql'], error)

            # 更新智能分析显示
            self.result_display.update_smart_analysis(analysis, success=False)

            # 存储当前分析结果
            self.current_error_analysis = analysis

        except Exception as e:
            self.result_display.log(f"智能分析失败: {str(e)}")

    def _batch_fix_failed_statements(self, all_results, parent_dialog):
        """批量修复失败的语句"""
        failed_results = [r for r in all_results if not r['success']]

        if not failed_results:
            messagebox.showinfo("提示", "没有失败的语句需要修复")
            return

        # 创建修复对话框
        fix_dialog = tk.Toplevel(parent_dialog)
        fix_dialog.title("🔧 批量智能修复")
        fix_dialog.geometry("800x600")
        fix_dialog.transient(parent_dialog)
        fix_dialog.grab_set()

        main_frame = ttk.Frame(fix_dialog, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(
            main_frame,
            text=f"发现 {len(failed_results)} 条失败的SQL语句，正在进行智能分析...",
            font=("Arial", 12)
        ).pack(pady=(0, 10))

        # 进度条
        progress = ttk.Progressbar(main_frame, mode='indeterminate')
        progress.pack(fill=tk.X, pady=(0, 10))
        progress.start()

        # 结果显示区域
        result_frame = ttk.Frame(main_frame)
        result_frame.pack(fill=tk.BOTH, expand=True)

        # 在单独线程中进行批量分析
        def analyze_thread():
            try:
                fixed_statements = []

                for i, failed_result in enumerate(failed_results):
                    try:
                        error = Exception(failed_result['error'])
                        analysis = self.db_manager.sql_corrector.analyze_and_suggest(
                            failed_result['sql'], error
                        )

                        if analysis.get('corrected_sql_options'):
                            best_fix = analysis['corrected_sql_options'][0]
                            fixed_statements.append({
                                'original_index': failed_result['index'],
                                'original_sql': failed_result['sql'],
                                'fixed_sql': best_fix['sql'],
                                'description': best_fix['description'],
                                'confidence': best_fix['confidence']
                            })

                    except Exception as e:
                        print(f"分析语句 {failed_result['index']} 时出错: {e}")

                # 更新UI
                fix_dialog.after(0, lambda: self._show_fix_results(
                    fix_dialog, result_frame, progress, fixed_statements
                ))

            except Exception as e:
                fix_dialog.after(0, lambda: messagebox.showerror("分析失败", str(e)))

        thread = threading.Thread(target=analyze_thread)
        thread.daemon = True
        thread.start()

    def _show_fix_results(self, dialog, result_frame, progress, fixed_statements):
        """显示修复结果"""
        progress.stop()
        progress.destroy()

        if not fixed_statements:
            ttk.Label(
                result_frame,
                text="😔 未能自动修复任何语句，建议手动检查语法错误",
                font=("Arial", 10)
            ).pack(pady=20)
        else:
            ttk.Label(
                result_frame,
                text=f"🎉 成功分析并提供 {len(fixed_statements)} 条修复建议:",
                font=("Arial", 10, "bold")
            ).pack(anchor=tk.W, pady=(0, 10))

            # 创建修复建议列表
            fix_frame = ttk.Frame(result_frame)
            fix_frame.pack(fill=tk.BOTH, expand=True)

            fix_text = scrolledtext.ScrolledText(fix_frame, height=15, font=("Consolas", 9))
            fix_text.pack(fill=tk.BOTH, expand=True)

            content = ""
            for i, fix in enumerate(fixed_statements, 1):
                content += f"{i}. 语句 #{fix['original_index']} - {fix['description']}\n"
                content += f"   置信度: {fix['confidence']:.1%}\n"
                content += f"   原始: {fix['original_sql']}\n"
                content += f"   修复: {fix['fixed_sql']}\n"
                content += "-" * 60 + "\n\n"

            fix_text.insert(1.0, content)
            fix_text.configure(state=tk.DISABLED)

        # 按钮
        button_frame = ttk.Frame(result_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))

        if fixed_statements:
            ttk.Button(
                button_frame,
                text="✅ 应用所有修复",
                command=lambda: self._apply_batch_fixes(fixed_statements, dialog)
            ).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Button(button_frame, text="关闭", command=dialog.destroy).pack(side=tk.RIGHT)

    def _apply_batch_fixes(self, fixed_statements, dialog):
        """应用批量修复"""
        if messagebox.askyesno("确认",
                               f"确定要应用 {len(fixed_statements)} 条修复建议吗？\n修复后的SQL将替换当前输入框内容。"):
            # 构建修复后的SQL
            current_sql_lines = self.sql_text.get(1.0, tk.END).strip().split('\n')

            # 简单替换：重新构建所有SQL
            fixed_sql_list = []
            original_statements = self._parse_sql_statements(self.sql_text.get(1.0, tk.END).strip())

            for i, original_sql in enumerate(original_statements, 1):
                # 查找是否有对应的修复
                fix_found = False
                for fix in fixed_statements:
                    if fix['original_index'] == i:
                        fixed_sql_list.append(fix['fixed_sql'])
                        fix_found = True
                        break

                if not fix_found:
                    fixed_sql_list.append(original_sql)

            # 更新输入框
            new_content = '\n\n'.join(fixed_sql_list)
            self.sql_text.delete(1.0, tk.END)
            self.sql_text.insert(1.0, new_content)

            dialog.destroy()
            messagebox.showinfo("完成", "批量修复已应用！可以重新执行SQL查看效果。")

    def _export_batch_results(self, all_results):
        """导出批量执行结果"""
        try:
            from tkinter import filedialog
            import csv

            filename = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV文件", "*.csv"), ("所有文件", "*.*")],
                title="导出批量执行结果"
            )

            if filename:
                with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(["序号", "SQL语句", "执行状态", "结果/错误信息"])

                    for result in all_results:
                        status = "成功" if result['success'] else "失败"
                        result_info = f"{len(result['result'])}行数据" if result['success'] and result[
                            'result'] else result.get('error', '')
                        writer.writerow([result['index'], result['sql'], status, result_info])

                messagebox.showinfo("导出完成", f"结果已导出到: {filename}")

        except Exception as e:
            messagebox.showerror("导出失败", f"导出过程中出错: {str(e)}")

    def _format_error_analysis(self, analysis):
        """格式化错误分析内容"""
        content = "🔍 SQL 智能错误分析\n" + "=" * 60 + "\n\n"

        if analysis['has_error']:
            content += f"❌ 错误信息:\n{analysis['error_message']}\n\n"

            if analysis['suggestions']:
                content += "💡 错误分析和建议:\n" + "-" * 40 + "\n"
                for i, suggestion in enumerate(analysis['suggestions'], 1):
                    confidence_bar = "█" * int(suggestion['confidence'] * 10)
                    content += f"\n{i}. 问题类型: {suggestion['type']}\n"
                    content += f"   描述: {suggestion['description']}\n"
                    content += f"   建议: {suggestion['suggestion']}\n"
                    content += f"   置信度: {confidence_bar} ({suggestion['confidence']:.1%})\n"

            if analysis['corrected_sql_options']:
                content += "\n🔧 建议的修正版本:\n" + "-" * 40 + "\n"
                for i, option in enumerate(analysis['corrected_sql_options'], 1):
                    content += f"\n{i}. {option['description']}\n"
                    content += f"   置信度: {option['confidence']:.1%}\n"
                    content += f"   修正SQL: {option['sql']}\n"

        return content

    def _show_diagnosis_details(self):
        """显示诊断详情"""
        if self.current_error_analysis:
            if self.current_error_analysis.get('has_error'):
                self._show_error_analysis_dialog(self.current_error_analysis)
            else:
                self._show_improvement_tips_dialog(self.current_error_analysis)
        else:
            messagebox.showinfo("提示", "暂无诊断信息")

    def _show_correction_options_dialog(self):
        """显示修正选项对话框"""
        if not self.current_error_analysis or not self.current_error_analysis.get('corrected_sql_options'):
            messagebox.showinfo("提示", "没有可用的修正选项")
            return
        dialog = tk.Toplevel(self.root)
        dialog.title("🔧 选择修正选项")
        dialog.geometry("700x400")
        dialog.transient(self.root)
        dialog.grab_set()

        main_frame = ttk.Frame(dialog, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 说明标签
        ttk.Label(
            main_frame,
            text="请选择要应用的修正版本:",
            font=("Arial", 12, "bold")
        ).pack(pady=(0, 15))

        # 选项框架
        options_frame = ttk.Frame(main_frame)
        options_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 15))

        # 单选按钮变量
        correction_choice = tk.StringVar(value="0")

        # 修正选项
        for i, option in enumerate(self.current_error_analysis['corrected_sql_options']):
            option_frame = ttk.LabelFrame(options_frame, text=f"选项 {i + 1}", padding="5")
            option_frame.pack(fill=tk.X, pady=2)

            ttk.Radiobutton(
                option_frame,
                text=f"{option['description']} (置信度: {option['confidence']:.1%})",
                variable=correction_choice,
                value=str(i)
            ).pack(anchor=tk.W)

            # 显示SQL预览
            sql_preview = tk.Text(
                option_frame,
                height=2,
                wrap=tk.WORD,
                font=("Consolas", 8),
                bg="#f8f8f8"
            )
            sql_preview.pack(fill=tk.X, pady=(2, 0))
            sql_preview.insert(1.0, option['sql'])
            sql_preview.configure(state=tk.DISABLED)

        # 按钮框架
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

    def _apply_corrected_sql(self, corrected_sql):
        """应用修正后的SQL"""
        # 将修正后的SQL放入输入框
        self.sql_text.delete(1.0, tk.END)
        self.sql_text.insert(1.0, corrected_sql)

        # 显示确认对话框
        result = messagebox.askyesno(
            "应用修正",
            f"修正已应用到SQL输入框。\n\n是否立即执行？"
        )

        if result:
            self._execute_sql()
        else:
            messagebox.showinfo("提示", "修正已应用，可以手动执行或进一步编辑")

    def receive_sql_from_nl_tab(self, sql_content, execute=False):
        """接收来自自然语言标签页的SQL"""
        # 将SQL内容设置到输入框
        self.sql_text.delete(1.0, tk.END)
        self.sql_text.insert(1.0, sql_content)

        # 如果需要立即执行
        if execute:
            # 给用户一个短暂的时间看到SQL内容
            self.frame.after(500, self._execute_sql)
            messagebox.showinfo("执行中", "SQL已填入，正在执行...")
        else:
            messagebox.showinfo("已填入", "SQL已填入查询标签页")

    def get_current_sql(self):
        """获取当前SQL内容"""
        return self.sql_text.get(1.0, tk.END).strip()

    def set_sql_content(self, sql_content):
        """设置SQL内容"""
        self.sql_text.delete(1.0, tk.END)
        self.sql_text.insert(1.0, sql_content)