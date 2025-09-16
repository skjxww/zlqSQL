import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
from datetime import datetime
from gui.utils.syntax_highlight import SyntaxHighlighter
from extensions.smart_completion import CompletionUI

class SQLQueryTab:
    def __init__(self, parent, db_manager, ai_manager, result_display):
        self.db_manager = db_manager
        self.ai_manager = ai_manager
        self.result_display = result_display
        self.frame = ttk.Frame(parent)

        self._create_widgets()
        self._setup_bindings()

        # 当前错误分析
        self.current_error_analysis = None

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
            ("创建表", "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100));"),
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
        """在线程中执行SQL语句列表"""
        try:
            start_time = datetime.now()
            all_results = []
            execution_summary = {
                'total': len(sql_statements),
                'successful': 0,
                'failed': 0,
                'errors': []
            }

            # 逐个执行SQL语句
            for i, sql in enumerate(sql_statements, 1):
                try:
                    # 使用局部变量捕获当前的值
                    current_i = i
                    total_count = len(sql_statements)

                    # 更新按钮状态显示进度
                    self.frame.after(0, lambda i=current_i, total=total_count:
                    self.execute_btn.configure(text=f"执行中...({i}/{total})"))

                    # 执行单个SQL
                    result, plan = self.db_manager.execute_query(sql)

                    all_results.append({
                        'index': i,
                        'sql': sql,
                        'result': result,
                        'plan': plan,
                        'success': True,
                        'error': None
                    })

                    execution_summary['successful'] += 1

                except Exception as e:
                    # 单个SQL执行失败
                    error_msg = str(e)
                    all_results.append({
                        'index': i,
                        'sql': sql,
                        'result': None,
                        'plan': None,
                        'success': False,
                        'error': error_msg
                    })

                    execution_summary['failed'] += 1
                    execution_summary['errors'].append({
                        'index': i,
                        'sql': sql[:50] + '...' if len(sql) > 50 else sql,
                        'error': error_msg
                    })

            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()

            # 更新UI
            self.frame.after(0, self._update_batch_result_ui, all_results, execution_summary, execution_time)

        except Exception as e:
            self.frame.after(0, self._update_error_ui, f"批量执行错误: {str(e)}")

    def _update_batch_result_ui(self, all_results, execution_summary, execution_time):
        """更新批量执行结果UI（无弹窗版本）"""
        try:
            # 显示执行摘要在日志中
            summary_msg = (f"批量执行完成！总计: {execution_summary['total']} 条语句，"
                           f"成功: {execution_summary['successful']} 条，"
                           f"失败: {execution_summary['failed']} 条，"
                           f"总耗时: {execution_time:.3f}s")

            self.result_display.log(summary_msg)

            # 创建批量结果的汇总数据显示
            batch_results = []
            for result_info in all_results:
                status = "✅ 成功" if result_info['success'] else "❌ 失败"
                row_info = ""

                if result_info['success']:
                    if isinstance(result_info['result'], list):
                        row_info = f"返回 {len(result_info['result'])} 行"
                    else:
                        row_info = "执行成功"
                else:
                    row_info = result_info['error'][:50] + "..." if len(result_info['error']) > 50 else result_info[
                        'error']

                batch_results.append({
                    '序号': result_info['index'],
                    'SQL语句': result_info['sql'][:60] + "..." if len(result_info['sql']) > 60 else result_info['sql'],
                    '执行状态': status,
                    '结果信息': row_info
                })

            # 显示批量结果汇总
            self.result_display.display_result(batch_results)

            # 显示最后一个成功的详细结果在执行计划中
            last_successful_result = None
            for result_info in reversed(all_results):
                if result_info['success'] and result_info['result'] is not None:
                    last_successful_result = result_info
                    break

            if last_successful_result:
                plan_info = {
                    "批量执行汇总": {
                        "总语句数": execution_summary['total'],
                        "成功数": execution_summary['successful'],
                        "失败数": execution_summary['failed'],
                        "总执行时间": f"{execution_time:.3f}s"
                    },
                    "最后成功语句的执行计划": last_successful_result.get('plan', '无执行计划信息')
                }
                self.result_display.update_execution_plan(plan_info)

            # 在智能分析中显示失败语句的分析
            if execution_summary['failed'] > 0:
                self._show_batch_analysis_in_tab(all_results, execution_summary)
            else:
                # 如果全部成功，显示成功信息
                success_analysis = {
                    'has_error': False,
                    'original_sql': f"批量执行 {execution_summary['total']} 条语句",
                    'improvement_tips': [{
                        'suggestion': f"所有 {execution_summary['total']} 条SQL语句都执行成功！"
                    }]
                }
                self.result_display.update_smart_analysis(success_analysis, success=True)

            # 添加到历史记录
            all_sql = '\n'.join([r['sql'] for r in all_results])
            success = execution_summary['failed'] == 0
            error_msg = None if success else f"{execution_summary['failed']} 条语句执行失败"
            self.result_display.add_to_history(all_sql, execution_time, success, error_msg)

        except Exception as e:
            self.result_display.log(f"UI更新错误: {str(e)}")
        finally:
            # 重新启用执行按钮
            self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _show_batch_analysis_in_tab(self, all_results, execution_summary):
        """在智能分析标签页中显示批量分析结果"""
        failed_results = [r for r in all_results if not r['success']]

        # 构建批量分析内容
        analysis_content = {
            'has_error': True,
            'original_sql': f"批量执行 {execution_summary['total']} 条语句",
            'error_message': f"批量执行中有 {execution_summary['failed']} 条语句失败",
            'suggestions': [],
            'corrected_sql_options': [],
            'batch_details': {
                'total': execution_summary['total'],
                'successful': execution_summary['successful'],
                'failed': execution_summary['failed'],
                'failed_statements': []
            }
        }

        # 收集失败语句的详细信息
        for failed_result in failed_results[:5]:  # 最多显示前5个失败的语句
            analysis_content['batch_details']['failed_statements'].append({
                'index': failed_result['index'],
                'sql': failed_result['sql'],
                'error': failed_result['error']
            })

            # 添加通用建议
            analysis_content['suggestions'].append({
                'type': '语法错误',
                'description': f"语句 #{failed_result['index']} 执行失败",
                'suggestion': f"检查SQL语法: {failed_result['error'][:100]}",
                'confidence': 0.8
            })

        # 如果失败语句较少，尝试提供修复建议
        if len(failed_results) <= 3:
            try:
                # 对第一个失败的语句进行详细分析
                first_failed = failed_results[0]
                error = Exception(first_failed['error'])
                detailed_analysis = self.db_manager.sql_corrector.analyze_and_suggest(first_failed['sql'], error)

                # 合并详细分析结果
                if detailed_analysis.get('corrected_sql_options'):
                    analysis_content['corrected_sql_options'] = detailed_analysis['corrected_sql_options']
                if detailed_analysis.get('suggestions'):
                    analysis_content['suggestions'].extend(detailed_analysis['suggestions'])

            except Exception as e:
                self.result_display.log(f"详细分析失败: {str(e)}")

        # 更新智能分析显示
        self.result_display.update_smart_analysis(analysis_content, success=False)

        # 自动切换到智能分析标签页
        self.result_display.show_analysis_tab()

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


    def _update_error_ui(self, error_msg):
        """更新错误UI（简单版本）"""
        messagebox.showerror("执行错误", error_msg)
        self.result_display.log(error_msg)
        self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _smart_check(self):
        """智能检查SQL"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showinfo("提示", "请先输入SQL语句")
            return

        # 进行智能分析
        try:
            analysis = self.db_manager.sql_corrector.analyze_and_suggest(sql)
            self.result_display.update_smart_analysis(analysis, success=None)

            # 切换到智能分析标签页
            self.result_display.show_analysis_tab()

            # 显示提示信息
            has_issues = analysis.get('suggestions') or analysis.get('corrected_sql_options')
            if has_issues:
                messagebox.showinfo("智能检查", "分析完成！SQL有点问题呢。")
            else:
                messagebox.showinfo("智能检查", "✅ 未发现任何问题，SQL看起来很完美！")

        except Exception as e:
            messagebox.showerror("智能检查失败", f"分析过程出错: {str(e)}")

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

