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
        if self.ai_manager.completion_engine:
            self.completion = CompletionUI(self.sql_text, self.ai_manager.completion_engine)
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
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句")
            return

        if not sql.endswith(';'):
            messagebox.showwarning("警告", "SQL语句必须以分号(;)结尾")
            return

        # 禁用执行按钮
        self.execute_btn.configure(state=tk.DISABLED, text="执行中...")

        # 在单独线程中执行
        thread = threading.Thread(target=self._execute_in_thread, args=(sql,))
        thread.daemon = True
        thread.start()

    def _execute_in_thread(self, sql):
        """在线程中执行SQL"""
        try:
            start_time = datetime.now()

            try:
                # 尝试执行SQL
                result, plan = self.db_manager.execute_query(sql)
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()

                # 获取优化建议
                improvement_analysis = self.db_manager.sql_corrector.analyze_and_suggest(sql)

                # 更新UI
                self.frame.after(0, self._update_result_ui, result, sql, execution_time, improvement_analysis, plan)

            except Exception as e:
                # 发生错误时进行完整的智能分析
                error_analysis = self.db_manager.sql_corrector.analyze_and_suggest(sql, e)
                self.frame.after(0, self._update_error_ui_with_analysis, e, error_analysis)

        except Exception as e:
            self.frame.after(0, self._update_error_ui, f"执行错误: {str(e)}")

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

