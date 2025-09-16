import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from datetime import datetime
import json


class ResultDisplay:
    def __init__(self, parent):
        self.parent = parent
        self.frame = ttk.Frame(parent)
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

        # 当前错误分析
        self.current_error_analysis = None

        self._create_widgets()

    def _create_widgets(self):
        """创建结果显示组件"""
        # 创建Notebook来显示不同类型的结果
        self.result_notebook = ttk.Notebook(self.frame)
        self.result_notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

        # 数据结果标签页
        self.data_frame = ttk.Frame(self.result_notebook)
        self.result_notebook.add(self.data_frame, text="数据结果")
        self.data_frame.columnconfigure(0, weight=1)
        self.data_frame.rowconfigure(0, weight=1)

        # 创建表格显示数据
        self._create_result_table(self.data_frame)

        # 执行计划标签页
        self.plan_frame = ttk.Frame(self.result_notebook)
        self.result_notebook.add(self.plan_frame, text="执行计划")
        self.plan_frame.columnconfigure(0, weight=1)
        self.plan_frame.rowconfigure(0, weight=1)

        # 执行计划文本框
        self.plan_text = scrolledtext.ScrolledText(
            self.plan_frame,
            font=("Consolas", 10),
            wrap=tk.WORD
        )
        self.plan_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 智能分析标签页
        self.analysis_frame = ttk.Frame(self.result_notebook)
        self.result_notebook.add(self.analysis_frame, text="智能分析")
        self.analysis_frame.columnconfigure(0, weight=1)
        self.analysis_frame.rowconfigure(0, weight=1)

        # 创建分析框架的布局
        analysis_container = ttk.Frame(self.analysis_frame)
        analysis_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        analysis_container.columnconfigure(0, weight=1)
        analysis_container.rowconfigure(0, weight=1)

        # 智能分析文本框
        self.analysis_text = scrolledtext.ScrolledText(
            analysis_container,
            font=("Consolas", 9),
            wrap=tk.WORD
        )
        self.analysis_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 按钮框架 - 放在分析文本框下方
        self.analysis_button_frame = ttk.Frame(analysis_container)
        self.analysis_button_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(5, 0))

        # 应用修正按钮
        self.apply_correction_btn = ttk.Button(
            self.analysis_button_frame,
            text="🔧 应用修正",
            command=self._apply_correction_from_analysis,
            state=tk.DISABLED
        )
        self.apply_correction_btn.pack(side=tk.LEFT, padx=(0, 5))

        # 重新检查按钮
        self.recheck_btn = ttk.Button(
            self.analysis_button_frame,
            text="🔄 重新检查",
            command=self._recheck_sql
        )
        self.recheck_btn.pack(side=tk.LEFT)

        # 日志标签页
        self.log_frame = ttk.Frame(self.result_notebook)
        self.result_notebook.add(self.log_frame, text="执行日志")
        self.log_frame.columnconfigure(0, weight=1)
        self.log_frame.rowconfigure(0, weight=1)

        # 日志文本框
        self.log_text = scrolledtext.ScrolledText(
            self.log_frame,
            font=("Consolas", 9),
            wrap=tk.WORD,
            state=tk.DISABLED
        )
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 查询历史区域
        self._create_history_area()

    def _create_result_table(self, parent):
        """创建结果表格"""
        # 创建主框架
        table_container = ttk.Frame(parent)
        table_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        table_container.columnconfigure(0, weight=1)
        table_container.rowconfigure(0, weight=1)

        # 创建Treeview表格
        self.result_tree = ttk.Treeview(table_container)
        self.result_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 添加滚动条
        v_scrollbar = ttk.Scrollbar(table_container, orient=tk.VERTICAL, command=self.result_tree.yview)
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.result_tree.configure(yscrollcommand=v_scrollbar.set)

        h_scrollbar = ttk.Scrollbar(table_container, orient=tk.HORIZONTAL, command=self.result_tree.xview)
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        self.result_tree.configure(xscrollcommand=h_scrollbar.set)

        # 配置网格权重，让表格可以扩展
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)
        table_container.columnconfigure(0, weight=1)
        table_container.rowconfigure(0, weight=1)

    def _create_history_area(self):
        """创建查询历史区域"""
        history_frame = ttk.LabelFrame(self.frame, text="查询历史", padding="5")
        history_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        history_frame.columnconfigure(0, weight=1)
        history_frame.rowconfigure(0, weight=1)

        # 历史列表
        self.history_listbox = tk.Listbox(
            history_frame,
            font=("Consolas", 9),
            selectmode=tk.SINGLE
        )
        self.history_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.history_listbox.bind('<Double-1>', self._load_history_query)

        # 历史滚动条
        history_scrollbar = ttk.Scrollbar(history_frame, orient=tk.VERTICAL, command=self.history_listbox.yview)
        history_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.history_listbox.configure(yscrollcommand=history_scrollbar.set)

        # 查询历史数据
        self.query_history = []

    def display_result(self, result):
        """显示查询结果"""
        # 清除之前的结果
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        try:
            if isinstance(result, list) and result:
                # 如果结果是字典列表，显示为表格
                if isinstance(result[0], dict):
                    # 设置列
                    columns = list(result[0].keys())
                    self.result_tree["columns"] = columns
                    self.result_tree["show"] = "headings"

                    # 设置列标题和宽度
                    for col in columns:
                        self.result_tree.heading(col, text=col)
                        # 根据列内容调整宽度
                        max_width = max(
                            len(str(col)),
                            max(len(str(row.get(col, ""))) for row in result[:10])  # 只检查前10行
                        )
                        self.result_tree.column(col, width=min(max_width * 8 + 20, 200))

                    # 插入数据
                    for i, row in enumerate(result):
                        values = [row.get(col, "") for col in columns]
                        # 为交替行添加不同的标签
                        tag = "evenrow" if i % 2 == 0 else "oddrow"
                        self.result_tree.insert("", tk.END, values=values, tags=(tag,))

                    # 配置行颜色
                    self.result_tree.tag_configure("evenrow", background="#f0f0f0")
                    self.result_tree.tag_configure("oddrow", background="white")

                else:
                    # 简单列表显示
                    self.result_tree["columns"] = ("result",)
                    self.result_tree["show"] = "headings"
                    self.result_tree.heading("result", text="结果")
                    self.result_tree.column("result", width=300)

                    for i, item in enumerate(result):
                        tag = "evenrow" if i % 2 == 0 else "oddrow"
                        self.result_tree.insert("", tk.END, values=(str(item),), tags=(tag,))
            else:
                # 单个结果或字符串结果
                self.result_tree["columns"] = ("result",)
                self.result_tree["show"] = "headings"
                self.result_tree.heading("result", text="结果")
                self.result_tree.column("result", width=300)
                self.result_tree.insert("", tk.END, values=(str(result),))

            # 切换到数据结果标签页
            self.result_notebook.select(self.data_frame)

        except Exception as e:
            # 如果显示结果时出错，在日志中记录
            self.log(f"显示结果时出错: {str(e)}")
            messagebox.showerror("显示错误", f"结果显示失败: {str(e)}")

    def update_execution_plan(self, plan):
        """更新执行计划显示"""
        self.plan_text.delete(1.0, tk.END)
        if isinstance(plan, dict):
            self.plan_text.insert(1.0, json.dumps(plan, indent=2, ensure_ascii=False))
        else:
            self.plan_text.insert(1.0, str(plan))

    def update_smart_analysis(self, analysis, success=None):
        """更新智能分析显示"""
        self.current_error_analysis = analysis
        self._update_analysis_text(analysis)

        # 控制应用修正按钮状态
        if analysis.get('corrected_sql_options'):
            self.apply_correction_btn.configure(state=tk.NORMAL)
        else:
            self.apply_correction_btn.configure(state=tk.DISABLED)

    def _update_analysis_text(self, analysis):
        """更新智能分析文本框内容"""
        self.analysis_text.delete(1.0, tk.END)

        content = "🧠 智能SQL分析报告\n" + "=" * 50 + "\n\n"

        # 基本信息
        content += f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        content += f"SQL语句: {analysis.get('original_sql', '').strip()}\n"

        # 检查是否为批量执行
        if analysis.get('batch_details'):
            batch_info = analysis['batch_details']
            content += f"执行类型: 批量执行\n"
            content += f"执行状态: {batch_info['successful']}/{batch_info['total']} 成功\n\n"

            # 显示失败语句详情
            if batch_info['failed_statements']:
                content += "❌ 失败语句详情:\n"
                for failed_stmt in batch_info['failed_statements']:
                    content += f"   语句 #{failed_stmt['index']}: {failed_stmt['sql'][:50]}...\n"
                    content += f"   错误: {failed_stmt['error']}\n\n"
        else:
            content += f"执行状态: {'成功' if not analysis.get('has_error') else '失败'}\n\n"

        # 错误分析
        if analysis.get('has_error') and analysis.get('error_message'):
            content += "❌ 错误信息:\n"
            content += f"   {analysis['error_message']}\n\n"

        # 错误建议
        if analysis.get('suggestions'):
            content += "💡 分析建议:\n"
            for i, suggestion in enumerate(analysis['suggestions'], 1):
                confidence_bar = "█" * int(suggestion['confidence'] * 10)
                content += f"{i}. {suggestion['description']}\n"
                content += f"   建议: {suggestion['suggestion']}\n"
                content += f"   置信度: {confidence_bar} ({suggestion['confidence']:.1%})\n\n"

        # 修正建议
        if analysis.get('corrected_sql_options'):
            content += "🔧 建议的修正版本:\n"
            for i, option in enumerate(analysis['corrected_sql_options'], 1):
                content += f"{i}. {option['description']} (置信度: {option['confidence']:.1%})\n"
                content += f"   修正SQL: {option['sql']}\n\n"

        # 改进建议
        if analysis.get('improvement_tips'):
            content += "💡 SQL 优化建议:\n"
            for i, tip in enumerate(analysis['improvement_tips'], 1):
                content += f"{i}. {tip['suggestion']}\n"

        if not any([analysis.get('suggestions'), analysis.get('corrected_sql_options'),
                    analysis.get('improvement_tips'), analysis.get('batch_details', {}).get('failed_statements')]):
            content += "✅ 未发现明显问题，SQL看起来不错！"

        self.analysis_text.insert(1.0, content)

    def _apply_correction_from_analysis(self):
        """从分析结果应用修正"""
        if not self.current_error_analysis or not self.current_error_analysis.get('corrected_sql_options'):
            messagebox.showinfo("提示", "没有可用的修正选项")
            return

        # 如果有多个修正选项，显示选择对话框
        if len(self.current_error_analysis['corrected_sql_options']) > 1:
            self._show_correction_options_dialog()
        else:
            # 只有一个修正选项，直接应用
            corrected_sql = self.current_error_analysis['corrected_sql_options'][0]['sql']
            self._apply_corrected_sql(corrected_sql)

    def _show_correction_options_dialog(self):
        """显示修正选项对话框"""
        dialog = tk.Toplevel(self.parent)
        dialog.title("🔧 选择修正选项")
        dialog.geometry("700x400")
        dialog.transient(self.parent)
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

        def apply_selected_correction():
            choice_idx = int(correction_choice.get())
            corrected_sql = self.current_error_analysis['corrected_sql_options'][choice_idx]['sql']
            dialog.destroy()
            self._apply_corrected_sql(corrected_sql)

        ttk.Button(
            button_frame,
            text="🚀 应用并执行",
            command=apply_selected_correction,
            style="Execute.TButton"
        ).pack(side=tk.RIGHT, padx=(5, 0))

        ttk.Button(button_frame, text="取消", command=dialog.destroy).pack(side=tk.RIGHT)

    def _apply_corrected_sql(self, corrected_sql):
        """应用修正后的SQL"""
        # 这里需要回调到SQL查询标签页来应用修正
        # 在实际实现中，应该通过回调函数或事件机制来处理
        messagebox.showinfo("应用修正", f"修正已准备好应用:\n\n{corrected_sql}")

    def _recheck_sql(self):
        """重新检查SQL"""
        # 这里需要回调到SQL查询标签页来重新检查
        pass

    def log(self, message):
        """记录日志"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"

        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def add_to_history(self, sql, execution_time, success=True, error_msg=None):
        """添加到查询历史"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        status_icon = "✅" if success else "❌"
        sql_preview = sql[:40] + "..." if len(sql) > 40 else sql

        history_entry_text = f"[{timestamp}] {status_icon} {sql_preview}"

        history_entry_data = {
            'sql': sql,
            'timestamp': timestamp,
            'execution_time': execution_time,
            'success': success,
            'error_msg': error_msg
        }

        self.query_history.insert(0, history_entry_data)
        self.history_listbox.insert(0, history_entry_text)

        # 限制历史记录数量
        if len(self.query_history) > 50:
            self.query_history.pop()
            self.history_listbox.delete(tk.END)

    def _load_history_query(self, event):
        """加载历史查询"""
        selection = self.history_listbox.curselection()
        if selection:
            index = selection[0]
            if index < len(self.query_history):
                history_item = self.query_history[index]
                sql = history_item['sql']

                # 显示历史详情
                details = f"时间: {history_item['timestamp']}\n"
                details += f"状态: {'成功' if history_item['success'] else '失败'}\n"
                if history_item['success']:
                    details += f"执行时间: {history_item['execution_time']:.3f}s\n"
                else:
                    details += f"错误: {history_item.get('error_msg', 'Unknown')}\n"
                details += f"SQL: {sql}"

                messagebox.showinfo("查询历史详情", details)

    def clear_analysis(self):
        """清除分析内容"""
        self.current_error_analysis = None
        self.analysis_text.delete(1.0, tk.END)
        self.apply_correction_btn.configure(state=tk.DISABLED)

    def show_analysis_tab(self):
        """显示智能分析标签页"""
        self.result_notebook.select(self.analysis_frame)

    def show_data_tab(self):
        """显示数据结果标签页"""
        self.result_notebook.select(self.data_frame)

    def show_plan_tab(self):
        """显示执行计划标签页"""
        self.result_notebook.select(self.plan_frame)

    def show_log_tab(self):
        """显示日志标签页"""
        self.result_notebook.select(self.log_frame)