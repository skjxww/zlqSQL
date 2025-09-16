import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from datetime import datetime
import json
from sql_compiler.diagnostics.error_analyzer import SmartSQLCorrector


class ResultDisplay:
    def __init__(self, parent):
        self.parent = parent
        self.frame = ttk.Frame(parent)
        self.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

        # 添加智能纠错器引用
        self.sql_corrector = None
        self.current_error_analysis = None
        self.sql_query_callback = None  # 用于回调到SQL查询标签页

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

        # 优化过程标签页
        self.optimization_frame = ttk.Frame(self.result_notebook)
        self.result_notebook.add(self.optimization_frame, text="优化过程")
        self.optimization_frame.columnconfigure(0, weight=1)
        self.optimization_frame.rowconfigure(0, weight=1)

        # 优化过程文本框
        self.optimization_text = scrolledtext.ScrolledText(
            self.optimization_frame,
            font=("Consolas", 10),
            wrap=tk.WORD,
            background="#f8f9fa"
        )
        self.optimization_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 查询历史区域
        self._create_history_area()

        # 配置文本样式
        self._configure_text_styles()

    def set_sql_corrector(self, corrector):
        """设置智能纠错器"""
        self.sql_corrector = corrector

    def set_sql_query_callback(self, callback):
        """设置SQL查询回调函数"""
        self.sql_query_callback = callback

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

    def update_smart_analysis(self, analysis, success=None, original_sql=None, error=None):
        """更新智能分析显示"""
        self.analysis_text.configure(state=tk.NORMAL)
        self.analysis_text.delete(1.0, tk.END)

        if analysis:
            # 检查分析结果的结构
            suggestions = None
            corrected_options = None
            improvement_tips = None

            if isinstance(analysis, dict):
                suggestions = analysis.get('suggestions', [])
                corrected_options = analysis.get('corrected_sql_options', [])
                improvement_tips = analysis.get('improvement_tips', [])
            else:
                suggestions = getattr(analysis, 'suggestions', [])
                corrected_options = getattr(analysis, 'corrected_sql_options', [])
                improvement_tips = getattr(analysis, 'improvement_tips', [])

            # 如果有错误建议或改进建议，显示分析结果
            if suggestions or improvement_tips:
                self.current_error_analysis = {
                    'original_sql': original_sql,
                    'error': error,
                    'suggestions': suggestions,
                    'corrected_sql_options': corrected_options,
                    'improvement_tips': improvement_tips
                }

                # 显示分析结果
                self.analysis_text.insert(tk.END, "🔍 智能分析结果\n", "header")
                self.analysis_text.insert(tk.END, "=" * 50 + "\n\n")

                # 显示错误分析建议
                if suggestions:
                    self.analysis_text.insert(tk.END, "❌ 错误分析:\n", "section_header")
                    for i, suggestion in enumerate(suggestions, 1):
                        self._display_suggestion(suggestion, i)

                # 显示改进建议
                if improvement_tips:
                    if suggestions:  # 如果前面有错误分析，添加分隔符
                        self.analysis_text.insert(tk.END, "\n" + "=" * 30 + "\n\n")

                    self.analysis_text.insert(tk.END, "💡 性能优化建议:\n", "section_header")
                    for i, tip in enumerate(improvement_tips, 1):
                        self._display_suggestion(tip, i, is_improvement=True)

                # 启用或禁用应用修正按钮
                if corrected_options or any(self._get_corrected_sql(s) for s in suggestions + improvement_tips):
                    self.apply_correction_btn.configure(state=tk.NORMAL)
                else:
                    self.apply_correction_btn.configure(state=tk.DISABLED)
            else:
                self.analysis_text.insert(tk.END, "✅ 智能分析完成\n", "info")
                self.analysis_text.insert(tk.END, "未发现明显的问题或可优化的地方。\n")
                self.apply_correction_btn.configure(state=tk.DISABLED)

        elif success is True:
            self.analysis_text.insert(tk.END, "✅ SQL语句检查通过\n", "success")
            self.analysis_text.insert(tk.END, "未发现明显的语法或逻辑错误。\n")
            self.apply_correction_btn.configure(state=tk.DISABLED)

        else:
            self.analysis_text.insert(tk.END, "❌ 无法进行智能分析\n", "error")
            if error:
                self.analysis_text.insert(tk.END, f"错误信息: {error}\n")
            self.apply_correction_btn.configure(state=tk.DISABLED)

        self.analysis_text.configure(state=tk.DISABLED)

    def _display_suggestion(self, suggestion, index, is_improvement=False):
        """显示单个建议"""
        # 处理建议项
        if isinstance(suggestion, dict):
            suggestion_type = suggestion.get('type', suggestion.get('error_type', '未知'))
            description = suggestion.get('description', '')
            suggestion_text = suggestion.get('suggestion', '')
            confidence = suggestion.get('confidence', 0.0)
            corrected_sql = suggestion.get('corrected_sql', '')
        else:
            suggestion_type = getattr(suggestion, 'type', getattr(suggestion, 'error_type', '未知'))
            description = getattr(suggestion, 'description', '')
            suggestion_text = getattr(suggestion, 'suggestion', '')
            confidence = getattr(suggestion, 'confidence', 0.0)
            corrected_sql = getattr(suggestion, 'corrected_sql', '')

        icon = "💡" if is_improvement else "📋"
        self.analysis_text.insert(tk.END, f"{icon} 建议 {index}: {description}\n", "suggestion_title")
        self.analysis_text.insert(tk.END, f"   类型: {suggestion_type}\n")
        self.analysis_text.insert(tk.END, f"   建议: {suggestion_text}\n")
        self.analysis_text.insert(tk.END, f"   置信度: {'█' * int(confidence * 10)} ({confidence:.1%})\n")

        if corrected_sql:
            self.analysis_text.insert(tk.END, "   🔧 修正SQL:\n", "corrected_sql")
            self.analysis_text.insert(tk.END, f"   {corrected_sql}\n", "sql_code")

        self.analysis_text.insert(tk.END, "\n")

    def _get_corrected_sql(self, suggestion):
        """获取建议中的修正SQL"""
        if isinstance(suggestion, dict):
            return suggestion.get('corrected_sql', '')
        else:
            return getattr(suggestion, 'corrected_sql', '')

    def _configure_text_styles(self):
        """配置文本样式"""
        self.analysis_text.tag_configure("header", font=("Consolas", 12, "bold"), foreground="blue")
        self.analysis_text.tag_configure("section_header", font=("Consolas", 11, "bold"), foreground="purple")
        self.analysis_text.tag_configure("suggestion_title", font=("Consolas", 10, "bold"), foreground="darkgreen")
        self.analysis_text.tag_configure("corrected_sql", font=("Consolas", 10, "bold"), foreground="green")
        self.analysis_text.tag_configure("sql_code", font=("Consolas", 9), background="#f0f0f0")
        self.analysis_text.tag_configure("success", font=("Consolas", 11, "bold"), foreground="green")
        self.analysis_text.tag_configure("error", font=("Consolas", 11, "bold"), foreground="red")
        self.analysis_text.tag_configure("info", font=("Consolas", 11, "bold"), foreground="blue")

    def _apply_correction_from_analysis(self):
        """从分析结果应用修正"""
        if not self.current_error_analysis:
            messagebox.showwarning("无修正选项", "当前没有可用的修正选项")
            return

        # 收集所有有修正SQL的建议
        all_corrections = []

        # 从错误建议中收集
        suggestions = self.current_error_analysis.get('suggestions', [])
        for suggestion in suggestions:
            corrected_sql = self._get_corrected_sql(suggestion)
            if corrected_sql:
                description = suggestion.get('description', '') if isinstance(suggestion, dict) else getattr(suggestion, 'description', '')
                confidence = suggestion.get('confidence', 0.0) if isinstance(suggestion, dict) else getattr(suggestion, 'confidence', 0.0)
                all_corrections.append({
                    'sql': corrected_sql,
                    'description': description,
                    'confidence': confidence
                })

        # 从改进建议中收集
        improvement_tips = self.current_error_analysis.get('improvement_tips', [])
        for tip in improvement_tips:
            corrected_sql = self._get_corrected_sql(tip)
            if corrected_sql:
                description = tip.get('description', '') if isinstance(tip, dict) else getattr(tip, 'description', '')
                confidence = tip.get('confidence', 0.0) if isinstance(tip, dict) else getattr(tip, 'confidence', 0.0)
                all_corrections.append({
                    'sql': corrected_sql,
                    'description': description,
                    'confidence': confidence
                })

        # 从预设的修正选项中收集
        corrected_options = self.current_error_analysis.get('corrected_sql_options', [])
        all_corrections.extend(corrected_options)

        if not all_corrections:
            messagebox.showwarning("无修正选项", "当前没有可用的修正选项")
            return

        if len(all_corrections) == 1:
            # 只有一个选项，直接应用
            self._apply_corrected_sql(all_corrections[0]['sql'])
        else:
            # 多个选项，显示选择对话框
            self._show_correction_dialog(all_corrections)

    def _show_correction_dialog(self, corrections):
        """显示修正选择对话框"""
        dialog = tk.Toplevel(self.frame)
        dialog.title("选择修正方案")
        dialog.geometry("800x600")
        dialog.transient(self.frame.winfo_toplevel())
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
        for i, option in enumerate(corrections):
            option_frame = ttk.LabelFrame(options_frame, text=f"选项 {i + 1}", padding="5")
            option_frame.pack(fill=tk.X, pady=2)

            ttk.Radiobutton(
                option_frame,
                text=f"{option['description']} (置信度: {option['confidence']:.1%})",
                variable=correction_choice,
                value=str(i)
            ).pack(anchor=tk.W)

            # SQL预览
            sql_text = tk.Text(option_frame, height=3, font=("Consolas", 9), wrap=tk.WORD)
            sql_text.pack(fill=tk.X, pady=(5, 0))
            sql_text.insert(1.0, option['sql'])
            sql_text.configure(state=tk.DISABLED)

        # 按钮框架
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        def apply_selected_correction():
            choice_idx = int(correction_choice.get())
            corrected_sql = corrections[choice_idx]['sql']
            dialog.destroy()
            self._apply_corrected_sql(corrected_sql)

        ttk.Button(
            button_frame,
            text="🚀 应用并执行",
            command=apply_selected_correction
        ).pack(side=tk.RIGHT, padx=(5, 0))

        ttk.Button(button_frame, text="取消", command=dialog.destroy).pack(side=tk.RIGHT)

    def _apply_corrected_sql(self, corrected_sql):
        """应用修正后的SQL"""
        if self.sql_query_callback:
            # 回调到SQL查询标签页应用修正
            self.sql_query_callback('apply_correction', corrected_sql)
        else:
            messagebox.showinfo("应用修正", f"修正已准备好应用:\n\n{corrected_sql}")

    def _recheck_sql(self):
        """重新检查SQL"""
        if self.sql_query_callback:
            # 回调到SQL查询标签页重新检查
            self.sql_query_callback('recheck', None)
        else:
            messagebox.showinfo("提示", "无法重新检查，请在SQL查询标签页中手动检查")

    def display_error_with_analysis(self, error_msg, sql=None):
        """显示错误并进行智能分析"""
        # 显示错误
        self.log(f"❌ 执行错误: {error_msg}")

        # 如果有SQL和纠错器，进行智能分析
        if sql and self.sql_corrector:
            try:
                analysis = self.sql_corrector.analyze_and_suggest(sql, Exception(error_msg))
                self.update_smart_analysis(analysis, success=False, original_sql=sql, error=error_msg)

                # 切换到智能分析标签页
                self.result_notebook.select(2)  # 智能分析是第3个标签页（索引2）

            except Exception as e:
                self.log(f"智能分析失败: {str(e)}")
                self.update_smart_analysis(None, success=False, error=f"分析失败: {str(e)}")

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
        self.analysis_text.configure(state=tk.NORMAL)
        self.analysis_text.delete(1.0, tk.END)
        self.analysis_text.configure(state=tk.DISABLED)
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

    def clear_optimization_process(self):
        """清除优化过程显示"""
        self.optimization_text.configure(state=tk.NORMAL)
        self.optimization_text.delete(1.0, tk.END)
        self.optimization_text.configure(state=tk.DISABLED)

    def update_optimization_process(self, optimization_output):
        """更新优化过程显示"""
        self.optimization_text.configure(state=tk.NORMAL)
        self.optimization_text.delete(1.0, tk.END)

        if optimization_output:
            # 直接显示捕获的输出
            self.optimization_text.insert(tk.END, optimization_output)
        else:
            self.optimization_text.insert(tk.END, "暂无优化过程信息")

        self.optimization_text.configure(state=tk.DISABLED)

    def show_optimization_tab(self):
        """显示优化过程标签页"""
        self.result_notebook.select(self.optimization_frame)