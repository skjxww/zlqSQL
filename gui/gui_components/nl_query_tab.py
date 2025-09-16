import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import traceback
from typing import Dict, Optional


class NLQueryTab:
    def __init__(self, parent_notebook, nl2sql_engine, sql_tab_callback=None):
        self.nl2sql_engine = nl2sql_engine
        self.sql_tab_callback = sql_tab_callback  # 回调函数，用于将SQL传递到SQL标签页
        self.sql_tab_instance = None  # 直接引用SQL标签页实例

        # 创建自然语言查询标签页
        self.frame = ttk.Frame(parent_notebook)
        parent_notebook.add(self.frame, text="🤖 自然语言查询")

        # 创建界面组件
        self._create_widgets()

    def _create_widgets(self):
        """创建自然语言查询界面组件"""
        # 输入区域
        input_frame = ttk.LabelFrame(self.frame, text="自然语言输入", padding="10")
        input_frame.pack(fill=tk.X, padx=10, pady=5)

        # 查询输入框
        self.nl_query_var = tk.StringVar()
        nl_entry = ttk.Entry(input_frame, textvariable=self.nl_query_var, font=("Arial", 12))
        nl_entry.pack(fill=tk.X, pady=(0, 10))
        nl_entry.bind('<Return>', lambda e: self._process_nl_query())

        # 执行和清空按钮
        action_frame = ttk.Frame(input_frame)
        action_frame.pack(fill=tk.X)

        ttk.Button(action_frame, text="🚀 生成SQL", command=self._process_nl_query).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(action_frame, text="📋 清空", command=self._clear_nl_query).pack(side=tk.LEFT)

        # 结果显示区域
        result_frame = ttk.LabelFrame(self.frame, text="转换结果", padding="10")
        result_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # 创建结果显示的Notebook
        self.nl_result_notebook = ttk.Notebook(result_frame)
        self.nl_result_notebook.pack(fill=tk.BOTH, expand=True)

        # SQL结果标签页
        sql_result_frame = ttk.Frame(self.nl_result_notebook)
        self.nl_result_notebook.add(sql_result_frame, text="生成的SQL")

        self.nl_sql_text = scrolledtext.ScrolledText(sql_result_frame, height=8, font=("Consolas", 10))
        self.nl_sql_text.pack(fill=tk.BOTH, expand=True)

        # 分析结果标签页
        analysis_frame = ttk.Frame(self.nl_result_notebook)
        self.nl_result_notebook.add(analysis_frame, text="分析详情")

        self.nl_analysis_text = scrolledtext.ScrolledText(analysis_frame, height=8)
        self.nl_analysis_text.pack(fill=tk.BOTH, expand=True)

        # 操作按钮
        nl_action_frame = ttk.Frame(result_frame)
        nl_action_frame.pack(fill=tk.X, pady=(10, 0))

        self.copy_sql_btn = ttk.Button(nl_action_frame, text="📋 复制SQL",
                                       command=self._copy_generated_sql, state=tk.DISABLED)
        self.copy_sql_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.copy_to_sql_btn = ttk.Button(nl_action_frame, text="📄 复制到SQL页",
                                          command=self._copy_to_sql_tab, state=tk.DISABLED)
        self.copy_to_sql_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.execute_generated_btn = ttk.Button(nl_action_frame, text="▶️ 执行SQL",
                                                command=self._execute_generated_sql, state=tk.DISABLED)
        self.execute_generated_btn.pack(side=tk.LEFT)


    def _process_nl_query(self):
        """处理自然语言查询"""
        if not self.nl2sql_engine:
            messagebox.showwarning("功能不可用", "自然语言转SQL功能未初始化")
            return

        natural_query = self.nl_query_var.get().strip()
        if not natural_query:
            messagebox.showinfo("提示", "请输入自然语言查询")
            return

        try:
            # 显示处理状态
            self.nl_sql_text.delete(1.0, tk.END)
            self.nl_analysis_text.delete(1.0, tk.END)
            self.nl_sql_text.insert(tk.END, "🔄 正在处理自然语言查询...")
            self.frame.update()

            # 禁用按钮
            self._set_buttons_state(tk.DISABLED)

            # 调用转换
            result = self.nl2sql_engine.translate(natural_query)

            # 显示结果
            self._display_nl_result(natural_query, result)

        except Exception as e:
            self.nl_sql_text.delete(1.0, tk.END)
            self.nl_sql_text.insert(tk.END, f"❌ 处理失败: {str(e)}")
            self.nl_analysis_text.delete(1.0, tk.END)
            self.nl_analysis_text.insert(tk.END, f"错误详情:\n{traceback.format_exc()}")
            self._set_buttons_state(tk.DISABLED)

    def _display_nl_result(self, query, result):
        """显示自然语言转换结果"""
        # 显示生成的SQL
        self.nl_sql_text.delete(1.0, tk.END)
        if result.get('sql'):
            self.nl_sql_text.insert(tk.END, result['sql'])
            self._set_buttons_state(tk.NORMAL)

            # 自动切换到SQL结果标签页
            self.nl_result_notebook.select(0)
        else:
            self.nl_sql_text.insert(tk.END, "未能生成SQL语句")
            self._set_buttons_state(tk.DISABLED)

        # 显示分析详情
        self._update_analysis_display(query, result)

    def _update_analysis_display(self, query, result):
        """更新分析显示"""
        self.nl_analysis_text.delete(1.0, tk.END)
        analysis_content = f"🔍 查询: {query}\n\n"

        # 基本信息
        analysis_content += f"📊 置信度: {result.get('confidence', 0):.1%}\n"
        analysis_content += f"🔧 方法: {result.get('method', '未知')}\n"
        analysis_content += f"⏱️ 处理时间: {result.get('processing_time', 0):.3f}s\n\n"

        # 详细分析
        if result.get('explanation'):
            analysis_content += f"📝 解释:\n{result['explanation']}\n\n"

        if result.get('reasoning'):
            analysis_content += f"🤔 推理过程:\n{result['reasoning']}\n\n"

        if result.get('tables_used'):
            analysis_content += f"📋 涉及表: {', '.join(result['tables_used'])}\n\n"

        if result.get('suggestions'):
            analysis_content += "💡 优化建议:\n"
            for suggestion in result['suggestions']:
                analysis_content += f"  • {suggestion}\n"
            analysis_content += "\n"

        if result.get('warnings'):
            analysis_content += "⚠️ 注意事项:\n"
            for warning in result['warnings']:
                analysis_content += f"  • {warning}\n"
            analysis_content += "\n"

        if result.get('error'):
            analysis_content += f"❌ 错误信息:\n{result['error']}\n"

        self.nl_analysis_text.insert(tk.END, analysis_content)

    def _set_buttons_state(self, state):
        """设置按钮状态"""
        self.copy_sql_btn.configure(state=state)
        self.copy_to_sql_btn.configure(state=state)
        self.execute_generated_btn.configure(state=state)

    def _copy_generated_sql(self):
        """复制生成的SQL"""
        sql_content = self.nl_sql_text.get(1.0, tk.END).strip()
        if sql_content:
            self.frame.clipboard_clear()
            self.frame.clipboard_append(sql_content)
            messagebox.showinfo("复制成功", "SQL已复制到剪贴板")

    def _execute_generated_sql(self):
        """执行生成的SQL"""
        sql_content = self.nl_sql_text.get(1.0, tk.END).strip()
        if sql_content:
            # 通过回调函数将SQL传递到SQL查询标签页并执行
            if self.sql_tab_instance:
                self.sql_tab_instance.receive_sql_from_nl_tab(sql_content, execute=True)
            elif callable(self.sql_tab_callback):
                self.sql_tab_callback(sql_content, execute=True)
            else:
                messagebox.showwarning("功能限制", "执行功能需要与SQL查询标签页集成")

    def _copy_to_sql_tab(self):
        """复制到SQL查询标签页"""
        sql_content = self.nl_sql_text.get(1.0, tk.END).strip()
        if sql_content:
            # 通过回调函数将SQL传递到SQL查询标签页
            if self.sql_tab_instance:
                self.sql_tab_instance.receive_sql_from_nl_tab(sql_content, execute=False)
            elif callable(self.sql_tab_callback):
                self.sql_tab_callback(sql_content, execute=False)
            else:
                messagebox.showwarning("功能限制", "复制功能需要与SQL查询标签页集成")

    def _clear_nl_query(self):
        """清空自然语言查询"""
        self.nl_query_var.set("")
        self.nl_sql_text.delete(1.0, tk.END)
        self.nl_analysis_text.delete(1.0, tk.END)
        self._set_buttons_state(tk.DISABLED)

    # 集成方法
    def set_sql_tab_instance(self, sql_tab_instance):
        """设置SQL标签页实例的直接引用"""
        self.sql_tab_instance = sql_tab_instance

    def set_sql_tab_callback(self, callback):
        """设置SQL标签页回调函数"""
        self.sql_tab_callback = callback

    def update_nl2sql_engine(self, nl2sql_engine):
        """更新NL2SQL引擎"""
        self.nl2sql_engine = nl2sql_engine

    def get_current_query(self):
        """获取当前查询"""
        return self.nl_query_var.get()

    def get_generated_sql(self):
        """获取生成的SQL"""
        return self.nl_sql_text.get(1.0, tk.END).strip()