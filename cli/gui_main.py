import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import json
from datetime import datetime
import traceback
from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer
from sql_compiler.parser.syntax_analyzer import SyntaxAnalyzer
from sql_compiler.codegen.plan_generator import PlanGenerator
from storage.core.page_manager import PageManager
from storage.core.buffer_pool import BufferPool
from storage.core.storage_manager import StorageManager
from sql_compiler.catalog.catalog_manager import CatalogManager
from engine.storage_engine import StorageEngine
from engine.execution_engine import ExecutionEngine
from sql_compiler.diagnostics.error_analyzer import SmartSQLCorrector


class SimpleDBGUI:
    def __init__(self):
        # 初始化数据库组件
        self._init_database()

        # 创建GUI
        self.root = tk.Tk()
        self.root.title("SimpleDB - SQL Database Management System with Smart Correction")
        self.root.geometry("1400x900")
        self.root.configure(bg="#f0f0f0")

        # 设置样式
        self.style = ttk.Style()
        self.style.theme_use("clam")

        # 创建界面组件
        self._create_widgets()

        # 执行历史
        self.query_history = []

        # 智能纠错相关变量
        self.correction_choice = tk.StringVar(value="none")
        self.current_error_analysis = None

    def _init_database(self):
        """初始化数据库组件"""
        try:
            # 初始化存储组件
            self.page_manager = PageManager()
            self.buffer_pool = BufferPool()
            self.storage_manager = StorageManager()

            # 初始化 TableStorage
            from storage.core.table_storage import TableStorage
            self.table_storage = TableStorage(self.storage_manager)

            # 初始化数据库引擎组件
            self.catalog_manager = CatalogManager()
            self.storage_engine = StorageEngine(
                storage_manager=self.storage_manager,
                table_storage=self.table_storage,
                catalog_manager=self.catalog_manager
            )
            self.execution_engine = ExecutionEngine(
                storage_engine=self.storage_engine,
                catalog_manager=self.catalog_manager
            )

            # 初始化SQL编译器组件
            self.lexer = LexicalAnalyzer

            # 初始化智能纠错器
            self.sql_corrector = SmartSQLCorrector(self.catalog_manager)

        except Exception as e:
            messagebox.showerror("初始化错误", f"数据库初始化失败: {str(e)}")

    def _create_widgets(self):
        """创建GUI组件"""
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(2, weight=1)

        # 标题
        title_label = ttk.Label(
            main_frame,
            text="SimpleDB - SQL Database Management System with Smart Correction",
            font=("Arial", 16, "bold")
        )
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 20))

        # 左侧面板
        left_panel = ttk.LabelFrame(main_frame, text="数据库操作", padding="10")
        left_panel.grid(row=1, column=0, rowspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        left_panel.columnconfigure(0, weight=1)

        # 右侧面板
        right_panel = ttk.Frame(main_frame)
        right_panel.grid(row=1, column=1, rowspan=2, sticky=(tk.W, tk.E, tk.N, tk.S))
        right_panel.columnconfigure(0, weight=1)
        right_panel.rowconfigure(1, weight=1)

        # SQL输入区域
        self._create_sql_input_area(left_panel)

        # 控制按钮
        self._create_control_buttons(left_panel)

        # 数据库信息区域
        self._create_database_info_area(left_panel)

        # 智能诊断区域
        self._create_smart_diagnosis_area(left_panel)

        # 结果显示区域
        self._create_result_area(right_panel)

        # 查询历史区域
        self._create_history_area(right_panel)

    def _create_sql_input_area(self, parent):
        """创建SQL输入区域"""
        # SQL输入标签
        sql_label = ttk.Label(parent, text="SQL查询:", font=("Arial", 12, "bold"))
        sql_label.grid(row=0, column=0, sticky=tk.W, pady=(0, 5))

        # SQL输入文本框
        self.sql_text = scrolledtext.ScrolledText(
            parent,
            height=8,
            width=50,
            font=("Consolas", 11),
            wrap=tk.WORD
        )
        self.sql_text.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))

        # 绑定快捷键
        self.sql_text.bind('<Control-Return>', lambda e: self._execute_sql())

        # 添加语法高亮（简单版本）
        self._setup_syntax_highlighting()

        # 示例SQL按钮
        self._create_example_buttons(parent)

    def _setup_syntax_highlighting(self):
        """设置简单的SQL语法高亮"""
        # 定义SQL关键字颜色
        self.sql_text.tag_configure("keyword", foreground="blue", font=("Consolas", 11, "bold"))
        self.sql_text.tag_configure("string", foreground="green")
        self.sql_text.tag_configure("number", foreground="red")
        self.sql_text.tag_configure("comment", foreground="gray")

    def _create_example_buttons(self, parent):
        """创建示例SQL按钮"""
        example_frame = ttk.LabelFrame(parent, text="示例SQL", padding="5")
        example_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        example_frame.columnconfigure(0, weight=1)
        example_frame.columnconfigure(1, weight=1)

        examples = [
            ("创建表", "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100));"),
            ("插入数据", "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');"),
            ("查询数据", "SELECT * FROM users;"),
            ("聚合查询", "SELECT city, COUNT(*) FROM customers GROUP BY city HAVING COUNT(*) > 1;"),
            ("更新数据", "UPDATE users SET email = 'newemail@example.com' WHERE id = 1;"),
            ("连接查询", "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id;"),
        ]

        for i, (name, sql) in enumerate(examples):
            btn = ttk.Button(
                example_frame,
                text=name,
                command=lambda s=sql: self._insert_example_sql(s),
                width=12
            )
            btn.grid(row=i // 2, column=i % 2, padx=2, pady=2, sticky=(tk.W, tk.E))

    def _insert_example_sql(self, sql):
        """插入示例SQL"""
        self.sql_text.delete(1.0, tk.END)
        self.sql_text.insert(1.0, sql)

    def _create_control_buttons(self, parent):
        """创建控制按钮"""
        button_frame = ttk.Frame(parent)
        button_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))

        # 执行按钮
        self.execute_btn = ttk.Button(
            button_frame,
            text="🚀 执行SQL",
            command=self._execute_sql,
            style="Execute.TButton"
        )
        self.execute_btn.grid(row=0, column=0, padx=(0, 5), sticky=(tk.W, tk.E))

        # 清除按钮
        clear_btn = ttk.Button(
            button_frame,
            text="🗑️ 清除",
            command=self._clear_sql
        )
        clear_btn.grid(row=0, column=1, padx=(5, 0), sticky=(tk.W, tk.E))

        # 智能检查按钮
        check_btn = ttk.Button(
            button_frame,
            text="🔍 智能检查",
            command=self._smart_check_sql
        )
        check_btn.grid(row=1, column=0, columnspan=2, pady=(5, 0), sticky=(tk.W, tk.E))

        # 配置按钮样式
        self.style.configure("Execute.TButton", font=("Arial", 10, "bold"))

        button_frame.columnconfigure(0, weight=1)
        button_frame.columnconfigure(1, weight=1)

    def _create_database_info_area(self, parent):
        """创建数据库信息区域"""
        info_frame = ttk.LabelFrame(parent, text="数据库信息", padding="5")
        info_frame.grid(row=4, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))

        # 状态信息
        self.status_label = ttk.Label(info_frame, text="状态: 就绪", foreground="green")
        self.status_label.grid(row=0, column=0, sticky=tk.W)

        # 表信息框架
        table_frame = ttk.Frame(info_frame)
        table_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(5, 0))

        # 表标签
        ttk.Label(table_frame, text="表:").grid(row=0, column=0, sticky=tk.W)

        # 表列表（可点击）
        self.tables_listbox = tk.Listbox(
            table_frame,
            height=4,
            width=30,
            font=("Consolas", 9),
            selectmode=tk.SINGLE
        )
        self.tables_listbox.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(2, 0))
        self.tables_listbox.bind('<Double-1>', self._show_table_details)

        # 刷新按钮
        refresh_btn = ttk.Button(
            info_frame,
            text="🔄 刷新信息",
            command=self._refresh_database_info
        )
        refresh_btn.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(5, 0))

        table_frame.columnconfigure(0, weight=1)

    def _create_smart_diagnosis_area(self, parent):
        """创建智能诊断区域"""
        diagnosis_frame = ttk.LabelFrame(parent, text="智能诊断", padding="5")
        diagnosis_frame.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        diagnosis_frame.columnconfigure(0, weight=1)

        # 诊断状态
        self.diagnosis_label = ttk.Label(diagnosis_frame, text="诊断状态: 待检查", foreground="gray")
        self.diagnosis_label.grid(row=0, column=0, sticky=tk.W)

        # 建议计数
        self.suggestion_label = ttk.Label(diagnosis_frame, text="建议: 0 项")
        self.suggestion_label.grid(row=1, column=0, sticky=tk.W)

        # 查看详情按钮
        self.details_btn = ttk.Button(
            diagnosis_frame,
            text="📋 查看诊断详情",
            command=self._show_diagnosis_details,
            state=tk.DISABLED
        )
        self.details_btn.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(5, 0))

    def _create_result_area(self, parent):
        """创建结果显示区域"""
        result_frame = ttk.LabelFrame(parent, text="执行结果", padding="5")
        result_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)

        # 创建Notebook来显示不同类型的结果
        self.result_notebook = ttk.Notebook(result_frame)
        self.result_notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 数据结果标签页
        self.data_frame = ttk.Frame(self.result_notebook)
        self.result_notebook.add(self.data_frame, text="数据结果")

        # 创建表格显示数据
        self._create_result_table(self.data_frame)

        # 执行计划标签页
        self.plan_frame = ttk.Frame(self.result_notebook)
        self.result_notebook.add(self.plan_frame, text="执行计划")

        # 执行计划文本框
        self.plan_text = scrolledtext.ScrolledText(
            self.plan_frame,
            font=("Consolas", 10),
            wrap=tk.WORD
        )
        self.plan_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.plan_frame.columnconfigure(0, weight=1)
        self.plan_frame.rowconfigure(0, weight=1)

        # 智能分析标签页
        self.analysis_frame = ttk.Frame(self.result_notebook)
        self.result_notebook.add(self.analysis_frame, text="智能分析")

        # 智能分析文本框
        self.analysis_text = scrolledtext.ScrolledText(
            self.analysis_frame,
            font=("Consolas", 9),
            wrap=tk.WORD
        )
        self.analysis_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.analysis_frame.columnconfigure(0, weight=1)
        self.analysis_frame.rowconfigure(0, weight=1)

        # 日志标签页
        self.log_frame = ttk.Frame(self.result_notebook)
        self.result_notebook.add(self.log_frame, text="执行日志")

        # 日志文本框
        self.log_text = scrolledtext.ScrolledText(
            self.log_frame,
            font=("Consolas", 9),
            wrap=tk.WORD,
            state=tk.DISABLED
        )
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.log_frame.columnconfigure(0, weight=1)
        self.log_frame.rowconfigure(0, weight=1)

    def _create_result_table(self, parent):
        """创建结果表格"""
        # 表格框架
        table_frame = ttk.Frame(parent)
        table_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        # 创建Treeview表格
        self.result_tree = ttk.Treeview(table_frame)
        self.result_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 添加滚动条
        v_scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.result_tree.yview)
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.result_tree.configure(yscrollcommand=v_scrollbar.set)

        h_scrollbar = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL, command=self.result_tree.xview)
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        self.result_tree.configure(xscrollcommand=h_scrollbar.set)

    def _create_history_area(self, parent):
        """创建查询历史区域"""
        history_frame = ttk.LabelFrame(parent, text="查询历史", padding="5")
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

    def _show_table_details(self, event):
        """显示表详细信息"""
        selection = self.tables_listbox.curselection()
        if not selection:
            return

        table_name = self.tables_listbox.get(selection[0])

        try:
            # 获取表详细信息
            tables_dict = self.catalog_manager.get_all_tables()
            table_info = tables_dict.get(table_name, {})

            # 创建详情对话框
            dialog = tk.Toplevel(self.root)
            dialog.title(f"表详细信息: {table_name}")
            dialog.geometry("800x600")
            dialog.transient(self.root)
            dialog.grab_set()

            main_frame = ttk.Frame(dialog, padding="10")
            main_frame.pack(fill=tk.BOTH, expand=True)

            # 基本信息
            info_frame = ttk.LabelFrame(main_frame, text="表基本信息", padding="10")
            info_frame.pack(fill=tk.X, pady=(0, 10))

            # 创建基本信息表格
            info_tree = ttk.Treeview(info_frame, columns=("property", "value"), show="tree", height=3)
            info_tree.heading("#0", text="属性")
            info_tree.heading("property", text="")
            info_tree.heading("value", text="值")

            # 添加基本信息
            info_tree.insert("", tk.END, text="表名", values=("", table_name))
            info_tree.insert("", tk.END, text="列数", values=("", str(len(table_info.get('columns', [])))))
            info_tree.insert("", tk.END, text="行数", values=("", str(table_info.get('rows', 0))))

            info_tree.pack(fill=tk.X)

            # 列信息
            columns_frame = ttk.LabelFrame(main_frame, text="列信息", padding="10")
            columns_frame.pack(fill=tk.BOTH, expand=True)

            # 创建列信息表格
            columns_tree = ttk.Treeview(columns_frame, columns=("name", "type", "nullable", "default"), show="headings")
            columns_tree.heading("name", text="列名")
            columns_tree.heading("type", text="类型")
            columns_tree.heading("nullable", text="可空")
            columns_tree.heading("default", text="默认值")

            # 设置列宽
            columns_tree.column("name", width=150)
            columns_tree.column("type", width=100)
            columns_tree.column("nullable", width=60)
            columns_tree.column("default", width=100)

            # 添加列信息（这里简化处理，实际应该从表信息中获取）
            columns = table_info.get('columns', [])
            for i, column in enumerate(columns):
                columns_tree.insert("", tk.END, values=(column, "VARCHAR", "YES", "NULL"))

            # 添加滚动条
            scrollbar = ttk.Scrollbar(columns_frame, orient=tk.VERTICAL, command=columns_tree.yview)
            columns_tree.configure(yscrollcommand=scrollbar.set)

            columns_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

            # 操作按钮
            button_frame = ttk.Frame(main_frame)
            button_frame.pack(fill=tk.X, pady=(10, 0))

            # 查看数据按钮
            ttk.Button(
                button_frame,
                text="📊 查看数据",
                command=lambda: self._view_table_data(table_name)
            ).pack(side=tk.LEFT, padx=(0, 5))

            # 生成查询按钮
            ttk.Button(
                button_frame,
                text="🔍 生成SELECT查询",
                command=lambda: self._generate_select_query(table_name)
            ).pack(side=tk.LEFT)

            ttk.Button(button_frame, text="关闭", command=dialog.destroy).pack(side=tk.RIGHT)

        except Exception as e:
            messagebox.showerror("错误", f"获取表信息失败: {str(e)}")

    def _view_table_data(self, table_name):
        """查看表数据"""
        try:
            # 生成SELECT查询
            query = f"SELECT * FROM {table_name};"
            self.sql_text.delete(1.0, tk.END)
            self.sql_text.insert(1.0, query)
            self._execute_sql()
        except Exception as e:
            messagebox.showerror("错误", f"查看表数据失败: {str(e)}")

    def _generate_select_query(self, table_name):
        """生成SELECT查询"""
        query = f"SELECT * FROM {table_name} WHERE condition;"
        self.sql_text.delete(1.0, tk.END)
        self.sql_text.insert(1.0, query)
        messagebox.showinfo("提示", f"已生成SELECT查询，请修改WHERE条件")

    def _execute_sql(self):
        """执行SQL语句"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句")
            return

        if not sql.endswith(';'):
            messagebox.showwarning("警告", "SQL语句必须以分号(;)结尾")
            return

        # 禁用执行按钮
        self.execute_btn.configure(state=tk.DISABLED, text="执行中...")
        self.status_label.configure(text="状态: 执行中...", foreground="orange")

        # 在单独线程中执行SQL，避免界面卡顿
        thread = threading.Thread(target=self._execute_sql_thread, args=(sql,))
        thread.daemon = True
        thread.start()

    def _execute_sql_thread(self, sql):
        """在单独线程中执行SQL - 集成智能纠错"""
        try:
            start_time = datetime.now()
            self._log(f"执行SQL: {sql}")

            try:
                # 尝试执行SQL
                result = self._execute_database_query(sql)
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()

                # 成功时也分析SQL以提供改进建议
                improvement_analysis = self.sql_corrector.analyze_and_suggest(sql)

                self.root.after(0, self._update_result_ui, result, sql, execution_time, improvement_analysis)

            except Exception as e:
                # 发生错误时进行智能分析
                error_analysis = self.sql_corrector.analyze_and_suggest(sql, e)
                self.root.after(0, self._update_error_ui_with_analysis, e, error_analysis)

        except Exception as e:
            self.root.after(0, self._update_error_ui, f"执行错误: {str(e)}")

    def _execute_database_query(self, sql):
        """执行数据库查询"""
        # 1. 词法分析
        lexer = self.lexer(sql)
        tokens = lexer.tokenize()

        # 2. 语法分析
        parser = SyntaxAnalyzer(tokens)
        ast = parser.parse()

        # 3. 生成执行计划
        planner = PlanGenerator(
            enable_optimization=True,
            silent_mode=True,  # GUI模式下不显示详细日志
            catalog_manager=self.catalog_manager
        )
        plan = planner.generate(ast)

        # 记录执行计划
        plan_json = json.dumps(plan.to_dict(), indent=2, ensure_ascii=False)
        self._update_execution_plan(plan_json)

        # 4. 执行计划
        result = self.execution_engine.execute_plan(plan)
        return result

    def _update_result_ui(self, result, sql, execution_time, improvement_analysis=None):
        """更新结果UI"""
        try:
            # 更新状态
            self.status_label.configure(text=f"状态: 执行完成 ({execution_time:.3f}s)", foreground="green")

            # 添加到历史
            self._add_to_history(sql, execution_time, True)

            # 更新结果显示
            self._display_result(result)

            # 记录成功日志
            self._log(f"执行成功，耗时: {execution_time:.3f}s")

            # 更新智能分析
            if improvement_analysis:
                self._update_smart_analysis(improvement_analysis, success=True)

        except Exception as e:
            self._log(f"UI更新错误: {str(e)}")
        finally:
            # 重新启用执行按钮
            self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _update_error_ui_with_analysis(self, error, analysis):
        """更新错误UI并显示智能分析"""
        # 更新状态
        self.status_label.configure(text="状态: 执行失败", foreground="red")

        # 记录错误日志
        self._log(f"执行失败: {str(error)}")

        # 添加到历史
        sql = self.sql_text.get(1.0, tk.END).strip()
        self._add_to_history(sql, 0, False, str(error))

        # 更新智能分析
        self._update_smart_analysis(analysis, success=False)

        # 显示错误分析对话框
        if analysis.get('suggestions') or analysis.get('corrected_sql_options'):
            self.root.after(100, lambda: self._show_error_analysis_dialog(analysis))
        else:
            messagebox.showerror("执行错误", str(error))

        # 重新启用执行按钮
        self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _update_error_ui(self, error_msg):
        """更新错误UI（简单版本）"""
        self.status_label.configure(text="状态: 执行失败", foreground="red")
        messagebox.showerror("执行错误", error_msg)
        self._log(error_msg)
        self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _smart_check_sql(self):
        """智能检查SQL（不执行）"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            messagebox.showinfo("提示", "请先输入SQL语句")
            return

        # 进行智能分析
        try:
            analysis = self.sql_corrector.analyze_and_suggest(sql)
            self._update_smart_analysis(analysis, success=None)

            if analysis.get('improvement_tips'):
                self._show_improvement_tips_dialog(analysis)
            else:
                messagebox.showinfo("智能检查", "未发现明显问题，SQL看起来不错！")

        except Exception as e:
            messagebox.showerror("智能检查失败", f"分析过程出错: {str(e)}")

    def _update_smart_analysis(self, analysis, success=None):
        """更新智能分析显示"""
        self.current_error_analysis = analysis

        # 更新诊断标签
        if success is True:
            self.diagnosis_label.configure(text="诊断状态: ✅ 执行成功", foreground="green")
        elif success is False:
            self.diagnosis_label.configure(text="诊断状态: ❌ 执行失败", foreground="red")
        else:
            self.diagnosis_label.configure(text="诊断状态: 🔍 已分析", foreground="blue")

        # 统计建议数量
        suggestion_count = 0
        if analysis.get('suggestions'):
            suggestion_count += len(analysis['suggestions'])
        if analysis.get('improvement_tips'):
            suggestion_count += len(analysis['improvement_tips'])
        if analysis.get('corrected_sql_options'):
            suggestion_count += len(analysis['corrected_sql_options'])

        self.suggestion_label.configure(text=f"建议: {suggestion_count} 项")

        # 启用详情按钮
        if suggestion_count > 0:
            self.details_btn.configure(state=tk.NORMAL)
        else:
            self.details_btn.configure(state=tk.DISABLED)

        # 更新智能分析文本框
        self._update_analysis_text(analysis)

    def _update_analysis_text(self, analysis):
        """更新智能分析文本框内容"""
        self.analysis_text.delete(1.0, tk.END)

        content = "🧠 智能SQL分析报告\n" + "=" * 50 + "\n\n"

        # 基本信息
        content += f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        content += f"SQL语句: {analysis.get('original_sql', '').strip()}\n"
        content += f"执行状态: {'成功' if not analysis.get('has_error') else '失败'}\n\n"

        # 错误分析
        if analysis.get('has_error') and analysis.get('error_message'):
            content += "❌ 错误信息:\n"
            content += f"   {analysis['error_message']}\n\n"

        # 错误建议
        if analysis.get('suggestions'):
            content += "💡 错误分析和建议:\n"
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

        if not any(
                [analysis.get('suggestions'), analysis.get('corrected_sql_options'), analysis.get('improvement_tips')]):
            content += "✅ 未发现明显问题，SQL看起来不错！"

        self.analysis_text.insert(1.0, content)

    def _show_diagnosis_details(self):
        """显示诊断详情"""
        if self.current_error_analysis:
            if self.current_error_analysis.get('has_error'):
                self._show_error_analysis_dialog(self.current_error_analysis)
            else:
                self._show_improvement_tips_dialog(self.current_error_analysis)
        else:
            messagebox.showinfo("提示", "暂无诊断信息")

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

    def _show_improvement_tips_dialog(self, analysis):
        """显示改进建议对话框"""
        dialog = tk.Toplevel(self.root)
        dialog.title("💡 SQL 优化建议")
        dialog.geometry("600x400")
        dialog.transient(self.root)
        dialog.grab_set()

        main_frame = ttk.Frame(dialog, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 标题
        ttk.Label(main_frame, text="SQL 优化建议", font=("Arial", 14, "bold")).pack(pady=(0, 10))

        # 建议列表
        tips_frame = ttk.LabelFrame(main_frame, text="优化建议", padding="10")
        tips_frame.pack(fill=tk.BOTH, expand=True)

        tips_text = scrolledtext.ScrolledText(tips_frame, wrap=tk.WORD, font=("Consolas", 10))
        tips_text.pack(fill=tk.BOTH, expand=True)

        content = ""
        if analysis.get('improvement_tips'):
            for i, tip in enumerate(analysis['improvement_tips'], 1):
                content += f"{i}. {tip['suggestion']}\n\n"
        else:
            content = "✅ 未发现明显的改进点，SQL看起来不错！"

        tips_text.insert(1.0, content)
        tips_text.configure(state=tk.DISABLED)

        # 关闭按钮
        ttk.Button(main_frame, text="关闭", command=dialog.destroy).pack(pady=(10, 0))

    def _show_correction_options(self, parent_dialog, analysis):
        """显示修正选项对话框"""
        parent_dialog.destroy()  # 关闭父对话框

        dialog = tk.Toplevel(self.root)
        dialog.title("🔧 SQL 修正选项")
        dialog.geometry("900x600")
        dialog.transient(self.root)
        dialog.grab_set()

        main_frame = ttk.Frame(dialog, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 说明标签
        instruction_label = ttk.Label(
            main_frame,
            text="发现可能的SQL修正版本，请选择要使用的修正：",
            font=("Arial", 12, "bold")
        )
        instruction_label.pack(pady=(0, 15))

        # 选项框架
        options_frame = ttk.LabelFrame(main_frame, text="修正选项", padding="10")
        options_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 15))

        # 创建滚动框架
        canvas = tk.Canvas(options_frame)
        scrollbar = ttk.Scrollbar(options_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # 单选按钮变量
        self.correction_choice = tk.StringVar(value="none")

        # "不使用修正"选项
        no_correction_frame = ttk.Frame(scrollable_frame)
        no_correction_frame.pack(fill=tk.X, pady=5)

        ttk.Radiobutton(
            no_correction_frame,
            text="❌ 不使用修正，返回原始错误",
            variable=self.correction_choice,
            value="none"
        ).pack(anchor=tk.W)

        # 分隔线
        ttk.Separator(scrollable_frame, orient='horizontal').pack(fill=tk.X, pady=10)

        # 修正选项
        for i, option in enumerate(analysis['corrected_sql_options']):
            option_frame = ttk.LabelFrame(scrollable_frame, text=f"修正选项 {i + 1}", padding="10")
            option_frame.pack(fill=tk.X, pady=5)

            # 单选按钮
            option_text = f"✅ {option['description']} (置信度: {option['confidence']:.1%})"
            ttk.Radiobutton(
                option_frame,
                text=option_text,
                variable=self.correction_choice,
                value=str(i)
            ).pack(anchor=tk.W)

            # 显示修正后的SQL
            sql_label = ttk.Label(option_frame, text="修正后的SQL:", font=("Arial", 10, "bold"))
            sql_label.pack(anchor=tk.W, pady=(10, 5))

            sql_text = tk.Text(
                option_frame,
                height=3,
                wrap=tk.WORD,
                font=("Consolas", 9),
                bg="#f8f8f8",
                relief=tk.SUNKEN,
                bd=1
            )
            sql_text.pack(fill=tk.X, pady=(0, 5))
            sql_text.insert(1.0, option['sql'])
            sql_text.configure(state=tk.DISABLED)

            # 显示置信度条
            confidence_frame = ttk.Frame(option_frame)
            confidence_frame.pack(fill=tk.X, pady=(5, 0))

            confidence_label = ttk.Label(confidence_frame, text="置信度:")
            confidence_label.pack(side=tk.LEFT)

            # 简单的置信度条
            progress = ttk.Progressbar(
                confidence_frame,
                length=200,
                mode='determinate',
                value=option['confidence'] * 100
            )
            progress.pack(side=tk.LEFT, padx=(5, 0))

            confidence_text = ttk.Label(confidence_frame, text=f"{option['confidence']:.1%}")
            confidence_text.pack(side=tk.LEFT, padx=(5, 0))

        # 按钮框架
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        def apply_correction():
            choice = self.correction_choice.get()
            dialog.destroy()

            if choice != "none":
                choice_idx = int(choice)
                corrected_sql = analysis['corrected_sql_options'][choice_idx]['sql']

                # 将修正后的SQL放入输入框
                self.sql_text.delete(1.0, tk.END)
                self.sql_text.insert(1.0, corrected_sql)

                # 显示确认对话框
                result = messagebox.askyesno(
                    "应用修正",
                    f"修正已应用到SQL输入框。\n\n修正后的SQL:\n{corrected_sql}\n\n是否立即执行？"
                )

                if result:
                    self._execute_sql()
                else:
                    messagebox.showinfo("提示", "修正已应用，可以手动执行或进一步编辑")

        # 按钮
        ttk.Button(
            button_frame,
            text="🚀 应用并执行",
            command=apply_correction,
            style="Execute.TButton"
        ).pack(side=tk.RIGHT, padx=(5, 0))

        ttk.Button(button_frame, text="取消", command=dialog.destroy).pack(side=tk.RIGHT)

    def _display_result(self, result):
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
            self._log(f"显示结果时出错: {str(e)}")
            messagebox.showerror("显示错误", f"结果显示失败: {str(e)}")

    def _update_execution_plan(self, plan_json):
        """更新执行计划显示"""
        self.plan_text.delete(1.0, tk.END)
        self.plan_text.insert(1.0, plan_json)

    def _log(self, message):
        """记录日志"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"

        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _add_to_history(self, sql, execution_time, success=True, error_msg=None):
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
                self.sql_text.delete(1.0, tk.END)
                self.sql_text.insert(1.0, sql)

                # 显示历史详情
                details = f"时间: {history_item['timestamp']}\n"
                details += f"状态: {'成功' if history_item['success'] else '失败'}\n"
                if history_item['success']:
                    details += f"执行时间: {history_item['execution_time']:.3f}s\n"
                else:
                    details += f"错误: {history_item.get('error_msg', 'Unknown')}\n"
                details += f"SQL: {sql}"

                messagebox.showinfo("查询历史详情", details)

    def _clear_sql(self):
        """清除SQL输入"""
        self.sql_text.delete(1.0, tk.END)
        # 清除智能分析
        self.current_error_analysis = None
        self.diagnosis_label.configure(text="诊断状态: 待检查", foreground="gray")
        self.suggestion_label.configure(text="建议: 0 项")
        self.details_btn.configure(state=tk.DISABLED)
        self.analysis_text.delete(1.0, tk.END)

    def _refresh_database_info(self):
        """刷新数据库信息"""
        try:
            # 获取表信息字典
            tables_dict = {}
            try:
                if hasattr(self.catalog_manager, 'get_all_tables'):
                    tables_dict = self.catalog_manager.get_all_tables()
            except Exception as e:
                self._log(f"获取表信息时出错: {str(e)}")
                raise e

            # 清空表列表
            self.tables_listbox.delete(0, tk.END)

            if tables_dict and isinstance(tables_dict, dict):
                # 获取表名列表
                table_names = list(tables_dict.keys())

                # 添加到列表框中
                for table_name in table_names:
                    self.tables_listbox.insert(tk.END, table_name)

                self._log(f"成功获取 {len(table_names)} 张表")

            else:
                self._log("未找到表信息")

            self.status_label.configure(text="状态: 就绪", foreground="green")
            self._log("数据库信息已刷新")

        except Exception as e:
            self._log(f"刷新信息失败: {str(e)}")
            messagebox.showerror("错误", f"刷新数据库信息失败: {str(e)}")

    def run(self):
        """启动GUI"""
        # 初始化刷新数据库信息
        self._refresh_database_info()

        # 添加欢迎日志
        self._log("SimpleDB GUI 已启动")
        self._log("🧠 智能SQL纠错功能已启用")
        self._log("💡 可以使用 Ctrl+Enter 快捷键执行SQL")
        self._log("🔍 点击'智能检查'按钮可以在执行前分析SQL")
        self._log("📋 双击表名可以查看表详细信息")

        # 启动主循环
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        self.root.mainloop()

    def _on_closing(self):
        """关闭程序时的处理"""
        try:
            # 关闭数据库连接
            self.storage_manager.shutdown()
            self._log("数据库连接已关闭")
        except Exception as e:
            print(f"关闭时出错: {e}")
        finally:
            self.root.destroy()


def main():
    """GUI主函数"""
    try:
        app = SimpleDBGUI()
        app.run()
    except Exception as e:
        messagebox.showerror("启动错误", f"应用程序启动失败: {str(e)}")
        print(f"详细错误信息: {traceback.format_exc()}")


if __name__ == "__main__":
    main()