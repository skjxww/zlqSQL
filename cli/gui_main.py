import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import json
from datetime import datetime
from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer
from sql_compiler.parser.syntax_analyzer import SyntaxAnalyzer
from sql_compiler.codegen.plan_generator import PlanGenerator
from storage.core.page_manager import PageManager
from storage.core.buffer_pool import BufferPool
from storage.core.storage_manager import StorageManager
from sql_compiler.catalog.catalog_manager import CatalogManager
from engine.storage_engine import StorageEngine
from engine.execution_engine import ExecutionEngine


class SimpleDBGUI:
    def __init__(self):
        # 初始化数据库组件
        self._init_database()

        # 创建GUI
        self.root = tk.Tk()
        self.root.title("SimpleDB - SQL Database Management System")
        self.root.geometry("1400x900")
        self.root.configure(bg="#f8f9fa")

        # 设置应用程序图标（如果有的话）
        # self.root.iconbitmap("icon.ico")

        # 设置样式
        self.style = ttk.Style()
        self.style.theme_use("clam")
        self._configure_styles()

        # 创建界面组件
        self._create_widgets()

        # 执行历史
        self.query_history = []

    def _configure_styles(self):
        """配置自定义样式"""
        # 配置颜色方案 - 现代蓝色主题
        self.colors = {
            'primary': '#4a6fa5',  # 主色调 - 深蓝色
            'primary_light': '#6d8fc7',  # 浅蓝色
            'secondary': '#5bb98c',  # 辅助色 - 绿色
            'accent': '#ff7e5f',  # 强调色 - 珊瑚色
            'danger': '#e74c3c',  # 危险色 - 红色
            'warning': '#f39c12',  # 警告色 - 橙色
            'dark': '#2c3e50',  # 深色文字
            'light': '#ecf0f1',  # 浅色背景
            'background': '#f8f9fa',  # 主背景色
            'text': '#34495e',  # 文本颜色
            'border': '#dce4ec',  # 边框颜色
            'highlight': '#e9ecef',  # 高亮背景
            'success': '#27ae60'  # 成功颜色
        }

        # 配置样式
        self.style.configure('TFrame', background=self.colors['background'])
        self.style.configure('TLabel', background=self.colors['background'], foreground=self.colors['text'])
        self.style.configure('TButton', padding=8, font=('Segoe UI', 10))
        self.style.configure('Title.TLabel', font=('Segoe UI', 18, 'bold'), foreground=self.colors['primary'])
        self.style.configure('Section.TLabelframe',
                             font=('Segoe UI', 12, 'bold'),
                             foreground=self.colors['dark'],
                             background=self.colors['background'],
                             borderwidth=2,
                             relief=tk.GROOVE)
        self.style.configure('Section.TLabelframe.Label',
                             font=('Segoe UI', 11, 'bold'),
                             foreground=self.colors['primary'])

        # 按钮样式
        self.style.configure('Primary.TButton',
                             background=self.colors['primary'],
                             foreground='white',
                             borderwidth=1,
                             focusthickness=3,
                             focuscolor=self.colors['primary_light'])
        self.style.map('Primary.TButton',
                       background=[('active', self.colors['primary_light']),
                                   ('pressed', '#3a5a84')])

        self.style.configure('Secondary.TButton',
                             background=self.colors['secondary'],
                             foreground='white')
        self.style.map('Secondary.TButton',
                       background=[('active', '#6dca9e'),
                                   ('pressed', '#4a966e')])

        self.style.configure('Accent.TButton',
                             background=self.colors['accent'],
                             foreground='white')
        self.style.map('Accent.TButton',
                       background=[('active', '#ff9b82'),
                                   ('pressed', '#cc654c')])

        self.style.configure('Danger.TButton',
                             background=self.colors['danger'],
                             foreground='white')
        self.style.map('Danger.TButton',
                       background=[('active', '#ff6b6b'),
                                   ('pressed', '#c0392b')])

        # 输入框样式
        self.style.configure('Custom.TEntry',
                             fieldbackground='white',
                             borderwidth=1,
                             relief=tk.SOLID,
                             padding=5)

        # 树形视图样式
        self.style.configure('Custom.Treeview',
                             background="white",
                             fieldbackground="white",
                             foreground=self.colors['text'],
                             rowheight=28,
                             font=('Segoe UI', 10))
        self.style.configure('Custom.Treeview.Heading',
                             background=self.colors['primary'],
                             foreground="white",
                             padding=8,
                             font=('Segoe UI', 10, 'bold'))
        self.style.map('Custom.Treeview.Heading',
                       background=[('active', self.colors['primary_light'])])

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

        except Exception as e:
            messagebox.showerror("初始化错误", f"数据库初始化失败: {str(e)}")

    def _create_widgets(self):
        """创建GUI组件"""
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(1, weight=1)

        # 标题区域
        title_frame = ttk.Frame(main_frame)
        title_frame.grid(row=0, column=0, columnspan=2, pady=(0, 20), sticky=(tk.W, tk.E))
        title_frame.columnconfigure(0, weight=1)

        # 标题
        title_label = ttk.Label(
            title_frame,
            text="SimpleDB - SQL Database Management System",
            style="Title.TLabel"
        )
        title_label.grid(row=0, column=0, pady=(0, 5))

        # 副标题
        subtitle_label = ttk.Label(
            title_frame,
            text="轻量级SQL数据库管理系统",
            font=('Segoe UI', 12),
            foreground=self.colors['text']
        )
        subtitle_label.grid(row=1, column=0)

        # 左侧面板
        left_panel = ttk.LabelFrame(main_frame, text="数据库操作", padding="15", style="Section.TLabelframe")
        left_panel.grid(row=1, column=0, rowspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 15))
        left_panel.columnconfigure(0, weight=1)
        left_panel.rowconfigure(1, weight=1)

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

        # 结果显示区域
        self._create_result_area(right_panel)

        # 查询历史区域
        self._create_history_area(right_panel)

        # 状态栏
        self._create_status_bar(main_frame)

    def _create_sql_input_area(self, parent):
        """创建SQL输入区域"""
        # SQL输入标签
        sql_label = ttk.Label(parent, text="SQL查询:", font=("Segoe UI", 11, "bold"))
        sql_label.grid(row=0, column=0, sticky=tk.W, pady=(0, 10))

        # SQL输入文本框
        sql_frame = ttk.Frame(parent)
        sql_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        sql_frame.columnconfigure(0, weight=1)

        self.sql_text = scrolledtext.ScrolledText(
            sql_frame,
            height=10,
            width=50,
            font=("Consolas", 11),
            wrap=tk.WORD,
            relief=tk.SOLID,
            borderwidth=1,
            padx=12,
            pady=12,
            bg='white',
            fg=self.colors['text'],
            insertbackground=self.colors['primary']  # 光标颜色
        )
        self.sql_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 添加语法高亮（简单版本）
        self._setup_syntax_highlighting()

        # 示例SQL按钮
        self._create_example_buttons(parent)

    def _setup_syntax_highlighting(self):
        """设置简单的SQL语法高亮"""
        # 定义SQL关键字颜色
        self.sql_text.tag_configure("keyword", foreground=self.colors['primary'], font=("Consolas", 11, "bold"))
        self.sql_text.tag_configure("string", foreground=self.colors['accent'])
        self.sql_text.tag_configure("number", foreground=self.colors['warning'])
        self.sql_text.tag_configure("comment", foreground="#95a5a6", font=("Consolas", 11, "italic"))
        self.sql_text.tag_configure("function", foreground=self.colors['secondary'], font=("Consolas", 11, "bold"))

    def _create_example_buttons(self, parent):
        """创建示例SQL按钮"""
        example_frame = ttk.LabelFrame(parent, text="示例查询", padding="10", style="Section.TLabelframe")
        example_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        example_frame.columnconfigure((0, 1, 2), weight=1)

        examples = [
            ("创建表", "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100));"),
            ("插入数据", "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');"),
            ("查询数据", "SELECT * FROM users;"),
            ("条件查询", "SELECT name, email FROM users WHERE id > 100;"),
            ("聚合查询", "SELECT city, COUNT(*) FROM customers GROUP BY city HAVING COUNT(*) > 1;"),
            ("连接查询", "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;"),
        ]

        for i, (name, sql) in enumerate(examples):
            btn = ttk.Button(
                example_frame,
                text=name,
                command=lambda s=sql: self._insert_example_sql(s),
                style="Secondary.TButton",
                width=15
            )
            btn.grid(row=i // 3, column=i % 3, padx=5, pady=5, sticky=(tk.W, tk.E))

    def _insert_example_sql(self, sql):
        """插入示例SQL"""
        self.sql_text.delete(1.0, tk.END)
        self.sql_text.insert(1.0, sql)

    def _create_control_buttons(self, parent):
        """创建控制按钮"""
        button_frame = ttk.Frame(parent)
        button_frame.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        button_frame.columnconfigure((0, 1, 2), weight=1)

        # 执行按钮
        self.execute_btn = ttk.Button(
            button_frame,
            text="🚀 执行SQL",
            command=self._execute_sql,
            style="Primary.TButton"
        )
        self.execute_btn.grid(row=0, column=0, padx=(0, 5), sticky=(tk.W, tk.E))

        # 清除按钮
        clear_btn = ttk.Button(
            button_frame,
            text="🗑️ 清除",
            command=self._clear_sql,
            style="Danger.TButton"
        )
        clear_btn.grid(row=0, column=1, padx=5, sticky=(tk.W, tk.E))

        # 格式化按钮
        format_btn = ttk.Button(
            button_frame,
            text="✨ 格式化",
            command=self._format_sql,
            style="Accent.TButton"
        )
        format_btn.grid(row=0, column=2, padx=(5, 0), sticky=(tk.W, tk.E))

    def _create_database_info_area(self, parent):
        """创建数据库信息区域"""
        info_frame = ttk.LabelFrame(parent, text="数据库状态", padding="12", style="Section.TLabelframe")
        info_frame.grid(row=4, column=0, sticky=(tk.W, tk.E))
        info_frame.columnconfigure(0, weight=1)

        # 状态信息
        status_frame = ttk.Frame(info_frame)
        status_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

        ttk.Label(status_frame, text="状态:", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky=tk.W)
        self.status_label = ttk.Label(status_frame, text="就绪", foreground=self.colors['success'],
                                      font=("Segoe UI", 10))
        self.status_label.grid(row=0, column=1, sticky=tk.W, padx=(8, 0))

        # 表数量信息
        table_frame = ttk.Frame(info_frame)
        table_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

        ttk.Label(table_frame, text="表数量:", font=("Segoe UI", 10)).grid(row=0, column=0, sticky=tk.W)
        self.table_count_label = ttk.Label(table_frame, text="0", font=("Segoe UI", 10))
        self.table_count_label.grid(row=0, column=1, sticky=tk.W, padx=(8, 0))

        # 数据库大小信息
        size_frame = ttk.Frame(info_frame)
        size_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(0, 15))

        ttk.Label(size_frame, text="数据库大小:", font=("Segoe UI", 10)).grid(row=0, column=0, sticky=tk.W)
        self.db_size_label = ttk.Label(size_frame, text="0 MB", font=("Segoe UI", 10))
        self.db_size_label.grid(row=0, column=1, sticky=tk.W, padx=(8, 0))

        # 刷新按钮
        refresh_btn = ttk.Button(
            info_frame,
            text="🔄 刷新状态",
            command=self._refresh_database_info,
            style="Secondary.TButton"
        )
        refresh_btn.grid(row=3, column=0, sticky=(tk.W, tk.E))

    def _create_result_area(self, parent):
        """创建结果显示区域"""
        result_frame = ttk.LabelFrame(parent, text="执行结果", padding="10", style="Section.TLabelframe")
        result_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 15))
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)

        # 创建Notebook来显示不同类型的结果
        self.result_notebook = ttk.Notebook(result_frame)
        self.result_notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 数据结果标签页
        self.data_frame = ttk.Frame(self.result_notebook, padding="8")
        self.result_notebook.add(self.data_frame, text="📊 数据结果")
        self.data_frame.columnconfigure(0, weight=1)
        self.data_frame.rowconfigure(0, weight=1)

        # 创建表格显示数据
        self._create_result_table(self.data_frame)

        # 执行计划标签页
        self.plan_frame = ttk.Frame(self.result_notebook, padding="8")
        self.result_notebook.add(self.plan_frame, text="📋 执行计划")
        self.plan_frame.columnconfigure(0, weight=1)
        self.plan_frame.rowconfigure(0, weight=1)

        # 执行计划文本框
        self.plan_text = scrolledtext.ScrolledText(
            self.plan_frame,
            font=("Consolas", 10),
            wrap=tk.WORD,
            relief=tk.SOLID,
            borderwidth=1,
            padx=10,
            pady=10,
            bg='#fcfcfc'
        )
        self.plan_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 日志标签页
        self.log_frame = ttk.Frame(self.result_notebook, padding="8")
        self.result_notebook.add(self.log_frame, text="📝 执行日志")
        self.log_frame.columnconfigure(0, weight=1)
        self.log_frame.rowconfigure(0, weight=1)

        # 日志文本框
        self.log_text = scrolledtext.ScrolledText(
            self.log_frame,
            font=("Consolas", 9),
            wrap=tk.WORD,
            state=tk.DISABLED,
            relief=tk.SOLID,
            borderwidth=1,
            padx=10,
            pady=10,
            bg='#fcfcfc'
        )
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    def _create_result_table(self, parent):
        """创建结果表格"""
        # 表格框架
        table_frame = ttk.Frame(parent)
        table_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        # 创建Treeview表格
        self.result_tree = ttk.Treeview(table_frame, style="Custom.Treeview")
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
        history_frame = ttk.LabelFrame(parent, text="📚 查询历史", padding="10", style="Section.TLabelframe")
        history_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        history_frame.columnconfigure(0, weight=1)
        history_frame.rowconfigure(0, weight=1)

        # 历史列表框架
        history_list_frame = ttk.Frame(history_frame)
        history_list_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        history_list_frame.columnconfigure(0, weight=1)
        history_list_frame.rowconfigure(0, weight=1)

        # 历史列表
        self.history_listbox = tk.Listbox(
            history_list_frame,
            font=("Segoe UI", 9),
            selectmode=tk.SINGLE,
            relief=tk.SOLID,
            borderwidth=1,
            highlightthickness=0,
            bg='white',
            fg=self.colors['text'],
            selectbackground=self.colors['primary_light'],
            selectforeground="white"
        )
        self.history_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.history_listbox.bind('<Double-1>', self._load_history_query)

        # 历史滚动条
        history_scrollbar = ttk.Scrollbar(history_list_frame, orient=tk.VERTICAL, command=self.history_listbox.yview)
        history_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.history_listbox.configure(yscrollcommand=history_scrollbar.set)

        # 历史操作按钮
        history_btn_frame = ttk.Frame(history_frame)
        history_btn_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(10, 0))
        history_btn_frame.columnconfigure((0, 1), weight=1)

        clear_history_btn = ttk.Button(
            history_btn_frame,
            text="清除历史",
            command=self._clear_history,
            style="Danger.TButton"
        )
        clear_history_btn.grid(row=0, column=0, padx=(0, 5), sticky=(tk.W, tk.E))

        export_history_btn = ttk.Button(
            history_btn_frame,
            text="导出历史",
            command=self._export_history,
            style="Secondary.TButton"
        )
        export_history_btn.grid(row=0, column=1, padx=(5, 0), sticky=(tk.W, tk.E))

    def _create_status_bar(self, parent):
        """创建状态栏"""
        status_bar = ttk.Frame(parent, relief=tk.SUNKEN, borderwidth=1)
        status_bar.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(15, 0))
        status_bar.columnconfigure(0, weight=1)

        self.status_message = ttk.Label(
            status_bar,
            text="就绪 | 欢迎使用SimpleDB数据库管理系统",
            font=('Segoe UI', 9),
            foreground=self.colors['text']
        )
        self.status_message.grid(row=0, column=0, sticky=tk.W, padx=10, pady=3)

        # 版本信息
        version_label = ttk.Label(
            status_bar,
            text="v1.0.0",
            font=('Segoe UI', 9),
            foreground=self.colors['text']
        )
        version_label.grid(row=0, column=1, sticky=tk.E, padx=10, pady=3)

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
        self.status_label.configure(text="执行中...", foreground=self.colors['warning'])
        self.status_message.configure(text="正在执行SQL查询...")

        # 在单独线程中执行SQL，避免界面卡顿
        thread = threading.Thread(target=self._execute_sql_thread, args=(sql,))
        thread.daemon = True
        thread.start()

    def _execute_sql_thread(self, sql):
        """在单独线程中执行SQL"""
        try:
            start_time = datetime.now()

            # 记录到日志
            self._log(f"执行SQL: {sql}")

            # 执行SQL
            result = self._execute_database_query(sql)

            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()

            # 在主线程中更新UI
            self.root.after(0, self._update_result_ui, result, sql, execution_time)

        except Exception as e:
            error_msg = f"执行错误: {str(e)}"
            self.root.after(0, self._update_error_ui, error_msg)

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

    def _update_result_ui(self, result, sql, execution_time):
        """更新结果UI"""
        try:
            # 更新状态
            self.status_label.configure(text=f"执行完成 ({execution_time:.3f}s)", foreground=self.colors['success'])
            self.status_message.configure(text=f"SQL执行成功，耗时: {execution_time:.3f}秒")

            # 添加到历史
            self._add_to_history(sql, execution_time)

            # 更新结果显示
            self._display_result(result)

            # 记录成功日志
            self._log(f"执行成功，耗时: {execution_time:.3f}s")

        except Exception as e:
            self._log(f"UI更新错误: {str(e)}")
        finally:
            # 重新启用执行按钮
            self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _update_error_ui(self, error_msg):
        """更新错误UI"""
        # 更新状态
        self.status_label.configure(text="执行失败", foreground=self.colors['danger'])
        self.status_message.configure(text="SQL执行失败，请查看日志")

        # 显示错误消息
        messagebox.showerror("执行错误", error_msg)

        # 记录错误日志
        self._log(error_msg)

        # 重新启用执行按钮
        self.execute_btn.configure(state=tk.NORMAL, text="🚀 执行SQL")

    def _display_result(self, result):
        """显示查询结果"""
        # 清除之前的结果
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        if isinstance(result, list) and result:
            # 如果结果是字典列表，显示为表格
            if isinstance(result[0], dict):
                # 设置列
                columns = list(result[0].keys())
                self.result_tree["columns"] = columns
                self.result_tree["show"] = "headings"

                # 设置列标题
                for col in columns:
                    self.result_tree.heading(col, text=col)
                    self.result_tree.column(col, width=120, minwidth=80)

                # 插入数据
                for row in result:
                    values = [row.get(col, "") for col in columns]
                    self.result_tree.insert("", tk.END, values=values)
            else:
                # 简单列表显示
                self.result_tree["columns"] = ("result",)
                self.result_tree["show"] = "headings"
                self.result_tree.heading("result", text="结果")

                for item in result:
                    self.result_tree.insert("", tk.END, values=(str(item),))
        else:
            # 单个结果或字符串结果
            self.result_tree["columns"] = ("result",)
            self.result_tree["show"] = "headings"
            self.result_tree.heading("result", text="结果")
            self.result_tree.insert("", tk.END, values=(str(result),))

        # 切换到数据结果标签页
        self.result_notebook.select(self.data_frame)

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

    def _add_to_history(self, sql, execution_time):
        """添加到查询历史"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        history_entry = f"[{timestamp}] ({execution_time:.2f}s) {sql[:50]}{'...' if len(sql) > 50 else ''}"

        self.query_history.append({
            'sql': sql,
            'timestamp': timestamp,
            'execution_time': execution_time
        })

        self.history_listbox.insert(0, history_entry)

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
                sql = self.query_history[index]['sql']
                self.sql_text.delete(1.0, tk.END)
                self.sql_text.insert(1.0, sql)

    def _clear_sql(self):
        """清除SQL输入"""
        self.sql_text.delete(1.0, tk.END)

    def _format_sql(self):
        """格式化SQL语句"""
        sql = self.sql_text.get(1.0, tk.END).strip()
        if not sql:
            return

        # 简单的SQL格式化（可以替换为更复杂的格式化库）
        formatted_sql = sql.upper()
        keywords = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES',
                    'UPDATE', 'SET', 'DELETE', 'CREATE', 'TABLE', 'DROP',
                    'ALTER', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
                    'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET']

        for keyword in keywords:
            formatted_sql = formatted_sql.replace(keyword, f"\n{keyword} ")

        formatted_sql = formatted_sql.replace(',', ',\n\t')

        self.sql_text.delete(1.0, tk.END)
        self.sql_text.insert(1.0, formatted_sql.strip())

    def _clear_history(self):
        """清除查询历史"""
        self.query_history.clear()
        self.history_listbox.delete(0, tk.END)
        self._log("查询历史已清除")

    def _export_history(self):
        """导出查询历史"""
        # 这里可以实现导出历史到文件的功能
        messagebox.showinfo("导出历史", "导出功能将在未来版本中实现")

    def _refresh_database_info(self):
        """刷新数据库信息"""
        try:
            # 这里可以添加获取数据库状态信息的代码
            table_count = len(self.catalog_manager.get_all_tables())
            self.table_count_label.configure(text=str(table_count))
            self.status_label.configure(text="就绪", foreground=self.colors['success'])
            self.status_message.configure(text="数据库状态已刷新")
            self._log("数据库信息已刷新")
        except Exception as e:
            self._log(f"刷新信息失败: {str(e)}")

    def run(self):
        """启动GUI"""
        # 添加欢迎日志
        self._log("SimpleDB GUI 已启动")
        self._log("请在左侧输入SQL语句并点击执行")

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


if __name__ == "__main__":
    main()