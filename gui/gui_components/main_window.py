import tkinter as tk
from tkinter import ttk, messagebox
from .sql_query_tab import SQLQueryTab
from .nl_query_tab import NLQueryTab
from .plan_visualization_tab import PlanVisualizationTab


class MainWindow:
    def __init__(self, database_manager, ai_manager):
        self.database_manager = database_manager
        self.ai_manager = ai_manager

        # 创建GUI
        self.root = tk.Tk()
        self.root.title("SimpleDB - SQL Database Management System with Smart Correction")
        self.root.geometry("1400x900")
        self.root.configure(bg="#f0f0f0")

        # 设置样式
        self.style = ttk.Style()
        self.style.theme_use("clam")

        # 智能纠错相关变量
        self.correction_choice = tk.StringVar(value="none")
        self.current_error_analysis = None

        # 创建界面组件
        self._create_widgets()

    def _create_widgets(self):
        """创建GUI组件"""
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 标题
        title_label = ttk.Label(
            main_frame,
            text="SimpleDB - SQL Database Management System with Smart Correction",
            font=("Arial", 16, "bold")
        )
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 20))

        # 创建标签页
        self.main_notebook = ttk.Notebook(main_frame)
        self.main_notebook.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=10)

        # 创建各个标签页
        self.sql_query_tab = SQLQueryTab(self.main_notebook, self.database_manager, self.ai_manager)
        self.nl_query_tab = NLQueryTab(self.main_notebook, self.database_manager, self.ai_manager)
        self.plan_visualization_tab = PlanVisualizationTab(self.main_notebook, self.ai_manager)

        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(1, weight=1)

    def run(self):
        """启动GUI"""
        # 初始化刷新数据库信息
        self.sql_query_tab.refresh_database_info()

        # 添加欢迎日志
        self.sql_query_tab.log("SimpleDB GUI 已启动")
        self.sql_query_tab.log("🧠 智能SQL纠错功能已启用")
        self.sql_query_tab.log("💡 可以使用 Ctrl+Enter 快捷键执行SQL")
        self.sql_query_tab.log("🔍 点击'智能检查'按钮可以在执行前分析SQL")
        self.sql_query_tab.log("📋 双击表名可以查看表详细信息")

        # 启动主循环
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        self.root.mainloop()

    def _on_closing(self):
        """关闭程序时的处理"""
        try:
            # 关闭数据库连接
            self.database_manager.shutdown()
            self.sql_query_tab.log("数据库连接已关闭")
        except Exception as e:
            print(f"关闭时出错: {e}")
        finally:
            self.root.destroy()