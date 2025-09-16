from gui_components.sql_query_tab import SQLQueryTab
from gui_components.nl_query_tab import NLQueryTab
from gui_components.plan_visualization_tab import PlanVisualizationTab
from gui_components.database_info import DatabaseInfoPanel
from gui_components.result_display import ResultDisplay
from core.database_manager import DatabaseManager
from core.ai_features import AIFeatureManager
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import traceback
from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer
from storage.core.page_manager import PageManager
from storage.core.buffer_pool import BufferPool
from storage.core.storage_manager import StorageManager
from catalog.catalog_manager import CatalogManager
from engine.storage_engine import StorageEngine
from engine.execution_engine import ExecutionEngine
from sql_compiler.diagnostics.error_analyzer import SmartSQLCorrector
try:
    from cli.storage_monitor_gui import StorageMonitorWindow
    STORAGE_MONITOR_AVAILABLE = True
except ImportError as e:
    STORAGE_MONITOR_AVAILABLE = False


class SimpleDBGUI:
    def __init__(self):
        # 初始化数据库组件
        self._init_database()
        # 初始化数据库组件
        self.db_manager = DatabaseManager()

        # 初始化AI功能
        self.ai_manager = AIFeatureManager(self.db_manager.catalog_manager)

        # 创建GUI
        self.root = tk.Tk()
        self.root.title("SimpleDB - SQL Database Management System")
        self.root.geometry("1400x900")
        self.root.configure(bg="#f0f0f0")

        # 设置样式
        self.style = ttk.Style()
        self.style.theme_use("clam")

        # 创建主框架
        self._create_main_frame()

        # 初始化组件
        self._init_components()

        # 执行历史
        self.query_history = []

        # 添加存储监控窗口引用
        self.storage_monitor_window = None

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

            # 🔧 修复：设置事务管理器
            # 确保存储引擎有事务管理器，然后设置给执行引擎
            if hasattr(self.storage_engine, 'transaction_manager'):
                self.execution_engine.set_transaction_manager(self.storage_engine.transaction_manager)
            else:
                # 如果存储引擎没有事务管理器，创建一个新的
                from storage.core.transaction_manager import TransactionManager
                transaction_manager = TransactionManager(self.storage_manager)
                self.execution_engine.set_transaction_manager(transaction_manager)

            # 初始化SQL编译器组件
            self.lexer = LexicalAnalyzer

            # 初始化智能纠错器
            self.sql_corrector = SmartSQLCorrector(self.catalog_manager)

        except Exception as e:
            messagebox.showerror("初始化错误", f"数据库初始化失败: {str(e)}")

    def _create_main_frame(self):
        """创建主框架"""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 标题
        title_label = ttk.Label(
            main_frame,
            text="SimpleDB - SQL Database Management System",
            font=("Arial", 16, "bold")
        )
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 20))

        # 新增：工具栏
        toolbar_frame = ttk.Frame(main_frame)
        toolbar_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))

        # 存储监控按钮
        if STORAGE_MONITOR_AVAILABLE:
            monitor_btn = ttk.Button(
                toolbar_frame,
                text="📊 存储监控",
                command=self._open_storage_monitor
            )
            monitor_btn.pack(side=tk.LEFT, padx=(0, 10))

        # 左侧面板 - 输入区域
        self.left_panel = ttk.LabelFrame(main_frame, text="查询输入", padding="10")
        self.left_panel.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        self.left_panel.columnconfigure(0, weight=1)
        self.left_panel.rowconfigure(1, weight=1)  # 让Notebook可以扩展

        # 右侧面板 - 输出区域
        self.right_panel = ttk.LabelFrame(main_frame, text="查询结果", padding="10")
        self.right_panel.grid(row=2, column=1, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.right_panel.columnconfigure(0, weight=1)
        self.right_panel.rowconfigure(0, weight=1)

        # 配置网格权重
        main_frame.columnconfigure(0, weight=1)  # 左侧列
        main_frame.columnconfigure(1, weight=2)  # 右侧列更宽
        main_frame.rowconfigure(2, weight=1)     # 主内容行
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

    def _init_components(self):
        """初始化各个组件"""
        # 在左侧面板创建Notebook用于不同类型的查询输入
        self.input_notebook = ttk.Notebook(self.left_panel)
        self.input_notebook.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))
        self.left_panel.rowconfigure(1, weight=1)
        self.left_panel.columnconfigure(0, weight=1)

        # SQL查询标签页
        self.sql_tab = SQLQueryTab(self.input_notebook, self.db_manager, self.ai_manager, None)
        self.input_notebook.add(self.sql_tab.frame, text="📝 SQL查询")

        # 自然语言查询标签页
        self.nl_tab = NLQueryTab(self.input_notebook, self.ai_manager.nl2sql_engine, self.sql_tab)
        self.input_notebook.add(self.nl_tab.frame, text="🤖 自然语言查询")

        # 执行计划可视化标签页
        self.plan_tab = PlanVisualizationTab(self.input_notebook, ai_manager=self.ai_manager)
        self.input_notebook.add(self.plan_tab.frame, text="📊 执行计划可视化")

        # 数据库信息面板 (放在左侧面板顶部)
        self.db_info = DatabaseInfoPanel(self.left_panel, self.db_manager)
        self.db_info.frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

        # 结果显示组件 (放在右侧面板)
        self.result_display = ResultDisplay(self.right_panel)
        self.result_display.frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.right_panel.rowconfigure(0, weight=1)
        self.right_panel.columnconfigure(0, weight=1)

        # 更新SQL查询标签页的结果显示引用
        self.sql_tab.result_display = self.result_display

    def run(self):
        """启动GUI"""
        try:
            self.db_info.refresh_info()
            self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
            self.root.mainloop()
        except Exception as e:
            messagebox.showerror("启动错误", f"应用程序启动失败: {str(e)}")

    def _on_closing(self):
        """关闭程序时的处理"""
        try:
            # 关闭存储监控窗口
            if self.storage_monitor_window is not None:
                try:
                    if hasattr(self.storage_monitor_window, 'window') and self.storage_monitor_window.window:
                        self.storage_monitor_window._stop_monitoring()
                        self.storage_monitor_window.window.destroy()
                except:
                    pass
            self.db_manager.shutdown()
        finally:
            self.root.destroy()

    def _open_storage_monitor(self):
        """打开存储监控窗口"""
        if not STORAGE_MONITOR_AVAILABLE:
            messagebox.showerror("功能不可用", "存储监控功能不可用")
            return

        try:
            if self.storage_monitor_window is None:
                # 强制使用DB StorageManager（实际执行SQL的那个）
                actual_storage_manager = self.db_manager.storage_manager
                print(f"监控连接到 StorageManager ID: {id(actual_storage_manager)}")
                self.storage_monitor_window = StorageMonitorWindow(actual_storage_manager)

            self.storage_monitor_window.show()
            print("存储监控窗口已打开")

        except Exception as e:
            print(f"打开存储监控失败: {e}")
            import traceback
            traceback.print_exc()
            messagebox.showerror("错误", f"无法打开存储监控: {str(e)}")


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