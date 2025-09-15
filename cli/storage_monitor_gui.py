# storage_monitor_gui.py
"""
独立的存储监控窗口
展示存储系统的实时状态、性能图表和控制操作
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
# 配置matplotlib中文字体支持
import matplotlib
matplotlib.use('TkAgg')  # 确保使用正确的backend
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS', 'DejaVu Sans']
matplotlib.rcParams['axes.unicode_minus'] = False
import matplotlib.font_manager as fm
import numpy as np
from collections import deque
import time
import threading
import random
from datetime import datetime

def check_available_fonts():
    """检查可用的中文字体"""
    fonts = [f.name for f in fm.fontManager.ttflist]
    chinese_fonts = [f for f in fonts if any(keyword in f for keyword in ['SimHei', 'Microsoft', 'YaHei', 'FangSong'])]
    print("可用中文字体:", chinese_fonts[:5])
    return chinese_fonts

class StorageMonitorWindow:
    """存储监控窗口类"""

    def __init__(self, storage_manager):
        self.storage_manager = storage_manager
        self.window = None
        self.monitoring_active = False
        self.monitor_timer = None

        # 添加基准值变量
        self.base_read_count = 0
        self.base_write_count = 0

        # 监控数据存储
        self.monitor_data = {
            'timestamps': deque(maxlen=50),
            'hit_rates': deque(maxlen=50),
            'page_allocations': deque(maxlen=50),
            'transaction_counts': deque(maxlen=50),
            'read_operations': deque(maxlen=50),
            'write_operations': deque(maxlen=50)
        }

        # 图表相关
        self.fig = None
        self.axes = None
        self.canvas = None

        check_available_fonts()  # 调试用

    def show(self):
        """显示监控窗口"""
        if self.window and self.window.winfo_exists():
            self.window.lift()
            self.window.focus()
            return

        self.window = tk.Toplevel()
        self.window.title("存储系统监控中心")
        self.window.geometry("1200x800")
        self.window.configure(bg="#f0f0f0")

        # 设置窗口图标和样式
        self.style = ttk.Style()
        self.style.theme_use("clam")

        # 创建界面
        self._create_widgets()

        # 绑定关闭事件
        self.window.protocol("WM_DELETE_WINDOW", self._on_closing)

        # 初始化数据
        self._initialize_data()

        # 自动启动监控
        self._start_monitoring()

    def _create_widgets(self):
        """创建界面组件"""
        # 主框架
        main_frame = ttk.Frame(self.window, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 配置网格
        main_frame.columnconfigure(0, weight=2)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(0, weight=0)
        main_frame.rowconfigure(1, weight=1)

        # 标题栏
        self._create_title_bar(main_frame)

        # 左侧：图表和统计面板
        left_frame = ttk.Frame(main_frame)
        left_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        left_frame.columnconfigure(0, weight=1)
        left_frame.rowconfigure(0, weight=0)
        left_frame.rowconfigure(1, weight=1)

        # 右侧：控制和详情面板
        right_frame = ttk.Frame(main_frame)
        right_frame.grid(row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S))
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(0, weight=0)
        right_frame.rowconfigure(1, weight=1)

        # 创建各个组件
        self._create_stats_panel(left_frame)
        self._create_chart_panel(left_frame)
        self._create_control_panel(right_frame)
        self._create_details_panel(right_frame)

    def _create_title_bar(self, parent):
        """创建标题栏"""
        title_frame = ttk.Frame(parent)
        title_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 15))
        title_frame.columnconfigure(1, weight=1)

        # 标题
        title_label = ttk.Label(
            title_frame,
            text="📊 存储系统监控中心",
            font=("Arial", 18, "bold"),
            foreground="#2c3e50"
        )
        title_label.grid(row=0, column=0, sticky=tk.W)

        # 状态指示器
        self.status_frame = ttk.Frame(title_frame)
        self.status_frame.grid(row=0, column=2, sticky=tk.E)

        self.status_indicator = ttk.Label(
            self.status_frame,
            text="●",
            font=("Arial", 16),
            foreground="red"
        )
        self.status_indicator.pack(side=tk.LEFT)

        self.status_text = ttk.Label(
            self.status_frame,
            text="监控已停止",
            font=("Arial", 10)
        )
        self.status_text.pack(side=tk.LEFT, padx=(5, 0))

    def _create_stats_panel(self, parent):
        """创建统计面板"""
        stats_frame = ttk.LabelFrame(parent, text="Real-time Statistics", padding="10")
        stats_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

        # 配置网格
        for i in range(5):
            stats_frame.columnconfigure(i, weight=1)

        # 创建统计卡片
        self._create_stat_card(stats_frame, 0, "缓存命中率", "0.0%", "blue")
        self._create_stat_card(stats_frame, 1, "页面分配", "0/0", "green")
        self._create_stat_card(stats_frame, 2, "活跃事务", "0", "orange")
        self._create_stat_card(stats_frame, 3, "缓存策略", "LRU", "purple")
        self._create_stat_card(stats_frame, 4, "系统状态", "运行中", "teal")

    def _create_stat_card(self, parent, column, label_text, value_text, color):
        """创建统计卡片"""
        card_frame = ttk.Frame(parent)
        card_frame.grid(row=0, column=column, padx=5, pady=5, sticky=(tk.W, tk.E))

        # 标签
        label = ttk.Label(card_frame, text=label_text, font=("Arial", 9, "bold"))
        label.pack()

        # 数值
        value_label = ttk.Label(
            card_frame,
            text=value_text,
            font=("Arial", 14, "bold"),
            foreground=color
        )
        value_label.pack()

        # 存储引用以便更新
        setattr(self, f"{label_text.replace(' ', '_').lower()}_label", value_label)

    def _create_chart_panel(self, parent):
        """创建图表面板"""
        chart_frame = ttk.LabelFrame(parent, text="Performance Charts", padding="5")
        chart_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.rowconfigure(0, weight=1)

        # 创建matplotlib图表
        plt.style.use('seaborn-v0_8' if 'seaborn-v0_8' in plt.style.available else 'default')
        self.fig, ((self.ax1, self.ax2), (self.ax3, self.ax4)) = plt.subplots(2, 2, figsize=(10, 8))
        self.fig.suptitle('Storage System Performance Monitor', fontsize=14, fontweight='bold')

        # 初始化图表
        self._init_charts()

        # 嵌入到tkinter
        self.canvas = FigureCanvasTkAgg(self.fig, chart_frame)
        self.canvas.get_tk_widget().grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 调整布局
        self.fig.tight_layout()

    def _init_charts(self):
        """Initialize charts"""
        # Cache hit rate chart
        self.ax1.set_title('Cache Hit Rate Trend', fontsize=12, fontweight='bold')
        self.ax1.set_ylabel('Hit Rate (%)')
        self.ax1.grid(True, alpha=0.3)
        self.ax1.set_ylim(0, 100)

        # Page allocation chart
        self.ax2.set_title('Page Allocation Stats', fontsize=12, fontweight='bold')
        self.ax2.set_ylabel('Page Count')
        self.ax2.grid(True, alpha=0.3)

        # Transaction statistics chart
        self.ax3.set_title('Transaction Activity', fontsize=12, fontweight='bold')
        self.ax3.set_ylabel('Transaction Count')
        self.ax3.set_xlabel('Time (minutes)')
        self.ax3.grid(True, alpha=0.3)

        # I/O operations chart
        self.ax4.set_title('I/O Operations', fontsize=12, fontweight='bold')
        self.ax4.set_ylabel('Operation Count')
        self.ax4.set_xlabel('Time (minutes)')
        self.ax4.grid(True, alpha=0.3)

    def _create_control_panel(self, parent):
        """创建控制面板"""
        control_frame = ttk.LabelFrame(parent, text="Control Panel", padding="10")
        control_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        control_frame.columnconfigure(0, weight=1)

        # 监控控制
        monitor_frame = ttk.Frame(control_frame)
        monitor_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        monitor_frame.columnconfigure(0, weight=1)
        monitor_frame.columnconfigure(1, weight=1)

        self.start_btn = ttk.Button(
            monitor_frame,
            text="🟢 启动监控",
            command=self._start_monitoring
        )
        self.start_btn.grid(row=0, column=0, padx=(0, 5), sticky=(tk.W, tk.E))

        self.stop_btn = ttk.Button(
            monitor_frame,
            text="🔴 停止监控",
            command=self._stop_monitoring,
            state=tk.DISABLED
        )
        self.stop_btn.grid(row=0, column=1, padx=(5, 0), sticky=(tk.W, tk.E))

        # 缓存策略控制
        strategy_frame = ttk.LabelFrame(control_frame, text="Cache Strategy", padding="5")
        strategy_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

        self.strategy_var = tk.StringVar(value="adaptive")
        strategies = [
            ("自适应", "adaptive"),
            ("LRU", "lru"),
            ("FIFO", "fifo")
        ]

        for i, (text, value) in enumerate(strategies):
            ttk.Radiobutton(
                strategy_frame,
                text=text,
                variable=self.strategy_var,
                value=value,
                command=self._change_strategy
            ).grid(row=i, column=0, sticky=tk.W, pady=2)

        # 缓存可视化
        cache_viz_frame = ttk.LabelFrame(control_frame, text="Cache Visualization", padding="5")
        cache_viz_frame.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        cache_viz_frame.columnconfigure(0, weight=1)

        ttk.Button(
            cache_viz_frame,
            text="🎬 缓存替换动画",
            command=self._open_cache_animation_window
        ).grid(row=0, column=0, sticky=(tk.W, tk.E), pady=2)

        # 性能测试
        test_frame = ttk.LabelFrame(control_frame, text="Performance Tests", padding="5")
        test_frame.grid(row=4, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        test_frame.columnconfigure(0, weight=1)

        tests = [
            ("📈 顺序访问测试", "sequential"),
            ("🔀 随机访问测试", "random"),
            ("🔄 重复访问测试", "repeat"),
            ("⚡ 压力测试", "stress")
        ]

        for i, (text, test_type) in enumerate(tests):
            ttk.Button(
                test_frame,
                text=text,
                command=lambda t=test_type: self._run_test(t)
            ).grid(row=i, column=0, sticky=(tk.W, tk.E), pady=2)

        # 系统操作
        system_frame = ttk.LabelFrame(control_frame, text="System Operations", padding="5")
        system_frame.grid(row=5, column=0, sticky=(tk.W, tk.E))
        system_frame.columnconfigure(0, weight=1)

        operations = [
            ("💾 强制刷盘", self._force_flush),
            ("🗑️ 清理缓存", self._clear_cache),
            ("📊 导出报告", self._export_report),
            ("🔄 重置统计", self._reset_statistics)
        ]

        for i, (text, command) in enumerate(operations):
            ttk.Button(
                system_frame,
                text=text,
                command=command
            ).grid(row=i, column=0, sticky=(tk.W, tk.E), pady=2)

    def _create_details_panel(self, parent):
        """创建详情面板"""
        details_frame = ttk.LabelFrame(parent, text="Detailed Information", padding="5")
        details_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        details_frame.columnconfigure(0, weight=1)
        details_frame.rowconfigure(0, weight=1)

        # 创建Notebook
        self.details_notebook = ttk.Notebook(details_frame)
        self.details_notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 表空间信息
        self._create_tablespace_tab()

        # 缓存详情
        self._create_cache_tab()

        # 事务信息
        self._create_transaction_tab()

        # 日志信息
        self._create_log_tab()

        # 页面分配可视化
        self._create_page_allocation_tab()

    def _create_tablespace_tab(self):
        """创建表空间标签页"""
        self.tablespace_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.tablespace_frame, text="表空间")

        self.tablespace_tree = ttk.Treeview(
            self.tablespace_frame,
            columns=("name", "size", "used", "usage", "status"),
            show="headings",
            height=10
        )

        # 设置列标题
        headers = [
            ("name", "表空间名称", 100),
            ("size", "总大小", 80),
            ("used", "已使用", 80),
            ("usage", "使用率", 80),
            ("status", "状态", 80)
        ]

        for col, text, width in headers:
            self.tablespace_tree.heading(col, text=text)
            self.tablespace_tree.column(col, width=width)

        self.tablespace_tree.pack(fill=tk.BOTH, expand=True)

        # 添加滚动条
        ts_scrollbar = ttk.Scrollbar(self.tablespace_frame, orient=tk.VERTICAL,
                                     command=self.tablespace_tree.yview)
        ts_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.tablespace_tree.configure(yscrollcommand=ts_scrollbar.set)

    def _create_cache_tab(self):
        """创建缓存标签页"""
        self.cache_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.cache_frame, text="缓存详情")

        self.cache_tree = ttk.Treeview(
            self.cache_frame,
            columns=("page_id", "dirty", "access_count", "last_access", "size"),
            show="headings",
            height=10
        )

        headers = [
            ("page_id", "页号", 60),
            ("dirty", "脏页", 50),
            ("access_count", "访问次数", 80),
            ("last_access", "最后访问", 120),
            ("size", "大小", 60)
        ]

        for col, text, width in headers:
            self.cache_tree.heading(col, text=text)
            self.cache_tree.column(col, width=width)

        self.cache_tree.pack(fill=tk.BOTH, expand=True)

        # 添加滚动条
        cache_scrollbar = ttk.Scrollbar(self.cache_frame, orient=tk.VERTICAL,
                                        command=self.cache_tree.yview)
        cache_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.cache_tree.configure(yscrollcommand=cache_scrollbar.set)

    def _create_transaction_tab(self):
        """创建事务标签页"""
        self.transaction_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.transaction_frame, text="事务状态")

        self.txn_tree = ttk.Treeview(
            self.transaction_frame,
            columns=("txn_id", "state", "start_time", "pages", "locks", "isolation"),
            show="headings",
            height=10
        )

        headers = [
            ("txn_id", "事务ID", 60),
            ("state", "状态", 80),
            ("start_time", "开始时间", 100),
            ("pages", "修改页数", 80),
            ("locks", "持有锁数", 80),
            ("isolation", "隔离级别", 100)
        ]

        for col, text, width in headers:
            self.txn_tree.heading(col, text=text)
            self.txn_tree.column(col, width=width)

        self.txn_tree.pack(fill=tk.BOTH, expand=True)

        # 添加滚动条
        txn_scrollbar = ttk.Scrollbar(self.transaction_frame, orient=tk.VERTICAL,
                                      command=self.txn_tree.yview)
        txn_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.txn_tree.configure(yscrollcommand=txn_scrollbar.set)

    def _create_log_tab(self):
        """创建日志标签页"""
        self.log_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.log_frame, text="监控日志")

        self.log_text = scrolledtext.ScrolledText(
            self.log_frame,
            height=15,
            font=("Consolas", 9),
            wrap=tk.WORD
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def _create_page_allocation_tab(self):
        """创建页面分配可视化标签页"""
        self.page_allocation_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.page_allocation_frame, text="页面分配地图")

        # 创建一个容器框架
        container = ttk.Frame(self.page_allocation_frame)
        container.pack(expand=True)

        # 添加说明文本
        ttk.Label(
            container,
            text="查看页面分配的可视化地图",
            font=("Arial", 11)
        ).pack(pady=10)

        # 添加打开地图按钮
        ttk.Button(
            container,
            text="🗺️ 打开页面分配地图",
            command=self._open_page_allocation_window
        ).pack(pady=5)

        # 添加简单统计信息
        self.page_stats_label = ttk.Label(
            container,
            text="",
            font=("Arial", 9)
        )
        self.page_stats_label.pack(pady=10)

        # 初始化页面地图窗口引用
        self.page_map_window = None

    def _open_page_allocation_window(self):
        """打开页面分配地图窗口"""
        # 如果窗口已存在，将其提前
        if self.page_map_window and self.page_map_window.winfo_exists():
            self.page_map_window.lift()
            self.page_map_window.focus()
            return

        # 创建新窗口
        self.page_map_window = tk.Toplevel(self.window)
        self.page_map_window.title("页面分配地图 - 存储系统可视化")
        self.page_map_window.geometry("900x600")

        # 主框架
        main_frame = ttk.Frame(self.page_map_window, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 顶部控制栏
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))

        # 刷新按钮
        ttk.Button(
            control_frame,
            text="🔄 刷新",
            command=lambda: self._draw_page_map_in_window()
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            control_frame,
            text="🎲 生成测试数据",
            command=self._generate_test_pages
        ).pack(side=tk.LEFT, padx=5)

        # 添加自动刷新选项
        self.auto_refresh_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            control_frame,
            text="自动刷新",
            variable=self.auto_refresh_var,
            command=self._toggle_auto_refresh
        ).pack(side=tk.LEFT, padx=5)

        # 图例
        self._create_page_map_legend(control_frame)

        # Canvas用于绘制
        canvas_frame = ttk.LabelFrame(main_frame, text="页面分配状态", padding="5")
        canvas_frame.pack(fill=tk.BOTH, expand=True)

        self.page_map_canvas = tk.Canvas(
            canvas_frame,
            bg='white',
            highlightthickness=0
        )
        self.page_map_canvas.pack(fill=tk.BOTH, expand=True)

        # 底部信息栏
        info_frame = ttk.Frame(main_frame)
        info_frame.pack(fill=tk.X, pady=(10, 0))

        self.page_map_info_label = ttk.Label(
            info_frame,
            text="鼠标悬停在页面方块上查看详细信息",
            font=("Arial", 10)
        )
        self.page_map_info_label.pack()

        # 初始绘制
        self.page_map_window.after(100, self._draw_page_map_in_window)

    def _create_page_map_legend(self, parent):
        """创建页面地图图例"""
        legend_frame = ttk.Frame(parent)
        legend_frame.pack(side=tk.RIGHT, padx=10)

        ttk.Label(legend_frame, text="图例：", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)

        legends = [
            ("■", "#ff6b6b", "脏页"),
            ("■", "#4ecdc4", "缓存中"),
            ("■", "#95e1d3", "系统表空间"),
            ("■", "#f3a683", "临时表空间"),
            ("■", "#c7ecee", "日志表空间"),
            ("■", "#dfe6e9", "普通页面")
        ]

        for symbol, color, desc in legends:
            frame = ttk.Frame(legend_frame)
            frame.pack(side=tk.LEFT, padx=3)

            # 使用Label显示颜色块
            color_label = tk.Label(frame, text=symbol, fg=color, font=("Arial", 12, "bold"))
            color_label.pack(side=tk.LEFT)

            ttk.Label(frame, text=desc, font=("Arial", 9)).pack(side=tk.LEFT)

    def _draw_page_map_in_window(self):
        """在独立窗口中绘制页面地图"""
        if not self.page_map_canvas:
            return

        # 清空画布
        self.page_map_canvas.delete("all")

        try:
            # 获取页面分配信息
            allocated_pages = list(self.storage_manager.page_manager.get_allocated_pages())

            if not allocated_pages:
                self.page_map_canvas.create_text(
                    450, 250,
                    text="暂无已分配页面",
                    font=("Arial", 14),
                    fill="gray"
                )
                return

            # 获取Canvas实际大小
            self.page_map_canvas.update_idletasks()
            canvas_width = self.page_map_canvas.winfo_width()
            canvas_height = self.page_map_canvas.winfo_height()

            # 计算布局参数
            padding = 20
            cols = 30  # 每行显示30个页面
            block_size = min(20, (canvas_width - 2 * padding) // (cols + 1))
            spacing = 2

            # 排序页面以便更好地显示
            sorted_pages = sorted(allocated_pages)

            # 绘制页面方块
            for i, page_id in enumerate(sorted_pages[:900]):  # 最多显示900个页面（30x30）
                row = i // cols
                col = i % cols
                x = padding + col * (block_size + spacing)
                y = padding + row * (block_size + spacing)

                # 获取页面颜色
                color = self._get_page_color(page_id)

                # 绘制方块
                rect = self.page_map_canvas.create_rectangle(
                    x, y, x + block_size, y + block_size,
                    fill=color,
                    outline="darkgray",
                    width=1,
                    tags=f"page_{page_id}"
                )

                # 绑定鼠标事件
                self.page_map_canvas.tag_bind(
                    f"page_{page_id}",
                    "<Enter>",
                    lambda e, pid=page_id: self._show_page_map_info(pid)
                )
                self.page_map_canvas.tag_bind(
                    f"page_{page_id}",
                    "<Leave>",
                    lambda e: self.page_map_info_label.configure(
                        text="鼠标悬停在页面方块上查看详细信息"
                    )
                )

            # 显示统计信息
            cache_stats = self.storage_manager.get_cache_stats()
            stats_text = (f"总页面数: {len(allocated_pages)} | "
                          f"缓存中: {cache_stats.get('cache_size', 0)} | "
                          f"脏页: {cache_stats.get('dirty_pages', 0)}")

            self.page_map_canvas.create_text(
                canvas_width // 2, canvas_height - 10,
                text=stats_text,
                font=("Arial", 10),
                fill="black"
            )

        except Exception as e:
            self._log(f"Failed to draw page map: {e}")

    def _show_page_map_info(self, page_id: int):
        """在独立窗口中显示页面详细信息"""
        try:
            info_parts = [f"页面 #{page_id}"]

            # 缓存状态
            if page_id in self.storage_manager.buffer_pool.cache:
                _, is_dirty, access_time = self.storage_manager.buffer_pool.cache[page_id]
                info_parts.append(f"缓存中({'脏页' if is_dirty else '干净'})")
                info_parts.append(f"最后访问: {time.time() - access_time:.1f}秒前")
            else:
                info_parts.append("不在缓存中")

            # 表空间信息
            tablespace = self.storage_manager.page_manager.metadata.page_tablespaces.get(
                str(page_id), "default"
            )
            info_parts.append(f"表空间: {tablespace}")

            # 页使用信息
            usage_info = self.storage_manager.page_manager.metadata.page_usage.get(str(page_id))
            if usage_info:
                access_count = usage_info.get('access_count', 0)
                if access_count > 0:
                    info_parts.append(f"访问次数: {access_count}")

            self.page_map_info_label.configure(text=" | ".join(info_parts))

        except Exception as e:
            self.page_map_info_label.configure(text=f"页面 #{page_id} (信息获取失败)")

    def _generate_test_pages(self):
        """生成测试页面以展示不同的表空间"""
        try:
            # 确保各种表空间存在
            tablespaces_to_create = [
                ("system", 50),
                ("temp", 100),
                ("log", 50),
                ("user_data", 200)
            ]

            for ts_name, size_mb in tablespaces_to_create:
                if ts_name not in self.storage_manager.tablespace_manager.tablespaces:
                    self.storage_manager.create_tablespace(ts_name, size_mb=size_mb)
                    self._log(f"Created tablespace: {ts_name}")

            # 分配一些页面到不同的表空间
            import random
            tablespaces = ["system", "temp", "log", "user_data", "default"]

            for i in range(20):  # 分配20个新页面
                ts = random.choice(tablespaces)
                page_id = self.storage_manager.allocate_page(tablespace_name=ts)

                # 随机将一些页面放入缓存
                if random.random() < 0.3:  # 30%概率放入缓存
                    test_data = f"Test data for page {page_id}".encode().ljust(4096, b'\0')
                    self.storage_manager.write_page(page_id, test_data)

                    # 随机标记为脏页
                    if random.random() < 0.5:
                        self.storage_manager.buffer_pool.mark_dirty(page_id)

            self._log("Generated test pages in various tablespaces")

            # 刷新显示
            self._draw_page_map_in_window()

        except Exception as e:
            self._log(f"Failed to generate test pages: {e}")
            import traceback
            traceback.print_exc()

    def _toggle_auto_refresh(self):
        """切换自动刷新"""
        if self.auto_refresh_var.get():
            self._auto_refresh_page_map()
        else:
            if hasattr(self, 'page_map_refresh_timer'):
                self.window.after_cancel(self.page_map_refresh_timer)

    def _draw_page_allocation_map(self):
        """绘制页面分配地图"""
        # 清空画布
        self.page_canvas.delete("all")

        try:
            # 获取页面分配信息
            page_stats = self.storage_manager.get_page_stats()
            allocated_pages = list(self.storage_manager.page_manager.get_allocated_pages())

            if not allocated_pages:
                self.page_canvas.create_text(
                    200, 150,
                    text="暂无已分配页面",
                    font=("Arial", 12),
                    fill="gray"
                )
                return

            # 计算布局参数
            canvas_width = self.page_canvas.winfo_width()
            canvas_height = self.page_canvas.winfo_height()
            if canvas_width <= 1:  # Canvas还未渲染
                canvas_width = 400
                canvas_height = 300

            # 每行显示的页面数
            cols = 20
            # 页面方块的大小
            block_size = min(15, (canvas_width - 20) // cols)

            # 绘制页面方块
            for i, page_id in enumerate(sorted(allocated_pages)[:500]):  # 最多显示500个页面
                row = i // cols
                col = i % cols
                x = 10 + col * (block_size + 2)
                y = 10 + row * (block_size + 2)

                # 根据页面状态选择颜色
                color = self._get_page_color(page_id)

                # 绘制方块
                rect = self.page_canvas.create_rectangle(
                    x, y, x + block_size, y + block_size,
                    fill=color,
                    outline="gray",
                    tags=f"page_{page_id}"
                )

                # 绑定鼠标事件
                self.page_canvas.tag_bind(
                    f"page_{page_id}",
                    "<Enter>",
                    lambda e, pid=page_id: self._show_page_info(pid)
                )

        except Exception as e:
            self._log(f"Failed to draw page allocation map: {e}")

    def _get_page_color(self, page_id: int) -> str:
        """根据页面状态获取显示颜色"""
        try:
            # 检查页面是否在缓存中
            if page_id in self.storage_manager.buffer_pool.cache:
                _, is_dirty, _ = self.storage_manager.buffer_pool.cache[page_id]
                if is_dirty:
                    return "#ff6b6b"  # 红色 - 脏页
                else:
                    return "#4ecdc4"  # 青色 - 缓存中的干净页

            # 检查页面所属的表空间
            tablespace = self.storage_manager.page_manager.metadata.page_tablespaces.get(str(page_id), "default")
            if tablespace == "system":
                return "#95e1d3"  # 浅绿 - 系统表空间
            elif tablespace == "temp":
                return "#f3a683"  # 橙色 - 临时表空间
            elif tablespace == "log":
                return "#c7ecee"  # 浅蓝 - 日志表空间
            else:
                return "#dfe6e9"  # 灰色 - 普通页面

        except Exception:
            return "#dfe6e9"  # 默认灰色

    def _show_page_info(self, page_id: int):
        """显示页面详细信息"""
        try:
            info = f"页面 #{page_id}"

            # 检查缓存状态
            if page_id in self.storage_manager.buffer_pool.cache:
                _, is_dirty, access_time = self.storage_manager.buffer_pool.cache[page_id]
                info += f" | 缓存中({'脏页' if is_dirty else '干净'})"
                info += f" | 最后访问: {time.time() - access_time:.1f}秒前"
            else:
                info += " | 不在缓存中"

            # 显示表空间信息
            tablespace = self.storage_manager.page_manager.metadata.page_tablespaces.get(str(page_id), "default")
            info += f" | 表空间: {tablespace}"

            self.page_info_label.configure(text=info)

        except Exception as e:
            self.page_info_label.configure(text=f"页面 #{page_id} (信息获取失败)")

    def _open_cache_animation_window(self):
        """打开缓存替换动画窗口"""
        # 如果窗口已存在，将其提前
        if hasattr(self, 'cache_anim_window') and self.cache_anim_window and self.cache_anim_window.winfo_exists():
            self.cache_anim_window.lift()
            self.cache_anim_window.focus()
            return

        # 创建新窗口
        self.cache_anim_window = tk.Toplevel(self.window)
        self.cache_anim_window.title("缓存替换动画 - LRU/FIFO可视化")
        self.cache_anim_window.geometry("1000x700")

        # 主框架
        main_frame = ttk.Frame(self.cache_anim_window, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 顶部信息栏
        info_frame = ttk.Frame(main_frame)
        info_frame.pack(fill=tk.X, pady=(0, 10))

        # 缓存策略显示
        self.cache_strategy_label = ttk.Label(
            info_frame,
            text=f"当前策略: {self._get_current_strategy()}",
            font=("Arial", 12, "bold")
        )
        self.cache_strategy_label.pack(side=tk.LEFT, padx=10)

        # 缓存容量显示
        cache_info = self.storage_manager.buffer_pool.get_cache_info()
        self.cache_capacity_label = ttk.Label(
            info_frame,
            text=f"容量: {cache_info['capacity_info']['current']}/{cache_info['capacity_info']['capacity']}",
            font=("Arial", 12)
        )
        self.cache_capacity_label.pack(side=tk.LEFT, padx=10)

        # 命中率显示
        hit_rate = self.storage_manager.buffer_pool.get_hit_rate()
        self.cache_hit_rate_label = ttk.Label(
            info_frame,
            text=f"命中率: {hit_rate:.1f}%",
            font=("Arial", 12),
            foreground="green" if hit_rate > 70 else "orange" if hit_rate > 50 else "red"
        )
        self.cache_hit_rate_label.pack(side=tk.LEFT, padx=10)

        # 缓存内容显示区域
        cache_display_frame = ttk.LabelFrame(main_frame, text="缓存内容 (LRU顺序: 左侧最近使用，右侧最久未使用)",
                                             padding="10")
        cache_display_frame.pack(fill=tk.BOTH, expand=True)

        # 创建包含滚动条的框架
        canvas_container = ttk.Frame(cache_display_frame)
        canvas_container.pack(fill=tk.BOTH, expand=True)
        canvas_container.grid_columnconfigure(0, weight=1)
        canvas_container.grid_rowconfigure(0, weight=1)

        # 创建Canvas用于动画
        self.cache_canvas = tk.Canvas(
            canvas_container,
            bg='white',
            height=400,
            scrollregion=(0, 0, 2000, 800)  # 初始滚动区域
        )
        self.cache_canvas.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 添加水平滚动条
        h_scrollbar = ttk.Scrollbar(canvas_container, orient=tk.HORIZONTAL, command=self.cache_canvas.xview)
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        self.cache_canvas.configure(xscrollcommand=h_scrollbar.set)

        # 添加垂直滚动条
        v_scrollbar = ttk.Scrollbar(canvas_container, orient=tk.VERTICAL, command=self.cache_canvas.yview)
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.cache_canvas.configure(yscrollcommand=v_scrollbar.set)

        # 绑定鼠标滚轮事件
        self.cache_canvas.bind("<MouseWheel>", lambda e: self.cache_canvas.yview_scroll(-1 * (e.delta // 120), "units"))
        self.cache_canvas.bind("<Shift-MouseWheel>",
                               lambda e: self.cache_canvas.xview_scroll(-1 * (e.delta // 120), "units"))

        # 控制按钮栏
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=(10, 0))

        ttk.Button(
            control_frame,
            text="📖 模拟读取",
            command=self._simulate_cache_read
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            control_frame,
            text="✏️ 模拟写入",
            command=self._simulate_cache_write
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            control_frame,
            text="🔄 刷新显示",
            command=self._draw_cache_content
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            control_frame,
            text="🎯 触发替换",
            command=self._trigger_cache_eviction
        ).pack(side=tk.LEFT, padx=5)

        # 动画速度控制
        ttk.Label(control_frame, text="动画速度:").pack(side=tk.LEFT, padx=(20, 5))
        self.animation_speed = tk.Scale(
            control_frame,
            from_=100,
            to=2000,
            orient=tk.HORIZONTAL,
            length=150
        )
        self.animation_speed.set(500)  # 默认500ms
        self.animation_speed.pack(side=tk.LEFT)

        # 事件日志
        log_frame = ttk.LabelFrame(main_frame, text="缓存事件日志", padding="5")
        log_frame.pack(fill=tk.X, pady=(10, 0))

        self.cache_log_text = scrolledtext.ScrolledText(
            log_frame,
            height=6,
            font=("Consolas", 9),
            wrap=tk.WORD
        )
        self.cache_log_text.pack(fill=tk.X)

        # 初始绘制
        self.cache_anim_window.after(100, self._draw_cache_content)

        # 添加窗口引用
        self.cache_blocks = {}  # 存储页面方块的引用

    def _draw_cache_content(self):
        """绘制缓存内容"""
        if not hasattr(self, 'cache_canvas') or not self.cache_canvas:
            return

        # 清空画布
        self.cache_canvas.delete("all")
        self.cache_blocks = {}

        try:
            # 获取缓存信息
            cache_info = self.storage_manager.buffer_pool.get_cache_info()
            lru_order = cache_info['lru_order']  # LRU顺序的页面列表

            if not lru_order:
                self.cache_canvas.create_text(
                    500, 200,
                    text="缓存为空",
                    font=("Arial", 16),
                    fill="gray"
                )
                return

            # 计算布局
            canvas_width = self.cache_canvas.winfo_width()
            if canvas_width <= 1:
                canvas_width = 1000

            block_width = 80
            block_height = 60
            spacing = 10
            start_x = 20
            start_y = 50

            # 绘制每个缓存页
            for i, page_id in enumerate(lru_order):
                x = start_x + i * (block_width + spacing)
                y = start_y

                # 如果超出画布宽度，换行
                if x + block_width > canvas_width - 20:
                    row = i // ((canvas_width - 40) // (block_width + spacing))
                    col = i % ((canvas_width - 40) // (block_width + spacing))
                    x = start_x + col * (block_width + spacing)
                    y = start_y + row * (block_height + spacing + 30)

                # 获取页面信息
                _, is_dirty, access_time = self.storage_manager.buffer_pool.cache[page_id]

                # 选择颜色
                if is_dirty:
                    color = "#ff6b6b"  # 红色-脏页
                else:
                    color = "#4ecdc4"  # 青色-干净页

                # 绘制方块
                rect = self.cache_canvas.create_rectangle(
                    x, y, x + block_width, y + block_height,
                    fill=color,
                    outline="black",
                    width=2,
                    tags=f"cache_block_{page_id}"
                )

                # 添加页号文本
                text = self.cache_canvas.create_text(
                    x + block_width // 2,
                    y + block_height // 2 - 10,
                    text=f"Page {page_id}",
                    font=("Arial", 10, "bold"),
                    fill="white"
                )

                # 添加状态文本
                status = "Dirty" if is_dirty else "Clean"
                status_text = self.cache_canvas.create_text(
                    x + block_width // 2,
                    y + block_height // 2 + 10,
                    text=status,
                    font=("Arial", 8),
                    fill="white"
                )

                # 添加LRU位置标签
                if i == 0:
                    self.cache_canvas.create_text(
                        x + block_width // 2,
                        y - 10,
                        text="MRU\n(最近使用)",
                        font=("Arial", 8),
                        fill="green"
                    )
                elif i == len(lru_order) - 1:
                    self.cache_canvas.create_text(
                        x + block_width // 2,
                        y + block_height + 10,
                        text="LRU\n(最久未用)",
                        font=("Arial", 8),
                        fill="red"
                    )

                # 保存引用
                self.cache_blocks[page_id] = {
                    'rect': rect,
                    'text': text,
                    'status': status_text,
                    'x': x,
                    'y': y
                }

            # 更新滚动区域
            if lru_order:
                # 计算实际需要的画布大小
                max_x = start_x + len(lru_order) * (block_width + spacing) + 20
                max_y = start_y + ((len(lru_order) - 1) // ((canvas_width - 40) // (block_width + spacing)) + 1) * (
                            block_height + spacing + 30) + 50
                self.cache_canvas.configure(scrollregion=(0, 0, max_x, max_y))

            # 更新信息标签
            self._update_cache_info_labels()

        except Exception as e:
            self._cache_log(f"绘制缓存内容失败: {e}")

    def _update_cache_info_labels(self):
        """更新缓存信息标签"""
        try:
            # 更新策略
            self.cache_strategy_label.configure(text=f"当前策略: {self._get_current_strategy()}")

            # 更新容量
            cache_info = self.storage_manager.buffer_pool.get_cache_info()
            self.cache_capacity_label.configure(
                text=f"容量: {cache_info['capacity_info']['current']}/{cache_info['capacity_info']['capacity']}"
            )

            # 更新命中率
            hit_rate = self.storage_manager.buffer_pool.get_hit_rate()
            self.cache_hit_rate_label.configure(
                text=f"命中率: {hit_rate:.1f}%",
                foreground="green" if hit_rate > 70 else "orange" if hit_rate > 50 else "red"
            )
        except Exception:
            pass

    def _cache_log(self, message):
        """向缓存事件日志添加消息"""
        if hasattr(self, 'cache_log_text'):
            timestamp = datetime.now().strftime("%H:%M:%S")
            log_entry = f"[{timestamp}] {message}\n"
            self.cache_log_text.insert(tk.END, log_entry)
            self.cache_log_text.see(tk.END)

    def _simulate_cache_read(self):
        """模拟缓存读取操作"""
        try:
            # 获取一个随机页面进行读取
            allocated_pages = list(self.storage_manager.page_manager.get_allocated_pages())
            if not allocated_pages:
                self._cache_log("没有已分配的页面")
                return

            import random
            page_id = random.choice(allocated_pages)

            # 检查是否在缓存中
            was_in_cache = page_id in self.storage_manager.buffer_pool.cache

            # 执行读取
            self._cache_log(f"读取页面 {page_id}...")
            data = self.storage_manager.read_page(page_id)

            if was_in_cache:
                # 缓存命中 - 显示命中动画
                self._animate_cache_hit(page_id)
                self._cache_log(f"✅ 缓存命中！页面 {page_id} 已在缓存中")
            else:
                # 缓存未命中 - 显示加载动画
                self._animate_cache_miss(page_id)
                self._cache_log(f"❌ 缓存未命中！从磁盘加载页面 {page_id}")

            # 刷新显示
            self.cache_anim_window.after(self.animation_speed.get(), self._draw_cache_content)

        except Exception as e:
            self._cache_log(f"读取失败: {e}")

    def _simulate_cache_write(self):
        """模拟缓存写入操作"""
        try:
            # 获取一个随机页面进行写入
            allocated_pages = list(self.storage_manager.page_manager.get_allocated_pages())
            if not allocated_pages:
                # 分配新页面
                page_id = self.storage_manager.allocate_page()
                self._cache_log(f"分配新页面 {page_id}")
            else:
                import random
                page_id = random.choice(allocated_pages)

            # 生成测试数据
            test_data = f"Test write at {time.time()}".encode().ljust(4096, b'\0')

            # 执行写入
            self._cache_log(f"写入页面 {page_id}...")
            self.storage_manager.write_page(page_id, test_data)

            # 显示写入动画
            self._animate_cache_write_effect(page_id)
            self._cache_log(f"✏️ 页面 {page_id} 已写入并标记为脏页")

            # 刷新显示
            self.cache_anim_window.after(self.animation_speed.get(), self._draw_cache_content)

        except Exception as e:
            self._cache_log(f"写入失败: {e}")

    def _trigger_cache_eviction(self):
        """触发缓存替换"""
        try:
            # 填满缓存以触发替换
            self._cache_log("触发缓存替换...")

            # 获取当前缓存大小
            cache_info = self.storage_manager.buffer_pool.get_cache_info()
            current_size = cache_info['capacity_info']['current']
            capacity = cache_info['capacity_info']['capacity']

            if current_size < capacity:
                # 缓存未满，添加页面直到满
                self._cache_log(f"缓存未满 ({current_size}/{capacity})，添加页面...")

                # 分配新页面并写入缓存
                for _ in range(capacity - current_size + 1):
                    page_id = self.storage_manager.allocate_page()
                    test_data = f"Eviction test {page_id}".encode().ljust(4096, b'\0')
                    self.storage_manager.write_page(page_id, test_data)
                    self._cache_log(f"添加页面 {page_id} 到缓存")

                    # 短暂延迟以便观察
                    self.cache_anim_window.update()
                    time.sleep(0.1)
            else:
                # 缓存已满，添加新页面触发替换
                self._cache_log("缓存已满，添加新页面将触发替换...")

                # 记录LRU页面（将被替换）
                lru_order = cache_info['lru_order']
                lru_page = lru_order[-1] if lru_order else None

                # 分配并写入新页面
                page_id = self.storage_manager.allocate_page()
                test_data = f"New page {page_id}".encode().ljust(4096, b'\0')

                # 显示替换动画
                if lru_page:
                    self._animate_eviction(lru_page, page_id)

                self.storage_manager.write_page(page_id, test_data)

                self._cache_log(f"🔄 页面 {lru_page} 被替换为页面 {page_id}")

            # 刷新显示
            self.cache_anim_window.after(self.animation_speed.get(), self._draw_cache_content)

        except Exception as e:
            self._cache_log(f"触发替换失败: {e}")

    def _animate_cache_hit(self, page_id):
        """缓存命中动画"""
        if page_id not in self.cache_blocks:
            return

        block = self.cache_blocks[page_id]
        rect = block['rect']

        # 闪烁效果 - 变绿
        original_color = self.cache_canvas.itemcget(rect, 'fill')

        for _ in range(3):  # 闪烁3次
            self.cache_canvas.itemconfig(rect, fill="#2ecc71", width=4)
            self.cache_canvas.update()
            time.sleep(0.1)
            self.cache_canvas.itemconfig(rect, fill=original_color, width=2)
            self.cache_canvas.update()
            time.sleep(0.1)

    def _animate_cache_miss(self, page_id):
        """缓存未命中动画 - 新页面加入"""
        # 从顶部滑入效果
        temp_rect = self.cache_canvas.create_rectangle(
            400, -60, 480, 0,
            fill="#f39c12",
            outline="black",
            width=2
        )

        temp_text = self.cache_canvas.create_text(
            440, -30,
            text=f"Page {page_id}\nLoading...",
            font=("Arial", 10, "bold"),
            fill="white"
        )

        # 滑入动画
        for i in range(10):
            self.cache_canvas.move(temp_rect, 0, 5)
            self.cache_canvas.move(temp_text, 0, 5)
            self.cache_canvas.update()
            time.sleep(0.03)

        # 停留一下
        time.sleep(0.3)

        # 删除临时对象
        self.cache_canvas.delete(temp_rect)
        self.cache_canvas.delete(temp_text)

    def _animate_cache_write_effect(self, page_id):
        """缓存写入动画效果"""
        if page_id not in self.cache_blocks:
            # 如果页面不在缓存中，先显示加载动画
            self._animate_cache_miss(page_id)
            return

        block = self.cache_blocks[page_id]
        rect = block['rect']

        # 脉冲效果 - 变红表示脏页
        for i in range(3):
            scale = 1.1 if i % 2 == 0 else 1.0
            # 简单的闪烁效果代替缩放
            self.cache_canvas.itemconfig(rect, fill="#ff6b6b", width=4 if scale > 1 else 2)
            self.cache_canvas.update()
            time.sleep(0.1)

    def _animate_eviction(self, evicted_page, new_page):
        """缓存替换动画"""
        self._cache_log(f"动画: 页面 {evicted_page} 将被页面 {new_page} 替换")

        if evicted_page in self.cache_blocks:
            block = self.cache_blocks[evicted_page]
            rect = block['rect']
            text = block['text']
            status = block['status']

            # 淡出效果 - 被替换的页面
            for alpha in range(10, 0, -2):
                color = f"#{alpha}0{alpha}0{alpha}0"
                self.cache_canvas.itemconfig(rect, fill=color)
                self.cache_canvas.update()
                time.sleep(0.05)

            # 显示替换文本
            self.cache_canvas.itemconfig(text, text=f"→ Page {new_page}")

            # 淡入新页面
            for alpha in range(0, 10, 2):
                color_val = 40 + alpha * 10
                color = f"#{color_val:02x}{180:02x}{180:02x}"
                self.cache_canvas.itemconfig(rect, fill=color)
                self.cache_canvas.update()
                time.sleep(0.05)

    def _auto_refresh_page_map(self):
        """自动刷新页面地图"""
        if self.auto_refresh_var.get() and hasattr(self,
                                                   'page_map_window') and self.page_map_window and self.page_map_window.winfo_exists():
            self._draw_page_map_in_window()
            # 继续定时刷新
            self.page_map_refresh_timer = self.window.after(2000, self._auto_refresh_page_map)

    # 监控控制方法
    def _start_monitoring(self):
        """启动监控"""
        if not self.monitoring_active:
            # 记录当前的计数作为基准
            self.base_read_count = getattr(self.storage_manager, 'read_count', 0)
            self.base_write_count = getattr(self.storage_manager, 'write_count', 0)

            # 清空之前的监控数据，从0开始
            for key in self.monitor_data:
                self.monitor_data[key].clear()

            self.monitoring_active = True
            self.start_btn.configure(state=tk.DISABLED)
            self.stop_btn.configure(state=tk.NORMAL)
            self.status_indicator.configure(foreground="green")
            self.status_text.configure(text="监控运行中")
            self._log(f"存储监控已启动 (基准: 读={self.base_read_count}, 写={self.base_write_count})")

            # 立即添加第一个数据点（从0开始）
            current_time = time.time()
            self.monitor_data['timestamps'].append(current_time)
            self.monitor_data['hit_rates'].append(self.storage_manager.get_cache_stats().get('hit_rate', 0))
            self.monitor_data['page_allocations'].append(self.storage_manager.get_page_stats()['pages']['allocated'])
            self.monitor_data['transaction_counts'].append(0)
            self.monitor_data['read_operations'].append(0)  # 从0开始
            self.monitor_data['write_operations'].append(0)  # 从0开始

            self._update_monitoring_data()

    def _stop_monitoring(self):
        """停止监控"""
        if self.monitoring_active:
            self.monitoring_active = False
            self.start_btn.configure(state=tk.NORMAL)
            self.stop_btn.configure(state=tk.DISABLED)
            self.status_indicator.configure(foreground="red")
            self.status_text.configure(text="监控已停止")
            if self.monitor_timer:
                self.window.after_cancel(self.monitor_timer)
            self._log("存储监控已停止")

    def _initialize_data(self):
        """初始化监控数据"""
        try:
            # 获取初始数据
            cache_stats = self.storage_manager.get_cache_stats()
            page_stats = self.storage_manager.get_page_stats()

            current_time = time.time()
            self.monitor_data['timestamps'].append(current_time)
            self.monitor_data['hit_rates'].append(cache_stats.get('hit_rate', 0))
            self.monitor_data['page_allocations'].append(page_stats['pages']['allocated'])
            self.monitor_data['transaction_counts'].append(0)
            self.monitor_data['read_operations'].append(page_stats['operations']['reads'])
            self.monitor_data['write_operations'].append(page_stats['operations']['writes'])
            # 初始绘制页面地图
            self.window.after(100, self._draw_page_allocation_map)

        except Exception as e:
            self._log(f"初始化数据失败: {e}")

    def _update_monitoring_data(self):
        """更新监控数据"""
        if not self.monitoring_active:
            return

        try:
            # 获取最新统计信息
            cache_stats = self.storage_manager.get_cache_stats()
            page_stats = self.storage_manager.get_page_stats()

            # 获取事务统计
            txn_count = 0
            if hasattr(self.storage_manager, 'transaction_manager'):
                active_txns = self.storage_manager.get_active_transactions()
                txn_count = len(active_txns)

            # 添加数据点
            current_time = time.time()
            self.monitor_data['timestamps'].append(current_time)
            self.monitor_data['hit_rates'].append(cache_stats.get('hit_rate', 0))
            self.monitor_data['page_allocations'].append(page_stats['pages']['allocated'])
            self.monitor_data['transaction_counts'].append(txn_count)

            # 使用相对值（当前值 - 基准值）
            current_read_count = getattr(self.storage_manager, 'read_count', 0)
            current_write_count = getattr(self.storage_manager, 'write_count', 0)

            relative_read_ops = current_read_count - self.base_read_count
            relative_write_ops = current_write_count - self.base_write_count

            self.monitor_data['read_operations'].append(relative_read_ops)
            self.monitor_data['write_operations'].append(relative_write_ops)

            # 更新界面显示
            self._update_stats_display(cache_stats, page_stats, txn_count)
            self._update_charts()
            self._update_detail_panels()

        except Exception as e:
            self._log(f"监控数据更新失败: {e}")

        # 继续定时更新
        if self.monitoring_active:
            self.monitor_timer = self.window.after(2000, self._update_monitoring_data)

    def _update_stats_display(self, cache_stats, page_stats, txn_count):
        """更新统计显示"""
        try:
            # 更新缓存命中率
            hit_rate = cache_stats.get('hit_rate', 0)
            self.缓存命中率_label.configure(text=f"{hit_rate:.1f}%")

            # 更新页面统计
            allocated = page_stats['pages']['allocated']
            max_pages = page_stats['pages'].get('max_pages', allocated)
            self.页面分配_label.configure(text=f"{allocated}/{max_pages}")

            # 更新活跃事务
            self.活跃事务_label.configure(text=str(txn_count))

            # 更新缓存策略
            strategy = self._get_current_strategy()
            self.缓存策略_label.configure(text=strategy)

            # 更新系统状态
            status = "运行正常" if not self.storage_manager.is_shutdown else "已关闭"
            self.系统状态_label.configure(text=status)

        except Exception as e:
            self._log(f"统计显示更新失败: {e}")

    def _get_current_strategy(self):
        """获取当前缓存策略"""
        try:
            if hasattr(self.storage_manager.buffer_pool, '_strategy'):
                strategy_name = type(self.storage_manager.buffer_pool._strategy).__name__
                if "LRU" in strategy_name:
                    return "LRU"
                elif "FIFO" in strategy_name:
                    return "FIFO"
                elif "Adaptive" in strategy_name:
                    return "自适应"
            return "LRU"
        except:
            return "未知"

    def _update_charts(self):
        """更新图表"""
        try:
            if len(self.monitor_data['timestamps']) < 2:
                return

            # 清除旧图表
            for ax in [self.ax1, self.ax2, self.ax3, self.ax4]:
                ax.clear()

            # 重新初始化图表
            self._init_charts()

            # 准备数据
            times = list(self.monitor_data['timestamps'])
            start_time = times[0]
            relative_times = [(t - start_time) / 60 for t in times]  # 转换为分钟

            # 绘制缓存命中率
            self.ax1.plot(relative_times, list(self.monitor_data['hit_rates']),
                          'b-', linewidth=2, marker='o', markersize=3, alpha=0.8)

            # 绘制页面分配
            self.ax2.plot(relative_times, list(self.monitor_data['page_allocations']),
                          'g-', linewidth=2, marker='s', markersize=3, alpha=0.8)

            # 绘制事务统计
            self.ax3.plot(relative_times, list(self.monitor_data['transaction_counts']),
                          'orange', linewidth=2, marker='^', markersize=3, alpha=0.8)

            # 绘制I/O操作
            reads = list(self.monitor_data['read_operations'])
            writes = list(self.monitor_data['write_operations'])
            self.ax4.plot(relative_times, reads, 'r-', linewidth=2, label='Read Ops', alpha=0.8)
            self.ax4.plot(relative_times, writes, 'purple', linewidth=2, label='Write Ops', alpha=0.8)
            self.ax4.legend()

            # 设置x轴标签
            for ax in [self.ax3, self.ax4]:
                ax.set_xlabel('Time (minutes)')

            # 刷新图表
            self.canvas.draw()

        except Exception as e:
            self._log(f"图表更新失败: {e}")

    def _update_detail_panels(self):
        """更新详细信息面板"""
        try:
            self._update_tablespace_panel()
            self._update_cache_panel()
            self._update_transaction_panel()
            # 移除 self._draw_page_allocation_map() 这一行

            # 添加：更新页面分配统计
            if hasattr(self, 'page_stats_label'):
                allocated = len(self.storage_manager.page_manager.get_allocated_pages())
                free = self.storage_manager.page_manager.get_free_page_count()
                self.page_stats_label.configure(
                    text=f"已分配: {allocated} 页 | 空闲: {free} 页"
                )

            # 如果页面地图窗口打开，也更新它
            if hasattr(self, 'page_map_window') and self.page_map_window and self.page_map_window.winfo_exists():
                self._draw_page_map_in_window()

        except Exception as e:
            self._log(f"详细面板更新失败: {e}")

    def _update_tablespace_panel(self):
        """更新表空间面板"""
        # 清空现有数据
        for item in self.tablespace_tree.get_children():
            self.tablespace_tree.delete(item)

        try:
            tablespaces = self.storage_manager.list_tablespaces()
            for ts in tablespaces:
                name = ts.get('name', 'unknown')
                size_mb = ts.get('size_mb', 0)
                used_mb = ts.get('used_mb', 0)
                usage_pct = (used_mb / size_mb * 100) if size_mb > 0 else 0
                status = ts.get('status', 'active')

                self.tablespace_tree.insert("", tk.END, values=(
                    name, f"{size_mb}MB", f"{used_mb}MB",
                    f"{usage_pct:.1f}%", status
                ))
        except Exception as e:
            self._log(f"表空间信息更新失败: {e}")

    def _update_cache_panel(self):
        """更新缓存面板"""
        # 清空现有数据
        for item in self.cache_tree.get_children():
            self.cache_tree.delete(item)

        try:
            cache_info = self.storage_manager.buffer_pool.get_cache_info()
            cache_details = cache_info.get('cache_details', {})

            for page_id, details in list(cache_details.items())[:20]:  # 只显示前20个
                is_dirty = "是" if details.get('is_dirty', False) else "否"
                data_size = details.get('data_size', 0)
                access_time = details.get('age_seconds', 0)

                self.cache_tree.insert("", tk.END, values=(
                    page_id, is_dirty, "N/A", f"{access_time:.1f}s前",
                    f"{data_size}B"
                ))
        except Exception as e:
            self._log(f"缓存信息更新失败: {e}")

    def _update_transaction_panel(self):
        """更新事务面板"""
        # 清空现有数据
        for item in self.txn_tree.get_children():
            self.txn_tree.delete(item)

        try:
            if hasattr(self.storage_manager, 'transaction_manager'):
                active_txns = self.storage_manager.get_active_transactions()

                for txn_id in active_txns:
                    txn_info = self.storage_manager.get_transaction_info(txn_id)
                    if txn_info and not txn_info.get('error'):
                        state = txn_info.get('state', 'UNKNOWN')
                        start_time = txn_info.get('start_time', 0)
                        modified_pages = len(txn_info.get('modified_pages', []))
                        isolation = txn_info.get('isolation_level', 'Unknown')

                        time_str = time.strftime("%H:%M:%S", time.localtime(start_time))

                        self.txn_tree.insert("", tk.END, values=(
                            txn_id, state, time_str, modified_pages, "N/A", isolation
                        ))
        except Exception as e:
            self._log(f"事务信息更新失败: {e}")

    # 控制操作方法
    def _change_strategy(self):
        """改变缓存策略"""
        strategy = self.strategy_var.get()
        self._log(f"请求切换缓存策略到: {strategy}")
        messagebox.showinfo("策略切换", f"缓存策略已切换到: {strategy}")

    def _run_test(self, test_type):
        """运行性能测试"""
        self._log(f"开始执行{test_type}测试...")

        # 在新线程中运行测试，避免阻塞界面
        def run_test_thread():
            try:
                if test_type == "sequential":
                    self._sequential_test()
                elif test_type == "random":
                    self._random_test()
                elif test_type == "repeat":
                    self._repeat_test()
                elif test_type == "stress":
                    self._stress_test()

                self.window.after(0, lambda: messagebox.showinfo("测试完成", f"{test_type}测试已完成"))
            except Exception as e:
                self.window.after(0, lambda: messagebox.showerror("测试失败", f"测试执行失败: {e}"))

        thread = threading.Thread(target=run_test_thread, daemon=True)
        thread.start()

    def _sequential_test(self):
        """顺序访问测试"""
        self._log("执行顺序访问测试...")
        allocated_pages = []

        # 顺序分配和访问页面
        for i in range(15):
            try:
                page_id = self.storage_manager.allocate_page()
                test_data = f"Sequential test data {i}".ljust(4096, '0').encode()
                self.storage_manager.write_page(page_id, test_data)
                allocated_pages.append(page_id)
                time.sleep(0.1)  # 短暂延迟以观察效果
            except Exception as e:
                self._log(f"顺序测试步骤 {i} 失败: {e}")

        # 顺序读取
        for page_id in allocated_pages:
            try:
                self.storage_manager.read_page(page_id)
                time.sleep(0.05)
            except Exception as e:
                self._log(f"顺序读取页面 {page_id} 失败: {e}")

        self._log("顺序访问测试完成")

    def _random_test(self):
        """随机访问测试"""
        self._log("执行随机访问测试...")
        allocated_pages = []

        # 先分配一些页面
        for i in range(10):
            try:
                page_id = self.storage_manager.allocate_page()
                test_data = f"Random test data {i}".ljust(4096, '0').encode()
                self.storage_manager.write_page(page_id, test_data)
                allocated_pages.append(page_id)
            except Exception as e:
                self._log(f"随机测试准备 {i} 失败: {e}")

        # 随机访问
        for _ in range(30):
            if allocated_pages:
                try:
                    page_id = random.choice(allocated_pages)
                    self.storage_manager.read_page(page_id)
                    time.sleep(0.05)
                except Exception as e:
                    self._log(f"随机访问失败: {e}")

        self._log("随机访问测试完成")

    def _repeat_test(self):
        """重复访问测试"""
        self._log("执行重复访问测试...")
        allocated_pages = []

        # 分配少量页面
        for i in range(3):
            try:
                page_id = self.storage_manager.allocate_page()
                test_data = f"Repeat test data {i}".ljust(4096, '0').encode()
                self.storage_manager.write_page(page_id, test_data)
                allocated_pages.append(page_id)
            except Exception as e:
                self._log(f"重复测试准备 {i} 失败: {e}")

        # 重复访问前两个页面
        if allocated_pages:
            for _ in range(25):
                try:
                    page_id = allocated_pages[0] if random.random() < 0.7 else allocated_pages[1]
                    self.storage_manager.read_page(page_id)
                    time.sleep(0.03)
                except Exception as e:
                    self._log(f"重复访问失败: {e}")

        self._log("重复访问测试完成")

    def _stress_test(self):
        """压力测试"""
        self._log("执行压力测试...")

        # 快速分配和访问大量页面
        allocated_pages = []
        for i in range(50):
            try:
                page_id = self.storage_manager.allocate_page()
                test_data = f"Stress test {i}".ljust(4096, '0').encode()
                self.storage_manager.write_page(page_id, test_data)
                allocated_pages.append(page_id)

                # 混合读写操作
                if i > 10 and random.random() < 0.3:
                    read_page = random.choice(allocated_pages)
                    self.storage_manager.read_page(read_page)

            except Exception as e:
                self._log(f"压力测试步骤 {i} 失败: {e}")

        self._log("压力测试完成")

    def _force_flush(self):
        """强制刷盘"""
        try:
            flushed_count = self.storage_manager.flush_all_pages()
            self._log(f"强制刷盘完成，刷新了 {flushed_count} 个页面")
            messagebox.showinfo("操作完成", f"成功刷新 {flushed_count} 个脏页到磁盘")
        except Exception as e:
            self._log(f"强制刷盘失败: {e}")
            messagebox.showerror("操作失败", f"刷盘失败: {e}")

    def _clear_cache(self):
        """清理缓存"""
        try:
            flushed_count = self.storage_manager.flush_all_pages()
            self.storage_manager.buffer_pool.clear()
            self._log(f"缓存已清理，刷新了 {flushed_count} 个脏页")
            messagebox.showinfo("操作完成", f"缓存已清理，刷新了 {flushed_count} 个脏页")
        except Exception as e:
            self._log(f"清理缓存失败: {e}")
            messagebox.showerror("操作失败", f"清理失败: {e}")

    def _export_report(self):
        """导出性能报告"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"storage_performance_report_{timestamp}.txt"

            # 获取统计信息
            cache_stats = self.storage_manager.get_cache_stats()
            page_stats = self.storage_manager.get_page_stats()

            # 生成报告内容
            report = f"""存储系统性能报告
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'=' * 50}

缓存统计:
- 命中率: {cache_stats.get('hit_rate', 0):.2f}%
- 总请求数: {cache_stats.get('total_requests', 0)}
- 命中次数: {cache_stats.get('hit_count', 0)}
- 缓存大小: {cache_stats.get('cache_size', 0)}/{cache_stats.get('cache_capacity', 0)}
- 脏页数: {cache_stats.get('dirty_pages', 0)}

页面统计:
- 已分配页面: {page_stats['pages']['allocated']}
- 空闲页面: {page_stats['pages']['free']}
- 读操作次数: {page_stats['operations']['reads']}
- 写操作次数: {page_stats['operations']['writes']}
- 分配操作次数: {page_stats['operations']['allocations']}
- 释放操作次数: {page_stats['operations']['deallocations']}

监控数据摘要:
- 监控时长: {len(self.monitor_data['timestamps'])} 个数据点
- 平均命中率: {sum(self.monitor_data['hit_rates']) / max(len(self.monitor_data['hit_rates']), 1):.2f}%
- 当前页面分配: {list(self.monitor_data['page_allocations'])[-1] if self.monitor_data['page_allocations'] else 0}
"""

            with open(filename, 'w', encoding='utf-8') as f:
                f.write(report)

            self._log(f"性能报告已导出到: {filename}")
            messagebox.showinfo("导出成功", f"性能报告已保存到: {filename}")

        except Exception as e:
            self._log(f"导出报告失败: {e}")
            messagebox.showerror("导出失败", f"导出失败: {e}")

    def _reset_statistics(self):
        """重置统计数据"""
        try:
            # 更新基准值为当前值
            self.base_read_count = getattr(self.storage_manager, 'read_count', 0)
            self.base_write_count = getattr(self.storage_manager, 'write_count', 0)

            # 清空监控数据
            for key in self.monitor_data:
                self.monitor_data[key].clear()

            # 重新初始化（从0开始）
            current_time = time.time()
            self.monitor_data['timestamps'].append(current_time)
            self.monitor_data['hit_rates'].append(self.storage_manager.get_cache_stats().get('hit_rate', 0))
            self.monitor_data['page_allocations'].append(self.storage_manager.get_page_stats()['pages']['allocated'])
            self.monitor_data['transaction_counts'].append(0)
            self.monitor_data['read_operations'].append(0)  # 从0开始
            self.monitor_data['write_operations'].append(0)  # 从0开始

            # 清空日志
            self.log_text.delete(1.0, tk.END)

            self._log(f"统计数据已重置 (新基准: 读={self.base_read_count}, 写={self.base_write_count})")
            messagebox.showinfo("重置完成", "所有统计数据已重置")

        except Exception as e:
            self._log(f"重置失败: {e}")
            messagebox.showerror("重置失败", f"重置失败: {e}")

    def _log(self, message):
        """记录日志"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"

        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)

    def _on_closing(self):
        """关闭窗口时的处理"""
        if self.monitoring_active:
            self._stop_monitoring()
        self.window.destroy()


# 测试函数
def test_monitor_window():
    """测试存储监控窗口"""

    # 这里需要模拟一个storage_manager
    class MockStorageManager:
        def __init__(self):
            self.is_shutdown = False
            self.buffer_pool = self

        def get_cache_stats(self):
            return {'hit_rate': random.uniform(70, 95), 'total_requests': 1000,
                    'hit_count': 800, 'cache_size': 10, 'cache_capacity': 20, 'dirty_pages': 3}

        def get_page_stats(self):
            return {'pages': {'allocated': random.randint(10, 50), 'free': 5},
                    'operations': {'reads': 500, 'writes': 300, 'allocations': 50, 'deallocations': 5}}

        def get_active_transactions(self):
            return []

        def list_tablespaces(self):
            return [{'name': 'default', 'size_mb': 100, 'used_mb': 30, 'status': 'active'}]

        def get_cache_info(self):
            return {'cache_details': {}}

        def flush_all_pages(self):
            return 5

        def clear(self):
            pass

        def allocate_page(self):
            return random.randint(1, 1000)

        def write_page(self, page_id, data):
            pass

        def read_page(self, page_id):
            return b'test data'

    root = tk.Tk()
    root.withdraw()  # 隐藏主窗口

    mock_storage = MockStorageManager()
    monitor = StorageMonitorWindow(mock_storage)
    monitor.show()

    root.mainloop()


if __name__ == "__main__":
    test_monitor_window()