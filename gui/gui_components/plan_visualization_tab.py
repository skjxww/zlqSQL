import json
import tkinter as tk
from tkinter import ttk, messagebox


class PlanVisualizationTab:
    def __init__(self, parent_notebook, ai_manager):
        self.ai_manager = ai_manager
        self.last_execution_plan = None  # 初始化执行计划数据

        # 创建执行计划可视化标签页
        self.plan_frame = ttk.Frame(parent_notebook)
        self.frame = self.plan_frame
        parent_notebook.add(self.plan_frame, text="执行计划")

        # 创建界面组件
        self._create_plan_visualization_interface()

        # 初始化时尝试获取数据
        self._refresh_plan()

    def _create_plan_visualization_interface(self):
        """创建执行计划可视化界面"""
        # 主框架
        main_frame = ttk.Frame(self.plan_frame, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 标题
        ttk.Label(main_frame, text="执行计划可视化", font=("Arial", 14, "bold")).grid(row=0, column=0, pady=(0, 20))

        # 显示区域
        self.plan_canvas = tk.Canvas(main_frame, bg="white", width=800, height=600, relief=tk.SUNKEN, borderwidth=2)
        self.plan_canvas.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # 滚动条
        v_scrollbar = ttk.Scrollbar(main_frame, orient=tk.VERTICAL, command=self.plan_canvas.yview)
        v_scrollbar.grid(row=1, column=1, sticky=(tk.N, tk.S))
        self.plan_canvas.configure(yscrollcommand=v_scrollbar.set)

        h_scrollbar = ttk.Scrollbar(main_frame, orient=tk.HORIZONTAL, command=self.plan_canvas.xview)
        h_scrollbar.grid(row=2, column=0, sticky=(tk.W, tk.E))
        self.plan_canvas.configure(xscrollcommand=h_scrollbar.set)

        # 控制按钮
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=3, column=0, pady=(10, 0))

        ttk.Button(button_frame, text="刷新计划", command=self._refresh_plan).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(button_frame, text="保存图片", command=self._save_plan).pack(side=tk.LEFT)

        # 配置权重
        self.plan_frame.columnconfigure(0, weight=1)
        self.plan_frame.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)

        # 显示提示信息
        self._show_placeholder()

    def _show_placeholder(self):
        """显示占位提示"""
        self.plan_canvas.delete("all")  # 清空现有内容
        self.plan_canvas.create_text(
            400, 300,
            text="执行计划可视化\n\n请先在SQL查询标签页执行一条SQL语句\n然后返回此处查看执行计划",
            font=("Arial", 12),
            fill="gray",
            justify=tk.CENTER
        )

    def _update_plan_visualization(self, event=None):
        """更新执行计划可视化"""
        if not self.plan_visualizer or not self.last_execution_plan:
            self.plan_text.delete(1.0, tk.END)
            self.plan_text.insert(tk.END, "没有可用的执行计划数据。\n请先在SQL查询标签页执行一条SQL语句。")
            return

        try:
            format_type = self.plan_format_var.get()
            visualization = self.plan_visualizer.visualize_plan(self.last_execution_plan, format_type)

            self.plan_text.delete(1.0, tk.END)
            self.plan_text.insert(tk.END, visualization)

            # 更新统计信息
            self._update_plan_statistics()

        except Exception as e:
            self.plan_text.delete(1.0, tk.END)
            self.plan_text.insert(tk.END,
                                  f"可视化生成失败: {str(e)}\n\n原始执行计划:\n{json.dumps(self.last_execution_plan, indent=2, ensure_ascii=False)}")

    def _update_plan_statistics(self):
        """更新执行计划统计"""
        if not self.last_execution_plan:
            return

        try:
            stats = self._analyze_plan_stats(self.last_execution_plan)

            stats_content = "📊 执行计划统计\n" + "=" * 30 + "\n"
            stats_content += f"节点总数: {stats['node_count']}\n"
            stats_content += f"最大深度: {stats['max_depth']}\n"
            stats_content += f"扫描操作: {stats['scan_count']}\n"
            stats_content += f"连接操作: {stats['join_count']}\n"
            stats_content += f"过滤操作: {stats['filter_count']}\n"

            self.plan_stats_text.delete(1.0, tk.END)
            self.plan_stats_text.insert(tk.END, stats_content)

        except Exception as e:
            self.plan_stats_text.delete(1.0, tk.END)
            self.plan_stats_text.insert(tk.END, f"统计分析失败: {e}")

    def _analyze_plan_stats(self, plan):
        """分析执行计划统计"""
        stats = {
            'node_count': 0,
            'max_depth': 0,
            'scan_count': 0,
            'join_count': 0,
            'filter_count': 0
        }

        def analyze_node(node, depth=0):
            stats['node_count'] += 1
            stats['max_depth'] = max(stats['max_depth'], depth)

            node_type = node.get('type', '')
            if 'Scan' in node_type:
                stats['scan_count'] += 1
            elif 'Join' in node_type:
                stats['join_count'] += 1
            elif 'Filter' in node_type:
                stats['filter_count'] += 1

            for child in node.get('children', []):
                if isinstance(child, dict):
                    analyze_node(child, depth + 1)

        analyze_node(plan)
        return stats

    def _show_plan_visualization(self):
        """显示执行计划可视化"""
        if hasattr(self, 'last_execution_plan') and self.last_execution_plan:
            from extensions.plan_visualizer import PlanVisualizationDialog
            PlanVisualizationDialog(self.root, self.last_execution_plan)
        else:
            messagebox.showinfo("提示", "请先执行一条SQL语句")

    def _export_plan(self):
        """导出执行计划"""
        if not self.last_execution_plan:
            messagebox.showinfo("提示", "没有可导出的执行计划")
            return

        from tkinter import filedialog

        filename = filedialog.asksaveasfilename(
            defaultextension=".html",
            filetypes=[("HTML文件", "*.html"), ("文本文件", "*.txt"), ("JSON文件", "*.json")]
        )

        if filename:
            try:
                if filename.endswith('.html'):
                    content = self.plan_visualizer.visualize_plan(self.last_execution_plan, 'html')
                elif filename.endswith('.json'):
                    content = json.dumps(self.last_execution_plan, indent=2, ensure_ascii=False)
                else:
                    content = self.plan_visualizer.visualize_plan(self.last_execution_plan, 'text')

                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content)

                messagebox.showinfo("导出成功", f"执行计划已导出到: {filename}")
            except Exception as e:
                messagebox.showerror("导出失败", f"导出执行计划失败: {e}")

    def _copy_plan(self):
        """复制执行计划"""
        content = self.plan_text.get(1.0, tk.END)
        self.root.clipboard_clear()
        self.root.clipboard_append(content)
        messagebox.showinfo("复制成功", "执行计划已复制到剪贴板")

    def _refresh_plan(self):
        """刷新执行计划"""
        try:
            # 从ai_manager获取最新的执行计划数据
            if hasattr(self.ai_manager, 'last_execution_plan') and self.ai_manager.last_execution_plan:
                self.last_execution_plan = self.ai_manager.last_execution_plan
                print(f"获取到执行计划数据: {type(self.last_execution_plan)}")  # 调试信息
                self._draw_plan_visualization()
            else:
                print("未找到执行计划数据")  # 调试信息
                self._show_placeholder()
        except Exception as e:
            print(f"刷新执行计划时出错: {e}")  # 调试信息
            self._show_placeholder()
            messagebox.showerror("错误", f"刷新执行计划失败: {e}")

    def _save_plan(self):
        """保存执行计划图片"""
        if not hasattr(self, 'last_execution_plan') or not self.last_execution_plan:
            messagebox.showinfo("提示", "没有可保存的执行计划")
            return

        from tkinter import filedialog

        filename = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG文件", "*.png"), ("JPEG文件", "*.jpg"), ("所有文件", "*.*")]
        )

        if filename:
            try:
                # 这里需要实现将canvas保存为图片的功能
                # 由于tkinter的canvas不能直接保存为图片，可能需要使用PIL库
                messagebox.showinfo("提示", "保存图片功能需要安装PIL库才能使用")
            except Exception as e:
                messagebox.showerror("保存失败", f"保存图片失败: {e}")

    def _draw_plan_visualization(self):
        """绘制执行计划可视化"""
        if not self.last_execution_plan:
            self._show_placeholder()
            return

        # 清空画布
        self.plan_canvas.delete("all")

        try:
            print(f"开始绘制执行计划: {json.dumps(self.last_execution_plan, indent=2, default=str)[:200]}...")  # 调试信息

            # 绘制执行计划树形结构
            self._draw_plan_node(self.last_execution_plan, 400, 50, 0)

            # 更新滚动区域
            self.plan_canvas.configure(scrollregion=self.plan_canvas.bbox("all"))

        except Exception as e:
            print(f"绘制执行计划时出错: {e}")  # 调试信息
            self.plan_canvas.create_text(
                400, 300,
                text=f"绘制执行计划失败: {str(e)}",
                font=("Arial", 12),
                fill="red",
                justify=tk.CENTER
            )

    def _draw_plan_node(self, node, x, y, level, parent_x=None, parent_y=None):
        """递归绘制执行计划节点"""
        if not isinstance(node, dict):
            print(f"节点不是字典类型: {type(node)}")  # 调试信息
            return y

        # 节点信息 - 兼容不同的执行计划格式
        node_type = node.get('Node Type', node.get('type', node.get('operation', 'Unknown')))
        relation_name = node.get('Relation Name', node.get('relation', node.get('table', '')))
        cost = node.get('Total Cost', node.get('cost', node.get('estimated_cost', 0)))

        # 节点文本
        node_text = f"{node_type}"
        if relation_name:
            node_text += f"\n({relation_name})"

        # 处理成本显示
        if isinstance(cost, (int, float)):
            node_text += f"\nCost: {cost:.2f}"
        elif cost:
            node_text += f"\nCost: {cost}"

        # 绘制节点矩形
        rect_width = 120
        rect_height = 60
        rect = self.plan_canvas.create_rectangle(
            x - rect_width // 2, y - rect_height // 2,
            x + rect_width // 2, y + rect_height // 2,
            fill="lightblue", outline="blue", width=2
        )

        # 绘制节点文本
        text = self.plan_canvas.create_text(
            x, y, text=node_text, font=("Arial", 9),
            justify=tk.CENTER, width=rect_width - 10
        )

        # 绘制连接线到父节点
        if parent_x is not None and parent_y is not None:
            self.plan_canvas.create_line(
                parent_x, parent_y + 30, x, y - 30,
                fill="gray", width=2, arrow=tk.LAST
            )

        # 处理子节点 - 兼容不同的子节点字段名
        children = node.get('Plans', node.get('children', node.get('inputs', [])))
        if children and isinstance(children, list):
            child_y = y + 120
            child_count = len(children)
            if child_count == 1:
                child_x_positions = [x]
            else:
                spacing = min(200, 800 // child_count)
                start_x = x - (child_count - 1) * spacing // 2
                child_x_positions = [start_x + i * spacing for i in range(child_count)]

            max_y = child_y
            for i, child in enumerate(children):
                if isinstance(child, dict):
                    child_x = child_x_positions[i]
                    end_y = self._draw_plan_node(child, child_x, child_y, level + 1, x, y)
                    max_y = max(max_y, end_y)

            return max_y

        return y + 60

    def update_plan(self, execution_plan):
        """外部调用此方法来更新执行计划数据"""
        self.last_execution_plan = execution_plan
        self._draw_plan_visualization()
