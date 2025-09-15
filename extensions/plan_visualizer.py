import json
import tkinter as tk
from typing import Dict, Any, List, Optional
from sql_compiler.codegen.operators import Operator


class ExecutionPlanVisualizer:
    """执行计划可视化器"""

    def __init__(self):
        self.node_counter = 0
        self.style_config = self._load_style_config()

    def visualize_plan(self, execution_plan: Dict[str, Any], format: str = 'text') -> str:
        """可视化执行计划"""
        self.node_counter = 0

        if format == 'text':
            return self._generate_text_tree(execution_plan)
        elif format == 'html':
            return self._generate_html_tree(execution_plan)
        elif format == 'mermaid':
            return self._generate_mermaid_diagram(execution_plan)
        elif format == 'json':
            return self._generate_enhanced_json(execution_plan)
        else:
            return self._generate_text_tree(execution_plan)

    def _generate_text_tree(self, plan: Dict[str, Any], level: int = 0, is_last: bool = True) -> str:
        """生成美化的文本树"""
        if level == 0:
            result = "📊 SQL执行计划树\n" + "=" * 50 + "\n\n"
        else:
            result = ""

        # 生成缩进和连接符
        indent = ""
        for i in range(level):
            if i == level - 1:
                indent += "└── " if is_last else "├── "
            else:
                indent += "    " if i in getattr(self, '_last_at_level', {}) else "│   "

        # 节点信息
        node_type = plan.get('type', 'Unknown')
        node_info = self._get_detailed_node_info(plan)
        cost_info = self._estimate_cost(plan)

        # 主节点行
        result += f"{indent}🔧 {node_type}\n"

        # 详细信息
        if node_info:
            for info_line in node_info:
                detail_indent = "    " * level + ("    " if is_last else "│   ")
                result += f"{detail_indent}   📋 {info_line}\n"

        # 成本信息
        if cost_info:
            detail_indent = "    " * level + ("    " if is_last else "│   ")
            result += f"{detail_indent}   💰 估计成本: {cost_info}\n"

        # 处理子节点
        children = plan.get('children', [])
        if children:
            # 记录当前层级是否是最后一个
            if not hasattr(self, '_last_at_level'):
                self._last_at_level = {}

            for i, child in enumerate(children):
                child_is_last = (i == len(children) - 1)
                if not child_is_last:
                    self._last_at_level[level] = False
                else:
                    self._last_at_level[level] = True

                if isinstance(child, dict):
                    result += self._generate_text_tree(child, level + 1, child_is_last)

        return result

    def _generate_html_tree(self, plan: Dict[str, Any]) -> str:
        """生成交互式HTML树"""
        html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>SQL执行计划可视化</title>
    <style>
        body {{ 
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace; 
            margin: 20px; 
            background: #f5f5f5; 
        }}
        .plan-header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px 10px 0 0;
            text-align: center;
        }}
        .tree-container {{
            background: white;
            border-radius: 0 0 10px 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .tree {{ list-style-type: none; margin: 0; padding: 0; }}
        .tree li {{ 
            margin: 5px 0; 
            padding: 10px 0 0 20px; 
            position: relative; 
            cursor: pointer;
        }}
        .tree li::before {{ 
            content: ''; 
            position: absolute; 
            top: 18px; 
            left: -1px; 
            border-left: 2px solid #ddd; 
            border-bottom: 2px solid #ddd; 
            width: 20px; 
            height: 20px; 
        }}
        .tree li::after {{ 
            content: ''; 
            position: absolute; 
            left: -1px; 
            bottom: -7px; 
            border-left: 2px solid #ddd; 
            height: 100%; 
        }}
        .tree li:last-child::after {{ display: none; }}
        .node {{ 
            border: 2px solid #333; 
            border-radius: 8px; 
            padding: 12px 16px; 
            display: inline-block; 
            background: #ffffff;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .node:hover {{ 
            transform: translateY(-2px); 
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }}
        .node-details {{ 
            background: #f8f9fa; 
            border: 1px solid #e9ecef; 
            padding: 10px; 
            margin: 8px 0; 
            border-radius: 5px;
            font-size: 0.9em;
            display: none;
        }}
        .cost-info {{ color: #28a745; font-weight: bold; }}
        .warning {{ color: #ffc107; }}
        .error {{ color: #dc3545; }}

# extensions/plan_visualizer.py (续)

        /* 操作符特定样式 */
        .SeqScanOp { border-color: #28a745; background: linear-gradient(135deg, #d4edda, #c3e6cb); }
        .IndexScanOp { border-color: #007bff; background: linear-gradient(135deg, #d1ecf1, #bee5eb); }
        .JoinOp { border-color: #fd7e14; background: linear-gradient(135deg, #ffeaa7, #fdcb6e); }
        .FilterOp { border-color: #6f42c1; background: linear-gradient(135deg, #e2d9f3, #d1a7dd); }
        .ProjectOp { border-color: #6c757d; background: linear-gradient(135deg, #f8f9fa, #e9ecef); }
        .GroupByOp { border-color: #795548; background: linear-gradient(135deg, #efebe9, #d7ccc8); }
        .SortOp { border-color: #e91e63; background: linear-gradient(135deg, #fce4ec, #f8bbd9); }
        
        .expand-btn {
            background: #007bff;
            color: white;
            border: none;
            padding: 4px 8px;
            border-radius: 3px;
            cursor: pointer;
            font-size: 12px;
            margin-left: 10px;
        }
    </style>
    <script>
        function toggleDetails(nodeId) {
            const details = document.getElementById('details-' + nodeId);
            const btn = document.getElementById('btn-' + nodeId);
            
            if (details.style.display === 'none' || !details.style.display) {
                details.style.display = 'block';
                btn.textContent = '收起';
            } else {
                details.style.display = 'none';
                btn.textContent = '详情';
            }
        }
        
        function highlightPath(nodeId) {
            // 高亮从根到当前节点的路径
            const node = document.getElementById('node-' + nodeId);
            node.style.boxShadow = '0 0 15px rgba(255, 193, 7, 0.8)';
            
            setTimeout(() => {
                node.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
            }, 2000);
        }
    </script>
</head>
<body>
    <div class="plan-header">
        <h2>🚀 SQL执行计划可视化分析</h2>
        <p>交互式执行计划树 - 点击节点查看详情</p>
    </div>
    <div class="tree-container">
        {tree_content}
    </div>
    <div style="margin-top: 20px; padding: 15px; background: #e9f7ef; border-radius: 5px;">
        <h4>📊 执行统计</h4>
        <p><strong>总节点数:</strong> {total_nodes}</p>
        <p><strong>预估总成本:</strong> {total_cost}</p>
        <p><strong>关键路径:</strong> {critical_path}</p>
    </div>
</body>
</html>
"""

        tree_content, stats = self._build_interactive_html_tree(plan, 0)
        return html_template.format(
            tree_content=tree_content,
            total_nodes=stats['total_nodes'],
            total_cost=stats['total_cost'],
            critical_path=stats['critical_path']
        )

    def _build_interactive_html_tree(self, plan: Dict[str, Any], node_id: int = 0) -> tuple:
        """构建交互式HTML树"""
        node_type = plan.get('type', 'Unknown')
        node_info = self._get_detailed_node_info(plan)
        cost = self._estimate_cost(plan)

        stats = {'total_nodes': 1, 'total_cost': cost, 'critical_path': node_type}

        html = '<ul class="tree"><li>'
        html += f'<div id="node-{node_id}" class="node {node_type}" onclick="highlightPath({node_id})">'
        html += f'<strong>🔧 {node_type}</strong>'
        html += f'<button id="btn-{node_id}" class="expand-btn" onclick="toggleDetails({node_id})">详情</button>'
        html += '</div>'

        # 详情面板
        html += f'<div id="details-{node_id}" class="node-details">'
        if node_info:
            for info_line in node_info:
                html += f'<div>📋 {info_line}</div>'
        html += f'<div class="cost-info">💰 估计成本: {cost}</div>'

        # 添加性能建议
        suggestions = self._get_performance_suggestions(plan)
        if suggestions:
            html += '<div><strong>💡 优化建议:</strong></div>'
            for suggestion in suggestions:
                html += f'<div style="margin-left: 10px;">• {suggestion}</div>'

        html += '</div>'

        # 处理子节点
        children = plan.get('children', [])
        if children:
            for i, child in enumerate(children):
                if isinstance(child, dict):
                    child_html, child_stats = self._build_interactive_html_tree(child, node_id * 10 + i + 1)
                    html += child_html
                    stats['total_nodes'] += child_stats['total_nodes']
                    stats['total_cost'] += child_stats['total_cost']

        html += '</li></ul>'
        return html, stats

    def _generate_mermaid_diagram(self, plan: Dict[str, Any]) -> str:
        """生成Mermaid流程图"""
        nodes = []
        edges = []
        styles = []
        self.node_counter = 0

        def process_node(node_plan, parent_id=None):
            current_id = f"N{self.node_counter}"
            self.node_counter += 1

            node_type = node_plan.get('type', 'Unknown')
            node_info = self._get_simple_node_info(node_plan)
            cost = self._estimate_cost(node_plan)

            # 创建节点标签
            label = f"{node_type}\\n成本: {cost}"
            if node_info:
                label += f"\\n{node_info[:30]}..."  # 限制长度

            # 选择节点形状和样式
            shape, style_class = self._get_mermaid_node_style(node_type)
            nodes.append(f"{current_id}{shape}[\"{label}\"]")
            styles.append(f"class {current_id} {style_class}")

            # 添加边
            if parent_id:
                edge_label = self._get_edge_label(node_plan)
                if edge_label:
                    edges.append(f"{parent_id} -->|{edge_label}| {current_id}")
                else:
                    edges.append(f"{parent_id} --> {current_id}")

            # 处理子节点
            children = node_plan.get('children', [])
            for child in children:
                if isinstance(child, dict):
                    process_node(child, current_id)

            return current_id

        root_id = process_node(plan)

        # 生成完整的Mermaid代码
        mermaid_code = "graph TD\n"
        mermaid_code += "\n".join(f"    {node}" for node in nodes) + "\n"
        mermaid_code += "\n".join(f"    {edge}" for edge in edges) + "\n"
        mermaid_code += "\n".join(f"    {style}" for style in styles) + "\n"

        # 添加样式定义
        mermaid_code += """
    classDef scan fill:#d4edda,stroke:#28a745,stroke-width:3px
    classDef index fill:#d1ecf1,stroke:#007bff,stroke-width:3px
    classDef join fill:#ffeaa7,stroke:#fd7e14,stroke-width:3px
    classDef filter fill:#e2d9f3,stroke:#6f42c1,stroke-width:3px
    classDef project fill:#f8f9fa,stroke:#6c757d,stroke-width:3px
    classDef aggregate fill:#efebe9,stroke:#795548,stroke-width:3px
    classDef sort fill:#fce4ec,stroke:#e91e63,stroke-width:3px
"""

        return mermaid_code

    def _get_detailed_node_info(self, plan: Dict[str, Any]) -> List[str]:
        """获取节点的详细信息"""
        node_type = plan.get('type', '')
        info = []

        if node_type == 'SeqScanOp':
            table_name = plan.get('table_name', '未知表')
            info.append(f"表名: {table_name}")
            info.append("扫描类型: 全表扫描")
            if plan.get('requires_transaction'):
                info.append("需要事务上下文")

        elif node_type == 'IndexScanOp':
            table_name = plan.get('table_name', '未知表')
            index_name = plan.get('index_name', '未知索引')
            info.append(f"表名: {table_name}")
            info.append(f"使用索引: {index_name}")
            info.append("扫描类型: 索引扫描")

        elif node_type == 'FilterOp':
            condition = plan.get('condition', {})
            if condition:
                info.append(f"过滤条件: {self._format_condition(condition)}")
            info.append("操作: 条件过滤")

        elif node_type == 'ProjectOp':
            columns = plan.get('columns', [])
            if columns:
                info.append(f"投影列: {', '.join(columns[:5])}")
                if len(columns) > 5:
                    info.append(f"... 共{len(columns)}列")
            info.append("操作: 列投影")

        elif node_type == 'JoinOp':
            join_type = plan.get('join_type', 'INNER')
            info.append(f"连接类型: {join_type} JOIN")
            condition = plan.get('on_condition', {})
            if condition:
                info.append(f"连接条件: {self._format_condition(condition)}")

        elif node_type == 'GroupByOp':
            group_cols = plan.get('group_columns', [])
            agg_funcs = plan.get('aggregate_functions', [])
            if group_cols:
                info.append(f"分组列: {', '.join(group_cols)}")
            if agg_funcs:
                info.append(f"聚合函数: {', '.join(f'{f[0]}({f[1]})' for f in agg_funcs)}")

        elif node_type == 'SortOp':
            sort_cols = plan.get('sort_columns', [])
            if sort_cols:
                sort_info = ', '.join(f"{col[0]} {col[1]}" for col in sort_cols)
                info.append(f"排序: {sort_info}")

        return info

    def _estimate_cost(self, plan: Dict[str, Any]) -> str:
        """估算节点成本"""
        node_type = plan.get('type', '')

        # 简单的成本估算模型
        base_costs = {
            'SeqScanOp': 100,
            'IndexScanOp': 10,
            'FilterOp': 5,
            'ProjectOp': 2,
            'JoinOp': 200,
            'GroupByOp': 150,
            'SortOp': 300,
        }

        base_cost = base_costs.get(node_type, 50)

        # 根据具体参数调整成本
        if node_type == 'JoinOp':
            join_type = plan.get('join_type', 'INNER')
            if join_type == 'CROSS':
                base_cost *= 3

        elif node_type == 'SortOp':
            sort_cols = plan.get('sort_columns', [])
            base_cost += len(sort_cols) * 50

        # 考虑子节点成本
        children = plan.get('children', [])
        child_cost = sum(int(self._estimate_cost(child).replace('units', ''))
                        for child in children if isinstance(child, dict))

        total_cost = base_cost + child_cost
        return f"{total_cost} units"

    def _get_performance_suggestions(self, plan: Dict[str, Any]) -> List[str]:
        """获取性能优化建议"""
        suggestions = []
        node_type = plan.get('type', '')

        if node_type == 'SeqScanOp':
            table_name = plan.get('table_name', '')
            suggestions.append(f"考虑为表 {table_name} 添加适当的索引")
            suggestions.append("如果只需要部分列，考虑使用SELECT指定列名而不是SELECT *")

        elif node_type == 'JoinOp':
            if not plan.get('on_condition'):
                suggestions.append("缺少JOIN条件可能导致笛卡尔积，检查JOIN条件")

            join_type = plan.get('join_type', 'INNER')
            if join_type == 'CROSS':
                suggestions.append("CROSS JOIN可能产生大量结果，确认是否必要")

        elif node_type == 'GroupByOp':
            group_cols = plan.get('group_columns', [])
            if len(group_cols) > 3:
                suggestions.append("过多的GROUP BY列可能影响性能，考虑优化分组条件")

        elif node_type == 'SortOp':
            suggestions.append("排序操作成本较高，考虑使用索引优化ORDER BY")

        return suggestions

# 集成到GUI的可视化界面
class PlanVisualizationDialog:
    """执行计划可视化对话框"""

    def __init__(self, parent, execution_plan):
        self.parent = parent
        self.execution_plan = execution_plan
        self.visualizer = ExecutionPlanVisualizer()
        self.create_dialog()

    def create_dialog(self):
        """创建可视化对话框"""
        import tkinter as tk
        from tkinter import ttk, scrolledtext
        import webbrowser
        import tempfile
        import os

        self.dialog = tk.Toplevel(self.parent)
        self.dialog.title("🔍 执行计划可视化")
        self.dialog.geometry("1000x700")
        self.dialog.transient(self.parent)

        # 主框架
        main_frame = ttk.Frame(self.dialog)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 工具栏
        toolbar = ttk.Frame(main_frame)
        toolbar.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(toolbar, text="可视化格式:").pack(side=tk.LEFT, padx=(0, 5))

        self.format_var = tk.StringVar(value="text")
        format_combo = ttk.Combobox(toolbar, textvariable=self.format_var,
                                  values=["text", "html", "mermaid", "json"])
        format_combo.pack(side=tk.LEFT, padx=(0, 10))
        format_combo.bind('<<ComboboxSelected>>', self.update_visualization)

        ttk.Button(toolbar, text="🔄 刷新",
                  command=self.update_visualization).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(toolbar, text="💾 导出HTML",
                  command=self.export_html).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(toolbar, text="📋 复制",
                  command=self.copy_to_clipboard).pack(side=tk.LEFT)

        # 可视化内容区域
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # 文本视图
        self.text_frame = ttk.Frame(self.notebook)
        self.text_area = scrolledtext.ScrolledText(self.text_frame, wrap=tk.WORD,
                                                  font=('Consolas', 10))
        self.text_area.pack(fill=tk.BOTH, expand=True)
        self.notebook.add(self.text_frame, text="📄 文本视图")

        # 统计信息视图
        self.stats_frame = ttk.Frame(self.notebook)
        self.stats_area = scrolledtext.ScrolledText(self.stats_frame, wrap=tk.WORD,
                                                   font=('Consolas', 10))
        self.stats_area.pack(fill=tk.BOTH, expand=True)
        self.notebook.add(self.stats_frame, text="📊 统计信息")

        # 初始化显示
        self.update_visualization()
        self.update_statistics()

    def update_visualization(self, event=None):
        """更新可视化显示"""
        format_type = self.format_var.get()

        try:
            if format_type == "html":
                # HTML格式直接保存到临时文件并打开浏览器
                content = self.visualizer.visualize_plan(self.execution_plan, 'html')
                self._show_html_content(content)
            else:
                content = self.visualizer.visualize_plan(self.execution_plan, format_type)
                self.text_area.delete(1.0, tk.END)
                self.text_area.insert(1.0, content)

        except Exception as e:
            error_msg = f"可视化生成失败: {str(e)}\n\n原始执行计划:\n{json.dumps(self.execution_plan, indent=2, ensure_ascii=False)}"
            self.text_area.delete(1.0, tk.END)
            self.text_area.insert(1.0, error_msg)

    def _show_html_content(self, html_content):
        """显示HTML内容"""
        self.text_area.delete(1.0, tk.END)
        self.text_area.insert(1.0, "HTML可视化已生成。点击'导出HTML'按钮在浏览器中查看交互式版本。\n\n预览:\n")

        # 显示简化的HTML结构
        preview = html_content[:1000] + "..." if len(html_content) > 1000 else html_content
        self.text_area.insert(tk.END, preview)

    def update_statistics(self):
        """更新统计信息"""
        stats = self._analyze_plan_statistics()

        stats_text = "📊 执行计划统计分析\n" + "=" * 50 + "\n\n"
        stats_text += f"总节点数: {stats['total_nodes']}\n"
        stats_text += f"最大深度: {stats['max_depth']}\n"
        stats_text += f"扫描操作: {stats['scan_ops']}\n"
        stats_text += f"连接操作: {stats['join_ops']}\n"
        stats_text += f"过滤操作: {stats['filter_ops']}\n"
        stats_text += f"聚合操作: {stats['aggregate_ops']}\n\n"

        stats_text += "🔍 性能分析:\n"
        for analysis in stats['performance_analysis']:
            stats_text += f"• {analysis}\n"

        stats_text += "\n💡 优化建议:\n"
        for suggestion in stats['optimization_suggestions']:
            stats_text += f"• {suggestion}\n"

        self.stats_area.delete(1.0, tk.END)
        self.stats_area.insert(1.0, stats_text)

    def _analyze_plan_statistics(self) -> Dict:
        """分析执行计划统计信息"""
        stats = {
            'total_nodes': 0,
            'max_depth': 0,
            'scan_ops': 0,
            'join_ops': 0,
            'filter_ops': 0,
            'aggregate_ops': 0,
            'performance_analysis': [],
            'optimization_suggestions': []
        }

        def analyze_node(node, depth=0):
            stats['total_nodes'] += 1
            stats['max_depth'] = max(stats['max_depth'], depth)

            node_type = node.get('type', '')
            if 'Scan' in node_type:
                stats['scan_ops'] += 1
            elif 'Join' in node_type:
                stats['join_ops'] += 1
            elif 'Filter' in node_type:
                stats['filter_ops'] += 1
            elif 'Group' in node_type:
                stats['aggregate_ops'] += 1

            # 递归分析子节点
            for child in node.get('children', []):
                if isinstance(child, dict):
                    analyze_node(child, depth + 1)

        analyze_node(self.execution_plan)

        # 生成性能分析
        if stats['scan_ops'] > stats['total_nodes'] * 0.7:
            stats['performance_analysis'].append("扫描操作占比较高，可能存在性能瓶颈")

        if stats['max_depth'] > 10:
            stats['performance_analysis'].append("执行计划层级过深，可能影响执行效率")

        if stats['join_ops'] > 3:
            stats['performance_analysis'].append("多表连接操作较多，注意连接顺序优化")

        # 生成优化建议
        if stats['scan_ops'] > 0:
            stats['optimization_suggestions'].append("考虑添加适当的索引减少全表扫描")

        if stats['join_ops'] > 0:
            stats['optimization_suggestions'].append("确保JOIN条件使用了索引列")

        return stats

    def export_html(self):
        """导出HTML文件"""
        import tempfile
        import webbrowser

        html_content = self.visualizer.visualize_plan(self.execution_plan, 'html')

        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
            f.write(html_content)
            html_file = f.name

        webbrowser.open(f'file://{html_file}')

        import tkinter.messagebox as messagebox
        messagebox.showinfo("导出成功", f"HTML文件已导出并在浏览器中打开:\n{html_file}")

    def copy_to_clipboard(self):
        """复制到剪贴板"""
        content = self.text_area.get(1.0, tk.END)
        self.dialog.clipboard_clear()
        self.dialog.clipboard_append(content)

        import tkinter.messagebox as messagebox
        messagebox.showinfo("复制成功", "内容已复制到剪贴板")