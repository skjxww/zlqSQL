import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext


class SmartDiagnosisPanel:
    def __init__(self, parent):
        self.parent = parent
        self.frame = ttk.LabelFrame(parent, text="智能诊断", padding="5")
        self.frame.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        self.frame.columnconfigure(0, weight=1)

        # 当前错误分析
        self.current_error_analysis = None

        self._create_widgets()

    def _create_widgets(self):
        """创建智能诊断组件"""
        # 诊断状态
        self.diagnosis_label = ttk.Label(self.frame, text="诊断状态: 待检查", foreground="gray")
        self.diagnosis_label.grid(row=0, column=0, sticky=tk.W)

        # 建议计数
        self.suggestion_label = ttk.Label(self.frame, text="建议: 0 项")
        self.suggestion_label.grid(row=1, column=0, sticky=tk.W)

        # 查看详情按钮
        self.details_btn = ttk.Button(
            self.frame,
            text="📋 查看诊断详情",
            command=self._show_diagnosis_details,
            state=tk.DISABLED
        )
        self.details_btn.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(5, 0))

    def update_diagnosis(self, analysis, success=None):
        """更新诊断显示"""
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
        dialog = tk.Toplevel(self.parent)
        dialog.title("🔍 SQL 智能错误分析")
        dialog.geometry("700x500")
        dialog.transient(self.parent)
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
        dialog = tk.Toplevel(self.parent)
        dialog.title("💡 SQL 优化建议")
        dialog.geometry("600x400")
        dialog.transient(self.parent)
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

        dialog = tk.Toplevel(self.parent)
        dialog.title("🔧 SQL 修正选项")
        dialog.geometry("900x600")
        dialog.transient(self.parent)
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
        correction_choice = tk.StringVar(value="none")

        # "不使用修正"选项
        no_correction_frame = ttk.Frame(scrollable_frame)
        no_correction_frame.pack(fill=tk.X, pady=5)

        ttk.Radiobutton(
            no_correction_frame,
            text="❌ 不使用修正，返回原始错误",
            variable=correction_choice,
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
                variable=correction_choice,
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
            choice = correction_choice.get()
            dialog.destroy()

            if choice != "none":
                choice_idx = int(choice)
                corrected_sql = analysis['corrected_sql_options'][choice_idx]['sql']
                self._on_correction_selected(corrected_sql)

        # 按钮
        ttk.Button(
            button_frame,
            text="🚀 应用并执行",
            command=apply_correction,
            style="Execute.TButton"
        ).pack(side=tk.RIGHT, padx=(5, 0))

        ttk.Button(button_frame, text="取消", command=dialog.destroy).pack(side=tk.RIGHT)

    def _on_correction_selected(self, corrected_sql):
        """当修正被选择时的回调"""
        # 这里应该通过事件或回调机制通知SQL查询标签页
        messagebox.showinfo("修正选择", f"已选择修正: {corrected_sql}")

    def clear_diagnosis(self):
        """清除诊断信息"""
        self.current_error_analysis = None
        self.diagnosis_label.configure(text="诊断状态: 待检查", foreground="gray")
        self.suggestion_label.configure(text="建议: 0 项")
        self.details_btn.configure(state=tk.DISABLED)