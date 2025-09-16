import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox


class DatabaseInfoPanel:
    def __init__(self, parent, db_manager):
        self.parent = parent
        self.db_manager = db_manager
        self.frame = ttk.LabelFrame(parent, text="数据库信息", padding="5")
        self.frame.grid(row=4, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        self.frame.columnconfigure(0, weight=1)

        self._create_widgets()
        self.tables_dict = {}

    def _create_widgets(self):
        """创建数据库信息组件"""
        # 状态信息
        self.status_label = ttk.Label(self.frame, text="状态: 就绪", foreground="green")
        self.status_label.grid(row=0, column=0, sticky=tk.W)

        # 表信息框架
        table_frame = ttk.Frame(self.frame)
        table_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(5, 0))
        table_frame.columnconfigure(0, weight=1)

        # 表标签
        ttk.Label(table_frame, text="表:").grid(row=0, column=0, sticky=tk.W)

        # 表列表（可点击）
        self.tables_listbox = tk.Listbox(
            table_frame,
            height=6,
            width=30,
            font=("Consolas", 9),
            selectmode=tk.SINGLE
        )
        self.tables_listbox.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(2, 0))
        self.tables_listbox.bind('<Double-1>', self._show_table_details)

        # 表列表滚动条
        table_scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.tables_listbox.yview)
        table_scrollbar.grid(row=1, column=1, sticky=(tk.N, tk.S))
        self.tables_listbox.configure(yscrollcommand=table_scrollbar.set)

        # 统计信息框架
        stats_frame = ttk.Frame(self.frame)
        stats_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(5, 0))

        # 表数量统计
        self.table_count_label = ttk.Label(stats_frame, text="表数量: 0")
        self.table_count_label.grid(row=0, column=0, sticky=tk.W, padx=(0, 10))

        # 总行数统计
        self.total_rows_label = ttk.Label(stats_frame, text="总行数: 0")
        self.total_rows_label.grid(row=0, column=1, sticky=tk.W)

        # 刷新按钮
        refresh_btn = ttk.Button(
            self.frame,
            text="🔄 刷新信息",
            command=self.refresh_info
        )
        refresh_btn.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(5, 0))

        # 操作按钮框架
        action_frame = ttk.Frame(self.frame)
        action_frame.grid(row=4, column=0, sticky=(tk.W, tk.E), pady=(5, 0))

        # # 查看表结构按钮
        # self.view_structure_btn = ttk.Button(
        #     action_frame,
        #     text="📊 查看表结构",
        #     command=self._view_selected_table_structure,
        #     state=tk.DISABLED
        # )
        # self.view_structure_btn.pack(side=tk.LEFT, padx=(0, 5))
        #
        # # 查看表数据按钮
        # self.view_data_btn = ttk.Button(
        #     action_frame,
        #     text="🔍 查看数据",
        #     command=self._view_selected_table_data,
        #     state=tk.DISABLED
        # )
        # self.view_data_btn.pack(side=tk.LEFT)

        # 绑定表选择事件
        self.tables_listbox.bind('<<ListboxSelect>>', self._on_table_select)

    def refresh_info(self):
        """刷新数据库信息"""
        try:
            print("DatabaseInfoPanel.refresh_info() 开始执行...")

            # 清空现有数据
            self.tables_listbox.delete(0, tk.END)
            self.tables_dict = {}

            # 检查 db_manager 是否存在
            if not hasattr(self, 'db_manager') or self.db_manager is None:
                self._log("错误: db_manager 不存在")
                self.status_label.configure(text="状态: db_manager 不存在", foreground="red")
                return

            # 检查 get_all_tables 方法是否存在
            if not hasattr(self.db_manager, 'get_all_tables'):
                self._log("错误: db_manager 没有 get_all_tables 方法")
                self.status_label.configure(text="状态: 方法不存在", foreground="red")
                return

            # 获取表信息字典
            print("正在调用 db_manager.get_all_tables()...")
            tables_dict = self.db_manager.get_all_tables()
            print(f"获取到的表字典类型: {type(tables_dict)}")
            print(f"获取到的表字典内容: {tables_dict}")

            if not tables_dict:
                self._log("警告: 获取到空的表字典")
                self.status_label.configure(text="状态: 无表数据", foreground="orange")
                # 更新统计信息
                self.table_count_label.configure(text="表数量: 0")
                self.total_rows_label.configure(text="总行数: 0")
                return

            if not isinstance(tables_dict, dict):
                self._log(f"错误: 期望字典类型，但得到了 {type(tables_dict)}")
                self.status_label.configure(text="状态: 数据格式错误", foreground="red")
                return

            # 保存表字典
            self.tables_dict = tables_dict

            # 添加到列表框中
            table_names = list(tables_dict.keys())
            total_rows = 0

            for table_name in table_names:
                table_info = tables_dict[table_name]

                # 获取行数
                row_count = 0
                if isinstance(table_info, dict):
                    row_count = table_info.get('rows', 0)

                # 显示格式: "表名 (行数: N)"
                display_text = table_name
                self.tables_listbox.insert(tk.END, display_text)
                total_rows += row_count

            # 更新统计信息
            self.table_count_label.configure(text=f"表数量: {len(table_names)}")
            self.total_rows_label.configure(text="")

            # 更新状态
            self.status_label.configure(text="状态: 就绪", foreground="green")
            self._log(f"成功获取 {len(table_names)} 张表")

        except Exception as e:
            error_msg = f"刷新信息失败: {str(e)}"
            self._log(error_msg)
            print(f"DatabaseInfoPanel.refresh_info() 异常: {e}")
            import traceback
            traceback.print_exc()

            # 显示错误状态
            self.status_label.configure(text="状态: 刷新失败", foreground="red")
            messagebox.showerror("错误", f"刷新数据库信息失败: {str(e)}")

    def _on_table_select(self, event):
        """当表被选择时的事件处理"""
        selection = self.tables_listbox.curselection()
        # if selection:
        #     self.view_structure_btn.configure(state=tk.NORMAL)
        #     self.view_data_btn.configure(state=tk.NORMAL)
        # else:
        #     self.view_structure_btn.configure(state=tk.DISABLED)
        #     self.view_data_btn.configure(state=tk.DISABLED)

    def _show_table_details(self, event):
        """显示表详细信息"""
        selection = self.tables_listbox.curselection()
        if not selection:
            return

        table_name = self._get_selected_table_name(selection[0])
        if not table_name:
            return

        try:
            # 获取表详细信息
            table_info = self.tables_dict.get(table_name, {})

            # 创建详情对话框
            dialog = tk.Toplevel(self.parent)
            dialog.title(f"表详细信息: {table_name}")
            dialog.geometry("800x600")
            dialog.transient(self.parent)
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
            info_tree.insert("", tk.END, text="创建时间", values=("", table_info.get('created_at', '未知')))
            info_tree.insert("", tk.END, text="更新时间", values=("", table_info.get('updated_at', '未知')))

            info_tree.pack(fill=tk.X)

            # 列信息
            columns_frame = ttk.LabelFrame(main_frame, text="列信息", padding="10")
            columns_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

            # 创建列信息表格
            columns_tree = ttk.Treeview(columns_frame, columns=("name", "type", "nullable", "default", "primary_key"),
                                        show="headings")
            columns_tree.heading("name", text="列名")
            columns_tree.heading("type", text="类型")
            columns_tree.heading("nullable", text="可空")
            columns_tree.heading("default", text="默认值")
            columns_tree.heading("primary_key", text="主键")

            # 设置列宽
            columns_tree.column("name", width=120)
            columns_tree.column("type", width=80)
            columns_tree.column("nullable", width=50)
            columns_tree.column("default", width=80)
            columns_tree.column("primary_key", width=50)

            # 添加列信息
            columns = table_info.get('columns', [])
            for column_info in columns:
                if isinstance(column_info, dict):
                    # 处理字典格式的列信息
                    name = column_info.get('name', '')
                    type_ = column_info.get('type', '')
                    nullable = "YES" if column_info.get('nullable', True) else "NO"
                    default = str(column_info.get('default', 'NULL'))
                    primary_key = "YES" if column_info.get('primary_key', False) else "NO"
                else:
                    # 处理字符串格式的列信息
                    name = str(column_info)
                    type_ = "VARCHAR"
                    nullable = "YES"
                    default = "NULL"
                    primary_key = "NO"

                columns_tree.insert("", tk.END, values=(name, type_, nullable, default, primary_key))

            # 添加滚动条
            scrollbar = ttk.Scrollbar(columns_frame, orient=tk.VERTICAL, command=columns_tree.yview)
            columns_tree.configure(yscrollcommand=scrollbar.set)

            columns_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

            # 索引信息（如果可用）
            if 'indexes' in table_info and table_info['indexes']:
                indexes_frame = ttk.LabelFrame(main_frame, text="索引信息", padding="10")
                indexes_frame.pack(fill=tk.X, pady=(0, 10))

                indexes_tree = ttk.Treeview(indexes_frame, columns=("name", "columns", "unique"), show="headings")
                indexes_tree.heading("name", text="索引名")
                indexes_tree.heading("columns", text="列")
                indexes_tree.heading("unique", text="唯一")

                indexes_tree.column("name", width=120)
                indexes_tree.column("columns", width=200)
                indexes_tree.column("unique", width=50)

                for index_info in table_info['indexes']:
                    index_name = index_info.get('name', '')
                    index_columns = ', '.join(index_info.get('columns', []))
                    index_unique = "YES" if index_info.get('unique', False) else "NO"
                    indexes_tree.insert("", tk.END, values=(index_name, index_columns, index_unique))

                indexes_tree.pack(fill=tk.X)

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
            ).pack(side=tk.LEFT, padx=(0, 5))

            # 生成插入语句按钮
            ttk.Button(
                button_frame,
                text="➕ 生成INSERT语句",
                command=lambda: self._generate_insert_query(table_name)
            ).pack(side=tk.LEFT, padx=(0, 5))

            ttk.Button(button_frame, text="关闭", command=dialog.destroy).pack(side=tk.RIGHT)

        except Exception as e:
            messagebox.showerror("错误", f"获取表信息失败: {str(e)}")

    def _view_selected_table_structure(self):
        """查看选中表的结构"""
        selection = self.tables_listbox.curselection()
        if selection:
            table_name = self._get_selected_table_name(selection[0])
            if table_name:
                self._show_table_details(None)

    def _view_selected_table_data(self):
        """查看选中表的数据"""
        selection = self.tables_listbox.curselection()
        if selection:
            table_name = self._get_selected_table_name(selection[0])
            if table_name:
                self._view_table_data(table_name)

    def _view_table_data(self, table_name):
        """查看表数据"""
        try:
            # 生成SELECT查询
            query = f"SELECT * FROM {table_name} LIMIT 100;"

            # 这里需要通过回调或事件机制将查询传递给SQL查询标签页
            self._on_table_data_requested(table_name, query)

        except Exception as e:
            messagebox.showerror("错误", f"查看表数据失败: {str(e)}")

    def _generate_select_query(self, table_name):
        """生成SELECT查询"""
        query = f"SELECT * FROM {table_name} WHERE condition;"
        self._on_query_generated(query, "SELECT")

    def _generate_insert_query(self, table_name):
        """生成INSERT语句"""
        query = f"INSERT INTO {table_name} (column1, column2) VALUES (value1, value2);"
        self._on_query_generated(query, "INSERT")

    def _get_selected_table_name(self, index):
        """获取选中表的真实名称"""
        if index < self.tables_listbox.size():
            display_text = self.tables_listbox.get(index)
            # 从显示文本中提取表名（去掉行数信息）
            table_name = display_text.split(' (')[0]
            return table_name
        return None

    def _log(self, message):
        """记录日志"""
        # 这里可以通过事件机制将日志传递给主日志系统
        print(f"[DatabaseInfo] {message}")

    def _on_table_data_requested(self, table_name, query):
        """当请求表数据时的回调"""
        # 这里应该通过事件或回调机制通知SQL查询标签页
        print(f"Table data requested: {table_name}, Query: {query}")

    def _on_query_generated(self, query, query_type):
        """当查询生成时的回调"""
        # 这里应该通过事件或回调机制通知SQL查询标签页
        print(f"Query generated ({query_type}): {query}")

    def update_status(self, message, color="green"):
        """更新状态信息"""
        self.status_label.configure(text=f"状态: {message}", foreground=color)

    def clear_tables(self):
        """清空表列表"""
        self.tables_listbox.delete(0, tk.END)
        self.table_count_label.configure(text="表数量: 0")
        self.total_rows_label.configure(text="总行数: 0")
        self.tables_dict = {}
        self.view_structure_btn.configure(state=tk.DISABLED)
        self.view_data_btn.configure(state=tk.DISABLED)

    def get_table_names(self):
        """获取所有表名"""
        return list(self.tables_dict.keys())

    def get_table_info(self, table_name):
        """获取指定表的信息"""
        return self.tables_dict.get(table_name, {})