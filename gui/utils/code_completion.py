import tkinter as tk
from tkinter import ttk


class CodeCompletion:
    def __init__(self, text_widget, completion_engine):
        self.text_widget = text_widget
        self.completion_engine = completion_engine
        self.completion_window = None
        self.completion_listbox = None
        self.current_suggestions = []

        self._setup_bindings()

    def _setup_bindings(self):
        """设置事件绑定"""
        self.text_widget.bind('<KeyRelease>', self._on_key_release)
        self.text_widget.bind('<Control-space>', self._trigger_completion)
        self.text_widget.bind('<Tab>', self._handle_tab_completion)
        self.text_widget.bind('<Escape>', self._hide_completion)

    def _on_key_release(self, event):
        """键盘释放事件处理"""
        if not self.completion_engine:
            return

        # 忽略特殊键
        if event.keysym in ['Up', 'Down', 'Left', 'Right', 'Return', 'Tab', 'Escape']:
            self._hide_completion()
            return

        # 自动触发补全
        if event.char and (event.char.isalnum() or event.char in '._'):
            self.text_widget.after(500, self._auto_complete)

    def _trigger_completion(self, event):
        """手动触发补全"""
        self._show_completion()
        return 'break'

    def _auto_complete(self):
        """自动补全"""
        try:
            cursor_pos = self.text_widget.index(tk.INSERT)
            sql_content = self.text_widget.get('1.0', tk.END)

            # 将Tkinter位置转换为字符位置
            line_no, col_no = map(int, cursor_pos.split('.'))
            lines = sql_content.split('\n')
            char_pos = sum(len(lines[i]) + 1 for i in range(line_no - 1)) + col_no

            suggestions = self.completion_engine.get_completions(sql_content, char_pos)

            if suggestions and len(suggestions) > 0:
                self._show_completion_popup(suggestions)
            else:
                self._hide_completion()

        except Exception as e:
            print(f"自动补全错误: {e}")

    def _show_completion_popup(self, suggestions):
        """显示补全弹窗"""
        self._hide_completion()

        if not suggestions:
            return

        # 创建弹窗
        self.completion_window = tk.Toplevel(self.text_widget)
        self.completion_window.wm_overrideredirect(True)
        self.completion_window.configure(bg='white', relief='solid', bd=1)

        # 创建列表框
        self.completion_listbox = tk.Listbox(
            self.completion_window,
            height=min(8, len(suggestions)),
            font=("Consolas", 9),
            selectmode=tk.SINGLE
        )
        self.completion_listbox.pack()

        # 填充建议
        self.current_suggestions = suggestions[:8]  # 限制数量
        for i, suggestion in enumerate(self.current_suggestions):
            text = suggestion['text']
            desc = suggestion.get('description', '')
            display_text = f"{text} - {desc}" if desc else text
            self.completion_listbox.insert(tk.END, display_text)

        # 选中第一项
        if self.current_suggestions:
            self.completion_listbox.selection_set(0)

        # 绑定事件
        self.completion_listbox.bind('<Double-Button-1>', self._apply_completion)
        self.completion_listbox.bind('<Return>', self._apply_completion)
        self.completion_window.bind('<Escape>', lambda e: self._hide_completion())

        # 定位弹窗
        cursor_pos = self.text_widget.index(tk.INSERT)
        bbox = self.text_widget.bbox(cursor_pos)
        if bbox:
            x, y, _, _ = bbox
            x += self.text_widget.winfo_rootx()
            y += self.text_widget.winfo_rooty() + 20
            self.completion_window.geometry(f"+{x}+{y}")

        # 自动隐藏计时器
        self.text_widget.after(10000, self._hide_completion)

    def _apply_completion(self, event=None):
        """应用补全建议"""
        if not self.completion_listbox or not self.current_suggestions:
            return

        try:
            selection = self.completion_listbox.curselection()
            if selection:
                suggestion = self.current_suggestions[selection[0]]
                completion_text = suggestion['text']

                # 获取当前光标位置
                cursor_pos = self.text_widget.index(tk.INSERT)

                # 删除部分输入的词
                line_no, col_no = map(int, cursor_pos.split('.'))
                line_start = f"{line_no}.0"
                line_text = self.text_widget.get(line_start, f"{line_no}.end")

                # 找到当前词的开始位置
                word_start = col_no
                while word_start > 0 and (line_text[word_start - 1].isalnum() or line_text[word_start - 1] in '._'):
                    word_start -= 1

                # 删除部分词并插入补全
                delete_start = f"{line_no}.{word_start}"
                self.text_widget.delete(delete_start, cursor_pos)
                self.text_widget.insert(delete_start, completion_text)

        except Exception as e:
            print(f"应用补全错误: {e}")
        finally:
            self._hide_completion()

    def _hide_completion(self, event=None):
        """隐藏补全弹窗"""
        if self.completion_window:
            self.completion_window.destroy()
            self.completion_window = None
            self.completion_listbox = None
            self.current_suggestions = []
        return 'break' if event else None

    def _handle_tab_completion(self, event):
        """处理Tab键补全"""
        if self.completion_window and self.completion_listbox:
            # 如果补全窗口打开，Tab键选择下一个建议
            current = self.completion_listbox.curselection()
            if current:
                next_index = (current[0] + 1) % self.completion_listbox.size()
            else:
                next_index = 0

            self.completion_listbox.selection_clear(0, tk.END)
            self.completion_listbox.selection_set(next_index)
            return 'break'

        # 如果没有补全窗口，插入制表符
        self.text_widget.insert(tk.INSERT, '    ')
        return 'break'

    def _show_completion(self):
        """显示补全"""
        self._auto_complete()