import tkinter as tk
import re


class SyntaxHighlighter:
    def __init__(self, text_widget):
        self.text_widget = text_widget
        self._setup_tags()
        self._setup_bindings()

    def _setup_tags(self):
        """设置语法高亮标签"""
        # SQL关键字
        self.text_widget.tag_configure("keyword", foreground="blue", font=("Consolas", 11, "bold"))
        # 字符串
        self.text_widget.tag_configure("string", foreground="green")
        # 数字
        self.text_widget.tag_configure("number", foreground="red")
        # 注释
        self.text_widget.tag_configure("comment", foreground="gray")
        # 函数
        self.text_widget.tag_configure("function", foreground="purple")
        # 操作符
        self.text_widget.tag_configure("operator", foreground="orange")

        # SQL关键字列表
        self.sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
            'DELETE', 'CREATE', 'TABLE', 'ALTER', 'DROP', 'INDEX', 'VIEW', 'TRIGGER',
            'PROCEDURE', 'FUNCTION', 'CASCADE', 'RESTRICT', 'UNIQUE', 'PRIMARY', 'KEY',
            'FOREIGN', 'REFERENCES', 'CHECK', 'DEFAULT', 'NOT', 'NULL', 'AND', 'OR',
            'LIKE', 'IN', 'BETWEEN', 'IS', 'EXISTS', 'ALL', 'ANY', 'SOME', 'DISTINCT',
            'ORDER', 'BY', 'GROUP', 'HAVING', 'ASC', 'DESC', 'LIMIT', 'OFFSET', 'JOIN',
            'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'ON', 'AS', 'CASE', 'WHEN',
            'THEN', 'ELSE', 'END', 'UNION', 'INTERSECT', 'EXCEPT', 'CAST', 'CONVERT',
            'COMMIT', 'ROLLBACK', 'BEGIN', 'TRANSACTION', 'SAVEPOINT', 'GRANT', 'REVOKE'
        ]

        # SQL函数列表
        self.sql_functions = [
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'UPPER', 'LOWER', 'LENGTH', 'SUBSTR',
            'TRIM', 'LTRIM', 'RTRIM', 'REPLACE', 'COALESCE', 'NULLIF', 'NOW', 'CURRENT_DATE',
            'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'DATE', 'TIME', 'DATETIME', 'STRFTIME',
            'ABS', 'ROUND', 'CEIL', 'FLOOR', 'RANDOM', 'RAND', 'PI', 'POWER', 'SQRT',
            'EXP', 'LOG', 'LN', 'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN'
        ]

    def _setup_bindings(self):
        """设置事件绑定"""
        self.text_widget.bind('<KeyRelease>', self._on_key_release)
        self.text_widget.bind('<<Modified>>', self._on_text_modified)

    def _on_key_release(self, event):
        """键盘释放事件处理"""
        # 延迟执行语法高亮，避免性能问题
        if event.char and event.char.strip():
            self.text_widget.after_idle(self.highlight_syntax)

    def _on_text_modified(self, event):
        """文本修改事件处理"""
        if self.text_widget.edit_modified():
            self.text_widget.after_idle(self.highlight_syntax)
        return False

    def highlight_syntax(self):
        """执行语法高亮"""
        # 清除所有标签
        for tag in ["keyword", "string", "number", "comment", "function", "operator"]:
            self.text_widget.tag_remove(tag, "1.0", tk.END)

        # 获取文本内容
        content = self.text_widget.get("1.0", tk.END)

        # 高亮注释
        self._highlight_pattern(r'--.*?$', "comment")
        self._highlight_pattern(r'/\*.*?\*/', "comment", re.DOTALL)

        # 高亮字符串
        self._highlight_pattern(r"'.*?'", "string")
        self._highlight_pattern(r'".*?"', "string")

        # 高亮数字
        self._highlight_pattern(r'\b\d+\b', "number")
        self._highlight_pattern(r'\b\d+\.\d+\b', "number")

        # 高亮关键字
        for keyword in self.sql_keywords:
            self._highlight_pattern(r'\b' + re.escape(keyword) + r'\b', "keyword", re.IGNORECASE)

        # 高亮函数
        for func in self.sql_functions:
            self._highlight_pattern(r'\b' + re.escape(func) + r'\b', "function", re.IGNORECASE)

        # 高亮操作符
        operators = ['=', '<', '>', '<=', '>=', '<>', '!=', '\+', '-', '\*', '/', '%', '\|\|']
        for op in operators:
            self._highlight_pattern(op, "operator")

    def _highlight_pattern(self, pattern, tag, flags=0):
        """高亮匹配的模式"""
        start = "1.0"
        while True:
            pos = self.text_widget.search(pattern, start, tk.END, regexp=True, flags=flags)
            if not pos:
                break
            end = f"{pos}+{len(self.text_widget.get(pos, f'{pos} lineend'))}c"
            self.text_widget.tag_add(tag, pos, end)
            start = end

    def highlight_line(self, event=None):
        """高亮当前行"""
        # 获取当前光标位置
        cursor_pos = self.text_widget.index(tk.INSERT)
        line_no = cursor_pos.split('.')[0]

        # 清除之前的行高亮
        self.text_widget.tag_remove("current_line", "1.0", tk.END)

        # 高亮当前行
        line_start = f"{line_no}.0"
        line_end = f"{line_no}.end"
        self.text_widget.tag_add("current_line", line_start, line_end)

        # 确保当前行可见
        self.text_widget.see(cursor_pos)

    def setup_line_highlight(self):
        """设置行高亮"""
        # 创建当前行高亮标签
        self.text_widget.tag_configure("current_line", background="#f0f8ff")

        # 绑定事件
        self.text_widget.bind('<KeyRelease>', self.highlight_line)
        self.text_widget.bind('<ButtonRelease-1>', self.highlight_line)

        # 初始高亮
        self.highlight_line()