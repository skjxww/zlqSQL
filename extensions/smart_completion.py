import re
import tkinter as tk
from typing import List, Dict, Tuple, Optional
from sql_compiler.lexer.token import TokenType
from catalog.catalog_manager import CatalogManager
from sql_compiler.lexer.lexical_analyzer import LexicalAnalyzer


class SmartSQLCompletion:
    """智能SQL代码补全"""

    def __init__(self, catalog: CatalogManager):
        self.catalog = catalog
        self.keywords = self._load_sql_keywords()
        self.functions = self._load_sql_functions()

    def get_completions(self, sql: str, cursor_pos: int) -> List[Dict]:
        """获取智能补全建议"""
        try:
            # 分析上下文
            context = self._analyze_context(sql, cursor_pos)
            suggestions = []

            # 根据上下文类型生成建议
            if context['type'] == 'after_select':
                suggestions.extend(self._get_column_completions(context))
                suggestions.extend(self._get_function_completions())

            elif context['type'] == 'after_from':
                suggestions.extend(self._get_table_completions())
                suggestions.extend(self._get_view_completions())

            elif context['type'] == 'after_where':
                suggestions.extend(self._get_column_completions(context))
                suggestions.extend(self._get_operator_completions())

            elif context['type'] == 'after_join':
                suggestions.extend(self._get_table_completions())
                suggestions.extend(self._get_join_conditions(context))

            elif context['type'] == 'keyword_context':
                suggestions.extend(self._get_keyword_completions(context))

            # 智能排序和过滤
            return self._rank_suggestions(suggestions, context)

        except Exception as e:
            return [{'text': 'SELECT', 'type': 'keyword', 'description': 'Basic SELECT statement'}]

    def _analyze_context(self, sql: str, cursor_pos: int) -> Dict:
        """智能分析SQL上下文"""
        before_cursor = sql[:cursor_pos].strip()
        after_cursor = sql[cursor_pos:].strip()

        # 使用你现有的词法分析器
        try:
            lexer = LexicalAnalyzer(before_cursor)
            tokens = lexer.tokenize()

            context = {
                'tokens': tokens,
                'partial_word': self._get_partial_word(sql, cursor_pos),
                'tables': self._extract_tables_from_tokens(tokens),
                'last_keyword': self._get_last_keyword_from_tokens(tokens),
                'type': 'unknown'
            }

            # 基于token序列确定上下文类型
            context['type'] = self._determine_context_type(tokens, cursor_pos)

            return context

        except Exception:
            # 回退到简单的字符串分析
            return self._fallback_context_analysis(before_cursor, cursor_pos)

    def _determine_context_type(self, tokens: List, cursor_pos: int) -> str:
        """基于token序列确定上下文类型"""
        if not tokens:
            return 'keyword_context'

        token_types = [token.type for token in tokens]

        # 检查SELECT后的上下文
        if TokenType.SELECT in token_types:
            select_idx = len(token_types) - 1 - token_types[::-1].index(TokenType.SELECT)
            remaining_tokens = token_types[select_idx + 1:]

            if TokenType.FROM not in remaining_tokens:
                return 'after_select'

        # 检查FROM后的上下文
        if TokenType.FROM in token_types:
            from_idx = len(token_types) - 1 - token_types[::-1].index(TokenType.FROM)
            remaining_tokens = token_types[from_idx + 1:]

            if not remaining_tokens or remaining_tokens[-1] == TokenType.FROM:
                return 'after_from'

        # 检查WHERE后的上下文
        if TokenType.WHERE in token_types:
            return 'after_where'

        # 检查JOIN上下文
        join_tokens = [TokenType.JOIN, TokenType.LEFT, TokenType.RIGHT, TokenType.INNER]
        if any(jt in token_types for jt in join_tokens):
            return 'after_join'

        return 'keyword_context'

    def _get_column_completions(self, context: Dict) -> List[Dict]:
        """获取列名补全建议"""
        suggestions = []

        for table_info in context.get('tables', []):
            table_name = table_info['name']
            alias = table_info.get('alias')

            if self.catalog.table_exists(table_name):
                columns = self.catalog.get_table_columns(table_name)
                for col in columns:
                    # 添加带表前缀的列名
                    prefix = alias or table_name
                    suggestions.append({
                        'text': f"{prefix}.{col}",
                        'type': 'column',
                        'description': f'Column from {table_name}',
                        'detail': self._get_column_detail(table_name, col)
                    })

                    # 添加简单列名
                    suggestions.append({
                        'text': col,
                        'type': 'column',
                        'description': f'Column from {table_name}',
                        'detail': self._get_column_detail(table_name, col)
                    })

        return suggestions

    def _get_join_conditions(self, context: Dict) -> List[Dict]:
        """智能生成JOIN条件建议"""
        suggestions = []
        tables = context.get('tables', [])

        if len(tables) >= 2:
            # 分析可能的连接条件
            for i, table1 in enumerate(tables):
                for j, table2 in enumerate(tables[i + 1:], i + 1):
                    join_suggestions = self._suggest_join_conditions(table1, table2)
                    suggestions.extend(join_suggestions)

        return suggestions

    def _suggest_join_conditions(self, table1: Dict, table2: Dict) -> List[Dict]:
        """基于列名相似性建议JOIN条件"""
        suggestions = []

        if not (self.catalog.table_exists(table1['name']) and
                self.catalog.table_exists(table2['name'])):
            return suggestions

        cols1 = self.catalog.get_table_columns(table1['name'])
        cols2 = self.catalog.get_table_columns(table2['name'])

        # 查找可能的连接列
        for col1 in cols1:
            for col2 in cols2:
                if self._is_likely_join_column(col1, col2, table1['name'], table2['name']):
                    t1_ref = table1.get('alias', table1['name'])
                    t2_ref = table2.get('alias', table2['name'])

                    suggestions.append({
                        'text': f"ON {t1_ref}.{col1} = {t2_ref}.{col2}",
                        'type': 'join_condition',
                        'description': f'Suggested join condition',
                        'confidence': self._calculate_join_confidence(col1, col2, table1['name'], table2['name'])
                    })

        return suggestions

    def _is_likely_join_column(self, col1: str, col2: str, table1: str, table2: str) -> bool:
        """判断两列是否可能用于JOIN"""
        # 完全匹配
        if col1 == col2:
            return True

        # ID列匹配模式
        if (col1.endswith('_id') and col2 == col1[:-3]) or \
                (col2.endswith('_id') and col1 == col2[:-3]):
            return True

        # 表名匹配模式
        if (col1 == f"{table2.lower()}_id" and col2 == "id") or \
                (col2 == f"{table1.lower()}_id" and col1 == "id"):
            return True

        return False

    def _calculate_join_confidence(self, col1: str, col2: str, table1: str, table2: str) -> float:
        """计算JOIN条件的置信度"""
        if col1 == col2:
            return 0.9
        if col1.endswith('_id') or col2.endswith('_id'):
            return 0.8
        return 0.6

    def _load_sql_keywords(self) -> List[str]:
        """加载SQL关键字"""
        return [
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY',
            'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
            'CREATE', 'TABLE', 'VIEW', 'INDEX', 'DROP', 'ALTER',
            'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'OUTER JOIN',
            'UNION', 'UNION ALL', 'DISTINCT', 'AS', 'AND', 'OR', 'NOT',
            'IN', 'BETWEEN', 'LIKE', 'IS NULL', 'IS NOT NULL',
            'LIMIT', 'OFFSET', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        ]

    def _load_sql_functions(self) -> List[Dict]:
        """加载SQL函数"""
        return [
            {'name': 'COUNT(*)', 'type': 'aggregate', 'desc': '统计行数'},
            {'name': 'COUNT(column)', 'type': 'aggregate', 'desc': '统计非空值数量'},
            {'name': 'SUM(column)', 'type': 'aggregate', 'desc': '求和'},
            {'name': 'AVG(column)', 'type': 'aggregate', 'desc': '平均值'},
            {'name': 'MAX(column)', 'type': 'aggregate', 'desc': '最大值'},
            {'name': 'MIN(column)', 'type': 'aggregate', 'desc': '最小值'},
            {'name': 'UPPER(column)', 'type': 'string', 'desc': '转大写'},
            {'name': 'LOWER(column)', 'type': 'string', 'desc': '转小写'},
            {'name': 'LENGTH(column)', 'type': 'string', 'desc': '字符串长度'},
            {'name': 'NOW()', 'type': 'datetime', 'desc': '当前时间'},
            {'name': 'DATE(column)', 'type': 'datetime', 'desc': '提取日期部分'},
        ]


# 集成到GUI
class CompletionUI:
    """补全功能的UI集成"""

    def __init__(self, sql_text_widget, completion_engine):
        self.text_widget = sql_text_widget
        self.completion_engine = completion_engine
        self.completion_window = None
        self.suggestions = []

        # 绑定事件
        self.text_widget.bind('<KeyRelease>', self._on_key_release)
        self.text_widget.bind('<Control-space>', self._trigger_completion)

    def _on_key_release(self, event):
        """键盘释放事件处理"""
        if event.keysym in ['Up', 'Down', 'Left', 'Right', 'Return', 'Tab']:
            self._hide_completion()
            return

        # 自动触发补全
        if event.char and event.char.isalnum():
            self.text_widget.after(300, self._auto_complete)

    def _trigger_completion(self, event):
        """手动触发补全"""
        self._show_completion()
        return 'break'

    def _auto_complete(self):
        """自动补全逻辑"""
        cursor_pos = self.text_widget.index(tk.INSERT)
        sql_content = self.text_widget.get('1.0', tk.END)

        # 转换cursor位置为字符偏移
        char_pos = len(
            sql_content[:sql_content.find('\n') * int(cursor_pos.split('.')[0]) + int(cursor_pos.split('.')[1])])

        suggestions = self.completion_engine.get_completions(sql_content, char_pos)

        if suggestions and len(suggestions) > 0:
            self._show_completion_popup(suggestions)