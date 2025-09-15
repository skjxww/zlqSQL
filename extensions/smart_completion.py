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
        print(f"获取补全建议: SQL长度={len(sql)}, 光标位置={cursor_pos}")  # 调试输出

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

    def _get_partial_word(self, sql: str, cursor_pos: int) -> str:
        """获取光标处的部分单词"""
        if cursor_pos <= 0:
            return ""

        start = cursor_pos
        while start > 0 and sql[start - 1].isalnum() or sql[start - 1] == '_':
            start -= 1

        return sql[start:cursor_pos]

    def _extract_tables_from_tokens(self, tokens) -> List[Dict]:
        """从tokens中提取表信息"""
        tables = []
        # 简化实现，实际应该更复杂
        for i, token in enumerate(tokens):
            if hasattr(token, 'type') and token.type == TokenType.FROM:
                if i + 1 < len(tokens):
                    next_token = tokens[i + 1]
                    if hasattr(next_token, 'value'):
                        tables.append({'name': next_token.value, 'alias': None})
        return tables

    def _get_last_keyword_from_tokens(self, tokens) -> Optional[str]:
        """从tokens中获取最后一个关键字"""
        if not tokens:
            return None

        # 从后往前查找关键字
        for token in reversed(tokens):
            if hasattr(token, 'type') and hasattr(token, 'value'):
                if token.type in [TokenType.SELECT, TokenType.FROM, TokenType.WHERE,
                                  TokenType.GROUP, TokenType.ORDER, TokenType.JOIN]:
                    return token.value.upper()
        return None

    def _fallback_context_analysis(self, before_cursor: str, cursor_pos: int) -> Dict:
        """回退的上下文分析（基于字符串）"""
        before_cursor_upper = before_cursor.upper()

        context = {
            'tokens': [],
            'partial_word': self._get_partial_word(before_cursor, len(before_cursor)),
            'tables': [],
            'last_keyword': None,
            'type': 'keyword_context'
        }

        # 简单的关键字检测
        if 'SELECT' in before_cursor_upper:
            if 'FROM' not in before_cursor_upper:
                context['type'] = 'after_select'
            elif 'WHERE' in before_cursor_upper:
                context['type'] = 'after_where'
            elif 'JOIN' in before_cursor_upper:
                context['type'] = 'after_join'
        elif 'FROM' in before_cursor_upper and before_cursor_upper.strip().endswith('FROM'):
            context['type'] = 'after_from'

        # 提取表名（简单实现）
        import re
        from_match = re.search(r'FROM\s+(\w+)', before_cursor_upper)
        if from_match:
            context['tables'] = [{'name': from_match.group(1), 'alias': None}]

        return context

    def _get_view_completions(self) -> List[Dict]:
        """获取视图补全建议"""
        suggestions = []
        try:
            # 如果catalog支持视图，获取视图列表
            if hasattr(self.catalog, 'get_all_view_names'):
                view_names = self.catalog.get_all_view_names()
                for view_name in view_names:
                    suggestions.append({
                        'text': view_name,
                        'type': 'view',
                        'description': f'视图: {view_name}'
                    })
        except:
            # 如果不支持视图，返回空列表
            pass
        return suggestions

    def _get_operator_completions(self) -> List[Dict]:
        """获取操作符补全建议"""
        operators = [
            {'text': '=', 'desc': '等于'},
            {'text': '!=', 'desc': '不等于'},
            {'text': '<>', 'desc': '不等于'},
            {'text': '>', 'desc': '大于'},
            {'text': '<', 'desc': '小于'},
            {'text': '>=', 'desc': '大于等于'},
            {'text': '<=', 'desc': '小于等于'},
            {'text': 'LIKE', 'desc': '模糊匹配'},
            {'text': 'IN', 'desc': '在列表中'},
            {'text': 'NOT IN', 'desc': '不在列表中'},
            {'text': 'IS NULL', 'desc': '是空值'},
            {'text': 'IS NOT NULL', 'desc': '不是空值'},
            {'text': 'BETWEEN', 'desc': '在范围内'},
            {'text': 'AND', 'desc': '逻辑与'},
            {'text': 'OR', 'desc': '逻辑或'},
        ]

        suggestions = []
        for op in operators:
            suggestions.append({
                'text': op['text'],
                'type': 'operator',
                'description': op['desc']
            })
        return suggestions

    def _get_column_detail(self, table_name: str, col: str) -> str:
        """获取列的详细信息"""
        try:
            # 如果catalog支持获取列详情
            if hasattr(self.catalog, 'get_column_info'):
                column_info = self.catalog.get_column_info(table_name, col)
                return f"类型: {column_info.get('type', 'unknown')}"
            else:
                return f"来自表 {table_name}"
        except:
            return f"来自表 {table_name}"

    def _load_sql_keywords(self) -> List[str]:
        """加载SQL关键字"""
        return [
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY',
            'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
            'CREATE', 'TABLE', 'VIEW', 'INDEX', 'DROP', 'ALTER',
            'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'OUTER JOIN',
            'UNION', 'UNION ALL', 'DISTINCT', 'AS', 'AND', 'OR', 'NOT',
            'IN', 'BETWEEN', 'LIKE', 'IS NULL', 'IS NOT NULL',
            'LIMIT', 'OFFSET', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK', 'DEFAULT',
            'AUTO_INCREMENT', 'NOT NULL', 'NULL'
        ]

    def _load_sql_functions(self) -> List[Dict]:
        """加载SQL函数"""
        return [
            # 聚合函数
            {'name': 'COUNT(*)', 'type': 'aggregate', 'desc': '统计行数'},
            {'name': 'COUNT(column)', 'type': 'aggregate', 'desc': '统计非空值数量'},
            {'name': 'SUM(column)', 'type': 'aggregate', 'desc': '求和'},
            {'name': 'AVG(column)', 'type': 'aggregate', 'desc': '平均值'},
            {'name': 'MAX(column)', 'type': 'aggregate', 'desc': '最大值'},
            {'name': 'MIN(column)', 'type': 'aggregate', 'desc': '最小值'},

            # 字符串函数
            {'name': 'UPPER(column)', 'type': 'string', 'desc': '转大写'},
            {'name': 'LOWER(column)', 'type': 'string', 'desc': '转小写'},
            {'name': 'LENGTH(column)', 'type': 'string', 'desc': '字符串长度'},
            {'name': 'TRIM(column)', 'type': 'string', 'desc': '去除空格'},
            {'name': 'SUBSTRING(column, start, length)', 'type': 'string', 'desc': '截取子字符串'},
            {'name': 'CONCAT(column1, column2)', 'type': 'string', 'desc': '连接字符串'},

            # 日期时间函数
            {'name': 'NOW()', 'type': 'datetime', 'desc': '当前时间'},
            {'name': 'DATE(column)', 'type': 'datetime', 'desc': '提取日期部分'},
            {'name': 'TIME(column)', 'type': 'datetime', 'desc': '提取时间部分'},
            {'name': 'YEAR(column)', 'type': 'datetime', 'desc': '提取年份'},
            {'name': 'MONTH(column)', 'type': 'datetime', 'desc': '提取月份'},
            {'name': 'DAY(column)', 'type': 'datetime', 'desc': '提取日期'},

            # 数学函数
            {'name': 'ABS(column)', 'type': 'math', 'desc': '绝对值'},
            {'name': 'ROUND(column, decimals)', 'type': 'math', 'desc': '四舍五入'},
            {'name': 'CEIL(column)', 'type': 'math', 'desc': '向上取整'},
            {'name': 'FLOOR(column)', 'type': 'math', 'desc': '向下取整'},

            # 条件函数
            {'name': 'IF(condition, value1, value2)', 'type': 'conditional', 'desc': '条件判断'},
            {'name': 'CASE WHEN condition THEN value END', 'type': 'conditional', 'desc': 'CASE表达式'},
            {'name': 'COALESCE(value1, value2)', 'type': 'conditional', 'desc': '返回第一个非空值'},
        ]

    def _suggest_join_conditions(self, table1: Dict, table2: Dict) -> List[Dict]:
        """基于列名相似性建议JOIN条件"""
        suggestions = []

        if not (self.catalog.table_exists(table1['name']) and
                self.catalog.table_exists(table2['name'])):
            return suggestions

        try:
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
                            'description': f'建议的连接条件',
                            'confidence': self._calculate_join_confidence(col1, col2, table1['name'], table2['name'])
                        })
        except Exception as e:
            print(f"生成JOIN条件建议时出错: {e}")

        return suggestions

    def _is_likely_join_column(self, col1: str, col2: str, table1: str, table2: str) -> bool:
        """判断两列是否可能用于JOIN"""
        col1_lower = col1.lower()
        col2_lower = col2.lower()
        table1_lower = table1.lower()
        table2_lower = table2.lower()

        # 完全匹配
        if col1_lower == col2_lower:
            return True

        # ID列匹配模式
        if (col1_lower.endswith('_id') and col2_lower == col1_lower[:-3]) or \
                (col2_lower.endswith('_id') and col1_lower == col2_lower[:-3]):
            return True

        # 表名匹配模式
        if (col1_lower == f"{table2_lower}_id" and col2_lower == "id") or \
                (col2_lower == f"{table1_lower}_id" and col1_lower == "id"):
            return True

        # 常见的外键模式
        if col1_lower == "id" and col2_lower == f"{table1_lower}_id":
            return True
        if col2_lower == "id" and col1_lower == f"{table2_lower}_id":
            return True

        return False

    def _calculate_join_confidence(self, col1: str, col2: str, table1: str, table2: str) -> float:
        """计算JOIN条件的置信度"""
        col1_lower = col1.lower()
        col2_lower = col2.lower()

        # 完全匹配
        if col1_lower == col2_lower:
            if col1_lower == "id":
                return 0.95
            return 0.9

        # ID相关匹配
        if col1_lower.endswith('_id') or col2_lower.endswith('_id'):
            return 0.8

        # 表名相关匹配
        if table1.lower() in col2_lower or table2.lower() in col1_lower:
            return 0.75

        return 0.6

    def _rank_suggestions(self, suggestions: List[Dict], context: Dict) -> List[Dict]:
        """智能排序和过滤建议"""
        if not suggestions:
            return []

        partial = context.get('partial_word', '').lower()

        def calculate_score(suggestion):
            text = suggestion['text'].lower()
            score = 0

            # 前缀匹配得分最高
            if text.startswith(partial):
                score += 100
                # 越短的匹配得分越高
                score += (50 - len(text))

            # 包含匹配
            elif partial in text:
                score += 50
                score += (25 - len(text))

            # 类型权重
            type_weights = {
                'keyword': 10,
                'table': 15,
                'column': 20,
                'function': 12,
                'operator': 8,
                'view': 14,
                'join_condition': 25
            }
            score += type_weights.get(suggestion.get('type', ''), 0)

            # 置信度权重（如果有的话）
            if 'confidence' in suggestion:
                score += suggestion['confidence'] * 10

            return score

        # 排序
        suggestions.sort(key=calculate_score, reverse=True)

        # 过滤和限制数量
        filtered_suggestions = []
        seen_texts = set()

        for suggestion in suggestions:
            text = suggestion['text']
            # 避免重复
            if text not in seen_texts:
                seen_texts.add(text)
                filtered_suggestions.append(suggestion)

            # 限制数量
            if len(filtered_suggestions) >= 15:
                break

        return filtered_suggestions

    def _show_completion_popup(self, suggestions):
        """显示补全弹窗"""
        print(f"显示补全建议: {len(suggestions)} 个建议")
        for i, suggestion in enumerate(suggestions[:5]):  # 只打印前5个
            print(f"  {i + 1}. {suggestion['text']} ({suggestion['type']})")

        # 实际的UI显示逻辑会在CompletionUI类中实现
        if hasattr(self, '_create_completion_window'):
            self._create_completion_window(suggestions)

    def _get_keyword_completions(self, context: Dict) -> List[Dict]:
        """获取关键字补全"""
        suggestions = []
        partial = context.get('partial_word', '').upper()

        for keyword in self.keywords:
            if keyword.startswith(partial):
                suggestions.append({
                    'text': keyword,
                    'type': 'keyword',
                    'description': f'SQL关键字: {keyword}'
                })
        return suggestions

    def _get_function_completions(self) -> List[Dict]:
        """获取函数补全"""
        suggestions = []
        for func in self.functions:
            suggestions.append({
                'text': func['name'],
                'type': 'function',
                'description': func['desc']
            })
        return suggestions

    def _get_table_completions(self) -> List[Dict]:
        """获取表名补全"""
        suggestions = []
        try:
            table_names = self.catalog.get_all_table_names()
            for table_name in table_names:
                suggestions.append({
                    'text': table_name,
                    'type': 'table',
                    'description': f'表: {table_name}'
                })
        except:
            pass
        return suggestions

    def _rank_suggestions(self, suggestions: List[Dict], context: Dict) -> List[Dict]:
        """对建议进行排序"""
        partial = context.get('partial_word', '').lower()

        def score_suggestion(suggestion):
            text = suggestion['text'].lower()
            if text.startswith(partial):
                return 100 - len(text)  # 优先短的匹配
            elif partial in text:
                return 50 - len(text)
            return 0

        suggestions.sort(key=score_suggestion, reverse=True)
        return suggestions[:10]  # 限制数量

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
    def __init__(self, sql_text_widget, completion_engine):
        self.text_widget = sql_text_widget
        self.completion_engine = completion_engine
        self.completion_window = None
        self.suggestions = []
        self.selected_index = 0

        # 绑定事件
        self.text_widget.bind('<KeyRelease>', self._on_key_release)
        self.text_widget.bind('<Control-space>', self._trigger_completion)

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

    def _show_completion(self):
        """显示补全窗口"""
        cursor_pos = self.text_widget.index(tk.INSERT)
        sql_content = self.text_widget.get('1.0', tk.END)

        # 计算字符位置
        lines = sql_content.split('\n')
        line_num = int(cursor_pos.split('.')[0]) - 1
        col_num = int(cursor_pos.split('.')[1])
        char_pos = sum(len(line) + 1 for line in lines[:line_num]) + col_num

        suggestions = self.completion_engine.get_completions(sql_content, char_pos)

        if suggestions:
            self._create_completion_window(suggestions)

    def _hide_completion(self):
        """隐藏补全窗口"""
        if self.completion_window:
            self.completion_window.destroy()
            self.completion_window = None

    def _calculate_char_position(self, cursor_pos, sql_content):
        """修复字符位置计算"""
        lines = sql_content.split('\n')
        line_num = int(cursor_pos.split('.')[0]) - 1
        col_num = int(cursor_pos.split('.')[1])

        char_pos = 0
        for i in range(line_num):
            char_pos += len(lines[i]) + 1  # +1 for newline
        char_pos += col_num

        return char_pos

    def _create_completion_window(self, suggestions):
        """创建补全窗口的完整实现"""
        if self.completion_window:
            self.completion_window.destroy()

        # 获取光标位置
        cursor_pos = self.text_widget.index(tk.INSERT)
        x, y, _, _ = self.text_widget.bbox(cursor_pos)
        x += self.text_widget.winfo_rootx()
        y += self.text_widget.winfo_rooty() + 20

        # 创建弹出窗口
        self.completion_window = tk.Toplevel(self.text_widget)
        self.completion_window.wm_overrideredirect(True)
        self.completion_window.geometry(f"+{x}+{y}")

        # 创建列表框
        listbox = tk.Listbox(self.completion_window, height=min(8, len(suggestions)))
        listbox.pack()

        # 填充建议
        self.suggestions = suggestions
        for suggestion in suggestions:
            display_text = f"{suggestion['text']} ({suggestion['type']})"
            listbox.insert(tk.END, display_text)

        # 绑定选择事件
        listbox.bind('<Double-Button-1>', self._on_suggestion_select)
        listbox.bind('<Return>', self._on_suggestion_select)

        # 绑定键盘事件
        self.completion_window.bind('<Escape>', lambda e: self._hide_completion())

        # 选中第一项
        if suggestions:
            listbox.selection_set(0)
            listbox.focus_set()

    def _on_suggestion_select(self, event):
        """处理建议选择"""
        if not self.completion_window:
            return

        listbox = event.widget
        selection = listbox.curselection()
        if selection:
            index = selection[0]
            suggestion = self.suggestions[index]

            # 插入建议文本
            cursor_pos = self.text_widget.index(tk.INSERT)
            self.text_widget.insert(cursor_pos, suggestion['text'])

        self._hide_completion()

    def _on_key_release(self, event):
        """键盘释放事件处理"""
        if event.keysym in ['Up', 'Down', 'Left', 'Right', 'Return', 'Tab', 'Escape']:
            self._hide_completion()
            return

        # 检查是否应该触发补全
        if event.char and (event.char.isalnum() or event.char in '._'):
            self.text_widget.after(300, self._auto_complete)

    def _auto_complete(self):
        """自动补全逻辑"""
        try:
            cursor_pos = self.text_widget.index(tk.INSERT)
            sql_content = self.text_widget.get('1.0', tk.END)

            # 修复字符位置计算
            char_pos = self._calculate_char_position(cursor_pos, sql_content)

            suggestions = self.completion_engine.get_completions(sql_content, char_pos)
            print(f"获得建议数量: {len(suggestions)}")  # 调试输出

            if suggestions and len(suggestions) > 0:
                self._create_completion_window(suggestions)

        except Exception as e:
            print(f"自动补全出错: {e}")