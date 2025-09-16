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
        print(f"=== SmartSQLCompletion.get_completions ===")
        print(f"输入SQL: '{sql.strip()}'")
        print(f"光标位置: {cursor_pos}")

        # 获取当前输入的部分词
        partial_word = self._get_current_word(sql, cursor_pos)
        sql_upper = sql.upper().strip()

        print(f"部分词: '{partial_word}'")

        suggestions = []

        # 根据上下文和部分词生成建议
        if not sql_upper or not partial_word:
            # 空内容，返回基础关键字
            suggestions = self._get_basic_keywords()
        elif self._matches_keywords(partial_word):
            # 匹配关键字
            suggestions = self._get_matching_keywords(partial_word)
            if 'SELECT' in sql_upper and 'FROM' not in sql_upper:
                suggestions.extend(self._get_simple_column_suggestions(partial_word))  # 修改这里
            elif 'FROM' in sql_upper:
                suggestions.extend(self._get_table_suggestions(partial_word))
        else:
            # 上下文相关建议
            if 'SELECT' in sql_upper and 'FROM' not in sql_upper:
                suggestions = self._get_simple_column_suggestions(partial_word)  # 修改这里
            elif 'FROM' in sql_upper:
                suggestions = self._get_table_suggestions(partial_word)
            else:
                suggestions = self._get_matching_keywords(partial_word)

        # 过滤和排序
        filtered_suggestions = self._filter_and_rank(suggestions, partial_word)

        print(f"返回建议数量: {len(filtered_suggestions)}")
        return filtered_suggestions

    def _get_simple_column_suggestions(self, partial_word: str) -> List[Dict]:
        """获取简单的列建议（不需要context）"""
        columns = [
            {'text': '*', 'type': 'column', 'description': '所有列'},
            {'text': 'COUNT(*)', 'type': 'function', 'description': '统计行数'},
            {'text': 'SUM()', 'type': 'function', 'description': '求和函数'},
            {'text': 'AVG()', 'type': 'function', 'description': '平均值函数'},
            {'text': 'MAX()', 'type': 'function', 'description': '最大值函数'},
            {'text': 'MIN()', 'type': 'function', 'description': '最小值函数'},
            {'text': 'id', 'type': 'column', 'description': 'ID列'},
            {'text': 'name', 'type': 'column', 'description': '名称列'},
            {'text': 'email', 'type': 'column', 'description': '邮箱列'},
            {'text': 'created_at', 'type': 'column', 'description': '创建时间'},
            {'text': 'updated_at', 'type': 'column', 'description': '更新时间'},
            {'text': 'status', 'type': 'column', 'description': '状态列'},
            {'text': 'price', 'type': 'column', 'description': '价格列'},
            {'text': 'quantity', 'type': 'column', 'description': '数量列'},
        ]

        if not partial_word:
            return columns
        return [col for col in columns if col['text'].upper().startswith(partial_word)]

    def _get_column_suggestions(self, partial_word: str) -> List[Dict]:
        """获取列建议"""
        columns = [
            {'text': '*', 'type': 'column', 'description': '所有列'},
            {'text': 'COUNT(*)', 'type': 'function', 'description': '统计行数'},
            {'text': 'id', 'type': 'column', 'description': 'ID列'},
            {'text': 'name', 'type': 'column', 'description': '名称列'},
            {'text': 'email', 'type': 'column', 'description': '邮箱列'},
            {'text': 'created_at', 'type': 'column', 'description': '创建时间'},
            {'text': 'updated_at', 'type': 'column', 'description': '更新时间'},
            {'text': 'status', 'type': 'column', 'description': '状态列'},
            {'text': 'price', 'type': 'column', 'description': '价格列'},
            {'text': 'quantity', 'type': 'column', 'description': '数量列'},
        ]

        if not partial_word:
            return columns
        return [col for col in columns if col['text'].upper().startswith(partial_word)]

    def _get_current_word(self, sql: str, cursor_pos: int) -> str:
        """获取光标处的当前单词"""
        if cursor_pos <= 0:
            return ""

        # 向前查找单词的开始
        start = cursor_pos - 1
        while start >= 0 and (sql[start].isalnum() or sql[start] in '_'):
            start -= 1
        start += 1

        return sql[start:cursor_pos].upper()

    def _get_basic_keywords(self) -> List[Dict]:
        """获取基础关键字"""
        return [
            {'text': 'SELECT', 'type': 'keyword', 'description': '选择数据'},
            {'text': 'INSERT', 'type': 'keyword', 'description': '插入数据'},
            {'text': 'UPDATE', 'type': 'keyword', 'description': '更新数据'},
            {'text': 'DELETE', 'type': 'keyword', 'description': '删除数据'},
            {'text': 'CREATE', 'type': 'keyword', 'description': '创建表'},
            {'text': 'DROP', 'type': 'keyword', 'description': '删除表'},
        ]

    def _matches_keywords(self, partial_word: str) -> bool:
        """检查部分词是否匹配关键字"""
        # 扩展关键字列表
        keywords = [
            'SELECT', 'SET', 'SHOW', 'INSERT', 'INTO', 'UPDATE', 'DELETE', 'DROP',
            'CREATE', 'ALTER', 'FROM', 'WHERE', 'ORDER', 'GROUP', 'HAVING', 'LIMIT',
            'JOIN', 'LEFT', 'RIGHT', 'INNER', 'UNION', 'DISTINCT', 'COUNT', 'SUM',
            'AVG', 'MAX', 'MIN', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN'
        ]
        return any(keyword.startswith(partial_word) or partial_word in keyword for keyword in keywords)

    def _get_matching_keywords(self, partial_word: str) -> List[Dict]:
        """获取匹配的关键字"""
        all_keywords = [
            {'text': 'SELECT', 'type': 'keyword', 'description': '查询数据'},
            {'text': 'SET', 'type': 'keyword', 'description': '设置值'},
            {'text': 'SHOW', 'type': 'keyword', 'description': '显示信息'},
            {'text': 'INSERT', 'type': 'keyword', 'description': '插入数据'},
            {'text': 'INTO', 'type': 'keyword', 'description': '插入到表'},
            {'text': 'UPDATE', 'type': 'keyword', 'description': '更新数据'},
            {'text': 'DELETE', 'type': 'keyword', 'description': '删除数据'},
            {'text': 'DROP', 'type': 'keyword', 'description': '删除表/数据库'},
            {'text': 'CREATE', 'type': 'keyword', 'description': '创建表/数据库'},
            {'text': 'ALTER', 'type': 'keyword', 'description': '修改表结构'},
            {'text': 'FROM', 'type': 'keyword', 'description': '指定数据源'},
            {'text': 'WHERE', 'type': 'keyword', 'description': '过滤条件'},
            {'text': 'ORDER BY', 'type': 'keyword', 'description': '排序'},
            {'text': 'GROUP BY', 'type': 'keyword', 'description': '分组'},
            {'text': 'HAVING', 'type': 'keyword', 'description': '分组过滤条件'},
            {'text': 'LIMIT', 'type': 'keyword', 'description': '限制结果数量'},
            {'text': 'JOIN', 'type': 'keyword', 'description': '连接表'},
            {'text': 'LEFT JOIN', 'type': 'keyword', 'description': '左连接'},
            {'text': 'RIGHT JOIN', 'type': 'keyword', 'description': '右连接'},
            {'text': 'INNER JOIN', 'type': 'keyword', 'description': '内连接'},
            {'text': 'UNION', 'type': 'keyword', 'description': '合并结果'},
            {'text': 'DISTINCT', 'type': 'keyword', 'description': '去重'},
            {'text': 'COUNT', 'type': 'function', 'description': '计数函数'},
            {'text': 'SUM', 'type': 'function', 'description': '求和函数'},
            {'text': 'AVG', 'type': 'function', 'description': '平均值函数'},
            {'text': 'MAX', 'type': 'function', 'description': '最大值函数'},
            {'text': 'MIN', 'type': 'function', 'description': '最小值函数'},
        ]

        # 匹配以partial_word开头的关键字
        matches = [kw for kw in all_keywords if kw['text'].startswith(partial_word)]

        # 如果没有前缀匹配，尝试包含匹配
        if not matches:
            matches = [kw for kw in all_keywords if partial_word in kw['text']]

        return matches

    def _get_table_suggestions(self, partial_word: str) -> List[Dict]:
        """获取表建议"""
        tables = [
            {'text': 'users', 'type': 'table', 'description': '用户表'},
            {'text': 'orders', 'type': 'table', 'description': '订单表'},
            {'text': 'products', 'type': 'table', 'description': '产品表'},
            {'text': 'customers', 'type': 'table', 'description': '客户表'},
        ]

        if not partial_word:
            return tables
        return [table for table in tables if table['text'].upper().startswith(partial_word)]

    def _filter_and_rank(self, suggestions: List[Dict], partial_word: str) -> List[Dict]:
        """过滤和排序建议"""
        if not suggestions:
            return []

        # 按匹配度排序
        def score_suggestion(suggestion):
            text = suggestion['text'].upper()
            if text.startswith(partial_word):
                return 100 - len(text)  # 前缀匹配，越短越好
            elif partial_word in text:
                return 50 - len(text)  # 包含匹配
            return 0

        suggestions.sort(key=score_suggestion, reverse=True)
        return suggestions[:8]  # 限制数量

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



# 集成到GUI
class CompletionUI:
    def __init__(self, sql_text_widget, completion_engine):
        self.text_widget = sql_text_widget
        self.completion_engine = completion_engine
        self.completion_window = None
        self.suggestions = []
        self.current_suggestions = []
        self.listbox = None

        # 绑定事件
        self.text_widget.bind('<KeyPress>', self._on_key_press)
        self.text_widget.bind('<KeyRelease>', self._on_key_release)
        self.text_widget.bind('<Control-space>', self._trigger_completion)

        # 绑定特殊按键处理
        self.text_widget.bind('<Escape>', self._on_escape)
        self.text_widget.bind('<Up>', self._on_arrow_key)
        self.text_widget.bind('<Down>', self._on_arrow_key)
        self.text_widget.bind('<Return>', self._on_return_key)
        self.text_widget.bind('<Tab>', self._on_tab_key)

        self.text_widget.focus_set()
        print("事件绑定完成")

    def _on_key_press(self, event):
        """按键按下事件"""
        print(f"KEY PRESS: {event.keysym} ('{event.char}')")

        # 如果补全窗口打开，处理特殊按键
        if self.completion_window:
            if event.keysym == 'Escape':
                self._hide_completion()
                return 'break'
            elif event.keysym == 'Return' or event.keysym == 'Tab':
                self._apply_current_selection()
                return 'break'
            elif event.keysym == 'Up':
                self._move_selection(-1)
                return 'break'
            elif event.keysym == 'Down':
                self._move_selection(1)
                return 'break'
            elif event.keysym in ['space', 'BackSpace']:
                # 空格或退格时隐藏补全，允许正常输入
                self._hide_completion()
                return None  # 让事件继续传播，允许正常输入
            elif event.char and (event.char.isalnum() or event.char in '._'):
                # 继续输入字符时，隐藏当前窗口，准备显示新的
                self._hide_completion()
                self.text_widget.after(100, self._auto_complete)
                return None  # 让事件继续传播，允许字符输入
            else:
                # 其他按键隐藏补全
                self._hide_completion()
                return None

        # 如果没有补全窗口，正常处理输入
        if event.char and (event.char.isalnum() or event.char in '._'):
            self.text_widget.after(100, self._auto_complete)

    def _move_selection(self, direction):
        """移动选择（改进版本）"""
        if not self.listbox:
            return

        current = self.listbox.curselection()
        if current:
            index = current[0]
            new_index = index + direction

            # 边界检查
            if new_index < 0:
                new_index = 0
            elif new_index >= self.listbox.size():
                new_index = self.listbox.size() - 1

            self.listbox.selection_clear(0, tk.END)
            self.listbox.selection_set(new_index)
            self.listbox.activate(new_index)
            self.listbox.see(new_index)

        # 确保文本框保持焦点
        self.text_widget.focus_set()

    def _on_key_release(self, event):
        """按键释放事件"""
        print(f"KEY RELEASE: {event.keysym} ('{event.char}')")

    def _trigger_completion(self, event):
        """手动触发补全 (Ctrl+Space)"""
        print("手动触发补全 (Ctrl+Space)")
        self._auto_complete()
        return 'break'

    def _auto_complete(self):
        """自动补全"""
        print("=== 执行自动补全 ===")
        try:
            cursor_pos = self.text_widget.index(tk.INSERT)
            sql_content = self.text_widget.get('1.0', tk.END)

            print(f"光标位置: {cursor_pos}")
            print(f"SQL内容: '{sql_content.strip()}'")
            print(f"SQL长度: {len(sql_content.strip())}")

            if len(sql_content.strip()) == 0:
                print("内容为空，不触发补全")
                return

            # 调用补全引擎
            char_pos = self._calculate_char_position(cursor_pos, sql_content)
            print(f"字符位置: {char_pos}")

            suggestions = self.completion_engine.get_completions(sql_content, char_pos)
            print(f"获取到建议: {len(suggestions)}")

            if suggestions:
                print("建议列表:")
                for i, suggestion in enumerate(suggestions):
                    print(f"  {i + 1}. {suggestion['text']} ({suggestion['type']})")

                # 创建简单的补全窗口
                self._create_completion_window(suggestions)
            else:
                print("没有获取到建议")

        except Exception as e:
            print(f"自动补全出错: {e}")
            import traceback
            traceback.print_exc()

    def _calculate_char_position(self, cursor_pos, sql_content):
        """计算字符位置"""
        try:
            lines = sql_content.split('\n')
            line_num = int(cursor_pos.split('.')[0]) - 1
            col_num = int(cursor_pos.split('.')[1])

            char_pos = 0
            for i in range(line_num):
                char_pos += len(lines[i]) + 1  # +1 for newline
            char_pos += col_num

            return char_pos
        except:
            return len(sql_content)

    def _create_completion_window(self, suggestions):
        """创建补全窗口"""
        print(f"=== 创建UI补全窗口，建议数量: {len(suggestions)} ===")

        # 隐藏之前的窗口
        if self.completion_window:
            self.completion_window.destroy()
            self.completion_window = None

        if not suggestions:
            return

        try:
            # 获取光标位置
            cursor_pos = self.text_widget.index(tk.INSERT)
            bbox = self.text_widget.bbox(cursor_pos)

            if bbox:
                x = bbox[0] + self.text_widget.winfo_rootx()
                y = bbox[1] + bbox[3] + self.text_widget.winfo_rooty()
            else:
                # 如果无法获取位置，使用默认位置
                x = self.text_widget.winfo_rootx() + 50
                y = self.text_widget.winfo_rooty() + 50

            # 创建弹出窗口
            self.completion_window = tk.Toplevel(self.text_widget)
            self.completion_window.wm_overrideredirect(True)  # 去掉窗口边框
            self.completion_window.configure(bg='white', relief='solid', bd=1)

            # 重要：设置窗口属性，让它不抢夺焦点
            self.completion_window.wm_attributes('-topmost', True)

            # 设置窗口位置
            self.completion_window.geometry(f"+{x}+{y}")

            # 创建列表框
            self.listbox = tk.Listbox(
                self.completion_window,
                height=min(8, len(suggestions)),
                width=max(30, max(len(s['text'] + ' - ' + s.get('description', '')) for s in suggestions)),
                font=("Consolas", 9),
                activestyle='dotbox',
                selectmode='single'
            )
            self.listbox.pack(padx=2, pady=2)

            # 填充建议
            self.current_suggestions = suggestions
            for suggestion in suggestions:
                display_text = f"{suggestion['text']} - {suggestion.get('description', '')}"
                self.listbox.insert(tk.END, display_text)

            # 选中第一项，但不给焦点
            if suggestions:
                self.listbox.selection_set(0)
                self.listbox.activate(0)

            # 绑定鼠标事件
            self.listbox.bind('<Double-Button-1>', self._on_suggestion_select)
            self.listbox.bind('<Button-1>', self._on_listbox_click)

            # 不要给列表框焦点！让文本框保持焦点
            # self.listbox.focus_set()  # 删除这行！

            # 确保文本框保持焦点
            self.text_widget.focus_set()

            # 移除FocusOut绑定，因为会干扰正常输入
            # self.completion_window.bind('<FocusOut>', lambda e: self._hide_completion())

            print(f"✅ UI补全窗口已显示在位置 ({x}, {y})")

        except Exception as e:
            print(f"创建补全窗口出错: {e}")
            import traceback
            traceback.print_exc()

    def _on_listbox_click(self, event):
        """处理列表框点击事件"""
        # 点击列表框时选择对应项目
        index = self.listbox.nearest(event.y)
        self.listbox.selection_clear(0, tk.END)
        self.listbox.selection_set(index)
        self.listbox.activate(index)

        # 但立即将焦点还给文本框
        self.text_widget.focus_set()

    def _on_suggestion_select(self, event):
        """处理建议选择"""
        if not self.completion_window or not self.current_suggestions:
            return

        try:
            selection = self.listbox.curselection()
            if selection:
                index = selection[0]
                suggestion = self.current_suggestions[index]

                print(f"用户选择了: {suggestion['text']}")

                # 获取当前单词的位置
                cursor_pos = self.text_widget.index(tk.INSERT)
                current_line = self.text_widget.get(f"{cursor_pos.split('.')[0]}.0", cursor_pos)

                # 找到当前单词的开始位置
                words = current_line.split()
                if words:
                    last_word_start = current_line.rfind(words[-1])
                    start_pos = f"{cursor_pos.split('.')[0]}.{last_word_start}"

                    # 删除当前部分单词，插入完整建议
                    self.text_widget.delete(start_pos, cursor_pos)
                    self.text_widget.insert(start_pos, suggestion['text'])

                    # 如果是关键字，添加空格
                    if suggestion['type'] == 'keyword':
                        self.text_widget.insert(tk.INSERT, ' ')

                self._hide_completion()

        except Exception as e:
            print(f"选择建议出错: {e}")
            self._hide_completion()

    def _on_list_up(self, event):
        """列表向上导航"""
        current = self.listbox.curselection()
        if current:
            index = current[0]
            if index > 0:
                self.listbox.selection_clear(index)
                self.listbox.selection_set(index - 1)
                self.listbox.activate(index - 1)
        return 'break'

    def _on_list_down(self, event):
        """列表向下导航"""
        current = self.listbox.curselection()
        if current:
            index = current[0]
            if index < self.listbox.size() - 1:
                self.listbox.selection_clear(index)
                self.listbox.selection_set(index + 1)
                self.listbox.activate(index + 1)
        return 'break'


    def _handle_completion_navigation(self, event):
        """处理补全窗口的键盘导航"""
        if not self.completion_window or not self.listbox:
            return None

        if event.keysym == 'Escape':
            self._hide_completion()
            return 'break'

        elif event.keysym == 'Return' or event.keysym == 'Tab':
            self._apply_current_selection()
            return 'break'

        elif event.keysym == 'Up':
            self._move_selection(-1)
            return 'break'

        elif event.keysym == 'Down':
            self._move_selection(1)
            return 'break'

        return None

    def _apply_current_selection(self):
        """应用当前选择的建议"""
        if not self.listbox or not self.current_suggestions:
            return

        selection = self.listbox.curselection()
        if selection:
            index = selection[0]
            suggestion = self.current_suggestions[index]
            self._insert_suggestion(suggestion)

    def _insert_suggestion(self, suggestion):
        """插入选择的建议"""
        try:
            cursor_pos = self.text_widget.index(tk.INSERT)

            # 获取当前行内容
            line_start = f"{cursor_pos.split('.')[0]}.0"
            line_content = self.text_widget.get(line_start, cursor_pos)

            # 找到当前单词的开始位置
            import re
            words = re.findall(r'\b\w+', line_content)
            if words:
                last_word = words[-1]
                word_start_pos = line_content.rfind(last_word)
                delete_start = f"{cursor_pos.split('.')[0]}.{word_start_pos}"

                # 删除部分输入，插入完整建议
                self.text_widget.delete(delete_start, cursor_pos)
                self.text_widget.insert(delete_start, suggestion['text'])

                # 对于关键字，添加空格
                if suggestion['type'] == 'keyword':
                    self.text_widget.insert(tk.INSERT, ' ')

            self._hide_completion()

            # 重新获得焦点
            self.text_widget.focus_set()

        except Exception as e:
            print(f"插入建议失败: {e}")
            self._hide_completion()

    def _on_escape(self, event):
        """Escape键处理"""
        if self.completion_window:
            self._hide_completion()
            return 'break'
        return None

    def _on_return_key(self, event):
        """回车键处理"""
        if self.completion_window:
            self._apply_current_selection()
            return 'break'
        return None

    def _on_tab_key(self, event):
        """Tab键处理"""
        if self.completion_window:
            self._apply_current_selection()
            return 'break'
        return None

    def _on_arrow_key(self, event):
        """方向键处理"""
        if self.completion_window:
            if event.keysym == 'Up':
                self._move_selection(-1)
                return 'break'
            elif event.keysym == 'Down':
                self._move_selection(1)
                return 'break'
        return None

    def _hide_completion(self):
        """隐藏补全窗口"""
        if self.completion_window:
            print("隐藏补全窗口")
            self.completion_window.destroy()
            self.completion_window = None
            self.current_suggestions = []
            self.listbox = None

            # 确保文本框重新获得焦点
            self.text_widget.focus_set()