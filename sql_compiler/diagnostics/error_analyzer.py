import re
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass
from difflib import get_close_matches
from catalog.catalog_manager import CatalogManager


@dataclass
class ErrorSuggestion:
    """错误建议"""
    error_type: str
    description: str
    suggestion: str
    corrected_sql: Optional[str] = None
    confidence: float = 0.0  # 0.0 - 1.0


class SQLErrorAnalyzer:
    """SQL错误分析器"""

    def __init__(self, catalog_manager: CatalogManager = None):
        self.catalog_manager = catalog_manager
        self.sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER', 'BY',
            'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'CREATE',
            'TABLE', 'DROP', 'ALTER', 'INDEX', 'PRIMARY', 'KEY', 'FOREIGN',
            'REFERENCES', 'NOT', 'NULL', 'UNIQUE', 'DEFAULT', 'AUTO_INCREMENT',
            'INT', 'VARCHAR', 'CHAR', 'TEXT', 'DATE', 'DATETIME', 'TIMESTAMP',
            'DECIMAL', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'TINYINT', 'SMALLINT',
            'MEDIUMINT', 'BIGINT', 'AND', 'OR', 'IN', 'LIKE', 'BETWEEN',
            'IS', 'EXISTS', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'JOIN',
            'ON', 'UNION', 'DISTINCT', 'AS', 'ASC', 'DESC', 'LIMIT', 'OFFSET',
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'UPPER', 'LOWER', 'TRIM'
        }

        self.common_functions = {
            'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'UPPER', 'LOWER', 'TRIM',
            'SUBSTRING', 'CONCAT', 'LENGTH', 'ROUND', 'ABS', 'NOW', 'CURDATE'
        }

    def analyze_error(self, sql: str, error: Exception) -> List[ErrorSuggestion]:
        """分析SQL错误并提供建议"""
        suggestions = []
        error_str = str(error).lower()

        # 根据错误类型进行分析
        if "syntax error" in error_str or "unexpected token" in error_str:
            suggestions.extend(self._analyze_syntax_errors(sql, error))

        if "table" in error_str and ("not found" in error_str or "doesn't exist" in error_str):
            suggestions.extend(self._analyze_table_errors(sql, error))

        if "column" in error_str and ("not found" in error_str or "unknown" in error_str):
            suggestions.extend(self._analyze_column_errors(sql, error))

        if "function" in error_str and ("not found" in error_str or "unknown" in error_str):
            suggestions.extend(self._analyze_function_errors(sql, error))

        # 通用分析
        suggestions.extend(self._analyze_common_mistakes(sql))

        # 按置信度排序
        suggestions.sort(key=lambda x: x.confidence, reverse=True)
        return suggestions[:5]

    def suggest_corrections(self, sql: str) -> List[ErrorSuggestion]:
        """为SQL提供改进建议（即使没有错误）- 修复版本"""
        suggestions = []
        sql_upper = sql.upper()

        # 🔑 新增：主动进行语法检查
        suggestions.extend(self._proactive_syntax_check(sql))

        # 1. 性能建议
        if 'SELECT *' in sql_upper and 'WHERE' not in sql_upper:
            suggestions.append(ErrorSuggestion(
                error_type="PERFORMANCE_TIP",
                description="性能提示",
                suggestion="考虑只选择需要的列，并添加WHERE条件来限制结果集",
                confidence=0.4
            ))

        # 2. 安全建议
        if any(dangerous in sql_upper for dangerous in ['DROP', 'DELETE FROM', 'TRUNCATE']):
            suggestions.append(ErrorSuggestion(
                error_type="SAFETY_WARNING",
                description="安全警告",
                suggestion="这个操作会修改或删除数据，请确认操作的正确性",
                confidence=0.8
            ))

        return suggestions

    def _proactive_syntax_check(self, sql: str) -> List[ErrorSuggestion]:
        """主动进行语法检查（不需要错误即可检查）"""
        suggestions = []
        sql_upper = sql.upper()

        # 1. 缺少分号检查
        if not sql.strip().endswith(';'):
            suggestions.append(ErrorSuggestion(
                error_type="MISSING_SEMICOLON",
                description="SQL语句缺少结尾分号",
                suggestion="在SQL语句末尾添加分号 (;)",
                corrected_sql=sql.strip() + ';',
                confidence=0.9
            ))

        # 2. 括号不匹配检查
        open_parens = sql.count('(')
        close_parens = sql.count(')')
        if open_parens != close_parens:
            suggestions.append(ErrorSuggestion(
                error_type="UNMATCHED_PARENTHESES",
                description=f"括号不匹配：开括号{open_parens}个，闭括号{close_parens}个",
                suggestion="检查并修正括号匹配",
                confidence=0.8
            ))

        # 3. 常见关键字拼写错误检查 - 只在关键字位置检查
        # 提取SQL中的关键字位置
        keyword_positions = self._find_keyword_positions(sql_upper)

        for position, word in keyword_positions:
            if word not in self.sql_keywords and len(word) > 2:
                matches = get_close_matches(word, self.sql_keywords, n=3, cutoff=0.6)
                if matches:
                    # 只替换关键字位置的单词，而不是所有出现的地方
                    sql_list = list(sql_upper)
                    sql_list[position:position + len(word)] = list(matches[0])
                    corrected_sql = ''.join(sql_list).lower()

                    suggestions.append(ErrorSuggestion(
                        error_type="KEYWORD_TYPO",
                        description=f"可能的关键字拼写错误：'{word}'",
                        suggestion=f"你是否想写 '{matches[0]}'？其他可能：{', '.join(matches[1:])}",
                        corrected_sql=corrected_sql,
                        confidence=0.7
                    ))

        # 4. SELECT后缺少列名检查
        if re.search(r'SELECT\s+FROM', sql_upper):
            suggestions.append(ErrorSuggestion(
                error_type="MISSING_COLUMNS",
                description="SELECT和FROM之间缺少列名",
                suggestion="在SELECT和FROM之间指定要查询的列，或使用 * 查询所有列",
                corrected_sql=sql.upper().replace('SELECT FROM', 'SELECT * FROM').lower(),
                confidence=0.8
            ))

        # 5. 字符串值可能缺少引号
        equals_pattern = r'=\s*([a-zA-Z]\w*)\b'
        matches = re.findall(equals_pattern, sql)
        if matches:
            suggestions.append(ErrorSuggestion(
                error_type="MISSING_QUOTES",
                description="字符串值可能缺少引号",
                suggestion="字符串值应该用单引号或双引号包围，例如：name = 'John'",
                confidence=0.5
            ))

        # 6. JOIN缺少ON子句检查
        if 'JOIN' in sql_upper and 'ON' not in sql_upper:
            suggestions.append(ErrorSuggestion(
                error_type="MISSING_JOIN_CONDITION",
                description="JOIN语句缺少ON条件",
                suggestion="JOIN语句需要指定连接条件，例如：LEFT JOIN table2 ON table1.id = table2.id",
                confidence=0.7
            ))

        return suggestions

    def _find_keyword_positions(self, sql_upper: str) -> List[Tuple[int, str]]:
        """找到SQL中可能的关键字位置"""
        keyword_positions = []

        # 查找SQL关键字的位置（在特定上下文中）
        patterns = [
            (
            r'\b(SELECT|FROM|WHERE|GROUP BY|HAVING|ORDER BY|INSERT INTO|VALUES|UPDATE|SET|DELETE FROM|CREATE TABLE|DROP TABLE|ALTER TABLE|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|OUTER JOIN)\b',
            1),
            (r'\b(AND|OR|NOT|IN|LIKE|BETWEEN|IS NULL|IS NOT NULL|EXISTS)\b', 1),
            (r'\b(INT|VARCHAR|CHAR|TEXT|DATE|DATETIME|TIMESTAMP|DECIMAL|FLOAT|DOUBLE|BOOLEAN)\b', 1),
            (r'\b(PRIMARY KEY|FOREIGN KEY|REFERENCES|UNIQUE|NOT NULL|DEFAULT|AUTO_INCREMENT)\b', 2)
        ]

        for pattern, group in patterns:
            for match in re.finditer(pattern, sql_upper):
                keyword = match.group(group) if group <= len(match.groups()) else match.group(0)
                keyword_positions.append((match.start(), keyword))

        return keyword_positions

    def _analyze_syntax_errors(self, sql: str, error: Exception) -> List[ErrorSuggestion]:
        """分析语法错误"""
        suggestions = []
        sql_upper = sql.upper()

        # 1. 缺少分号
        if not sql.strip().endswith(';'):
            suggestions.append(ErrorSuggestion(
                error_type="MISSING_SEMICOLON",
                description="SQL语句缺少结尾分号",
                suggestion="在SQL语句末尾添加分号 (;)",
                corrected_sql=sql.strip() + ';',
                confidence=0.9
            ))

        # 2. 括号不匹配
        open_parens = sql.count('(')
        close_parens = sql.count(')')
        if open_parens != close_parens:
            suggestions.append(ErrorSuggestion(
                error_type="UNMATCHED_PARENTHESES",
                description=f"括号不匹配：开括号{open_parens}个，闭括号{close_parens}个",
                suggestion="检查并修正括号匹配",
                confidence=0.8
            ))

        # 3. 常见关键字拼写错误 - 只在关键字位置检查
        keyword_positions = self._find_keyword_positions(sql_upper)

        for position, word in keyword_positions:
            if word not in self.sql_keywords and len(word) > 2:
                matches = get_close_matches(word, self.sql_keywords, n=3, cutoff=0.6)
                if matches:
                    sql_list = list(sql_upper)
                    sql_list[position:position + len(word)] = list(matches[0])
                    corrected_sql = ''.join(sql_list).lower()

                    suggestions.append(ErrorSuggestion(
                        error_type="KEYWORD_TYPO",
                        description=f"可能的关键字拼写错误：'{word}'",
                        suggestion=f"你是否想写 '{matches[0]}'？其他可能：{', '.join(matches[1:])}",
                        corrected_sql=corrected_sql,
                        confidence=0.7
                    ))

        # 4. SELECT后缺少列名
        if re.search(r'SELECT\s+FROM', sql_upper):
            suggestions.append(ErrorSuggestion(
                error_type="MISSING_COLUMNS",
                description="SELECT和FROM之间缺少列名",
                suggestion="在SELECT和FROM之间指定要查询的列，或使用 * 查询所有列",
                corrected_sql=sql.upper().replace('SELECT FROM', 'SELECT * FROM').lower(),
                confidence=0.8
            ))

        # 5. GROUP BY后缺少HAVING的错误使用
        if 'GROUP BY' in sql_upper and 'WHERE' in sql_upper:
            # 检查是否在WHERE中使用了聚合函数
            where_part = sql_upper.split('GROUP BY')[0].split('WHERE')[1] if 'WHERE' in sql_upper.split('GROUP BY')[
                0] else ""
            if any(func in where_part for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']):
                suggestions.append(ErrorSuggestion(
                    error_type="AGGREGATE_IN_WHERE",
                    description="WHERE子句中不能使用聚合函数",
                    suggestion="聚合函数的条件应该放在HAVING子句中",
                    confidence=0.7
                ))

        return suggestions

    def _analyze_table_errors(self, sql: str, error: Exception) -> List[ErrorSuggestion]:
        """分析表相关错误"""
        suggestions = []

        if not self.catalog_manager:
            return suggestions

        # 提取SQL中的表名
        table_pattern = r'FROM\s+(\w+)|JOIN\s+(\w+)|INTO\s+(\w+)|UPDATE\s+(\w+)'
        matches = re.findall(table_pattern, sql, re.IGNORECASE)

        mentioned_tables = []
        for match_group in matches:
            for table in match_group:
                if table:
                    mentioned_tables.append(table.lower())

        # 获取数据库中实际存在的表
        try:
            existing_tables = [t.lower() for t in self.catalog_manager.get_all_tables()]
        except:
            existing_tables = []

        for table in mentioned_tables:
            if table not in existing_tables:
                # 查找相似的表名
                similar_tables = get_close_matches(table, existing_tables, n=3, cutoff=0.6)

                if similar_tables:
                    corrected_sql = sql.lower().replace(table, similar_tables[0])
                    suggestions.append(ErrorSuggestion(
                        error_type="TABLE_NOT_FOUND",
                        description=f"表 '{table}' 不存在",
                        suggestion=f"你是否想要查询表 '{similar_tables[0]}'？其他可能：{', '.join(similar_tables[1:])}",
                        corrected_sql=corrected_sql,
                        confidence=0.8
                    ))
                else:
                    # 显示存在的表
                    if existing_tables:
                        suggestions.append(ErrorSuggestion(
                            error_type="TABLE_NOT_FOUND",
                            description=f"表 '{table}' 不存在",
                            suggestion=f"当前数据库中的表有：{', '.join(existing_tables)}",
                            confidence=0.6
                        ))
                    else:
                        suggestions.append(ErrorSuggestion(
                            error_type="NO_TABLES",
                            description="数据库中没有任何表",
                            suggestion="请先创建表，例如：CREATE TABLE table_name (id INT, name VARCHAR(50));",
                            confidence=0.7
                        ))

        return suggestions

    def _analyze_column_errors(self, sql: str, error: Exception) -> List[ErrorSuggestion]:
        """分析列相关错误"""
        suggestions = []

        if not self.catalog_manager:
            return suggestions

        # 简化的列名提取（实际项目中需要更复杂的解析）
        # 提取SELECT后的列名
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            columns_str = select_match.group(1)
            # 简单分割列名（忽略复杂的表达式）
            mentioned_columns = []
            for col in columns_str.split(','):
                col = col.strip()
                if col != '*' and not any(func in col.upper() for func in self.common_functions):
                    # 提取纯列名（去除别名等）
                    col_name = col.split()[0] if col else ""
                    if col_name and col_name.isalpha():
                        mentioned_columns.append(col_name.lower())

        # 提取表名
        table_match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1).lower()

            try:
                # 获取表的列信息
                table_columns = []
                schema = self.catalog_manager.get_table_schema(table_name)
                if schema:
                    table_columns = [col[0].lower() for col in schema]

                for mentioned_col in mentioned_columns:
                    if mentioned_col not in table_columns:
                        # 查找相似的列名
                        similar_columns = get_close_matches(mentioned_col, table_columns, n=3, cutoff=0.6)

                        if similar_columns:
                            corrected_sql = sql.lower().replace(mentioned_col, similar_columns[0])
                            suggestions.append(ErrorSuggestion(
                                error_type="COLUMN_NOT_FOUND",
                                description=f"表 '{table_name}' 中不存在列 '{mentioned_col}'",
                                suggestion=f"你是否想要查询列 '{similar_columns[0]}'？其他可能：{', '.join(similar_columns[1:])}",
                                corrected_sql=corrected_sql,
                                confidence=0.8
                            ))
                        else:
                            suggestions.append(ErrorSuggestion(
                                error_type="COLUMN_NOT_FOUND",
                                description=f"表 '{table_name}' 中不存在列 '{mentioned_col}'",
                                suggestion=f"表 '{table_name}' 中的列有：{', '.join(table_columns)}",
                                confidence=0.6
                            ))
            except Exception:
                pass

        return suggestions

    def _analyze_function_errors(self, sql: str, error: Exception) -> List[ErrorSuggestion]:
        """分析函数相关错误"""
        suggestions = []

        # 提取可能的函数名
        function_pattern = r'(\w+)\s*\('
        functions = re.findall(function_pattern, sql, re.IGNORECASE)

        for func in functions:
            func_upper = func.upper()
            if func_upper not in self.common_functions and func_upper not in self.sql_keywords:
                # 查找相似的函数名
                similar_functions = get_close_matches(func_upper, self.common_functions, n=3, cutoff=0.6)

                if similar_functions:
                    corrected_sql = sql.replace(func, similar_functions[0].lower())
                    suggestions.append(ErrorSuggestion(
                        error_type="FUNCTION_NOT_FOUND",
                        description=f"未知函数 '{func}'",
                        suggestion=f"你是否想使用 '{similar_functions[0]}'？其他可能：{', '.join(similar_functions[1:])}",
                        corrected_sql=corrected_sql,
                        confidence=0.7
                    ))

        return suggestions

    def _analyze_common_mistakes(self, sql: str) -> List[ErrorSuggestion]:
        """分析常见错误"""
        suggestions = []
        sql_upper = sql.upper()

        # 1. 字符串值没有引号
        # 简化检测：查找 = 后面的非数字值
        equals_pattern = r'=\s*([a-zA-Z]\w*)\b'
        matches = re.findall(equals_pattern, sql)
        if matches:
            suggestions.append(ErrorSuggestion(
                error_type="MISSING_QUOTES",
                description="字符串值可能缺少引号",
                suggestion="字符串值应该用单引号或双引号包围，例如：name = 'John'",
                confidence=0.5
            ))

        # 2. LIMIT子句语法错误
        if 'LIMIT' in sql_upper and 'OFFSET' not in sql_upper:
            limit_pattern = r'LIMIT\s+(\d+)\s*,\s*(\d+)'
            if re.search(limit_pattern, sql, re.IGNORECASE):
                suggestions.append(ErrorSuggestion(
                    error_type="LIMIT_SYNTAX",
                    description="LIMIT子句语法可能不正确",
                    suggestion="标准语法是 LIMIT count 或 LIMIT offset, count",
                    confidence=0.6
                ))

        # 3. JOIN缺少ON子句
        if 'JOIN' in sql_upper and 'ON' not in sql_upper:
            suggestions.append(ErrorSuggestion(
                error_type="MISSING_JOIN_CONDITION",
                description="JOIN语句缺少ON条件",
                suggestion="JOIN语句需要指定连接条件，例如：LEFT JOIN table2 ON table1.id = table2.id",
                confidence=0.7
            ))

        # 4. 聚合函数与非聚合列混用
        if 'GROUP BY' not in sql_upper:
            has_aggregate = any(func in sql_upper for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN('])
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if has_aggregate and select_match:
                columns_str = select_match.group(1)
                # 简化检测：如果有聚合函数，但也有其他非聚合列
                if ',' in columns_str and any(
                        func not in columns_str.upper() for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']):
                    suggestions.append(ErrorSuggestion(
                        error_type="AGGREGATE_WITHOUT_GROUP_BY",
                        description="使用聚合函数时可能需要GROUP BY",
                        suggestion="当SELECT中有聚合函数时，所有非聚合列都需要在GROUP BY中",
                        confidence=0.6
                    ))

        return suggestions


class SmartSQLCorrector:
    """智能SQL纠错器"""

    def __init__(self, catalog_manager: CatalogManager = None):
        self.analyzer = SQLErrorAnalyzer(catalog_manager)
        self.correction_history = []

    def analyze_and_suggest(self, sql: str, error: Exception = None) -> Dict[str, Any]:
        """分析SQL并提供建议 - 修复版本"""
        result = {
            'original_sql': sql,
            'has_error': error is not None,
            'error_message': str(error) if error else None,
            'suggestions': [],
            'corrected_sql_options': [],
            'improvement_tips': []
        }

        if error:
            # 有错误时进行错误分析
            suggestions = self.analyzer.analyze_error(sql, error)
            result['suggestions'] = [self._format_suggestion(s) for s in suggestions]

            # 提供可能的修正版本
            corrected_options = [s for s in suggestions if s.corrected_sql]
            result['corrected_sql_options'] = [
                {
                    'sql': s.corrected_sql,
                    'description': s.suggestion,
                    'confidence': s.confidence
                }
                for s in corrected_options[:3]  # 最多3个选项
            ]
        else:
            # 🔑 修复：没有错误时也进行完整的检查
            improvements = self.analyzer.suggest_corrections(sql)

            # 🔑 将语法问题也归类为改进建议
            syntax_issues = [s for s in improvements if s.error_type in [
                'MISSING_SEMICOLON', 'UNMATCHED_PARENTHESES', 'KEYWORD_TYPO',
                'MISSING_COLUMNS', 'MISSING_QUOTES', 'MISSING_JOIN_CONDITION'
            ]]

            other_improvements = [s for s in improvements if s.error_type not in [
                'MISSING_SEMICOLON', 'UNMATCHED_PARENTHESES', 'KEYWORD_TYPO',
                'MISSING_COLUMNS', 'MISSING_QUOTES', 'MISSING_JOIN_CONDITION'
            ]]

            # 如果有语法问题，放到suggestions中
            if syntax_issues:
                result['suggestions'] = [self._format_suggestion(s) for s in syntax_issues]

                # 提供修正选项
                corrected_options = [s for s in syntax_issues if s.corrected_sql]
                result['corrected_sql_options'] = [
                    {
                        'sql': s.corrected_sql,
                        'description': s.suggestion,
                        'confidence': s.confidence
                    }
                    for s in corrected_options[:3]
                ]

            # 其他改进建议
            result['improvement_tips'] = [self._format_suggestion(s) for s in other_improvements]

        return result

    def _format_suggestion(self, suggestion: ErrorSuggestion) -> Dict[str, Any]:
        """格式化建议"""
        return {
            'type': suggestion.error_type,
            'description': suggestion.description,
            'suggestion': suggestion.suggestion,
            'confidence': suggestion.confidence,
            'corrected_sql': suggestion.corrected_sql
        }