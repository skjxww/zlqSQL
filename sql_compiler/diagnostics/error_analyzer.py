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
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER', 'BY','INSERT'
            'INSERT INTO', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'CREATE',
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

    def _basic_syntax_check(self, sql: str) -> List[ErrorSuggestion]:
        """基础语法检查"""
        print(f"DEBUG: _basic_syntax_check 被调用")
        suggestions = []

        # 检查分号
        if not sql.strip().endswith(';'):
            print(f"DEBUG: 检测到缺少分号")
            suggestions.append(ErrorSuggestion(
                error_type="MISSING_SEMICOLON",
                description="SQL语句缺少结尾分号",
                suggestion="在SQL语句末尾添加分号 (;)",
                corrected_sql=sql.strip() + ';',
                confidence=0.9
            ))

        print(f"DEBUG: _basic_syntax_check 返回 {len(suggestions)} 个建议")
        return suggestions

    def analyze_error(self, sql: str, error: Exception) -> List[ErrorSuggestion]:
        """分析SQL错误并提供建议"""
        print(f"DEBUG: SQLErrorAnalyzer.analyze_error 被调用")
        print(f"DEBUG: SQL = {sql}")
        print(f"DEBUG: Error = {error}")

        suggestions = []
        error_str = str(error).lower()
        print(f"DEBUG: 错误字符串 = {error_str}")

        # 无条件进行基础语法检查（包括分号检测）
        basic_suggestions = self._basic_syntax_check(sql)
        print(f"DEBUG: 基础语法检查返回 {len(basic_suggestions)} 个建议")
        suggestions.extend(basic_suggestions)

        # 根据错误类型进行分析
        if "syntax error" in error_str or "unexpected token" in error_str:
            print(f"DEBUG: 检测到语法错误，进行主动语法检查")
            # 在错误检查时启用语法推测
            proactive_suggestions = self._proactive_syntax_check(sql, is_error_check=True)
            print(f"DEBUG: 主动语法检查返回 {len(proactive_suggestions)} 个建议")
            suggestions.extend(proactive_suggestions)

        if "table" in error_str and ("not found" in error_str or "doesn't exist" in error_str):
            print(f"DEBUG: 检测到表错误")
            table_suggestions = self._analyze_table_errors(sql, error)
            print(f"DEBUG: 表错误分析返回 {len(table_suggestions)} 个建议")
            suggestions.extend(table_suggestions)

        if "column" in error_str and ("not found" in error_str or "unknown" in error_str):
            print(f"DEBUG: 检测到列错误")
            column_suggestions = self._analyze_column_errors(sql, error)
            print(f"DEBUG: 列错误分析返回 {len(column_suggestions)} 个建议")
            suggestions.extend(column_suggestions)

        if "function" in error_str and ("not found" in error_str or "unknown" in error_str):
            print(f"DEBUG: 检测到函数错误")
            function_suggestions = self._analyze_function_errors(sql, error)
            print(f"DEBUG: 函数错误分析返回 {len(function_suggestions)} 个建议")
            suggestions.extend(function_suggestions)

        # 通用分析
        common_suggestions = self._analyze_common_mistakes(sql)
        print(f"DEBUG: 通用错误分析返回 {len(common_suggestions)} 个建议")
        suggestions.extend(common_suggestions)

        print(f"DEBUG: 总共收集到 {len(suggestions)} 个建议")
        # 按置信度排序
        suggestions.sort(key=lambda x: x.confidence, reverse=True)
        final_suggestions = suggestions[:5]
        print(f"DEBUG: 排序后返回前 {len(final_suggestions)} 个建议")

        return final_suggestions

    def suggest_corrections(self, sql: str) -> List[ErrorSuggestion]:
        """为SQL提供改进建议（即使没有错误）- 修复版本：只返回明确的优化建议"""
        suggestions = []
        sql_upper = sql.upper()

        # 1. 性能建议 - 明确的优化
        if 'SELECT *' in sql_upper and 'WHERE' not in sql_upper:
            suggestions.append(ErrorSuggestion(
                error_type="PERFORMANCE_TIP",
                description="性能提示",
                suggestion="考虑只选择需要的列，并添加WHERE条件来限制结果集",
                confidence=0.4
            ))

        # 2. 安全建议 - 明确的警告
        if any(dangerous in sql_upper for dangerous in ['DROP', 'DELETE FROM', 'TRUNCATE']):
            suggestions.append(ErrorSuggestion(
                error_type="SAFETY_WARNING",
                description="安全警告",
                suggestion="这个操作会修改或删除数据，请确认操作的正确性",
                confidence=0.8
            ))

        # 3. 聚合函数与非聚合列混用 - 明确的建议
        if 'GROUP BY' not in sql_upper:
            has_aggregate = any(func in sql_upper for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN('])
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if has_aggregate and select_match:
                columns_str = select_match.group(1)
                if ',' in columns_str and any(
                        func not in columns_str.upper() for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']):
                    suggestions.append(ErrorSuggestion(
                        error_type="AGGREGATE_WITHOUT_GROUP_BY",
                        description="使用聚合函数时可能需要GROUP BY",
                        suggestion="当SELECT中有聚合函数时，所有非聚合列都需要在GROUP BY中",
                        confidence=0.6
                    ))

        return suggestions

    def _proactive_syntax_check(self, sql: str, is_error_check: bool = False) -> List[ErrorSuggestion]:
        """主动进行语法检查（不需要错误即可检查）"""
        suggestions = []
        sql_upper = sql.upper()

        if is_error_check:
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

            # 3. 引号不匹配检查
            single_quotes = sql.count("'") - sql.count("\\'")  # 排除转义的引号
            double_quotes = sql.count('"') - sql.count('\\"')
            if single_quotes % 2 != 0:
                suggestions.append(ErrorSuggestion(
                    error_type="UNMATCHED_QUOTES",
                    description="单引号不匹配",
                    suggestion="检查字符串是否正确闭合单引号",
                    confidence=0.8
                ))
            if double_quotes % 2 != 0:
                suggestions.append(ErrorSuggestion(
                    error_type="UNMATCHED_QUOTES",
                    description="双引号不匹配",
                    suggestion="检查字符串是否正确闭合双引号",
                    confidence=0.8
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

            # 5. FROM后缺少表名检查 - 修复版本
            # 检查FROM后直接跟关键字或结束的情况
            if re.search(r'FROM\s*(?:WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|UNION|;|\s*$)', sql_upper.strip()):
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_TABLE_NAME",
                    description="FROM后缺少表名",
                    suggestion="在FROM关键字后指定要查询的表名",
                    confidence=0.9
                ))

            # 6. WHERE后缺少条件检查 - 修复版本
            if re.search(r'WHERE\s*(?:GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|UNION|;|\s*$)', sql_upper.strip()):
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_WHERE_CONDITION",
                    description="WHERE后缺少查询条件",
                    suggestion="在WHERE关键字后添加查询条件，例如：WHERE id = 1",
                    confidence=0.8
                ))

            # 7. INSERT INTO后缺少表名检查
            if re.search(r'INSERT\s+INTO\s*(?:VALUES|\(|;|\s*$)', sql_upper.strip()):
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_INSERT_TABLE",
                    description="INSERT INTO后缺少表名",
                    suggestion="在INSERT INTO后指定要插入数据的表名",
                    confidence=0.9
                ))

            # 8. UPDATE后缺少表名检查
            if re.search(r'UPDATE\s*(?:SET|WHERE|;|\s*$)', sql_upper.strip()):
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_UPDATE_TABLE",
                    description="UPDATE后缺少表名",
                    suggestion="在UPDATE后指定要更新的表名",
                    confidence=0.9
                ))

            # 9. DELETE FROM后缺少表名检查
            if re.search(r'DELETE\s+FROM\s*(?:WHERE|;|\s*$)', sql_upper.strip()):
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_DELETE_TABLE",
                    description="DELETE FROM后缺少表名",
                    suggestion="在DELETE FROM后指定要删除数据的表名",
                    confidence=0.9
                ))

            # 10. INSERT语句缺少VALUES检查 - 修复版本
            if re.search(r'INSERT\s+INTO\s+\w+', sql_upper) and 'VALUES' not in sql_upper and 'SELECT' not in sql_upper:
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_VALUES",
                    description="INSERT语句缺少VALUES子句",
                    suggestion="INSERT语句应该包含VALUES子句或SELECT子句",
                    confidence=0.9
                ))

            # 11. UPDATE语句缺少SET检查 - 修复版本
            if re.search(r'UPDATE\s+\w+', sql_upper) and 'SET' not in sql_upper:
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_SET",
                    description="UPDATE语句缺少SET子句",
                    suggestion="UPDATE语句必须包含SET子句来指定要更新的列",
                    confidence=0.9
                ))

            # 12. 比较运算符后缺少值检查 - 修复版本
            # 查找 column = (后面直接跟关键字或结束)
            comparison_patterns = [
                r'=\s*(?:WHERE|AND|OR|GROUP|ORDER|HAVING|LIMIT|;|\s*$)',
                r'<>\s*(?:WHERE|AND|OR|GROUP|ORDER|HAVING|LIMIT|;|\s*$)',
                r'!=\s*(?:WHERE|AND|OR|GROUP|ORDER|HAVING|LIMIT|;|\s*$)',
                r'<=\s*(?:WHERE|AND|OR|GROUP|ORDER|HAVING|LIMIT|;|\s*$)',
                r'>=\s*(?:WHERE|AND|OR|GROUP|ORDER|HAVING|LIMIT|;|\s*$)',
                r'<\s*(?:WHERE|AND|OR|GROUP|ORDER|HAVING|LIMIT|;|\s*$)',
                r'>\s*(?:WHERE|AND|OR|GROUP|ORDER|HAVING|LIMIT|;|\s*$)'
            ]

            for pattern in comparison_patterns:
                if re.search(pattern, sql_upper.strip()):
                    suggestions.append(ErrorSuggestion(
                        error_type="MISSING_COMPARISON_VALUE",
                        description="比较运算符后缺少值",
                        suggestion="比较运算符后应该跟一个值，例如：column = 'value'",
                        confidence=0.8
                    ))
                    break  # 只报告一次

            # 13. ORDER BY后缺少列名检查 - 修复版本
            if re.search(r'ORDER\s+BY\s*(?:;|\s*$)', sql_upper.strip()):
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_ORDER_COLUMN",
                    description="ORDER BY后缺少排序列名",
                    suggestion="ORDER BY后应该指定排序的列名，例如：ORDER BY column_name",
                    confidence=0.8
                ))

            # 14. GROUP BY后缺少列名检查 - 修复版本
            if re.search(r'GROUP\s+BY\s*(?:HAVING|ORDER|LIMIT|;|\s*$)', sql_upper.strip()):
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_GROUP_COLUMN",
                    description="GROUP BY后缺少分组列名",
                    suggestion="GROUP BY后应该指定分组的列名，例如：GROUP BY column_name",
                    confidence=0.8
                ))

            # 15. HAVING后缺少条件检查
            if re.search(r'HAVING\s*(?:ORDER\s+BY|LIMIT|;|\s*$)', sql_upper.strip()):
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_HAVING_CONDITION",
                    description="HAVING后缺少条件",
                    suggestion="HAVING后应该指定条件，例如：HAVING COUNT(*) > 1",
                    confidence=0.8
                ))

            # 16. SET后缺少赋值检查
            if re.search(r'SET\s*(?:WHERE|;|\s*$)', sql_upper.strip()):
                suggestions.append(ErrorSuggestion(
                    error_type="MISSING_SET_ASSIGNMENT",
                    description="SET后缺少赋值语句",
                    suggestion="SET后应该指定要更新的列和值，例如：SET column = value",
                    confidence=0.8
                ))

            # 17. JOIN后缺少表名检查
            join_patterns = [
                r'(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s*(?:ON|WHERE|GROUP|ORDER|;|\s*$)'
            ]

            for pattern in join_patterns:
                if re.search(pattern, sql_upper.strip()):
                    suggestions.append(ErrorSuggestion(
                        error_type="MISSING_JOIN_TABLE",
                        description="JOIN后缺少表名",
                        suggestion="JOIN后应该指定要连接的表名",
                        confidence=0.8
                    ))
                    break

            # 18. 检查单独的关键字（没有跟任何内容）
            standalone_keywords = ['SELECT', 'FROM', 'WHERE', 'UPDATE', 'INSERT', 'DELETE']
            for keyword in standalone_keywords:
                # 检查关键字后面直接是结束或分号
                if re.search(rf'\b{keyword}\s*(?:;|\s*$)', sql_upper.strip()):
                    suggestions.append(ErrorSuggestion(
                        error_type="INCOMPLETE_STATEMENT",
                        description=f"{keyword}语句不完整",
                        suggestion=f"{keyword}后应该跟相应的内容",
                        confidence=0.9
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
        """分析SQL并提供建议"""
        print(f"DEBUG: SmartSQLCorrector.analyze_and_suggest 被调用")
        print(f"DEBUG: SQL = {sql}")
        print(f"DEBUG: Error = {error}")

        result = {
            'original_sql': sql,
            'has_error': error is not None,
            'error_message': str(error) if error else None,
            'suggestions': [],
            'corrected_sql_options': [],
            'improvement_tips': []
        }

        if error:
            print(f"DEBUG: 有错误，开始错误分析")
            # 有错误时进行错误分析
            suggestions = self.analyzer.analyze_error(sql, error)
            print(f"DEBUG: 分析器返回 {len(suggestions)} 个建议")

            result['suggestions'] = [self._format_suggestion(s) for s in suggestions]

            # 提供可能的修正版本
            corrected_options = [s for s in suggestions if s.corrected_sql]
            print(f"DEBUG: 有 {len(corrected_options)} 个修正选项")
            result['corrected_sql_options'] = [
                {
                    'sql': s.corrected_sql,
                    'description': s.suggestion,
                    'confidence': s.confidence
                }
                for s in corrected_options[:3]
            ]
        else:
            print(f"DEBUG: 没有错误，检查改进建议和完整语法")

            # 1. 检查基础语法问题（即使没有错误）
            basic_syntax_issues = self.analyzer._basic_syntax_check(sql)
            print(f"DEBUG: 基础语法检查返回 {len(basic_syntax_issues)} 个建议")

            # 2. 进行主动语法检查（包括括号、缺少列名等）
            proactive_issues = self.analyzer._proactive_syntax_check(sql, is_error_check=True)
            print(f"DEBUG: 主动语法检查返回 {len(proactive_issues)} 个建议")

            # 3. 检查改进建议
            improvements = self.analyzer.suggest_corrections(sql)
            print(f"DEBUG: 改进建议分析器返回 {len(improvements)} 个建议")

            # 合并所有语法问题
            all_syntax_issues = basic_syntax_issues + proactive_issues

            # 去重（避免重复的分号检查）
            unique_issues = []
            seen_types = set()
            for issue in all_syntax_issues:
                if issue.error_type not in seen_types:
                    unique_issues.append(issue)
                    seen_types.add(issue.error_type)

            print(f"DEBUG: 去重后有 {len(unique_issues)} 个语法问题")

            # 如果有语法问题，将它们作为建议
            if unique_issues:
                result['suggestions'] = [self._format_suggestion(s) for s in unique_issues]

                # 提供修正选项
                corrected_options = [s for s in unique_issues if s.corrected_sql]
                result['corrected_sql_options'] = [
                    {
                        'sql': s.corrected_sql,
                        'description': s.suggestion,
                        'confidence': s.confidence
                    }
                    for s in corrected_options[:3]
                ]

            # 过滤改进建议
            optimization_tips = [s for s in improvements if s.error_type in [
                'PERFORMANCE_TIP', 'SAFETY_WARNING', 'AGGREGATE_WITHOUT_GROUP_BY'
            ]]
            print(f"DEBUG: 过滤后有 {len(optimization_tips)} 个优化建议")

            result['improvement_tips'] = [self._format_suggestion(s) for s in optimization_tips]

        print(f"DEBUG: 最终结果: {result}")
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