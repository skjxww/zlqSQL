"""
智能错误诊断器
提供智能纠错提示和错误分析
"""

import re
from typing import List, Dict, Any, Optional
from difflib import SequenceMatcher, get_close_matches
from sql_compiler.exceptions.compiler_errors import CompilerError


class ErrorDiagnostics:
    """错误诊断器"""

    def __init__(self, catalog_manager=None):
        self.catalog = catalog_manager
        self.sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
            'DELETE', 'CREATE', 'TABLE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'ON',
            'GROUP', 'BY', 'ORDER', 'HAVING', 'ASC', 'DESC', 'COUNT', 'SUM', 'AVG',
            'MAX', 'MIN', 'AND', 'OR', 'NOT', 'IN', 'INT', 'VARCHAR', 'CHAR'
        }

        # 常见错误模式
        self.common_patterns = {
            r'SELECT\s*\*\s*FROM\s*$': "SELECT * FROM 后面缺少表名",
            r'INSERT\s+INTO\s+\w+\s*$': "INSERT INTO 语句缺少 VALUES 子句",
            r'UPDATE\s+\w+\s*$': "UPDATE 语句缺少 SET 子句",
            r'DELETE\s+FROM\s*$': "DELETE FROM 后面缺少表名",
            r'WHERE\s*$': "WHERE 子句缺少条件",
            r'GROUP\s+BY\s*$': "GROUP BY 后面缺少列名",
            r'ORDER\s+BY\s*$': "ORDER BY 后面缺少列名",
        }

    def diagnose_error(self, error: CompilerError, sql: str) -> Dict[str, Any]:
        """诊断错误并提供修复建议"""
        diagnosis = {
            'original_error': str(error),
            'error_type': self._classify_error(error),
            'severity': 'ERROR',
            'suggestions': [],
            'corrected_sql': None,
            'explanation': '',
            'examples': []
        }

        # 根据错误类型提供不同的诊断
        if 'lexical' in error.__class__.__name__.lower():
            diagnosis.update(self._diagnose_lexical_error(error, sql))
        elif 'syntax' in error.__class__.__name__.lower():
            diagnosis.update(self._diagnose_syntax_error(error, sql))
        elif 'semantic' in error.__class__.__name__.lower():
            diagnosis.update(self._diagnose_semantic_error(error, sql))
        else:
            diagnosis.update(self._diagnose_general_error(error, sql))

        return diagnosis

    def _classify_error(self, error: CompilerError) -> str:
        """分类错误类型"""
        error_class = error.__class__.__name__.lower()
        if 'lexical' in error_class:
            return 'LEXICAL'
        elif 'syntax' in error_class:
            return 'SYNTAX'
        elif 'semantic' in error_class:
            return 'SEMANTIC'
        else:
            return 'GENERAL'

    def _diagnose_lexical_error(self, error: CompilerError, sql: str) -> Dict[str, Any]:
        """诊断词法错误"""
        result = {}
        error_msg = str(error)

        # 未识别字符错误
        if "未识别的字符" in error_msg:
            char_match = re.search(r"未识别的字符: '(.)'", error_msg)
            if char_match:
                char = char_match.group(1)
                result.update(self._handle_unrecognized_char(char, sql, error))

        return result

    def _diagnose_syntax_error(self, error: CompilerError, sql: str) -> Dict[str, Any]:
        """诊断语法错误"""
        result = {}
        error_msg = str(error)

        # 期望token但遇到其他token
        if "期望" in error_msg and "但遇到" in error_msg:
            result.update(self._handle_unexpected_token(error_msg, sql))

        # 检查常见语法模式错误
        for pattern, suggestion in self.common_patterns.items():
            if re.search(pattern, sql.upper()):
                result['suggestions'].append(suggestion)
                result['corrected_sql'] = self._suggest_pattern_fix(pattern, sql)
                break

        return result

    def _diagnose_semantic_error(self, error: CompilerError, sql: str) -> Dict[str, Any]:
        """诊断语义错误"""
        result = {}
        error_msg = str(error)

        # 表不存在错误
        if "不存在" in error_msg and "表" in error_msg:
            result.update(self._handle_missing_table(error_msg, sql))

        # 列不存在错误
        elif "不存在" in error_msg and ("列" in error_msg or "column" in error_msg.lower()):
            result.update(self._handle_missing_column(error_msg, sql))

        # 类型不匹配错误
        elif "类型" in error_msg and ("期望" in error_msg or "不匹配" in error_msg):
            result.update(self._handle_type_mismatch(error_msg, sql))

        # GROUP BY错误
        elif "GROUP BY" in error_msg or "聚合" in error_msg:
            result.update(self._handle_groupby_error(error_msg, sql))

        return result

    def _diagnose_general_error(self, error: CompilerError, sql: str) -> Dict[str, Any]:
        """诊断一般错误"""
        return {
            'suggestions': ['检查SQL语法是否正确', '确认表名和列名存在'],
            'explanation': '发生了未分类的错误，请检查SQL语句'
        }

    def _handle_unrecognized_char(self, char: str, sql: str, error: CompilerError) -> Dict[str, Any]:
        """处理未识别字符错误"""
        suggestions = []
        corrected_sql = sql

        # 常见字符错误
        char_fixes = {
            '"': "使用单引号 ' 而不是双引号 \"",
            '`': "使用标准SQL标识符，避免使用反引号",
            '？': "使用英文问号 ? 而不是中文问号 ？",
            '，': "使用英文逗号 , 而不是中文逗号 ，",
            '（': "使用英文括号 ( 而不是中文括号 （",
            '）': "使用英文括号 ) 而不是中文括号 ）",
        }

        if char in char_fixes:
            suggestions.append(char_fixes[char])
            # 尝试修复
            if char == '"':
                corrected_sql = sql.replace('"', "'")
            elif char in '，（）？':
                replace_map = {'，': ',', '（': '(', '）': ')', '？': '?'}
                corrected_sql = sql.replace(char, replace_map.get(char, char))
        else:
            suggestions.append(f"移除不支持的字符 '{char}'")
            corrected_sql = sql.replace(char, '')

        return {
            'suggestions': suggestions,
            'corrected_sql': corrected_sql,
            'explanation': f"字符 '{char}' 不是有效的SQL字符",
            'examples': ["正确: SELECT * FROM users;", "错误: SELECT * FROM users；"]
        }

    def _handle_unexpected_token(self, error_msg: str, sql: str) -> Dict[str, Any]:
        """处理意外token错误"""
        # 解析期望的token和实际的token
        expected_match = re.search(r"期望.*?'([^']*)'", error_msg)
        actual_match = re.search(r"但遇到.*?'([^']*)'", error_msg)

        suggestions = []
        corrected_sql = sql

        if expected_match and actual_match:
            expected = expected_match.group(1)
            actual = actual_match.group(1)

            # 检查是否是关键字拼写错误
            if actual.upper() not in self.sql_keywords:
                close_keywords = get_close_matches(actual.upper(), self.sql_keywords, n=3, cutoff=0.6)
                if close_keywords:
                    suggestions.append(f"'{actual}' 可能是 '{close_keywords[0]}' 的拼写错误")
                    corrected_sql = sql.replace(actual, close_keywords[0])
            # 常见的token替换建议
            token_fixes = {
                ';': {
                    'FROM': "在FROM后添加表名，然后使用分号",
                    'SET': "在SET后添加赋值表达式，然后使用分号",
                    'VALUES': "在VALUES后添加值列表，然后使用分号"
                }
            }

            if expected in token_fixes:
                if actual in token_fixes[expected]:
                    suggestions.append(token_fixes[expected][actual])

        return {
            'suggestions': suggestions,
            'corrected_sql': corrected_sql,
            'explanation': f"语法不符合期望，期望 '{expected}' 但遇到 '{actual}'",
            'examples': self._get_syntax_examples(expected)
        }

    def _handle_missing_table(self, error_msg: str, sql: str) -> Dict[str, Any]:
        """处理表不存在错误"""
        # 提取表名
        table_match = re.search(r"表\s*'([^']*)'.*不存在", error_msg)
        if not table_match:
            return {}

        missing_table = table_match.group(1)
        suggestions = []
        corrected_sql = None

        if self.catalog:
            # 获取所有存在的表
            existing_tables = list(self.catalog.get_all_tables().keys())

            # 查找相似的表名
            similar_tables = get_close_matches(missing_table, existing_tables, n=3, cutoff=0.6)

            if similar_tables:
                suggestions.append(f"表名 '{missing_table}' 不存在，您是否想要使用:")
                for table in similar_tables:
                    suggestions.append(f"  • {table}")

                # 提供修正建议
                best_match = similar_tables[0]
                corrected_sql = sql.replace(missing_table, best_match)
                suggestions.append(f"建议修正: 将 '{missing_table}' 改为 '{best_match}'")
            else:
                suggestions.append(f"表 '{missing_table}' 不存在")
                if existing_tables:
                    suggestions.append("当前数据库中的表有:")
                    for table in existing_tables[:5]:  # 只显示前5个
                        suggestions.append(f"  • {table}")
                    if len(existing_tables) > 5:
                        suggestions.append(f"  ... 还有 {len(existing_tables) - 5} 个表")
        else:
            suggestions.append(f"表 '{missing_table}' 不存在，请先创建该表")

        return {
            'suggestions': suggestions,
            'corrected_sql': corrected_sql,
            'explanation': f"引用了不存在的表 '{missing_table}'",
            'examples': [
                f"创建表: CREATE TABLE {missing_table} (id INT, name VARCHAR(50));",
                "或检查表名拼写是否正确"
            ]
        }

    def _handle_missing_column(self, error_msg: str, sql: str) -> Dict[str, Any]:
        """处理列不存在错误"""
        # 提取列名
        column_match = re.search(r"列.*?'([^']*)'.*不存在", error_msg) or \
                       re.search(r"无效的列引用:\s*([^\s]+)", error_msg)

        if not column_match:
            return {}

        missing_column = column_match.group(1)
        suggestions = []
        corrected_sql = None

        # 提取可能的表名
        table_name = self._extract_table_from_sql(sql)

        if self.catalog and table_name:
            table_info = self.catalog.get_table(table_name)
            if table_info:
                existing_columns = [col['name'] for col in table_info['columns']]

                # 查找相似的列名
                similar_columns = get_close_matches(missing_column, existing_columns, n=3, cutoff=0.6)

                if similar_columns:
                    suggestions.append(f"列 '{missing_column}' 不存在，您是否想要使用:")
                    for col in similar_columns:
                        suggestions.append(f"  • {col}")

                    best_match = similar_columns[0]
                    corrected_sql = sql.replace(missing_column, best_match)
                    suggestions.append(f"建议修正: 将 '{missing_column}' 改为 '{best_match}'")
                else:
                    suggestions.append(f"列 '{missing_column}' 不存在")
                    suggestions.append(f"表 '{table_name}' 中的列有:")
                    for col in existing_columns:
                        suggestions.append(f"  • {col}")
        else:
            suggestions.append(f"列 '{missing_column}' 不存在，请检查列名拼写")

        return {
            'suggestions': suggestions,
            'corrected_sql': corrected_sql,
            'explanation': f"引用了不存在的列 '{missing_column}'",
            'examples': [
                f"检查表结构: DESCRIBE {table_name};",
                "确认列名拼写正确"
            ]
        }

    def _handle_type_mismatch(self, error_msg: str, sql: str) -> Dict[str, Any]:
        """处理类型不匹配错误"""
        suggestions = []
        corrected_sql = None

        # 解析类型不匹配信息
        type_match = re.search(r"期望类型\s*'([^']*)'.*得到\s*'([^']*)'", error_msg)
        if type_match:
            expected_type = type_match.group(1)
            actual_type = type_match.group(2)

            suggestions.append(f"类型不匹配: 期望 {expected_type}，但得到 {actual_type}")

            # 提供类型转换建议
            if expected_type == 'INT' and actual_type == 'VARCHAR':
                suggestions.append("如果是数字字符串，请移除引号")
                suggestions.append("例如: 将 '123' 改为 123")
                corrected_sql = re.sub(r"'(\d+)'", r'\1', sql)

            elif expected_type.startswith('VARCHAR') and actual_type == 'INT':
                suggestions.append("如果是字符串，请添加引号")
                suggestions.append("例如: 将 123 改为 '123'")
                # 简单的数字到字符串转换
                corrected_sql = re.sub(r'\b(\d+)\b', r"'\1'", sql)

        return {
            'suggestions': suggestions,
            'corrected_sql': corrected_sql,
            'explanation': '数据类型不匹配',
            'examples': [
                "正确: INSERT INTO users (id, name) VALUES (1, 'Alice');",
                "错误: INSERT INTO users (id, name) VALUES ('1', 123);"
            ]
        }

    def _handle_groupby_error(self, error_msg: str, sql: str) -> Dict[str, Any]:
        """处理GROUP BY相关错误"""
        suggestions = []
        corrected_sql = None

        if "必须使用GROUP BY" in error_msg:
            # 提取非聚合列
            non_agg_match = re.search(r"非聚合列:\s*([^}]+)", error_msg)
            if non_agg_match:
                non_agg_columns = non_agg_match.group(1).strip()
                suggestions.append(f"查询包含聚合函数和非聚合列，需要添加GROUP BY子句")
                suggestions.append(f"添加: GROUP BY {non_agg_columns}")

                # 尝试修正SQL
                if not re.search(r'GROUP\s+BY', sql, re.IGNORECASE):
                    # 在适当位置添加GROUP BY
                    corrected_sql = self._add_group_by_clause(sql, non_agg_columns)

        elif "必须出现在GROUP BY子句中" in error_msg:
            column_match = re.search(r"列\s*'([^']*)'", error_msg)
            if column_match:
                column = column_match.group(1)
                suggestions.append(f"列 '{column}' 必须添加到GROUP BY子句中")
                corrected_sql = self._add_column_to_group_by(sql, column)

        return {
            'suggestions': suggestions,
            'corrected_sql': corrected_sql,
            'explanation': 'GROUP BY 规则违反',
            'examples': [
                "正确: SELECT dept, COUNT(*) FROM employees GROUP BY dept;",
                "错误: SELECT dept, COUNT(*) FROM employees;"
            ]
        }

    def _extract_table_from_sql(self, sql: str) -> str:
        """从SQL中提取表名"""
        # 简单的表名提取
        from_match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
        if from_match:
            return from_match.group(1)

        into_match = re.search(r'INTO\s+(\w+)', sql, re.IGNORECASE)
        if into_match:
            return into_match.group(1)

        update_match = re.search(r'UPDATE\s+(\w+)', sql, re.IGNORECASE)
        if update_match:
            return update_match.group(1)

        return None

    def _get_syntax_examples(self, expected_token: str) -> List[str]:
        """获取语法示例"""
        examples = {
            ';': [
                "完整语句: SELECT * FROM users;",
                "完整语句: INSERT INTO users VALUES (1, 'Alice');"
            ],
            'FROM': [
                "正确: SELECT * FROM users;",
                "错误: SELECT *;"
            ],
            'SET': [
                "正确: UPDATE users SET name = 'Alice';",
                "错误: UPDATE users;"
            ]
        }
        return examples.get(expected_token, ["检查SQL语法手册"])

    def _suggest_pattern_fix(self, pattern: str, sql: str) -> str:
        """根据模式建议修复"""
        if "FROM.*$" in pattern:
            return sql + " table_name"
        elif "VALUES.*$" in pattern:
            return sql + " VALUES (value1, value2)"
        elif "SET.*$" in pattern:
            return sql + " SET column = value"
        return sql

    def _add_group_by_clause(self, sql: str, columns: str) -> str:
        """添加GROUP BY子句"""
        # 查找插入点（在WHERE之后，ORDER BY之前，或语句末尾）
        order_by_match = re.search(r'\s+ORDER\s+BY', sql, re.IGNORECASE)
        having_match = re.search(r'\s+HAVING', sql, re.IGNORECASE)

        if order_by_match:
            insert_pos = order_by_match.start()
        elif having_match:
            insert_pos = having_match.start()
        else:
            # 在分号前插入
            semicolon_pos = sql.rfind(';')
            insert_pos = semicolon_pos if semicolon_pos != -1 else len(sql)

        return sql[:insert_pos] + f" GROUP BY {columns}" + sql[insert_pos:]

    def _add_column_to_group_by(self, sql: str, column: str) -> str:
        """向GROUP BY子句添加列"""
        group_by_match = re.search(r'GROUP\s+BY\s+([^;]+)', sql, re.IGNORECASE)
        if group_by_match:
            existing_columns = group_by_match.group(1).strip()
            new_columns = existing_columns + f", {column}"
            return sql.replace(group_by_match.group(0), f"GROUP BY {new_columns}")
        return sql


class SmartErrorReporter:
    """智能错误报告器"""

    def __init__(self, catalog_manager=None):
        self.diagnostics = ErrorDiagnostics(catalog_manager)

    def report_error(self, error: CompilerError, sql: str):
        """报告错误并提供智能诊断"""
        print(f"\n{'❌' * 20}")
        print("SQL编译错误诊断")
        print("❌" * 20)

        diagnosis = self.diagnostics.diagnose_error(error, sql)

        # 显示基本错误信息
        print(f"\n🔍 错误类型: {diagnosis['error_type']}")
        print(f"📍 原始错误: {diagnosis['original_error']}")

        # 显示错误说明
        if diagnosis['explanation']:
            print(f"\n💡 问题说明: {diagnosis['explanation']}")

        # 显示建议
        if diagnosis['suggestions']:
            print(f"\n🛠️  修复建议:")
            for i, suggestion in enumerate(diagnosis['suggestions'], 1):
                if suggestion.startswith('  •'):
                    print(f"    {suggestion}")
                else:
                    print(f"   {i}. {suggestion}")

        # 显示修正后的SQL
        if diagnosis['corrected_sql'] and diagnosis['corrected_sql'] != sql:
            print(f"\n✨ 建议的修正:")
            print(f"   原始: {sql.strip()}")
            print(f"   修正: {diagnosis['corrected_sql'].strip()}")

        # 显示示例
        if diagnosis['examples']:
            print(f"\n📚 参考示例:")
            for example in diagnosis['examples']:
                print(f"   • {example}")

        print("❌" * 50)
