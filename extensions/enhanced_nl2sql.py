import re
import json
import requests
from typing import Dict, List, Optional
from catalog.catalog_manager import CatalogManager


class EnhancedNL2SQL:
    """增强的自然语言转SQL"""

    def __init__(self, catalog: CatalogManager, api_key: str = None, api_type: str = "deepseek"):
        self.catalog = catalog
        self.api_key = api_key
        self.api_type = api_type
        self.query_patterns = self._load_query_patterns()
        self.entity_mapping = self._build_entity_mapping()

        # DeepSeek API 配置
        if api_type == "deepseek":
            self.api_url = "https://api.deepseek.com/chat/completions"
            self.model_name = "deepseek-chat"
        elif api_type == "openai":
            self.api_url = "https://api.openai.com/v1/chat/completions"
            self.model_name = "gpt-3.5-turbo"

    def translate(self, natural_query: str) -> Dict:
        """将自然语言查询转换为SQL"""
        try:
            # 1. 预处理和实体识别
            processed_query = self._preprocess_query(natural_query)
            entities = self._extract_entities(processed_query)

            # 2. 模式匹配（快速路径）
            pattern_result = self._pattern_matching(processed_query, entities)
            if pattern_result['confidence'] > 0.8:
                return self._enhance_result(pattern_result, natural_query)

            # 3. 使用 DeepSeek API 处理复杂查询
            if self.api_key:
                ai_result = self._translate_with_deepseek(natural_query, entities)
                return self._enhance_result(ai_result, natural_query)
            else:
                return self._enhance_result(pattern_result, natural_query)

        except Exception as e:
            return {
                'sql': '',
                'confidence': 0.0,
                'explanation': f'转换失败: {str(e)}',
                'suggestions': ['请检查查询语句的表达方式'],
                'entities': {},
                'error': str(e)
            }

    def _translate_with_deepseek(self, natural_query: str, entities: Dict) -> Dict:
        """使用 DeepSeek API 翻译"""
        try:
            schema_context = self._get_schema_context()

            # 构建更详细的提示词
            prompt = self._build_detailed_prompt(natural_query, entities, schema_context)

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }

            data = {
                "model": self.model_name,
                "messages": [
                    {
                        "role": "system",
                        "content": "你是一个专业的SQL专家，专门将中文自然语言转换为准确的SQL语句。你必须根据提供的数据库结构生成语法正确的SQL。"
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": 0.1,
                "max_tokens": 1000,
                "stream": False
            }

            response = requests.post(self.api_url, headers=headers, json=data, timeout=30)

            if response.status_code == 200:
                result_data = response.json()
                result_text = result_data['choices'][0]['message']['content']

                # 解析结果
                parsed_result = self._parse_api_response(result_text)
                parsed_result['method'] = 'deepseek_api'
                return parsed_result

            else:
                return {
                    'sql': '',
                    'confidence': 0.0,
                    'explanation': f'API请求失败: {response.status_code}',
                    'error': f'HTTP {response.status_code}: {response.text}',
                    'method': 'deepseek_api'
                }

        except requests.exceptions.Timeout:
            return {
                'sql': '',
                'confidence': 0.0,
                'explanation': 'API请求超时',
                'error': 'Request timeout',
                'method': 'deepseek_api'
            }
        except Exception as e:
            return {
                'sql': '',
                'confidence': 0.0,
                'explanation': f'DeepSeek API处理失败: {str(e)}',
                'error': str(e),
                'method': 'deepseek_api'
            }

    def _build_detailed_prompt(self, natural_query: str, entities: Dict, schema_context: Dict) -> str:
        """构建详细的提示词"""
        prompt = f"""
请将以下中文自然语言查询转换为SQL语句。

数据库结构信息:
{json.dumps(schema_context, indent=2, ensure_ascii=False)}

已识别的实体信息:
- 表名: {entities.get('tables', [])}
- 列名: {[f"{e.get('table', 'unknown')}.{e.get('column', 'unknown')}" for e in entities.get('columns', [])]}
- 聚合函数: {entities.get('aggregates', [])}
- 数值条件: {entities.get('values', [])}

自然语言查询: "{natural_query}"

请按以下JSON格式返回结果（确保返回有效的JSON）:
{{
    "sql": "生成的SQL语句（必须以分号结尾）",
    "explanation": "详细的中文解释",
    "confidence": 0.9,
    "reasoning": "推理过程说明",
    "tables_used": ["使用的表名列表"],
    "potential_issues": ["可能的问题或注意事项"]
}}

重要要求:
1. SQL语句必须语法正确且符合标准SQL语法
2. 只使用提供的表名和列名
3. 如果查询不明确，选择最合理的解释
4. 确保返回的是有效的JSON格式
5. SQL语句必须以分号结尾
"""
        return prompt

    def _parse_api_response(self, response_text: str) -> Dict:
        """解析API响应"""
        try:
            # 尝试直接解析JSON
            if response_text.strip().startswith('{'):
                result = json.loads(response_text)

                # 验证必要字段
                if 'sql' in result:
                    # 确保SQL以分号结尾
                    sql = result['sql'].strip()
                    if not sql.endswith(';'):
                        sql += ';'
                    result['sql'] = sql

                    # 设置默认值
                    result.setdefault('confidence', 0.8)
                    result.setdefault('explanation', '由AI生成的SQL查询')
                    result.setdefault('reasoning', '基于自然语言理解生成')

                    return result

            # 如果不是JSON，尝试提取SQL
            sql_extracted = self._extract_sql_from_text(response_text)
            if sql_extracted:
                return {
                    'sql': sql_extracted,
                    'confidence': 0.7,
                    'explanation': '从AI响应中提取的SQL语句',
                    'reasoning': response_text[:200] + '...' if len(response_text) > 200 else response_text
                }

            # 都失败了，返回原文本作为解释
            return {
                'sql': '',
                'confidence': 0.0,
                'explanation': f'无法解析AI响应: {response_text[:200]}...',
                'reasoning': response_text
            }

        except json.JSONDecodeError as e:
            # JSON解析失败，尝试提取SQL
            sql_extracted = self._extract_sql_from_text(response_text)
            return {
                'sql': sql_extracted or '',
                'confidence': 0.6 if sql_extracted else 0.0,
                'explanation': f'JSON解析失败，提取到的SQL: {sql_extracted or "无"}',
                'error': f'JSON解析错误: {str(e)}',
                'raw_response': response_text
            }

    def _extract_sql_from_text(self, text: str) -> Optional[str]:
        """从文本中提取SQL语句"""
        import re

        # 查找SQL代码块
        patterns = [
            r'```sql\s*(.*?)\s*```',  # ```sql 代码块
            r'```\s*(SELECT.*?;)\s*```',  # 普通代码块中的SQL
            r'"sql":\s*"([^"]*)"',  # JSON中的sql字段
            r'(SELECT.*?;)',  # 直接的SQL语句
            r'(INSERT.*?;)',
            r'(UPDATE.*?;)',
            r'(DELETE.*?;)',
            r'(CREATE.*?;)',
            r'(DROP.*?;)'
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                sql = match.group(1).strip()
                if not sql.endswith(';'):
                    sql += ';'
                return sql

        return None

    def _preprocess_query(self, query: str) -> str:
        """查询预处理"""
        query = query.strip().lower()

        # 标准化常见表达
        replacements = {
            r'显示|展示|查看|列出|给我看': 'show',
            r'所有的?|全部|全体': 'all',
            r'员工|雇员|职员': 'employee',
            r'部门|科室': 'department',
            r'薪资|工资|薪水|收入': 'salary',
            r'姓名|名字|名称': 'name',
            r'大于|超过|高于|多于': 'greater than',
            r'小于|低于|少于|不到': 'less than',
            r'等于|是|为': 'equals',
            r'平均|平均值|均值': 'average',
            r'总数|数量|个数|计数': 'count',
            r'最大|最高|最多': 'max',
            r'最小|最低|最少': 'min',
            r'求和|总和|合计': 'sum',
            r'按照|根据|依据': 'by',
            r'分组|分类': 'group',
            r'排序|排列': 'order'
        }

        for pattern, replacement in replacements.items():
            query = re.sub(pattern, replacement, query)

        return query

    def _extract_entities(self, query: str) -> Dict:
        """实体提取"""
        entities = {
            'tables': [],
            'columns': [],
            'conditions': [],
            'aggregates': [],
            'values': []
        }

        # 提取表名（支持中英文）
        all_tables = self.catalog.get_all_tables()
        for table_name in all_tables.keys():
            # 检查英文表名
            if table_name.lower() in query:
                entities['tables'].append(table_name)
            # 检查单数形式
            elif table_name.lower().rstrip('s') in query:
                entities['tables'].append(table_name)
            # 检查关键词匹配
            elif ('employee' in query and 'employee' in table_name.lower()) or \
                    ('department' in query and 'department' in table_name.lower()):
                entities['tables'].append(table_name)

        # 提取列名
        for table_name, table_info in all_tables.items():
            columns = [col['name'] for col in table_info.get('columns', [])]
            for col in columns:
                if col.lower() in query:
                    entities['columns'].append({'table': table_name, 'column': col})

        # 提取数值
        import re
        numbers = re.findall(r'\b\d+(?:\.\d+)?\b', query)
        entities['values'].extend(numbers)

        # 提取聚合函数
        agg_keywords = {
            'count': 'COUNT',
            'average': 'AVG',
            'avg': 'AVG',
            'sum': 'SUM',
            'max': 'MAX',
            'min': 'MIN'
        }

        for keyword, func in agg_keywords.items():
            if keyword in query:
                entities['aggregates'].append(func)

        return entities

    def _pattern_matching(self, query: str, entities: Dict) -> Dict:
        """增强的模式匹配"""
        # 简单查询模式
        if any(word in query for word in ['show all', 'list all', 'select all']):
            if entities['tables']:
                table = entities['tables'][0]
                return {
                    'sql': f"SELECT * FROM {table};",
                    'confidence': 0.9,
                    'explanation': f'查询{table}表的所有记录',
                    'pattern': 'select_all'
                }

        # 条件查询模式
        if entities['conditions'] or any(op in query for op in ['greater than', 'less than', 'equals']):
            return self._build_conditional_query(query, entities)

        # 聚合查询模式
        if entities['aggregates']:
            return self._build_aggregate_query(query, entities)

        # 分组查询模式
        if 'group by' in query or 'by department' in query:
            return self._build_group_by_query(query, entities)

        return {'sql': '', 'confidence': 0.0, 'pattern': 'unknown'}

    def _build_conditional_query(self, query: str, entities: Dict) -> Dict:
        """构建条件查询"""
        if not entities['tables']:
            return {'sql': '', 'confidence': 0.0}

        table = entities['tables'][0]
        sql_parts = [f"SELECT * FROM {table}"]

        conditions = []

        # 解析数值条件
        for value in entities['values']:
            if 'greater than' in query:
                # 猜测是salary条件
                if any(col['column'] == 'salary' for col in entities['columns']):
                    conditions.append(f"salary > {value}")
                else:
                    # 使用第一个数值列
                    numeric_cols = self._get_numeric_columns(table)
                    if numeric_cols:
                        conditions.append(f"{numeric_cols[0]} > {value}")

            elif 'less than' in query:
                numeric_cols = self._get_numeric_columns(table)
                if numeric_cols:
                    conditions.append(f"{numeric_cols[0]} < {value}")

        if conditions:
            sql_parts.append(f"WHERE {' AND '.join(conditions)}")

        sql = ' '.join(sql_parts) + ';'

        return {
            'sql': sql,
            'confidence': 0.8,
            'explanation': f'从{table}表中查询满足条件的记录',
            'pattern': 'conditional'
        }

    def _build_aggregate_query(self, query: str, entities: Dict) -> Dict:
        """构建聚合查询"""
        if not entities['tables'] or not entities['aggregates']:
            return {'sql': '', 'confidence': 0.0}

        table = entities['tables'][0]
        aggregate = entities['aggregates'][0]

        if aggregate == 'COUNT':
            sql = f"SELECT COUNT(*) as count FROM {table};"
            explanation = f"统计{table}表的记录数"
        else:
            # 找到合适的列
            target_column = None
            if aggregate in ['AVG', 'SUM', 'MAX', 'MIN']:
                numeric_cols = self._get_numeric_columns(table)
                if numeric_cols:
                    target_column = numeric_cols[0]

            if target_column:
                sql = f"SELECT {aggregate}({target_column}) as result FROM {table};"
                explanation = f"计算{table}表中{target_column}列的{aggregate.lower()}"
            else:
                sql = f"SELECT COUNT(*) as count FROM {table};"
                explanation = f"统计{table}表的记录数"

        return {
            'sql': sql,
            'confidence': 0.85,
            'explanation': explanation,
            'pattern': 'aggregate'
        }

    def _ai_enhanced_translation(self, natural_query: str, entities: Dict) -> Dict:
        """AI增强的翻译"""
        schema_context = self._get_schema_context()

        prompt = f"""
你是一个专业的SQL生成助手。根据以下信息将自然语言查询转换为SQL语句。

数据库结构:
{json.dumps(schema_context, indent=2, ensure_ascii=False)}

已识别的实体:
- 表: {entities.get('tables', [])}
- 列: {[f"{e['table']}.{e['column']}" for e in entities.get('columns', [])]}
- 聚合函数: {entities.get('aggregates', [])}
- 数值: {entities.get('values', [])}

自然语言查询: {natural_query}

请生成SQL语句并提供以下JSON格式的响应:
{{
    "sql": "生成的SQL语句",
    "explanation": "中文解释",
    "confidence": 0.9,
    "reasoning": "推理过程"
}}
"""

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "你是一个SQL专家，专门将中文自然语言转换为准确的SQL语句。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1
            )

            result_text = response.choices[0].message.content
            result = json.loads(result_text)
            result['method'] = 'ai_enhanced'

            # 验证生成的SQL
            if self._validate_generated_sql(result.get('sql', '')):
                result['confidence'] = min(result.get('confidence', 0.7) + 0.1, 1.0)

            return result

        except Exception as e:
            return {
                'sql': '',
                'confidence': 0.0,
                'explanation': f'AI处理失败: {str(e)}',
                'method': 'ai_enhanced',
                'error': str(e)
            }

    def _get_schema_context(self) -> Dict:
        """获取数据库结构上下文"""
        schema = {'tables': {}, 'views': {}}

        # 获取所有表
        try:
            tables = self.catalog.get_all_tables()
            for table_name, table_info in tables.items():
                columns_info = []
                for col in table_info.get('columns', []):
                    col_info = {
                        'name': col.get('name', col) if isinstance(col, dict) else col,
                        'type': col.get('type', 'VARCHAR') if isinstance(col, dict) else 'VARCHAR'
                    }
                    columns_info.append(col_info)

                schema['tables'][table_name] = {
                    'columns': columns_info,
                    'description': f'表 {table_name}'
                }
        except Exception as e:
            # 如果获取表信息失败，返回空结构
            schema['tables'] = {}

        # 获取所有视图
        try:
            if hasattr(self.catalog, 'get_all_views'):
                views = self.catalog.get_all_views()
                for view_name in views:
                    schema['views'][view_name] = {
                        'type': 'view',
                        'description': f'视图 {view_name}'
                    }
        except Exception:
            schema['views'] = {}

        return schema

    def _get_numeric_columns(self, table_name: str) -> List[str]:
        """获取表的数值类型列"""
        numeric_columns = []
        try:
            if self.catalog.table_exists(table_name):
                table_info = self.catalog.get_table(table_name)
                columns = table_info.get('columns', [])

                for col in columns:
                    col_name = col.get('name', col) if isinstance(col, dict) else col
                    col_type = col.get('type', '') if isinstance(col, dict) else ''

                    # 检查是否是数值类型
                    if any(num_type in col_type.upper() for num_type in ['INT', 'FLOAT', 'DECIMAL', 'NUMBER']):
                        numeric_columns.append(col_name)
                    elif col_name.lower() in ['salary', 'age', 'price', 'amount', 'count']:
                        # 基于列名推测
                        numeric_columns.append(col_name)
        except Exception:
            pass

        return numeric_columns

    def _build_group_by_query(self, query: str, entities: Dict) -> Dict:
        """构建分组查询"""
        if not entities['tables']:
            return {'sql': '', 'confidence': 0.0}

        table = entities['tables'][0]

        # 默认按部门分组
        group_by_col = 'department'

        # 检查是否有其他分组列
        for col_info in entities.get('columns', []):
            col_name = col_info.get('column', '')
            if 'department' in col_name.lower() or 'dept' in col_name.lower():
                group_by_col = col_name
                break

        # 确定聚合函数
        if entities.get('aggregates'):
            agg_func = entities['aggregates'][0]
            if agg_func == 'COUNT':
                sql = f"SELECT {group_by_col}, COUNT(*) as count FROM {table} GROUP BY {group_by_col};"
                explanation = f"按{group_by_col}分组统计{table}表的记录数"
            else:
                # 找到合适的聚合列
                agg_col = self._get_numeric_columns(table)
                if agg_col:
                    sql = f"SELECT {group_by_col}, {agg_func}({agg_col[0]}) as result FROM {table} GROUP BY {group_by_col};"
                    explanation = f"按{group_by_col}分组计算{table}表中{agg_col[0]}的{agg_func.lower()}"
                else:
                    sql = f"SELECT {group_by_col}, COUNT(*) as count FROM {table} GROUP BY {group_by_col};"
                    explanation = f"按{group_by_col}分组统计{table}表的记录数"
        else:
            # 默认使用COUNT
            sql = f"SELECT {group_by_col}, COUNT(*) as count FROM {table} GROUP BY {group_by_col};"
            explanation = f"按{group_by_col}分组统计{table}表的记录数"

        return {
            'sql': sql,
            'confidence': 0.8,
            'explanation': explanation,
            'pattern': 'group_by'
        }

    def _enhance_result(self, result: Dict, original_query: str) -> Dict:
        """增强结果信息"""
        enhanced_result = result.copy()

        # 添加原始查询
        enhanced_result['original_query'] = original_query

        # 添加时间戳
        from datetime import datetime
        enhanced_result['timestamp'] = datetime.now().isoformat()

        # 如果有SQL，添加验证信息
        if enhanced_result.get('sql'):
            enhanced_result['sql_valid'] = self._validate_generated_sql(enhanced_result['sql'])

        # 添加建议
        if not enhanced_result.get('suggestions'):
            enhanced_result['suggestions'] = self._generate_suggestions(enhanced_result)

        return enhanced_result

    def _validate_generated_sql(self, sql: str) -> bool:
        """验证生成的SQL语句"""
        if not sql or not sql.strip():
            return False

        sql = sql.strip().upper()

        # 基本语法检查
        valid_starts = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']
        if not any(sql.startswith(start) for start in valid_starts):
            return False

        # 检查是否以分号结尾
        if not sql.endswith(';'):
            return False

        # 检查括号匹配
        if sql.count('(') != sql.count(')'):
            return False

        # 检查基本的SELECT语句结构
        if sql.startswith('SELECT'):
            if 'FROM' not in sql:
                return False

        return True

    def _generate_suggestions(self, result: Dict) -> List[str]:
        """生成改进建议"""
        suggestions = []

        confidence = result.get('confidence', 0)

        if confidence < 0.5:
            suggestions.append("查询转换置信度较低，请检查自然语言表达是否准确")
            suggestions.append("尝试使用更具体的表名和列名")

        if confidence < 0.8:
            suggestions.append("建议验证生成的SQL语句是否符合预期")

        sql = result.get('sql', '')
        if sql:
            if 'SELECT *' in sql.upper():
                suggestions.append("使用SELECT *可能影响性能，建议指定具体列名")

            if 'WHERE' not in sql.upper() and 'SELECT' in sql.upper():
                suggestions.append("考虑添加WHERE条件以提高查询效率")

        if not suggestions:
            suggestions.append("SQL生成成功，建议在执行前先验证结果")

        return suggestions

    def _load_query_patterns(self) -> Dict:
        """加载查询模式"""
        return {
        'create_table': {
            'patterns': [
                r'创建.*?表',
                r'生成.*?表',
                r'建立.*?表',
                r'create.*?table'
            ],
            'template': 'CREATE TABLE {table} ({columns});'
        },
            'select_all': [
                r'显示所有',
                r'查看全部',
                r'列出所有'
            ],
            'count': [
                r'统计数量',
                r'有多少',
                r'计算总数'
            ],
            'filter': [
                r'条件查询',
                r'筛选',
                r'过滤'
            ],
            'aggregate': [
                r'平均值',
                r'最大值',
                r'最小值',
                r'总和'
            ],
            'group_by': [
                r'按.*分组',
                r'分类统计'
            ]
        }

    def _build_entity_mapping(self) -> Dict:
        """构建实体映射"""
        return {
            'table_aliases': {
                '员工': 'employees',
                '雇员': 'employees',
                '部门': 'departments',
                '用户': 'users'
            },
            'column_aliases': {
                '姓名': 'name',
                '名字': 'name',
                '薪资': 'salary',
                '工资': 'salary',
                '部门': 'department'
            }
        }