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
        print(f"🚀 开始翻译查询: '{natural_query}'")

        try:
            # 1. 预处理和实体识别
            processed_query = self._preprocess_query(natural_query)
            print(f"📝 预处理后: '{processed_query}'")

            entities = self._extract_entities(processed_query)
            print(f"🎯 提取到的实体: {json.dumps(entities, indent=2, ensure_ascii=False)}")

            # 2. 模式匹配（快速路径）
            pattern_result = self._pattern_matching(processed_query, entities)
            print(f"🔍 模式匹配结果 - 置信度: {pattern_result.get('confidence', 0):.2f}")

            if pattern_result['confidence'] > 0.8:
                print("✅ 使用模式匹配结果")
                return self._enhance_result(pattern_result, natural_query)

            # 3. 使用 DeepSeek API 处理复杂查询
            if self.api_key:
                print("🤖 使用AI API处理...")
                ai_result = self._translate_with_deepseek(natural_query, entities)
                print(f"🎭 AI结果置信度: {ai_result.get('confidence', 0):.2f}")
                return self._enhance_result(ai_result, natural_query)
            else:
                print("⚠️ 无API密钥，使用模式匹配结果")
                return self._enhance_result(pattern_result, natural_query)

        except Exception as e:
            print(f"❌ 翻译过程出错: {str(e)}")
            return {
                'sql': '',
                'confidence': 0.0,
                'explanation': f'转换失败: {str(e)}',
                'suggestions': ['请检查查询语句的表达方式'],
                'entities': {},
                'error': str(e),
                'method': 'error'
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
        """查询预处理 - 修复版本"""
        print(f"📋 原始查询: '{query}'")

        if not query or not query.strip():
            print("⚠️ 查询为空")
            return ""

        # 保留原始查询用于表名匹配
        original_query = query.strip()
        query = query.strip().lower()

        # 标准化常见表达，但保留表名
        replacements = {
            r'显示|展示|查看|列出|给我看|查询': 'show',
            r'所有的?数据|全部数据|全体数据': 'all data',  # 修改这里
            r'员工|雇员|职员': 'employee',
            r'部门|科室': 'department',
            r'薪资|工资|薪水|收入': 'salary',
            r'姓名|名字|名称': 'name',
            # 移除对"表"的替换，保留原始表名
        }

        processed_query = query
        for pattern, replacement in replacements.items():
            processed_query = re.sub(pattern, replacement, processed_query)

        print(f"🔄 预处理结果: '{processed_query}'")
        return processed_query
    def _extract_entities(self, query: str) -> Dict:
        """实体提取"""
        print(f"🔎 开始实体提取: '{query}'")

        entities = {
            'tables': [],
            'columns': [],
            'conditions': [],
            'aggregates': [],
            'values': []
        }

        # 获取所有可用的表
        try:
            all_tables = self.catalog.get_all_tables()
            print(f"📊 可用表: {list(all_tables.keys())}")
        except Exception as e:
            print(f"⚠️ 获取表信息失败: {e}")
            all_tables = {}

        # 提取表名（支持中英文）
        table_found = False
        for table_name in all_tables.keys():
            # 检查完整表名（包括数字）
            if table_name.lower() in query.lower():  # 改为不区分大小写
                entities['tables'].append(table_name)
                table_found = True
                print(f"✅ 找到表名: {table_name} (完全匹配)")
            # 检查表名的各种变形
            elif self._fuzzy_table_match(query, table_name):
                entities['tables'].append(table_name)
                table_found = True
                print(f"✅ 找到表名: {table_name} (模糊匹配)")
            # 检查单数形式
            elif table_name.lower().rstrip('s') in query:
                entities['tables'].append(table_name)
                table_found = True
                print(f"✅ 找到表名: {table_name} (单数匹配)")
            # 检查关键词匹配
            elif self._fuzzy_table_match(query, table_name):
                entities['tables'].append(table_name)
                table_found = True
                print(f"✅ 找到表名: {table_name} (模糊匹配)")

        if not table_found:
            print("⚠️ 未找到匹配的表名")
            # 尝试从实体映射中查找
            for alias, real_name in self.entity_mapping.get('table_aliases', {}).items():
                if alias in query:
                    entities['tables'].append(real_name)
                    print(f"✅ 通过别名找到表: {alias} -> {real_name}")

        # 提取列名
        for table_name, table_info in all_tables.items():
            if table_name in entities['tables']:  # 只检查已识别的表
                columns = []
                if isinstance(table_info, dict):
                    columns = [col.get('name', col) if isinstance(col, dict) else col
                               for col in table_info.get('columns', [])]

                for col in columns:
                    if col.lower() in query:
                        entities['columns'].append({'table': table_name, 'column': col})
                        print(f"✅ 找到列: {table_name}.{col}")

        # 提取数值
        numbers = re.findall(r'\b\d+(?:\.\d+)?\b', query)
        entities['values'].extend(numbers)
        if numbers:
            print(f"🔢 找到数值: {numbers}")

        # 提取聚合函数
        agg_keywords = {
            'count': 'COUNT',
            'show all': 'SELECT_ALL',
            'all': 'SELECT_ALL',
            'average': 'AVG',
            'avg': 'AVG',
            'sum': 'SUM',
            'max': 'MAX',
            'min': 'MIN'
        }

        for keyword, func in agg_keywords.items():
            if keyword in query:
                entities['aggregates'].append(func)
                print(f"📊 找到聚合函数: {keyword} -> {func}")

        print(f"🎯 最终提取结果: {json.dumps(entities, indent=2, ensure_ascii=False)}")
        return entities

    def _pattern_matching(self, query: str, entities: Dict) -> Dict:
        """增强的模式匹配"""
        print(f"🔍 模式匹配开始")
        print(f"  查询: '{query}'")
        print(f"  找到的表: {entities.get('tables', [])}")

        # 检查是否是查询所有数据的模式
        all_data_patterns = [
            'show all', 'list all', 'select all', 'all data',
            '所有数据', '全部数据', '全体数据'
        ]

        is_select_all = any(pattern in query for pattern in all_data_patterns)
        print(f"  是否为查询全部: {is_select_all}")

        if is_select_all and entities['tables']:
            # 选择最匹配的表名，而不是第一个
            table = self._select_best_table(query, entities['tables'])
            print(f"✅ 构建查询 - 使用表: {table}")

            result = {
                'sql': f"SELECT * FROM {table};",
                'confidence': 0.9,
                'explanation': f'查询{table}表的所有记录',
                'pattern': 'select_all',
                'method': 'pattern_matching'
            }

            print(f"📝 生成SQL: {result['sql']}")
            return result

        # 条件查询模式
        if entities['conditions'] or any(op in query for op in ['greater than', 'less than', 'equals']):
            return self._build_conditional_query(query, entities)

        # 聚合查询模式
        if entities['aggregates']:
            return self._build_aggregate_query(query, entities)

        # 分组查询模式
        if 'group by' in query or 'by department' in query:
            return self._build_group_by_query(query, entities)

        print("❌ 未匹配到任何模式")
        return {'sql': '', 'confidence': 0.0, 'pattern': 'unknown'}

    def _select_best_table(self, query: str, tables: List[str]) -> str:
        """从多个表中选择最匹配的表"""
        print(f"🎯 选择最佳表名")
        print(f"  候选表: {tables}")
        print(f"  查询: '{query}'")

        if len(tables) == 1:
            print(f"  只有一个表，直接选择: {tables[0]}")
            return tables[0]

        # 计算每个表名的匹配分数
        table_scores = {}

        for table in tables:
            score = 0
            print(f"  评估表 '{table}':")

            # 1. 完全匹配得分最高
            if table.lower() in query.lower():
                exact_matches = len([m for m in re.finditer(re.escape(table.lower()), query.lower())])
                score += exact_matches * 10
                print(f"    完全匹配次数: {exact_matches}, 得分: +{exact_matches * 10}")

            # 2. 长度优先（更具体的表名）
            score += len(table) * 0.1
            print(f"    长度得分: +{len(table) * 0.1}")

            # 3. 包含数字的表名优先（如果查询中提到了数字）
            if re.search(r'\d', table) and re.search(r'\d', query):
                score += 5
                print(f"    数字匹配得分: +5")

            # 4. 检查表名在查询中的位置（越靠前越重要）
            try:
                position = query.lower().find(table.lower())
                if position >= 0:
                    # 位置越靠前得分越高
                    position_score = max(0, 10 - position * 0.1)
                    score += position_score
                    print(f"    位置得分: +{position_score}")
            except:
                pass

            table_scores[table] = score
            print(f"    总得分: {score}")

        # 选择得分最高的表
        best_table = max(table_scores, key=table_scores.get)
        print(f"🏆 最佳匹配: {best_table} (得分: {table_scores[best_table]})")

        return best_table

    def _fuzzy_table_match(self, query: str, table_name: str) -> bool:
        """模糊表名匹配"""
        # 检查orders表的特殊情况
        if 'order' in table_name.lower() and ('order' in query or 'orders' in query):
            return True

        # 检查employee相关
        if 'employee' in table_name.lower() and 'employee' in query:
            return True

        # 检查department相关
        if 'department' in table_name.lower() and 'department' in query:
            return True

        return False

    def _build_conditional_query(self, query: str, entities: Dict) -> Dict:
        """构建条件查询"""
        if not entities['tables']:
            return {'sql': '', 'confidence': 0.0}

        # 使用最佳匹配的表名
        table = self._select_best_table(query, entities['tables'])
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
        print(f"📊 构建聚合查询")

        if not entities['tables']:
            print("❌ 没有表名，无法构建聚合查询")
            return {'sql': '', 'confidence': 0.0}

        # 使用最佳匹配的表名
        table = self._select_best_table(query, entities['tables'])

        # 处理SELECT_ALL类型
        if 'SELECT_ALL' in entities['aggregates']:
            sql = f"SELECT * FROM {table};"
            explanation = f"查询{table}表的所有记录"
            confidence = 0.9
        elif 'COUNT' in entities['aggregates']:
            sql = f"SELECT COUNT(*) as count FROM {table};"
            explanation = f"统计{table}表的记录数"
            confidence = 0.85
        else:
            # 其他聚合函数
            aggregate = entities['aggregates'][0]
            numeric_cols = self._get_numeric_columns(table)

            if numeric_cols:
                target_column = numeric_cols[0]
                sql = f"SELECT {aggregate}({target_column}) as result FROM {table};"
                explanation = f"计算{table}表中{target_column}列的{aggregate.lower()}"
                confidence = 0.8
            else:
                sql = f"SELECT COUNT(*) as count FROM {table};"
                explanation = f"统计{table}表的记录数（未找到数值列）"
                confidence = 0.6

        result = {
            'sql': sql,
            'confidence': confidence,
            'explanation': explanation,
            'pattern': 'aggregate',
            'method': 'pattern_matching'
        }

        print(f"📊 聚合查询结果: {result}")
        return result

    def _build_group_by_query(self, query: str, entities: Dict) -> Dict:
        """构建分组查询"""
        if not entities['tables']:
            return {'sql': '', 'confidence': 0.0}

        # 使用最佳匹配的表名
        table = self._select_best_table(query, entities['tables'])

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
            all_tables = self.catalog.get_all_tables()
            if table_name in all_tables:
                table_info = all_tables[table_name]
                columns = table_info.get('columns', [])

                for col in columns:
                    col_name = col.get('name', col) if isinstance(col, dict) else col
                    col_type = col.get('type', '') if isinstance(col, dict) else ''

                    # 检查是否是数值类型
                    if any(num_type in col_type.upper() for num_type in ['INT', 'FLOAT', 'DECIMAL', 'NUMBER']):
                        numeric_columns.append(col_name)
                    elif col_name.lower() in ['salary', 'age', 'price', 'amount', 'count', 'id']:
                        # 基于列名推测
                        numeric_columns.append(col_name)

            print(f"🔢 表 {table_name} 的数值列: {numeric_columns}")
        except Exception as e:
            print(f"⚠️ 获取数值列失败: {e}")

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
            'select_all': {
                'patterns': [
                    r'显示.*?所有',
                    r'查看.*?全部',
                    r'列出.*?所有',
                    r'show.*?all',
                    r'select.*?all'
                ],
                'confidence': 0.9
            },
            'count': {
                'patterns': [
                    r'统计.*?数量',
                    r'有多少',
                    r'计算.*?总数',
                    r'count'
                ],
                'confidence': 0.85
            },
            'aggregate': {
                'patterns': [
                    r'平均.*?值',
                    r'最大.*?值',
                    r'最小.*?值',
                    r'总和',
                    r'avg|max|min|sum'
                ],
                'confidence': 0.8
            }
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