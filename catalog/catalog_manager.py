import json
import os
from typing import Dict, List, Optional, Any
from sql_compiler.btree.BPlusTreeIndex import BPlusTreeIndex


class CatalogManager:
    def __init__(self, catalog_file: str = "system_catalog.json"):
        self.catalog_file = catalog_file
        self.catalog_data = self._load_catalog()
        self.indexes = {}  # 索引信息: index_name -> index_info
        self.table_indexes = {}  # 表索引映射: table_name -> [index_names]

        # 从持久化数据中恢复索引信息
        self._load_indexes_from_catalog()

    def _load_indexes_from_catalog(self):
        """从目录文件中加载索引信息"""
        if "indexes" in self.catalog_data:
            for index_name, index_data in self.catalog_data["indexes"].items():
                # 重建内存中的索引信息
                self.indexes[index_name] = {
                    "name": index_data["name"],
                    "table": index_data["table"],
                    "columns": index_data["columns"],
                    "unique": index_data.get("unique", False),
                    "type": index_data.get("type", "BTREE"),
                    "btree": BPlusTreeIndex()  # 重新创建B+树实例
                }

                # 重建表索引映射
                table_name = index_data["table"]
                if table_name not in self.table_indexes:
                    self.table_indexes[table_name] = []
                self.table_indexes[table_name].append(index_name)

    def _load_catalog(self) -> Dict[str, Any]:
        """加载系统目录"""
        if os.path.exists(self.catalog_file):
            try:
                with open(self.catalog_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass

        # 返回默认的空目录结构
        return {
            "tables": {},
            "indexes": {},  # 添加索引持久化支持
            "metadata": {
                "version": "1.0",
                "created_at": None
            }
        }

    def _save_catalog(self):
        """保存系统目录"""
        try:
            # 将索引信息添加到持久化数据中
            self.catalog_data["indexes"] = {}
            for index_name, index_info in self.indexes.items():
                self.catalog_data["indexes"][index_name] = {
                    "name": index_info["name"],
                    "table": index_info["table"],
                    "columns": index_info["columns"],
                    "unique": index_info["unique"],
                    "type": index_info["type"],
                    "created_at": self._get_current_time()
                    # 注意：不保存 btree 对象，因为不能序列化
                }

            with open(self.catalog_file, 'w', encoding='utf-8') as f:
                json.dump(self.catalog_data, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"保存目录失败: {e}")

    def create_table(self, table_name: str, columns: List[tuple]) -> bool:
        """创建表"""
        if table_name in self.catalog_data["tables"]:
            return False  # 表已存在

        table_info = {
            "name": table_name,
            "columns": [
                {
                    "name": col_name,
                    "type": col_type,
                    "constraints": col_constraints
                }
                for col_name, col_type, col_constraints in columns
            ],
            "created_at": self._get_current_time(),
            "row_count": 0
        }

        self.catalog_data["tables"][table_name] = table_info
        self._save_catalog()
        return True

    def drop_table(self, table_name: str) -> bool:
        """删除表 - 增强版，同时删除相关索引"""
        if table_name not in self.catalog_data["tables"]:
            return False

        # 删除表相关的所有索引
        if table_name in self.table_indexes:
            indexes_to_drop = self.table_indexes[table_name].copy()
            for index_name in indexes_to_drop:
                self.drop_index(index_name)

        del self.catalog_data["tables"][table_name]
        self._save_catalog()
        return True

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.catalog_data["tables"]

    def column_exists(self, table_name: str, column_name: str) -> bool:
        """检查表中的列是否存在"""
        if table_name not in self.tables:
            return False

        table_info = self.tables[table_name]
        return any(col['name'] == column_name for col in table_info['columns'])

    def get_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表信息"""
        return self.catalog_data["tables"].get(table_name)

    def get_all_tables(self) -> Dict[str, Dict[str, Any]]:
        """获取所有表的完整信息"""
        return self.catalog_data["tables"].copy()

    def get_all_table_names(self) -> List[str]:
        """获取所有表名"""
        return list(self.catalog_data["tables"].keys())

    def get_tables_info(self) -> Dict[str, Dict[str, Any]]:
        """获取所有表的完整信息（别名方法）"""
        return self.get_all_tables()

    def reset_for_testing(self):
        """重置数据库状态（专用于测试）"""
        self.clear_all_tables()

    def table_count(self) -> int:
        """获取表的数量"""
        return len(self.catalog_data["tables"])

    def is_empty(self) -> bool:
        """检查目录是否为空"""
        return len(self.catalog_data["tables"]) == 0

    def export_schema(self) -> str:
        """导出数据库结构为SQL"""
        sql_statements = []

        for table_name, table_info in self.catalog_data["tables"].items():
            columns_def = []
            for col in table_info["columns"]:
                col_def = f"{col['name']} {col['type']}"
                if col.get('constraints'):
                    col_def += f" {col['constraints']}"
                columns_def.append(col_def)

            create_sql = f"CREATE TABLE {table_name} ({', '.join(columns_def)});"
            sql_statements.append(create_sql)

        return '\n'.join(sql_statements)

    def validate_catalog(self) -> bool:
        """验证目录数据的完整性"""
        try:
            # 检查基本结构
            if "tables" not in self.catalog_data:
                return False

            if not isinstance(self.catalog_data["tables"], dict):
                return False

            # 检查每个表的结构
            for table_name, table_info in self.catalog_data["tables"].items():
                if not isinstance(table_info, dict):
                    return False

                if "columns" not in table_info:
                    return False

                if not isinstance(table_info["columns"], list):
                    return False

                # 检查每个列的结构
                for col in table_info["columns"]:
                    if not isinstance(col, dict):
                        return False

                    if "name" not in col or "type" not in col:
                        return False

            return True

        except Exception:
            return False

    def get_table_schema(self, table_name: str) -> Optional[List[tuple]]:
        """获取表结构"""
        if table_name not in self.catalog_data["tables"]:
            return None

        table_info = self.catalog_data["tables"][table_name]
        return [
            (col["name"], col["type"], col["constraints"])
            for col in table_info["columns"]
        ]

    def get_column_info(self, table_name: str, column_name: str) -> Optional[Dict[str, Any]]:
        """获取列信息"""
        if table_name not in self.catalog_data["tables"]:
            return None

        for col in self.catalog_data["tables"][table_name]["columns"]:
            if col["name"] == column_name:
                return col
        return None

    def get_table_columns(self, table_name: str) -> List[str]:
        """获取表的所有列名"""
        table_info = self.get_table(table_name)
        if not table_info:
            return []

        return [col["name"] for col in table_info["columns"]]

    def update_row_count(self, table_name: str, delta: int):
        """更新表行数"""
        if table_name in self.catalog_data["tables"]:
            current_count = self.catalog_data["tables"][table_name].get("row_count", 0)
            self.catalog_data["tables"][table_name]["row_count"] = max(0, current_count + delta)
            self._save_catalog()

    def get_catalog_stats(self) -> Dict[str, Any]:
        """获取目录统计信息"""
        total_tables = len(self.catalog_data["tables"])
        total_columns = sum(
            len(table_info["columns"])
            for table_info in self.catalog_data["tables"].values()
        )
        total_rows = sum(
            table_info.get("row_count", 0)
            for table_info in self.catalog_data["tables"].values()
        )

        return {
            "total_tables": total_tables,
            "total_columns": total_columns,
            "total_rows": total_rows,
            "tables": list(self.catalog_data["tables"].keys())
        }

    def _get_current_time(self) -> str:
        """获取当前时间字符串"""
        import datetime
        return datetime.datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return self.catalog_data.copy()

    def clear_all_tables(self):
        """清空所有表（用于测试）"""
        self.catalog_data["tables"] = {}
        self._save_catalog()

    def print_catalog_info(self):
        """打印目录信息（用于调试）"""
        stats = self.get_catalog_stats()
        print(f"目录统计:")
        print(f"  总表数: {stats['total_tables']}")
        print(f"  总列数: {stats['total_columns']}")
        print(f"  总行数: {stats['total_rows']}")

        if stats['tables']:
            print(f"  表列表: {', '.join(stats['tables'])}")

            for table_name in stats['tables']:
                table_info = self.get_table(table_name)
                columns = [col['name'] for col in table_info['columns']]
                print(f"    {table_name}: [{', '.join(columns)}]")

    def get_column_type(self, table_name: str, column_name: str) -> Optional[str]:
        """获取列的数据类型"""
        column_info = self.get_column_info(table_name, column_name)
        if column_info:
            return column_info.get("type")
        return None

    def get_table_column_types(self, table_name: str) -> Dict[str, str]:
        """获取表的所有列及其类型"""
        table_info = self.get_table(table_name)
        if not table_info:
            return {}

        column_types = {}
        for col in table_info["columns"]:
            column_types[col["name"]] = col["type"]

        return column_types

    def create_index(self, index_name: str, table_name: str, columns: List[str],
                     unique: bool = False, index_type: str = "BTREE") -> bool:
        """创建索引 - 使用适配后的B+树"""
        if index_name in self.indexes:
            return False

        if table_name not in self.catalog_data["tables"]:
            return False

        # 验证列是否存在
        table_columns = self.get_table_columns(table_name)
        for col in columns:
            if col not in table_columns:
                return False

        # 创建B+树实例
        btree_instance = BPlusTreeIndex(index_name)

        index_info = {
            "name": index_name,
            "table": table_name,
            "columns": columns,
            "unique": unique,
            "type": index_type,
            "btree": btree_instance,  # 使用适配后的B+树
            "created_at": self._get_current_time()
        }

        self.indexes[index_name] = index_info

        if table_name not in self.table_indexes:
            self.table_indexes[table_name] = []
        self.table_indexes[table_name].append(index_name)

        # 保存到持久化存储
        self._save_catalog()

        print(f"✅ 创建索引 {index_name}，类型: {btree_instance.implementation}")
        return True

    def drop_index(self, index_name: str) -> bool:
        """删除索引"""
        if index_name not in self.indexes:
            return False

        index_info = self.indexes[index_name]
        table_name = index_info["table"]

        del self.indexes[index_name]
        if table_name in self.table_indexes:
            self.table_indexes[table_name].remove(index_name)

            # 如果表没有索引了，清理映射
            if not self.table_indexes[table_name]:
                del self.table_indexes[table_name]

        # 保存到持久化存储
        self._save_catalog()
        return True

    def get_table_indexes(self, table_name: str) -> List[Dict]:
        """获取表的所有索引"""
        if table_name not in self.table_indexes:
            return []

        return [self.indexes[idx_name] for idx_name in self.table_indexes[table_name]]

    def find_best_index(self, table_name: str, condition_columns: List[str]) -> Optional[str]:
        """为查询条件找到最佳索引 - 增强版"""
        if table_name not in self.table_indexes:
            return None

        best_index = None
        best_score = 0

        for index_name in self.table_indexes[table_name]:
            index_info = self.indexes[index_name]
            index_columns = index_info["columns"]

            # 计算匹配度 - 改进的算法
            score = 0

            # 前缀匹配得分更高
            for i, col in enumerate(condition_columns):
                if i < len(index_columns) and col == index_columns[i]:
                    score += (len(index_columns) - i) * 10  # 前缀匹配权重更高
                elif col in index_columns:
                    score += 1  # 普通匹配

            # 唯一索引加分
            if index_info.get("unique", False):
                score += 5

            # 考虑索引列数（较少的列数可能更高效）
            if len(index_columns) <= len(condition_columns):
                score += 2

            if score > best_score:
                best_score = score
                best_index = index_name

        return best_index

    def get_index_info(self, index_name: str) -> Optional[Dict[str, Any]]:
        """获取索引信息"""
        return self.indexes.get(index_name)

    def index_exists(self, index_name: str) -> bool:
        """检查索引是否存在"""
        return index_name in self.indexes

    def get_all_indexes(self) -> Dict[str, Dict[str, Any]]:
        """获取所有索引信息"""
        return self.indexes.copy()

    def find_indexes_for_columns(self, table_name: str, columns: List[str]) -> List[str]:
        """查找包含指定列的索引"""
        if table_name not in self.table_indexes:
            return []

        matching_indexes = []
        for index_name in self.table_indexes[table_name]:
            index_info = self.indexes[index_name]
            index_columns = set(index_info["columns"])
            query_columns = set(columns)

            # 如果索引包含所有查询列
            if query_columns.issubset(index_columns):
                matching_indexes.append(index_name)

        return matching_indexes

    def is_covering_index(self, index_name: str, required_columns: List[str]) -> bool:
        """检查索引是否为覆盖索引"""
        if index_name not in self.indexes:
            return False

        index_info = self.indexes[index_name]
        index_columns = set(index_info["columns"])
        required_set = set(required_columns)

        return required_set.issubset(index_columns)

    def get_unique_indexes(self, table_name: str) -> List[str]:
        """获取表的所有唯一索引"""
        if table_name not in self.table_indexes:
            return []

        unique_indexes = []
        for index_name in self.table_indexes[table_name]:
            index_info = self.indexes[index_name]
            if index_info.get("unique", False):
                unique_indexes.append(index_name)

        return unique_indexes

    def get_index_statistics(self) -> Dict[str, Any]:
        """获取索引统计信息"""
        total_indexes = len(self.indexes)
        unique_indexes = sum(1 for idx in self.indexes.values() if idx.get("unique", False))
        btree_indexes = sum(1 for idx in self.indexes.values() if idx.get("type") == "BTREE")

        indexes_by_table = {}
        for table_name, index_list in self.table_indexes.items():
            indexes_by_table[table_name] = len(index_list)

        return {
            "total_indexes": total_indexes,
            "unique_indexes": unique_indexes,
            "btree_indexes": btree_indexes,
            "indexes_by_table": indexes_by_table,
            "tables_with_indexes": len(self.table_indexes)
        }

    def validate_index_integrity(self) -> Dict[str, List[str]]:
        """验证索引完整性"""
        issues = {
            "missing_tables": [],
            "missing_columns": [],
            "orphaned_indexes": []
        }

        for index_name, index_info in self.indexes.items():
            table_name = index_info["table"]

            # 检查表是否存在
            if table_name not in self.catalog_data["tables"]:
                issues["missing_tables"].append(f"索引 {index_name} 引用不存在的表 {table_name}")
                continue

            # 检查列是否存在
            table_columns = self.get_table_columns(table_name)
            for col in index_info["columns"]:
                if col not in table_columns:
                    issues["missing_columns"].append(
                        f"索引 {index_name} 引用表 {table_name} 中不存在的列 {col}"
                    )

            # 检查映射关系
            if (table_name not in self.table_indexes or
                    index_name not in self.table_indexes[table_name]):
                issues["orphaned_indexes"].append(f"索引 {index_name} 缺少表映射关系")

        return issues

    def repair_index_mappings(self):
        """修复索引映射关系"""
        self.table_indexes.clear()

        for index_name, index_info in self.indexes.items():
            table_name = index_info["table"]
            if table_name not in self.table_indexes:
                self.table_indexes[table_name] = []
            self.table_indexes[table_name].append(index_name)

        self._save_catalog()

    def print_index_info(self):
        """打印索引信息（调试用）"""
        stats = self.get_index_statistics()
        print(f"索引统计:")
        print(f"  总索引数: {stats['total_indexes']}")
        print(f"  唯一索引数: {stats['unique_indexes']}")
        print(f"  B+树索引数: {stats['btree_indexes']}")
        print(f"  有索引的表数: {stats['tables_with_indexes']}")

        for table_name, index_count in stats['indexes_by_table'].items():
            print(f"  表 {table_name}: {index_count} 个索引")
            for index_name in self.table_indexes[table_name]:
                index_info = self.indexes[index_name]
                columns_str = ", ".join(index_info["columns"])
                unique_str = " (UNIQUE)" if index_info.get("unique") else ""
                print(f"    - {index_name}: [{columns_str}]{unique_str}")