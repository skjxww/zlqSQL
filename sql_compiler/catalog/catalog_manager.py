import json
import os
from typing import Dict, List, Optional, Any


class CatalogManager:
    def __init__(self, catalog_file: str = "system_catalog.json"):
        self.catalog_file = catalog_file
        self.catalog_data = self._load_catalog()

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
            "metadata": {
                "version": "1.0",
                "created_at": None
            }
        }

    def _save_catalog(self):
        """保存系统目录"""
        try:
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
        """删除表"""
        if table_name not in self.catalog_data["tables"]:
            return False

        del self.catalog_data["tables"][table_name]
        self._save_catalog()
        return True

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.catalog_data["tables"]

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
