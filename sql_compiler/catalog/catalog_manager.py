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
        """获取表信息 - 修复缺失的方法"""
        return self.catalog_data["tables"].get(table_name)

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

    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        return list(self.catalog_data["tables"].keys())

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取完整表信息"""
        return self.catalog_data["tables"].get(table_name)

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