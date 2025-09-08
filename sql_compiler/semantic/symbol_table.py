from typing import Dict, List, Optional, Any
from enum import Enum


class SymbolType(Enum):
    TABLE = "TABLE"
    COLUMN = "COLUMN"


class Symbol:
    def __init__(self, name: str, symbol_type: SymbolType, data_type: str = None,
                 table_name: str = None, attributes: Dict[str, Any] = None):
        self.name = name
        self.symbol_type = symbol_type
        self.data_type = data_type  # 对于列：INT, VARCHAR等
        self.table_name = table_name  # 列所属的表
        self.attributes = attributes or {}


class SymbolTable:
    def __init__(self):
        self.tables: Dict[str, Symbol] = {}  # 表符号
        self.columns: Dict[str, List[Symbol]] = {}  # 表名 -> 列符号列表

    def add_table(self, table_name: str, columns: List[tuple]) -> bool:
        """添加表定义"""
        if table_name in self.tables:
            return False  # 表已存在

        # 添加表符号
        table_symbol = Symbol(table_name, SymbolType.TABLE)
        self.tables[table_name] = table_symbol

        # 添加列符号
        column_symbols = []
        for column_name, column_type, constraints in columns:
            column_symbol = Symbol(
                name=column_name,
                symbol_type=SymbolType.COLUMN,
                data_type=column_type,
                table_name=table_name,
                attributes={'constraints': constraints}
            )
            column_symbols.append(column_symbol)

        self.columns[table_name] = column_symbols
        return True

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.tables

    def get_table_columns(self, table_name: str) -> Optional[List[Symbol]]:
        """获取表的所有列"""
        return self.columns.get(table_name)

    def column_exists(self, table_name: str, column_name: str) -> bool:
        """检查列是否存在"""
        if table_name not in self.columns:
            return False

        for column in self.columns[table_name]:
            if column.name == column_name:
                return True
        return False

    def get_column_type(self, table_name: str, column_name: str) -> Optional[str]:
        """获取列类型"""
        if table_name not in self.columns:
            return None

        for column in self.columns[table_name]:
            if column.name == column_name:
                return column.data_type
        return None

    def get_all_tables(self) -> List[str]:
        """获取所有表名"""
        return list(self.tables.keys())

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {
            'tables': {},
            'columns': {}
        }

        for table_name, table_symbol in self.tables.items():
            result['tables'][table_name] = {
                'name': table_symbol.name,
                'type': table_symbol.symbol_type.value
            }

        for table_name, columns in self.columns.items():
            result['columns'][table_name] = [
                {
                    'name': col.name,
                    'data_type': col.data_type,
                    'table_name': col.table_name,
                    'attributes': col.attributes
                }
                for col in columns
            ]

        return result