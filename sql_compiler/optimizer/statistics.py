"""
生产级统计信息管理系统
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import json
import pickle
import threading
from collections import defaultdict
import math
from sql_compiler.parser.ast_nodes import Expression,BinaryExpr

@dataclass
class Histogram:
    """直方图统计信息"""
    buckets: List[Tuple[Any, Any, int]]  # (min_val, max_val, count)
    total_rows: int

    def estimate_selectivity(self, operator: str, value: Any) -> float:
        """基于直方图估算选择率"""
        if not self.buckets:
            return 0.1  # 默认选择率

        total_qualifying = 0

        for min_val, max_val, count in self.buckets:
            try:
                if operator == '=':
                    # 等值查询：假设值在桶内均匀分布
                    if min_val <= value <= max_val:
                        if min_val == max_val:
                            return count / self.total_rows
                        else:
                            # 估算桶内选择率
                            bucket_selectivity = 1.0 / (max_val - min_val + 1)
                            total_qualifying += count * bucket_selectivity

                elif operator in ['<', '<=']:
                    if value >= max_val:
                        total_qualifying += count
                    elif value > min_val:
                        # 部分桶内的行满足条件
                        ratio = (value - min_val) / (max_val - min_val)
                        total_qualifying += count * ratio

                elif operator in ['>', '>=']:
                    if value <= min_val:
                        total_qualifying += count
                    elif value < max_val:
                        ratio = (max_val - value) / (max_val - min_val)
                        total_qualifying += count * ratio

            except (TypeError, ZeroDivisionError):
                continue

        return min(1.0, total_qualifying / self.total_rows)


@dataclass
class ColumnStatistics:
    """列统计信息"""
    table_name: str
    column_name: str
    data_type: str
    distinct_values: int  # NDV - Number of Distinct Values
    null_count: int
    min_value: Any
    max_value: Any
    avg_length: float  # 平均长度（字符串类型）
    histogram: Optional[Histogram] = None
    most_frequent_values: List[Tuple[Any, int]] = field(default_factory=list)  # MFV

    @property
    def null_ratio(self) -> float:
        """空值比例"""
        return self.null_count / (self.distinct_values + self.null_count) if (
                                                                                         self.distinct_values + self.null_count) > 0 else 0

    @property
    def selectivity_per_value(self) -> float:
        """每个不同值的平均选择率"""
        return 1.0 / self.distinct_values if self.distinct_values > 0 else 0.5


@dataclass
class IndexStatistics:
    """索引统计信息"""
    table_name: str
    index_name: str
    columns: List[str]
    index_type: str  # B-Tree, Hash, Bitmap等
    unique: bool
    height: int  # B-Tree高度
    leaf_pages: int
    clustering_factor: float  # 聚簇因子
    last_analyzed: str


@dataclass
class TableStatistics:
    """表统计信息"""
    table_name: str
    row_count: int
    page_count: int
    avg_row_size: float
    empty_pages: int
    indexes: Dict[str, IndexStatistics] = field(default_factory=dict)
    last_analyzed: str = ""

    @property
    def density(self) -> float:
        """表密度（每页行数）"""
        return self.row_count / self.page_count if self.page_count > 0 else 1


class StatisticsManager:
    """统计信息管理器"""

    def __init__(self, stats_file: str = "optimizer_stats.pkl"):
        self.stats_file = stats_file
        self.table_stats: Dict[str, TableStatistics] = {}
        self.column_stats: Dict[str, Dict[str, ColumnStatistics]] = defaultdict(dict)
        self.index_stats: Dict[str, Dict[str, IndexStatistics]] = defaultdict(dict)
        self.join_stats: Dict[Tuple[str, str], float] = {}  # 表间连接选择率
        self._lock = threading.RLock()
        self.btree_stats: Dict[str, Dict[str, 'BTreeIndexStatistics']] = defaultdict(dict)

        self._load_statistics()

    def analyze_btree_index(self, index_name: str, table_name: str, btree: 'BPlusTreeIndex'):
        """分析B+树索引统计信息"""
        with self._lock:
            # 计算B+树高度和页数
            height = self._calculate_btree_height(btree)
            leaf_pages = self._estimate_leaf_pages(btree)

            # 计算聚簇因子（数据的物理有序性）
            clustering_factor = self._calculate_clustering_factor(table_name, index_name)

            index_stats = IndexStatistics(
                table_name=table_name,
                index_name=index_name,
                columns=self._get_index_columns(index_name),
                index_type="BTREE",
                unique=self._is_unique_index(index_name),
                height=height,
                leaf_pages=leaf_pages,
                clustering_factor=clustering_factor,
                last_analyzed=self._get_current_time()
            )

            self.index_stats[table_name][index_name] = index_stats
            self._save_statistics()

    def get_btree_selectivity(self, index_name: str, condition: Expression) -> float:
        """计算B+树索引的选择率"""
        # 根据条件类型计算选择率
        if isinstance(condition, BinaryExpr):
            operator = condition.operator
            if operator == '=':
                return 0.001  # 等值查询选择率很低
            elif operator in ['<', '>', '<=', '>=']:
                return 0.1  # 范围查询选择率适中
            elif operator in ['LIKE']:
                return 0.05  # 模糊查询选择率
        return 0.5  # 默认选择率

    def estimate_index_pages_accessed(self, index_name: str, selectivity: float) -> int:
        """估算索引访问的页数"""
        table_name = self._get_table_by_index(index_name)
        index_stats = self.index_stats[table_name].get(index_name)

        if index_stats:
            # 内部节点访问：等于树的高度
            internal_pages = index_stats.height - 1

            # 叶子节点访问：根据选择率估算
            leaf_pages_accessed = max(1, int(index_stats.leaf_pages * selectivity))

            return internal_pages + leaf_pages_accessed

        return 10  # 默认值

    def _calculate_btree_height(self, btree: 'BPlusTreeIndex') -> int:
        """计算B+树高度"""
        if btree.root is None:
            return 0

        height = 1
        current = btree.root
        while not current.is_leaf:
            height += 1
            if current.values:  # 有子节点
                current = current.values[0]
            else:
                break

        return height

    def analyze_table(self, table_name: str, sample_data: List[Dict] = None):
        """分析表，生成统计信息"""
        with self._lock:
            if sample_data is None:
                # 在真实系统中，这里会从存储引擎获取数据
                sample_data = self._get_sample_data(table_name)

            if not sample_data:
                return

            # 生成表级统计
            table_stats = self._analyze_table_level(table_name, sample_data)
            self.table_stats[table_name] = table_stats

            # 生成列级统计
            for column_name in sample_data[0].keys():
                column_stats = self._analyze_column_level(table_name, column_name, sample_data)
                self.column_stats[table_name][column_name] = column_stats

            self._save_statistics()

    def _analyze_table_level(self, table_name: str, sample_data: List[Dict]) -> TableStatistics:
        """分析表级统计信息"""
        row_count = len(sample_data)
        avg_row_size = sum(len(str(row)) for row in sample_data) / row_count if row_count > 0 else 0
        page_count = max(1, row_count // 100)  # 假设每页100行

        return TableStatistics(
            table_name=table_name,
            row_count=row_count,
            page_count=page_count,
            avg_row_size=avg_row_size,
            empty_pages=0,
            last_analyzed=self._get_current_time()
        )

    def _analyze_column_level(self, table_name: str, column_name: str, sample_data: List[Dict]) -> ColumnStatistics:
        """分析列级统计信息"""
        values = [row.get(column_name) for row in sample_data]
        non_null_values = [v for v in values if v is not None]

        if not non_null_values:
            return ColumnStatistics(
                table_name=table_name,
                column_name=column_name,
                data_type="unknown",
                distinct_values=0,
                null_count=len(values),
                min_value=None,
                max_value=None,
                avg_length=0
            )

        # 基本统计
        distinct_values = len(set(non_null_values))
        null_count = len(values) - len(non_null_values)
        min_value = min(non_null_values)
        max_value = max(non_null_values)

        # 数据类型推断
        data_type = self._infer_data_type(non_null_values[0])

        # 平均长度（字符串类型）
        avg_length = sum(len(str(v)) for v in non_null_values) / len(non_null_values)

        # 生成直方图
        histogram = self._create_histogram(non_null_values, distinct_values)

        # 最频繁值
        value_counts = defaultdict(int)
        for v in non_null_values:
            value_counts[v] += 1

        most_frequent_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)[:10]

        return ColumnStatistics(
            table_name=table_name,
            column_name=column_name,
            data_type=data_type,
            distinct_values=distinct_values,
            null_count=null_count,
            min_value=min_value,
            max_value=max_value,
            avg_length=avg_length,
            histogram=histogram,
            most_frequent_values=most_frequent_values
        )

    def _create_histogram(self, values: List[Any], distinct_count: int, bucket_count: int = 10) -> Histogram:
        """创建直方图"""
        if distinct_count <= bucket_count:
            # 值较少，每个值一个桶
            value_counts = defaultdict(int)
            for v in values:
                value_counts[v] += 1

            buckets = [(v, v, count) for v, count in value_counts.items()]
        else:
            # 等频直方图
            sorted_values = sorted(values)
            bucket_size = len(sorted_values) // bucket_count
            buckets = []

            for i in range(bucket_count):
                start_idx = i * bucket_size
                end_idx = start_idx + bucket_size if i < bucket_count - 1 else len(sorted_values)

                bucket_values = sorted_values[start_idx:end_idx]
                if bucket_values:
                    buckets.append((bucket_values[0], bucket_values[-1], len(bucket_values)))

        return Histogram(buckets=buckets, total_rows=len(values))

    def estimate_selectivity(self, table_name: str, column_name: str, operator: str, value: Any) -> float:
        """估算选择率"""
        with self._lock:
            if table_name not in self.column_stats or column_name not in self.column_stats[table_name]:
                return self._default_selectivity(operator)

            col_stats = self.column_stats[table_name][column_name]

            # 使用直方图估算
            if col_stats.histogram:
                return col_stats.histogram.estimate_selectivity(operator, value)

            # 使用基本统计信息
            if operator == '=':
                return col_stats.selectivity_per_value
            elif operator in ['<', '<=', '>', '>=']:
                return 0.33  # 范围查询默认选择率
            else:
                return 0.1

    def estimate_join_selectivity(self, left_table: str, right_table: str,
                                  left_column: str, right_column: str) -> float:
        """估算连接选择率"""
        # 检查是否有预计算的连接统计
        join_key = (f"{left_table}.{left_column}", f"{right_table}.{right_column}")
        if join_key in self.join_stats:
            return self.join_stats[join_key]

        # 使用列统计信息估算
        left_col_stats = self.column_stats.get(left_table, {}).get(left_column)
        right_col_stats = self.column_stats.get(right_table, {}).get(right_column)

        if left_col_stats and right_col_stats:
            # 连接选择率 = 1 / max(left_ndv, right_ndv)
            max_distinct = max(left_col_stats.distinct_values, right_col_stats.distinct_values)
            return 1.0 / max_distinct if max_distinct > 0 else 0.1

        return 0.1  # 默认连接选择率

    def get_table_stats(self, table_name: str) -> Optional[TableStatistics]:
        """获取表统计信息"""
        with self._lock:
            return self.table_stats.get(table_name)

    def get_column_stats(self, table_name: str, column_name: str) -> Optional[ColumnStatistics]:
        """获取列统计信息"""
        with self._lock:
            return self.column_stats.get(table_name, {}).get(column_name)

    def _default_selectivity(self, operator: str) -> float:
        """默认选择率"""
        defaults = {
            '=': 0.1,
            '<>': 0.9,
            '<': 0.33,
            '>': 0.33,
            '<=': 0.33,
            '>=': 0.33,
            'LIKE': 0.2,
            'IN': 0.3
        }
        return defaults.get(operator, 0.1)

    def _infer_data_type(self, value: Any) -> str:
        """推断数据类型"""
        if isinstance(value, int):
            return "INT"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, str):
            return "VARCHAR"
        elif isinstance(value, bool):
            return "BOOLEAN"
        else:
            return "UNKNOWN"

    def _get_sample_data(self, table_name: str) -> List[Dict]:
        """获取样本数据（在真实系统中从存储引擎获取）"""
        # 这里返回模拟数据，实际中会从存储引擎采样
        return []

    def _get_current_time(self) -> str:
        """获取当前时间"""
        import datetime
        return datetime.datetime.now().isoformat()

    def _save_statistics(self):
        """保存统计信息到文件"""
        try:
            stats_data = {
                'table_stats': self.table_stats,
                'column_stats': dict(self.column_stats),
                'index_stats': dict(self.index_stats),
                'join_stats': self.join_stats
            }
            with open(self.stats_file, 'wb') as f:
                pickle.dump(stats_data, f)
        except Exception as e:
            print(f"保存统计信息失败: {e}")

    def _load_statistics(self):
        """从文件加载统计信息"""
        try:
            with open(self.stats_file, 'rb') as f:
                stats_data = pickle.load(f)
                self.table_stats = stats_data.get('table_stats', {})
                self.column_stats = defaultdict(dict, stats_data.get('column_stats', {}))
                self.index_stats = defaultdict(dict, stats_data.get('index_stats', {}))
                self.join_stats = stats_data.get('join_stats', {})
        except (FileNotFoundError, Exception):
            # 文件不存在或损坏，使用空统计信息
            pass