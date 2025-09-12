from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import math
from sql_compiler.codegen.operators import *
from sql_compiler.optimizer.statistics import StatisticsManager


@dataclass
class SystemParameters:
    """系统参数配置"""
    # I/O 成本
    random_page_cost: float = 4.0  # 随机页读取成本
    seq_page_cost: float = 1.0  # 顺序页读取成本

    # CPU 成本
    cpu_tuple_cost: float = 0.01  # 处理每行的CPU成本
    cpu_index_tuple_cost: float = 0.005  # 处理索引行的CPU成本
    cpu_operator_cost: float = 0.0025  # 操作符处理成本

    # 内存相关
    work_mem: int = 4 * 1024 * 1024  # 工作内存大小（字节）
    effective_cache_size: int = 128 * 1024 * 1024  # 有效缓存大小

    # JOIN 相关
    hash_mem_multiplier: float = 2.0  # 哈希表内存乘数
    sort_mem_multiplier: float = 1.5  # 排序内存乘数


class CostModel:
    """精确的成本计算模型"""

    def __init__(self, stats_manager: StatisticsManager, params: SystemParameters = None):
        self.stats_manager = stats_manager
        self.params = params or SystemParameters()
        self.btree_params = {
            'btree_page_cost': 0.1,      # B+树页访问成本
            'btree_cpu_cost': 0.001,     # B+树CPU处理成本
            'index_correlation': 0.8,     # 索引与数据的相关性
        }

    def _cost_btree_index_scan(self, btree_scan_op: 'BTreeIndexScanOp') -> Dict[str, float]:
        """计算B+树索引扫描成本 - 精确模型"""
        table_stats = self.stats_manager.get_table_stats(btree_scan_op.table_name)
        index_stats = self.stats_manager.index_stats[btree_scan_op.table_name].get(btree_scan_op.index_name)

        if not table_stats or not index_stats:
            return self._fallback_index_cost()

        # 1. 计算选择率
        selectivity = self.stats_manager.get_btree_selectivity(
            btree_scan_op.index_name,
            btree_scan_op.scan_condition
        )

        # 2. 索引访问成本
        index_pages_accessed = self.stats_manager.estimate_index_pages_accessed(
            btree_scan_op.index_name,
            selectivity
        )
        index_io_cost = index_pages_accessed * self.btree_params['btree_page_cost']

        # 3. 表数据访问成本（如果不是覆盖索引）
        rows_returned = table_stats.row_count * selectivity

        if btree_scan_op.is_covering_index:
            # 覆盖索引，无需回表
            table_io_cost = 0
        else:
            # 需要回表，考虑聚簇因子
            clustering_factor = index_stats.clustering_factor
            if clustering_factor < 0.1:  # 数据高度有序
                pages_accessed = max(1, rows_returned / table_stats.density)
            else:  # 数据无序，可能每行都需要随机访问
                pages_accessed = min(rows_returned, table_stats.page_count)

            table_io_cost = pages_accessed * self.params.random_page_cost

        # 4. CPU 成本
        cpu_cost = (rows_returned * self.params.cpu_tuple_cost +
                    index_pages_accessed * self.btree_params['btree_cpu_cost'])

        # 5. 总成本
        startup_cost = index_stats.height * self.btree_params['btree_page_cost']
        total_cost = startup_cost + index_io_cost + table_io_cost + cpu_cost

        return {
            'startup_cost': startup_cost,
            'total_cost': total_cost,
            'rows': rows_returned,
            'width': table_stats.avg_row_size,
            'index_pages': index_pages_accessed,
            'table_pages': pages_accessed if not btree_scan_op.is_covering_index else 0
        }

    def _cost_index_nested_loop_join(self, join_op: 'IndexNestedLoopJoinOp') -> Dict[str, float]:
        """计算索引嵌套循环连接成本"""
        outer_cost = self.calculate_cost(join_op.children[0])

        # 内表每次通过索引查找的成本
        inner_index_cost = self._cost_btree_index_scan(join_op.inner_index_scan)

        # 外表每行都要在内表索引中查找
        total_inner_cost = outer_cost['rows'] * inner_index_cost['total_cost']

        join_selectivity = self._estimate_join_selectivity(join_op)
        output_rows = outer_cost['rows'] * inner_index_cost['rows'] * join_selectivity

        return {
            'startup_cost': outer_cost['startup_cost'] + inner_index_cost['startup_cost'],
            'total_cost': outer_cost['total_cost'] + total_inner_cost,
            'rows': output_rows,
            'width': outer_cost['width'] + inner_index_cost['width']
        }

    def calculate_cost(self, operator: Operator) -> Dict[str, float]:
        """计算操作符的详细成本"""
        if isinstance(operator, SeqScanOp):
            return self._cost_seq_scan(operator)
        elif isinstance(operator, IndexScanOp):
            return self._cost_index_scan(operator)
        elif isinstance(operator, FilterOp):
            return self._cost_filter(operator)
        elif isinstance(operator, ProjectOp):
            return self._cost_project(operator)
        elif isinstance(operator, JoinOp):
            return self._cost_join(operator)
        elif isinstance(operator, NestedLoopJoinOp):
            return self._cost_nested_loop_join(operator)
        elif isinstance(operator, HashJoinOp):
            return self._cost_hash_join(operator)
        elif isinstance(operator, SortMergeJoinOp):
            return self._cost_sort_merge_join(operator)
        elif isinstance(operator, GroupByOp):
            return self._cost_group_by(operator)
        elif isinstance(operator, OrderByOp):
            return self._cost_order_by(operator)
        else:
            return self._cost_generic(operator)

    def _cost_seq_scan(self, scan_op: SeqScanOp) -> Dict[str, float]:
        """计算全表扫描成本"""
        table_stats = self.stats_manager.get_table_stats(scan_op.table_name)
        if not table_stats:
            # 使用默认估算
            return {
                'startup_cost': 0.0,
                'total_cost': 1000.0,
                'rows': 1000,
                'width': 100
            }

        # I/O 成本：顺序读取所有页
        io_cost = table_stats.page_count * self.params.seq_page_cost

        # CPU 成本：处理所有行
        cpu_cost = table_stats.row_count * self.params.cpu_tuple_cost

        total_cost = io_cost + cpu_cost

        return {
            'startup_cost': 0.0,
            'total_cost': total_cost,
            'rows': table_stats.row_count,
            'width': table_stats.avg_row_size
        }

    def _cost_index_scan(self, index_scan_op: IndexScanOp) -> Dict[str, float]:
        """计算索引扫描成本"""
        table_stats = self.stats_manager.get_table_stats(index_scan_op.table_name)
        if not table_stats:
            return {
                'startup_cost': 0.0,
                'total_cost': 100.0,
                'rows': 100,
                'width': 100
            }

        # 假设索引选择率为10%
        index_selectivity = 0.1
        rows_fetched = table_stats.row_count * index_selectivity

        # 索引扫描成本
        index_pages = max(1, table_stats.page_count // 10)  # 假设索引占表的1/10
        index_io_cost = math.log2(index_pages) * self.params.random_page_cost

        # 随机页访问成本
        pages_fetched = min(table_stats.page_count, rows_fetched / table_stats.density)
        table_io_cost = pages_fetched * self.params.random_page_cost

        # CPU 成本
        cpu_cost = rows_fetched * self.params.cpu_index_tuple_cost

        total_cost = index_io_cost + table_io_cost + cpu_cost

        return {
            'startup_cost': index_io_cost,
            'total_cost': total_cost,
            'rows': rows_fetched,
            'width': table_stats.avg_row_size
        }

    def _cost_filter(self, filter_op: FilterOp) -> Dict[str, float]:
        """计算过滤操作成本"""
        child_cost = self.calculate_cost(filter_op.children[0])

        # 估算选择率（简化）
        selectivity = 0.1  # 可以根据具体条件进行更精确的估算

        # 过滤CPU成本
        filter_cpu_cost = child_cost['rows'] * self.params.cpu_operator_cost

        total_cost = child_cost['total_cost'] + filter_cpu_cost
        output_rows = child_cost['rows'] * selectivity

        return {
            'startup_cost': child_cost['startup_cost'],
            'total_cost': total_cost,
            'rows': output_rows,
            'width': child_cost['width']
        }

    def _cost_project(self, project_op: ProjectOp) -> Dict[str, float]:
        """计算投影操作成本"""
        child_cost = self.calculate_cost(project_op.children[0])

        # 投影CPU成本（很小）
        project_cpu_cost = child_cost['rows'] * self.params.cpu_operator_cost * 0.1

        # 估算输出行宽度
        width_factor = len(project_op.columns) / 10.0 if project_op.columns != ['*'] else 1.0
        output_width = child_cost['width'] * width_factor

        return {
            'startup_cost': child_cost['startup_cost'],
            'total_cost': child_cost['total_cost'] + project_cpu_cost,
            'rows': child_cost['rows'],
            'width': output_width
        }

    def _cost_nested_loop_join(self, join_op: NestedLoopJoinOp) -> Dict[str, float]:
        """计算嵌套循环连接成本"""
        outer_cost = self.calculate_cost(join_op.children[0])
        inner_cost = self.calculate_cost(join_op.children[1])

        # 嵌套循环：外表每行都要扫描内表
        inner_rescan_cost = outer_cost['rows'] * inner_cost['total_cost']

        # 连接条件处理成本
        join_cpu_cost = outer_cost['rows'] * inner_cost['rows'] * self.params.cpu_operator_cost

        # 输出行数（假设连接选择率为0.01）
        join_selectivity = 0.01
        output_rows = outer_cost['rows'] * inner_cost['rows'] * join_selectivity

        total_cost = outer_cost['total_cost'] + inner_rescan_cost + join_cpu_cost

        return {
            'startup_cost': outer_cost['startup_cost'] + inner_cost['startup_cost'],
            'total_cost': total_cost,
            'rows': output_rows,
            'width': outer_cost['width'] + inner_cost['width']
        }

    def _cost_hash_join(self, join_op: HashJoinOp) -> Dict[str, float]:
        """计算哈希连接成本"""
        outer_cost = self.calculate_cost(join_op.children[0])
        inner_cost = self.calculate_cost(join_op.children[1])

        # 选择较小的表作为构建表
        if outer_cost['rows'] <= inner_cost['rows']:
            build_cost = outer_cost
            probe_cost = inner_cost
        else:
            build_cost = inner_cost
            probe_cost = outer_cost

        # 构建哈希表的CPU成本
        build_cpu_cost = build_cost['rows'] * self.params.cpu_operator_cost * 2

        # 探测哈希表的CPU成本
        probe_cpu_cost = probe_cost['rows'] * self.params.cpu_operator_cost

        # 内存检查：如果哈希表太大，可能需要分批处理
        hash_table_size = build_cost['rows'] * build_cost['width'] * self.params.hash_mem_multiplier
        if hash_table_size > self.params.work_mem:
            # 需要分批，增加I/O成本
            batches = math.ceil(hash_table_size / self.params.work_mem)
            batch_penalty = batches * self.params.random_page_cost * 10
        else:
            batch_penalty = 0

        # 输出行数
        join_selectivity = 0.1  # 可以根据统计信息更精确计算
        output_rows = outer_cost['rows'] * inner_cost['rows'] * join_selectivity

        startup_cost = build_cost['total_cost'] + build_cpu_cost
        total_cost = startup_cost + probe_cost['total_cost'] + probe_cpu_cost + batch_penalty

        return {
            'startup_cost': startup_cost,
            'total_cost': total_cost,
            'rows': output_rows,
            'width': outer_cost['width'] + inner_cost['width']
        }

    def _cost_sort_merge_join(self, join_op: SortMergeJoinOp) -> Dict[str, float]:
        """计算排序合并连接成本"""
        left_cost = self.calculate_cost(join_op.children[0])
        right_cost = self.calculate_cost(join_op.children[1])

        # 排序成本
        left_sort_cost = self._cost_sort(left_cost['rows'], left_cost['width'])
        right_sort_cost = self._cost_sort(right_cost['rows'], right_cost['width'])

        # 合并成本
        merge_cpu_cost = (left_cost['rows'] + right_cost['rows']) * self.params.cpu_operator_cost

        # 输出行数
        join_selectivity = 0.1
        output_rows = left_cost['rows'] * right_cost['rows'] * join_selectivity

        startup_cost = left_cost['total_cost'] + right_cost['total_cost'] + left_sort_cost + right_sort_cost
        total_cost = startup_cost + merge_cpu_cost

        return {
            'startup_cost': startup_cost,
            'total_cost': total_cost,
            'rows': output_rows,
            'width': left_cost['width'] + right_cost['width']
        }

    def _cost_sort(self, rows: float, width: float) -> float:
        """计算排序成本"""
        if rows <= 1:
            return 0

        # 排序的CPU成本：O(n log n)
        cpu_cost = rows * math.log2(rows) * self.params.cpu_operator_cost

        # 内存使用
        sort_mem_needed = rows * width
        if sort_mem_needed > self.params.work_mem:
            # 需要外部排序，增加I/O成本
            passes = math.log2(sort_mem_needed / self.params.work_mem)
            io_cost = passes * rows * width / 8192 * self.params.seq_page_cost  # 假设页大小8KB
        else:
            io_cost = 0

        return cpu_cost + io_cost

    def _cost_group_by(self, group_op: GroupByOp) -> Dict[str, float]:
        """计算分组操作成本"""
        child_cost = self.calculate_cost(group_op.children[0])

        # 假设需要排序进行分组
        sort_cost = self._cost_sort(child_cost['rows'], child_cost['width'])

        # 分组处理CPU成本
        group_cpu_cost = child_cost['rows'] * self.params.cpu_operator_cost

        # 输出行数（假设有10%的不同组）
        output_rows = child_cost['rows'] * 0.1

        return {
            'startup_cost': child_cost['startup_cost'] + sort_cost,
            'total_cost': child_cost['total_cost'] + sort_cost + group_cpu_cost,
            'rows': output_rows,
            'width': child_cost['width']
        }

    def _cost_order_by(self, order_op: OrderByOp) -> Dict[str, float]:
        """计算排序操作成本"""
        child_cost = self.calculate_cost(order_op.children[0])

        # 排序成本
        sort_cost = self._cost_sort(child_cost['rows'], child_cost['width'])

        return {
            'startup_cost': child_cost['startup_cost'] + sort_cost,
            'total_cost': child_cost['total_cost'] + sort_cost,
            'rows': child_cost['rows'],
            'width': child_cost['width']
        }

    def _cost_generic(self, operator: Operator) -> Dict[str, float]:
        """通用操作符成本计算"""
        if not operator.children:
            return {
                'startup_cost': 0.0,
                'total_cost': 1.0,
                'rows': 1,
                'width': 50
            }

        total_startup = 0
        total_cost = 0
        total_rows = 0
        max_width = 0

        for child in operator.children:
            child_cost = self.calculate_cost(child)
            total_startup += child_cost['startup_cost']
            total_cost += child_cost['total_cost']
            total_rows += child_cost['rows']
            max_width = max(max_width, child_cost['width'])

        return {
            'startup_cost': total_startup,
            'total_cost': total_cost,
            'rows': total_rows,
            'width': max_width
        }

    def _cost_join(self, join_op: JoinOp) -> Dict[str, float]:
        """计算通用连接操作成本"""
        # 如果没有指定具体的连接算法，使用默认策略
        left_cost = self.calculate_cost(join_op.children[0])
        right_cost = self.calculate_cost(join_op.children[1])

        # 根据数据量选择最优的连接算法
        if left_cost['rows'] * right_cost['rows'] < 10000:
            # 小数据量使用嵌套循环
            return self._cost_nested_loop_join_generic(join_op, left_cost, right_cost)
        elif min(left_cost['rows'], right_cost['rows']) * left_cost['width'] < self.params.work_mem:
            # 可以放入内存时使用哈希连接
            return self._cost_hash_join_generic(join_op, left_cost, right_cost)
        else:
            # 大数据量使用排序合并连接
            return self._cost_sort_merge_join_generic(join_op, left_cost, right_cost)

    def _cost_nested_loop_join_generic(self, join_op: JoinOp, left_cost: Dict, right_cost: Dict) -> Dict[str, float]:
        """通用嵌套循环连接成本计算"""
        # 选择较小的表作为外表
        if left_cost['rows'] <= right_cost['rows']:
            outer_cost, inner_cost = left_cost, right_cost
        else:
            outer_cost, inner_cost = right_cost, left_cost

        # 嵌套循环成本计算
        inner_rescan_cost = outer_cost['rows'] * inner_cost['total_cost']
        join_cpu_cost = outer_cost['rows'] * inner_cost['rows'] * self.params.cpu_operator_cost

        # 估算连接选择率
        join_selectivity = self._estimate_join_selectivity(join_op)
        output_rows = outer_cost['rows'] * inner_cost['rows'] * join_selectivity

        total_cost = left_cost['total_cost'] + right_cost['total_cost'] + inner_rescan_cost + join_cpu_cost

        return {
            'startup_cost': left_cost['startup_cost'] + right_cost['startup_cost'],
            'total_cost': total_cost,
            'rows': output_rows,
            'width': left_cost['width'] + right_cost['width']
        }

    def _cost_hash_join_generic(self, join_op: JoinOp, left_cost: Dict, right_cost: Dict) -> Dict[str, float]:
        """通用哈希连接成本计算"""
        # 选择较小的表作为构建表
        if left_cost['rows'] <= right_cost['rows']:
            build_cost, probe_cost = left_cost, right_cost
        else:
            build_cost, probe_cost = right_cost, left_cost

        # 构建哈希表的CPU成本
        build_cpu_cost = build_cost['rows'] * self.params.cpu_operator_cost * 2

        # 探测哈希表的CPU成本
        probe_cpu_cost = probe_cost['rows'] * self.params.cpu_operator_cost

        # 内存检查
        hash_table_size = build_cost['rows'] * build_cost['width'] * self.params.hash_mem_multiplier
        if hash_table_size > self.params.work_mem:
            batches = math.ceil(hash_table_size / self.params.work_mem)
            batch_penalty = batches * self.params.random_page_cost * 10
        else:
            batch_penalty = 0

        # 估算连接选择率
        join_selectivity = self._estimate_join_selectivity(join_op)
        output_rows = left_cost['rows'] * right_cost['rows'] * join_selectivity

        startup_cost = build_cost['total_cost'] + build_cpu_cost
        total_cost = startup_cost + probe_cost['total_cost'] + probe_cpu_cost + batch_penalty

        return {
            'startup_cost': startup_cost,
            'total_cost': total_cost,
            'rows': output_rows,
            'width': left_cost['width'] + right_cost['width']
        }

    def _cost_sort_merge_join_generic(self, join_op: JoinOp, left_cost: Dict, right_cost: Dict) -> Dict[str, float]:
        """通用排序合并连接成本计算"""
        # 排序成本
        left_sort_cost = self._cost_sort(left_cost['rows'], left_cost['width'])
        right_sort_cost = self._cost_sort(right_cost['rows'], right_cost['width'])

        # 合并成本
        merge_cpu_cost = (left_cost['rows'] + right_cost['rows']) * self.params.cpu_operator_cost

        # 估算连接选择率
        join_selectivity = self._estimate_join_selectivity(join_op)
        output_rows = left_cost['rows'] * right_cost['rows'] * join_selectivity

        startup_cost = left_cost['total_cost'] + right_cost['total_cost'] + left_sort_cost + right_sort_cost
        total_cost = startup_cost + merge_cpu_cost

        return {
            'startup_cost': startup_cost,
            'total_cost': total_cost,
            'rows': output_rows,
            'width': left_cost['width'] + right_cost['width']
        }

    def _estimate_join_selectivity(self, join_op: JoinOp) -> float:
        """估算连接选择率"""
        # 这里可以根据连接条件进行更精确的估算
        # 目前使用简单的默认值
        if hasattr(join_op, 'join_condition') and join_op.join_condition:
            # 可以根据连接条件类型进行不同的估算
            return 0.1  # 等值连接的典型选择率
        else:
            return 0.01  # 笛卡尔积的低选择率