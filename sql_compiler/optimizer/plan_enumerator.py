from typing import List, Dict, Any, Set, Tuple, Optional
from itertools import permutations, combinations
import copy
from sql_compiler.codegen.operators import *
from sql_compiler.parser.ast_nodes import *
from sql_compiler.optimizer.cost_model import CostModel
from sql_compiler.optimizer.statistics import StatisticsManager


class PlanSpace:
    """执行计划空间"""

    def __init__(self, tables: List[str], join_conditions: List[Any],
                 filters: List[Any], projections: List[str]):
        self.tables = tables
        self.join_conditions = join_conditions
        self.filters = filters
        self.projections = projections
        self.table_aliases = {}  # 表别名映射


class PlanEnumerator:
    """智能执行计划枚举器"""

    def __init__(self, cost_model: CostModel, max_join_tables: int = 8):
        self.cost_model = cost_model
        self.max_join_tables = max_join_tables
        self.memo = {}  # 记忆化缓存

    def enumerate_plans(self, plan_space: PlanSpace) -> List[Tuple[Operator, float]]:
        """枚举所有可能的执行计划"""
        if len(plan_space.tables) > self.max_join_tables:
            # 表太多，使用贪心算法
            return self._greedy_enumeration(plan_space)
        else:
            # 使用动态规划进行完全枚举
            return self._dp_enumeration(plan_space)

    def _dp_enumeration(self, plan_space: PlanSpace) -> List[Tuple[Operator, float]]:
        """动态规划枚举（适用于小查询）"""
        tables = plan_space.tables
        n = len(tables)

        if n == 1:
            # 单表查询
            return self._enumerate_single_table_plans(plan_space)

        # 清空记忆化缓存
        self.memo = {}

        # 生成所有表的子集的最优计划
        all_plans = []

        # 从单表开始，逐步增加表的数量
        for subset_size in range(1, n + 1):
            for table_subset in combinations(tables, subset_size):
                table_set = frozenset(table_subset)

                if subset_size == 1:
                    # 单表访问路径
                    single_plans = self._generate_access_paths(table_subset[0], plan_space)
                    self.memo[table_set] = single_plans
                else:
                    # 多表连接
                    join_plans = self._generate_join_plans(table_set, plan_space)
                    self.memo[table_set] = join_plans

        # 返回完整查询的所有计划
        full_set = frozenset(tables)
        final_plans = self.memo.get(full_set, [])

        # 应用最终的投影和排序
        final_plans = self._apply_final_operations(final_plans, plan_space)

        return final_plans

    def _generate_access_paths(self, table: str, plan_space: PlanSpace) -> List[Tuple[Operator, float]]:
        """生成单表访问路径"""
        plans = []

        # 1. 全表扫描
        seq_scan = SeqScanOp(table)
        cost_info = self.cost_model.calculate_cost(seq_scan)
        plans.append((seq_scan, cost_info['total_cost']))

        # 2. 索引扫描（如果适用）
        table_filters = [f for f in plan_space.filters if self._filter_involves_table(f, table)]
        if table_filters:
            # 为每个可能有用的索引生成计划
            for filter_cond in table_filters:
                index_scan = IndexScanOp(table, index_name=f"idx_{table}_auto")
                cost_info = self.cost_model.calculate_cost(index_scan)
                plans.append((index_scan, cost_info['total_cost']))

        # 3. 应用表级过滤器
        filtered_plans = []
        for plan, cost in plans:
            for filter_cond in table_filters:
                filtered_plan = FilterOp(filter_cond, [plan])
                filtered_cost_info = self.cost_model.calculate_cost(filtered_plan)
                filtered_plans.append((filtered_plan, filtered_cost_info['total_cost']))

        # 保留原始扫描计划和过滤后的计划
        plans.extend(filtered_plans)

        # 按成本排序，只保留前几个最优计划
        plans.sort(key=lambda x: x[1])
        return plans[:5]  # 最多保留5个计划

    def _generate_join_plans(self, table_set: frozenset, plan_space: PlanSpace) -> List[Tuple[Operator, float]]:
        """生成连接计划"""
        plans = []

        # 枚举所有可能的二元分割
        for left_size in range(1, len(table_set)):
            for left_tables in combinations(table_set, left_size):
                left_set = frozenset(left_tables)
                right_set = table_set - left_set

                if len(right_set) == 0:
                    continue

                # 获取子计划
                left_plans = self.memo.get(left_set, [])
                right_plans = self.memo.get(right_set, [])

                # 找到连接这两个子集的连接条件
                join_conditions = self._find_join_conditions(left_set, right_set, plan_space.join_conditions)

                if not join_conditions:
                    continue  # 没有连接条件，跳过

                # 生成所有连接算法的组合
                for left_plan, left_cost in left_plans[:3]:  # 只考虑前3个最优的左子计划
                    for right_plan, right_cost in right_plans[:3]:  # 只考虑前3个最优的右子计划
                        for join_condition in join_conditions:
                            # 生成不同的连接算法
                            join_plans_for_condition = self._generate_join_algorithms(
                                left_plan, right_plan, join_condition
                            )
                            plans.extend(join_plans_for_condition)

        # 按成本排序并剪枝
        plans.sort(key=lambda x: x[1])
        return plans[:10]  # 最多保留10个最优计划

    def _generate_join_algorithms(self, left_plan: Operator, left_cost: float,
                                 right_plan: Operator, right_cost: float,
                                 join_condition: Expression) -> List[Tuple[Operator, float]]:
        """为给定的子计划生成不同的连接算法"""
        algorithms = []

        # 1. 嵌套循环连接
        nl_join = NestedLoopJoinOp("INNER", join_condition, [left_plan, right_plan])
        nl_cost = self.cost_model.calculate_cost(nl_join)['total_cost']
        algorithms.append((nl_join, nl_cost))

        # 2. 哈希连接
        hash_join = HashJoinOp("INNER", join_condition, [left_plan, right_plan])
        hash_cost = self.cost_model.calculate_cost(hash_join)['total_cost']
        algorithms.append((hash_join, hash_cost))

        # 3. 排序合并连接
        sm_join = SortMergeJoinOp("INNER", join_condition, [left_plan, right_plan])
        sm_cost = self.cost_model.calculate_cost(sm_join)['total_cost']
        algorithms.append((sm_join, sm_cost))

        # 4. 交换左右子树（对于某些连接算法有意义）
        nl_join_swapped = NestedLoopJoinOp("INNER", join_condition, [right_plan, left_plan])
        nl_cost_swapped = self.cost_model.calculate_cost(nl_join_swapped)['total_cost']
        algorithms.append((nl_join_swapped, nl_cost_swapped))

        # 5.  引嵌套循环连接
        right_table = self._extract_table_name(right_plan)
        if right_table:
            join_columns = self._extract_join_columns(join_condition, right_table)
            suitable_indexes = self._find_suitable_indexes(right_table, join_columns)

            for index_name in suitable_indexes:
                # 创建内表的索引扫描
                inner_index_scan = BTreeIndexScanOp(
                    table_name=right_table,
                    index_name=index_name,
                    scan_condition=join_condition
                )

                # 创建索引嵌套循环连接
                index_nl_join = IndexNestedLoopJoinOp(
                    join_type="INNER",
                    join_condition=join_condition,
                    outer_child=left_plan,
                    inner_index_scan=inner_index_scan
                )

                cost_info = self.cost_model.calculate_cost(index_nl_join)
                algorithms.append((index_nl_join, cost_info['total_cost']))

        return algorithms

    def _get_available_btree_indexes(self, table: str, plan_space: PlanSpace) -> List[Dict]:
        """获取表上可用的B+树索引"""
        # 从目录管理器获取索引信息
        if hasattr(plan_space, 'catalog_manager'):
            return plan_space.catalog_manager.get_table_indexes(table)
        return []

    def _is_covering_index(self, index_info: Dict, select_columns: List[str]) -> bool:
        """判断是否为覆盖索引"""
        if select_columns == ['*']:
            return False

        index_columns = set(index_info['columns'])
        required_columns = set(select_columns)

        return required_columns.issubset(index_columns)

    def _greedy_enumeration(self, plan_space: PlanSpace) -> List[Tuple[Operator, float]]:
        """贪心算法枚举（适用于大查询）"""
        # 使用贪心策略：每次选择成本最低的连接
        remaining_tables = set(plan_space.tables)
        current_plan = None
        current_cost = 0

        # 选择最佳的起始表
        best_start_table = self._choose_best_start_table(plan_space)
        access_plans = self._generate_access_paths(best_start_table, plan_space)
        current_plan, current_cost = min(access_plans, key=lambda x: x[1])
        remaining_tables.remove(best_start_table)

        # 逐步加入其他表
        while remaining_tables:
            best_join = None
            best_join_cost = float('inf')
            best_table = None

            for table in remaining_tables:
                # 尝试连接这个表
                table_plans = self._generate_access_paths(table, plan_space)
                for table_plan, _ in table_plans[:2]:  # 只考虑前2个最优计划

                    # 找连接条件
                    join_conditions = self._find_join_conditions(
                        frozenset([best_start_table]), frozenset([table]),
                        plan_space.join_conditions
                    )

                    if join_conditions:
                        for join_condition in join_conditions:
                            join_algorithms = self._generate_join_algorithms(
                                current_plan, table_plan, join_condition
                            )

                            for join_plan, join_cost in join_algorithms:
                                if join_cost < best_join_cost:
                                    best_join = join_plan
                                    best_join_cost = join_cost
                                    best_table = table

            if best_join is None:
                # 没有找到合适的连接，使用笛卡尔积
                remaining_table = remaining_tables.pop()
                table_plans = self._generate_access_paths(remaining_table, plan_space)
                table_plan, _ = min(table_plans, key=lambda x: x[1])

                cartesian_join = NestedLoopJoinOp("CROSS", None, [current_plan, table_plan])
                current_cost = self.cost_model.calculate_cost(cartesian_join)['total_cost']
                current_plan = cartesian_join
            else:
                current_plan = best_join
                current_cost = best_join_cost
                remaining_tables.remove(best_table)

        # 应用最终操作
        final_plans = self._apply_final_operations([(current_plan, current_cost)], plan_space)
        return final_plans

    def _choose_best_start_table(self, plan_space: PlanSpace) -> str:
        """选择最佳的起始表"""
        # 简单策略：选择有最多过滤条件的表
        table_filter_count = {}
        for table in plan_space.tables:
            table_filter_count[table] = sum(1 for f in plan_space.filters
                                            if self._filter_involves_table(f, table))

        return max(table_filter_count.keys(), key=lambda t: table_filter_count[t])

    def _find_join_conditions(self, left_set: frozenset, right_set: frozenset,
                              all_join_conditions: List[Any]) -> List[Any]:
        """找到连接两个表集合的连接条件"""
        applicable_conditions = []

        for condition in all_join_conditions:
            if self._condition_connects_sets(condition, left_set, right_set):
                applicable_conditions.append(condition)

        return applicable_conditions

    def _condition_connects_sets(self, condition: Any, left_set: frozenset, right_set: frozenset) -> bool:
        """检查条件是否连接两个表集合"""
        # 简化实现：假设条件涉及的表分别在两个集合中
        # 实际实现需要解析条件中涉及的表
        return True  # 简化假设

    def _filter_involves_table(self, filter_condition: Any, table: str) -> bool:
        """检查过滤条件是否涉及指定表"""
        # 简化实现：检查条件字符串是否包含表名
        return table in str(filter_condition)

    def _apply_final_operations(self, plans: List[Tuple[Operator, float]],
                                plan_space: PlanSpace) -> List[Tuple[Operator, float]]:
        """应用最终的投影、排序等操作"""
        final_plans = []

        for plan, cost in plans:
            current_plan = plan
            current_cost = cost

            # 应用投影
            if plan_space.projections and plan_space.projections != ['*']:
                project_op = ProjectOp(plan_space.projections, [current_plan])
                project_cost_info = self.cost_model.calculate_cost(project_op)
                current_plan = project_op
                current_cost = project_cost_info['total_cost']

            final_plans.append((current_plan, current_cost))

        return final_plans


class AdvancedPlanEnumerator(PlanEnumerator):
    """高级执行计划枚举器 - 支持更复杂的优化"""

    def __init__(self, cost_model: CostModel, max_join_tables: int = 12):
        super().__init__(cost_model, max_join_tables)
        self.bushy_trees = True  # 支持bushy树形结构
        self.star_join_optimization = True  # 星型连接优化

    def _generate_access_paths(self, table: str, plan_space: PlanSpace) -> List[Tuple[Operator, float]]:
        """生成单表访问路径 - 增强B+树索引支持"""
        plans = []

        # 1. 全表扫描
        seq_scan = SeqScanOp(table)
        cost_info = self.cost_model.calculate_cost(seq_scan)
        plans.append((seq_scan, cost_info['total_cost']))

        # 2. B+树索引扫描
        available_indexes = self._get_available_btree_indexes(table, plan_space)

        for index_info in available_indexes:
            index_name = index_info['name']
            applicable_conditions = self._find_applicable_conditions(
                index_info['columns'],
                plan_space.filters
            )

            if applicable_conditions:
                for condition in applicable_conditions:
                    # 普通B+树索引扫描
                    btree_scan = BTreeIndexScanOp(
                        table_name=table,
                        index_name=index_name,
                        scan_condition=condition,
                        is_covering_index=self._is_covering_index(index_info, plan_space.select_columns)
                    )

                    cost_info = self.cost_model.calculate_cost(btree_scan)
                    plans.append((btree_scan, cost_info['total_cost']))

                    # 如果是范围查询，考虑仅索引扫描
                    if self._is_range_condition(condition) and btree_scan.is_covering_index:
                        index_only_scan = IndexOnlyScanOp(
                            table_name=table,
                            index_name=index_name,
                            scan_condition=condition
                        )
                        cost_info = self.cost_model.calculate_cost(index_only_scan)
                        plans.append((index_only_scan, cost_info['total_cost']))

        return plans

    def enumerate_plans(self, plan_space: PlanSpace) -> List[Tuple[Operator, float]]:
        """高级计划枚举"""
        # 检查是否是星型查询
        if self.star_join_optimization and self._is_star_query(plan_space):
            return self._enumerate_star_join_plans(plan_space)

        # 检查是否启用bushy树
        if self.bushy_trees and len(plan_space.tables) <= 6:
            return self._enumerate_bushy_plans(plan_space)

        # 使用标准枚举
        return super().enumerate_plans(plan_space)

    def _is_star_query(self, plan_space: PlanSpace) -> bool:
        """检查是否为星型查询（一个中心表连接多个维度表）"""
        # 简化实现：如果有一个表与所有其他表都有连接条件，则认为是星型
        table_connections = {table: 0 for table in plan_space.tables}

        for condition in plan_space.join_conditions:
            # 简化：假设每个连接条件涉及两个表
            # 实际需要解析条件获取涉及的表
            for table in plan_space.tables:
                if self._filter_involves_table(condition, table):
                    table_connections[table] += 1

        # 如果有表的连接数等于其他表的数量，可能是中心表
        max_connections = max(table_connections.values())
        return max_connections >= len(plan_space.tables) - 1

    def _enumerate_star_join_plans(self, plan_space: PlanSpace) -> List[Tuple[Operator, float]]:
        """星型连接专用枚举"""
        # 找到中心表（连接最多的表）
        table_connections = {}
        for table in plan_space.tables:
            connections = sum(1 for cond in plan_space.join_conditions
                              if self._filter_involves_table(cond, table))
            table_connections[table] = connections

        fact_table = max(table_connections.keys(), key=lambda t: table_connections[t])
        dimension_tables = [t for t in plan_space.tables if t != fact_table]

        plans = []

        # 生成事实表的访问路径
        fact_plans = self._generate_access_paths(fact_table, plan_space)

        # 为每个事实表访问路径生成星型连接
        for fact_plan, fact_cost in fact_plans[:3]:
            current_plan = fact_plan
            current_cost = fact_cost

            # 逐个连接维度表
            for dim_table in dimension_tables:
                dim_plans = self._generate_access_paths(dim_table, plan_space)
                best_dim_plan, _ = min(dim_plans, key=lambda x: x[1])

                # 找连接条件
                join_conditions = self._find_join_conditions(
                    frozenset([fact_table]), frozenset([dim_table]),
                    plan_space.join_conditions
                )

                if join_conditions:
                    join_condition = join_conditions[0]

                    # 星型连接通常使用哈希连接
                    hash_join = HashJoinOp("INNER", join_condition, [current_plan, best_dim_plan])
                    join_cost_info = self.cost_model.calculate_cost(hash_join)

                    current_plan = hash_join
                    current_cost = join_cost_info['total_cost']

            plans.append((current_plan, current_cost))

        # 应用最终操作
        return self._apply_final_operations(plans, plan_space)

    def _enumerate_bushy_plans(self, plan_space: PlanSpace) -> List[Tuple[Operator, float]]:
        """枚举bushy树形计划"""
        # Bushy树允许更灵活的连接顺序，不限于左深度树
        # 这里实现一个简化版本

        tables = plan_space.tables
        if len(tables) <= 2:
            return super().enumerate_plans(plan_space)

        plans = []

        # 尝试不同的分组方式
        for partition_size in range(2, len(tables)):
            for left_partition in combinations(tables, partition_size):
                right_partition = [t for t in tables if t not in left_partition]

                if len(right_partition) < 2:
                    continue

                # 递归生成左右子树的计划
                left_space = PlanSpace(
                    list(left_partition),
                    [c for c in plan_space.join_conditions if self._condition_in_tables(c, left_partition)],
                    [f for f in plan_space.filters if self._filter_in_tables(f, left_partition)],
                    plan_space.projections
                )

                right_space = PlanSpace(
                    right_partition,
                    [c for c in plan_space.join_conditions if self._condition_in_tables(c, right_partition)],
                    [f for f in plan_space.filters if self._filter_in_tables(f, right_partition)],
                    plan_space.projections
                )

                left_plans = self.enumerate_plans(left_space)
                right_plans = self.enumerate_plans(right_space)

                # 连接左右子树
                for left_plan, left_cost in left_plans[:2]:
                    for right_plan, right_cost in right_plans[:2]:
                        # 找连接条件
                        join_conditions = self._find_join_conditions(
                            frozenset(left_partition), frozenset(right_partition),
                            plan_space.join_conditions
                        )

                        if join_conditions:
                            for join_condition in join_conditions:
                                join_algorithms = self._generate_join_algorithms(
                                    left_plan, right_plan, join_condition
                                )
                                plans.extend(join_algorithms)

        # 应用最终操作并返回最优计划
        final_plans = self._apply_final_operations(plans, plan_space)
        final_plans.sort(key=lambda x: x[1])

        return final_plans[:5]  # 返回前5个最优计划

    def _condition_in_tables(self, condition: Any, tables: List[str]) -> bool:
        """检查条件是否只涉及指定的表"""
        # 简化实现
        for table in tables:
            if self._filter_involves_table(condition, table):
                return True
        return False

    def _filter_in_tables(self, filter_cond: Any, tables: List[str]) -> bool:
        """检查过滤条件是否只涉及指定的表"""
        return self._condition_in_tables(filter_cond, tables)