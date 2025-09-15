"""
预读策略实现 - 包含多种预读策略的具体实现
"""

import time
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Set
from collections import deque

from ...utils.logger import get_logger
from .preread_config import PrereadConfig, PrereadMode
from .preread_detector import AccessPatternDetector, AccessPattern


class PrereadRequest:
    """预读请求对象"""

    def __init__(self, page_ids: List[int], priority: int = 1,
                 strategy_name: str = "unknown", table_name: str = None):
        self.page_ids = page_ids
        self.priority = priority  # 1-5，数字越大优先级越高
        self.strategy_name = strategy_name
        self.table_name = table_name
        self.created_time = time.time()
        self.executed = False

    def __lt__(self, other):
        """支持优先队列排序"""
        return self.priority > other.priority  # 优先级高的排在前面


class BasePrereadStrategy(ABC):
    """预读策略基类"""

    def __init__(self, name: str, config: PrereadConfig):
        self.name = name
        self.config = config
        self.logger = get_logger(f"preread_{name}")

        # 统计信息
        self.requests_generated = 0
        self.pages_preread = 0
        self.hits = 0  # 预读命中次数
        self.misses = 0  # 预读未命中次数
        self.last_request_time = 0

        self.logger.info(f"PrereadStrategy '{name}' initialized")

    @abstractmethod
    def should_preread(self, page_id: int, table_name: str = None,
                       access_pattern: AccessPattern = AccessPattern.UNKNOWN) -> bool:
        """判断是否应该执行预读"""
        pass

    @abstractmethod
    def generate_preread_request(self, page_id: int, table_name: str = None,
                                 detector: AccessPatternDetector = None) -> Optional[PrereadRequest]:
        """生成预读请求"""
        pass

    def record_hit(self):
        """记录预读命中"""
        self.hits += 1

    def record_miss(self):
        """记录预读未命中"""
        self.misses += 1

    def get_hit_rate(self) -> float:
        """获取命中率"""
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0

    def get_statistics(self) -> Dict[str, Any]:
        """获取策略统计信息"""
        return {
            'name': self.name,
            'requests_generated': self.requests_generated,
            'pages_preread': self.pages_preread,
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': self.get_hit_rate(),
            'last_request_time': self.last_request_time
        }


class SequentialPrereadStrategy(BasePrereadStrategy):
    """顺序预读策略 - 检测顺序访问并预读后续页面"""

    def __init__(self, config: PrereadConfig):
        super().__init__("sequential", config)

        # 顺序访问历史
        self.access_history = deque(maxlen=10)  # 保存最近10次访问
        self.sequential_sequences = []  # 检测到的顺序序列

        # 配置参数
        self.window_size = config.sequential_window_size
        self.trigger_count = config.sequential_trigger_count

        # 状态跟踪
        self.last_page = None
        self.sequential_count = 0  # 连续顺序访问计数

    def should_preread(self, page_id: int, table_name: str = None,
                       access_pattern: AccessPattern = AccessPattern.UNKNOWN) -> bool:
        """判断是否应该执行顺序预读"""

        # 更新访问历史
        self.access_history.append((page_id, time.time()))

        # 检测顺序访问
        if self.last_page is not None:
            if page_id == self.last_page + 1:
                self.sequential_count += 1
            else:
                self.sequential_count = 0

        self.last_page = page_id

        # 判断是否触发预读
        should_preread = (
                self.sequential_count >= self.trigger_count or
                access_pattern == AccessPattern.SEQUENTIAL
        )

        if should_preread:
            self.logger.debug(f"Sequential preread triggered: page={page_id}, count={self.sequential_count}")

        return should_preread

    def generate_preread_request(self, page_id: int, table_name: str = None,
                                 detector: AccessPatternDetector = None) -> Optional[PrereadRequest]:
        """生成顺序预读请求"""

        if not self.should_preread(page_id, table_name):
            return None

        # 计算预读页面
        preread_pages = []

        # 基础策略：预读后续连续页面
        for i in range(1, self.window_size + 1):
            next_page = page_id + i
            preread_pages.append(next_page)

        # 如果有检测器，使用其预测结果优化预读列表
        if detector:
            predicted_pages = detector.predict_next_pages(page_id, table_name, self.window_size)
            # 合并预测结果，优先使用预测的页面
            combined_pages = predicted_pages + preread_pages
            # 去重，保持顺序
            seen = set()
            preread_pages = []
            for p in combined_pages:
                if p not in seen and len(preread_pages) < self.window_size:
                    preread_pages.append(p)
                    seen.add(p)

        # 限制预读页面数量
        preread_pages = preread_pages[:self.config.max_preread_pages]

        if preread_pages:
            self.requests_generated += 1
            self.last_request_time = time.time()

            request = PrereadRequest(
                page_ids=preread_pages,
                priority=3,  # 中等优先级
                strategy_name=self.name,
                table_name=table_name
            )

            self.logger.debug(f"Generated sequential preread request: {preread_pages}")
            return request

        return None


class TableAwarePrereadStrategy(BasePrereadStrategy):
    """表感知预读策略 - 基于表的特性进行预读"""

    def __init__(self, config: PrereadConfig):
        super().__init__("table_aware", config)

        # 表级访问模式
        self.table_patterns: Dict[str, Dict] = {}

        # 表级配置
        self.table_configs = config.table_specific_config

    def should_preread(self, page_id: int, table_name: str = None,
                       access_pattern: AccessPattern = AccessPattern.UNKNOWN) -> bool:
        """基于表特性判断是否预读"""

        if not table_name or table_name == "unknown":
            return False

        # 获取表的配置
        table_config = self.config.get_table_config(table_name)
        aggressiveness = table_config.get('aggressiveness', 0.5)

        # 更新表的访问模式
        if table_name not in self.table_patterns:
            self.table_patterns[table_name] = {
                'access_count': 0,
                'last_access_time': 0,
                'frequent_pages': set(),
                'access_frequency': {}
            }

        pattern = self.table_patterns[table_name]
        pattern['access_count'] += 1
        pattern['last_access_time'] = time.time()
        pattern['access_frequency'][page_id] = pattern['access_frequency'].get(page_id, 0) + 1

        # 降低触发阈值（修改这里）
        # 策略1：基于访问频率 - 降低阈值从2到1
        if pattern['access_frequency'][page_id] >= 1:
            return True

        # 策略2：基于表的激进程度 - 降低阈值
        if aggressiveness > 0.5:  # 从0.7降低到0.5
            return True

        # 策略3：基于访问模式
        if access_pattern in [AccessPattern.SEQUENTIAL, AccessPattern.HOTSPOT]:
            return True

        # 新增：如果表有足够访问次数就触发
        if pattern['access_count'] >= 3:
            return True

        return False

    def generate_preread_request(self, page_id: int, table_name: str = None,
                                 detector: AccessPatternDetector = None) -> Optional[PrereadRequest]:
        """生成表感知预读请求"""

        if not self.should_preread(page_id, table_name):
            return None

        # 获取表配置
        table_config = self.config.get_table_config(table_name)
        preread_size = table_config.get('preread_size', 4)
        aggressiveness = table_config.get('aggressiveness', 0.5)

        preread_pages = []

        # 策略1：基于表的历史访问模式
        if table_name in self.table_patterns:
            pattern = self.table_patterns[table_name]

            # 获取该表最常访问的页面
            frequent_pages = sorted(pattern['access_frequency'].items(),
                                    key=lambda x: x[1], reverse=True)

            for p, count in frequent_pages[:preread_size]:
                if p != page_id:  # 排除当前页面
                    preread_pages.append(p)

        # 策略2：如果有检测器，结合预测结果
        if detector and len(preread_pages) < preread_size:
            predicted_pages = detector.predict_next_pages(page_id, table_name, preread_size)
            for p in predicted_pages:
                if p not in preread_pages and len(preread_pages) < preread_size:
                    preread_pages.append(p)

        # 策略3：如果还不够，预读相邻页面
        if len(preread_pages) < preread_size:
            for i in range(1, preread_size - len(preread_pages) + 1):
                next_page = page_id + i
                if next_page not in preread_pages:
                    preread_pages.append(next_page)

        # 根据激进程度调整优先级
        priority = int(2 + aggressiveness * 3)  # 2-5的优先级

        if preread_pages:
            self.requests_generated += 1
            self.last_request_time = time.time()

            request = PrereadRequest(
                page_ids=preread_pages[:self.config.max_preread_pages],
                priority=priority,
                strategy_name=self.name,
                table_name=table_name
            )

            self.logger.debug(f"Generated table-aware preread request for '{table_name}': {preread_pages}")
            return request

        return None

    def get_table_statistics(self, table_name: str) -> Optional[Dict]:
        """获取指定表的统计信息"""
        if table_name in self.table_patterns:
            pattern = self.table_patterns[table_name]
            return {
                'table_name': table_name,
                'access_count': pattern['access_count'],
                'last_access_time': pattern['last_access_time'],
                'unique_pages': len(pattern['access_frequency']),
                'most_frequent_pages': sorted(pattern['access_frequency'].items(),
                                              key=lambda x: x[1], reverse=True)[:5]
            }
        return None


class ExtentPrereadStrategy(BasePrereadStrategy):
    """基于区的预读策略 - 利用区的概念进行预读"""

    def __init__(self, config: PrereadConfig):
        super().__init__("extent_based", config)

        # 区级访问模式
        self.extent_access_patterns: Dict[int, Dict] = {}

        # 配置参数
        self.preread_ratio = config.extent_preread_ratio

        # 外部依赖（运行时注入）
        self.extent_manager = None

    def set_extent_manager(self, extent_manager):
        """设置区管理器引用"""
        self.extent_manager = extent_manager
        self.logger.debug("ExtentManager reference set")

    def should_preread(self, page_id: int, table_name: str = None,
                       access_pattern: AccessPattern = AccessPattern.UNKNOWN) -> bool:
        """基于区的访问模式判断是否预读"""

        if not self.extent_manager:
            return False

        # 检查页面是否属于某个区
        extent_id = self._get_extent_for_page(page_id)
        if extent_id is None:
            return False

        # 更新区的访问模式
        if extent_id not in self.extent_access_patterns:
            self.extent_access_patterns[extent_id] = {
                'access_count': 0,
                'pages_accessed': set(),
                'last_access_time': 0
            }

        pattern = self.extent_access_patterns[extent_id]
        pattern['access_count'] += 1
        pattern['pages_accessed'].add(page_id)
        pattern['last_access_time'] = time.time()

        # 降低触发阈值（修改这里）
        # 策略1：如果区内已有1个页面被访问，就可能需要预读更多（从2降低到1）
        if len(pattern['pages_accessed']) >= 1:
            return True

        # 策略2：基于访问模式
        if access_pattern == AccessPattern.SEQUENTIAL:
            return True

        # 新增：访问次数超过阈值就触发
        if pattern['access_count'] >= 2:
            return True

        return False

    def generate_preread_request(self, page_id: int, table_name: str = None,
                                 detector: AccessPatternDetector = None) -> Optional[PrereadRequest]:
        """生成基于区的预读请求"""

        if not self.should_preread(page_id, table_name) or not self.extent_manager:
            return None

        extent_id = self._get_extent_for_page(page_id)
        if extent_id is None:
            return None

        # 获取区的信息
        extent_info = self._get_extent_info(extent_id)
        if not extent_info:
            return None

        preread_pages = []

        # 策略1：预读区内的其他页面
        extent_pages = self._get_extent_pages(extent_id)
        accessed_pages = self.extent_access_patterns[extent_id]['pages_accessed']

        # 选择区内未访问的页面进行预读
        unaccessed_pages = [p for p in extent_pages if p not in accessed_pages and p != page_id]

        # 根据预读比例确定预读数量
        max_preread_count = max(1, int(len(extent_pages) * self.preread_ratio))
        preread_pages.extend(unaccessed_pages[:max_preread_count])

        # 策略2：如果区内页面不够，预读相邻区的页面
        if len(preread_pages) < self.config.max_preread_pages // 2:
            adjacent_pages = self._get_adjacent_extent_pages(extent_id)
            remaining_count = self.config.max_preread_pages - len(preread_pages)
            preread_pages.extend(adjacent_pages[:remaining_count])

        if preread_pages:
            self.requests_generated += 1
            self.last_request_time = time.time()

            request = PrereadRequest(
                page_ids=preread_pages[:self.config.max_preread_pages],
                priority=4,  # 较高优先级，因为基于区的预读通常更精确
                strategy_name=self.name,
                table_name=table_name
            )

            self.logger.debug(f"Generated extent-based preread request for extent {extent_id}: {preread_pages}")
            return request

        return None

    def _get_extent_for_page(self, page_id: int) -> Optional[int]:
        """获取页面所属的区ID"""
        if not self.extent_manager:
            return None

        # 使用区管理器的映射关系
        return self.extent_manager.page_to_extent.get(page_id)

    def _get_extent_info(self, extent_id: int) -> Optional[Dict]:
        """获取区的信息"""
        if not self.extent_manager or extent_id not in self.extent_manager.extents:
            return None

        extent = self.extent_manager.extents[extent_id]
        return {
            'extent_id': extent_id,
            'start_page': extent.start_page,
            'size': extent.size,
            'allocated_pages': len(extent.allocated_pages),
            'free_pages': extent.free_count
        }

    def _get_extent_pages(self, extent_id: int) -> List[int]:
        """获取区内的所有页面"""
        if not self.extent_manager or extent_id not in self.extent_manager.extents:
            return []

        extent = self.extent_manager.extents[extent_id]
        return list(extent.allocated_pages)

    def _get_adjacent_extent_pages(self, extent_id: int) -> List[int]:
        """获取相邻区的页面"""
        if not self.extent_manager:
            return []

        adjacent_pages = []

        # 简单策略：获取相邻extent_id的页面
        for adj_id in [extent_id - 1, extent_id + 1]:
            if adj_id in self.extent_manager.extents:
                adj_extent = self.extent_manager.extents[adj_id]
                adjacent_pages.extend(list(adj_extent.allocated_pages)[:2])  # 每个相邻区最多取2个页面

        return adjacent_pages

    def get_extent_statistics(self) -> Dict[str, Any]:
        """获取区级别的统计信息"""
        total_extents = len(self.extent_access_patterns)
        total_accesses = sum(pattern['access_count'] for pattern in self.extent_access_patterns.values())

        return {
            'total_extents_accessed': total_extents,
            'total_extent_accesses': total_accesses,
            'average_accesses_per_extent': total_accesses / max(total_extents, 1),
            'extent_details': {
                extent_id: {
                    'access_count': pattern['access_count'],
                    'pages_accessed': len(pattern['pages_accessed']),
                    'last_access_time': pattern['last_access_time']
                }
                for extent_id, pattern in self.extent_access_patterns.items()
            }
        }


def create_strategy(strategy_type: PrereadMode, config: PrereadConfig) -> BasePrereadStrategy:
    """
    策略工厂函数

    Args:
        strategy_type: 策略类型
        config: 预读配置

    Returns:
        BasePrereadStrategy: 对应的策略实例
    """
    if strategy_type == PrereadMode.SEQUENTIAL:
        return SequentialPrereadStrategy(config)
    elif strategy_type == PrereadMode.TABLE_AWARE:
        return TableAwarePrereadStrategy(config)
    elif strategy_type == PrereadMode.EXTENT_BASED:
        return ExtentPrereadStrategy(config)
    else:
        raise ValueError(f"Unsupported preread strategy: {strategy_type}")