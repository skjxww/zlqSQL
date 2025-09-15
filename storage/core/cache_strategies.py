"""
缓存策略实现 - 支持LRU、FIFO和自适应策略
"""

from abc import ABC, abstractmethod
from collections import OrderedDict, deque
from typing import Optional, Tuple, Dict, Any
import time

from ..utils.constants import (
    ADAPTIVE_ANALYSIS_INTERVAL, ADAPTIVE_MIN_SWITCH_INTERVAL,
    ADAPTIVE_DECISION_THRESHOLD, REPEAT_ACCESS_THRESHOLD,
    SEQUENTIAL_ACCESS_THRESHOLD, CACHE_STRATEGY_LRU, CACHE_STRATEGY_FIFO
)
from ..utils.logger import get_logger


class CacheStrategy(ABC):
    """缓存策略抽象基类"""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.logger = get_logger("cache_strategy")

    @abstractmethod
    def get(self, key: int) -> Optional[Tuple[bytes, bool, float]]:
        """获取缓存项"""
        pass

    @abstractmethod
    def put(self, key: int, value: Tuple[bytes, bool, float]):
        """添加缓存项"""
        pass

    @abstractmethod
    def evict(self) -> Optional[Tuple[int, bytes, bool]]:
        """淘汰一个缓存项"""
        pass

    @abstractmethod
    def remove(self, key: int) -> Optional[Tuple[bytes, bool]]:
        """移除指定缓存项"""
        pass

    @abstractmethod
    def clear(self):
        """清空缓存"""
        pass

    @abstractmethod
    def __len__(self):
        """返回缓存大小"""
        pass

    @abstractmethod
    def __contains__(self, key):
        """检查是否包含指定键"""
        pass


class LRUStrategy(CacheStrategy):
    """LRU策略实现"""

    def __init__(self, capacity: int):
        super().__init__(capacity)
        self.cache = OrderedDict()

    def get(self, key: int) -> Optional[Tuple[bytes, bool, float]]:
        if key in self.cache:
            value = self.cache.pop(key)
            self.cache[key] = value  # 移到最后
            return value
        return None

    def put(self, key: int, value: Tuple[bytes, bool, float]):
        if key in self.cache:
            self.cache.pop(key)
        self.cache[key] = value

    def evict(self) -> Optional[Tuple[int, bytes, bool]]:
        if self.cache:
            key, (data, is_dirty, access_time) = self.cache.popitem(last=False)
            return key, data, is_dirty
        return None

    def remove(self, key: int) -> Optional[Tuple[bytes, bool]]:
        if key in self.cache:
            data, is_dirty, _ = self.cache.pop(key)
            return data, is_dirty
        return None

    def clear(self):
        self.cache.clear()

    def __len__(self):
        return len(self.cache)

    def __contains__(self, key):
        return key in self.cache


class FIFOStrategy(CacheStrategy):
    """FIFO策略实现"""

    def __init__(self, capacity: int):
        super().__init__(capacity)
        self.cache = {}  # {key: (data, is_dirty, access_time)}
        self.order = deque()  # 记录插入顺序

    def get(self, key: int) -> Optional[Tuple[bytes, bool, float]]:
        if key in self.cache:
            return self.cache[key]
        return None

    def put(self, key: int, value: Tuple[bytes, bool, float]):
        if key not in self.cache:
            self.order.append(key)
        self.cache[key] = value

    def evict(self) -> Optional[Tuple[int, bytes, bool]]:
        if self.order:
            key = self.order.popleft()
            data, is_dirty, _ = self.cache.pop(key)
            return key, data, is_dirty
        return None

    def remove(self, key: int) -> Optional[Tuple[bytes, bool]]:
        if key in self.cache:
            data, is_dirty, _ = self.cache.pop(key)
            # 从顺序队列中移除
            try:
                self.order.remove(key)
            except ValueError:
                pass  # key不在队列中
            return data, is_dirty
        return None

    def clear(self):
        self.cache.clear()
        self.order.clear()

    def __len__(self):
        return len(self.cache)

    def __contains__(self, key):
        return key in self.cache


class AccessPatternAnalyzer:
    """访问模式分析器"""

    def __init__(self):
        self.access_history = deque(maxlen=ADAPTIVE_ANALYSIS_INTERVAL)
        self.last_page_id = None
        self.repeat_count = 0
        self.sequential_count = 0
        self.total_accesses = 0

    def record_access(self, page_id: int):
        """记录一次访问"""
        self.total_accesses += 1

        # 检查是否重复访问
        if page_id in self.access_history:
            self.repeat_count += 1

        # 检查是否顺序访问
        if self.last_page_id is not None and page_id == self.last_page_id + 1:
            self.sequential_count += 1

        self.access_history.append(page_id)
        self.last_page_id = page_id

    def get_pattern_stats(self) -> Dict[str, float]:
        """获取访问模式统计"""
        if self.total_accesses == 0:
            return {
                'repeat_rate': 0.0,
                'sequential_rate': 0.0,
                'total_accesses': 0
            }

        repeat_rate = self.repeat_count / self.total_accesses
        sequential_rate = self.sequential_count / self.total_accesses

        return {
            'repeat_rate': repeat_rate,
            'sequential_rate': sequential_rate,
            'total_accesses': self.total_accesses
        }

    def reset_counters(self):
        """重置计数器（保留历史记录用于重复检测）"""
        self.repeat_count = 0
        self.sequential_count = 0
        self.total_accesses = 0


class AdaptiveStrategy(CacheStrategy):
    """自适应缓存策略"""

    def __init__(self, capacity: int):
        super().__init__(capacity)
        self.lru_strategy = LRUStrategy(capacity)
        self.fifo_strategy = FIFOStrategy(capacity)

        # 当前使用的策略
        self.current_strategy = self.lru_strategy
        self.current_strategy_name = CACHE_STRATEGY_LRU

        # 访问模式分析
        self.analyzer = AccessPatternAnalyzer()

        # 决策控制
        self.last_switch_time = 0  # 允许第一次切换
        self.decision_counter = 0
        self.consecutive_decisions = []

        self.logger.info(f"AdaptiveStrategy initialized with LRU as default")

    def _should_analyze(self) -> bool:
        """是否应该进行模式分析"""
        return self.analyzer.total_accesses > 0 and self.analyzer.total_accesses % ADAPTIVE_ANALYSIS_INTERVAL == 0

    def _analyze_and_decide(self):
        """分析访问模式并决定策略"""
        stats = self.analyzer.get_pattern_stats()

        # 简单的决策逻辑
        recommended_strategy = self._make_decision(stats)

        # 记录决策
        self.consecutive_decisions.append(recommended_strategy)
        if len(self.consecutive_decisions) > ADAPTIVE_DECISION_THRESHOLD:
            self.consecutive_decisions.pop(0)

        # 检查是否应该切换
        if self._should_switch_strategy(recommended_strategy):
            self._switch_strategy(recommended_strategy)

        # 重置分析器
        self.analyzer.reset_counters()

    def _make_decision(self, stats: Dict[str, float]) -> str:
        """基于统计数据做出策略决策"""
        repeat_rate = stats['repeat_rate']
        sequential_rate = stats['sequential_rate']

        if repeat_rate > REPEAT_ACCESS_THRESHOLD:
            return CACHE_STRATEGY_LRU
        elif sequential_rate > SEQUENTIAL_ACCESS_THRESHOLD:
            return CACHE_STRATEGY_FIFO
        else:
            return self.current_strategy_name

    def _should_switch_strategy(self, recommended_strategy: str) -> bool:

        # 如果推荐的策略就是当前策略，不切换
        if recommended_strategy == self.current_strategy_name:
            return False

        # 检查时间间隔
        current_time = time.time()
        time_diff = current_time - self.last_switch_time

        # 特殊处理：如果last_switch_time是初始化时间且从未切换过，允许第一次切换
        if time_diff < ADAPTIVE_MIN_SWITCH_INTERVAL and self.last_switch_time > 0:
            return False

        if len(self.consecutive_decisions) >= ADAPTIVE_DECISION_THRESHOLD:
            recent_decisions = self.consecutive_decisions[-ADAPTIVE_DECISION_THRESHOLD:]
            all_same = all(decision == recommended_strategy for decision in recent_decisions)

            if all_same:
                return True

        return False

    def _switch_strategy(self, new_strategy_name: str):
        """切换策略"""
        old_strategy_name = self.current_strategy_name

        # 迁移数据到新策略
        if new_strategy_name == CACHE_STRATEGY_LRU:
            new_strategy = LRUStrategy(self.capacity)
        else:  # FIFO
            new_strategy = FIFOStrategy(self.capacity)

        # 将当前缓存内容迁移到新策略
        for key in list(self.current_strategy.cache.keys() if hasattr(self.current_strategy, 'cache') else []):
            value = self.current_strategy.cache.get(key)
            if value:
                new_strategy.put(key, value)

        # 切换策略
        self.current_strategy = new_strategy
        self.current_strategy_name = new_strategy_name
        self.last_switch_time = time.time()

        self.logger.info(f"Switched cache strategy from {old_strategy_name} to {new_strategy_name}")

    def get(self, key: int) -> Optional[Tuple[bytes, bool, float]]:
        """获取缓存项"""
        self.analyzer.record_access(key)

        if self._should_analyze():
            self._analyze_and_decide()

        return self.current_strategy.get(key)

    def put(self, key: int, value: Tuple[bytes, bool, float]):
        """添加缓存项"""
        self.analyzer.record_access(key)  # 添加这一行

        if self._should_analyze():
            self._analyze_and_decide()

        return self.current_strategy.put(key, value)

    def evict(self) -> Optional[Tuple[int, bytes, bool]]:
        """淘汰一个缓存项"""
        return self.current_strategy.evict()

    def remove(self, key: int) -> Optional[Tuple[bytes, bool]]:
        """移除指定缓存项"""
        return self.current_strategy.remove(key)

    def clear(self):
        """清空缓存"""
        self.current_strategy.clear()
        self.analyzer = AccessPatternAnalyzer()

    def __len__(self):
        return len(self.current_strategy)

    def __contains__(self, key):
        return key in self.current_strategy

    def get_current_strategy(self) -> str:
        """获取当前使用的策略名称"""
        return self.current_strategy_name

    def get_strategy_stats(self) -> Dict[str, Any]:
        """获取策略统计信息"""
        pattern_stats = self.analyzer.get_pattern_stats()
        return {
            'current_strategy': self.current_strategy_name,
            'pattern_stats': pattern_stats,
            'consecutive_decisions': self.consecutive_decisions.copy(),
            'last_switch_time': self.last_switch_time
        }