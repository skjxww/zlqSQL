"""
访问模式检测器 - 分析页面访问模式以优化预读策略
"""

import time
from typing import List, Dict, Optional, Tuple, Set
from collections import deque, defaultdict
from enum import Enum

from ...utils.logger import get_logger


class AccessPattern(Enum):
    """访问模式类型"""
    UNKNOWN = "unknown"  # 未知模式
    SEQUENTIAL = "sequential"  # 顺序访问
    RANDOM = "random"  # 随机访问
    HOTSPOT = "hotspot"  # 热点访问
    CYCLIC = "cyclic"  # 循环访问


class AccessRecord:
    """单次访问记录"""

    def __init__(self, page_id: int, table_name: str = None, access_type: str = "read"):
        self.page_id = page_id
        self.table_name = table_name or "unknown"
        self.access_type = access_type  # "read" or "write"
        self.timestamp = time.time()
        self.access_count = 1


class AccessPatternDetector:
    """访问模式检测器"""

    def __init__(self, window_size: int = 100, table_aware: bool = True):
        """
        初始化访问模式检测器

        Args:
            window_size: 分析窗口大小
            table_aware: 是否启用表感知分析
        """
        self.window_size = window_size
        self.table_aware = table_aware
        self.logger = get_logger("preread_detector")

        # 访问历史记录 - 使用deque保持固定窗口大小
        self.access_history = deque(maxlen=window_size)

        # 表级访问历史 - 如果启用表感知
        self.table_access_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))

        # 访问频率统计
        self.page_access_count: Dict[int, int] = defaultdict(int)
        self.table_page_access_count: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))

        # 顺序访问检测
        self.sequential_sequences: List[List[int]] = []  # 检测到的顺序序列
        self.current_sequence: List[int] = []  # 当前正在构建的序列

        # 热点检测
        self.hotspot_threshold = 5  # 热点页面的访问次数阈值
        self.hotspot_pages: Set[int] = set()

        # 统计信息
        self.total_accesses = 0
        self.pattern_stats = {
            AccessPattern.SEQUENTIAL: 0,
            AccessPattern.RANDOM: 0,
            AccessPattern.HOTSPOT: 0,
            AccessPattern.CYCLIC: 0,
            AccessPattern.UNKNOWN: 0
        }

        self.logger.info(f"AccessPatternDetector initialized (window_size={window_size}, table_aware={table_aware})")

    def record_access(self, page_id: int, table_name: str = None, access_type: str = "read"):
        """
        记录一次页面访问

        Args:
            page_id: 页号
            table_name: 表名
            access_type: 访问类型 ("read" 或 "write")
        """
        self.total_accesses += 1

        # 创建访问记录
        record = AccessRecord(page_id, table_name, access_type)

        # 添加到全局历史
        self.access_history.append(record)

        # 添加到表级历史（如果启用）
        if self.table_aware and table_name:
            self.table_access_history[table_name].append(record)

        # 更新访问计数
        self.page_access_count[page_id] += 1
        if table_name:
            self.table_page_access_count[table_name][page_id] += 1

        # 检测顺序访问
        self._detect_sequential_access(page_id)

        # 检测热点
        self._detect_hotspot(page_id)

        # 定期分析模式（每10次访问分析一次）
        if self.total_accesses % 10 == 0:
            self._analyze_patterns()

        self.logger.debug(f"Recorded access: page={page_id}, table={table_name}, type={access_type}")

    def _detect_sequential_access(self, page_id: int):
        """检测顺序访问模式"""
        if not self.current_sequence:
            self.current_sequence = [page_id]
            return

        last_page = self.current_sequence[-1]

        # 检查是否是连续的页面访问
        if page_id == last_page + 1:
            # 继续当前序列
            self.current_sequence.append(page_id)
        elif page_id == last_page:
            # 重复访问同一页面，不影响序列
            pass
        else:
            # 序列中断，保存当前序列（如果长度>=3）
            if len(self.current_sequence) >= 3:
                self.sequential_sequences.append(self.current_sequence.copy())
                self.pattern_stats[AccessPattern.SEQUENTIAL] += 1
                self.logger.debug(f"Detected sequential sequence: {self.current_sequence}")

            # 开始新序列
            self.current_sequence = [page_id]

        # 限制序列历史数量
        if len(self.sequential_sequences) > 20:
            self.sequential_sequences = self.sequential_sequences[-20:]

    def _detect_hotspot(self, page_id: int):
        """检测热点页面"""
        if self.page_access_count[page_id] >= self.hotspot_threshold:
            if page_id not in self.hotspot_pages:
                self.hotspot_pages.add(page_id)
                self.pattern_stats[AccessPattern.HOTSPOT] += 1
                self.logger.debug(f"Detected hotspot page: {page_id} (count={self.page_access_count[page_id]})")

    def _analyze_patterns(self):
        """分析访问模式"""
        if len(self.access_history) < 5:
            return

        recent_pages = [record.page_id for record in list(self.access_history)[-10:]]

        # 分析随机访问模式
        unique_pages = len(set(recent_pages))
        if unique_pages >= 8:  # 10个访问中有8个不同页面
            self.pattern_stats[AccessPattern.RANDOM] += 1

        # 分析循环访问模式
        if len(recent_pages) >= 6:
            self._detect_cyclic_pattern(recent_pages)

    def _detect_cyclic_pattern(self, pages: List[int]):
        """检测循环访问模式"""
        # 简单的循环检测：查找重复的子序列
        for cycle_len in range(2, len(pages) // 2 + 1):
            for start in range(len(pages) - 2 * cycle_len + 1):
                pattern1 = pages[start:start + cycle_len]
                pattern2 = pages[start + cycle_len:start + 2 * cycle_len]

                if pattern1 == pattern2:
                    self.pattern_stats[AccessPattern.CYCLIC] += 1
                    self.logger.debug(f"Detected cyclic pattern: {pattern1}")
                    return

    def predict_next_pages(self, current_page: int, table_name: str = None, count: int = 4) -> List[int]:
        """
        预测接下来可能访问的页面

        Args:
            current_page: 当前访问的页面
            table_name: 表名
            count: 预测的页面数量

        Returns:
            List[int]: 预测的页面号列表
        """
        predictions = []

        # 策略1：基于顺序访问模式预测
        sequential_predictions = self._predict_sequential(current_page, count)
        predictions.extend(sequential_predictions)

        # 策略2：基于历史访问模式预测
        if self.table_aware and table_name:
            table_predictions = self._predict_from_table_history(current_page, table_name, count)
            predictions.extend(table_predictions)

        # 策略3：基于热点页面预测
        hotspot_predictions = self._predict_hotspots(current_page, count)
        predictions.extend(hotspot_predictions)

        # 去重并限制数量
        unique_predictions = []
        seen = set([current_page])  # 排除当前页面

        for page_id in predictions:
            if page_id not in seen and len(unique_predictions) < count:
                unique_predictions.append(page_id)
                seen.add(page_id)

        return unique_predictions

    def _predict_sequential(self, current_page: int, count: int) -> List[int]:
        """基于顺序访问模式预测"""
        # 检查当前页面是否在某个顺序序列中
        for sequence in self.sequential_sequences[-5:]:  # 检查最近的5个序列
            if current_page in sequence:
                current_index = sequence.index(current_page)
                # 预测序列中的后续页面
                predictions = []
                for i in range(1, count + 1):
                    next_page = current_page + i
                    predictions.append(next_page)
                return predictions

        # 如果不在已知序列中，简单预测后续连续页面
        return [current_page + i for i in range(1, count + 1)]

    def _predict_from_table_history(self, current_page: int, table_name: str, count: int) -> List[int]:
        """基于表历史访问模式预测"""
        if table_name not in self.table_access_history:
            return []

        table_history = list(self.table_access_history[table_name])
        if len(table_history) < 5:
            return []

        # 查找当前页面在历史中的位置，预测后续访问的页面
        predictions = []
        for i, record in enumerate(table_history[:-1]):
            if record.page_id == current_page:
                # 查看历史上访问这个页面后通常访问什么页面
                next_record = table_history[i + 1]
                if next_record.page_id != current_page:  # 避免重复
                    predictions.append(next_record.page_id)

        # 返回最常见的后续页面
        if predictions:
            from collections import Counter
            common_pages = Counter(predictions).most_common(count)
            return [page_id for page_id, _ in common_pages]

        return []

    def _predict_hotspots(self, current_page: int, count: int) -> List[int]:
        """基于热点页面预测"""
        # 返回访问频率最高的页面（排除当前页面）
        sorted_pages = sorted(self.page_access_count.items(),
                              key=lambda x: x[1], reverse=True)

        hotspot_predictions = []
        for page_id, access_count in sorted_pages:
            if page_id != current_page and len(hotspot_predictions) < count:
                hotspot_predictions.append(page_id)

        return hotspot_predictions

    def get_current_pattern(self, table_name: str = None) -> AccessPattern:
        """获取当前主要的访问模式"""
        if not self.access_history:
            return AccessPattern.UNKNOWN

        # 分析最近的访问记录
        recent_records = list(self.access_history)[-10:]
        recent_pages = [r.page_id for r in recent_records]

        # 检查顺序访问
        sequential_count = 0
        for i in range(len(recent_pages) - 1):
            if recent_pages[i + 1] == recent_pages[i] + 1:
                sequential_count += 1

        if sequential_count >= 3:
            return AccessPattern.SEQUENTIAL

        # 检查热点访问
        if len(recent_records) >= 5:
            page_counts = {}
            for record in recent_records:
                page_counts[record.page_id] = page_counts.get(record.page_id, 0) + 1

            max_count = max(page_counts.values())
            if max_count >= 3:
                return AccessPattern.HOTSPOT

        # 检查随机访问
        unique_pages = len(set(recent_pages))
        if unique_pages >= len(recent_pages) * 0.8:
            return AccessPattern.RANDOM

        return AccessPattern.UNKNOWN

    def get_statistics(self) -> Dict:
        """获取访问模式统计信息"""
        return {
            'total_accesses': self.total_accesses,
            'window_size': self.window_size,
            'pattern_stats': {pattern.value: count for pattern, count in self.pattern_stats.items()},
            'sequential_sequences_count': len(self.sequential_sequences),
            'hotspot_pages_count': len(self.hotspot_pages),
            'unique_pages_accessed': len(self.page_access_count),
            'tables_tracked': len(self.table_access_history) if self.table_aware else 0,
            'current_pattern': self.get_current_pattern().value
        }

    def reset_statistics(self):
        """重置统计信息"""
        self.pattern_stats = {pattern: 0 for pattern in AccessPattern}
        self.sequential_sequences.clear()
        self.hotspot_pages.clear()
        self.total_accesses = 0
        self.logger.info("Access pattern statistics reset")