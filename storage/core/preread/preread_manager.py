"""
预读管理器 - 预读系统的核心控制器
负责协调各种预读策略，管理预读执行，提供统一接口
"""

import time
import threading
from typing import Dict, List, Optional, Set, Any
from queue import PriorityQueue, Empty
from collections import defaultdict, deque

from ...utils.logger import get_logger
from ...utils.exceptions import StorageException
from .preread_config import PrereadConfig, PrereadMode
from .preread_detector import AccessPatternDetector, AccessPattern
from .preread_strategies import (
    BasePrereadStrategy, PrereadRequest,
    SequentialPrereadStrategy, TableAwarePrereadStrategy, ExtentPrereadStrategy,
    create_strategy
)


class PrereadStatistics:
    """预读统计信息管理"""

    def __init__(self):
        # 基础统计
        self.total_requests = 0
        self.total_preread_pages = 0
        self.successful_prereads = 0  # 预读后被实际访问的页面数
        self.wasted_prereads = 0  # 预读后未被访问的页面数

        # 策略级别统计
        self.strategy_stats = defaultdict(lambda: {
            'requests': 0,
            'pages': 0,
            'hits': 0,
            'misses': 0
        })

        # 时间统计
        self.start_time = time.time()
        self.last_reset_time = time.time()

        # 最近的预读记录 (用于计算命中率)
        self.recent_prereads: Dict[int, float] = {}  # {page_id: preread_time}
        self.recent_accesses: Set[int] = set()  # 最近访问的页面

        # 性能统计
        self.avg_preread_time = 0.0
        self.max_preread_time = 0.0
        self.preread_time_samples = deque(maxlen=100)

    def record_preread_request(self, request: PrereadRequest):
        """记录预读请求"""
        self.total_requests += 1
        self.total_preread_pages += len(request.page_ids)

        strategy_stat = self.strategy_stats[request.strategy_name]
        strategy_stat['requests'] += 1
        strategy_stat['pages'] += len(request.page_ids)

        # 记录预读的页面
        current_time = time.time()
        for page_id in request.page_ids:
            self.recent_prereads[page_id] = current_time

    def record_page_access(self, page_id: int):
        """记录页面访问（用于计算命中率）"""
        self.recent_accesses.add(page_id)

        # 检查是否是预读命中
        if page_id in self.recent_prereads:
            preread_time = self.recent_prereads[page_id]
            access_time = time.time()

            # 如果预读后5分钟内被访问，算作命中
            if access_time - preread_time <= 300:  # 5分钟
                self.successful_prereads += 1
                # 找到对应的策略并记录命中
                # (这里简化处理，实际可以维护更详细的映射)

            # 移除已处理的预读记录
            del self.recent_prereads[page_id]

    def record_preread_execution_time(self, execution_time: float):
        """记录预读执行时间"""
        self.preread_time_samples.append(execution_time)
        self.max_preread_time = max(self.max_preread_time, execution_time)

        if self.preread_time_samples:
            self.avg_preread_time = sum(self.preread_time_samples) / len(self.preread_time_samples)

    def cleanup_old_prereads(self):
        """清理过期的预读记录"""
        current_time = time.time()
        expired_pages = []

        for page_id, preread_time in self.recent_prereads.items():
            if current_time - preread_time > 300:  # 5分钟过期
                expired_pages.append(page_id)
                self.wasted_prereads += 1

        for page_id in expired_pages:
            del self.recent_prereads[page_id]

    def get_hit_rate(self) -> float:
        """计算总体命中率"""
        total_completed = self.successful_prereads + self.wasted_prereads
        return (self.successful_prereads / total_completed * 100) if total_completed > 0 else 0.0

    def get_statistics(self) -> Dict[str, Any]:
        """获取完整统计信息"""
        uptime = time.time() - self.start_time

        return {
            'uptime_seconds': uptime,
            'total_requests': self.total_requests,
            'total_preread_pages': self.total_preread_pages,
            'successful_prereads': self.successful_prereads,
            'wasted_prereads': self.wasted_prereads,
            'hit_rate': self.get_hit_rate(),
            'avg_preread_time': self.avg_preread_time,
            'max_preread_time': self.max_preread_time,
            'pending_prereads': len(self.recent_prereads),
            'strategy_stats': dict(self.strategy_stats),
            'requests_per_second': self.total_requests / max(uptime, 1)
        }

    def reset(self):
        """重置统计信息"""
        self.__init__()


class PrereadManager:
    """预读管理器 - 预读系统的核心控制器"""

    def __init__(self, storage_manager, config: PrereadConfig = None):
        """
        初始化预读管理器

        Args:
            storage_manager: 存储管理器引用
            config: 预读配置，如果为None则使用默认配置
        """
        self.storage_manager = storage_manager
        self.config = config or PrereadConfig()
        self.logger = get_logger("preread_manager")

        # 核心组件
        self.detector = AccessPatternDetector(
            window_size=100,
            table_aware=True
        )
        self.statistics = PrereadStatistics()

        # 策略管理
        self.strategies: Dict[str, BasePrereadStrategy] = {}
        self.active_strategy: Optional[BasePrereadStrategy] = None
        self._init_strategies()

        # 预读请求队列
        self.preread_queue = PriorityQueue()
        self.processing_pages: Set[int] = set()  # 正在处理的页面，避免重复预读

        # 状态管理
        self.enabled = self.config.enabled
        self.shutdown_flag = False
        self._lock = threading.RLock()

        # 性能控制
        self.last_cleanup_time = time.time()
        self.cleanup_interval = 60  # 60秒清理一次

        self.logger.info(f"PrereadManager initialized (mode={self.config.mode.value}, enabled={self.enabled})")

    def _init_strategies(self):
        """初始化预读策略"""
        try:
            # 创建所有支持的策略
            self.strategies['sequential'] = SequentialPrereadStrategy(self.config)
            self.strategies['table_aware'] = TableAwarePrereadStrategy(self.config)
            self.strategies['extent_based'] = ExtentPrereadStrategy(self.config)

            # 为区级策略设置区管理器引用
            if hasattr(self.storage_manager, 'extent_manager') and self.storage_manager.extent_manager:
                extent_strategy = self.strategies['extent_based']
                extent_strategy.set_extent_manager(self.storage_manager.extent_manager)
                self.logger.debug("ExtentManager reference set for extent-based strategy")

            # 设置活跃策略
            self._set_active_strategy(self.config.mode)

            self.logger.info(f"Initialized {len(self.strategies)} preread strategies")

        except Exception as e:
            self.logger.error(f"Failed to initialize strategies: {e}")
            self.enabled = False

    def _set_active_strategy(self, mode: PrereadMode):
        """设置活跃的预读策略"""
        if mode == PrereadMode.DISABLED:
            self.active_strategy = None
            return

        strategy_map = {
            PrereadMode.SEQUENTIAL: 'sequential',
            PrereadMode.TABLE_AWARE: 'table_aware',
            PrereadMode.EXTENT_BASED: 'extent_based',
            PrereadMode.ADAPTIVE: 'sequential'  # 自适应模式默认从顺序开始
        }

        strategy_name = strategy_map.get(mode)
        if strategy_name and strategy_name in self.strategies:
            self.active_strategy = self.strategies[strategy_name]
            self.logger.info(f"Active strategy set to: {strategy_name}")
        else:
            self.logger.warning(f"Strategy not found for mode {mode}, disabling preread")
            self.active_strategy = None

    def on_page_access(self, page_id: int, table_name: str = None, access_type: str = "read"):
        """
        处理页面访问事件 - 这是预读系统的主要入口点

        Args:
            page_id: 访问的页面ID
            table_name: 表名
            access_type: 访问类型 ("read" 或 "write")
        """
        if not self.enabled or self.shutdown_flag:
            return

        try:
            with self._lock:
                # 记录访问到检测器
                self.detector.record_access(page_id, table_name, access_type)

                # 记录到统计
                self.statistics.record_page_access(page_id)

                # 分析当前访问模式
                current_pattern = self.detector.get_current_pattern(table_name)

                # 自适应策略：根据访问模式动态切换策略
                if self.config.mode == PrereadMode.ADAPTIVE:
                    self._adaptive_strategy_selection(current_pattern, table_name)

                # 生成预读请求
                self._generate_preread_request(page_id, table_name, current_pattern)

                # 执行预读
                self._process_preread_queue()

                # 定期清理
                self._periodic_cleanup()

        except Exception as e:
            self.logger.error(f"Error in on_page_access: {e}")

    def _adaptive_strategy_selection(self, pattern: AccessPattern, table_name: str = None):
        """自适应策略选择"""
        current_strategy_name = self.active_strategy.name if self.active_strategy else None
        new_strategy_name = None

        # 根据访问模式选择最适合的策略
        if pattern == AccessPattern.SEQUENTIAL:
            new_strategy_name = 'sequential'
        elif pattern == AccessPattern.HOTSPOT and table_name:
            new_strategy_name = 'table_aware'
        elif pattern in [AccessPattern.RANDOM, AccessPattern.CYCLIC]:
            # 对于随机访问，如果有区管理器就用区级策略
            if 'extent_based' in self.strategies and self.storage_manager.extent_manager:
                new_strategy_name = 'extent_based'
            else:
                new_strategy_name = 'table_aware'

        # 切换策略（如果需要）
        if new_strategy_name and new_strategy_name != current_strategy_name:
            if new_strategy_name in self.strategies:
                self.active_strategy = self.strategies[new_strategy_name]
                self.logger.debug(
                    f"Adaptive strategy switch: {current_strategy_name} -> {new_strategy_name} (pattern: {pattern.value})")

    def _generate_preread_request(self, page_id: int, table_name: str, pattern: AccessPattern):
        """生成预读请求"""
        if not self.active_strategy:
            return

        try:
            # 检查缓存使用率，避免过度预读
            cache_stats = self.storage_manager.get_cache_stats()
            cache_usage = cache_stats.get('cache_usage', 0)

            if cache_usage > self.config.max_cache_usage_for_preread * 100:
                self.logger.debug(f"Cache usage too high ({cache_usage}%), skipping preread")
                return

            # 生成预读请求
            request = self.active_strategy.generate_preread_request(
                page_id, table_name, self.detector
            )

            if request:
                # 过滤已在缓存中或正在处理的页面
                filtered_pages = []
                for p in request.page_ids:
                    if (p not in self.processing_pages and
                            not self._is_page_in_cache(p)):
                        filtered_pages.append(p)

                if filtered_pages:
                    request.page_ids = filtered_pages
                    self.preread_queue.put((request.priority, time.time(), request))

                    # 标记为正在处理
                    self.processing_pages.update(filtered_pages)

                    self.logger.debug(
                        f"Generated preread request: {len(filtered_pages)} pages from {self.active_strategy.name}")

        except Exception as e:
            self.logger.error(f"Error generating preread request: {e}")

    def _process_preread_queue(self):
        """处理预读队列"""
        processed_count = 0
        max_process_per_call = 3  # 限制每次处理的请求数

        while processed_count < max_process_per_call and not self.preread_queue.empty():
            try:
                # 获取优先级最高的请求
                priority, timestamp, request = self.preread_queue.get_nowait()

                # 检查请求是否过期
                if time.time() - timestamp > self.config.preread_timeout_seconds:
                    self.logger.debug(f"Preread request expired, skipping")
                    self._cleanup_processing_pages(request.page_ids)
                    continue

                # 执行预读
                start_time = time.time()
                self._execute_preread_request(request)
                execution_time = time.time() - start_time

                # 记录统计
                self.statistics.record_preread_request(request)
                self.statistics.record_preread_execution_time(execution_time)

                processed_count += 1

            except Empty:
                break
            except Exception as e:
                self.logger.error(f"Error processing preread queue: {e}")

    def _execute_preread_request(self, request: PrereadRequest):
        """执行预读请求"""
        try:
            successful_prereads = 0

            for page_id in request.page_ids:
                try:
                    # 检查页面是否已在缓存中
                    if self._is_page_in_cache(page_id):
                        continue

                    # 从磁盘读取页面数据
                    page_data = self.storage_manager.page_manager.read_page_from_disk(page_id)

                    # 将页面放入缓存，标记为预读页面（低优先级）
                    self.storage_manager.buffer_pool.put(page_id, page_data, is_dirty=False)

                    successful_prereads += 1

                except Exception as e:
                    self.logger.debug(f"Failed to preread page {page_id}: {e}")

            # 清理处理标记
            self._cleanup_processing_pages(request.page_ids)

            if successful_prereads > 0:
                self.logger.debug(
                    f"Successfully preread {successful_prereads}/{len(request.page_ids)} pages using {request.strategy_name}")

        except Exception as e:
            self.logger.error(f"Error executing preread request: {e}")
            self._cleanup_processing_pages(request.page_ids)

    def _is_page_in_cache(self, page_id: int) -> bool:
        """检查页面是否已在缓存中"""
        return page_id in self.storage_manager.buffer_pool.cache

    def _cleanup_processing_pages(self, page_ids: List[int]):
        """清理正在处理的页面标记"""
        for page_id in page_ids:
            self.processing_pages.discard(page_id)

    def _periodic_cleanup(self):
        """定期清理任务"""
        current_time = time.time()

        if current_time - self.last_cleanup_time > self.cleanup_interval:
            try:
                # 清理过期的预读记录
                self.statistics.cleanup_old_prereads()

                # 清理处理队列中的过期标记
                expired_pages = []
                for page_id in self.processing_pages:
                    # 这里可以添加更复杂的过期逻辑
                    pass

                for page_id in expired_pages:
                    self.processing_pages.discard(page_id)

                self.last_cleanup_time = current_time
                self.logger.debug("Periodic cleanup completed")

            except Exception as e:
                self.logger.error(f"Error in periodic cleanup: {e}")

    def set_config(self, config: PrereadConfig):
        """更新配置"""
        with self._lock:
            old_mode = self.config.mode
            self.config = config
            self.enabled = config.enabled

            # 如果模式改变，更新活跃策略
            if config.mode != old_mode:
                self._set_active_strategy(config.mode)

            self.logger.info(f"Configuration updated (mode: {old_mode.value} -> {config.mode.value})")

    def get_statistics(self) -> Dict[str, Any]:
        """获取预读系统统计信息"""
        with self._lock:
            stats = self.statistics.get_statistics()

            # 添加策略级别的统计
            strategy_details = {}
            for name, strategy in self.strategies.items():
                strategy_details[name] = strategy.get_statistics()

            # 添加检测器统计
            detector_stats = self.detector.get_statistics()

            # 添加配置信息
            config_info = self.config.to_dict()

            return {
                'enabled': self.enabled,
                'active_strategy': self.active_strategy.name if self.active_strategy else None,
                'preread_statistics': stats,
                'strategy_details': strategy_details,
                'access_pattern_stats': detector_stats,
                'configuration': config_info,
                'queue_size': self.preread_queue.qsize(),
                'processing_pages_count': len(self.processing_pages)
            }

    def force_preread(self, page_ids: List[int], priority: int = 5) -> bool:
        """
        强制执行预读（用于测试或特殊场景）

        Args:
            page_ids: 要预读的页面ID列表
            priority: 优先级 (1-5)

        Returns:
            bool: 是否成功添加到队列
        """
        if not self.enabled:
            return False

        try:
            with self._lock:
                request = PrereadRequest(
                    page_ids=page_ids,
                    priority=priority,
                    strategy_name="manual",
                    table_name=None
                )

                self.preread_queue.put((priority, time.time(), request))
                self.processing_pages.update(page_ids)

                self.logger.info(f"Force preread queued: {len(page_ids)} pages")
                return True

        except Exception as e:
            self.logger.error(f"Error in force_preread: {e}")
            return False

    def shutdown(self):
        """关闭预读管理器"""
        self.logger.info("Shutting down PrereadManager")

        with self._lock:
            self.shutdown_flag = True
            self.enabled = False

            # 清空队列
            while not self.preread_queue.empty():
                try:
                    self.preread_queue.get_nowait()
                except Empty:
                    break

            # 清理状态
            self.processing_pages.clear()

        self.logger.info("PrereadManager shutdown completed")

    def __str__(self) -> str:
        """字符串表示"""
        return (f"PrereadManager(enabled={self.enabled}, "
                f"mode={self.config.mode.value}, "
                f"active_strategy={self.active_strategy.name if self.active_strategy else 'None'})")

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()