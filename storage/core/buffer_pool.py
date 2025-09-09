"""
缓存池：实现LRU缓存机制（重构版）
增加了异常处理、日志记录和性能监控
"""

from typing import Optional, Dict, Tuple, List
from collections import OrderedDict
import time

from ..utils.constants import BUFFER_SIZE, MAX_CACHE_SIZE, MIN_CACHE_SIZE
from ..utils.exceptions import (
    BufferPoolException, BufferFullException,
    handle_storage_exceptions, StorageException
)
from ..utils.logger import get_logger, PerformanceTimer, performance_monitor


class BufferPool:
    """缓存池类，实现LRU缓存算法（增强版）"""

    def __init__(self, capacity: int = BUFFER_SIZE):
        """
        初始化缓存池

        Args:
            capacity: 缓存池容量（最多缓存的页数）

        Raises:
            BufferPoolException: 容量设置无效
        """
        if not MIN_CACHE_SIZE <= capacity <= MAX_CACHE_SIZE:
            raise BufferPoolException(
                f"Invalid buffer capacity: {capacity}. Must be between {MIN_CACHE_SIZE} and {MAX_CACHE_SIZE}",
                capacity=capacity
            )

        self.capacity = capacity
        self.cache = OrderedDict()  # {page_id: (data, is_dirty, access_time)}

        # 统计信息
        self.hit_count = 0  # 缓存命中次数
        self.total_requests = 0  # 总请求次数
        self.eviction_count = 0  # 淘汰次数
        self.write_count = 0  # 写入次数

        # 性能统计
        self.access_times = []  # 访问时间记录
        self.creation_time = time.time()

        # 日志器
        self.logger = get_logger("buffer")

        self.logger.info(f"BufferPool initialized",
                         capacity=capacity,
                         max_size=MAX_CACHE_SIZE,
                         min_size=MIN_CACHE_SIZE)

    @handle_storage_exceptions
    @performance_monitor("buffer_get")
    def get(self, page_id: int) -> Optional[bytes]:
        """
        从缓存中获取页数据

        Args:
            page_id: 页号

        Returns:
            bytes: 页数据，如果不在缓存中返回None

        Raises:
            BufferPoolException: 页号无效
        """
        if page_id < 0:
            raise BufferPoolException(f"Invalid page_id: {page_id}", page_id=page_id)

        self.total_requests += 1
        current_time = time.time()

        if page_id in self.cache:
            # 缓存命中，移到最后（最近使用）
            data, is_dirty, _ = self.cache.pop(page_id)
            self.cache[page_id] = (data, is_dirty, current_time)
            self.hit_count += 1

            self.logger.debug(f"Cache hit for page {page_id}",
                              page_id=page_id,
                              hit_rate=self.get_hit_rate())
            return data
        else:
            self.logger.debug(f"Cache miss for page {page_id}",
                              page_id=page_id,
                              cache_size=len(self.cache))
            return None

    @handle_storage_exceptions
    @performance_monitor("buffer_put")
    def put(self, page_id: int, data: bytes, is_dirty: bool = False):
        """
        将页数据放入缓存

        Args:
            page_id: 页号
            data: 页数据
            is_dirty: 是否为脏页（已修改但未写入磁盘）

        Raises:
            BufferPoolException: 参数无效
        """
        if page_id < 0:
            raise BufferPoolException(f"Invalid page_id: {page_id}", page_id=page_id)

        if not isinstance(data, bytes):
            raise BufferPoolException(f"Data must be bytes, got {type(data)}",
                                      page_id=page_id)

        current_time = time.time()

        if page_id in self.cache:
            # 更新已存在的页
            old_data, old_dirty, _ = self.cache.pop(page_id)
            # 保持脏页标记（一旦标记为脏页，直到写入磁盘前都是脏的）
            final_dirty = is_dirty or old_dirty
            self.cache[page_id] = (data, final_dirty, current_time)

            self.logger.debug(f"Updated cache entry for page {page_id}",
                              page_id=page_id,
                              is_dirty=final_dirty,
                              data_size=len(data))
        else:
            # 添加新页
            if len(self.cache) >= self.capacity:
                # 缓存已满，执行LRU淘汰
                evicted_page = self._evict_lru()
                if evicted_page:
                    evicted_id, evicted_data, evicted_dirty = evicted_page
                    self.logger.debug(f"LRU evicted page {evicted_id}",
                                      evicted_page=evicted_id,
                                      was_dirty=evicted_dirty)

            self.cache[page_id] = (data, is_dirty, current_time)
            self.write_count += 1

            self.logger.debug(f"Added new cache entry for page {page_id}",
                              page_id=page_id,
                              is_dirty=is_dirty,
                              cache_size=len(self.cache),
                              data_size=len(data))

    def _evict_lru(self) -> Optional[Tuple[int, bytes, bool]]:
        """
        执行LRU淘汰算法

        Returns:
            被淘汰页的信息 (page_id, data, is_dirty) 或 None
        """
        if not self.cache:
            return None

        # OrderedDict的第一个元素是最久未使用的
        page_id, (data, is_dirty, access_time) = self.cache.popitem(last=False)
        self.eviction_count += 1

        self.logger.debug(f"Evicted page {page_id} from cache",
                          page_id=page_id,
                          was_dirty=is_dirty,
                          eviction_count=self.eviction_count)

        return page_id, data, is_dirty

    @handle_storage_exceptions
    def mark_dirty(self, page_id: int):
        """
        标记页为脏页

        Args:
            page_id: 页号

        Raises:
            BufferPoolException: 页不在缓存中
        """
        if page_id in self.cache:
            data, _, access_time = self.cache[page_id]
            self.cache[page_id] = (data, True, access_time)
            self.logger.debug(f"Marked page {page_id} as dirty", page_id=page_id)
        else:
            raise BufferPoolException(f"Page {page_id} not in cache, cannot mark dirty",
                                      page_id=page_id)

    def get_dirty_pages(self) -> Dict[int, bytes]:
        """
        获取所有脏页

        Returns:
            Dict[int, bytes]: {page_id: data} 脏页字典
        """
        dirty_pages = {}
        for page_id, (data, is_dirty, _) in self.cache.items():
            if is_dirty:
                dirty_pages[page_id] = data

        self.logger.debug(f"Retrieved {len(dirty_pages)} dirty pages")
        return dirty_pages

    @handle_storage_exceptions
    def clear_dirty_flag(self, page_id: int):
        """
        清除页的脏标记

        Args:
            page_id: 页号

        Raises:
            BufferPoolException: 页不在缓存中
        """
        if page_id in self.cache:
            data, _, access_time = self.cache[page_id]
            self.cache[page_id] = (data, False, access_time)
            self.logger.debug(f"Cleared dirty flag for page {page_id}", page_id=page_id)
        else:
            raise BufferPoolException(f"Page {page_id} not in cache, cannot clear dirty flag",
                                      page_id=page_id)

    @handle_storage_exceptions
    def remove(self, page_id: int) -> Optional[Tuple[bytes, bool]]:
        """
        从缓存中移除页

        Args:
            page_id: 页号

        Returns:
            被移除页的数据和脏标记 (data, is_dirty) 或 None
        """
        if page_id in self.cache:
            data, is_dirty, _ = self.cache.pop(page_id)
            self.logger.debug(f"Removed page {page_id} from cache",
                              page_id=page_id,
                              was_dirty=is_dirty)
            return data, is_dirty
        else:
            self.logger.debug(f"Page {page_id} not in cache", page_id=page_id)
            return None

    def get_statistics(self) -> dict:
        """
        获取缓存统计信息

        Returns:
            dict: 统计信息字典
        """
        hit_rate = self.get_hit_rate()
        dirty_count = sum(1 for _, (_, is_dirty, _) in self.cache.items() if is_dirty)
        uptime = time.time() - self.creation_time

        stats = {
            "total_requests": self.total_requests,
            "hit_count": self.hit_count,
            "miss_count": self.total_requests - self.hit_count,
            "hit_rate": hit_rate,
            "cache_size": len(self.cache),
            "cache_capacity": self.capacity,
            "dirty_pages": dirty_count,
            "cache_usage": round(len(self.cache) / self.capacity * 100, 2),
            "eviction_count": self.eviction_count,
            "write_count": self.write_count,
            "uptime_seconds": round(uptime, 2)
        }

        return stats

    def get_hit_rate(self) -> float:
        """获取缓存命中率"""
        if self.total_requests == 0:
            return 0.0
        return round(self.hit_count / self.total_requests * 100, 2)

    def flush_all(self) -> Dict[int, bytes]:
        """
        获取所有脏页并清除脏标记（用于刷盘）

        Returns:
            Dict[int, bytes]: 所有脏页的数据
        """
        dirty_pages = {}
        current_time = time.time()

        for page_id, (data, is_dirty, access_time) in self.cache.items():
            if is_dirty:
                dirty_pages[page_id] = data
                # 清除脏标记
                self.cache[page_id] = (data, False, current_time)

        self.logger.info(f"Flushed {len(dirty_pages)} dirty pages")
        return dirty_pages

    def clear(self):
        """清空缓存池"""
        cache_size = len(self.cache)
        self.cache.clear()
        self.hit_count = 0
        self.total_requests = 0
        self.eviction_count = 0
        self.write_count = 0
        self.access_times.clear()

        self.logger.info(f"Cache cleared, removed {cache_size} pages")

    def get_cache_info(self) -> dict:
        """
        获取缓存详细信息

        Returns:
            dict: 缓存详细信息
        """
        cache_details = {}
        for page_id, (data, is_dirty, access_time) in self.cache.items():
            cache_details[page_id] = {
                "data_size": len(data),
                "is_dirty": is_dirty,
                "access_time": access_time,
                "age_seconds": round(time.time() - access_time, 2)
            }

        return {
            "cache_details": cache_details,
            "lru_order": list(self.cache.keys()),  # LRU顺序
            "capacity_info": {
                "current": len(self.cache),
                "capacity": self.capacity,
                "usage_percent": round(len(self.cache) / self.capacity * 100, 2)
            }
        }

    def resize(self, new_capacity: int):
        """
        调整缓存容量

        Args:
            new_capacity: 新的容量大小

        Raises:
            BufferPoolException: 新容量无效
        """
        if not MIN_CACHE_SIZE <= new_capacity <= MAX_CACHE_SIZE:
            raise BufferPoolException(
                f"Invalid new capacity: {new_capacity}. Must be between {MIN_CACHE_SIZE} and {MAX_CACHE_SIZE}"
            )

        old_capacity = self.capacity
        self.capacity = new_capacity

        # 如果新容量小于当前缓存大小，需要淘汰一些页
        while len(self.cache) > new_capacity:
            evicted = self._evict_lru()
            if not evicted:
                break

        self.logger.info(f"Buffer capacity changed from {old_capacity} to {new_capacity}")

    def get_performance_metrics(self) -> dict:
        """
        获取性能指标

        Returns:
            dict: 性能指标
        """
        stats = self.get_statistics()

        return {
            "hit_rate": stats["hit_rate"],
            "eviction_rate": round(self.eviction_count / max(self.write_count, 1) * 100, 2),
            "cache_efficiency": round(stats["cache_usage"] * (stats["hit_rate"] / 100), 2),
            "requests_per_second": round(self.total_requests / max(stats["uptime_seconds"], 1), 2)
        }

    def __str__(self) -> str:
        """字符串表示"""
        stats = self.get_statistics()
        return (f"BufferPool(capacity={self.capacity}, "
                f"size={stats['cache_size']}, "
                f"hit_rate={stats['hit_rate']}%, "
                f"dirty={stats['dirty_pages']})")

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()