# buffer_pool.py
"""
缓存池：实现LRU缓存机制
"""

from typing import Optional, Dict, Tuple
from collections import OrderedDict
from constants import BUFFER_SIZE


class BufferPool:
    """缓存池类，实现LRU缓存算法"""

    def __init__(self, capacity: int = BUFFER_SIZE):
        """
        初始化缓存池

        Args:
            capacity: 缓存池容量（最多缓存的页数）
        """
        self.capacity = capacity
        self.cache = OrderedDict()  # {page_id: (data, is_dirty)}
        self.hit_count = 0  # 缓存命中次数
        self.total_requests = 0  # 总请求次数

        print(f"初始化缓存池，容量: {capacity}")

    def get(self, page_id: int) -> Optional[bytes]:
        """
        从缓存中获取页数据

        Args:
            page_id: 页号

        Returns:
            bytes: 页数据，如果不在缓存中返回None
        """
        self.total_requests += 1

        if page_id in self.cache:
            # 缓存命中，移到最后（最近使用）
            data, is_dirty = self.cache.pop(page_id)
            self.cache[page_id] = (data, is_dirty)
            self.hit_count += 1

            print(f"缓存命中: 页 {page_id}")
            return data
        else:
            print(f"缓存未命中: 页 {page_id}")
            return None

    def put(self, page_id: int, data: bytes, is_dirty: bool = False):
        """
        将页数据放入缓存

        Args:
            page_id: 页号
            data: 页数据
            is_dirty: 是否为脏页（已修改但未写入磁盘）
        """
        if page_id in self.cache:
            # 更新已存在的页
            self.cache.pop(page_id)
            self.cache[page_id] = (data, is_dirty)
            print(f"更新缓存: 页 {page_id}, 脏页: {is_dirty}")
        else:
            # 添加新页
            if len(self.cache) >= self.capacity:
                # 缓存已满，执行LRU淘汰
                evicted_page = self._evict_lru()
                if evicted_page:
                    print(f"LRU淘汰页: {evicted_page}")

            self.cache[page_id] = (data, is_dirty)
            print(f"添加到缓存: 页 {page_id}, 脏页: {is_dirty}")

    def _evict_lru(self) -> Optional[Tuple[int, bytes, bool]]:
        """
        执行LRU淘汰算法

        Returns:
            被淘汰页的信息 (page_id, data, is_dirty) 或 None
        """
        if not self.cache:
            return None

        # OrderedDict的第一个元素是最久未使用的
        page_id, (data, is_dirty) = self.cache.popitem(last=False)
        return page_id, data, is_dirty

    def mark_dirty(self, page_id: int):
        """
        标记页为脏页

        Args:
            page_id: 页号
        """
        if page_id in self.cache:
            data, _ = self.cache[page_id]
            self.cache[page_id] = (data, True)
            print(f"标记页 {page_id} 为脏页")
        else:
            print(f"警告: 页 {page_id} 不在缓存中，无法标记为脏页")

    def get_dirty_pages(self) -> Dict[int, bytes]:
        """
        获取所有脏页

        Returns:
            Dict[int, bytes]: {page_id: data} 脏页字典
        """
        dirty_pages = {}
        for page_id, (data, is_dirty) in self.cache.items():
            if is_dirty:
                dirty_pages[page_id] = data

        print(f"获取脏页列表，共 {len(dirty_pages)} 个脏页")
        return dirty_pages

    def clear_dirty_flag(self, page_id: int):
        """
        清除页的脏标记

        Args:
            page_id: 页号
        """
        if page_id in self.cache:
            data, _ = self.cache[page_id]
            self.cache[page_id] = (data, False)
            print(f"清除页 {page_id} 的脏标记")
        else:
            print(f"警告: 页 {page_id} 不在缓存中，无法清除脏标记")

    def remove(self, page_id: int) -> Optional[Tuple[bytes, bool]]:
        """
        从缓存中移除页

        Args:
            page_id: 页号

        Returns:
            被移除页的数据和脏标记 (data, is_dirty) 或 None
        """
        if page_id in self.cache:
            data, is_dirty = self.cache.pop(page_id)
            print(f"从缓存移除页 {page_id}")
            return data, is_dirty
        else:
            print(f"页 {page_id} 不在缓存中")
            return None

    def get_statistics(self) -> dict:
        """
        获取缓存统计信息

        Returns:
            dict: 统计信息字典
        """
        hit_rate = (self.hit_count / self.total_requests * 100) if self.total_requests > 0 else 0

        dirty_count = sum(1 for _, (_, is_dirty) in self.cache.items() if is_dirty)

        stats = {
            "total_requests": self.total_requests,
            "hit_count": self.hit_count,
            "miss_count": self.total_requests - self.hit_count,
            "hit_rate": round(hit_rate, 2),
            "cache_size": len(self.cache),
            "cache_capacity": self.capacity,
            "dirty_pages": dirty_count,
            "cache_usage": round(len(self.cache) / self.capacity * 100, 2)
        }

        return stats

    def flush_all(self) -> Dict[int, bytes]:
        """
        获取所有脏页并清除脏标记（用于刷盘）

        Returns:
            Dict[int, bytes]: 所有脏页的数据
        """
        dirty_pages = {}

        for page_id, (data, is_dirty) in self.cache.items():
            if is_dirty:
                dirty_pages[page_id] = data
                # 清除脏标记
                self.cache[page_id] = (data, False)

        print(f"刷新所有脏页，共 {len(dirty_pages)} 个")
        return dirty_pages

    def clear(self):
        """清空缓存池"""
        self.cache.clear()
        self.hit_count = 0
        self.total_requests = 0
        print("缓存池已清空")

    def get_cache_info(self) -> dict:
        """
        获取缓存详细信息

        Returns:
            dict: 缓存详细信息
        """
        cache_details = {}
        for page_id, (data, is_dirty) in self.cache.items():
            cache_details[page_id] = {
                "data_size": len(data),
                "is_dirty": is_dirty
            }

        return {
            "cache_details": cache_details,
            "lru_order": list(self.cache.keys())  # LRU顺序
        }