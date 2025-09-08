# storage_manager.py
"""
存储管理器：对外统一接口，整合页管理器和缓存池
"""

import time
from typing import Optional, Dict
from page_manager import PageManager
from buffer_pool import BufferPool
from constants import BUFFER_SIZE, DATA_FILE, META_FILE


class StorageManager:
    """存储管理器类 - 提供统一的存储访问接口"""

    def __init__(self, buffer_size: int = BUFFER_SIZE,
                 data_file: str = DATA_FILE,
                 meta_file: str = META_FILE):
        """
        初始化存储管理器

        Args:
            buffer_size: 缓存池大小
            data_file: 数据文件路径
            meta_file: 元数据文件路径
        """
        self.page_manager = PageManager(data_file, meta_file)
        self.buffer_pool = BufferPool(buffer_size)
        self.is_shutdown = False

        print(f"存储管理器初始化完成")
        print(f"数据文件: {data_file}")
        print(f"元数据文件: {meta_file}")
        print(f"缓存池大小: {buffer_size}")

    def read_page(self, page_id: int) -> bytes:
        """
        读取页数据（优先从缓存读取）

        Args:
            page_id: 页号

        Returns:
            bytes: 页数据
        """
        if self.is_shutdown:
            raise RuntimeError("存储管理器已关闭")

        # 首先从缓存中尝试获取
        data = self.buffer_pool.get(page_id)

        if data is not None:
            # 缓存命中
            return data
        else:
            # 缓存未命中，从磁盘读取
            data = self.page_manager.read_page_from_disk(page_id)

            # 将数据放入缓存
            self.buffer_pool.put(page_id, data, is_dirty=False)

            return data

    def write_page(self, page_id: int, data: bytes):
        """
        写入页数据（写入缓存，标记为脏页）

        Args:
            page_id: 页号
            data: 要写入的数据
        """
        if self.is_shutdown:
            raise RuntimeError("存储管理器已关闭")

        # 将数据写入缓存并标记为脏页
        self.buffer_pool.put(page_id, data, is_dirty=True)

        print(f"写入页 {page_id} 到缓存（脏页）")

    def allocate_page(self) -> int:
        """
        分配一个新页

        Returns:
            int: 新分配的页号
        """
        if self.is_shutdown:
            raise RuntimeError("存储管理器已关闭")

        return self.page_manager.allocate_page()

    def deallocate_page(self, page_id: int):
        """
        释放一个页

        Args:
            page_id: 要释放的页号
        """
        if self.is_shutdown:
            raise RuntimeError("存储管理器已关闭")

        # 从缓存中移除
        removed = self.buffer_pool.remove(page_id)
        if removed:
            data, is_dirty = removed
            if is_dirty:
                # 如果是脏页，先写入磁盘
                self.page_manager.write_page_to_disk(page_id, data)
                print(f"释放前刷新脏页 {page_id}")

        # 从页管理器中释放
        self.page_manager.deallocate_page(page_id)

    def flush_page(self, page_id: int):
        """
        刷新指定页到磁盘

        Args:
            page_id: 页号
        """
        if self.is_shutdown:
            raise RuntimeError("存储管理器已关闭")

        # 检查页是否在缓存中且为脏页
        if page_id in self.buffer_pool.cache:
            data, is_dirty = self.buffer_pool.cache[page_id]
            if is_dirty:
                # 写入磁盘
                self.page_manager.write_page_to_disk(page_id, data)
                # 清除脏标记
                self.buffer_pool.clear_dirty_flag(page_id)
                print(f"刷新页 {page_id} 到磁盘")
            else:
                print(f"页 {page_id} 不是脏页，无需刷新")
        else:
            print(f"页 {page_id} 不在缓存中")

    def flush_all_pages(self):
        """刷新所有脏页到磁盘"""
        if self.is_shutdown:
            raise RuntimeError("存储管理器已关闭")

        dirty_pages = self.buffer_pool.flush_all()

        for page_id, data in dirty_pages.items():
            self.page_manager.write_page_to_disk(page_id, data)

        print(f"刷新所有脏页完成，共 {len(dirty_pages)} 个页面")

    def get_cache_stats(self) -> dict:
        """
        获取缓存统计信息

        Returns:
            dict: 缓存统计信息
        """
        return self.buffer_pool.get_statistics()

    def get_storage_info(self) -> dict:
        """
        获取存储系统信息

        Returns:
            dict: 存储系统信息
        """
        cache_stats = self.buffer_pool.get_statistics()
        page_info = self.page_manager.get_metadata_info()

        return {
            "cache_statistics": cache_stats,
            "page_manager_info": page_info,
            "system_status": "running" if not self.is_shutdown else "shutdown"
        }

    def force_eviction(self):
        """强制执行一次缓存淘汰（用于测试）"""
        if self.buffer_pool.cache:
            evicted = self.buffer_pool._evict_lru()
            if evicted:
                page_id, data, is_dirty = evicted
                if is_dirty:
                    self.page_manager.write_page_to_disk(page_id, data)
                    print(f"强制淘汰脏页 {page_id} 并写入磁盘")
                else:
                    print(f"强制淘汰页 {page_id}")
        else:
            print("缓存为空，无法执行淘汰")

    def shutdown(self):
        """
        关闭存储管理器，确保所有数据持久化
        """
        if self.is_shutdown:
            print("存储管理器已经关闭")
            return

        print("正在关闭存储管理器...")

        # 刷新所有脏页
        self.flush_all_pages()

        # 获取最终统计信息
        final_stats = self.get_cache_stats()

        # 标记为已关闭
        self.is_shutdown = True

        print("存储管理器关闭完成，所有数据已持久化")
        print(f"最终缓存统计: {final_stats}")

    def __enter__(self):
        """支持上下文管理器"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出时自动关闭"""
        self.shutdown()


# 便捷函数
def create_storage_manager(buffer_size: int = BUFFER_SIZE) -> StorageManager:
    """
    创建存储管理器实例

    Args:
        buffer_size: 缓存池大小

    Returns:
        StorageManager: 存储管理器实例
    """
    return StorageManager(buffer_size=buffer_size)