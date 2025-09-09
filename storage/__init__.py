# storage/__init__.py
"""
存储系统对外接口 - 专注于页式存储和缓存管理
"""

from .core.storage_manager import StorageManager
from .core.table_storage import TableStorage
from .utils.exceptions import *

class StorageSystem:
    """存储系统 - 你的模块对外接口"""
    
    def __init__(self, buffer_size=100, data_dir="data"):
        """
        初始化存储系统
        
        Args:
            buffer_size: 缓存池大小
            data_dir: 数据目录
        """
        self.storage_manager = StorageManager(
            buffer_size=buffer_size,
            data_file=f"{data_dir}/database.db",
            meta_file=f"{data_dir}/metadata.json"
        )
        self.table_storage = TableStorage(self.storage_manager)
    
    # === 页级存储接口（底层接口）===
    def read_page(self, page_id: int) -> bytes:
        """读取页数据"""
        return self.storage_manager.read_page(page_id)
    
    def write_page(self, page_id: int, data: bytes):
        """写入页数据"""
        self.storage_manager.write_page(page_id, data)
    
    def allocate_page(self) -> int:
        """分配新页"""
        return self.storage_manager.allocate_page()
    
    def deallocate_page(self, page_id: int):
        """释放页"""
        self.storage_manager.deallocate_page(page_id)
    
    def flush_page(self, page_id: int):
        """刷新指定页到磁盘"""
        return self.storage_manager.flush_page(page_id)
    
    def flush_all_pages(self):
        """刷新所有页到磁盘"""
        return self.storage_manager.flush_all_pages()
    
    # === 表存储接口（你的职责部分）===
    def create_table_storage(self, table_name: str, estimated_record_size: int) -> bool:
        """为表创建存储空间"""
        return self.table_storage.create_table_storage(table_name, estimated_record_size)
    
    def drop_table_storage(self, table_name: str) -> bool:
        """删除表的存储空间"""
        return self.table_storage.drop_table_storage(table_name)
    
    def get_table_pages(self, table_name: str) -> list:
        """获取表占用的页号列表"""
        return self.table_storage.get_table_pages(table_name)
    
    def allocate_table_page(self, table_name: str) -> int:
        """为表分配新页"""
        return self.table_storage.allocate_table_page(table_name)
    
    def read_table_page(self, table_name: str, page_index: int) -> bytes:
        """读取表的指定页"""
        return self.table_storage.read_table_page(table_name, page_index)
    
    def write_table_page(self, table_name: str, page_index: int, data: bytes):
        """写入表的指定页"""
        self.table_storage.write_table_page(table_name, page_index, data)
    
    # === 统计和管理接口 ===
    def get_cache_stats(self) -> dict:
        """获取缓存统计"""
        return self.storage_manager.get_cache_stats()
    
    def get_storage_stats(self) -> dict:
        """获取存储统计"""
        return self.storage_manager.get_storage_info()
    
    def get_table_storage_info(self, table_name: str = None) -> dict:
        """获取表存储信息"""
        return self.table_storage.get_storage_info(table_name)
    
    def shutdown(self):
        """关闭存储系统"""
        self.table_storage.shutdown()
        self.storage_manager.shutdown()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()


def create_storage_system(buffer_size=100, data_dir="data"):
    """便捷创建函数"""
    return StorageSystem(buffer_size, data_dir)