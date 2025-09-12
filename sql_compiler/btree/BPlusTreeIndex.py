from typing import Any, List, Optional, Tuple, Union, Dict
import json
import pickle

# 导入存储管理器和B+树
try:
    from storage.core.storage_manager import StorageManager, create_storage_manager
    from storage.core.btree.btree import BPlusTree as StorageBPlusTree
    from storage.core.btree.btree_node import BTreeNode

    STORAGE_AVAILABLE = True
except ImportError:
    try:
        from storage.core.storage_manager import StorageManager, create_storage_manager
        from storage.core.btree.btree import BPlusTree as StorageBPlusTree
        from storage.core.btree.btree_node import BTreeNode

        STORAGE_AVAILABLE = True
    except ImportError:
        STORAGE_AVAILABLE = False


class BPlusTreeIndex:
    """
    B+树索引适配器
    """

    # 类级别的存储管理器缓存
    _storage_managers: Dict[str, StorageManager] = {}
    _storage_lock = None

    @classmethod
    def _get_storage_manager(cls, index_name: str) -> Optional[StorageManager]:
        """获取或创建存储管理器实例"""
        if cls._storage_lock is None:
            import threading
            cls._storage_lock = threading.Lock()

        with cls._storage_lock:
            if index_name not in cls._storage_managers:
                if STORAGE_AVAILABLE:
                    try:
                        # 为每个索引创建独立的存储管理器
                        # 使用索引名作为数据目录的一部分
                        data_dir = f"index_data/{index_name}"
                        cls._storage_managers[index_name] = create_storage_manager(
                            buffer_size=64,  # 较小的缓存，因为索引通常不会很大
                            data_dir=data_dir,
                            auto_flush=True
                        )
                        print(f"为索引 {index_name} 创建存储管理器: {data_dir}")
                    except Exception as e:
                        print(f"创建存储管理器失败: {e}")
                        return None
                else:
                    return None

            return cls._storage_managers.get(index_name)

    def __init__(self, index_name: str = "default_index", order: int = None):
        """
        初始化B+树索引

        Args:
            index_name: 索引名称
            order: B+树阶数
        """
        self.index_name = index_name
        self.order = order or 100

        # 尝试使用真实存储管理器
        self.storage_manager = self._get_storage_manager(index_name)

        if self.storage_manager and STORAGE_AVAILABLE:
            self._use_storage_btree()
        else:
            self._use_memory_btree()

    def _use_storage_btree(self):
        """使用存储版B+树"""
        try:
            self.btree = StorageBPlusTree(
                self.storage_manager,
                self.index_name,
                self.order
            )
            self.implementation = "storage"
            print(f"✅ 使用存储版B+树: {self.index_name}")
        except Exception as e:
            print(f"⚠️ 存储版B+树初始化失败，回退到内存版: {e}")
            self._use_memory_btree()

    def _use_memory_btree(self):
        """使用内存版B+树（回退方案）"""
        self.data: Dict[Any, Any] = {}
        self.btree = None
        self.storage_manager = None
        self.implementation = "memory"
        print(f"使用内存版B+树（回退）: {self.index_name}")

    def insert(self, key: Any, value: Any) -> bool:
        """
        插入键值对

        Args:
            key: 键
            value: 值

        Returns:
            bool: 是否插入成功
        """
        try:
            if self.implementation == "storage" and self.btree:
                # 转换键为整数
                int_key = self._convert_key_to_int(key)
                # 转换值为 (page_id, slot_id) 格式
                page_id, slot_id = self._convert_value_to_tuple(value)
                success = self.btree.insert(int_key, (page_id, slot_id))

                # 定期刷盘
                if success and hasattr(self.storage_manager, 'flush_if_needed'):
                    self.storage_manager.flush_if_needed()

                return success
            else:
                # 内存版本
                self.data[key] = value
                return True

        except Exception as e:
            print(f"插入失败 {key}: {e}")
            return False

    def search(self, key: Any) -> Optional[Any]:
        """
        查找键对应的值

        Args:
            key: 要查找的键

        Returns:
            值或None
        """
        try:
            if self.implementation == "storage" and self.btree:
                int_key = self._convert_key_to_int(key)
                result = self.btree.search(int_key)
                if result:
                    return self._convert_tuple_to_value(result)
                return None
            else:
                return self.data.get(key)

        except Exception as e:
            print(f"查找失败 {key}: {e}")
            return None

    def delete(self, key: Any) -> bool:
        """
        删除键

        Args:
            key: 要删除的键

        Returns:
            bool: 是否删除成功
        """
        try:
            if self.implementation == "storage" and self.btree:
                int_key = self._convert_key_to_int(key)
                success = self.btree.delete(int_key)

                # 删除后刷盘
                if success and hasattr(self.storage_manager, 'flush_if_needed'):
                    self.storage_manager.flush_if_needed()

                return success
            else:
                if key in self.data:
                    del self.data[key]
                    return True
                return False

        except Exception as e:
            print(f"删除失败 {key}: {e}")
            return False

    def range_search(self, start_key: Any, end_key: Any) -> List[Tuple[Any, Any]]:
        """
        范围查询

        Args:
            start_key: 起始键
            end_key: 结束键

        Returns:
            键值对列表
        """
        try:
            if self.implementation == "storage" and self.btree:
                start_int = self._convert_key_to_int(start_key)
                end_int = self._convert_key_to_int(end_key)

                storage_results = self.btree.range_search(start_int, end_int)

                # 转换结果格式
                results = []
                for int_key, (page_id, slot_id) in storage_results:
                    original_key = self._convert_int_to_key(int_key)
                    original_value = self._convert_tuple_to_value((page_id, slot_id))
                    results.append((original_key, original_value))

                return results
            else:
                # 内存版本的简单范围查询
                results = []
                for key, value in self.data.items():
                    if start_key <= key <= end_key:
                        results.append((key, value))
                return sorted(results)

        except Exception as e:
            print(f"范围查询失败 [{start_key}, {end_key}]: {e}")
            return []

    def flush(self):
        """强制刷盘"""
        if self.implementation == "storage" and self.storage_manager:
            try:
                self.storage_manager.flush_all_pages()
                print(f"索引 {self.index_name} 刷盘完成")
            except Exception as e:
                print(f"刷盘失败: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        """获取索引统计信息"""
        stats = {
            "index_name": self.index_name,
            "implementation": self.implementation,
            "order": self.order,
        }

        if self.implementation == "storage":
            try:
                stats.update({
                    "height": self.get_height(),
                    "node_count": self.get_node_count(),
                    "is_empty": self.is_empty(),
                })

                # 添加存储管理器统计
                if self.storage_manager:
                    storage_stats = self.storage_manager.get_statistics()
                    stats["storage_stats"] = {
                        "buffer_hits": storage_stats.get("buffer_hits", 0),
                        "buffer_misses": storage_stats.get("buffer_misses", 0),
                        "total_operations": storage_stats.get("total_operations", 0),
                        "flush_count": storage_stats.get("flush_count", 0)
                    }
            except Exception as e:
                print(f"获取存储统计失败: {e}")
        else:
            stats.update({
                "data_size": len(self.data),
                "memory_usage": "估算",
            })

        return stats

    def get_height(self) -> int:
        """获取B+树高度"""
        if self.implementation == "storage" and self.btree:
            try:
                return self._calculate_storage_height()
            except:
                return 1
        else:
            # 内存版本简单估算
            if not self.data:
                return 1
            count = len(self.data)
            if count <= self.order:
                return 1
            elif count <= self.order * self.order:
                return 2
            else:
                return 3

    def get_node_count(self) -> int:
        """获取节点数量"""
        if self.implementation == "storage" and self.storage_manager:
            try:
                # 从存储管理器获取页数
                stats = self.storage_manager.get_statistics()
                return stats.get("total_pages", 1)
            except:
                return 1
        else:
            # 内存版本估算
            count = len(self.data)
            return max(1, (count + self.order - 1) // self.order)

    def is_empty(self) -> bool:
        """检查是否为空"""
        if self.implementation == "storage" and self.btree:
            try:
                # 尝试读取根节点
                root = self.btree._read_node(self.btree.root_page_id)
                return len(root.keys) == 0
            except:
                return True
        else:
            return len(self.data) == 0

    def close(self):
        """关闭索引，清理资源"""
        if self.implementation == "storage":
            try:
                # 最后一次刷盘
                self.flush()

                # 关闭存储管理器（如果是最后一个使用者）
                if self.storage_manager and hasattr(self.storage_manager, 'shutdown'):
                    # 注意：不要直接关闭共享的存储管理器
                    # self.storage_manager.shutdown()
                    pass

                print(f"索引 {self.index_name} 已关闭")
            except Exception as e:
                print(f"关闭索引失败: {e}")

    def print_debug_info(self):
        """打印调试信息"""
        print(f"\n=== B+树索引调试信息 ===")
        stats = self.get_statistics()

        for key, value in stats.items():
            if isinstance(value, dict):
                print(f"{key}:")
                for sub_key, sub_value in value.items():
                    print(f"  {sub_key}: {sub_value}")
            else:
                print(f"{key}: {value}")

        # 打印树结构
        if self.implementation == "storage" and self.btree:
            try:
                print("\n=== B+树结构 ===")
                self.btree.print_tree()
            except Exception as e:
                print(f"打印树结构失败: {e}")
        else:
            print(f"\n内存数据示例: {dict(list(self.data.items())[:5])}")

    def _calculate_storage_height(self) -> int:
        """计算存储版B+树的高度"""
        try:
            if not hasattr(self.btree, 'root_page_id'):
                return 1

            current_page = self.btree.root_page_id
            height = 1

            current = self.btree._read_node(current_page)
            while not current.is_leaf:
                height += 1
                if current.children:
                    current = self.btree._read_node(current.children[0])
                else:
                    break

            return height
        except Exception as e:
            print(f"计算高度失败: {e}")
            return 1

    def _convert_key_to_int(self, key: Any) -> int:
        """将键转换为整数"""
        if isinstance(key, int):
            return max(0, key)  # 确保非负
        elif isinstance(key, str):
            return abs(hash(key)) % (2 ** 30 - 1)
        elif isinstance(key, float):
            return max(0, int(abs(key)))
        else:
            return abs(hash(str(key))) % (2 ** 30 - 1)

    def _convert_int_to_key(self, int_key: int) -> Any:
        """将整数键转换回原始类型（简化版）"""
        return int_key

    def _convert_value_to_tuple(self, value: Any) -> Tuple[int, int]:
        """将值转换为 (page_id, slot_id) 格式"""
        if isinstance(value, tuple) and len(value) >= 2:
            return (int(value[0]), int(value[1]))
        elif isinstance(value, (int, float)):
            # 数字类型：使用数值本身和0
            return (int(abs(value)) % (2 ** 30), 0)
        else:
            # 其他类型：序列化
            value_str = str(value)
            page_id = abs(hash(value_str)) % (2 ** 30 - 1)
            slot_id = len(value_str) % 65535
            return (page_id, slot_id)

    def _convert_tuple_to_value(self, tuple_value: Tuple[int, int]) -> Any:
        """将 (page_id, slot_id) 转换回值"""
        page_id, slot_id = tuple_value
        # 简化实现：返回格式化字符串
        return f"record_{page_id}_{slot_id}"

    def __del__(self):
        """析构函数"""
        try:
            self.close()
        except:
            pass


# 清理函数
def cleanup_storage_managers():
    """清理所有存储管理器"""
    try:
        for name, storage_manager in BPlusTreeIndex._storage_managers.items():
            if hasattr(storage_manager, 'shutdown'):
                storage_manager.shutdown()
            print(f"清理存储管理器: {name}")
        BPlusTreeIndex._storage_managers.clear()
    except Exception as e:
        print(f"清理存储管理器失败: {e}")


# 注册清理函数
import atexit

atexit.register(cleanup_storage_managers)