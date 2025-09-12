"""
B+树实现 - 索引的核心数据结构（修复版）
"""

import bisect
from typing import Optional, Tuple, List
from .btree_node import BTreeNode
from ...utils.logger import get_logger
from ...utils.exceptions import StorageException


class BPlusTree:
    """
    B+树实现
    用于数据库索引，每个节点对应一个4KB的页
    """

    def __init__(self, storage_manager, index_name: str, order: int = None):
        """
        初始化B+树

        Args:
            storage_manager: 存储管理器实例
            index_name: 索引名称
            order: B+树的阶数（每个节点最大键数）
        """
        self.storage = storage_manager
        self.index_name = index_name
        self.logger = get_logger(f"btree_{index_name}")

        # 计算合适的阶数
        if order is None:
            # 根据页大小计算
            # 头部16字节，每个键4字节，每个值6字节（页号4+槽位2）
            # 预留一些空间防止溢出
            self.order = 300
        else:
            self.order = order

        # 初始化根节点
        self.root_page_id = None
        self._initialize_tree()

        self.logger.info(f"B+树初始化完成，索引名: {index_name}, 阶数: {self.order}")

    def _initialize_tree(self):
        """初始化树，创建根节点"""
        # 为根节点分配页
        self.root_page_id = self.storage.allocate_page()

        # 创建空的叶子节点作为根
        root = BTreeNode(self.root_page_id, is_leaf=True, order=self.order)

        # 写入存储
        self._write_node(root)

        self.logger.debug(f"创建根节点，页号: {self.root_page_id}")

    def _read_node(self, page_id: int) -> BTreeNode:
        """
        从存储读取节点

        Args:
            page_id: 页号

        Returns:
            BTreeNode: 节点对象
        """
        page_data = self.storage.read_page(page_id)
        node = BTreeNode.deserialize(page_data, self.order)
        node.page_id = page_id  # 设置正确的页号
        return node

    def _write_node(self, node: BTreeNode):
        """
        将节点写入存储

        Args:
            node: 节点对象
        """
        page_data = node.serialize()
        self.storage.write_page(node.page_id, page_data)

    def search(self, key: int) -> Optional[Tuple[int, int]]:
        """
        查找键对应的值

        Args:
            key: 要查找的键

        Returns:
            (page_id, slot_id) 或 None
        """
        # 从根节点开始
        current = self._read_node(self.root_page_id)

        # 向下查找直到叶子节点
        while not current.is_leaf:
            # 在内部节点中找到合适的子节点
            child_index = self._find_child_index(current, key)
            child_page_id = current.children[child_index]
            current = self._read_node(child_page_id)

        # 在叶子节点中查找键
        try:
            key_index = current.keys.index(key)
            value = current.values[key_index]
            self.logger.debug(f"找到键 {key}，值: {value}")
            return value
        except ValueError:
            # 键不在当前节点，检查链表中的后续节点
            while current.next_leaf_id is not None:
                current = self._read_node(current.next_leaf_id)
                if current.keys and key <= current.keys[-1]:
                    try:
                        key_index = current.keys.index(key)
                        value = current.values[key_index]
                        self.logger.debug(f"在链表中找到键 {key}，值: {value}")
                        return value
                    except ValueError:
                        continue
                if current.keys and key < current.keys[0]:
                    break

            self.logger.debug(f"未找到键 {key}")
            return None

    def insert(self, key: int, value: Tuple[int, int]) -> bool:
        """
        插入键值对

        Args:
            key: 键
            value: (page_id, slot_id) 记录位置

        Returns:
            bool: 插入是否成功
        """
        try:
            # 查找应该插入的叶子节点
            leaf = self._find_leaf_for_insert(key)

            # 检查键是否已存在
            if key in leaf.keys:
                self.logger.warning(f"键 {key} 已存在")
                return False

            # 插入键值对
            insert_index = bisect.bisect_left(leaf.keys, key)
            leaf.keys.insert(insert_index, key)
            leaf.values.insert(insert_index, value)

            # 写回存储
            self._write_node(leaf)

            # 检查是否需要分裂
            if len(leaf.keys) > self.order:
                self._split_leaf(leaf)

            self.logger.debug(f"插入键 {key}，值: {value}")
            return True

        except Exception as e:
            self.logger.error(f"插入失败: {e}")
            raise StorageException(f"B+树插入失败: {e}")

    def _find_leaf_for_insert(self, key: int) -> BTreeNode:
        """
        找到应该插入键的叶子节点

        Args:
            key: 要插入的键

        Returns:
            BTreeNode: 叶子节点
        """
        current = self._read_node(self.root_page_id)

        # 向下查找直到叶子节点
        while not current.is_leaf:
            child_index = self._find_child_index(current, key)
            child_page_id = current.children[child_index]
            current = self._read_node(child_page_id)

        return current

    def _find_child_index(self, node: BTreeNode, key: int) -> int:
        """
        在内部节点中找到合适的子节点索引

        Args:
            node: 内部节点
            key: 键

        Returns:
            int: 子节点索引
        """
        # 使用二分查找
        index = bisect.bisect_right(node.keys, key)
        return index

    def _split_leaf(self, leaf: BTreeNode):
        """
        分裂叶子节点（修复版）

        Args:
            leaf: 需要分裂的叶子节点
        """
        # 创建新节点
        new_page_id = self.storage.allocate_page()
        new_leaf = BTreeNode(new_page_id, is_leaf=True, order=self.order)

        # 计算分裂点
        mid = len(leaf.keys) // 2

        # 分配键值
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]

        # 保存原始的下一个节点
        old_next = leaf.next_leaf_id

        # 更新原节点
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]

        # 更新链表指针
        new_leaf.next_leaf_id = old_next
        leaf.next_leaf_id = new_page_id

        # 写入两个节点
        self._write_node(leaf)
        self._write_node(new_leaf)

        self.logger.info(f"节点 {leaf.page_id} 分裂，创建新节点 {new_page_id}")
        self.logger.debug(f"原节点保留键: {leaf.keys}")
        self.logger.debug(f"新节点获得键: {new_leaf.keys}")

        # TODO: 处理父节点的更新
        # 这里需要实现向父节点插入新键的逻辑
        # 暂时简化处理

    def range_search(self, start_key: int, end_key: int) -> List[Tuple[int, Tuple[int, int]]]:
        """
        范围查询

        Args:
            start_key: 起始键（包含）
            end_key: 结束键（包含）

        Returns:
            List[(key, (page_id, slot_id))]: 键值对列表
        """
        result = []

        # 找到起始叶子节点
        current = self._find_leaf_for_insert(start_key)

        # 遍历叶子节点链表
        while current is not None:
            for i, key in enumerate(current.keys):
                if key > end_key:
                    # 超出范围，结束查询
                    return result
                if key >= start_key:
                    result.append((key, current.values[i]))

            # 移动到下一个叶子节点
            if current.next_leaf_id is not None:
                current = self._read_node(current.next_leaf_id)
            else:
                break

        self.logger.debug(f"范围查询 [{start_key}, {end_key}]，找到 {len(result)} 条记录")
        return result

    def delete(self, key: int) -> bool:
        """
        删除键（简化版本）

        Args:
            key: 要删除的键

        Returns:
            bool: 删除是否成功
        """
        # 找到包含键的叶子节点
        leaf = self._find_leaf_for_insert(key)

        try:
            key_index = leaf.keys.index(key)
            leaf.keys.pop(key_index)
            leaf.values.pop(key_index)

            # 写回存储
            self._write_node(leaf)

            self.logger.debug(f"删除键 {key}")
            return True

        except ValueError:
            self.logger.warning(f"删除失败，键 {key} 不存在")
            return False

    def print_tree(self):
        """打印树结构（用于调试）"""
        print(f"\n=== B+树结构 (索引: {self.index_name}) ===")
        self._print_node(self.root_page_id, 0)

        # 打印叶子节点链表
        print("\n叶子节点链表:")
        leaf = self._find_leftmost_leaf()
        while leaf is not None:
            print(f"  节点{leaf.page_id}: 键={leaf.keys}, next={leaf.next_leaf_id}")
            if leaf.next_leaf_id is not None:
                leaf = self._read_node(leaf.next_leaf_id)
            else:
                break

    def _find_leftmost_leaf(self) -> BTreeNode:
        """找到最左边的叶子节点"""
        current = self._read_node(self.root_page_id)
        while not current.is_leaf:
            if current.children:
                current = self._read_node(current.children[0])
            else:
                break
        return current

    def _print_node(self, page_id: int, level: int):
        """递归打印节点"""
        node = self._read_node(page_id)
        indent = "  " * level

        if node.is_leaf:
            print(f"{indent}[叶子 {page_id}] 键: {node.keys[:5]}{'...' if len(node.keys) > 5 else ''}")
        else:
            print(f"{indent}[内部 {page_id}] 键: {node.keys}")
            for child_id in node.children:
                self._print_node(child_id, level + 1)