"""
B+树节点实现 - 负责节点的内存表示和序列化
"""

import struct
from typing import List, Optional, Tuple
from ...utils.constants import PAGE_SIZE


class BTreeNode:
    """
    B+树节点类
    每个节点对应一个4KB的页
    """

    # 节点头部格式（16字节）
    # 1字节: 节点类型(0=叶子, 1=内部)
    # 2字节: 键数量
    # 4字节: 父节点页号
    # 4字节: 右兄弟页号(叶子节点用)
    # 5字节: 预留
    HEADER_FORMAT = 'B H I I 5x'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, page_id: int, is_leaf: bool = True, order: int = 300):
        """
        初始化B+树节点

        Args:
            page_id: 节点对应的页号
            is_leaf: 是否为叶子节点
            order: 节点最大键数
        """
        self.page_id = page_id
        self.is_leaf = is_leaf
        self.order = order

        self.keys: List[int] = []  # 键列表
        self.parent_id: Optional[int] = None  # 父节点页号

        if is_leaf:
            # 叶子节点特有
            self.values: List[Tuple[int, int]] = []  # 值列表 [(page_id, slot_id), ...]
            self.next_leaf_id: Optional[int] = None  # 下一个叶子节点
        else:
            # 内部节点特有
            self.children: List[int] = []  # 子节点页号列表

    def is_full(self) -> bool:
        """检查节点是否已满"""
        return len(self.keys) >= self.order

    def is_empty(self) -> bool:
        """检查节点是否为空"""
        return len(self.keys) == 0

    def serialize(self) -> bytes:
        """
        将节点序列化为字节数据（4KB页）

        Returns:
            bytes: 长度为PAGE_SIZE的字节数据
        """
        # 创建4KB的字节数组
        page_data = bytearray(PAGE_SIZE)

        # 1. 写入头部信息
        node_type = 0 if self.is_leaf else 1
        key_count = len(self.keys)
        parent_id = self.parent_id if self.parent_id is not None else 0
        next_leaf = self.next_leaf_id if self.is_leaf and self.next_leaf_id is not None else 0

        header = struct.pack(
            self.HEADER_FORMAT,
            node_type,  # 节点类型
            key_count,  # 键数量
            parent_id,  # 父节点页号
            next_leaf  # 下一个叶子节点（仅叶子节点使用）
        )
        page_data[:self.HEADER_SIZE] = header

        # 2. 写入键数据
        offset = self.HEADER_SIZE
        for key in self.keys:
            # 每个键使用4字节整数
            page_data[offset:offset + 4] = struct.pack('I', key)
            offset += 4

        # 3. 写入值或子节点数据
        if self.is_leaf:
            # 叶子节点：写入值 (page_id, slot_id)
            for page_id, slot_id in self.values:
                page_data[offset:offset + 4] = struct.pack('I', page_id)
                offset += 4
                page_data[offset:offset + 2] = struct.pack('H', slot_id)
                offset += 2
        else:
            # 内部节点：写入子节点页号
            for child_id in self.children:
                page_data[offset:offset + 4] = struct.pack('I', child_id)
                offset += 4

        return bytes(page_data)

    @staticmethod
    def deserialize(page_data: bytes, order: int = 300) -> 'BTreeNode':
        """
        从字节数据反序列化节点

        Args:
            page_data: 页数据
            order: 节点最大键数

        Returns:
            BTreeNode: 反序列化后的节点对象
        """
        # 1. 读取头部
        header_data = page_data[:BTreeNode.HEADER_SIZE]
        node_type, key_count, parent_id, next_leaf = struct.unpack(
            BTreeNode.HEADER_FORMAT,
            header_data
        )

        is_leaf = (node_type == 0)

        # 2. 创建节点对象（注意：page_id需要外部传入，这里暂时用0）
        node = BTreeNode(page_id=0, is_leaf=is_leaf, order=order)
        node.parent_id = parent_id if parent_id != 0 else None

        if is_leaf:
            node.next_leaf_id = next_leaf if next_leaf != 0 else None

        # 3. 读取键
        offset = BTreeNode.HEADER_SIZE
        for i in range(key_count):
            key = struct.unpack('I', page_data[offset:offset + 4])[0]
            node.keys.append(key)
            offset += 4

        # 4. 读取值或子节点
        if is_leaf:
            # 读取值
            for i in range(key_count):
                page_id = struct.unpack('I', page_data[offset:offset + 4])[0]
                offset += 4
                slot_id = struct.unpack('H', page_data[offset:offset + 2])[0]
                offset += 2
                node.values.append((page_id, slot_id))
        else:
            # 读取子节点（子节点数 = 键数 + 1）
            for i in range(key_count + 1):
                child_id = struct.unpack('I', page_data[offset:offset + 4])[0]
                node.children.append(child_id)
                offset += 4

        return node