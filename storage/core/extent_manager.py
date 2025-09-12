"""
区管理器 - 第一版：基础版本
只实现核心的区概念，暂不涉及复杂的智能决策
"""

import time
from typing import Dict, List, Optional, Set
from ..utils.logger import get_logger
from ..utils.exceptions import StorageException


class ExtentMetadata:
    """区元数据 - 简化版"""

    def __init__(self, extent_id: int, start_page: int, size: int = 64):
        self.extent_id = extent_id
        self.start_page = start_page  # 区的起始页号
        self.size = size  # 区内页数，默认64页
        self.allocated_pages: Set[int] = set()  # 已分配的页号
        self.created_time = time.time()
        self.tablespace = "default"  # 添加这一行

    @property
    def end_page(self) -> int:
        """区的结束页号"""
        return self.start_page + self.size - 1

    @property
    def free_count(self) -> int:
        """空闲页数"""
        return self.size - len(self.allocated_pages)

    def is_full(self) -> bool:
        """区是否已满"""
        return len(self.allocated_pages) >= self.size

    def is_empty(self) -> bool:  # 添加这个方法
        """区是否为空"""
        return len(self.allocated_pages) == 0

    def allocate_page_in_extent(self) -> Optional[int]:
        """在区内分配一个页"""
        if self.is_full():
            return None

        # 找到区内第一个未使用的页号
        for offset in range(self.size):
            page_id = self.start_page + offset
            if page_id not in self.allocated_pages:
                self.allocated_pages.add(page_id)
                return page_id

        return None

    def deallocate_page_in_extent(self, page_id: int) -> bool:  # 添加这个方法！
        """在区内释放一个页"""
        if page_id in self.allocated_pages:
            self.allocated_pages.remove(page_id)
            return True
        return False


class ExtentManager:
    """区管理器 - 简化版本"""

    def __init__(self, page_manager, extent_size: int = 64):
        self.page_manager = page_manager
        self.extent_size = extent_size
        self.extents: Dict[int, ExtentMetadata] = {}  # extent_id -> ExtentMetadata
        self.page_to_extent: Dict[int, int] = {}  # 添加这一行！！！
        self.next_extent_id = 1
        self.logger = get_logger("extent_manager")

        # 简单的统计
        self.total_extents_created = 0

        self.logger.info(f"ExtentManager initialized (basic version)")

    def get_stats(self) -> dict:
        """获取基础统计信息"""
        return {
            'total_extents': len(self.extents),
            'extents_created': self.total_extents_created,
            'extent_size': self.extent_size
        }

    def list_extents(self) -> List[dict]:
        """列出所有区的信息"""
        result = []
        for extent_id, extent in self.extents.items():
            result.append({
                'extent_id': extent_id,
                'start_page': extent.start_page,
                'end_page': extent.end_page,
                'allocated_pages': len(extent.allocated_pages),
                'free_pages': extent.free_count,
                'is_full': extent.is_full()
            })
        return result

    def allocate_page_smart(self, table_name: str = "unknown",
                            tablespace_name: str = "default") -> int:
        """
        智能页分配 - 真正的实现版本
        """
        self.logger.info(
            f"=== EXTENT DEBUG: allocate_page_smart called with table='{table_name}', tablespace='{tablespace_name}' ===")
        self.logger.debug(f"Smart allocation requested for table '{table_name}' in tablespace '{tablespace_name}'")

        # 简单的智能决策：如果表名不是"unknown"，尝试使用区分配
        if table_name != "unknown":
            # 策略1：尝试从现有区分配
            page_id = self._try_allocate_from_existing_extent(table_name, tablespace_name)
            if page_id is not None:
                return page_id

            # 策略2：考虑创建新区
            if self._should_create_new_extent(table_name):
                return self._create_extent_and_allocate(table_name, tablespace_name)

        # 回退到单页分配
        page_id = self.page_manager.allocate_page(tablespace_name)
        self.logger.debug(f"Smart allocated single page {page_id} for table '{table_name}'")
        return page_id

    def _try_allocate_from_existing_extent(self, table_name: str, tablespace_name: str) -> Optional[int]:
        """从现有区中尝试分配页"""
        for extent_id, extent in self.extents.items():
            # 简单策略：同一个表空间的区都可以使用
            if extent.tablespace == tablespace_name and not extent.is_full():
                page_id = extent.allocate_page_in_extent()
                if page_id is not None:
                    self.page_to_extent[page_id] = extent_id
                    self.logger.info(f"Allocated page {page_id} from existing extent {extent_id}")
                    return page_id

        return None

    def _should_create_new_extent(self, table_name: str) -> bool:
        """简单决策：是否应该创建新区"""
        # 策略：如果还没有区，就创建一个
        # 或者如果表名包含"large"、"big"等关键字
        if len(self.extents) == 0:
            self.logger.debug(f"No extents exist, will create new extent for table '{table_name}'")
            return True

        # 检查表名特征
        large_table_indicators = ["large", "big", "user", "log", "data", "main"]
        if any(indicator in table_name.lower() for indicator in large_table_indicators):
            self.logger.debug(f"Table '{table_name}' matches large table pattern")
            return True

        return False

    def _create_extent_and_allocate(self, table_name: str, tablespace_name: str) -> int:
        """创建新区并分配第一个页"""
        try:
            # 分配第一个页作为区的"基础页"
            first_page = self.page_manager.allocate_page(tablespace_name)

            # 创建新区（使用逻辑区概念，不要求物理连续）
            extent_id = self.next_extent_id
            self.next_extent_id += 1

            # 创建区元数据（起始页就是第一个分配的页）
            extent = ExtentMetadata(extent_id, first_page, self.extent_size)
            extent.tablespace = tablespace_name
            extent.allocated_pages.add(first_page)  # 标记第一个页已分配

            # 注册区
            self.extents[extent_id] = extent
            self.page_to_extent[first_page] = extent_id
            self.total_extents_created += 1

            # 预分配更多页到这个区（可选）
            self._pre_allocate_pages_to_extent(extent, tablespace_name, 3)  # 预分配3个页

            self.logger.info(f"Created extent {extent_id} for table '{table_name}' starting with page {first_page}")
            return first_page

        except Exception as e:
            self.logger.error(f"Failed to create extent for table '{table_name}': {e}")
            # 回退到单页分配
            return self.page_manager.allocate_page(tablespace_name)

    def _pre_allocate_pages_to_extent(self, extent: ExtentMetadata, tablespace_name: str, count: int):
        """为区预分配一些页（优化性能）"""
        try:
            allocated_count = 0
            for i in range(count):
                if extent.is_full():
                    break

                page_id = self.page_manager.allocate_page(tablespace_name)

                # 我们不要求页号连续，只要在区的管理范围内即可
                # 调整区的范围以包含新页
                if page_id < extent.start_page:
                    extent.start_page = page_id
                elif page_id > extent.end_page:
                    # 动态扩展区的范围
                    extent.size = page_id - extent.start_page + 1

                self.page_to_extent[page_id] = extent.extent_id
                allocated_count += 1

            if allocated_count > 0:
                self.logger.debug(f"Pre-allocated {allocated_count} additional pages to extent {extent.extent_id}")

        except Exception as e:
            self.logger.warning(f"Pre-allocation failed: {e}")

    def deallocate_page_smart(self, page_id: int):
        """智能页释放 - 完整实现"""
        # 检查页是否属于某个区
        if page_id in self.page_to_extent:
            extent_id = self.page_to_extent[page_id]
            extent = self.extents.get(extent_id)

            if extent and extent.deallocate_page_in_extent(page_id):
                del self.page_to_extent[page_id]
                self.logger.info(f"Deallocated page {page_id} from extent {extent_id}")

                # 如果区变空了，考虑回收
                if extent.is_empty():
                    self._recycle_extent(extent_id)

                # 通过page_manager释放页
                self.page_manager.deallocate_page(page_id)
                return

        # 不属于区的页，直接释放
        self.page_manager.deallocate_page(page_id)
        self.logger.debug(f"Deallocated single page {page_id}")

    def _recycle_extent(self, extent_id: int):
        """回收空区"""
        extent = self.extents.get(extent_id)
        if not extent or not extent.is_empty():
            return

        try:
            # 清理页映射
            pages_to_remove = [page_id for page_id, eid in self.page_to_extent.items()
                               if eid == extent_id]
            for page_id in pages_to_remove:
                del self.page_to_extent[page_id]

            # 移除区
            del self.extents[extent_id]

            self.logger.info(f"Recycled empty extent {extent_id}")

        except Exception as e:
            self.logger.error(f"Failed to recycle extent {extent_id}: {e}")

    # 修改ExtentMetadata以支持动态大小
    def deallocate_page_in_extent(self, page_id: int) -> bool:
        """在区内释放一个页"""
        if page_id in self.allocated_pages:
            self.allocated_pages.remove(page_id)
            return True
        return False

