"""
页管理器：负责硬盘文件的实际读写操作（重构版）
增加了异常处理、日志记录和更完善的元数据管理
"""

import os
import json
import time
import threading
from typing import Optional, Dict, List, Set, Any
from pathlib import Path

from ..utils.constants import PAGE_SIZE, DATA_FILE, META_FILE, MAX_PAGES
from ..utils.exceptions import (
    PageException, InvalidPageIdException, PageNotAllocatedException,
    DiskIOException, handle_storage_exceptions, StorageException
)
from ..utils.logger import get_logger, PerformanceTimer, performance_monitor


class PageMetadata:
    """页元数据类"""

    def __init__(self):
        self.next_page_id = 1  # 下一个可分配的页号
        self.free_pages = []  # 已释放可重用的页号列表
        self.allocated_pages = set()  # 已分配的页号集合
        self.page_usage = {}  # 页使用情况 {page_id: usage_info}
        self.last_modification = time.time()  # 最后修改时间
        self.version = "1.0"  # 元数据版本

    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            "next_page_id": self.next_page_id,
            "free_pages": self.free_pages,
            "allocated_pages": list(self.allocated_pages),
            "page_usage": self.page_usage,
            "last_modification": self.last_modification,
            "version": self.version,
            "total_allocated": len(self.allocated_pages),
            "total_free": len(self.free_pages)
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'PageMetadata':
        """从字典创建页元数据"""
        metadata = cls()
        metadata.next_page_id = data.get("next_page_id", 1)
        metadata.free_pages = data.get("free_pages", [])
        metadata.allocated_pages = set(data.get("allocated_pages", []))
        metadata.page_usage = data.get("page_usage", {})
        metadata.last_modification = data.get("last_modification", time.time())
        metadata.version = data.get("version", "1.0")
        return metadata


class PageManager:
    """页管理器类（增强版）"""

    def __init__(self, data_file: str = DATA_FILE, meta_file: str = META_FILE):
        """
        初始化页管理器

        Args:
            data_file: 数据文件路径
            meta_file: 元数据文件路径

        Raises:
            DiskIOException: 文件访问错误
        """
        self.data_file = Path(data_file)
        self.meta_file = Path(meta_file)
        self.metadata = PageMetadata()

        # 线程锁，确保并发安全
        self._lock = threading.RLock()

        # 统计信息
        self.read_count = 0
        self.write_count = 0
        self.allocation_count = 0
        self.deallocation_count = 0

        # 日志器
        self.logger = get_logger("page")

        # 初始化
        self._init_directories()
        self._load_metadata()
        self._init_data_file()

        self.logger.info("PageManager initialized",
                         data_file=str(self.data_file),
                         meta_file=str(self.meta_file),
                         allocated_pages=len(self.metadata.allocated_pages))

    def _init_directories(self):
        """初始化目录结构"""
        try:
            # 创建数据文件目录
            self.data_file.parent.mkdir(parents=True, exist_ok=True)
            self.meta_file.parent.mkdir(parents=True, exist_ok=True)

            self.logger.debug("Directories initialized",
                              data_dir=str(self.data_file.parent),
                              meta_dir=str(self.meta_file.parent))
        except Exception as e:
            raise DiskIOException(f"Failed to create directories: {e}",
                                  operation="directory_creation")

    def _init_data_file(self):
        """初始化数据文件"""
        try:
            if not self.data_file.exists():
                # 创建空的数据文件
                with open(self.data_file, 'wb') as f:
                    pass
                self.logger.info(f"Created new data file", file_path=str(self.data_file))
            else:
                # 验证现有文件
                file_size = self.data_file.stat().st_size
                expected_size = max(self.metadata.allocated_pages) * PAGE_SIZE if self.metadata.allocated_pages else 0

                if file_size < expected_size:
                    self.logger.warning("Data file size inconsistent with metadata",
                                        file_size=file_size,
                                        expected_size=expected_size)

        except Exception as e:
            raise DiskIOException(f"Failed to initialize data file: {e}",
                                  file_path=str(self.data_file),
                                  operation="file_initialization")

    @handle_storage_exceptions
    def _load_metadata(self):
        """从文件加载元数据"""
        try:
            if self.meta_file.exists():
                with open(self.meta_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.metadata = PageMetadata.from_dict(data)

                self.logger.info("Metadata loaded successfully",
                                 next_page_id=self.metadata.next_page_id,
                                 allocated_count=len(self.metadata.allocated_pages),
                                 free_count=len(self.metadata.free_pages))
            else:
                self.logger.info("Metadata file not found, using default configuration")

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid metadata file format: {e}")
            # 备份损坏的文件
            backup_file = self.meta_file.with_suffix('.backup')
            self.meta_file.rename(backup_file)
            self.logger.warning(f"Corrupted metadata backed up to {backup_file}")
            # 使用默认配置
            self.metadata = PageMetadata()

        except Exception as e:
            raise DiskIOException(f"Failed to load metadata: {e}",
                                  file_path=str(self.meta_file),
                                  operation="metadata_load")

    @handle_storage_exceptions
    def _save_metadata(self):
        """保存元数据到文件"""
        try:
            with self._lock:
                self.metadata.last_modification = time.time()

                # 原子写入：先写临时文件，再重命名
                temp_file = self.meta_file.with_suffix('.tmp')

                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(self.metadata.to_dict(), f, indent=2)

                # 原子替换
                temp_file.replace(self.meta_file)

                self.logger.debug("Metadata saved successfully")

        except Exception as e:
            # 清理临时文件
            temp_file = self.meta_file.with_suffix('.tmp')
            if temp_file.exists():
                temp_file.unlink()

            raise DiskIOException(f"Failed to save metadata: {e}",
                                  file_path=str(self.meta_file),
                                  operation="metadata_save")

    @handle_storage_exceptions
    @performance_monitor("page_allocation")
    def allocate_page(self) -> int:
        """
        分配一个新页

        Returns:
            int: 新分配的页号

        Raises:
            PageException: 页分配失败
        """
        with self._lock:
            try:
                # 检查页数限制
                if len(self.metadata.allocated_pages) >= MAX_PAGES:
                    raise PageException(f"Maximum page limit reached: {MAX_PAGES}")

                # 优先重用释放的页号
                if self.metadata.free_pages:
                    page_id = self.metadata.free_pages.pop(0)
                    self.logger.debug(f"Reusing freed page {page_id}")
                else:
                    # 分配新的页号
                    page_id = self.metadata.next_page_id
                    self.metadata.next_page_id += 1
                    self.logger.debug(f"Allocated new page {page_id}")

                # 记录到已分配列表
                self.metadata.allocated_pages.add(page_id)

                # 记录页使用信息
                self.metadata.page_usage[str(page_id)] = {
                    "allocated_time": time.time(),
                    "access_count": 0,
                    "last_access": None
                }

                # 保存元数据
                self._save_metadata()

                # 更新统计
                self.allocation_count += 1

                self.logger.info(f"Page allocated",
                                 page_id=page_id,
                                 total_allocated=len(self.metadata.allocated_pages))

                return page_id

            except Exception as e:
                raise PageException(f"Failed to allocate page: {e}")

    @handle_storage_exceptions
    @performance_monitor("page_deallocation")
    def deallocate_page(self, page_id: int):
        """
        释放一个页

        Args:
            page_id: 要释放的页号

        Raises:
            PageNotAllocatedException: 页未分配
        """
        with self._lock:
            if page_id not in self.metadata.allocated_pages:
                raise PageNotAllocatedException(page_id)

            try:
                # 从已分配列表移除
                self.metadata.allocated_pages.remove(page_id)

                # 添加到空闲列表
                if page_id not in self.metadata.free_pages:
                    self.metadata.free_pages.append(page_id)

                # 清理页使用信息
                if str(page_id) in self.metadata.page_usage:
                    del self.metadata.page_usage[str(page_id)]

                # 保存元数据
                self._save_metadata()

                # 更新统计
                self.deallocation_count += 1

                self.logger.info(f"Page deallocated",
                                 page_id=page_id,
                                 total_allocated=len(self.metadata.allocated_pages))

            except Exception as e:
                raise PageException(f"Failed to deallocate page {page_id}: {e}", page_id)

    @handle_storage_exceptions
    @performance_monitor("page_read")
    def read_page_from_disk(self, page_id: int) -> bytes:
        """
        从磁盘读取指定页的数据

        Args:
            page_id: 页号

        Returns:
            bytes: 页数据（长度为PAGE_SIZE）

        Raises:
            InvalidPageIdException: 页号无效
            PageNotAllocatedException: 页未分配
            DiskIOException: 磁盘读取错误
        """
        # 验证页号
        if page_id <= 0:
            raise InvalidPageIdException(page_id)

        # 检查页是否已分配（允许读取未分配的页，返回空数据）
        # if page_id not in self.metadata.allocated_pages:
        #     raise PageNotAllocatedException(page_id)

        try:
            with open(self.data_file, 'rb') as f:
                # 定位到指定页的位置
                offset = (page_id - 1) * PAGE_SIZE
                f.seek(offset)

                # 读取页数据
                data = f.read(PAGE_SIZE)

                # 如果读取的数据不足PAGE_SIZE，用0填充
                if len(data) < PAGE_SIZE:
                    data += b'\x00' * (PAGE_SIZE - len(data))

                # 更新统计和页使用信息
                self.read_count += 1
                if str(page_id) in self.metadata.page_usage:
                    usage = self.metadata.page_usage[str(page_id)]
                    usage["access_count"] += 1
                    usage["last_access"] = time.time()

                self.logger.debug(f"Read page from disk",
                                  page_id=page_id,
                                  data_length=len(data),
                                  file_offset=offset)

                return data

        except FileNotFoundError:
            raise DiskIOException(f"Data file not found: {self.data_file}",
                                  file_path=str(self.data_file),
                                  operation="page_read")
        except OSError as e:
            raise DiskIOException(f"Failed to read page {page_id}: {e}",
                                  file_path=str(self.data_file),
                                  operation="page_read")

    @handle_storage_exceptions
    @performance_monitor("page_write")
    def write_page_to_disk(self, page_id: int, data: bytes):
        """
        将数据写入磁盘指定页

        Args:
            page_id: 页号
            data: 要写入的数据

        Raises:
            InvalidPageIdException: 页号无效
            DiskIOException: 磁盘写入错误
        """
        # 验证页号
        if page_id <= 0:
            raise InvalidPageIdException(page_id)

        # 验证数据
        if not isinstance(data, bytes):
            raise PageException(f"Data must be bytes, got {type(data)}", page_id)

        try:
            # 确保数据长度为PAGE_SIZE
            if len(data) > PAGE_SIZE:
                data = data[:PAGE_SIZE]
                self.logger.warning(f"Data truncated to {PAGE_SIZE} bytes for page {page_id}")
            elif len(data) < PAGE_SIZE:
                data += b'\x00' * (PAGE_SIZE - len(data))

            # 确保文件存在且足够大
            offset = (page_id - 1) * PAGE_SIZE

            # 如果文件不存在或太小，扩展文件
            if not self.data_file.exists() or self.data_file.stat().st_size < offset + PAGE_SIZE:
                self._extend_file(offset + PAGE_SIZE)

            with open(self.data_file, 'r+b') as f:
                # 定位到指定页的位置
                f.seek(offset)

                # 写入数据
                f.write(data)
                f.flush()  # 确保写入磁盘
                os.fsync(f.fileno())  # 强制同步到磁盘

                # 更新统计和页使用信息
                self.write_count += 1
                if str(page_id) in self.metadata.page_usage:
                    usage = self.metadata.page_usage[str(page_id)]
                    usage["access_count"] += 1
                    usage["last_access"] = time.time()

                self.logger.debug(f"Wrote page to disk",
                                  page_id=page_id,
                                  data_length=len(data),
                                  file_offset=offset)

        except OSError as e:
            raise DiskIOException(f"Failed to write page {page_id}: {e}",
                                  file_path=str(self.data_file),
                                  operation="page_write")

    def _extend_file(self, target_size: int):
        """
        扩展数据文件到指定大小

        Args:
            target_size: 目标文件大小
        """
        try:
            with open(self.data_file, 'ab') as f:
                current_size = f.tell()
                if current_size < target_size:
                    f.write(b'\x00' * (target_size - current_size))
                    f.flush()

            self.logger.debug(f"Extended data file to {target_size} bytes")

        except Exception as e:
            raise DiskIOException(f"Failed to extend file: {e}",
                                  file_path=str(self.data_file),
                                  operation="file_extend")

    def get_page_count(self) -> int:
        """获取已分配的页数量"""
        return len(self.metadata.allocated_pages)

    def get_free_page_count(self) -> int:
        """获取空闲页数量"""
        return len(self.metadata.free_pages)

    def get_metadata_info(self) -> dict:
        """获取元数据信息"""
        info = self.metadata.to_dict()
        info.update({
            "data_file": str(self.data_file),
            "meta_file": str(self.meta_file),
            "data_file_size": self.data_file.stat().st_size if self.data_file.exists() else 0,
            "statistics": {
                "read_count": self.read_count,
                "write_count": self.write_count,
                "allocation_count": self.allocation_count,
                "deallocation_count": self.deallocation_count
            }
        })
        return info

    def is_page_allocated(self, page_id: int) -> bool:
        """检查页是否已分配"""
        return page_id in self.metadata.allocated_pages

    def get_allocated_pages(self) -> Set[int]:
        """获取所有已分配的页号"""
        return self.metadata.allocated_pages.copy()

    def get_free_pages(self) -> List[int]:
        """获取所有空闲页号"""
        return self.metadata.free_pages.copy()

    def compact_free_pages(self):
        """整理空闲页列表，移除重复项并排序"""
        with self._lock:
            # 去重并排序
            self.metadata.free_pages = sorted(list(set(self.metadata.free_pages)))
            # 移除已分配的页
            self.metadata.free_pages = [
                page_id for page_id in self.metadata.free_pages
                if page_id not in self.metadata.allocated_pages
            ]
            self._save_metadata()
            self.logger.info(f"Compacted free pages list, {len(self.metadata.free_pages)} pages available")

    def validate_metadata(self) -> Dict[str, bool]:
        """
        验证元数据一致性

        Returns:
            Dict[str, bool]: 验证结果
        """
        results = {}

        # 检查页号范围
        results['page_ids_valid'] = all(
            page_id > 0 for page_id in self.metadata.allocated_pages
        )

        # 检查已分配页和空闲页没有重叠
        allocated_set = set(self.metadata.allocated_pages)
        free_set = set(self.metadata.free_pages)
        results['no_overlap'] = allocated_set.isdisjoint(free_set)

        # 检查next_page_id的合理性
        max_allocated = max(self.metadata.allocated_pages) if self.metadata.allocated_pages else 0
        results['next_page_id_valid'] = self.metadata.next_page_id > max_allocated

        # 检查文件大小
        if self.data_file.exists():
            file_size = self.data_file.stat().st_size
            required_size = max_allocated * PAGE_SIZE if max_allocated > 0 else 0
            results['file_size_sufficient'] = file_size >= required_size
        else:
            results['file_size_sufficient'] = len(self.metadata.allocated_pages) == 0

        return results

    def repair_metadata(self) -> Dict[str, Any]:
        """
        修复元数据不一致问题

        Returns:
            Dict[str, Any]: 修复结果
        """
        with self._lock:
            repair_log = []

            # 修复页号重叠问题
            allocated_set = set(self.metadata.allocated_pages)
            free_set = set(self.metadata.free_pages)
            overlap = allocated_set & free_set

            if overlap:
                # 从空闲页列表中移除已分配的页
                self.metadata.free_pages = [
                    page_id for page_id in self.metadata.free_pages
                    if page_id not in allocated_set
                ]
                repair_log.append(f"Removed {len(overlap)} overlapping pages from free list")

            # 修复next_page_id
            if self.metadata.allocated_pages:
                max_allocated = max(self.metadata.allocated_pages)
                if self.metadata.next_page_id <= max_allocated:
                    self.metadata.next_page_id = max_allocated + 1
                    repair_log.append(f"Updated next_page_id to {self.metadata.next_page_id}")

            # 保存修复后的元数据
            self._save_metadata()

            self.logger.info("Metadata repaired", repairs=repair_log)

            return {
                "repairs_made": repair_log,
                "validation_results": self.validate_metadata()
            }

    def get_statistics(self) -> dict:
        """获取页管理器统计信息"""
        return {
            "pages": {
                "allocated": len(self.metadata.allocated_pages),
                "free": len(self.metadata.free_pages),
                "next_id": self.metadata.next_page_id,
                "max_pages": MAX_PAGES
            },
            "operations": {
                "reads": self.read_count,
                "writes": self.write_count,
                "allocations": self.allocation_count,
                "deallocations": self.deallocation_count
            },
            "files": {
                "data_file": str(self.data_file),
                "data_file_size": self.data_file.stat().st_size if self.data_file.exists() else 0,
                "meta_file": str(self.meta_file),
                "last_modification": self.metadata.last_modification
            }
        }

    def cleanup(self):
        """清理资源"""
        try:
            self._save_metadata()
            self.logger.info("PageManager cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.cleanup()