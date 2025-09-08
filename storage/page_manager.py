# page_manager.py
"""
页管理器：负责硬盘文件的实际读写操作
"""

import os
import json
from typing import Optional
from constants import PAGE_SIZE, DATA_FILE, META_FILE


class PageManager:
    """页管理器类"""

    def __init__(self, data_file: str = DATA_FILE, meta_file: str = META_FILE):
        """
        初始化页管理器

        Args:
            data_file: 数据文件路径
            meta_file: 元数据文件路径
        """
        self.data_file = data_file
        self.meta_file = meta_file
        self.metadata = {
            "next_page_id": 1,  # 下一个可分配的页号
            "free_pages": [],  # 已释放可重用的页号列表
            "allocated_pages": []  # 已分配的页号列表
        }
        self._load_metadata()
        self._init_data_file()

    def _init_data_file(self):
        """初始化数据文件"""
        if not os.path.exists(self.data_file):
            # 创建空的数据文件
            with open(self.data_file, 'wb') as f:
                pass
            print(f"创建数据文件: {self.data_file}")

    def _load_metadata(self):
        """从文件加载元数据"""
        try:
            if os.path.exists(self.meta_file):
                with open(self.meta_file, 'r', encoding='utf-8') as f:
                    self.metadata = json.load(f)
                print(f"加载元数据成功，下一个页号: {self.metadata['next_page_id']}")
            else:
                print("元数据文件不存在，使用默认配置")
        except Exception as e:
            print(f"加载元数据失败: {e}，使用默认配置")
            self.metadata = {
                "next_page_id": 1,
                "free_pages": [],
                "allocated_pages": []
            }

    def _save_metadata(self):
        """保存元数据到文件"""
        try:
            with open(self.meta_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=2)
            print(f"元数据保存成功")
        except Exception as e:
            print(f"保存元数据失败: {e}")

    def allocate_page(self) -> int:
        """
        分配一个新页

        Returns:
            int: 新分配的页号
        """
        # 优先重用释放的页号
        if self.metadata["free_pages"]:
            page_id = self.metadata["free_pages"].pop(0)
            print(f"重用页号: {page_id}")
        else:
            # 分配新的页号
            page_id = self.metadata["next_page_id"]
            self.metadata["next_page_id"] += 1
            print(f"分配新页号: {page_id}")

        # 记录到已分配列表
        if page_id not in self.metadata["allocated_pages"]:
            self.metadata["allocated_pages"].append(page_id)

        self._save_metadata()
        return page_id

    def deallocate_page(self, page_id: int):
        """
        释放一个页

        Args:
            page_id: 要释放的页号
        """
        if page_id in self.metadata["allocated_pages"]:
            self.metadata["allocated_pages"].remove(page_id)
            if page_id not in self.metadata["free_pages"]:
                self.metadata["free_pages"].append(page_id)
            self._save_metadata()
            print(f"释放页号: {page_id}")
        else:
            print(f"警告: 页号 {page_id} 未分配，无法释放")

    def read_page_from_disk(self, page_id: int) -> bytes:
        """
        从磁盘读取指定页的数据

        Args:
            page_id: 页号

        Returns:
            bytes: 页数据（长度为PAGE_SIZE）
        """
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

                print(f"从磁盘读取页 {page_id}，数据长度: {len(data)}")
                return data

        except Exception as e:
            print(f"读取页 {page_id} 失败: {e}")
            # 返回空页
            return b'\x00' * PAGE_SIZE

    def write_page_to_disk(self, page_id: int, data: bytes):
        """
        将数据写入磁盘指定页

        Args:
            page_id: 页号
            data: 要写入的数据
        """
        try:
            # 确保数据长度为PAGE_SIZE
            if len(data) > PAGE_SIZE:
                data = data[:PAGE_SIZE]
                print(f"警告: 数据被截断到 {PAGE_SIZE} 字节")
            elif len(data) < PAGE_SIZE:
                data += b'\x00' * (PAGE_SIZE - len(data))

            with open(self.data_file, 'r+b') as f:
                # 定位到指定页的位置
                offset = (page_id - 1) * PAGE_SIZE
                f.seek(offset)

                # 写入数据
                f.write(data)
                f.flush()  # 确保写入磁盘

                print(f"向磁盘写入页 {page_id}，数据长度: {len(data)}")

        except FileNotFoundError:
            # 如果文件不存在，创建文件并写入
            with open(self.data_file, 'wb') as f:
                # 填充到目标位置
                offset = (page_id - 1) * PAGE_SIZE
                f.write(b'\x00' * offset)
                f.write(data)
                f.flush()
                print(f"创建文件并写入页 {page_id}")

        except Exception as e:
            print(f"写入页 {page_id} 失败: {e}")

    def get_page_count(self) -> int:
        """获取已分配的页数量"""
        return len(self.metadata["allocated_pages"])

    def get_metadata_info(self) -> dict:
        """获取元数据信息"""
        return {
            "next_page_id": self.metadata["next_page_id"],
            "allocated_pages_count": len(self.metadata["allocated_pages"]),
            "free_pages_count": len(self.metadata["free_pages"]),
            "allocated_pages": self.metadata["allocated_pages"].copy(),
            "free_pages": self.metadata["free_pages"].copy()
        }