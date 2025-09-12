import os
import json
import time
from typing import Dict, List, Optional
from ..utils.exceptions import StorageException
from ..utils.constants import PAGE_SIZE


class TablespaceManager:
    """表空间管理器 - 管理数据库的表空间和文件组织"""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.tablespaces: Dict[str, dict] = {}
        self.metadata_file = os.path.join(data_dir, "tablespaces.json")

        # 确保数据目录存在
        os.makedirs(data_dir, exist_ok=True)

        # 加载或创建默认表空间
        self._load_metadata()
        self._ensure_default_tablespace()

    def _load_metadata(self):
        """加载表空间元数据"""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    self.tablespaces = json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load tablespace metadata: {e}")
                self.tablespaces = {}
        else:
            self.tablespaces = {}

    def _save_metadata(self):
        """保存表空间元数据"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.tablespaces, f, indent=2)
        except Exception as e:
            raise StorageException(f"Failed to save tablespace metadata: {e}")

    def _ensure_default_tablespace(self):
        """确保默认表空间存在"""
        if "default" not in self.tablespaces:
            default_file = os.path.join(self.data_dir, "default_tablespace.db")
            self.tablespaces["default"] = {
                "name": "default",
                "file_path": default_file,
                "size_mb": 100,
                "used_mb": 0,
                "created_time": time.time(),
                "is_default": True,
                "status": "active"
            }
            self._save_metadata()

    def create_tablespace(self, name: str, file_path: str = None, size_mb: int = 100) -> bool:
        """创建新的表空间"""
        if name in self.tablespaces:
            raise StorageException(f"Tablespace '{name}' already exists")

        # 如果没有指定文件路径，使用默认路径
        if file_path is None:
            file_path = os.path.join(self.data_dir, f"{name}_tablespace.db")

        # 创建表空间元数据
        tablespace_info = {
            "name": name,
            "file_path": file_path,
            "size_mb": size_mb,
            "used_mb": 0,
            "created_time": time.time(),
            "is_default": False,
            "status": "active"
        }

        # 创建物理文件（如果不存在）
        try:
            if not os.path.exists(file_path):
                # 创建空文件
                with open(file_path, 'wb') as f:
                    f.write(b'\x00' * (size_mb * 1024 * 1024))  # 预分配空间

            self.tablespaces[name] = tablespace_info
            self._save_metadata()

            # 新增：通知其他组件更新文件映射
            self._notify_file_mapping_update()

            return True

        except Exception as e:
            raise StorageException(f"Failed to create tablespace '{name}': {e}")

    def _notify_file_mapping_update(self):
        """通知需要更新文件映射的组件"""
        # 这是一个钩子方法，在实际集成时会被 StorageManager 重写
        pass

    def get_tablespace_info(self, name: str) -> Optional[dict]:
        """获取表空间信息"""
        return self.tablespaces.get(name)

    def list_tablespaces(self) -> List[dict]:
        """列出所有表空间"""
        return list(self.tablespaces.values())

    def drop_tablespace(self, name: str, force: bool = False) -> bool:
        """删除表空间"""
        if name == "default":
            raise StorageException("Cannot drop default tablespace")

        if name not in self.tablespaces:
            raise StorageException(f"Tablespace '{name}' does not exist")

        tablespace = self.tablespaces[name]

        try:
            # 如果force=True，删除物理文件
            if force and os.path.exists(tablespace["file_path"]):
                os.remove(tablespace["file_path"])

            # 从元数据中移除
            del self.tablespaces[name]
            self._save_metadata()
            return True

        except Exception as e:
            raise StorageException(f"Failed to drop tablespace '{name}': {e}")

    def get_tablespace_file_path(self, name: str = "default") -> str:
        """获取表空间的文件路径"""
        if name not in self.tablespaces:
            name = "default"  # 回退到默认表空间

        return self.tablespaces[name]["file_path"]

    def choose_tablespace_for_table(self, table_name: str) -> str:
        """为表选择合适的表空间（简单策略：使用默认表空间）"""
        # 简单实现：总是返回默认表空间
        # 未来可以扩展更复杂的选择策略
        return "default"

    def get_tablespace_for_table(self, table_name: str) -> str:
        """
        根据表名获取其所属的表空间
        如果没有指定，返回默认表空间
        """
        # 这里可以实现表到表空间的映射逻辑
        # 简单起见，我们先都返回默认表空间
        return "default"

    def allocate_tablespace_for_table(self, table_name: str, preferred_tablespace: str = None) -> str:
        """
        智能分配表空间策略 - 根据表名自动选择合适的表空间

        Args:
            table_name: 表名
            preferred_tablespace: 首选表空间，如果不指定则使用智能策略

        Returns:
            str: 分配的表空间名称
        """
        if preferred_tablespace and preferred_tablespace in self.tablespaces:
            print(f"使用指定表空间 '{preferred_tablespace}' 为表 '{table_name}'")
            return preferred_tablespace

        # 确保系统需要的表空间都存在
        self._ensure_system_tablespaces()

        # 基于表名的智能分配策略
        if table_name.startswith(('sys_', 'pg_', 'system_', 'catalog_')):
            tablespace = "system"
            print(f"检测到系统表 '{table_name}' → 分配到系统表空间")
        elif table_name.startswith(('temp_', 'tmp_', 'sort_')):
            tablespace = "temp"
            print(f"检测到临时表 '{table_name}' → 分配到临时表空间")
        elif table_name.startswith(('log_', 'audit_', 'history_')):
            tablespace = "log"
            print(f"检测到日志表 '{table_name}' → 分配到日志表空间")
        else:
            tablespace = "user_data"
            print(f"检测到用户表 '{table_name}' → 分配到用户数据表空间")

        # 确保目标表空间存在
        if tablespace not in self.tablespaces:
            print(f"表空间 '{tablespace}' 不存在，回退到默认表空间")
            tablespace = "default"

        return tablespace

    def _ensure_system_tablespaces(self):
        """
        确保系统需要的表空间都存在
        如果不存在则自动创建
        """
        # 定义系统需要的表空间配置
        required_tablespaces = {
            "system": {
                "size_mb": 50,
                "description": "系统表和元数据"
            },
            "user_data": {
                "size_mb": 200,
                "description": "用户业务数据"
            },
            "temp": {
                "size_mb": 100,
                "description": "临时表和排序数据"
            },
            "log": {
                "size_mb": 50,
                "description": "日志和审计数据"
            }
        }

        created_count = 0
        for name, config in required_tablespaces.items():
            if name not in self.tablespaces:
                try:
                    # 生成表空间文件路径
                    file_path = os.path.join(self.data_dir, f"{name}_tablespace.db")

                    # 创建表空间
                    success = self.create_tablespace(
                        name=name,
                        file_path=file_path,
                        size_mb=config["size_mb"]
                    )

                    if success:
                        created_count += 1
                        print(f"自动创建表空间: {name} ({config['description']}) - {config['size_mb']}MB")
                    else:
                        print(f"警告: 创建表空间 '{name}' 失败")

                except Exception as e:
                    print(f"错误: 无法创建表空间 '{name}': {e}")

        if created_count > 0:
            print(f"成功自动创建了 {created_count} 个系统表空间")
        else:
            print("所有必需的表空间都已存在")

    def get_all_tablespace_files(self) -> Dict[str, str]:
        """获取所有表空间的文件路径映射"""
        return {name: info["file_path"] for name, info in self.tablespaces.items()}