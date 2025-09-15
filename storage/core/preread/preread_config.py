"""
预读配置管理
"""

from enum import Enum
from typing import Dict, Any
import time


class PrereadMode(Enum):
    """预读模式枚举"""
    DISABLED = "disabled"  # 禁用预读
    SEQUENTIAL = "sequential"  # 仅顺序预读
    TABLE_AWARE = "table_aware"  # 表感知预读
    EXTENT_BASED = "extent_based"  # 基于区的预读
    ADAPTIVE = "adaptive"  # 自适应预读


class PrereadConfig:
    """预读配置类"""

    def __init__(self):
        # 基础配置
        self.enabled = True
        self.mode = PrereadMode.ADAPTIVE
        self.max_preread_pages = 8  # 最大预读页数
        self.preread_threshold = 2  # 触发预读的连续访问阈值

        # 顺序预读配置
        self.sequential_window_size = 4  # 顺序预读窗口大小
        self.sequential_trigger_count = 2  # 触发顺序预读的连续访问次数

        # 表感知配置
        self.table_specific_config = {
            'default': {
                'preread_size': 4,
                'aggressiveness': 0.5  # 0.0-1.0，预读激进程度
            }
        }

        # 区级预读配置
        self.extent_preread_ratio = 0.25  # 预读区内页面的比例

        # 性能控制
        self.max_cache_usage_for_preread = 0.3  # 预读页面最多占用缓存的30%
        self.preread_timeout_seconds = 0.1  # 预读操作超时时间

        # 统计配置
        self.enable_statistics = True
        self.statistics_window_size = 1000  # 统计窗口大小

        # 创建时间
        self.created_time = time.time()

    def get_table_config(self, table_name: str) -> Dict[str, Any]:
        """获取表的特定配置"""
        return self.table_specific_config.get(table_name,
                                              self.table_specific_config['default'])

    def set_table_config(self, table_name: str, config: Dict[str, Any]):
        """设置表的特定配置"""
        self.table_specific_config[table_name] = config

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'enabled': self.enabled,
            'mode': self.mode.value,
            'max_preread_pages': self.max_preread_pages,
            'preread_threshold': self.preread_threshold,
            'sequential_window_size': self.sequential_window_size,
            'sequential_trigger_count': self.sequential_trigger_count,
            'table_specific_config': self.table_specific_config,
            'extent_preread_ratio': self.extent_preread_ratio,
            'max_cache_usage_for_preread': self.max_cache_usage_for_preread,
            'preread_timeout_seconds': self.preread_timeout_seconds,
            'enable_statistics': self.enable_statistics,
            'statistics_window_size': self.statistics_window_size,
            'created_time': self.created_time
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PrereadConfig':
        """从字典创建配置对象"""
        config = cls()
        config.enabled = data.get('enabled', True)
        config.mode = PrereadMode(data.get('mode', 'adaptive'))
        config.max_preread_pages = data.get('max_preread_pages', 8)
        config.preread_threshold = data.get('preread_threshold', 2)
        config.sequential_window_size = data.get('sequential_window_size', 4)
        config.sequential_trigger_count = data.get('sequential_trigger_count', 2)
        config.table_specific_config = data.get('table_specific_config', {})
        config.extent_preread_ratio = data.get('extent_preread_ratio', 0.25)
        config.max_cache_usage_for_preread = data.get('max_cache_usage_for_preread', 0.3)
        config.preread_timeout_seconds = data.get('preread_timeout_seconds', 0.1)
        config.enable_statistics = data.get('enable_statistics', True)
        config.statistics_window_size = data.get('statistics_window_size', 1000)
        config.created_time = data.get('created_time', time.time())
        return config