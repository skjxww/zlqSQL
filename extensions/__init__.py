"""
SQL编译器扩展模块
包含AI驱动的智能功能
"""

__version__ = "1.0.0"
__author__ = "Your Name"

# 导出主要类
try:
    from .enhanced_nl2sql import EnhancedNL2SQL
    from .plan_visualizer import ExecutionPlanVisualizer
    from .smart_completion import SmartSQLCompletion

    __all__ = [
        'EnhancedNL2SQL',
        'ExecutionPlanVisualizer',
        'SmartSQLCompletion'
    ]

except ImportError as e:
    print(f"Warning: Some extensions failed to import: {e}")
    __all__ = []