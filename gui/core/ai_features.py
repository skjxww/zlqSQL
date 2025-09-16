import os
from typing import Optional


class AIFeatureManager:
    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager
        self._init_ai_features()

    def _init_ai_features(self):
        """初始化AI功能"""
        try:
            import sys
            import os

            from extensions.enhanced_nl2sql import EnhancedNL2SQL
            from extensions.smart_completion import SmartSQLCompletion

            api_key = self._get_api_key()
            self.nl2sql_engine = EnhancedNL2SQL(self.catalog_manager, api_key=api_key)
            self.completion_engine = SmartSQLCompletion(self.catalog_manager)

            print("AI功能模块初始化成功")

        except ImportError as e:
            print(f"AI功能模块导入失败: {e}")
            self.nl2sql_engine = None
            self.completion_engine = None
        except Exception as e:
            print(f"AI功能初始化失败: {e}")
            self.nl2sql_engine = None
            self.completion_engine = None

    from dotenv import load_dotenv
    load_dotenv()

    def _get_api_key(self) -> Optional[str]:
        return os.getenv('DEEPSEEK_API')