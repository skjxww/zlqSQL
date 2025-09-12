"""
WAL统计和监控
提供详细的性能指标和健康检查
"""

import time
import json
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict, field
from pathlib import Path

from ...utils.logger import get_logger


@dataclass
class WALMetrics:
    """WAL性能指标"""
    # 写入指标
    total_writes: int = 0
    total_bytes_written: int = 0
    write_latency_ms: List[float] = field(default_factory=list)
    average_write_latency: float = 0.0
    max_write_latency: float = 0.0

    # 同步指标
    total_syncs: int = 0
    sync_latency_ms: List[float] = field(default_factory=list)
    average_sync_latency: float = 0.0

    # 检查点指标
    checkpoint_count: int = 0
    checkpoint_duration_ms: List[float] = field(default_factory=list)
    average_checkpoint_duration: float = 0.0

    # 恢复指标
    recovery_count: int = 0
    recovery_duration_ms: float = 0.0
    pages_recovered: int = 0
    transactions_rolled_back: int = 0

    # 文件指标
    current_file_count: int = 0
    total_file_size_mb: float = 0.0
    file_rotation_count: int = 0

    # 错误指标
    write_errors: int = 0
    sync_errors: int = 0
    corruption_detected: int = 0

    # 吞吐量指标
    writes_per_second: float = 0.0
    mb_per_second: float = 0.0

    def to_dict(self) -> dict:
        """转换为字典（不包括详细延迟列表）"""
        result = asdict(self)
        # 移除详细的延迟列表，只保留统计值
        result.pop('write_latency_ms', None)
        result.pop('sync_latency_ms', None)
        result.pop('checkpoint_duration_ms', None)
        return result


class WALStatistics:
    """
    WAL统计管理器

    特性：
    - 实时性能监控
    - 健康检查
    - 性能报告生成
    - 异常检测
    - 趋势分析
    """

    def __init__(self, stats_file: str = "data/wal/wal_stats.json"):
        """
        初始化统计管理器

        Args:
            stats_file: 统计数据持久化文件
        """
        self.stats_file = Path(stats_file)
        self.metrics = WALMetrics()
        self.start_time = time.time()

        # 实时监控数据
        self.last_checkpoint_time = time.time()
        self.last_write_time = time.time()
        self.last_stats_save_time = time.time()

        # 健康阈值
        self.health_thresholds = {
            'max_write_latency_ms': 100,
            'max_sync_latency_ms': 500,
            'max_checkpoint_duration_ms': 5000,
            'max_file_size_mb': 100,
            'max_corruption_rate': 0.01,
            'min_writes_per_second': 10
        }

        # 滑动窗口（用于计算移动平均）
        self.window_size = 100
        self.write_latency_window = []
        self.sync_latency_window = []

        # 日志器
        self.logger = get_logger("wal_stats")

        # 加载历史统计
        self._load_stats()

        self.logger.info("WAL Statistics initialized")

    def record_write(self, bytes_written: int, latency_ms: float):
        """
        记录写入操作

        Args:
            bytes_written: 写入字节数
            latency_ms: 延迟（毫秒）
        """
        self.metrics.total_writes += 1
        self.metrics.total_bytes_written += bytes_written

        # 更新延迟统计
        self.metrics.write_latency_ms.append(latency_ms)
        self.write_latency_window.append(latency_ms)

        # 保持窗口大小
        if len(self.write_latency_window) > self.window_size:
            self.write_latency_window.pop(0)
        if len(self.metrics.write_latency_ms) > 1000:  # 限制历史记录
            self.metrics.write_latency_ms = self.metrics.write_latency_ms[-1000:]

        # 更新统计值
        self.metrics.average_write_latency = sum(self.write_latency_window) / len(self.write_latency_window)
        self.metrics.max_write_latency = max(self.metrics.max_write_latency, latency_ms)

        # 更新吞吐量
        self.last_write_time = time.time()
        self._update_throughput()

        # 检查性能异常
        if latency_ms > self.health_thresholds['max_write_latency_ms']:
            self.logger.warning(f"High write latency: {latency_ms:.2f}ms")

    def record_sync(self, latency_ms: float):
        """记录同步操作"""
        self.metrics.total_syncs += 1

        self.metrics.sync_latency_ms.append(latency_ms)
        self.sync_latency_window.append(latency_ms)

        if len(self.sync_latency_window) > self.window_size:
            self.sync_latency_window.pop(0)
        if len(self.metrics.sync_latency_ms) > 1000:
            self.metrics.sync_latency_ms = self.metrics.sync_latency_ms[-1000:]

        self.metrics.average_sync_latency = sum(self.sync_latency_window) / len(self.sync_latency_window)

        if latency_ms > self.health_thresholds['max_sync_latency_ms']:
            self.logger.warning(f"High sync latency: {latency_ms:.2f}ms")

    def record_checkpoint(self, duration_ms: float, pages_flushed: int):
        """记录检查点操作"""
        self.metrics.checkpoint_count += 1
        self.metrics.checkpoint_duration_ms.append(duration_ms)

        if len(self.metrics.checkpoint_duration_ms) > 100:
            self.metrics.checkpoint_duration_ms = self.metrics.checkpoint_duration_ms[-100:]

        self.metrics.average_checkpoint_duration = (
                sum(self.metrics.checkpoint_duration_ms) / len(self.metrics.checkpoint_duration_ms)
        )

        self.last_checkpoint_time = time.time()

        if duration_ms > self.health_thresholds['max_checkpoint_duration_ms']:
            self.logger.warning(f"Slow checkpoint: {duration_ms:.2f}ms for {pages_flushed} pages")

    def record_recovery(self, duration_ms: float, pages: int, transactions: int):
        """记录恢复操作"""
        self.metrics.recovery_count += 1
        self.metrics.recovery_duration_ms = duration_ms
        self.metrics.pages_recovered = pages
        self.metrics.transactions_rolled_back = transactions

        self.logger.info(f"Recovery completed in {duration_ms:.2f}ms",
                         pages=pages,
                         transactions=transactions)

    def record_error(self, error_type: str):
        """记录错误"""
        if error_type == 'write':
            self.metrics.write_errors += 1
        elif error_type == 'sync':
            self.metrics.sync_errors += 1
        elif error_type == 'corruption':
            self.metrics.corruption_detected += 1

        self.logger.error(f"WAL error recorded: {error_type}")

    def update_file_stats(self, file_count: int, total_size_bytes: int, rotations: int):
        """更新文件统计"""
        self.metrics.current_file_count = file_count
        self.metrics.total_file_size_mb = total_size_bytes / (1024 * 1024)
        self.metrics.file_rotation_count = rotations

        if self.metrics.total_file_size_mb > self.health_thresholds['max_file_size_mb']:
            self.logger.warning(f"WAL files size exceeds threshold: {self.metrics.total_file_size_mb:.2f}MB")

    def _update_throughput(self):
        """更新吞吐量指标"""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            self.metrics.writes_per_second = self.metrics.total_writes / elapsed
            self.metrics.mb_per_second = (self.metrics.total_bytes_written / (1024 * 1024)) / elapsed

    def get_health_status(self) -> Dict[str, any]:
        """
        获取健康状态

        Returns:
            dict: 健康状态报告
        """
        issues = []
        status = 'healthy'

        # 检查写入延迟
        if self.metrics.average_write_latency > self.health_thresholds['max_write_latency_ms']:
            issues.append(f"High average write latency: {self.metrics.average_write_latency:.2f}ms")
            status = 'degraded'

        # 检查同步延迟
        if self.metrics.average_sync_latency > self.health_thresholds['max_sync_latency_ms']:
            issues.append(f"High average sync latency: {self.metrics.average_sync_latency:.2f}ms")
            status = 'degraded'

        # 检查错误率
        if self.metrics.total_writes > 0:
            error_rate = (self.metrics.write_errors + self.metrics.sync_errors) / self.metrics.total_writes
            if error_rate > 0.01:
                issues.append(f"High error rate: {error_rate:.2%}")
                status = 'unhealthy'

        # 检查损坏率
        if self.metrics.corruption_detected > 0:
            issues.append(f"Corruption detected: {self.metrics.corruption_detected} cases")
            status = 'unhealthy'

        # 检查吞吐量
        if self.metrics.writes_per_second < self.health_thresholds['min_writes_per_second']:
            issues.append(f"Low throughput: {self.metrics.writes_per_second:.2f} writes/sec")
            if status == 'healthy':
                status = 'degraded'

        # 检查检查点间隔
        time_since_checkpoint = time.time() - self.last_checkpoint_time
        if time_since_checkpoint > 600:  # 10分钟
            issues.append(f"Long time since last checkpoint: {time_since_checkpoint:.0f} seconds")
            if status == 'healthy':
                status = 'degraded'

        return {
            'status': status,
            'issues': issues,
            'uptime_seconds': time.time() - self.start_time,
            'last_checkpoint_ago': time_since_checkpoint,
            'metrics_summary': self.get_summary()
        }

    def get_summary(self) -> dict:
        """获取统计摘要"""
        return {
            'total_writes': self.metrics.total_writes,
            'total_mb_written': self.metrics.total_bytes_written / (1024 * 1024),
            'average_write_latency_ms': round(self.metrics.average_write_latency, 2),
            'max_write_latency_ms': round(self.metrics.max_write_latency, 2),
            'writes_per_second': round(self.metrics.writes_per_second, 2),
            'mb_per_second': round(self.metrics.mb_per_second, 2),
            'checkpoint_count': self.metrics.checkpoint_count,
            'error_count': self.metrics.write_errors + self.metrics.sync_errors,
            'corruption_count': self.metrics.corruption_detected
        }

    def get_detailed_report(self) -> dict:
        """生成详细性能报告"""
        return {
            'summary': self.get_summary(),
            'health': self.get_health_status(),
            'metrics': self.metrics.to_dict(),
            'performance_analysis': self._analyze_performance(),
            'recommendations': self._generate_recommendations()
        }

    def _analyze_performance(self) -> dict:
        """分析性能趋势"""
        analysis = {}

        # 分析写入延迟趋势
        if len(self.write_latency_window) >= 10:
            recent = self.write_latency_window[-10:]
            older = self.write_latency_window[-20:-10] if len(
                self.write_latency_window) >= 20 else self.write_latency_window[:10]

            recent_avg = sum(recent) / len(recent)
            older_avg = sum(older) / len(older)

            if recent_avg > older_avg * 1.5:
                analysis['write_latency_trend'] = 'degrading'
            elif recent_avg < older_avg * 0.7:
                analysis['write_latency_trend'] = 'improving'
            else:
                analysis['write_latency_trend'] = 'stable'

        # 分析吞吐量
        analysis['throughput_efficiency'] = 'good' if self.metrics.mb_per_second > 10 else 'poor'

        return analysis

    def _generate_recommendations(self) -> List[str]:
        """生成优化建议"""
        recommendations = []

        if self.metrics.average_write_latency > 50:
            recommendations.append("Consider increasing WAL buffer size to reduce write frequency")

        if self.metrics.average_sync_latency > 200:
            recommendations.append("Consider using less aggressive sync mode (e.g., FLUSH instead of FSYNC)")

        if self.metrics.file_rotation_count > 100:
            recommendations.append("Consider increasing WAL file size limit to reduce rotations")

        if self.metrics.corruption_detected > 0:
            recommendations.append("Data corruption detected - check disk health and consider full recovery")

        return recommendations

    def save_stats(self):
        """保存统计数据"""
        try:
            self.stats_file.parent.mkdir(parents=True, exist_ok=True)

            stats_data = {
                'timestamp': time.time(),
                'uptime': time.time() - self.start_time,
                'metrics': self.metrics.to_dict(),
                'health': self.get_health_status()
            }

            with open(self.stats_file, 'w') as f:
                json.dump(stats_data, f, indent=2)

            self.last_stats_save_time = time.time()

        except Exception as e:
            self.logger.error(f"Failed to save statistics: {e}")

    def _load_stats(self):
        """加载历史统计数据"""
        if not self.stats_file.exists():
            return

        try:
            with open(self.stats_file, 'r') as f:
                data = json.load(f)
                # 可以选择性地恢复某些累计值
                if 'metrics' in data:
                    metrics = data['metrics']
                    # 恢复累计值但重置实时指标
                    self.metrics.total_writes = metrics.get('total_writes', 0)
                    self.metrics.total_bytes_written = metrics.get('total_bytes_written', 0)

        except Exception as e:
            self.logger.error(f"Failed to load statistics: {e}")

    def reset(self):
        """重置统计数据"""
        self.metrics = WALMetrics()
        self.start_time = time.time()
        self.write_latency_window.clear()
        self.sync_latency_window.clear()
        self.logger.info("Statistics reset")