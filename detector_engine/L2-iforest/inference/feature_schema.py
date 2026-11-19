"""定义训练与 L2 窗口快照推理共用的 23 个特征列及取值路径。

字典顺序就是模型输入列顺序；路径相对于快照的 ``features`` 对象。
"""

from __future__ import annotations


FEATURE_PATHS: dict[str, tuple[str, str]] = {
    "metrics_cpu_mean_pct": ("metrics", "cpu_mean_pct"),
    "metrics_cpu_max_pct": ("metrics", "cpu_max_pct"),
    "metrics_memory_mean_pct": ("metrics", "memory_mean_pct"),
    "metrics_memory_max_pct": ("metrics", "memory_max_pct"),
    "metrics_latency_mean_ms": ("metrics", "latency_mean_ms"),
    "metrics_latency_max_ms": ("metrics", "latency_max_ms"),
    "metrics_packet_loss_mean_ratio": ("metrics", "packet_loss_mean_ratio"),
    "metrics_network_traffic_mean_mbps": ("metrics", "network_traffic_mean_mbps"),
    "metrics_bandwidth_utilization_mean_pct": ("metrics", "bandwidth_utilization_mean_pct"),
    "logs_total_per_minute": ("logs", "total_per_minute"),
    "logs_error_rate": ("logs", "error_rate"),
    "logs_warn_rate": ("logs", "warn_rate"),
    "logs_info_rate": ("logs", "info_rate"),
    "logs_error_trace_match_ratio": ("logs", "error_trace_match_ratio"),
    "traces_span_per_minute": ("traces", "span_per_minute"),
    "traces_failed_span_per_minute": ("traces", "failed_span_per_minute"),
    "traces_span_error_rate": ("traces", "span_error_rate"),
    "traces_avg_duration_ms": ("traces", "avg_duration_ms"),
    "traces_max_duration_ms": ("traces", "max_duration_ms"),
    "alarms_alarm_per_minute": ("alarms", "alarm_per_minute"),
    "alarms_critical_rate": ("alarms", "critical_rate"),
    "alarms_major_rate": ("alarms", "major_rate"),
    "alarms_minor_rate": ("alarms", "minor_rate"),
}

FEATURE_COLUMNS = tuple(FEATURE_PATHS)

