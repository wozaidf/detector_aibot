"""对齐窗口构建。

将全局时间轴上的原始事件按左闭右开区间 [start, end) 切分到重叠窗口，
计算特征与融合特征，并补充上下文、数据质量和样本计数。
"""

from __future__ import annotations

from bisect import bisect_left
from datetime import datetime, timedelta

from .common import iso, parse_time
from .feature_calculator import calculate
from .scenario_generator import Scenario


def build_windows(entity_ids: list[str], start: datetime, duration: timedelta, size_sec: int, slide_sec: int, event_store: dict, scenarios: list[Scenario], max_windows: int | None = None, metric_interval_sec: int = 30) -> list[dict]:
    starts = int(max(0, (duration.total_seconds() - size_sec) // slide_sec + 1))
    candidates = [(start + timedelta(seconds=i * slide_sec), entity) for i in range(starts) for entity in entity_ids]
    candidates.sort(key=lambda x: (x[0] + timedelta(seconds=size_sec), x[1]))
    if max_windows: candidates = candidates[:max_windows]
    indexed = {}
    for entity in entity_ids:
        indexed[entity] = {}
        for kind, records in event_store[entity].items():
            indexed[entity][kind] = (records, [parse_time(r["event_time"]) for r in records])
    windows = []
    for number, (ws, entity) in enumerate(candidates, 1):
        we = ws + timedelta(seconds=size_sec); events = {}
        for kind in ("metrics", "logs", "traces", "alarms"):
            records, times = indexed[entity][kind]
            events[kind] = records[bisect_left(times, ws):bisect_left(times, we)]
        features, fusion, missing = calculate(events, size_sec)
        expected_metrics = max(1, size_sec // metric_interval_sec)
        metric_status = "complete" if len(events["metrics"]) >= expected_metrics else "incomplete"
        statuses = {"metrics_status": metric_status, "logs_status": "complete" if events["logs"] else "incomplete", "traces_status": "complete" if events["traces"] else "incomplete", "alarms_status": "complete"}
        status = "complete" if all(v == "complete" for v in statuses.values()) else "partial"
        active = [s for s in scenarios if s.entity_id == entity and s.start < we and s.end > ws]
        maintenance = any(s.anomaly_type == "maintenance" for s in active)
        sample_counts = {"cpu_usage": len(events["metrics"]), "memory_usage": len(events["metrics"]), "latency": len(events["metrics"]), "packet_loss": len(events["metrics"]), "network_traffic": len(events["metrics"]), "bandwidth_utilization": len(events["metrics"]), "logs": len(events["logs"]), "spans": len(events["traces"]), "alarms": len(events["alarms"])}
        windows.append({"schema_version": "1.0", "window_id": f"WIN-{number:08d}", "record_type": "aligned_window", "entity_id": entity, "entity_type": "host", "window": {"start": iso(ws), "end": iso(we), "size_sec": size_sec, "slide_sec": slide_sec}, "events": events, "features": features, "context": {"active_ticket_ids": [f"WO-{entity[-3:]}"] if active else [], "active_change_ids": [f"CHG-{entity[-3:]}"] if maintenance else [], "maintenance_active": maintenance, "related_entity_ids": [entity, f"database-{entity[-3:]}"]}, "quality": {"status": status, **statuses, "sample_counts": sample_counts, "missing_features": missing}, "fusion_features": fusion})
    return windows
