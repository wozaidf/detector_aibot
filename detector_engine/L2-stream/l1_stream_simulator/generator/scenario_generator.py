"""异常场景定义与生成。

定义 Scenario 数据类和各类异常模式（CPU 持续高负载、内存增长、网络劣化等），
按实体与时间范围生成可复现的注入场景及其真值。
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from .common import iso


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    entity_id: str
    anomaly_type: str
    start: datetime
    end: datetime
    affected_metrics: tuple[str, ...]
    expected_pattern: str
    intensity: float = 1.0

    def contains(self, when: datetime) -> bool:
        return self.start <= when < self.end

    def as_truth(self) -> dict:
        return {
            "scenario_id": self.scenario_id,
            "entity_id": self.entity_id,
            "anomaly_type": self.anomaly_type,
            "start_time": iso(self.start),
            "end_time": iso(self.end),
            "affected_metrics": list(self.affected_metrics),
            "expected_pattern": self.expected_pattern,
        }


DEFS = {
    "cpu_sustained": (("cpu_usage_pct",), "at least 3 consecutive windows with cpu_mean_pct > 80"),
    "cpu_spike": (("cpu_usage_pct",), "rapid CPU rise followed by recovery"),
    "memory_growth": (("memory_usage_pct",), "gradual memory increase and high plateau"),
    "network_degradation": (("latency_ms", "packet_loss_ratio"), "correlated latency and packet-loss increase"),
    "multi_metric": (("cpu_usage_pct", "memory_usage_pct", "latency_ms"), "correlated multi-metric degradation"),
    "log_anomaly": ((), "elevated WARN and ERROR log rate"),
    "trace_anomaly": ((), "increased span duration and failure ratio"),
    "cross_type": ((), "ERROR logs correlate with failed spans by trace_id"),
    "maintenance": (("cpu_usage_pct",), "anomaly while maintenance_active is true"),
    "data_quality": ((), "missing metric samples and incomplete quality state"),
}


def build_scenarios(entity_ids: list[str], start: datetime, duration: timedelta, enabled: list[str], frequency: float, intensity: float, seed: int, scenario_duration_sec: int = 600) -> list[Scenario]:
    rng = random.Random(seed + 997)
    total = duration.total_seconds()
    if total < 1200:
        return []
    result: list[Scenario] = []
    for eidx, entity in enumerate(entity_ids):
        names = [n for i, n in enumerate(enabled) if (i + eidx) % len(entity_ids) == 0 or n == "cpu_sustained"]
        for idx, name in enumerate(names):
            if name not in DEFS or (name != "cpu_sustained" and rng.random() > frequency):
                continue
            span = max(480, scenario_duration_sec)
            if name == "cpu_sustained": span = max(720, span)
            room = max(1, int(total - span - 600))
            offset = 300 + ((eidx * 811 + idx * 977 + rng.randrange(room)) % room)
            s = start + timedelta(seconds=offset)
            metrics, pattern = DEFS[name]
            result.append(Scenario(f"SCN-{len(result)+1:04d}", entity, name, s, s + timedelta(seconds=span), metrics, pattern, intensity))
    return sorted(result, key=lambda x: (x.start, x.entity_id, x.scenario_id))
