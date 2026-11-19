"""原始事件生成。

按时间刻度为每个实体生成指标、日志、链路和告警事件，并根据当前生效的
异常场景调整数值与事件属性，形成可识别的异常模式。
"""

from __future__ import annotations

import math
import random
from collections import defaultdict
from datetime import datetime, timedelta

from .common import iso, rounded
from .scenario_generator import Scenario


SERVICES = ("payment-service", "order-service", "gateway-service")


def _active(scenarios: list[Scenario], entity: str, when: datetime) -> dict[str, Scenario]:
    return {s.anomaly_type: s for s in scenarios if s.entity_id == entity and s.contains(when)}


def generate_events(entity_ids: list[str], start: datetime, duration: timedelta, interval: int, scenarios: list[Scenario], seed: int) -> dict[str, dict[str, list[dict]]]:
    rng = random.Random(seed)
    result = {e: {k: [] for k in ("metrics", "logs", "traces", "alarms")} for e in entity_ids}
    alarm_seen: set[tuple[str, str, str]] = set()
    ticks = int(duration.total_seconds() // interval)
    for eidx, entity in enumerate(entity_ids):
        seq = defaultdict(int)
        phase = rng.random() * math.tau
        for tick in range(ticks):
            when = start + timedelta(seconds=tick * interval)
            active = _active(scenarios, entity, when)
            def strength(name: str) -> float:
                return active[name].intensity if name in active else 0.0
            qdrop = "data_quality" in active and tick % 3 != 0
            base_cpu = 39 + 9 * math.sin(tick / 55 + phase) + rng.gauss(0, 2.2)
            memory = 54 + 5 * math.sin(tick / 90 + phase / 2) + rng.gauss(0, 1.2)
            latency = 48 + 9 * math.sin(tick / 33) + rng.gauss(0, 4)
            loss = max(0, 0.002 + rng.gauss(0, 0.0008))
            cpu = base_cpu
            if "cpu_sustained" in active:
                cpu = base_cpu + (89 + rng.gauss(0, 2) - base_cpu) * strength("cpu_sustained")
            if "cpu_spike" in active:
                p = (when - active["cpu_spike"].start).total_seconds() / 480
                cpu += (45 + 50 * (p if p < .5 else 1 - p) * 2 + rng.gauss(0, 1) - base_cpu) * strength("cpu_spike")
            if "memory_growth" in active:
                p = (when - active["memory_growth"].start).total_seconds() / 480
                memory += (58 + 34 * min(1, p * 1.5) + rng.gauss(0, 1) - memory) * strength("memory_growth")
            if "network_degradation" in active:
                latency += (180 + rng.gauss(0, 15)) * strength("network_degradation"); loss += (.055 + rng.gauss(0, .004)) * strength("network_degradation")
            if "multi_metric" in active:
                s = strength("multi_metric"); cpu += 35 * s; memory += 25 * s; latency += 120 * s; loss += .025 * s
            if "maintenance" in active:
                cpu += 25 * strength("maintenance")
            cpu = min(100, max(0, cpu)); memory = min(100, max(0, memory)); latency = max(0.1, latency); loss = min(1, max(0, loss))
            traffic = min(1000, max(0, 180 + cpu * 4.2 + rng.gauss(0, 18)))
            bandwidth = min(100, max(0, traffic / 10))
            if not qdrop:
                seq["metric"] += 1
                result[entity]["metrics"].append({"record_id": f"{entity}-metric-{seq['metric']:06d}", "event_time": iso(when), "source": "cloud_monitor", "values": {"cpu_usage_pct": rounded(cpu), "memory_usage_pct": rounded(memory), "latency_ms": rounded(latency), "packet_loss_ratio": rounded(loss), "network_traffic_mbps": rounded(traffic), "bandwidth_utilization_pct": rounded(bandwidth)}})

            anomalous = bool(set(active) & {"log_anomaly", "trace_anomaly", "cross_type", "multi_metric", "network_degradation"})
            if tick % 2 == 0:
                trace_num = f"{eidx+1:02d}{tick:07d}"
                trace_id = f"trace-{trace_num}"
                service = SERVICES[(tick + eidx) % len(SERVICES)]
                failed = anomalous and rng.random() < .62
                duration_ms = max(5, rng.gauss(650 if anomalous else 85, 100 if anomalous else 20))
                seq["span"] += 1
                root_id = f"{entity}-span-{seq['span']:06d}"
                result[entity]["traces"].append({"record_id": root_id, "event_time": iso(when + timedelta(seconds=3)), "source": "trace_monitor", "trace_id": trace_id, "span_id": root_id, "parent_span_id": None, "service_name": service, "operation": "POST /api/request", "duration_ms": rounded(duration_ms), "status": "ERROR" if failed else "OK"})
                seq["span"] += 1
                child_id = f"{entity}-span-{seq['span']:06d}"
                result[entity]["traces"].append({"record_id": child_id, "event_time": iso(when + timedelta(seconds=4)), "source": "trace_monitor", "trace_id": trace_id, "span_id": child_id, "parent_span_id": root_id, "service_name": "database-service", "operation": "SELECT orders", "duration_ms": rounded(duration_ms * .7), "status": "ERROR" if failed else "OK"})
                level = "ERROR" if failed else ("WARN" if anomalous and rng.random() < .35 else "INFO")
                seq["log"] += 1
                messages = {"ERROR": "Database connection timeout", "WARN": "Upstream response is slow", "INFO": "Request completed"}
                result[entity]["logs"].append({"record_id": f"{entity}-log-{seq['log']:06d}", "event_time": iso(when + timedelta(seconds=5)), "source": "huawei_lts", "level": level, "message": messages[level], "service_id": service, "trace_id": trace_id})
            if tick % 5 == 0:
                seq["log"] += 1
                result[entity]["logs"].append({"record_id": f"{entity}-log-{seq['log']:06d}", "event_time": iso(when + timedelta(seconds=8)), "source": "huawei_lts", "level": "WARN" if anomalous else "INFO", "message": "Network connection unstable" if anomalous else "Health check passed", "service_id": SERVICES[eidx % 3], "trace_id": None})

            candidates = []
            if cpu > 82: candidates.append(("CPU_HIGH", "critical", "CPU usage exceeded threshold"))
            if memory > 85: candidates.append(("MEMORY_HIGH", "major", "Memory usage exceeded threshold"))
            if latency > 160: candidates.append(("NETWORK_LATENCY_HIGH", "major", "Network latency exceeded threshold"))
            if loss > .03: candidates.append(("PACKET_LOSS_HIGH", "major", "Packet loss exceeded threshold"))
            if anomalous and tick % 4 == 0: candidates.append(("SERVICE_ERROR", "critical", "Service error rate elevated"))
            for code, level, message in candidates:
                bucket = tick // 10
                key = (entity, code, str(bucket))
                if key not in alarm_seen and rng.random() < .72:
                    alarm_seen.add(key); seq["alarm"] += 1
                    result[entity]["alarms"].append({"record_id": f"{entity}-alarm-{seq['alarm']:06d}", "event_time": iso(when + timedelta(seconds=10)), "source": "alarm_platform", "alarm_id": f"ALM-{eidx+1:02d}-{seq['alarm']:05d}", "alarm_code": code, "alarm_level": level, "alarm_status": "firing", "message": message})
    return result
