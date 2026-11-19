"""窗口特征计算。

根据窗口内的事件计算指标统计、日志/链路/告警速率特征、融合特征
（含 key_level 关联）以及缺失特征列表。
"""

from __future__ import annotations

from statistics import mean

from .common import rounded


METRICS = {
    "cpu": ("cpu_usage_pct", "cpu_mean_pct", "cpu_max_pct"),
    "memory": ("memory_usage_pct", "memory_mean_pct", "memory_max_pct"),
    "latency": ("latency_ms", "latency_mean_ms", "latency_max_ms"),
}


def calculate(events: dict, size_sec: int) -> tuple[dict, dict, dict]:
    rows = events["metrics"]
    mf = {}
    missing = []
    for prefix, (raw, avg, mx) in METRICS.items():
        vals = [r["values"][raw] for r in rows if r["values"].get(raw) is not None]
        mf[avg] = rounded(mean(vals)) if vals else None; mf[mx] = rounded(max(vals)) if vals else None
        if not vals: missing += [f"metrics.{avg}", f"metrics.{mx}"]
    for raw, out in (("packet_loss_ratio", "packet_loss_mean_ratio"), ("network_traffic_mbps", "network_traffic_mean_mbps"), ("bandwidth_utilization_pct", "bandwidth_utilization_mean_pct")):
        vals = [r["values"][raw] for r in rows if r["values"].get(raw) is not None]
        mf[out] = rounded(mean(vals)) if vals else None
        if not vals: missing.append(f"metrics.{out}")
    logs = events["logs"]; errors = [x for x in logs if x["level"] == "ERROR"]
    minutes = size_sec / 60.0
    warn_count = sum(x["level"] == "WARN" for x in logs)
    info_count = sum(x["level"] == "INFO" for x in logs)
    lf = {"total_count": len(logs), "total_per_minute": rounded(len(logs) / minutes) if minutes else 0.0,
          "error_count": len(errors), "warn_count": warn_count, "info_count": info_count,
          "error_rate": rounded(len(errors) / len(logs)) if logs else 0.0,
          "warn_rate": rounded(warn_count / len(logs)) if logs else 0.0,
          "info_rate": rounded(info_count / len(logs)) if logs else 0.0,
          "error_trace_match_ratio": 0.0}
    spans = events["traces"]; failed = [x for x in spans if x["status"] == "ERROR"]
    tf = {"span_count": len(spans), "span_per_minute": rounded(len(spans) / minutes) if minutes else 0.0,
          "failed_span_count": len(failed), "failed_span_per_minute": rounded(len(failed) / minutes) if minutes else 0.0,
          "span_error_rate": rounded(len(failed) / len(spans)) if spans else 0.0,
          "avg_duration_ms": rounded(mean(x["duration_ms"] for x in spans)) if spans else None,
          "max_duration_ms": rounded(max(x["duration_ms"] for x in spans)) if spans else None}
    if not spans: missing += ["traces.avg_duration_ms", "traces.max_duration_ms"]
    alarms = events["alarms"]
    critical_count = sum(x["alarm_level"] == "critical" for x in alarms)
    major_count = sum(x["alarm_level"] == "major" for x in alarms)
    minor_count = sum(x["alarm_level"] == "minor" for x in alarms)
    af = {"alarm_count": len(alarms), "alarm_per_minute": rounded(len(alarms) / minutes) if minutes else 0.0,
          "critical_count": critical_count, "major_count": major_count, "minor_count": minor_count,
          "critical_rate": rounded(critical_count / len(alarms)) if alarms else 0.0,
          "major_rate": rounded(major_count / len(alarms)) if alarms else 0.0,
          "minor_rate": rounded(minor_count / len(alarms)) if alarms else 0.0}
    features = {"metrics": mf, "logs": lf, "traces": tf, "alarms": af}
    span_by_trace = {}
    for span in spans: span_by_trace.setdefault(span["trace_id"], []).append(span)
    log_by_trace = {}
    for log in logs:
        if log["trace_id"] is not None: log_by_trace.setdefault(log["trace_id"], []).append(log)
    relations = []
    for key in sorted(set(span_by_trace) & set(log_by_trace)):
        ls, ss = log_by_trace[key], span_by_trace[key]
        relations.append({"relation_key": key, "relation_type": "log_trace", "log_record_ids": [x["record_id"] for x in ls], "span_record_ids": [x["record_id"] for x in ss], "error_log_count": sum(x["level"] == "ERROR" for x in ls), "failed_span_count": sum(x["status"] == "ERROR" for x in ss)})
    matched_errors = sum(r["error_log_count"] for r in relations); matched_failed = sum(r["failed_span_count"] for r in relations)
    lf["error_trace_match_ratio"] = rounded(matched_errors / lf["error_count"]) if lf["error_count"] else 0.0
    overlap = sum(r["error_log_count"] > 0 and r["failed_span_count"] > 0 for r in relations)
    fusion = {"window_level": {"feature_vector": {"cpu_mean_pct": mf["cpu_mean_pct"], "memory_mean_pct": mf["memory_mean_pct"], "error_log_count": lf["error_count"], "error_logs_per_minute": rounded(lf["error_count"] / (size_sec / 60)), "span_count": tf["span_count"], "failed_span_count": tf["failed_span_count"], "span_failure_rate": tf["span_error_rate"], "alarm_count": af["alarm_count"], "alarms_per_minute": rounded(af["alarm_count"] / (size_sec / 60))}, "participating_types": [k for k in ("metrics", "logs", "traces", "alarms") if events[k]]}, "key_level": {"relation_statistics": {"error_failed_trace_overlap_count": overlap, "matched_error_log_count": matched_errors, "matched_failed_span_count": matched_failed, "error_log_match_ratio": rounded(matched_errors / lf["error_count"]) if lf["error_count"] else 0.0}, "relations": relations}}
    return features, fusion, missing
