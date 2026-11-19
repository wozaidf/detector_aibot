"""将 L2/L2.5 事件压缩为 Laya 的事件级决策状态。"""

from __future__ import annotations

from collections import defaultdict
from math import isfinite
from statistics import mean
from typing import Any


RULE_SOURCES = {"DROOLS_WINDOW", "FLINK_CEP"}
IFOREST_SOURCES = {"PYOD_IFOREST", "L25_IFOREST", "IFOREST"}
MAX_WINDOW_DETAILS = 12


def _dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def _compact_evidence(value: Any, depth: int = 0) -> Any:
    """递归裁剪 L2 证据中的重复窗口明细；窗口快照由下方摘要单独提取。"""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if key in {"matched_windows", "window_results", "feature_values"}:
                continue
            if key in {"total_errors", "total_failed_spans"} and depth > 0:
                continue
            compact = _compact_evidence(item, depth + 1)
            if compact not in ({}, [], None):
                result[key] = compact
        return result
    if isinstance(value, list):
        return [_compact_evidence(item, depth + 1) for item in value[:MAX_WINDOW_DETAILS]]
    return value


def _feature_summary(snapshots: list[dict], scores: dict[str, dict]) -> dict:
    """按事件汇总窗口特征范围，不把跨窗口统计解释为唯一原始记录数。"""
    values: dict[str, list[float]] = defaultdict(list)
    for snapshot in snapshots:
        features = _dict(snapshot.get("features"))
        for group in ("metrics", "logs", "traces", "alarms"):
            for name, value in _dict(features.get(group)).items():
                if _finite_number(value):
                    values[f"{group}.{name}"].append(float(value))

    summary = {}
    for path, numbers in values.items():
        summary[path] = {
            "min": min(numbers),
            "max": max(numbers),
            "window_mean": round(mean(numbers), 6),
            "observed_windows": len(numbers),
        }

    scored = [
        (window_id, result) for window_id, result in scores.items()
        if _finite_number(result.get("anomaly_score"))
    ]
    score_values = [float(result["anomaly_score"]) for _, result in scored]
    top = sorted(scored, key=lambda pair: pair[1]["anomaly_score"], reverse=True)[:8]
    score_summary = {
        "scored_window_count": len(scored),
        "anomalous_window_count": sum(bool(result.get("anomaly")) for _, result in scored),
        "score_min": min(score_values) if score_values else None,
        "score_max": max(score_values) if score_values else None,
        "score_mean": round(mean(score_values), 6) if score_values else None,
        "thresholds": sorted({result.get("threshold") for _, result in scored if _finite_number(result.get("threshold"))}),
        "top_scored_windows": [
            {"window_id": window_id, "anomaly_score": result.get("anomaly_score"),
             "threshold": result.get("threshold"), "anomaly": result.get("anomaly")}
            for window_id, result in top
        ],
    }
    return {"features": summary, "iforest": score_summary}


def json_build_state(event: dict) -> dict:
    """把真实 L2/L2.5 输入压缩为 Laya 可读的事件级状态。

    完整快照和原始事件仍由 L4 输出保留；这里只向模型提供检测摘要、
    跨窗口特征范围、有限窗口证据及逐窗 IForest 分数。
    """
    if not isinstance(event, dict):
        raise TypeError("Kafka 消息必须是字典")

    l2 = _dict(event.get("l2_result"))
    l25 = _dict(event.get("l25_result"))
    original = _dict(event.get("original_data"))
    snapshots = [
        item for item in _list(original.get("window_snapshots"))
        if isinstance(item, dict)
        and item.get("entity_id") == event.get("entity_id")
    ]
    matched_ids = list(dict.fromkeys(
        x for x in _list(l2.get("matched_window_ids")) if isinstance(x, str)
    ))
    if not matched_ids:
        matched_ids = list(dict.fromkeys(
            _dict(x).get("window_id") for x in _list(l25.get("window_results"))
            if _dict(x).get("window_id")
        ))
    selected = set(matched_ids)
    snapshots = [x for x in snapshots if not selected or x.get("window_id") in selected]

    raw_window_results = [
        x for x in _list(l25.get("window_results")) if isinstance(x, dict)
    ]
    scores = {
        x.get("window_id"): x for x in raw_window_results if x.get("window_id")
    }
    sources = list(dict.fromkeys(
        x for x in _list(l2.get("detection_sources")) if isinstance(x, str)
    ))
    model = l25.get("model")
    if l25 and model:
        sources.append(model)
    sources = list(dict.fromkeys(sources))

    # 每窗只保留引用、时间、规则标记、质量提示与 IForest 评分。
    window_details = []
    snapshots_by_id = {x.get("window_id"): x for x in snapshots if x.get("window_id")}
    for window_id in matched_ids[:MAX_WINDOW_DETAILS]:
        snapshot = snapshots_by_id.get(window_id, {})
        window = _dict(snapshot.get("window"))
        score = scores.get(window_id, {})
        detail = {
            "window_id": window_id,
            "start": window.get("start"),
            "end": window.get("end"),
        }
        quality = _dict(snapshot.get("quality"))
        missing = _list(quality.get("missing_features"))
        if missing:
            detail["missing_features"] = missing[:8]
        if detail["start"] is not None or detail["end"] is not None or score or missing:
            window_details.append(detail)

    rule_evidence = {}
    common_evidence = _compact_evidence(l2.get("evidence"))
    for source in sorted(RULE_SOURCES):
        raw = _dict(_dict(l2.get("source_evidence")).get(source))
        if source in sources or raw:
            compact = _compact_evidence(raw or l2.get("evidence"))
            rule_evidence[source] = {"same_as": "l2_evidence"} if compact == common_evidence else compact

    iforest_sources = {
        source: _compact_evidence(_dict(_dict(l2.get("source_evidence")).get(source)))
        for source in sorted(IFOREST_SOURCES & set(_dict(l2.get("source_evidence"))))
        if _dict(_dict(l2.get("source_evidence")).get(source))
    }
    iforest_summary = {
        "model": model or next((x for x in sources if x in IFOREST_SOURCES), None),
        "scored_window_count": len(raw_window_results),
        "unscored_window_ids": _list(l25.get("unscored_window_ids"))[:MAX_WINDOW_DETAILS],
        "anomalous_window_count": sum(bool(x.get("anomaly")) for x in raw_window_results),
        "score_summary": _feature_summary([], scores)["iforest"],
        "source_evidence": iforest_sources,
    } if l25 else {}
    if not l25 and iforest_sources:
        iforest_summary = {"source_evidence": iforest_sources}

    decision = _dict(l2.get("decision"))
    if raw_window_results:
        decision = _dict(raw_window_results[0].get("decision")) or decision

    return {
        "event_info": {
            "event_type": event.get("event_type"),
            "stage": event.get("stage"),
            "event_id": event.get("event_id"),
            "entity_id": event.get("entity_id"),
            "entity_type": event.get("entity_type"),
            "anomaly_type": l2.get("anomaly_type"),
            "severity": l2.get("severity"),
            "start_time": l2.get("start_time"),
            "end_time": l2.get("end_time"),
            "window_count": len(matched_ids) or l2.get("window_count", 0),
            "matched_window_ids": matched_ids[:MAX_WINDOW_DETAILS],
            "additional_window_count": max(0, len(matched_ids) - MAX_WINDOW_DETAILS),
            "source_refs": [
                {key: ref.get(key) for key in ("topic", "window_id") if ref.get(key) is not None}
                for ref in _list(original.get("source_refs"))[:3]
                if isinstance(ref, dict)
            ],
            "quality_status": l2.get("quality_status"),
        },
        "detection_results": {
            "detection_sources": sources,
            "matched_rules": _list(l2.get("matched_rules")),
            "merge_status": l2.get("merge_status"),
            "decision_reason": l2.get("decision_reason"),
            "rule_evidence": rule_evidence,
            "l2_evidence": _compact_evidence(l2.get("evidence")),
            "iforest": iforest_summary,
        },
        "window_summary": _feature_summary(snapshots, scores),
        "window_details": window_details,
        "upstream_decision": {
            "anomaly": decision.get("anomaly", l2.get("anomaly")),
            "action": decision.get("action"),
            "reason": decision.get("reason") or l2.get("decision_reason"),
            "payload_quality": _compact_evidence(l2.get("payload_quality")),
        },
    }
