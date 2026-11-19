"""读取统一格式的 L2 事件，并构造 L2.5 窗口评分结果。"""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Any

import numpy as np

from .feature_schema import FEATURE_PATHS

FEATURE_SCHEMA_VERSION = "l1-window-features-v1"
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def _object(value: Any) -> dict[str, Any]:
    """提供安全读取事件对象的入口。"""
    return value if isinstance(value, dict) else {}


def _source_ref(window: dict[str, Any], original_data: dict[str, Any]) -> dict[str, Any] | None:
    """获取窗口对应的 L1 来源引用。"""
    ref = window.get("source_ref")
    if isinstance(ref, dict):
        return ref
    window_id = window.get("window_id")
    for candidate in original_data.get("source_refs", []):
        if isinstance(candidate, dict) and candidate.get("window_id") == window_id:
            return candidate
    return None


def expand_l2_event(event: Any) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    """为 L2.5 逐窗口评分准备输入，并保留无法评分的窗口 ID。"""
    event = _object(event)
    original_data = _object(event.get("original_data"))
    l2_result = _object(event.get("l2_result"))
    matched_ids = l2_result.get("matched_window_ids", [])
    matched_ids = list(dict.fromkeys(
        window_id for window_id in matched_ids
        if isinstance(window_id, str) and window_id
    )) if isinstance(matched_ids, list) else []
    snapshots = original_data.get("window_snapshots", [])
    snapshots = snapshots if isinstance(snapshots, list) else []

    # 将有效窗口 ID 去重后用于匹配快照。
    selected = set(matched_ids)
    windows = []
    present = set()
    for raw_window in snapshots:
        window = _object(raw_window)
        window_id = window.get("window_id")
        if not isinstance(window_id, str) or window_id not in selected:
            continue
        present.add(window_id)
        windows.append({"window": window, "source_ref": _source_ref(window, original_data)})

    # 合并 L2 已声明缺失的 ID 和实际未找到快照的 ID。
    quality = _object(l2_result.get("payload_quality"))
    declared_missing = quality.get("missing_window_ids", [])
    declared_missing = [
        window_id for window_id in declared_missing
        if isinstance(window_id, str) and window_id
    ] if isinstance(declared_missing, list) else []
    unscored = list(dict.fromkeys(
        declared_missing + [window_id for window_id in matched_ids if window_id not in present]
    ))
    return event, windows, unscored


def extract_window_features(record: Any) -> tuple[dict[str, Any], list[str]]:
    """为 IForest 准备窗口特征及缺失值警告。"""
    record = _object(record)
    feature_tree = _object(record.get("features"))
    values: dict[str, Any] = {}
    warnings: list[str] = []

    for column, (group_name, field_name) in FEATURE_PATHS.items():
        value = _object(feature_tree.get(group_name)).get(field_name)
        try:
            number = float(value) if value is not None else math.nan
        except (TypeError, ValueError):
            number = math.nan
        if not math.isfinite(number):
            warnings.append(f"{column} missing or invalid; training median imputation applied")
            number = np.nan
        values[column] = number
    return values, warnings


def build_window_result(
    window: dict[str, Any], source_ref: dict[str, Any] | None,
    features: dict[str, Any], warnings: list[str], score: float, threshold: float,
    anomaly: bool,
) -> dict[str, Any]:
    """生成单个窗口的 IForest 评分结果。"""
    evidence_features = {
        key: float(value) if value is not None and math.isfinite(float(value)) else None
        for key, value in features.items()
    }
    return {
        "window_id": window.get("window_id"),
        "source_ref": source_ref,
        "anomaly": anomaly,
        "anomaly_type": "IFOREST_WINDOW_ANOMALY" if anomaly else "IFOREST_WINDOW_EVALUATION",
        "anomaly_score": score,
        "threshold": threshold,
        "evidence": {
            "feature_source": f"L2 original_data.window_snapshots.features ({FEATURE_SCHEMA_VERSION})",
            "feature_values": evidence_features,
        },
        "decision": {
            "anomaly": anomaly,
            "action": "CANDIDATE" if anomaly else "NORMAL",
            "reason": "score_met_validation_threshold" if anomaly else "score_below_validation_threshold",
        },
        "input_warnings": warnings,
    }


def build_prediction_event(
    source_event: dict[str, Any], window_results: list[dict[str, Any]],
    unscored_window_ids: list[str], model_name: str,
) -> dict[str, Any]:
    """生成包含 L2.5 结果的阶段事件。"""
    updated = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        **source_event,
        "stage": "L25",
        "updated_at": updated,
        "l25_result": {
            "model": model_name,
            "input_stage": "L2",
            "window_results": window_results,
            "unscored_window_ids": unscored_window_ids,
        },
        "l4_result": None,
    }
