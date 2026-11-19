"""Convert an upstream anomaly event to the /diagnose request body."""

from copy import deepcopy
from typing import Annotated, Any, TypedDict

from pydantic import AfterValidator, TypeAdapter, ValidationError


def _require_non_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("must be a non-empty string")
    return value


NonBlankString = Annotated[str, AfterValidator(_require_non_blank)]


class DetectionFields(TypedDict):
    anomaly_type: NonBlankString
    severity: NonBlankString
    start_time: NonBlankString
    end_time: NonBlankString


class EventFields(TypedDict):
    event_id: NonBlankString
    entity_id: NonBlankString
    entity_type: NonBlankString
    l2_result: DetectionFields


event_adapter = TypeAdapter(EventFields)


def _select(data: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: deepcopy(data[key]) for key in keys if key in data}


def to_diagnose_request(event: dict[str, Any]) -> dict[str, Any]:
    """将上游结果事件转换为 /diagnose 请求体。"""
    try:
        fields = event_adapter.validate_python(event, strict=True)
    except ValidationError as exc:
        error = exc.errors(include_input=False)[0]
        path = ".".join(map(str, error["loc"])) or "event"
        raise ValueError(f"{path}: {error['msg']}") from exc

    detection = event["l2_result"]
    original = event.get("original_data") or {}

    anomaly_data = {
        **_select(fields, "event_id", "entity_id", "entity_type"),
        "device_id": fields["entity_id"],
        **_select(
            detection, "anomaly_type", "severity", "start_time", "end_time",
            "detected_at", "detection_sources", "matched_rules", "matched_window_ids",
            "quality_status", "payload_quality",
        ),
        **_select(original, "feature_schema_version", "source_refs"),
    }

    if "evidence" in detection:
        anomaly_data["detection_evidence"] = deepcopy(detection["evidence"])
    if len(detection.get("detection_sources") or []) > 1 and "source_evidence" in detection:
        anomaly_data["source_evidence"] = deepcopy(detection["source_evidence"])

    if "window_snapshots" in original:
        anomaly_data["windows"] = [
            _select(snapshot, "window_id", "window", "features", "quality", "context", "relation_statistics")
            for snapshot in original["window_snapshots"]
        ]

    corroboration = event.get("l25_result")
    if corroboration:
        anomaly_data["corroboration"] = {
            **_select(corroboration, "model", "unscored_window_ids"),
            "window_results": [
                _select(
                    result, "window_id", "anomaly", "anomaly_type",
                    "anomaly_score", "threshold", "input_warnings",
                )
                for result in corroboration.get("window_results", [])
            ],
        }

    return {"anomaly_data": anomaly_data}
