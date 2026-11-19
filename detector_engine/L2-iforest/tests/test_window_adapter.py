import json
import math
import unittest

from inference.feature_schema import FEATURE_COLUMNS
from inference.window_adapter import (
    build_prediction_event,
    build_window_result,
    expand_l2_event,
    extract_window_features,
)


def l2_event():
    snapshot = {
        "schema_version": "1.0",
        "record_type": "aligned_window",
        "window_id": "WIN-1",
        "entity_id": "server-1",
        "entity_type": "virtual_machine",
        "window": {"start": "2026-10-09T09:00:00Z", "end": "2026-10-09T09:05:00Z", "size_sec": 300, "slide_sec": 60},
        "features": {
            "metrics": {
                "cpu_mean_pct": 95, "cpu_max_pct": 98, "memory_mean_pct": 60, "memory_max_pct": 65,
                "latency_mean_ms": 40, "latency_max_ms": 50, "packet_loss_mean_ratio": 0.01,
                "network_traffic_mean_mbps": 20, "bandwidth_utilization_mean_pct": 30,
            },
            "logs": {"total_per_minute": 2, "error_rate": 0.1, "warn_rate": 0.2, "info_rate": 0.7, "error_trace_match_ratio": 0.5},
            "traces": {"span_per_minute": 4, "failed_span_per_minute": 1, "span_error_rate": 0.25, "avg_duration_ms": 12, "max_duration_ms": 20},
            "alarms": {"alarm_per_minute": 0, "critical_rate": 0, "major_rate": 0, "minor_rate": 0},
        },
        "quality": {"status": "complete", "sample_counts": {}, "missing_features": []},
        "context": {"maintenance_active": False},
        "relation_statistics": {},
        "source_ref": {
            "topic": "l1-aligned-windows", "window_id": "WIN-1", "content_sha256": "a" * 64,
        },
    }
    return {
        "event_id": "L2-event-1", "event_type": "ANOMALY_EVENT", "stage": "L2",
        "entity_id": "server-1", "entity_type": "virtual_machine",
        "created_at": "2026-10-09T09:00:00Z", "updated_at": "2026-10-09T09:00:00Z",
        "original_data": {"feature_schema_version": "l1-window-features-v1", "source_refs": [snapshot["source_ref"]], "window_snapshots": [snapshot]},
        "l2_result": {
            "anomaly": True, "anomaly_type": "CPU_HIGH", "severity": "critical",
            "matched_window_ids": ["WIN-1"], "detection_sources": ["DROOLS_WINDOW"],
            "decision": {"action": "ALERT", "anomaly": True},
            "payload_quality": {"snapshot_status": "COMPLETE", "missing_window_ids": []},
        },
        "l25_result": None, "l4_result": None,
    }


class WindowAdapterTest(unittest.TestCase):
    def test_expands_each_matched_l2_window_separately(self):
        record = l2_event()
        second = json.loads(json.dumps(record["original_data"]["window_snapshots"][0]))
        second["window_id"] = "WIN-2"
        second["source_ref"]["window_id"] = "WIN-2"
        second["source_ref"]["content_sha256"] = "b" * 64
        second["window"]["start"] = "2026-10-09T09:01:00Z"
        second["window"]["end"] = "2026-10-09T09:06:00Z"
        record["l2_result"]["matched_window_ids"].append("WIN-2")
        record["original_data"]["window_snapshots"].append(second)

        _, expanded, missing = expand_l2_event(record)
        self.assertEqual(["WIN-1", "WIN-2"], [item["window"]["window_id"] for item in expanded])
        self.assertEqual([], missing)

    def test_missing_per_minute_feature_is_not_recomputed(self):
        snapshot = l2_event()["original_data"]["window_snapshots"][0]
        del snapshot["features"]["logs"]["total_per_minute"]
        values, warnings = extract_window_features(snapshot)
        self.assertTrue(math.isnan(values["logs_total_per_minute"]))
        self.assertTrue(any("logs_total_per_minute" in warning for warning in warnings))

    def test_preserves_l1_reference_and_parent_l2_context(self):
        record = l2_event()
        _, items, _ = expand_l2_event(record)
        item = items[0]
        features, warnings = extract_window_features(item["window"])
        result = build_window_result(item["window"], item["source_ref"], features, warnings, 0.42, 0.1, True)
        event = build_prediction_event(record, [result], [], "PYOD_IFOREST")
        self.assertEqual("L2-event-1", event["event_id"])
        self.assertEqual("l1-aligned-windows", event["l25_result"]["window_results"][0]["source_ref"]["topic"])
        self.assertEqual("WIN-1", event["l25_result"]["window_results"][0]["window_id"])
        self.assertEqual(FEATURE_COLUMNS, tuple(event["l25_result"]["window_results"][0]["evidence"]["feature_values"]))

    def test_rejects_snapshot_not_referenced_by_parent_event(self):
        record = l2_event()
        record["l2_result"]["matched_window_ids"] = ["OTHER"]
        _, expanded, _ = expand_l2_event(record)
        self.assertEqual([], expanded)

    def test_rejects_l1_window_message(self):
        _, expanded, _ = expand_l2_event({"window_id": "WIN-L1", "entity_id": "server-1"})
        self.assertEqual([], expanded)

    def test_reports_parent_referenced_window_without_snapshot(self):
        record = l2_event()
        record["l2_result"]["matched_window_ids"].append("WIN-MISSING")
        _, expanded, missing = expand_l2_event(record)
        self.assertEqual(["WIN-MISSING"], missing)
        self.assertEqual(1, len(expanded))

    def test_uses_new_l2_field_paths_and_does_not_score_all_when_matches_missing(self):
        record = l2_event()
        record["l2_result"].pop("matched_window_ids")
        _, expanded, missing = expand_l2_event(record)
        self.assertEqual([], expanded)
        self.assertEqual([], missing)

    def test_includes_l2_declared_missing_windows(self):
        record = l2_event()
        record["l2_result"]["payload_quality"]["missing_window_ids"] = ["WIN-MISSING"]
        _, expanded, missing = expand_l2_event(record)
        self.assertEqual(1, len(expanded))
        self.assertEqual(["WIN-MISSING"], missing)

    def test_l2_prediction_event_id_is_stable_and_scoped_to_parent_and_window_version(self):
        record = l2_event()
        _, items, _ = expand_l2_event(record)
        item = items[0]
        features, warnings = extract_window_features(item["window"])
        result = build_window_result(item["window"], item["source_ref"], features, warnings, 0.4, 0.1, True)
        first = build_prediction_event(record, [result], [], "PYOD_IFOREST")
        second = build_prediction_event(record, [result], [], "PYOD_IFOREST")
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual("L2-event-1", first["event_id"])


if __name__ == "__main__":
    unittest.main()
