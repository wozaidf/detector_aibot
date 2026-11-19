"""只读校验 L2 JSON Schema、示例及与当前 IForest 特征定义的一致性。"""

import ast
from copy import deepcopy
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

MODULE = Path(__file__).resolve().parents[1]


def load(path):
    def reject(value):
        raise ValueError(f"non-standard JSON constant: {value}")
    return json.loads(path.read_text(encoding="utf-8"), parse_constant=reject)


def check_relations(event):
    result = event["l2_result"]
    source = event["original_data"]
    expected = set(result["matched_window_ids"])
    assert result["window_count"] == len(expected)
    snapshots = source["window_snapshots"]
    assert all(s["entity_id"] == event["entity_id"] for s in snapshots)
    assert all(s["window_id"] in expected for s in snapshots)
    assert not any("events" in s for s in snapshots)
    present = {s["window_id"] for s in snapshots}
    assert set(result["payload_quality"]["missing_window_ids"]) == expected - present
    if result["payload_quality"]["snapshot_status"] == "COMPLETE":
        assert present == expected


def main():
    schema = load(MODULE / "docs/l2-anomaly-event.schema.json")
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    examples = {}
    for name in ("single", "cep"):
        event = load(MODULE / f"docs/examples/l2-{name}-event.json")
        validator.validate(event)
        check_relations(event)
        examples[name] = event

    # null/缺失原窗口/冲突版本可被显式表示，不能冒充 COMPLETE。
    partial = deepcopy(examples["single"])
    partial["original_data"]["window_snapshots"][0]["features"]["metrics"]["memory_max_pct"] = None
    assert not validator.is_valid(partial)
    partial["l2_result"]["payload_quality"]["snapshot_status"] = "INCOMPLETE"
    partial["l2_result"]["payload_quality"]["feature_warnings"] = [{
        "window_id": partial["l2_result"]["matched_window_ids"][0],
        "path": "/features/metrics/memory_max_pct", "reason": "null value",
    }]
    validator.validate(partial)
    check_relations(partial)

    missing = deepcopy(examples["single"])
    missing["original_data"]["window_snapshots"] = []
    missing["entity_type"] = None
    missing["l2_result"]["payload_quality"]["snapshot_status"] = "INCOMPLETE"
    missing["l2_result"]["payload_quality"]["missing_window_ids"] = missing["l2_result"]["matched_window_ids"][:]
    validator.validate(missing)
    check_relations(missing)

    conflict = deepcopy(examples["single"])
    version = deepcopy(conflict["original_data"]["window_snapshots"][0])
    original_hash = version["source_ref"]["content_sha256"]
    version["source_ref"]["content_sha256"] = "f" * 64
    conflict["original_data"]["window_snapshots"].append(version)
    conflict["l2_result"]["payload_quality"]["snapshot_status"] = "CONFLICT"
    conflict["l2_result"]["payload_quality"]["snapshot_conflicts"] = [{
        "entity_id": conflict["entity_id"], "window_id": version["window_id"],
        "content_sha256s": sorted([original_hash, "f" * 64]),
    }]
    validator.validate(conflict)
    check_relations(conflict)

    contract = load(MODULE / "src/main/resources/contracts/l1-window-features-v1.json")
    workspace = MODULE.parent.parent
    manifest_path = workspace / "L2-iforest/models/feature_manifest.json"
    schema_path = workspace / "L2-iforest/inference/feature_schema.py"
    if manifest_path.exists() and schema_path.exists():
        manifest = load(manifest_path)
        assert [f["column"] for f in contract["features"]] == manifest["feature_columns"]
        tree = ast.parse(schema_path.read_text(encoding="utf-8"))
        assignment = next(n for n in tree.body if isinstance(n, ast.AnnAssign)
                          and isinstance(n.target, ast.Name) and n.target.id == "FEATURE_PATHS")
        paths = ast.literal_eval(assignment.value)
        assert {f["column"]: f["path"] for f in contract["features"]} == {
            name: "/features/" + "/".join(path) for name, path in paths.items()
        }
        print("23 feature names, order and source paths match current IForest files (read only).")
    else:
        print("Sibling IForest files unavailable; cross-project feature check skipped.")
    print("Schema valid; 2 complete examples and 3 quality-state cases passed; invalid COMPLETE input rejected.")


if __name__ == "__main__":
    main()
