"""L1 对齐窗口 JSONL 校验工具。

逐行校验顶层字段、窗口时间/尺寸、特征与融合特征重算一致性、质量统计、
记录引用、滑动步长以及 CPU 持续高负载序列等约束，并返回校验报告。
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

from .generator.common import parse_time
from .generator.feature_calculator import calculate


TOP = {"schema_version", "window_id", "record_type", "entity_id", "entity_type", "window", "events", "features", "context", "quality", "fusion_features"}


def validate(path: Path) -> dict:
    errors, windows, seen = [], [], set()
    with path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            try: row = json.loads(line)
            except Exception as exc: errors.append(f"line {lineno}: invalid JSON: {exc}"); continue
            windows.append(row)
            missing = TOP - set(row)
            if missing: errors.append(f"line {lineno}: missing top-level fields {sorted(missing)}"); continue
            if row["record_type"] != "aligned_window": errors.append(f"line {lineno}: invalid record_type")
            if row["window_id"] in seen: errors.append(f"line {lineno}: duplicate window_id")
            seen.add(row["window_id"])
            ws, we = parse_time(row["window"]["start"]), parse_time(row["window"]["end"])
            if (we - ws).total_seconds() != row["window"]["size_sec"]: errors.append(f"line {lineno}: inconsistent window size")
            features, fusion, missing_features = calculate(row["events"], row["window"]["size_sec"])
            if features != row["features"]: errors.append(f"line {lineno}: features mismatch")
            if fusion != row["fusion_features"]: errors.append(f"line {lineno}: fusion_features mismatch")
            if missing_features != row["quality"]["missing_features"]: errors.append(f"line {lineno}: missing_features mismatch")
            ids = {k: {x["record_id"] for x in row["events"][k]} for k in row["events"]}
            for rel in row["fusion_features"]["key_level"]["relations"]:
                if not set(rel["log_record_ids"]) <= ids["logs"] or not set(rel["span_record_ids"]) <= ids["traces"]: errors.append(f"line {lineno}: invalid relation record ID")
            counts = row["quality"]["sample_counts"]
            if counts["logs"] != len(row["events"]["logs"]) or counts["spans"] != len(row["events"]["traces"]) or counts["alarms"] != len(row["events"]["alarms"]): errors.append(f"line {lineno}: quality counts mismatch")
    record_versions = {}
    by_entity = defaultdict(list)
    for row in windows:
        by_entity[row["entity_id"]].append(row)
        for kind, records in row["events"].items():
            for record in records:
                canonical = json.dumps(record, sort_keys=True)
                old = record_versions.setdefault(record["record_id"], canonical)
                if old != canonical: errors.append(f"record content changed: {record['record_id']}")
    for entity, rows in by_entity.items():
        rows.sort(key=lambda x: x["window"]["start"])
        for a, b in zip(rows, rows[1:]):
            delta = (parse_time(b["window"]["start"]) - parse_time(a["window"]["start"])).total_seconds()
            if delta not in (0, a["window"]["slide_sec"]): errors.append(f"{entity}: invalid slide {delta}")
    has_cpu_sequence = False
    for rows in by_entity.values():
        run = 0
        for row in sorted(rows, key=lambda x: x["window"]["start"]):
            value = row["features"]["metrics"]["cpu_mean_pct"]
            run = run + 1 if value is not None and value > 80 else 0
            has_cpu_sequence |= run >= 3
    if windows and not has_cpu_sequence: errors.append("no sequence of 3 consecutive windows with cpu_mean_pct > 80")
    ends = [(parse_time(w["window"]["end"]), w["entity_id"]) for w in windows]
    if ends != sorted(ends): errors.append("windows are not sorted by end time and entity_id")
    return {"valid": not errors, "window_count": len(windows), "entity_count": len(by_entity), "unique_record_count": len(record_versions), "cpu_sequence_found": has_cpu_sequence, "errors": errors[:100]}


def main():
    p = argparse.ArgumentParser(); p.add_argument("input", type=Path, nargs="?", default=Path("L1生成结果/l1_windows.jsonl")); args = p.parse_args()
    result = validate(args.input); print(json.dumps(result, indent=2, ensure_ascii=False)); raise SystemExit(0 if result["valid"] else 1)


if __name__ == "__main__": main()
