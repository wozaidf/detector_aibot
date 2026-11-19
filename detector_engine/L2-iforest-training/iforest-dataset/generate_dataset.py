#!/usr/bin/env python3
"""Generate IForest CSV splits using the L1 simulator's exact feature pipeline."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent
TRAINING_ROOT = ROOT.parent
WORKSPACE_ROOT = TRAINING_ROOT.parent
L1_ROOT = WORKSPACE_ROOT / "L2-stream" / "l1_stream_simulator"
DEFAULT_OUTPUT = ROOT / "datasets"
sys.path.insert(0, str(WORKSPACE_ROOT / "L2-iforest"))
from inference.feature_schema import FEATURE_COLUMNS, FEATURE_PATHS

WINDOW_SIZE_SEC = 300
SLIDE_SEC = 60
METRIC_INTERVAL_SEC = 30
ENTITY_IDS = ("server-001", "server-002", "server-003")
SCENARIOS = (
    "cpu_sustained", "cpu_spike", "memory_growth", "network_degradation",
    "multi_metric", "log_anomaly", "trace_anomaly", "cross_type", "maintenance",
    "data_quality",
)


def load_l1_pipeline():
    """Load the same event generators, slicer and calculator used by L1."""
    sys.path.insert(0, str(L1_ROOT.parent))
    from l1_stream_simulator.generator.common import parse_time
    from l1_stream_simulator.generator.event_generator import generate_events
    from l1_stream_simulator.generator.feature_calculator import calculate
    from l1_stream_simulator.generator.scenario_generator import build_scenarios
    from l1_stream_simulator.generator.window_builder import build_windows

    return parse_time, generate_events, calculate, build_scenarios, build_windows


def load_columns(source: Path | None = None) -> list[str]:
    """Return the one shared model schema; source is kept for CLI compatibility."""
    return list(FEATURE_COLUMNS)


def label_window(window: dict[str, Any], scenarios: list[Any]) -> int:
    start = window["window"]["start"]
    end = window["window"]["end"]
    parse_time, *_ = load_l1_pipeline()
    ws, we = parse_time(start), parse_time(end)
    return int(any(s.entity_id == window["entity_id"] and s.start < we and s.end > ws for s in scenarios))


def make_split(
    name: str, count: int, start_time: str, seed: int, columns: list[str],
    window_size_sec: int, slide_sec: int, metric_interval_sec: int,
    anomaly_ratio: float, scenario_frequency: float,
) -> pd.DataFrame:
    parse_time, generate_events, calculate, build_scenarios, build_windows = load_l1_pipeline()
    if count <= 0:
        raise ValueError(f"{name} 样本数必须大于零")
    if not 0 <= anomaly_ratio < 0.5:
        raise ValueError("异常比例必须在 [0, 0.5) 范围内")
    # Every entity receives a contiguous time segment; segments are separated by
    # one full window so no raw L1 interval can leak across train/val/test.
    per_entity = (count + len(ENTITY_IDS) - 1) // len(ENTITY_IDS)
    duration = timedelta(seconds=window_size_sec + (per_entity - 1) * slide_sec)
    start = parse_time(start_time)
    enabled = list(SCENARIOS) if name != "train" else []
    scenarios = build_scenarios(
        list(ENTITY_IDS), start, duration, enabled, scenario_frequency, 1.0,
        seed, max(600, window_size_sec * 2),
    )
    raw_events = generate_events(list(ENTITY_IDS), start, duration, metric_interval_sec, scenarios, seed)
    windows = build_windows(
        list(ENTITY_IDS), start, duration, window_size_sec, slide_sec,
        raw_events, scenarios, count, metric_interval_sec,
    )
    labels = [int(label_window(w, scenarios)) for w in windows]
    rows = []
    for i, window in enumerate(windows):
        row: dict[str, Any] = {
            "window_id": f"{name.upper()}-{window['window_id']}",
            "entity_id": window["entity_id"],
            "window_end": window["window"]["end"],
            "label": labels[i],
        }
        features = window["features"]
        for col in columns:
            group, feature = FEATURE_PATHS[col]
            row[col] = features[group][feature]
        rows.append(row)
    return pd.DataFrame(rows)


def generate(args: argparse.Namespace) -> dict[str, pd.DataFrame]:
    columns = load_columns(args.source)
    parse_time, *_ = load_l1_pipeline()
    starts = {}
    cursor = parse_time(args.start_time)
    per_entity_counts = [
        (args.train_count + 2) // 3,
        (args.val_count + 2) // 3,
        (args.test_count + 2) // 3,
    ]
    for name, count in zip(("train", "val", "test"), per_entity_counts):
        starts[name] = cursor.isoformat(timespec="seconds").replace("+00:00", "Z")
        cursor += timedelta(seconds=args.window_size_sec + count * args.slide_sec)
    return {
        "train": make_split("train", args.train_count, starts["train"], args.seed, columns, args.window_size_sec, args.slide_sec, args.metric_interval_sec, 0.0, 0.0),
        "val": make_split("val", args.val_count, starts["val"], args.seed + 1, columns, args.window_size_sec, args.slide_sec, args.metric_interval_sec, args.val_anomaly_ratio, args.scenario_frequency),
        "test": make_split("test", args.test_count, starts["test"], args.seed + 2, columns, args.window_size_sec, args.slide_sec, args.metric_interval_sec, args.test_anomaly_ratio, args.scenario_frequency),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=None, help="旧 CLI 兼容参数；特征 schema 由 inference/feature_schema.py 定义")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--train-count", type=int, default=14000)
    parser.add_argument("--val-count", type=int, default=3000)
    parser.add_argument("--test-count", type=int, default=3000)
    parser.add_argument("--val-anomaly-ratio", type=float, default=0.10, help="兼容旧CLI；标签现在由 L1 场景与窗口重叠直接生成")
    parser.add_argument("--test-anomaly-ratio", type=float, default=0.10, help="兼容旧CLI；标签现在由 L1 场景与窗口重叠直接生成")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-time", default="2026-01-01T00:00:00Z")
    parser.add_argument("--window-size-sec", type=int, default=300)
    parser.add_argument("--slide-sec", type=int, default=60)
    parser.add_argument("--metric-interval-sec", type=int, default=30)
    parser.add_argument("--scenario-frequency", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    datasets = generate(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for name, frame in datasets.items():
        path = args.output_dir / f"{name}.csv"
        frame.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"{path}: {len(frame):,} rows, {len(frame.columns)} columns, labels={frame['label'].value_counts().sort_index().to_dict()}")
    manifest = {
        "generator": "L1 simulator event_generator + window_builder + feature_calculator.calculate",
        "feature_calculator": str((L1_ROOT / "generator" / "feature_calculator.py").resolve()),
        "entity_ids": list(ENTITY_IDS),
        "window_size_sec": args.window_size_sec,
        "slide_sec": args.slide_sec,
        "metric_interval_sec": args.metric_interval_sec,
        "seed": args.seed,
        "scenario_frequency": args.scenario_frequency,
        "scenario_duration_sec": max(600, args.window_size_sec * 2),
        "anomaly_intensity": 1.0,
        "enabled_scenarios": list(SCENARIOS),
        "split_seeds": {"train": args.seed, "val": args.seed + 1, "test": args.seed + 2},
        "label_policy": "L1 scenario interval overlaps the aligned window; train has scenarios disabled",
        "feature_columns": list(FEATURE_COLUMNS),
        "count_normalization": "event counts divided by window_size_sec / 60; event category composition represented as rates",
        "splits": {},
    }
    for name, frame in datasets.items():
        csv_path = args.output_dir / f"{name}.csv"
        digest = hashlib.sha256(csv_path.read_bytes()).hexdigest()
        manifest["splits"][name] = {
            "rows": len(frame),
            "label_counts": {str(k): int(v) for k, v in frame["label"].value_counts().sort_index().items()},
            "entity_counts": {str(k): int(v) for k, v in frame["entity_id"].value_counts().items()},
            "csv_sha256": digest,
        }
    (args.output_dir / "generation_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
