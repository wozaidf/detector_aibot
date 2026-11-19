"""L1 流模拟器命令行入口。

读取配置、生成异常场景与原始事件、构建对齐窗口，并输出
l1_windows.jsonl、ground_truth.jsonl 与 manifest.json。
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import timedelta
from pathlib import Path

from .generator.common import parse_time
from .generator.event_generator import generate_events
from .generator.scenario_generator import build_scenarios
from .generator.window_builder import build_windows


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def generate(config: dict, output_dir: Path) -> dict:
    start = parse_time(config["start_time"]); duration = timedelta(hours=float(config["duration_hours"]))
    entities = [f"server-{i:03d}" for i in range(1, int(config["entity_count"]) + 1)]
    scenario_horizon = duration
    if config.get("max_windows"):
        time_slots = (int(config["max_windows"]) + len(entities) - 1) // len(entities)
        covered_sec = (max(1, time_slots) - 1) * int(config["slide_sec"]) + int(config["window_size_sec"])
        scenario_horizon = min(duration, timedelta(seconds=covered_sec))
    scenarios = build_scenarios(entities, start, scenario_horizon, config["enabled_scenarios"], float(config["scenario_frequency"]), float(config["anomaly_intensity"]), int(config["seed"]), int(config.get("scenario_duration_sec", 600)))
    store = generate_events(entities, start, duration, int(config["metric_interval_sec"]), scenarios, int(config["seed"]))
    windows = build_windows(entities, start, duration, int(config["window_size_sec"]), int(config["slide_sec"]), store, scenarios, config.get("max_windows"), int(config["metric_interval_sec"]))
    output_dir.mkdir(parents=True, exist_ok=True)
    def write_jsonl(path: Path, rows):
        with path.open("w", encoding="utf-8", newline="\n") as fh:
            for row in rows: fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    write_jsonl(output_dir / "l1_windows.jsonl", windows); write_jsonl(output_dir / "ground_truth.jsonl", [s.as_truth() for s in scenarios])
    counts = {k: len({record["record_id"] for window in windows for record in window["events"][k]}) for k in ("metrics", "logs", "traces", "alarms")}
    manifest = {"schema_version": "1.0", "entity_count": len(entities), "window_count": len(windows), "raw_event_counts": counts, "raw_event_total": sum(counts.values()), "anomaly_scenario_count": len(scenarios), "anomaly_distribution": dict(sorted(Counter(s.anomaly_type for s in scenarios).items())), "seed": config["seed"], "start_time": config["start_time"], "duration_hours": config["duration_hours"], "window_size_sec": config["window_size_sec"], "slide_sec": config["slide_sec"]}
    (output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def main():
    default = Path(__file__).with_name("config") / "default.yaml"
    p = argparse.ArgumentParser(description="Generate deterministic L1 aligned windows")
    p.add_argument("--config", type=Path, default=default); p.add_argument("--output", type=Path, default=Path("L1生成结果"))
    p.add_argument("--entities", type=int); p.add_argument("--hours", type=float); p.add_argument("--seed", type=int); p.add_argument("--start-time"); p.add_argument("--slide-sec", type=int); p.add_argument("--window-size-sec", type=int); p.add_argument("--max-windows", type=int)
    args = p.parse_args(); cfg = load_config(args.config)
    for arg, key in (("entities", "entity_count"), ("hours", "duration_hours"), ("seed", "seed"), ("start_time", "start_time"), ("slide_sec", "slide_sec"), ("window_size_sec", "window_size_sec"), ("max_windows", "max_windows")):
        val = getattr(args, arg)
        if val is not None: cfg[key] = val
    print(json.dumps(generate(cfg, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
