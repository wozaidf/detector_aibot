#!/usr/bin/env python3
"""Validate synthetic dataset files, feature contract, logic, and time splits."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from generate_dataset import WINDOW_SIZE_SEC, SLIDE_SEC, load_columns


ROOT = Path(__file__).resolve().parent
DEFAULT_DATASETS = ROOT / "datasets"


def check(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def validate(args: argparse.Namespace) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    report: list[str] = []
    feature_columns = load_columns()
    expected_columns = ["window_id", "entity_id", "window_end", "label", *feature_columns]
    expected_sizes = {"train": args.train_count, "val": args.val_count, "test": args.test_count}
    frames: dict[str, pd.DataFrame] = {}
    intervals: dict[str, list[tuple[str, pd.Timestamp, pd.Timestamp, str, int]]] = {}

    for name in ("train", "val", "test"):
        path = args.datasets_dir / f"{name}.csv"
        check(path.exists(), f"缺少文件: {path}", errors)
        if not path.exists():
            continue
        frame = pd.read_csv(path, encoding="utf-8-sig")
        frames[name] = frame
        csv_column_count = len(frame.columns)
        check(list(frame.columns) == expected_columns, f"{name}.csv 列名或列顺序不正确", errors)
        check(len(frame) == expected_sizes[name], f"{name}.csv 行数应为 {expected_sizes[name]}，实际 {len(frame)}", errors)
        check(csv_column_count == 27, f"{name}.csv 应为 27 列，实际 {csv_column_count}", errors)
        check(not frame.isna().any().any(), f"{name}.csv 存在缺失值", errors)
        check(frame["window_id"].is_unique, f"{name}.csv window_id 不唯一", errors)
        check(set(frame["label"].unique()).issubset({0, 1}), f"{name}.csv label 只能为 0/1", errors)
        check(frame["window_end"].notna().all(), f"{name}.csv window_end 存在无效时间", errors)
        if frame["window_end"].notna().all():
            frame["_end"] = pd.to_datetime(frame["window_end"], utc=True, errors="coerce")
            check(frame["_end"].notna().all(), f"{name}.csv window_end 时间格式无效", errors)
            if frame["_end"].notna().all():
                frame["_start"] = frame["_end"] - pd.to_timedelta(WINDOW_SIZE_SEC, unit="s")
                for entity, group in frame.groupby("entity_id", sort=False):
                    sorted_group = group.sort_values("_end")
                    ends = sorted_group["_end"]
                    check(ends.is_monotonic_increasing, f"{name}.csv {entity} 时间未递增", errors)
                    deltas = ends.diff().dropna().dt.total_seconds()
                    check((deltas == SLIDE_SEC).all(), f"{name}.csv {entity} 窗口步长不是 {SLIDE_SEC} 秒", errors)
                    for _, row in sorted_group.iterrows():
                        intervals.setdefault(entity, []).append((name, row["_start"], row["_end"], str(row["window_id"]), int(row["label"])))

        for col in feature_columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        check(not frame[feature_columns].isna().any().any(), f"{name}.csv 存在非数值特征", errors)
        check(np.isfinite(frame[feature_columns].to_numpy(dtype=float)).all(), f"{name}.csv 存在无穷特征", errors)
        if frame[feature_columns].notna().all().all():
            value = lambda key: frame[f"{key}"]
            for metric in ("cpu", "memory"):
                mean_col, max_col = f"metrics_{metric}_mean_pct", f"metrics_{metric}_max_pct"
                check(value(mean_col).between(0, 100).all(), f"{name}.{mean_col} 超出 [0,100]", errors)
                check((value(max_col) >= value(mean_col)).all(), f"{name}.{max_col} 小于均值", errors)
                check(value(max_col).between(0, 100).all(), f"{name}.{max_col} 超出 [0,100]", errors)
            check(value("metrics_bandwidth_utilization_mean_pct").between(0, 100).all(), f"{name} 带宽利用率超界", errors)
            check(value("metrics_packet_loss_mean_ratio").between(0, 1).all(), f"{name} 丢包率超界", errors)
            check((value("metrics_latency_mean_ms") >= 0).all() and (value("metrics_latency_max_ms") >= value("metrics_latency_mean_ms")).all(), f"{name} 延迟统计不合理", errors)
            check((value("metrics_network_traffic_mean_mbps") >= 0).all(), f"{name} 网络流量为负数", errors)

            rate_cols = [
                "logs_error_rate", "logs_warn_rate", "logs_info_rate", "logs_error_trace_match_ratio",
                "traces_span_error_rate", "alarms_critical_rate", "alarms_major_rate", "alarms_minor_rate",
            ]
            check(frame[rate_cols].apply(lambda column: column.between(0, 1)).all().all(), f"{name} 比例特征超出 [0,1]", errors)
            volume_cols = [
                "logs_total_per_minute", "traces_span_per_minute", "traces_failed_span_per_minute",
                "alarms_alarm_per_minute",
            ]
            check((frame[volume_cols] >= 0).all().all(), f"{name} 每分钟数量特征不能为负", errors)
            log_rate_sum = value("logs_error_rate") + value("logs_warn_rate") + value("logs_info_rate")
            has_logs = value("logs_total_per_minute") > 0
            check(np.allclose(log_rate_sum[has_logs], 1.0, atol=2e-6), f"{name} 非空窗口日志等级比例和不为 1", errors)
            check((log_rate_sum[~has_logs] == 0).all(), f"{name} 空日志窗口的等级比例应为 0", errors)
            check((value("traces_failed_span_per_minute") <= value("traces_span_per_minute") + 1e-6).all(), f"{name} 失败 Span/分钟超过 Span/分钟", errors)
            check((value("traces_avg_duration_ms") >= 0).all() and (value("traces_max_duration_ms") >= value("traces_avg_duration_ms")).all(), f"{name} Trace 耗时统计不合理", errors)
            alarm_rate_sum = value("alarms_critical_rate") + value("alarms_major_rate") + value("alarms_minor_rate")
            has_alarms = value("alarms_alarm_per_minute") > 0
            check(np.allclose(alarm_rate_sum[has_alarms], 1.0, atol=2e-6), f"{name} 告警等级比例和不为 1", errors)
            check((alarm_rate_sum[~has_alarms] == 0).all(), f"{name} 无告警窗口的等级比例应为 0", errors)
        positives = int((frame["label"] == 1).sum())
        negatives = len(frame) - positives
        report.append(f"{name}: {len(frame):,} 条；正常 {negatives:,}，异常 {positives:,}；列数 {csv_column_count}")

    for name in ("val", "test"):
        if name in frames:
            check(frames[name]["label"].nunique() == 2,
                  f"{name}.csv 必须包含正常和由 L1 场景生成的异常窗口", errors)
    if "train" in frames:
        check((frames["train"]["label"] == 0).all(), "train.csv 必须全部为正常样本", errors)

    all_ids: list[str] = []
    for frame in frames.values():
        if "window_id" in frame:
            all_ids.extend(frame["window_id"].astype(str).tolist())
    check(len(all_ids) == len(set(all_ids)), "不同数据集之间 window_id 重复", errors)
    for entity, rows in intervals.items():
        rows.sort(key=lambda x: x[1])
        last_end: pd.Timestamp | None = None
        last_split: str | None = None
        for split, start, end, _, _ in rows:
            if last_end is not None and split != last_split:
                check(start >= last_end, f"{entity} 的 {last_split}/{split} 时间窗口有重叠", errors)
                check(start >= last_end, f"{entity} 的异常事件可能跨越数据集边界", errors)
            if last_end is None or end > last_end:
                last_end = end
                last_split = split
    if intervals:
        report.append(f"时间检查: {sum(len(v) for v in intervals.values()):,} 个窗口；跨数据集无时间重叠，窗口大小 {WINDOW_SIZE_SEC}s、步长 {SLIDE_SEC}s")
    report.append(f"特征列 ({len(feature_columns)}): " + ", ".join(feature_columns))
    report.append("校验结果: " + ("通过" if not errors else f"失败（{len(errors)} 项）"))
    return report, errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--datasets-dir", type=Path, default=DEFAULT_DATASETS)
    parser.add_argument("--train-count", type=int, default=14000)
    parser.add_argument("--val-count", type=int, default=3000)
    parser.add_argument("--test-count", type=int, default=3000)
    parser.add_argument("--val-anomaly-ratio", type=float, default=0.10)
    parser.add_argument("--test-anomaly-ratio", type=float, default=0.10)
    return parser.parse_args()


def main() -> None:
    report, errors = validate(parse_args())
    print("\n".join(report))
    if errors:
        print("\n问题明细:")
        for error in errors:
            print(f"- {error}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
