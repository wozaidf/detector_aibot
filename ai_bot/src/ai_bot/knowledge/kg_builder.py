"""Aggregate cleaned AIOps CSV data and load its knowledge graph into Neo4j."""

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import math
from pathlib import Path
import re
from collections import defaultdict

DATABASE = "knowledge"
DATASET = "AIOps-Challenge-2018"
DEFAULT_SOURCE = Path(__file__).parent / "AI-Bot-raw-knowledge-expanded" / "data_clean"
BATCH_SIZE = 1_000
CSV_FENCE = "~" * 64
DAY_IN_PATH = re.compile(r"\d{4}_\d{2}_\d{2}")


def stable_id(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


def rows_from_clean_markdown(path: Path):
    """Stream CSV records from the fenced CSV block in a cleaned Markdown file."""
    with path.open("r", encoding="utf-8", newline="") as source:
        for line in source:
            if line.strip() == CSV_FENCE + "csv":
                break
        else:
            return

        def csv_lines():
            for line in source:
                if line.strip() == CSV_FENCE:
                    return
                yield line

        yield from csv.DictReader(csv_lines())


def hour_bucket(timestamp: str) -> str:
    milliseconds = int(float(timestamp))
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).strftime("%Y-%m-%dT%H:00:00Z")


def aggregate_clean_data(root: Path):
    files = sorted(root.rglob("*.csv.md"))
    if not files:
        raise ValueError(f"在清洗目录中没有找到 CSV Markdown 文件: {root}")

    components = {}
    metrics = {}
    metric_windows = {}
    call_patterns = {}

    for path in files:
        relative = path.relative_to(root)
        parts = relative.parts
        day_match = DAY_IN_PATH.search(relative.as_posix())
        if day_match is None:
            continue
        category_dir = relative.parent.name
        source_file = relative.as_posix()

        for row in rows_from_clean_markdown(path):
            if category_dir == "调用链指标":
                timestamp = (row.get("startTime") or "").strip()
                call_type = (row.get("callType") or path.name.removesuffix(".csv.md")).strip()
                component_id = (row.get("cmdb_id") or "").strip()
                if not timestamp or not call_type or not component_id:
                    continue
                try:
                    start_time = hour_bucket(timestamp)
                    elapsed = float(row.get("elapsedTime") or "")
                except (ValueError, OverflowError):
                    continue
                if not math.isfinite(elapsed):
                    continue
                component_type = "trace_component"
                component_uid = stable_id(DATASET, component_id)
                component_name = (row.get("serviceName") or row.get("dsName") or component_id).strip()
                components.setdefault(component_uid, {
                    "dataset": DATASET,
                    "component_uid": component_uid,
                    "component_id": component_id,
                    "component_type": component_type,
                    "name": component_name,
                })

                service_name = (row.get("serviceName") or "").strip()
                data_source_name = (row.get("dsName") or "").strip()
                pattern_id = stable_id(DATASET, component_uid, call_type, start_time, service_name, data_source_name)
                pattern = call_patterns.setdefault(pattern_id, {
                    "dataset": DATASET,
                    "pattern_id": pattern_id,
                    "component_uid": component_uid,
                    "call_type": call_type,
                    "service_name": service_name,
                    "data_source_name": data_source_name,
                    "start_time": start_time,
                    "end_time": start_time,
                    "span_count": 0,
                    "success_count": 0,
                    "failure_count": 0,
                    "elapsed_total_ms": 0.0,
                    "elapsed_max_ms": 0.0,
                    "source_file": source_file,
                })
                pattern["span_count"] += 1
                pattern["elapsed_total_ms"] += elapsed
                pattern["elapsed_max_ms"] = max(pattern["elapsed_max_ms"], elapsed)
                pattern["start_time"] = min(pattern["start_time"], start_time)
                pattern["end_time"] = max(pattern["end_time"], start_time)
                if (row.get("success") or "").strip().lower() == "true":
                    pattern["success_count"] += 1
                else:
                    pattern["failure_count"] += 1
                continue

            metric_name = (row.get("name") or "").strip()
            component_id = (row.get("cmdb_id") or row.get("serviceName") or "").strip()
            timestamp = (row.get("timestamp") or row.get("startTime") or "").strip()
            if not metric_name or not component_id or not timestamp:
                continue
            try:
                start_time = hour_bucket(timestamp)
            except (ValueError, OverflowError):
                continue

            category = path.name.removesuffix(".csv.md").removesuffix(".csv")
            component_type = "service" if category_dir == "业务指标" else category
            component_uid = stable_id(DATASET, component_id)
            components.setdefault(component_uid, {
                "dataset": DATASET,
                "component_uid": component_uid,
                "component_id": component_id,
                "component_type": component_type,
                "name": (row.get("serviceName") or component_id).strip(),
            })
            metric_id = stable_id(DATASET, category, metric_name)
            metrics.setdefault(metric_id, {
                "dataset": DATASET,
                "metric_id": metric_id,
                "name": metric_name,
                "category": category,
            })
            window_id = stable_id(DATASET, component_uid, metric_id, start_time)
            value_text = (row.get("value") or row.get(metric_name) or "").strip()
            try:
                value = float(value_text)
            except ValueError:
                continue
            if not math.isfinite(value):
                continue

            bucket = metric_windows.setdefault(window_id, {
                "dataset": DATASET,
                "window_id": window_id,
                "component_uid": component_uid,
                "metric_id": metric_id,
                "start_time": start_time,
                "end_time": start_time,
                "sample_count": 0,
                "value_sum": 0.0,
                "value_min": math.inf,
                "value_max": -math.inf,
                "source_file": source_file,
            })
            bucket["sample_count"] += 1
            bucket["value_sum"] += value
            bucket["value_min"] = min(bucket["value_min"], value)
            bucket["value_max"] = max(bucket["value_max"], value)
            bucket["start_time"] = min(bucket["start_time"], start_time)
            bucket["end_time"] = max(bucket["end_time"], start_time)

    for bucket in metric_windows.values():
        bucket["value_avg"] = bucket.pop("value_sum") / bucket["sample_count"]
    for pattern in call_patterns.values():
        pattern["elapsed_avg_ms"] = pattern.pop("elapsed_total_ms") / pattern["span_count"]

    return components, metrics, metric_windows, call_patterns


def batches(values, size):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def write_to_neo4j(components, metrics, windows, patterns, batch_size=BATCH_SIZE):
    from ai_bot.knowledge.neo4j_driver import neo4j_driver

    constraints = [
        "CREATE CONSTRAINT kg_component_uid IF NOT EXISTS FOR (n:Component) REQUIRE n.component_uid IS UNIQUE",
        "CREATE CONSTRAINT kg_metric_id IF NOT EXISTS FOR (n:Metric) REQUIRE n.metric_id IS UNIQUE",
        "CREATE CONSTRAINT kg_metric_window_id IF NOT EXISTS FOR (n:MetricWindow) REQUIRE n.window_id IS UNIQUE",
        "CREATE CONSTRAINT kg_call_pattern_id IF NOT EXISTS FOR (n:CallPattern) REQUIRE n.pattern_id IS UNIQUE",
    ]
    try:
        for query in constraints:
            neo4j_driver.execute_query(query, database_=DATABASE)

        for batch in batches(list(components.values()), batch_size):
            neo4j_driver.execute_query(
                "UNWIND $rows AS row MERGE (c:Component {component_uid: row.component_uid}) "
                "SET c += row",
                rows=batch,
                database_=DATABASE,
            )
        for batch in batches(list(metrics.values()), batch_size):
            neo4j_driver.execute_query(
                "UNWIND $rows AS row MERGE (m:Metric {metric_id: row.metric_id}) SET m += row",
                rows=batch,
                database_=DATABASE,
            )

        window_rows = []
        for item in windows.values():
            window_rows.append({
                "window_id": item["window_id"],
                "component_uid": item["component_uid"],
                "metric_id": item["metric_id"],
                "properties": {key: value for key, value in item.items() if key not in {"component_uid", "metric_id"}},
            })
        for batch in batches(window_rows, batch_size):
            neo4j_driver.execute_query(
                "UNWIND $rows AS row "
                "MATCH (c:Component {component_uid: row.component_uid}) "
                "MATCH (m:Metric {metric_id: row.metric_id}) "
                "MERGE (w:MetricWindow {window_id: row.window_id}) SET w += row.properties "
                "MERGE (c)-[:HAS_METRIC_WINDOW]->(w) MERGE (w)-[:MEASURES]->(m)",
                rows=batch,
                database_=DATABASE,
            )

        pattern_rows = []
        for item in patterns.values():
            pattern_rows.append({
                "pattern_id": item["pattern_id"],
                "component_uid": item["component_uid"],
                "properties": {key: value for key, value in item.items() if key != "component_uid"},
            })
        for batch in batches(pattern_rows, batch_size):
            neo4j_driver.execute_query(
                "UNWIND $rows AS row MATCH (c:Component {component_uid: row.component_uid}) "
                "MERGE (p:CallPattern {pattern_id: row.pattern_id}) SET p += row.properties "
                "MERGE (c)-[:HAS_CALL_PATTERN]->(p)",
                rows=batch,
                database_=DATABASE,
            )
    finally:
        neo4j_driver.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", nargs="?", type=Path, default=DEFAULT_SOURCE,
                        help="清洗后的 Markdown 目录")
    parser.add_argument("--write", action="store_true", help="写入 Neo4j，默认只聚合并预览规模")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size 必须大于 0")
    if not args.source.is_dir():
        parser.error(f"找不到清洗数据目录: {args.source}")

    components, metrics, windows, patterns = aggregate_clean_data(args.source)
    print(f"数据集: {DATASET}")
    print(f"组件: {len(components):,}，指标: {len(metrics):,}，指标小时汇总: {len(windows):,}，调用模式小时汇总: {len(patterns):,}")
    print("未将原始采样点或 span 逐条写入图谱。")
    if args.write:
        write_to_neo4j(components, metrics, windows, patterns, args.batch_size)
        print(f"已写入 Neo4j 数据库 {DATABASE}。")
    else:
        print("预览模式，没有连接或写入 Neo4j。添加 --write 执行导入。")


if __name__ == "__main__":
    main()
