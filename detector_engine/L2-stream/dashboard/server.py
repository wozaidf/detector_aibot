import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FLINK_REST = os.getenv("FLINK_REST_URL", "http://localhost:8081").rstrip("/")
RULES_FILE = Path(os.getenv("RULES_CONFIG_FILE", "config/l2/rules.env"))
TOPICS = {"input": "l1-aligned-windows", "output": "l2-anomaly-events"}
L1_REPLAY_URL = os.getenv("L1_REPLAY_URL", "http://l1-replay:8091").rstrip("/")
LOCK = threading.RLock()
STATE = {
    "input_count": 0,
    "output_count": 0,
    "anomaly_count": 0,
    "final_event_ids": set(),
    "anomaly_event_ids": set(),
    "invalid_count": 0,
    "results": [],
    "sources": Counter(),
    "rules": Counter(),
    "recent": deque(maxlen=300),
    "last_offsets": {},
    "last_input": None,
    "last_output": None,
    "kafka_status": "connecting",
    "flink": {"status": "checking", "job": None, "operators": []},
}

DEFAULT_RULES = {
    "L2_CPU_HIGH_PCT": 80.0,
    "L2_MEMORY_HIGH_PCT": 85.0,
    "L2_LATENCY_HIGH_MS": 160.0,
    "L2_PACKET_LOSS_HIGH_RATIO": 0.03,
    "L2_LOG_ERROR_RATE": 0.30,
    "L2_SPAN_FAILURE_RATE": 0.30,
    "L2_CPU_SEVERE_PCT": 95.0,
    "L2_MEMORY_SEVERE_PCT": 92.0,
    "L2_LATENCY_SEVERE_MS": 220.0,
    "L2_PACKET_LOSS_SEVERE_RATIO": 0.05,
    "L2_LOG_ERROR_SEVERE_RATE": 0.60,
    "L2_SPAN_FAILURE_SEVERE_RATE": 0.80,
    "L2_JOINT_NETWORK_LATENCY_MS": 160.0,
    "L2_JOINT_NETWORK_PACKET_LOSS_RATIO": 0.03,
    "L2_JOINT_LOG_ERROR_RATE": 0.30,
    "L2_JOINT_SPAN_FAILURE_RATE": 0.30,
    "L2_JOINT_LOG_TRACE_MIN_OVERLAP_COUNT": 1,
    "L2_JOINT_MULTI_CPU_PCT": 80.0,
    "L2_JOINT_MULTI_MIN_FAILED_SPAN_COUNT": 1,
    "L2_JOINT_MULTI_MIN_ALARM_COUNT": 1,
    "L2_REPEATED_ERROR_COUNT": 3,
    "L2_CEP_OCCURRENCES": 3,
    "L2_CEP_COMPLEX_HORIZON_SECONDS": 600,
    "L2_WATERMARK_SECONDS": 5,
    "L2_IDLE_SECONDS": 30,
    "L2_DEDUP_SECONDS": 300,
    "L2_ASSOCIATION_WINDOW_SECONDS": 300,
    "L2_CEP_MAX_SEQUENCE_SECONDS": 604800,
}
RULE_BOUNDS = {
    "L2_CPU_HIGH_PCT": (0, 100),
    "L2_MEMORY_HIGH_PCT": (0, 100),
    "L2_LATENCY_HIGH_MS": (0, 60000),
    "L2_PACKET_LOSS_HIGH_RATIO": (0, 1),
    "L2_LOG_ERROR_RATE": (0, 1),
    "L2_SPAN_FAILURE_RATE": (0, 1),
    "L2_CPU_SEVERE_PCT": (0, 100),
    "L2_MEMORY_SEVERE_PCT": (0, 100),
    "L2_LATENCY_SEVERE_MS": (0, 60000),
    "L2_PACKET_LOSS_SEVERE_RATIO": (0, 1),
    "L2_LOG_ERROR_SEVERE_RATE": (0, 1),
    "L2_SPAN_FAILURE_SEVERE_RATE": (0, 1),
    "L2_JOINT_NETWORK_LATENCY_MS": (0, 60000),
    "L2_JOINT_NETWORK_PACKET_LOSS_RATIO": (0, 1),
    "L2_JOINT_LOG_ERROR_RATE": (0, 1),
    "L2_JOINT_SPAN_FAILURE_RATE": (0, 1),
    "L2_JOINT_LOG_TRACE_MIN_OVERLAP_COUNT": (1, 1000000),
    "L2_JOINT_MULTI_CPU_PCT": (0, 100),
    "L2_JOINT_MULTI_MIN_FAILED_SPAN_COUNT": (1, 1000000),
    "L2_JOINT_MULTI_MIN_ALARM_COUNT": (1, 1000000),
    "L2_REPEATED_ERROR_COUNT": (1, 1000),
    "L2_CEP_OCCURRENCES": (2, 100),
    "L2_CEP_COMPLEX_HORIZON_SECONDS": (1, 604800),
    "L2_WATERMARK_SECONDS": (0, 3600),
    "L2_IDLE_SECONDS": (1, 86400),
    "L2_DEDUP_SECONDS": (0, 86400),
    "L2_ASSOCIATION_WINDOW_SECONDS": (0, 86400),
    "L2_CEP_MAX_SEQUENCE_SECONDS": (60, 2592000),
}


def load_rules():
    values = dict(DEFAULT_RULES)
    try:
        for line in RULES_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key in values:
                values[key] = float(value) if isinstance(values[key], float) else int(value)
    except (OSError, ValueError):
        pass
    return values


def write_rules(values):
    """写回规则值并保留每个环境变量前的中文注释。"""
    comments_by_key = {}
    pending_comments = []
    try:
        existing_lines = RULES_FILE.read_text(encoding="utf-8").splitlines()
    except OSError:
        existing_lines = []
    for line in existing_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            pending_comments.append(line)
            continue
        key = stripped.split("=", 1)[0].strip()
        if pending_comments:
            comments_by_key[key] = pending_comments
        pending_comments = []

    output = []
    for key, value in values.items():
        output.extend(comments_by_key.get(key, []))
        output.append(f"{key}={value}")
    RULES_FILE.write_text("\n".join(output) + "\n", encoding="utf-8")


def watch_topic(kind, topic):
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=BOOTSTRAP,
                enable_auto_commit=False,
                consumer_timeout_ms=1500,
                value_deserializer=lambda value: value.decode("utf-8", errors="replace"),
            )
            deadline = time.time() + 10
            partitions = consumer.partitions_for_topic(topic)
            while not partitions and time.time() < deadline:
                consumer.poll(timeout_ms=200)
                partitions = consumer.partitions_for_topic(topic)
            assigned = [TopicPartition(topic, partition) for partition in (partitions or [])]
            if not assigned:
                raise RuntimeError(f"Kafka topic not found: {topic}")
            consumer.assign(assigned)
            consumer.seek_to_beginning(*assigned)
            with LOCK:
                STATE["kafka_status"] = "connected"
            while True:
                batches = consumer.poll(timeout_ms=1000, max_records=200)
                for records in batches.values():
                    for record in records:
                        receive_record(kind, record.value, record.topic, record.partition, record.offset)
        except Exception as exc:
            with LOCK:
                STATE["kafka_status"] = f"retrying: {type(exc).__name__}"
            time.sleep(3)
        finally:
            try:
                consumer.close()
            except Exception:
                pass


def receive_record(kind, raw, topic=None, partition=None, offset=None):
    now = time.time()
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        value = None
    with LOCK:
        if topic is not None and partition is not None and offset is not None:
            partition_key = f"{kind}:{topic}:{partition}"
            last_offset = STATE["last_offsets"].get(partition_key, -1)
            # Reconnects seek to the beginning; ignore offsets already observed by this dashboard process.
            if offset <= last_offset:
                return
            STATE["last_offsets"][partition_key] = offset
        STATE["recent"].append((now, kind))
        if kind == "input":
            STATE["input_count"] += 1
            STATE["last_input"] = now
            if value is None:
                STATE["invalid_count"] += 1
        elif kind == "output":
            STATE["output_count"] += 1
            STATE["last_output"] = now
            if isinstance(value, dict):
                event_id = value.get("event_id")
                first_observation = not event_id or event_id not in STATE["final_event_ids"]
                if event_id:
                    STATE["final_event_ids"].add(event_id)
                if first_observation:
                    sources = value.get("detection_sources") or []
                    rules = value.get("matched_rules") or []
                    for source in sources:
                        STATE["sources"]["DROOLS" if source == "DROOLS_WINDOW" else source] += 1
                    for rule in rules:
                        STATE["rules"][rule] += 1
                    result = dict(value)
                    result["_received_at"] = now
                    STATE["results"].insert(0, result)
                    del STATE["results"][500:]
                if value.get("anomaly") is True and event_id:
                    STATE["anomaly_event_ids"].add(event_id)
                    STATE["anomaly_count"] = len(STATE["anomaly_event_ids"])


def flink_snapshot():
    try:
        with urllib.request.urlopen(f"{FLINK_REST}/jobs/overview", timeout=1.5) as response:
            jobs = json.load(response).get("jobs", [])
        running = next((job for job in jobs if job.get("state") == "RUNNING"), None)
        if not running:
            return {"status": "no running job", "job": None, "operators": []}
        with urllib.request.urlopen(f"{FLINK_REST}/jobs/{running['jid']}", timeout=1.5) as response:
            vertices = json.load(response).get("vertices", [])
        operators = [{
                "name": item.get("name", "operator"),
                "status": item.get("status", "UNKNOWN"),
                "parallelism": item.get("parallelism", 1),
                "records_in": item.get("metrics", {}).get("read-records", "—"),
                "records_out": item.get("metrics", {}).get("write-records", "—"),
            } for item in vertices]
        return {"status": "RUNNING", "job": running.get("name"), "operators": operators,
                "pipeline_counts": flink_pipeline_counts(running["jid"], vertices)}
    except (OSError, urllib.error.URLError, json.JSONDecodeError, KeyError):
        return {"status": "unavailable", "job": None, "operators": []}


def snapshot():
    now = time.time()
    with LOCK:
        STATE["recent"] = deque((item for item in STATE["recent"] if now - item[0] <= 60), maxlen=300)
        recent_input = sum(kind == "input" for _, kind in STATE["recent"])
        recent_output = sum(kind == "output" for _, kind in STATE["recent"])
        result = {
            "input_count": STATE["input_count"],
            "invalid_count": STATE["invalid_count"],
            "output_count": STATE["output_count"],
            "anomaly_count": STATE["anomaly_count"],
            "source_counts": dict(STATE["sources"]),
            "rule_counts": dict(STATE["rules"]),
            "results": list(STATE["results"]),
            "input_per_minute": recent_input,
            "output_per_minute": recent_output,
            "kafka_status": STATE["kafka_status"],
            "rules": load_rules(),
            "replay": replay_status(),
        }
    result["flink"] = flink_snapshot()
    return result


def flink_pipeline_counts(job_id, vertices):
    counts = {
        "drools_ordinary_marked_windows": None,
        "drools_candidate_events": None,
        "cep_matched_sequences": None,
    }
    suffixes = {
        "drools_ordinary_marked_windows": "drools.ordinary_marked_windows",
        "drools_candidate_events": "drools.candidate_events",
        "cep_matched_sequences": "cep.matched_sequences",
    }
    for vertex in vertices:
        base = f"{FLINK_REST}/jobs/{job_id}/vertices/{vertex['id']}/metrics"
        try:
            with urllib.request.urlopen(base, timeout=1.0) as response:
                available = json.load(response)
            selected = {entry["id"]: key for key, suffix in suffixes.items()
                        for entry in available if entry.get("id", "").endswith(suffix)}
            if not selected:
                continue
            query = urllib.parse.urlencode({"get": ",".join(selected)})
            with urllib.request.urlopen(f"{base}?{query}", timeout=1.0) as response:
                values = json.load(response)
            for item in values:
                key = selected.get(item.get("id"))
                if key is not None:
                    current = counts[key] or 0
                    counts[key] = current + int(float(item.get("value", 0)))
        except (OSError, urllib.error.URLError, json.JSONDecodeError, KeyError, ValueError):
            continue
    return counts


def replay_status():
    """Return the L1 replay service state while keeping the dashboard response schema stable."""
    try:
        with urllib.request.urlopen(f"{L1_REPLAY_URL}/replay/status", timeout=2) as response:
            return json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        return {"status": "error", "sent": 0, "confirmed": 0, "total": 0,
                "speed": 600, "action": "replay", "message": f"L1 回放服务不可用：{exc}",
                "started_at": None, "finished_at": None}


def start_replay(payload):
    """Forward the existing dashboard control request to the independent L1 service."""
    request = urllib.request.Request(
        f"{L1_REPLAY_URL}/replay", data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8"))
        except (OSError, json.JSONDecodeError):
            return exc.code, {"error": f"L1 回放服务返回 HTTP {exc.code}"}
    except (OSError, urllib.error.URLError) as exc:
        return 503, {"error": f"L1 回放服务连接失败：{exc}"}


class Handler(BaseHTTPRequestHandler):
    def send_json(self, status, value):
        body = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/state":
            self.send_json(200, snapshot())
            return
        if self.path == "/api/health":
            self.send_json(200, {"ok": True})
            return
        if self.path in ("/", "/index.html"):
            body = Path(__file__).with_name("index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length))
            if self.path == "/api/replay":
                status, result = start_replay(payload)
                self.send_json(status, result)
                return
            if self.path != "/api/rules":
                self.send_error(404)
                return
            current = load_rules()
            updated = {}
            for key, old in current.items():
                value = float(payload[key])
                low, high = RULE_BOUNDS[key]
                if not (low <= value <= high):
                    raise ValueError(f"{key} 超出允许范围")
                if isinstance(old, int) and not value.is_integer():
                    raise ValueError(f"{key} 必须是整数")
                updated[key] = int(value) if isinstance(old, int) else value
            write_rules(updated)
            self.send_json(200, {"saved": True, "rules": updated, "apply": "docker compose up -d --force-recreate jobmanager taskmanager"})
        except (KeyError, ValueError, TypeError, json.JSONDecodeError, OSError) as exc:
            self.send_json(400, {"error": str(exc)})

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    for kind, topic in TOPICS.items():
        threading.Thread(target=watch_topic, args=(kind, topic), daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
