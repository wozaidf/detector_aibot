"""轻量级 L1 回放控制服务；实际 Kafka 发送复用 replay.replay。"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .replay import replay


REPLAY_FILE = Path(os.getenv("L1_REPLAY_FILE", "L1生成结果/l1_windows.jsonl"))
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("L1_INPUT_TOPIC", "l1-aligned-windows")
HOST = os.getenv("L1_REPLAY_HOST", "0.0.0.0")
PORT = int(os.getenv("L1_REPLAY_PORT", "8091"))
LOCK = threading.Lock()
STATE_LOCK = threading.RLock()
STATE = {"status": "idle", "sent": 0, "confirmed": 0, "total": 0, "speed": None,
         "action": None, "message": "等待开始", "started_at": None, "finished_at": None}


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run(speed: float, total: int) -> None:
    def update(progress):
        with STATE_LOCK:
            STATE["sent"] = progress["sent"]
            STATE["confirmed"] = progress["confirmed"]
            STATE["message"] = f"已提交 {progress['sent']}/{total}，Kafka 已确认 {progress['confirmed']} 条"

    try:
        confirmed = replay(REPLAY_FILE, speed=speed, kafka_bootstrap=BOOTSTRAP,
                           topic=TOPIC, progress_callback=update)
        with STATE_LOCK:
            STATE.update(status="completed", confirmed=confirmed,
                         message=f"回放完成：提交 {STATE['sent']} 条，Kafka 确认 {confirmed} 条",
                         finished_at=_timestamp())
    except Exception as exc:
        with STATE_LOCK:
            STATE.update(status="error", message=f"回放失败：{type(exc).__name__}: {exc}",
                         finished_at=_timestamp())
    finally:
        LOCK.release()


def _count_records(path: Path) -> int:
    if not path.is_file():
        raise FileNotFoundError(f"找不到样本文件：{path}")
    with path.open(encoding="utf-8") as source:
        count = sum(1 for line in source if line.strip())
    if count == 0:
        raise ValueError("样本文件为空")
    return count


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"ok": True, "service": "l1-replay"})
        elif self.path == "/replay/status":
            with STATE_LOCK:
                self._send(200, dict(STATE))
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/replay":
            self._send(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(payload, dict):
                raise ValueError("请求内容必须是 JSON 对象")
            if payload.get("action", "replay") != "replay":
                raise ValueError("无效的回放操作")
            speed = float(payload.get("speed", 600))
            if not 1 <= speed <= 10000:
                raise ValueError("回放速度需在 1 到 10000 之间")
            total = _count_records(REPLAY_FILE)
        except (ValueError, TypeError, json.JSONDecodeError, OSError) as exc:
            self._send(400, {"error": str(exc)})
            return
        if not LOCK.acquire(blocking=False):
            self._send(409, {"error": "当前回放尚未结束，请稍候"})
            return
        now = _timestamp()
        with STATE_LOCK:
            STATE.update(status="running", sent=0, confirmed=0, total=total, speed=speed,
                         action="replay", message="正在连接 Kafka…", started_at=now, finished_at=None)
        threading.Thread(target=_run, args=(speed, total), daemon=True).start()
        self._send(202, {"started": True, "total": total, "speed": speed, "action": "replay"})

    def log_message(self, *_):
        pass


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"L1 replay control listening on {HOST}:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
