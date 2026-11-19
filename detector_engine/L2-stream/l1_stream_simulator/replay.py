"""L1 对齐窗口回放工具：默认按模拟时间逐行发送到 Kafka。"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from pathlib import Path
from typing import Callable

# Kafka 是回放的必需依赖，使用前需安装 requirements.txt 中的 kafka-python。
from kafka import KafkaProducer

from .generator.common import parse_time


def replay(path: Path, speed: float = 60.0, kafka_bootstrap: str = "localhost:9092", topic: str = "l1-aligned-windows",
           sleep=time.sleep, producer_factory=KafkaProducer,
           progress_callback: Callable[[dict], None] | None = None) -> int:
    """按模拟时间将 JSONL 窗口发送到 Kafka，返回 broker 确认的消息数。

    path 为每行一个窗口 JSON 的文件，输入应已按窗口结束时间排序。
    speed 为模拟时间的加速倍数，例如 600 表示模拟 600 秒仅等待 1 秒。
    kafka_bootstrap 和 topic 分别指定 Kafka 地址和输入主题。
    sleep、producer_factory 允许测试注入替代实现；进度回调分别报告已提交和已确认数量。
    """
    if speed <= 0: raise ValueError("speed must be greater than zero")
    # 始终创建 Producer；连接或配置错误会向上抛出，不切换为终端输出。
    producer = producer_factory(bootstrap_servers=kafka_bootstrap)
    print(f"已连接 Kafka：{kafka_bootstrap}，Topic：{topic}", file=sys.stderr, flush=True)
    previous = None
    sent = 0
    confirmed = 0
    failures = []
    progress_lock = threading.Lock()

    def report():
        if progress_callback:
            with progress_lock:
                snapshot = {"sent": sent, "confirmed": confirmed, "total": None}
            progress_callback(snapshot)

    def on_success(_metadata):
        nonlocal confirmed
        with progress_lock:
            confirmed += 1
        report()

    def on_failure(error):
        failures.append(error)
        report()

    try:
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                # 空行跳过；非空行必须是合法 JSON，并包含实体和窗口时间字段。
                if not line.strip():
                    continue
                row = json.loads(line)
                current = parse_time(row["window"]["end"])
                if previous is not None:
                    # 以相邻窗口结束时间差控制节奏；相同时间或倒序时间不等待。
                    delay = max(0, (current - previous).total_seconds() / speed)
                    if delay: sleep(delay)
                # 保留完整 L1 窗口结构，中文直接编码为 UTF-8。
                payload = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                # entity_id 作为消息 Key，供 Kafka 按实体选择分区。
                # send 为异步提交，因此 count 不是逐条确认成功的 Kafka 写入计数。
                future = producer.send(topic, key=row["entity_id"].encode("utf-8"), value=payload.encode("utf-8"))
                if hasattr(future, "add_callback") and hasattr(future, "add_errback"):
                    future.add_callback(on_success)
                    future.add_errback(on_failure)
                else:
                    # 兼容简单的测试 Producer；真实 kafka-python Future 使用上面的异步回调。
                    on_success(None)
                previous = current
                with progress_lock:
                    sent += 1
                report()
                if sent % 100 == 0:
                    print(f"已提交 {sent} 条消息，已确认 {confirmed} 条", file=sys.stderr, flush=True)
        producer.flush()
        report()
        if failures:
            raise RuntimeError(f"Kafka 有 {len(failures)} 条消息发送失败") from failures[0]
    except Exception:
        # 发生读取、解析或发送异常时仍关闭 Producer，并保留原异常供调用方处理。
        producer.close()
        raise
    producer.close()
    print(f"发送完成：已提交 {sent} 条，Kafka 已确认 {confirmed} 条", file=sys.stderr, flush=True)
    return confirmed


def main():
    """解析命令行参数并启动 Kafka 回放；默认输入路径相对于当前工作目录。"""
    p = argparse.ArgumentParser(description="Replay L1 JSONL to Kafka in simulated-time order")
    p.add_argument("input", type=Path, nargs="?", default=Path("L1生成结果/l1_windows.jsonl"))
    p.add_argument("--speed", type=float, default=60.0)
    p.add_argument("--kafka-bootstrap", default="localhost:9092", help="Kafka 地址，默认 localhost:9092")
    p.add_argument("--topic", default="l1-aligned-windows", help="Kafka 输入 Topic")
    a = p.parse_args()
    # 将命令行指定的速度、Kafka 地址和 Topic 传入回放函数。
    replay(a.input, a.speed, a.kafka_bootstrap, a.topic)


if __name__ == "__main__": main()
