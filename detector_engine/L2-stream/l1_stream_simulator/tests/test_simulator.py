"""L1 流模拟器单元测试。

覆盖生成可复现性、窗口数量、校验通过、回放输出、特征归一化与异常场景。
"""

import json
import tempfile
import unittest
from pathlib import Path

from l1_stream_simulator.generate import generate, load_config
from l1_stream_simulator.replay import replay
from l1_stream_simulator.validate import validate
from l1_stream_simulator.generator.feature_calculator import calculate


ROOT = Path(__file__).parents[1]
TEST_TMP = ROOT / ".test_tmp"
TEST_TMP.mkdir(exist_ok=True)


class SimulatorTests(unittest.TestCase):
    def config(self):
        cfg = load_config(ROOT / "config/default.yaml")
        cfg.update(entity_count=2, duration_hours=1, max_windows=None)
        return cfg

    def test_generation_validation_reproducibility_and_replay(self):
        with tempfile.TemporaryDirectory(dir=TEST_TMP) as a, tempfile.TemporaryDirectory(dir=TEST_TMP) as b:
            ma = generate(self.config(), Path(a)); mb = generate(self.config(), Path(b))
            one = Path(a, "l1_windows.jsonl"); two = Path(b, "l1_windows.jsonl")
            self.assertEqual(one.read_bytes(), two.read_bytes())
            self.assertEqual(Path(a, "ground_truth.jsonl").read_bytes(), Path(b, "ground_truth.jsonl").read_bytes())
            report = validate(one)
            self.assertTrue(report["valid"], report["errors"])
            self.assertEqual(ma, mb)
            class FakeProducer:
                def __init__(self, **kwargs):
                    self.kwargs = kwargs
                    self.messages = []
                    self.flushed = False
                    self.closed = False

                def send(self, topic, key, value):
                    self.messages.append((topic, key, value))

                def flush(self):
                    self.flushed = True

                def close(self):
                    self.closed = True

            producer = FakeProducer()
            sleeps = []
            self.assertEqual(
                replay(one, 600, sleep=sleeps.append, producer_factory=lambda **kwargs: producer),
                ma["window_count"],
            )
            self.assertEqual(len(producer.messages), ma["window_count"])
            self.assertTrue(producer.flushed)
            self.assertTrue(producer.closed)
            self.assertTrue(
                all(
                    json.loads(value.decode("utf-8"))["record_type"] == "aligned_window"
                    for _, _, value in producer.messages
                )
            )

    def test_default_is_500_windows(self):
        with tempfile.TemporaryDirectory(dir=TEST_TMP) as folder:
            manifest = generate(load_config(ROOT / "config/default.yaml"), Path(folder))
            self.assertEqual(manifest["window_count"], 500)

    def test_rate_features_normalize_counts_and_preserve_raw_counts(self):
        events = {
            "metrics": [],
            "logs": [
                {"record_id": "l1", "level": "ERROR", "trace_id": "trace-a"},
                {"record_id": "l2", "level": "INFO", "trace_id": "trace-b"},
            ],
            "traces": [
                {"record_id": "s1", "trace_id": "trace-a", "status": "ERROR", "duration_ms": 20},
                {"record_id": "s2", "trace_id": "trace-b", "status": "OK", "duration_ms": 10},
            ],
            "alarms": [
                {"alarm_level": "critical"},
                {"alarm_level": "minor"},
            ],
        }
        features, _, _ = calculate(events, 300)

        self.assertEqual(features["logs"]["total_count"], 2)
        self.assertEqual(features["logs"]["total_per_minute"], 0.4)
        self.assertEqual(features["logs"]["error_rate"], 0.5)
        self.assertEqual(features["logs"]["info_rate"], 0.5)
        self.assertEqual(features["logs"]["error_trace_match_ratio"], 1.0)
        self.assertEqual(features["traces"]["span_count"], 2)
        self.assertEqual(features["traces"]["span_per_minute"], 0.4)
        self.assertEqual(features["traces"]["failed_span_per_minute"], 0.2)
        self.assertEqual(features["traces"]["span_error_rate"], 0.5)
        self.assertEqual(features["alarms"]["alarm_count"], 2)
        self.assertEqual(features["alarms"]["alarm_per_minute"], 0.4)
        self.assertEqual(features["alarms"]["critical_rate"], 0.5)
        self.assertEqual(features["alarms"]["minor_rate"], 0.5)


if __name__ == "__main__": unittest.main()
