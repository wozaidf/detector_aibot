import tempfile
import unittest
from pathlib import Path

from l1_stream_simulator.replay import replay

TEST_TMP = Path(__file__).parents[1] / ".test_tmp"
TEST_TMP.mkdir(exist_ok=True)


class Future:
    def __init__(self):
        self.success = None
        self.failure = None

    def add_callback(self, callback):
        self.success = callback

    def add_errback(self, callback):
        self.failure = callback

    def complete(self):
        self.success(object())


class Producer:
    def __init__(self, **_):
        self.futures = []
        self.closed = False

    def send(self, *_args, **_kwargs):
        future = Future()
        self.futures.append(future)
        return future

    def flush(self):
        for future in self.futures:
            future.complete()

    def close(self):
        self.closed = True


class ReplayDeliveryTests(unittest.TestCase):
    def test_reports_submitted_and_broker_confirmed_separately(self):
        with tempfile.TemporaryDirectory(dir=TEST_TMP) as folder:
            path = Path(folder, "windows.jsonl")
            path.write_text('{"entity_id":"device-1","window":{"end":"2026-01-01T00:00:00Z"}}\n', encoding="utf-8")
            producer = Producer()
            reports = []
            confirmed = replay(path, speed=600, sleep=lambda _: None,
                               producer_factory=lambda **kwargs: producer,
                               progress_callback=reports.append)

        self.assertEqual(confirmed, 1)
        self.assertTrue(producer.closed)
        self.assertTrue(any(item["sent"] == 1 and item["confirmed"] == 0 for item in reports))
        self.assertEqual(reports[-1]["confirmed"], 1)


if __name__ == "__main__":
    unittest.main()
