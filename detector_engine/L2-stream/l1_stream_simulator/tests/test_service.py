import json
import io
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from l1_stream_simulator import service

TEST_TMP = Path(__file__).parents[1] / ".test_tmp"
TEST_TMP.mkdir(exist_ok=True)


class ReplayServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=TEST_TMP)
        self.file = Path(self.tmp.name, "windows.jsonl")
        self.file.write_text('{"window_id":"w1"}\n', encoding="utf-8")
        self.old_file = service.REPLAY_FILE
        service.REPLAY_FILE = self.file
        service.STATE.update(status="idle", sent=0, confirmed=0, total=0, speed=None,
                             action=None, message="等待开始", started_at=None, finished_at=None)
    def tearDown(self):
        service.REPLAY_FILE = self.old_file
        service.LOCK.release() if service.LOCK.locked() else None
        self.tmp.cleanup()

    def post(self, data):
        handler = object.__new__(service.Handler)
        handler.path = "/replay"
        handler.headers = {"Content-Length": str(len(json.dumps(data).encode()))}
        handler.rfile = io.BytesIO(json.dumps(data).encode())
        handler._send = lambda status, payload: setattr(handler, "response", (status, payload))
        handler.do_POST()
        return handler.response

    def test_replay_starts_and_updates_confirmed_progress(self):
        completed = threading.Event()

        def fake_replay(path, speed, kafka_bootstrap, topic, progress_callback):
            progress_callback({"sent": 1, "confirmed": 1})
            completed.set()
            return 1

        with patch.object(service, "replay", side_effect=fake_replay):
            status, response = self.post({"action": "replay", "speed": 600})
            self.assertEqual(status, 202)
            self.assertTrue(completed.wait(2))
            for _ in range(100):
                if service.STATE["status"] != "running":
                    break
                threading.Event().wait(0.01)
            state = dict(service.STATE)
            self.assertEqual(state["status"], "completed")
            self.assertEqual(state["sent"], 1)
            self.assertEqual(state["confirmed"], 1)

    def test_running_replay_rejects_second_request(self):
        service.LOCK.acquire()
        status, _ = self.post({"speed": 600})
        self.assertEqual(status, 409)

    def test_invalid_speed_and_empty_file_are_rejected(self):
        status, _ = self.post({"speed": 0})
        self.assertEqual(status, 400)
        self.file.write_text("\n", encoding="utf-8")
        status, _ = self.post({"speed": 600})
        self.assertEqual(status, 400)

    def test_missing_file_is_reported(self):
        service.REPLAY_FILE = Path(self.tmp.name, "missing.jsonl")
        status, _ = self.post({"speed": 600})
        self.assertEqual(status, 400)

    def test_kafka_failure_is_exposed_in_replay_status(self):
        completed = threading.Event()

        def failing_replay(*_args, **_kwargs):
            completed.set()
            raise RuntimeError("broker unavailable")

        with patch.object(service, "replay", side_effect=failing_replay):
            status, _ = self.post({"speed": 600})
            self.assertEqual(status, 202)
            self.assertTrue(completed.wait(2))
            for _ in range(100):
                if service.STATE["status"] != "running":
                    break
                threading.Event().wait(0.01)
        self.assertEqual(service.STATE["status"], "error")
        self.assertIn("broker unavailable", service.STATE["message"])


if __name__ == "__main__":
    unittest.main()
