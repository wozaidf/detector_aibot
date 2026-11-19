import json
import unittest
from unittest.mock import patch

from dashboard import server


class ReplayProxyTests(unittest.TestCase):
    def test_dashboard_forwards_replay_request_to_l1_service(self):
        class Response:
            status = 202

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self):
                return json.dumps({"started": True, "total": 500}).encode()

        payload = {"action": "replay", "speed": 600}
        with patch.object(server, "L1_REPLAY_URL", "http://l1-replay:8091"), \
                patch.object(server.urllib.request, "urlopen", return_value=Response()) as open_url:
            status, body = server.start_replay(payload)

        self.assertEqual(status, 202)
        self.assertEqual(body["total"], 500)
        request = open_url.call_args.args[0]
        self.assertEqual(request.full_url, "http://l1-replay:8091/replay")
        self.assertEqual(json.loads(request.data), payload)


if __name__ == "__main__":
    unittest.main()
