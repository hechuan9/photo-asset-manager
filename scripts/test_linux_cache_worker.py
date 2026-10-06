import hashlib
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("worker", Path(__file__).with_name("linux_cache_worker.py"))
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)


class WorkerTests(unittest.TestCase):
    def setUp(self):
        worker.STOP.clear()

    def test_rejects_corrupt_download_and_cleans_scratch(self):
        with tempfile.TemporaryDirectory() as directory:
            instance = worker.Worker.__new__(worker.Worker)
            instance.root = Path(directory)
            instance.reserve = 1024
            instance.request = lambda *args, **kwargs: io.BytesIO(b"wrong bytes")
            calls = []
            instance.json_request = lambda *args: calls.append(args)
            with self.assertRaisesRegex(RuntimeError, "SHA256 mismatch"):
                instance.process({"taskID": "test", "leaseSeconds": 1800, "filename": "photo.raw", "inputHash": hashlib.sha256(b"correct").hexdigest()})
            self.assertEqual(list(instance.root.iterdir()), [])
            self.assertEqual(calls[0][1], "tasks/test/fail")

    def test_refuses_oversize_input_before_render(self):
        with tempfile.TemporaryDirectory() as directory:
            instance = worker.Worker.__new__(worker.Worker)
            instance.root = Path(directory)
            instance.reserve = 8
            instance.request = lambda *args, **kwargs: io.BytesIO(b"12345")
            instance.json_request = lambda *args: {}
            with self.assertRaisesRegex(RuntimeError, "disk allowance"):
                instance.process({"taskID": "test", "leaseSeconds": 1800, "filename": "photo.raw"})
            self.assertEqual(list(instance.root.iterdir()), [])

    def test_video_proxy_forwards_ranges_without_exposing_auth(self):
        instance = worker.Worker.__new__(worker.Worker)
        class Response(io.BytesIO):
            status = 206
            headers = {"Content-Length": "3", "Content-Range": "bytes 7-9/10", "Accept-Ranges": "bytes"}
        calls = []
        def request(*args, **kwargs):
            calls.append((args, kwargs))
            return Response(b"789")
        instance.request = request
        with instance.video_source("tasks/video") as (url, transfer):
            with worker.urllib.request.urlopen(worker.urllib.request.Request(url, headers={"Range": "bytes=7-9"})) as response:
                self.assertEqual(response.status, 206)
                self.assertEqual(response.headers["Content-Range"], "bytes 7-9/10")
                self.assertEqual(response.read(), b"789")
        self.assertEqual(transfer["bytes"], 3)
        self.assertEqual(calls, [(("GET", "tasks/video/source"), {"extra_headers": {"Range": "bytes=7-9"}})])

    def test_no_redirect_with_credentials(self):
        with self.assertRaisesRegex(RuntimeError, "redirects"):
            worker.NoRedirect().redirect_request(None, None, 302, None, {}, "https://other.example")

    def test_lease_conflict_is_not_retried(self):
        instance = worker.Worker.__new__(worker.Worker)
        error = worker.urllib.error.HTTPError("https://nas/task", 409, "expired", {}, None)
        with patch.object(instance, "request", side_effect=error) as request:
            with self.assertRaises(worker.urllib.error.HTTPError):
                instance.json_request("POST", "tasks/test/complete", {})
            self.assertEqual(request.call_count, 1)


if __name__ == "__main__":
    unittest.main()
