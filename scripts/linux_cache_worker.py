#!/usr/bin/env python3
"""Bounded remote cache worker; NAS remains the sole state/file publisher."""
import concurrent.futures
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

LOG = logging.getLogger("keeps-worker")
STOP = threading.Event()


def tree_bytes(root):
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            pass
    return total


class Worker:
    def __init__(self):
        self.base = os.environ["KEEPS_WORKER_URL"].rstrip("/")
        self.library = urllib.parse.quote(os.environ["KEEPS_LIBRARY_ID"], safe="")
        token_file = os.environ.get("KEEPS_TOKEN_FILE")
        self.token = Path(token_file).read_text().strip() if token_file else os.environ["KEEPS_API_TOKEN"]
        self.root = Path(os.environ.get("KEEPS_WORKER_TMP", "/work"))
        self.root.mkdir(parents=True, exist_ok=True)
        self.concurrency = int(os.environ.get("KEEPS_WORKER_CONCURRENCY", "4"))
        self.budget = int(os.environ.get("KEEPS_WORKER_MAX_BYTES", str(32 * 1024**3)))
        self.reserve = int(os.environ.get("KEEPS_WORKER_TASK_BYTES", str(8 * 1024**3)))
        if not 1 <= self.concurrency <= 32 or self.reserve <= 0 or self.budget < self.concurrency * self.reserve:
            raise ValueError("invalid concurrency or disk budget")
        self.opener = urllib.request.build_opener(NoRedirect())
        # Refuse expansion over crash leftovers; never delete arbitrary scratch contents.
        if tree_bytes(self.root) + self.concurrency * self.reserve > self.budget:
            raise RuntimeError("scratch contains crash leftovers exceeding disk budget; inspect while worker is stopped")

    def request(self, method, path, body=None, raw=False):
        headers = {"Authorization": "Bearer " + self.token}
        if body is not None:
            headers["Content-Type"] = "image/heic" if raw else "application/json"
            if not raw:
                body = json.dumps(body).encode()
        request = urllib.request.Request(self.base + "/libraries/" + self.library + "/worker/" + path, data=body, headers=headers, method=method)
        return self.opener.open(request, timeout=120)

    def json_request(self, method, path, body):
        for attempt in range(3):
            try:
                with self.request(method, path, body) as response:
                    return json.load(response)
            except urllib.error.HTTPError as error:
                if error.code < 500 or attempt == 2:
                    raise
            except (OSError, TimeoutError):
                if attempt == 2:
                    raise
            STOP.wait(2 ** attempt)

    def run_one(self, slot):
        worker_id = os.environ.get("KEEPS_WORKER_ID", "linux") + "-" + str(slot)
        while not STOP.is_set():
            if shutil.disk_usage(self.root).free < self.budget + 2 * 1024**3:
                LOG.warning("Waiting for local free space")
                STOP.wait(60)
                continue
            try:
                # Claim is intentionally not retried: a lost response may already own a lease.
                with self.request("POST", "claim", {"workerID": worker_id}) as response:
                    task = json.load(response).get("task")
                if task is None:
                    STOP.wait(15)
                    continue
                self.process(task)
            except Exception:
                LOG.exception("Worker slot %s failed", slot)
                STOP.wait(15)

    def process(self, task):
        started = time.monotonic()
        task_id = task["taskID"]
        prefix = "tasks/" + urllib.parse.quote(task_id, safe="")
        done = threading.Event()
        lease_lost = threading.Event()

        def renew():
            interval = min(60, max(5, int(task["leaseSeconds"]) // 3))
            while not done.wait(interval):
                try:
                    self.json_request("POST", prefix + "/heartbeat", {})
                except Exception:
                    LOG.exception("Lease renewal failed for %s", task_id)
                    lease_lost.set()
                    return

        heartbeat = threading.Thread(target=renew, daemon=True)
        heartbeat.start()
        try:
            with tempfile.TemporaryDirectory(prefix="task-", dir=self.root) as temp:
                directory = Path(temp)
                source = directory / ("source" + Path(task["filename"]).suffix.lower())
                digest = hashlib.sha256()
                count = 0
                with self.request("GET", prefix + "/source") as response, source.open("xb") as output:
                    while chunk := response.read(1024 * 1024):
                        count += len(chunk)
                        if count > self.reserve // 2:
                            raise RuntimeError("source exceeds per-task disk allowance")
                        digest.update(chunk)
                        output.write(chunk)
                if digest.hexdigest() != task["inputHash"]:
                    raise RuntimeError("download SHA256 mismatch")
                env = dict(os.environ, TMPDIR=temp)
                command = ["keeps-render", str(source), temp, str(bool(task["generateStandard"])).lower(), str(task["thumbnailEdge"]), str(task["thumbnailQuality"])]
                process = subprocess.Popen(command, env=env, start_new_session=True)
                deadline = time.monotonic() + 1500
                try:
                    while process.poll() is None:
                        used = tree_bytes(directory)
                        if lease_lost.is_set() or used > self.reserve or time.monotonic() > deadline:
                            raise RuntimeError("render stopped: lease, disk budget or deadline")
                        time.sleep(1)
                    if process.returncode:
                        raise RuntimeError("renderer exit " + str(process.returncode))
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                for role in (["standard", "thumbnail"] if task["generateStandard"] else ["thumbnail"]):
                    if lease_lost.is_set():
                        raise RuntimeError("lease lost before upload")
                    path = directory / (role + ".heic")
                    if path.stat().st_size > 256 * 1024**2:
                        raise RuntimeError("result exceeds upload limit")
                    for attempt in range(3):
                        try:
                            with path.open("rb") as data:
                                # urllib streams file bodies; explicit size avoids chunked uploads.
                                request = urllib.request.Request(self.base + "/libraries/" + self.library + "/worker/" + prefix + "/" + role, data=data, method="PUT", headers={"Authorization": "Bearer " + self.token, "Content-Type": "image/heic", "Content-Length": str(path.stat().st_size)})
                                with self.opener.open(request, timeout=120) as response:
                                    response.read()
                            break
                        except urllib.error.HTTPError as error:
                            if error.code < 500 or attempt == 2:
                                raise
                        except OSError:
                            if attempt == 2:
                                raise
                        time.sleep(2 ** attempt)
                self.json_request("POST", prefix + "/complete", {})
                output_bytes = sum((directory / (role + ".heic")).stat().st_size for role in (["standard", "thumbnail"] if task["generateStandard"] else ["thumbnail"]))
                LOG.info("Completed task %s elapsedSeconds=%.2f sourceBytes=%s outputBytes=%s", task_id, time.monotonic() - started, count, output_bytes)
        except Exception:
            LOG.exception("Task %s failed", task_id)
            try:
                self.json_request("POST", prefix + "/fail", {"error": "Linux worker failed; see worker logs for full traceback"})
            except Exception:
                LOG.exception("Failure report failed for %s", task_id)
            raise
        finally:
            done.set()
            heartbeat.join(timeout=125)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RuntimeError("worker API redirects are not permitted")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: STOP.set())
    worker = Worker()
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker.concurrency) as pool:
        list(pool.map(worker.run_one, range(worker.concurrency)))


if __name__ == "__main__":
    main()
