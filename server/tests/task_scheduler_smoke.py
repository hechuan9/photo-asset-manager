"""Real-media scheduler/restart integration; disposable Docker volumes and optional temporary host state.

KEEPS_TEST_IMAGE=keeps-server:task-scheduler python3 server/tests/task_scheduler_smoke.py
Optional KEEPS_TEST_SERVER overrides the server binary with a read-only bind.
Optional KEEPS_TEST_STATE_ROOT stores temporary state under that host directory.
The image must already exist. This script never builds it or contacts the NAS.
"""
import json
import os
import secrets
import sqlite3
import subprocess
import tempfile
from pathlib import Path
import time
import urllib.request
import uuid


DOCKER = os.environ.get("DOCKER_BIN", "docker")


def docker(*args):
    result = subprocess.run([DOCKER, *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(f"docker {args[0]} failed ({result.returncode}): {result.stdout}{result.stderr}")
    return result.stdout.strip()


def wait(check, label, seconds=90):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                print("PASS:", label, flush=True)
                return value
        except (OSError, ValueError) as error:
            last = repr(error)
        time.sleep(0.25)
    raise AssertionError(f"{label}: timed out; last error={last}")


WRAPPER = r'''#!/usr/bin/perl
use strict;
use warnings;
use IO::Handle;
if (grep { $_ eq '-j' } @ARGV) {
    my $photo = $ARGV[-1];
    open(my $log, '>>', '/control/inspections.log') or die $!;
    print $log "$photo\n";
    $log->flush();
    close($log);
    if ($photo eq '/originals/a.jpg') {
        while (-e '/control/block-a') { select(undef, undef, undef, 0.05); }
    }
}
exec '/usr/bin/exiftool', @ARGV;
'''


def main():
    image = os.environ.get("KEEPS_TEST_IMAGE", "keeps-server:task-scheduler")
    docker("image", "inspect", image)
    name = "keeps-scheduler-" + uuid.uuid4().hex[:10]
    writer = name + "-writer"
    volumes = [name + suffix for suffix in ("-originals", "-state", "-control")]
    created = []
    containers = []
    token = secrets.token_hex(32)
    state_directory = None
    binary_mount = []
    if binary := os.environ.get("KEEPS_TEST_SERVER"):
        binary = Path(binary).resolve(strict=True)
        if not binary.is_file():
            raise ValueError("KEEPS_TEST_SERVER must be a binary file")
        binary_mount = ["--mount", f"type=bind,src={binary},dst=/usr/local/bin/keeps-server,readonly"]
    try:
        if state_root := os.environ.get("KEEPS_TEST_STATE_ROOT"):
            state_directory = tempfile.TemporaryDirectory(prefix="keeps-scheduler-", dir=Path(state_root).resolve(strict=True))
        for volume in volumes:
            if state_directory is not None and volume == volumes[1]:
                continue
            docker("volume", "create", volume)
            created.append(volume)
        mounts = []
        for volume, path in zip(volumes, ("/originals", "/keeps", "/control")):
            mount = (f"type=bind,src={state_directory.name},dst=/keeps"
                     if state_directory is not None and path == "/keeps"
                     else f"type=volume,src={volume},dst={path}")
            mounts.extend(["--mount", mount])
        docker("run", "-d", "--name", writer, *mounts, "--entrypoint", "sleep", image, "infinity")
        containers.append(writer)

        subprocess.run([DOCKER, "exec", "-i", writer, "sh", "-c",
                        "cat > /control/exiftool && chmod 755 /control/exiftool && touch /control/block-a"],
                       input=WRAPPER, text=True, check=True)
        docker("exec", writer, "mkdir", "/fixtures")
        for filename, color in (("a.jpg", "red"), ("b.jpg", "blue")):
            docker("exec", writer, "convert", "-size", "1600x800", f"gradient:{color}-black", f"/fixtures/{filename}")
            docker("exec", writer, "/usr/bin/exiftool", "-overwrite_original", "-Make=Test", "-Model=Camera",
                   "-DateTimeOriginal=2024:01:02 12:00:00", f"-XMP-xmpMM:OriginalDocumentID=xmp.did:{uuid.uuid4()}", f"/fixtures/{filename}")
        docker("exec", writer, "cp", "/fixtures/a.jpg", "/originals/a.jpg")
        docker("exec", writer, "touch", "-d", "2024-01-02 12:00:00 UTC", "/originals/a.jpg")
        def hashes(directory):
            lines = docker("exec", writer, "sha256sum", directory + "/a.jpg", directory + "/b.jpg").splitlines()
            return {Path(line.split()[1]).name: line.split()[0] for line in lines}
        original_hashes = hashes("/fixtures")
        docker("run", "-d", "--name", name, "-p", "127.0.0.1::2283", *mounts, *binary_mount,
               "-e", f"KEEPS_ACCESS_TOKEN={token}", "-e", "KEEPS_ROOT=/keeps",
               "-e", "ORIGINAL_ROOT=/originals", "-e", "KEEPS_LIBRARY_ID=test",
               "-e", "CONTROL_PLANE_AUTO_CREATE_SCHEMA=1", "-e", "CONTROL_PLANE_PUBLIC_BASE_URL=http://localhost:2283",
               "--entrypoint", "sh", image, "-c", "cp /control/exiftool /usr/local/bin/exiftool && exec keeps-server")
        containers.append(name)
        address = "http://" + docker("port", name, "2283/tcp")

        def request(path, body=None):
            req = urllib.request.Request(address + path, headers={"Authorization": "Bearer " + token,
                                         "Content-Type": "application/json"},
                                         data=None if body is None else json.dumps(body).encode())
            with urllib.request.urlopen(req, timeout=5) as response:
                return json.load(response)

        def query(sql, database="jobs"):
            with tempfile.TemporaryDirectory(prefix="keeps-query-") as scratch:
                if state_directory is not None:
                    path = Path(state_directory.name) / "db" / (database + ".sqlite")
                else:
                    docker("cp", name + ":/keeps/db/.", scratch)
                    path = Path(scratch) / (database + ".sqlite")
                with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
                    db.row_factory = sqlite3.Row
                    return [dict(row) for row in db.execute(sql)]

        def inspections():
            lines = docker("exec", writer, "sh", "-c", "cat /control/inspections.log 2>/dev/null || true").splitlines()
            return [{"photo": line} for line in lines]

        def a_reads():
            return sum(item["photo"] == "/originals/a.jpg" for item in inspections())

        def tasks():
            result = request("/libraries/test/task-status")
            assert set(result) == {"automatic", "longTask"}, result
            assert isinstance(result["automatic"]["remainingPhotos"], int), result
            assert isinstance(result["longTask"], dict), result
            assert not any(isinstance(v, list) for v in result.values()), result
            return result

        def ready(filename):
            items = request("/libraries/test/assets")["items"]
            asset = next((a for a in items if a["originalFilename"] == filename and a.get("thumbnail")), None)
            if not asset:
                return None
            return asset if query("SELECT status FROM media_cache WHERE asset_id='" + asset["id"] + "' AND status='ready'", "control_plane") else None

        wait(lambda: request("/healthz"), "server healthy")
        wait(lambda: a_reads() == 1, "startup reconciliation paused inside first photo")
        activity = query("SELECT * FROM worker_activity")[0]
        assert activity["work_class"] == "reconcile" and activity["current_photo"] == "/originals/a.jpg", activity
        initial = query("SELECT * FROM jobs WHERE work_class='reconcile' AND status='running'")[0]
        checkpoint = json.loads(initial["checkpoint"])
        assert checkpoint["last_name"] is None and initial["processed"] == 0, initial
        folder = request("/libraries/test/folders")["folders"][0]
        manual = request(f"/libraries/test/folders/{folder['id']}/scan", {})
        assert manual["workClass"] == "manual" and manual["id"] != initial["id"], manual
        docker("exec", writer, "cp", "/fixtures/b.jpg", "/originals/b.jpg")
        wait(lambda: query("SELECT id FROM jobs WHERE work_class='automatic' AND scope_path='/originals/b.jpg' AND status='pending' AND available_at<=unixepoch()"), "new photo receives independent automatic job")
        before = tasks()
        assert before["automatic"]["remainingPhotos"] >= 1, before
        assert before["longTask"]["status"] == "running", before
        assert a_reads() == 1, inspections()
        print("PASS: manual, automatic and full-library jobs remain independent; exactly two status sections", flush=True)

        docker("kill", "--signal=KILL", name)
        interrupted = query("SELECT * FROM jobs WHERE id='" + initial["id"] + "'")[0]
        assert interrupted["checkpoint"] == initial["checkpoint"], interrupted
        assert interrupted["processed"] == 0, interrupted
        assert query("SELECT current_photo FROM worker_activity")[0]["current_photo"] == "/originals/a.jpg"
        docker("start", name)
        address = "http://" + docker("port", name, "2283/tcp")
        wait(lambda: request("/healthz"), "restart healthy")
        wait(lambda: ready("b.jpg"), "automatic photo and thumbnail finish while full-library work is incomplete")
        wait(lambda: a_reads() >= 2, "interrupted photo restarts metadata processing from beginning")
        reconcile = query("SELECT status,checkpoint,processed FROM jobs WHERE id='" + initial["id"] + "'")[0]
        assert reconcile["status"] in ("pending", "running"), reconcile
        assert reconcile["checkpoint"] == initial["checkpoint"] and reconcile["processed"] == 0, reconcile
        tasks()
        docker("exec", writer, "rm", "/control/block-a")
        wait(lambda: ready("a.jpg"), "replayed photo reaches ready thumbnail")
        wait(lambda: not query("SELECT id FROM jobs WHERE status IN ('pending','running')"), "all bounded jobs finish")
        final = tasks()
        assert final["automatic"]["remainingPhotos"] == 0 and final["automatic"]["status"] == "idle", final
        errors = query("SELECT id,error FROM jobs WHERE status='failed'")
        assert not errors, errors
        assert hashes("/originals") == original_hashes, "Original bytes changed"
        print("PASS: SIGKILL replay, checkpoint boundary, actual JPEG thumbnails, and unchanged original bytes", flush=True)
    except BaseException:
        for container in containers:
            print(f"--- {container} logs ---", flush=True)
            subprocess.run([DOCKER, "logs", container], check=False)
        raise
    finally:
        for container in reversed(containers):
            subprocess.run([DOCKER, "rm", "-f", container], check=False, stdout=subprocess.DEVNULL)
        for volume in reversed(created):
            subprocess.run([DOCKER, "volume", "rm", volume], check=False, stdout=subprocess.DEVNULL)
        if state_directory is not None:
            state_directory.cleanup()


if __name__ == "__main__":
    main()
