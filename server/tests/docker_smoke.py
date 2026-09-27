"""Exercise NAS catalog migration and background ingest using disposable originals."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib


def docker(*args):
    return subprocess.check_output([os.environ.get("DOCKER_BIN", "docker"), *args], text=True).strip()


@contextmanager
def test_directory():
    base = os.environ.get("KEEPS_TEST_ROOT")
    if base:
        base = Path(base).expanduser().resolve()
        base.mkdir(parents=True, exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="keeps-nas-validation-", dir=str(base)))
        print("ARTIFACTS: " + str(root), flush=True)
        yield root
    else:
        with tempfile.TemporaryDirectory(prefix=".keeps-rust-smoke-", dir=str(Path.home())) as temp:
            yield Path(temp)


def fixture_png(red=255):
    def chunk(kind, data):
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 16, 8, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress((b"\0" + bytes([red, 32, 64]) * 16) * 8)) + chunk(b"IEND", b""))


def legacy_database(path):
    asset = "00000000-0000-0000-0000-00000000a001"
    snapshot = {"assetID": asset, "captureTime": "2024-01-01T00:00:00.000Z", "cameraMake": "Canon",
                "cameraModel": "R3", "lensModel": "50mm", "originalFilename": "legacy.jpg",
                "contentFingerprint": "legacy-hash", "metadataFingerprint": "legacy-fingerprint",
                "rating": 1, "flagState": "unflagged", "tags": [],
                "createdAt": "2024-01-01T00:00:00.000Z", "updatedAt": "2024-01-01T00:00:00.000Z"}
    payload = json.dumps({"assetSnapshotDeclared": {"snapshot": snapshot}}, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(path) as db:
        db.executescript((Path(__file__).parent / "fixtures/legacy_schema.sql").read_text())
        db.execute("INSERT INTO ledger_sequence_counters VALUES ('smoke',2)")
        db.execute("""INSERT INTO ledger_events(library_id,global_seq,op_id,device_id,device_seq,
                      hybrid_logical_time,actor_id,entity_type,entity_id,op_type,payload_json,payload_hash,
                      base_version,committed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                   ("smoke", 1, str(uuid.uuid4()), "legacy-mac", 1,
                    json.dumps({"wallTimeMilliseconds": 1704067200000, "counter": 0, "nodeID": "legacy-mac"}),
                    "user", "asset", asset, "asset_snapshot_declared", payload,
                    hashlib.sha256(payload.encode()).hexdigest(), None, "2024-01-01 00:00:00.000"))
        assert db.execute("PRAGMA user_version").fetchone()[0] == 0
    return asset, payload


def main():
    image = os.environ.get("KEEPS_TEST_IMAGE", "keeps-server:rust")
    container = f"keeps-rust-smoke-{uuid.uuid4().hex[:10]}"
    os.environ["KEEPS_ACCESS_TOKEN"] = secrets.token_hex(32)
    with test_directory() as root:
        keeps, originals = root / "keeps", root / "originals"
        (keeps / "db").mkdir(parents=True)
        originals.mkdir()
        sentinel = originals / "sentinel.txt"
        sentinel.write_text("original must remain unchanged")
        photo = originals / "new.png"
        photo.write_bytes(fixture_png())
        original_hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in originals.iterdir()}
        database = keeps / "db/control_plane.sqlite"
        legacy_id, legacy_payload = legacy_database(database)
        mount = f"type=bind,src={keeps},dst=/myphoto/keeps"
        started = False
        evidence = {"image": image, "container": container, "checks": [], "status": "running"}

        def record(name, detail=None):
            evidence["checks"].append({"name": name, "detail": detail})
            print("PASS: " + name, flush=True)
            (root / "validation.json").write_text(json.dumps(evidence, indent=2, ensure_ascii=False))

        def event_count():
            with sqlite3.connect(str(database)) as db:
                return db.execute("SELECT count(*) FROM ledger_events").fetchone()[0]

        def add_original(relative, content):
            path = originals / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as handle:
                handle.write(content)
            original_hashes[relative] = hashlib.sha256(content).hexdigest()

        try:
            docker("run", "--rm", "-e", "KEEPS_ROOT=/myphoto/keeps", "--mount", mount, image, "migrate")
            with sqlite3.connect(database) as db:
                assert db.execute("PRAGMA user_version").fetchone()[0] == 1
                assert db.execute("SELECT payload_json FROM ledger_events WHERE global_seq=1").fetchone()[0] == legacy_payload
                assert db.execute("SELECT count(*) FROM catalog_assets").fetchone()[0] == 1
            record("legacy migration preserves event payload")
            docker("run", "-d", "--name", container, "-p", "127.0.0.1::2283",
                   "-e", "KEEPS_ACCESS_TOKEN", "-e", "KEEPS_ROOT=/myphoto/keeps",
                   "-e", "ORIGINAL_ROOT=/myphoto/library", "-e", "CONTROL_PLANE_AUTO_CREATE_SCHEMA=0",
                   "-e", "CONTROL_PLANE_PUBLIC_BASE_URL=http://localhost:2283",
                   "-e", "TZ=America/New_York", "-e", "KEEPS_SCAN_INTERVAL_SECONDS=3600",
                   "--mount", mount,
                   "--mount", f"type=bind,src={originals},dst=/myphoto/library,readonly", image)
            started = True
            address = "http://" + docker("port", container, "2283/tcp")

            def request(path, body=None, auth=True, method=None):
                headers = {"Content-Type": "application/json"}
                if auth:
                    headers["Authorization"] = "Bearer " + os.environ["KEEPS_ACCESS_TOKEN"]
                req = urllib.request.Request(address + path,
                    data=None if body is None else body if isinstance(body, bytes) else json.dumps(body).encode(), headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=10) as response:
                    data = response.read()
                    return json.loads(data) if data else None

            def reject(path, status, body=None, method=None, auth=True):
                try:
                    request(path, body, auth=auth, method=method)
                except urllib.error.HTTPError as error:
                    assert error.code == status, (path, error.code, error.read().decode())
                else:
                    raise AssertionError("invalid request succeeded: " + path)

            def wait_job(job_id=None, folder_id=None, expected="completed"):
                for _ in range(900):
                    jobs = request("/libraries/smoke/jobs")["jobs"]
                    matches = [j for j in jobs if (job_id is None or j["id"] == job_id)
                               and (folder_id is None or j["folderID"] == folder_id)]
                    if matches and matches[0]["status"] in ("completed", "failed", "cancelled"):
                        assert matches[0]["status"] == expected, matches[0]
                        return matches[0]
                    time.sleep(0.2)
                raise AssertionError("NAS scan timed out: " + docker("logs", container))

            def ready():
                for _ in range(150):
                    try:
                        assert request("/healthz", auth=False) == {"status": "ok"}
                        return
                    except (OSError, urllib.error.URLError):
                        time.sleep(0.2)
                raise RuntimeError("container did not become ready: " + docker("logs", container))

            ready()
            reject("/libraries/smoke/assets", 401, auth=False)
            record("missing bearer credential rejected")
            assert request("/libraries/smoke/assets")["items"][0]["id"] == legacy_id
            legacy_path = f"/libraries/smoke/assets/{legacy_id}"
            patch = {"rating": 4, "flagState": "picked", "tags": ["smoke"]}
            assert request(legacy_path, patch, method="PATCH")["rating"] == 4
            before_repeat = event_count()
            assert request(legacy_path, patch, method="PATCH")["rating"] == 4
            assert event_count() == before_repeat
            record("repeated metadata command creates no events", {"eventCount": before_repeat})
            reject(legacy_path, 422, {"rating": 6}, "PATCH")
            reject(legacy_path, 422, {"originalFilename": "changed.jpg"}, "PATCH")
            reject(legacy_path, 400, b"{broken", "PATCH")
            reject("/libraries/smoke/assets?cursor=invalid", 422)
            reject("/libraries/smoke/folders", 422, {"path": ".."})
            reject("/libraries/smoke/folders", 422, {"path": "/etc"})
            reject("/libraries/smoke/folders", 422, {"path": "missing-directory"})
            record("malformed commands and out-of-root folders rejected")
            assert request(legacy_path + "/trash", method="POST")["trashed"] is True
            assert request(legacy_path + "/restore", method="POST")["trashed"] is False
            folder = request("/libraries/smoke/folders", {"path": "."})
            assert folder["libraryID"] == "smoke"
            initial_job = wait_job(folder_id=folder["id"])
            record("background ingest completed", initial_job)
            page = request("/libraries/smoke/assets")
            assert page["total"] == 2, page
            scanned = next(a for a in page["items"] if a["originalFilename"] == "new.png")
            assert scanned["preview"]["width"] <= 1200 and scanned["preview"]["height"] <= 1200
            preview_path = urllib.parse.urlsplit(scanned["preview"]["downloadURL"]).path
            with urllib.request.urlopen(address + preview_path, timeout=10) as response:
                preview_bytes = response.read()
            assert len(preview_bytes) > 0
            assert hashlib.sha256(preview_bytes).hexdigest() == scanned["preview"]["version"]
            reject(preview_path + "tampered", 400, auth=False)
            record("preview content hash and signed URL tamper rejection")
            before_rescan = event_count()
            repeat_job = request(f"/libraries/smoke/folders/{folder['id']}/scan", method="POST")
            repeat_job = wait_job(job_id=repeat_job["id"])
            assert repeat_job["skipped"] == 1 and repeat_job["processed"] == 0, repeat_job
            assert request("/libraries/smoke/assets")["total"] == 2
            assert event_count() == before_rescan
            record("unchanged rescan skips photo without duplicate asset or event", repeat_job)
            request(f"/libraries/smoke/folders/{folder['id']}", method="DELETE")
            assert request("/libraries/smoke/folders")["folders"] == []
            reject("/libraries/smoke/ops", 404, {"operations": []})
            record("tracking removal retains catalog and retired client writes reject")

            add_original("mixed/a-broken.jpg", b"intentionally invalid JPEG fixture")
            add_original("mixed/after-error/z-valid.png", fixture_png(64))
            mixed = request("/libraries/smoke/folders", {"path": "mixed"})
            failed = wait_job(folder_id=mixed["id"], expected="failed")
            assert failed["processed"] == 1 and failed["failed"] == 1, failed
            assert "a-broken.jpg" in failed["error"], failed
            assert "stderr:" in failed["error"], failed
            assert request("/libraries/smoke/assets")["total"] == 4
            bad_assets = request("/libraries/smoke/assets?q=a-broken.jpg")
            assert bad_assets["total"] == 1, bad_assets
            bad_asset_id = bad_assets["items"][0]["id"]
            assert bad_assets["items"][0]["preview"] is None, bad_assets
            valid_assets = request("/libraries/smoke/assets?q=z-valid.png")
            assert valid_assets["total"] == 1 and valid_assets["items"][0]["preview"], valid_assets
            record("bad JPEG fails visibly while valid PNG is ingested", failed)
            retried = request(f"/libraries/smoke/jobs/{failed['id']}/retry", method="POST")
            assert retried["id"] == failed["id"]
            retried = wait_job(job_id=retried["id"], expected="failed")
            assert retried["failed"] == 1 and retried["skipped"] == 1, retried
            assert request("/libraries/smoke/assets")["total"] == 4
            bad_assets = request("/libraries/smoke/assets?q=a-broken.jpg")
            assert bad_assets["total"] == 1, bad_assets
            assert bad_assets["items"][0]["id"] == bad_asset_id, bad_assets
            assert bad_assets["items"][0]["preview"] is None, bad_assets
            assert (originals / "mixed/a-broken.jpg").is_file()
            request(f"/libraries/smoke/folders/{mixed['id']}", method="DELETE")
            record("failed job retries while preserving bad source and skipping completed work", retried)

            resume_count = 12
            for index in range(resume_count):
                add_original("resume/{:02d}.png".format(index), fixture_png(100 + index))
            resume_folder = request("/libraries/smoke/folders", {"path": "resume"})
            checkpoint = None
            for _ in range(600):
                matches = [j for j in request("/libraries/smoke/jobs")["jobs"]
                           if j["folderID"] == resume_folder["id"]]
                if matches and matches[0]["status"] == "running" and 0 < matches[0]["processed"] < resume_count:
                    checkpoint = matches[0]
                    break
                if matches and matches[0]["status"] in ("completed", "failed"):
                    raise AssertionError("resume fixture did not reach an interruptible checkpoint: " + str(matches[0]))
                time.sleep(0.05)
            assert checkpoint is not None, "no running checkpoint reached"
            docker("kill", "--signal=KILL", container)
            with sqlite3.connect(str(keeps / "db/jobs.sqlite")) as db:
                row = db.execute("SELECT status,processed FROM jobs WHERE id=?", (checkpoint["id"],)).fetchone()
                assert row[0] == "running" and row[1] >= 1, row
            docker("start", container)
            address = "http://" + docker("port", container, "2283/tcp")
            ready()
            resumed = wait_job(job_id=checkpoint["id"])
            assert resumed["skipped"] >= 1, resumed
            assert resumed["processed"] + resumed["skipped"] == resume_count, resumed
            assert request("/libraries/smoke/assets")["total"] == 4 + resume_count
            request(f"/libraries/smoke/folders/{resume_folder['id']}", method="DELETE")
            record("interrupted running job resumes after SIGKILL without reprocessing completed files",
                   {"checkpoint": checkpoint, "resumed": resumed})
            docker("restart", container)
            address = "http://" + docker("port", container, "2283/tcp")
            ready()
            assert request(legacy_path)["rating"] == 4
            assert request(legacy_path)["tags"] == ["smoke"]
            assert request("/libraries/smoke/assets")["total"] == 4 + resume_count
            assert request("/libraries/smoke/folders")["folders"] == []
            mounts = json.loads(docker("inspect", container))[0]["Mounts"]
            assert next(m for m in mounts if m["Destination"] == "/myphoto/library")["RW"] is False
            assert {str(p.relative_to(originals)): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in originals.rglob("*") if p.is_file()} == original_hashes
            with sqlite3.connect(database) as db:
                assert db.execute("SELECT payload_json FROM ledger_events WHERE global_seq=1").fetchone()[0] == legacy_payload
            record("restart persists catalog and inactive folders; every original hash remains unchanged",
                   {"assets": 4 + resume_count, "originalFiles": len(original_hashes)})
            evidence["status"] = "passed"
            print("PASS: all isolated NAS service checks", flush=True)
        except Exception as error:
            evidence["status"] = "failed"
            evidence["error"] = repr(error)
            raise
        finally:
            if started:
                (root / "container.log").write_text(docker("logs", container))
                inspection = json.loads(docker("inspect", container))[0]
                evidence["imageID"] = inspection["Image"]
                evidence["mounts"] = inspection["Mounts"]
                if os.environ.get("KEEPS_TEST_ROOT"):
                    docker("stop", container)
                    evidence["retainedContainer"] = container
                else:
                    docker("rm", "-f", container)
            (root / "original-hashes.json").write_text(json.dumps(original_hashes, indent=2, ensure_ascii=False))
            (root / "validation.json").write_text(json.dumps(evidence, indent=2, ensure_ascii=False))
            os.environ.pop("KEEPS_ACCESS_TOKEN", None)


if __name__ == "__main__":
    main()
