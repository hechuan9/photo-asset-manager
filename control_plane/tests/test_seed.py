from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from control_plane.seed import seed_sqlite_from_macos_library


def _create_legacy_library(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE operation_ledger (
          op_id TEXT PRIMARY KEY,
          library_id TEXT NOT NULL,
          device_id TEXT NOT NULL,
          device_seq INTEGER NOT NULL,
          hybrid_logical_time TEXT NOT NULL,
          actor_id TEXT NOT NULL,
          entity_type TEXT NOT NULL,
          entity_id TEXT NOT NULL,
          op_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          base_version TEXT,
          created_at TEXT NOT NULL,
          upload_status TEXT NOT NULL,
          remote_cursor TEXT,
          global_seq INTEGER
        );
        CREATE TABLE sync_cursors (
          peer_id TEXT PRIMARY KEY,
          cursor TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        INSERT INTO operation_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "00000000-0000-0000-0000-000000000001",
            "local-library",
            "mac",
            1,
            "00000001776783945172:00000000000000000001:mac",
            "hechuan",
            "derivative_object",
            "asset-a:thumbnail:hash-a",
            "derivative_declared",
            json.dumps(
                {
                    "derivativeDeclared": {
                        "assetID": "00000000-0000-0000-0000-0000000000a1",
                        "derivative": {
                            "assetID": "00000000-0000-0000-0000-0000000000a1",
                            "role": "thumbnail",
                            "fileObject": {"contentHash": "hash-a", "sizeBytes": 12, "role": "thumbnail"},
                            "s3Object": {"bucket": "legacy-bucket", "key": "legacy/thumb-a"},
                            "pixelSize": {"width": 120, "height": 80},
                        },
                    }
                }
            ),
            None,
            "2026-06-15T00:00:00Z",
            "acknowledged",
            "1",
            1,
        ),
    )
    conn.execute(
        "INSERT INTO sync_cursors VALUES (?, ?, ?)",
        ("control-plane", "1", "2026-06-15T00:00:00Z"),
    )
    conn.commit()
    conn.close()


def test_seed_normalizes_legacy_s3object_and_skips_missing_derivative_projection(tmp_path: Path) -> None:
    source = tmp_path / "Library.sqlite"
    target = tmp_path / "keeps" / "db" / "control_plane.sqlite"
    keeps_root = tmp_path / "keeps"
    _create_legacy_library(source)

    summary = seed_sqlite_from_macos_library(
        source_database=source,
        target_database=target,
        keeps_root=keeps_root,
        include_missing_derivative_projection=False,
    )

    assert summary.ledger_events == 1
    assert summary.derivative_objects == 0
    conn = sqlite3.connect(target)
    payload = json.loads(conn.execute("SELECT payload_json FROM ledger_events").fetchone()[0])
    assert payload["derivativeDeclared"]["derivative"]["objectRef"] == {
        "bucket": "keeps-previews",
        "key": "legacy/thumb-a",
    }
    assert "s3Object" not in payload["derivativeDeclared"]["derivative"]
    assert conn.execute("SELECT next_global_seq FROM ledger_sequence_counters").fetchone()[0] == 2
    assert conn.execute("SELECT count(*) FROM derivative_objects").fetchone()[0] == 0
    conn.close()


def test_seed_includes_derivative_projection_when_file_exists(tmp_path: Path) -> None:
    source = tmp_path / "Library.sqlite"
    target = tmp_path / "keeps" / "db" / "control_plane.sqlite"
    keeps_root = tmp_path / "keeps"
    _create_legacy_library(source)
    preview = keeps_root / "previews" / "legacy" / "thumb-a"
    preview.parent.mkdir(parents=True)
    preview.write_bytes(b"preview")

    summary = seed_sqlite_from_macos_library(
        source_database=source,
        target_database=target,
        keeps_root=keeps_root,
        include_missing_derivative_projection=False,
    )

    assert summary.derivative_objects == 1
    conn = sqlite3.connect(target)
    assert conn.execute("SELECT object_bucket, object_key FROM derivative_objects").fetchone() == (
        "keeps-previews",
        "legacy/thumb-a",
    )
    conn.close()
