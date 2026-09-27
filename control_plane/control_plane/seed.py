from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .db import create_engine_for_url, initialize_database
from .schemas import OperationPayload


@dataclass(frozen=True)
class SeedSummary:
    ledger_events: int
    derivative_objects: int
    archive_receipts: int
    next_global_seq: int
    sha256: str


def seed_sqlite_from_macos_library(
    *,
    source_database: Path,
    target_database: Path,
    keeps_root: Path,
    include_missing_derivative_projection: bool = False,
) -> SeedSummary:
    if target_database.exists():
        raise FileExistsError(f"refusing to overwrite existing seed database: {target_database}")

    keeps_root.mkdir(parents=True, exist_ok=True)
    for dirname in ("db", "ledger", "previews", "cache", "ingest", "exports", "backups", "logs", "tmp"):
        (keeps_root / dirname).mkdir(parents=True, exist_ok=True)

    temp_database = target_database.with_name(f".{target_database.name}.tmp")
    temp_database.unlink(missing_ok=True)
    existing_derivative_keys = None
    if not include_missing_derivative_projection:
        existing_derivative_keys = _existing_derivative_keys(keeps_root)
    engine = create_engine_for_url(f"sqlite+pysqlite:///{temp_database}")
    initialize_database(engine)
    engine.dispose()

    src = sqlite3.connect(source_database)
    src.row_factory = sqlite3.Row
    dst = sqlite3.connect(temp_database)
    dst.execute("PRAGMA journal_mode=OFF")
    dst.execute("PRAGMA synchronous=OFF")
    dst.execute("PRAGMA temp_store=MEMORY")

    ledger_count = 0
    derivative_count = 0
    archive_count = 0
    max_seq = 0
    ledger_batch: list[tuple] = []
    derivative_batch: list[tuple] = []
    archive_batch: list[tuple] = []

    try:
        for row in src.execute("SELECT * FROM operation_ledger ORDER BY global_seq"):
            global_seq = int(row["global_seq"])
            max_seq = max(max_seq, global_seq)
            payload_json, payload_hash = _normalized_payload(row["payload_json"])
            committed_at = row["created_at"]
            ledger_batch.append(
                (
                    row["library_id"],
                    global_seq,
                    row["op_id"],
                    row["device_id"],
                    int(row["device_seq"]),
                    json.dumps(_parse_hybrid_logical_time(row["hybrid_logical_time"]), separators=(",", ":")),
                    row["actor_id"],
                    row["entity_type"],
                    row["entity_id"],
                    row["op_type"],
                    json.dumps(payload_json, separators=(",", ":")),
                    payload_hash,
                    row["base_version"],
                    committed_at,
                )
            )
            ledger_count += 1

            derivative = _derivative_projection(payload_json)
            if derivative is not None and (include_missing_derivative_projection or derivative["object_key"] in (existing_derivative_keys or set())):
                derivative_batch.append(
                    (
                        row["library_id"],
                        derivative["asset_id"],
                        derivative["role"],
                        json.dumps(derivative["file_object"], separators=(",", ":")),
                        derivative["object_bucket"],
                        derivative["object_key"],
                        derivative["object_etag"],
                        derivative["pixel_width"],
                        derivative["pixel_height"],
                        global_seq,
                        committed_at,
                    )
                )
                derivative_count += 1

            receipt = _archive_projection(payload_json)
            if receipt is not None:
                archive_batch.append(
                    (
                        row["library_id"],
                        receipt["asset_id"],
                        global_seq,
                        json.dumps(receipt["file_object"], separators=(",", ":")),
                        json.dumps(receipt["server_placement"], separators=(",", ":")),
                        committed_at,
                    )
                )
                archive_count += 1

            if ledger_count % 5000 == 0:
                _flush_batches(dst, ledger_batch, derivative_batch, archive_batch)

        _flush_batches(dst, ledger_batch, derivative_batch, archive_batch)
        dst.execute(
            "INSERT OR REPLACE INTO ledger_sequence_counters (library_id, next_global_seq) VALUES (?, ?)",
            ("local-library", max_seq + 1),
        )
        _seed_device_states(src, dst, source_database, max_seq)
        dst.commit()
        dst.execute("PRAGMA optimize")
        dst.execute("VACUUM")
    finally:
        dst.close()
        src.close()

    target_database.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(temp_database), target_database)
    sha = _sha256(target_database)
    target_database.with_suffix(target_database.suffix + ".sha256").write_text(f"{sha}  {target_database}\n")
    return SeedSummary(
        ledger_events=ledger_count,
        derivative_objects=derivative_count,
        archive_receipts=archive_count,
        next_global_seq=max_seq + 1,
        sha256=sha,
    )


def _normalized_payload(raw_payload: str) -> tuple[dict, str]:
    payload_obj = json.loads(raw_payload)
    _normalize_derivative_payload(payload_obj)
    payload = OperationPayload.model_validate(payload_obj)
    canonical = json.dumps(payload.model_dump(mode="json", by_alias=True, exclude_none=True), sort_keys=True, separators=(",", ":"))
    return json.loads(canonical), hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _normalize_derivative_payload(payload_obj: dict) -> None:
    case = payload_obj.get("derivativeDeclared")
    if not isinstance(case, dict):
        return
    derivative = case.get("derivative")
    if not isinstance(derivative, dict):
        return
    object_ref = derivative.get("objectRef")
    s3_object = derivative.pop("s3Object", None)
    if not isinstance(object_ref, dict):
        object_ref = s3_object if isinstance(s3_object, dict) else {}
        derivative["objectRef"] = object_ref
    object_ref["bucket"] = "keeps-previews"


def _parse_hybrid_logical_time(value: str) -> dict:
    wall_time, counter, node_id = value.split(":", 2)
    return {
        "wallTimeMilliseconds": int(wall_time),
        "counter": int(counter),
        "nodeID": node_id,
    }


def _derivative_projection(payload_json: dict) -> dict | None:
    case = payload_json.get("derivativeDeclared")
    if not isinstance(case, dict):
        return None
    derivative = case["derivative"]
    object_ref = derivative["objectRef"]
    pixel_size = derivative["pixelSize"]
    return {
        "asset_id": case["assetID"],
        "role": derivative["role"],
        "file_object": derivative["fileObject"],
        "object_bucket": object_ref["bucket"],
        "object_key": object_ref["key"],
        "object_etag": object_ref.get("eTag"),
        "pixel_width": int(pixel_size["width"]),
        "pixel_height": int(pixel_size["height"]),
    }


def _archive_projection(payload_json: dict) -> dict | None:
    receipt = payload_json.get("originalArchiveReceiptRecorded")
    if not isinstance(receipt, dict):
        return None
    return {
        "asset_id": receipt["assetID"],
        "file_object": receipt["fileObject"],
        "server_placement": receipt["serverPlacement"],
    }


def _derivative_file_exists(keeps_root: Path, object_key: str) -> bool:
    key_path = Path(object_key)
    if key_path.is_absolute() or any(part in {"", ".", ".."} for part in key_path.parts):
        return False
    preview_path = (keeps_root / "previews" / key_path).resolve(strict=False)
    previews_root = (keeps_root / "previews").resolve(strict=False)
    return (preview_path == previews_root or previews_root in preview_path.parents) and preview_path.is_file()


def _existing_derivative_keys(keeps_root: Path) -> set[str]:
    previews_root = keeps_root / "previews"
    if not previews_root.exists():
        return set()
    return {str(path.relative_to(previews_root)) for path in previews_root.rglob("*") if path.is_file()}


def _flush_batches(
    dst: sqlite3.Connection,
    ledger_batch: list[tuple],
    derivative_batch: list[tuple],
    archive_batch: list[tuple],
) -> None:
    if ledger_batch:
        dst.executemany(
            """
            INSERT INTO ledger_events (
              library_id, global_seq, op_id, device_id, device_seq, hybrid_logical_time,
              actor_id, entity_type, entity_id, op_type, payload_json, payload_hash,
              base_version, committed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ledger_batch,
        )
        ledger_batch.clear()
    if derivative_batch:
        dst.executemany(
            """
            INSERT OR REPLACE INTO derivative_objects (
              library_id, asset_id, role, file_object, object_bucket, object_key, object_etag,
              pixel_width, pixel_height, declared_event_seq, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            derivative_batch,
        )
        derivative_batch.clear()
    if archive_batch:
        dst.executemany(
            """
            INSERT OR REPLACE INTO archive_receipts (
              library_id, asset_id, receipt_event_seq, file_object, server_placement, committed_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            archive_batch,
        )
        archive_batch.clear()
    dst.commit()


def _seed_device_states(src: sqlite3.Connection, dst: sqlite3.Connection, source_database: Path, max_seq: int) -> None:
    for row in src.execute("SELECT peer_id, cursor, updated_at FROM sync_cursors"):
        dst.execute(
            """
            INSERT OR REPLACE INTO device_states (
              library_id, device_id, actor_id, last_seen_at, last_uploaded_device_seq, last_pull_cursor, capabilities
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "local-library",
                row["peer_id"],
                "seed-import",
                datetime.now(timezone.utc).isoformat(),
                max_seq,
                int(row["cursor"]),
                json.dumps(
                    {
                        "seededFrom": str(source_database),
                        "sourceCursor": row["cursor"],
                        "sourceUpdatedAt": row["updated_at"],
                    },
                    separators=(",", ":"),
                ),
            ),
        )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed a NAS Keeps control-plane SQLite DB from the macOS local library.")
    parser.add_argument("--source-database", type=Path, required=True)
    parser.add_argument("--target-database", type=Path, required=True)
    parser.add_argument("--keeps-root", type=Path, required=True)
    parser.add_argument("--include-missing-derivative-projection", action="store_true")
    args = parser.parse_args()
    summary = seed_sqlite_from_macos_library(
        source_database=args.source_database,
        target_database=args.target_database,
        keeps_root=args.keeps_root,
        include_missing_derivative_projection=args.include_missing_derivative_projection,
    )
    print(json.dumps(summary.__dict__, sort_keys=True))


if __name__ == "__main__":
    main()
