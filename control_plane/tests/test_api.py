from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from fastapi.testclient import TestClient
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql

from control_plane.app import _database_url_from_connection_secret, create_app
from control_plane.db import ArchiveReceiptRecord, DerivativePresigner, DeviceStateRecord, DerivativeObjectRecord, LedgerEventRecord, SyncConflictRecord, create_engine_for_url
from control_plane.schemas import (
    AssetMetadataField,
    AuthorityRole,
    Availability,
    DerivativeRole,
    DerivativeObject,
    DerivativeUploadRequest,
    DeviceHeartbeatRequest,
    FileObjectID,
    FilePlacement,
    FileRole,
    HybridLogicalTime,
    LedgerEntityType,
    LedgerOperationType,
    LedgerValue,
    MetadataSetPayload,
    OperationLedgerEntry,
    OperationPayload,
    OriginalArchiveReceiptRecordedPayload,
    PixelSize,
    S3ObjectRef,
    StorageKind,
    TagsUpdatedPayload,
)


class FakeDerivativePresigner(DerivativePresigner):
    def presign_upload(self, bucket: str, key: str) -> str:
        return f"https://presign.test/upload/{bucket}/{key}"

    def presign_download(self, bucket: str, key: str) -> str:
        return f"https://presign.test/download/{bucket}/{key}"


def make_app(tmp_path: Path, trusted_device_ids: set[str] | None = None):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control_plane.sqlite'}"
    return create_app(
        database_url=database_url,
        derivative_bucket="test-derivative-bucket",
        derivative_presigner=FakeDerivativePresigner(),
        trusted_device_ids=trusted_device_ids,
    )


def make_client(tmp_path: Path) -> TestClient:
    return TestClient(make_app(tmp_path))


def make_op(
    *,
    op_id: UUID,
    library_id: str,
    device_id: str,
    device_sequence: int,
    asset_id: UUID,
    rating: int,
    created_at: datetime,
) -> OperationLedgerEntry:
    return OperationLedgerEntry(
        opID=op_id,
        libraryID=library_id,
        deviceID=device_id,
        deviceSequence=device_sequence,
        hybridLogicalTime=HybridLogicalTime(wallTimeMilliseconds=1_700_000_000_000 + device_sequence, counter=0, nodeID=device_id),
        actorID="user",
        entityType=LedgerEntityType.asset,
        entityID=str(asset_id),
        opType=LedgerOperationType.metadataSet,
        payload=OperationPayload(
            metadataSet=MetadataSetPayload(
                assetID=asset_id,
                field=AssetMetadataField.rating,
                value=LedgerValue(intValue=rating),
            )
        ),
        createdAt=created_at,
    )


def test_append_operation_increments_global_seq_and_round_trips_payload_json(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    op = make_op(
        op_id=UUID("00000000-0000-0000-0000-000000000001"),
        library_id="library-a",
        device_id="mac",
        device_sequence=1,
        asset_id=UUID("00000000-0000-0000-0000-00000000a001"),
        rating=4,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    response = client.post("/libraries/library-a/ops", json={"operations": [op.model_dump(mode="json", by_alias=True, exclude_none=True)]})
    assert response.status_code == 200
    body = response.json()
    assert body["accepted"][0]["globalSeq"] == 1
    assert body["cursor"] == "1"

    fetch = client.get("/libraries/library-a/ops?after=0")
    assert fetch.status_code == 200
    payload = fetch.json()
    assert payload["cursor"] == "1"
    assert payload["hasMore"] is False
    assert payload["operations"][0]["payload"] == op.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert payload["operations"][0]["payloadHash"]


def test_metadata_set_accepts_swift_synthesized_ledger_value_shape(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    body = {
        "operations": [
            {
                "opID": "00000000-0000-0000-0000-00000000000a",
                "libraryID": "library-a",
                "deviceID": "mac",
                "deviceSequence": 1,
                "hybridLogicalTime": {
                    "wallTimeMilliseconds": 1_700_000_000_010,
                    "counter": 0,
                    "nodeID": "mac",
                },
                "actorID": "user",
                "entityType": "asset",
                "entityID": "00000000-0000-0000-0000-00000000a00a",
                "opType": "metadata_set",
                "payload": {
                    "metadataSet": {
                        "assetID": "00000000-0000-0000-0000-00000000a00a",
                        "field": "rating",
                        "value": {"int": {"_0": 4}},
                    }
                },
                "createdAt": "2024-01-01T00:00:00Z",
            }
        ]
    }

    response = client.post("/libraries/library-a/ops", json=body)
    assert response.status_code == 200
    fetched = client.get("/libraries/library-a/ops?after=0").json()["operations"][0]
    assert fetched["payload"]["metadataSet"]["value"] == {"int": {"_0": 4}}


def test_uppercase_uuid_entity_id_is_accepted_for_swift_events(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    asset_id = UUID("00000000-0000-0000-0000-00000000a0af")
    op = OperationLedgerEntry(
        opID=UUID("00000000-0000-0000-0000-0000000000af"),
        libraryID="library-a",
        deviceID="mac",
        deviceSequence=1,
        hybridLogicalTime=HybridLogicalTime(wallTimeMilliseconds=1_700_000_000_011, counter=0, nodeID="mac"),
        actorID="user",
        entityType=LedgerEntityType.asset,
        entityID=str(asset_id).upper(),
        opType=LedgerOperationType.metadataSet,
        payload=OperationPayload(
            metadataSet=MetadataSetPayload(
                assetID=asset_id,
                field=AssetMetadataField.rating,
                value=LedgerValue(intValue=4),
            )
        ),
        createdAt=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    response = client.post("/libraries/library-a/ops", json={"operations": [op.model_dump(mode="json", by_alias=True, exclude_none=True)]})
    assert response.status_code == 200
    assert response.json()["accepted"][0]["globalSeq"] == 1


def test_duplicate_op_id_same_payload_is_idempotent(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    op = make_op(
        op_id=UUID("00000000-0000-0000-0000-000000000002"),
        library_id="library-a",
        device_id="mac",
        device_sequence=1,
        asset_id=UUID("00000000-0000-0000-0000-00000000a002"),
        rating=2,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    payload = {"operations": [op.model_dump(mode="json", by_alias=True, exclude_none=True)]}

    first = client.post("/libraries/library-a/ops", json=payload)
    second = client.post("/libraries/library-a/ops", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["accepted"][0]["globalSeq"] == 1
    assert second.json()["accepted"][0]["globalSeq"] == 1


def test_duplicate_op_id_same_payload_across_libraries_conflicts(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    shared_op_id = UUID("00000000-0000-0000-0000-000000000002")
    op1 = make_op(
        op_id=shared_op_id,
        library_id="library-a",
        device_id="mac-a",
        device_sequence=1,
        asset_id=UUID("00000000-0000-0000-0000-00000000a102"),
        rating=2,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    op2 = make_op(
        op_id=shared_op_id,
        library_id="library-b",
        device_id="mac-b",
        device_sequence=1,
        asset_id=UUID("00000000-0000-0000-0000-00000000a102"),
        rating=2,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    first = client.post("/libraries/library-a/ops", json={"operations": [op1.model_dump(mode="json", by_alias=True, exclude_none=True)]})
    second = client.post("/libraries/library-b/ops", json={"operations": [op2.model_dump(mode="json", by_alias=True, exclude_none=True)]})

    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json()["detail"]["conflicts"][0]["conflictType"] == "duplicate_op_id_identity_mismatch"
    assert client.app.state.store.get_ledger_state("library-b") == []


def test_duplicate_op_id_different_payload_conflicts(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    op1 = make_op(
        op_id=UUID("00000000-0000-0000-0000-000000000003"),
        library_id="library-a",
        device_id="mac",
        device_sequence=1,
        asset_id=UUID("00000000-0000-0000-0000-00000000a003"),
        rating=1,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    op2 = make_op(
        op_id=op1.opID,
        library_id="library-a",
        device_id="mac",
        device_sequence=2,
        asset_id=UUID(op1.entityID),
        rating=5,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    assert client.post("/libraries/library-a/ops", json={"operations": [op1.model_dump(mode="json", by_alias=True, exclude_none=True)]}).status_code == 200
    response = client.post("/libraries/library-a/ops", json={"operations": [op2.model_dump(mode="json", by_alias=True, exclude_none=True)]})
    assert response.status_code == 409
    assert response.json()["detail"]["conflicts"][0]["conflictType"] == "duplicate_op_id_payload_mismatch"


def test_duplicate_device_sequence_different_op_conflicts(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    asset_id = UUID("00000000-0000-0000-0000-00000000a004")
    op1 = make_op(
        op_id=UUID("00000000-0000-0000-0000-000000000004"),
        library_id="library-a",
        device_id="mac",
        device_sequence=1,
        asset_id=asset_id,
        rating=1,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    op2 = make_op(
        op_id=UUID("00000000-0000-0000-0000-000000000005"),
        library_id="library-a",
        device_id="mac",
        device_sequence=1,
        asset_id=asset_id,
        rating=5,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    assert client.post("/libraries/library-a/ops", json={"operations": [op1.model_dump(mode="json", by_alias=True, exclude_none=True)]}).status_code == 200
    response = client.post("/libraries/library-a/ops", json={"operations": [op2.model_dump(mode="json", by_alias=True, exclude_none=True)]})
    assert response.status_code == 409
    assert response.json()["detail"]["conflicts"][0]["conflictType"] == "duplicate_device_sequence"


def test_integrityerror_fallback_maps_duplicate_device_sequence(tmp_path: Path, monkeypatch) -> None:
    client = make_client(tmp_path)
    store = client.app.state.store
    assert store is not None

    asset_id = UUID("00000000-0000-0000-0000-00000000a005")
    first = make_op(
        op_id=UUID("00000000-0000-0000-0000-000000000006"),
        library_id="library-a",
        device_id="mac",
        device_sequence=1,
        asset_id=asset_id,
        rating=1,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    second = make_op(
        op_id=UUID("00000000-0000-0000-0000-000000000007"),
        library_id="library-a",
        device_id="mac",
        device_sequence=1,
        asset_id=asset_id,
        rating=5,
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    assert client.post("/libraries/library-a/ops", json={"operations": [first.model_dump(mode="json", by_alias=True, exclude_none=True)]}).status_code == 200

    calls = {"count": 0}
    original_duplicate_device_sequence_conflict = store._duplicate_device_sequence_conflict

    def fake_duplicate_device_sequence_conflict(session, library_id, operation):
        calls["count"] += 1
        if calls["count"] == 1:
            return None
        return original_duplicate_device_sequence_conflict(session, library_id, operation)

    def fake_reserve_sequence(session, library_id):
        raise IntegrityError("insert", {}, Exception("boom"))

    monkeypatch.setattr(store, "_duplicate_device_sequence_conflict", fake_duplicate_device_sequence_conflict)
    monkeypatch.setattr(store, "_reserve_sequence", fake_reserve_sequence)

    response = client.post("/libraries/library-a/ops", json={"operations": [second.model_dump(mode="json", by_alias=True, exclude_none=True)]})
    assert response.status_code == 409
    assert response.json()["detail"]["conflicts"][0]["conflictType"] == "duplicate_device_sequence"
    conflicts = store.get_sync_conflicts("library-a")
    assert len(conflicts) == 1
    assert conflicts[0].conflict_type == "duplicate_device_sequence"
    assert conflicts[0].entity_id == second.entityID


def test_invalid_operation_semantics_are_rejected_before_write(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    asset_id = UUID("00000000-0000-0000-0000-00000000a006")

    op_type_mismatch = {
        "opID": "00000000-0000-0000-0000-000000000060",
        "libraryID": "library-a",
        "deviceID": "mac",
        "deviceSequence": 1,
        "hybridLogicalTime": {"wallTimeMilliseconds": 1_700_000_000_060, "counter": 0, "nodeID": "mac"},
        "actorID": "user",
        "entityType": "asset",
        "entityID": str(asset_id),
        "opType": "metadata_set",
        "payload": {
            "originalArchiveReceiptRecorded": {
                "assetID": str(asset_id),
                "fileObject": {"contentHash": "hash", "sizeBytes": 1, "role": "raw_original"},
                "serverPlacement": {
                    "fileObjectID": {"contentHash": "hash", "sizeBytes": 1, "role": "raw_original"},
                    "holderID": "server",
                    "storageKind": "nas",
                    "authorityRole": "canonical",
                    "availability": "online",
                },
            }
        },
        "createdAt": "2024-01-01T00:00:00Z",
    }
    response = client.post("/libraries/library-a/ops", json={"operations": [op_type_mismatch]})
    assert response.status_code == 422
    assert client.app.state.store.get_ledger_state("library-a") == []

    entity_mismatch = {
        "opID": "00000000-0000-0000-0000-000000000061",
        "libraryID": "library-a",
        "deviceID": "mac",
        "deviceSequence": 1,
        "hybridLogicalTime": {"wallTimeMilliseconds": 1_700_000_000_061, "counter": 0, "nodeID": "mac"},
        "actorID": "user",
        "entityType": "file_placement",
        "entityID": str(UUID("00000000-0000-0000-0000-00000000ffff")),
        "opType": "file_placement_snapshot_declared",
        "payload": {
            "filePlacementSnapshotDeclared": {
                "assetID": str(asset_id),
                "fileObject": {"contentHash": "hash", "sizeBytes": 1, "role": "raw_original"},
                "placement": {
                    "fileObjectID": {"contentHash": "hash", "sizeBytes": 1, "role": "raw_original"},
                    "holderID": "mac",
                    "storageKind": "local",
                    "authorityRole": "working_copy",
                    "availability": "online",
                },
            }
        },
        "createdAt": "2024-01-01T00:00:00Z",
    }
    response = client.post("/libraries/library-a/ops", json={"operations": [entity_mismatch]})
    assert response.status_code == 422
    assert client.app.state.store.get_ledger_state("library-a") == []


def test_imported_original_declared_uses_stable_key_and_commits(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    asset_id = UUID("00000000-0000-0000-0000-00000000b0b0")
    file_object = FileObjectID(contentHash="abc123", sizeBytes=4096, role=FileRole.rawOriginal)
    placement = FilePlacement(
        fileObjectID=file_object,
        holderID="mac",
        storageKind=StorageKind.local,
        authorityRole=AuthorityRole.workingCopy,
        availability=Availability.online,
    )
    op = OperationLedgerEntry(
        opID=UUID("00000000-0000-0000-0000-0000000000b0"),
        libraryID="library-a",
        deviceID="mac",
        deviceSequence=1,
        hybridLogicalTime=HybridLogicalTime(wallTimeMilliseconds=1_700_000_000_700, counter=0, nodeID="mac"),
        actorID="user",
        entityType=LedgerEntityType.fileObject,
        entityID=f"{file_object.role.value}:{file_object.sizeBytes}:{file_object.contentHash}",
        opType=LedgerOperationType.importedOriginalDeclared,
        payload=OperationPayload(
            importedOriginalDeclared={
                "assetID": str(asset_id),
                "fileObject": file_object.model_dump(mode="json", by_alias=True, exclude_none=True),
                "placement": placement.model_dump(mode="json", by_alias=True, exclude_none=True),
            }
        ),
        createdAt=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    response = client.post("/libraries/library-a/ops", json={"operations": [op.model_dump(mode="json", by_alias=True, exclude_none=True)]})
    assert response.status_code == 200
    assert response.json()["accepted"][0]["globalSeq"] == 1
    ledger = client.app.state.store.get_ledger_state("library-a")
    assert len(ledger) == 1
    assert ledger[0].entity_id == f"{file_object.role.value}:{file_object.sizeBytes}:{file_object.contentHash}"


def test_derivative_declared_rejects_mismatched_nested_asset_id_before_write(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    outer_asset_id = UUID("00000000-0000-0000-0000-00000000d001")
    inner_asset_id = UUID("00000000-0000-0000-0000-00000000d002")
    derivative = DerivativeObject(
        assetID=inner_asset_id,
        role=DerivativeRole.thumbnail,
        fileObject=FileObjectID(contentHash="hash-diff", sizeBytes=12, role=FileRole.thumbnail),
        s3Object=S3ObjectRef(bucket="bucket", key="key", eTag="etag"),
        pixelSize=PixelSize(width=128, height=128),
    )
    op = OperationLedgerEntry(
        opID=UUID("00000000-0000-0000-0000-0000000000d1"),
        libraryID="library-a",
        deviceID="mac",
        deviceSequence=1,
        hybridLogicalTime=HybridLogicalTime(wallTimeMilliseconds=1_700_000_000_700, counter=0, nodeID="mac"),
        actorID="user",
        entityType=LedgerEntityType.derivativeObject,
        entityID=f"{outer_asset_id}:{derivative.role.value}:{derivative.fileObject.contentHash}",
        opType=LedgerOperationType.derivativeDeclared,
        payload=OperationPayload(
            derivativeDeclared={
                "assetID": str(outer_asset_id),
                "derivative": derivative.model_dump(mode="json", by_alias=True, exclude_none=True),
            }
        ),
        createdAt=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    response = client.post("/libraries/library-a/ops", json={"operations": [op.model_dump(mode="json", by_alias=True, exclude_none=True)]})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "derivative_asset_id_mismatch"
    store = client.app.state.store
    assert store.get_ledger_state("library-a") == []
    assert store.get_derivative_row("library-a", outer_asset_id, DerivativeRole.thumbnail) is None


def test_get_after_cursor_orders_and_pages(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    library_id = "library-a"
    asset_id = UUID("00000000-0000-0000-0000-00000000a010")
    for idx in range(3):
        op = make_op(
            op_id=UUID(int=100 + idx),
            library_id=library_id,
            device_id="mac",
            device_sequence=idx + 1,
            asset_id=asset_id,
            rating=idx,
            created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        assert client.post(f"/libraries/{library_id}/ops", json={"operations": [op.model_dump(mode="json", by_alias=True, exclude_none=True)]}).status_code == 200

    response = client.get(f"/libraries/{library_id}/ops?after=1")
    assert response.status_code == 200
    body = response.json()
    assert [item["globalSeq"] for item in body["operations"]] == [2, 3]
    assert body["cursor"] == "3"
    assert body["hasMore"] is False


def test_get_after_cursor_paginates_at_five_hundred(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    library_id = "library-a"
    operations = []
    for idx in range(501):
        operations.append(
            make_op(
                op_id=UUID(int=10_000 + idx),
                library_id=library_id,
                device_id="mac",
                device_sequence=idx + 1,
                asset_id=UUID(int=20_000 + idx),
                rating=idx % 5,
                created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            ).model_dump(mode="json", by_alias=True, exclude_none=True)
        )

    response = client.post(f"/libraries/{library_id}/ops", json={"operations": operations})
    assert response.status_code == 200

    page = client.get(f"/libraries/{library_id}/ops?after=0")
    assert page.status_code == 200
    body = page.json()
    assert len(body["operations"]) == 500
    assert body["hasMore"] is True
    assert body["cursor"] == "500"


def test_payload_json_round_trip_preserves_nested_union_and_sets(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    asset_id = UUID("00000000-0000-0000-0000-00000000a020")
    op = OperationLedgerEntry(
        opID=UUID("00000000-0000-0000-0000-000000000020"),
        libraryID="library-a",
        deviceID="mac",
        deviceSequence=1,
        hybridLogicalTime=HybridLogicalTime(wallTimeMilliseconds=1_700_000_000_100, counter=1, nodeID="mac"),
        actorID="user",
        entityType=LedgerEntityType.asset,
        entityID=str(asset_id),
        opType=LedgerOperationType.tagsUpdated,
        payload=OperationPayload(
            tagsUpdated=TagsUpdatedPayload(assetID=asset_id, add={"print", "family"}, remove={"reject"})
        ),
        createdAt=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    response = client.post("/libraries/library-a/ops", json={"operations": [op.model_dump(mode="json", by_alias=True, exclude_none=True)]})
    assert response.status_code == 200
    fetched = client.get("/libraries/library-a/ops?after=0").json()["operations"][0]
    assert fetched["payload"]["tagsUpdated"]["add"] == ["family", "print"]
    assert fetched["payload"]["tagsUpdated"]["remove"] == ["reject"]


def test_heartbeat_upserts_device_state(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    request = DeviceHeartbeatRequest(
        deviceID="mac",
        libraryID="library-a",
        actorID="user",
        appVersion="1.0.0",
        localPendingCount=3,
        placements=[],
        sentAt=datetime(2024, 1, 1, tzinfo=timezone.utc),
        lastUploadedDeviceSeq=9,
        lastPullCursor=4,
    )

    response = client.post("/devices/mac/heartbeat", json=request.model_dump(mode="json", by_alias=True, exclude_none=True))
    assert response.status_code == 200
    body = response.json()
    assert body["lastUploadedDeviceSeq"] == 9
    assert body["lastPullCursor"] == 4
    assert body["capabilities"]["appVersion"] == "1.0.0"
    assert body["capabilities"]["localPendingCount"] == 3

    updated = request.model_copy(update={
        "actorID": "user-2",
        "appVersion": "1.0.1",
        "localPendingCount": 1,
        "lastUploadedDeviceSeq": 10,
        "lastPullCursor": 5,
        "sentAt": datetime(2024, 1, 2, tzinfo=timezone.utc),
    })
    updated_response = client.post("/devices/mac/heartbeat", json=updated.model_dump(mode="json", by_alias=True, exclude_none=True))
    assert updated_response.status_code == 200

    state = client.app.state.store.get_device_state("library-a", "mac")
    assert state is not None
    assert state.actor_id == "user-2"
    assert state.last_uploaded_device_seq == 10
    assert state.last_pull_cursor == 5
    assert state.capabilities["appVersion"] == "1.0.1"
    assert state.capabilities["localPendingCount"] == 1


def test_heartbeat_omitted_sequence_fields_return_persisted_values(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    first = DeviceHeartbeatRequest(
        deviceID="mac",
        libraryID="library-a",
        actorID="user",
        appVersion="1.0.0",
        localPendingCount=1,
        placements=[],
        sentAt=datetime(2024, 1, 1, tzinfo=timezone.utc),
        lastUploadedDeviceSeq=9,
        lastPullCursor=4,
    )
    first_response = client.post("/devices/mac/heartbeat", json=first.model_dump(mode="json", by_alias=True, exclude_none=True))
    assert first_response.status_code == 200
    assert first_response.json()["lastUploadedDeviceSeq"] == 9
    assert first_response.json()["lastPullCursor"] == 4

    second = DeviceHeartbeatRequest(
        deviceID="mac",
        libraryID="library-a",
        actorID="user",
        appVersion="1.0.1",
        localPendingCount=2,
        placements=[],
        sentAt=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    second_response = client.post("/devices/mac/heartbeat", json=second.model_dump(mode="json", by_alias=True, exclude_none=True))
    assert second_response.status_code == 200
    assert second_response.json()["lastUploadedDeviceSeq"] == 9
    assert second_response.json()["lastPullCursor"] == 4

    state = client.app.state.store.get_device_state("library-a", "mac")
    assert state is not None
    assert state.last_uploaded_device_seq == 9
    assert state.last_pull_cursor == 4


def test_derivative_metadata_returns_404_when_not_declared(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    response = client.get("/derivatives/00000000-0000-0000-0000-00000000b001?role=thumbnail")
    assert response.status_code == 404


def test_derivative_upload_returns_controlled_metadata(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    request = DerivativeUploadRequest(
        libraryID="library-a",
        assetID=UUID("00000000-0000-0000-0000-00000000b010"),
        role=DerivativeRole.thumbnail,
        fileObject=FileObjectID(contentHash="abc123", sizeBytes=12, role=FileRole.thumbnail),
        pixelSize=PixelSize(width=128, height=128),
    )
    response = client.post("/derivatives/uploads", json=request.model_dump(mode="json", by_alias=True, exclude_none=True))
    assert response.status_code == 200
    body = response.json()
    assert body["assetID"] == str(request.assetID)
    assert body["role"] == "thumbnail"
    assert body["s3Object"]["bucket"] == "test-derivative-bucket"
    assert body["uploadURL"].startswith("https://presign.test/upload/test-derivative-bucket/")


def test_archive_receipt_requires_server_actor(tmp_path: Path) -> None:
    client = TestClient(make_app(tmp_path, trusted_device_ids={"archive-device"}))
    op = OperationLedgerEntry(
        opID=UUID("00000000-0000-0000-0000-000000000030"),
        libraryID="library-a",
        deviceID="mac",
        deviceSequence=1,
        hybridLogicalTime=HybridLogicalTime(wallTimeMilliseconds=1_700_000_000_300, counter=0, nodeID="mac"),
        actorID="user",
        entityType=LedgerEntityType.filePlacement,
        entityID=str(UUID("00000000-0000-0000-0000-00000000a030")),
        opType=LedgerOperationType.originalArchiveReceiptRecorded,
        payload=OperationPayload(
            originalArchiveReceiptRecorded=OriginalArchiveReceiptRecordedPayload(
                assetID=UUID("00000000-0000-0000-0000-00000000a030"),
                fileObject=FileObjectID(contentHash="hash", sizeBytes=1, role=FileRole.rawOriginal),
                serverPlacement=FilePlacement(
                    fileObjectID=FileObjectID(contentHash="hash", sizeBytes=1, role=FileRole.rawOriginal),
                    holderID="server",
                    storageKind=StorageKind.nas,
                    authorityRole=AuthorityRole.canonical,
                    availability=Availability.online,
                ),
            )
        ),
        createdAt=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    forbidden = client.post("/archive/receipts", json={"operation": op.model_dump(mode="json", by_alias=True, exclude_none=True)})
    assert forbidden.status_code == 403

    server_op = op.model_copy(update={"actorID": "server", "deviceID": "archive-server"})
    allowed = client.post("/archive/receipts", json={"operation": server_op.model_dump(mode="json", by_alias=True, exclude_none=True)})
    assert allowed.status_code == 200
    assert allowed.json()["status"] == "committed"

    trusted_device_op = op.model_copy(
        update={
            "opID": UUID("00000000-0000-0000-0000-000000000130"),
            "actorID": "archive-agent",
            "deviceID": "archive-device",
            "deviceSequence": 2,
            "hybridLogicalTime": HybridLogicalTime(
                wallTimeMilliseconds=1_700_000_000_301,
                counter=0,
                nodeID="archive-device",
            ),
        }
    )
    trusted = client.post("/archive/receipts", json={"operation": trusted_device_op.model_dump(mode="json", by_alias=True, exclude_none=True)})
    assert trusted.status_code == 200
    assert trusted.json()["status"] == "committed"


def test_archive_receipt_rejects_payload_case_mismatch(tmp_path: Path) -> None:
    client = TestClient(make_app(tmp_path, trusted_device_ids={"archive-device"}))
    op = OperationLedgerEntry(
        opID=UUID("00000000-0000-0000-0000-000000000031"),
        libraryID="library-a",
        deviceID="archive-device",
        deviceSequence=1,
        hybridLogicalTime=HybridLogicalTime(wallTimeMilliseconds=1_700_000_000_301, counter=0, nodeID="archive-device"),
        actorID="archive-agent",
        entityType=LedgerEntityType.filePlacement,
        entityID=str(UUID("00000000-0000-0000-0000-00000000a031")),
        opType=LedgerOperationType.metadataSet,
        payload=OperationPayload(
            originalArchiveReceiptRecorded=OriginalArchiveReceiptRecordedPayload(
                assetID=UUID("00000000-0000-0000-0000-00000000a031"),
                fileObject=FileObjectID(contentHash="hash", sizeBytes=1, role=FileRole.rawOriginal),
                serverPlacement=FilePlacement(
                    fileObjectID=FileObjectID(contentHash="hash", sizeBytes=1, role=FileRole.rawOriginal),
                    holderID="server",
                    storageKind=StorageKind.nas,
                    authorityRole=AuthorityRole.canonical,
                    availability=Availability.online,
                ),
            )
        ),
        createdAt=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    response = client.post("/archive/receipts", json={"operation": op.model_dump(mode="json", by_alias=True, exclude_none=True)})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "payload_case_mismatch"


def test_archive_receipt_rejects_invalid_entity_id_before_write(tmp_path: Path) -> None:
    client = TestClient(make_app(tmp_path, trusted_device_ids={"archive-device"}))
    op = OperationLedgerEntry(
        opID=UUID("00000000-0000-0000-0000-000000000032"),
        libraryID="library-a",
        deviceID="archive-device",
        deviceSequence=1,
        hybridLogicalTime=HybridLogicalTime(wallTimeMilliseconds=1_700_000_000_302, counter=0, nodeID="archive-device"),
        actorID="archive-agent",
        entityType=LedgerEntityType.filePlacement,
        entityID="not-a-uuid",
        opType=LedgerOperationType.originalArchiveReceiptRecorded,
        payload=OperationPayload(
            originalArchiveReceiptRecorded=OriginalArchiveReceiptRecordedPayload(
                assetID=UUID("00000000-0000-0000-0000-00000000a032"),
                fileObject=FileObjectID(contentHash="hash", sizeBytes=1, role=FileRole.rawOriginal),
                serverPlacement=FilePlacement(
                    fileObjectID=FileObjectID(contentHash="hash", sizeBytes=1, role=FileRole.rawOriginal),
                    holderID="server",
                    storageKind=StorageKind.nas,
                    authorityRole=AuthorityRole.canonical,
                    availability=Availability.online,
                ),
            )
        ),
        createdAt=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    response = client.post("/archive/receipts", json={"operation": op.model_dump(mode="json", by_alias=True, exclude_none=True)})
    assert response.status_code == 422
    store = client.app.state.store
    assert store.get_ledger_state("library-a") == []
    assert store.get_archive_receipts("library-a", UUID("00000000-0000-0000-0000-00000000a032")) == []


def test_create_app_requires_explicit_database_config_when_env_missing(monkeypatch) -> None:
    monkeypatch.delenv("CONTROL_PLANE_DATABASE_URL", raising=False)
    monkeypatch.delenv("CONTROL_PLANE_ALLOW_SQLITE_DEV", raising=False)

    from control_plane.app import create_app as load_app

    try:
        load_app()
        raise AssertionError("expected RuntimeError")
    except RuntimeError as exc:
        assert "CONTROL_PLANE_DATABASE_URL" in str(exc)


def test_create_app_can_disable_auto_schema_creation(tmp_path: Path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control_plane.sqlite'}"
    app = create_app(
        database_url=database_url,
        derivative_bucket="test-derivative-bucket",
        derivative_presigner=FakeDerivativePresigner(),
        auto_create_schema=False,
    )
    engine = app.state.engine
    assert inspect(engine).get_table_names() == []


def test_database_url_can_be_built_from_aurora_connection_secret(monkeypatch) -> None:
    class FakeSecretsManager:
        def get_secret_value(self, SecretId: str):
            assert SecretId == "arn:aws:secretsmanager:secret"
            return {
                "SecretString": (
                    '{"username":"eventstore_admin","password":"p@ss/word",'
                    '"host":"writer.cluster.local","port":5432,"database":"photo_asset_manager"}'
                )
            }

    class FakeBoto3:
        @staticmethod
        def client(service_name: str):
            assert service_name == "secretsmanager"
            return FakeSecretsManager()

    monkeypatch.setattr("control_plane.app.boto3", FakeBoto3)

    url = _database_url_from_connection_secret("arn:aws:secretsmanager:secret")

    assert url == "postgresql+psycopg://eventstore_admin:p%40ss%2Fword@writer.cluster.local:5432/photo_asset_manager"


def test_postgresql_driver_and_ddl_smoke() -> None:
    engine = create_engine_for_url("postgresql+psycopg://user:pass@localhost:5432/photo_asset_manager")
    assert engine.dialect.name == "postgresql"
    assert engine.dialect.driver == "psycopg"

    ddl_map = {
        "ledger_events": str(CreateTable(LedgerEventRecord.__table__).compile(dialect=postgresql.dialect())),
        "device_states": str(CreateTable(DeviceStateRecord.__table__).compile(dialect=postgresql.dialect())),
        "derivative_objects": str(CreateTable(DerivativeObjectRecord.__table__).compile(dialect=postgresql.dialect())),
        "archive_receipts": str(CreateTable(ArchiveReceiptRecord.__table__).compile(dialect=postgresql.dialect())),
        "sync_conflicts": str(CreateTable(SyncConflictRecord.__table__).compile(dialect=postgresql.dialect())),
    }
    assert "UUID" in ddl_map["ledger_events"]
    assert "JSONB" in ddl_map["ledger_events"]
    assert "JSONB" in ddl_map["device_states"]
    assert "JSONB" in ddl_map["derivative_objects"]
    assert "JSONB" in ddl_map["archive_receipts"]
    assert "JSONB" in ddl_map["sync_conflicts"]
