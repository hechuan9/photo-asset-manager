from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import boto3
from sqlalchemy import BigInteger, DateTime, Index, JSON, String, UniqueConstraint, Uuid, create_engine, func, select
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from .schemas import (
    ArchiveReceiptRequest,
    ArchiveReceiptResponse,
    AssetMetadataField,
    AuthorityRole,
    Availability,
    CommittedOperationLedgerEntry,
    DerivativeDeclaredPayload,
    DerivativeMetadataResponse,
    DerivativeObject,
    DerivativeRole,
    DerivativeUploadRequest,
    DerivativeUploadResponse,
    DeviceHeartbeatRequest,
    DeviceHeartbeatResponse,
    FilePlacement,
    FilePlacementSnapshotDeclaredPayload,
    FileObjectID,
    LedgerEntityType,
    LedgerOperationType,
    OperationLedgerEntry,
    OperationPayload,
    OriginalArchiveReceiptRecordedPayload,
    S3ObjectRef,
    OperationSemanticError,
    SyncOpsFetchResponse,
    SyncOpsUploadRequest,
    SyncOpsUploadResponse,
)


class Base(DeclarativeBase):
    pass


_UUID_TYPE = Uuid(as_uuid=False).with_variant(String(36), "sqlite")
_JSON_TYPE = JSON().with_variant(JSONB(), "postgresql")


def _uuid_type() -> Any:
    return _UUID_TYPE


def _json_type() -> Any:
    return _JSON_TYPE


class LedgerSequenceCounter(Base):
    __tablename__ = "ledger_sequence_counters"

    library_id: Mapped[str] = mapped_column(String, primary_key=True)
    next_global_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)


class LedgerEventRecord(Base):
    __tablename__ = "ledger_events"
    __table_args__ = (
        UniqueConstraint("op_id", name="uq_ledger_events_op_id"),
        UniqueConstraint("library_id", "device_id", "device_seq", name="uq_ledger_events_device_seq"),
        Index("ix_ledger_events_entity", "library_id", "entity_type", "entity_id", "global_seq"),
        Index("ix_ledger_events_op_type", "library_id", "op_type", "global_seq"),
    )

    library_id: Mapped[str] = mapped_column(String, primary_key=True)
    global_seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    op_id: Mapped[str] = mapped_column(_uuid_type(), nullable=False)
    device_id: Mapped[str] = mapped_column(String, nullable=False)
    device_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    hybrid_logical_time: Mapped[dict[str, Any]] = mapped_column(_json_type(), nullable=False)
    actor_id: Mapped[str] = mapped_column(String, nullable=False)
    entity_type: Mapped[str] = mapped_column(String, nullable=False)
    entity_id: Mapped[str] = mapped_column(String, nullable=False)
    op_type: Mapped[str] = mapped_column(String, nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(_json_type(), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    base_version: Mapped[str | None] = mapped_column(String, nullable=True)
    committed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class DeviceStateRecord(Base):
    __tablename__ = "device_states"

    library_id: Mapped[str] = mapped_column(String, primary_key=True)
    device_id: Mapped[str] = mapped_column(String, primary_key=True)
    actor_id: Mapped[str] = mapped_column(String, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_uploaded_device_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_pull_cursor: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    capabilities: Mapped[dict[str, Any]] = mapped_column(_json_type(), nullable=False, default=dict)


class DerivativeObjectRecord(Base):
    __tablename__ = "derivative_objects"

    library_id: Mapped[str] = mapped_column(String, primary_key=True)
    asset_id: Mapped[str] = mapped_column(_uuid_type(), primary_key=True)
    role: Mapped[str] = mapped_column(String, primary_key=True)
    file_object: Mapped[dict[str, Any]] = mapped_column(_json_type(), nullable=False)
    s3_bucket: Mapped[str] = mapped_column(String, nullable=False)
    s3_key: Mapped[str] = mapped_column(String, nullable=False)
    s3_etag: Mapped[str | None] = mapped_column(String, nullable=True)
    pixel_width: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pixel_height: Mapped[int] = mapped_column(BigInteger, nullable=False)
    declared_event_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ArchiveReceiptRecord(Base):
    __tablename__ = "archive_receipts"

    library_id: Mapped[str] = mapped_column(String, primary_key=True)
    asset_id: Mapped[str] = mapped_column(_uuid_type(), primary_key=True)
    receipt_event_seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    file_object: Mapped[dict[str, Any]] = mapped_column(_json_type(), nullable=False)
    server_placement: Mapped[dict[str, Any]] = mapped_column(_json_type(), nullable=False)
    committed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SyncConflictRecord(Base):
    __tablename__ = "sync_conflicts"

    id: Mapped[str] = mapped_column(_uuid_type(), primary_key=True)
    library_id: Mapped[str] = mapped_column(String, nullable=False)
    entity_type: Mapped[str] = mapped_column(String, nullable=False)
    entity_id: Mapped[str] = mapped_column(String, nullable=False)
    conflict_type: Mapped[str] = mapped_column(String, nullable=False)
    left_op_id: Mapped[str | None] = mapped_column(_uuid_type(), nullable=True)
    right_op_id: Mapped[str | None] = mapped_column(_uuid_type(), nullable=True)
    detail: Mapped[dict[str, Any]] = mapped_column(_json_type(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


@dataclass(slots=True)
class AcceptedOperation:
    opID: UUID
    globalSeq: int
    committedAt: datetime
    payloadHash: str


@dataclass(slots=True)
class ConflictOperation:
    opID: UUID
    conflictType: str
    detail: dict[str, Any]


@dataclass(slots=True)
class UploadOutcome:
    accepted: list[AcceptedOperation]
    conflicts: list[ConflictOperation]
    cursor: int


def create_engine_for_url(database_url: str) -> Engine:
    connect_args: dict[str, Any] = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(database_url, future=True, connect_args=connect_args)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def initialize_database(engine: Engine) -> None:
    Base.metadata.create_all(engine)


class DerivativePresigner:
    def presign_upload(self, bucket: str, key: str) -> str:
        raise NotImplementedError

    def presign_download(self, bucket: str, key: str) -> str:
        raise NotImplementedError


class Boto3DerivativePresigner(DerivativePresigner):
    def __init__(self, expires_in_seconds: int = 900) -> None:
        self._client = boto3.client("s3")
        self._expires_in_seconds = expires_in_seconds

    def presign_upload(self, bucket: str, key: str) -> str:
        return self._client.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=self._expires_in_seconds,
        )

    def presign_download(self, bucket: str, key: str) -> str:
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=self._expires_in_seconds,
        )


class ControlPlaneStore:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        derivative_bucket: str,
        derivative_presigner: DerivativePresigner | None = None,
        trusted_device_ids: set[str] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._derivative_bucket = derivative_bucket
        self._derivative_presigner = derivative_presigner or Boto3DerivativePresigner()
        self._trusted_device_ids = trusted_device_ids or set()

    def append_operations(self, library_id: str, request: SyncOpsUploadRequest) -> UploadOutcome:
        for operation in request.operations:
            self.validate_operation_semantics(operation)

        accepted: list[AcceptedOperation] = []
        conflicts: list[ConflictOperation] = []

        for operation in request.operations:
            outcome = self._append_single_operation(library_id, operation)
            if isinstance(outcome, AcceptedOperation):
                accepted.append(outcome)
            else:
                conflicts.append(outcome)

        return UploadOutcome(accepted=accepted, conflicts=conflicts, cursor=self._current_cursor(library_id))

    def fetch_operations(self, library_id: str, after: int, limit: int = 500) -> SyncOpsFetchResponse:
        with self._session_factory() as session:
            rows = list(
                session.execute(
                    select(LedgerEventRecord)
                    .where(LedgerEventRecord.library_id == library_id, LedgerEventRecord.global_seq > after)
                    .order_by(LedgerEventRecord.global_seq.asc())
                    .limit(limit + 1)
                ).scalars()
            )

        has_more = len(rows) > limit
        rows = rows[:limit]
        cursor = str(rows[-1].global_seq) if rows else str(after)
        operations = [self._row_to_committed_operation(row) for row in rows]
        return SyncOpsFetchResponse(operations=operations, cursor=cursor, hasMore=has_more)

    def upsert_heartbeat(self, request: DeviceHeartbeatRequest) -> DeviceHeartbeatResponse:
        seen_at = request.sentAt or datetime.now(timezone.utc)
        capabilities = {
            "appVersion": request.appVersion,
            "localPendingCount": request.localPendingCount,
            "placementSummary": [placement.model_dump(mode="json", by_alias=True, exclude_none=True) for placement in (request.placementSummary or [])],
            "placements": [placement.model_dump(mode="json", by_alias=True, exclude_none=True) for placement in request.placements],
        }
        if request.lastUploadedDeviceSeq is not None:
            capabilities["lastUploadedDeviceSeq"] = request.lastUploadedDeviceSeq
        if request.lastPullCursor is not None:
            capabilities["lastPullCursor"] = request.lastPullCursor

        with self._session_factory() as session, session.begin():
            row = session.get(DeviceStateRecord, {"library_id": request.libraryID, "device_id": request.deviceID})
            if row is None:
                row = DeviceStateRecord(
                    library_id=request.libraryID,
                    device_id=request.deviceID,
                    actor_id=request.actorID or "unknown",
                    last_seen_at=seen_at,
                    last_uploaded_device_seq=request.lastUploadedDeviceSeq or 0,
                    last_pull_cursor=request.lastPullCursor or 0,
                    capabilities=capabilities,
                )
                session.add(row)
            else:
                row.actor_id = request.actorID or row.actor_id
                row.last_seen_at = seen_at
                if request.lastUploadedDeviceSeq is not None:
                    row.last_uploaded_device_seq = request.lastUploadedDeviceSeq
                if request.lastPullCursor is not None:
                    row.last_pull_cursor = request.lastPullCursor
                row.capabilities = capabilities
            persisted = session.get(DeviceStateRecord, {"library_id": request.libraryID, "device_id": request.deviceID})
            assert persisted is not None
            return DeviceHeartbeatResponse(
                libraryID=persisted.library_id,
                deviceID=persisted.device_id,
                actorID=persisted.actor_id,
                lastSeenAt=persisted.last_seen_at,
                lastUploadedDeviceSeq=persisted.last_uploaded_device_seq,
                lastPullCursor=persisted.last_pull_cursor,
                capabilities=persisted.capabilities,
            )

    def create_derivative_upload(self, request: DerivativeUploadRequest) -> DerivativeUploadResponse:
        key = f"libraries/{request.libraryID}/assets/{request.assetID}/derivatives/{request.role.value}/{uuid4().hex}"
        s3_object = S3ObjectRef(bucket=self._derivative_bucket, key=key, eTag=None)
        return DerivativeUploadResponse(
            libraryID=request.libraryID,
            assetID=request.assetID,
            role=request.role,
            fileObject=request.fileObject,
            s3Object=s3_object,
            uploadURL=self._derivative_presigner.presign_upload(self._derivative_bucket, key),
        )

    def get_derivative_metadata(self, library_id: str | None, asset_id: UUID, role: DerivativeRole) -> DerivativeMetadataResponse:
        with self._session_factory() as session:
            query = select(DerivativeObjectRecord).where(
                DerivativeObjectRecord.asset_id == str(asset_id),
                DerivativeObjectRecord.role == role.value,
            )
            if library_id is not None:
                query = query.where(DerivativeObjectRecord.library_id == library_id)
            rows = list(session.execute(query.order_by(DerivativeObjectRecord.updated_at.desc())).scalars())

        if not rows:
            raise LookupError("derivative not found")
        if library_id is None and len(rows) > 1:
            raise ValueError("libraryID required when the asset exists in multiple libraries")

        row = rows[0]
        derivative = DerivativeObject.model_validate(
            {
                "assetID": row.asset_id,
                "role": row.role,
                "fileObject": row.file_object,
                "s3Object": {"bucket": row.s3_bucket, "key": row.s3_key, "eTag": row.s3_etag},
                "pixelSize": {"width": row.pixel_width, "height": row.pixel_height},
            }
        )
        return DerivativeMetadataResponse(
            derivative=derivative,
            downloadURL=self._derivative_presigner.presign_download(row.s3_bucket, row.s3_key),
        )

    def record_archive_receipt(self, request: ArchiveReceiptRequest) -> ArchiveReceiptResponse:
        self.validate_operation_semantics(request.operation)
        if request.operation.actorID != "server" and request.operation.deviceID not in self._trusted_device_ids:
            raise PermissionError("archive receipts require actorID=server or a trusted deviceID")

        outcome = self._append_single_operation(request.operation.libraryID, request.operation)
        if isinstance(outcome, ConflictOperation):
            raise RuntimeError(f"archive receipt conflicted: {outcome.conflictType}")

        payload = request.operation.payload.originalArchiveReceiptRecorded
        assert payload is not None
        return ArchiveReceiptResponse(status="committed", globalSeq=outcome.globalSeq, assetID=payload.assetID)

    def get_ledger_state(self, library_id: str) -> list[LedgerEventRecord]:
        with self._session_factory() as session:
            return list(
                session.execute(
                    select(LedgerEventRecord)
                    .where(LedgerEventRecord.library_id == library_id)
                    .order_by(LedgerEventRecord.global_seq.asc())
                ).scalars()
            )

    def get_device_state(self, library_id: str, device_id: str) -> DeviceStateRecord | None:
        with self._session_factory() as session:
            return session.get(DeviceStateRecord, {"library_id": library_id, "device_id": device_id})

    def get_derivative_row(self, library_id: str, asset_id: UUID, role: DerivativeRole) -> DerivativeObjectRecord | None:
        with self._session_factory() as session:
            return session.get(DerivativeObjectRecord, {"library_id": library_id, "asset_id": str(asset_id), "role": role.value})

    def get_archive_receipts(self, library_id: str, asset_id: UUID) -> list[ArchiveReceiptRecord]:
        with self._session_factory() as session:
            return list(
                session.execute(
                    select(ArchiveReceiptRecord)
                    .where(ArchiveReceiptRecord.library_id == library_id, ArchiveReceiptRecord.asset_id == str(asset_id))
                    .order_by(ArchiveReceiptRecord.receipt_event_seq.asc())
                ).scalars()
            )

    def get_sync_conflicts(self, library_id: str) -> list[SyncConflictRecord]:
        with self._session_factory() as session:
            return list(
                session.execute(
                    select(SyncConflictRecord)
                    .where(SyncConflictRecord.library_id == library_id)
                    .order_by(SyncConflictRecord.created_at.asc())
                ).scalars()
            )

    def _append_single_operation(self, library_id: str, operation: OperationLedgerEntry) -> AcceptedOperation | ConflictOperation:
        if operation.libraryID != library_id:
            return ConflictOperation(
                opID=operation.opID,
                conflictType="library_mismatch",
                detail={"expectedLibraryID": library_id, "actualLibraryID": operation.libraryID},
            )

        canonical_payload = self._canonical_payload_json(operation.payload)
        payload_hash = self._payload_hash(canonical_payload)
        committed_at = datetime.now(timezone.utc)

        try:
            with self._session_factory() as session, session.begin():
                existing = session.execute(
                    select(LedgerEventRecord).where(LedgerEventRecord.op_id == str(operation.opID))
                ).scalar_one_or_none()
                if existing is not None:
                    if existing.payload_hash == payload_hash and self._same_operation_identity(existing, operation):
                        return AcceptedOperation(operation.opID, existing.global_seq, existing.committed_at, existing.payload_hash)
                    if existing.payload_hash == payload_hash:
                        self._insert_conflict(
                            session,
                            library_id=library_id,
                            entity_type=operation.entityType.value,
                            entity_id=operation.entityID,
                            conflict_type="duplicate_op_id_identity_mismatch",
                            left_op_id=existing.op_id,
                            right_op_id=str(operation.opID),
                            detail={
                                "existingLibraryID": existing.library_id,
                                "libraryID": library_id,
                                "existingDeviceID": existing.device_id,
                                "deviceID": operation.deviceID,
                                "existingDeviceSequence": existing.device_seq,
                                "deviceSequence": operation.deviceSequence,
                                "existingEntityType": existing.entity_type,
                                "entityType": operation.entityType.value,
                                "existingEntityID": existing.entity_id,
                                "entityID": operation.entityID,
                                "existingOpType": existing.op_type,
                                "opType": operation.opType.value,
                                "globalSeq": existing.global_seq,
                            },
                        )
                        return ConflictOperation(
                            opID=operation.opID,
                            conflictType="duplicate_op_id_identity_mismatch",
                            detail={
                                "existingLibraryID": existing.library_id,
                                "libraryID": library_id,
                                "existingDeviceID": existing.device_id,
                                "deviceID": operation.deviceID,
                                "existingDeviceSequence": existing.device_seq,
                                "deviceSequence": operation.deviceSequence,
                                "existingEntityType": existing.entity_type,
                                "entityType": operation.entityType.value,
                                "existingEntityID": existing.entity_id,
                                "entityID": operation.entityID,
                                "existingOpType": existing.op_type,
                                "opType": operation.opType.value,
                                "globalSeq": existing.global_seq,
                            },
                        )
                    self._insert_conflict(
                        session,
                        library_id=library_id,
                        entity_type=operation.entityType.value,
                        entity_id=operation.entityID,
                        conflict_type="duplicate_op_id_payload_mismatch",
                        left_op_id=existing.op_id,
                        right_op_id=str(operation.opID),
                        detail={
                            "payloadHash": payload_hash,
                            "existingPayloadHash": existing.payload_hash,
                            "globalSeq": existing.global_seq,
                        },
                    )
                    return ConflictOperation(
                        opID=operation.opID,
                        conflictType="duplicate_op_id_payload_mismatch",
                        detail={
                            "payloadHash": payload_hash,
                            "existingPayloadHash": existing.payload_hash,
                            "globalSeq": existing.global_seq,
                        },
                    )

                device_seq_existing = session.execute(
                    select(LedgerEventRecord).where(
                        LedgerEventRecord.library_id == library_id,
                        LedgerEventRecord.device_id == operation.deviceID,
                        LedgerEventRecord.device_seq == operation.deviceSequence,
                    )
                ).scalar_one_or_none()
                if device_seq_existing is not None and device_seq_existing.op_id != str(operation.opID):
                    self._insert_conflict(
                        session,
                        library_id=library_id,
                        entity_type=operation.entityType.value,
                        entity_id=operation.entityID,
                        conflict_type="duplicate_device_sequence",
                        left_op_id=device_seq_existing.op_id,
                        right_op_id=str(operation.opID),
                        detail={
                            "deviceID": operation.deviceID,
                            "deviceSequence": operation.deviceSequence,
                            "existingGlobalSeq": device_seq_existing.global_seq,
                        },
                    )
                    return ConflictOperation(
                        opID=operation.opID,
                        conflictType="duplicate_device_sequence",
                        detail={
                            "deviceID": operation.deviceID,
                            "deviceSequence": operation.deviceSequence,
                            "existingGlobalSeq": device_seq_existing.global_seq,
                        },
                    )

                global_seq = self._reserve_sequence(session, library_id)
                session.add(
                    LedgerEventRecord(
                        library_id=library_id,
                        global_seq=global_seq,
                        op_id=str(operation.opID),
                        device_id=operation.deviceID,
                        device_seq=operation.deviceSequence,
                        hybrid_logical_time=operation.hybridLogicalTime.model_dump(mode="json", by_alias=True),
                        actor_id=operation.actorID,
                        entity_type=operation.entityType.value,
                        entity_id=operation.entityID,
                        op_type=operation.opType.value,
                        payload_json=json.loads(canonical_payload),
                        payload_hash=payload_hash,
                        base_version=operation.baseVersion,
                        committed_at=committed_at,
                    )
                )
                self._apply_projection(session, library_id, global_seq, committed_at, operation)
                return AcceptedOperation(operation.opID, global_seq, committed_at, payload_hash)
        except IntegrityError:
            conflict = self._resolve_integrity_error_conflict(library_id, operation, payload_hash)
            if conflict is not None:
                self._persist_conflict(
                    library_id=library_id,
                    conflict=conflict,
                    entity_type=operation.entityType.value,
                    entity_id=operation.entityID,
                )
                return conflict
            return ConflictOperation(
                opID=operation.opID,
                conflictType="unique_constraint_violation",
                detail={"libraryID": library_id},
            )

    def _reserve_sequence(self, session: Session, library_id: str) -> int:
        bind = session.get_bind()
        if bind is not None and bind.dialect.name == "postgresql":
            stmt = (
                pg_insert(LedgerSequenceCounter)
                .values(library_id=library_id, next_global_seq=2)
                .on_conflict_do_update(
                    index_elements=[LedgerSequenceCounter.library_id],
                    set_={"next_global_seq": LedgerSequenceCounter.next_global_seq + 1},
                )
                .returning((LedgerSequenceCounter.next_global_seq - 1).label("global_seq"))
            )
            return int(session.execute(stmt).scalar_one())

        counter = session.get(LedgerSequenceCounter, library_id)
        if counter is None:
            counter = LedgerSequenceCounter(library_id=library_id, next_global_seq=1)
            session.add(counter)
            session.flush()
        global_seq = counter.next_global_seq
        counter.next_global_seq += 1
        return global_seq

    def _current_cursor(self, library_id: str) -> int:
        with self._session_factory() as session:
            result = session.execute(
                select(func.max(LedgerEventRecord.global_seq)).where(LedgerEventRecord.library_id == library_id)
            ).scalar_one()
        return int(result or 0)

    def _apply_projection(
        self,
        session: Session,
        library_id: str,
        global_seq: int,
        committed_at: datetime,
        operation: OperationLedgerEntry,
    ) -> None:
        if operation.opType == LedgerOperationType.derivativeDeclared and operation.payload.derivativeDeclared is not None:
            payload = operation.payload.derivativeDeclared
            derivative = payload.derivative
            session.merge(
                DerivativeObjectRecord(
                    library_id=library_id,
                    asset_id=str(payload.assetID),
                    role=derivative.role.value,
                    file_object=derivative.fileObject.model_dump(mode="json", by_alias=True),
                    s3_bucket=derivative.s3Object.bucket,
                    s3_key=derivative.s3Object.key,
                    s3_etag=derivative.s3Object.eTag,
                    pixel_width=derivative.pixelSize.width,
                    pixel_height=derivative.pixelSize.height,
                    declared_event_seq=global_seq,
                    updated_at=committed_at,
                )
            )
        elif operation.opType == LedgerOperationType.originalArchiveReceiptRecorded and operation.payload.originalArchiveReceiptRecorded is not None:
            payload = operation.payload.originalArchiveReceiptRecorded
            session.merge(
                ArchiveReceiptRecord(
                    library_id=library_id,
                    asset_id=str(payload.assetID),
                    receipt_event_seq=global_seq,
                    file_object=payload.fileObject.model_dump(mode="json", by_alias=True),
                    server_placement=payload.serverPlacement.model_dump(mode="json", by_alias=True),
                    committed_at=committed_at,
                )
            )

    def _insert_conflict(
        self,
        session: Session,
        library_id: str,
        entity_type: str,
        entity_id: str,
        conflict_type: str,
        left_op_id: str | None,
        right_op_id: str | None,
        detail: dict[str, Any],
    ) -> None:
        session.add(
            SyncConflictRecord(
                id=str(uuid4()),
                library_id=library_id,
                entity_type=entity_type,
                entity_id=entity_id,
                conflict_type=conflict_type,
                left_op_id=left_op_id,
                right_op_id=right_op_id,
                detail=detail,
                created_at=datetime.now(timezone.utc),
            )
        )

    def _resolve_integrity_error_conflict(
        self,
        library_id: str,
        operation: OperationLedgerEntry,
        payload_hash: str,
    ) -> AcceptedOperation | ConflictOperation | None:
        with self._session_factory() as session:
            existing = session.execute(
                select(LedgerEventRecord).where(LedgerEventRecord.op_id == str(operation.opID))
            ).scalar_one_or_none()
            if existing is not None and existing.payload_hash == payload_hash and self._same_operation_identity(existing, operation):
                return AcceptedOperation(operation.opID, existing.global_seq, existing.committed_at, existing.payload_hash)
            if existing is not None and existing.payload_hash == payload_hash:
                return ConflictOperation(
                    opID=operation.opID,
                    conflictType="duplicate_op_id_identity_mismatch",
                    detail={
                        "existingLibraryID": existing.library_id,
                        "libraryID": library_id,
                        "existingDeviceID": existing.device_id,
                        "deviceID": operation.deviceID,
                        "existingDeviceSequence": existing.device_seq,
                        "deviceSequence": operation.deviceSequence,
                        "existingEntityType": existing.entity_type,
                        "entityType": operation.entityType.value,
                        "existingEntityID": existing.entity_id,
                        "entityID": operation.entityID,
                        "existingOpType": existing.op_type,
                        "opType": operation.opType.value,
                        "globalSeq": existing.global_seq,
                    },
                )
            if existing is not None:
                return ConflictOperation(
                    opID=operation.opID,
                    conflictType="duplicate_op_id_payload_mismatch",
                    detail={
                        "payloadHash": payload_hash,
                        "existingPayloadHash": existing.payload_hash,
                        "globalSeq": existing.global_seq,
                    },
                )
            device_seq_conflict = self._duplicate_device_sequence_conflict(session, library_id, operation)
            if device_seq_conflict is not None:
                return device_seq_conflict
        return None

    def _same_operation_identity(self, existing: LedgerEventRecord, operation: OperationLedgerEntry) -> bool:
        return (
            existing.library_id == operation.libraryID
            and existing.device_id == operation.deviceID
            and existing.device_seq == operation.deviceSequence
            and existing.actor_id == operation.actorID
            and existing.entity_type == operation.entityType.value
            and existing.entity_id == operation.entityID
            and existing.op_type == operation.opType.value
            and existing.base_version == operation.baseVersion
            and existing.hybrid_logical_time == operation.hybridLogicalTime.model_dump(mode="json", by_alias=True)
        )

    def _persist_conflict(
        self,
        library_id: str,
        conflict: ConflictOperation,
        entity_type: str,
        entity_id: str,
    ) -> None:
        with self._session_factory() as session, session.begin():
            self._insert_conflict(
                session,
                library_id=library_id,
                entity_type=entity_type,
                entity_id=entity_id,
                conflict_type=conflict.conflictType,
                left_op_id=None,
                right_op_id=str(conflict.opID),
                detail=conflict.detail,
            )

    def _row_to_committed_operation(self, row: LedgerEventRecord) -> CommittedOperationLedgerEntry:
        payload = OperationPayload.model_validate(row.payload_json)
        return CommittedOperationLedgerEntry(
            opID=UUID(str(row.op_id)),
            libraryID=row.library_id,
            deviceID=row.device_id,
            deviceSequence=row.device_seq,
            hybridLogicalTime=row.hybrid_logical_time,
            actorID=row.actor_id,
            entityType=LedgerEntityType(row.entity_type),
            entityID=row.entity_id,
            opType=LedgerOperationType(row.op_type),
            payload=payload,
            baseVersion=row.base_version,
            createdAt=row.committed_at,
            globalSeq=row.global_seq,
            payloadHash=row.payload_hash,
            committedAt=row.committed_at,
        )

    def _canonical_payload_json(self, payload: OperationPayload) -> str:
        payload_obj = payload.model_dump(mode="json", by_alias=True, exclude_none=True)
        return json.dumps(payload_obj, sort_keys=True, separators=(",", ":"))

    def _payload_hash(self, canonical_payload_json: str) -> str:
        return hashlib.sha256(canonical_payload_json.encode("utf-8")).hexdigest()

    def validate_operation_semantics(self, operation: OperationLedgerEntry) -> None:
        expected_case = {
            LedgerOperationType.assetSnapshotDeclared: "assetSnapshotDeclared",
            LedgerOperationType.filePlacementSnapshotDeclared: "filePlacementSnapshotDeclared",
            LedgerOperationType.metadataSet: "metadataSet",
            LedgerOperationType.tagsUpdated: "tagsUpdated",
            LedgerOperationType.moveToTrash: "moveToTrash",
            LedgerOperationType.restoreFromTrash: "restoreFromTrash",
            LedgerOperationType.importedOriginalDeclared: "importedOriginalDeclared",
            LedgerOperationType.archiveRequested: "archiveRequested",
            LedgerOperationType.originalArchiveReceiptRecorded: "originalArchiveReceiptRecorded",
            LedgerOperationType.derivativeDeclared: "derivativeDeclared",
        }[operation.opType]

        payload_case = self._operation_payload_case(operation)
        if payload_case != expected_case:
            raise OperationSemanticError(
                code="payload_case_mismatch",
                message=f"operation payload case must be {expected_case}, got {payload_case or 'none'}",
            )

        expected_entity_type = {
            LedgerOperationType.assetSnapshotDeclared: LedgerEntityType.asset,
            LedgerOperationType.filePlacementSnapshotDeclared: LedgerEntityType.filePlacement,
            LedgerOperationType.metadataSet: LedgerEntityType.asset,
            LedgerOperationType.tagsUpdated: LedgerEntityType.asset,
            LedgerOperationType.moveToTrash: LedgerEntityType.asset,
            LedgerOperationType.restoreFromTrash: LedgerEntityType.asset,
            LedgerOperationType.importedOriginalDeclared: LedgerEntityType.fileObject,
            LedgerOperationType.archiveRequested: LedgerEntityType.asset,
            LedgerOperationType.originalArchiveReceiptRecorded: LedgerEntityType.filePlacement,
            LedgerOperationType.derivativeDeclared: LedgerEntityType.derivativeObject,
        }[operation.opType]
        if operation.entityType != expected_entity_type:
            raise OperationSemanticError(
                code="entity_type_mismatch",
                message=f"operation entityType must be {expected_entity_type.value}",
            )

        expected_entity_id = self._expected_entity_id(operation)
        if expected_entity_id is not None and not self._entity_id_matches(operation, expected_entity_id):
            raise OperationSemanticError(
                code="entity_id_mismatch",
                message="operation entityID does not match payload asset/file identity",
            )

        if operation.opType == LedgerOperationType.derivativeDeclared and operation.payload.derivativeDeclared is not None:
            payload = operation.payload.derivativeDeclared
            if payload.assetID != payload.derivative.assetID:
                raise OperationSemanticError(
                    code="derivative_asset_id_mismatch",
                    message="derivativeDeclared payload assetID must match nested derivative.assetID",
                )

    def _operation_payload_case(self, operation: OperationLedgerEntry) -> str | None:
        payload = operation.payload
        for field_name in (
            "assetSnapshotDeclared",
            "filePlacementSnapshotDeclared",
            "metadataSet",
            "tagsUpdated",
            "moveToTrash",
            "restoreFromTrash",
            "importedOriginalDeclared",
            "archiveRequested",
            "originalArchiveReceiptRecorded",
            "derivativeDeclared",
        ):
            if getattr(payload, field_name) is not None:
                return field_name
        return None

    def _expected_entity_id(self, operation: OperationLedgerEntry) -> str | None:
        payload = operation.payload
        if payload.assetSnapshotDeclared is not None:
            return str(payload.assetSnapshotDeclared.snapshot.assetID)
        if payload.filePlacementSnapshotDeclared is not None:
            return str(payload.filePlacementSnapshotDeclared.assetID)
        if payload.metadataSet is not None:
            return str(payload.metadataSet.assetID)
        if payload.tagsUpdated is not None:
            return str(payload.tagsUpdated.assetID)
        if payload.moveToTrash is not None:
            return str(payload.moveToTrash.assetID)
        if payload.restoreFromTrash is not None:
            return str(payload.restoreFromTrash.assetID)
        if payload.importedOriginalDeclared is not None:
            return self._file_object_stable_key(payload.importedOriginalDeclared.fileObject)
        if payload.archiveRequested is not None:
            return str(payload.archiveRequested.assetID)
        if payload.originalArchiveReceiptRecorded is not None:
            return str(payload.originalArchiveReceiptRecorded.assetID)
        if payload.derivativeDeclared is not None:
            derivative = payload.derivativeDeclared.derivative
            return f"{payload.derivativeDeclared.assetID}:{derivative.role.value}:{derivative.fileObject.contentHash}"
        return None

    def _entity_id_matches(self, operation: OperationLedgerEntry, expected_entity_id: str) -> bool:
        if operation.opType in {
            LedgerOperationType.assetSnapshotDeclared,
            LedgerOperationType.filePlacementSnapshotDeclared,
            LedgerOperationType.metadataSet,
            LedgerOperationType.tagsUpdated,
            LedgerOperationType.moveToTrash,
            LedgerOperationType.restoreFromTrash,
            LedgerOperationType.archiveRequested,
            LedgerOperationType.originalArchiveReceiptRecorded,
            LedgerOperationType.derivativeDeclared,
        }:
            return operation.entityID.lower() == expected_entity_id.lower()
        return operation.entityID == expected_entity_id

    def _file_object_stable_key(self, file_object: FileObjectID) -> str:
        return f"{file_object.role.value}:{file_object.sizeBytes}:{file_object.contentHash}"

    def _duplicate_device_sequence_conflict(
        self,
        session: Session,
        library_id: str,
        operation: OperationLedgerEntry,
    ) -> ConflictOperation | None:
        device_seq_existing = session.execute(
            select(LedgerEventRecord).where(
                LedgerEventRecord.library_id == library_id,
                LedgerEventRecord.device_id == operation.deviceID,
                LedgerEventRecord.device_seq == operation.deviceSequence,
            )
        ).scalar_one_or_none()
        if device_seq_existing is None:
            return None

        self._insert_conflict(
            session,
            library_id=library_id,
            entity_type=operation.entityType.value,
            entity_id=operation.entityID,
            conflict_type="duplicate_device_sequence",
            left_op_id=device_seq_existing.op_id,
            right_op_id=str(operation.opID),
            detail={
                "deviceID": operation.deviceID,
                "deviceSequence": operation.deviceSequence,
                "existingGlobalSeq": device_seq_existing.global_seq,
            },
        )
        return ConflictOperation(
            opID=operation.opID,
            conflictType="duplicate_device_sequence",
            detail={
                "deviceID": operation.deviceID,
                "deviceSequence": operation.deviceSequence,
                "existingGlobalSeq": device_seq_existing.global_seq,
            },
        )
