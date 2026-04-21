from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer, model_validator


class CamelModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class LedgerEntityType(str, Enum):
    asset = "asset"
    fileObject = "file_object"
    filePlacement = "file_placement"
    derivativeObject = "derivative_object"


class LedgerOperationType(str, Enum):
    assetSnapshotDeclared = "asset_snapshot_declared"
    filePlacementSnapshotDeclared = "file_placement_snapshot_declared"
    metadataSet = "metadata_set"
    tagsUpdated = "tags_updated"
    moveToTrash = "move_to_trash"
    restoreFromTrash = "restore_from_trash"
    importedOriginalDeclared = "imported_original_declared"
    archiveRequested = "archive_requested"
    originalArchiveReceiptRecorded = "original_archive_receipt_recorded"
    derivativeDeclared = "derivative_declared"


class AssetMetadataField(str, Enum):
    rating = "rating"
    flagState = "flag_state"
    colorLabel = "color_label"
    caption = "caption"


class FileRole(str, Enum):
    rawOriginal = "raw_original"
    jpegOriginal = "jpeg_original"
    sidecar = "sidecar"
    preview = "preview"
    thumbnail = "thumbnail"
    export = "export"


class StorageKind(str, Enum):
    local = "local"
    nas = "nas"
    externalDrive = "external_drive"
    cloudPreview = "cloud_preview"


class AuthorityRole(str, Enum):
    canonical = "canonical"
    workingCopy = "working_copy"
    sourceCopy = "source_copy"
    cache = "cache"


class Availability(str, Enum):
    online = "online"
    offline = "offline"
    missing = "missing"


class DerivativeRole(str, Enum):
    thumbnail = "thumbnail"
    preview = "preview"


class AssetFlagState(str, Enum):
    unflagged = "unflagged"
    picked = "picked"
    rejected = "rejected"


class AssetColorLabel(str, Enum):
    red = "red"
    yellow = "yellow"
    green = "green"
    blue = "blue"
    purple = "purple"


class LedgerValue(CamelModel):
    intValue: int | None = Field(default=None, alias="int")
    stringValue: str | None = Field(default=None, alias="string")
    null: dict[str, Any] | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_null_case(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        if "int" in value:
            int_value = value["int"]
            if isinstance(int_value, dict):
                int_value = int_value.get("_0", int_value)
            return {"intValue": int_value}

        if "string" in value:
            string_value = value["string"]
            if isinstance(string_value, dict):
                string_value = string_value.get("_0", string_value)
            return {"stringValue": string_value}

        if "null" in value:
            return {"null": {}}

        return value

    @model_validator(mode="after")
    def validate_exclusive(self) -> "LedgerValue":
        active = sum(item is not None for item in (self.intValue, self.stringValue, self.null))
        if active != 1:
            raise ValueError("LedgerValue must contain exactly one case")
        return self

    @field_serializer("intValue", when_used="json")
    def serialize_int(self, value: int | None) -> dict[str, int] | None:
        if value is None:
            return None
        return {"_0": value}

    @field_serializer("stringValue", when_used="json")
    def serialize_string(self, value: str | None) -> dict[str, str] | None:
        if value is None:
            return None
        return {"_0": value}

    @field_serializer("null", when_used="json")
    def serialize_null(self, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is None:
            return None
        return {}


class HybridLogicalTime(CamelModel):
    wallTimeMilliseconds: int
    counter: int
    nodeID: str


class FileObjectID(CamelModel):
    contentHash: str
    sizeBytes: int
    role: FileRole


class FilePlacement(CamelModel):
    fileObjectID: FileObjectID
    holderID: str
    storageKind: StorageKind
    authorityRole: AuthorityRole
    availability: Availability


class PixelSize(CamelModel):
    width: int
    height: int


class S3ObjectRef(CamelModel):
    bucket: str
    key: str
    eTag: str | None = None


class DerivativeObject(CamelModel):
    assetID: UUID
    role: DerivativeRole
    fileObject: FileObjectID
    s3Object: S3ObjectRef
    pixelSize: PixelSize


class AssetSnapshot(CamelModel):
    assetID: UUID
    captureTime: datetime | None = None
    cameraMake: str
    cameraModel: str
    lensModel: str
    originalFilename: str
    contentFingerprint: str
    metadataFingerprint: str
    rating: int
    flagState: AssetFlagState
    colorLabel: AssetColorLabel | None = None
    tags: list[str]
    createdAt: datetime
    updatedAt: datetime


class AssetSnapshotDeclaredPayload(CamelModel):
    snapshot: AssetSnapshot


class FilePlacementSnapshotDeclaredPayload(CamelModel):
    assetID: UUID
    fileObject: FileObjectID
    placement: FilePlacement


class MetadataSetPayload(CamelModel):
    assetID: UUID
    field: AssetMetadataField
    value: LedgerValue


class TagsUpdatedPayload(CamelModel):
    assetID: UUID
    add: set[str] = Field(default_factory=set)
    remove: set[str] = Field(default_factory=set)

    @field_serializer("add", "remove")
    def serialize_tags(self, values: set[str]) -> list[str]:
        return sorted(values)


class MoveToTrashPayload(CamelModel):
    assetID: UUID
    reason: str


class RestoreFromTrashPayload(CamelModel):
    assetID: UUID


class ImportedOriginalDeclaredPayload(CamelModel):
    assetID: UUID
    fileObject: FileObjectID
    placement: FilePlacement


class ArchiveRequestedPayload(CamelModel):
    assetID: UUID


class OriginalArchiveReceiptRecordedPayload(CamelModel):
    assetID: UUID
    fileObject: FileObjectID
    serverPlacement: FilePlacement


class DerivativeDeclaredPayload(CamelModel):
    assetID: UUID
    derivative: DerivativeObject


class OperationPayload(CamelModel):
    assetSnapshotDeclared: AssetSnapshotDeclaredPayload | None = None
    filePlacementSnapshotDeclared: FilePlacementSnapshotDeclaredPayload | None = None
    metadataSet: MetadataSetPayload | None = None
    tagsUpdated: TagsUpdatedPayload | None = None
    moveToTrash: MoveToTrashPayload | None = None
    restoreFromTrash: RestoreFromTrashPayload | None = None
    importedOriginalDeclared: ImportedOriginalDeclaredPayload | None = None
    archiveRequested: ArchiveRequestedPayload | None = None
    originalArchiveReceiptRecorded: OriginalArchiveReceiptRecordedPayload | None = None
    derivativeDeclared: DerivativeDeclaredPayload | None = None

    @model_validator(mode="after")
    def validate_exclusive(self) -> "OperationPayload":
        active = sum(
            item is not None
            for item in (
                self.assetSnapshotDeclared,
                self.filePlacementSnapshotDeclared,
                self.metadataSet,
                self.tagsUpdated,
                self.moveToTrash,
                self.restoreFromTrash,
                self.importedOriginalDeclared,
                self.archiveRequested,
                self.originalArchiveReceiptRecorded,
                self.derivativeDeclared,
            )
        )
        if active != 1:
            raise ValueError("OperationPayload must contain exactly one case")
        return self


class OperationSemanticError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class OperationLedgerEntry(CamelModel):
    opID: UUID
    libraryID: str
    deviceID: str
    deviceSequence: int
    hybridLogicalTime: HybridLogicalTime
    actorID: str
    entityType: LedgerEntityType
    entityID: str
    opType: LedgerOperationType
    payload: OperationPayload
    baseVersion: str | None = None
    createdAt: datetime
    globalSeq: int | None = None
    payloadHash: str | None = None
    committedAt: datetime | None = None


class CommittedOperationLedgerEntry(OperationLedgerEntry):
    globalSeq: int
    payloadHash: str
    committedAt: datetime


class SyncOpsUploadRequest(CamelModel):
    operations: list[OperationLedgerEntry]


class SyncOpsFetchResponse(CamelModel):
    operations: list[CommittedOperationLedgerEntry]
    cursor: str
    hasMore: bool


class SyncOpsAccepted(CamelModel):
    opID: UUID
    globalSeq: int
    status: Literal["committed"]


class SyncOpsConflict(CamelModel):
    opID: UUID
    conflictType: str
    detail: dict[str, Any]


class SyncOpsUploadResponse(CamelModel):
    accepted: list[SyncOpsAccepted]
    cursor: str
    conflicts: list[SyncOpsConflict] = Field(default_factory=list)


class DeviceHeartbeatRequest(CamelModel):
    deviceID: str
    libraryID: str
    actorID: str | None = None
    appVersion: str | None = None
    localPendingCount: int | None = None
    placementSummary: list[FilePlacement] | None = None
    placements: list[FilePlacement] = Field(default_factory=list)
    sentAt: datetime | None = None
    lastUploadedDeviceSeq: int | None = None
    lastPullCursor: int | None = None


class DeviceHeartbeatResponse(CamelModel):
    libraryID: str
    deviceID: str
    actorID: str
    lastSeenAt: datetime
    lastUploadedDeviceSeq: int
    lastPullCursor: int
    capabilities: dict[str, Any]


class ArchiveReceiptRequest(CamelModel):
    operation: OperationLedgerEntry


class ArchiveReceiptResponse(CamelModel):
    status: Literal["committed"]
    globalSeq: int
    assetID: UUID


class DerivativeUploadRequest(CamelModel):
    libraryID: str
    assetID: UUID
    role: DerivativeRole
    fileObject: FileObjectID
    pixelSize: PixelSize


class DerivativeUploadResponse(CamelModel):
    libraryID: str
    assetID: UUID
    role: DerivativeRole
    fileObject: FileObjectID
    s3Object: S3ObjectRef
    uploadURL: str


class DerivativeMetadataResponse(CamelModel):
    derivative: DerivativeObject
    downloadURL: str


class ErrorResponse(CamelModel):
    code: str
    message: str
    detail: dict[str, Any] | None = None
