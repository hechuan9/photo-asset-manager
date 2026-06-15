from __future__ import annotations

from contextlib import asynccontextmanager
import os
from pathlib import Path
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status

from .db import ControlPlaneStore, FilesystemDerivativeStorage, create_engine_for_url, initialize_database, make_session_factory
from .schemas import (
    ArchiveReceiptRequest,
    ArchiveReceiptResponse,
    DerivativeMetadataResponse,
    DerivativeRole,
    DerivativeUploadRequest,
    DerivativeUploadResponse,
    DeviceHeartbeatRequest,
    DeviceHeartbeatResponse,
    OperationSemanticError,
    SyncOpsFetchResponse,
    SyncOpsUploadRequest,
    SyncOpsUploadResponse,
)


def create_app(
    database_url: str | None = None,
    derivative_storage: FilesystemDerivativeStorage | None = None,
    trusted_device_ids: set[str] | None = None,
    auto_create_schema: bool | None = None,
) -> FastAPI:
    if database_url is None:
        database_url = os.getenv("CONTROL_PLANE_DATABASE_URL")
        if database_url is None:
            if os.getenv("CONTROL_PLANE_ALLOW_SQLITE_DEV") == "1":
                database_url = "sqlite+pysqlite:///./control_plane.sqlite"
            else:
                raise RuntimeError(
                    "CONTROL_PLANE_DATABASE_URL is required; set CONTROL_PLANE_ALLOW_SQLITE_DEV=1 for local SQLite dev"
                )

    if auto_create_schema is None:
        auto_create_schema = _resolve_auto_create_schema(database_url)

    if derivative_storage is None:
        derivative_storage = _load_derivative_storage_from_env(required=os.getenv("CONTROL_PLANE_ALLOW_SQLITE_DEV") != "1")

    engine = create_engine_for_url(database_url)
    if auto_create_schema:
        initialize_database(engine)
    session_factory = make_session_factory(engine)
    store = ControlPlaneStore(
        session_factory=session_factory,
        derivative_storage=derivative_storage,
        trusted_device_ids=trusted_device_ids,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield

    app = FastAPI(
        title="Keeps Control Plane",
        version="0.1.0",
        description="NAS-hosted authoritative event store control-plane API.",
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.engine = engine
    app.state.auto_create_schema = auto_create_schema
    app.state.derivative_storage = derivative_storage

    def get_store() -> ControlPlaneStore:
        return app.state.store

    @app.get("/healthz", tags=["system"])
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/libraries/{libraryID}/ops",
        response_model=SyncOpsUploadResponse,
        response_model_exclude_none=True,
        tags=["sync"],
    )
    def upload_operations(
        libraryID: str,
        request: SyncOpsUploadRequest,
        store: ControlPlaneStore = Depends(get_store),
    ) -> SyncOpsUploadResponse:
        try:
            outcome = store.append_operations(libraryID, request)
        except OperationSemanticError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail={"code": exc.code, "message": exc.message},
            ) from exc
        response = SyncOpsUploadResponse(
            accepted=[
                {"opID": item.opID, "globalSeq": item.globalSeq, "status": "committed"}
                for item in outcome.accepted
            ],
            cursor=str(outcome.cursor),
            conflicts=[
                {"opID": item.opID, "conflictType": item.conflictType, "detail": item.detail}
                for item in outcome.conflicts
            ],
        )
        if outcome.conflicts:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=response.model_dump(mode="json", by_alias=True, exclude_none=True))
        return response

    @app.get("/libraries/{libraryID}/ops", response_model=SyncOpsFetchResponse, response_model_exclude_none=True, tags=["sync"])
    def fetch_operations(
        libraryID: str,
        store: ControlPlaneStore = Depends(get_store),
        after: str | None = Query(default=None),
    ) -> SyncOpsFetchResponse:
        try:
            cursor = int(after or "0")
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "invalid_cursor", "message": "after must be an integer cursor"}) from exc
        return store.fetch_operations(libraryID, cursor)

    @app.post("/devices/{deviceID}/heartbeat", response_model=DeviceHeartbeatResponse, response_model_exclude_none=True, tags=["devices"])
    def device_heartbeat(
        deviceID: str,
        request: DeviceHeartbeatRequest,
        store: ControlPlaneStore = Depends(get_store),
    ) -> DeviceHeartbeatResponse:
        if request.deviceID != deviceID:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "device_mismatch", "message": "path deviceID must match body deviceID"})
        return store.upsert_heartbeat(request)

    @app.post("/derivatives/uploads", response_model=DerivativeUploadResponse, response_model_exclude_none=True, tags=["derivatives"])
    def derivative_upload(
        request: DerivativeUploadRequest,
        store: ControlPlaneStore = Depends(get_store),
    ) -> DerivativeUploadResponse:
        try:
            return store.create_derivative_upload(request)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail={"code": "unsupported_derivative_role", "message": str(exc)}) from exc

    @app.get("/derivatives/{assetID}", response_model=DerivativeMetadataResponse, response_model_exclude_none=True, tags=["derivatives"])
    def derivative_metadata(
        assetID: UUID,
        store: ControlPlaneStore = Depends(get_store),
        role: DerivativeRole = Query(...),
        libraryID: str | None = Query(default=None),
    ) -> DerivativeMetadataResponse:
        try:
            return store.get_derivative_metadata(libraryID, assetID, role)
        except LookupError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"code": "derivative_not_found", "message": "derivative metadata not declared"}) from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "library_required", "message": str(exc)}) from exc

    @app.delete("/libraries/{libraryID}/derivatives/{assetID}", status_code=status.HTTP_204_NO_CONTENT, tags=["derivatives"])
    def delete_derivative(
        libraryID: str,
        assetID: UUID,
        role: DerivativeRole = Query(...),
        store: ControlPlaneStore = Depends(get_store),
    ) -> None:
        store.delete_derivative(libraryID, assetID, role)

    @app.put("/derivatives/local-upload/{token}", status_code=status.HTTP_204_NO_CONTENT, tags=["derivatives"])
    async def local_derivative_upload(token: str, request: Request) -> None:
        storage = app.state.derivative_storage
        if storage is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"code": "local_derivative_storage_disabled"})
        try:
            bucket, key = storage.decode_token(token)
            storage.write_object(bucket, key, await request.body())
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "invalid_derivative_storage_token", "message": str(exc)}) from exc

    @app.get("/derivatives/local-download/{token}", tags=["derivatives"])
    def local_derivative_download(token: str) -> Response:
        storage = app.state.derivative_storage
        if storage is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"code": "local_derivative_storage_disabled"})
        try:
            bucket, key = storage.decode_token(token)
            content = storage.read_object(bucket, key)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"code": "derivative_object_not_found"}) from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "invalid_derivative_storage_token", "message": str(exc)}) from exc
        return Response(content=content, media_type="application/octet-stream")

    @app.post("/archive/receipts", response_model=ArchiveReceiptResponse, response_model_exclude_none=True, tags=["archive"])
    def archive_receipt(
        request: ArchiveReceiptRequest,
        store: ControlPlaneStore = Depends(get_store),
    ) -> ArchiveReceiptResponse:
        try:
            return store.record_archive_receipt(request)
        except OperationSemanticError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail={"code": exc.code, "message": exc.message}) from exc
        except PermissionError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail={"code": "archive_receipt_forbidden", "message": str(exc)}) from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"code": "invalid_archive_receipt", "message": str(exc)}) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={"code": "archive_receipt_conflict", "message": str(exc)}) from exc

    return app


def _load_default_app() -> FastAPI | None:
    database_url = os.getenv("CONTROL_PLANE_DATABASE_URL")
    trusted_device_ids = _load_trusted_device_ids()
    derivative_storage = _load_derivative_storage_from_env(required=database_url is not None)
    if database_url:
        return create_app(
            database_url=database_url,
            derivative_storage=derivative_storage,
            trusted_device_ids=trusted_device_ids,
            auto_create_schema=None,
        )
    if os.getenv("CONTROL_PLANE_ALLOW_SQLITE_DEV") == "1":
        return create_app(
            database_url="sqlite+pysqlite:///./control_plane.sqlite",
            derivative_storage=derivative_storage,
            trusted_device_ids=trusted_device_ids,
            auto_create_schema=True,
        )
    return None


def _load_trusted_device_ids() -> set[str]:
    raw = os.getenv("CONTROL_PLANE_TRUSTED_DEVICE_IDS", "")
    return {item.strip() for item in raw.split(",") if item.strip()}


def _load_derivative_storage_from_env(required: bool = False) -> FilesystemDerivativeStorage | None:
    backend = os.getenv("DERIVATIVE_STORAGE_BACKEND", "filesystem").strip().lower()
    if backend != "filesystem":
        raise RuntimeError(f"unsupported DERIVATIVE_STORAGE_BACKEND: {backend}")

    keeps_root = os.getenv("KEEPS_ROOT")
    if not keeps_root:
        if required:
            raise RuntimeError("KEEPS_ROOT is required")
        return None
    public_base_url = os.getenv("CONTROL_PLANE_PUBLIC_BASE_URL")
    if not public_base_url:
        if required:
            raise RuntimeError("CONTROL_PLANE_PUBLIC_BASE_URL is required")
        return None

    original_root = os.getenv("ORIGINAL_ROOT")
    return FilesystemDerivativeStorage(
        keeps_root=Path(keeps_root),
        public_base_url=public_base_url,
        original_root=Path(original_root) if original_root else None,
    )


def _resolve_auto_create_schema(database_url: str) -> bool:
    explicit = os.getenv("CONTROL_PLANE_AUTO_CREATE_SCHEMA")
    if explicit is not None:
        return explicit == "1"
    return database_url.startswith("sqlite")


app = _load_default_app()
