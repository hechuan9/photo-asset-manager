from __future__ import annotations

from contextlib import asynccontextmanager
import json
import os
from typing import Annotated
from urllib.parse import quote
from uuid import UUID

import boto3
from fastapi import Depends, FastAPI, HTTPException, Query, status

from .db import ControlPlaneStore, DerivativePresigner, create_engine_for_url, initialize_database, make_session_factory
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
    derivative_bucket: str | None = None,
    derivative_presigner: DerivativePresigner | None = None,
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

    derivative_bucket = derivative_bucket or os.getenv("DERIVATIVE_BUCKET_NAME")
    if derivative_bucket is None:
        if os.getenv("CONTROL_PLANE_ALLOW_SQLITE_DEV") == "1":
            derivative_bucket = "photo-asset-manager-dev-derivatives"
        else:
            raise RuntimeError("DERIVATIVE_BUCKET_NAME is required")

    engine = create_engine_for_url(database_url)
    if auto_create_schema:
        initialize_database(engine)
    session_factory = make_session_factory(engine)
    store = ControlPlaneStore(
        session_factory=session_factory,
        derivative_bucket=derivative_bucket,
        derivative_presigner=derivative_presigner,
        trusted_device_ids=trusted_device_ids,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield

    app = FastAPI(
        title="Photo Asset Manager Control Plane",
        version="0.1.0",
        description="Aurora PostgreSQL authoritative event store control-plane API.",
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.engine = engine
    app.state.auto_create_schema = auto_create_schema

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
        return store.create_derivative_upload(request)

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
    if database_url is None and os.getenv("DATABASE_CONNECTION_SECRET_ARN"):
        database_url = _database_url_from_connection_secret(os.environ["DATABASE_CONNECTION_SECRET_ARN"])
    trusted_device_ids = _load_trusted_device_ids()
    if database_url:
        return create_app(database_url=database_url, trusted_device_ids=trusted_device_ids, auto_create_schema=None)
    if os.getenv("CONTROL_PLANE_ALLOW_SQLITE_DEV") == "1":
        return create_app(
            database_url="sqlite+pysqlite:///./control_plane.sqlite",
            trusted_device_ids=trusted_device_ids,
            auto_create_schema=True,
        )
    return None


def _database_url_from_connection_secret(secret_arn: str) -> str:
    response = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError("DATABASE_CONNECTION_SECRET_ARN did not resolve to a SecretString")
    payload = json.loads(secret_string)
    required = ["username", "password", "host", "port", "database"]
    missing = [key for key in required if key not in payload]
    if missing:
        raise RuntimeError(f"Aurora connection secret missing fields: {', '.join(missing)}")
    return (
        "postgresql+psycopg://"
        f"{quote(str(payload['username']), safe='')}:{quote(str(payload['password']), safe='')}"
        f"@{payload['host']}:{payload['port']}/{payload['database']}"
    )


def _load_trusted_device_ids() -> set[str]:
    raw = os.getenv("CONTROL_PLANE_TRUSTED_DEVICE_IDS", "")
    return {item.strip() for item in raw.split(",") if item.strip()}


def _resolve_auto_create_schema(database_url: str) -> bool:
    explicit = os.getenv("CONTROL_PLANE_AUTO_CREATE_SCHEMA")
    if explicit is not None:
        return explicit == "1"
    return database_url.startswith("sqlite")


app = _load_default_app()
