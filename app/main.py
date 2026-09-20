"""FastAPI application: telemetry ingest, window state and gap replay."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Path, Query

from . import schemas
from .config import get_settings
from .db import connect, init_db
from .service import TelemetryService, UnknownDeviceError
from .sweeper import GapSweeper

logger = logging.getLogger("telemetry")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    conn = connect(settings.database_path)
    init_db(conn)
    service = TelemetryService(conn, settings)
    sweeper = GapSweeper(service, settings.sweep_interval_ms)

    app.state.db = conn
    app.state.service = service
    app.state.settings = settings
    app.state.sweeper = sweeper

    if settings.enable_sweeper:
        await sweeper.start()

    logger.info(
        "telemetry API ready (db=%s, window_size=%d, wait_timeout_ms=%d, port=%d)",
        settings.database_path,
        settings.window_size,
        settings.wait_timeout_ms,
        settings.port,
    )
    try:
        yield
    finally:
        await sweeper.stop()
        conn.close()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Local Device Telemetry API",
        version="1.0.0",
        description=(
            "Per-device sliding-window telemetry collector: idempotent ingest, "
            "gap buffering with configurable timeout, break declaration and replay."
        ),
        lifespan=lifespan,
    )

    def service() -> TelemetryService:
        return app.state.service

    @app.exception_handler(UnknownDeviceError)
    async def _unknown_device_handler(_request: Any, exc: UnknownDeviceError):
        raise HTTPException(status_code=404, detail=f"unknown device: {exc.device_id}")

    @app.get("/", include_in_schema=False)
    async def root() -> dict[str, str]:
        return {"service": "device-telemetry-api", "docs": "/docs", "health": "/health"}

    @app.get("/health", response_model=schemas.HealthResponse, tags=["system"])
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "database": settings.database_path,
            "sweeper_running": app.state.sweeper.running,
        }

    @app.post(
        "/api/v1/devices/{device_id}/batches",
        response_model=schemas.SubmitResponse,
        status_code=202,
        tags=["ingest"],
        summary="Submit a telemetry batch",
    )
    async def submit_batch(
        body: schemas.BatchRequest,
        device_id: str = Path(..., min_length=1, max_length=128),
    ) -> dict[str, Any]:
        return await service().submit_batch(
            device_id=device_id,
            entries=body.entries,
            initial_seq=body.initial_seq,
            window_size=body.window_size,
            wait_timeout_ms=body.wait_timeout_ms,
        )

    @app.get(
        "/api/v1/devices/{device_id}/window",
        response_model=schemas.DeviceWindow,
        tags=["state"],
        summary="Query the per-device sliding-window state",
    )
    async def get_window(
        device_id: str = Path(..., min_length=1, max_length=128),
    ) -> dict[str, Any]:
        return await service().get_window(device_id)

    @app.get(
        "/api/v1/devices/{device_id}/gaps",
        response_model=schemas.GapListResponse,
        tags=["state"],
        summary="List gaps (OPEN holes and MISSING finalized breaks)",
    )
    async def list_gaps(
        device_id: str = Path(..., min_length=1, max_length=128),
        status: str | None = Query(default=None, pattern="^(OPEN|MISSING)$"),
    ) -> dict[str, Any]:
        return await service().list_gaps(device_id, status)

    @app.post(
        "/api/v1/devices/{device_id}/gaps/replay",
        response_model=schemas.ReplayResponse,
        tags=["replay"],
        summary="Replay telemetry to repair finalized (MISSING) breaks",
    )
    async def replay_gaps(
        body: schemas.ReplayRequest,
        device_id: str = Path(..., min_length=1, max_length=128),
    ) -> dict[str, Any]:
        return await service().replay(device_id, body.entries)

    return app


app = create_app()
