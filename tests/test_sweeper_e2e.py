"""End-to-end test: real background sweeper promotes gaps using wall clock."""
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.db import connect, init_db
from app.main import create_app
from app.service import TelemetryService
from app.sweeper import GapSweeper

pytestmark = pytest.mark.asyncio


async def test_background_sweeper_finalizes_gaps(monkeypatch):
    # Real wall clock, but a very short timeout and sweep interval.
    settings = Settings(
        database_path=":memory:",
        enable_sweeper=True,
        window_size=100,
        wait_timeout_ms=300,
        sweep_interval_ms=100,
    )
    conn = connect(":memory:")
    init_db(conn)
    svc = TelemetryService(conn, settings)
    sweeper = GapSweeper(svc, settings.sweep_interval_ms)

    app = create_app()
    app.state.service = svc
    app.state.settings = settings
    app.state.db = conn
    app.state.sweeper = sweeper
    await sweeper.start()
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            r = await client.post(
                "/api/v1/devices/sensor-a/batches",
                json={"entries": [{"seq": 2, "payload": "x"}]},
            )
            assert r.json()["window"]["open_gap_count"] == 2

            # Wait well beyond wait_timeout_ms; the sweeper must break 0,1
            # and deliver buffered 2 without any client request triggering it.
            await asyncio.sleep(0.9)

            w = (await client.get("/api/v1/devices/sensor-a/window")).json()
            assert w["contiguous_next"] == 3
            assert w["missing_gap_count"] == 2
            assert w["pending_count"] == 0

            # Replay repairs the two breaks.
            r = await client.post(
                "/api/v1/devices/sensor-a/gaps/replay",
                json={"entries": [{"seq": 0, "payload": "r0"}, {"seq": 1, "payload": "r1"}]},
            )
            assert r.json()["filled"] == 2
            assert r.json()["window"]["missing_gap_count"] == 0
    finally:
        await sweeper.stop()
        conn.close()
