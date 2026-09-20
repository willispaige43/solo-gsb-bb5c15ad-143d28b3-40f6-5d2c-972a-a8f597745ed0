"""Shared pytest fixtures: in-memory SQLite app with a controllable clock."""
from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app import service as service_module
from app.config import Settings
from app.db import connect, init_db
from app.main import create_app
from app.service import TelemetryService
from app.sweeper import GapSweeper


class FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def fake_clock(monkeypatch) -> FakeClock:
    clock = FakeClock()
    monkeypatch.setattr(service_module, "now", clock)
    return clock


@pytest_asyncio.fixture
async def client(fake_clock):
    settings = Settings(
        database_path=":memory:",
        enable_sweeper=False,
        window_size=10,
        wait_timeout_ms=5_000,
        initial_seq=0,
    )
    conn = connect(":memory:")
    init_db(conn)
    svc = TelemetryService(conn, settings)
    sweeper = GapSweeper(svc, 10_000)

    app = create_app()
    app.state.service = svc
    app.state.settings = settings
    app.state.db = conn
    app.state.sweeper = sweeper

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    await sweeper.stop()
    conn.close()


@pytest.fixture
def loop_factory():
    return asyncio.new_event_loop
