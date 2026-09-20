"""Background task that promotes expired leading gaps across all devices."""
from __future__ import annotations

import asyncio
import logging

from .service import TelemetryService, UnknownDeviceError

logger = logging.getLogger("telemetry.sweeper")


class GapSweeper:
    def __init__(self, service: TelemetryService, interval_ms: int):
        self.service = service
        self.interval = interval_ms / 1000.0
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.running = False

    async def _run(self) -> None:
        logger.info("gap sweeper started (interval %.3fs)", self.interval)
        while not self._stop.is_set():
            try:
                if self.service.any_expirable_gap():
                    for device_id in self.service.device_ids():
                        try:
                            await self.service.reconcile_device(device_id)
                        except UnknownDeviceError:
                            continue
            except Exception:  # never let the loop die
                logger.exception("sweep cycle failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass
        self.running = False
        logger.info("gap sweeper stopped")

    async def start(self) -> None:
        if self._task is None:
            self.running = True
            self._stop.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._stop.set()
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                self._task.cancel()
            self._task = None
            self.running = False
