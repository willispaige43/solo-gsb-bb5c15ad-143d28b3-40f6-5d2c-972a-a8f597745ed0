"""Pydantic request / response models."""
from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field

DeviceId = Annotated[
    str,
    Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.\-:]+$"),
]
Seq = Annotated[int, Field(ge=0, description="Device-relative monotonically increasing sequence number")]

EntryStatus = Literal["accepted", "filled", "duplicate", "already_pending", "out_of_window"]
GapStatus = Literal["OPEN", "MISSING"]


class TelemetryEntry(BaseModel):
    seq: Seq
    ts: float | None = Field(default=None, ge=0, description="Optional device-side timestamp (epoch seconds)")
    payload: Any = Field(default=None, description="Arbitrary JSON-serialisable telemetry payload")


class BatchRequest(BaseModel):
    initial_seq: int | None = Field(
        default=None,
        ge=0,
        description="First expected seq, used only when the device is registered for the first time.",
    )
    window_size: int | None = Field(default=None, ge=0)
    wait_timeout_ms: int | None = Field(default=None, ge=0)
    entries: list[TelemetryEntry] = Field(min_length=1, max_length=10_000)


class EntryResult(BaseModel):
    seq: int
    status: EntryStatus
    delivered: bool


class DeviceWindow(BaseModel):
    device_id: str
    contiguous_next: int
    window_size: int
    wait_timeout_ms: int
    window_low_watermark: int
    pending_count: int
    pending_min: int | None
    pending_max: int | None
    open_gap_count: int
    missing_gap_count: int
    open_gaps: list[int]
    next_gap_expires_in_ms: int | None


class SubmitResponse(BaseModel):
    device_id: str
    accepted: int
    filled: int
    duplicate: int
    already_pending: int
    out_of_window: int
    delivered_seq: list[int]
    window: DeviceWindow


class GapItem(BaseModel):
    seq: int
    status: GapStatus
    opened_at: float
    finalized_at: float | None = None
    expires_in_ms: int | None = None


class GapListResponse(BaseModel):
    device_id: str
    contiguous_next: int
    open_gap_count: int
    missing_gap_count: int
    gaps: list[GapItem]


class ReplayRequest(BaseModel):
    entries: list[TelemetryEntry] = Field(min_length=1, max_length=10_000)


class ReplayResponse(BaseModel):
    device_id: str
    filled: int
    duplicate: int
    already_pending: int
    already_gap: int
    not_a_gap: int
    filled_seq: list[int]
    window: DeviceWindow


class HealthResponse(BaseModel):
    status: Literal["ok"]
    database: str
    sweeper_running: bool
