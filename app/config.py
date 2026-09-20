"""Runtime configuration, read once from environment variables.

All values have sane local defaults so the service can also be started with a
plain ``uvicorn app.main:app`` without any environment set.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TELEMETRY_", env_file=None, extra="ignore")

    # Storage
    database_path: str = Field(default="/data/telemetry.db")

    # Network
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8080)

    # Sliding-window behaviour
    initial_seq: int = Field(
        default=0,
        description="First expected sequence for a device that has never been seen.",
    )
    window_size: int = Field(
        default=1000,
        description="Acceptable forward span; seq < contiguous_next-window_size is rejected.",
    )
    wait_timeout_ms: int = Field(
        default=30_000,
        description="Time a leading gap is held before being marked as a permanent break.",
    )

    # Background gap sweeper
    sweep_interval_ms: int = Field(
        default=1_000,
        description="How often the background task promotes expired gaps.",
    )
    enable_sweeper: bool = Field(default=True)

    @field_validator("window_size", "wait_timeout_ms", "sweep_interval_ms")
    @classmethod
    def _non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    @field_validator("port")
    @classmethod
    def _port_range(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError("port must be between 1 and 65535")
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()
