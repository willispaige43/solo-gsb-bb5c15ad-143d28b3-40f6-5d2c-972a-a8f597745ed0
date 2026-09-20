"""SQLite connection, schema and a small transaction helper."""
from __future__ import annotations

import os
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id        TEXT PRIMARY KEY,
    initial_seq      INTEGER NOT NULL,
    contiguous_next  INTEGER NOT NULL,  -- all seq < this are delivered or finalized gaps
    window_size      INTEGER NOT NULL,
    wait_timeout_ms  INTEGER NOT NULL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);

-- Telemetry that has arrived but lies beyond a gap (waiting inside the window).
CREATE TABLE IF NOT EXISTS pending_entries (
    device_id  TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    payload    TEXT,
    ts         REAL,
    received_at REAL NOT NULL,
    PRIMARY KEY (device_id, seq)
);

-- Telemetry that has been delivered in order (kept for idempotency + replay audit).
CREATE TABLE IF NOT EXISTS delivered_entries (
    device_id  TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    payload    TEXT,
    ts         REAL,
    received_at REAL NOT NULL,
    delivered_at REAL NOT NULL,
    PRIMARY KEY (device_id, seq)
);

-- Gaps: OPEN (still waiting) or MISSING (timeout -> permanent break / replayable).
CREATE TABLE IF NOT EXISTS gaps (
    device_id  TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('OPEN', 'MISSING')),
    opened_at  REAL NOT NULL,
    finalized_at REAL,
    PRIMARY KEY (device_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_gaps_device_status ON gaps (device_id, status, seq);
"""


def connect(database_path: str) -> sqlite3.Connection:
    """Open the application-wide SQLite connection.

    A single connection guarded by a per-device asyncio lock (see service.py)
    serialises writes for each device; ``isolation_level=None`` puts us in
    autocommit mode so we control transactions explicitly with BEGIN/COMMIT.
    """
    if database_path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(database_path)), exist_ok=True)

    conn = sqlite3.connect(
        database_path,
        check_same_thread=False,
        isolation_level=None,
        timeout=30.0,
    )
    conn.row_factory = sqlite3.Row
    # WAL allows readers while a writer is committing; the other pragmas give
    # durability/performance balance suitable for a local collector.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit transaction with retry on SQLITE_BLOCKED (only inside BEGIN).

    Because we are in autocommit mode, a database lock conflict surfaces when
    the BEGIN itself is attempted. On the rare contention it is retried briefly.
    """
    delay = 0.005
    last_error: sqlite3.OperationalError | None = None
    for _ in range(50):
        try:
            conn.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError as exc:  # database is locked
            last_error = exc
            time.sleep(delay)
            delay = min(delay * 1.5, 0.1)
    else:
        assert last_error is not None
        raise last_error

    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
