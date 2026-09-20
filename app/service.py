"""Per-device sliding-window state machine.

State model (per device)
------------------------
``contiguous_next``
    Every sequence strictly smaller than this has already been delivered in
    order or permanently declared missing (a finalized gap / "断档").
``pending_entries``
    Telemetry that arrived inside the forward window but lies beyond a gap.
``gaps``
    Missing sequence positions. ``OPEN`` gaps are still within the configured
    wait time; an ``OPEN`` gap sitting at the delivery frontier whose age
    exceeds ``wait_timeout_ms`` is promoted to ``MISSING`` (a permanent break
    that can be repaired through the replay endpoint).

Concurrency
-----------
Writers are serialised *per device* with an asyncio lock, and every mutation
runs inside a single ``BEGIN IMMEDIATE`` SQLite transaction, so concurrent
batches for the same device produce exactly-once delivery and a consistent
window. Different devices stay independent.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from typing import Any

from .db import transaction

OPEN = "OPEN"
MISSING = "MISSING"


def now() -> float:
    """Wall clock in seconds; tests monkeypatch this symbol."""
    return time.time()


class UnknownDeviceError(Exception):
    def __init__(self, device_id: str):
        self.device_id = device_id
        super().__init__(f"unknown device: {device_id}")


class _DeviceLocks:
    """Lazily created, event-loop-bound per-device asyncio locks."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: dict[str, asyncio.Lock] = {}

    def get(self, device_id: str) -> asyncio.Lock:
        with self._guard:
            lock = self._locks.get(device_id)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[device_id] = lock
            return lock


def _payload_text(payload: Any) -> str | None:
    if payload is None:
        return None
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


class TelemetryService:
    def __init__(self, conn: sqlite3.Connection, settings: Any):
        self.conn = conn
        self.settings = settings
        self.locks = _DeviceLocks()

    # ------------------------------------------------------------------ helpers

    def _ensure_device(
        self,
        conn: sqlite3.Connection,
        device_id: str,
        initial_seq: int | None = None,
        window_size: int | None = None,
        wait_timeout_ms: int | None = None,
    ) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if row is not None:
            return row

        t = now()
        initial = initial_seq if initial_seq is not None else self.settings.initial_seq
        conn.execute(
            """
            INSERT INTO devices (device_id, initial_seq, contiguous_next,
                                 window_size, wait_timeout_ms, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                device_id,
                initial,
                initial,
                window_size if window_size is not None else self.settings.window_size,
                wait_timeout_ms
                if wait_timeout_ms is not None
                else self.settings.wait_timeout_ms,
                t,
                t,
            ),
        )
        return conn.execute(
            "SELECT * FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()  # type: ignore[return-value]

    def _get_device(self, conn: sqlite3.Connection, device_id: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if row is None:
            raise UnknownDeviceError(device_id)
        return row

    # ------------------------------------------------------- core state machine

    @staticmethod
    def _advance(
        conn: sqlite3.Connection, device_id: str, seq: int, current: float
    ) -> None:
        conn.execute(
            "UPDATE devices SET contiguous_next = ?, updated_at = ? "
            "WHERE device_id = ?",
            (seq + 1, current, device_id),
        )

    def _reconcile(
        self, conn: sqlite3.Connection, device_id: str, current: float
    ) -> list[int]:
        """Advance the contiguous frontier as far as possible.

        Delivers pending entries whose predecessors are complete, skips
        previously filled breaks, and promotes expired leading OPEN gaps to
        MISSING. Returns the list of sequences delivered during this call.
        """
        delivered_seq: list[int] = []

        while True:
            device = self._get_device(conn, device_id)
            s = device["contiguous_next"]

            pending = conn.execute(
                "SELECT payload, ts, received_at FROM pending_entries "
                "WHERE device_id = ? AND seq = ?",
                (device_id, s),
            ).fetchone()
            if pending is not None:
                # In-order delivery: pending -> delivered.
                conn.execute(
                    "INSERT INTO delivered_entries "
                    "(device_id, seq, payload, ts, received_at, delivered_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        device_id,
                        s,
                        pending["payload"],
                        pending["ts"],
                        pending["received_at"],
                        current,
                    ),
                )
                conn.execute(
                    "DELETE FROM pending_entries WHERE device_id = ? AND seq = ?",
                    (device_id, s),
                )
                conn.execute(
                    "DELETE FROM gaps WHERE device_id = ? AND seq = ?",
                    (device_id, s),
                )
                delivered_seq.append(s)
                self._advance(conn, device_id, s, current)
                continue

            if conn.execute(
                "SELECT 1 FROM delivered_entries WHERE device_id = ? AND seq = ?",
                (device_id, s),
            ).fetchone() is not None:
                # Break repaired through replay: row is already delivered.
                conn.execute(
                    "DELETE FROM gaps WHERE device_id = ? AND seq = ?",
                    (device_id, s),
                )
                self._advance(conn, device_id, s, current)
                continue

            gap = conn.execute(
                "SELECT status, opened_at FROM gaps WHERE device_id = ? AND seq = ?",
                (device_id, s),
            ).fetchone()

            if gap is None:
                break  # no information about this seq yet

            if gap["status"] == OPEN:
                # The hole segment was discovered when the earliest buffered
                # entry beyond it arrived; use that as the wait baseline even
                # when the leading marker was materialised lazily.
                earliest_pending = conn.execute(
                    "SELECT MIN(received_at) AS r FROM pending_entries "
                    "WHERE device_id = ? AND seq > ?",
                    (device_id, s),
                ).fetchone()["r"]
                discovered_at = gap["opened_at"]
                if earliest_pending is not None:
                    discovered_at = min(discovered_at, earliest_pending)
                age_ms = (current - discovered_at) * 1000.0
                if age_ms < device["wait_timeout_ms"]:
                    break  # still waiting
                # Promote to MISSING, then fall through and cross it below.
                conn.execute(
                    "UPDATE gaps SET status = ?, finalized_at = ? "
                    "WHERE device_id = ? AND seq = ?",
                    (MISSING, current, device_id, s),
                )

            # MISSING (either pre-existing or just promoted): declare the
            # permanent break and move past it. The marker is KEPT so it stays
            # queryable and can be repaired through replay; repaired seqs also
            # remain in delivered_entries for audit/idempotency.
            self._advance(conn, device_id, s, current)

        # Keep the OPEN-gap markers contiguous from the frontier up to the
        # earliest still-pending entry. Existing markers keep their opened_at.
        device = self._get_device(conn, device_id)
        frontier = device["contiguous_next"]
        min_pending_row = conn.execute(
            "SELECT MIN(seq) AS m, MIN(received_at) AS r "
            "FROM pending_entries WHERE device_id = ?",
            (device_id,),
        ).fetchone()
        min_pending = min_pending_row["m"]

        if min_pending is not None:
            # Every hole between the frontier and the first buffered entry was
            # discovered when that entry arrived, so they share one deadline
            # (INSERT OR IGNORE preserves the original discovered_at for
            # pre-existing markers). Materialisation is capped at the forward
            # edge of the window; deeper holes appear lazily as it slides.
            discovered_at = min_pending_row["r"]
            materialise_end = min(
                min_pending, frontier + max(1, device["window_size"])
            )
            for s in range(frontier, materialise_end):
                conn.execute(
                    "INSERT OR IGNORE INTO gaps (device_id, seq, status, opened_at) "
                    "VALUES (?, ?, ?, ?)",
                    (device_id, s, OPEN, discovered_at),
                )
            # OPEN markers beyond the first buffered entry / window edge are stale.
            conn.execute(
                "DELETE FROM gaps WHERE device_id = ? AND status = ? AND seq > ?",
                (device_id, OPEN, materialise_end),
            )
        else:
            conn.execute(
                "DELETE FROM gaps WHERE device_id = ? AND status = ? AND seq > ?",
                (device_id, OPEN, frontier),
            )
        conn.execute(
            "UPDATE devices SET updated_at = ? WHERE device_id = ?",
            (current, device_id),
        )
        return delivered_seq

    # ----------------------------------------------------------------- snapshots

    def _window_snapshot(
        self, conn: sqlite3.Connection, device_id: str, current: float
    ) -> dict[str, Any]:
        device = self._get_device(conn, device_id)
        stats = conn.execute(
            "SELECT COUNT(*) AS c, MIN(seq) AS lo, MAX(seq) AS hi "
            "FROM pending_entries WHERE device_id = ?",
            (device_id,),
        ).fetchone()
        open_gaps = [
            r["seq"]
            for r in conn.execute(
                "SELECT seq FROM gaps WHERE device_id = ? AND status = ? ORDER BY seq",
                (device_id, OPEN),
            )
        ]
        missing_count = conn.execute(
            "SELECT COUNT(*) AS c FROM gaps WHERE device_id = ? AND status = ?",
            (device_id, MISSING),
        ).fetchone()["c"]
        oldest_open = conn.execute(
            "SELECT MIN(opened_at) AS o FROM gaps WHERE device_id = ? AND status = ?",
            (device_id, OPEN),
        ).fetchone()["o"]

        next_expiry: int | None = None
        if oldest_open is not None:
            remaining = device["wait_timeout_ms"] - (current - oldest_open) * 1000.0
            next_expiry = max(0, int(round(remaining)))

        return {
            "device_id": device_id,
            "contiguous_next": device["contiguous_next"],
            "window_size": device["window_size"],
            "wait_timeout_ms": device["wait_timeout_ms"],
            "window_low_watermark": device["contiguous_next"] - device["window_size"],
            "pending_count": stats["c"],
            "pending_min": stats["lo"],
            "pending_max": stats["hi"],
            "open_gap_count": len(open_gaps),
            "missing_gap_count": missing_count,
            "open_gaps": open_gaps,
            "next_gap_expires_in_ms": next_expiry,
        }

    # ------------------------------------------------------------------ endpoints

    async def submit_batch(
        self,
        device_id: str,
        entries: list[Any],
        initial_seq: int | None = None,
        window_size: int | None = None,
        wait_timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """Ingest a batch. Idempotent: repeated seqs are discarded, not stored."""
        async with self.locks.get(device_id):
            with transaction(self.conn) as conn:
                current = now()
                self._ensure_device(
                    conn, device_id, initial_seq, window_size, wait_timeout_ms
                )
                device = self._get_device(conn, device_id)
                low_watermark = device["contiguous_next"] - device["window_size"]
                # Forward edge: accepting seq s would open (s - frontier)
                # holes; allow strictly fewer than window_size of them so the
                # pending region cannot span more than one full window.
                high_watermark = (
                    device["contiguous_next"] + device["window_size"]
                )  # seq >= high_watermark would open >= window_size holes

                results: list[dict[str, Any]] = []
                seen: set[int] = set()

                for entry in entries:
                    seq = entry.seq

                    if conn.execute(
                        "SELECT 1 FROM delivered_entries WHERE device_id = ? AND seq = ?",
                        (device_id, seq),
                    ).fetchone() is not None:
                        status = "duplicate"
                    elif conn.execute(
                        "SELECT 1 FROM pending_entries WHERE device_id = ? AND seq = ?",
                        (device_id, seq),
                    ).fetchone() is not None:
                        status = "already_pending"
                    elif seq in seen:
                        status = "duplicate"
                    elif not (low_watermark <= seq < high_watermark):
                        # Outside the sliding-window span: too far behind to be
                        # ordered (a break that aged out), or too far ahead
                        # (holes would overflow the window).
                        status = "out_of_window"
                    else:
                        gap = conn.execute(
                            "SELECT status FROM gaps WHERE device_id = ? AND seq = ?",
                            (device_id, seq),
                        ).fetchone()

                        if gap is not None and seq < device["contiguous_next"]:
                            # A break behind the frontier that is still inside
                            # the window: late original telemetry repairs it
                            # directly (same write path as the replay API).
                            conn.execute(
                                "DELETE FROM gaps WHERE device_id = ? AND seq = ?",
                                (device_id, seq),
                            )
                            conn.execute(
                                "INSERT INTO delivered_entries "
                                "(device_id, seq, payload, ts, received_at, delivered_at) "
                                "VALUES (?, ?, ?, ?, ?, ?)",
                                (
                                    device_id,
                                    seq,
                                    _payload_text(entry.payload),
                                    entry.ts,
                                    current,
                                    current,
                                ),
                            )
                            seen.add(seq)
                            status = "accepted"
                        else:
                            # OPEN hole ahead of the frontier, or a future seq:
                            # buffer it and the frontier reaches it in order.
                            conn.execute(
                                "INSERT INTO pending_entries "
                                "(device_id, seq, payload, ts, received_at) "
                                "VALUES (?, ?, ?, ?, ?)",
                                (
                                    device_id,
                                    seq,
                                    _payload_text(entry.payload),
                                    entry.ts,
                                    current,
                                ),
                            )
                            conn.execute(
                                "DELETE FROM gaps WHERE device_id = ? AND seq = ?",
                                (device_id, seq),
                            )
                            seen.add(seq)
                            status = "accepted"

                    results.append({"seq": seq, "status": status, "delivered": False})

                delivered_seq = self._reconcile(conn, device_id, now())
                window = self._window_snapshot(conn, device_id, now())

            delivered_set = set(delivered_seq)
            counts = {
                "accepted": 0,
                "filled": 0,
                "duplicate": 0,
                "already_pending": 0,
                "out_of_window": 0,
            }
            for r in results:
                if r["status"] == "accepted":
                    r["delivered"] = r["seq"] in delivered_set
                    counts["accepted"] += 1
                    if r["delivered"]:
                        counts["filled"] += 1
                else:
                    counts[r["status"]] += 1

            return {
                "device_id": device_id,
                **counts,
                "delivered_seq": delivered_seq,
                "results": results,
                "window": window,
            }

    async def replay(
        self, device_id: str, entries: list[Any]
    ) -> dict[str, Any]:
        """Repair finalized (MISSING) breaks by supplying the lost telemetry."""
        async with self.locks.get(device_id):
            with transaction(self.conn) as conn:
                current = now()
                self._ensure_device(conn, device_id)
                device = self._get_device(conn, device_id)
                low_watermark = device["contiguous_next"] - device["window_size"]
                frontier = device["contiguous_next"]

                results: list[dict[str, Any]] = []
                filled_seq: list[int] = []
                seen: set[int] = set()

                for entry in entries:
                    seq = entry.seq

                    if conn.execute(
                        "SELECT 1 FROM delivered_entries WHERE device_id = ? AND seq = ?",
                        (device_id, seq),
                    ).fetchone() is not None:
                        status, delivered = "duplicate", False
                    elif conn.execute(
                        "SELECT 1 FROM pending_entries WHERE device_id = ? AND seq = ?",
                        (device_id, seq),
                    ).fetchone() is not None:
                        status, delivered = "already_pending", False
                    elif seq in seen:
                        status, delivered = "duplicate", False
                    else:
                        gap = conn.execute(
                            "SELECT status FROM gaps WHERE device_id = ? AND seq = ?",
                            (device_id, seq),
                        ).fetchone()
                        if gap is None or seq < low_watermark or seq > frontier:
                            # Replay only backfills a declared break:
                            #  * no gap row      -> never declared missing;
                            #  * behind watermark-> break aged out of the window;
                            #  * ahead of frontier-> future seq, use normal ingest.
                            status, delivered = "not_a_gap", False
                        elif gap["status"] == OPEN:
                            # Gap not finalized yet; submit it through the
                            # normal ingest endpoint instead.
                            status, delivered = "already_gap", False
                        else:
                            conn.execute(
                                "DELETE FROM gaps WHERE device_id = ? AND seq = ?",
                                (device_id, seq),
                            )
                            conn.execute(
                                "INSERT INTO delivered_entries "
                                "(device_id, seq, payload, ts, received_at, delivered_at) "
                                "VALUES (?, ?, ?, ?, ?, ?)",
                                (
                                    device_id,
                                    seq,
                                    _payload_text(entry.payload),
                                    entry.ts,
                                    current,
                                    current,
                                ),
                            )
                            seen.add(seq)
                            filled_seq.append(seq)
                            status, delivered = "filled", True

                    results.append({"seq": seq, "status": status, "delivered": delivered})

                # Frontier may now run across repaired breaks and queued pending.
                self._reconcile(conn, device_id, now())
                window = self._window_snapshot(conn, device_id, now())

            counts = {
                "filled": 0,
                "duplicate": 0,
                "already_pending": 0,
                "already_gap": 0,
                "not_a_gap": 0,
            }
            for r in results:
                counts[r["status"]] += 1

            return {
                "device_id": device_id,
                **counts,
                "filled_seq": filled_seq,
                "results": results,
                "window": window,
            }

    async def get_window(self, device_id: str) -> dict[str, Any]:
        """Return the current window state; expiry is applied on read too."""
        async with self.locks.get(device_id):
            with transaction(self.conn) as conn:
                device = self._get_device(conn, device_id)  # raises if unknown
                self._reconcile(conn, device_id, now())
                return self._window_snapshot(conn, device_id, now())

    async def list_gaps(
        self, device_id: str, status: str | None = None
    ) -> dict[str, Any]:
        """List OPEN / MISSING gap markers for a device."""
        async with self.locks.get(device_id):
            with transaction(self.conn) as conn:
                device = self._get_device(conn, device_id)
                current = now()
                self._reconcile(conn, device_id, current)

                if status in (OPEN, MISSING):
                    rows = conn.execute(
                        "SELECT * FROM gaps WHERE device_id = ? AND status = ? ORDER BY seq",
                        (device_id, status),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM gaps WHERE device_id = ? ORDER BY seq",
                        (device_id,),
                    ).fetchall()

                gaps = []
                for r in rows:
                    item: dict[str, Any] = {
                        "seq": r["seq"],
                        "status": r["status"],
                        "opened_at": r["opened_at"],
                        "finalized_at": r["finalized_at"],
                        "expires_in_ms": None,
                    }
                    if r["status"] == OPEN:
                        remaining = (
                            device["wait_timeout_ms"] - (current - r["opened_at"]) * 1000.0
                        )
                        item["expires_in_ms"] = max(0, int(round(remaining)))
                    gaps.append(item)

                missing_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM gaps WHERE device_id = ? AND status = ?",
                    (device_id, MISSING),
                ).fetchone()["c"]
                open_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM gaps WHERE device_id = ? AND status = ?",
                    (device_id, OPEN),
                ).fetchone()["c"]

            return {
                "device_id": device_id,
                "contiguous_next": device["contiguous_next"],
                "open_gap_count": open_count,
                "missing_gap_count": missing_count,
                "gaps": gaps,
            }

    async def reconcile_device(self, device_id: str) -> None:
        """Used by the background sweeper."""
        async with self.locks.get(device_id):
            with transaction(self.conn) as conn:
                self._get_device(conn, device_id)
                self._reconcile(conn, device_id, now())

    def device_ids(self) -> list[str]:
        return [
            r["device_id"]
            for r in self.conn.execute("SELECT device_id FROM devices ORDER BY device_id")
        ]

    def any_expirable_gap(self) -> bool:
        """Cheap read-only check used by the sweeper to skip no-op cycles.

        The leading gap has timed out when it has been OPEN for longer than the
        device wait, or when the earliest buffered entry has waited that long
        (its arrival is what actually discovered the whole hole segment, even
        if the leading marker was materialised lazily).
        """
        row = self.conn.execute(
            """
            SELECT 1
            FROM devices d
            WHERE (
                SELECT (? - opened_at) * 1000.0
                FROM gaps
                WHERE device_id = d.device_id AND seq = d.contiguous_next
                LIMIT 1
            ) >= d.wait_timeout_ms
               OR (
                SELECT (? - MIN(received_at)) * 1000.0
                FROM pending_entries
                WHERE device_id = d.device_id
            ) >= d.wait_timeout_ms
            LIMIT 1
            """,
            (now(), now()),
        ).fetchone()
        return row is not None
