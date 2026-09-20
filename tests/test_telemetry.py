"""Functional and concurrency tests for the telemetry window state machine."""
from __future__ import annotations

import asyncio

import pytest

pytestmark = pytest.mark.asyncio


async def _batch(client, device, seqs, **kw):
    return await client.post(
        f"/api/v1/devices/{device}/batches",
        json={"entries": [{"seq": s, "payload": {"v": s}} for s in seqs], **kw},
    )


async def test_in_order_delivery_and_window(client):
    r = await _batch(client, "dev-1", [0, 1, 2])
    assert r.status_code == 202
    body = r.json()
    assert body["accepted"] == 3
    assert body["filled"] == 3
    assert body["delivered_seq"] == [0, 1, 2]
    assert body["window"]["contiguous_next"] == 3
    assert body["window"]["pending_count"] == 0


async def test_out_of_order_gap_buffered_then_filled(client):
    # 3,4 arrive first: frontier stays at 0 and holes 0,1,2 are opened.
    r = await _batch(client, "dev-2", [3, 4])
    w = r.json()["window"]
    assert w["contiguous_next"] == 0
    assert w["pending_min"] == 3 and w["pending_max"] == 4
    assert w["open_gaps"] == [0, 1, 2]
    assert w["next_gap_expires_in_ms"] == 5000

    # Fill the hole; pending chain delivers 0..4.
    r = await _batch(client, "dev-2", [0, 1, 2])
    body = r.json()
    assert body["delivered_seq"] == [0, 1, 2, 3, 4]
    assert body["window"]["contiguous_next"] == 5
    assert body["window"]["open_gap_count"] == 0


async def test_duplicates_are_idempotent(client):
    await _batch(client, "dev-3", [0, 1, 2])
    r = await _batch(client, "dev-3", [1, 2, 5])
    body = r.json()
    assert body["duplicate"] == 2
    assert body["accepted"] == 1
    # Delivered sequences are not re-delivered.
    assert body["delivered_seq"] == []

    # Duplicate inside the same batch is also discarded (one stored, one drop).
    r = await _batch(client, "dev-3", [6, 6])
    body = r.json()
    assert body["accepted"] == 1
    assert body["duplicate"] + body["already_pending"] == 1


async def test_timeout_marks_break_and_replay_repairs(client, fake_clock):
    await _batch(client, "dev-4", [3])
    # Wait longer than wait_timeout_ms (5s): 0,1,2 break, then 3 delivers.
    fake_clock.advance(6)
    r = await client.get("/api/v1/devices/dev-4/gaps?status=MISSING")
    gaps = r.json()["gaps"]
    assert [g["seq"] for g in gaps] == [0, 1, 2]
    assert all(g["status"] == "MISSING" for g in gaps)

    w = (await client.get("/api/v1/devices/dev-4/window")).json()
    assert w["contiguous_next"] == 4
    assert w["missing_gap_count"] == 3

    # A late-arriving original inside the window backfills the break directly.
    r = await _batch(client, "dev-4", [0])
    assert r.json()["accepted"] == 1
    assert r.json()["window"]["missing_gap_count"] == 2

    # Replay explicitly repairs the remaining breaks: holes filled.
    r = await client.post(
        "/api/v1/devices/dev-4/gaps/replay",
        json={"entries": [{"seq": 1, "payload": {"late": True}}, {"seq": 2}]},
    )
    body = r.json()
    assert body["filled"] == 2
    assert body["filled_seq"] == [1, 2]
    assert body["window"]["contiguous_next"] == 4
    assert body["window"]["missing_gap_count"] == 0

    # Replaying again is a no-op duplicate.
    r2 = await client.post(
        "/api/v1/devices/dev-4/gaps/replay",
        json={"entries": [{"seq": 0}]},
    )
    assert r2.json()["duplicate"] == 1

    # Replaying something that was never a break is rejected.
    r = await client.post(
        "/api/v1/devices/dev-4/gaps/replay",
        json={"entries": [{"seq": 99}]},
    )
    assert r.json()["not_a_gap"] == 1


async def test_open_gap_filled_before_timeout_is_not_finalized(client, fake_clock):
    await _batch(client, "dev-5", [2])
    fake_clock.advance(3)  # within 5s window
    r = await _batch(client, "dev-5", [0, 1])
    assert r.json()["delivered_seq"] == [0, 1, 2]
    fake_clock.advance(60)
    w = (await client.get("/api/v1/devices/dev-5/window")).json()
    assert w["missing_gap_count"] == 0
    assert w["contiguous_next"] == 3


async def test_sliding_window_rejects_old_unknown_seqs(client, fake_clock):
    # Window 5; seq 10 is inside forward edge [0, ...). When breaks 0..9
    # expire the frontier reaches 11, low watermark becomes 6.
    await _batch(
        client,
        "dev-7",
        [10],
        initial_seq=0,
        window_size=20,
        wait_timeout_ms=1000,
    )
    # Shrink the effective gap span is not possible post-registration; instead
    # demonstrate rejection using a separate tight-window device below.
    fake_clock.advance(2)
    w = (await client.get("/api/v1/devices/dev-7/window")).json()
    assert w["contiguous_next"] == 11
    assert w["window_low_watermark"] == -9

    # seq 1..9 are declared breaks still inside the window: late normal
    # telemetry backfills them directly; all 9 remain MISSING before that.
    r = await _batch(client, "dev-7", [5])
    assert r.json()["accepted"] == 1
    assert r.json()["window"]["missing_gap_count"] == 9

    # The remaining breaks can be repaired explicitly through replay.
    r = await client.post(
        "/api/v1/devices/dev-7/gaps/replay",
        json={"entries": [{"seq": s} for s in (0, 1, 2, 3, 4, 6, 7, 8, 9)]},
    )
    assert r.json()["filled"] == 9

    # Re-sending an already-delivered seq is always an idempotent duplicate.
    r = await _batch(client, "dev-7", [9])
    assert r.json()["duplicate"] == 1

    # Far-future seq is rejected as outside the forward edge.
    r = await _batch(client, "dev-7", [10_000])
    assert r.json()["out_of_window"] == 1


async def test_aged_out_break_is_rejected(client, fake_clock):
    # Register the device first with its full per-device configuration.
    await client.post(
        "/api/v1/devices/dev-12/batches",
        json={"window_size": 5, "wait_timeout_ms": 100, "entries": [{"seq": 0}]},
    )
    # frontier is now 1; seq 6 would open 5 holes (== window) -> rejected.
    r = await _batch(client, "dev-12", [6])
    assert r.json()["out_of_window"] == 1
    # seq 5 opens exactly 4 holes (< window) -> accepted.
    r = await _batch(client, "dev-12", [5])
    assert r.json()["accepted"] == 1

    # seq 4 fills the remaining prefix: gaps 1..3 wait behind 4.
    await _batch(client, "dev-12", [4])
    fake_clock.advance(1)  # 1..3 break, buffered 4,5 deliver -> frontier 6
    w = (await client.get("/api/v1/devices/dev-12/window")).json()
    assert w["contiguous_next"] == 6
    assert w["window_low_watermark"] == 1

    # seq 0 was delivered at registration: duplicate.
    r = await _batch(client, "dev-12", [0])
    assert r.json()["duplicate"] == 1

    # Advance again: buffer seq 10 (gaps 6..9), let them break; 10 delivers.
    await _batch(client, "dev-12", [10])
    fake_clock.advance(1)
    w = (await client.get("/api/v1/devices/dev-12/window")).json()
    assert w["contiguous_next"] == 11
    assert w["window_low_watermark"] == 6
    # Never-delivered break 1 is exactly at/behind the watermark now: rejected.
    r = await _batch(client, "dev-12", [1])
    assert r.json()["out_of_window"] == 1
    # seq 6 is exactly on the watermark: its declared break is backfillable.
    r = await _batch(client, "dev-12", [6])
    assert r.json()["accepted"] == 1


async def test_unknown_device_and_config_on_first_seen(client):
    r = await client.get("/api/v1/devices/nope/window")
    assert r.status_code == 404

    r = await _batch(
        client,
        "dev-8",
        [10, 11],
        initial_seq=10,
        window_size=5,
        wait_timeout_ms=2000,
    )
    w = r.json()["window"]
    # Frontier starts at 10; the batch fills it and delivers 10,11.
    assert w["contiguous_next"] == 12
    assert w["window_size"] == 5
    assert w["wait_timeout_ms"] == 2000
    assert w["open_gaps"] == []  # nothing beyond frontier buffered yet

    # Per-batch config overrides only apply at registration time.
    r = await _batch(client, "dev-8", [12], window_size=999)
    assert r.json()["window"]["window_size"] == 5


async def test_concurrent_batches_deliver_each_seq_once(client):
    # 20 concurrent batches, each carrying a shuffled slice of seq 0..199.
    # Register the device first with a window wide enough to buffer the run.
    await client.post(
        "/api/v1/devices/dev-conc/batches",
        json={"window_size": 500, "entries": [{"seq": 0}]},
    )
    seqs = list(range(1, 200))
    chunks = [seqs[i::20] for i in range(20)]

    responses = await asyncio.gather(
        *(_batch(client, "dev-conc", c) for c in chunks)
    )
    delivered: list[int] = []
    accepted_total = 0
    for resp in responses:
        body = resp.json()
        delivered.extend(body["delivered_seq"])
        accepted_total += body["accepted"]

    # Every seq accepted exactly once; seq 0 was delivered by the setup batch.
    assert accepted_total == 199
    assert sorted(delivered) == list(range(1, 200))

    w = (await client.get("/api/v1/devices/dev-conc/window")).json()
    assert w["contiguous_next"] == 200
    assert w["pending_count"] == 0
    assert w["open_gap_count"] == 0


async def test_concurrent_duplicate_batches_are_idempotent(client):
    payload = [{"seq": s, "payload": s} for s in range(50)]

    async def send():
        return await client.post(
            "/api/v1/devices/dev-dup/batches", json={"entries": payload}
        )

    responses = await asyncio.gather(*[send() for _ in range(8)])
    accepted = sum(r.json()["accepted"] for r in responses)
    assert accepted == 50  # across all 8 requests, only one copy each

    w = (await client.get("/api/v1/devices/dev-dup/window")).json()
    assert w["contiguous_next"] == 50


async def test_gap_timer_starts_from_hole_discovery(client, fake_clock):
    # Hole at 0 discovered when seq 5 arrives.
    await _batch(client, "dev-9", [5])
    fake_clock.advance(4.5)  # not expired yet
    w = (await client.get("/api/v1/devices/dev-9/window")).json()
    assert w["open_gap_count"] == 5
    assert w["missing_gap_count"] == 0

    # Filling seq 0 just before timeout stops the whole chain expiring.
    r = await _batch(client, "dev-9", [0, 1, 2, 3, 4])
    assert r.json()["delivered_seq"] == [0, 1, 2, 3, 4, 5]
    assert r.json()["window"]["missing_gap_count"] == 0


async def test_replay_is_order_independent_but_watermark_bounded(client, fake_clock):
    # Declare breaks 0..9 by waiting past timeout behind buffered seq 10.
    await _batch(client, "dev-10", [10], window_size=20, wait_timeout_ms=1000)
    fake_clock.advance(2)
    # Frontier crossed the breaks and delivered 10 -> frontier 11, watermark -9.
    w = (await client.get("/api/v1/devices/dev-10/window")).json()
    assert w["contiguous_next"] == 11
    missing = [
        g["seq"]
        for g in (
            await client.get("/api/v1/devices/dev-10/gaps?status=MISSING")
        ).json()["gaps"]
    ]
    assert missing == list(range(10))

    # Backfilling still-repairable breaks works in any order.
    r = await client.post(
        "/api/v1/devices/dev-10/gaps/replay",
        json={"entries": [{"seq": 9}, {"seq": 8}]},
    )
    body = r.json()
    assert sorted(body["filled_seq"]) == [8, 9]

    # A seq that was never a break (beyond everything) is refused.
    r = await client.post(
        "/api/v1/devices/dev-10/gaps/replay",
        json={"entries": [{"seq": 500}]},
    )
    assert r.json()["not_a_gap"] == 1
    assert r.json()["filled"] == 0


async def test_concurrent_batches_keep_devices_independent(client, fake_clock):
    # Two devices with interleaved concurrent batches must not share state.
    async def send(device, chunk, body=None):
        b = body or {"entries": [{"seq": s} for s in chunk]}
        return await client.post(f"/api/v1/devices/{device}/batches", json=b)

    # Register devices with windows wide enough for their forward spans.
    await send("alpha", [], body={"window_size": 50, "entries": [{"seq": 0}]})
    await send("beta", [], body={"window_size": 500, "entries": [{"seq": 100}]})

    # alpha: 10 concurrent batches partition 1..29 (10..19 stay buffered).
    alpha_seqs = list(range(1, 30))
    alpha_chunks = [alpha_seqs[i::10] for i in range(10)]
    # beta: 10 concurrent batches partition 101..120.
    beta_seqs = list(range(101, 121))
    beta_chunks = [beta_seqs[i::10] for i in range(10)]

    calls = []
    for i in range(10):
        calls.append(send("alpha", alpha_chunks[i]))
        calls.append(send("beta", beta_chunks[i]))
    await asyncio.gather(*calls)

    wa = (await client.get("/api/v1/devices/alpha/window")).json()
    wb = (await client.get("/api/v1/devices/beta/window")).json()

    # alpha accepted unique 0..29 = 30; beta accepted 100..120 = 21.
    # Delivery frontier + pending buffer always accounts for every accepted seq.
    assert wa["contiguous_next"] + wa["pending_count"] == 30
    assert wb["contiguous_next"] + wb["pending_count"] == 21
    # beta holes 0..99 are still OPEN, nothing delivered yet.
    assert wb["contiguous_next"] == 0
    assert wb["open_gaps"] == list(range(0, 100))
    # No OPEN hole appears beyond the earliest buffered entry (window invariant).
    if wa["pending_count"]:
        assert max(wa["open_gaps"], default=wa["contiguous_next"] - 1) < wa["pending_min"]
    # Devices never share sequence state.
    assert wa["device_id"] == "alpha" and wb["device_id"] == "beta"
