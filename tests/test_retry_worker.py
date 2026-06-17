"""
Tests for RetryWorker + end-to-end retry pipeline integration.

Coverage:
1. Empty queue -> run_once does nothing, all counts zero.
2. Due record + successful delivery -> mark_succeeded (status SUCCEEDED).
3. Due record + failing delivery -> mark_failed (retry_count++, stays PENDING).
4. Repeated failures -> record lands in DLQ (FAILED_PERMANENTLY) at max_retries.
5. Exception isolation: one failing record does not block others in the batch.
6. END-TO-END: enqueue -> fail a few times -> finally succeed -> SUCCEEDED.
"""
from datetime import datetime, timedelta, timezone

import pytest

from app.retry_queue import RetryQueueStore
from app.retry_worker import RetryWorker


@pytest.fixture
def store():
    """A fresh in-memory RetryQueueStore for each test."""
    s = RetryQueueStore(":memory:")
    yield s
    s.close()


def _backdate(store, event_id, seconds_ago=120):
    """Push next_retry_at into the past so claim_due_retries picks it up."""
    past = (
        datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    ).strftime("%Y-%m-%d %H:%M:%S")
    store._conn.execute(
        "UPDATE retry_queue SET next_retry_at = ? WHERE event_id = ?",
        (past, event_id),
    )
    store._conn.commit()


def _status(store, event_id):
    return store._conn.execute(
        "SELECT status FROM retry_queue WHERE event_id = ?",
        (event_id,),
    ).fetchone()[0]


def test_run_once_empty_queue_is_noop(store):
    """Nothing due -> worker reports all-zero and never calls delivery_fn."""
    calls = []
    worker = RetryWorker(store, delivery_fn=lambda payload: calls.append(payload))

    summary = worker.run_once()

    assert summary == {"claimed": 0, "succeeded": 0, "failed": 0}
    assert calls == []


def test_run_once_successful_delivery_marks_succeeded(store):
    """A due record delivered successfully ends in SUCCEEDED."""
    store.enqueue("evt_ok", '{"amount": 100}')
    _backdate(store, "evt_ok")
    worker = RetryWorker(store, delivery_fn=lambda payload: None)  # never raises

    summary = worker.run_once()

    assert summary == {"claimed": 1, "succeeded": 1, "failed": 0}
    assert _status(store, "evt_ok") == "SUCCEEDED"


def test_run_once_failed_delivery_marks_failed_and_reschedules(store):
    """A due record whose delivery raises stays PENDING with retry_count++."""
    store.enqueue("evt_fail", "{}", max_retries=5)
    _backdate(store, "evt_fail")

    def boom(payload):
        raise RuntimeError("downstream 503")

    worker = RetryWorker(store, delivery_fn=boom)
    summary = worker.run_once()

    assert summary == {"claimed": 1, "succeeded": 0, "failed": 1}
    row = store._conn.execute(
        "SELECT retry_count, status, last_error FROM retry_queue "
        "WHERE event_id = ?",
        ("evt_fail",),
    ).fetchone()
    assert row == (1, "PENDING", "downstream 503")


def test_repeated_failures_land_in_dlq(store):
    """With max_retries=3, 3 failing ticks flip the record to DLQ."""
    store.enqueue("evt_dlq", "{}", max_retries=3)

    def boom(payload):
        raise RuntimeError("permanent failure")

    worker = RetryWorker(store, delivery_fn=boom)
    for _ in range(3):
        _backdate(store, "evt_dlq")  # make it due again each tick
        worker.run_once()

    assert _status(store, "evt_dlq") == "FAILED_PERMANENTLY"
    # DLQ record is never claimed again even if backdated
    _backdate(store, "evt_dlq")
    assert worker.run_once() == {"claimed": 0, "succeeded": 0, "failed": 0}


def test_one_failure_does_not_block_others(store):
    """A raised exception on one record is isolated; the batch continues."""
    store.enqueue("evt_bad", "{}")
    store.enqueue("evt_good", "{}")
    _backdate(store, "evt_bad")
    _backdate(store, "evt_good")

    def selective(payload):
        # Fails only for the bad event's payload marker.
        if payload == "BAD":
            raise RuntimeError("nope")

    # Re-enqueue with distinguishable payloads.
    store.close()
    s2 = RetryQueueStore(":memory:")
    s2.enqueue("evt_bad", "BAD")
    s2.enqueue("evt_good", "GOOD")
    _backdate(s2, "evt_bad")
    _backdate(s2, "evt_good")

    worker = RetryWorker(s2, delivery_fn=selective)
    summary = worker.run_once()

    assert summary == {"claimed": 2, "succeeded": 1, "failed": 1}
    assert _status(s2, "evt_good") == "SUCCEEDED"
    assert _status(s2, "evt_bad") == "PENDING"  # rescheduled, not DLQ yet
    s2.close()


def test_end_to_end_fail_then_eventually_succeed(store):
    """
    Full pipeline: a delivery fails twice, then succeeds on the 3rd tick.

    Simulates a downstream that is temporarily down and then recovers — the
    worker keeps the record PENDING through failures, then closes it out as
    SUCCEEDED once delivery works. This is the core reliability guarantee:
    transient failures are retried; a recovered downstream is eventually
    delivered, not lost.
    """
    store.enqueue("evt_e2e", '{"order": "A1"}', max_retries=5)

    attempts = {"n": 0}

    def flaky(payload):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError(f"downstream down (attempt {attempts['n']})")
        # 3rd attempt succeeds

    worker = RetryWorker(store, delivery_fn=flaky)

    # Tick 1: fails
    _backdate(store, "evt_e2e")
    assert worker.run_once() == {"claimed": 1, "succeeded": 0, "failed": 1}
    assert _status(store, "evt_e2e") == "PENDING"

    # Tick 2: fails again
    _backdate(store, "evt_e2e")
    assert worker.run_once() == {"claimed": 1, "succeeded": 0, "failed": 1}
    assert _status(store, "evt_e2e") == "PENDING"

    # Tick 3: downstream recovered -> success
    _backdate(store, "evt_e2e")
    assert worker.run_once() == {"claimed": 1, "succeeded": 1, "failed": 0}
    assert _status(store, "evt_e2e") == "SUCCEEDED"
    assert attempts["n"] == 3
