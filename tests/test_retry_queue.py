"""
Tests for RetryQueueStore.

Coverage:
1. A freshly enqueued event is not yet due (next_retry_at = now + 60s).
2. Duplicate enqueue with the same event_id raises IntegrityError.
3. mark_failed increments retry_count, pushes next_retry_at forward, and
   keeps status PENDING (still claimable later).
4. mark_failed flips status to FAILED_PERMANENTLY once retry_count reaches
   max_retries (DLQ); DLQ records are never returned by claim_due_retries.
5. compute_backoff produces the expected exponential series and caps at
   MAX_SECONDS (1 hour).
"""
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from app.retry_queue import RetryQueueStore, compute_backoff


@pytest.fixture
def store():
    """A fresh in-memory RetryQueueStore for each test."""
    s = RetryQueueStore(":memory:")
    yield s
    s.close()


def _backdate(store, event_id, seconds_ago=120):
    """
    Push next_retry_at into the past so claim_due_retries picks it up.

    Test helper: avoids waiting wall-clock 60 seconds for the first retry
    to become due. Mirrors how a production worker would naturally see
    records age past their backoff window.
    """
    past = (
        datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    ).strftime("%Y-%m-%d %H:%M:%S")
    store._conn.execute(
        "UPDATE retry_queue SET next_retry_at = ? WHERE event_id = ?",
        (past, event_id),
    )
    store._conn.commit()


def test_just_enqueued_event_is_not_yet_due(store):
    """Fresh enqueue sets next_retry_at = now + 60s, so claim returns []."""
    store.enqueue("evt_001", '{"amount": 100}')

    due = store.claim_due_retries(limit=10)

    assert due == []


def test_duplicate_enqueue_raises_integrity_error(store):
    """PRIMARY KEY on event_id prevents the same event entering twice."""
    store.enqueue("evt_001", "{}")

    with pytest.raises(sqlite3.IntegrityError):
        store.enqueue("evt_001", "{}")


def test_mark_failed_increments_count_and_reschedules(store):
    """
    After 1 failure: retry_count=1, status=PENDING, last_error stored,
    and next_retry_at is pushed forward (no longer claimable).
    """
    store.enqueue("evt_001", "{}", max_retries=5)
    _backdate(store, "evt_001")

    store.mark_failed("evt_001", "downstream timeout")

    row = store._conn.execute(
        "SELECT retry_count, status, last_error "
        "FROM retry_queue WHERE event_id = ?",
        ("evt_001",),
    ).fetchone()

    assert row == (1, "PENDING", "downstream timeout")
    # next_retry_at was pushed to now+120s → claim returns nothing
    assert store.claim_due_retries(limit=10) == []


def test_mark_failed_hits_dlq_at_max_retries(store):
    """
    With max_retries=3, the 3rd failure flips status to FAILED_PERMANENTLY.
    DLQ records are never claimed even when their next_retry_at is past.
    """
    store.enqueue("evt_001", "{}", max_retries=3)

    for i in range(3):
        _backdate(store, "evt_001")
        store.mark_failed("evt_001", f"attempt {i + 1}")

    row = store._conn.execute(
        "SELECT retry_count, status FROM retry_queue WHERE event_id = ?",
        ("evt_001",),
    ).fetchone()

    assert row == (3, "FAILED_PERMANENTLY")
    # DLQ records are never returned by claim, even if backdated
    _backdate(store, "evt_001")
    assert store.claim_due_retries(limit=10) == []


def test_compute_backoff_produces_exponential_series_and_caps():
    """Backoff doubles each retry up to MAX_SECONDS (3600) cap."""
    series = [compute_backoff(i) for i in range(8)]

    assert series == [60, 120, 240, 480, 960, 1920, 3600, 3600]
