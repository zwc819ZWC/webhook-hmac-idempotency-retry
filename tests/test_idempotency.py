"""
Tests for IdempotencyStore.

Coverage:
1. Looking up a never-seen event returns None.
2. Round-trip: save then get returns the cached (status_code, body).
3. Saving the same event_id twice raises IntegrityError (concurrency guard).
4. Cleanup deletes old records based on age.
5. Different event_ids are independent (no cross-contamination).
"""
import sqlite3
import time

import pytest

from app.idempotency import IdempotencyStore


@pytest.fixture
def store():
    """A fresh in-memory IdempotencyStore for each test."""
    s = IdempotencyStore(":memory:")
    yield s
    s.close()


def test_never_seen_event_returns_none(store):
    """get_processed on an unknown event_id returns None."""
    result = store.get_processed("evt_does_not_exist")
    
    assert result is None


def test_save_then_get_returns_cached_response(store):
    """After saving, get_processed returns the (status_code, body) tuple."""
    store.save("evt_abc123", 200, '{"status": "ok"}')
    
    result = store.get_processed("evt_abc123")
    
    assert result == (200, '{"status": "ok"}')


def test_duplicate_save_raises_integrity_error(store):
    """Saving the same event_id twice raises sqlite3.IntegrityError."""
    store.save("evt_abc123", 200, '{"status": "ok"}')
    
    with pytest.raises(sqlite3.IntegrityError):
        store.save("evt_abc123", 200, '{"status": "ok_again"}')


def test_different_event_ids_are_independent(store):
    """Different event_ids don't interfere with each other."""
    store.save("evt_alpha", 200, '{"id": "alpha"}')
    store.save("evt_beta", 201, '{"id": "beta"}')
    
    assert store.get_processed("evt_alpha") == (200, '{"id": "alpha"}')
    assert store.get_processed("evt_beta") == (201, '{"id": "beta"}')


def test_cleanup_removes_records_older_than_threshold(tmp_path):
    """cleanup_old deletes records past the retention window."""
    # Use a file DB so we can manipulate processed_at via raw SQL
    db_path = str(tmp_path / "idempotency.db")
    store = IdempotencyStore(db_path)
    try:
        store.save("evt_recent", 200, "{}")
        store.save("evt_old", 200, "{}")
        
        # Backdate evt_old to 60 days ago
        store._conn.execute(
            "UPDATE idempotency_keys SET processed_at = datetime('now', '-60 days') "
            "WHERE event_id = ?",
            ("evt_old",),
        )
        store._conn.commit()
        
        # Cleanup with 30-day retention
        deleted = store.cleanup_old(days=30)
        
        assert deleted == 1
        assert store.get_processed("evt_old") is None
        assert store.get_processed("evt_recent") == (200, "{}")
    finally:
        store.close()