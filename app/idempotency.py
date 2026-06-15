"""
Idempotency store using SQLite for webhook event deduplication.

Pattern:
1. Before processing a webhook event, call get_processed(event_id).
   - Returns (status_code, body) if already processed → return that cached response.
   - Returns None if first time → process the event normally.
2. After processing, call save(event_id, status_code, body) to record the result.
3. Future duplicate requests with the same event_id get the cached response.

Storage: SQLite (file-based, persistent, single-process for take-home demo).
Concurrency: PRIMARY KEY on event_id provides write-level uniqueness.
"""
import sqlite3
from typing import Optional, Tuple


class IdempotencyStore:
    """SQLite-backed idempotency store for webhook events."""

    def __init__(self, db_path: str):
        """
        Initialize the store and ensure the table exists.

        Args:
            db_path: Path to SQLite DB file. Use ":memory:" for in-memory (tests).
        """
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS idempotency_keys (
                event_id      TEXT PRIMARY KEY,
                status_code   INTEGER NOT NULL,
                response_body TEXT NOT NULL,
                processed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self._conn.commit()

    def get_processed(self, event_id: str) -> Optional[Tuple[int, str]]:
        """
        Look up a previously processed event.

        Args:
            event_id: The unique event identifier from the webhook payload.

        Returns:
            (status_code, response_body) if event was already processed.
            None if event was never seen.
        """
        cursor = self._conn.execute(
            "SELECT status_code, response_body FROM idempotency_keys WHERE event_id = ?",
            (event_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return (row[0], row[1])

    def save(self, event_id: str, status_code: int, response_body: str) -> None:
        """
        Record the result of processing an event.

        If the event_id already exists (concurrent duplicate), raises 
        sqlite3.IntegrityError. Callers should catch this and re-query via 
        get_processed() to retrieve the winning response from the first request.

        Args:
            event_id: The unique event identifier.
            status_code: HTTP status code we returned to the sender.
            response_body: The response body string we returned.

        Raises:
            sqlite3.IntegrityError: If event_id already exists.
        """
        self._conn.execute(
            "INSERT INTO idempotency_keys (event_id, status_code, response_body) "
            "VALUES (?, ?, ?)",
            (event_id, status_code, response_body),
        )
        self._conn.commit()

    def cleanup_old(self, days: int = 30) -> int:
        """
        Delete records older than `days` days to prevent unbounded table growth.

        Args:
            days: Retention window. Default 30 days (covers Stripe's 3-day retry 
                  window with margin).

        Returns:
            Number of records deleted.
        """
        cursor = self._conn.execute(
            "DELETE FROM idempotency_keys WHERE processed_at < datetime('now', ?)",
            (f"-{days} days",),
        )
        self._conn.commit()
        return cursor.rowcount

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()