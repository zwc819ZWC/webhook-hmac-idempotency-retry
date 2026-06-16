"""
Retry queue using SQLite for webhook delivery failures.

Pattern:
1. When a webhook's business handler fails, call enqueue(event_id, payload, error).
   - Inserts a PENDING record with next_retry_at = now + backoff(0).
   - Duplicate event_id raises sqlite3.IntegrityError (caller silently skips).
2. Worker periodically calls claim_due_retries(limit) to fetch records where
   status='PENDING' AND next_retry_at <= now, ordered by next_retry_at ASC.
3. Worker retries each. On success call mark_succeeded(event_id).
   On failure call mark_failed(event_id, error) — increments retry_count,
   pushes next_retry_at forward by exponential backoff.
4. When retry_count reaches max_retries, mark_failed flips status to
   FAILED_PERMANENTLY (dead-letter queue) — no further automatic retries.
   Human review / manual replay required.

Storage: SQLite (file-based, persistent, single-process for take-home demo).
Backoff: exponential, base 60s, capped at 1 hour (60 * 2^retry_count).
DLQ: status='FAILED_PERMANENTLY' preserves the record for audit & manual replay.
"""
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple


# ---- Module-level helper ----

def compute_backoff(retry_count: int) -> int:
    """
    Compute exponential backoff delay in seconds.

    Formula: min(MAX_SECONDS, BASE_SECONDS * 2 ** retry_count)

    Time series:
        retry 0 -> 60s   (1 min)
        retry 1 -> 120s  (2 min)
        retry 2 -> 240s  (4 min)
        retry 3 -> 480s  (8 min)
        retry 4 -> 960s  (16 min)
        retry 5 -> 1920s (32 min)
        retry 6 -> 3600s (capped at 1 hour)
        retry 7+ -> 3600s (no growth)

    Production-grade implementations should add jitter (random offset) to
    avoid thundering herd on downstream recovery. Omitted here for simplicity.

    Args:
        retry_count: Number of retries already attempted (0-indexed).

    Returns:
        Seconds to wait before next retry attempt.
    """
    BASE_SECONDS = 60
    MAX_SECONDS = 3600
    return min(MAX_SECONDS, BASE_SECONDS * (2 ** retry_count))

# ---- Main class ----

class RetryQueueStore:
    """SQLite-backed retry queue for failed webhook deliveries."""

    # ---- Status constants ----
    PENDING = "PENDING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_PERMANENTLY = "FAILED_PERMANENTLY"

    def __init__(self, db_path: str):
        """
        Initialize the store and ensure the retry_queue table exists.

        Args:
            db_path: Path to SQLite DB file. Use ":memory:" for in-memory (tests).
        """
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS retry_queue (
                event_id        TEXT PRIMARY KEY,
                payload         TEXT NOT NULL,
                retry_count     INTEGER NOT NULL DEFAULT 0,
                max_retries     INTEGER NOT NULL DEFAULT 5,
                next_retry_at   TIMESTAMP NOT NULL,
                status          TEXT NOT NULL DEFAULT 'PENDING',
                last_error      TEXT,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self._conn.commit()

    def enqueue(
        self,
        event_id: str,
        payload: str,
        last_error: Optional[str] = None,
        max_retries: int = 5,
    ) -> None:
        """
        Insert a new failed delivery into the retry queue.

        next_retry_at is set to now + compute_backoff(0) — i.e. the first
        retry happens 60 seconds after enqueue. retry_count starts at 0;
        the worker will increment it on each failed retry.

        Duplicate event_id raises sqlite3.IntegrityError. Caller should catch
        and silently skip — the event is already in the queue (or has been
        moved to SUCCEEDED / FAILED_PERMANENTLY), and we never want two
        concurrent retry pipelines for the same event_id.

        Args:
            event_id:   Unique event identifier from the webhook payload.
            payload:    Original webhook body (JSON string). Stored verbatim
                        so the worker can replay it later.
            last_error: Optional error message from the failed business
                        handler (for audit / debugging).
            max_retries: Maximum retry attempts before flipping to
                         FAILED_PERMANENTLY (DLQ). Default 5.

        Raises:
            sqlite3.IntegrityError: If event_id already exists in the queue.
        """
        next_retry_at = (datetime.now(timezone.utc) + timedelta(seconds=compute_backoff(0))).strftime("%Y-%m-%d %H:%M:%S")
        self._conn.execute(
            "INSERT INTO retry_queue "
            "(event_id, payload, retry_count, max_retries, next_retry_at, "
            " status, last_error) "
            "VALUES (?, ?, 0, ?, ?, ?, ?)",
            (
                event_id,
                payload,
                max_retries,
                next_retry_at,
                self.PENDING,
                last_error,
            ),
        )
        self._conn.commit()

    def claim_due_retries(self, limit: int = 50) -> List[Tuple[str, str, int, int]]:
        """
        Fetch up to `limit` records whose retries are due.

        A record is "due" when:
            status = 'PENDING'  AND  next_retry_at <= datetime('now')

        Ordered by next_retry_at ASC so the oldest-due record is retried first
        (FIFO fairness — a record that has been waiting 1 hour is processed
        before one that just became due).

        Worker workflow:
            1. for (event_id, payload, retry_count, max_retries) in claim:
            2.     try: replay business handler with payload
            3.            -> on success: mark_succeeded(event_id)
            4.     except Exception as e:
            5.            -> on failure: mark_failed(event_id, str(e))

        Args:
            limit: Maximum number of records to return per scan. Default 50
                   keeps each worker tick bounded (one slow downstream call
                   times out 50 quickly enough).

        Returns:
            List of (event_id, payload, retry_count, max_retries) tuples.
            Empty list if nothing is due.
        """
        cursor = self._conn.execute(
            "SELECT event_id, payload, retry_count, max_retries "
            "FROM retry_queue "
            "WHERE status = ? AND next_retry_at <= datetime('now') "
            "ORDER BY next_retry_at ASC "
            "LIMIT ?",
            (self.PENDING, limit),
        )
        return cursor.fetchall()

    def mark_succeeded(self, event_id: str) -> None:
        """
        Mark an event as successfully retried — terminal state.

        Status flips to SUCCEEDED. The record is retained for audit / SLA
        reporting; can be purged later via cleanup_old().

        Args:
            event_id: The event identifier.
        """
        self._conn.execute(
            "UPDATE retry_queue "
            "SET status = ?, updated_at = CURRENT_TIMESTAMP "
            "WHERE event_id = ?",
            (self.SUCCEEDED, event_id),
        )
        self._conn.commit()

    def mark_failed(self, event_id: str, error: str) -> None:
        """
        Record a failed retry attempt.

        Two outcomes depending on retry_count:
        1. If new retry_count >= max_retries: flip to FAILED_PERMANENTLY (DLQ).
           No further automatic retries. Operator must manually review and,
           if appropriate, reset status to PENDING for manual replay.
        2. Otherwise: stay PENDING, push next_retry_at forward by
           compute_backoff(new retry_count). Worker will pick it up again
           after the backoff window.

        If event_id is not in the queue, silently no-ops (avoids crashing
        the worker on race conditions).

        Args:
            event_id: The event identifier.
            error:    Error message from the failed retry attempt; stored
                      in last_error for audit.
        """
        cursor = self._conn.execute(
            "SELECT retry_count, max_retries FROM retry_queue "
            "WHERE event_id = ?",
            (event_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return  # Event not in queue; nothing to do.

        new_retry_count = row[0] + 1
        max_retries = row[1]

        if new_retry_count >= max_retries:
            # DLQ
            self._conn.execute(
                "UPDATE retry_queue "
                "SET retry_count = ?, status = ?, last_error = ?, "
                "    updated_at = CURRENT_TIMESTAMP "
                "WHERE event_id = ?",
                (new_retry_count, self.FAILED_PERMANENTLY, error, event_id),
            )
        else:
            # Schedule next retry
            next_retry_at = (
                datetime.now(timezone.utc)
                + timedelta(seconds=compute_backoff(new_retry_count))
            ).strftime("%Y-%m-%d %H:%M:%S")
            self._conn.execute(
                "UPDATE retry_queue "
                "SET retry_count = ?, next_retry_at = ?, last_error = ?, "
                "    updated_at = CURRENT_TIMESTAMP "
                "WHERE event_id = ?",
                (new_retry_count, next_retry_at, error, event_id),
            )

        self._conn.commit()

    def cleanup_old(self, days: int = 30) -> int:
        """
        Delete terminal records (SUCCEEDED / FAILED_PERMANENTLY) older than 
        `days` days to prevent unbounded table growth.

        Only terminal states are purged — PENDING records are never deleted
        regardless of age (they may still be in active retry).

        Args:
            days: Retention window. Default 30 days. 
                  Match this to your audit / SLA reporting window.

        Returns:
            Number of records deleted.
        """
        cursor = self._conn.execute(
            "DELETE FROM retry_queue "
            "WHERE status IN (?, ?) "
            "  AND updated_at < datetime('now', ?)",
            (self.SUCCEEDED, self.FAILED_PERMANENTLY, f"-{days} days"),
        )
        self._conn.commit()
        return cursor.rowcount
    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()