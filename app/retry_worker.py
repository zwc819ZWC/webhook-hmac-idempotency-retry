"""
Retry worker that drains the RetryQueueStore and replays failed webhooks.

This is the *active scanner* half of the retry system. RetryQueueStore is the
state (what is due, how many attempts, when next); RetryWorker is the motion
(claim due records, replay each, report the outcome back to the store).

Separation of concerns (important):
- The worker NEVER computes backoff or decides DLQ. It only calls
  mark_succeeded / mark_failed. The store owns the backoff formula and the
  "retry_count >= max_retries -> FAILED_PERMANENTLY" decision. The worker is
  deliberately dumb so the retry policy lives in exactly one place.
- The worker does not know HOW to deliver a webhook. The delivery function is
  injected (dependency injection): delivery_fn(payload) returns on success and
  raises any Exception on failure. This makes the worker trivially testable
  with a fake delivery_fn and reusable for any downstream.

Real-world parallel: this mirrors a Transactional-Outbox / scheduled relay
worker (e.g. a Spring @Scheduled job that scans PENDING rows and re-publishes
them to a message broker). Same pattern: a worker actively scans a durable
"pending" table and pushes records forward — it does not passively wait.
"""
import time
from typing import Callable, Dict, Optional

from app.retry_queue import RetryQueueStore


class RetryWorker:
    """Drains due retries from a RetryQueueStore and replays them."""

    def __init__(
        self,
        store: RetryQueueStore,
        delivery_fn: Callable[[str], None],
        limit: int = 50,
    ):
        """
        Args:
            store:       The RetryQueueStore holding pending failed deliveries.
            delivery_fn: Callable invoked as delivery_fn(payload). Returns on
                         success; raises any Exception on failure. The worker
                         treats any raised exception as a failed retry.
            limit:       Max records claimed per run_once tick. Bounds the work
                         per scan so one slow downstream can't make a tick
                         unbounded.
        """
        self._store = store
        self._delivery_fn = delivery_fn
        self._limit = limit

    def run_once(self) -> Dict[str, int]:
        """
        Process one batch of due retries.

        Workflow:
            1. claim_due_retries(limit) -> due records (status=PENDING and
               next_retry_at <= now).
            2. For each record, call delivery_fn(payload).
               - returns        -> mark_succeeded(event_id)
               - raises          -> mark_failed(event_id, str(error))
                 (store decides: reschedule with backoff, or DLQ at max_retries)
            3. One record's failure is isolated: a raised exception is caught
               per-record so the rest of the batch still gets processed.

        Returns:
            Summary counts: {"claimed": N, "succeeded": N, "failed": N}.
        """
        due = self._store.claim_due_retries(limit=self._limit)
        succeeded = 0
        failed = 0

        for event_id, payload, _retry_count, _max_retries in due:
            try:
                self._delivery_fn(payload)
            except Exception as e:  # noqa: BLE001 - any failure is a failed retry
                self._store.mark_failed(event_id, str(e))
                failed += 1
            else:
                self._store.mark_succeeded(event_id)
                succeeded += 1

        return {"claimed": len(due), "succeeded": succeeded, "failed": failed}

    def run_forever(
        self,
        interval_seconds: float = 60.0,
        max_ticks: Optional[int] = None,
    ) -> None:
        """
        Continuously call run_once, sleeping interval_seconds between ticks.

        Args:
            interval_seconds: Sleep between scans. In production this is the
                              worker's polling cadence (e.g. 60s).
            max_ticks:        If set, stop after this many ticks (used by tests
                              and bounded demos). None = run indefinitely.

        Note: single-threaded blocking loop — adequate for a take-home demo.
        A production deployment would run this in its own process/thread (or as
        a cron-triggered one-shot calling run_once) and add graceful shutdown.
        """
        ticks = 0
        while max_ticks is None or ticks < max_ticks:
            self.run_once()
            ticks += 1
            if max_ticks is not None and ticks >= max_ticks:
                break
            time.sleep(interval_seconds)
