## Status

**Current version: v0.4** (HMAC + Idempotency + Retry Queue + Retry Worker)

### ✅ Implemented

- **HMAC-SHA256 signature verification** (`app/hmac_verify.py`)
  - Constant-time comparison to prevent timing attacks
  - Timestamp-based replay protection (5-minute window by default)
  - Graceful handling of malformed timestamps
  - 4 unit tests passing

- **Idempotency store using SQLite** (`app/idempotency.py`)
  - PRIMARY KEY on event_id for concurrency safety
  - get/save/cleanup_old API
  - In-memory mode for tests, file-based for production demo
  - 5 unit tests passing

- **Retry queue with exponential backoff** (`app/retry_queue.py`)
  - PRIMARY KEY on event_id prevents duplicate enqueue (mirrors idempotency design)
  - Exponential backoff: 60s → 120s → 240s → ... capped at 1 hour
  - Dead-letter queue (DLQ): records flip to `FAILED_PERMANENTLY` after `max_retries` failures (default 5), preserved for audit and manual replay
  - API: `enqueue` / `claim_due_retries` / `mark_succeeded` / `mark_failed` / `cleanup_old`
  - Timezone-aware UTC datetimes (no `datetime.utcnow()` deprecation)
  - 5 unit tests passing

- **Retry worker** (`app/retry_worker.py`)
  - Active scanner: claims due retries, replays each via an injected `delivery_fn`, reports outcome back to the store
  - Separation of concerns: worker never computes backoff or decides DLQ — the store owns retry policy; the worker only calls `mark_succeeded` / `mark_failed`
  - Per-record exception isolation: one failing delivery does not block the rest of the batch
  - `run_once()` (testable single tick) + `run_forever(interval, max_ticks)`
  - 6 tests passing, including an end-to-end fail→fail→succeed pipeline test

### 🚧 In progress (v1.0 by 2026-06-19)

- FastAPI receiver endpoint wiring HMAC + Idempotency + Retry (the only remaining v1.0 piece)

### ✅ Done toward v1.0

- Background worker scanning retry queue (v0.4)
- End-to-end integration tests (v0.4)

# Webhook Receiver with HMAC + Idempotency + Retry

A production-grade webhook receiver that demonstrates the three patterns most commonly missed in webhook integrations:

1. **HMAC signature verification** — reject forged requests
2. **Idempotency** — handle duplicate deliveries safely
3. **Exponential backoff retry** — survive transient downstream failures

## Why this exists

Most webhook tutorials skip the three things that actually break in production. This repo is a minimal reference implementation that shows the patterns end-to-end.

## Architecture
[External Sender] → POST /webhook → [HMAC Verify] → [Idempotency Check] → [Persist Event] → [Async Worker] → [Downstream API]
│
└─ on failure → [Retry Queue with Backoff]

## Tech stack

- **Language**: Python 3.11 + FastAPI（待定，可换 Node.js + Express）
- **Storage**: PostgreSQL（event log + idempotency keys）
- **Queue**: 内置 in-process queue（避免依赖 RabbitMQ 让 reviewer 跑不起来）
- **Tests**: pytest + httpx

## Design decisions

### 1. HMAC verification

- Algorithm: HMAC-SHA256
- Signature header: `X-Webhook-Signature: sha256=<hex>`
- Timestamp header: `X-Webhook-Timestamp: <unix_seconds>` for replay protection
- Replay window: 5 minutes
- **Tradeoff**: Replay window vs clock skew tolerance

### 2. Idempotency

- Idempotency key: `event_id` from request body (sender-provided)
- Storage: `idempotency_keys` table with `(event_id, processed_at)` PK
- On duplicate: return 200 with cached response (not 409, to avoid sender retries)
- **Tradeoff**: TTL on idempotency keys vs storage cost

### 3. Retry with exponential backoff

- Schedule: 60s → 120s → 240s → 480s → ... capped at 1 hour (`compute_backoff`)
- Max attempts: `max_retries` (default 5); on exhaustion the record flips to `FAILED_PERMANENTLY` (DLQ) for audit / manual replay
- Persistence: failed events stored in SQLite, retried by the background worker (`RetryWorker`)
- **Tradeoff**: aggressive retry vs downstream overload; production would add jitter to avoid thundering-herd on downstream recovery

## How to run

```bash
# Install
pip install -r requirements.txt

# Set up DB
docker-compose up -d postgres
alembic upgrade head

# Run
uvicorn app.main:app --reload

# Send test webhook
python scripts/send_test_webhook.py
```

## Test

```bash
pytest
pytest --cov=app
```

## API

### POST /webhook

Receive an inbound webhook.

**Headers**:
- `X-Webhook-Signature` (required): HMAC-SHA256 hex digest
- `X-Webhook-Timestamp` (required): Unix timestamp
- `Content-Type`: application/json

**Body**:
```json
{
  "event_id": "evt_abc123",
  "event_type": "user.created",
  "data": { "user_id": "u_123" }
}
```

**Response**:
- 200 OK: accepted (new or duplicate)
- 401 Unauthorized: HMAC invalid
- 400 Bad Request: missing headers or malformed
- 408 Request Timeout: timestamp outside replay window

## What's not in scope

- Production rate limiting (would add nginx / Redis token bucket in real prod)
- Multi-tenant tenant_id scoping (single-tenant for demo simplicity)
- Distributed retry queue (in-process queue for demo; production would use Redis / RabbitMQ)

## License

MIT