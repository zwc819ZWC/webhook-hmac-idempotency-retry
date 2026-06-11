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

- Schedule: 1s → 2s → 4s → 8s → 16s → ... up to max 1 hour
- Max attempts: 8
- Persistence: failed events stored, retried by background worker
- **Tradeoff**: Aggressive retry vs downstream overload

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