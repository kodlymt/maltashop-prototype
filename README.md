# MaltaShop – "Place an Order" microservices prototype

Prototype for **Unit 5 – SOA and Microservices Fundamentals** (MaltaShop Ltd case study).
It shows how MaltaShop's checkout can be split out of the PHP monolith without losing
consistency, and how a failure in notifications can no longer take checkout down.

## What the system is

| Service | Responsibility | Owns data store | Port |
|---|---|---|---|
| `order-service` | REST API `POST /orders`, place-order **saga orchestrator**, transactional **outbox** publisher | PostgreSQL `orders` (order-db) | 3001 |
| `inventory-service` | Stock levels and **idempotent** stock reservations (reserve / release / commit, TTL expiry) | PostgreSQL `inventory` (inventory-db) | 3002 |
| `notification-service` | Consumes `OrderPlaced`, sends (simulated) confirmation email, **idempotent consumer** (inbox table), dead-letter queue | PostgreSQL `notifications` (notification-db) | 3003 |
| `rabbitmq` | Message broker – topic exchange `maltashop.events` | – | 15672 (UI) |

```
client ──REST──▶ order-service ──REST (sync, timeout+retry)──▶ inventory-service
                      │  (mock payment provider adapter)
                      │  orders + outbox (one local transaction)
                      ▼
               outbox relay ──AMQP OrderPlaced (persistent, publisher confirms)──▶ RabbitMQ
                                                                                   │
                                                  notification-service ◀───────────┘
                                                  (manual ack, inbox de-dup, DLQ)
```

Flow of `POST /orders`:
1. Order is stored as `PENDING` (unique `Idempotency-Key` ⇒ a retried request returns the same order).
2. Stock is reserved in Inventory (reservation id = order id ⇒ retries are safe).
3. Card is charged through the payment-provider adapter (idempotency key = order id).
   On decline the **compensating action** releases the reservation and the order becomes `PAYMENT_FAILED`.
4. Order becomes `CONFIRMED` and an `OrderPlaced` event is written to the outbox **in the same transaction**.
5. The relay publishes it to RabbitMQ; Notification consumes it and acks only after its DB commit.

Errors use one format everywhere: `{ "status", "code", "message", "requestId" }`, where `requestId`
is the correlation ID.

Container hardening (Lecture 5): databases sit on an `internal` network with no route to the host;
published ports bind to `127.0.0.1` only; service containers are read-only with an in-memory `/tmp`;
each runs as the non-root `node` user with a memory limit.

Every log line is JSON on stdout with `service`, `version`, `environment`, `event` and the
`correlation_id` that travels in the `X-Correlation-ID` HTTP header and the AMQP `correlationId` property.

## How to run it

Requirements: Docker Desktop (or Docker Engine with Compose v2) and Node.js 22 (only for the tests/smoke test).

```bash
cp .env.example .env          # then change the passwords
docker compose up --build     # starts all 7 containers; wait until all are "healthy"
```

Place an order (PowerShell users: use `curl.exe`):

```bash
curl -i -X POST http://localhost:3001/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-001" \
  -H "X-Correlation-ID: demo-corr-001" \
  -d '{"customerId":"cust-1001","paymentToken":"tok_visa","items":[{"sku":"TV-55-OLED","qty":1,"unitPriceCents":129900}]}'
```

See the event being consumed and the shared correlation ID:

```bash
docker compose logs order-service notification-service | grep demo-corr-001
curl "http://localhost:3003/notifications?orderId=<orderId from the response>"
```

Other scenarios: `"paymentToken":"tok_decline"` (compensation), `"sku":"LAMP-DESK","qty":99` (out of stock),
repeat the same `Idempotency-Key` (idempotent replay). RabbitMQ UI: http://localhost:15672.

Run the automated end-to-end smoke test (stack must be running):

```bash
node scripts/smoke-test.mjs
```

Run unit + contract tests for one service:

```bash
cd order-service && npm ci && npm test
```

### Replaying the March incident (fault isolation)

Set `SIMULATED_LEAK_KB_PER_MESSAGE=20000` in `.env`, run `docker compose up -d`, then place ~15 orders.
`notification-service` grows until its 256 MB limit, is OOM-killed and restarted by Docker,
while `order-service` keeps returning `201` and the events wait in the queue until it is back.

## Environment variables

| Variable | Used by | Purpose | Default |
|---|---|---|---|
| `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | compose, order, notification | Broker credentials | **required** |
| `ORDER_DB_PASSWORD` / `INVENTORY_DB_PASSWORD` / `NOTIFICATION_DB_PASSWORD` | compose | Password of each service's own database | **required** |
| `APP_VERSION` | all | Image tag and `version` field in logs (CI sets it to the commit SHA) | `dev` |
| `DATABASE_URL` | all services | Connection string to the service's own DB (built by compose) | – |
| `RABBITMQ_URL` | order, notification | AMQP URL (built by compose) | – |
| `INVENTORY_URL` | order | Base URL of inventory-service | – |
| `HTTP_TIMEOUT_MS`, `HTTP_MAX_ATTEMPTS` | order | Timeout and retry budget for calls to Inventory | `2000`, `3` |
| `OUTBOX_POLL_MS` | order | How often the outbox relay runs | `500` |
| `EVENTS_EXCHANGE` | order, notification | Topic exchange name | `maltashop.events` |
| `RESERVATION_TTL_SECONDS` | inventory | Unconfirmed holds expire after this | `900` |
| `QUEUE_NAME`, `PREFETCH` | notification | Queue name and max unacked messages | `notification.order-placed`, `10` |
| `SIMULATED_LEAK_KB_PER_MESSAGE` | notification | Fault injection for the incident demo | `0` |
| `PORT`, `APP_ENV`, `SERVICE_NAME` | all | Listening port, environment label, log label | per service |

## CI/CD

`.github/workflows/ci.yml` follows the stages from Lecture 6, fastest first:
1. lint, unit and contract tests per service (in parallel), plus a dependency audit;
2. secret scan (gitleaks);
3. build and package each image **once**, tagged with the commit SHA;
4. integration / end-to-end: `docker compose up --wait` and `scripts/smoke-test.mjs`;
5. (main only) Trivy image scan, then push the **same** images to GHCR;
6. (main only) staging, then production behind a manual approval (placeholders: no cluster in the prototype).

Actions are pinned to commit SHAs. For the production approval gate, add "Required reviewers"
to the `production` environment in the repository settings.

## Known limitations (deliberate for a prototype)

- Payment is a mock adapter inside `order-service`, not a separate Payment service; prices are taken from the request instead of the Catalogue service.
- Saga state is not resumed after a crash mid-saga (reservation TTL is the safety net); production would persist the saga step and run a recovery job.
- Committing the stock hold after confirmation is a best-effort HTTP call; production would have Inventory consume `OrderPlaced`.
- No authentication: in production the API gateway validates customer and partner tokens before traffic reaches the services.
- No metrics or tracing export yet (logs only); production adds OpenTelemetry.

## Repository layout

```
contracts/               OrderPlaced v1 JSON Schema + example (shared contract)
order-service/           Express API, saga, outbox relay, tests
inventory-service/       Express API, reservations, tests
notification-service/    AMQP consumer, inbox de-duplication, tests
scripts/smoke-test.mjs   End-to-end test used by CI
docker-compose.yml       Whole system with one command
.github/workflows/ci.yml Pipeline
```

Author: Kurt – Undergraduate Diploma in Software Design, Learn Key Institute Malta.
