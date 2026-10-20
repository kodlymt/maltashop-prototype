// The Order service owns its own PostgreSQL database (database-per-service).
// No other service has credentials for it.
const { Pool } = require('pg');

const SCHEMA = `
CREATE TABLE IF NOT EXISTS orders (
  id               uuid PRIMARY KEY,
  idempotency_key  text NOT NULL UNIQUE,         -- duplicate POSTs return the same order
  customer_id      text NOT NULL,
  status           text NOT NULL CHECK (status IN ('PENDING','CONFIRMED','CANCELLED','PAYMENT_FAILED')),
  failure_reason   text,
  total_cents      integer NOT NULL CHECK (total_cents >= 0),
  currency         char(3) NOT NULL DEFAULT 'EUR',
  items            jsonb NOT NULL,
  payment_ref      text,
  correlation_id   text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Transactional outbox: the event row is written in the SAME transaction as the
-- order status change, so an order can never be confirmed without its event.
CREATE TABLE IF NOT EXISTS outbox (
  event_id        uuid PRIMARY KEY,
  aggregate_id    uuid NOT NULL,
  event_type      text NOT NULL,
  routing_key     text NOT NULL,
  payload         jsonb NOT NULL,
  correlation_id  text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  published_at    timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
`;

function createPool(connectionString) {
  return new Pool({ connectionString, max: 10 });
}

async function migrate(pool) {
  await pool.query(SCHEMA);
}

module.exports = { createPool, migrate };
