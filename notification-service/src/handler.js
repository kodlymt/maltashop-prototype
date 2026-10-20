// Idempotent consumer ("inbox" pattern).
// RabbitMQ delivers at-least-once, and the outbox relay may republish after a crash,
// so the same OrderPlaced can arrive twice. The event_id is inserted into
// processed_messages in the same transaction as the notification record;
// a duplicate hits the primary key and is acknowledged without a second email.
const SCHEMA = `
CREATE TABLE IF NOT EXISTS processed_messages (
  event_id      uuid PRIMARY KEY,
  processed_at  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS notifications (
  id              bigserial PRIMARY KEY,
  event_id        uuid NOT NULL UNIQUE REFERENCES processed_messages(event_id),
  order_id        uuid NOT NULL,
  customer_id     text NOT NULL,
  channel         text NOT NULL,
  template        text NOT NULL,
  status          text NOT NULL,
  correlation_id  text,
  created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS notifications_order ON notifications (order_id);
`;

class InvalidMessageError extends Error {}

// Consumer-side expectations of OrderPlaced v1 - exactly the fields this service reads.
// (Tolerant reader: extra fields are ignored, missing required ones are rejected.)
function parseOrderPlaced(raw) {
  let event;
  try { event = JSON.parse(raw); } catch { throw new InvalidMessageError('body is not JSON'); }
  const d = event?.data;
  if (event?.event_type !== 'OrderPlaced' || event.event_version !== 1) throw new InvalidMessageError('unsupported event type/version');
  if (typeof event.event_id !== 'string' || typeof d?.order_id !== 'string' || typeof d?.customer_id !== 'string' || !Number.isInteger(d?.total_cents)) {
    throw new InvalidMessageError('missing required OrderPlaced fields');
  }
  return event;
}

async function migrate(pool) {
  await pool.query(SCHEMA);
}

async function handleOrderPlaced({ pool, sender, logger }, event) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const fresh = await client.query(
      'INSERT INTO processed_messages (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING',
      [event.event_id]
    );
    if (fresh.rowCount === 0) {
      await client.query('COMMIT');
      logger.info({ event: 'event.duplicate_ignored', event_id: event.event_id, order_id: event.data.order_id }, 'Duplicate OrderPlaced ignored (already processed)');
      return 'duplicate';
    }
    await sender.sendOrderConfirmation(event);
    await client.query(
      `INSERT INTO notifications (event_id, order_id, customer_id, channel, template, status, correlation_id)
       VALUES ($1, $2, $3, 'email', 'order-confirmation', 'SENT', $4)`,
      [event.event_id, event.data.order_id, event.data.customer_id, event.correlation_id]
    );
    await client.query('COMMIT');
    return 'sent';
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

module.exports = { migrate, parseOrderPlaced, handleOrderPlaced, InvalidMessageError };
