// Data access for orders and the outbox. Kept behind a small interface so the
// saga can be unit-tested with an in-memory fake.
const { randomUUID } = require('node:crypto');

const toOrder = (row) =>
  row && {
    id: row.id,
    customerId: row.customer_id,
    status: row.status,
    failureReason: row.failure_reason,
    totalCents: row.total_cents,
    currency: row.currency.trim(),
    items: row.items,
    paymentRef: row.payment_ref,
    correlationId: row.correlation_id,
    createdAt: row.created_at,
  };

function createOrderRepository(pool) {
  return {
    async findById(id) {
      const { rows } = await pool.query('SELECT * FROM orders WHERE id = $1', [id]);
      return toOrder(rows[0]);
    },

    async findByIdempotencyKey(key) {
      const { rows } = await pool.query('SELECT * FROM orders WHERE idempotency_key = $1', [key]);
      return toOrder(rows[0]);
    },

    // Insert a PENDING order. ON CONFLICT makes a retried request a no-op:
    // the caller gets { created: false } and the original order back.
    async insertPending({ idempotencyKey, customerId, items, totalCents, correlationId }) {
      const { rows } = await pool.query(
        `INSERT INTO orders (id, idempotency_key, customer_id, status, total_cents, items, correlation_id)
         VALUES ($1, $2, $3, 'PENDING', $4, $5, $6)
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING *`,
        [randomUUID(), idempotencyKey, customerId, totalCents, JSON.stringify(items), correlationId]
      );
      if (rows.length) return { created: true, order: toOrder(rows[0]) };
      return { created: false, order: await this.findByIdempotencyKey(idempotencyKey) };
    },

    async markFailed(id, status, failureReason) {
      const { rows } = await pool.query(
        `UPDATE orders SET status = $2, failure_reason = $3, updated_at = now()
         WHERE id = $1 AND status = 'PENDING' RETURNING *`,
        [id, status, failureReason]
      );
      return toOrder(rows[0]) || this.findById(id);
    },

    // Confirm the order and record the OrderPlaced event atomically (outbox pattern).
    async confirmWithEvent(id, paymentRef, event, routingKey) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const { rows } = await client.query(
          `UPDATE orders SET status = 'CONFIRMED', payment_ref = $2, updated_at = now()
           WHERE id = $1 AND status = 'PENDING' RETURNING *`,
          [id, paymentRef]
        );
        if (rows.length) {
          await client.query(
            `INSERT INTO outbox (event_id, aggregate_id, event_type, routing_key, payload, correlation_id)
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [event.event_id, id, event.event_type, routingKey, JSON.stringify(event), event.correlation_id]
          );
        }
        await client.query('COMMIT');
        return toOrder(rows[0]) || this.findById(id);
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      } finally {
        client.release();
      }
    },
  };
}

module.exports = { createOrderRepository };
