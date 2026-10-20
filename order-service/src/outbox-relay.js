// Outbox relay: publishes unpublished outbox rows to RabbitMQ.
// - Publisher confirms: a row is marked published only after the broker has
//   acknowledged that it stored the message.
// - Persistent messages on a durable exchange/queue survive a broker restart.
// - FOR UPDATE SKIP LOCKED lets several replicas run the relay without double work.
// Delivery is therefore AT-LEAST-ONCE: if the process dies after the confirm but
// before the UPDATE, the event is sent again and the consumer's inbox de-duplicates it.
const { context } = require('./logger');

function createOutboxRelay({ pool, channel, exchange, logger, batchSize = 50 }) {
  async function relayOnce() {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT event_id, event_type, routing_key, payload, correlation_id
         FROM outbox WHERE published_at IS NULL
         ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`,
        [batchSize]
      );
      if (rows.length === 0) {
        await client.query('COMMIT');
        return 0;
      }
      for (const row of rows) {
        channel.publish(exchange, row.routing_key, Buffer.from(JSON.stringify(row.payload)), {
          persistent: true,
          contentType: 'application/json',
          messageId: row.event_id,
          type: row.event_type,
          correlationId: row.correlation_id,
          timestamp: Math.floor(Date.now() / 1000),
          headers: { 'x-correlation-id': row.correlation_id, 'x-event-version': 1 },
        });
      }
      await channel.waitForConfirms(); // throws if the broker NACKs
      await client.query('UPDATE outbox SET published_at = now() WHERE event_id = ANY($1::uuid[])', [
        rows.map((r) => r.event_id),
      ]);
      await client.query('COMMIT');
      for (const row of rows) {
        context.run({ correlationId: row.correlation_id }, () =>
          logger.info(
            { event: 'event.published', event_type: row.event_type, event_id: row.event_id, order_id: row.payload.data.order_id, routing_key: row.routing_key },
            `${row.event_type} published to ${exchange}`
          )
        );
      }
      return rows.length;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      logger.error({ event: 'outbox.publish_failed', err: err.message }, 'Outbox publish failed; will retry');
      return 0;
    } finally {
      client.release();
    }
  }

  let timer;
  let running = false;
  return {
    relayOnce,
    start(intervalMs) {
      timer = setInterval(async () => {
        if (running) return;
        running = true;
        try { await relayOnce(); } finally { running = false; }
      }, intervalMs);
    },
    stop() { clearInterval(timer); },
  };
}

module.exports = { createOutboxRelay };
