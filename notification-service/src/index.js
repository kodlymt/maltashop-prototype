const amqp = require('amqplib');
const express = require('express');
const { Pool } = require('pg');
const { logger, context } = require('./logger');
const { migrate, parseOrderPlaced, handleOrderPlaced, InvalidMessageError } = require('./handler');

const required = (n) => { if (!process.env[n]) throw new Error(`Missing required environment variable ${n}`); return process.env[n]; };
const config = {
  port: Number(process.env.PORT || 3003),
  databaseUrl: required('DATABASE_URL'),
  rabbitmqUrl: required('RABBITMQ_URL'),
  exchange: process.env.EVENTS_EXCHANGE || 'maltashop.events',
  queue: process.env.QUEUE_NAME || 'notification.order-placed',
  prefetch: Number(process.env.PREFETCH || 10),
  // Fault-injection switch to replay the March incident: each message leaks this many KB.
  // With the container memory limit in docker-compose.yml the service is OOM-killed and
  // restarted, while order-service keeps taking orders and events wait in the queue.
  leakKbPerMessage: Number(process.env.SIMULATED_LEAK_KB_PER_MESSAGE || 0),
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const leaked = [];

async function withRetry(name, fn, attempts = 30) {
  for (let i = 1; ; i++) {
    try { return await fn(); } catch (err) {
      if (i >= attempts) throw err;
      logger.warn({ event: 'startup.retry', dependency: name, attempt: i, err: err.message }, `Waiting for ${name}`);
      await sleep(Math.min(1000 * i, 5000));
    }
  }
}

// Simulated email provider. In production this would call an email API (e.g. via SMTP relay),
// looking up the customer's address from the Customer service - the event itself carries no PII.
const sender = {
  async sendOrderConfirmation(event) {
    logger.info(
      { event: 'email.sent', order_id: event.data.order_id, customer_id: event.data.customer_id, template: 'order-confirmation', simulated: true },
      `Order confirmation email sent (simulated) for order ${event.data.order_id}`
    );
  },
};

async function main() {
  const pool = new Pool({ connectionString: config.databaseUrl, max: 5 });
  await withRetry('postgres', () => migrate(pool));

  const connection = await withRetry('rabbitmq', () => amqp.connect(config.rabbitmqUrl));
  const channel = await connection.createChannel();
  let brokerUp = true;
  connection.on('close', () => { brokerUp = false; logger.error({ event: 'amqp.closed' }, 'Broker connection lost - exiting'); process.exit(1); });

  // Topology owned by the consumer: durable queue bound to the shared topic exchange,
  // with a dead-letter exchange so poison messages are parked, not lost or looped forever.
  const dlx = `${config.exchange}.dlx`;
  const dlq = `${config.queue}.dlq`;
  await channel.assertExchange(config.exchange, 'topic', { durable: true });
  await channel.assertExchange(dlx, 'fanout', { durable: true });
  await channel.assertQueue(dlq, { durable: true });
  await channel.bindQueue(dlq, dlx, '');
  await channel.assertQueue(config.queue, { durable: true, deadLetterExchange: dlx });
  await channel.bindQueue(config.queue, config.exchange, 'order.placed');
  await channel.prefetch(config.prefetch); // back-pressure: at most N unacked messages in flight

  await channel.consume(config.queue, (msg) => {
    if (!msg) return;
    const correlationId = msg.properties.correlationId || msg.properties.headers?.['x-correlation-id'];
    context.run({ correlationId }, async () => {
      try {
        const event = parseOrderPlaced(msg.content.toString());
        logger.info({ event: 'event.received', event_type: event.event_type, event_id: event.event_id, order_id: event.data.order_id, redelivered: msg.fields.redelivered }, 'OrderPlaced consumed from queue');
        await handleOrderPlaced({ pool, sender, logger }, event);
        if (config.leakKbPerMessage > 0) leaked.push(Buffer.alloc(config.leakKbPerMessage * 1024, 1));
        channel.ack(msg); // manual ack only AFTER the DB transaction committed
      } catch (err) {
        if (err instanceof InvalidMessageError) {
          logger.error({ event: 'event.rejected', err: err.message }, 'Invalid message sent to DLQ');
          channel.nack(msg, false, false); // -> dead-letter queue
        } else if (!msg.fields.redelivered) {
          logger.warn({ event: 'event.retry', err: err.message }, 'Processing failed - requeueing once');
          channel.nack(msg, false, true);
        } else {
          logger.error({ event: 'event.dead_lettered', err: err.message }, 'Processing failed twice - sent to DLQ');
          channel.nack(msg, false, false);
        }
      }
    });
  }, { noAck: false });

  const app = express();
  app.get('/health/live', (req, res) => res.json({ status: 'ok' }));
  app.get('/health/ready', async (req, res) => {
    const db = await pool.query('SELECT 1').then(() => true, () => false);
    const ok = db && brokerUp;
    res.status(ok ? 200 : 503).json({ status: ok ? 'ready' : 'not_ready', checks: { database: db, broker: brokerUp } });
  });
  // Read-only endpoint used by the smoke test and for demo evidence.
  app.get('/notifications', async (req, res) => {
    const { rows } = await pool.query(
      'SELECT order_id, channel, template, status, correlation_id, created_at FROM notifications WHERE order_id::text = $1 ORDER BY id',
      [String(req.query.orderId || '')]
    );
    res.json(rows);
  });

  const server = app.listen(config.port, () => logger.info({ event: 'startup.ready', port: config.port, queue: config.queue }, 'notification-service consuming'));
  const shutdown = async (signal) => {
    logger.info({ event: 'shutdown', signal }, 'Graceful shutdown');
    server.close();
    connection.removeAllListeners('close');
    await channel.close().catch(() => {});
    await connection.close().catch(() => {});
    await pool.end();
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((err) => {
  logger.fatal({ event: 'startup.failed', err: err.message }, 'notification-service failed to start');
  process.exit(1);
});
