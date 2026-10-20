const amqp = require('amqplib');
const config = require('./config');
const { logger } = require('./logger');
const { createPool, migrate } = require('./db');
const { createOrderRepository } = require('./order-repository');
const { createInventoryClient } = require('./inventory-client');
const { createMockPaymentGateway } = require('./payment-gateway');
const { createOutboxRelay } = require('./outbox-relay');
const { createApp } = require('./app');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function withRetry(name, fn, attempts = 30) {
  for (let i = 1; ; i++) {
    try { return await fn(); } catch (err) {
      if (i >= attempts) throw err;
      logger.warn({ event: 'startup.retry', dependency: name, attempt: i, err: err.message }, `Waiting for ${name}`);
      await sleep(Math.min(1000 * i, 5000));
    }
  }
}

async function main() {
  const pool = createPool(config.databaseUrl);
  await withRetry('postgres', () => migrate(pool));

  const connection = await withRetry('rabbitmq', () => amqp.connect(config.rabbitmqUrl));
  const channel = await connection.createConfirmChannel();
  await channel.assertExchange(config.eventsExchange, 'topic', { durable: true });
  let brokerUp = true;
  // Disposability (12-factor IX): if the broker connection is lost, exit and let the
  // orchestrator restart us cleanly instead of running in a half-broken state.
  connection.on('close', () => { brokerUp = false; logger.error({ event: 'amqp.closed' }, 'Broker connection lost - exiting'); process.exit(1); });

  const repo = createOrderRepository(pool);
  const inventory = createInventoryClient({ baseUrl: config.inventoryUrl, timeoutMs: config.httpTimeoutMs, maxAttempts: config.httpMaxAttempts });
  const payments = createMockPaymentGateway();
  const relay = createOutboxRelay({ pool, channel, exchange: config.eventsExchange, logger });
  relay.start(config.outboxPollMs);

  const readiness = async () => ({
    database: await pool.query('SELECT 1').then(() => true, () => false),
    broker: brokerUp,
  });

  const server = createApp({ repo, inventory, payments, logger, readiness }).listen(config.port, () =>
    logger.info({ event: 'startup.ready', port: config.port }, 'order-service listening')
  );

  const shutdown = async (signal) => {
    logger.info({ event: 'shutdown', signal }, 'Graceful shutdown');
    server.close();
    relay.stop();
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
  logger.fatal({ event: 'startup.failed', err: err.message }, 'order-service failed to start');
  process.exit(1);
});
