const express = require('express');
const { Pool } = require('pg');
const { randomUUID } = require('node:crypto');
const { logger, context } = require('./logger');
const { migrateAndSeed, createReservationService } = require('./reservations');

const required = (n) => { if (!process.env[n]) throw new Error(`Missing required environment variable ${n}`); return process.env[n]; };
const config = {
  port: Number(process.env.PORT || 3002),
  databaseUrl: required('DATABASE_URL'),
  ttlSeconds: Number(process.env.RESERVATION_TTL_SECONDS || 900),
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function main() {
  const pool = new Pool({ connectionString: config.databaseUrl, max: 10 });
  for (let i = 1; ; i++) {
    try { await migrateAndSeed(pool); break; } catch (err) {
      if (i >= 30) throw err;
      logger.warn({ event: 'startup.retry', dependency: 'postgres', attempt: i, err: err.message }, 'Waiting for postgres');
      await sleep(Math.min(1000 * i, 5000));
    }
  }
  const reservations = createReservationService(pool, { ttlSeconds: config.ttlSeconds });

  const app = express();
  app.use(express.json({ limit: '50kb' }));
  app.use((req, res, next) => {
    const correlationId = req.get('x-correlation-id') || randomUUID();
    res.set('x-correlation-id', correlationId);
    context.run({ correlationId }, next);
  });

  app.get('/health/live', (req, res) => res.json({ status: 'ok' }));
  app.get('/health/ready', async (req, res) => {
    const db = await pool.query('SELECT 1').then(() => true, () => false);
    res.status(db ? 200 : 503).json({ status: db ? 'ready' : 'not_ready', checks: { database: db } });
  });

  app.post('/reservations', async (req, res) => {
    const { reservationId, items } = req.body || {};
    if (!UUID.test(reservationId || '') || !Array.isArray(items) || items.length === 0 ||
        items.some((i) => typeof i.sku !== 'string' || !Number.isInteger(i.qty) || i.qty < 1)) {
      return res.status(400).json({ error: 'validation_failed' });
    }
    const result = await reservations.reserve(reservationId, items);
    logger.info({ event: 'reservation.reserve', order_id: reservationId, outcome: result.body.status || result.body.error, replay: !!result.body.replay }, 'Reserve request handled');
    res.status(result.code).json(result.body);
  });

  app.post('/reservations/:id/release', async (req, res) => {
    if (!UUID.test(req.params.id)) return res.status(400).json({ error: 'validation_failed' });
    const result = await reservations.release(req.params.id);
    logger.info({ event: 'reservation.release', order_id: req.params.id, outcome: result.body.status || result.body.error }, 'Release (compensation) handled');
    res.status(result.code).json(result.body);
  });

  app.post('/reservations/:id/commit', async (req, res) => {
    if (!UUID.test(req.params.id)) return res.status(400).json({ error: 'validation_failed' });
    const result = await reservations.commit(req.params.id);
    logger.info({ event: 'reservation.commit', order_id: req.params.id, outcome: result.body.status || result.body.error }, 'Commit handled');
    res.status(result.code).json(result.body);
  });

  app.get('/stock/:sku', async (req, res) => {
    const row = await reservations.getStock(req.params.sku);
    if (!row) return res.status(404).json({ error: 'not_found' });
    res.json(row);
  });

  app.use((err, req, res, next) => {
    logger.error({ event: 'http.unhandled_error', err: err.message }, 'Unhandled error');
    res.status(500).json({ error: 'internal_error' });
  });

  const sweeper = setInterval(() => {
    reservations.releaseExpired()
      .then((n) => n && logger.info({ event: 'reservation.expired_released', count: n }, 'Expired holds released'))
      .catch((err) => logger.error({ event: 'reservation.sweeper_failed', err: err.message }, 'Sweeper failed'));
  }, 30_000);

  const server = app.listen(config.port, () => logger.info({ event: 'startup.ready', port: config.port }, 'inventory-service listening'));
  const shutdown = async (signal) => {
    logger.info({ event: 'shutdown', signal }, 'Graceful shutdown');
    clearInterval(sweeper);
    server.close();
    await pool.end();
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((err) => {
  logger.fatal({ event: 'startup.failed', err: err.message }, 'inventory-service failed to start');
  process.exit(1);
});
