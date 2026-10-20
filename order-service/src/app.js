const express = require('express');
const { randomUUID } = require('node:crypto');
const { context } = require('./logger');
const { placeOrder } = require('./place-order-saga');

// Error body used by every endpoint: a fixed code apps can check, a readable message,
// and the correlation ID as requestId so support can find the request in the logs.
function sendError(res, status, code, message, details) {
  return res.status(status).json({ status, code, message, details, requestId: res.get('x-correlation-id') });
}

const STATUS_TO_HTTP = { CONFIRMED: 201, CANCELLED: 409, PAYMENT_FAILED: 402, PENDING: 202 };

function validate(body) {
  const errors = [];
  if (typeof body?.customerId !== 'string' || !body.customerId) errors.push('customerId is required');
  if (typeof body?.paymentToken !== 'string' || !body.paymentToken) errors.push('paymentToken is required');
  if (!Array.isArray(body?.items) || body.items.length < 1 || body.items.length > 50) errors.push('items must contain 1-50 lines');
  for (const [i, item] of (Array.isArray(body?.items) ? body.items : []).entries()) {
    if (typeof item.sku !== 'string' || !item.sku) errors.push(`items[${i}].sku is required`);
    if (!Number.isInteger(item.qty) || item.qty < 1 || item.qty > 100) errors.push(`items[${i}].qty must be 1-100`);
    if (!Number.isInteger(item.unitPriceCents) || item.unitPriceCents < 0) errors.push(`items[${i}].unitPriceCents must be a non-negative integer`);
  }
  return errors;
}

const present = (order) => ({
  orderId: order.id,
  status: order.status,
  failureReason: order.failureReason || undefined,
  totalCents: order.totalCents,
  currency: order.currency,
  paymentRef: order.paymentRef || undefined,
  items: order.items,
});

function createApp({ repo, inventory, payments, logger, readiness }) {
  const app = express();
  app.use(express.json({ limit: '100kb' }));

  // Correlation ID: reuse the caller's (set by the API gateway in production) or create one.
  app.use((req, res, next) => {
    const correlationId = req.get('x-correlation-id') || randomUUID();
    res.set('x-correlation-id', correlationId);
    context.run({ correlationId }, next);
  });

  // Liveness: is the process alive? (never checks dependencies, to avoid restart storms)
  app.get('/health/live', (req, res) => res.json({ status: 'ok' }));
  // Readiness: can this replica serve traffic right now? (DB + broker reachable)
  app.get('/health/ready', async (req, res) => {
    const checks = await readiness();
    const ok = Object.values(checks).every(Boolean);
    res.status(ok ? 200 : 503).json({ status: ok ? 'ready' : 'not_ready', checks });
  });

  app.post('/orders', async (req, res) => {
    const idempotencyKey = req.get('idempotency-key');
    if (!idempotencyKey) return sendError(res, 400, 'IDEMPOTENCY_KEY_REQUIRED', 'The Idempotency-Key header is required.');
    const errors = validate(req.body);
    if (errors.length) return sendError(res, 400, 'VALIDATION_FAILED', 'The order request is not valid.', errors);

    logger.info({ event: 'http.order_requested', customer_id: req.body.customerId, lines: req.body.items.length }, 'POST /orders received');
    const { order, replay } = await placeOrder(
      { repo, inventory, payments, logger },
      { ...req.body, idempotencyKey, correlationId: res.get('x-correlation-id') }
    );
    const status = replay ? 200 : STATUS_TO_HTTP[order.status] || 200;
    res.status(order.failureReason === 'INVENTORY_UNAVAILABLE' ? 503 : status).json(present(order));
  });

  app.get('/orders/:id', async (req, res) => {
    if (!/^[0-9a-f-]{36}$/i.test(req.params.id)) return sendError(res, 404, 'ORDER_NOT_FOUND', 'There is no order with that ID.');
    const order = await repo.findById(req.params.id);
    if (!order) return sendError(res, 404, 'ORDER_NOT_FOUND', 'There is no order with that ID.');
    res.json(present(order));
  });

  // Express 5 forwards rejected promises here.
  app.use((err, req, res, next) => {
    logger.error({ event: 'http.unhandled_error', err: err.message, stack: err.stack }, 'Unhandled error');
    sendError(res, 500, 'INTERNAL_ERROR', 'Something went wrong. Quote the requestId when contacting support.');
  });

  return app;
}

module.exports = { createApp };
