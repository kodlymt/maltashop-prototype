// Unit tests for the saga: no database, broker or network - fast enough to run on every push.
const test = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { placeOrder } = require('../src/place-order-saga');

const silentLogger = { info() {}, warn() {}, error() {} };

function fakeRepo() {
  const orders = new Map();
  const outbox = [];
  return {
    orders, outbox,
    async insertPending(o) {
      const existing = [...orders.values()].find((x) => x.idempotencyKey === o.idempotencyKey);
      if (existing) return { created: false, order: existing };
      const order = { ...o, id: randomUUID(), status: 'PENDING' };
      orders.set(order.id, order);
      return { created: true, order };
    },
    async markFailed(id, status, failureReason) {
      return Object.assign(orders.get(id), { status, failureReason });
    },
    async confirmWithEvent(id, paymentRef, event, routingKey) {
      outbox.push({ event, routingKey });
      return Object.assign(orders.get(id), { status: 'CONFIRMED', paymentRef });
    },
  };
}

function fakeInventory({ outOfStock = false, down = false } = {}) {
  const calls = [];
  return {
    calls,
    async reserve(id) {
      calls.push(['reserve', id]);
      if (down) throw new Error('timeout');
      return outOfStock ? { status: 409, body: { error: 'OUT_OF_STOCK', sku: 'LAMP-DESK' } } : { status: 201, body: { status: 'HELD' } };
    },
    async release(id) { calls.push(['release', id]); return { status: 200, body: { status: 'RELEASED' } }; },
    async commit(id) { calls.push(['commit', id]); return { status: 200, body: { status: 'COMMITTED' } }; },
  };
}

function fakePayments() {
  const calls = [];
  return {
    calls,
    async charge(req) {
      calls.push(req);
      return req.token === 'tok_decline' ? { status: 'declined', reason: 'card_declined' } : { status: 'approved', reference: 'psp_ch_1' };
    },
  };
}

const cmd = (overrides = {}) => ({
  idempotencyKey: 'key-1',
  customerId: 'cust-1001',
  paymentToken: 'tok_visa',
  correlationId: 'corr-1',
  items: [{ sku: 'TV-55-OLED', qty: 2, unitPriceCents: 50000 }],
  ...overrides,
});

test('happy path: order is CONFIRMED and exactly one OrderPlaced event is stored in the outbox', async () => {
  const deps = { repo: fakeRepo(), inventory: fakeInventory(), payments: fakePayments(), logger: silentLogger };
  const { order } = await placeOrder(deps, cmd());
  assert.equal(order.status, 'CONFIRMED');
  assert.equal(deps.repo.outbox.length, 1);
  assert.equal(deps.repo.outbox[0].routingKey, 'order.placed');
  assert.equal(deps.repo.outbox[0].event.data.total_cents, 100000);
  assert.equal(deps.payments.calls[0].idempotencyKey, order.id, 'order id is reused as payment idempotency key');
});

test('idempotency: retrying with the same Idempotency-Key does not reserve or charge twice', async () => {
  const deps = { repo: fakeRepo(), inventory: fakeInventory(), payments: fakePayments(), logger: silentLogger };
  const first = await placeOrder(deps, cmd());
  const second = await placeOrder(deps, cmd());
  assert.equal(second.replay, true);
  assert.equal(second.order.id, first.order.id);
  assert.equal(deps.inventory.calls.filter(([c]) => c === 'reserve').length, 1);
  assert.equal(deps.payments.calls.length, 1);
});

test('out of stock: order is CANCELLED and the customer is never charged', async () => {
  const deps = { repo: fakeRepo(), inventory: fakeInventory({ outOfStock: true }), payments: fakePayments(), logger: silentLogger };
  const { order } = await placeOrder(deps, cmd());
  assert.equal(order.status, 'CANCELLED');
  assert.equal(order.failureReason, 'OUT_OF_STOCK');
  assert.equal(deps.payments.calls.length, 0);
  assert.equal(deps.repo.outbox.length, 0);
});

test('payment declined: compensating action releases the reservation', async () => {
  const deps = { repo: fakeRepo(), inventory: fakeInventory(), payments: fakePayments(), logger: silentLogger };
  const { order } = await placeOrder(deps, cmd({ paymentToken: 'tok_decline' }));
  assert.equal(order.status, 'PAYMENT_FAILED');
  assert.deepEqual(deps.inventory.calls.map(([c]) => c), ['reserve', 'release']);
  assert.equal(deps.repo.outbox.length, 0, 'no OrderPlaced event for a failed order');
});

test('inventory unreachable: order is CANCELLED and a defensive release is sent', async () => {
  const deps = { repo: fakeRepo(), inventory: fakeInventory({ down: true }), payments: fakePayments(), logger: silentLogger };
  const { order } = await placeOrder(deps, cmd());
  assert.equal(order.status, 'CANCELLED');
  assert.equal(order.failureReason, 'INVENTORY_UNAVAILABLE');
  assert.deepEqual(deps.inventory.calls.map(([c]) => c), ['reserve', 'release']);
  assert.equal(deps.payments.calls.length, 0);
});
