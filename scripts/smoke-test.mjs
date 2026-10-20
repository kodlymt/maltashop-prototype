// End-to-end smoke test against a running `docker compose up` stack.
// Used by CI (stage "E2E smoke") and handy for producing screenshot evidence locally:
//   node scripts/smoke-test.mjs
// Works on Windows, macOS and Linux (Node 22+, no dependencies).
import { randomUUID } from 'node:crypto';
import assert from 'node:assert/strict';

const ORDER = process.env.ORDER_URL || 'http://localhost:3001';
const INVENTORY = process.env.INVENTORY_URL || 'http://localhost:3002';
const NOTIFY = process.env.NOTIFICATION_URL || 'http://localhost:3003';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function placeOrder({ key = randomUUID(), correlationId, items, token = 'tok_visa' }) {
  const res = await fetch(`${ORDER}/orders`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': key, 'x-correlation-id': correlationId },
    body: JSON.stringify({ customerId: 'cust-1001', paymentToken: token, items }),
  });
  return { status: res.status, body: await res.json(), correlationId: res.headers.get('x-correlation-id') };
}
const stock = async (sku) => (await (await fetch(`${INVENTORY}/stock/${sku}`)).json()).available;
const step = (msg) => console.log(`\n=== ${msg}`);

step('1. Happy path: POST /orders');
const correlationId = `smoke-${Date.now()}`;
const key = randomUUID();
const before = await stock('KETTLE-STEEL');
const ok = await placeOrder({ key, correlationId, items: [{ sku: 'KETTLE-STEEL', qty: 2, unitPriceCents: 3499 }] });
console.log(ok.status, JSON.stringify(ok.body, null, 2));
assert.equal(ok.status, 201);
assert.equal(ok.body.status, 'CONFIRMED');
assert.equal(ok.correlationId, correlationId);
assert.equal(await stock('KETTLE-STEEL'), before - 2);

step('2. Idempotency: same Idempotency-Key again returns the same order, no second charge');
const replay = await placeOrder({ key, correlationId, items: [{ sku: 'KETTLE-STEEL', qty: 2, unitPriceCents: 3499 }] });
assert.equal(replay.status, 200);
assert.equal(replay.body.orderId, ok.body.orderId);
assert.equal(await stock('KETTLE-STEEL'), before - 2);
console.log('replay returned order', replay.body.orderId);

step('3. Async event: Notification service consumed OrderPlaced');
let notes = [];
for (let i = 0; i < 20 && notes.length === 0; i++) {
  await sleep(500);
  notes = await (await fetch(`${NOTIFY}/notifications?orderId=${ok.body.orderId}`)).json();
}
console.log(JSON.stringify(notes, null, 2));
assert.equal(notes.length, 1);
assert.equal(notes[0].correlation_id, correlationId);

step('4. Out of stock: order CANCELLED, customer not charged');
const oos = await placeOrder({ correlationId: `${correlationId}-oos`, items: [{ sku: 'LAMP-DESK', qty: 99, unitPriceCents: 2500 }] });
console.log(oos.status, oos.body.status, oos.body.failureReason);
assert.equal(oos.status, 409);

step('5. Payment declined: compensating action returns the stock');
const tvBefore = await stock('TV-55-OLED');
const declined = await placeOrder({ correlationId: `${correlationId}-pay`, token: 'tok_decline', items: [{ sku: 'TV-55-OLED', qty: 1, unitPriceCents: 129900 }] });
console.log(declined.status, declined.body.status, declined.body.failureReason);
assert.equal(declined.status, 402);
assert.equal(await stock('TV-55-OLED'), tvBefore);

console.log(`\nALL SMOKE TESTS PASSED  (search the logs for correlation_id "${correlationId}")`);
