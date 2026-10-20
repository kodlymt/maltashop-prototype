// PROVIDER side of the Order -> Notification contract.
// The event the Order service actually builds must satisfy the shared OrderPlaced v1
// schema that the Notification service (consumer) depends on. If someone renames or
// removes a field, this test fails in CI before the change can be merged.
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const Ajv = require('ajv');
const addFormats = require('ajv-formats');
const { buildOrderPlacedEvent } = require('../src/events');
const schema = require(path.join(__dirname, '../../contracts/order-placed.v1.schema.json'));

const ajv = new Ajv({ allErrors: true });
addFormats(ajv);
const validate = ajv.compile(schema);

test('OrderPlaced built by order-service satisfies contracts/order-placed.v1.schema.json', () => {
  const order = {
    id: '0b6c3a7e-2f4d-4c55-8a1b-9d2e6f7a8b90',
    customerId: 'cust-1001',
    totalCents: 129900,
    items: [{ sku: 'TV-55-OLED', qty: 1, unitPriceCents: 129900 }],
  };
  const event = buildOrderPlacedEvent(order, 'psp_ch_demo', 'corr-123');
  const ok = validate(event);
  assert.ok(ok, JSON.stringify(validate.errors, null, 2));
});

test('event carries no personal data beyond the opaque customer_id (GDPR minimisation)', () => {
  const event = buildOrderPlacedEvent(
    { id: '0b6c3a7e-2f4d-4c55-8a1b-9d2e6f7a8b90', customerId: 'cust-1', totalCents: 1, items: [{ sku: 'A', qty: 1, unitPriceCents: 1 }] },
    'ref', 'corr'
  );
  const json = JSON.stringify(event);
  for (const field of ['email', 'phone', 'address', 'name', 'card']) assert.ok(!json.includes(`"${field}`), `unexpected field ${field}`);
});
