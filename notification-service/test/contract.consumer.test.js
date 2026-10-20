// CONSUMER side of the Order -> Notification contract, plus the idempotent-consumer rule.
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const Ajv = require('ajv');
const addFormats = require('ajv-formats');
const { parseOrderPlaced, handleOrderPlaced, InvalidMessageError } = require('../src/handler');

const contractDir = path.join(__dirname, '../../contracts');
const schema = require(path.join(contractDir, 'order-placed.v1.schema.json'));
const exampleRaw = fs.readFileSync(path.join(contractDir, 'examples/order-placed.v1.example.json'), 'utf8');

test('the published contract example is valid against the shared schema', () => {
  const ajv = new Ajv({ allErrors: true });
  addFormats(ajv);
  const validate = ajv.compile(schema);
  assert.ok(validate(JSON.parse(exampleRaw)), JSON.stringify(validate.errors));
});

test('notification-service can parse every event that satisfies the contract', () => {
  const event = parseOrderPlaced(exampleRaw);
  assert.equal(event.data.order_id, '0b6c3a7e-2f4d-4c55-8a1b-9d2e6f7a8b90');
});

test('every field the consumer requires is marked required in the contract', () => {
  for (const f of ['event_id', 'event_type', 'event_version', 'data']) assert.ok(schema.required.includes(f), f);
  for (const f of ['order_id', 'customer_id', 'total_cents']) assert.ok(schema.properties.data.required.includes(f), f);
});

test('an event that breaks the contract is rejected (goes to the DLQ, not retried)', () => {
  const broken = JSON.parse(exampleRaw);
  delete broken.data.order_id;
  assert.throws(() => parseOrderPlaced(JSON.stringify(broken)), InvalidMessageError);
});

test('a redelivered event is processed once: one email, one notification row', async () => {
  const processed = new Set();
  const notifications = [];
  const client = {
    async query(sql, params) {
      if (sql.startsWith('INSERT INTO processed_messages')) {
        if (processed.has(params[0])) return { rowCount: 0 };
        processed.add(params[0]);
        return { rowCount: 1 };
      }
      if (sql.includes('INSERT INTO notifications')) notifications.push(params);
      return { rowCount: 1 };
    },
    release() {},
  };
  const pool = { connect: async () => client };
  let emails = 0;
  const sender = { async sendOrderConfirmation() { emails++; } };
  const logger = { info() {} };
  const event = parseOrderPlaced(exampleRaw);

  assert.equal(await handleOrderPlaced({ pool, sender, logger }, event), 'sent');
  assert.equal(await handleOrderPlaced({ pool, sender, logger }, event), 'duplicate');
  assert.equal(emails, 1);
  assert.equal(notifications.length, 1);
});
