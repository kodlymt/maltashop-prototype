const test = require('node:test');
const assert = require('node:assert/strict');
const { normaliseItems } = require('../src/reservations');

test('duplicate SKUs are merged and lines are sorted to give a stable lock order', () => {
  const items = normaliseItems([
    { sku: 'TV-55-OLED', qty: 1 },
    { sku: 'KETTLE-STEEL', qty: 2 },
    { sku: 'TV-55-OLED', qty: 3 },
  ]);
  assert.deepEqual(items, [
    { sku: 'KETTLE-STEEL', qty: 2 },
    { sku: 'TV-55-OLED', qty: 4 },
  ]);
});
