const { randomUUID } = require('node:crypto');

const ORDER_PLACED_ROUTING_KEY = 'order.placed';

// Builds an OrderPlaced v1 event. The shape is fixed by contracts/order-placed.v1.schema.json
// and checked in CI by test/contract.provider.test.js.
function buildOrderPlacedEvent(order, paymentRef, correlationId) {
  return {
    event_id: randomUUID(),
    event_type: 'OrderPlaced',
    event_version: 1,
    occurred_at: new Date().toISOString(),
    correlation_id: correlationId,
    data: {
      order_id: order.id,
      customer_id: order.customerId,
      total_cents: order.totalCents,
      currency: 'EUR',
      payment_ref: paymentRef,
      items: order.items.map((i) => ({ sku: i.sku, qty: i.qty, unit_price_cents: i.unitPriceCents })),
    },
  };
}

module.exports = { buildOrderPlacedEvent, ORDER_PLACED_ROUTING_KEY };
