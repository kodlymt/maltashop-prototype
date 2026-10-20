// Orchestration-based saga for "place an order".
// There is no transaction spanning Order, Inventory and Payment, so each step is a
// local transaction in one service, and every forward step that can fail has a
// compensating action:
//
//   step                     local transaction in   compensation if a later step fails
//   1. create PENDING order  Order DB               mark CANCELLED / PAYMENT_FAILED
//   2. reserve stock         Inventory DB           release reservation
//   3. charge card           Payment provider       (last step - nothing after it can fail
//                                                    except recording the result)
//   4. confirm + outbox      Order DB               -
//
// Idempotency: the client's Idempotency-Key makes step 1 a no-op on retry; the order id
// is reused as the reservation id and the payment idempotency key, so steps 2 and 3
// are also safe to retry.
const { buildOrderPlacedEvent, ORDER_PLACED_ROUTING_KEY } = require('./events');

async function placeOrder({ repo, inventory, payments, logger }, cmd) {
  const totalCents = cmd.items.reduce((sum, i) => sum + i.qty * i.unitPriceCents, 0);

  // Step 1 - local transaction: PENDING order (idempotent on Idempotency-Key)
  const { created, order } = await repo.insertPending({
    idempotencyKey: cmd.idempotencyKey,
    customerId: cmd.customerId,
    items: cmd.items,
    totalCents,
    correlationId: cmd.correlationId,
  });
  if (!created) {
    logger.info({ event: 'order.idempotent_replay', order_id: order.id, status: order.status }, 'Duplicate request - returning original order');
    return { order, replay: true };
  }
  logger.info({ event: 'order.pending', order_id: order.id, total_cents: totalCents }, 'Order created (PENDING)');

  // Step 2 - reserve stock in Inventory (synchronous, with timeout + retries)
  let reservation;
  try {
    reservation = await inventory.reserve(order.id, cmd.items);
  } catch (err) {
    logger.error({ event: 'saga.inventory_unavailable', order_id: order.id, err: err.message }, 'Inventory unreachable');
    // The reserve may have succeeded before the timeout, so compensate defensively.
    // Release is idempotent and leaves a tombstone, so a late reserve becomes a no-op.
    await inventory.release(order.id).catch(() => {});
    return { order: await repo.markFailed(order.id, 'CANCELLED', 'INVENTORY_UNAVAILABLE') };
  }
  if (reservation.status === 409 || !['HELD', 'COMMITTED'].includes(reservation.body.status)) {
    logger.info({ event: 'saga.out_of_stock', order_id: order.id, sku: reservation.body.sku }, 'Stock not available - order cancelled');
    return { order: await repo.markFailed(order.id, 'CANCELLED', 'OUT_OF_STOCK') };
  }
  logger.info({ event: 'saga.stock_reserved', order_id: order.id }, 'Stock reserved');

  // Step 3 - charge through the payment provider (idempotency key = order id)
  let payment;
  try {
    payment = await payments.charge({ idempotencyKey: order.id, amountCents: totalCents, token: cmd.paymentToken });
  } catch (err) {
    // In production an unknown outcome must be checked with the PSP before compensating.
    payment = { status: 'error', reason: err.message };
  }
  if (payment.status !== 'approved') {
    logger.warn({ event: 'saga.payment_failed', order_id: order.id, reason: payment.reason }, 'Payment failed - compensating');
    await compensateReservation(inventory, logger, order.id);
    return { order: await repo.markFailed(order.id, 'PAYMENT_FAILED', payment.reason || 'PAYMENT_ERROR') };
  }

  // Step 4 - confirm the order and write OrderPlaced to the outbox in one local transaction
  const event = buildOrderPlacedEvent(order, payment.reference, cmd.correlationId);
  const confirmed = await repo.confirmWithEvent(order.id, payment.reference, event, ORDER_PLACED_ROUTING_KEY);
  logger.info({ event: 'order.confirmed', order_id: order.id, event_id: event.event_id }, 'Order confirmed; OrderPlaced stored in outbox');

  // Turning the hold into a sale is retried (3 attempts with backoff) but not guaranteed.
  // KNOWN GAP: if every attempt fails, the reservation TTL would later return sold stock.
  // Production fix: Inventory subscribes to OrderPlaced and commits the hold itself,
  // getting the outbox's at-least-once delivery instead of a best-effort HTTP call.
  inventory.commit(order.id).catch((err) =>
    logger.warn({ event: 'saga.commit_deferred', order_id: order.id, err: err.message }, 'Reservation commit deferred')
  );

  return { order: confirmed };
}

async function compensateReservation(inventory, logger, orderId) {
  try {
    await inventory.release(orderId);
    logger.info({ event: 'saga.compensated', order_id: orderId }, 'Reservation released (compensating action)');
  } catch (err) {
    // Reservations expire automatically (TTL) in Inventory, so stock is never lost permanently.
    logger.error({ event: 'saga.compensation_failed', order_id: orderId, err: err.message }, 'Release failed - TTL will free stock');
  }
}

module.exports = { placeOrder };
