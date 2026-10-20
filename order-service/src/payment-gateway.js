// STAND-IN for the Payment service + external payment provider (PSP).
// The prototype does not include a separate Payment service; this adapter mimics
// the behaviour that matters for the saga:
//   - card data never reaches MaltaShop: the client sends a PSP token
//   - the PSP de-duplicates on an idempotency key, so a retried charge is never taken twice
// Test tokens: "tok_visa" -> approved, "tok_decline" -> declined.
const { randomUUID } = require('node:crypto');

function createMockPaymentGateway() {
  const charges = new Map(); // idempotencyKey -> result (simulates the PSP's own idempotency store)

  return {
    async charge({ idempotencyKey, amountCents, token }) {
      if (charges.has(idempotencyKey)) return charges.get(idempotencyKey);
      const result =
        token === 'tok_decline'
          ? { status: 'declined', reason: 'card_declined' }
          : { status: 'approved', reference: `psp_ch_${randomUUID().slice(0, 8)}`, amountCents };
      charges.set(idempotencyKey, result);
      return result;
    },
  };
}

module.exports = { createMockPaymentGateway };
