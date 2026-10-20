// Synchronous calls to the Inventory service. The customer must know at checkout
// whether stock is available, so reserve is request/response (see interaction matrix).
const { createHttpClient } = require('./http-client');

function createInventoryClient(options) {
  const request = createHttpClient(options);
  return {
    reserve: (reservationId, items) =>
      request('POST', '/reservations', {
        reservationId,
        items: items.map(({ sku, qty }) => ({ sku, qty })),
      }),
    release: (reservationId) => request('POST', `/reservations/${reservationId}/release`),
    commit: (reservationId) => request('POST', `/reservations/${reservationId}/commit`),
  };
}

module.exports = { createInventoryClient };
