// All configuration comes from environment variables (12-factor III).
// Missing required values stop the process at start-up rather than failing later.
function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable ${name}`);
  return value;
}

module.exports = {
  port: Number(process.env.PORT || 3001),
  databaseUrl: required('DATABASE_URL'),
  rabbitmqUrl: required('RABBITMQ_URL'),
  inventoryUrl: required('INVENTORY_URL'),
  eventsExchange: process.env.EVENTS_EXCHANGE || 'maltashop.events',
  httpTimeoutMs: Number(process.env.HTTP_TIMEOUT_MS || 2000),
  httpMaxAttempts: Number(process.env.HTTP_MAX_ATTEMPTS || 3),
  outboxPollMs: Number(process.env.OUTBOX_POLL_MS || 500),
};
