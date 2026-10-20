// Structured JSON logging to stdout (12-factor XI: logs as event streams).
// Every line carries service, version, environment and - when inside a request
// or message handler - the correlation_id, so one order can be traced across services.
const pino = require('pino');
const { AsyncLocalStorage } = require('node:async_hooks');

const context = new AsyncLocalStorage();

const logger = pino({
  messageKey: 'message',
  timestamp: pino.stdTimeFunctions.isoTime,
  base: {
    service: process.env.SERVICE_NAME || 'inventory-service',
    version: process.env.APP_VERSION || 'dev',
    environment: process.env.APP_ENV || 'local',
  },
  formatters: { level: (label) => ({ level: label }) },
  mixin() {
    const store = context.getStore();
    return store ? { correlation_id: store.correlationId } : {};
  },
});

const currentCorrelationId = () => context.getStore()?.correlationId;

module.exports = { logger, context, currentCorrelationId };
