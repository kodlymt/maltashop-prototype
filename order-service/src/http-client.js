// Small resilient HTTP client: every call has a timeout, and only network errors
// or 5xx responses are retried, with exponential backoff and full jitter.
// Retrying is safe because every downstream operation is idempotent
// (the reservation id is the order id).
const { currentCorrelationId, logger } = require('./logger');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function createHttpClient({ baseUrl, timeoutMs, maxAttempts, baseDelayMs = 100 }) {
  return async function request(method, path, body) {
    for (let attempt = 1; ; attempt++) {
      try {
        const res = await fetch(`${baseUrl}${path}`, {
          method,
          headers: {
            'content-type': 'application/json',
            'x-correlation-id': currentCorrelationId() || '',
          },
          body: body === undefined ? undefined : JSON.stringify(body),
          signal: AbortSignal.timeout(timeoutMs),
        });
        if (res.status >= 500) throw Object.assign(new Error(`HTTP ${res.status}`), { retryable: true });
        const json = await res.json().catch(() => ({}));
        return { status: res.status, body: json };
      } catch (err) {
        const retryable = err.retryable || err.name === 'TimeoutError' || err.name === 'TypeError';
        if (!retryable || attempt >= maxAttempts) throw err;
        const delay = Math.random() * baseDelayMs * 2 ** attempt; // full jitter
        logger.warn({ event: 'http.retry', target: `${method} ${path}`, attempt, delay_ms: Math.round(delay), err: err.message }, 'Retrying downstream call');
        await sleep(delay);
      }
    }
  };
}

module.exports = { createHttpClient };
