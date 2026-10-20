// Stock reservations. Every operation is idempotent because the saga may retry it:
//   reserve  - INSERT ... ON CONFLICT: a repeated reserve returns the existing reservation
//   release  - only HELD reservations give stock back; releasing an unknown id writes a
//              RELEASED "tombstone", so a reserve that arrives late (after a timeout and
//              compensation) cannot take stock for an order that has already failed
//   commit   - HELD -> COMMITTED, otherwise a no-op
const SCHEMA = `
CREATE TABLE IF NOT EXISTS stock (
  sku        text PRIMARY KEY,
  available  integer NOT NULL CHECK (available >= 0)
);
CREATE TABLE IF NOT EXISTS reservations (
  id          uuid PRIMARY KEY,                 -- = order id
  status      text NOT NULL CHECK (status IN ('HELD','COMMITTED','RELEASED')),
  expires_at  timestamptz,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS reservation_items (
  reservation_id  uuid NOT NULL REFERENCES reservations(id),
  sku             text NOT NULL REFERENCES stock(sku),
  qty             integer NOT NULL CHECK (qty > 0),
  PRIMARY KEY (reservation_id, sku)
);
`;

const SEED = [
  ['TV-55-OLED', 25],
  ['KETTLE-STEEL', 100],
  ['AIRFRYER-5L', 40],
  ['LAMP-DESK', 3], // deliberately low, to demonstrate the out-of-stock path
];

async function migrateAndSeed(pool) {
  await pool.query(SCHEMA);
  for (const [sku, qty] of SEED) {
    await pool.query('INSERT INTO stock (sku, available) VALUES ($1, $2) ON CONFLICT (sku) DO NOTHING', [sku, qty]);
  }
}

// Merge duplicate SKUs and sort them so concurrent reservations always lock rows
// in the same order (prevents deadlocks under Black Friday load).
function normaliseItems(items) {
  const totals = new Map();
  for (const { sku, qty } of items) totals.set(sku, (totals.get(sku) || 0) + qty);
  return [...totals.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([sku, qty]) => ({ sku, qty }));
}

async function inTransaction(pool, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query(result?.rollback ? 'ROLLBACK' : 'COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

function createReservationService(pool, { ttlSeconds }) {
  return {
    reserve: (reservationId, rawItems) =>
      inTransaction(pool, async (db) => {
        const inserted = await db.query(
          `INSERT INTO reservations (id, status, expires_at)
           VALUES ($1, 'HELD', now() + make_interval(secs => $2))
           ON CONFLICT (id) DO NOTHING RETURNING id`,
          [reservationId, ttlSeconds]
        );
        if (inserted.rowCount === 0) {
          const { rows } = await db.query('SELECT status FROM reservations WHERE id = $1', [reservationId]);
          return { code: 200, body: { reservationId, status: rows[0].status, replay: true } };
        }
        for (const { sku, qty } of normaliseItems(rawItems)) {
          // Conditional update = atomic "check and decrement"; no read-then-write race.
          const updated = await db.query(
            'UPDATE stock SET available = available - $2 WHERE sku = $1 AND available >= $2',
            [sku, qty]
          );
          if (updated.rowCount === 0) {
            return { rollback: true, code: 409, body: { error: 'OUT_OF_STOCK', sku } };
          }
          await db.query('INSERT INTO reservation_items (reservation_id, sku, qty) VALUES ($1, $2, $3)', [reservationId, sku, qty]);
        }
        return { code: 201, body: { reservationId, status: 'HELD' } };
      }),

    release: (reservationId) =>
      inTransaction(pool, async (db) => {
        const { rows } = await db.query('SELECT status FROM reservations WHERE id = $1 FOR UPDATE', [reservationId]);
        if (rows.length === 0) {
          await db.query(`INSERT INTO reservations (id, status) VALUES ($1, 'RELEASED') ON CONFLICT (id) DO NOTHING`, [reservationId]);
          return { code: 200, body: { reservationId, status: 'RELEASED', note: 'tombstone' } };
        }
        if (rows[0].status === 'COMMITTED') return { code: 409, body: { error: 'ALREADY_COMMITTED' } };
        if (rows[0].status === 'HELD') {
          await db.query(
            `UPDATE stock s SET available = s.available + ri.qty
             FROM reservation_items ri WHERE ri.reservation_id = $1 AND ri.sku = s.sku`,
            [reservationId]
          );
          await db.query(`UPDATE reservations SET status = 'RELEASED', updated_at = now() WHERE id = $1`, [reservationId]);
        }
        return { code: 200, body: { reservationId, status: 'RELEASED' } };
      }),

    commit: (reservationId) =>
      inTransaction(pool, async (db) => {
        const { rows } = await db.query(
          `UPDATE reservations SET status = 'COMMITTED', expires_at = NULL, updated_at = now()
           WHERE id = $1 AND status IN ('HELD','COMMITTED') RETURNING status`,
          [reservationId]
        );
        if (rows.length === 0) return { code: 409, body: { error: 'NOT_HELD' } };
        return { code: 200, body: { reservationId, status: 'COMMITTED' } };
      }),

    // Safety net for the saga: holds that were never committed or released expire.
    async releaseExpired() {
      const { rows } = await pool.query(
        `SELECT id FROM reservations WHERE status = 'HELD' AND expires_at < now() LIMIT 100`
      );
      for (const { id } of rows) await this.release(id);
      return rows.length;
    },

    async getStock(sku) {
      const { rows } = await pool.query('SELECT sku, available FROM stock WHERE sku = $1', [sku]);
      return rows[0];
    },
  };
}

module.exports = { migrateAndSeed, createReservationService, normaliseItems };
