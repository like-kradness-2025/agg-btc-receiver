// lib/book-updates-adapter.mjs — B1: normalizes exchange depth events into
// book_updates_v1 canonical envelopes with fail-closed validation.
//
// B1 scope only: no state machine, no pipeline join, no quarantine,
// no kind checkpoint, no board columns, no rollup.
// prev_seq is always null — sequencing belongs to a downstream stage.

const ADAPTER_NAME = 'book-updates-adapter';
const ADAPTER_VERSION = '1.0.0';
const VALID_TYPES = new Set(['snapshot', 'update']);

/**
 * Normalize a raw exchange depth event into a book_updates_v1 canonical envelope.
 *
 * @param {*} raw — raw event with {market, type, bids, asks, ts, seq?, source?}
 * @returns {{valid: boolean, errors: string[]|null, envelope?: object}}
 */
export function toCanonicalBookEnvelope(raw) {
  const errors = [];

  // ── Top-level type guard ────────────────────────────────────────────
  if (raw === null || raw === undefined || typeof raw !== 'object' || Array.isArray(raw)) {
    return { valid: false, errors: ['Input must be a non-null object'] };
  }

  // ── market ──────────────────────────────────────────────────────────
  if (typeof raw.market !== 'string' || raw.market.trim() === '') {
    errors.push('market: must be a non-empty string');
  }

  // ── type ────────────────────────────────────────────────────────────
  if (typeof raw.type !== 'string' || !VALID_TYPES.has(raw.type)) {
    errors.push('type: must be "snapshot" or "update"');
  }

  // ── ts (timestamp) ──────────────────────────────────────────────────
  if (typeof raw.ts !== 'number' || !Number.isFinite(raw.ts) || raw.ts < 0) {
    errors.push('ts: must be a finite non-negative number');
  }

  // ── seq (sequence number — may be null for unsequenced exchanges) ───
  if (raw.seq !== undefined && raw.seq !== null) {
    if (typeof raw.seq !== 'number' || !Number.isFinite(raw.seq) || raw.seq < 0 || !Number.isSafeInteger(raw.seq)) {
      errors.push('seq: must be a non-negative integer or null');
    }
  }

  // ── bids ────────────────────────────────────────────────────────────
  if (!Array.isArray(raw.bids)) {
    errors.push('bids: must be an array');
  } else {
    for (let i = 0; i < raw.bids.length; i++) {
      const entry = raw.bids[i];
      if (!Array.isArray(entry) || entry.length !== 2) {
        errors.push(`bids[${i}]: must be a 2-element array [price, qty]`);
        continue;
      }
      const price = parseFloat(entry[0]);
      const qty = parseFloat(entry[1]);
      if (!Number.isFinite(price) || price <= 0) {
        errors.push(`bids[${i}]: price must be a positive number, got "${entry[0]}"`);
      }
      if (!Number.isFinite(qty) || qty < 0) {
        errors.push(`bids[${i}]: qty must be a non-negative number, got "${entry[1]}"`);
      }
    }
  }

  // ── asks ────────────────────────────────────────────────────────────
  if (!Array.isArray(raw.asks)) {
    errors.push('asks: must be an array');
  } else {
    for (let i = 0; i < raw.asks.length; i++) {
      const entry = raw.asks[i];
      if (!Array.isArray(entry) || entry.length !== 2) {
        errors.push(`asks[${i}]: must be a 2-element array [price, qty]`);
        continue;
      }
      const price = parseFloat(entry[0]);
      const qty = parseFloat(entry[1]);
      if (!Number.isFinite(price) || price <= 0) {
        errors.push(`asks[${i}]: price must be a positive number, got "${entry[0]}"`);
      }
      if (!Number.isFinite(qty) || qty < 0) {
        errors.push(`asks[${i}]: qty must be a non-negative number, got "${entry[1]}"`);
      }
    }
  }

  // ── Fail closed ─────────────────────────────────────────────────────
  if (errors.length > 0) {
    return { valid: false, errors };
  }

  // ── Build envelope source metadata ──────────────────────────────────
  // Merge any pre-existing source info from the raw event, but ensure
  // adapter metadata always identifies this adapter.
  const source = {
    ...(raw.source || {}),
    adapter: ADAPTER_NAME,
    adapter_version: ADAPTER_VERSION,
    channel: 'book',
  };

  // ── Canonical envelope ──────────────────────────────────────────────
  const envelope = {
    schema_version: 'book_updates_v1',
    market: raw.market,
    type: raw.type,
    event_ts_ms: Math.floor(raw.ts),
    seq: raw.seq !== undefined ? raw.seq : null,
    prev_seq: null,
    bids: raw.bids.map(e => [...e]),
    asks: raw.asks.map(e => [...e]),
    source,
  };

  return { valid: true, errors: null, envelope };
}
