// lib/book-updates-adapter.mjs — B1: normalizes exchange depth events into
// book_updates_v1 canonical envelopes with fail-closed validation.
//
// B1 scope only: no state machine, no pipeline join, no quarantine,
// no kind checkpoint, no board columns, no rollup.
// prev_seq is always null — sequencing belongs to a downstream stage.

import { normalizeBookLevels, bookQuantityMetadata, BOOK_QUANTITY_SCHEMA } from './market-registry.mjs';

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

  // Preserve exchange sequence bridges when the connector has them.  The
  // canonical state machine uses these to distinguish a valid range bridge
  // from a silently skipped update.
  for (const field of ['prev_seq', 'seq_start', 'seq_end']) {
    if (raw[field] !== undefined && raw[field] !== null
        && (!Number.isSafeInteger(raw[field]) || raw[field] < 0)) {
      errors.push(`${field}: must be a non-negative safe integer or null`);
    }
  }
  if (raw.seq_start != null && raw.seq_end != null && raw.seq_start > raw.seq_end) {
    errors.push('seq_start: must not exceed seq_end');
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
      // Several connectors (notably Bitfinex) encode a level deletion as an
      // empty quantity string. Canonical book_updates_v1 represents that as
      // explicit zero so the state machine can delete the level.
      const qty = entry[1] === '' ? 0 : parseFloat(entry[1]);
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
      const qty = entry[1] === '' ? 0 : parseFloat(entry[1]);
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

  const normalizedBids = normalizeBookLevels(raw.market, raw.bids);
  const normalizedAsks = normalizeBookLevels(raw.market, raw.asks);
  if (!normalizedBids.valid || !normalizedAsks.valid) {
    return { valid: false, errors: ['book quantity normalization failed'] };
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
  // Kraken's `c` field is a CRC checksum, not an order-book sequence.
  // Coinbase REST fallback uses sequence=0 as an unknown-sequence sentinel.
  const canonicalSeq = raw.market === 'kraken_spot'
    ? null
    : (raw.market === 'coinbase_spot' && raw.type === 'snapshot' && raw.seq === 0 ? null : raw.seq);

  const envelope = {
    schema_version: 'book_updates_v1',
    market: raw.market,
    type: raw.type,
    event_ts_ms: Math.floor(raw.ts),
    seq: canonicalSeq !== undefined ? canonicalSeq : null,
    prev_seq: raw.prev_seq !== undefined ? raw.prev_seq : null,
    ...(raw.seq_start != null ? { seq_start: raw.seq_start } : {}),
    ...(raw.seq_end != null ? { seq_end: raw.seq_end } : {}),
    bids: normalizedBids.levels,
    asks: normalizedAsks.levels,
    qty_unit: 'BTC',
    qty_normalization: raw.qty_normalization || bookQuantityMetadata(raw.market),
    source: {
      ...source,
      quantity_schema: raw.qty_normalization?.schema || BOOK_QUANTITY_SCHEMA,
    },
  };

  return { valid: true, errors: null, envelope };
}
