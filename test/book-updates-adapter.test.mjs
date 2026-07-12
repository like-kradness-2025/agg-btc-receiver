// test/book-updates-adapter.test.mjs — B1 canonical adapter unit tests (RED→GREEN)
//
// Tests the pure adapter that normalizes exchange-specific shallow depth events
// (type/bids/asks/ts/seq) into book_updates_v1 canonical envelopes with fail-closed validation.
//
// B1 scope only: no state machine, no pipeline join, no quarantine, no kind checkpoint,
// no board columns, no rollup.
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { toCanonicalBookEnvelope } from '../lib/book-updates-adapter.mjs';

// ─── Helpers ──────────────────────────────────────────────────────────────

const validRawEvent = () => ({
  market: 'binance_spot',
  type: 'snapshot',
  bids: [['50000', '1.5'], ['49900', '2.0']],
  asks: [['50010', '2.0'], ['50020', '1.0']],
  ts: 1000,
  seq: 100,
});

const validUpdateEvent = () => ({
  market: 'bybit_perp',
  type: 'update',
  bids: [['50005', '0.5']],
  asks: [['50015', '1.5']],
  ts: 2000,
  seq: 200,
});

// ─── Tests ────────────────────────────────────────────────────────────────

describe('toCanonicalBookEnvelope', () => {
  // ── Valid inputs ──

  it('transforms a valid snapshot event to canonical envelope', () => {
    const raw = validRawEvent();
    const env = toCanonicalBookEnvelope(raw);

    assert.equal(env.valid, true);
    assert.equal(env.errors, null);

    const e = env.envelope;
    assert.equal(e.schema_version, 'book_updates_v1');
    assert.equal(e.market, 'binance_spot');
    assert.equal(e.type, 'snapshot');
    assert.equal(e.event_ts_ms, 1000);
    assert.equal(e.seq, 100);
    // prev_seq is not supplied by the raw event; adapter must not invent one
    assert.equal(e.prev_seq, null);
    assert.deepEqual(e.bids, [['50000', '1.5'], ['49900', '2.0']]);
    assert.deepEqual(e.asks, [['50010', '2.0'], ['50020', '1.0']]);
    // source must be present with adapter metadata
    assert.ok(e.source);
    assert.equal(e.source.adapter, 'book-updates-adapter');
    assert.equal(e.source.adapter_version, '1.0.0');
    assert.equal(e.source.channel, 'book');
  });

  it('transforms a valid update event to canonical envelope', () => {
    const raw = validUpdateEvent();
    const env = toCanonicalBookEnvelope(raw);

    assert.equal(env.valid, true);
    assert.equal(env.errors, null);

    const e = env.envelope;
    assert.equal(e.schema_version, 'book_updates_v1');
    assert.equal(e.market, 'bybit_perp');
    assert.equal(e.type, 'update');
    assert.equal(e.event_ts_ms, 2000);
    assert.equal(e.seq, 200);
    assert.equal(e.prev_seq, null);
    assert.deepEqual(e.bids, [['50005', '0.5']]);
    assert.deepEqual(e.asks, [['50015', '1.5']]);
    assert.ok(e.source);
  });

  it('handles null seq gracefully (unsequenced exchanges)', () => {
    const raw = validRawEvent();
    raw.seq = null;
    const env = toCanonicalBookEnvelope(raw);

    assert.equal(env.valid, true);
    assert.equal(env.envelope.seq, null);
    assert.equal(env.envelope.prev_seq, null);
  });

  it('handles fractional ts by rounding down to integer ms', () => {
    const raw = validRawEvent();
    raw.ts = 1234.567;
    const env = toCanonicalBookEnvelope(raw);

    assert.equal(env.valid, true);
    assert.equal(env.envelope.event_ts_ms, 1234);
  });

  it('passes through source if already present in raw event', () => {
    const raw = validRawEvent();
    raw.source = { exchange: 'custom', extra: true };
    const env = toCanonicalBookEnvelope(raw);

    assert.equal(env.valid, true);
    assert.equal(env.envelope.source.exchange, 'custom');
    // Should NOT overwrite with default
    assert.equal(env.envelope.source.adapter, 'book-updates-adapter');
  });

  it('handles empty bids/asks arrays (valid-empty sides)', () => {
    const raw = validRawEvent();
    raw.bids = [];
    raw.asks = [];
    const env = toCanonicalBookEnvelope(raw);

    assert.equal(env.valid, true);
    assert.deepEqual(env.envelope.bids, []);
    assert.deepEqual(env.envelope.asks, []);
  });

  // ── Fail-closed: type validation ──

  it('rejects null input', () => {
    const env = toCanonicalBookEnvelope(null);
    assert.equal(env.valid, false);
    assert.ok(env.errors.length > 0);
  });

  it('rejects undefined input', () => {
    const env = toCanonicalBookEnvelope(undefined);
    assert.equal(env.valid, false);
    assert.ok(env.errors.length > 0);
  });

  it('rejects non-object input', () => {
    const env = toCanonicalBookEnvelope('not-an-object');
    assert.equal(env.valid, false);
    assert.ok(env.errors.length > 0);
  });

  // ── Fail-closed: market ──

  it('rejects missing market', () => {
    const raw = validRawEvent();
    delete raw.market;
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
    assert.ok(env.errors.some(e => e.includes('market')));
  });

  it('rejects empty market string', () => {
    const raw = validRawEvent();
    raw.market = '';
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  // ── Fail-closed: type field ──

  it('rejects invalid type', () => {
    const raw = validRawEvent();
    raw.type = 'invalid_type';
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
    assert.ok(env.errors.some(e => e.includes('type')));
  });

  it('rejects missing type', () => {
    const raw = validRawEvent();
    delete raw.type;
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  // ── Fail-closed: event_ts_ms ──

  it('rejects missing ts', () => {
    const raw = validRawEvent();
    delete raw.ts;
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
    assert.ok(env.errors.some(e => e.includes('ts')));
  });

  it('rejects non-finite ts', () => {
    const raw = validRawEvent();
    raw.ts = NaN;
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  it('rejects infinite ts', () => {
    const raw = validRawEvent();
    raw.ts = Infinity;
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  it('rejects negative ts', () => {
    const raw = validRawEvent();
    raw.ts = -500;
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  // ── Fail-closed: seq ──

  it('rejects negative seq', () => {
    const raw = validRawEvent();
    raw.seq = -1;
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
    assert.ok(env.errors.some(e => e.includes('seq')));
  });

  it('rejects non-integer seq', () => {
    const raw = validRawEvent();
    raw.seq = 100.5;
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  it('rejects non-numeric seq', () => {
    const raw = validRawEvent();
    raw.seq = 'abc';
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  // ── Fail-closed: bids/asks ──

  it('rejects non-array bids', () => {
    const raw = validRawEvent();
    raw.bids = 'not-array';
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  it('rejects non-array asks', () => {
    const raw = validRawEvent();
    raw.asks = 'not-array';
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  it('rejects bids entry that is not a 2-element array', () => {
    const raw = validRawEvent();
    raw.bids = [['50000']]; // only price, no qty
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
    assert.ok(env.errors.some(e => e.includes('bids')));
  });

  it('rejects asks entry with non-numeric price', () => {
    const raw = validRawEvent();
    raw.asks = [['abc', '1.0']];
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  it('rejects asks entry with non-positive price', () => {
    const raw = validRawEvent();
    raw.asks = [['0', '1.0']];
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  it('rejects bids entry with negative qty', () => {
    const raw = validRawEvent();
    raw.bids = [['50000', '-1']];
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  it('rejects bids entry with non-numeric qty', () => {
    const raw = validRawEvent();
    raw.bids = [['50000', 'abc']];
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, false);
  });

  // qty=0 is valid (delete signal)
  it('accepts qty=0 (valid-level delete)', () => {
    const raw = validRawEvent();
    raw.bids = [['50000', '0']];
    raw.asks = [['50010', '0']];
    const env = toCanonicalBookEnvelope(raw);
    assert.equal(env.valid, true);
  });

  // ── Multiple errors ──

  it('reports multiple validation errors in one call', () => {
    const env = toCanonicalBookEnvelope({
      market: '',
      type: 'bad',
      bids: 'invalid',
      asks: 'invalid',
      ts: NaN,
      seq: -1,
    });
    assert.equal(env.valid, false);
    // Should have accumulated errors for each invalid field
    assert.ok(env.errors.length >= 3);
  });
});
