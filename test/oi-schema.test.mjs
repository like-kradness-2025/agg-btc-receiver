import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  DEFAULT_OI_MAX_AGE_MS,
  OI_CAPABILITIES,
  getOICapability,
  joinOpenInterestAsOf,
  normalizeOpenInterest,
  openInterestAsOf,
  supportsOpenInterest,
} from '../lib/oi-schema.mjs';

describe('OI schema and capabilities', () => {
  it('exposes exactly the six perp capabilities and no spot capability', () => {
    assert.deepEqual(Object.keys(OI_CAPABILITIES).sort(), [
      'binance_perp', 'binance_perp_btcusdc', 'bitmex_perp',
      'bybit_perp', 'hyperliquid_perp', 'okx_perp',
    ]);
    assert.equal(supportsOpenInterest('binance_spot'), false);
    assert.equal(getOICapability('binance_spot'), null);
  });

  it('normalizes BTC-native OI into BTC and USD', () => {
    const row = normalizeOpenInterest({
      market: 'binance_perp', ts: 1_000, open_interest: '2.5', mark_price: '40000',
    }, { nowMs: 1_500 });
    assert.equal(row.status, 'fresh');
    assert.equal(row.native_unit, 'BTC');
    assert.equal(row.oi_native, 2.5);
    assert.equal(row.oi_btc, 2.5);
    assert.equal(row.oi_usd, 100_000);
  });

  it('normalizes contract-native OI using the capability unit', () => {
    const bitmex = normalizeOpenInterest({ market: 'bitmex_perp', ts: 1_000, open_interest: 100, mark_price: 40_000 }, { nowMs: 1_000 });
    assert.equal(bitmex.oi_usd, 100);
    assert.equal(bitmex.oi_btc, 0.0025);
  });

  it('marks old samples stale and rejects spot OI', () => {
    const stale = normalizeOpenInterest({ market: 'bybit_perp', ts: 0, open_interest: 1, mark_price: 40_000 }, { nowMs: DEFAULT_OI_MAX_AGE_MS + 1 });
    assert.equal(stale.status, 'stale');
    assert.equal(stale.oi_btc, 1);

    const spot = normalizeOpenInterest({ market: 'binance_spot', ts: 1_000, open_interest: 1, mark_price: 40_000 }, { nowMs: 1_000 });
    assert.equal(spot.status, 'error');
    assert.equal(spot.error_code, 'oi_not_supported');
    assert.equal(spot.oi_btc, null);
  });

  it('reports source and conversion errors instead of fabricating zeros', () => {
    const sourceError = normalizeOpenInterest({ market: 'bybit_perp', ts: 1_000, error: 'HTTP 503' }, { nowMs: 1_000 });
    assert.equal(sourceError.status, 'error');
    assert.equal(sourceError.error_code, 'source_error');

    const missingPrice = normalizeOpenInterest({ market: 'bybit_perp', ts: 1_000, open_interest: 1 }, { nowMs: 1_000 });
    assert.equal(missingPrice.status, 'error');
    assert.equal(missingPrice.error_code, 'missing_price');
  });
});

describe('OI as-of join', () => {
  const samples = [
    { market: 'binance_perp', ts: 1_000, open_interest: 2, mark_price: 40_000 },
    { market: 'binance_perp', ts: 4_000, open_interest: 3, mark_price: 41_000 },
  ];

  it('uses only the latest sample at or before the anchor', () => {
    const row = openInterestAsOf(samples, 3_000, { maxAgeMs: 5_000 });
    assert.equal(row.status, 'fresh');
    assert.equal(row.as_of_ms, 1_000);
    assert.equal(row.oi_btc, 2);
  });

  it('does not look ahead or forward-fill a stale sample', () => {
    const joined = joinOpenInterestAsOf([
      { ts: 500 },
      { ts: 1_000 },
      { ts: 7_000 },
    ], samples, { maxAgeMs: 2_000 });
    assert.equal(joined[0].open_interest, null);
    assert.equal(joined[0].open_interest_status, 'error');
    assert.equal(joined[1].open_interest.oi_btc, 2);
    assert.equal(joined[2].open_interest, null);
    assert.equal(joined[2].open_interest_status, 'stale');
    assert.equal(joined[2].open_interest_as_of_ms, 4_000);
  });
});

describe('OI unit regression', () => {
  it('prefers OKX source oiCcy/oiUsd and keeps oi contracts for audit', () => {
    const row = normalizeOpenInterest({
      market: 'okx_perp', ts: 1_000, open_interest: 200, oiCcy: '2', oiUsd: '60000', mark_price: 30_000,
    }, { nowMs: 1_000 });
    assert.equal(row.status, 'fresh');
    assert.equal(row.oi_native, 200);
    assert.equal(row.oi_btc, 2);
    assert.equal(row.oi_usd, 60_000);
  });

  it('complements missing OKX oiUsd from oiCcy and mark price', () => {
    const row = normalizeOpenInterest({
      market: 'okx_perp', ts: 1_000, open_interest: 200, oiCcy: '2', mark_price: 30_000,
    }, { nowMs: 1_000 });
    assert.equal(row.status, 'fresh');
    assert.equal(row.oi_native, 200);
    assert.equal(row.oi_btc, 2);
    assert.equal(row.oi_usd, 60_000);
  });

  it('complements missing OKX oiCcy from oiUsd and mark price', () => {
    const row = normalizeOpenInterest({
      market: 'okx_perp', ts: 1_000, open_interest: 200, oiUsd: '60000', mark_price: 30_000,
    }, { nowMs: 1_000 });
    assert.equal(row.status, 'fresh');
    assert.equal(row.oi_native, 200);
    assert.equal(row.oi_btc, 2);
    assert.equal(row.oi_usd, 60_000);
  });

  it('reports OKX oiCcy/oiUsd mismatch instead of fabricating', () => {
    const row = normalizeOpenInterest({
      market: 'okx_perp', ts: 1_000, open_interest: 200, oiCcy: '3', oiUsd: '600000', mark_price: 30_000,
    }, { nowMs: 1_000 });
    assert.equal(row.status, 'error');
    assert.equal(row.error_code, 'oi_mismatch');
    assert.equal(row.oi_btc, null);
    assert.equal(row.oi_usd, null);
  });

  it('errors for OKX when neither oiCcy nor oiUsd is provided', () => {
    const row = normalizeOpenInterest({
      market: 'okx_perp', ts: 1_000, open_interest: 200, mark_price: 30_000,
    }, { nowMs: 1_000 });
    assert.equal(row.status, 'error');
    assert.equal(row.error_code, 'unsupported_native_unit');
    assert.equal(row.oi_btc, null);
    assert.equal(row.oi_usd, null);
  });

  it('computes BitMEX inverse USD OI as USD with BTC = USD / price', () => {
    const row = normalizeOpenInterest({
      market: 'bitmex_perp', ts: 1_000, open_interest: 100_000, mark_price: 50_000,
    }, { nowMs: 1_000 });
    assert.equal(row.oi_usd, 100_000);
    assert.equal(row.oi_btc, 2);
  });

  it('reports age_ms and status relative to source_ts', () => {
    const fresh = normalizeOpenInterest({
      market: 'bybit_perp', ts: 1_000, open_interest: 1, mark_price: 40_000,
    }, { nowMs: 1_500 });
    assert.equal(fresh.age_ms, 500);
    assert.equal(fresh.status, 'fresh');

    const stale = normalizeOpenInterest({
      market: 'bybit_perp', ts: 1_000, open_interest: 1, mark_price: 40_000,
    }, { nowMs: 1_000 + DEFAULT_OI_MAX_AGE_MS + 1 });
    assert.equal(stale.status, 'stale');
    assert.equal(stale.age_ms, DEFAULT_OI_MAX_AGE_MS + 1);
  });
});
