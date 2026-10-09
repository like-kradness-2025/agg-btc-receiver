/**
 * Set 8: open interest / mark / funding normalization, ported from v1's `lib/oi-schema.mjs`.
 *
 * v1 wrote its auxiliary sample as one `open_interest` row in `raw_batches`, and the payload is the
 * normalizer's output joined with the collector's own sample (`lib/derivatives-helper.mjs:180-195`).
 * The measured payload (2026-10-10, `~/Tool/agg-btc-receiver/data/sqlite/<market>.sqlite`) is the
 * specification this module reproduces field for field, so the downstream sees the same `oi_v1`
 * shape it has always seen:
 *
 *   { ts, source_ts, market, mark_price, funding_rate, open_interest, next_funding_time, source,
 *     schema:'oi_v1', instrument_type, status, as_of_ms, age_ms, native_unit, oi_native, oi_btc,
 *     oi_usd, price_usd, error_code, error_message }
 *
 * Only the five perpetual markets v1 collected are admitted (`OI_CAPABILITIES`): the capabilities
 * (exchange, symbol, native unit and its conversion) are v1's, verbatim. The normalizer is pure -
 * it fetches nothing and starts nothing.
 */

export const OI_SCHEMA = 'oi_v1';
export const DEFAULT_OI_MAX_AGE_MS = 30_000;
export const DEFAULT_OI_CLOCK_SKEW_TOLERANCE_MS = 2_000;

function finite(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

const capability = (exchange, symbol, nativeUnit, extra = {}) =>
  Object.freeze({ instrument_type: 'perp', exchange, symbol, native_unit: nativeUnit, ...extra });

/** v1's five perp registrations, verbatim (`lib/oi-schema.mjs:22-35`). */
export const OI_CAPABILITIES = Object.freeze({
  binance_perp: capability('binance', 'BTCUSDT', 'BTC', { btc_per_native: 1 }),
  binance_perp_btcusdc: capability('binance', 'BTCUSDC', 'BTC', { btc_per_native: 1 }),
  bybit_perp: capability('bybit', 'BTCUSDT', 'BTC', { btc_per_native: 1 }),
  okx_perp: capability('okx', 'BTC-USDT-SWAP', 'contract', {}),
  hyperliquid_perp: capability('hyperliquid', 'BTC', 'BTC', { btc_per_native: 1 }),
});

export function getOICapability(market) {
  return OI_CAPABILITIES[market] || null;
}

/** v1's `source` column: the market's exchange prefix (`market.replace(/_perp$|_.*$/, '')`). */
export function sourceOf(market) {
  return String(market).replace(/_perp$|_.*$/, '');
}

function errorRow(market, asOfMs, nativeUnit, errorCode, errorMessage) {
  return {
    schema: OI_SCHEMA,
    market,
    instrument_type: 'perp',
    status: 'error',
    as_of_ms: asOfMs,
    age_ms: null,
    native_unit: nativeUnit,
    oi_native: null,
    oi_btc: null,
    oi_usd: null,
    price_usd: null,
    error_code: errorCode,
    error_message: errorMessage,
  };
}

/**
 * Normalize one sample (this is v1's `normalizeOpenInterest`, `lib/oi-schema.mjs:109-230`).
 * `input` carries `{ market, open_interest/oi_native, mark_price/price_usd, oiCcy/oi_btc, oiUsd/oi_usd,
 * ts/as_of_ms, error, error_code }`; `options` carries `nowMs` (the collector clock).
 */
export function normalizeOpenInterest(input = {}, options = {}) {
  const market = input.market;
  const spec = getOICapability(market);
  const asOfMs = finite(input.as_of_ms ?? input.timestamp_ms ?? input.observed_at_ms ?? input.ts);
  const nowMs = finite(options.nowMs ?? Date.now());
  const maxAgeMs = finite(options.maxAgeMs ?? DEFAULT_OI_MAX_AGE_MS);
  const clockSkewToleranceMs = Math.max(
    0,
    finite(options.clockSkewToleranceMs ?? DEFAULT_OI_CLOCK_SKEW_TOLERANCE_MS) ?? DEFAULT_OI_CLOCK_SKEW_TOLERANCE_MS,
  );

  if (!spec) {
    return errorRow(market, asOfMs, null, 'oi_not_supported', `open interest is not supported for ${market || 'unknown market'}`);
  }
  if (input.error || input.error_code || input.status === 'error') {
    return errorRow(market, asOfMs, spec.native_unit, input.error_code || 'source_error', String(input.error || input.error_message || 'source error'));
  }
  if (asOfMs === null || !Number.isFinite(nowMs) || !Number.isFinite(maxAgeMs) || maxAgeMs < 0) {
    return errorRow(market, asOfMs, spec.native_unit, 'invalid_timestamp', 'valid timestamp and maxAgeMs are required');
  }

  const oiNative = finite(input.oi_native ?? input.open_interest ?? input.openInterest ?? input.value);
  const oiBtcSource = finite(input.oi_btc ?? input.oiCcy);
  const oiUsdSource = finite(input.oi_usd ?? input.oiUsd);
  const priceUsd = finite(input.price_usd ?? input.mark_price ?? input.markPrice ?? input.last_price);

  if (oiNative !== null && oiNative < 0) {
    return errorRow(market, asOfMs, spec.native_unit, 'invalid_open_interest', 'open interest must be a finite non-negative number');
  }
  if (oiBtcSource !== null && oiBtcSource < 0) {
    return errorRow(market, asOfMs, spec.native_unit, 'invalid_open_interest', 'oiCcy/oi_btc must be non-negative');
  }
  if (oiUsdSource !== null && oiUsdSource < 0) {
    return errorRow(market, asOfMs, spec.native_unit, 'invalid_open_interest', 'oiUsd/oi_usd must be non-negative');
  }
  if (oiNative === null && oiBtcSource === null && oiUsdSource === null) {
    return errorRow(market, asOfMs, spec.native_unit, 'invalid_open_interest', 'open interest must be a finite non-negative number');
  }

  let oiBtc;
  let oiUsd;
  if (oiBtcSource !== null && oiUsdSource !== null) {
    if (priceUsd !== null && priceUsd > 0) {
      const expectedUsd = oiBtcSource * priceUsd;
      const tolerance = finite(options.mismatchTolerance) ?? 0.01;
      const denom = Math.max(Math.abs(oiUsdSource), 1e-9);
      if (Math.abs(expectedUsd - oiUsdSource) / denom > tolerance) {
        return errorRow(market, asOfMs, spec.native_unit, 'oi_mismatch', `oiCcy/oi_btc (${oiBtcSource} BTC) and oiUsd/oi_usd (${oiUsdSource} USD) are inconsistent at mark price ${priceUsd}`);
      }
    }
    oiBtc = oiBtcSource;
    oiUsd = oiUsdSource;
  } else if (oiBtcSource !== null) {
    if (priceUsd === null || priceUsd <= 0) {
      return errorRow(market, asOfMs, spec.native_unit, 'missing_price', 'positive price_usd/mark_price is required to derive USD notional');
    }
    oiBtc = oiBtcSource;
    oiUsd = oiBtcSource * priceUsd;
  } else if (oiUsdSource !== null) {
    if (priceUsd === null || priceUsd <= 0) {
      return errorRow(market, asOfMs, spec.native_unit, 'missing_price', 'positive price_usd/mark_price is required to derive BTC notional');
    }
    oiUsd = oiUsdSource;
    oiBtc = oiUsdSource / priceUsd;
  } else {
    if (priceUsd === null || priceUsd <= 0) {
      return errorRow(market, asOfMs, spec.native_unit, 'missing_price', 'positive price_usd/mark_price is required for BTC and USD normalization');
    }
    if (spec.btc_per_native !== undefined) {
      oiBtc = oiNative * spec.btc_per_native;
      oiUsd = oiBtc * priceUsd;
    } else if (spec.contract_value_btc !== undefined) {
      oiBtc = oiNative * spec.contract_value_btc;
      oiUsd = oiBtc * priceUsd;
    } else if (spec.contract_value_usd !== undefined) {
      oiUsd = oiNative * spec.contract_value_usd;
      oiBtc = oiUsd / priceUsd;
    } else {
      return errorRow(market, asOfMs, spec.native_unit, 'unsupported_native_unit', `cannot convert native unit ${spec.native_unit} without source oiCcy/oiUsd`);
    }
  }

  const futureSkewMs = asOfMs - nowMs;
  const effectiveAsOfMs = futureSkewMs > 0 && futureSkewMs <= clockSkewToleranceMs ? nowMs : asOfMs;
  const ageMs = nowMs - effectiveAsOfMs;
  if (ageMs < 0) {
    return errorRow(market, asOfMs, spec.native_unit, 'future_sample', 'sample timestamp is after the as-of clock');
  }
  return {
    schema: OI_SCHEMA,
    market,
    instrument_type: spec.instrument_type,
    status: ageMs <= maxAgeMs ? 'fresh' : 'stale',
    as_of_ms: effectiveAsOfMs,
    age_ms: ageMs,
    native_unit: spec.native_unit,
    oi_native: oiNative,
    oi_btc: oiBtc,
    oi_usd: oiUsd,
    price_usd: priceUsd,
    error_code: null,
    error_message: null,
  };
}

/** The persisted event time: at or before the collector receive time (`lib/oi-schema.mjs:95-100`). */
export function openInterestEventTimestamp(row) {
  const receiveTs = finite(row?.ts);
  if (receiveTs === null) return null;
  const eventTs = finite(row?.as_of_ms) ?? finite(row?.source_ts) ?? receiveTs;
  return Math.min(eventTs, receiveTs);
}

/**
 * Set 8: one `open_interest` raw record, from a venue sample. The payload is v1's collector row - the
 * sample's own fields first, then the normalizer's output - so the shape matches the measured store.
 * `ts` is the collector clock, `source_ts` the exchange sample time (`lib/derivatives-helper.mjs:180-193`).
 */
export function openInterestRecord({ market, sample, ts, nowMs = ts }) {
  const sourceTs = finite(sample?.source_ts);
  const asOf = finite(sample?.ts) ?? sourceTs;
  const normalized = normalizeOpenInterest({ market, ts: asOf, ...sample }, { nowMs });
  const row = {
    ts,
    source_ts: sourceTs,
    market,
    mark_price: finite(sample?.mark_price) ?? null,
    funding_rate: finite(sample?.funding_rate) ?? null,
    open_interest: finite(sample?.open_interest) ?? null,
    next_funding_time: finite(sample?.next_funding_time) ?? null,
    source: sourceOf(market),
    ...normalized,
    ts,
    source_ts: sourceTs,
  };
  const eventTs = openInterestEventTimestamp(row);
  const event_ts_ms = Number.isInteger(eventTs) && eventTs > 0 ? eventTs : ts;
  return {
    stream: 'open_interest',
    event_ts_ms,
    source_event_ts_ms: sourceTs,
    source_event_time_known: sourceTs !== null,
    source_id: row.source,
    payload: row,
  };
}
