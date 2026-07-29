// Open-interest contract shared by collectors and downstream joins.
// This module is intentionally pure: it does not fetch data or start services.

export const OI_SCHEMA = 'oi_v1';
export const DEFAULT_OI_MAX_AGE_MS = 30_000;
export const OI_STATUSES = Object.freeze(['fresh', 'stale', 'error']);

const capability = (exchange, symbol, nativeUnit, extra = {}) => Object.freeze({
  instrument_type: 'perp',
  exchange,
  symbol,
  native_unit: nativeUnit,
  ...extra,
});

// Keep this list aligned with the existing standalone aux collector's six perp
// registrations. Spot markets are deliberately absent.
export const OI_CAPABILITIES = Object.freeze({
  binance_perp: capability('binance', 'BTCUSDT', 'BTC', { btc_per_native: 1 }),
  bitmex_perp: capability('bitmex', 'XBTUSD', 'contract', {
    // BitMEX inverse XBTUSD is one USD per contract. Keep conversion explicit.
    contract_value_usd: 1,
  }),
  binance_perp_btcusdc: capability('binance', 'BTCUSDC', 'BTC', { btc_per_native: 1 }),
  bybit_perp: capability('bybit', 'BTCUSDT', 'BTC', { btc_per_native: 1 }),
  okx_perp: capability('okx', 'BTC-USDT-SWAP', 'contract', {
    // OKX provides oiCcy (BTC) and oiUsd (USD) directly. Do not assume a
    // fixed BTC-per-contract multiplier; use source values when present.
  }),
  hyperliquid_perp: capability('hyperliquid', 'BTC', 'BTC', { btc_per_native: 1 }),
});

export const PERP_OI_CAPABILITIES = OI_CAPABILITIES;

export function getOICapability(market) {
  return OI_CAPABILITIES[market] || null;
}

export const getOiCapability = getOICapability;

export function supportsOpenInterest(market) {
  return getOICapability(market) !== null;
}

function finite(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function inputParts(marketOrInput, row, options) {
  if (typeof marketOrInput === 'string') {
    return { input: { ...(row || {}), market: marketOrInput }, options };
  }
  return { input: marketOrInput || {}, options: row || {} };
}

function errorRow(market, asOfMs, nativeUnit, errorCode, errorMessage, extra = {}) {
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
    ...extra,
  };
}

function readNativeValue(input) {
  return finite(input.oi_native ?? input.open_interest ?? input.openInterest ?? input.value);
}

function readAsOfMs(input) {
  return finite(input.as_of_ms ?? input.timestamp_ms ?? input.observed_at_ms ?? input.ts);
}

function readPriceUsd(input) {
  return finite(input.price_usd ?? input.mark_price ?? input.markPrice ?? input.last_price);
}

/**
 * Normalize one raw or already-shaped OI sample.
 *
 * Signature may be either normalizeOpenInterest({ market, ... }, options) or
 * normalizeOpenInterest(market, row, options). Stale rows retain their
 * normalized values for diagnostics; as-of joins intentionally discard them.
 */
export function normalizeOpenInterest(marketOrInput, row, maybeOptions) {
  const { input, options } = inputParts(marketOrInput, row, maybeOptions);
  const market = input.market;
  const spec = getOICapability(market);
  const asOfMs = readAsOfMs(input);
  const nowMs = finite(options.nowMs ?? Date.now());
  const maxAgeMs = finite(options.maxAgeMs ?? DEFAULT_OI_MAX_AGE_MS);

  if (!spec) {
    return errorRow(market, asOfMs, null, 'oi_not_supported',
      `open interest is not supported for ${market || 'unknown market'}`);
  }
  if (input.error || input.error_code || input.status === 'error') {
    return errorRow(market, asOfMs, spec.native_unit,
      input.error_code || 'source_error', String(input.error || input.error_message || 'source error'));
  }
  if (asOfMs === null || !Number.isFinite(nowMs) || !Number.isFinite(maxAgeMs) || maxAgeMs < 0) {
    return errorRow(market, asOfMs, spec.native_unit, 'invalid_timestamp', 'valid timestamp and maxAgeMs are required');
  }

  const oiNative = readNativeValue(input);
  const oiBtcSource = finite(input.oi_btc ?? input.oiCcy);
  const oiUsdSource = finite(input.oi_usd ?? input.oiUsd);
  const priceUsd = readPriceUsd(input);

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
    // Source provides both BTC and USD notional values. Trust them, but
    // validate consistency against the mark price when one is available.
    if (priceUsd !== null && priceUsd > 0) {
      const expectedUsd = oiBtcSource * priceUsd;
      const tolerance = finite(options.mismatchTolerance) ?? 0.01;
      const denom = Math.max(Math.abs(oiUsdSource), 1e-9);
      if (Math.abs(expectedUsd - oiUsdSource) / denom > tolerance) {
        return errorRow(market, asOfMs, spec.native_unit, 'oi_mismatch',
          `oiCcy/oi_btc (${oiBtcSource} BTC) and oiUsd/oi_usd (${oiUsdSource} USD) are inconsistent at mark price ${priceUsd}`);
      }
    }
    oiBtc = oiBtcSource;
    oiUsd = oiUsdSource;
  } else if (oiBtcSource !== null) {
    // Only BTC notional provided: derive USD notional from mark price.
    if (priceUsd === null || priceUsd <= 0) {
      return errorRow(market, asOfMs, spec.native_unit, 'missing_price',
        'positive price_usd/mark_price is required to derive USD notional');
    }
    oiBtc = oiBtcSource;
    oiUsd = oiBtcSource * priceUsd;
  } else if (oiUsdSource !== null) {
    // Only USD notional provided: derive BTC notional from mark price.
    if (priceUsd === null || priceUsd <= 0) {
      return errorRow(market, asOfMs, spec.native_unit, 'missing_price',
        'positive price_usd/mark_price is required to derive BTC notional');
    }
    oiUsd = oiUsdSource;
    oiBtc = oiUsdSource / priceUsd;
  } else {
    // No source BTC/USD notional values: fall back to capability-defined
    // conversion from the native unit. This path keeps BitMEX etc. working.
    if (priceUsd === null || priceUsd <= 0) {
      return errorRow(market, asOfMs, spec.native_unit, 'missing_price',
        'positive price_usd/mark_price is required for BTC and USD normalization');
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
      return errorRow(market, asOfMs, spec.native_unit, 'unsupported_native_unit',
        `cannot convert native unit ${spec.native_unit} without source oiCcy/oiUsd`);
    }
  }

  const ageMs = nowMs - asOfMs;
  if (ageMs < 0) {
    return errorRow(market, asOfMs, spec.native_unit, 'future_sample', 'sample timestamp is after the as-of clock');
  }
  return {
    schema: OI_SCHEMA,
    market,
    instrument_type: spec.instrument_type,
    status: ageMs <= maxAgeMs ? 'fresh' : 'stale',
    as_of_ms: asOfMs,
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

function sampleTime(sample, timeField) {
  return finite(sample?.[timeField] ?? sample?.as_of_ms ?? sample?.timestamp_ms ?? sample?.ts);
}

function latestAtOrBefore(samples, anchorMs, timeField) {
  const ordered = samples
    .map((sample, index) => ({ sample, index, time: sampleTime(sample, timeField) }))
    .filter(entry => entry.time !== null && entry.time <= anchorMs)
    .sort((a, b) => a.time - b.time || a.index - b.index);
  return ordered.length ? ordered[ordered.length - 1].sample : null;
}

/**
 * Return the latest OI sample at or before anchorMs.
 * A stale candidate is returned as status=stale, but callers should not use
 * its values as current data.
 */
export function openInterestAsOf(samples, anchorMs, options = {}) {
  const time = finite(anchorMs);
  if (time === null) {
    return errorRow(null, null, null, 'invalid_anchor', 'anchor timestamp must be finite');
  }
  const candidate = latestAtOrBefore(samples || [], time, options.timeField || 'as_of_ms');
  if (!candidate) return errorRow(null, null, null, 'no_asof_sample', 'no OI sample at or before anchor');
  return normalizeOpenInterest(candidate, { ...options, nowMs: time });
}

export const asOfOpenInterest = openInterestAsOf;

/**
 * Join normalized OI onto anchor rows without forward-filling stale values.
 * The output keeps the reason/status while setting `open_interest` to null
 * unless the as-of sample is fresh.
 */
export function joinOpenInterestAsOf(anchorRows, oiSamples, options = {}) {
  const anchorTimeField = options.anchorTimeField || 'ts';
  const outputField = options.outputField || 'open_interest';
  return (anchorRows || []).map(anchor => {
    const anchorMs = finite(anchor?.[anchorTimeField]);
    const joined = openInterestAsOf(oiSamples, anchorMs, options);
    const usable = joined.status === 'fresh' ? joined : null;
    return {
      ...anchor,
      [outputField]: usable,
      open_interest_status: joined.status,
      open_interest_as_of_ms: joined.as_of_ms,
    };
  });
}

export const asOfJoinOpenInterest = joinOpenInterestAsOf;
