/**
 * Set 8: the auxiliary (open interest / mark / funding) REST poller.
 *
 * v1 fetched these on a 30 second clock (`lib/derivatives-helper.mjs:DEFAULT_INTERVAL_MS`, the
 * `_tick` loop) and wrote one `open_interest` row per market per tick. This is the same idea, owned
 * by the ingest process - the single writer of the market's raw (`src/raw.mjs`) - so the row lands in
 * the market's `raw_batches` beside the frames.
 *
 * The venues with a self-contained WS channel (Bybit `tickers`, OKX `open-interest`+`funding-rate`+
 * `mark-price`, Hyperliquid `activeAssetCtx`) get their sample from the socket, so this poller is the
 * one that covers the venues whose only source is REST (Binance's `premiumIndex` + `openInterest`,
 * and Hyperliquid's `metaAndAssetCtxs` as a complement). The fetch logic is v1's, per market.
 *
 * Failure is loud, like the raw itself: a tick that cannot fetch or cannot hand the row to the raw
 * reports to `onError`, and the owner stops reception. A poller that silently skipped a tick would
 * leave a run that looks healthy while the auxiliary stream has holes.
 */

import { openInterestRecord } from './oi.mjs';

export const DEFAULT_OI_POLL_INTERVAL_MS = 30_000;
export const DEFAULT_OI_FETCH_TIMEOUT_MS = 10_000;

function finite(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function responseTimestamp(value) {
  const timestamp = finite(value);
  if (timestamp !== null) return timestamp;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === '') return null;
    if (/^\d+(\.\d+)?$/.test(trimmed)) {
      const parsed = Number(trimmed);
      return Number.isFinite(parsed) ? parsed : null;
    }
    const parsed = Date.parse(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

/** One GET the way v1's `_fetchJson` did: any non-ok answer is an error, not a partial sample. */
async function fetchJson(fetchImpl, url, options = {}, timeoutMs = DEFAULT_OI_FETCH_TIMEOUT_MS) {
  if (!url) throw new Error('missing REST URL');
  const response = await fetchImpl(url, { signal: AbortSignal.timeout(timeoutMs), ...options });
  if (!response?.ok) throw new Error(`HTTP ${response?.status ?? 'unknown'} from ${url}`);
  return response.json();
}

/**
 * The per-market REST sample builders, v1's `_fetchBinancePerp` / `_fetchBybit` / `_fetchOkx` /
 * `_fetchHyperliquid` (`lib/derivatives-helper.mjs:232-399`). Each returns
 * `{ open_interest, oiCcy?, oiUsd?, mark_price, funding_rate, next_funding_time, source_ts, error? }`.
 */
export const REST_OI_SOURCES = Object.freeze({
  async binance_perp(fetchImpl, { symbol = 'BTCUSDT', apiRoot = 'https://fapi.binance.com/fapi/v1' } = {}) {
    const now = Date.now();
    let markPrice = null;
    let fundingRate = null;
    let nextFundingTime = null;
    let openInterest = null;
    let sourceTs = null;
    let oiError = null;
    try {
      const d = await fetchJson(fetchImpl, `${apiRoot}/premiumIndex?symbol=${symbol}`);
      markPrice = finite(d.markPrice);
      fundingRate = finite(d.lastFundingRate);
      nextFundingTime = finite(d.nextFundingTime);
      sourceTs = responseTimestamp(d.time);
    } catch {
      // OI remains usable if only premiumIndex failed.
    }
    try {
      const d = await fetchJson(fetchImpl, `${apiRoot}/openInterest?symbol=${symbol}`);
      openInterest = finite(d.openInterest);
      sourceTs = responseTimestamp(d.time) ?? sourceTs ?? now;
    } catch (error) {
      oiError = error;
    }
    return {
      mark_price: markPrice,
      funding_rate: fundingRate,
      open_interest: openInterest,
      next_funding_time: nextFundingTime,
      source_ts: oiError ? null : sourceTs ?? now,
      error: oiError?.message ?? null,
      error_code: oiError ? 'source_error' : null,
    };
  },

  async binance_perp_btcusdc(fetchImpl, options = {}) {
    return REST_OI_SOURCES.binance_perp(fetchImpl, { symbol: 'BTCUSDC', ...options });
  },

  async bybit_perp(fetchImpl, { symbol = 'BTCUSDT', url = 'https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT' } = {}) {
    const now = Date.now();
    try {
      const d = await fetchJson(fetchImpl, url);
      if (!d.result?.list?.length) throw new Error('Bybit ticker response has no BTCUSDT row');
      const t = d.result.list[0];
      return {
        mark_price: finite(t.markPrice),
        funding_rate: finite(t.fundingRate),
        open_interest: finite(t.openInterest),
        next_funding_time: finite(t.nextFundingTime),
        source_ts: responseTimestamp(d.time) ?? now,
      };
    } catch (error) {
      return { error: error.message, error_code: 'source_error', source_ts: null };
    }
  },

  async okx_perp(fetchImpl, { instId = 'BTC-USDT-SWAP', root = 'https://www.okx.com/api/v5' } = {}) {
    const now = Date.now();
    let markPrice = null;
    let fundingRate = null;
    let nextFundingTime = null;
    let openInterest = null;
    let oiCcy = null;
    let oiUsd = null;
    let sourceTs = null;
    let oiError = null;
    try {
      const d = await fetchJson(fetchImpl, `${root}/public/funding-rate?instId=${instId}`);
      const row = d.data?.[0];
      if (row) {
        fundingRate = finite(row.fundingRate);
        nextFundingTime = finite(row.fundingTime);
      }
    } catch {
      // Funding is optional; keep OI independent.
    }
    try {
      const d = await fetchJson(fetchImpl, `${root}/public/open-interest?instType=SWAP&instId=${instId}`);
      const row = d.data?.[0];
      if (!row) throw new Error('OKX open-interest response has no row');
      openInterest = finite(row.oi);
      oiCcy = finite(row.oiCcy);
      oiUsd = finite(row.oiUsd);
      sourceTs = responseTimestamp(row.ts) ?? now;
    } catch (error) {
      oiError = error;
    }
    try {
      const d = await fetchJson(fetchImpl, `${root}/public/mark-price?instType=SWAP&instId=${instId}`);
      markPrice = finite(d.data?.[0]?.markPx);
    } catch {
      // Mark is optional.
    }
    return {
      mark_price: markPrice,
      funding_rate: fundingRate,
      open_interest: openInterest,
      oiCcy,
      oiUsd,
      next_funding_time: nextFundingTime,
      source_ts: oiError ? null : sourceTs ?? now,
      error: oiError?.message ?? null,
      error_code: oiError ? 'source_error' : null,
    };
  },

  async hyperliquid_perp(fetchImpl, { url = 'https://api.hyperliquid.xyz/info' } = {}) {
    const now = Date.now();
    try {
      const d = await fetchJson(fetchImpl, url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'metaAndAssetCtxs' }),
      });
      if (!Array.isArray(d) || d.length < 2 || !Array.isArray(d[1]) || !d[1][0]) {
        throw new Error('Hyperliquid response has no BTC asset context');
      }
      const btc = d[1][0];
      return {
        mark_price: finite(btc.markPx),
        funding_rate: finite(btc.funding),
        open_interest: finite(btc.openInterest),
        next_funding_time: null,
        source_ts: now,
      };
    } catch (error) {
      return { error: error.message, error_code: 'source_error', source_ts: null };
    }
  },
});

/** Whether this market has a REST sample builder (the poller can run for it). */
export function hasRestOiSource(market) {
  return typeof REST_OI_SOURCES[market] === 'function';
}

/**
 * The poller. It owns one interval timer; each tick fetches the market's sample, builds one
 * `open_interest` record with `onSample` (a fake fetch and a captured clock in tests) and hands it to
 * `append`. A failure anywhere in the tick is reported to `onError` - never swallowed.
 */
export function createAuxCollector({
  market,
  fetchImpl = globalThis.fetch,
  intervalMs = DEFAULT_OI_POLL_INTERVAL_MS,
  nowMs = () => Date.now(),
  setTimer = setInterval,
  clearTimer = clearInterval,
  append,
  sourceOptions = {},
  onError = null,
} = {}) {
  const source = REST_OI_SOURCES[market];
  if (typeof source !== 'function') throw new TypeError(`no REST open-interest source for market ${market}`);
  if (typeof append !== 'function') throw new TypeError('the auxiliary collector needs an append sink');
  if (!Number.isFinite(intervalMs) || intervalMs <= 0) throw new TypeError('intervalMs must be a positive number of milliseconds');

  let timer = null;
  let closed = false;
  let stopped = false;
  let inFlight = false;

  async function tick() {
    if (closed || stopped || inFlight) return undefined;
    inFlight = true;
    try {
      const sample = await source(fetchImpl, sourceOptions);
      // A stop can land while the fetch is in flight: nothing may be appended past it.
      if (closed || stopped) return undefined;
      // A sample that could not be fetched is a loud failure, not a silent error row: the raw's own
      // discipline is that a run which cannot record what it should must stop rather than look healthy.
      if (sample?.error || sample?.error_code) {
        throw new Error(sample.error || sample.error_code || 'source error');
      }
      const now = nowMs();
      const record = openInterestRecord({ market, sample, ts: now, nowMs: now });
      append(record);
      return record;
    } catch (error) {
      if (typeof onError === 'function') onError(error);
      else throw error;
      return undefined;
    } finally {
      inFlight = false;
    }
  }

  return {
    start() {
      if (timer !== null || closed || stopped) return false;
      timer = setTimer(() => {
        // A tick that throws synchronously is a programming error; anything it would report is
        // already the `onError` path inside `tick`. Returning the promise lets a test drive one tick
        // and await it.
        return tick();
      }, intervalMs);
      if (typeof timer?.unref === 'function') timer.unref();
      return true;
    },
    /** Run one tick now (used by tests and by nothing else in production). */
    tick,
    stop() {
      // A stop is terminal: the poller is only ever stopped when the reception is fenced or the
      // process is closing, and a tick already in flight must not write after that fence either.
      stopped = true;
      if (timer !== null) {
        clearTimer(timer);
        timer = null;
      }
    },
    close() {
      this.stop();
      closed = true;
    },
    get running() {
      return timer !== null && !closed;
    },
  };
}
