// @deprecated — lib/burst-reducer/agg-trades-reader.mjs
// This module is no longer referenced by any production code path (post receiver
// simplification). It was originally used to build a per-second traded notional
// lookup from agg_trades data for burst reducer #12 denominator calculation.
// Retained for historical reference only — do not import in new code.
// Follows plan Task 6

/**
 * Build per-second traded notional lookup for #12 denominator.
 * Authoritative agg JSON columns: ts, volume, vwap.
 * Notional = volume * vwap.
 *
 * @param {Object} aggResult - output of validateAggLookback: { aggRows, coverageComplete }
 * @param {number} blockStartMs - N's block start ms
 * @returns {Map<number,number>} secondTs → sum(notional) for [secondTs-30000, secondTs)
 * @throws {Error} E007 if coverage incomplete
 */
export function buildTradedNotionalLookup(aggResult, blockStartMs) {
  // FAIL CLOSED: verify coverageComplete before making map
  if (!aggResult.coverageComplete) {
    throw new Error('E007: agg_trades lookback coverage is incomplete — cannot build notional lookup');
  }

  const lookup = new Map();
  const aggRows = aggResult.aggRows;

  // Validate each agg row: finite nonnegative volume, finite positive vwap
  for (const row of aggRows) {
    if (!isFinite(row.volume) || row.volume < 0) throw new Error(`E007: invalid agg volume ${row.volume}`);
    if (!isFinite(row.vwap) || row.vwap <= 0) throw new Error(`E007: invalid agg vwap ${row.vwap}`);
    row._notional = row.volume * row.vwap;
  }

  // Generate ALL 30 secondTs keys for block N
  for (let s = blockStartMs; s < blockStartMs + 30000; s += 1000) {
    let sumNotional = 0;
    // Sum rows where secondTs-30000 <= row.ts < secondTs
    for (const row of aggRows) {
      if (s - 30000 <= row.ts && row.ts < s) {
        sumNotional += row._notional;
      }
    }
    lookup.set(s, sumNotional);
  }

  return lookup;
}
