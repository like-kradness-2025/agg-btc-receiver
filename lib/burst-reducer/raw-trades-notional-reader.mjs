// lib/burst-reducer/raw-trades-notional-reader.mjs — Read raw trades and build #12 notional lookup
// Follows plan Task 1: raw trades直計算へ移す

import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';

const BLOCK_DURATION_MS = 30000;

/**
 * Compute sha256 of file content.
 */
function sha256File(path) {
  if (!existsSync(path)) return null;
  return createHash('sha256').update(readFileSync(path, 'utf8')).digest('hex');
}

/**
 * Format a block start ms into date and time path components:
 *   date: YYYY-MM-DD
 *   time: HH-MM-SS
 */
function blockPathParts(blockStartMs) {
  const d = new Date(blockStartMs);
  const dateStr = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return { dateStr, time: `${hh}-${mm}-${ss}` };
}

/**
 * Build a relative path for a raw trade block.
 */
function rawTradeBlockPath(market, blockStartMs) {
  const { dateStr, time } = blockPathParts(blockStartMs);
  return `trades/${market}/${dateStr}/${time}.jsonl`;
}

/**
 * Validate raw trade lookback blocks intersecting [targetBlockStartMs-30000, targetBlockStartMs+30000).
 *
 * Reads the two 30s blocks covering:
 *   - lookback block: [targetBlockStartMs - 30000, targetBlockStartMs)
 *   - target block:   [targetBlockStartMs, targetBlockStartMs + 30000)
 *
 * Absent blocks are valid-empty — their trades contribute zero and their block
 * start is recorded in `assumedEmptyBlockStarts`. Existing malformed content
 * still throws E007.
 *
 * @param {string} dataDir - root data directory
 * @param {string} market - market name
 * @param {number} targetBlockStartMs - target block start (30s-aligned)
 * @returns {{ coverageComplete: boolean, missing: string[], hashes: Record<string,string>, trades: Array<{ts:number, price:number, qty:number}>, assumedEmptyBlockStarts: number[] }}
 * @throws {Error} E007 on malformed lines, non-finite price/qty, or invalid ts (non-finite, non-integer, out of block range)
 */
export function validateRawTradeLookback(dataDir, market, targetBlockStartMs) {
  const requiredStarts = [
    targetBlockStartMs - BLOCK_DURATION_MS,  // lookback block
    targetBlockStartMs,                       // target block
  ];

  const hashes = {};
  const allTrades = [];
  const missing = [];
  const assumedEmptyBlockStarts = [];

  for (const bs of requiredStarts) {
    const relPath = rawTradeBlockPath(market, bs);
    const fullPath = join(dataDir, relPath);

    if (!existsSync(fullPath)) {
      // Absent block is valid-empty — record it and skip (zero contribution)
      assumedEmptyBlockStarts.push(bs);
      continue;
    }

    const content = readFileSync(fullPath, 'utf8');
    const hash = createHash('sha256').update(content).digest('hex');
    hashes[String(bs)] = hash;

    const lines = content.trim().split('\n').filter(l => l);
    for (let lineIdx = 0; lineIdx < lines.length; lineIdx++) {
      const line = lines[lineIdx];
      let obj;
      try {
        obj = JSON.parse(line);
      } catch (e) {
        throw new Error(`E007: malformed JSON in ${relPath} at line ${lineIdx + 1}: ${e.message}`);
      }

      const ts = Number(obj.ts);
      const price = Number(obj.price);
      const qty = Number(obj.qty);

      // Validate ts: must be finite, integer, and within block range.
      // Explicit null/undefined check: Number(null)=0, Number(undefined)=NaN
      if (obj.ts === null || obj.ts === undefined) {
        throw new Error(`E007: invalid ts ${obj.ts} in ${relPath} at line ${lineIdx + 1} (expected finite integer)`);
      }
      if (!Number.isFinite(ts) || !Number.isInteger(ts)) {
        throw new Error(`E007: invalid ts ${obj.ts} in ${relPath} at line ${lineIdx + 1} (expected finite integer)`);
      }
      if (ts < bs || ts >= bs + BLOCK_DURATION_MS) {
        throw new Error(`E007: ts ${ts} out of range [${bs}, ${bs + BLOCK_DURATION_MS}) in ${relPath} at line ${lineIdx + 1}`);
      }

      if (!isFinite(price) || price <= 0) {
        throw new Error(`E007: non-finite/invalid price ${obj.price} in ${relPath} at line ${lineIdx + 1}`);
      }
      if (!isFinite(qty) || qty <= 0) {
        throw new Error(`E007: non-finite/invalid qty ${obj.qty} in ${relPath} at line ${lineIdx + 1}`);
      }

      allTrades.push({ ts, price, qty });
    }
  }

  // coverageComplete is always true — absent blocks are valid-empty (zero contribution)
  return { coverageComplete: true, missing, hashes, trades: allTrades, assumedEmptyBlockStarts };
}

/**
 * Build per-second traded notional lookup for #12 denominator.
 *
 * For each secondTs s in [blockStartMs, blockStartMs+30000):
 *   denom = Σ price * qty for raw trades where s-30000 <= trade.ts < s
 *
 * Valid-empty files (0 trades) produce denom=0 for all 30 seconds.
 * Absent raw blocks are treated as valid-empty (zero contribution).
 * Malformed rows in existing files throw E007 (via validateRawTradeLookback).
 *
 * @param {string} dataDir - root data directory
 * @param {string} market - market name
 * @param {number} blockStartMs - N's block start ms (30s-aligned)
 * @returns {Map<number,number>} secondTs → sum(price*qty) for [secondTs-30000, secondTs)
 */
export function buildRawTradedNotionalLookup(dataDir, market, blockStartMs) {
  // validateRawTradeLookback no longer returns false for absent blocks;
  // absent blocks contribute zero and are recorded in assumedEmptyBlockStarts.
  const validated = validateRawTradeLookback(dataDir, market, blockStartMs);

  const lookup = new Map();
  const trades = validated.trades;

  // Generate ALL 30 secondTs keys for block N
  for (let s = blockStartMs; s < blockStartMs + BLOCK_DURATION_MS; s += 1000) {
    let sumNotional = 0;
    const windowStart = s - BLOCK_DURATION_MS; // inclusive
    const windowEnd = s;                        // exclusive

    for (const t of trades) {
      if (t.ts >= windowStart && t.ts < windowEnd) {
        sumNotional += t.price * t.qty;
      }
    }
    lookup.set(s, sumNotional);
  }

  return lookup;
}
