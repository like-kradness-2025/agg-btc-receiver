// lib/burst-reducer/block-scanner.mjs — Scan 30s block files for trades or book_updates
// Follows plan Task 4 + P0-1 kind parameterization

import { readdirSync, existsSync } from 'node:fs';
import { join } from 'node:path';

/**
 * @typedef {{ ms: number, fullPath: string, market: string, date: string }} BlockInfo
 */

/**
 * Scan a kind-specific directory for 30s blocks in the given time range.
 * @param {string} dataDir - 'data/live_v3'
 * @param {string} kind - 'trades' or 'book_updates'
 * @param {string} market
 * @param {number} fromMs - epoch ms (30s boundary)
 * @param {number} toMs - epoch ms (30s boundary)
 * @returns {BlockInfo[]} blocks sorted ascending by start ms
 */
export function scanBlocks(dataDir, kind, market, fromMs, toMs) {
  const kindDir = join(dataDir, kind, market);
  if (!existsSync(kindDir)) return [];

  const blocks = [];
  const dateDirs = readdirSync(kindDir).filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d));

  for (const dateDir of dateDirs) {
    const datePath = join(kindDir, dateDir);
    const timeFiles = readdirSync(datePath).filter(f => f.endsWith('.jsonl'));

    for (const tf of timeFiles) {
      const base = tf.replace('.jsonl', '');
      const [h, m, s] = base.split('-').map(Number);
      if (isNaN(h) || isNaN(m) || isNaN(s)) continue;

      // E006: 00/30 boundary check (quarantine/fail, no skip+warn)
      if (s !== 0 && s !== 30) {
        throw new Error(`E006: filename not on 00/30 boundary: ${tf} (seconds=${s})`);
      }

      const fileMs = Date.UTC(
        parseInt(dateDir.slice(0, 4)), parseInt(dateDir.slice(5, 7)) - 1, parseInt(dateDir.slice(8, 10)),
        h, m, s
      );

      if (fileMs < toMs && fileMs + 30000 > fromMs) {
        blocks.push({ ms: fileMs, fullPath: join(datePath, tf), market, date: dateDir });
      }
    }
  }

  blocks.sort((a, b) => a.ms - b.ms);
  return blocks;
}

/**
 * Scan trades directory for 30s blocks in the given time range.
 * Backward-compatible wrapper calling scanBlocks(dataDir, 'trades', ...).
 * @param {string} dataDir
 * @param {string} market
 * @param {number} fromMs
 * @param {number} toMs
 * @returns {BlockInfo[]}
 */
export function scanTradeBlocks(dataDir, market, fromMs, toMs) {
  return scanBlocks(dataDir, 'trades', market, fromMs, toMs);
}

/**
 * Scan book_updates directory for 30s blocks in the given time range.
 * @param {string} dataDir
 * @param {string} market
 * @param {number} fromMs
 * @param {number} toMs
 * @returns {BlockInfo[]}
 */
export function scanBookUpdateBlocks(dataDir, market, fromMs, toMs) {
  return scanBlocks(dataDir, 'book_updates', market, fromMs, toMs);
}
