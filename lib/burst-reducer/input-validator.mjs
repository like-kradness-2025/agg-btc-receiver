// lib/burst-reducer/input-validator.mjs — Validate and parse 30s trade block JSONL
// Follows plan Task 3 + P0 raw/aux distinction

import { createHash } from 'node:crypto';
import { existsSync, statSync, readFileSync } from 'node:fs';

/**
 * File classification for raw/aux input (P0: absent vs valid-empty vs invalid).
 * @typedef {'absent'|'valid-empty'|'valid-nonempty'|'invalid'|'partial'} FileClass
 */

/**
 * Classify a raw/aux input file.
 * @param {string} filePath - absolute path to the input file
 * @param {number} blockStartMs - expected block start (for context in errors)
 * @returns {{ class: FileClass, reason?: string, size?: number }}
 */
export function classifyInputFile(filePath, blockStartMs) {
  if (!existsSync(filePath)) {
    return { class: 'absent', reason: `file not found: ${filePath}` };
  }

  let stat;
  try {
    stat = statSync(filePath);
  } catch (e) {
    return { class: 'invalid', reason: `cannot stat: ${e.message}` };
  }

  if (!stat.isFile()) {
    return { class: 'invalid', reason: `not a regular file: ${filePath}` };
  }

  if (stat.size === 0) {
    // Empty file = valid-empty (zero-volume block is a valid observation)
    return { class: 'valid-empty', size: 0 };
  }

  let content;
  try {
    content = readFileSync(filePath, 'utf8');
  } catch (e) {
    return { class: 'invalid', reason: `cannot read file: ${e.message}`, size: stat.size };
  }

  const trimmed = content.trim();
  if (trimmed.length === 0) {
    return { class: 'valid-empty', size: stat.size };
  }

  // Try parsing first line to detect partial/corrupt
  try {
    JSON.parse(trimmed.split('\n')[0]);
  } catch (e) {
    return { class: 'invalid', reason: `first line not valid JSON: ${e.message}`, size: stat.size };
  }

  // Check for trailing partial line (no newline at end while having content)
  if (content.length > 0 && !content.endsWith('\n') && trimmed.length > 0) {
    return { class: 'partial', reason: 'file may be truncated (no trailing newline)', size: stat.size };
  }

  return { class: 'valid-nonempty', size: stat.size };
}

/**
 * Validate and parse a 30s block of trades JSONL.
 * §4.2: E004 no longer throws — counts inversions and stable-sorts by ts.
 *
 * @param {string} blockContent - raw JSONL string
 * @param {number} blockStartMs - block start ms (30s boundary)
 * @returns {{ trades: Array, inputSha256: string, reordered_input: boolean, timestamp_inversion_count: number }}
 * @throws {Error} on E001/E002/E003/E005 (still fail-closed)
 */
export function validateAndParseTrades(blockContent, blockStartMs) {
  const blockEndMs = blockStartMs + 30000;
  const rawTrades = [];
  const lines = blockContent.trim().split('\n');
  let timestamp_inversion_count = 0;
  let prevTs = -Infinity;

  for (let lineIdx = 0; lineIdx < lines.length; lineIdx++) {
    const line = lines[lineIdx];
    if (!line) continue;

    let obj;
    try {
      obj = JSON.parse(line);
    } catch (e) {
      throw new Error(`E001: JSON parse error at line ${lineIdx + 1}: ${e.message}`);
    }

    // E002: required fields
    if (obj.ts === undefined || obj.side === undefined || obj.price === undefined || obj.qty === undefined) {
      throw new Error(`E002: missing required field at line ${lineIdx + 1}. Got: ${Object.keys(obj).join(',')}`);
    }

    const ts = Number(obj.ts);
    const price = Number(obj.price);
    const qty = Number(obj.qty);
    const side = String(obj.side);

    // E003: value range
    if (price <= 0 || !isFinite(price)) throw new Error(`E003: invalid price ${obj.price} at line ${lineIdx + 1}`);
    if (qty <= 0 || !isFinite(qty)) throw new Error(`E003: invalid qty ${obj.qty} at line ${lineIdx + 1}`);
    if (side !== 'buy' && side !== 'sell') throw new Error(`E003: invalid side "${side}" at line ${lineIdx + 1}`);

    // §4.2: E004 — count inversions instead of throwing
    if (ts < prevTs) {
      timestamp_inversion_count++;
    }
    prevTs = ts;

    // E005: block range
    if (ts < blockStartMs || ts >= blockEndMs) {
      throw new Error(`E005: ts ${ts} outside block [${blockStartMs}, ${blockEndMs}) at line ${lineIdx + 1}`);
    }

    rawTrades.push({
      ts,
      price,
      qty,
      side,
      _idx: lineIdx,
      tradeId: obj.tradeId || undefined,
      market: obj.market || 'unknown',
    });
  }

  // §4.2: Stable sort by ts ASC only — preserve original row order for ties.
  // tradeId does NOT affect same-ts ordering.
  rawTrades.sort((a, b) => {
    if (a.ts !== b.ts) return a.ts - b.ts;
    return a._idx - b._idx;
  });

  const reordered_input = timestamp_inversion_count > 0;
  const inputSha256 = computeSha256(blockContent);

  return { trades: rawTrades, inputSha256, reordered_input, timestamp_inversion_count };
}

function computeSha256(str) {
  return createHash('sha256').update(str).digest('hex');
}
