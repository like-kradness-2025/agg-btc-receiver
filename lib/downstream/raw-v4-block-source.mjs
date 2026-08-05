// ⚠️  LEGACY — Virtual 30s block source over v4 hourly segments.  The live
//    production pipeline uses SQLite-based block reading, not v4 segments.
//    See docs/current/canonical-pipeline.md for the canonical architecture.
//
// lib/downstream/raw-v4-block-source.mjs — Virtual 30s block source over v4 hourly append segments.
// Groups raw_v4 envelopes into 30s windows in memory. Does not regenerate 30s raw files.
// Tracks byte cursors per segment for resume and output proof.

import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { access, stat } from 'node:fs/promises';
import path from 'node:path';
import {
  RawV4SegmentReader,
  RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION,
} from './raw-v4-segment-reader.mjs';
import { validateAndParseTrades } from '../burst-reducer/input-validator.mjs';
import { toCanonicalBookEnvelope } from '../book-updates-adapter.mjs';

export const RAW_V4_BLOCK_SOURCE_SCHEMA_VERSION = 'raw_v4_block_source_v1';
const BLOCK_DURATION_MS = 30000;

function blockStartMs(eventTsMs) {
  return Math.floor(eventTsMs / BLOCK_DURATION_MS) * BLOCK_DURATION_MS;
}

function formatDate(blockStartMs) {
  const d = new Date(blockStartMs);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

function sha256Content(content) {
  return createHash('sha256').update(content).digest('hex');
}

async function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = createHash('sha256');
    const stream = createReadStream(filePath);
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

async function sha256FilePrefix(filePath, prefixSize) {
  if (!Number.isSafeInteger(prefixSize) || prefixSize < 0) {
    throw new TypeError(`invalid raw_v4 prefix size: ${prefixSize}`);
  }
  if (prefixSize === 0) return sha256Content('');
  return new Promise((resolve, reject) => {
    const hash = createHash('sha256');
    let bytesRead = 0;
    const stream = createReadStream(filePath, { start: 0, end: prefixSize - 1 });
    stream.on('data', (chunk) => {
      bytesRead += chunk.length;
      hash.update(chunk);
    });
    stream.on('end', () => {
      if (bytesRead !== prefixSize) {
        reject(new Error(`raw_v4 source prefix truncated: expected ${prefixSize}, got ${bytesRead}`));
        return;
      }
      resolve(hash.digest('hex'));
    });
    stream.on('error', reject);
  });
}

async function fileSize(filePath) {
  const s = await stat(filePath);
  return s.size;
}

async function pathExists(filePath) {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

function emptyBlock({ ms, market }) {
  return {
    ms,
    fullPath: `__raw_v4__:${market}:${ms}`,
    market,
    date: formatDate(ms),
    content: '',
    _contentParts: [],
    sha256: sha256Content(''),
    trades: [],
    segmentProof: null,
    active: false,
    v4Cursor: null,
    reordered_input: false,
    timestamp_inversion_count: 0,
  };
}

function logicalPath(root, filePath) {
  return path.relative(root, filePath).replaceAll(path.sep, '/').replace(/\.active$/, '');
}

function canonicalBookPayload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return false;
  const raw = payload.schema_version === 'book_updates_v1'
    ? { ...payload, ts: payload.event_ts_ms }
    : payload;
  return toCanonicalBookEnvelope(raw).valid;
}

function parseCursor(cursor) {
  if (cursor == null) return null;
  if (typeof cursor === 'string') {
    try {
      return JSON.parse(cursor);
    } catch {
      return null;
    }
  }
  if (typeof cursor === 'object' && cursor.schema_version === RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION) {
    return cursor;
  }
  return null;
}

export class RawV4BlockSource {
  constructor({ root, kind, market, cursor = null } = {}) {
    if (!root || !kind || !market) throw new TypeError('root, kind and market are required');
    this.root = root;
    this.kind = kind;
    this.market = market;
    this.cursor = parseCursor(cursor);
    this.reader = null;
    this.blocks = new Map();
    this.activeSegment = false;
    this.segmentInfoCache = new Map();
  }

  async open() {
    this.reader = new RawV4SegmentReader({
      root: this.root,
      kind: this.kind,
      market: this.market,
      cursor: this.cursor,
    });
    await this.reader.open();
  }

  async close() {
    if (this.reader) {
      await this.reader.close();
      this.reader = null;
    }
  }

  /**
   * Read segments and group envelopes into 30s virtual blocks.
   * @param {Object} opts
   * @param {number} opts.fromMs - inclusive lower bound (30s-aligned)
   * @param {number} opts.toMs - exclusive upper bound (30s-aligned)
   * @returns {{ blocks: Array, activeBlockMs: number|null, cursor: object|null }}
   */
  async loadBlocks({ fromMs, toMs }) {
    this.blocks = new Map();
    this.activeSegment = false;
    this.segmentInfoCache.clear();

    let prevCursor = this.cursor || { schema_version: RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION, date: '', segment: '', byte_offset: 0 };
    let lastCursor = prevCursor;
    const blockStartCursors = new Map();

    while (true) {
      const result = await this.reader.read({ maxRecords: 1, maxBytes: 64 * 1024 });
      lastCursor = result.cursor ? JSON.parse(result.cursor) : lastCursor;

      if (this.kind === 'book_updates' && this.reader.getInvalidRecords().length > 0) {
        const error = this.reader.getInvalidRecords()[0];
        throw new Error(`E041: invalid raw_v4 book_updates record at byte ${error.byte_offset}: ${error.reason}`);
      }

      if (result.records.length === 0) {
        if (result.done) {
          this.activeSegment = false;
          break;
        }
        if (result.eof) {
          this.activeSegment = true;
          break;
        }
        prevCursor = lastCursor;
        continue;
      }

      const record = result.records[0];
      const recordStartOffset = record._meta.byte_offset;
      const recordEndOffset = record._meta.byte_offset + record._meta.byte_length;
      const segmentLogicalId = record._meta.logical_id;
      const recordCursor = {
        schema_version: RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION,
        date: record._meta.logical_id.split('/')[0],
        segment: record._meta.logical_id.split('/')[1],
        byte_offset: recordStartOffset,
      };

      const ms = blockStartMs(record.event_ts_ms);

      if (this.kind === 'book_updates' && !canonicalBookPayload(record.payload)) {
        throw new Error(`E041: invalid raw_v4 book_updates payload at ${record._meta.logical_id}:${record._meta.byte_offset}`);
      }
      if (!blockStartCursors.has(ms)) blockStartCursors.set(ms, recordCursor);

      if (ms >= fromMs && ms < toMs) {
        let block = this.blocks.get(ms);
        if (!block) {
          block = emptyBlock({ ms, market: this.market });
          block.segmentProof = [{
            segmentLogicalId,
            byteOffsetStart: recordStartOffset,
            byteOffsetEnd: recordEndOffset,
          }];
          block.v4Cursor = { ...recordCursor };
          this.blocks.set(ms, block);
        } else {
          const lastRange = block.segmentProof[block.segmentProof.length - 1];
          if (lastRange.segmentLogicalId === segmentLogicalId) {
            lastRange.byteOffsetEnd = recordEndOffset;
          } else {
            block.segmentProof.push({
              segmentLogicalId,
              byteOffsetStart: recordStartOffset,
              byteOffsetEnd: recordEndOffset,
            });
          }
        }
        block._contentParts.push(`${JSON.stringify(record.payload)}\n`);
      }

      prevCursor = lastCursor;
    }

    // Compute sha256 and parse trades for materialized blocks.
    for (const block of this.blocks.values()) {
      block.content = block._contentParts.join('');
      delete block._contentParts;
      block.sha256 = sha256Content(block.content);
      if (this.kind === 'trades') {
        const parsed = this._parseTrades(block.content, block.ms);
        block.trades = parsed.trades;
        block.reordered_input = parsed.reordered_input;
        block.timestamp_inversion_count = parsed.timestamp_inversion_count;
      }
    }

    const dataBlockStarts = Array.from(this.blocks.keys()).sort((a, b) => a - b);
    if (dataBlockStarts.length === 0) {
      this.cursor = lastCursor;
      return { blocks: [], activeBlockMs: null, cursor: lastCursor };
    }

    // Fill empty blocks between fromMs and the last seen data block (capped at toMs).
    const endMs = Math.min(toMs, dataBlockStarts[dataBlockStarts.length - 1] + BLOCK_DURATION_MS);
    if (fromMs >= endMs) {
      this.cursor = lastCursor;
      return { blocks: [], activeBlockMs: null, cursor: lastCursor };
    }

    const blocks = [];
    for (let ms = fromMs; ms < endMs; ms += BLOCK_DURATION_MS) {
      let block = this.blocks.get(ms);
      if (!block) {
        block = emptyBlock({ ms, market: this.market });
        const nextDataStart = dataBlockStarts.find((start) => start > ms);
        const cursor = blockStartCursors.get(ms) || (nextDataStart !== undefined ? blockStartCursors.get(nextDataStart) : null);
        if (cursor) block.v4Cursor = { ...cursor };
        this.blocks.set(ms, block);
      }
      blocks.push(block);
    }

    let activeBlockMs = null;
    if (this.activeSegment && blocks.length > 0) {
      const last = blocks[blocks.length - 1];
      last.active = true;
      activeBlockMs = last.ms;
    }

    this.cursor = lastCursor;
    await this._attachSegmentProofs();

    return { blocks, activeBlockMs, cursor: lastCursor };
  }

  async _attachSegmentProofs() {
    for (const block of this.blocks.values()) {
      if (!block.segmentProof) continue;
      for (const proof of block.segmentProof) {
        const info = await this._segmentInfo(proof.segmentLogicalId);
        if (!info) continue;
        const prefixSha256 = await sha256FilePrefix(info.filePath, proof.byteOffsetEnd);
        Object.assign(proof, {
          sourceLogicalPath: info.logicalPath,
          sourcePath: info.logicalPath,
          sourceSize: info.size,
          sourceSha256: info.sha256,
          source_path: info.logicalPath,
          source_size: info.size,
          source_hash: info.sha256,
          active: info.active,
          status: info.active ? 'active' : 'committed',
          sourcePrefixSize: proof.byteOffsetEnd,
          sourcePrefixSha256: prefixSha256,
          source_prefix_size: proof.byteOffsetEnd,
          source_prefix_sha256: prefixSha256,
          source_prefix_hash: prefixSha256,
          byte_offset: proof.byteOffsetEnd,
        });
      }
    }
  }

  async _segmentInfo(segmentLogicalId) {
    if (this.segmentInfoCache.has(segmentLogicalId)) return this.segmentInfoCache.get(segmentLogicalId);
    const [date, segment] = String(segmentLogicalId).split('/');
    if (!date || !segment) return null;
    const dir = path.join(path.resolve(this.root), this.kind, this.market, date);
    const candidates = [path.join(dir, `${segment}.jsonl`), path.join(dir, `${segment}.jsonl.active`)];
    let filePath = null;
    for (const candidate of candidates) {
      if (await pathExists(candidate)) { filePath = candidate; break; }
    }
    if (!filePath) return null;
    const size = await fileSize(filePath);
    const sha256 = await sha256File(filePath);
    const active = filePath.endsWith('.active');
    const info = { filePath, logicalPath: logicalPath(path.resolve(this.root), filePath), size, sha256, active };
    this.segmentInfoCache.set(segmentLogicalId, info);
    return info;
  }

  _parseTrades(content, blockStartMs) {
    if (!content) {
      return { trades: [], inputSha256: sha256Content(''), reordered_input: false, timestamp_inversion_count: 0 };
    }
    return validateAndParseTrades(content, blockStartMs);
  }

  /**
   * Build per-second traded notional lookup for #12 denominator from in-memory v4 blocks.
   * @param {number} blockStartMs
   * @returns {Map<number,number>}
   */
  buildTradedNotionalLookup(blockStartMs) {
    const requiredStarts = [blockStartMs - BLOCK_DURATION_MS, blockStartMs];
    const trades = [];
    for (const bs of requiredStarts) {
      const block = this.blocks.get(bs);
      if (block) trades.push(...block.trades);
    }

    const lookup = new Map();
    for (let s = blockStartMs; s < blockStartMs + BLOCK_DURATION_MS; s += 1000) {
      let sum = 0;
      const windowStart = s - BLOCK_DURATION_MS;
      const windowEnd = s;
      for (const t of trades) {
        if (t.ts >= windowStart && t.ts < windowEnd) sum += t.price * t.qty;
      }
      lookup.set(s, sum);
    }
    return lookup;
  }

  /**
   * Read the two preceding blocks plus the target block for Phase 0 rolling features.
   * @param {number} blockStartMs
   * @returns {{trades: Array, hashes: Record<string,string>, assumedEmptyBlockStarts: number[]}}
   */
  buildTradeHistory(blockStartMs) {
    const starts = [blockStartMs - 60_000, blockStartMs - 30_000, blockStartMs];
    const trades = [];
    const hashes = {};
    const assumedEmptyBlockStarts = [];

    for (const bs of starts) {
      const block = this.blocks.get(bs);
      if (!block) {
        assumedEmptyBlockStarts.push(bs);
        continue;
      }
      trades.push(...block.trades);
      hashes[String(bs)] = block.sha256;
    }

    trades.sort((a, b) => a.ts - b.ts || (a._idx ?? 0) - (b._idx ?? 0));
    return { trades, hashes, assumedEmptyBlockStarts };
  }

  /**
   * Retrieve a loaded virtual block by start ms.
   * @param {number} ms
   * @returns {object|undefined}
   */
  getBlock(ms) {
    return this.blocks.get(ms);
  }

  /**
   * Validate raw trade lookback blocks intersecting the target window.
   * @param {number} targetBlockStartMs
   * @returns {{ coverageComplete: boolean, missing: string[], hashes: Record<string,string>, trades: Array, assumedEmptyBlockStarts: number[] }}
   */
  validateRawTradeLookback(targetBlockStartMs) {
    const requiredStarts = [targetBlockStartMs - BLOCK_DURATION_MS, targetBlockStartMs];
    const trades = [];
    const hashes = {};
    const assumedEmptyBlockStarts = [];

    for (const bs of requiredStarts) {
      const block = this.blocks.get(bs);
      if (!block) {
        assumedEmptyBlockStarts.push(bs);
        continue;
      }
      trades.push(...block.trades);
      hashes[String(bs)] = block.sha256;
    }

    return { coverageComplete: true, missing: [], hashes, trades, assumedEmptyBlockStarts };
  }
}
