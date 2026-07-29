// ⚠️  LEGACY — Shared sequential byte-offset reader for v4 hourly append JSONL
//    segments. The live production pipeline does not use v4 segments.
//    See docs/current/canonical-pipeline.md for the canonical architecture.
//
// Shared sequential byte-offset reader for v4 hourly append JSONL segments.
//
// Enumerates closed ({date}/{segment}.jsonl) and active ({date}/{segment}.jsonl.active)
// segments under {root}/{kind}/{market} in chronological order, treating an
// .active -> .closed rename as the same logical segment. Reads only forward from
// the current byte offset; no full file re-reads. Partial final lines are kept in
// memory for the current reader instance. The cursor is a single JSON string that
// captures date, segment and byte_offset.

import { access, open, readdir } from 'node:fs/promises';
import path from 'node:path';

export const RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION = 'raw_v4_segment_cursor_v1';
export const RAW_V4_SCHEMA = 'raw_v4';

const safe = value => String(value).replace(/[^a-zA-Z0-9._-]/g, '_');

function normalizeCursor(value) {
  const cursor = { ...(value || {}) };
  cursor.schema_version = RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION;
  if (!cursor.date || typeof cursor.date !== 'string') {
    throw new TypeError('cursor.date must be a non-empty string');
  }
  if (!cursor.segment || typeof cursor.segment !== 'string') {
    throw new TypeError('cursor.segment must be a non-empty string');
  }
  cursor.byte_offset = Number(cursor.byte_offset) || 0;
  if (!Number.isSafeInteger(cursor.byte_offset) || cursor.byte_offset < 0) {
    throw new TypeError('cursor.byte_offset must be a non-negative safe integer');
  }
  return cursor;
}

function parseCursor(value) {
  if (value == null) return null;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return normalizeCursor(parsed);
      }
    } catch {
      // fall through
    }
    throw new TypeError(`Invalid cursor string: ${value}`);
  }
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return normalizeCursor(value);
  }
  throw new TypeError(`Invalid cursor: ${value}`);
}

function formatCursor(cursor) {
  return JSON.stringify(cursor || { schema_version: RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION, date: '', segment: '', byte_offset: 0 });
}

async function listSegments(root, kind, market) {
  const baseDir = path.join(root, safe(kind), safe(market));
  const segments = [];

  let dateDirs;
  try {
    dateDirs = await readdir(baseDir, { withFileTypes: true });
  } catch (e) {
    if (e.code === 'ENOENT') return [];
    throw e;
  }

  for (const dateDir of dateDirs) {
    if (!dateDir.isDirectory()) continue;
    const date = dateDir.name;
    const datePath = path.join(baseDir, date);

    let files;
    try {
      files = await readdir(datePath, { withFileTypes: true });
    } catch (e) {
      if (e.code === 'ENOENT') continue;
      throw e;
    }

    const bySeg = new Map();
    for (const file of files) {
      if (!file.isFile()) continue;
      const name = file.name;
      if (!name.endsWith('.jsonl') && !name.endsWith('.jsonl.active')) continue;
      const segment = name.replace(/\.jsonl(\.active)?$/, '');
      const key = `${date}/${segment}`;
      const entry = bySeg.get(key) || { date, segment, active: false };
      if (name.endsWith('.jsonl.active')) entry.active = true;
      bySeg.set(key, entry);
    }

    for (const seg of bySeg.values()) segments.push(seg);
  }

  return segments.sort((a, b) => {
    if (a.date !== b.date) return a.date.localeCompare(b.date);
    return a.segment.localeCompare(b.segment);
  });
}

function findSegmentIndex(segments, cursor) {
  if (!cursor) return 0;
  const idx = segments.findIndex(s => s.date === cursor.date && s.segment === cursor.segment);
  if (idx >= 0) return idx;
  // Cursor segment no longer exists; resume at the first segment after the cursor.
  return segments.findIndex(s => s.date > cursor.date || (s.date === cursor.date && s.segment > cursor.segment));
}

export class RawV4SegmentReader {
  constructor({ root, market, kind, cursor = null, chunkSize = 64 * 1024 } = {}) {
    if (!root || !market || !kind) throw new TypeError('root, market and kind are required');
    this.root = path.resolve(root);
    this.market = safe(market);
    this.kind = safe(kind);
    this.cursor = parseCursor(cursor);
    this.chunkSize = Math.max(1024, Math.min(Number(chunkSize) || 64 * 1024, 1024 * 1024));

    this.segments = [];
    this.segmentIndex = -1;
    this.current = null;
    this.currentHandle = null;
    this.currentPath = null;
    this.currentActive = false;

    // Buffer holds bytes after the last complete line (partial line tail).
    // bufferOffset is the file byte offset of the first byte in this.buffer.
    this.buffer = Buffer.alloc(0);
    this.bufferOffset = 0;
    this.invalidRecords = [];
  }

  async open() {
    this.segments = await listSegments(this.root, this.kind, this.market);
    this.segmentIndex = this.cursor ? findSegmentIndex(this.segments, this.cursor) : 0;
    if (this.segmentIndex < 0 || this.segmentIndex >= this.segments.length) {
      this.segmentIndex = this.segments.length;
      this.current = null;
      return;
    }
    this.current = this.segments[this.segmentIndex];
    this.bufferOffset = this.cursor?.byte_offset ?? 0;
    this.cursor = {
      schema_version: RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION,
      date: this.current.date,
      segment: this.current.segment,
      byte_offset: this.bufferOffset,
    };
    await this._openCurrent();
    if (!this.currentHandle) {
      // Initial segment disappeared; advance to the first available one.
      await this._advanceSegment();
    }
  }

  async _openCurrent() {
    if (!this.current) return;
    const resolved = await this._resolveSegmentFile(this.current.date, this.current.segment);
    if (!resolved) {
      this.currentHandle = null;
      this.currentPath = null;
      this.currentActive = false;
      return;
    }
    this.currentHandle = resolved.handle;
    this.currentPath = resolved.path;
    this.currentActive = resolved.active;
  }

  async _resolveSegmentFile(date, segment) {
    const dir = path.join(this.root, this.kind, this.market, date);
    const activePath = path.join(dir, `${segment}.jsonl.active`);
    const closedPath = path.join(dir, `${segment}.jsonl`);

    try {
      const handle = await open(activePath, 'r');
      return { handle, path: activePath, active: true };
    } catch (e) {
      if (e.code !== 'ENOENT') throw e;
    }

    try {
      const handle = await open(closedPath, 'r');
      return { handle, path: closedPath, active: false };
    } catch (e) {
      if (e.code !== 'ENOENT') throw e;
    }

    return null;
  }

  async read({ maxRecords = 1000, maxBytes = 256 * 1024 } = {}) {
    if (!this.current && this.segmentIndex >= this.segments.length) {
      return { records: [], cursor: this._cursorString(), eof: true, done: true };
    }
    if (!this.current) {
      await this._advanceSegment();
      if (!this.current) {
        return { records: [], cursor: this._cursorString(), eof: true, done: true };
      }
    }

    const records = [];
    let bytesReadTotal = 0;

    while (records.length < maxRecords) {
      this._drainBuffer(records, maxRecords);
      if (records.length >= maxRecords) break;

      const remaining = maxBytes - bytesReadTotal;
      if (remaining <= 0) break;

      const chunk = Buffer.alloc(Math.min(this.chunkSize, remaining));
      const readOffset = this.bufferOffset + this.buffer.length;
      const { bytesRead } = await this.currentHandle.read(chunk, 0, chunk.length, readOffset);

      if (bytesRead === 0) {
        const result = await this._handleEof(records);
        if (result) return result;
        continue;
      }

      bytesReadTotal += bytesRead;
      this.buffer = Buffer.concat([this.buffer, chunk.subarray(0, bytesRead)]);
    }

    // If we stopped because of a limit and the buffer is empty, probe one byte
    // so we can report accurate eof/done for the current segment.
    if (this.current && this.buffer.length === 0 && this.currentHandle) {
      const probe = Buffer.alloc(1);
      const { bytesRead } = await this.currentHandle.read(probe, 0, 1, this.bufferOffset);
      if (bytesRead === 0) {
        const result = await this._handleEof(records);
        if (result) return result;
      } else {
        this.buffer = probe.subarray(0, bytesRead);
      }
    }

    return { records, cursor: this._cursorString(), eof: false, done: false };
  }

  _drainBuffer(records, maxRecords) {
    let from = 0;
    while (records.length < maxRecords) {
      const nl = this.buffer.indexOf(0x0a, from);
      if (nl === -1) break;
      const lineStart = from;
      const lineBytes = this.buffer.subarray(lineStart, nl);
      const lineLength = nl - lineStart + 1; // include trailing newline
      from = nl + 1;
      if (lineBytes.length === 0) continue;
      const byteOffset = this.bufferOffset + lineStart;
      const record = this._parseLine(lineBytes, byteOffset, lineLength);
      if (record) records.push(record);
      this.cursor = {
        schema_version: RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION,
        date: this.current.date,
        segment: this.current.segment,
        byte_offset: this.bufferOffset + from,
      };
    }
    if (from > 0) {
      this.buffer = this.buffer.subarray(from);
      this.bufferOffset += from;
    }
  }

  _parseLine(lineBytes, byteOffset, byteLength) {
    let envelope;
    try {
      envelope = JSON.parse(lineBytes.toString('utf8'));
    } catch {
      this.invalidRecords.push({ byte_offset: byteOffset, byte_length: byteLength, reason: 'invalid-json' });
      return null;
    }
    if (!envelope || typeof envelope !== 'object' || Array.isArray(envelope)) {
      this.invalidRecords.push({ byte_offset: byteOffset, byte_length: byteLength, reason: 'invalid-envelope' });
      return null;
    }
    if (envelope.schema !== RAW_V4_SCHEMA) {
      this.invalidRecords.push({ byte_offset: byteOffset, byte_length: byteLength, reason: 'unexpected-schema' });
      return null;
    }
    if (envelope.event_ts_ms == null || envelope.payload === undefined) {
      this.invalidRecords.push({ byte_offset: byteOffset, byte_length: byteLength, reason: 'missing-event-or-payload' });
      return null;
    }
    const eventTsMs = Number(envelope.event_ts_ms);
    if (!Number.isSafeInteger(eventTsMs)) {
      this.invalidRecords.push({ byte_offset: byteOffset, byte_length: byteLength, reason: 'invalid-event-ts' });
      return null;
    }
    return {
      event_ts_ms: eventTsMs,
      payload: envelope.payload,
      _meta: {
        source_path: this.currentPath || null,
        logical_id: this.current ? `${this.current.date}/${this.current.segment}` : null,
        active: this.currentActive,
        byte_offset: byteOffset,
        byte_length: byteLength,
      },
    };
  }

  async _handleEof(records) {
    if (this.currentActive) {
      const stillActive = await this._pathExists(this.currentPath);
      if (stillActive) {
        // Multiple active segments are a writer-level violation; if a later
        // active segment exists, this active is stale and we must not block forever.
        if (this._hasLaterActive()) {
          await this.currentHandle.close();
          this.currentHandle = null;
          this.currentPath = null;
          this.currentActive = false;
          this.buffer = Buffer.alloc(0);
          await this._advanceSegment();
          if (!this.current) {
            return { records, cursor: this._cursorString(), eof: true, done: true };
          }
          return null;
        }
        // Active segment may grow; keep the partial tail for the next read.
        return { records, cursor: this._cursorString(), eof: true, done: false };
      }
      // Active was renamed to closed. Switch handles and continue; the next read
      // will EOF on the closed file and advance to the following segment.
      await this.currentHandle.close();
      this.currentHandle = null;
      this.currentPath = null;
      this.buffer = Buffer.alloc(0);
      this.bufferOffset = this.cursor.byte_offset;
      const resolved = await this._resolveSegmentFile(this.current.date, this.current.segment);
      if (!resolved) {
        await this._advanceSegment();
        return null;
      }
      this.currentHandle = resolved.handle;
      this.currentPath = resolved.path;
      this.currentActive = resolved.active;
      return null;
    }

    // Closed EOF: discard any dangling partial tail and advance.
    this.buffer = Buffer.alloc(0);
    await this.currentHandle.close();
    this.currentHandle = null;
    this.currentPath = null;
    this.currentActive = false;
    await this._advanceSegment();
    if (!this.current) {
      return { records, cursor: this._cursorString(), eof: true, done: true };
    }
    return null;
  }

  _hasLaterActive() {
    for (let i = this.segmentIndex + 1; i < this.segments.length; i++) {
      if (this.segments[i].active) return true;
    }
    return false;
  }

  async _advanceSegment() {
    if (this.currentHandle) {
      await this.currentHandle.close();
      this.currentHandle = null;
      this.currentPath = null;
      this.currentActive = false;
    }
    while (true) {
      this.segmentIndex++;
      if (this.segmentIndex >= this.segments.length) {
        this.current = null;
        return;
      }
      this.current = this.segments[this.segmentIndex];
      this.buffer = Buffer.alloc(0);
      this.bufferOffset = 0;
      this.cursor = {
        schema_version: RAW_V4_SEGMENT_CURSOR_SCHEMA_VERSION,
        date: this.current.date,
        segment: this.current.segment,
        byte_offset: 0,
      };
      await this._openCurrent();
      if (this.currentHandle) return;
      // Missing segment: skip to the next one.
    }
  }

  async _pathExists(p) {
    try {
      await access(p);
      return true;
    } catch (e) {
      if (e.code === 'ENOENT') return false;
      throw e;
    }
  }

  _cursorString() {
    return formatCursor(this.cursor);
  }

  getInvalidRecords() {
    return this.invalidRecords.slice();
  }

  async close() {
    if (this.currentHandle) {
      await this.currentHandle.close();
      this.currentHandle = null;
    }
  }
}
