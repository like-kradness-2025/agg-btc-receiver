// ⚠️  LEGACY — v4 hourly JSONL segment writer. The live production pipeline
//    uses market-split SQLite (data/sqlite/<market>.sqlite) instead of JSONL
//    segments. See docs/current/canonical-pipeline.md for the canonical architecture.
//
// Append-only v4 raw writer.  One active JSONL segment exists per market/kind.
// Segments roll on UTC hour or size; closed segments are immutable.

import { mkdir, open, readdir, rename, stat } from 'node:fs/promises';
import path from 'node:path';

export const RAW_V4_SCHEMA = 'raw_v4';
export const DEFAULT_MAX_SEGMENT_BYTES = 256 * 1024 * 1024;

const safe = value => String(value).replace(/[^a-zA-Z0-9._-]/g, '_');
const pad = n => String(n).padStart(2, '0');
const durableSync = handle => typeof handle.datasync === 'function' ? handle.datasync() : handle.sync();

function parseSegmentPath(absPath, root) {
  const rel = path.relative(root, absPath);
  const parts = rel.split(path.sep);
  const date = parts[parts.length - 2];
  const base = parts[parts.length - 1];
  const m = base.match(/^(\d{2})-(\d{2})\.jsonl\.active$/);
  if (!m || !date) return null;
  return { date, hour: m[1], index: Number(m[2]) };
}

async function listSegmentFiles(dir) {
  let entries;
  try { entries = await readdir(dir, { withFileTypes: true }); } catch (e) {
    if (e.code === 'ENOENT') return [];
    throw e;
  }
  return entries
    .filter(e => e.isFile() && (e.name.endsWith('.jsonl') || e.name.endsWith('.jsonl.active')))
    .map(e => path.join(dir, e.name));
}

function hourParts(ts) {
  const d = new Date(ts);
  return {
    date: `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`,
    hour: pad(d.getUTCHours()),
  };
}

async function listFiles(root) {
  const out = [];
  async function walk(dir) {
    let entries;
    try { entries = await readdir(dir, { withFileTypes: true }); } catch (e) {
      if (e.code === 'ENOENT') return;
      throw e;
    }
    for (const entry of entries) {
      const file = path.join(dir, entry.name);
      if (entry.isDirectory()) await walk(file);
      else if (entry.name.endsWith('.jsonl.active')) out.push(file);
    }
  }
  await walk(root);
  return out.sort();
}

const TAIL_REPAIR_CHUNK = 64 * 1024;

async function readChunk(handle, position, size) {
  const buf = Buffer.alloc(size);
  const { bytesRead } = await handle.read(buf, 0, size, position);
  return { buf, bytesRead };
}

async function countRowsBounded(handle, limit) {
  if (limit === 0) return 0;
  let rows = 0;
  let position = 0;
  const buf = Buffer.alloc(TAIL_REPAIR_CHUNK);
  while (position < limit) {
    const size = Math.min(TAIL_REPAIR_CHUNK, limit - position);
    const { bytesRead } = await handle.read(buf, 0, size, position);
    if (bytesRead === 0) break;
    for (let i = 0; i < bytesRead; i += 1) {
      if (buf[i] === 0x0a) rows += 1;
    }
    position += bytesRead;
  }
  return rows;
}

async function repairTail(file) {
  const handle = await open(file, 'r+');
  try {
    const { size } = await handle.stat();
    if (size === 0) {
      await handle.truncate(0);
      await durableSync(handle);
      return { bytes: 0, rows: 0 };
    }

    let valid = 0;
    let position = Math.max(0, size - TAIL_REPAIR_CHUNK);
    while (true) {
      const chunkSize = Math.min(TAIL_REPAIR_CHUNK, size - position);
      const { buf, bytesRead } = await readChunk(handle, position, chunkSize);
      const chunk = buf.subarray(0, bytesRead);
      const idx = chunk.lastIndexOf(0x0a);
      if (idx !== -1) {
        valid = position + idx + 1;
        break;
      }
      if (position === 0) break;
      position = Math.max(0, position - TAIL_REPAIR_CHUNK);
    }

    if (valid !== size) {
      await handle.truncate(valid);
      await durableSync(handle);
    }
    const rows = await countRowsBounded(handle, valid);
    return { bytes: valid, rows };
  } finally {
    await handle.close();
  }
}

export class RawV4Writer {
  constructor({ root, market, kind, maxSegmentBytes = DEFAULT_MAX_SEGMENT_BYTES, now = () => Date.now(), sessionId = `${process.pid}` } = {}) {
    if (!root || !market || !kind) throw new TypeError('root, market and kind are required');
    this.root = path.resolve(root);
    this.market = safe(market);
    this.kind = safe(kind);
    this.maxSegmentBytes = maxSegmentBytes;
    this.now = now;
    this.sessionId = sessionId;
    this.active = null;
    this.queue = Promise.resolve();
  }

  _enqueue(fn) {
    const result = this.queue.then(fn);
    this.queue = result.catch(() => {});
    return result;
  }

  async _findActive() {
    const files = await listFiles(path.join(this.root, this.kind, this.market));
    return files.at(-1) || null;
  }

  async _openFor(ts, requiredBytes = 0) {
    const { date, hour } = hourParts(ts);
    if (this.active && (this.active.date !== date || this.active.hour !== hour ||
      this.active.bytes + requiredBytes > this.maxSegmentBytes)) {
      await this.close();
    }
    if (this.active) return this.active;

    const dir = path.join(this.root, this.kind, this.market, date);
    const existing = await listSegmentFiles(dir);
    let index = 0;
    const prefix = `${hour}-`;
    for (const file of existing) {
      const match = path.basename(file).match(new RegExp(`^${prefix}(\\d+)\\.jsonl(\\.active)?$`));
      if (match) index = Math.max(index, Number(match[1]) + 1);
    }
    const file = path.join(dir, `${hour}-${pad(index)}.jsonl.active`);
    await mkdir(dir, { recursive: true });
    const handle = await open(file, 'a+');
    const repaired = await repairTail(file);
    this.active = { file, handle, date, hour, index, bytes: repaired.bytes, rows: repaired.rows };
    return this.active;
  }

  async append(records, { eventTs = this.now() } = {}) {
    return this._enqueue(async () => {
      const input = Array.isArray(records) ? records : [records];
      let written = 0;
      for (const payload of input) {
        const row = payload?.schema === RAW_V4_SCHEMA ? payload : {
          schema: RAW_V4_SCHEMA,
          market: this.market,
          stream: this.kind,
          event_ts_ms: Number(payload?.event_ts_ms ?? payload?.ts ?? eventTs),
          recv_ts_ms: Number(payload?.recv_ts_ms ?? this.now()),
          writer_session_id: this.sessionId,
          ingest_seq: payload?.ingest_seq ?? null,
          source_id: payload?.source_id ?? null,
          payload,
        };
        const line = Buffer.from(`${JSON.stringify(row)}\n`);
        const target = await this._openFor(row.recv_ts_ms || eventTs, line.length);
        await target.handle.write(line);
        target.bytes += line.length;
        target.rows += 1;
        written += 1;
      }
      return { written, file: this.active?.file || null, byte_offset: this.active?.bytes || 0 };
    });
  }

  // Compatibility surface for orderflow-worker's existing batch queue.
  async writeBatch(batch) {
    const records = (batch || []).map(([payload, eventTs]) => ({
      ...payload,
      event_ts_ms: payload?.event_ts_ms ?? eventTs,
    }));
    return this.append(records);
  }

  async _startupRecoveryImpl() {
    const marketDir = path.join(this.root, this.kind, this.market);
    const activeFiles = await listFiles(marketDir);
    if (activeFiles.length === 0) return null;
    activeFiles.sort();
    const chosen = activeFiles.pop();

    // Normalize to a single active file per market/kind: close all older actives.
    for (const other of activeFiles) {
      const parsed = parseSegmentPath(other, this.root);
      if (!parsed) throw new Error(`Invalid active segment name: ${other}`);
      const closed = other.replace(/\.active$/, '');
      let exists = false;
      try { await stat(closed); exists = true; } catch (e) {
        if (e.code !== 'ENOENT') throw e;
      }
      if (exists) throw new Error(`Closed segment collision: ${closed} already exists; refusing to overwrite`);
      await rename(other, closed);
      const dirHandle = await open(path.dirname(closed), 'r');
      try { await dirHandle.sync(); } finally { await dirHandle.close(); }
    }

    const repaired = await repairTail(chosen);
    const parsed = parseSegmentPath(chosen, this.root);
    if (!parsed) throw new Error(`Invalid active segment name: ${chosen}`);
    const dir = path.dirname(chosen);
    await mkdir(dir, { recursive: true });
    const handle = await open(chosen, 'a+');
    this.active = {
      file: chosen,
      handle,
      date: parsed.date,
      hour: parsed.hour,
      index: parsed.index,
      bytes: repaired.bytes,
      rows: repaired.rows,
    };
    return { file: chosen, ...repaired };
  }

  async startupRecovery() {
    return this._startupRecoveryImpl();
  }

  async checkStale() { return null; }

  getIoFailure() { return null; }

  async flush({ durable = true } = {}) {
    return this._enqueue(async () => {
      if (!this.active) return;
      if (durable) await durableSync(this.active.handle);
    });
  }

  async close() {
    if (!this.active) return;
    const current = this.active;
    this.active = null;
    await durableSync(current.handle);
    await current.handle.close();
    const closed = current.file.replace(/\.active$/, '');
    await rename(current.file, closed);
    const dirHandle = await open(path.dirname(closed), 'r');
    try { await dirHandle.sync(); } finally { await dirHandle.close(); }
    return closed;
  }

  async shutdown() {
    return this._enqueue(async () => {
      if (this.active) {
        await durableSync(this.active.handle);
        await this.active.handle.close();
        this.active = null;
      }
    });
  }
}

export async function recoverRawV4Active(root, market, kind) {
  const writer = new RawV4Writer({ root, market, kind });
  const result = await writer.startupRecovery();
  if (writer.active) {
    await writer.active.handle.close();
    writer.active = null;
  }
  return result;
}
