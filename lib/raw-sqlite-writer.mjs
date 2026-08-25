// Market-split SQLite WAL sink for receiver raw events.
// One process owns the writers; other local processes may read each DB while
// it is being written because WAL keeps readers on a stable snapshot.

import { DatabaseSync } from 'node:sqlite';
import { mkdir, readdir } from 'node:fs/promises';
import { gunzipSync, gzipSync } from 'node:zlib';
import path from 'node:path';

export const RAW_SQLITE_SCHEMA = 'raw_v6_sqlite';
export const DEFAULT_RAW_RETENTION_DAYS = 90;

function finiteMs(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.trunc(number) : fallback;
}

function scalarText(value) {
  return value === null || value === undefined ? null : String(value);
}

function isBusyError(error) {
  return error?.code === 'SQLITE_BUSY'
    || /database is locked|SQLITE_BUSY/i.test(String(error?.message ?? ''));
}

function normalizeEnvelope(envelope, nowMs, writerSessionId, ingestSeq) {
  if (!envelope || typeof envelope !== 'object') throw new TypeError('raw envelope must be an object');
  if (typeof envelope.market !== 'string' || !envelope.market) throw new TypeError('raw envelope market is required');
  if (typeof envelope.stream !== 'string' || !envelope.stream) throw new TypeError('raw envelope stream is required');
  const rawLine = typeof envelope.raw_line === 'string'
    ? envelope.raw_line.replace(/\n$/, '')
    : JSON.stringify(envelope);
  return {
    schema: typeof envelope.schema === 'string' ? envelope.schema : RAW_SQLITE_SCHEMA,
    market: envelope.market,
    stream: envelope.stream,
    event_ts_ms: finiteMs(envelope.event_ts_ms),
    recv_ts_ms: finiteMs(envelope.recv_ts_ms, nowMs),
    writer_session_id: scalarText(envelope.writer_session_id) ?? writerSessionId,
    ingest_seq: scalarText(envelope.ingest_seq) ?? String(ingestSeq),
    source_id: scalarText(envelope.source_id),
    raw_line: rawLine,
    written_at_ms: nowMs,
  };
}

function makeBatch(rows) {
  const rawText = `${rows.map((row) => row.raw_line).join('\n')}\n`;
  const eventTimes = rows.map((row) => row.event_ts_ms).filter((value) => value !== null);
  const recvTimes = rows.map((row) => row.recv_ts_ms).filter((value) => value !== null);
  return {
    schema: rows[0].schema,
    market: rows[0].market,
    stream: rows[0].stream,
    first_event_ts_ms: eventTimes.length ? Math.min(...eventTimes) : null,
    last_event_ts_ms: eventTimes.length ? Math.max(...eventTimes) : null,
    first_recv_ts_ms: Math.min(...recvTimes),
    last_recv_ts_ms: Math.max(...recvTimes),
    row_count: rows.length,
    raw_gzip: gzipSync(Buffer.from(rawText, 'utf8'), { level: 1 }),
    raw_bytes: Buffer.byteLength(rawText, 'utf8'),
    written_at_ms: rows.at(-1).written_at_ms,
  };
}

function eventOrderKey(row) {
  const eventTs = row.event_ts_ms;
  return eventTs === null ? Number.MAX_SAFE_INTEGER : eventTs;
}

function sortBatchRows(rows) {
  // IPC到着順をイベント順へ変換する。event_ts_ms欠落は末尾へ。
  return rows.map((row, index) => ({ row, index }))
    .sort((a, b) => eventOrderKey(a.row) - eventOrderKey(b.row)
      || Number(a.row.ingest_seq) - Number(b.row.ingest_seq)
      || a.index - b.index)
    .map(({ row }) => row);
}

// 遅着マージ用: raw_line 1行からソートキーを抽出する。
// raw_line は正規化エンベロープJSONなので先頭付近に event_ts_ms / ingest_seq を持つ。
function lineSortKey(line) {
  let parsed;
  try {
    parsed = JSON.parse(line);
  } catch (error) {
    throw new TypeError(`raw batch contains invalid JSON: ${error.message}`, { cause: error });
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new TypeError('raw batch line must be a JSON object');
  }
  const ts = finiteMs(parsed.event_ts_ms);
  const seqValue = parsed.ingest_seq;
  const seqNumber = seqValue === null || seqValue === undefined ? null : Number(seqValue);
  return {
    ts,
    seq: Number.isFinite(seqNumber) ? seqNumber : null,
    recv: finiteMs(parsed.recv_ts_ms),
  };
}

function compareSortKey(a, b) {
  const aTs = a.ts === null ? Number.MAX_SAFE_INTEGER : a.ts;
  const bTs = b.ts === null ? Number.MAX_SAFE_INTEGER : b.ts;
  if (aTs !== bTs) return aTs - bTs;
  if (a.seq !== null && b.seq !== null && a.seq !== b.seq) return a.seq - b.seq;
  return 0;
}

// 遅着行を既存batchへマージする。
// meta: raw_batches行 (raw_gzip等), incoming: 正規化rowの配列
// 戻り値: 更新後のメタ + raw_gzip
export function mergeLinesIntoBatch(meta, incoming) {
  const text = gunzipSync(Buffer.from(meta.raw_gzip)).toString('utf8');
  const lines = text.length > 0 ? text.replace(/\n$/, '').split('\n') : [];
  const keys = lines.map(lineSortKey);

  for (const row of incoming) {
    // 正規化済みrowからraw_lineを取り出し、キーも抽出
    const line = typeof row.raw_line === 'string' ? row.raw_line : JSON.stringify(row);
    const key = lineSortKey(line);
    let lo = 0;
    let hi = lines.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (compareSortKey(keys[mid], key) <= 0) lo = mid + 1;
      else hi = mid;
    }
    lines.splice(lo, 0, line);
    keys.splice(lo, 0, key);
  }

  const rawText = `${lines.join('\n')}\n`;
  const eventTimes = keys.map((k) => k.ts).filter((v) => v !== null);
  // recv timestamps were already extracted by lineSortKey — no second parse.
  const recvTimes = keys.map((k) => k.recv).filter((v) => v !== null);
  const firstRecv = Math.min(...incoming.map((r) => r.recv_ts_ms),
    ...(recvTimes.length ? [Math.min(...recvTimes)] : []));
  const lastRecv = Math.max(...incoming.map((r) => r.recv_ts_ms),
    ...(recvTimes.length ? [Math.max(...recvTimes)] : []));

  return {
    first_event_ts_ms: eventTimes.length ? Math.min(...eventTimes) : null,
    last_event_ts_ms: eventTimes.length ? Math.max(...eventTimes) : null,
    first_recv_ts_ms: Number.isFinite(firstRecv) ? firstRecv : meta.first_recv_ts_ms,
    last_recv_ts_ms: Number.isFinite(lastRecv) ? lastRecv : meta.last_recv_ts_ms,
    row_count: lines.length,
    raw_gzip: gzipSync(Buffer.from(rawText, 'utf8'), { level: 1 }),
    raw_bytes: Buffer.byteLength(rawText, 'utf8'),
  };
}

function safeMarketName(market) {
  const name = String(market).replace(/[^a-zA-Z0-9_-]/g, '_');
  if (!name) throw new TypeError('market must produce a non-empty filename');
  return name;
}

function configureDatabase(db) {
  db.exec(`
    PRAGMA busy_timeout=5000;
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA wal_autocheckpoint=1000;
    CREATE TABLE IF NOT EXISTS raw_batches (
      batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
      schema TEXT NOT NULL,
      market TEXT NOT NULL,
      stream TEXT NOT NULL,
      first_event_ts_ms INTEGER,
      last_event_ts_ms INTEGER,
      first_recv_ts_ms INTEGER NOT NULL,
      last_recv_ts_ms INTEGER NOT NULL,
      row_count INTEGER NOT NULL,
      raw_gzip BLOB NOT NULL,
      raw_bytes INTEGER NOT NULL,
      written_at_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS raw_batches_recv_idx
      ON raw_batches(last_recv_ts_ms);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_idx
      ON raw_batches(stream, last_recv_ts_ms);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_last_event_idx
      ON raw_batches(stream, last_event_ts_ms);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_first_event_idx
      ON raw_batches(stream, first_event_ts_ms);
  `);
}

export class RawSqliteWriter {
  constructor({ databaseDir, retentionDays = DEFAULT_RAW_RETENTION_DAYS, now = () => Date.now() } = {}) {
    if (!databaseDir) throw new TypeError('databaseDir is required');
    if (!Number.isInteger(retentionDays) || retentionDays < 1) {
      throw new TypeError('retentionDays must be a positive integer');
    }
    this.databaseDir = path.resolve(databaseDir);
    this.retentionDays = retentionDays;
    this.now = now;
    this.databases = new Map();
    this.queue = Promise.resolve();
    this.closed = false;
    this.writerSessionId = `sqlite:${process.pid}:${Date.now()}`;
    this.ingestSeq = 0;
    // 遅着イベントの可視性カウンタ (market\0stream -> {merged, minibatches, mergedRows})
    this.lateEventStats = new Map();
  }

  _bumpLateStats(key, field, amount = 1) {
    if (!this.lateEventStats.has(key)) {
      this.lateEventStats.set(key, { merged: 0, mergedRows: 0, minibatches: 0 });
    }
    const stats = this.lateEventStats.get(key);
    stats[field] += amount;
    return stats;
  }

  lateEventSummary() {
    const summary = {};
    for (const [key, value] of this.lateEventStats) {
      const [market, stream] = key.split('\u0000');
      summary[`${market}.${stream}`] = { ...value };
    }
    return summary;
  }

  async open() {
    await mkdir(this.databaseDir, { recursive: true });
    const entries = await readdir(this.databaseDir, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.isFile() && entry.name.endsWith('.sqlite')) {
        this._openMarket(path.basename(entry.name, '.sqlite'));
      }
    }
    return this;
  }

  _openMarket(market) {
    if (this.databases.has(market)) return this.databases.get(market);
    const databasePath = path.join(this.databaseDir, `${safeMarketName(market)}.sqlite`);
    const db = new DatabaseSync(databasePath);
    try {
      configureDatabase(db);
    } catch (error) {
      try { db.close(); } catch (_) {}
      throw error;
    }
    const entry = { market, databasePath, db };
    this.databases.set(market, entry);
    return entry;
  }

  append(envelopes) {
    if (this.closed) return Promise.reject(new Error('raw SQLite writer is closed'));
    const rows = envelopes.map((envelope) => normalizeEnvelope(
      envelope, this.now(), this.writerSessionId, ++this.ingestSeq,
    ));
    if (rows.length === 0) return Promise.resolve();

    // Keep watermark reads and writes in the same serialized queue.  This is
    // required for callers that issue append() without awaiting each promise.
    const operation = this.queue.then(() => {
      const groups = new Map();
      for (const row of rows) {
        const key = `${row.market}\u0000${row.stream}`;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(row);
      }
      const freshGroups = [];
      const lateGroups = [];
      for (const [key, groupRows] of groups) {
        const sorted = sortBatchRows(groupRows);
        const fresh = this._partitionLate(key, sorted, lateGroups);
        if (fresh.length > 0) freshGroups.push(fresh);
      }
      return this._insertBatches(freshGroups.map(makeBatch), lateGroups);
    });
    this.queue = operation;
    return operation;
  }

  // groupRows (event順ソート済み) の先頭が既存データより古いか判定し、
  // 遅着分と通常分に分割する。戻り値は通常分 (makeBatch 済み配列へpushされる)。
  _partitionLate(key, sortedRows, lateGroups) {
    const [market, stream] = key.split('\u0000');
    const entry = this.databases.get(market);
    let lastEventTs = null;
    if (entry) {
      try {
        const row = entry.db.prepare(`
          SELECT max(last_event_ts_ms) AS ts
          FROM raw_batches WHERE stream = ?
        `).get(stream);
        lastEventTs = row?.ts === null || row?.ts === undefined ? null : Number(row.ts);
      } catch (error) {
        throw new Error(
          `failed to read raw batch watermark for ${market}/${stream}`,
          { cause: error },
        );
      }
    }
    // Equal-to-watermark timestamps stay on the fresh path.  This avoids
    // duplicate boundary rows in a new backfill batch; strictly older rows
    // are the late-event path below.
    if (lastEventTs === null || sortedRows.length === 0
      || eventOrderKey(sortedRows[0]) >= lastEventTs) {
      return sortedRows; // 通常フロー: 全部新着
    }
    // 遅着と新着の境界を見つける (sortedRows は event_ts 昇順)
    let split = 0;
    while (split < sortedRows.length && eventOrderKey(sortedRows[split]) < lastEventTs) split++;
    const [lateRows, freshRows] = [sortedRows.slice(0, split), sortedRows.slice(split)];
    lateGroups.push({ market, stream, rows: lateRows });
    console.error(
      `[RawSqliteWriter] ${market}/${stream}: ${lateRows.length} late event(s) `
      + `(oldest=${eventOrderKey(lateRows[0])}, watermark=${lastEventTs}) routed to backfill`,
    );
    return freshRows;
  }

  _insertBatches(batches, lateGroups = []) {
    const byMarket = new Map();
    for (const batch of batches) {
      if (!byMarket.has(batch.market)) byMarket.set(batch.market, []);
      byMarket.get(batch.market).push(batch);
    }
    return (async () => {
    for (const [market, marketBatches] of byMarket) {
      await this._withMarketDb(market, async ({ db }) => {
        const insert = db.prepare(`
          INSERT INTO raw_batches (
            schema, market, stream, first_event_ts_ms, last_event_ts_ms,
            first_recv_ts_ms, last_recv_ts_ms, row_count, raw_gzip, raw_bytes,
            written_at_ms
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        await this._runWithBusyRetry(db, () => {
          db.exec('BEGIN IMMEDIATE');
          try {
            for (const batch of marketBatches) {
              insert.run(
                batch.schema, batch.market, batch.stream,
                batch.first_event_ts_ms, batch.last_event_ts_ms,
                batch.first_recv_ts_ms, batch.last_recv_ts_ms, batch.row_count,
                batch.raw_gzip, batch.raw_bytes, batch.written_at_ms,
              );
            }
            db.exec('COMMIT');
          } catch (error) {
            try { db.exec('ROLLBACK'); } catch (_) {}
            throw error;
          }
        });
      });
    }
    for (const group of lateGroups) {
      await this._backfillLateGroup(group);
    }
    })();
  }

  async _withMarketDb(market, fn) {
    let entry;
    for (let attempt = 0; ; attempt++) {
      try {
        entry = this._openMarket(market);
        break;
      } catch (error) {
        if (!isBusyError(error) || attempt >= 2) throw error;
        await new Promise((resolve) => setTimeout(resolve, [100, 250, 500][attempt]));
      }
    }
    return fn(entry);
  }

  async _runWithBusyRetry(db, fn) {
    for (let attempt = 0; ; attempt++) {
      try {
        await fn();
        return;
      } catch (error) {
        if (!isBusyError(error) || attempt >= 2) throw error;
        await new Promise((resolve) => setTimeout(resolve, [100, 250, 500][attempt]));
      }
    }
  }

  // 遅着イベントの本来位置への差し込み。
  // - 既存batchの [first,last_event_ts_ms] 範囲内に収まる行はそのbatchへマージ
  //   (batch_id 不変なので下流の batch_id cursor は安全)
  // - どのbatchにも属しない行(時間ギャップ内)は mini-batch として新規作成
  async _backfillLateGroup({ market, stream, rows }) {
    if (!rows || rows.length === 0) return;
    const key = `${market}\u0000${stream}`;
    await this._withMarketDb(market, async ({ db }) => {
      const minTs = eventOrderKey(rows[0]);
      const maxKey = eventOrderKey(rows[rows.length - 1]);
      const maxTs = maxKey === Number.MAX_SAFE_INTEGER ? minTs : maxKey;
      const stats = { mergedBatches: 0, mergedRows: 0, minibatches: 0 };
      await this._runWithBusyRetry(db, () => {
        db.exec('BEGIN IMMEDIATE');
        try {
          // Re-read candidate metadata after acquiring the write lock.  This
          // keeps selection and mutation on one SQLite snapshot and retries
          // both together when the database is busy.
          const candidates = db.prepare(`
            SELECT batch_id, first_event_ts_ms, last_event_ts_ms, raw_gzip,
                   first_recv_ts_ms, last_recv_ts_ms, written_at_ms
            FROM raw_batches
            WHERE stream = ? AND first_event_ts_ms <= ? AND last_event_ts_ms >= ?
            ORDER BY first_event_ts_ms ASC
          `).all(stream, maxTs, minTs);
          const byBatch = new Map(); // batch_id -> rows
          const leftovers = [];
          for (const row of rows) {
            const ts = eventOrderKey(row);
            if (ts === Number.MAX_SAFE_INTEGER) { leftovers.push(row); continue; }
            const matches = candidates.filter((c) => ts >= Number(c.first_event_ts_ms)
              && ts <= Number(c.last_event_ts_ms));
            if (matches.length > 1) {
              throw new Error(
                `overlapping raw batch ranges for ${market}/${stream} at event_ts_ms=${ts}`,
              );
            }
            const target = matches[0];
            if (target) {
              if (!byBatch.has(target.batch_id)) byBatch.set(target.batch_id, []);
              byBatch.get(target.batch_id).push(row);
            } else {
              leftovers.push(row);
            }
          }

          for (const [batchId, incoming] of byBatch) {
            const meta = candidates.find((c) => Number(c.batch_id) === Number(batchId));
            const updated = mergeLinesIntoBatch(meta, incoming);
            db.prepare(`
              UPDATE raw_batches SET
                first_event_ts_ms = ?, last_event_ts_ms = ?,
                first_recv_ts_ms = ?, last_recv_ts_ms = ?,
                row_count = ?, raw_gzip = ?, raw_bytes = ?
              WHERE batch_id = ?
            `).run(
              updated.first_event_ts_ms, updated.last_event_ts_ms,
              updated.first_recv_ts_ms, updated.last_recv_ts_ms,
              updated.row_count, updated.raw_gzip, updated.raw_bytes,
              Number(batchId),
            );
            stats.mergedBatches++;
            stats.mergedRows += incoming.length;
          }
          if (leftovers.length > 0) {
            stats.minibatches = 1;
            const insert = db.prepare(`
              INSERT INTO raw_batches (
                schema, market, stream, first_event_ts_ms, last_event_ts_ms,
                first_recv_ts_ms, last_recv_ts_ms, row_count, raw_gzip, raw_bytes,
                written_at_ms
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);
            // leftovers全体を1つのソート済みmini-batchとして登録する
            const batch = makeBatch(sortBatchRows(leftovers));
            insert.run(
              batch.schema, batch.market, batch.stream,
              batch.first_event_ts_ms, batch.last_event_ts_ms,
              batch.first_recv_ts_ms, batch.last_recv_ts_ms, batch.row_count,
              batch.raw_gzip, batch.raw_bytes, batch.written_at_ms,
            );
          }
          db.exec('COMMIT');
        } catch (error) {
          try { db.exec('ROLLBACK'); } catch (_) {}
          throw error;
        }
      });

      const s = this._bumpLateStats(key, 'merged', stats.mergedBatches);
      this._bumpLateStats(key, 'mergedRows', stats.mergedRows);
      this._bumpLateStats(key, 'minibatches', stats.minibatches);
      console.error(
        `[RawSqliteWriter] ${market}/${stream}: backfilled ${rows.length} late row(s) `
        + `into ${stats.mergedBatches} batch(es) (${stats.mergedRows} merged, `
        + `${stats.minibatches} mini-batch${stats.minibatches === 1 ? '' : 'es'}) `
        + `[total merged=${s.merged}]`,
      );
    });
  }

  pruneExpired(nowMs = this.now()) {
    if (this.closed) return Promise.reject(new Error('raw SQLite writer is closed'));
    const cutoff = Math.trunc(nowMs - this.retentionDays * 24 * 60 * 60 * 1000);
    this.queue = this.queue.then(() => {
      for (const { db } of this.databases.values()) {
        db.prepare('DELETE FROM raw_batches WHERE last_recv_ts_ms < ?').run(cutoff);
        db.exec('PRAGMA wal_checkpoint(PASSIVE)');
      }
    });
    return this.queue;
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    await this.queue;
    for (const { db } of this.databases.values()) db.close();
    this.databases.clear();
  }
}

export function decodeRawBatch(rawGzip) {
  return gunzipSync(rawGzip).toString('utf8').trim().split('\n').filter(Boolean);
}
