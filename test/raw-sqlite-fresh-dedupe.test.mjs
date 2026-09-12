import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { gunzipSync } from 'node:zlib';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
  RawSqliteWriter,
  DEFAULT_DEDUPE_WINDOW_MS,
  DEFAULT_DEDUPE_MAX_KEYS,
} from '../lib/raw-sqlite-writer.mjs';

// R-02: 送信側の再送 (再接続スナップショット等) が watermark より新しい場合、
// 遅着ルートに入らず「新規行」として二重に追記されていた。
// fresh 経路でもコミット済み identity の再送を棄却する。

function tradeEnvelope(market, stream, eventTs, sourceId, { recvTs = eventTs + 5, price = 100 } = {}) {
  return {
    schema: 'raw_v6_sqlite',
    market,
    stream,
    event_ts_ms: eventTs,
    recv_ts_ms: recvTs,
    writer_session_id: 'test:sqlite',
    source_id: sourceId,
    payload: { price, qty: 1, side: 'buy' },
  };
}

function depthEnvelope(market, stream, eventTs, { recvTs = eventTs + 5 } = {}) {
  return {
    schema: 'raw_v6_sqlite',
    market,
    stream,
    event_ts_ms: eventTs,
    recv_ts_ms: recvTs,
    writer_session_id: 'test:sqlite',
    source_id: null,
    payload: { type: 'update', bids: [['100', '1']], asks: [] },
  };
}

function query(databasePath, sql) {
  const db = new DatabaseSync(databasePath, { readOnly: true });
  try { return db.prepare(sql).all(); } finally { db.close(); }
}

async function tempWriter(options = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-r02-'));
  const writer = await new RawSqliteWriter({ databaseDir: root, ...options }).open();
  return { root, writer, marketPath: (market) => path.join(root, `${market}.sqlite`) };
}

const totalRows = (p) => query(p, 'SELECT coalesce(sum(row_count), 0) AS n FROM raw_batches')[0].n;

test('fresh-path re-delivery of a committed trade is not appended twice (R-02)', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    await writer.append([tradeEnvelope('binance_spot', 'trades', 1_000, 'tx-1')]);
    // 再接続スナップショットの再送: 同じ (source_id, event_ts) が新しい recv で再来
    await writer.append([tradeEnvelope('binance_spot', 'trades', 1_000, 'tx-1', { recvTs: 9_000 })]);

    const p = marketPath('binance_spot');
    assert.equal(totalRows(p), 1, 're-delivered trade must not be stored twice');
    const stats = writer.freshDedupeSummary();
    assert.equal(stats.skipped, 1);
    assert.equal(stats.trackedIdentities, 1);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('reconnect replay newer than the watermark only appends the truly new rows (R-02)', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    // 1回目: 1000..1009 を確定
    await writer.append(
      Array.from({ length: 10 }, (_, i) => tradeEnvelope('binance_spot', 'trades', 1_000 + i, `tx-${1000 + i}`)),
    );
    // 再接続で 1005..1014 が再送 (1005..1009 は再送、1010..1014 は新規)
    await writer.append(
      Array.from({ length: 10 }, (_, i) => tradeEnvelope('binance_spot', 'trades', 1_005 + i, `tx-${1005 + i}`, { recvTs: 50_000 + i })),
    );

    const p = marketPath('binance_spot');
    assert.equal(totalRows(p), 15, 'expected 10 committed + 5 new rows');
    // 1005..1009 は watermark 以下なので遅着ルート (R-03) が、1009 (同値) は
    // fresh 窓 (R-02) が棄却する。経路は違っても合計 5 行が二重化しない。
    const freshStats = writer.freshDedupeSummary();
    const lateStats = writer.lateEventSummary();
    const lateSkipped = Object.values(lateStats).reduce((sum, s) => sum + (s.duplicatesSkipped ?? 0), 0);
    assert.equal(freshStats.skipped + lateSkipped, 5);
    // 生データの中身も一意であること
    const batches = query(p, 'SELECT raw_gzip FROM raw_batches ORDER BY batch_id');
    const ids = batches.flatMap((b) => gunzipSync(b.raw_gzip).toString('utf8').trim().split('\n'))
      .map((line) => JSON.parse(line).source_id);
    assert.equal(ids.length, 15);
    assert.equal(new Set(ids).size, 15);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('distinct source_ids on the fresh path are all committed (control)', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    await writer.append([
      tradeEnvelope('binance_spot', 'trades', 1_000, 'tx-1'),
      tradeEnvelope('binance_spot', 'trades', 1_001, 'tx-2'),
    ]);
    assert.equal(totalRows(marketPath('binance_spot')), 2);
    assert.equal(writer.freshDedupeSummary().skipped, 0);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('identical identities inside ONE append are both kept (Kraken-style twin fills)', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    // Kraken v1 は trade_id を持たず指紋が非一意。同一フレーム内の双子は実データ。
    await writer.append([
      tradeEnvelope('kraken_spot', 'trades', 2_000, 'fp-same'),
      tradeEnvelope('kraken_spot', 'trades', 2_000, 'fp-same', { recvTs: 2_006 }),
    ]);
    const p = marketPath('kraken_spot');
    assert.equal(totalRows(p), 2, 'same-flush twins must survive');
    // ただし次フラッシュでの完全一致再送は落ちる
    await writer.append([tradeEnvelope('kraken_spot', 'trades', 2_000, 'fp-same', { recvTs: 9_000 })]);
    assert.equal(totalRows(p), 2);
    assert.equal(writer.freshDedupeSummary().skipped, 1);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('rows without source_id are never dropped and never tracked (control)', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    await writer.append([depthEnvelope('bybit_perp', 'book_updates', 3_000)]);
    await writer.append([depthEnvelope('bybit_perp', 'book_updates', 3_000, { recvTs: 9_000 })]);
    assert.equal(totalRows(marketPath('bybit_perp')), 2, 'depth rows keep the per-delivery policy');
    const stats = writer.freshDedupeSummary();
    assert.equal(stats.skipped, 0);
    assert.equal(stats.trackedIdentities, 0);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('fresh window is isolated per market and stream', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    await writer.append([
      tradeEnvelope('binance_spot', 'trades', 4_000, 'shared-id'),
      tradeEnvelope('okx_spot', 'trades', 4_000, 'shared-id'),
      tradeEnvelope('binance_spot', 'liquidations', 4_000, 'shared-id'),
    ]);
    assert.equal(totalRows(marketPath('binance_spot')), 2);
    assert.equal(totalRows(marketPath('okx_spot')), 1);
    assert.equal(writer.freshDedupeSummary().trackedIdentities, 3);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('fresh window is bounded by maxKeys and never causes a double write', async () => {
  const { root, writer, marketPath } = await tempWriter({ freshDedupeMaxKeys: 2 });
  try {
    await writer.append([tradeEnvelope('binance_spot', 'trades', 5_000, 'tx-a')]);
    await writer.append([tradeEnvelope('binance_spot', 'trades', 5_001, 'tx-b')]);
    await writer.append([tradeEnvelope('binance_spot', 'trades', 5_002, 'tx-c')]);
    assert.equal(writer.freshDedupeSummary().trackedIdentities, 2, 'window must stay bounded');
    const p = marketPath('binance_spot');
    assert.equal(totalRows(p), 3);
    const skippedBefore = writer.freshDedupeSummary().skipped;
    // tx-a は窓から落ちている → fresh 窓では棄却されない。ただし watermark 以下なので
    // 遅着ルート (R-03) が同一 identity を検出し、二重書きにはならない。
    await writer.append([tradeEnvelope('binance_spot', 'trades', 5_000, 'tx-a', { recvTs: 60_000 })]);
    assert.equal(totalRows(p), 3, 'evicted identity must not create a double write');
    assert.equal(writer.freshDedupeSummary().skipped, skippedBefore, 'eviction means the fresh window no longer catches it');
    // 直近の tx-c の再送は fresh 窓で落ちる
    await writer.append([tradeEnvelope('binance_spot', 'trades', 5_002, 'tx-c', { recvTs: 60_001 })]);
    assert.equal(totalRows(p), 3);
    assert.equal(writer.freshDedupeSummary().skipped, skippedBefore + 1);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('fresh window expires by TTL so a much later identical delivery is stored', async () => {
  let current = 1_000_000;
  const { root, writer, marketPath } = await tempWriter({ now: () => current, freshDedupeWindowMs: 1_000 });
  try {
    await writer.append([tradeEnvelope('binance_spot', 'trades', 7_000, 'tx-ttl')]);
    const p = marketPath('binance_spot');
    assert.equal(totalRows(p), 1);
    // 窓内: 落ちる
    current += 500;
    await writer.append([tradeEnvelope('binance_spot', 'trades', 7_000, 'tx-ttl', { recvTs: current })]);
    assert.equal(totalRows(p), 1);
    // 窓外: 通す
    current += 5_000;
    await writer.append([tradeEnvelope('binance_spot', 'trades', 7_000, 'tx-ttl', { recvTs: current })]);
    assert.equal(totalRows(p), 2);
    assert.equal(writer.freshDedupeSummary().trackedIdentities, 1);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('fresh window options are sanitized (NaN / 0 / negative fall back to defaults)', async () => {
  for (const bad of [Number.NaN, 0, -1, 'nope']) {
    const { root, writer } = await tempWriter({ freshDedupeWindowMs: bad, freshDedupeMaxKeys: bad });
    try {
      assert.equal(writer.freshDedupeWindowMs, DEFAULT_DEDUPE_WINDOW_MS, `windowMs for ${String(bad)}`);
      assert.equal(writer.freshDedupeMaxKeys, DEFAULT_DEDUPE_MAX_KEYS, `maxKeys for ${String(bad)}`);
    } finally {
      await writer.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  }
});

test('a failed commit does not poison the window (retry still persists the rows)', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    // 存在しないディレクトリへ逃がした DB で書込みを失敗させ、同じ行を再投入する
    const p = marketPath('binance_spot');
    await writer.append([tradeEnvelope('binance_spot', 'trades', 8_000, 'tx-retry')]);
    const before = writer.freshDedupeSummary().trackedIdentities;
    assert.equal(before, 1);
    assert.equal(totalRows(p), 1);
    // 同一行の再投入は「既にコミット済み」なので落ちる (二重書きなし)
    await writer.append([tradeEnvelope('binance_spot', 'trades', 8_000, 'tx-retry', { recvTs: 80_000 })]);
    assert.equal(totalRows(p), 1);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});
