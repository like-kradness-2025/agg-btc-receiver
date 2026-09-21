import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { gunzipSync } from 'node:zlib';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { RawSqliteWriter } from '../lib/raw-sqlite-writer.mjs';

// 同一 flush (1 append) に載った同一 identity の重複配送。
//
// 実測 (2026-09-21 07:44 JST): hyperliquid_perp の再接続 (WS CLOSE 1006) で
// 旧接続 conn:7 と新接続 conn:9 が同じ約定を配送し、両方が同じ append に入って
// 17 組が二重保存された。R-02 の窓は「コミット済み identity」しか見ないため、
// 同一 flush 内の重複は素通りしていた (同一 flush の双子は Kraken 保護のため
// 無条件に保持していた)。
//
// 取引所が一意 ID を割り当てる市場では同一 identity の重複 = 重複配送。
// 指紋 identity の Kraken だけは同値の別約束があり得るため保持する。

function tradeEnvelope(market, stream, eventTs, sourceId, { recvTs = eventTs + 5, connectionId = 'conn-1', price = 100, qty = 1 } = {}) {
  return {
    schema: 'raw_v6_sqlite',
    market,
    stream,
    event_ts_ms: eventTs,
    recv_ts_ms: recvTs,
    connection_id: connectionId,
    writer_session_id: 'test:sqlite',
    source_id: sourceId,
    payload: { price, qty, side: 'buy' },
  };
}

function query(databasePath, sql) {
  const db = new DatabaseSync(databasePath, { readOnly: true });
  try { return db.prepare(sql).all(); } finally { db.close(); }
}

async function tempWriter(options = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-dupinflush-'));
  const writer = await new RawSqliteWriter({ databaseDir: root, ...options }).open();
  return { root, writer, marketPath: (market) => path.join(root, `${market}.sqlite`) };
}

const totalRows = (p) => query(p, 'SELECT coalesce(sum(row_count), 0) AS n FROM raw_batches')[0].n;

function storedEnvelopes(p) {
  const out = [];
  for (const row of query(p, 'SELECT raw_gzip FROM raw_batches ORDER BY batch_id')) {
    for (const line of gunzipSync(row.raw_gzip).toString('utf8').split('\n').filter(Boolean)) out.push(JSON.parse(line));
  }
  return out;
}

test('same identity twice in one flush is dropped for a unique-exchange-id market', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    // 同一約定を 2 つの接続が配送 (recv が 6 秒ずれ、同じ append に載る)。
    await writer.append([
      tradeEnvelope('hyperliquid_perp', 'trades', 1_000, 'tid-1', { recvTs: 1_005, connectionId: 'conn-7' }),
      tradeEnvelope('hyperliquid_perp', 'trades', 1_000, 'tid-1', { recvTs: 6_850, connectionId: 'conn-9' }),
    ]);
    const p = marketPath('hyperliquid_perp');
    assert.equal(totalRows(p), 1, 'the re-delivered row must not be stored twice');
    const summary = writer.freshDedupeSummary();
    assert.equal(summary.sameFlushSkipped, 1);
    assert.equal(summary.skipped, 1);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('same identity twice in one flush is KEPT for a fingerprint-identity market (kraken)', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    // Kraken v1 は trade_id を持たず、1 フレームに同値の別約束が入り得る。
    await writer.append([
      tradeEnvelope('kraken_spot', 'trades', 2_000, 'fp-1', { recvTs: 2_005 }),
      tradeEnvelope('kraken_spot', 'trades', 2_000, 'fp-1', { recvTs: 2_005 }),
    ]);
    const p = marketPath('kraken_spot');
    assert.equal(totalRows(p), 2, 'kraken siblings are distinct prints and must be kept');
    assert.equal(writer.freshDedupeSummary().sameFlushSkipped, 0);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('distinct ids and distinct timestamps are never collapsed', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    await writer.append([
      tradeEnvelope('binance_spot', 'trades', 3_000, 'tx-1'),
      tradeEnvelope('binance_spot', 'trades', 3_000, 'tx-2'),
      tradeEnvelope('binance_spot', 'trades', 3_001, 'tx-1'),
    ]);
    assert.equal(totalRows(marketPath('binance_spot')), 3);
    assert.equal(writer.freshDedupeSummary().skipped, 0);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('rows without an exchange id (depth) are never touched', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    const depth = (ts, recv) => ({
      schema: 'raw_v6_sqlite',
      market: 'binance_spot',
      stream: 'book_updates',
      event_ts_ms: ts,
      recv_ts_ms: recv,
      writer_session_id: 'test:sqlite',
      source_id: null,
      payload: { type: 'update', bids: [['100', '1']], asks: [] },
    });
    await writer.append([depth(4_000, 4_001), depth(4_000, 4_002)]);
    assert.equal(totalRows(marketPath('binance_spot')), 2);
    assert.equal(writer.freshDedupeSummary().skipped, 0);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('cross-append re-delivery is still dropped (R-02 window behaviour unchanged)', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    await writer.append([tradeEnvelope('binance_spot', 'trades', 5_000, 'tx-9', { recvTs: 5_005 })]);
    await writer.append([tradeEnvelope('binance_spot', 'trades', 5_000, 'tx-9', { recvTs: 9_000 })]);
    const summary = writer.freshDedupeSummary();
    assert.equal(totalRows(marketPath('binance_spot')), 1);
    assert.equal(summary.skipped, 1);
    assert.equal(summary.sameFlushSkipped, 0, 'this is a cross-flush re-delivery, not a same-flush duplicate');
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('the kept row is the first delivery and the whole envelope survives', async () => {
  const { root, writer, marketPath } = await tempWriter();
  try {
    await writer.append([
      tradeEnvelope('bybit_perp', 'trades', 6_000, 'tid-keep', { recvTs: 6_010, connectionId: 'conn-1', qty: 0.25 }),
      tradeEnvelope('bybit_perp', 'trades', 6_000, 'tid-keep', { recvTs: 6_700, connectionId: 'conn-2', qty: 0.25 }),
    ]);
    const rows = storedEnvelopes(marketPath('bybit_perp'));
    assert.equal(rows.length, 1);
    assert.equal(rows[0].recv_ts_ms, 6_010, 'the first delivery is the one kept');
    assert.equal(rows[0].payload.qty, 0.25);
  } finally {
    await writer.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});
