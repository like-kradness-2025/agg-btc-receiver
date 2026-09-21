import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { RawSqliteWriter } from '../lib/raw-sqlite-writer.mjs';

// 遅着候補の選択 (2026-09-21): 述語は変えずアクセスパスだけ固定したので、
// 「同じ結果集合を返すこと」と「索引が無い DB では従来経路へ落ちること」を
// 直接検証する。

function trade(market, eventTs, recvTs, tradeId = `${eventTs}`) {
  return {
    schema: 'raw_v6_sqlite',
    market,
    stream: 'trades',
    event_ts_ms: eventTs,
    recv_ts_ms: recvTs,
    writer_session_id: 'test:late-candidates',
    payload: { price: 100, qty: 1, tradeId },
  };
}

const OLD_QUERY = `
SELECT batch_id, first_event_ts_ms, last_event_ts_ms, raw_gzip,
       first_recv_ts_ms, last_recv_ts_ms, written_at_ms
FROM raw_batches
WHERE stream = ? AND first_event_ts_ms <= ? AND last_event_ts_ms >= ?
ORDER BY first_event_ts_ms ASC`;

const FORCED_QUERY = `
SELECT batch_id, first_event_ts_ms, last_event_ts_ms, raw_gzip,
       first_recv_ts_ms, last_recv_ts_ms, written_at_ms
FROM raw_batches INDEXED BY raw_batches_stream_last_event_idx
WHERE stream = ? AND first_event_ts_ms <= ? AND last_event_ts_ms >= ?
ORDER BY first_event_ts_ms ASC`;

async function seed(root, market, eventTimes) {
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  for (const ts of eventTimes) {
    await writer.append([trade(market, ts, ts + 2)]);
  }
  await writer.close();
}

test('アクセスパスを固定しても候補集合は従来クエリと同一 (差分検証)', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-late-diff-'));
  const market = 'm1';
  // 隣接/離散/広い範囲が混在した履歴を作る。
  await seed(root, market, [1_000, 2_000, 3_000, 10_000, 20_000, 20_500, 60_000]);
  const db = new DatabaseSync(path.join(root, `${market}.sqlite`), { readOnly: true });

  const oldStmt = db.prepare(OLD_QUERY);
  const newStmt = db.prepare(FORCED_QUERY);
  const probes = [
    [0, 100_000], [1_000, 1_000], [1_500, 9_000], [10_000, 20_000],
    [20_000, 20_500], [30_000, 59_000], [60_000, 60_000], [999_999, 999_999],
  ];
  for (const [minTs, maxTs] of probes) {
    const oldIds = oldStmt.all(market, maxTs, minTs).map((r) => Number(r.batch_id)).sort((a, b) => a - b);
    const newIds = newStmt.all(market, maxTs, minTs).map((r) => Number(r.batch_id)).sort((a, b) => a - b);
    assert.deepEqual(newIds, oldIds, `probe [${minTs},${maxTs}]`);
  }
  db.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('速い経路が使われ、遅着行は既存 batch へマージされる', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-late-fast-'));
  const reports = [];
  const writer = await new RawSqliteWriter({
    databaseDir: root,
    observer: { report: (record) => reports.push(record) },
    slowAppendMs: 0,
  }).open();

  await writer.append([trade('m1', 10_000, 10_002), trade('m1', 20_000, 20_002)]);
  reports.length = 0;
  await writer.append([trade('m1', 15_000, 30_000)]);

  assert.equal(reports.length, 1);
  const selectSpan = reports[0].spans.find((span) => span.name === 'writer.backfill.select');
  assert.equal(selectSpan.details.path, 'last_event_idx');
  assert.equal(selectSpan.details.candidates, 1);

  const db = new DatabaseSync(path.join(root, 'm1.sqlite'), { readOnly: true });
  const rows = db.prepare('SELECT first_event_ts_ms, last_event_ts_ms, row_count FROM raw_batches').all();
  db.close();
  assert.equal(rows.length, 1);
  assert.equal(Number(rows[0].row_count), 3);
  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('索引の有無でアクセスパスを選ぶ (フォールバック経路の担保)', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-late-fallback-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([trade('m1', 10_000, 10_002)]);
  await writer.close();

  // ライタが作る DB には (stream,last_event_ts_ms) 索引がある → 速い経路。
  const real = new DatabaseSync(path.join(root, 'm1.sqlite'), { readOnly: true });
  assert.equal(writer._hasLastEventIndex(real), true);
  const fast = writer._selectLateCandidates(real, 'trades', 9_000, 11_000);
  assert.equal(fast.path, 'last_event_idx');
  assert.equal(fast.candidates.length, 1);
  real.close();

  // 索引を持たない DB (旧世代の外部作成 DB 等) では従来クエリへ落ちる。
  const barePath = path.join(root, 'bare.sqlite');
  const bare = new DatabaseSync(barePath);
  bare.exec(`CREATE TABLE raw_batches (
    batch_id INTEGER PRIMARY KEY AUTOINCREMENT, schema TEXT, market TEXT, stream TEXT,
    first_event_ts_ms INTEGER, last_event_ts_ms INTEGER, first_recv_ts_ms INTEGER,
    last_recv_ts_ms INTEGER, row_count INTEGER, raw_gzip BLOB, raw_bytes INTEGER,
    written_at_ms INTEGER)`);
  bare.exec("INSERT INTO raw_batches (stream, first_event_ts_ms, last_event_ts_ms, first_recv_ts_ms, last_recv_ts_ms, row_count, raw_gzip, raw_bytes, written_at_ms) VALUES ('trades', 1000, 1010, 1000, 1010, 1, x'00', 0, 0)");
  assert.equal(writer._hasLastEventIndex(bare), false);
  const legacy = writer._selectLateCandidates(bare, 'trades', 9_000, 11_000);
  assert.equal(legacy.path, 'default');
  assert.equal(legacy.candidates.length, 0);
  const hit = writer._selectLateCandidates(bare, 'trades', 1_005, 1_005);
  assert.equal(hit.path, 'default');
  assert.equal(hit.candidates.length, 1);
  bare.close();
  await fs.rm(root, { recursive: true, force: true });
});
