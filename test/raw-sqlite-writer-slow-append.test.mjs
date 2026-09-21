import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { RawSqliteWriter } from '../lib/raw-sqlite-writer.mjs';

// 遅い append の内訳計測 (2026-09-21): 実測で raw.append 1 回が 50 秒
// event loop を止めたが、append は gzip / 書きロック待ち / INSERT / 遅着マージを
// すべて含むため、どの区間が支配的かは分からなかった。ここでは内訳報告の
// 契約だけを検証する (実測は本番の観測ログで行う)。

function trade(market, eventTs, recvTs, price = 100) {
  return {
    schema: 'raw_v6_sqlite',
    market,
    stream: 'trades',
    event_ts_ms: eventTs,
    recv_ts_ms: recvTs,
    writer_session_id: 'test:slow-append',
    payload: { price, qty: 1, tradeId: `${eventTs}` },
  };
}

function collector() {
  const reports = [];
  return { reports, observer: { report: (record) => reports.push(record) } };
}

const EXPECTED_FRESH_SPANS = [
  'writer.normalize',
  'writer.partition',
  'writer.gzip_fresh',
  'writer.openDb',
  'writer.market',
  'writer.lockWait',
  'writer.insert',
];

test('slow append は区間内訳つきで observer へ報告される', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-slow-append-'));
  const { reports, observer } = collector();
  const writer = await new RawSqliteWriter({ databaseDir: root, observer, slowAppendMs: 0 }).open();

  await writer.append([
    trade('binance_perp', 1_000, 1_002),
    trade('binance_perp', 1_001, 1_003),
  ]);

  assert.equal(reports.length, 1);
  const record = reports[0];
  assert.equal(record.kind, 'slow_append');
  assert.equal(record.outcome, 'ok');
  assert.equal(record.events, 2);
  assert.equal(record.groups, 1);
  assert.equal(Number.isFinite(record.total_ms), true);
  assert.equal(record.total_ms >= 0, true);
  assert.equal(Number.isFinite(record.queue_wait_ms), true);
  assert.equal(record.queue_wait_ms >= 0, true);

  const names = record.spans.map((span) => span.name);
  for (const expected of EXPECTED_FRESH_SPANS) {
    assert.equal(names.includes(expected), true, `span ${expected} missing: ${names.join(',')}`);
  }
  for (const span of record.spans) {
    assert.equal(Number.isFinite(span.dur_ms), true);
    assert.equal(span.dur_ms >= 0, true);
  }
  // 総時間は必ず個々の区間以上 (区間は total の中に含まれる)。
  const longest = Math.max(...record.spans.map((span) => span.dur_ms));
  assert.equal(record.total_ms + 1 >= longest, true);

  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('閾値未満の append は記録しない (異常時のみの方針)', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-slow-threshold-'));
  const { reports, observer } = collector();
  const writer = await new RawSqliteWriter({
    databaseDir: root, observer, slowAppendMs: 60_000,
  }).open();
  await writer.append([trade('m1', 1_000, 1_002)]);
  assert.deepEqual(reports, []);
  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('observer 無しでも従来どおり書ける (後方互換)', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-slow-none-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([trade('m1', 1_000, 1_002)]);
  const db = new DatabaseSync(path.join(root, 'm1.sqlite'), { readOnly: true });
  const rows = db.prepare('SELECT sum(row_count) AS n FROM raw_batches').all();
  db.close();
  assert.equal(rows[0].n, 1);
  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('遅着マージ (backfill) の内訳も報告される', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-slow-backfill-'));
  const { reports, observer } = collector();
  const writer = await new RawSqliteWriter({ databaseDir: root, observer, slowAppendMs: 0 }).open();

  await writer.append([
    trade('binance_perp', 10_000, 10_002),
    trade('binance_perp', 20_000, 20_002),
  ]);
  reports.length = 0;
  // watermark (20_000) より古く、既存 batch の [10_000,20_000] 内に収まる行は
  // merge 経路 (遅着マージ本体) に入る。
  await writer.append([trade('binance_perp', 15_000, 30_000)]);

  assert.equal(reports.length, 1);
  const names = reports[0].spans.map((span) => span.name);
  for (const expected of [
    'writer.backfill.lockWait',
    'writer.backfill.select',
    'writer.backfill.match',
    'writer.backfill.merge',
    'writer.backfill.commit',
  ]) {
    assert.equal(names.includes(expected), true, `span ${expected} missing: ${names.join(',')}`);
  }

  const db = new DatabaseSync(path.join(root, 'binance_perp.sqlite'), { readOnly: true });
  const counts = db.prepare('SELECT batch_id, first_event_ts_ms, last_event_ts_ms, row_count FROM raw_batches ORDER BY batch_id').all();
  db.close();
  // 遅着行は既存 batch の範囲内へマージされ、batch 数は増えない (batch_id 不変)。
  assert.equal(counts.length, 1);
  assert.equal(Number(counts[0].first_event_ts_ms), 10_000);
  assert.equal(Number(counts[0].last_event_ts_ms), 20_000);
  assert.equal(Number(counts[0].row_count), 3);

  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('report() は probe 経由で onAnomaly へ届く', async () => {
  const { StallProbe } = await import('../lib/stall-probe.mjs');
  const seen = [];
  const probe = new StallProbe({
    label: 'main',
    now: () => 1_700_000_000_000,
    onAnomaly: (record) => seen.push(record),
  });
  const returned = probe.report({ kind: 'slow_append', total_ms: 1234 });
  assert.equal(seen.length, 1);
  assert.equal(seen[0].kind, 'slow_append');
  assert.equal(seen[0].label, 'main');
  assert.equal(seen[0].ts, '2023-11-14T22:13:20.000Z');
  assert.equal(returned.total_ms, 1234);
  probe.stop();
});
