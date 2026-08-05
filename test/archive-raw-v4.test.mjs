import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { eligibleDate, main } from '../scripts/archive-raw-v4.mjs';

function rawLine(payload) {
  return JSON.stringify({
    schema: 'raw_v4',
    market: 'binance_perp',
    stream: 'trades',
    event_ts_ms: payload.ts,
    recv_ts_ms: payload.ts + 5,
    writer_session_id: 'test',
    ingest_seq: null,
    source_id: null,
    payload,
  });
}

test('archives closed raw-v4 segments and removes them only after verification', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'agg-archive-'));
  const data = path.join(root, 'live_v4');
  const archive = path.join(root, 'archive', 'raw_v4');
  const manifests = path.join(root, 'archive', 'manifests');
  const source = path.join(data, 'trades', 'binance_perp', '2026-01-01', '00-00.jsonl');
  fs.mkdirSync(path.dirname(source), { recursive: true });
  fs.writeFileSync(source, `${rawLine({ ts: 1767225600000, price: 1, qty: 2, side: 'buy' })}\n${rawLine({ ts: 1767225601000, price: 2, qty: 3, side: 'sell' })}\n`);

  await main([
    '--data', data,
    '--archive', archive,
    '--manifests', manifests,
    '--raw-retention-hours', '24',
    '--archive-retention-days', '180',
  ], Date.parse('2026-01-03T00:00:00Z'));

  const parquet = path.join(archive, 'trades', 'market=binance_perp', 'date=2026-01-01', '00-00.parquet');
  const manifest = JSON.parse(fs.readFileSync(path.join(manifests, '2026-01-01.json'), 'utf8'));
  assert.equal(fs.existsSync(source), false);
  assert.equal(fs.existsSync(parquet), true);
  assert.equal(manifest.status, 'committed');
  assert.equal(Object.values(manifest.entries)[0].source_rows, 2);

  await main([
    '--data', data,
    '--archive', archive,
    '--manifests', manifests,
    '--archive-retention-days', '180',
  ], Date.parse('2026-08-01T00:00:00Z'));

  assert.equal(fs.existsSync(parquet), false);
  assert.equal(fs.existsSync(path.join(manifests, '2026-01-01.json')), false);
  fs.rmSync(root, { recursive: true, force: true });
});

test('computes the last eligible UTC date after the raw grace period', () => {
  assert.equal(eligibleDate(Date.parse('2026-07-21T08:00:00Z'), 24), '2026-07-19');
});
