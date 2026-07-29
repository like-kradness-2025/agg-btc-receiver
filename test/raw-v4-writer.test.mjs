import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { RawV4Writer, recoverRawV4Active, RAW_V4_SCHEMA } from '../lib/raw-v4-writer.mjs';
import { RawV4SegmentReader } from '../lib/downstream/raw-v4-segment-reader.mjs';

async function files(root) {
  const out = [];
  for (const day of await readdir(path.join(root, 'trades', 'm'))) {
    for (const f of await readdir(path.join(root, 'trades', 'm', day))) out.push(path.join(day, f));
  }
  return out.sort();
}

function env(payload, eventTs, extra = {}) {
  return {
    schema: RAW_V4_SCHEMA,
    market: 'm',
    stream: 'trades',
    event_ts_ms: eventTs,
    recv_ts_ms: eventTs,
    writer_session_id: 'legacy',
    ingest_seq: null,
    source_id: null,
    payload,
    ...extra,
  };
}

async function readAll(root, market = 'm', kind = 'trades') {
  const reader = new RawV4SegmentReader({ root, market, kind });
  await reader.open();
  const records = [];
  while (true) {
    const r = await reader.read({ maxRecords: 1000 });
    records.push(...r.records);
    if (r.done || (r.eof && r.records.length === 0)) break;
  }
  await reader.close();
  return records;
}

describe('RawV4Writer', () => {
  it('appends envelopes and closes an hourly segment', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const writer = new RawV4Writer({ root, market: 'm', kind: 'trades', now: () => 3_600_000 });
    await writer.append({ ts: 3_600_000, recv_ts_ms: 3_600_000, price: 100 });
    await writer.flush();
    await writer.append({ ts: 7_200_000, recv_ts_ms: 7_200_000, price: 101 });
    await writer.shutdown();
    const names = await files(root);
    assert.equal(names.length, 2);
    assert.ok(names.some(name => name.endsWith('.jsonl')));
    assert.ok(names.some(name => name.endsWith('.jsonl.active')));
    const active = names.find(n => n.endsWith('.active'));
    const line = JSON.parse(await readFile(path.join(root, 'trades', 'm', active), 'utf8'));
    assert.equal(line.schema, RAW_V4_SCHEMA);
  });

  it('rolls on size and repairs an incomplete last line', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const writer = new RawV4Writer({ root, market: 'm', kind: 'trades', maxSegmentBytes: 120 });
    await writer.append({ ts: 1, recv_ts_ms: 1, value: 'a'.repeat(30) });
    await writer.append({ ts: 2, recv_ts_ms: 2, value: 'b'.repeat(30) });
    await writer.shutdown();
    const activeName = (await files(root)).find(name => name.endsWith('.active'));
    const active = path.join(root, 'trades', 'm', activeName);
    await writeFile(active, `${await readFile(active, 'utf8')}{"partial":`, 'utf8');
    const corruptedLength = (await readFile(active)).length;
    const recovered = await recoverRawV4Active(root, 'm', 'trades');
    assert.ok(recovered.bytes < corruptedLength);
    const text = await readFile(active, 'utf8');
    assert.equal(text.endsWith('\n'), true);
    assert.equal(text.includes('partial'), false);
  });

  it('accepts the receiver batch queue shape', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const writer = new RawV4Writer({ root, market: 'm', kind: 'trades' });
    const result = await writer.writeBatch([[{ ts: 123, price: 100 }, 123]]);
    await writer.flush();
    assert.equal(result.written, 1);
    const activeName = (await files(root)).find(name => name.endsWith('.active'));
    const text = await readFile(path.join(root, 'trades', 'm', activeName), 'utf8');
    assert.equal(JSON.parse(text).event_ts_ms, 123);
    await writer.shutdown();
  });

  it('startupRecovery normalizes multiple actives to one and reuses the latest', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const dir = path.join(root, 'trades', 'm', '1970-01-01');
    await mkdir(dir, { recursive: true });
    await writeFile(path.join(dir, '01-00.jsonl.active'), `${JSON.stringify(env({ n: 1 }, 3_600_000))}\n`);
    await writeFile(path.join(dir, '01-01.jsonl.active'), `${JSON.stringify(env({ n: 2 }, 3_700_000))}\n`);

    const writer = new RawV4Writer({ root, market: 'm', kind: 'trades', now: () => 3_800_000 });
    const recovered = await writer.startupRecovery();
    assert.ok(recovered);
    assert.match(recovered.file, /01-01\.jsonl\.active$/);
    await writer.append({ ts: 3_800_000, recv_ts_ms: 3_800_000, n: 3 });
    await writer.shutdown();

    const names = await files(root);
    assert.equal(names.filter(n => n.endsWith('.active')).length, 1);
    assert.equal(names.filter(n => n.endsWith('.jsonl') && !n.endsWith('.active')).length, 1);
    const records = await readAll(root);
    assert.deepEqual(records.map(r => r.payload.n), [1, 2, 3]);
  });

  it('survives consecutive restarts with a single active', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const writer1 = new RawV4Writer({ root, market: 'm', kind: 'trades', now: () => 3_600_000 });
    await writer1.append({ ts: 3_600_000, recv_ts_ms: 3_600_000, n: 1 });
    await writer1.shutdown();

    const writer2 = new RawV4Writer({ root, market: 'm', kind: 'trades', now: () => 3_650_000 });
    await writer2.startupRecovery();
    await writer2.append({ ts: 3_650_000, recv_ts_ms: 3_650_000, n: 2 });
    await writer2.shutdown();

    const writer3 = new RawV4Writer({ root, market: 'm', kind: 'trades', now: () => 3_700_000 });
    await writer3.startupRecovery();
    await writer3.append({ ts: 3_700_000, recv_ts_ms: 3_700_000, n: 3 });
    await writer3.shutdown();

    const names = await files(root);
    assert.equal(names.filter(n => n.endsWith('.active')).length, 1);
    const records = await readAll(root);
    assert.deepEqual(records.map(r => r.payload.n), [1, 2, 3]);
  });

  it('rolls to a new segment after UTC hour rollover during recovery', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const writer1 = new RawV4Writer({ root, market: 'm', kind: 'trades', now: () => 3_600_000 });
    await writer1.append({ ts: 3_600_000, recv_ts_ms: 3_600_000, n: 1 });
    await writer1.shutdown();

    const writer2 = new RawV4Writer({ root, market: 'm', kind: 'trades', now: () => 7_200_000 });
    await writer2.startupRecovery();
    await writer2.append({ ts: 7_200_000, recv_ts_ms: 7_200_000, n: 2 });
    await writer2.shutdown();

    const names = await files(root);
    assert.equal(names.filter(n => n.endsWith('.active')).length, 1);
    assert.equal(names.filter(n => n.endsWith('.jsonl') && !n.endsWith('.active')).length, 1);
    const records = await readAll(root);
    assert.deepEqual(records.map(r => r.payload.n), [1, 2]);
  });

  it('repairs a partial tail during startupRecovery and then appends', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const writer1 = new RawV4Writer({ root, market: 'm', kind: 'trades', now: () => 3_600_000 });
    await writer1.append({ ts: 3_600_000, recv_ts_ms: 3_600_000, n: 1 });
    await writer1.shutdown();
    const activeName = (await files(root)).find(name => name.endsWith('.active'));
    const active = path.join(root, 'trades', 'm', activeName);
    await writeFile(active, '{"partial":', { flag: 'a' });

    const writer2 = new RawV4Writer({ root, market: 'm', kind: 'trades', now: () => 3_650_000 });
    await writer2.startupRecovery();
    const text = await readFile(active, 'utf8');
    assert.equal(text.endsWith('\n'), true);
    assert.equal(text.includes('partial'), false);
    await writer2.append({ ts: 3_650_000, recv_ts_ms: 3_650_000, n: 2 });
    await writer2.shutdown();

    const records = await readAll(root);
    assert.deepEqual(records.map(r => r.payload.n), [1, 2]);
  });

  it('rolls on size and picks the next index within the same hour', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const writer = new RawV4Writer({ root, market: 'm', kind: 'trades', maxSegmentBytes: 1 });
    await writer.append({ ts: 3_600_000, recv_ts_ms: 3_600_000, value: 'a' });
    await writer.append({ ts: 3_700_000, recv_ts_ms: 3_700_000, value: 'b' });
    await writer.shutdown();

    const names = await files(root);
    assert.ok(names.some(n => n.includes('01-00') && n.endsWith('.jsonl') && !n.endsWith('.active')));
    assert.ok(names.some(n => n.includes('01-01') && n.endsWith('.active')));
  });

  it('bounded-memory repairTail truncates and counts rows without loading whole file', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const dir = path.join(root, 'trades', 'm', '1970-01-01');
    await mkdir(dir, { recursive: true });
    const active = path.join(dir, '01-00.jsonl.active');
    const line = (i) => `${JSON.stringify({ seq: i, pad: 'x'.repeat(500) })}
`;
    const totalRows = 2000;
    const validText = Array.from({ length: totalRows }, (_, i) => line(i)).join('');
    await writeFile(active, validText, 'utf8');
    await writeFile(active, '{"partial":', { flag: 'a' });
    const corruptedLength = (await readFile(active)).length;

    const recovered = await recoverRawV4Active(root, 'm', 'trades');
    assert.equal(recovered.bytes, Buffer.byteLength(validText, 'utf8'));
    assert.equal(recovered.rows, totalRows);
    const text = await readFile(active, 'utf8');
    assert.equal(text.endsWith('\n'), true);
    assert.equal(text.includes('partial'), false);
    assert.ok(recovered.bytes < corruptedLength);
  });

  it('startupRecovery refuses to overwrite existing closed segments', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const dir = path.join(root, 'trades', 'm', '1970-01-01');
    await mkdir(dir, { recursive: true });
    await writeFile(path.join(dir, '01-00.jsonl.active'), `${JSON.stringify(env({ n: 1 }, 3_600_000))}\n`);
    await writeFile(path.join(dir, '01-01.jsonl.active'), `${JSON.stringify(env({ n: 2 }, 3_700_000))}\n`);
    // Older active (01-00) would rename to this closed file; must fail-closed.
    await writeFile(path.join(dir, '01-00.jsonl'), `${JSON.stringify(env({ n: 99 }, 3_500_000))}\n`);

    await assert.rejects(
      async () => recoverRawV4Active(root, 'm', 'trades'),
      /Closed segment collision: .* already exists; refusing to overwrite/
    );

    const names = await files(root);
    assert.ok(names.some(n => n.includes('01-00') && n.endsWith('.jsonl.active')));
    assert.ok(names.some(n => n.includes('01-01') && n.endsWith('.jsonl.active')));
  });

  it('startupRecovery rejects invalid active segment names', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'raw-v4-'));
    const dir = path.join(root, 'trades', 'm', '1970-01-01');
    await mkdir(dir, { recursive: true });
    await writeFile(path.join(dir, 'not-a-segment.jsonl.active'), `${JSON.stringify(env({ n: 1 }, 3_600_000))}\n`);

    await assert.rejects(
      async () => recoverRawV4Active(root, 'm', 'trades'),
      /Invalid active segment name:/
    );
  });
});
