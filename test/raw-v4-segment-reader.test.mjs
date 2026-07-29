import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rename, mkdir, readFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { RawV4SegmentReader, RAW_V4_SCHEMA } from '../lib/downstream/raw-v4-segment-reader.mjs';

function envelope(payload, eventTs, extra = {}) {
  return {
    schema: RAW_V4_SCHEMA,
    market: 'm',
    stream: 'trades',
    event_ts_ms: eventTs,
    recv_ts_ms: eventTs,
    writer_session_id: 'test',
    ingest_seq: null,
    source_id: null,
    payload,
    ...extra,
  };
}

async function writeSegment(root, kind, market, date, segment, records, active = false) {
  const dir = path.join(root, kind, market, date);
  await mkdir(dir, { recursive: true });
  const text = records.map(r => `${JSON.stringify(r)}\n`).join('');
  const ext = active ? '.jsonl.active' : '.jsonl';
  await writeFile(path.join(dir, `${segment}${ext}`), text);
}

async function appendBytes(root, kind, market, date, segment, active, bytes) {
  const dir = path.join(root, kind, market, date);
  const ext = active ? '.jsonl.active' : '.jsonl';
  const file = path.join(dir, `${segment}${ext}`);
  await writeFile(file, bytes, { flag: 'a' });
}

async function collect(reader, { maxRecords = 1000 } = {}) {
  const all = [];
  let last;
  while (true) {
    const result = await reader.read({ maxRecords });
    all.push(...result.records);
    last = result;
    if (result.done || (result.eof && result.records.length === 0)) break;
  }
  return { records: all, last };
}

describe('RawV4SegmentReader', () => {
  it('reads a closed segment and returns payload + event_ts_ms', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    await writeSegment(root, 'trades', 'm', '1970-01-01', '01-00', [
      envelope({ price: 100 }, 3_600_000),
      envelope({ price: 101 }, 7_200_000),
    ]);

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const result = await reader.read();
    await reader.close();

    assert.equal(result.records.length, 2);
    assert.equal(result.records[0].event_ts_ms, 3_600_000);
    assert.deepEqual(result.records[0].payload, { price: 100 });
    assert.equal(result.records[1].event_ts_ms, 7_200_000);
    assert.equal(result.eof, true);
    assert.equal(result.done, true);
  });

  it('enumerates segments chronologically across dates and indices', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    // Out-of-order creation on disk.
    await writeSegment(root, 'trades', 'm', '1970-01-02', '02-00', [envelope({ n: 4 }, 86_400_000 * 2 + 7_200_000)]);
    await writeSegment(root, 'trades', 'm', '1970-01-01', '23-01', [envelope({ n: 2 }, 86_400_000 + 3_600_000)]);
    await writeSegment(root, 'trades', 'm', '1970-01-01', '23-00', [envelope({ n: 1 }, 86_400_000)]);
    await writeSegment(root, 'trades', 'm', '1970-01-02', '01-00', [envelope({ n: 3 }, 86_400_000 * 2)]);

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const { records } = await collect(reader, { maxRecords: 1 });
    await reader.close();

    assert.deepEqual(records.map(r => r.payload.n), [1, 2, 3, 4]);
  });

  it('treats active and closed as one logical segment', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    await writeSegment(root, 'trades', 'm', '1970-01-01', '01-00', [envelope({ n: 1 }, 3_600_000)]);
    await writeSegment(root, 'trades', 'm', '1970-01-01', '01-00', [envelope({ n: 2 }, 7_200_000)], true);

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const { records, last } = await collect(reader, { maxRecords: 1 });
    await reader.close();

    assert.deepEqual(records.map(r => r.payload.n), [2]);
    assert.equal(last.eof, true);
    assert.equal(last.done, false); // active segment may still grow
  });

  it('does not advance past an active segment at EOF', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    await writeSegment(root, 'trades', 'm', '1970-01-01', '01-00', [envelope({ n: 1 }, 3_600_000)], true);
    await writeSegment(root, 'trades', 'm', '1970-01-01', '02-00', [envelope({ n: 2 }, 10_800_000)]);

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const first = await reader.read({ maxRecords: 1 });
    assert.equal(first.records.length, 1);
    assert.equal(first.records[0].payload.n, 1);
    assert.equal(first.eof, true);
    assert.equal(first.done, false);

    const second = await reader.read({ maxRecords: 1 });
    assert.equal(second.records.length, 0);
    assert.equal(second.eof, true);
    assert.equal(second.done, false);
    await reader.close();
  });

  it('advances from active to closed rename and then to the next segment', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    const dateDir = path.join(root, 'trades', 'm', '1970-01-01');
    await writeSegment(root, 'trades', 'm', '1970-01-01', '01-00', [envelope({ n: 1 }, 3_600_000)], true);
    await writeSegment(root, 'trades', 'm', '1970-01-01', '02-00', [envelope({ n: 2 }, 10_800_000)]);

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const first = await reader.read({ maxRecords: 1 });
    assert.equal(first.records[0].payload.n, 1);
    assert.equal(first.eof, true);

    // Rename active to closed mid-read.
    await rename(path.join(dateDir, '01-00.jsonl.active'), path.join(dateDir, '01-00.jsonl'));

    const second = await collect(reader, { maxRecords: 1 });
    await reader.close();

    assert.deepEqual(second.records.map(r => r.payload.n), [2]);
    assert.equal(second.last.done, true);
  });

  it('preserves a partial final line across reads on an active segment', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    const dateDir = path.join(root, 'trades', 'm', '1970-01-01');
    await mkdir(dateDir, { recursive: true });
    const activeFile = path.join(dateDir, '01-00.jsonl.active');
    const line1 = JSON.stringify(envelope({ n: 1 }, 3_600_000));
    const line2 = JSON.stringify(envelope({ n: 2 }, 7_200_000));
    const splitAt = Math.floor(line2.length / 2);
    await writeFile(activeFile, `${line1}\n${line2.slice(0, splitAt)}`);

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const first = await reader.read();
    assert.equal(first.records.length, 1);
    assert.equal(first.records[0].payload.n, 1);
    assert.equal(first.eof, true);
    assert.equal(first.done, false);

    // Complete the dangling partial line.
    await appendBytes(root, 'trades', 'm', '1970-01-01', '01-00', true, line2.slice(splitAt) + '\n');

    const second = await reader.read();
    assert.equal(second.records.length, 1);
    assert.equal(second.records[0].payload.n, 2);
    assert.equal(second.eof, true);
    assert.equal(second.done, false);
    await reader.close();
  });

  it('discards a dangling partial line at closed EOF', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    const dateDir = path.join(root, 'trades', 'm', '1970-01-01');
    await mkdir(dateDir, { recursive: true });
    const closedFile = path.join(dateDir, '01-00.jsonl');
    const line1 = JSON.stringify(envelope({ n: 1 }, 3_600_000));
    await writeFile(closedFile, `${line1}\n{"partial":`);
    await writeSegment(root, 'trades', 'm', '1970-01-01', '02-00', [envelope({ n: 2 }, 7_200_000)]);

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const { records, last } = await collect(reader, { maxRecords: 1 });
    await reader.close();

    assert.deepEqual(records.map(r => r.payload.n), [1, 2]);
    assert.equal(last.done, true);
  });

  it('returns an atomic cursor that can resume without re-reading earlier bytes', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    await writeSegment(root, 'trades', 'm', '1970-01-01', '01-00', [
      envelope({ n: 1 }, 3_600_000),
      envelope({ n: 2 }, 7_200_000),
      envelope({ n: 3 }, 10_800_000),
    ]);

    const reader1 = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader1.open();
    const first = await reader1.read({ maxRecords: 1 });
    await reader1.close();

    assert.equal(first.records.length, 1);
    const cursor = JSON.parse(first.cursor);
    assert.equal(cursor.date, '1970-01-01');
    assert.equal(cursor.segment, '01-00');
    assert.ok(cursor.byte_offset > 0);

    const reader2 = new RawV4SegmentReader({ root, market: 'm', kind: 'trades', cursor: first.cursor });
    await reader2.open();
    const { records, last } = await collect(reader2, { maxRecords: 1 });
    await reader2.close();

    assert.deepEqual(records.map(r => r.payload.n), [2, 3]);
    assert.equal(last.done, true);
  });

  it('does not read full files when resuming from a cursor', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    await writeSegment(root, 'trades', 'm', '1970-01-01', '01-00', [
      envelope({ n: 1 }, 3_600_000),
      envelope({ n: 2 }, 7_200_000),
    ]);

    const reader1 = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader1.open();
    const first = await reader1.read({ maxRecords: 1 });
    await reader1.close();

    assert.equal(first.records.length, 1);
    const cursorOffset = JSON.parse(first.cursor).byte_offset;

    // The first record must end before the cursor; the second record starts at/after the cursor.
    const file = path.join(root, 'trades', 'm', '1970-01-01', '01-00.jsonl');
    const fileSize = (await readFile(file)).length;
    assert.ok(cursorOffset > 0);
    assert.ok(cursorOffset < fileSize);

    const reader2 = new RawV4SegmentReader({ root, market: 'm', kind: 'trades', cursor: first.cursor });
    await reader2.open();
    const { records } = await collect(reader2, { maxRecords: 1 });
    await reader2.close();

    assert.equal(records.length, 1);
    assert.equal(records[0].payload.n, 2);
  });

  it('returns empty when no segments exist', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    await mkdir(path.join(root, 'trades', 'm', '1970-01-01'), { recursive: true });

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const result = await reader.read();
    await reader.close();

    assert.equal(result.records.length, 0);
    assert.equal(result.eof, true);
    assert.equal(result.done, true);
  });

  it('skips malformed and non-raw_v4 lines', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    const dateDir = path.join(root, 'trades', 'm', '1970-01-01');
    await mkdir(dateDir, { recursive: true });
    const lines = [
      JSON.stringify(envelope({ n: 1 }, 3_600_000)),
      'not valid json',
      JSON.stringify({ schema: 'other', event_ts_ms: 4_000_000, payload: {} }),
      JSON.stringify(envelope({ n: 2 }, 7_200_000)),
    ].join('\n') + '\n';
    await writeFile(path.join(dateDir, '01-00.jsonl'), lines);

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const { records } = await collect(reader, { maxRecords: 1 });
    await reader.close();

    assert.deepEqual(records.map(r => r.payload.n), [1, 2]);
  });

  it('handles an empty active segment without crashing', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    const dateDir = path.join(root, 'trades', 'm', '1970-01-01');
    await mkdir(dateDir, { recursive: true });
    await writeFile(path.join(dateDir, '01-00.jsonl.active'), '');

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const first = await reader.read();
    assert.equal(first.records.length, 0);
    assert.equal(first.eof, true);
    assert.equal(first.done, false);
    await reader.close();
  });

  it('advances past a stale active segment when later segments exist', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    // Multiple active segments violate the single-active contract; the reader
    // must still recover and reach the later active segment.
    await writeSegment(root, 'trades', 'm', '1970-01-01', '01-00', [envelope({ n: 1 }, 3_600_000)], true);
    await writeSegment(root, 'trades', 'm', '1970-01-01', '01-01', [envelope({ n: 2 }, 3_700_000)], true);

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades' });
    await reader.open();
    const { records, last } = await collect(reader, { maxRecords: 1 });
    await reader.close();

    assert.deepEqual(records.map(r => r.payload.n), [1, 2]);
    assert.equal(last.done, false);
  });

  it('resumes from a cursor whose segment was deleted by moving to the next segment', async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'rv4sr-'));
    await writeSegment(root, 'trades', 'm', '1970-01-01', '01-00', [envelope({ n: 1 }, 3_600_000)]);
    await writeSegment(root, 'trades', 'm', '1970-01-01', '02-00', [envelope({ n: 2 }, 7_200_000)]);

    const cursor = JSON.stringify({
      schema_version: 'raw_v4_segment_cursor_v1',
      date: '1970-01-01',
      segment: '00-00',
      byte_offset: 0,
    });

    const reader = new RawV4SegmentReader({ root, market: 'm', kind: 'trades', cursor });
    await reader.open();
    const { records, last } = await collect(reader, { maxRecords: 1 });
    await reader.close();

    assert.deepEqual(records.map(r => r.payload.n), [1, 2]);
    assert.equal(last.done, true);
  });
});
