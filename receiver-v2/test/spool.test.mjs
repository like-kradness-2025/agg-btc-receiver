import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { createSpool } from '../src/spool.mjs';

const envelope = (seq, connectionId = 'conn-1') =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq}}`,
  });

async function withSpool(fn, options = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'spool-'));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('what goes into the spool comes back out, oldest first and unchanged', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir });
    for (let i = 1; i <= 5; i += 1) assert.equal(spool.append(envelope(i)), true);
    spool.sync();
    const got = [...spool.drain()].filter((v) => v && typeof v === 'object');
    assert.deepEqual(got.map((e) => e.receive_seq), [1, 2, 3, 4, 5]);
    assert.equal(got[2].raw.toString('utf8'), '{"seq":3}', 'the raw bytes are the ones written');
    assert.equal(got[2].recv_ts_ms, 1_792_000_000_003, 'and the receive time travels with them');
    spool.close();
  });
});

test('records spill into a second segment and still come out in order', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir, segmentBytes: 300 });
    for (let i = 1; i <= 12; i += 1) spool.append(envelope(i));
    spool.sync();
    assert.ok(spool.segments.length >= 2, 'a small segment size has to produce more than one');
    const seqs = [...spool.drain()].filter((v) => v && typeof v === 'object').map((e) => e.receive_seq);
    assert.deepEqual(seqs, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    spool.close();
  });
});

test('a record over the bound is refused, not written and not dropped', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir, maxBytes: 400 });
    let accepted = 0;
    for (let i = 1; i <= 20; i += 1) if (spool.append(envelope(i))) accepted += 1;
    assert.ok(accepted > 0 && accepted < 20, 'the bound stopped it partway');
    // The bound is a ceiling on what is held, checked before each write, so it is normal for a
    // refusal to arrive while the spool is still under it: the next record would have crossed it.
    assert.ok(spool.bytes <= 400, 'the bound held');
    const before = spool.bytes;
    assert.equal(spool.append(envelope(99)), false, 'false is the stop signal for the caller');
    assert.equal(spool.bytes, before, 'and the refused record was not written');
    spool.close();
  });
});

test('a consumer that confirmed a position does not get that data again', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir });
    for (let i = 1; i <= 4; i += 1) spool.append(envelope(i));
    spool.sync();
    const first = [...spool.drain()].filter((v) => v && typeof v === 'object');
    assert.deepEqual(first.map((e) => e.receive_seq), [1, 2, 3, 4]);
    // Confirm through the end of segment 1: the cursor names the next thing to read, so segments
    // strictly before it are released. offset 0 in segment 2 means "everything before 2 is done".
    spool.advance({ segment: 2, offset: 0 });
    assert.deepEqual(spool.segments, [], 'confirmed data is released so the spool stays bounded');
    const again = [...spool.drain()].filter((v) => v && typeof v === 'object');
    assert.deepEqual(again, [], 'nothing is handed out twice');
    spool.close();
  });
});

test('the spool survives a restart with its segments and its cursor', async () => {
  await withSpool(async (dir) => {
    const first = createSpool({ dir });
    for (let i = 1; i <= 3; i += 1) first.append(envelope(i));
    first.sync();
    first.close();

    const second = createSpool({ dir });
    assert.equal(second.bytes > 0, true, 'what was waiting here is still waiting');
    const seqs = [...second.drain()].filter((v) => v && typeof v === 'object').map((e) => e.receive_seq);
    assert.deepEqual(seqs, [1, 2, 3]);
    second.close();
  });
});

test('a write that throws is accounted for exactly like one that returns zero', async () => {
  await withSpool(async (dir) => {
    const fsModule = { ...fs };
    let calls = 0;
    fsModule.writeSync = (fd, buf, off, len) => {
      calls += 1;
      if (calls > 1) throw new Error('EIO: i/o error'); // the syscall itself fails
      return fs.writeSync(fd, buf, off, Math.min(len, 2));
    };
    const spool = createSpool({ dir, fsModule });
    assert.throws(() => spool.append(envelope(1)), /EIO/);
    assert.notEqual(spool.failed, null, 'the spool still knows it holds a torn record');
    assert.equal(spool.bytes, 2, 'and still counts only what reached the disk');
    assert.equal(spool.append(envelope(2)), false, 'so nothing is appended after the tear');
  });
});

test('a torn write is counted honestly, and the spool takes nothing more', async () => {
  await withSpool(async (dir) => {
    const fsModule = { ...fs };
    let calls = 0;
    fsModule.writeSync = (fd, buf, off, len) => {
      calls += 1;
      if (calls > 1) return 0; // the disk fills up after a couple of bytes
      return fs.writeSync(fd, buf, off, Math.min(len, 2));
    };
    const spool = createSpool({ dir, fsModule });
    assert.throws(() => spool.append(envelope(1)), /no progress/);
    assert.notEqual(spool.failed, null, 'the spool knows it holds a torn record');
    assert.equal(spool.bytes, 2, 'only the bytes that reached the disk are counted');
    assert.equal(spool.append(envelope(2)), false, 'and it does not take anything more');
  });
});

test('a torn tail is reported, not parsed as a record', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir });
    for (let i = 1; i <= 3; i += 1) spool.append(envelope(i));
    spool.sync();
    spool.close();
    // Simulate a process that died mid-write: half a record appended after the last complete one.
    const segment = join(dir, 'segment-0000000001.spool');
    fs.appendFileSync(segment, Buffer.from([0x00, 0x00, 0x01, 0x00, 0x7b]));

    const reopened = createSpool({ dir });
    const seen = [];
    let torn = null;
    const iterator = reopened.drain();
    for (;;) {
      const step = iterator.next();
      if (step.done) {
        torn = step.value?.torn ?? null;
        break;
      }
      seen.push(step.value.receive_seq);
    }
    assert.deepEqual(seen, [1, 2, 3], 'the complete records are still handed out');
    assert.ok(torn, 'the incomplete tail is reported rather than guessed at');
    reopened.close();
  });
});
