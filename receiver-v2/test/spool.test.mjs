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
    for (let i = 1; i <= 5; i += 1) {
      const position = spool.append(envelope(i));
      assert.equal(Number.isInteger(position?.segment), true, 'a written record returns where it ends');
      assert.equal(Number.isInteger(position?.offset), true, 'with the offset just past it');
    }
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
    // The synchronous mode: every advance saves at once, which is the frame-by-frame contract this
    // test pins. The deferred mode (the default) is exercised by its own tests below.
    const spool = createSpool({ dir, cursorSaveMs: 0 });
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
    // Set 8c cuts a tear it finds at reopen, so the tear has to appear *during* the run to exercise
    // the walk's own verdict: this is a read-time tear, not a crash.
    const segment = join(dir, 'segment-0000000001.spool');

    const reopened = createSpool({ dir });
    fs.appendFileSync(segment, Buffer.from([0x00, 0x00, 0x01, 0x00, 0x7b]));
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

test('drainRecords gives the position after each record, and advancing there resumes cleanly', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir, segmentBytes: 300 });
    for (let i = 1; i <= 6; i += 1) spool.append(envelope(i));
    spool.sync();
    const records = [...spool.drainRecords()];
    assert.deepEqual(records.map((record) => record.envelope.receive_seq), [1, 2, 3, 4, 5, 6]);
    for (const record of records) {
      assert.ok(Number.isInteger(record.segment), 'a walked record knows which segment it is in');
      assert.ok(Number.isInteger(record.offset) && record.offset >= 0, 'and where it ends');
    }
    // Confirm through the third record: the cursor names where the fourth begins, and the walk
    // continues from there - nothing before the confirmed position comes back.
    const third = records[2];
    spool.advance({ segment: third.segment, offset: third.offset });
    assert.deepEqual(
      [...spool.drainRecords()].map((record) => record.envelope.receive_seq),
      [4, 5, 6],
      'the position after a record is exactly where its successor resumes',
    );
    spool.close();
  });
});

test('append returns the position just past the record, exactly as a walk reports it', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir, segmentBytes: 300 });
    const written = [];
    for (let i = 1; i <= 12; i += 1) {
      const position = spool.append(envelope(i));
      assert.equal(Number.isInteger(position?.segment), true, 'a written record returns its segment');
      assert.equal(Number.isInteger(position?.offset), true, 'and the offset just past it');
      written.push(position);
    }
    spool.sync();
    const walked = [...spool.drainRecords()].map((record) => ({ segment: record.segment, offset: record.offset }));
    assert.deepEqual(walked, written, 'the walk reports the same raw positions the appends returned');
    spool.close();
  });
});

test('a walk may start at a position of its own, ahead of the release cursor', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir });
    for (let i = 1; i <= 4; i += 1) spool.append(envelope(i));
    spool.sync();
    const records = [...spool.drainRecords()];
    assert.equal(records.length, 4, 'the whole spool reads');
    // Start past the first record: the walk yields from there, and the release cursor is untouched.
    const from = { segment: records[0].segment, offset: records[0].offset };
    const tail = [...spool.drainRecords({ from })];
    assert.deepEqual(
      tail.map((record) => record.envelope.receive_seq),
      [2, 3, 4],
      'the walk starts where it was told to',
    );
    assert.deepEqual(spool.cursor, { segment: null, offset: 0 }, 'and moves no cursor');
    // The whole spool is still readable from the cursor: `from` decides the reading start only.
    assert.deepEqual(
      [...spool.drainRecords()].map((record) => record.envelope.receive_seq),
      [1, 2, 3, 4],
    );
  });
});

test('a walk from a released segment starts at the beginning of the next segment', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir, segmentBytes: 300 });
    for (let i = 1; i <= 12; i += 1) spool.append(envelope(i));
    spool.sync();
    const records = [...spool.drainRecords()];
    const stalePosition = { segment: records[0].segment, offset: records[0].offset };
    // Release the first segment entirely: the position now names a segment that is gone.
    const firstSegmentRecords = records.filter((record) => record.segment === records[0].segment);
    spool.advance({ segment: firstSegmentRecords.at(-1).segment, offset: firstSegmentRecords.at(-1).offset });
    const remaining = [...spool.drainRecords()].map((record) => record.envelope.receive_seq);
    assert.ok(remaining.length > 0, 'records after the released segment remain');
    // The stale position's offset must not eat the next segment's beginning, and the positions the
    // walk reports must be the segment's own - a consumer resuming from a shifted one would skip
    // the bytes the stale offset named.
    const fromStaleRecords = [...spool.drainRecords({ from: stalePosition })];
    assert.deepEqual(
      fromStaleRecords.map((record) => record.envelope.receive_seq),
      remaining,
      'the walk starts at the next segment whole, not mid-way',
    );
    const normalRecords = [...spool.drainRecords()];
    assert.deepEqual(
      fromStaleRecords.map((record) => ({ segment: record.segment, offset: record.offset })),
      normalRecords.map((record) => ({ segment: record.segment, offset: record.offset })),
      'and reports the same positions the segment itself reports',
    );
    spool.close();
  });
});

test('a position that was an end when it was read is not an end once the segment has grown', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir });
    spool.append(envelope(1));
    spool.sync();
    const first = [...spool.drainRecords()].at(-1); // the segment's end as of this read
    spool.append(envelope(2)); // the segment grows after the position was taken
    spool.advance({ segment: first.segment, offset: first.offset });
    assert.deepEqual(
      spool.cursor,
      { segment: first.segment, offset: first.offset },
      'an end is only an end at the moment the advance uses it',
    );
    assert.equal(spool.segments.length, 1, 'the segment was not released');
    assert.deepEqual(
      [...spool.drainRecords()].map((record) => record.envelope.receive_seq),
      [2],
      'the record written after the position was taken is still readable',
    );
    spool.close();
  });
});

test('a fully consumed segment is released, and a record after the drain starts ahead of the cursor', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir, cursorSaveMs: 0 });
    for (const seq of [1, 2, 3]) spool.append(envelope(seq));
    spool.sync();
    const records = [...spool.drainRecords()];
    const last = records.at(-1);
    // The position is raw - the end of the last record's bytes - and the advance is where an end
    // becomes a release: at this moment it is still the end of the segment, so the cursor
    // normalizes to the start of the next segment and the whole segment is released.
    spool.advance({ segment: last.segment, offset: last.offset });
    assert.deepEqual(
      spool.cursor,
      { segment: last.segment + 1, offset: 0 },
      'an end advances to the start of the next segment',
    );
    assert.deepEqual(spool.segments, [], 'the whole consumed segment is released');
    assert.equal(spool.bytes, 0, 'and nothing is counted as held');
    assert.deepEqual([...spool.drainRecords()], [], 'what was confirmed does not come back');

    // A record arriving after a complete drain must land where the cursor can still read it: at or
    // after the confirmed position, never inside the released segment behind it.
    assert.notEqual(spool.append(envelope(4)), false, 'the spool still takes records');
    spool.sync();
    assert.deepEqual(
      [...spool.drainRecords()].map((record) => record.envelope.receive_seq),
      [4],
      'the new record is readable from the cursor, not written behind it',
    );
    spool.close();
  });
});

test('the free-space half of the bound refuses a record and deletes nothing', async () => {
  await withSpool(async (dir) => {
    // The bound is the earlier of 5% of free space or the fixed ceiling: 5% of 10_000 is 500 bytes,
    // well below the 2GB ceiling, so the free-space half is the one that applies. The reading is
    // injected so the condition is forced without filling a disk.
    const spool = createSpool({ dir, freeSpace: () => 10_000 });
    let accepted = 0;
    for (let i = 1; i <= 50; i += 1) if (spool.append(envelope(i))) accepted += 1;
    assert.ok(accepted > 0, 'the bound lets records through while they fit');
    assert.ok(accepted < 50, 'and stops before 5% of the free space is crossed');
    assert.ok(spool.bytes <= 500, 'what is held stays within the free-space share');
    const held = spool.segments.map((segment) => segment.name);
    const before = spool.bytes;
    assert.equal(spool.append(envelope(999)), false, 'false is the stop signal for the caller');
    assert.equal(spool.bytes, before, 'the refused record was not written');
    assert.deepEqual(spool.segments.map((segment) => segment.name), held, 'and nothing held was deleted');
    assert.deepEqual(
      [...spool.drainRecords()].map((record) => record.envelope.receive_seq),
      Array.from({ length: accepted }, (_, i) => i + 1),
      'a refusal never discards what is already spooled',
    );
    spool.close();
  });
});

// ------------------------------------------------------------------------------------------------
// Set 6b: the cursor save is deferred. The position moves in memory at the advance; the file write,
// its fsync and the segment deletion behind it happen on the save clock - or at an explicit save.
// ------------------------------------------------------------------------------------------------
function fakeClocks() {
  const timers = [];
  return {
    timers,
    setTimer: (fn, ms) => {
      const timer = { fn, ms, cleared: false, unref() {} };
      timers.push(timer);
      return timer;
    },
    clearTimer: (timer) => {
      timer.cleared = true;
    },
    fire: (ms) => {
      const timer = timers.find((entry) => entry.ms === ms && !entry.cleared);
      assert.ok(timer, `a timer of ${ms} ms was armed`);
      timer.cleared = true;
      timer.fn();
    },
  };
}

test('Set 6b: an advance moves the position in memory and leaves the file and the segments to the clock', async () => {
  await withSpool(async (dir) => {
    const clocks = fakeClocks();
    const spool = createSpool({ dir, cursorSaveMs: 100, setTimer: clocks.setTimer, clearTimer: clocks.clearTimer });
    for (let i = 1; i <= 4; i += 1) spool.append(envelope(i));
    spool.sync();
    spool.advance({ segment: 2, offset: 0 });
    assert.deepEqual(spool.cursor, { segment: 2, offset: 0 }, 'the position moved in memory at once');
    assert.equal(fs.existsSync(join(dir, 'cursor')), false, 'the file has not been written yet');
    assert.equal(spool.segments.length, 1, 'and nothing was deleted yet');
    assert.ok(spool.bytes > 0, 'the spool still counts what it holds');

    clocks.fire(100);
    assert.deepEqual(
      JSON.parse(fs.readFileSync(join(dir, 'cursor'), 'utf8')),
      { segment: 2, offset: 0 },
      'the clock wrote the position down',
    );
    assert.deepEqual(spool.segments, [], 'and only then released the segment behind it');
    assert.equal(spool.bytes, 0, 'and nothing is counted as held');
    spool.close();
  });
});

test('Set 6b: the save writes the file before it deletes anything, and a failed write deletes nothing', async () => {
  await withSpool(async (dir) => {
    // The ordering is observed where it matters: at the moment of the first unlink, the file must
    // already name the position the deletion is justified by.
    const observations = [];
    const events = [];
    let failCursorWrite = false;
    let shortWrite = false;
    const fsModule = {
      ...fs,
      unlinkSync: (file) => {
        observations.push(fs.readFileSync(join(dir, 'cursor'), 'utf8'));
        events.push('unlink');
        return fs.unlinkSync(file);
      },
      renameSync: (from, to) => {
        events.push('rename');
        return fs.renameSync(from, to);
      },
      openSync: (file, flags) => {
        if (failCursorWrite && file.includes('cursor')) throw new Error('EIO: the cursor file will not open');
        if (file === dir) events.push('dir-fsync-open');
        return fs.openSync(file, flags);
      },
      writeSync: (fd, buf, ...rest) => {
        if (shortWrite) return 4; // a write that stops part-way
        return fs.writeSync(fd, buf, ...rest);
      },
    };
    const spool = createSpool({ dir, cursorSaveMs: 0, fsModule });
    for (let i = 1; i <= 4; i += 1) spool.append(envelope(i));
    spool.sync();
    // A first save puts a position on disk, so the failure below has an old file to protect.
    spool.advance({ segment: 1, offset: 0 });

    failCursorWrite = true;
    assert.throws(() => spool.advance({ segment: 2, offset: 0 }), /EIO/);
    assert.equal(fs.readFileSync(join(dir, 'cursor'), 'utf8'), '{"segment":1,"offset":0}', 'the old file is intact');
    assert.equal(spool.segments.length, 1, 'a save that failed deleted nothing');
    assert.ok(spool.bytes > 0, 'and the spool still holds what it held');

    failCursorWrite = false;
    shortWrite = true;
    assert.throws(() => spool.saveCursor(), /partly written/);
    assert.equal(fs.readFileSync(join(dir, 'cursor'), 'utf8'), '{"segment":1,"offset":0}', 'a short write leaves the old file too');
    assert.equal(spool.segments.length, 1, 'and deletes nothing either');

    shortWrite = false;
    spool.saveCursor();
    assert.deepEqual(JSON.parse(fs.readFileSync(join(dir, 'cursor'), 'utf8')), { segment: 2, offset: 0 });
    assert.deepEqual(spool.segments, [], 'the retry released the segment');
    assert.deepEqual(observations, ['{"segment":2,"offset":0}'], 'at the deletion, the file already named the position');
    // The durability order: the rename is made durable by the directory fsync before any deletion
    // runs - without it a power loss could leave the old cursor with the segments it released.
    const renameAt = events.indexOf('rename');
    const dirFsyncAt = events.indexOf('dir-fsync-open');
    const unlinkAt = events.indexOf('unlink');
    assert.ok(renameAt !== -1 && dirFsyncAt !== -1 && unlinkAt !== -1, 'the save renamed, fsynced the directory and deleted');
    assert.ok(renameAt < dirFsyncAt && dirFsyncAt < unlinkAt, 'rename, then the directory fsync, then the deletion');
    spool.close();
  });
});

test('Set 6b: an advance to the position already confirmed saves nothing', async () => {
  await withSpool(async (dir) => {
    const clocks = fakeClocks();
    const spool = createSpool({ dir, cursorSaveMs: 100, setTimer: clocks.setTimer, clearTimer: clocks.clearTimer });
    for (let i = 1; i <= 4; i += 1) spool.append(envelope(i));
    spool.sync();
    spool.advance({ segment: 2, offset: 0 });
    spool.saveCursor();
    clocks.timers.length = 0;
    spool.advance({ segment: 2, offset: 0 });
    assert.equal(clocks.timers.filter((timer) => !timer.cleared).length, 0, 'the same position arms no save');
    spool.close();
  });
});

test('Set 6b: a crash with an unsaved position re-walks the records instead of losing them', async () => {
  await withSpool(async (dir) => {
    const clocks = fakeClocks();
    const first = createSpool({ dir, cursorSaveMs: 100, setTimer: clocks.setTimer, clearTimer: clocks.clearTimer });
    for (let i = 1; i <= 4; i += 1) first.append(envelope(i));
    first.sync();
    // The advance moves the position in memory, and the clock that would have saved it never fires:
    // this is the crash window, and the segments must still be there for the next life.
    first.advance({ segment: 2, offset: 0 });
    assert.equal(fs.existsSync(join(dir, 'cursor')), false, 'nothing was saved');

    const second = createSpool({ dir, cursorSaveMs: 0 });
    assert.deepEqual(second.cursor, { segment: null, offset: 0 }, 'the restart knows nothing was confirmed');
    assert.deepEqual(
      [...second.drainRecords()].map((record) => record.envelope.receive_seq),
      [1, 2, 3, 4],
      'everything still present is walked and offered again',
    );

    // With the position saved, the same crash resumes past it instead.
    first.saveCursor();
    const third = createSpool({ dir, cursorSaveMs: 0 });
    assert.deepEqual(third.cursor, { segment: 2, offset: 0 }, 'the saved position is where the restart resumes');
    assert.deepEqual(third.segments, [], 'and the released segment is gone');
    assert.deepEqual([...third.drainRecords()], [], 'nothing is walked back out of it');
    first.close();
    second.close();
    third.close();
  });
});

test('Set 6b: a save that fails on the clock is retried instead of taking the process down', async () => {
  await withSpool(async (dir) => {
    const clocks = fakeClocks();
    let dirFd = null;
    let failDirFsync = false;
    const fsModule = {
      ...fs,
      openSync: (file, flags) => {
        const fd = fs.openSync(file, flags);
        if (file === dir) dirFd = fd;
        return fd;
      },
      fsyncSync: (fd) => {
        if (failDirFsync && fd === dirFd) throw new Error('EIO: the directory fsync failed');
        return fs.fsyncSync(fd);
      },
    };
    const spool = createSpool({
      dir,
      cursorSaveMs: 100,
      setTimer: clocks.setTimer,
      clearTimer: clocks.clearTimer,
      fsModule,
    });
    for (let i = 1; i <= 4; i += 1) spool.append(envelope(i));
    spool.sync();
    spool.advance({ segment: 2, offset: 0 });

    // The clock fires into a failing directory fsync: the spool must not throw out of its own
    // timer (an uncaught exception here is a process that exits, and nothing restarts it), and the
    // position must stay unsaved with the segments untouched.
    failDirFsync = true;
    clocks.fire(100);
    assert.equal(spool.segments.length, 1, 'a failed save deleted nothing');
    assert.ok(spool.bytes > 0, 'and the spool still holds what it held');
    assert.equal(
      clocks.timers.filter((timer) => timer.ms === 100 && !timer.cleared).length,
      1,
      'the retry clock was re-armed',
    );

    // The failure clears: the retry finishes the save, deletion included.
    failDirFsync = false;
    clocks.fire(100);
    assert.deepEqual(JSON.parse(fs.readFileSync(join(dir, 'cursor'), 'utf8')), { segment: 2, offset: 0 });
    assert.deepEqual(spool.segments, [], 'the retried save released the segment');
    assert.equal(
      clocks.timers.filter((timer) => timer.ms === 100 && !timer.cleared).length,
      0,
      'and no further retry is armed',
    );
    spool.close();
  });
});

test('Set 8c: a torn tail at reopen makes the spool refuse to append rather than bury it', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir });
    for (let i = 1; i <= 3; i += 1) spool.append(envelope(i));
    spool.sync();
    spool.close();
    // A process that died mid-write: half a record appended after the last complete one.
    const segment = join(dir, 'segment-0000000001.spool');
    fs.appendFileSync(segment, Buffer.from([0x00, 0x00, 0x01, 0x00, 0x7b]));
    const sizeBefore = fs.statSync(segment).size;

    const reopened = createSpool({ dir });
    assert.ok(reopened.unreadableTail, 'the unreadable tail is reported');
    assert.equal(reopened.unreadableTail.unreadable, 5);
    assert.equal(fs.statSync(segment).size, sizeBefore, 'nothing is cut: the bytes are held');
    // Appending past the tear would bury the new record behind unreadable bytes, so the spool refuses
    // and the caller stops loudly (the same rule an in-process torn write follows). The complete
    // records before the tear are still readable.
    assert.throws(() => reopened.append(envelope(4)), /not a record/);
    const seen = [];
    const iterator = reopened.drain();
    for (;;) {
      const step = iterator.next();
      if (step.done) break;
      seen.push(step.value.receive_seq);
    }
    assert.deepEqual(seen, [1, 2, 3], 'the complete records before the tear are still handed out');
    reopened.close();
  });
});

test('Set 8c: a three-byte remnant (less than a length prefix) is refused the same way', async () => {
  await withSpool(async (dir) => {
    const spool = createSpool({ dir });
    spool.append(envelope(1));
    spool.sync();
    spool.close();
    const segment = join(dir, 'segment-0000000001.spool');
    fs.appendFileSync(segment, Buffer.from([0x01, 0x02, 0x03]));
    const reopened = createSpool({ dir });
    assert.ok(reopened.unreadableTail, 'a fragment shorter than a length prefix is unreadable');
    assert.equal(reopened.unreadableTail.unreadable, 3);
    assert.throws(() => reopened.append(envelope(2)), /not a record/);
    const seen = [];
    const iterator = reopened.drain();
    for (;;) {
      const step = iterator.next();
      if (step.done) break;
      seen.push(step.value.receive_seq);
    }
    assert.deepEqual(seen, [1], 'the record before the fragment is still handed out');
    reopened.close();
  });
});
