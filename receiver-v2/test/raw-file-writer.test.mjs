/**
 * The raw writer's durability: it must not answer true for a frame whose bytes did not all reach the
 * file. `writeSync` may write fewer bytes than asked - a short write is not an error - and a writer that
 * takes the first short write as the whole line makes the raw claim a durability it does not have: the
 * frame's bytes are lost, the torn line cannot be read back on the next start, and the frame is gone.
 *
 * These tests drive the writer with an injected `fs` whose `writeSync` writes short on purpose, so the
 * exact failure the review reproduced in a child process is examined here as a unit.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createFileRawWriter } from '../src/raw/file-writer.mjs';
import { makeEnvelope, dedupeKey } from '../src/envelope.mjs';

function envelope(seq) {
  return makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId: 'conn-1',
    runId: 'run-1',
    venue: 'kraken',
    generation: 1,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: JSON.stringify({ seq }),
    meta: { first_seq: 1 },
  });
}

async function withDir(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'raw-writer-'));
  try {
    return await fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test('a write that makes no progress answers false and does not claim the key was held', async () => {
  await withDir(async (dir) => {
    const filePath = join(dir, 'raw.log');
    const fsModule = { ...fs };
    let calls = 0;
    fsModule.writeSync = (fd, buf, off, len) => {
      calls += 1;
      // The first call writes half the line; every later call writes nothing at all - the disk is full
      // after the tear. A writer that trusted the first short write would answer true over lost bytes.
      if (calls === 1) return fs.writeSync(fd, buf, off, Math.max(1, Math.floor(len / 2)));
      return 0;
    };

    const write = createFileRawWriter({ path: filePath, fsModule });
    const frame = envelope(1);
    assert.equal(write(frame), false, 'a line that could not be finished is not durable');
    // The key was not recorded: the same frame is retried rather than answered true from memory.
    assert.equal(write(frame), false, 'the frame stays owed; nothing claims the raw holds it');
    write.close();
  });
});

test('a short write that eventually completes is written whole, not truncated', async () => {
  await withDir(async (dir) => {
    const filePath = join(dir, 'raw.log');
    const fsModule = { ...fs };
    fsModule.writeSync = (fd, buf, off, len) => fs.writeSync(fd, buf, off, Math.min(len, 3));

    const write = createFileRawWriter({ path: filePath, fsModule });
    const frame = envelope(1);
    assert.equal(write(frame), true, 'written in full, so the answer is true');

    const lines = readFileSync(filePath, 'utf8').trim().split('\n');
    assert.equal(lines.length, 1, 'exactly the one line');
    const record = JSON.parse(lines[0]);
    assert.equal(record.key, dedupeKey(frame), 'the whole line survived the short writes');
    assert.equal(record.envelope.raw, Buffer.from(JSON.stringify({ seq: 1 })).toString('base64'));
    write.close();
  });
});

test('a write that fails after a partial write leaves no fragment, and a retry writes one whole line', async () => {
  await withDir(async (dir) => {
    const filePath = join(dir, 'raw.log');
    const fsModule = { ...fs };
    let calls = 0;
    let failing = true;
    fsModule.writeSync = (fd, buf, off, len) => {
      calls += 1;
      // The failing attempt: the first call lands half the line, the second makes no progress at all, so
      // the line cannot be finished. Later attempts - after `failing` is cleared - write normally.
      if (failing) {
        if (calls === 1) return fs.writeSync(fd, buf, off, Math.max(1, Math.floor(len / 2)));
        if (calls === 2) return 0;
      }
      return fs.writeSync(fd, buf, off, len);
    };

    const write = createFileRawWriter({ path: filePath, fsModule });
    const frame = envelope(1);
    assert.equal(write(frame), false, 'the line could not be finished, so nothing is durable');
    // The fragment was cut back to where the append began: the file is exactly as it was before the attempt.
    assert.equal(readFileSync(filePath, 'utf8'), '', 'no half-written line is left behind');

    // The failure is lifted and the same frame is retried: one whole, parseable line, with no fragment
    // ahead of it - before the fix this appended a full line after the half line and answered true.
    failing = false;
    assert.equal(write(frame), true, 'the retry is durable');
    const text = readFileSync(filePath, 'utf8');
    const lines = text.split('\n').filter(Boolean);
    assert.equal(lines.length, 1, 'one record on disk, not a fragment plus a full line');
    assert.ok(text.endsWith('\n'), 'the file ends on a line boundary');
    const record = JSON.parse(lines[0]);
    assert.equal(record.key, dedupeKey(frame), 'the one line is the frame that was retried');
    write.close();
  });
});

test('a file left with a torn last line recovers on open instead of refusing to start', async () => {
  await withDir(async (dir) => {
    const filePath = join(dir, 'raw.log');
    // A previous life got one whole line down, then died mid-append: the tail is unparseable JSON with no
    // closing newline. Before the fix, opening this file threw `the raw file is not readable at byte 0`
    // and the process could never start again over it.
    const whole = `${JSON.stringify({ key: 'k-earlier', envelope: { raw: '' } })}\n`;
    writeFileSync(filePath, `${whole}{"key":"torn","envelope":{"raw":"AAAA`);

    // Opening cuts the torn tail back to the last newline rather than throwing.
    const write = createFileRawWriter({ path: filePath, fsModule: fs });
    assert.equal(readFileSync(filePath, 'utf8'), whole, 'the torn tail is gone; the whole line remains');

    // And the writer goes on normally: a new frame is one more whole line, and every line parses.
    const frame = envelope(2);
    assert.equal(write(frame), true, 'the recovered file accepts the next frame');
    const lines = readFileSync(filePath, 'utf8').trim().split('\n');
    assert.equal(lines.length, 2, 'the recovered line and the new one');
    for (const line of lines) assert.doesNotThrow(() => JSON.parse(line), 'every line is readable JSON');
    write.close();
  });
});

test('a rollback that fails poisons the writer, so a later write never claims success over the fragment', async () => {
  await withDir(async (dir) => {
    const filePath = join(dir, 'raw.log');
    const fsModule = { ...fs };
    let calls = 0;
    fsModule.writeSync = (fd, buf, off, len) => {
      calls += 1;
      // The failing attempt lands half the line, then makes no progress, so the append cannot finish.
      if (calls === 1) return fs.writeSync(fd, buf, off, Math.max(1, Math.floor(len / 2)));
      if (calls === 2) return 0;
      return fs.writeSync(fd, buf, off, len);
    };
    // The trim that would cut the fragment back fails. The review found this swallowed: the fragment
    // stayed and the next attempt appended a whole line after it and answered true over a corrupt file.
    fsModule.ftruncateSync = () => {
      throw Object.assign(new Error('injected trim IO failure'), { code: 'EIO' });
    };

    const write = createFileRawWriter({ path: filePath, fsModule });
    const frame = envelope(1);
    assert.equal(write(frame), false, 'the append did not finish and its fragment could not be cut back');
    const after = readFileSync(filePath, 'utf8');
    assert.ok(after.length > 0 && !after.endsWith('\n'), 'the half-written line is left behind');

    // The IO failures are lifted, but the writer may not claim the frame: the failed rollback poisoned it.
    fsModule.ftruncateSync = fs.ftruncateSync;
    assert.equal(write(frame), false, 'a poisoned writer never answers true');
    assert.equal(readFileSync(filePath, 'utf8'), after, 'the poisoned writer wrote nothing more than the fragment');
    write.close();
  });
});

test('a file another hand has moved is never rolled back: it is poisoned and the other line survives', async () => {
  await withDir(async (dir) => {
    const filePath = join(dir, 'raw.log');
    const fsModule = { ...fs };
    const otherLine = `${JSON.stringify({ key: 'k-other', envelope: { raw: '' } })}\n`;
    let observed = false;
    // The length this writer reads is taken *before* another writer appends - the exact ordering the
    // review reproduced. The other writer's successful line lands, then this writer's append fails.
    fsModule.fstatSync = (fd) => {
      const stat = fs.fstatSync(fd);
      if (!observed) {
        observed = true;
        fs.appendFileSync(filePath, otherLine);
      }
      return stat;
    };
    fsModule.writeSync = () => {
      throw Object.assign(new Error('write IO failure'), { code: 'EIO' });
    };

    const write = createFileRawWriter({ path: filePath, fsModule });
    assert.equal(write(envelope(1)), false, 'this writer did not become durable');
    // The other writer's line is still there - before the fix, this writer cut it back and lost it.
    assert.equal(readFileSync(filePath, 'utf8'), otherLine, "the other writer's line survives untouched");
    // And the failure poisoned this writer: it claims nothing from here on.
    assert.equal(write(envelope(2)), false, 'the poisoned writer answers false to everything');
    assert.equal(readFileSync(filePath, 'utf8'), otherLine, 'still nothing written over the other line');
    write.close();
  });
});

test('a poisoned writer is recovered by the next start: the torn tail is cut and the frame redelivered', async () => {
  await withDir(async (dir) => {
    const filePath = join(dir, 'raw.log');
    const failing = { ...fs };
    let calls = 0;
    failing.writeSync = (fd, buf, off, len) => {
      calls += 1;
      if (calls === 1) return fs.writeSync(fd, buf, off, Math.max(1, Math.floor(len / 2)));
      if (calls === 2) return 0;
      return fs.writeSync(fd, buf, off, len);
    };
    failing.ftruncateSync = () => {
      throw new Error('injected trim IO failure');
    };

    const poisoned = createFileRawWriter({ path: filePath, fsModule: failing });
    const frame = envelope(7);
    assert.equal(poisoned(frame), false, 'the append could not be finished and its trim failed');
    // A later write on the poisoned writer adds nothing. Were it to append a whole line after the
    // fragment, the restart would read one unparseable line and refuse the file for ever.
    assert.equal(poisoned(frame), false, 'the poisoned writer adds nothing after the fragment');
    poisoned.close();

    // The next start (a fresh writer on the same path) cuts the torn tail and writes the redelivered frame.
    const write = createFileRawWriter({ path: filePath, fsModule: fs });
    assert.equal(write(frame), true, 'the redelivered frame is durable after recovery');
    const lines = readFileSync(filePath, 'utf8').trim().split('\n').filter(Boolean);
    assert.equal(lines.length, 1, 'exactly one whole record');
    const record = JSON.parse(lines[0]);
    assert.equal(record.key, dedupeKey(frame), 'the recovered line is the frame that was redelivered');
    write.close();
  });
});

test('a second writer on a live path is refused at construction', async () => {
  await withDir(async (dir) => {
    const filePath = join(dir, 'raw.log');
    const first = createFileRawWriter({ path: filePath });
    assert.throws(
      () => createFileRawWriter({ path: filePath }),
      /already owns/,
      'one process may only own a raw destination once',
    );
    // Once the first writer is closed the path is free again - a restart in this process may reopen it.
    first.close();
    const second = createFileRawWriter({ path: filePath });
    second.close();
  });
});
