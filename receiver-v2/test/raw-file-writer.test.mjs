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
