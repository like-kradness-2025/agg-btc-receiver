/**
 * A minimal raw writer: the canonical bytes, appended to a file and made durable before it says so.
 *
 * The organizer's contract for a raw writer is small but exact (docs/fix-plan-sets.md §4.1):
 *  - it must be durable *before it returns true*: a true answer is what turns a frame into "the raw
 *    holds this", and the ledger's owed state is written on the strength of it;
 *  - it must be idempotent on the dedupe key (`connection_id:receive_seq`): a resend of a frame the raw
 *    already holds must answer true, not refuse. A refusal means "not durable", and a frame that is
 *    durable would then be stuck owed for ever.
 *
 * This is deliberately the smallest thing that satisfies both: one append-only file of JSON lines,
 * keyed by the frame's own dedupe key, with the frame's bytes base64-encoded. Keys already present are
 * read back at construction, so a restart's resend is a no-op. Each write is a single appended line
 * followed by an fsync; a write that fails answers false (not durable) rather than throwing.
 *
 * It is not the v6 sqlite raw format the spec names (`raw_v6_sqlite`); that writer is not part of this
 * package. It is reported as the minimal stand-in so nobody mistakes it for the downstream contract.
 */

import fs from 'node:fs';
import path from 'node:path';

import { dedupeKey } from '../envelope.mjs';

/**
 * Recover a file whose last line was torn by a crash or a power cut: an append only ever adds a whole
 * line ending in `\n`, so a file that does not end in one has an incomplete record at its very end and
 * nothing after it. Cutting back to the last newline (or to zero when there is none) drops exactly that
 * unreconstructable fragment. The data is not lost: the spool segment that carried the frame is still
 * there, so the frame is simply redelivered on this start. Without this, a torn tail would make every
 * later start refuse the file as unreadable - a permanent halt over a frame the raw never had.
 */
function recoverIncompleteTail(filePath, fsModule) {
  let size;
  try {
    size = fsModule.statSync(filePath).size;
  } catch {
    return; // no file yet: nothing to recover
  }
  if (size === 0) return;
  let buffer;
  try {
    buffer = fsModule.readFileSync(filePath);
  } catch {
    return; // unreadable for a reason that is not a torn tail; the key read below will report it
  }
  if (buffer[buffer.length - 1] === 0x0a) return; // the file ends on a line boundary: nothing torn
  const lastNewline = buffer.lastIndexOf(0x0a);
  fsModule.truncateSync(filePath, lastNewline + 1); // 0 when the whole file is one torn line
}

/** Read back the dedupe keys a previous life of the file already wrote down. */
function readWrittenKeys(filePath, fsModule) {
  const keys = new Set();
  let text;
  try {
    text = fsModule.readFileSync(filePath, 'utf8');
  } catch {
    return keys; // no file yet: nothing has been written
  }
  let offset = 0;
  for (const line of text.split('\n')) {
    offset += line.length + 1;
    if (line === '') continue;
    let record;
    try {
      record = JSON.parse(line);
    } catch {
      // A line in the middle of the file that cannot be read back is real corruption of the canonical
      // record - it cannot be a torn append, which only ever damages the end - so the process refuses to
      // start over it rather than silently ignore it. A torn *tail* was already cut back before this read.
      throw new Error(`the raw file is not readable at byte ${offset - line.length - 1}: ${filePath}`);
    }
    if (record !== null && typeof record === 'object' && typeof record.key === 'string') {
      keys.add(record.key);
    }
  }
  return keys;
}

/**
 * Write every byte of `buffer` before answering true. A `writeSync` may write fewer bytes than asked:
 * a short write is not an error, but treating it as the whole line would make the raw claim a durability
 * it does not have and quietly lose the rest of the frame - the frame is then gone with the restart
 * unable to read the torn line back. So the loop keeps writing at the offset it has reached until the
 * buffer is exhausted; a call that makes no progress (returns a non-positive or non-integer count) is a
 * failure, never a partial success.
 */
function writeAll(fsModule, fd, buffer) {
  let offset = 0;
  while (offset < buffer.length) {
    const written = fsModule.writeSync(fd, buffer, offset, buffer.length - offset);
    if (!Number.isInteger(written) || written <= 0) return false;
    offset += written;
  }
  return true;
}

/**
 * Cut the file back to the length it had before a failed append, so a half-written line cannot be left
 * behind. A `writeSync` that stops making progress has already put some bytes on disk; leaving them would
 * make the next attempt append a whole line *after* the fragment - two records where one was meant, the
 * first unparseable - and would make the next start refuse the file. Rolling back to the pre-write length
 * restores the exact state the append began from, so a retry writes one whole line and a restart reads a
 * file that ends on a line boundary.
 */
function rollbackTo(fsModule, fd, size) {
  try {
    fsModule.ftruncateSync(fd, size);
  } catch {
    // A trim that itself fails leaves the fragment; the write below still reports not-durable, and the
    // start-time recovery is the second line of defence for exactly this case.
  }
}

function serialise(envelope) {
  return {
    market: envelope.market,
    stream: envelope.stream,
    connection_id: envelope.connection_id,
    run_id: envelope.run_id,
    venue: envelope.venue,
    generation: envelope.generation,
    receive_seq: envelope.receive_seq,
    recv_ts_ms: envelope.recv_ts_ms,
    recv_mono_ns: envelope.recv_mono_ns,
    meta: envelope.meta ?? null,
    raw: envelope.raw.toString('base64'),
  };
}

/**
 * Open (or create) the raw file at `path` and return the writer the organizer calls. The parent
 * directory is created if it does not exist. The file handle is held open with O_APPEND for the life of
 * the process, which is also the life of the writer.
 */
export function createFileRawWriter({ path: filePath, fsModule = fs } = {}) {
  if (typeof filePath !== 'string' || filePath.length === 0) {
    throw new TypeError('a raw writer needs a non-empty file path');
  }
  fsModule.mkdirSync(path.dirname(filePath), { recursive: true });
  // Cut back a tail torn by a crash or a power cut *before* reading the keys, so a file that an earlier
  // life left mid-line recovers instead of refusing to start for ever. The spool still holds the frame, so
  // the fragment is redelivered rather than lost.
  recoverIncompleteTail(filePath, fsModule);
  const written = readWrittenKeys(filePath, fsModule);
  const fd = fsModule.openSync(filePath, 'a');

  function write(envelope) {
    const key = dedupeKey(envelope);
    if (written.has(key)) return true; // a resend is a no-op, not a refusal
    const line = Buffer.from(`${JSON.stringify({ key, envelope: serialise(envelope) })}\n`, 'utf8');
    let startSize = null;
    try {
      // Where this append begins. An O_APPEND handle always writes at the end, so this is the length to
      // return to if the line cannot be finished: no fragment may be left for the next start to trip on.
      startSize = fsModule.fstatSync(fd).size;
      // Every byte of the line, then fsync: only then is the frame durable and only then is true honest.
      if (!writeAll(fsModule, fd, line)) {
        if (startSize !== null) rollbackTo(fsModule, fd, startSize);
        return false;
      }
      fsModule.fsyncSync(fd); // durable before the true answer, as the contract requires
      written.add(key);
      return true;
    } catch {
      // not durable: the organizer treats this as a frame nothing could hold. Any bytes the failed append
      // did land are cut back first, so the file ends where it did before the attempt.
      if (startSize !== null) rollbackTo(fsModule, fd, startSize);
      return false;
    }
  }

  write.close = () => {
    try {
      fsModule.closeSync(fd);
    } catch {
      // A file that will not close is not a reason to fail the run that is ending.
    }
  };
  return write;
}
