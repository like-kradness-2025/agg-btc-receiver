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
 * ## Ownership of the tail
 *
 * A raw destination is owned exclusively by the one writer this process opens on it (the same rule the
 * store and the spool follow: one run owns them). Two writers on one file cannot coordinate their
 * appends without a lock, and the review found the two ways that goes wrong when they try to: a rollback
 * that failed was swallowed (the fragment stayed and the next attempt claimed success over a corrupt
 * file), and a rollback with no owner ship check cut back a *different* writer's whole line because the
 * length it read was taken before that writer appended (lost data).
 *
 * The fix is to stop reading the position out of the file and keep it in the writer itself:
 *  - the writer remembers its own tail (`myEnd`) - the offset *it* has brought the file to, seeded from
 *    the recovered length at open;
 *  - before it appends it checks that the file is still exactly at its tail; anything else means another
 *    hand touched the file, so it rolls *nothing* back and is poisoned;
 *  - it rolls back a partial append only when the bytes it would cut are provably its own (the file is
 *    exactly at `myEnd + confirmed bytes`); a rollback that fails poisons too, rather than being
 *    swallowed;
 *  - a poisoned writer answers false to *everything* from then on: it never claims a durability it
 *    cannot stand behind, so the run ends non-zero with the spool still holding the frames, and the next
 *    start recovers the torn tail and redelivers them.
 *
 * The exclusivity assumption is made explicit at construction: this process refuses to open a second
 * writer on a path it already owns. Cross-process co-ownership cannot be refused without a lock (which
 * this package deliberately does not add), so it surfaces as the poisoned-on-first-write case above.
 *
 * It is not the v6 sqlite raw format the spec names (`raw_v6_sqlite`); that writer is not part of this
 * package. It is reported as the minimal stand-in so nobody mistakes it for the downstream contract.
 */

import fs from 'node:fs';
import path from 'node:path';

import { dedupeKey } from '../envelope.mjs';

/**
 * The paths this process currently has a writer open on. The raw destination is owned by one writer at a
 * time; a second `createFileRawWriter` on a live path is refused outright rather than being left to
 * discover the conflict at write time.
 */
const ownedPaths = new Set();

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
 * Write every byte of `buffer`, answering how many bytes were confirmed written. A `writeSync` may write
 * fewer bytes than asked: a short write is not an error, but treating it as the whole line would make the
 * raw claim a durability it does not have and quietly lose the rest of the frame - the frame is then gone
 * with the restart unable to read the torn line back. So the loop keeps writing at the offset it has
 * reached until the buffer is exhausted; a call that makes no progress (returns a non-positive or
 * non-integer count) stops the loop. The returned count is the number of bytes this writer can *prove* it
 * put on disk, which is exactly how much it may later cut back.
 */
function writeAll(fsModule, fd, buffer) {
  let offset = 0;
  while (offset < buffer.length) {
    const written = fsModule.writeSync(fd, buffer, offset, buffer.length - offset);
    if (!Number.isInteger(written) || written <= 0) break;
    offset += written;
  }
  return offset;
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
  // The exclusivity assumption, made explicit at construction: one writer per path per process. A second
  // writer on the same file cannot be part of the same ownership, and leaving it to fail at write time
  // would only turn a configuration error into lost data later.
  if (ownedPaths.has(filePath)) {
    throw new Error(
      `a raw writer already owns ${filePath}: one process may only own a raw destination once`,
    );
  }
  fsModule.mkdirSync(path.dirname(filePath), { recursive: true });
  // Cut back a tail torn by a crash or a power cut *before* reading the keys, so a file that an earlier
  // life left mid-line recovers instead of refusing to start for ever. The spool still holds the frame, so
  // the fragment is redelivered rather than lost.
  recoverIncompleteTail(filePath, fsModule);
  const written = readWrittenKeys(filePath, fsModule);
  const fd = fsModule.openSync(filePath, 'a');

  // The tail this writer owns: the offset *this* process has brought the file to. It starts at the
  // recovered length - the whole point is that the position is the writer's own, never a fresh read of a
  // file another hand may have moved. Only this writer's confirmed appends advance it.
  let myEnd = fsModule.statSync(filePath).size;
  // Once poisoned, the writer has seen something it cannot stand behind (another writer's bytes, or a
  // rollback it could not complete). It answers false to every later write and never claims success.
  let poisoned = false;

  function currentSize() {
    return fsModule.fstatSync(fd).size;
  }

  /**
   * Undo an append this writer could not finish. The bytes may be cut only when they are provably the
   * writer's own: the file must sit at exactly `myEnd + confirmed`. Anything else is somebody else's
   * append (or a truncation), and cutting it would destroy their data - so the writer is poisoned and
   * nothing is cut. A trim that fails is not swallowed either: the fragment stays, the writer is
   * poisoned, and the start-time recovery is left to clean up at the next start.
   */
  function undoAppend(confirmed) {
    let sizeAfter;
    try {
      sizeAfter = currentSize();
    } catch {
      poisoned = true;
      return;
    }
    if (sizeAfter === myEnd) return; // nothing of this attempt landed; nothing to cut
    if (sizeAfter !== myEnd + confirmed) {
      // The file is not where this writer's own bytes alone would leave it: another hand wrote here.
      poisoned = true;
      return;
    }
    try {
      fsModule.ftruncateSync(fd, myEnd);
    } catch {
      // A trim that itself fails leaves the fragment. Never claim it was durable.
      poisoned = true;
    }
  }

  function write(envelope) {
    if (poisoned) return false; // a poisoned writer never claims anything, not even a resend
    const key = dedupeKey(envelope);
    if (written.has(key)) return true; // a resend is a no-op, not a refusal
    const line = Buffer.from(`${JSON.stringify({ key, envelope: serialise(envelope) })}\n`, 'utf8');

    // Is the file still exactly at this writer's tail? If anyone else has appended or trimmed since the
    // last write, this writer no longer owns the end and must not touch it. In particular it must not
    // roll back bytes that are not its own - that is how a successful line of another writer was lost.
    let sizeBefore;
    try {
      sizeBefore = currentSize();
    } catch {
      poisoned = true;
      return false;
    }
    if (sizeBefore !== myEnd) {
      poisoned = true;
      return false;
    }

    // Every byte of the line, then fsync: only then is the frame durable and only then is true honest.
    let wrote;
    try {
      wrote = writeAll(fsModule, fd, line);
    } catch {
      // An exception does not say how many bytes landed, so this writer cannot know that a rollback
      // would only cut its own bytes. It never guesses: it is poisoned and claims nothing.
      poisoned = true;
      return false;
    }
    if (wrote !== line.length) {
      // The line could not be finished. Cut back exactly what this writer proved it wrote.
      undoAppend(wrote);
      return false;
    }
    try {
      fsModule.fsyncSync(fd); // durable before the true answer, as the contract requires
    } catch {
      // The whole line landed but is not durable. Undo it if it is provably ours, and report not-durable.
      undoAppend(line.length);
      return false;
    }
    myEnd += line.length;
    written.add(key);
    return true;
  }

  write.close = () => {
    try {
      fsModule.closeSync(fd);
    } catch {
      // A file that will not close is not a reason to fail the run that is ending.
    } finally {
      // The path is free again once this writer is done with it.
      ownedPaths.delete(filePath);
    }
  };
  ownedPaths.add(filePath);
  return write;
}
