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
      // A torn tail is a record whose bytes did not survive; the raw is the canonical record, so the
      // process must refuse to start over a file it cannot read back rather than silently ignore it.
      throw new Error(`the raw file is not readable at byte ${offset - line.length - 1}: ${filePath}`);
    }
    if (record !== null && typeof record === 'object' && typeof record.key === 'string') {
      keys.add(record.key);
    }
  }
  return keys;
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
  const written = readWrittenKeys(filePath, fsModule);
  const fd = fsModule.openSync(filePath, 'a');

  function write(envelope) {
    const key = dedupeKey(envelope);
    if (written.has(key)) return true; // a resend is a no-op, not a refusal
    const line = Buffer.from(`${JSON.stringify({ key, envelope: serialise(envelope) })}\n`, 'utf8');
    try {
      fsModule.writeSync(fd, line);
      fsModule.fsyncSync(fd); // durable before the true answer, as the contract requires
      written.add(key);
      return true;
    } catch {
      return false; // not durable: the organizer treats this as a frame nothing could hold
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
