/**
 * The spool: where raw data goes when the normal path is full.
 *
 * The queue in front of organization is bounded on purpose, and when it is full the data is not
 * dropped - it comes here. This module is the second rung of the ladder: memory, then spool, then
 * stop reception and record a gap. It never decides on its own to throw data away, and it never
 * accepts more than its bound: append() returning false means the caller must stop, not retry.
 *
 * Layout, one directory per market:
 *   segment-0000000001.spool   append-only, length-prefixed envelopes, oldest first
 *   cursor                     the position a consumer has confirmed; fsynced on advance
 *
 * Ordering is the whole point: segments are read oldest first and within a segment in write order,
 * so a resend after a reconnect cannot overtake data that was already waiting. A record that was
 * written and then acknowledged is released by deleting the segments entirely behind the cursor -
 * which is why the caller must acknowledge a contiguous range and nothing else.
 *
 * Records are the canonical envelopes themselves, encoded the same way they travel between
 * processes, so what is recovered from the spool is byte-identical to what would have been sent.
 */

import fs from 'node:fs';
import path from 'node:path';

import { FRAME_MAX_BYTES, createFrameDecoder, decodeEnvelope, encodeEnvelope, frame } from './envelope.mjs';

export const DEFAULT_SEGMENT_BYTES = 64 * 1024 * 1024;
export const DEFAULT_FSYNC_MS = 1000;
export const DEFAULT_MAX_BYTES = 2 * 1024 * 1024 * 1024;

/** Envelopes are written length-prefixed so a torn tail is detected rather than parsed. */
const RECORD_OVERHEAD = 4;

function segmentName(index) {
  return `segment-${String(index).padStart(10, '0')}.spool`;
}

function segmentIndexOf(name) {
  const match = /^segment-(\d{10})\.spool$/.exec(name);
  return match ? Number(match[1]) : null;
}

function readDirNames(dir) {
  try {
    return fs.readdirSync(dir);
  } catch (error) {
    if (error.code === 'ENOENT') return [];
    throw error;
  }
}

/**
 * Create (or reopen) a spool.
 *
 * Reopening is the normal case, not the exception: the process may have died with data waiting here,
 * and that data must be drained before anything new is trusted. The cursor is read back so a restart
 * resumes where the consumer left off rather than from the beginning.
 */
export function createSpool(options = {}) {
  const {
    dir,
    segmentBytes = DEFAULT_SEGMENT_BYTES,
    fsyncMs = DEFAULT_FSYNC_MS,
    maxBytes = DEFAULT_MAX_BYTES,
    fsModule = fs,
    setTimer = setTimeout,
    clearTimer = clearTimeout,
  } = options;
  if (!dir) throw new TypeError('spool needs a directory');

  fsModule.mkdirSync(dir, { recursive: true });

  let segments = readDirNames(dir)
    .map((name) => ({ name, index: segmentIndexOf(name) }))
    .filter((entry) => entry.index !== null)
    .sort((a, b) => a.index - b.index)
    .map(({ name, index }) => ({ name, index, bytes: fsModule.statSync(path.join(dir, name)).size }));

  const cursorPath = path.join(dir, 'cursor');
  let cursor = { segment: null, offset: 0 };
  try {
    cursor = JSON.parse(fsModule.readFileSync(cursorPath, 'utf8'));
  } catch {
    // No cursor yet: nothing has been consumed, so everything present is still waiting.
  }

  let handle = null;
  let current = segments.length > 0 ? segments[segments.length - 1] : null;
  let dirty = false;
  let bytes = segments.reduce((sum, segment) => sum + segment.bytes, 0);
  let fsyncTimer = null;

  function openCurrent() {
    if (handle) return handle;
    if (!current) return null;
    const file = path.join(dir, current.name);
    handle = fsModule.openSync(file, 'a');
    return handle;
  }

  function flush() {
    if (handle && dirty) {
      fsModule.fsyncSync(handle);
      dirty = false;
    }
  }

  function scheduleFsync() {
    if (fsyncTimer !== null) return;
    fsyncTimer = setTimer(() => {
      fsyncTimer = null;
      flush();
    }, fsyncMs);
    if (typeof fsyncTimer.unref === 'function') fsyncTimer.unref();
  }

  function rotateIfNeeded(nextBytes) {
    if (!current) return;
    if (current.bytes + nextBytes <= segmentBytes) return;
    flush();
    if (handle) {
      fsModule.closeSync(handle);
      handle = null;
    }
    const index = current.index + 1;
    current = { name: segmentName(index), index, bytes: 0 };
    segments.push(current);
  }

  /**
   * Append one envelope.
   *
   * Returns false when the record would take the spool past its bound. False is a stop signal for
   * the caller - reception pauses and the gap is recorded - never a reason to drop this record.
   */
  function append(envelope) {
    const record = frame(encodeEnvelope(envelope));
    if (record.length > FRAME_MAX_BYTES) {
      throw new RangeError('a single record exceeds the frame limit');
    }
    if (current === null) {
      current = { name: segmentName(1), index: 1, bytes: 0 };
      segments.push(current);
    }
    if (bytes + record.length > maxBytes) return false;
    rotateIfNeeded(record.length);
    const file = openCurrent();
    fsModule.writeSync(file, record);
    current.bytes += record.length;
    bytes += record.length;
    dirty = true;
    scheduleFsync();
    return true;
  }

  /** Read every record from the cursor onwards, oldest segment first. */
  function* drain({ limit = Infinity } = {}) {
    let read = 0;
    let startOffset = cursor.offset;
    for (const segment of segments) {
      if (cursor.segment !== null && segment.index < cursor.segment) continue;
      const file = path.join(dir, segment.name);
      const fd = fsModule.openSync(file, 'r');
      let buf;
      try {
        buf = fsModule.readFileSync(fd);
      } finally {
        fsModule.closeSync(fd);
      }
      if (startOffset > 0) buf = buf.subarray(startOffset);
      const decoder = createFrameDecoder({ maxBytes: FRAME_MAX_BYTES + 1 });
      // A torn tail (the process died mid-write) is not recoverable and not guessed at: the bytes
      // after the last complete record are reported and left for the gap rules to settle.
      let records;
      try {
        records = decoder.push(buf);
      } catch (error) {
        return { torn: { segment: segment.index, offset: startOffset + buf.length }, error, read };
      }
      for (const record of records) {
        if (read >= limit) return { torn: null, read };
        yield decodeEnvelope(record);
        read += 1;
      }
      if (decoder.bufferedBytes > 0) {
        return { torn: { segment: segment.index, offset: startOffset + buf.length - decoder.bufferedBytes }, read };
      }
      startOffset = 0;
    }
    return { torn: null, read };
  }

  /**
   * Confirm everything up to this position as consumed and durable elsewhere.
   *
   * The caller may only pass a contiguous position; segments entirely behind it are deleted, which
   * is what keeps the spool bounded. Deleting more than the caller confirmed would be data loss.
   */
  function advance({ segment, offset }) {
    if (segment === null || segment === undefined) return;
    cursor = { segment, offset };
    const fd = fsModule.openSync(cursorPath, 'w');
    try {
      fsModule.writeSync(fd, JSON.stringify(cursor));
      fsModule.fsyncSync(fd);
    } finally {
      fsModule.closeSync(fd);
    }
    const kept = [];
    for (const entry of segments) {
      if (entry.index < segment) {
        bytes -= entry.bytes;
        try {
          fsModule.unlinkSync(path.join(dir, entry.name));
        } catch {
          /* already gone */
        }
        continue;
      }
      kept.push(entry);
    }
    segments = kept;
  }

  function close() {
    if (fsyncTimer !== null) {
      clearTimer(fsyncTimer);
      fsyncTimer = null;
    }
    flush();
    if (handle) {
      fsModule.closeSync(handle);
      handle = null;
    }
  }

  return {
    append,
    drain,
    advance,
    close,
    sync: flush,
    get bytes() {
      return bytes;
    },
    get segments() {
      return segments.map((segment) => ({ ...segment }));
    },
    get cursor() {
      return { ...cursor };
    },
    get isOverBound() {
      return bytes >= maxBytes;
    },
  };
}
