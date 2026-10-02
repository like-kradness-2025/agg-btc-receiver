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
/**
 * The second half of the bound: the spec says the spool may hold the earlier of 5% of the
 * filesystem's free space or the fixed ceiling, so the free-space share is what keeps a nearly
 * full disk from being filled by the spool before anyone notices.
 */
export const FREE_SPACE_FRACTION = 0.05;

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
    freeSpace = null,
  } = options;
  if (!dir) throw new TypeError('spool needs a directory');

  fsModule.mkdirSync(dir, { recursive: true });

  // How much the filesystem still has free, injected so the bound can be forced without filling a
  // disk. The default is a real reading of the spool's own filesystem; a filesystem that will not
  // answer is not reported as full (which would refuse every record) - the fixed ceiling still
  // applies, and the caller is never stopped over a number this process could not read.
  const readFreeSpace =
    freeSpace ??
    (() => {
      try {
        const stat = fsModule.statfsSync(dir);
        return stat.bavail * stat.bsize;
      } catch {
        return Number.POSITIVE_INFINITY;
      }
    });

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
  let failed = null;
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
   * The effective bound, recomputed before each record: the earlier of the fixed ceiling and 5% of
   * what the filesystem still has free. It is read per append rather than once, because the free
   * space is what the spool is consuming and a reading taken at construction says nothing about now.
   */
  function allowedBytes() {
    const free = readFreeSpace();
    const bySpace = Number.isFinite(free) ? Math.floor(free * FREE_SPACE_FRACTION) : Infinity;
    return Math.min(maxBytes, bySpace);
  }

  /**
   * The segment a new record may land in.
   *
   * The cursor is the consumer's confirmed position, and its file is released once the consumer has
   * moved past a whole segment. Writing into a segment the cursor has already left would put bytes
   * behind the consumer that it will never read - so the active segment is moved forward to meet the
   * cursor, and a record arriving after a complete drain starts a fresh segment at that position.
   */
  function ensureWritable() {
    if (current === null) {
      const index = cursor.segment !== null && cursor.offset === 0 ? cursor.segment : 1;
      current = { name: segmentName(index), index, bytes: 0 };
      segments.push(current);
      return;
    }
    if (cursor.segment !== null && current.index < cursor.segment) {
      // This segment lies entirely behind the confirmed position; advance() has removed its file, and a
      // handle still open to it would write into an inode nothing can name. Close it before moving on, so
      // the next record opens the new segment rather than appending to a file that is already gone.
      if (handle) {
        try {
          fsModule.closeSync(handle);
        } catch {
          /* already closed */
        }
        handle = null;
      }
      const index = cursor.segment;
      current = { name: segmentName(index), index, bytes: 0 };
      segments.push(current);
    }
  }

  /**
   * Append one envelope.
   *
   * Returns false when the record would take the spool past its bound. False is a stop signal for
   * the caller - reception pauses and the gap is recorded - never a reason to drop this record.
   */
  function append(envelope) {
    if (failed) return false; // a torn record was written: appending after it would bury the tear
    const record = frame(encodeEnvelope(envelope));
    if (record.length > FRAME_MAX_BYTES) {
      throw new RangeError('a single record exceeds the frame limit');
    }
    ensureWritable();
    if (bytes + record.length > allowedBytes()) return false;
    rotateIfNeeded(record.length);
    const file = openCurrent();
    // A write can be short - a full disk, an interrupted syscall - and treating that as a complete
    // record is how data disappears while everything still looks accepted. The whole record is
    // written or the caller is told it did not fit; a partial tail is what the torn-tail rules are
    // for, and no bytes are counted as held until the record is actually on the disk.
    let written = 0;
    try {
      while (written < record.length) {
        const wrote = fsModule.writeSync(file, record, written, record.length - written);
        if (!(wrote > 0)) {
          throw new Error(`spool write made no progress after ${written} of ${record.length} bytes`);
        }
        written += wrote;
      }
    } catch (error) {
      // Whatever the failure looked like - a syscall returning zero or throwing - the same honest
      // state has to be left behind: count the bytes that did reach the disk, close the spool to
      // further writes so a torn record is not buried, and pass the failure on.
      if (written > 0) {
        current.bytes += written;
        bytes += written;
        dirty = true;
      }
      if (!failed) failed = error instanceof Error ? error : new Error(String(error));
      flush();
      throw failed;
    }
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
   * Read every record from the cursor onwards, oldest segment first, each with the position to resume
   * from after it.
   *
   * drain() hands out the decoded envelopes and nothing else, which is what a consumer that only reads
   * wants. A consumer that has to *confirm* what it read needs more: the cursor may only be moved to a
   * contiguous place, and a decoded envelope does not say where it ended. A record's end is its length
   * prefix plus its payload - the same framing append wrote - so the position is derived from the
   * encoded length rather than guessed at, and a caller may advance with `{ segment, offset }` for the
   * last record it truly consumed.
   *
   * The position after the last record of a segment whose bytes it exactly fills is the start of the
   * next segment, not the end of this one: that is the position that releases a whole segment when the
   * caller advances, and it is the only place the cursor can sit that means "this segment is done".
   * A segment with anything left over - a torn tail - keeps the position inside itself, so those bytes
   * are never skipped by an advance that trusted this walk.
   */
  function* drainRecords({ limit = Infinity } = {}) {
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
      let records;
      try {
        records = decoder.push(buf);
      } catch {
        // A length the format cannot have written means the segment desynchronised, and nothing
        // after it can be trusted: the walk ends here rather than inventing positions.
        return;
      }
      // The end of the segment is reached exactly when every byte from the read point is a complete
      // record; anything else - a torn tail - must keep the resume position inside this segment.
      const completeBytes = records.reduce((sum, record) => sum + RECORD_OVERHEAD + record.length, 0);
      const reachesEnd = completeBytes === buf.length;
      let at = startOffset;
      for (let index = 0; index < records.length; index += 1) {
        if (read >= limit) return;
        const record = records[index];
        at += RECORD_OVERHEAD + record.length;
        const last = index === records.length - 1;
        const position =
          last && reachesEnd
            ? { segment: segment.index + 1, offset: 0 }
            : { segment: segment.index, offset: at };
        yield { envelope: decodeEnvelope(record), ...position };
        read += 1;
      }
      startOffset = 0;
    }
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
    drainRecords,
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
    /** Non-null once a write failed part-way: the spool holds a torn record and takes no more. */
    get failed() {
      return failed;
    },
  };
}
