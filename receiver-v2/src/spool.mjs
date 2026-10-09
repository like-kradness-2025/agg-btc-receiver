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
const DEFAULT_CURSOR_SAVE_MS = 100;
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
    // Set 6b: how long a confirmed position may wait before it is written to the cursor file. The
    // position itself moves in memory at the moment of the advance; what waits is the file write,
    // the fsync and the segment deletion behind it - one fsync per save instead of one per release.
    // Zero is the synchronous mode: every advance saves at once (tests pin it, and a caller that
    // wants the old frame-by-frame order can ask for it).
    cursorSaveMs = DEFAULT_CURSOR_SAVE_MS,
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
  // Set 6b: the cursor file lags the in-memory cursor by at most one save. The deletion of released
  // segments happens at the save, right after the file is written, so the file never falls behind
  // what has been deleted - a restart reads a position at or before the oldest surviving segment,
  // and everything it can still see is walked and offered again (the duplicate is answered from the
  // record, so the resend costs a round trip and nothing else).
  let cursorTimer = null;
  let cursorDirty = false;
  let failed = null;
  // Set by the latest drainRecords walk when a segment's bytes did not describe a length the format can
  // have written: the walk cannot continue, and a consumer has to be able to tell that "it ended" from
  // "it stopped at a desynchronised record". Null on a walk that read every record it was given.
  let unreadable = null;
  let bytes = segments.reduce((sum, segment) => sum + segment.bytes, 0);
  let fsyncTimer = null;
  // Set 8c: a process killed mid-write leaves an incomplete record at the tail of the segment it was
  // writing. Its bytes are not a record, so nothing after them can ever be read, and appending past
  // them would bury every later record behind unreadable bytes - a spool that can never drain again.
  // The bytes after the last complete record were never durable either: a record is confirmable only
  // once it is whole, and the cursor can only sit past a complete one, so cutting them cannot take
  // anything a consumer was told it had. The cut is reported rather than silent.
  let repairedTail = null;
  if (current && current.bytes > 0) {
    const tailFile = path.join(dir, current.name);
    let tailBytes = null;
    try {
      const fd = fsModule.openSync(tailFile, 'r');
      try {
        tailBytes = fsModule.readFileSync(fd);
      } finally {
        fsModule.closeSync(fd);
      }
    } catch {
      tailBytes = null;
    }
    if (tailBytes !== null) {
      const decoder = createFrameDecoder({ maxBytes: FRAME_MAX_BYTES + 1 });
      let end = 0;
      let torn = false;
      try {
        for (const record of decoder.push(tailBytes)) end += RECORD_OVERHEAD + record.length;
        // A prefix of a valid record shorter than the record itself: the process died mid-write.
        torn = decoder.bufferedBytes > 0 || end < tailBytes.length;
      } catch {
        // A length the format cannot have written is corruption, not a torn write. It is left exactly
        // where it is: the walk's own verdict ends the run loudly, and a reopen that quietly cut
        // bytes it cannot account for would turn a stopped run into a silent one.
        torn = false;
      }
      if (torn && end < current.bytes) {
        fsModule.truncateSync(tailFile, end);
        repairedTail = { segment: current.index, from: end, removed: current.bytes - end };
        bytes -= current.bytes - end;
        current.bytes = end;
      }
    }
  }

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
      try {
        flush();
      } catch {
        // A scheduled fsync that fails is retried on the next clock rather than crashing the
        // process: the bytes are still in the page cache and the next flush covers them, while a
        // process that died here would be a run stopped over a transient write error (the run
        // supervisor does not restart an ingest that exited). The explicit callers - `sync()`,
        // `close()` - still see the failure themselves.
        scheduleFsync();
      }
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
   * Returns the raw position just past the record - `{ segment, offset }` - once it is on the disk,
   * and false when it would take the spool past its bound. False is a stop signal for the caller -
   * reception pauses and the gap is recorded - never a reason to drop this record. The position is
   * deliberately raw: whether it coincides with the end of a segment is decided by `advance`, at
   * the moment of the advance, because the segment can still grow after this call.
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
    return { segment: current.index, offset: current.bytes };
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
   * Read every record from the cursor onwards, oldest segment first, each with the raw position just
   * past it.
   *
   * drain() hands out the decoded envelopes and nothing else, which is what a consumer that only reads
   * wants. A consumer that has to *confirm* what it read needs more: the cursor may only be moved to a
   * contiguous place, and a decoded envelope does not say where it ended. A record's end is its length
   * prefix plus its payload - the same framing append wrote - so the position is derived from the
   * encoded length rather than guessed at, and a caller may advance with `{ segment, offset }` for the
   * last record it truly consumed.
   *
   * The position is raw, with no adjustment for a segment's end: whether a position releases a whole
   * segment is decided by `advance`, at the moment of the advance. A walk's idea of "the end" is a
   * snapshot and the segment can grow after it; an advance that trusted the snapshot could release a
   * segment that has since grown, putting bytes behind the cursor that would never be read. A torn
   * tail keeps the position inside its segment here - the record it follows is the last complete one -
   * and only an advance that finds the position at the file's end releases the segment.
   */
  function* drainRecords({ limit = Infinity, from = null } = {}) {
    let read = 0;
    // Set 5: a walk may start at a position of its own - the ordered pump's send position, which is
    // ahead of the release cursor. `from` changes only where the reading starts; the release rule
    // still belongs to `advance` alone.
    const start = from === null ? cursor : from;
    let startOffset = start.offset;
    let startSegment = start.segment;
    unreadable = null; // this walk's verdict, not an earlier one's
    for (const segment of segments) {
      if (startSegment !== null && startSegment !== undefined && segment.index < startSegment) continue;
      const file = path.join(dir, segment.name);
      const fd = fsModule.openSync(file, 'r');
      let buf;
      try {
        buf = fsModule.readFileSync(fd);
      } finally {
        fsModule.closeSync(fd);
      }
      // The offset belongs to the segment it was read in. A position whose segment has since been
      // released must not have its offset applied to the next segment: that would skip the next
      // segment's beginning. The walk simply starts at the next segment's first record - and the
      // positions it reports must start there too, or a consumer resuming from one would skip the
      // bytes the stale offset named.
      const sliceFrom = startSegment === segment.index ? startOffset : 0;
      if (sliceFrom > 0) buf = buf.subarray(sliceFrom);
      const decoder = createFrameDecoder({ maxBytes: FRAME_MAX_BYTES + 1 });
      let records;
      try {
        records = decoder.push(buf);
      } catch (error) {
        // A length the format cannot have written means the segment desynchronised, and nothing
        // after it can be trusted: the walk ends here rather than inventing positions. The verdict
        // is recorded, so a consumer can stop on it instead of reading the end as a finished drain.
        unreadable = { segment: segment.index, offset: sliceFrom, error };
        return;
      }
      let at = sliceFrom;
      for (let index = 0; index < records.length; index += 1) {
        if (read >= limit) return;
        const record = records[index];
        at += RECORD_OVERHEAD + record.length;
        yield { envelope: decodeEnvelope(record), segment: segment.index, offset: at };
        read += 1;
      }
      startOffset = 0;
    }
  }

  /**
   * Confirm everything up to this position as consumed and durable elsewhere.
   *
   * The caller may only pass a contiguous position; segments entirely behind it are released, which
   * is what keeps the spool bounded. Releasing more than the caller confirmed would be data loss.
   *
   * This is also where a position at a segment's end becomes the start of the next one: an end is
   * only a release when it is the end *now*. A position read earlier is compared against the
   * segment as it is at this moment, so a segment that has grown since keeps the cursor inside
   * itself and the bytes written after the confirmed record stay readable; only a position that is
   * still the whole segment's end releases it.
   *
   * Set 6b: the advance moves the position in memory, and the file and the deletion follow on the
   * save clock (`cursorSaveMs`) rather than at the release. A release can arrive per acknowledged
   * frame, and one fsync for each of those is a cost the disk answers for the whole pipeline; the
   * save writes the latest position once, and the ordering (file first, deletion after) is what
   * keeps a restart from reading a position ahead of the segments it can still see.
   */
  function advance(position) {
    if (!position || position.segment === null || position.segment === undefined) return;
    const { segment, offset } = position;
    const entry = segments.find((candidate) => candidate.index === segment);
    const cursorAt =
      entry !== undefined && Number.isInteger(offset) && offset === entry.bytes
        ? { segment: segment + 1, offset: 0 }
        : { segment, offset };
    if (cursorAt.segment === cursor.segment && cursorAt.offset === cursor.offset) return;
    cursor = cursorAt;
    cursorDirty = true;
    if (cursorSaveMs > 0) scheduleCursorSave();
    else saveCursor();
  }

  function scheduleCursorSave() {
    if (cursorTimer !== null || !cursorDirty) return;
    cursorTimer = setTimer(() => {
      cursorTimer = null;
      try {
        saveCursor();
      } catch {
        // Same rule as the segment fsync, and the same reason: the position stays dirty and the
        // next clock retries it. Nothing is lost by waiting - the segments are still there - and
        // the paths where the position must be on disk before anything else happens (the
        // acceptance gate, the seal, the drain) save loudly and withhold what depends on them.
        scheduleCursorSave();
      }
    }, cursorSaveMs);
    if (typeof cursorTimer.unref === 'function') cursorTimer.unref();
  }

  /**
   * Write the confirmed position down, then release the segments entirely behind it.
   *
   * The order is the point: the file names where a restart resumes, so a segment may only be
   * deleted once the file says the consumer is past it. A write that fails leaves both the file
   * and the segments as they were - the position stays unsaved, the data stays readable, and the
   * caller sees the failure rather than a spool that looks further along than it is.
   */
  function saveCursor() {
    if (cursorTimer !== null) {
      clearTimer(cursorTimer);
      cursorTimer = null;
    }
    if (!cursorDirty) return;
    // The write goes to a temporary file and is renamed over the real one: the cursor file a
    // restart reads is always either the position it had or the new one, never a half-written
    // one. A write that fails part-way, or a short write, leaves the old file in place and the
    // segments untouched - the failure is reported to the caller, and the position stays unsaved
    // rather than becoming a file that reads as a different position than it is.
    const serialized = JSON.stringify(cursor);
    const tempPath = `${cursorPath}.tmp`;
    const fd = fsModule.openSync(tempPath, 'w');
    try {
      const written = fsModule.writeSync(fd, serialized);
      if (written !== Buffer.byteLength(serialized)) {
        throw new Error('the cursor was only partly written');
      }
      fsModule.fsyncSync(fd);
    } finally {
      fsModule.closeSync(fd);
    }
    fsModule.renameSync(tempPath, cursorPath);
    // The rename has to be durable before anything is allowed to follow it. Without the directory
    // fsync a power loss can leave the old cursor file behind while the segments it released are
    // still present - and a restart that walks them after the generation has switched would offer
    // records the new connection refuses, stopping the release order for good. The directory fsync
    // is what makes "saved" mean the file a restart will read; only then are segments deleted.
    const dirFd = fsModule.openSync(dir, 'r');
    try {
      fsModule.fsyncSync(dirFd);
    } finally {
      fsModule.closeSync(dirFd);
    }
    cursorDirty = false;
    const kept = [];
    for (const segmentEntry of segments) {
      if (segmentEntry.index < cursor.segment) {
        bytes -= segmentEntry.bytes;
        try {
          fsModule.unlinkSync(path.join(dir, segmentEntry.name));
        } catch {
          /* already gone */
        }
        continue;
      }
      kept.push(segmentEntry);
    }
    segments = kept;
  }

  function close() {
    if (fsyncTimer !== null) {
      clearTimer(fsyncTimer);
      fsyncTimer = null;
    }
    if (cursorTimer !== null) {
      clearTimer(cursorTimer);
      cursorTimer = null;
    }
    flush();
    try {
      // Best effort: a position that cannot be written down at closing is a position the next life
      // walks again (the segments are still there - nothing was deleted past it), which costs a
      // resend and nothing else. Closing must not be turned into a failure by a save the caller
      // cannot act on any more; the paths where the position matters - the drain, the seal, the
      // acceptance gate - save loudly.
      saveCursor();
    } catch {
      /* the position stays unwritten; the data stays readable */
    }
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
    saveCursor,
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
    /**
     * The verdict of the latest `drainRecords` walk: non-null when it stopped because a segment's bytes
     * did not describe a length the format can have written, so the consumer can stop rather than read
     * the walk's end as a finished drain.
     */
    get unreadable() {
      return unreadable;
    },
    /** Set 8c: what the reopen cut from the last segment, or null when there was nothing to cut. */
    get lastRepair() {
      return repairedTail;
    },
  };
}
