// Durable cursor for append-only JSONL sources.
// The cursor points past consumed bytes; an unterminated final line is kept
// separately so a later append can complete it without rereading old rows.

import {
  closeSync, existsSync, fstatSync, fsyncSync, mkdirSync, openSync,
  readFileSync, readSync, renameSync, writeFileSync,
} from 'node:fs';
import { randomUUID } from 'node:crypto';
import { dirname } from 'node:path';

export const INCREMENTAL_CURSOR_SCHEMA_VERSION = 'incremental_cursor_v1';

function fail(code, message, details = undefined) {
  const error = new Error(`${code}: ${message}`);
  error.code = code;
  if (details !== undefined) error.details = details;
  return error;
}

function durableWrite(path, content) {
  writeFileSync(path, content);
  const fd = openSync(path, 'r');
  try { fsyncSync(fd); } finally { closeSync(fd); }
}

export function fsyncDirectory(path) {
  const fd = openSync(path, 'r');
  try { fsyncSync(fd); } finally { closeSync(fd); }
}

/** Write JSON with a durable temp-file + rename. */
export function writeAtomicJson(path, value) {
  mkdirSync(dirname(path), { recursive: true });
  const temp = `${path}.tmp-${process.pid}-${randomUUID()}`;
  durableWrite(temp, `${JSON.stringify(value, null, 2)}\n`);
  renameSync(temp, path);
  fsyncDirectory(dirname(path));
}

function readJson(path) {
  if (!existsSync(path)) return null;
  let value;
  try {
    value = JSON.parse(readFileSync(path, 'utf8'));
  } catch (cause) {
    throw fail('E_CURSOR_CHECKPOINT_CORRUPT', `invalid JSON at ${path}`, { cause });
  }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw fail('E_CURSOR_CHECKPOINT_CORRUPT', `expected an object at ${path}`);
  }
  return value;
}

function normalizeCursor(value, sourcePath = null) {
  const cursor = { ...(value || {}) };
  cursor.schema_version ||= INCREMENTAL_CURSOR_SCHEMA_VERSION;
  if (cursor.schema_version !== INCREMENTAL_CURSOR_SCHEMA_VERSION) {
    throw fail('E_CURSOR_SCHEMA', `unsupported schema ${cursor.schema_version}`);
  }
  if (sourcePath !== null) {
    if (cursor.source_path && cursor.source_path !== sourcePath) {
      throw fail('E_CURSOR_SOURCE_MISMATCH', `${cursor.source_path} != ${sourcePath}`);
    }
    cursor.source_path = sourcePath;
  }
  cursor.byte_offset = cursor.byte_offset ?? 0;
  if (!Number.isSafeInteger(cursor.byte_offset) || cursor.byte_offset < 0) {
    throw fail('E_CURSOR_OFFSET', `invalid byte_offset ${cursor.byte_offset}`);
  }
  cursor.partial_line = cursor.partial_line ?? '';
  if (typeof cursor.partial_line !== 'string') {
    throw fail('E_CURSOR_PARTIAL_LINE', 'partial_line must be a string');
  }
  if (cursor.partial_line_base64 !== undefined && typeof cursor.partial_line_base64 !== 'string') {
    throw fail('E_CURSOR_PARTIAL_LINE', 'partial_line_base64 must be a string');
  }
  cursor.line_number = cursor.line_number ?? 0;
  if (!Number.isSafeInteger(cursor.line_number) || cursor.line_number < 0) {
    throw fail('E_CURSOR_LINE', `invalid line_number ${cursor.line_number}`);
  }
  return cursor;
}

export function loadCursor(checkpointPath, sourcePath = null) {
  return normalizeCursor(readJson(checkpointPath), sourcePath);
}

export function saveCursor(checkpointPath, cursor) {
  const normalized = normalizeCursor(cursor);
  normalized.updated_at = new Date().toISOString();
  writeAtomicJson(checkpointPath, normalized);
  return normalized;
}

function partialBytes(cursor) {
  if (cursor.partial_line_base64) return Buffer.from(cursor.partial_line_base64, 'base64');
  return Buffer.from(cursor.partial_line, 'utf8');
}

/**
 * Consume newly appended JSONL rows from one source file.
 * `onLine` is called as `(row, info)` and must finish before the cursor is
 * advanced. Blank lines are consumed but are not delivered.
 */
export function readIncrementalJsonl({
  sourcePath,
  checkpointPath,
  onLine,
  cursor: suppliedCursor,
} = {}) {
  if (!sourcePath || typeof sourcePath !== 'string') throw fail('E_CURSOR_SOURCE', 'sourcePath is required');
  if (!checkpointPath || typeof checkpointPath !== 'string') throw fail('E_CURSOR_CHECKPOINT', 'checkpointPath is required');

  const cursor = normalizeCursor(suppliedCursor ?? loadCursor(checkpointPath, sourcePath), sourcePath);
  const sourceFd = openSync(sourcePath, 'r');
  let size;
  try { size = fstatSync(sourceFd).size; } finally { closeSync(sourceFd); }
  if (cursor.byte_offset > size) {
    throw fail('E_CURSOR_SOURCE_SHRUNK', `${sourcePath} is ${size} bytes, cursor is ${cursor.byte_offset}`);
  }

  // Read only the newly appended byte range.  The previous implementation
  // used readFileSync(sourcePath), turning an incremental consumer back into
  // a full-file scan on every poll.
  const appendLength = size - cursor.byte_offset;
  const appended = Buffer.allocUnsafe(appendLength);
  if (appendLength > 0) {
    const fd = openSync(sourcePath, 'r');
    try {
      let read = 0;
      while (read < appendLength) {
        const count = readSync(fd, appended, read, appendLength - read, cursor.byte_offset + read);
        if (!count) break;
        read += count;
      }
      if (read !== appendLength) throw fail('E_CURSOR_READ_SHORT', `${sourcePath}: ${read}/${appendLength} bytes read`);
    } finally { closeSync(fd); }
  }
  const previousPartial = partialBytes(cursor);
  const combined = Buffer.concat([previousPartial, appended]);
  const baseOffset = cursor.byte_offset - previousPartial.length;
  let from = 0;
  let processed = 0;
  let lineNumber = cursor.line_number;
  const rows = [];

  const consume = (row, info, endOffset) => {
    if (onLine) onLine(row, info);
    else rows.push(row);
    lineNumber += 1;
    processed += 1;
    saveCursor(checkpointPath, {
      ...cursor,
      source_path: sourcePath,
      byte_offset: endOffset,
      partial_line: '',
      partial_line_base64: '',
      line_number: lineNumber,
    });
  };

  for (let newline = combined.indexOf(0x0a); newline !== -1; newline = combined.indexOf(0x0a, from)) {
    const lineStart = from;
    const raw = combined.subarray(from, newline);
    from = newline + 1;
    const text = raw.toString('utf8').replace(/\r$/, '');
    const endOffset = baseOffset + from;
    if (!text.trim()) {
      lineNumber += 1;
      saveCursor(checkpointPath, {
        ...cursor,
        source_path: sourcePath,
        byte_offset: endOffset,
        partial_line: '',
        partial_line_base64: '',
        line_number: lineNumber,
      });
      continue;
    }
    let row;
    try { row = JSON.parse(text); } catch (cause) {
      throw fail('E_CURSOR_INVALID_JSON', `invalid JSON at ${sourcePath}:${lineNumber + 1}`, { cause });
    }
    consume(row, {
      source_path: sourcePath,
      line_number: lineNumber + 1,
      byte_start: baseOffset + lineStart,
      byte_end: endOffset,
      raw_line: text,
    }, endOffset);
  }

  const partial = combined.subarray(from);
  const finalCursor = normalizeCursor({
    ...cursor,
    source_path: sourcePath,
    byte_offset: size,
    partial_line: partial.toString('utf8'),
    partial_line_base64: partial.length ? partial.toString('base64') : '',
    line_number: lineNumber,
  }, sourcePath);
  // Persist a partial line even when no complete row was available. This is
  // the byte position that makes the next append resumable.
  if (partial.length || processed === 0) saveCursor(checkpointPath, finalCursor);
  return { rows, processed, cursor: finalCursor, partial: partial.length > 0 };
}

export class IncrementalCursor {
  constructor(checkpointPath) {
    if (!checkpointPath) throw fail('E_CURSOR_CHECKPOINT', 'checkpointPath is required');
    this.checkpointPath = checkpointPath;
  }

  load(sourcePath = null) { return loadCursor(this.checkpointPath, sourcePath); }
  save(cursor) { return saveCursor(this.checkpointPath, cursor); }
  consume(sourcePath, onLine) {
    return readIncrementalJsonl({ sourcePath, checkpointPath: this.checkpointPath, onLine });
  }
  read(sourcePath, onLine) { return this.consume(sourcePath, onLine); }
}

export const readIncremental = readIncrementalJsonl;
