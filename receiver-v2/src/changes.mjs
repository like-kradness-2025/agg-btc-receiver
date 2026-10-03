/**
 * The level-changes block that rides on a frame's envelope (three-process design,
 * docs/fix-plan-sets.md §5.8, ruling ③ - Astra).
 *
 * In the single-process structure the venue adapter's `changesFor` is consulted by the consumer.
 * In the split that derivation moves to the side that actually parses the bytes - ingest - and the
 * result travels with the frame, so organize and the book never re-ask an adapter they do not have
 * (the supervisor in particular carries no adapter at all). The block is written into the envelope's
 * `meta`, which the envelope's wire format already preserves end to end: the spool stores encoded
 * envelopes, and the delivery ledger stores each frame's `meta` and `raw` verbatim, so a recovery or
 * a redelivery reuses the same derived result rather than deriving it again.
 *
 * The block has a version and a validation rule. A frame whose block is missing, of an unknown
 * version, or malformed is refused - a missing derivation is never smoothed over into "an empty
 * change", which would silently drop the levels the frame carried (C5).
 *
 * This module is pure: it opens no clock, no socket and no file.
 */

import { makeEnvelope } from './envelope.mjs';

/** The version of the changes block. Bumped only when its shape changes incompatibly. */
export const CHANGES_FORMAT = 'v1';

/** The `meta` keys the block occupies. Reserved: an adapter must not already use them. */
export const CHANGES_FORMAT_KEY = 'changes_format';
export const CHANGES_KEY = 'changes';

const SIDES = new Set(['bid', 'ask']);

/** True when a level object names a side, a finite price and a finite size. */
function isLevel(value) {
  return (
    value !== null &&
    typeof value === 'object' &&
    SIDES.has(value.side) &&
    Number.isFinite(value.price) &&
    Number.isFinite(value.size)
  );
}

/**
 * Validate one changes block against `v1` (C5). Returns `{ok:true, replace, changes}` with a
 * normalized, plain copy, or `{ok:false, reason}`. The two shapes are the contract's:
 *
 *   { replace: true,  levels:  [...] }   the whole board is replaced
 *   { replace: false, changes: [...] }   a diff (a size of 0 removes a level)
 *
 * Anything else - a bare array, a missing `replace`, a non-array list, a malformed level - is
 * refused rather than coerced, because guessing here is exactly how a replacement is mistaken for a
 * diff and the board is left crossed (C5).
 */
export function validateChanges(value, { format = CHANGES_FORMAT } = {}) {
  if (format !== CHANGES_FORMAT) {
    return { ok: false, reason: `unknown level-changes format ${JSON.stringify(format)}` };
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return { ok: false, reason: 'the level changes must be an object stating replace true or false' };
  }
  if (value.replace === true) {
    if (!Array.isArray(value.levels)) {
      return { ok: false, reason: 'a replacement must carry a levels array' };
    }
    const levels = [];
    for (const level of value.levels) {
      if (!isLevel(level)) return { ok: false, reason: 'a replacement carried a malformed level' };
      levels.push({ side: level.side, price: level.price, size: level.size });
    }
    return { ok: true, replace: true, levels };
  }
  if (value.replace === false) {
    if (!Array.isArray(value.changes)) {
      return { ok: false, reason: 'a diff must carry a changes array' };
    }
    const changes = [];
    for (const change of value.changes) {
      if (!isLevel(change)) return { ok: false, reason: 'a diff carried a malformed change' };
      changes.push({ side: change.side, price: change.price, size: change.size });
    }
    return { ok: true, replace: false, changes };
  }
  return { ok: false, reason: 'the level changes must state replace true or false' };
}

/**
 * Derive a frame's level changes with the venue adapter. Ingest owns this call (ruling ③): the
 * adapter is the only part that can read the raw bytes, and it is passed unchanged - a result of the
 * wrong shape, or no `changesFor` at all, is refused, never treated as "no changes".
 */
export function deriveChanges(adapter, envelope) {
  if (typeof adapter?.changesFor !== 'function') {
    return { ok: false, reason: 'the adapter declares no changesFor, so no level changes can be derived' };
  }
  let value;
  try {
    value = adapter.changesFor(envelope);
  } catch (error) {
    return { ok: false, reason: `the adapter refused to derive level changes: ${error.message}` };
  }
  if (value === null || value === undefined) {
    return { ok: false, reason: 'the adapter derived no level changes for the frame' };
  }
  return validateChanges(value);
}

/**
 * Attach a validated block to a frame, returning a fresh envelope. The original is untouched (the
 * envelope factory freezes what it returns); everything but `meta` is carried over verbatim, and
 * `meta` keeps whatever else the frame already declared (a `first_seq`, a `venue_seq`).
 */
export function attachChanges(envelope, derived, { format = CHANGES_FORMAT } = {}) {
  const block = derived.replace === true ? { replace: true, levels: derived.levels } : { replace: false, changes: derived.changes };
  return makeEnvelope({
    market: envelope.market,
    stream: envelope.stream,
    connectionId: envelope.connection_id,
    runId: envelope.run_id ?? null,
    venue: envelope.venue ?? null,
    generation: envelope.generation ?? null,
    receiveSeq: envelope.receive_seq,
    recvTsMs: envelope.recv_ts_ms,
    recvMonoNs: envelope.recv_mono_ns,
    raw: envelope.raw,
    meta: { ...(envelope.meta ?? {}), [CHANGES_FORMAT_KEY]: format, [CHANGES_KEY]: block },
  });
}

/**
 * Read the block off a frame. A missing version, an unknown version, a missing block or a malformed
 * one is refused with a reason - the frame is not applied on the strength of a derivation nobody
 * made, and an absent block is not read as an empty change.
 */
export function readChanges(envelope) {
  const meta = envelope?.meta;
  if (meta === undefined || meta === null || typeof meta !== 'object') {
    return { ok: false, reason: 'the frame carries no level-changes block' };
  }
  const format = meta[CHANGES_FORMAT_KEY];
  if (format !== CHANGES_FORMAT) {
    return {
      ok: false,
      reason: `the frame's level-changes format is not supported: ${JSON.stringify(format)}`,
    };
  }
  if (!Object.prototype.hasOwnProperty.call(meta, CHANGES_KEY) || meta[CHANGES_KEY] === undefined) {
    return { ok: false, reason: 'the frame carries no level changes' };
  }
  return validateChanges(meta[CHANGES_KEY], { format });
}
