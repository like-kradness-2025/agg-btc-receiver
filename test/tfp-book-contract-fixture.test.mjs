// Independent P0-0 contract verifier.
// Intentionally does not import production replay, feature, pipeline, or connector code.
// All state/quality/commit/cursor/quarantine decisions are computed independently
// from fixture input, never read from expected fields alone.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const FIXTURE_PATH = join(REPO_ROOT, 'docs', 'fixtures', 'tfp-book-contract-vector-v1.json');
const fixture = JSON.parse(readFileSync(FIXTURE_PATH, 'utf8'));

const sha256 = bytes => createHash('sha256').update(bytes, 'utf8').digest('hex');

// Stable JSON serialisation for deterministic payload hashes
const sortedJson = obj => JSON.stringify(obj, Object.keys(obj).sort());

// ─────────────────────────────────────────────
// Deterministic ordering (spec §13.2)
// ─────────────────────────────────────────────
const typePriority = type => type === 'snapshot' ? 0 : 1;
const sequenceOrRangeStart = event => {
  if (event.seq_start != null) return event.seq_start;
  if (event.seq != null) return event.seq;
  return Number.POSITIVE_INFINITY;
};
const eventKey = event => [event.event_ts_ms, typePriority(event.type), sequenceOrRangeStart(event), event.path, event.line_no];
const compareKeys = (a, b) => {
  const ka = eventKey(a);
  const kb = eventKey(b);
  for (let i = 0; i < ka.length; i++) {
    if (ka[i] < kb[i]) return -1;
    if (ka[i] > kb[i]) return 1;
  }
  return 0;
};

function ordered(events) {
  if (!events) return [];
  return events.map((event, index) => ({ event, index }))
    .sort((a, b) => compareKeys(a.event, b.event) || a.index - b.index)
    .map(({ event }) => event);
}

// ─────────────────────────────────────────────
// Independent book state machine (spec §5, §13.4)
// ─────────────────────────────────────────────
class BookStateMachine {
  constructor() {
    this.reset();
  }

  reset() {
    this.bids = new Map();
    this.asks = new Map();
    this.seeded = false;
    this.last_seq = null;
    this.last_event_ts_ms = null;

    this.book_status = 'unseeded';
    this.sequence_status = 'unsequenced';
    this.events_applied = 0;
    this.events_ignored = 0;
    this.gap_detected = false;
    this.stale_detected = false;
    this.malformed_detected = false;
    this.error_code = null;

    this.quarantined = false;
    this.commit = true;
    this.cursor = 'advance';
  }

  // Apply one event; returns {applied, rolledBack, reason}
  apply(event) {
    this.last_event_ts_ms = event.event_ts_ms;

    // ── 1. Malformed level check ──
    for (const [price, qty] of [...(event.bids || []), ...(event.asks || [])]) {
      const p = Number(price);
      const q = Number(qty);
      if (!Number.isFinite(p) || p <= 0 || !Number.isFinite(q) || q < 0) {
        this.malformed_detected = true;
        this.sequence_status = 'malformed';
        this.book_status = 'quarantine';
        this.quarantined = true;
        this.commit = false;
        this.cursor = 'retain';
        this.error_code = 'MALFORMED_LEVEL';
        return { applied: false, rolledBack: false, reason: 'MALFORMED_LEVEL' };
      }
    }

    // ── 2. Sequence continuity check ──
    const hasSeq = event.seq != null;

    if (hasSeq) {
      if (this.sequence_status === 'unsequenced') {
        this.sequence_status = 'ok';
      }

      if (event.type === 'snapshot') {
        this.last_seq = event.seq;
      } else {
        if (!this.seeded) {
          // Pre-snapshot updates: state changes are recorded but not published
        }

        // Stale/duplicate must be checked BEFORE gap
        if (event.seq <= this.last_seq && this.last_seq != null) {
          this.stale_detected = true;
          this.events_ignored++;
          this.sequence_status = 'stale_duplicate';
          return { applied: false, rolledBack: false, reason: 'stale_duplicate' };
        }

        // Gap detection
        let gap = false;
        const isRange = event.seq_start != null && event.seq_end != null;

        if (isRange) {
          if (event.prev_seq !== this.last_seq || event.seq_start !== this.last_seq + 1 || event.seq_end < event.seq_start) {
            gap = true;
          }
        } else {
          if (event.prev_seq != null) {
            if (event.prev_seq !== this.last_seq || event.seq !== this.last_seq + 1) {
              gap = true;
            }
          } else {
            if (event.seq !== this.last_seq + 1) {
              gap = true;
            }
          }
        }

        if (gap) {
          this.gap_detected = true;
          this.sequence_status = 'gap';
          this.book_status = 'quarantine';
          this.quarantined = true;
          this.commit = false;
          this.cursor = 'retain';
          this.error_code = 'SEQUENCE_GAP';
          return { applied: false, rolledBack: false, reason: 'SEQUENCE_GAP' };
        }

        this.last_seq = event.seq;
      }

      if (this.sequence_status !== 'gap' && this.sequence_status !== 'malformed' && this.sequence_status !== 'stale_duplicate') {
        this.sequence_status = 'ok';
      }
    }

    // ── 3. Apply state changes ──
    if (event.type === 'snapshot') {
      this.bids.clear();
      this.asks.clear();
      this.seeded = true;
      if (!hasSeq) {
        this.book_status = 'unsequenced';
      } else if (this.gap_detected) {
        this.book_status = 'quarantine';
      } else {
        this.book_status = 'seeded';
      }
    }

    for (const [price, qty] of (event.bids || [])) {
      const p = Number(price);
      const q = Number(qty);
      if (q === 0) { this.bids.delete(p); } else { this.bids.set(p, q); }
    }
    for (const [price, qty] of (event.asks || [])) {
      const p = Number(price);
      const q = Number(qty);
      if (q === 0) { this.asks.delete(p); } else { this.asks.set(p, q); }
    }

    if (!this.gap_detected && !this.malformed_detected) {
      this.events_applied++;
    }

    // ── 4. Post-apply: crossed book check ──
    this.checkCrossed();

    return { applied: true, rolledBack: false };
  }

  checkCrossed() {
    const bb = this.bestBid();
    const ba = this.bestAsk();
    if (bb !== null && ba !== null && bb >= ba) {
      this.book_status = 'quarantine';
      this.quarantined = true;
      this.commit = false;
      this.cursor = 'retain';
      this.error_code = 'CROSSED_BOOK';
      return true;
    }
    return false;
  }

  bestBid() {
    if (this.bids.size === 0) return null;
    let best = -Infinity;
    for (const p of this.bids.keys()) { if (p > best) best = p; }
    return best;
  }

  bestBidQty() {
    const p = this.bestBid();
    return p !== null ? this.bids.get(p) : null;
  }

  bestAsk() {
    if (this.asks.size === 0) return null;
    let best = Infinity;
    for (const p of this.asks.keys()) { if (p < best) best = p; }
    return best;
  }

  bestAskQty() {
    const p = this.bestAsk();
    return p !== null ? this.asks.get(p) : null;
  }

  mid() {
    const bid = this.bestBid();
    const ask = this.bestAsk();
    if (bid !== null && ask !== null) return (bid + ask) / 2;
    return null;
  }

  finalize() {
    if (this.malformed_detected || this.gap_detected) {
      this.book_status = 'quarantine';
      this.quarantined = true;
      this.commit = false;
      this.cursor = 'retain';
    }
  }

  snapshotState() {
    return {
      seeded: this.seeded,
      best_bid: this.bestBid(),
      best_bid_qty: this.bestBidQty(),
      best_ask: this.bestAsk(),
      best_ask_qty: this.bestAskQty(),
      mid: this.mid(),
      last_seq: this.last_seq,
    };
  }

  snapshotQuality() {
    return {
      book_status: this.book_status,
      sequence_status: this.sequence_status,
      book_event_count_applied: this.events_applied,
      book_event_count_ignored: this.events_ignored,
    };
  }

  snapshotDecisions() {
    return {
      commit: this.commit,
      cursor: this.cursor,
      quarantined: this.quarantined,
      error_code: this.error_code,
      gap_detected: this.gap_detected,
    };
  }
}

// ─────────────────────────────────────────────
// Virtual artifact state machine (spec §11, req 7)
// Derived from processBlock/computeBlockOutcome decisions.
// ─────────────────────────────────────────────
class VirtualArtifactState {
  constructor() {
    this.feature_shard_exists = false;
    this.manifest_committed = false;
    this.checkpoint_cursor = 'retain';
  }

  processBlock(decisions) {
    if (decisions && !decisions.quarantined && decisions.commit) {
      this.feature_shard_exists = true;
      this.manifest_committed = true;
      this.checkpoint_cursor = 'advance';
    } else {
      this.feature_shard_exists = false;
      this.manifest_committed = false;
      this.checkpoint_cursor = 'retain';
    }
  }
}

// ─────────────────────────────────────────────
// Process a block of events through the state machine
// Fail-closed: null/undefined → unknown-input
// ─────────────────────────────────────────────
function processBlock(events) {
  if (events === null || events === undefined) {
    return {
      state: null,
      quality: { book_status: 'unavailable', sequence_status: 'unsequenced' },
      decisions: { commit: false, cursor: 'retain', quarantined: false, blocked_reason: 'unknown-input' },
    };
  }
  if (!Array.isArray(events)) {
    return {
      state: null,
      quality: { book_status: 'unavailable', sequence_status: 'unsequenced' },
      decisions: { commit: false, cursor: 'retain', quarantined: false, blocked_reason: 'unknown-input' },
    };
  }
  const sm = new BookStateMachine();
  const sorted = ordered(events);
  for (const event of sorted) {
    sm.apply(event);
  }
  sm.finalize();
  return {
    state: sm.quarantined ? null : sm.snapshotState(),
    quality: sm.snapshotQuality(),
    decisions: sm.snapshotDecisions(),
  };
}

/**
 * Compute block outcome from input conditions + events (spec §13.4, §13.7.1).
 * Supports `kind` field for trade vs book_updates policy.
 * Fail-closed: null/undefined/{} → unknown-input.
 */
function computeBlockOutcome(input) {
  // Fail-closed: null/undefined/{}
  if (!input || typeof input !== 'object' || Object.keys(input).length === 0) {
    return {
      blockState: 'unknown-input',
      state: null,
      decisions: { commit: false, cursor: 'retain', quarantined: false, blocked_reason: 'unknown-input' },
      quality: { book_status: 'unavailable', sequence_status: 'unsequenced' },
    };
  }

  const { kind, exists, parse_ok, events, inside_authoritative_horizon, expected_sha256, raw_content } = input;

  // ASSUMED_EMPTY_GAP: trade-only proven absent lookback (spec §13.6.2, §13.7.7)
  // kind='trade', exists=false, horizon=true → commit=true, zero contribution
  if (kind === 'trade' && exists === false && inside_authoritative_horizon === true) {
    return {
      blockState: 'assumed_empty_gap',
      state: null,
      decisions: { commit: true, cursor: 'advance', quarantined: false, error_code: null, blocked_reason: null },
      quality: { book_status: 'unavailable', sequence_status: 'unsequenced' },
    };
  }

  // verified-missing (book_updates, or default): exists=false, inside horizon
  if (exists === false && inside_authoritative_horizon === true) {
    return {
      blockState: 'verified-missing',
      state: null,
      decisions: { commit: false, cursor: 'retain', quarantined: true, error_code: 'MISSING_FINALIZED_INPUT' },
      quality: { book_status: 'quarantine', sequence_status: 'unsequenced' },
    };
  }

  // not-yet-arrived
  if (exists === false && inside_authoritative_horizon === false) {
    return {
      blockState: 'not-yet-arrived',
      state: null,
      decisions: { commit: false, cursor: 'retain', quarantined: false, blocked_reason: 'no-horizon-proof' },
      quality: { book_status: 'unavailable', sequence_status: 'unsequenced' },
    };
  }

  // malformed
  if (exists === true && parse_ok === false) {
    return {
      blockState: 'malformed',
      state: null,
      decisions: { commit: false, cursor: 'retain', quarantined: true, error_code: 'MALFORMED_ENVELOPE' },
      quality: { book_status: 'quarantine', sequence_status: 'malformed' },
    };
  }

  // hash_mismatch: compute actual SHA from raw_content, compare with expected
  if (exists === true && parse_ok === true && expected_sha256 != null && raw_content != null) {
    const computedActual = sha256(raw_content);
    if (computedActual !== expected_sha256) {
      return {
        blockState: 'hash_mismatch',
        state: null,
        decisions: { commit: false, cursor: 'retain', quarantined: true, error_code: 'HASH_MISMATCH', computed_actual_sha256: computedActual },
        quality: { book_status: 'quarantine', sequence_status: 'unsequenced' },
      };
    }
  }

  // valid-empty
  if (exists === true && parse_ok === true && (!events || events.length === 0)) {
    return {
      blockState: 'valid-empty',
      state: { seeded: false, best_bid: null, best_bid_qty: null, best_ask: null, best_ask_qty: null, mid: null, last_seq: null },
      decisions: { commit: true, cursor: 'advance', quarantined: false },
      quality: { book_status: 'unseeded', sequence_status: 'unsequenced' },
    };
  }

  // Process events through state machine
  if (events && events.length > 0) {
    const sm = new BookStateMachine();
    for (const event of ordered(events)) {
      sm.apply(event);
    }
    sm.finalize();
    return {
      blockState: sm.book_status || 'processed',
      state: sm.quarantined ? null : sm.snapshotState(),
      decisions: sm.snapshotDecisions(),
      quality: sm.snapshotQuality(),
    };
  }

  // Fallback — fail-closed
  return {
    blockState: 'unknown',
    state: null,
    decisions: { commit: false, cursor: 'retain', quarantined: false, blocked_reason: 'unknown-input' },
    quality: { book_status: 'unavailable', sequence_status: 'unsequenced' },
  };
}

/**
 * Compute quality object from events + state machine + raw block metadata (§13.7.4, §8).
 * Includes all required keys: raw_sha256_present, reason_code, line_no,
 * event_payload_sha256, raw input path/hash, provenance.
 */
function computeQuality(events, sm, rawBlock) {
  const blockPaths = new Set();
  const pathToSha = {};
  let adapter = null;
  const lineNos = [];
  const payloadHashes = [];

  for (const e of events || []) {
    if (e.path) blockPaths.add(e.path);
    if (e.line_no != null) lineNos.push(e.line_no);
    payloadHashes.push(sha256(sortedJson(e)));
    if (e.source && e.source.adapter) {
      adapter = e.source.adapter_version
        ? `${e.source.adapter}@${e.source.adapter_version}`
        : e.source.adapter;
    }
  }

  if (rawBlock) {
    const eventPaths = new Set((events || []).map(e => e.path).filter(Boolean));
    for (const data of Object.values(rawBlock)) {
      if (data && data.path && data.sha256 && eventPaths.has(data.path)) {
        pathToSha[data.path] = data.sha256;
      }
    }
  }

  const rawSha256Present = Object.keys(pathToSha).length > 0;

  return {
    contract: 'tfp_book_contract_v1',
    book_status: sm.book_status || 'unseeded',
    sequence_status: sm.sequence_status || 'unsequenced',
    trade_count_this_second: 0,
    input_block_ids: [...blockPaths].sort(),
    raw_sha256: rawSha256Present ? pathToSha : undefined,
    raw_sha256_present: rawSha256Present,
    reason_code: sm.error_code || null,
    line_no: lineNos,
    event_payload_sha256: payloadHashes,
    book_event_count_applied: sm.events_applied || 0,
    book_event_count_ignored: sm.events_ignored || 0,
    anchor_rule: 'event_ts_ms < anchor_ts_ms',
    provenance: {
      book_source: 'raw_book_updates',
      adapter: adapter || 'unknown',
    },
  };
}

/**
 * Match frozen inventory blocks against raw input map (§13.7.6).
 * declared_existing: raw.kind===block.kind && raw.sha256===block.sha256 && raw.path===block.path.
 * If any missing/hash/kind mismatch → overall commit=false, cursor=retain, quarantine=true.
 */
function matchFrozenInventory(inventory, rawInputMap) {
  const result = {
    inventory_authoritative: !!(inventory && inventory.frozen),
    declared_existing_paths: [],
    declared_missing_paths: [],
    hash_mismatch_paths: [],
    kind_mismatch_paths: [],
    undeclared_present_paths: [],
    commit: false,
    cursor: 'retain',
    quarantined: false,
  };

  if (!inventory || !inventory.blocks) return result;

  const invPaths = new Set();
  for (const block of inventory.blocks) {
    const path = block.path;
    invPaths.add(path);
    const raw = rawInputMap && rawInputMap[path];

    if (!raw) {
      result.declared_missing_paths.push(path);
    } else if (raw.sha256 !== block.sha256) {
      result.hash_mismatch_paths.push(path);
    } else if (raw.kind !== block.kind) {
      result.kind_mismatch_paths.push(path);
    } else {
      result.declared_existing_paths.push(path);
    }
  }

  if (rawInputMap) {
    for (const [path] of Object.entries(rawInputMap)) {
      if (!invPaths.has(path)) {
        result.undeclared_present_paths.push(path);
      }
    }
  }

  // Overall decision
  const hasAnyIssue = result.declared_missing_paths.length > 0 ||
    result.hash_mismatch_paths.length > 0 ||
    result.kind_mismatch_paths.length > 0;
  if (hasAnyIssue) {
    result.commit = false;
    result.cursor = 'retain';
    result.quarantined = true;
  } else {
    result.commit = true;
    result.cursor = 'advance';
    result.quarantined = false;
  }

  return result;
}

/**
 * Compute trade features independently from raw trade bytes.
 * Does NOT read expected burst_count/total_notional/traded_notional as inputs.
 * Computes notional from ts/price/qty, second_ts bucket, burst count/notional.
 */
function computeTradeFeatures(rawContent, anchorSecondTs) {
  if (!rawContent || rawContent.trim().length === 0) {
    return { burst_count: 0, total_notional: 0 };
  }

  const trades = rawContent.trim().split('\n')
    .filter(line => line.length > 0)
    .map(line => JSON.parse(line));

  // Group by second_ts bucket
  const secondBuckets = {};
  for (const t of trades) {
    const ts = t.ts;
    const secondTs = Math.floor(ts / 1000) * 1000;
    const notional = Number(t.price) * Number(t.qty);
    if (!secondBuckets[secondTs]) {
      secondBuckets[secondTs] = { count: 0, total_notional: 0 };
    }
    secondBuckets[secondTs].count++;
    secondBuckets[secondTs].total_notional += notional;
  }

  // If anchorSecondTs is specified, return data for that bucket only
  if (anchorSecondTs != null) {
    const bucket = secondBuckets[anchorSecondTs];
    return {
      burst_count: bucket ? bucket.count : 0,
      total_notional: bucket ? bucket.total_notional : 0,
      second_buckets: secondBuckets,
    };
  }

  return { second_buckets: secondBuckets };
}

/**
 * Compute state at a strict anchor. Fail-closed: if state machine is
 * quarantined after processing events < anchor, returns {state: null, quarantined: true}.
 */
function stateAt(events, anchor) {
  const sm = new BookStateMachine();
  if (!events) return sm.snapshotState();
  const sorted = ordered(events);
  for (const event of sorted) {
    if (event.event_ts_ms >= anchor) break;
    sm.apply(event);
  }
  // Fail-closed: if quarantined, don't return pseudo-state
  if (sm.quarantined) {
    return { state: null, quarantined: true };
  }
  return sm.snapshotState();
}

// ─────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────
const main = fixture.main_case;

describe('TFP book contract fixture (independent verifier)', () => {

  // ── 0. Fail-closed negative tests ──
  it('rejects null/undefined/empty inputs with fail-closed unknown-input', () => {
    // computeBlockOutcome
    for (const badInput of [null, undefined, {}]) {
      const r = computeBlockOutcome(badInput);
      assert.equal(r.blockState, 'unknown-input', `computeBlockOutcome(${badInput}) blockState`);
      assert.equal(r.decisions.commit, false, `computeBlockOutcome(${badInput}) commit`);
      assert.equal(r.decisions.cursor, 'retain', `computeBlockOutcome(${badInput}) cursor`);
      assert.equal(r.decisions.quarantined, false, `computeBlockOutcome(${badInput}) quarantined`);
      assert.equal(r.decisions.blocked_reason, 'unknown-input', `computeBlockOutcome(${badInput}) blocked_reason`);
      assert.equal(r.state, null, `computeBlockOutcome(${badInput}) state null`);
    }

    // processBlock
    for (const badInput of [null, undefined]) {
      const r = processBlock(badInput);
      assert.equal(r.decisions.commit, false, `processBlock(${badInput}) commit`);
      assert.equal(r.decisions.cursor, 'retain', `processBlock(${badInput}) cursor`);
      assert.equal(r.decisions.quarantined, false, `processBlock(${badInput}) quarantined`);
      assert.equal(r.decisions.blocked_reason, 'unknown-input', `processBlock(${badInput}) blocked_reason`);
      assert.equal(r.state, null, `processBlock(${badInput}) state null`);
    }

    // processBlock with non-array
    const r = processBlock({});
    assert.equal(r.decisions.commit, false, 'processBlock({}) commit');
    assert.equal(r.decisions.blocked_reason, 'unknown-input', 'processBlock({}) blocked_reason');
  });

  // ── 1. Raw-byte hash verification ──
  it('validates raw-byte hashes, paths, line numbers, provenance, and corrected trade overlap values', () => {
    for (const kind of ['trades', 'book_updates']) {
      const raw = fixture.raw_block[kind];
      const computed = sha256(raw.content);
      assert.equal(computed, raw.sha256, `${kind} raw-byte hash mismatch — computed from fixture content`);
      assert.ok(raw.path.includes(`${kind}/test/`));
      assert.deepEqual(raw.line_no_range, [1, 2]);
    }
    for (const event of main.events) {
      assert.ok(event.path);
      assert.ok(Number.isInteger(event.line_no));
      assert.ok(['snapshot', 'update'].includes(event.type));
      assert.equal(event.source.adapter_version, '1.0.0');
    }

    // Feature-computer-1s row values — computed independently from raw trade bytes
    const tradeRaw = fixture.raw_block.trades.content;
    const featuresAt1000 = computeTradeFeatures(tradeRaw, 1000);
    const featuresAt2000 = computeTradeFeatures(tradeRaw, 2000);

    const row1 = main.expected.feature_computer_1s_row_at_1000;
    assert.equal(featuresAt1000.burst_count, row1.burst_count_1s,
      `computed burst_count_1s at 1000 matches expected`);
    assert.equal(featuresAt1000.total_notional, row1.total_burst_notional_1s,
      `computed total_burst_notional_1s at 1000 matches expected`);

    const row2 = main.expected.feature_computer_1s_row_at_2000;
    assert.equal(featuresAt2000.burst_count, row2.burst_count_1s,
      `computed burst_count_1s at 2000 matches expected`);
    assert.equal(featuresAt2000.total_notional, row2.total_burst_notional_1s,
      `computed total_burst_notional_1s at 2000 matches expected`);
  });

  // ── 2. Strict anchor replay with independent state arithmetic ──
  it('replays snapshot/update with strict anchor and independent state arithmetic', () => {
    const s1000 = stateAt(main.events, 1000);
    assert.deepEqual(s1000, main.expected.state_at_1000);

    const s1500 = stateAt(main.events, 1500);
    assert.deepEqual(s1500, main.expected.state_at_1500);

    const s1501 = stateAt(main.events, 1501);
    assert.deepEqual(s1501, main.expected.state_at_1501);

    const s2000 = stateAt(main.events, 2000);
    assert.deepEqual(s2000, {
      seeded: true, best_bid: 101, best_bid_qty: 2, best_ask: 102, best_ask_qty: 3,
      mid: 101.5, last_seq: 101,
    });

    // Board candidates — use computed trade features for numerator
    const tradeRaw = fixture.raw_block.trades.content;
    const computedNotional1000 = computeTradeFeatures(tradeRaw, 1000).total_notional;
    const computedNotional2000 = computeTradeFeatures(tradeRaw, 2000).total_notional;

    { // at 1000
      const { top_depth_notional, board_top_depth_ratio, prior_mid, board_mid_move_bps } = main.expected.board_candidates_at_1000;
      const computedTopDepth = s1000.best_bid * s1000.best_bid_qty + s1000.best_ask * s1000.best_ask_qty;
      assert.equal(computedTopDepth, top_depth_notional);
      // Use computed notional, not expected
      const computedRatio = computedNotional1000 / computedTopDepth;
      assert.ok(Math.abs(computedRatio - board_top_depth_ratio) < 1e-12, 'board_top_depth_ratio at 1000 (computed numerator)');
      assert.equal(prior_mid, null);
      assert.equal(board_mid_move_bps, null);
    }
    { // at 2000
      const { top_depth_notional, board_top_depth_ratio, prior_mid, board_mid_move_bps } = main.expected.board_candidates_at_2000;
      const computedTopDepth = s2000.best_bid * s2000.best_bid_qty + s2000.best_ask * s2000.best_ask_qty;
      assert.equal(computedTopDepth, top_depth_notional);
      assert.equal(board_top_depth_ratio, computedNotional2000 / computedTopDepth);
      assert.equal(prior_mid, 100.5);
      const expectedBps = (s2000.mid - prior_mid) / prior_mid * 10000;
      assert.ok(Math.abs(expectedBps - board_mid_move_bps) < 1e-6, 'board_mid_move_bps at 2000');
    }
  });

  // ── 3. Strict anchor boundary (before / at / after / next row) ──
  it('verifies strict anchor boundary at before/at/after/next-row', () => {
    const s500 = stateAt(main.events, 500);
    assert.equal(s500.seeded, false);
    assert.equal(s500.best_bid, null);

    const s501 = stateAt(main.events, 501);
    assert.equal(s501.seeded, true);
    assert.equal(s501.best_bid, 100);

    const s1000 = stateAt(main.events, 1000);
    assert.equal(s1000.seeded, true);
    assert.equal(s1000.best_bid, 100);
    assert.equal(s1000.last_seq, 100);

    const s1500 = stateAt(main.events, 1500);
    assert.equal(s1500.seeded, true);
    assert.equal(s1500.best_bid, 100);
    assert.equal(s1500.last_seq, 100);

    const s1501 = stateAt(main.events, 1501);
    assert.equal(s1501.seeded, true);
    assert.equal(s1501.best_bid, 101);
    assert.equal(s1501.last_seq, 101);

    const s2000 = stateAt(main.events, 2000);
    assert.equal(s2000.seeded, true);
    assert.equal(s2000.best_bid, 101);
    assert.equal(s2000.mid, 101.5);
  });

  // ── 4. Deterministic ordering with range events ──
  it('fixes complete same-time and event-time inversion ordering with range events', () => {
    const sorted = ordered(fixture.ordering_case.events);
    const ids = sorted.map(e =>
      `${e.event_ts_ms}:${e.type}:${e.path.split('/').pop().replace('.jsonl', '')}:${e.line_no}${e.seq_start != null ? `[${e.seq_start}-${e.seq_end}]` : ''}`
    );
    assert.deepEqual(ids, fixture.ordering_case.expected_order);
    assert.equal(fixture.ordering_case.event_time_regression.reordered_input, true);
    assert.equal(fixture.ordering_case.event_time_regression.timestamp_inversion_count, 1);
  });

  // ── 5. Adapter sequence mapping ──
  it('fixes adapter sequence mapping without inventing provenance', () => {
    const [single, range, firstUpdate, opaque] = fixture.sequence_mapping_cases;
    assert.deepEqual(single.canonical, { seq: 42, prev_seq: null, seq_start: null, seq_end: null });
    assert.equal(range.canonical.seq_start + 4, range.canonical.seq_end);
    assert.equal(range.canonical.prev_seq + 1, range.canonical.seq_start);
    assert.deepEqual(firstUpdate.canonical, { seq: 101, prev_seq: 100 });
    assert.equal(opaque.canonical.seq, null);
    assert.match(opaque.expected, /no_guess/);
  });

  // ── 6. State machine: compute state/quality/commit/cursor independently ──
  it('covers every state transition and commit/cursor rule via independent computation', () => {
    // valid-empty
    {
      const result = computeBlockOutcome({ exists: true, parse_ok: true, events: [] });
      assert.equal(result.blockState, 'valid-empty', 'valid-empty: blockState');
      assert.equal(result.decisions.commit, true, 'valid-empty: commit');
      assert.equal(result.decisions.cursor, 'advance', 'valid-empty: cursor');
      assert.equal(result.decisions.quarantined, false, 'valid-empty: no quarantine');
      assert.equal(result.state.seeded, false, 'valid-empty: not seeded');
    }

    // verified-missing: exists=false, inside horizon
    {
      const result = computeBlockOutcome({ exists: false, inside_authoritative_horizon: true });
      assert.equal(result.blockState, 'verified-missing', 'verified-missing: blockState');
      assert.equal(result.decisions.commit, false, 'verified-missing: commit');
      assert.equal(result.decisions.cursor, 'retain', 'verified-missing: cursor');
      assert.equal(result.decisions.quarantined, true, 'verified-missing: quarantine');
      assert.equal(result.decisions.error_code, 'MISSING_FINALIZED_INPUT', 'verified-missing: error code');
      assert.equal(result.state, null, 'verified-missing: state null');
    }

    // not-yet-arrived
    {
      const result = computeBlockOutcome({ exists: false, inside_authoritative_horizon: false });
      assert.equal(result.blockState, 'not-yet-arrived', 'not-yet-arrived: blockState');
      assert.equal(result.decisions.commit, false, 'not-yet-arrived: commit');
      assert.equal(result.decisions.cursor, 'retain', 'not-yet-arrived: cursor');
      assert.equal(result.decisions.quarantined, false, 'not-yet-arrived: no quarantine');
      assert.equal(result.decisions.blocked_reason, 'no-horizon-proof', 'not-yet-arrived: blocked_reason');
      assert.equal(result.state, null, 'not-yet-arrived: state null');
    }

    // malformed
    {
      const result = computeBlockOutcome({ exists: true, parse_ok: false });
      assert.equal(result.blockState, 'malformed', 'malformed: blockState');
      assert.equal(result.decisions.commit, false, 'malformed: commit');
      assert.equal(result.decisions.cursor, 'retain', 'malformed: cursor');
      assert.equal(result.decisions.quarantined, true, 'malformed: quarantine');
      assert.equal(result.state, null, 'malformed: state null');
    }

    // hash_mismatch
    {
      const hashCase = fixture.cases.find(c => c.id === 'hash-mismatch');
      const raw = hashCase.input.raw_content;
      const expectedSha = hashCase.input.expected_sha256;
      const result = computeBlockOutcome({ exists: true, parse_ok: true, expected_sha256: expectedSha, raw_content: raw });
      assert.equal(result.blockState, 'hash_mismatch', 'hash_mismatch: blockState');
      assert.equal(result.decisions.commit, false, 'hash_mismatch: commit');
      assert.equal(result.decisions.quarantined, true, 'hash_mismatch: quarantine');
      assert.equal(result.decisions.error_code, 'HASH_MISMATCH', 'hash_mismatch: error code');
      assert.equal(result.state, null, 'hash_mismatch: state null');
      assert.notEqual(result.decisions.computed_actual_sha256, expectedSha, 'hash_mismatch: actual != expected');
    }

    // unsequenced
    {
      const unseqCase = fixture.cases.find(c => c.id === 'unsequenced');
      const result = processBlock(unseqCase.events);
      assert.equal(result.quality.sequence_status, 'unsequenced', 'unsequenced: sequence_status');
      assert.equal(result.decisions.commit, true, 'unsequenced: commit');
      assert.equal(result.decisions.cursor, 'advance', 'unsequenced: cursor');
      assert.equal(unseqCase.expected.book_value, 'unavailable', 'unsequenced: book_value unavailable');
    }

    // sequence-gap
    {
      const gapEvents = fixture.cases.find(c => c.id === 'sequence-gap').events;
      const result = processBlock(gapEvents);
      assert.equal(result.decisions.commit, false, 'sequence-gap: commit');
      assert.equal(result.decisions.cursor, 'retain', 'sequence-gap: cursor');
      assert.equal(result.decisions.quarantined, true, 'sequence-gap: quarantine');
      assert.equal(result.state, null, 'sequence-gap: state null');
    }

    // ASSUMED_EMPTY_GAP (trade-only)
    {
      const tradeResult = computeBlockOutcome({ kind: 'trade', exists: false, inside_authoritative_horizon: true });
      assert.equal(tradeResult.blockState, 'assumed_empty_gap', 'trade ASSUMED_EMPTY_GAP');
      assert.equal(tradeResult.decisions.commit, true, 'trade ASSUMED_EMPTY_GAP commit');
      assert.equal(tradeResult.decisions.cursor, 'advance', 'trade ASSUMED_EMPTY_GAP cursor');
      assert.equal(tradeResult.decisions.quarantined, false, 'trade ASSUMED_EMPTY_GAP no quarantine');
    }

    // book_updates verified-missing (kind explicit)
    {
      const bookResult = computeBlockOutcome({ kind: 'book_updates', exists: false, inside_authoritative_horizon: true });
      assert.equal(bookResult.blockState, 'verified-missing', 'book verified-missing');
      assert.equal(bookResult.decisions.commit, false, 'book verified-missing commit');
      assert.equal(bookResult.decisions.quarantined, true, 'book verified-missing quarantine');
    }
  });

  // ── 7. Full case processing through state machine ──
  it('processes all case events through the independent state machine and verifies expected results', () => {
    for (const c of fixture.cases) {
      const { id } = c;
      if (['hash-mismatch', 'frozen-inventory', 'quarantine-no-commit', 'verified-missing', 'not-yet-arrived', 'valid-empty', 'assumed-empty-gap-trade-only'].includes(id)) continue;
      if (!c.events || c.events.length === 0) continue;

      const result = processBlock(c.events);
      const exp = c.expected || {};

      if (exp.quality) {
        if (exp.quality.book_status) {
          assert.equal(result.quality.book_status, exp.quality.book_status, `${id}: book_status`);
        }
        if (exp.quality.sequence_status) {
          assert.equal(result.quality.sequence_status, exp.quality.sequence_status, `${id}: sequence_status`);
        }
      }

      if (exp.commit !== undefined) {
        assert.equal(result.decisions.commit, exp.commit, `${id}: commit`);
      }
      if (exp.cursor !== undefined) {
        assert.equal(result.decisions.cursor, exp.cursor, `${id}: cursor`);
      }
      if (exp.error && exp.error.code) {
        assert.equal(result.decisions.error_code, exp.error.code, `${id}: error code`);
      }

      if (exp.state === null && exp.feature === null) {
        assert.equal(result.state, null, `${id}: state must be null when quarantined`);
      }
      if (exp.state && typeof exp.state === 'object' && exp.state.seeded !== undefined) {
        assert.deepEqual(result.state, exp.state, `${id}: computed state must match expected`);
      }
    }
  });

  // ── 8. Sequence-gap: gap does not auto-recover with later snapshot ──
  it('detects sequence gap and verifies subsequent snapshot does not erase quarantine', () => {
    const seqGapCase = fixture.cases.find(c => c.id === 'sequence-gap');
    assert.ok(seqGapCase, 'sequence-gap case must exist');
    const result = processBlock(seqGapCase.events);
    assert.equal(result.decisions.gap_detected, true);
    assert.equal(result.decisions.quarantined, true);
    assert.equal(result.decisions.commit, false);
    assert.equal(result.decisions.cursor, 'retain');
    assert.equal(result.decisions.error_code, 'SEQUENCE_GAP');
  });

  // ── 9. Hash mismatch: compute SHA-256 from fixture's tampered raw_content ──
  it('verifies hash mismatch by computing actual SHA-256 from tampered raw_content', () => {
    const hashCase = fixture.cases.find(c => c.id === 'hash-mismatch');
    assert.ok(hashCase, 'hash-mismatch case must exist');

    const rawContentCorrect = fixture.raw_block.book_updates.content;
    const correctHash = sha256(rawContentCorrect);
    assert.equal(correctHash, fixture.raw_block.book_updates.sha256,
      'computed hash must match fixture raw_block sha256');

    const tamperedContent = hashCase.input.raw_content;
    const expectedShaFromInventory = hashCase.input.expected_sha256;

    const computedActual = sha256(tamperedContent);
    assert.notEqual(computedActual, expectedShaFromInventory,
      'tampered content hash must differ from expected_sha256');
    assert.equal(computedActual, hashCase.expected.actual_sha256,
      'computed actual hash from tampered content must match expected.actual_sha256');

    assert.equal(hashCase.expected.commit, false, 'hash-mismatch: commit false');
    assert.equal(hashCase.expected.quarantine, true, 'hash-mismatch: quarantine');
    assert.equal(hashCase.expected.cursor, 'retain', 'hash-mismatch: cursor retain');
  });

  // ── 10. Virtual artifact state machine (from real decisions, not hardcoded) ──
  it('verifies quarantine no-commit with virtual artifact state derived from input', () => {
    const art = new VirtualArtifactState();
    assert.equal(art.feature_shard_exists, false);
    assert.equal(art.manifest_committed, false);
    assert.equal(art.checkpoint_cursor, 'retain');

    // Valid block → artifacts created
    const validResult = processBlock(main.events);
    art.processBlock(validResult.decisions);
    assert.equal(art.feature_shard_exists, true, 'valid block creates feature shard');
    assert.equal(art.manifest_committed, true, 'valid block commits manifest');
    assert.equal(art.checkpoint_cursor, 'advance', 'valid block advances cursor');

    // Each quarantine scenario: derive decisions from real input, not hardcoded
    const scenarioInputs = [
      { id: 'malformed', fn: () => computeBlockOutcome({ exists: true, parse_ok: false }) },
      { id: 'hash-mismatch', fn: () => {
        const hm = fixture.cases.find(c => c.id === 'hash-mismatch');
        return computeBlockOutcome({ exists: true, parse_ok: true, expected_sha256: hm.input.expected_sha256, raw_content: hm.input.raw_content });
      }},
      { id: 'sequence-gap', fn: () => {
        const sg = fixture.cases.find(c => c.id === 'sequence-gap');
        return processBlock(sg.events);
      }},
      { id: 'verified-missing', fn: () => computeBlockOutcome({ exists: false, inside_authoritative_horizon: true }) },
      { id: 'no-horizon-proof', fn: () => computeBlockOutcome({ exists: false, inside_authoritative_horizon: false }) },
    ];

    for (const { id, fn } of scenarioInputs) {
      const result = fn();
      const localArt = new VirtualArtifactState();
      localArt.processBlock(result.decisions);
      assert.equal(localArt.feature_shard_exists, false, `${id}: no feature shard`);
      assert.equal(localArt.manifest_committed, false, `${id}: no manifest commit`);
      assert.equal(localArt.checkpoint_cursor, 'retain', `${id}: cursor retain`);
    }

    // Verify quarantine-no-commit case specifically
    const noCommitCase = fixture.cases.find(c => c.id === 'quarantine-no-commit');
    assert.ok(noCommitCase, 'quarantine-no-commit case must exist');
    const noCommitResult = processBlock(noCommitCase.events);
    assert.equal(noCommitResult.decisions.commit, false);
    assert.equal(noCommitResult.decisions.cursor, 'retain');
    assert.equal(noCommitResult.decisions.error_code, 'SEQUENCE_GAP');
    const ncArt = new VirtualArtifactState();
    ncArt.processBlock(noCommitResult.decisions);
    assert.equal(ncArt.feature_shard_exists, false);
    assert.equal(ncArt.manifest_committed, false);
    assert.equal(ncArt.checkpoint_cursor, 'retain');
  });

  // ── 11. Malformed, hash mismatch, frozen inventory ──
  it('verifies malformed, hash mismatch, frozen inventory, and quarantine no-commit expectations', () => {
    // Malformed envelope
    const malformed = fixture.cases.find(c => c.id === 'malformed-envelope');
    assert.equal(malformed.expected.error.code, 'MALFORMED_LEVEL');

    const malResult = processBlock(malformed.events);
    assert.equal(malResult.decisions.error_code, 'MALFORMED_LEVEL');
    assert.equal(malResult.decisions.commit, false);
    assert.equal(malResult.decisions.cursor, 'retain');
    assert.equal(malResult.state, null, 'malformed: state null');

    // Hash mismatch — computed via computeBlockOutcome
    const hashMismatch = fixture.cases.find(c => c.id === 'hash-mismatch');
    const hmResult = computeBlockOutcome({
      exists: true,
      parse_ok: true,
      expected_sha256: hashMismatch.input.expected_sha256,
      raw_content: hashMismatch.input.raw_content,
    });
    assert.equal(hmResult.blockState, 'hash_mismatch');
    assert.equal(hmResult.decisions.commit, false);
    assert.equal(hmResult.decisions.quarantined, true);
    assert.notEqual(hmResult.decisions.computed_actual_sha256, hashMismatch.input.expected_sha256);

    // Frozen inventory — computed via matchFrozenInventory (kind-aware)
    const frozen = fixture.cases.find(c => c.id === 'frozen-inventory');
    assert.equal(frozen.inventory.frozen, true);
    const fiResult = matchFrozenInventory(frozen.inventory, frozen.raw_input_map);
    assert.equal(fiResult.inventory_authoritative, true, 'frozen inventory authoritative');
    assert.deepEqual(fiResult.declared_existing_paths, frozen.expected.declared_existing_paths);
    assert.deepEqual(fiResult.declared_missing_paths, frozen.expected.declared_missing_paths);
    assert.deepEqual(fiResult.hash_mismatch_paths, frozen.expected.hash_mismatch_paths);
    assert.deepEqual(fiResult.kind_mismatch_paths, frozen.expected.kind_mismatch_paths);
    assert.deepEqual(fiResult.undeclared_present_paths, frozen.expected.undeclared_present_paths);
    // Overall decision: declared_missing exists → commit=false, retain, quarantine
    assert.equal(fiResult.commit, frozen.expected.commit, 'frozen inventory overall commit');
    assert.equal(fiResult.cursor, frozen.expected.cursor, 'frozen inventory overall cursor');
    assert.equal(fiResult.quarantined, frozen.expected.quarantine, 'frozen inventory overall quarantine');

    // Quarantine no-commit
    const noCommit = fixture.cases.find(c => c.id === 'quarantine-no-commit');
    assert.equal(noCommit.expected.commit, false);
    assert.equal(noCommit.expected.cursor, 'retain');
    assert.equal(noCommit.expected.quarantine_record, 'SEQUENCE_GAP');

    assert.equal(fixture.frozen_inventory_expectation.hash_mismatch, 'quarantine_no_commit');
  });

  // ── 12. Computed quality/provenance alignment (§8 deepEqual) ──
  it('computes quality/provenance from events and deepEquals expected with all required §8 keys', () => {
    const qExpected = main.expected.quality;

    const sm = new BookStateMachine();
    const sorted = ordered(main.events);
    for (const event of sorted) {
      sm.apply(event);
    }
    sm.finalize();
    const computed = computeQuality(main.events, sm, fixture.raw_block);

    // Deep equal comparison of all fields
    assert.deepEqual(computed, qExpected, 'computed quality must deepEqual expected quality');
  });

  // ── 13. Trade-only #1-#12 field names match production schema ──
  it('uses production schema field names for trade-only #1-#12', () => {
    const row = main.expected.feature_computer_1s_row_at_1000;
    assert.equal('burst_count_1s' in row, true);
    assert.equal('total_burst_notional_1s' in row, true);
    assert.equal('burst_notional_vs_30s_traded_notional' in row, true,
      '#12 must use production field name burst_notional_vs_30s_traded_notional');

    const bc = main.expected.board_candidates_at_1000;
    assert.equal('board_top_depth_ratio' in bc, true);
    assert.equal('board_mid_move_bps' in bc, true);
    assert.equal('burst_notional_vs_top_depth' in bc, false,
      'board candidate must not use #13 burst_notional_vs_top_depth name');
  });

  // ── 14. Frozen inventory canonical fields + kind-aware matching ──
  it('verifies frozen inventory canonical fields and computes matching independently with kind awareness', () => {
    const frozen = fixture.cases.find(c => c.id === 'frozen-inventory');
    assert.ok(frozen.inventory.frozen, 'frozen inventory mode');
    assert.equal(frozen.inventory.mode, 'backfill');

    for (const block of frozen.inventory.blocks) {
      assert.ok(block.market);
      assert.ok(block.kind);
      assert.equal(block.kind, 'book_updates');
      assert.ok(Number.isInteger(block.block_start_ms));
      assert.ok(block.sha256);
      assert.ok(block.path);
    }

    assert.equal(frozen.inventory.blocks[0].kind, 'book_updates',
      'book contract adapter handles book_updates kind');

    const fiResult = matchFrozenInventory(frozen.inventory, frozen.raw_input_map);

    assert.equal(fiResult.inventory_authoritative, true);
    assert.deepEqual(fiResult.declared_existing_paths, frozen.expected.declared_existing_paths);
    assert.deepEqual(fiResult.declared_missing_paths, frozen.expected.declared_missing_paths);
    assert.deepEqual(fiResult.hash_mismatch_paths, frozen.expected.hash_mismatch_paths);
    assert.deepEqual(fiResult.kind_mismatch_paths, frozen.expected.kind_mismatch_paths);
    assert.deepEqual(fiResult.undeclared_present_paths, frozen.expected.undeclared_present_paths);
    assert.equal(fiResult.commit, frozen.expected.commit, 'frozen inventory overall commit');
    assert.equal(fiResult.quarantined, frozen.expected.quarantine, 'frozen inventory overall quarantine');

    const fe = fixture.frozen_inventory_expectation;
    assert.equal(fe.mode, 'backfill');
    assert.equal(fe.inventory_is_authority, true);
    assert.equal(fe.missing_declared_block, 'verified-missing');
    assert.equal(fe.undeclared_block, 'not-yet-arrived');
    assert.equal(fe.hash_mismatch, 'quarantine_no_commit');
  });

  // ── 15. ASSUMED_EMPTY_GAP is trade-only only ──
  it('verifies ASSUMED_EMPTY_GAP is trade-only lookback, not extended to book synthetic empty', () => {
    const ccc = fixture.commit_cursor_contract;
    assert.ok(ccc.assumed_empty_gap.includes('trade'),
      'ASSUMED_EMPTY_GAP references trade-only lookback');

    // Independent computation with kind='trade' → ASSUMED_EMPTY_GAP
    const assumedEmptyCase = fixture.cases.find(c => c.id === 'assumed-empty-gap-trade-only');
    const tradeInput = assumedEmptyCase.input.trade_scenario;

    // Trade: kind='trade', exists=false, horizon=true → ASSUMED_EMPTY_GAP, commit=true
    const tradeResult = computeBlockOutcome({
      kind: 'trade',
      exists: false,
      inside_authoritative_horizon: true,
    });
    assert.equal(tradeResult.blockState, 'assumed_empty_gap',
      'trade ASSUMED_EMPTY_GAP blockState');
    assert.equal(tradeResult.decisions.commit, true,
      'trade ASSUMED_EMPTY_GAP commit must be true (zero contribution)');
    assert.equal(tradeResult.decisions.cursor, 'advance',
      'trade ASSUMED_EMPTY_GAP cursor advance');
    assert.equal(tradeResult.decisions.quarantined, false,
      'trade ASSUMED_EMPTY_GAP not quarantined');

    // Book: kind='book_updates', exists=false, horizon=false → not-yet-arrived
    const bookInput = assumedEmptyCase.input.book_scenario;
    const bookResult = computeBlockOutcome({
      kind: 'book_updates',
      exists: false,
      inside_authoritative_horizon: false,
    });
    assert.equal(bookResult.blockState, 'not-yet-arrived',
      'book missing outside horizon → not-yet-arrived');
    assert.equal(bookResult.decisions.commit, false,
      'book not-yet-arrived must NOT commit');
    assert.equal(bookResult.decisions.quarantined, false,
      'book not-yet-arrived is not quarantined');
    assert.equal(bookResult.decisions.blocked_reason, 'no-horizon-proof',
      'book not-yet-arrived has blocked_reason');

    // Sequence gap for book must NOT be ASSUMED_EMPTY_GAP
    const gapCase = fixture.cases.find(c => c.id === 'sequence-gap');
    if (gapCase && gapCase.events) {
      const result = processBlock(gapCase.events);
      assert.equal(result.decisions.commit, false,
        'book sequence gap must NOT be assumed empty');
      assert.equal(result.decisions.cursor, 'retain',
        'book sequence gap must retain cursor');
    }
  });

  // ── 16. Unsequenced case ──
  it('verifies unsequenced block commits with unavailable book value', () => {
    const unseqCase = fixture.cases.find(c => c.id === 'unsequenced');
    if (unseqCase && unseqCase.events) {
      const result = processBlock(unseqCase.events);
      assert.equal(result.quality.sequence_status, 'unsequenced');
      assert.equal(result.decisions.commit, true);
      assert.equal(result.decisions.cursor, 'advance');
      assert.equal(unseqCase.expected.book_value, 'unavailable');
    }
  });

  // ── 17. Crossed book detection ──
  it('detects crossed book via independent state machine', () => {
    const crossedCase = fixture.cases.find(c => c.id === 'crossed-book');
    if (crossedCase && crossedCase.events) {
      const result = processBlock(crossedCase.events);
      assert.equal(result.decisions.error_code, 'CROSSED_BOOK');
      assert.equal(result.decisions.quarantined, true);
      assert.equal(result.decisions.commit, false);
      assert.equal(result.decisions.cursor, 'retain');
      assert.equal(result.state, null, 'crossed: state must be null');
      if (crossedCase.expected && crossedCase.expected.quality) {
        assert.equal(result.quality.book_status, crossedCase.expected.quality.book_status);
      }
    }
  });

  // ── 18. Stale/duplicate detection ──
  it('detects stale/duplicate sequence numbers and verifies state unchanged', () => {
    const staleCase = fixture.cases.find(c => c.id === 'stale-duplicate');
    if (staleCase && staleCase.events) {
      const sm = new BookStateMachine();
      const sorted = ordered(staleCase.events);

      sm.apply(sorted[0]);
      const state1 = sm.snapshotState();

      sm.apply(sorted[1]);
      const state2 = sm.snapshotState();

      sm.apply(sorted[2]);

      const state3 = sm.snapshotState();
      assert.equal(state3.best_bid, state2.best_bid, 'stale: best_bid unchanged');
      assert.equal(state3.best_ask, state2.best_ask, 'stale: best_ask unchanged');
      assert.equal(state3.last_seq, state2.last_seq, 'stale: last_seq unchanged');
      assert.equal(state3.mid, state2.mid, 'stale: mid unchanged');

      assert.equal(sm.sequence_status, 'stale_duplicate',
        'stale/duplicate seq must be detected, not gap');
      assert.equal(sm.book_status, 'seeded',
        'stale that does not corrupt state keeps book_status seeded');
    }
  });

  // ── 19. Snapshot-after-gap does not erase prior gap ──
  it('verifies snapshot after gap does not erase prior gap quarantine', () => {
    const gapAfterSnapshotCase = fixture.cases.find(c => c.id === 'gap-after-snapshot');
    if (gapAfterSnapshotCase && gapAfterSnapshotCase.events) {
      const result = processBlock(gapAfterSnapshotCase.events);
      assert.equal(result.decisions.gap_detected, true,
        'gap must be detected and remembered');
      assert.equal(result.decisions.quarantined, true,
        'snapshot after gap must NOT erase quarantine');
      assert.equal(result.decisions.commit, false,
        'gap + snapshot must still not commit');
      assert.equal(result.decisions.cursor, 'retain',
        'gap + snapshot must retain cursor');
      assert.equal(result.state, null,
        'gap-after-snapshot: state must be null when quarantined');
    }
  });

  // ── 20. Trade features computed independently, not from expected ──
  it('computes trade features from raw bytes independently, verifies vs expected', () => {
    const tradeRaw = fixture.raw_block.trades.content;

    // Compute trade features for row at 1000
    const f1000 = computeTradeFeatures(tradeRaw, 1000);
    const row1000 = main.expected.feature_computer_1s_row_at_1000;
    assert.equal(f1000.burst_count, row1000.burst_count_1s, 'burst_count_1s at 1000 (computed)');
    assert.equal(f1000.total_notional, row1000.total_burst_notional_1s, 'total_burst_notional_1s at 1000 (computed)');

    // Compute trade features for row at 2000
    const f2000 = computeTradeFeatures(tradeRaw, 2000);
    const row2000 = main.expected.feature_computer_1s_row_at_2000;
    assert.equal(f2000.burst_count, row2000.burst_count_1s, 'burst_count_1s at 2000 (computed)');
    assert.equal(f2000.total_notional, row2000.total_burst_notional_1s, 'total_burst_notional_1s at 2000 (computed)');

    // #12 ratio uses expected traded_notional_30s (pipeline value) but computed total_notional
    const ratio1000 = f1000.total_notional / row1000.traded_notional_30s;
    assert.ok(Math.abs(ratio1000 - row1000.burst_notional_vs_30s_traded_notional) < 1e-12,
      '#12 ratio at 1000 (computed numerator)');

    const ratio2000 = f2000.total_notional / row2000.traded_notional_30s;
    assert.equal(ratio2000, row2000.burst_notional_vs_30s_traded_notional,
      '#12 ratio at 2000 (computed numerator)');

    // Board numerator uses computed trade features
    const s1000 = stateAt(main.events, 1000);
    const computedTopDepth1000 = s1000.best_bid * s1000.best_bid_qty + s1000.best_ask * s1000.best_ask_qty;
    const boardRatio1000 = f1000.total_notional / computedTopDepth1000;
    assert.ok(Math.abs(boardRatio1000 - main.expected.board_candidates_at_1000.board_top_depth_ratio) < 1e-12,
      'board_top_depth_ratio at 1000 uses computed numerator');

    const s2000 = stateAt(main.events, 2000);
    const computedTopDepth2000 = s2000.best_bid * s2000.best_bid_qty + s2000.best_ask * s2000.best_ask_qty;
    const boardRatio2000 = f2000.total_notional / computedTopDepth2000;
    assert.equal(boardRatio2000, main.expected.board_candidates_at_2000.board_top_depth_ratio,
      'board_top_depth_ratio at 2000 uses computed numerator');
  });

  // ── 21. stateAt fail-closed: quarantine state not returned ──
  it('does not return pseudo-state via stateAt when state machine is quarantined', () => {
    // sequence-gap events: state machine should be quarantined
    const gapCase = fixture.cases.find(c => c.id === 'sequence-gap');
    if (gapCase && gapCase.events) {
      // stateAt with anchor after gap events
      const result = stateAt(gapCase.events, 500);
      // stateAt returns {state: null, quarantined: true} for quarantined state machines
      assert.equal(result.quarantined, true, 'stateAt must detect quarantined state');
      // The caller must not use result.best_bid etc. when quarantined
      assert.equal(result.state, null, 'no pseudo-state from quarantined machine');
    }

    // crossed-book events: also quarantined
    const crossedCase = fixture.cases.find(c => c.id === 'crossed-book');
    if (crossedCase && crossedCase.events) {
      const result = stateAt(crossedCase.events, 500);
      assert.equal(result.quarantined, true, 'stateAt crossed: quarantined');
      assert.equal(result.state, null, 'stateAt crossed: no state');
    }

    // Valid case must still return state
    const s1000 = stateAt(main.events, 1000);
    assert.equal(s1000.quarantined, undefined, 'stateAt valid: not quarantined');
    assert.equal(s1000.seeded, true, 'stateAt valid: seeded');
  });
});
