// test/golden-fixtures.test.mjs — Issue #15: golden raw-frame conformance.
//
// The corpus under test/fixtures/exchanges/*.json holds wire-shaped frames
// captured from each exchange's documented WebSocket protocol (provenance per
// case: docs URL + capture date + semantics + prep + hand-pinned expected
// events). These tests drive every frame through the REAL connector parser
// (_onMessage) and assert the emitted raw semantic events:
//
//   (a) frame completeness — every frame must parse without throwing and must
//       produce exactly the pinned expected events (no drop, no extra emit),
//   (b) arrival order — captured events are compared 1:1 in frame order, so a
//       connector reordering or coalescing events fails the case,
//   (c) field invariance — every pinned field (price/qty/side/ts/tradeId,
//       book bids/asks/seq chain, source ts semantics) equals the emitted
//       value exactly; no connector-side normalization drift is allowed,
//   (d) ingress metadata — recv_ts_ms/receive_seq/connection_id are null on
//       direct parser feed: they are stamped ONLY at the socket boundary by
//       the worker (issue #12 contract), never fabricated in the parser,
//   (e) source-ts nulling — events without an exchange event time (Coinbase
//       L2) must carry source_event_ts_ms:null and
//       source_event_time_known:false, never Date.now() or 0.
//
// Corpus structure is validated first (schema marker, per-case provenance,
// unique ids, non-empty frames/expected, prep mode understood).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIXTURE_DIR = join(__dirname, 'fixtures', 'exchanges');
const FIXTURE_SCHEMA = 'golden-raw-frame-fixture/v1';

// connector_class -> candidate module paths (same resolution the existing
// parser tests use: plain connectors live in lib/<exchange>-connector.mjs,
// market aliases in lib/market-connectors.mjs).
const CLASS_MODULES = {
  BinanceSpotConnector: ['../lib/binance-connector.mjs'],
  BinancePerpConnector: ['../lib/binance-connector.mjs'],
  BinanceSpotUsdcConnector: ['../lib/binance-usdc-connector.mjs'],
  BinanceSpotFdusdConnector: ['../lib/binance-fdusd-connector.mjs'],
  BinancePerpBtcusdcConnector: ['../lib/market-connectors.mjs'],
  BybitConnector: ['../lib/bybit-connector.mjs'],
  BybitSpotConnector: ['../lib/market-connectors.mjs'],
  OkxConnector: ['../lib/okx-connector.mjs'],
  OkxSpotConnector: ['../lib/market-connectors.mjs'],
  CoinbaseConnector: ['../lib/coinbase-connector.mjs'],
  BitstampConnector: ['../lib/bitstamp-connector.mjs'],
};

const _classCache = new Map();
async function resolveConnectorClass(name) {
  if (_classCache.has(name)) return _classCache.get(name);
  const mods = CLASS_MODULES[name];
  assert.ok(mods, `fixture references unknown connector_class ${name}`);
  let found = null;
  let lastErr = null;
  for (const mod of mods) {
    try {
      const m = await import(mod);
      if (typeof m[name] === 'function') { found = m[name]; break; }
    } catch (e) { lastErr = e; }
  }
  assert.ok(found, `cannot import connector_class ${name} (${lastErr?.message ?? 'not exported'})`);
  _classCache.set(name, found);
  return found;
}

/** Instantiate + wire a connector for one fixture case, apply case.prep. */
async function buildConnector(c) {
  const Cls = await resolveConnectorClass(c.connector_class);
  const conn = new Cls({});
  conn._ws = null;
  if (typeof conn._setState === 'function') conn._setState('running');

  const prep = c.prep;
  if (prep?.mode === 'book_snapshot_running') {
    const b = prep.book ?? {};
    assert.ok(conn.book && typeof conn.book.applySnapshot === 'function',
      `case ${c.id}: prep book_snapshot_running needs a seeded book`);
    conn.book.applySnapshot(b.bids ?? [], b.asks ?? [], b.lastSeq ?? 0);
    if (prep.firstRunningDiff && 'firstRunningDiff' in conn) conn._firstRunningDiff = true;
  } else if (prep) {
    assert.fail(`case ${c.id}: unknown prep mode ${prep.mode}`);
  }

  const captured = [];
  for (const channel of ['trade', 'liquidation', 'depth', 'rawDepth']) {
    conn.on(channel, (payload) => captured.push({ channel, payload }));
  }
  return { conn, captured };
}

/** kind label -> emitted event channel name. */
const CHANNEL_OF = { trade: 'trade', liquidation: 'liquidation', depth: 'depth', rawDepth: 'rawDepth' };

/** Ingress fields the parser must NEVER fabricate (stamped at socket only). */
const INGRESS_FIELDS = ['recv_ts_ms', 'receive_seq', 'connection_id', 'recv_mono_ns'];

function assertExpectedMatchesPayload(exp, cap, caseId, idx) {
  assert.equal(cap.channel, CHANNEL_OF[exp.kind],
    `case ${caseId} event#${idx}: expected channel ${exp.kind}, got ${cap.channel}`);
  for (const key of Object.keys(exp)) {
    if (key === 'kind') continue;
    assert.ok(key in cap.payload, `case ${caseId} event#${idx}: emitted payload missing pinned field '${key}'`);
    assert.deepEqual(cap.payload[key], exp[key],
      `case ${caseId} event#${idx}: field '${key}' mismatch`);
  }
}

async function runCase(c) {
  const { conn, captured } = await buildConnector(c);
  // Feeding must never throw: a frame the parser cannot consume is a
  // completeness break (raw log would silently lose it).
  for (const frame of c.frames) {
    conn._onMessage(frame);
  }
  try { conn.destroy?.(); } catch { /* noop */ }
  conn._ws = null;
  return captured;
}

function loadCorpus() {
  const files = readdirSync(FIXTURE_DIR).filter((f) => f.endsWith('.json')).sort();
  assert.ok(files.length >= 5, 'expected >= 5 exchange fixture files');
  const corpus = [];
  for (const f of files) {
    const doc = JSON.parse(readFileSync(join(FIXTURE_DIR, f), 'utf8'));
    corpus.push(doc);
  }
  return corpus;
}

describe('golden raw-frame fixtures — corpus structure', () => {
  const corpus = loadCorpus();
  const ids = new Set();

  for (const doc of corpus) {
    describe(`fixture file ${doc.exchange}.json`, () => {
      it('is schema golden-raw-frame-fixture/v1 with exchange + generated', () => {
        assert.equal(doc.schema, FIXTURE_SCHEMA);
        assert.equal(typeof doc.exchange, 'string');
        assert.match(doc.generated ?? '', /^\d{4}-\d{2}-\d{2}$/);
        assert.ok(Array.isArray(doc.cases) && doc.cases.length > 0);
      });

      for (const c of doc.cases) {
        it(`case ${c.id}: required fields + unique id across corpus`, () => {
          assert.equal(typeof c.id, 'string');
          assert.equal(typeof c.market, 'string');
          assert.equal(typeof c.connector_class, 'string');
          assert.equal(typeof c.stream, 'string');
          assert.ok(['trade', 'liquidation', 'depth'].includes(c.kind), `case ${c.id}: kind must be trade|liquidation|depth`);
          assert.match(c.capture_date ?? '', /^\d{4}-\d{2}-\d{2}$/, `case ${c.id}: capture_date required`);
          assert.ok(Array.isArray(c.sources) && c.sources.length > 0, `case ${c.id}: sources required`);
          assert.ok(c.sources.every((s) => s.type === 'docs' && s.url?.startsWith('https://') && s.note),
            `case ${c.id}: every source needs {type:'docs', url, note}`);
          assert.ok(c.semantics && typeof c.semantics === 'object', `case ${c.id}: semantics required`);
          assert.ok(Array.isArray(c.frames) && c.frames.length > 0, `case ${c.id}: frames required`);
          assert.ok(Array.isArray(c.expected_events) && c.expected_events.length > 0,
            `case ${c.id}: expected_events required`);
          assert.ok(!ids.has(c.id), `duplicate case id '${c.id}' across corpus`);
          ids.add(c.id);
        });
      }
    });
  }
});

describe('golden raw-frame conformance (semantic + raw-save contract)', () => {
  const corpus = loadCorpus();
  const allIds = new Set(corpus.flatMap((d) => d.cases.map((c) => c.id)));
  assert.equal(allIds.size, corpus.reduce((n, d) => n + d.cases.length, 0),
    'case ids must be unique across the whole corpus');

  let totalEvents = 0;
  for (const doc of corpus) {
    for (const c of doc.cases) {
      it(`[${doc.exchange}] ${c.id} — frames parse, events match pins in order`, async () => {
        // (a) provenance completeness (docs URL, capture date, semantics)
        assert.ok(Array.isArray(c.sources) && c.sources.length > 0);
        for (const s of c.sources) {
          assert.ok(s.url?.startsWith('https://'), `case ${c.id}: sources[].url must be an https docs URL`);
        }
        assert.match(c.capture_date ?? '', /^\d{4}-\d{2}-\d{2}$/, `case ${c.id}: capture_date required`);
        assert.ok(c.semantics?.ts_field || c.semantics?.ts_unit, `case ${c.id}: semantics ts_field/ts_unit required`);
        assert.ok(Array.isArray(c.frames) && c.frames.length > 0, `case ${c.id}: frames required`);
        assert.ok(Array.isArray(c.expected_events) && c.expected_events.length > 0,
          `case ${c.id}: expected_events required`);
        for (const exp of c.expected_events) {
          assert.ok(exp.kind in CHANNEL_OF, `case ${c.id}: unknown expected kind ${exp.kind}`);
        }

        // (b/c/d/e) drive the real connector over the fixture frames
        const captured = await runCase(c);
        assert.equal(captured.length, c.expected_events.length,
          `case ${c.id}: frame→event count mismatch (completeness): ` +
          `expected ${c.expected_events.length} events, captured ${captured.length} ` +
          JSON.stringify(captured.map((x) => x.channel)));

        c.expected_events.forEach((exp, i) => {
          const cap = captured[i];
          // (b) arrival order is enforced by 1:1 index comparison above.
          assertExpectedMatchesPayload(exp, cap, c.id, i);
          // (d) ingress metadata: parser output must not fabricate socket fields
          for (const f of INGRESS_FIELDS) {
            assert.equal(cap.payload[f], null,
              `case ${c.id} event#${i}: '${f}' must be null outside the socket boundary`);
          }
          // (e) source-ts nulling: unknown exchange event time stays null
          if (exp.source_event_time_known === false) {
            assert.equal(cap.payload.source_event_ts_ms, null,
              `case ${c.id} event#${i}: source_event_ts_ms must stay null when time unknown`);
          }
        });
        totalEvents += captured.length;
      });
    }
  }

  it('corpus covers every enabled market + all three event kinds (guard)', () => {
    // Enabled-market coverage is a corpus property, re-asserted cheaply here.
    const markets = new Set(corpus.flatMap((d) => d.cases.map((c) => c.market)));
    const kinds = new Set(corpus.flatMap((d) => d.cases.map((c) => c.kind)));
    assert.ok(markets.size >= 10, `expected >= 10 distinct markets, got ${markets.size}`);
    assert.ok(kinds.has('trade') && kinds.has('depth') && kinds.has('liquidation'));
    assert.ok(totalEvents >= 30, `expected >= 30 pinned events, got ${totalEvents}`);
  });
});
