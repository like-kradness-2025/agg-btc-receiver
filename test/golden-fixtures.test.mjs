// test/golden-fixtures.test.mjs — Issue #15: golden raw-frame conformance.
//
// The corpus under test/fixtures/exchanges/*.json holds wire-shaped frames
// captured from each exchange's documented WebSocket protocol (provenance per
// case: docs URL + capture date + semantics + prep + hand-pinned expected
// events). These tests drive every frame through the REAL connector parser
// (_onMessage) and assert the emitted raw semantic events:
//
//   (a) frame completeness — every frame must parse without throwing and must
//       produce exactly the events pinned to it (per-frame: no frame dropped,
//       no frame double-emitted — a total-count match alone cannot see a
//       frame#2 drop paired with a frame#3 double emit),
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
// Every expected_event pins `frame`: the index of the corpus frame that must
// produce it (0 = c.frames[0]). Zero-event frames (subscribe acks,
// heartbeats, 'te' previews) are valid and must stay silent.
//
// Corpus structure is validated first (schema marker, per-case provenance,
// unique ids, frame/event shape, frame indexes in range).

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
  KrakenSpotConnector: ['../lib/kraken-connector.mjs'],
  BitfinexConnector: ['../lib/bitfinex-connector.mjs'],
  HyperliquidConnector: ['../lib/hyperliquid-connector.mjs'],
};

// Provenance allowlist: the docs host(s) each exchange's source URLs may use.
// A fixture citing a host outside its exchange's allowlist is provenance
// drift (frame copied from the wrong protocol docs) and must fail.
const EXCHANGE_DOC_HOSTS = {
  binance: ['developers.binance.com'],
  bitfinex: ['docs.bitfinex.com'],
  bitstamp: ['www.bitstamp.net'],
  bybit: ['bybit-exchange.github.io'],
  coinbase: ['docs.cdp.coinbase.com'],
  hyperliquid: ['hyperliquid.gitbook.io'],
  kraken: ['docs.kraken.com'],
  okx: ['www.okx.com'],
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
    if (prep.firstRunningDiff) {
      // The real connector property is `_firstRunningDiff` (initialised by
      // base-connector). Engaging the prep must be LOUD: if the connector
      // does not support it the case silently tests the wrong book path.
      assert.ok('_firstRunningDiff' in conn,
        `case ${c.id}: prep firstRunningDiff requires a connector exposing _firstRunningDiff`);
      conn._firstRunningDiff = true;
    }
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

// 'kind' and 'frame' are fixture-side control keys, never payload pins.
const CONTROL_KEYS = new Set(['kind', 'frame']);

function assertExpectedMatchesPayload(exp, cap, caseId, frameIdx, idx) {
  assert.ok(cap, `case ${caseId} frame#${frameIdx} event#${idx}: missing captured event`);
  assert.equal(cap.channel, CHANNEL_OF[exp.kind],
    `case ${caseId} frame#${frameIdx} event#${idx}: expected channel ${exp.kind}, got ${cap.channel}`);
  for (const key of Object.keys(exp)) {
    if (CONTROL_KEYS.has(key)) continue;
    assert.ok(key in cap.payload, `case ${caseId} frame#${frameIdx} event#${idx}: emitted payload missing pinned field '${key}'`);
    assert.deepEqual(cap.payload[key], exp[key],
      `case ${caseId} frame#${frameIdx} event#${idx}: field '${key}' mismatch`);
  }
}

/**
 * Drive the real connector over the fixture frames, keeping the events each
 * frame produced separate so frame→event completeness can be asserted.
 */
async function runCasePerFrame(c) {
  const { conn, captured } = await buildConnector(c);
  const perFrame = [];
  for (const frame of c.frames) {
    const before = captured.length;
    // Feeding must never throw: a frame the parser cannot consume is a
    // completeness break (raw log would silently lose it). _onMessage is
    // synchronous today; a thenable return is awaited so a future async
    // parser cannot swallow rejections silently.
    const ret = conn._onMessage(frame);
    if (ret && typeof ret.then === 'function') await ret;
    perFrame.push(captured.slice(before));
  }
  // Tear-down must not be silently swallowed: a connector that cannot
  // destroy cleanly would leak sockets/timers into later cases.
  if (typeof conn.destroy === 'function') {
    const d = conn.destroy();
    if (d && typeof d.then === 'function') await d;
  }
  conn._ws = null;
  return { captured, perFrame };
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
          assert.ok(c.frames.every((f) => f !== null && typeof f === 'object'),
            `case ${c.id}: every frame must be a JSON object/array (parsed wire value)`);
          assert.ok(Array.isArray(c.expected_events) && c.expected_events.length > 0,
            `case ${c.id}: expected_events required`);
          assert.ok(!ids.has(c.id), `duplicate case id '${c.id}' across corpus`);
          ids.add(c.id);
        });

        it(`case ${c.id}: every expected event pins a valid kind + in-range frame`, () => {
          for (const [i, exp] of c.expected_events.entries()) {
            assert.ok(exp && typeof exp === 'object', `case ${c.id}: expected_events[${i}] must be an object`);
            assert.ok(exp.kind in CHANNEL_OF, `case ${c.id}: unknown expected kind ${exp.kind}`);
            assert.ok(Number.isInteger(exp.frame), `case ${c.id}: expected_events[${i}] must pin integer frame`);
            assert.ok(exp.frame >= 0 && exp.frame < c.frames.length,
              `case ${c.id}: expected_events[${i}] frame ${exp.frame} out of range (0..${c.frames.length - 1})`);
          }
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

  for (const doc of corpus) {
    for (const c of doc.cases) {
      it(`[${doc.exchange}] ${c.id} — frames parse, events match pins in order`, async () => {
        // (a) provenance completeness (docs URL host allowlist, capture date,
        // semantics ts_field/ts_unit)
        assert.ok(Array.isArray(c.sources) && c.sources.length > 0);
        const allowedHosts = EXCHANGE_DOC_HOSTS[doc.exchange] ?? [];
        for (const s of c.sources) {
          assert.ok(s.url?.startsWith('https://'), `case ${c.id}: sources[].url must be an https docs URL`);
          let host;
          try { host = new URL(s.url).host; } catch { assert.fail(`case ${c.id}: sources[].url not parseable: ${s.url}`); }
          assert.ok(allowedHosts.includes(host),
            `case ${c.id}: docs host '${host}' not allowlisted for exchange '${doc.exchange}' (allowed: ${allowedHosts.join(', ') || 'none'})`);
        }
        assert.match(c.capture_date ?? '', /^\d{4}-\d{2}-\d{2}$/, `case ${c.id}: capture_date required`);
        assert.ok(c.semantics?.ts_field || c.semantics?.ts_unit, `case ${c.id}: semantics ts_field/ts_unit required`);
        assert.ok(Array.isArray(c.frames) && c.frames.length > 0, `case ${c.id}: frames required`);
        assert.ok(Array.isArray(c.expected_events) && c.expected_events.length > 0,
          `case ${c.id}: expected_events required`);

        // Group the pinned events by the frame that must produce them.
        const perFrameExpected = c.frames.map(() => []);
        for (const exp of c.expected_events) perFrameExpected[exp.frame].push(exp);

        // (b/c/d/e) drive the real connector over the fixture frames
        const { captured, perFrame } = await runCasePerFrame(c);

        // (a) frame→event completeness, pinned per frame. Total-count equality
        // alone would pass a frame#2 drop paired with a frame#3 double emit;
        // per-frame equality cannot.
        for (let frameIdx = 0; frameIdx < c.frames.length; frameIdx++) {
          const expected = perFrameExpected[frameIdx];
          const got = perFrame[frameIdx] ?? [];
          assert.equal(got.length, expected.length,
            `case ${c.id} frame#${frameIdx}: produced ${got.length} event(s), pinned ${expected.length} ` +
            `(frame→event completeness: ${got.map((x) => x.channel).join(',') || 'none'})`);
          expected.forEach((exp, i) => {
            const cap = got[i];
            // (b) arrival order is enforced by the 1:1 index comparison above.
            assertExpectedMatchesPayload(exp, cap, c.id, frameIdx, i);
            // (d) ingress metadata: parser output must not fabricate socket fields
            for (const f of INGRESS_FIELDS) {
              assert.equal(cap.payload[f], null,
                `case ${c.id} frame#${frameIdx} event#${i}: '${f}' must be null outside the socket boundary`);
            }
            // (e) source-ts nulling: unknown exchange event time stays null
            if (exp.source_event_time_known === false) {
              assert.equal(cap.payload.source_event_ts_ms, null,
                `case ${c.id} frame#${frameIdx} event#${i}: source_event_ts_ms must stay null when time unknown`);
            }
          });
        }

        // Every emitted event must be attributable to a frame (events emitted
        // outside frame processing — prep/destroy side effects — are leaks).
        const frameTotal = perFrame.reduce((n, a) => n + a.length, 0);
        assert.equal(frameTotal, captured.length,
          `case ${c.id}: ${captured.length - frameTotal} event(s) emitted outside frame processing`);
        assert.equal(captured.length, c.expected_events.length,
          `case ${c.id}: total event count mismatch: expected ${c.expected_events.length}, captured ${captured.length}`);
      });
    }
  }

  it('corpus covers every enabled market + all three event kinds (guard)', () => {
    // Corpus property, computed statically — independent of test execution
    // order (no counters accumulated across earlier its()).
    const markets = new Set(corpus.flatMap((d) => d.cases.map((c) => c.market)));
    const kinds = new Set(corpus.flatMap((d) => d.cases.map((c) => c.kind)));
    const pinned = corpus.reduce(
      (n, d) => n + d.cases.reduce((m, c) => m + c.expected_events.length, 0), 0);
    assert.ok(markets.size >= 10, `expected >= 10 distinct markets, got ${markets.size}`);
    assert.ok(kinds.has('trade') && kinds.has('depth') && kinds.has('liquidation'));
    assert.ok(pinned >= 30, `expected >= 30 pinned events, got ${pinned}`);
  });
});
