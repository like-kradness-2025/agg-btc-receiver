/**
 * Bitstamp WebSocket API v2: the spot market this receiver admits - bitstamp_spot (BTC/USD).
 *
 * The wire contract here is the official one, checked against the live public stream on
 * `wss://ws.bitstamp.net` and the public REST order book:
 *
 *  - Two channels are asked for, each in its own `bts:subscribe` frame: `live_trades_btcusd` (trades)
 *    and `diff_order_book_btcusd` (the book diffs). The acknowledgement is
 *    `bts:subscription_succeeded` per channel; a bad subscription string answers with `bts:error`;
 *    a duplicate subscription is re-acknowledged without interrupting the stream (all measured
 *    live). `bts:request_reconnect` is the venue asking for a fresh socket.
 *  - The depth channel is a DIFF feed: each `data` frame carries only the changed levels as
 *    `[price, amount]` pairs with an absolute amount, `0.00000000` removing the level, plus a
 *    matching-engine `microtimestamp` (microseconds). There is no sequence number and no checksum;
 *    the venue's own ordering fact is that microsecond clock, and it is the proof this adapter
 *    carries: the diff stream must never move backwards in it (an equal stamp is not stale - the
 *    clock is microsecond-granular - but a regression fails closed until a new connection).
 *  - Because the diff feed cannot bootstrap itself, the sync is the venue's own documented
 *    reconciliation algorithm (WebSocket API v2, "Reconciliation algorithm for diff_order_book"),
 *    kept whole: subscribe first and buffer every diff, fetch the REST order book
 *    (`/api/v2/order_book/btcusd/?group=1` - group=1 is the documented default grouping the diff
 *    feed pairs with; the snapshot carries the same `microtimestamp`), discard every buffered diff
 *    whose stamp is at or before the snapshot's, replay the rest in order, then continue live. A
 *    buffered diff that regresses over an earlier buffered one is an anomaly the algorithm does
 *    not describe: it refetches a newer snapshot (bounded attempts), which provably covers all of
 *    them. This is why the adapter sends its own subscription frames inside connection
 *    preparation, before the REST fetch: the connection's fixed order is prepare-then-subscribe,
 *    and for this venue subscribing first is what the algorithm requires (buffer before the
 *    snapshot).
 *  - Keep-alive is the client heartbeat `{"event":"bts:heartbeat"}` (answered with
 *    `{"event":"bts:heartbeat","channel":"","data":{"status":"success"}}`, measured live), sent
 *    after 15 s without a word from the venue - the quiet-period cadence v1 kept with
 *    websocket-level pings, kept here as the adapter's no-activity form.
 */

import { rawTrade, rawBook, collapse } from './raw-shape.mjs';

const BITSTAMP_SYMBOLS = Object.freeze({
  bitstamp_spot: Object.freeze(['BTC/USD']),
});

const BITSTAMP_WS = 'wss://ws.bitstamp.net';
const BITSTAMP_REST_BASE = 'https://www.bitstamp.net/api/v2/order_book';

/** v1's bounded sync attempts: every refetch provably gets a strictly newer boundary. */
const MAX_SYNC_ATTEMPTS = 3;

/** v1's REST deadline: a snapshot that never arrives must not hold the preparation forever. */
const DEFAULT_REST_TIMEOUT_MS = 10_000;

/** A preparation buffer beyond this is not a sync waiting to happen; it is a run that must restart. */
const MAX_PREPARATION_DIFFS = 10_000;

/** The quiet-period cadence v1 kept with websocket-level pings. */
const KEEPALIVE_SILENCE_MS = 15_000;

function textOf(raw) {
  return typeof raw === 'string' ? raw : raw?.toString?.('utf8') ?? '';
}

/** Parse a raw that may be bytes, a string, or an already-parsed object. Null when it is none of those. */
function asObject(value) {
  if (typeof value === 'string' || Buffer.isBuffer(value)) {
    let parsed;
    try {
      parsed = JSON.parse(textOf(value));
    } catch {
      return null;
    }
    return parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
  }
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value : null;
}

function finiteNumber(value) {
  if (typeof value !== 'number' && typeof value !== 'string') return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

/** The matching-engine microsecond clock, as an integer. */
function usOf(value) {
  const n = finiteNumber(value);
  if (n === null || !Number.isInteger(n) || n <= 0) return null;
  return n;
}

/** The `[price, amount]` pairs of one side, or null when any entry is shapeless. A zero amount survives: it removes. */
function levelsOf(source) {
  if (!Array.isArray(source)) return null;
  const rows = [];
  for (const entry of source) {
    if (!Array.isArray(entry) || entry.length !== 2) return null;
    const price = finiteNumber(entry[0]);
    const size = finiteNumber(entry[1]);
    if (price === null || price <= 0) return null;
    if (size === null || size < 0) return null;
    rows.push({ price, size });
  }
  return rows;
}

/** A well-formed diff frame for our channel, normalized, or null. */
function makeDiffOf(diffChannel) {
  return (value) => {
    const data = asObject(value);
    if (data === null || data.event !== 'data' || data.channel !== diffChannel) return null;
    const payload = data.data;
    if (payload === null || typeof payload !== 'object') return null;
    const us = usOf(payload.microtimestamp);
    if (us === null) return null;
    const bids = levelsOf(payload.bids);
    const asks = levelsOf(payload.asks);
    if (bids === null || asks === null) return null;
    if (bids.length + asks.length === 0) return null;
    return { us, bids, asks };
  };
}

/** The REST order book as the sync's boundary: its own microtimestamp plus both sides, or a throw. */
function snapshotShape(payload) {
  if (payload === null || typeof payload !== 'object') throw new TypeError('the Bitstamp orderbook snapshot is not an object');
  const boundary = usOf(payload.microtimestamp);
  if (boundary === null) throw new TypeError('the Bitstamp orderbook snapshot has no source microtimestamp');
  const bids = levelsOf(payload.bids);
  const asks = levelsOf(payload.asks);
  if (bids === null || asks === null) throw new TypeError('the Bitstamp orderbook snapshot has invalid levels');
  const liveBids = bids.filter((level) => level.size > 0);
  const liveAsks = asks.filter((level) => level.size > 0);
  if (!liveBids.length || !liveAsks.length) throw new TypeError('the Bitstamp orderbook snapshot is incomplete');
  return { boundary, bids: liveBids, asks: liveAsks };
}

/**
 * The venue's boundary machine, kept whole: the REST snapshot's microtimestamp is the boundary.
 * Buffered diffs at or before it are inside the snapshot and discarded; diffs after it replay in
 * order; a buffered diff that regresses over an earlier one refetches (bounded attempts). The
 * steady state fails closed on a diff behind the cursor that is above the boundary - a frame the
 * stream has moved past - while a diff at or inside the boundary is stale news, quietly
 * discarded, and an equal stamp is not stale at all. A new connection resets everything; a sync
 * that gives up leaves the stream unsynchronized rather than half-synchronized.
 */
export function createBitstampSynchronizer({ fetchSnapshot, symbol = 'BTC/USD', onSnapshot = null } = {}) {
  const pair = String(symbol).replace('/', '').toLowerCase();
  const diffOf = makeDiffOf(`diff_order_book_${pair}`);

  let boundaryUs = null;
  let lastUs = null;
  let synced = false;
  let needsResync = true;
  let snapshot = null;
  let snapshotChangesPending = false;

  function reset() {
    boundaryUs = null;
    lastUs = null;
    synced = false;
    needsResync = true;
    snapshot = null;
    snapshotChangesPending = false;
  }

  function applySnapshot(payload) {
    const normalized = snapshotShape(payload);
    boundaryUs = normalized.boundary;
    lastUs = normalized.boundary;
    snapshot = normalized;
    snapshotChangesPending = true;
    synced = true;
    needsResync = false;
    // Set 7b: the REST snapshot is not a socket frame, so the adapter hands it to whoever is
    // recording the raw through this sink, at the exact point it is applied.
    if (typeof onSnapshot === 'function') onSnapshot({ payload, boundary: boundaryUs });
    return { status: 'snapshot', boundary: boundaryUs };
  }

  function accept(value) {
    const event = diffOf(value);
    if (event === null) return { status: 'malformed', reason: 'malformed Bitstamp diff event' };
    if (needsResync || !synced) return { status: 'resync', reason: 'depth stream is not synchronized' };
    if (event.us <= boundaryUs) {
      // The venue's own algorithm: a diff at or before the snapshot's stamp is inside it.
      return { status: 'discarded', reason: 'diff is at or inside the snapshot boundary' };
    }
    if (event.us < lastUs) {
      needsResync = true;
      synced = false;
      return { status: 'resync', reason: `diff regressed in source time (${event.us} < ${lastUs})` };
    }
    lastUs = event.us;
    return { status: 'applied', boundary: boundaryUs };
  }

  async function sync(bufferedEvents = [], { isCurrent = () => true } = {}) {
    if (typeof fetchSnapshot !== 'function') throw new TypeError('the Bitstamp depth sync needs fetchSnapshot');
    let lastError = null;
    for (let attempt = 0; attempt < MAX_SYNC_ATTEMPTS; attempt += 1) {
      try {
        const next = await fetchSnapshot();
        if (!isCurrent()) return { status: 'stale' };
        applySnapshot(next);
        const buffered = (typeof bufferedEvents === 'function' ? bufferedEvents() : bufferedEvents)
          .map(diffOf)
          .filter(Boolean);
        if (buffered.length > MAX_PREPARATION_DIFFS) {
          lastError = new Error(`the Bitstamp preparation buffer overflowed (${buffered.length} diffs)`);
          break;
        }
        let ok = true;
        let cursor = boundaryUs;
        for (const event of buffered) {
          if (event.us <= boundaryUs) continue; // at or inside the snapshot, per the venue's algorithm
          if (event.us < cursor) {
            ok = false; // a regression the algorithm does not describe: the next snapshot provably covers both
            break;
          }
          cursor = event.us;
        }
        if (ok) return { status: 'synced', boundary: boundaryUs };
        lastError = new Error('a buffered Bitstamp diff regresses over an earlier buffered one');
      } catch (error) {
        lastError = error;
      }
    }
    // Giving up leaves the stream unsynchronized: a direct accept must refuse, not ride on the
    // half-applied snapshot that the last attempt happened to leave behind.
    synced = false;
    needsResync = true;
    throw lastError ?? new Error('the Bitstamp snapshot/diff boundary could not be proven');
  }

  function replacementFor(event = null) {
    const levels = new Map();
    for (const [side, source] of [['bid', snapshot.bids], ['ask', snapshot.asks]]) {
      for (const level of source) levels.set(`${side}:${level.price}`, { side, price: level.price, size: level.size });
    }
    if (event !== null) {
      for (const [side, source] of [['bid', event.bids], ['ask', event.asks]]) {
        for (const level of source) {
          const key = `${side}:${level.price}`;
          if (level.size === 0) levels.delete(key);
          else levels.set(key, { side, price: level.price, size: level.size });
        }
      }
    }
    return { replace: true, levels: [...levels.values()] };
  }

  function takeSnapshotChanges(event = null) {
    if (!snapshotChangesPending || snapshot === null) return null;
    snapshotChangesPending = false;
    return replacementFor(event);
  }

  return {
    reset,
    applySnapshot,
    accept,
    sync,
    diffOf,
    get boundaryUs() {
      return boundaryUs;
    },
    get needsResync() {
      return needsResync;
    },
    get synced() {
      return synced;
    },
    takeSnapshotChanges,
  };
}

function isTradeFrame(payload) {
  if (payload === null || typeof payload !== 'object') return false;
  const price = finiteNumber(payload.price);
  const amount = finiteNumber(payload.amount);
  const us = usOf(payload.microtimestamp);
  return (
    price !== null &&
    price > 0 &&
    amount !== null &&
    amount > 0 &&
    us !== null &&
    (payload.type === 0 || payload.type === 1)
  );
}

function makeBitstampAdapter({ market = 'bitstamp_spot', symbol = 'BTC/USD', url = null, restUrl = null, fetchImpl = globalThis.fetch, restTimeoutMs = DEFAULT_REST_TIMEOUT_MS } = {}) {
  const known = BITSTAMP_SYMBOLS[market];
  if (!Array.isArray(known) || !known.includes(symbol)) {
    throw new TypeError(`unsupported Bitstamp instrument for ${market}: ${symbol}`);
  }
  const pair = symbol.replace('/', '').toLowerCase();
  const tradeChannel = `live_trades_${pair}`;
  const diffChannel = `diff_order_book_${pair}`;
  // The official reconciliation pairs the diff feed with the default grouping (group=1), pinned
  // explicitly so a future default change cannot silently re-pair them.
  const rest = restUrl ?? `${BITSTAMP_REST_BASE}/${pair}/?group=1`;
  const diffOf = makeDiffOf(diffChannel);
  const subscriptionMessages = [
    JSON.stringify({ event: 'bts:subscribe', data: { channel: tradeChannel } }),
    JSON.stringify({ event: 'bts:subscribe', data: { channel: diffChannel } }),
  ];

  // Set 7b: the raw sink for the REST snapshot. Null until the ingest process installs one; a
  // snapshot applied with no sink set is simply not recorded, never a crash.
  let rawSnapshotSink = null;
  const sync = createBitstampSynchronizer({
    symbol,
    onSnapshot: (snap) => {
      if (typeof rawSnapshotSink !== 'function') return;
      // v1's REST-sync snapshot (`lib/bitstamp-connector.mjs:380-386`): the snapshot's own source
      // microstamp is the event time (source time known), and the origin fields say the boundary came
      // from the REST reconciliation. The levels keep the REST payload's own strings.
      const pairs = (levels) => (Array.isArray(levels)
        ? levels
          .filter((entry) => Array.isArray(entry) && finiteNumber(entry[0]) !== null && finiteNumber(entry[1]) !== null && Number(entry[1]) > 0)
          .map((entry) => [String(entry[0]), String(entry[1])])
        : []);
      const boundary = Math.floor(snap.boundary / 1000);
      rawSnapshotSink({
        event_ts_ms: boundary,
        source_event_ts_ms: boundary,
        source_event_time_known: true,
        payload: {
          market,
          snapshot_origin: 'rest_sync',
          snapshot_asof_ts_ms: boundary,
          event_time_source: 'rest_snapshot_source',
          type: 'snapshot',
          bids: pairs(snap.payload?.bids),
          asks: pairs(snap.payload?.asks),
          ts: boundary,
          seq: null,
        },
      });
    },
    fetchSnapshot: async () => {
      if (typeof fetchImpl !== 'function') throw new TypeError('the Bitstamp depth sync needs fetch');
      const signal =
        typeof AbortSignal === 'function' && typeof AbortSignal.timeout === 'function'
          ? AbortSignal.timeout(restTimeoutMs)
          : undefined;
      const response = await fetchImpl(rest, signal === undefined ? undefined : { signal });
      if (response?.ok === false) throw new Error(`the Bitstamp orderbook snapshot failed (${response?.status ?? 'unknown'})`);
      return response.json();
    },
  });

  let activePreparation = 0;
  let preparationBuffer = [];

  return {
    market,
    symbol,
    url: url ?? BITSTAMP_WS,
    restUrl: rest,
    stream: 'trades',

    // The proof this venue gives is its matching-engine microsecond clock moving forward.
    boundary: 'sequence',

    ackMode: 'explicit',
    expectedSubscriptions: () => [tradeChannel, diffChannel],

    // The subscriptions are sent inside connection preparation, not here: see onConnectionOpen.
    subscribeMessages: () => [],

    // The venue closes quiet connections and sends no heartbeats of its own; the client heartbeat
    // is the keep-alive, in the no-activity form, and its answer is heard like any other frame.
    keepAlive: () => ({ noActivityMs: KEEPALIVE_SILENCE_MS, payload: () => JSON.stringify({ event: 'bts:heartbeat' }) }),

    /**
     * Connection preparation, in the order this venue needs: subscribe first (on the new socket),
     * then fetch the REST snapshot, then partition what arrived in between. While the fetch is in
     * flight the connection buffers every frame and hands it to `bufferDuringPreparation`; those
     * frames are re-delivered after preparation, where the ordinary `acceptDepthEvent` path
     * partitions them against the boundary. The acks are among them, so the link establishes the
     * same way as every other venue's. The fetch carries v1's deadline (`restTimeoutMs`) and the
     * buffer a hard cap, so a preparation that cannot complete fails bounded and the connection
     * replaces the socket. Establishment still rides on the redelivered acks: no ack-deadline
     * timer is armed in this configuration (the connection arms one only for listed subscription
     * messages), so a venue that never acknowledges is held by the run's startup window instead.
     */
    async onConnectionOpen({ socket = null } = {}) {
      const preparation = ++activePreparation;
      sync.reset();
      preparationBuffer = [];
      for (const message of subscriptionMessages) socket?.send?.(message);
      return sync.sync(() => preparationBuffer, { isCurrent: () => preparation === activePreparation });
    },

    bufferDuringPreparation(value) {
      preparationBuffer.push(value);
    },

    /** Drive the sync without a socket (tests and dry runs). */
    syncDepth() {
      return sync.sync([]);
    },

    syncSnapshot(payload) {
      return sync.applySnapshot(payload);
    },

    resetConnection() {
      sync.reset();
    },

    get needsResync() {
      return sync.needsResync;
    },

    get boundaryUs() {
      return sync.boundaryUs;
    },

    /**
     * Classify one frame. A frame that cannot be classified is thrown, which reception counts; a
     * shapeless frame of ours is a protocol error the link replaces itself for.
     */
    parse(raw) {
      const data = asObject(raw);
      if (data === null) throw new TypeError('unrecognised Bitstamp frame: not a JSON object');

      if (data.event === 'bts:error') {
        const detail = typeof data.data?.message === 'string' ? data.data.message : '';
        const code = typeof data.data?.code === 'number' ? ` (code ${data.data.code})` : '';
        return { kind: 'protocol-error', reason: `Bitstamp error frame: ${detail}${code}` };
      }

      if (data.event === 'bts:subscription_succeeded') {
        if (typeof data.channel !== 'string' || data.channel.length === 0) {
          throw new TypeError('unrecognised Bitstamp subscription frame');
        }
        return { kind: 'subscription', key: data.channel, ok: true, detail: '' };
      }

      if (data.event === 'bts:heartbeat') {
        // The answer to our heartbeat: liveness, nothing more.
        return { kind: 'heartbeat', answered: true };
      }

      if (data.event === 'bts:request_reconnect') {
        return { kind: 'shutdown', detail: 'the venue asked for a reconnect' };
      }

      if (data.event === 'data' && data.channel === diffChannel) {
        if (diffOf(data) !== null) return { kind: 'data', depth: true };
        return { kind: 'protocol-error', reason: 'malformed Bitstamp diff frame' };
      }

      if (data.event === 'trade' && data.channel === tradeChannel) {
        if (!isTradeFrame(data.data)) throw new TypeError('malformed Bitstamp trade frame');
        return { kind: 'data', trade: true };
      }

      throw new TypeError(`unrecognised Bitstamp frame (event ${JSON.stringify(data.event ?? null)})`);
    },

    /**
     * The board changes a frame carries: the first accepted frame after a sync is
     * `{replace:true, levels}` - the REST snapshot with that frame's own changes folded in, so the
     * board and the diff stream agree from one point - and every later frame is `{replace:false,
     * changes}` where an absolute amount replaces the level and 0 removes it. Trades carry no
     * level changes.
     */
    changesFor(envelope) {
      const event = diffOf(envelope?.raw);
      if (event === null) return { replace: false, changes: [] };
      const replacement = sync.takeSnapshotChanges(event);
      if (replacement !== null) return replacement;
      return {
        replace: false,
        changes: [
          ...event.bids.map((level) => ({ side: 'bid', price: level.price, size: level.size })),
          ...event.asks.map((level) => ({ side: 'ask', price: level.price, size: level.size })),
        ],
      };
    },

    /** The diff continuity rule, applied by reception before a frame is stamped. */
    acceptDepthEvent(value) {
      return sync.accept(value);
    },

    /**
     * The book-side half of the same rule, for the single-process structure that resolves the
     * boundary proof from the adapter. The synchronizer is the shared state, so the book and
     * reception cannot disagree: a frame the rule refuses is refused here too, and the refusal
     * persists until a new connection. Frames with no diff in them connect to nothing and are let
     * through. A frame the snapshot already covers (at or inside the boundary) answers true:
     * reception discards it before it is stamped, so the book never sees one, and it is stale news
     * rather than a broken stream.
     */
    connects({ current = null } = {}) {
      const raw = current?.raw ?? current;
      if (diffOf(raw) === null) return true;
      const result = sync.accept(raw);
      return result.status !== 'resync' && result.status !== 'malformed';
    },

    /** Set 7b: install the raw sink for REST snapshots applied by the depth synchronizer. */
    setRawSnapshotSink(fn) {
      rawSnapshotSink = typeof fn === 'function' ? fn : null;
    },

    /**
     * Set 7b: classify one received frame as a raw record, an array of records, or null. The payloads
     * are the running v1 store's, measured 2026-10-10 (`bitstamp_spot`): a diff frame is
     * `{type:'update', bids, asks, ts, seq:null}` with the venue's own level strings and a millisecond
     * `ts` (the microsecond matching-engine clock), a trade is v1's
     * `{market, price, qty, side, ts, tradeId}` (`type` 0 = buy, 1 = sell), and the REST snapshot is
     * delivered through `setRawSnapshotSink`, not here.
     */
    rawEventFor(frame) {
      const data = asObject(frame?.raw);
      if (data === null) return null;
      if (data.event === 'data' && data.channel === diffChannel) {
        const payload = data.data;
        if (payload === null || typeof payload !== 'object') return null;
        const us = usOf(payload.microtimestamp);
        if (us === null) return null;
        const pairs = (levels) => (Array.isArray(levels)
          ? levels.filter((entry) => Array.isArray(entry) && finiteNumber(entry[0]) !== null && finiteNumber(entry[1]) !== null).map((entry) => [String(entry[0]), String(entry[1])])
          : []);
        const ts = Math.floor(us / 1000);
        return rawBook({
          market, payload: { type: 'update', bids: pairs(payload.bids), asks: pairs(payload.asks), ts, seq: null },
          event_ts_ms: ts, source_event_ts_ms: ts, source_event_time_known: true,
        });
      }
      if (data.event === 'trade' && data.channel === tradeChannel) {
        const payload = data.data;
        if (!isTradeFrame(payload)) return null;
        const us = usOf(payload.microtimestamp);
        if (us === null) return null;
        const ts = Math.floor(us / 1000);
        return rawTrade({
          market, price: Number(payload.price), qty: Number(payload.amount),
          side: Number(payload.type) === 0 ? 'buy' : 'sell', ts,
          tradeId: payload.id ?? payload.trade_id ?? payload.microtimestamp,
        });
      }
      return null;
    },

    /** The diff's microsecond stamp, for the envelope's `meta.venue_seq`. Only diff frames have one. */
    venueSeqOf(raw) {
      const event = diffOf(raw);
      return event === null ? null : event.us;
    },
  };
}

/** The Spot market (BTC/USD): trades and the diff book on the public socket, REST for the sync. */
export function createBitstampAdapter({ market = 'bitstamp_spot', symbol = 'BTC/USD', url = null, restUrl = null, fetchImpl = globalThis.fetch, restTimeoutMs = DEFAULT_REST_TIMEOUT_MS } = {}) {
  return makeBitstampAdapter({ market, symbol, url, restUrl, fetchImpl, restTimeoutMs });
}
