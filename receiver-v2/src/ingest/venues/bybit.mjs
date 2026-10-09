/**
 * Bybit v5 public WS: the USDT perpetual (linear) and the Spot market.
 *
 * The wire contract here is the official one, checked against the live public stream:
 *
 *  - One connection per category. Linear is `wss://stream.bybit.com/v5/public/linear`, Spot is
 *    `wss://stream.bybit.com/v5/public/spot`. A topic is `orderbook.{depth}.{symbol}`,
 *    `publicTrade.{symbol}` or `allLiquidation.{symbol}` (linear only).
 *  - Each topic is asked for in its own `subscribe` request, under its own `req_id` - the ack
 *    echoes the `req_id` (verified live on both categories; the wording of `ret_msg` differs
 *    between them, so the wording is not the identity). Per-topic requests are also what makes a
 *    partial refusal visible: one failed key is a failed link, not a link that answered something.
 *  - The book topic pushes a `snapshot` after the subscription and `delta`s afterwards. A new
 *    snapshot replaces the local book. `u` is the update id: it must move forward (backwards and
 *    duplicates fail closed), but it is not a +1 counter - forward jumps happen and are accepted.
 *    `u=1` marks the documented service-restart snapshot: a snapshot frame overwrites the local
 *    book, while a delta carrying `u=1` is not trusted to be a complete book and fails closed into
 *    a resync (the venue's recovery path answers a resync with a fresh snapshot). `seq` is a cross
 *    sequence for comparing levels' order and is deliberately not used for continuity. A delta
 *    before any snapshot cannot be applied to a book that does not exist yet, so it fails closed
 *    and the connection resubscribes for a fresh snapshot.
 *  - `publicTrade` frames are trades; `allLiquidation` frames are liquidations. Both are
 *    classified, and neither carries level changes - the board only ever moves on book frames. A
 *    trade is then dropped by reception (as it is for every venue here: trades are not recorded
 *    frames), while a liquidation is stamped and travels as a frame with an empty diff, so it is
 *    recorded. A frame this adapter cannot classify at all is thrown, which reception counts as an
 *    unparsable frame rather than dropping it in silence.
 *  - The keep-alive is the documented JSON ping, sent when the socket has gone quiet (10 seconds,
 *    inside the connection's 15 second silence deadline) rather than on a fixed interval: a socket
 *    that is still delivering needs no ping, and a quiet one is pinged with its pong landing before
 *    the deadline judges the link dead. The venue answers `pong`.
 *
 * The REST orderbook endpoint is declared as the venue's public fallback location. The adapter
 * itself does not fetch it: the WS snapshot is the subscription's own synchronization path, and a
 * missing one fails closed into a resubscribe rather than a second source of the same book.
 */

import { rawTrade, rawBook, rawLiquidation, collapse } from './raw-shape.mjs';

const BYBIT_SYMBOLS = new Set(['BTCUSDT']);

const LINEAR_WS = 'wss://stream.bybit.com/v5/public/linear';
const SPOT_WS = 'wss://stream.bybit.com/v5/public/spot';
const REST_BASE = 'https://api.bybit.com/v5/market/orderbook';

function textOf(raw) {
  return typeof raw === 'string' ? raw : raw?.toString?.('utf8') ?? '';
}

/** Parse a frame that may be raw bytes, a string, or an already-parsed object. Null when it is none of those. */
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

/** A level is the venue's own pair: [price, size], both numeric strings, size 0 meaning gone. */
function validLevel(level) {
  if (!Array.isArray(level) || level.length !== 2) return false;
  const price = finiteNumber(level[0]);
  const size = finiteNumber(level[1]);
  return price !== null && price > 0 && size !== null && size >= 0;
}

function makeBybitAdapter({ market, category, symbol = 'BTCUSDT', bookDepth, url = null, restUrl = null } = {}) {
  if (typeof symbol !== 'string' || !BYBIT_SYMBOLS.has(symbol)) {
    throw new TypeError(`unsupported Bybit ${category} symbol: ${symbol}`);
  }
  const tradeTopic = `publicTrade.${symbol}`;
  const bookTopic = `orderbook.${bookDepth}.${symbol}`;
  const liquidationTopic = category === 'linear' ? `allLiquidation.${symbol}` : null;
  const topics = liquidationTopic === null ? [tradeTopic, bookTopic] : [tradeTopic, bookTopic, liquidationTopic];

  // Reception-side continuity (`acceptDepthEvent`) and book-side continuity (`connects`) are kept
  // apart on purpose: in the single-process structure both run over the same frames, and one shared
  // cursor would judge the second reader against the frame it just read.
  let lastAcceptedU = null;
  let lastProvenU = null;
  let failed = false;

  function resetState() {
    lastAcceptedU = null;
    lastProvenU = null;
    failed = false;
  }

  /**
   * A well-formed book frame, or null. Identity first - the exact topic we subscribed, our symbol -
   * then the shape the rules depend on: `u`/`seq` integers, both sides arrays of valid levels.
   */
  function bookFrameOf(value) {
    const data = asObject(value);
    if (data === null || data.topic !== bookTopic) return null;
    if (data.type !== 'snapshot' && data.type !== 'delta') return null;
    const body = data.data;
    if (body === null || typeof body !== 'object' || body.s !== symbol) return null;
    if (!Number.isInteger(body.u) || body.u < 1) return null;
    if (!Number.isInteger(body.seq) || body.seq < 0) return null;
    if (!Array.isArray(body.b) || !Array.isArray(body.a)) return null;
    if (!body.b.every(validLevel) || !body.a.every(validLevel)) return null;
    return data;
  }

  /** The trade frame's identity and numbers. L/BT/RPI and seq differ between categories and are not required. */
  function isTradeFrame(data) {
    if (data.type !== 'snapshot' || !Array.isArray(data.data) || data.data.length === 0) return false;
    return data.data.every(
      (row) =>
        row !== null &&
        typeof row === 'object' &&
        row.s === symbol &&
        (row.S === 'Buy' || row.S === 'Sell') &&
        finiteNumber(row.p) !== null &&
        Number(row.p) > 0 &&
        finiteNumber(row.v) !== null &&
        Number(row.v) > 0 &&
        Number.isInteger(row.T) &&
        row.T >= 0 &&
        (typeof row.i === 'string' || typeof row.i === 'number'),
    );
  }

  /** The liquidation frame's documented fields: T/s/S/v/p. Buy means a long position was liquidated. */
  function isLiquidationFrame(data) {
    if (data.type !== 'snapshot') return false;
    const rows = Array.isArray(data.data) ? data.data : [data.data];
    if (rows.length === 0) return false;
    return rows.every(
      (row) =>
        row !== null &&
        typeof row === 'object' &&
        !Array.isArray(row) &&
        row.s === symbol &&
        (row.S === 'Buy' || row.S === 'Sell') &&
        finiteNumber(row.p) !== null &&
        Number(row.p) > 0 &&
        finiteNumber(row.v) !== null &&
        Number(row.v) > 0 &&
        Number.isInteger(row.T) &&
        row.T >= 0,
    );
  }

  /** The level changes a book frame carries, in the v1 contract's shape. */
  function changesOfFrame(frame) {
    const changes = [];
    for (const [side, list] of [
      ['bid', frame.data.b],
      ['ask', frame.data.a],
    ]) {
      for (const level of list) changes.push({ side, price: Number(level[0]), size: Number(level[1]) });
    }
    return changes;
  }

  return {
    market,
    symbol,
    category,
    url: url ?? (category === 'linear' ? LINEAR_WS : SPOT_WS),
    restUrl: restUrl ?? `${REST_BASE}?category=${category}&symbol=${symbol}&limit=${bookDepth}`,
    stream: 'trades',

    // The proof this venue gives is its update id: u must increase over the range the rule judged.
    // No checksum and no strict +1 are claimed, because the venue gives neither.
    boundary: 'sequence',

    // The venue answers every subscribe request with an ack that echoes its req_id; each topic is
    // asked for under its own req_id, so an acknowledgement names the topic it answers.
    ackMode: 'explicit',
    expectedSubscriptions: () => [...topics],

    subscribeMessages: () => topics.map((topic) => JSON.stringify({ op: 'subscribe', req_id: topic, args: [topic] })),

    // The documented JSON ping, sent when the socket has gone quiet. The connection judges the link
    // by silence (15 seconds by default), so the ping goes out at 10 seconds - inside that window -
    // and the pong resets it. A socket that is still talking is not pinged at all; pinging only when
    // quiet stays within the venue's own "every 20 seconds" recommendation.
    keepAlive: () => ({ noActivityMs: 10_000, payload: () => JSON.stringify({ op: 'ping' }) }),

    // A new socket is a new stream: the update-id cursors describe a range that has ended.
    onConnectionOpen() {
      resetState();
    },

    /**
     * Classify one frame. Data kinds are the ones reception understands; a frame that cannot be
     * classified is thrown, which reception counts as an unparsable frame - never guessed at and
     * never dropped in silence.
     */
    parse(raw) {
      const data = asObject(raw);
      if (data === null) throw new TypeError('unrecognised Bybit frame: not a JSON object');

      if (data.op === 'subscribe') {
        const key = typeof data.req_id === 'string' && data.req_id.length > 0 ? data.req_id : 'subscribe';
        const detail = String(data.ret_msg ?? '');
        // "already subscribed" is the state we asked for, not a refusal. Everything else that is
        // not success:true is a failure - including a bare ack with no success field.
        const ok = data.success === true || /already subscribed/i.test(detail);
        return { kind: 'subscription', key, ok, detail };
      }
      if (data.op === 'ping' || data.op === 'pong') {
        return { kind: 'heartbeat', answered: data.op === 'pong' || data.ret_msg === 'pong' };
      }

      if (typeof data.topic !== 'string') {
        throw new TypeError('unrecognised Bybit frame: neither an op nor a topic');
      }
      if (data.topic === tradeTopic) {
        if (!isTradeFrame(data)) throw new TypeError('malformed Bybit trade frame');
        return { kind: 'data', trade: true };
      }
      if (liquidationTopic !== null && data.topic === liquidationTopic) {
        if (!isLiquidationFrame(data)) throw new TypeError('malformed Bybit liquidation frame');
        return { kind: 'data', liquidation: true };
      }
      if (data.topic === bookTopic) {
        // A frame on our own book topic that does not match the contract is not "some other
        // message": it is the venue's book stream changing shape, and believing any of it would be
        // guessing. It is reported as a protocol error so the connection resynchronizes.
        return bookFrameOf(data) !== null
          ? { kind: 'data', book: true }
          : { kind: 'protocol-error', reason: 'malformed Bybit orderbook frame' };
      }
      // A topic this connection never asked for is not data and is not silently dropped: it is
      // thrown, which reception counts - an anomalous stream stays visible instead of keeping the
      // link looking alive with nothing to show.
      throw new TypeError(`unrecognised Bybit topic ${JSON.stringify(data.topic)}`);
    },

    /**
     * The board changes a frame carries: a snapshot is `{replace:true, levels}` - it is the whole
     * book, u=1 or not - and a delta is `{replace:false, changes}` with size 0 removing a level.
     * Trades and liquidations carry no level changes; they are part of the stream but never move
     * the book.
     */
    changesFor(envelope) {
      const frame = bookFrameOf(envelope?.raw);
      if (frame === null) return { replace: false, changes: [] };
      const changes = changesOfFrame(frame);
      // Only a snapshot is a complete book. A delta claiming u=1 is refused at reception and never
      // reaches here as a replacement; as a diff it is refused by the connection rule.
      return frame.type === 'snapshot' ? { replace: true, levels: changes } : { replace: false, changes };
    },

    /**
     * The update-id rule, applied by reception before a frame is stamped: `u` must move forward.
     * Backwards and duplicates fail closed - and stay failed until a new connection, so nothing is
     * applied on the strength of a stream whose order was already broken. A forward jump is
     * accepted: `u` is an update id, not a counter. A delta before any snapshot fails closed; a
     * fresh subscription answers with a snapshot, so this is an anomaly, not a sync path. A
     * snapshot frame is the whole book and re-anchors the stream, u=1 or not; a delta carrying the
     * restart id (u=1) is not trusted to be complete and fails closed into a resync. Frames that
     * are not book frames (trades, liquidations) are accepted untouched - they have no update id
     * to judge.
     */
    acceptDepthEvent(raw) {
      const data = asObject(raw);
      if (data === null || typeof data.topic !== 'string') {
        return { status: 'malformed', reason: 'not a Bybit frame' };
      }
      if (data.topic === tradeTopic || (liquidationTopic !== null && data.topic === liquidationTopic)) {
        return { status: 'applied' };
      }
      if (data.topic !== bookTopic) {
        return { status: 'malformed', reason: `unexpected Bybit topic ${JSON.stringify(data.topic)}` };
      }
      const frame = bookFrameOf(data);
      if (frame === null) return { status: 'malformed', reason: 'malformed Bybit orderbook frame' };
      if (failed) return { status: 'resync', reason: 'the Bybit stream is failed closed' };

      const u = frame.data.u;
      if (frame.type === 'snapshot') {
        // A snapshot is the whole book, u=1 or not: it replaces and resets the stream.
        lastAcceptedU = u;
        return { status: 'applied' };
      }
      if (u === 1) {
        // The restart id on a delta: the venue says the service restarted, but nothing here proves
        // the frame carries a complete book. Believing it could replace the board with a fragment,
        // so it fails closed - the resync answers with a fresh snapshot.
        failed = true;
        return {
          status: 'resync',
          reason: 'the Bybit update id says the service restarted, but the frame is not a snapshot',
        };
      }
      if (lastAcceptedU === null) {
        failed = true;
        return { status: 'resync', reason: 'a Bybit delta arrived before any snapshot' };
      }
      if (u <= lastAcceptedU) {
        failed = true;
        return { status: 'resync', reason: 'the Bybit update id went backwards or repeated' };
      }
      lastAcceptedU = u;
      return { status: 'applied' };
    },

    /**
     * The book-side half of the same rule, for the single-process structure that resolves the
     * boundary proof from the adapter. A replacement (a snapshot, u=1 or not) re-anchors;
     * otherwise the frame's `u` must exceed the update id of the last book frame the rule proved.
     * The last frame the book proved carries its update id on the envelope, and it is adopted
     * before anything is judged: a non-book frame in between (a liquidation) must not erase what
     * the rule had already proven, or a backwards update after it would be accepted. Frames with
     * no update id connect to nothing and are let through; a delta carrying the restart id is not
     * a continuation and is refused.
     */
    connects({ previous = null, current = null } = {}) {
      const previousU = Number.isInteger(previous?.meta?.venue_seq) ? previous.meta.venue_seq : null;
      if (previousU !== null && (lastProvenU === null || previousU > lastProvenU)) lastProvenU = previousU;
      const frame = bookFrameOf(current?.raw ?? current);
      if (frame === null) return true;
      const u = frame.data.u;
      if (frame.type === 'snapshot') {
        lastProvenU = u;
        return true;
      }
      if (u === 1) return false;
      if (lastProvenU !== null && u <= lastProvenU) return false;
      lastProvenU = u;
      return true;
    },

    /**
     * Set 7b: classify one received frame as a raw record, or an array of records (a frame carrying
     * several trades, or a snapshot written to both `book_updates` and `snapshots`), or null. The
     * payloads are the running v1 store's, measured 2026-10-10 (`bybit_spot`/`bybit_perp`):
     * a delta is `{prev_seq: <the previous update id>, type:'update', bids, asks, ts, seq}` and a
     * snapshot is `{snapshot_origin:'ws_sync', type:'snapshot', bids, asks, ts, seq}` - the two are
     * not the same shape, and a delta is never written as a `snapshots` row. A trade is v1's
     * `{market, price, qty, side, ts, tradeId}` (`t.p`/`t.v`, `S === 'Buy'`, `T`, `i`).
     */
    rawEventFor(frame) {
      const data = asObject(frame?.raw);
      if (data === null || typeof data.topic !== 'string') return null;
      if (data.topic === tradeTopic) {
        if (!isTradeFrame(data)) return null;
        const out = [];
        for (const row of data.data) {
          const ts = Number.isInteger(row.T) && row.T > 0 ? row.T : null;
          if (ts === null) continue;
          out.push(rawTrade({ market, price: Number(row.p), qty: Number(row.v), side: row.S === 'Buy' ? 'buy' : 'sell', ts, tradeId: row.i }));
        }
        return collapse(out);
      }
      if (liquidationTopic !== null && data.topic === liquidationTopic) {
        if (!isLiquidationFrame(data)) return null;
        const rows = Array.isArray(data.data) ? data.data : [data.data];
        const out = [];
        for (const row of rows) {
          const price = Number(row.p);
          const qty = Number(row.v);
          if (!(price > 0) || !(qty > 0)) continue;
          out.push(rawLiquidation({
            market, exchange: 'bybit', symbol: row.s ?? symbol, side: row.S === 'Sell' ? 'sell' : 'buy',
            price, qty, notional: price * qty, raw_type: 'liquidation', trade_id: null,
            source_ts: Number.isInteger(row.T) ? row.T : null, ts: frame.atMs,
          }));
        }
        return collapse(out);
      }
      if (data.topic === bookTopic) {
        const book = bookFrameOf(data);
        if (book === null) return null;
        const bids = book.data.b.map((level) => [String(level[0]), String(level[1])]);
        const asks = book.data.a.map((level) => [String(level[0]), String(level[1])]);
        // Bybit puts the message time at the top level (`{topic, type, ts, data}`), and its orderbook
        // `data` carries no `ts` of its own; v1 falls back the same way (`data.data?.ts ?? data.ts`,
        // `lib/bybit-connector.mjs:61`). Reading only `data.ts` returned null and dropped every normal
        // frame - the whole board missing from the raw.
        const rawTs = book.data.ts ?? book.ts;
        const ts = Number.isInteger(rawTs) && rawTs > 0 ? rawTs : null;
        if (ts === null) return null;
        const updateId = book.data.u ?? book.u;
        if (book.type === 'snapshot') {
          const payload = { snapshot_origin: 'ws_sync', type: 'snapshot', bids, asks, ts, seq: updateId };
          const write = (stream) => rawBook({ market, stream, payload, event_ts_ms: ts, source_event_ts_ms: ts, source_event_time_known: true });
          return [write('book_updates'), write('snapshots')];
        }
        const payload = { prev_seq: lastAcceptedU, type: 'update', bids, asks, ts, seq: updateId };
        return rawBook({ market, payload, event_ts_ms: ts, source_event_ts_ms: ts, source_event_time_known: true });
      }
      return null;
    },

    /** The update id, for the envelope's `meta.venue_seq`. Only book frames have one. */
    venueSeqOf(raw) {
      const frame = bookFrameOf(raw);
      return frame === null ? null : frame.data.u;
    },
  };
}

/** The USDT perpetual (linear category): trade, book at 1000 levels, and liquidations. */
export function createBybitPerpAdapter({ market = 'bybit_perp', symbol = 'BTCUSDT', bookDepth = 1000, url = null, restUrl = null } = {}) {
  return makeBybitAdapter({ market, category: 'linear', symbol, bookDepth, url, restUrl });
}

/** The Spot market: trade and book at 200 levels. */
export function createBybitSpotAdapter({ market = 'bybit_spot', symbol = 'BTCUSDT', bookDepth = 200, url = null, restUrl = null } = {}) {
  return makeBybitAdapter({ market, category: 'spot', symbol, bookDepth, url, restUrl });
}
