/**
 * Hyperliquid public WS: the perpetual market this receiver admits - hyperliquid_perp (BTC).
 *
 * The wire contract here is the official one, checked against the live public stream on
 * `wss://api.hyperliquid.xyz/ws`:
 *
 *  - Two channels are asked for, each in its own frame: `l2Book` (the book) and `trades`. The
 *    acknowledgement is a `subscriptionResponse` frame echoing the subscription with its default
 *    fields filled in (`nSigFigs`, `mantissa`, `fast`); one ack per subscription. A duplicate
 *    subscription answers with an `error` channel frame, and an unknown coin is answered with a
 *    silent close (both measured live) - neither ever reaches the board as data.
 *  - `l2Book` pushes are full replacements, never diffs: each frame carries the whole book
 *    (`levels = [bids, asks]`, each level `{px, sz, n}` with string prices and sizes) and a `time`
 *    stamp - milliseconds on the live stream, nanoseconds in the v1-era fixtures (normalised by
 *    magnitude). The live cadence measured 2.7-5.6 s between pushes, 20 levels per side, strictly
 *    increasing times, no repeats and no byte-identical resends in the observed window.
 *  - Hyperliquid publishes no sequence number, no checksum and no replay cursor for market data.
 *    The ordering the stream does carry is the frame time itself, and that is the proof this
 *    adapter claims: the book time must not move backwards. A step backwards is a stale frame -
 *    the pushes are full replacements, so applying one would walk the whole book back - and it
 *    fails closed until a new connection. An equal time is not a stale frame: the stamp is
 *    millisecond-granular and a frame carrying the time already held is not behind it. The rule
 *    guarantees order, not content: a frame the stream says is current is applied as the venue
 *    sent it (the venue's own docs carry no content-health promise; their full-replacement design
 *    is the recovery path). It also gives no gap detection, because the venue publishes none -
 *    a missed push is corrected by the next replacement.
 *  - Keep-alive is the client ping (`{"method":"ping"}` -> `{channel:"pong"}`): the venue closes a
 *    connection that has carried nothing for 60 s, so the ping goes out after 30 s without a word
 *    from the venue. There is no server-side ping in the contract - the silence watch answers a
 *    different question and still runs.
 *  - Hyperliquid has no public liquidation stream; that is the venue's contract, not a gap here.
 *
 * The info endpoint is declared as the venue's public fallback location (v1 kept no REST snapshot
 * path either): the WS feed is the subscription's own synchronization path and the adapter never
 * fetches it.
 */

const HYPERLIQUID_COINS = Object.freeze({
  hyperliquid_perp: Object.freeze(['BTC']),
});

const HYPERLIQUID_WS = 'wss://api.hyperliquid.xyz/ws';
const HYPERLIQUID_INFO = 'https://api.hyperliquid.xyz/info';

/** The venue closes a connection silent for 60 s; 30 s keeps the margin on our side of it. */
const KEEPALIVE_SILENCE_MS = 30_000;

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

/** Hyperliquid stamps book time in nanoseconds in the v1-era docs and milliseconds live; normalise by magnitude. */
function normaliseTs(value) {
  const n = finiteNumber(value);
  if (n === null || n <= 0) return null;
  return n > 1e15 ? Math.floor(n / 1e6) : Math.floor(n);
}

function makeHyperliquidAdapter({ market = 'hyperliquid_perp', symbol = 'BTC', url = null, restUrl = null } = {}) {
  const known = HYPERLIQUID_COINS[market];
  if (!Array.isArray(known) || !known.includes(symbol)) {
    throw new TypeError(`unsupported Hyperliquid instrument for ${market}: ${symbol}`);
  }

  const subscriptions = [
    { key: `l2Book:${symbol}`, frame: { method: 'subscribe', subscription: { type: 'l2Book', coin: symbol } } },
    { key: `trades:${symbol}`, frame: { method: 'subscribe', subscription: { type: 'trades', coin: symbol } } },
  ];

  let lastAcceptedMs = null;
  let lastProvenMs = null;
  let failed = false;
  let provenFailed = false;

  function resetState() {
    lastAcceptedMs = null;
    lastProvenMs = null;
    failed = false;
    provenFailed = false;
  }

  /**
   * A well-formed l2Book frame for our coin, or null: both sides present, every level a positive
   * price and a non-negative size, and at least one level in total. A zero size is a level the
   * board drops by its own rule, not a malformed frame; the levels' order is not read, because the
   * book is a set, not a sort.
   */
  function bookFrameOf(value) {
    const data = asObject(value);
    if (data === null || data.channel !== 'l2Book') return null;
    const book = data.data;
    if (book === null || typeof book !== 'object') return null;
    if (book.coin !== symbol) return null;
    const ms = normaliseTs(book.time);
    if (ms === null) return null;
    const levels = book.levels;
    if (!Array.isArray(levels) || levels.length !== 2) return null;
    const sides = [];
    let total = 0;
    for (const side of levels) {
      if (!Array.isArray(side)) return null;
      const rows = [];
      for (const level of side) {
        if (level === null || typeof level !== 'object') return null;
        const price = finiteNumber(level.px);
        const size = finiteNumber(level.sz);
        if (price === null || price <= 0) return null;
        if (size === null || size < 0) return null;
        rows.push({ price, size });
      }
      total += rows.length;
      sides.push(rows);
    }
    if (total === 0) return null;
    return { ms, bids: sides[0], asks: sides[1] };
  }

  /** Whether every trade row in a trades frame is a well-formed trade of our coin. */
  function isTradeFrame(data) {
    if (!Array.isArray(data.data) || data.data.length === 0) return false;
    return data.data.every(
      (trade) =>
        trade !== null &&
        typeof trade === 'object' &&
        trade.coin === symbol &&
        (trade.side === 'B' || trade.side === 'A') &&
        finiteNumber(trade.px) !== null &&
        Number(trade.px) > 0 &&
        finiteNumber(trade.sz) !== null &&
        Number(trade.sz) > 0 &&
        finiteNumber(trade.time) !== null,
    );
  }

  return {
    market,
    symbol,
    url: url ?? HYPERLIQUID_WS,
    restUrl: restUrl ?? HYPERLIQUID_INFO,
    stream: 'trades',

    // The proof this venue gives is its frame time: strictly forward (see acceptDepthEvent).
    boundary: 'sequence',

    ackMode: 'explicit',
    expectedSubscriptions: () => subscriptions.map((entry) => entry.key),

    subscribeMessages: () => subscriptions.map((entry) => JSON.stringify(entry.frame)),

    // The venue closes a connection silent for 60 s; nothing is sent on open - the ping waits for
    // the silence it exists to answer (C4's no-activity form).
    keepAlive: () => ({ noActivityMs: KEEPALIVE_SILENCE_MS, payload: () => JSON.stringify({ method: 'ping' }) }),

    // A new socket is a new stream: the time cursors describe a range that has ended.
    onConnectionOpen() {
      resetState();
    },

    /**
     * Classify one frame. Data kinds are the ones reception understands; a frame that cannot be
     * classified is thrown, which reception counts. A foreign coin is an anomaly with a name and is
     * thrown as such; a shapeless frame of ours is a protocol error the link replaces itself for.
     */
    parse(raw) {
      const data = asObject(raw);
      if (data === null) throw new TypeError('unrecognised Hyperliquid frame: not a JSON object');

      if (data.channel === 'error') {
        const detail = typeof data.data === 'string' ? data.data : JSON.stringify(data.data ?? null);
        return { kind: 'protocol-error', reason: `Hyperliquid error frame: ${detail}` };
      }

      if (data.channel === 'subscriptionResponse') {
        const subscription = data.data?.subscription;
        if (
          data.data?.method !== 'subscribe' ||
          subscription === null ||
          typeof subscription !== 'object' ||
          typeof subscription.type !== 'string' ||
          subscription.type.length === 0 ||
          (subscription.coin !== undefined && (typeof subscription.coin !== 'string' || subscription.coin.length === 0))
        ) {
          throw new TypeError('unrecognised Hyperliquid subscription frame');
        }
        const key =
          typeof subscription.coin === 'string' && subscription.coin.length > 0
            ? `${subscription.type}:${subscription.coin}`
            : subscription.type;
        return { kind: 'subscription', key, ok: true, detail: '' };
      }

      if (data.channel === 'l2Book') {
        const frame = bookFrameOf(data);
        if (frame !== null) return { kind: 'data', book: true };
        if (data.data !== null && typeof data.data === 'object' && data.data.coin !== undefined && data.data.coin !== symbol) {
          throw new TypeError('unrecognised Hyperliquid l2Book frame for another coin');
        }
        return { kind: 'protocol-error', reason: 'malformed Hyperliquid l2Book frame' };
      }

      if (data.channel === 'trades') {
        if (Array.isArray(data.data) && data.data.some((trade) => trade?.coin !== undefined && trade.coin !== symbol)) {
          throw new TypeError('unrecognised Hyperliquid trades frame for another coin');
        }
        if (!isTradeFrame(data)) throw new TypeError('malformed Hyperliquid trade frame');
        return { kind: 'data', trade: true };
      }

      if (data.channel === 'pong') {
        // The answer to our ping: liveness, nothing more, nothing to read from it.
        return { kind: 'heartbeat', answered: true };
      }

      throw new TypeError(`unrecognised Hyperliquid frame (channel ${JSON.stringify(data.channel ?? null)})`);
    },

    /**
     * The board changes a frame carries: every l2Book push is `{replace:true, levels}` - it is the
     * whole book, never a diff. Trades carry no level changes.
     */
    changesFor(envelope) {
      const frame = bookFrameOf(envelope?.raw);
      if (frame === null) return { replace: false, changes: [] };
      const levels = [];
      for (const level of frame.bids) levels.push({ side: 'bid', price: level.price, size: level.size });
      for (const level of frame.asks) levels.push({ side: 'ask', price: level.price, size: level.size });
      return { replace: true, levels };
    },

    /**
     * The time rule, applied by reception before a frame is stamped: the book time must not move
     * backwards. A step backwards is a stale frame - the venue's pushes are full replacements, so
     * applying one would walk the whole book back - and it fails closed until a new connection. An
     * equal time is not stale: the stamp is millisecond-granular, and a frame carrying the time
     * already held is not behind it. There is no skip allowance: the forward distance is
     * block-driven and unbounded by design. Trades carry no book time to judge.
     */
    acceptDepthEvent(raw) {
      const data = asObject(raw);
      if (data === null) return { status: 'malformed', reason: 'not a Hyperliquid frame' };
      if (data.channel === 'trades') return { status: 'applied' };
      if (data.channel !== 'l2Book') {
        return { status: 'malformed', reason: `unexpected Hyperliquid frame (channel ${JSON.stringify(data.channel ?? null)})` };
      }
      const frame = bookFrameOf(data);
      if (frame === null) return { status: 'malformed', reason: 'malformed Hyperliquid l2Book frame' };
      if (failed) return { status: 'resync', reason: 'the Hyperliquid stream is failed closed' };

      if (lastAcceptedMs === null) {
        lastAcceptedMs = frame.ms;
        return { status: 'applied' };
      }
      if (frame.ms < lastAcceptedMs) {
        failed = true;
        return {
          status: 'resync',
          reason: `the Hyperliquid book time went backwards (${lastAcceptedMs} -> ${frame.ms})`,
        };
      }
      lastAcceptedMs = frame.ms;
      return { status: 'applied' };
    },

    /**
     * The book-side half of the same rule, for the single-process structure that resolves the
     * boundary proof from the adapter. Every book frame is judged on its own time: the first of a
     * range is its own proof, and each later one must not move backwards - an equal time connects,
     * because reception stamps equal frames too and the two sides must not disagree about a
     * stream. Reception refuses a stale stream until a new connection, and the book's half
     * persists its refusal the same way. The last frame the book proved carries its time on the
     * envelope and is adopted before anything is judged, so a non-book frame in between cannot
     * erase what the rule had already proven. Frames with no book time connect to nothing and are
     * let through.
     */
    connects({ previous = null, current = null } = {}) {
      const previousMs = Number.isInteger(previous?.meta?.venue_seq) ? previous.meta.venue_seq : null;
      if (previousMs !== null && (lastProvenMs === null || previousMs > lastProvenMs)) lastProvenMs = previousMs;
      const frame = bookFrameOf(current?.raw ?? current);
      if (frame === null) return true;
      if (provenFailed) return false;
      if (lastProvenMs === null) {
        lastProvenMs = frame.ms;
        return true;
      }
      if (frame.ms < lastProvenMs) {
        provenFailed = true;
        return false;
      }
      lastProvenMs = frame.ms;
      return true;
    },

    /** The book time (ms), for the envelope's `meta.venue_seq`. Only l2Book frames have one. */
    venueSeqOf(raw) {
      const frame = bookFrameOf(raw);
      return frame === null ? null : frame.ms;
    },
  };
}

/** The perpetual market (BTC): l2Book and trades on the public socket. */
export function createHyperliquidAdapter({ market = 'hyperliquid_perp', symbol = 'BTC', url = null, restUrl = null } = {}) {
  return makeHyperliquidAdapter({ market, symbol, url, restUrl });
}
