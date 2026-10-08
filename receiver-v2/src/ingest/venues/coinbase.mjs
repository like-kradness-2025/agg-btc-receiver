/**
 * Coinbase Advanced Trade public WS: the spot market this receiver admits - BTC-USD.
 *
 * The wire contract here is the official one, checked against the live public stream on
 * `wss://advanced-trade-ws.coinbase.com`:
 *
 *  - Three channels are asked for, each in its own frame: `level2` (the book), `market_trades`,
 *    and `heartbeats` - the venue's own liveness channel and its documented keep-alive. There is no
 *    client ping to send; the heartbeats subscription keeps the connection open and makes the link
 *    observable at one-second granularity.
 *  - The acknowledgement is a `subscriptions` frame carrying the whole acknowledged set at once,
 *    keyed by channel and product. It is read as exactly that: every key present is acknowledged.
 *    A refused product answers with an empty set and no error frame - a key that never appears is
 *    a link that never establishes, and the connection's ack deadline is what fails it.
 *  - `l2_data` pushes a full `snapshot` and then `update`s. `new_quantity` is the level's
 *    post-update quantity, not a delta, and 0 removes the level; the sides are `bid` and `offer`.
 *  - The sequence rule is the venue's own: `sequence_num` is a book-change counter, and the server
 *    coalesces changes into one frame, so forward skips of 2-3 are normal delivery (measured live,
 *    and the reason v1's strict +1 check was replaced by a tolerance). Continuity here is strictly
 *    forward, within a coalescing allowance of 32 (v1's calibrated ceiling): a repeat, a backwards
 *    step, a skip beyond the allowance, or an update before any snapshot fails closed. The
 *    allowance is re-calibratable through `seqSkipTolerance`.
 *  - `market_trades` frames are trades: classified, and dropped by reception like every venue's
 *    trades. A frame for another product cannot enter the stream: it is thrown, which reception
 *    counts, never guessed at.
 *
 * The REST books endpoint is declared as the venue's public fallback location; the WS snapshot is
 * the subscription's own synchronization path and the adapter never fetches it.
 */

const COINBASE_PRODUCTS = Object.freeze({
  coinbase_spot: Object.freeze(['BTC-USD']),
});

const COINBASE_WS = 'wss://advanced-trade-ws.coinbase.com';
const COINBASE_REST_BASE = 'https://api.exchange.coinbase.com/products';

/** v1's calibrated ceiling for the venue's coalescing skips (measured +2/+3 at rest). */
const DEFAULT_SEQ_SKIP_TOLERANCE = 32;

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

/** An l2 update row: the side, the price level, the post-update quantity, and a non-empty event time. */
function validUpdate(update) {
  if (update === null || typeof update !== 'object') return false;
  if (update.side !== 'bid' && update.side !== 'offer') return false;
  if (typeof update.event_time !== 'string' || update.event_time.length === 0) return false;
  const price = finiteNumber(update.price_level);
  const size = finiteNumber(update.new_quantity);
  return price !== null && price > 0 && size !== null && size >= 0;
}

function makeCoinbaseAdapter({ market = 'coinbase_spot', symbol = 'BTC-USD', url = null, restUrl = null, seqSkipTolerance = DEFAULT_SEQ_SKIP_TOLERANCE } = {}) {
  const known = COINBASE_PRODUCTS[market];
  if (!Array.isArray(known) || !known.includes(symbol)) {
    throw new TypeError(`unsupported Coinbase instrument for ${market}: ${symbol}`);
  }
  const tolerance = Number.isFinite(seqSkipTolerance) && seqSkipTolerance >= 1 ? Math.floor(seqSkipTolerance) : DEFAULT_SEQ_SKIP_TOLERANCE;

  const subscriptions = [
    { key: `level2:${symbol}`, frame: { type: 'subscribe', product_ids: [symbol], channel: 'level2' } },
    { key: `market_trades:${symbol}`, frame: { type: 'subscribe', product_ids: [symbol], channel: 'market_trades' } },
    { key: 'heartbeats:heartbeats', frame: { type: 'subscribe', channel: 'heartbeats' } },
  ];

  let lastAcceptedSeq = null;
  let lastProvenSeq = null;
  let failed = false;
  let provenFailed = false;

  function resetState() {
    lastAcceptedSeq = null;
    lastProvenSeq = null;
    failed = false;
    provenFailed = false;
  }

  /**
   * A well-formed l2_data frame for our product, or null: a non-negative integer sequence_num and
   * events of one type, each carrying validated update rows.
   */
  function l2FrameOf(value) {
    const data = asObject(value);
    if (data === null || data.channel !== 'l2_data') return null;
    if (!Number.isInteger(data.sequence_num) || data.sequence_num < 0) return null;
    if (!Array.isArray(data.events) || data.events.length === 0) return null;
    const events = [];
    for (const event of data.events) {
      if (event === null || typeof event !== 'object') return null;
      if (event.type !== 'snapshot' && event.type !== 'update') return null;
      if (event.product_id !== symbol) return null;
      if (!Array.isArray(event.updates)) return null;
      if (event.type === 'snapshot' && event.updates.length === 0) return null;
      if (!event.updates.every(validUpdate)) return null;
      events.push({ type: event.type, updates: event.updates });
    }
    if (!events.every((event) => event.type === events[0].type)) return null;
    return { seq: data.sequence_num, type: events[0].type, events };
  }

  /** Whether every trade row in a market_trades frame is a well-formed trade of our product. */
  function isTradeFrame(data) {
    if (!Array.isArray(data.events) || data.events.length === 0) return false;
    return data.events.every(
      (event) =>
        event !== null &&
        typeof event === 'object' &&
        (event.type === 'snapshot' || event.type === 'update') &&
        Array.isArray(event.trades) &&
        event.trades.length > 0 &&
        event.trades.every(
          (trade) =>
            trade !== null &&
            typeof trade === 'object' &&
            trade.product_id === symbol &&
            (typeof trade.trade_id === 'string' || typeof trade.trade_id === 'number') &&
            finiteNumber(trade.price) !== null &&
            Number(trade.price) > 0 &&
            finiteNumber(trade.size) !== null &&
            Number(trade.size) > 0 &&
            typeof trade.time === 'string' &&
            trade.time.length > 0 &&
            (trade.side === 'BUY' || trade.side === 'SELL'),
        ),
    );
  }

  /** The level changes an l2 frame carries, in the v1 contract's shape. */
  function changesOfFrame(frame) {
    const changes = [];
    for (const event of frame.events) {
      for (const update of event.updates) {
        changes.push({
          side: update.side === 'bid' ? 'bid' : 'ask',
          price: Number(update.price_level),
          size: Number(update.new_quantity),
        });
      }
    }
    return changes;
  }

  /** Whether an events array names a product that is not ours (an anomaly), rather than being shapeless. */
  function namesAnotherProduct(events, key) {
    if (!Array.isArray(events)) return false;
    for (const event of events) {
      if (key === 'l2_data') {
        if (event?.product_id !== undefined && event.product_id !== symbol) return true;
      } else {
        const trades = Array.isArray(event?.trades) ? event.trades : [];
        if (trades.some((trade) => trade?.product_id !== undefined && trade.product_id !== symbol)) return true;
      }
    }
    return false;
  }

  return {
    market,
    symbol,
    url: url ?? COINBASE_WS,
    restUrl: restUrl ?? `${COINBASE_REST_BASE}/${symbol}/book?level=3`,
    stream: 'trades',

    // The proof this venue gives is its sequence_num: forward, within the coalescing allowance.
    boundary: 'sequence',

    ackMode: 'explicit',
    expectedSubscriptions: () => subscriptions.map((entry) => entry.key),

    subscribeMessages: () => subscriptions.map((entry) => JSON.stringify(entry.frame)),

    // Nothing to send: the venue pushes heartbeats on its own channel once subscribed, and there is
    // no client ping in its contract. The silence watch still runs - it answers a different question.
    keepAlive: () => null,

    // A new socket is a new stream: the sequence cursors describe a range that has ended.
    onConnectionOpen() {
      resetState();
    },

    /**
     * Classify one frame. Data kinds are the ones reception understands; a frame that cannot be
     * classified is thrown, which reception counts - except the acknowledgement, whose empty set
     * is not a refusal to invent but a state that acknowledges nothing.
     */
    parse(raw) {
      const data = asObject(raw);
      if (data === null) throw new TypeError('unrecognised Coinbase frame: not a JSON object');

      if (data.type === 'error') {
        return { kind: 'protocol-error', reason: String(data.message ?? data.reason ?? 'Coinbase error frame') };
      }

      if (data.channel === 'subscriptions') {
        // The cumulative state: every channel/product pair present is acknowledged. The frame is
        // returned as the full state it is - an empty set included - so the connection can see a
        // set that shrank; the ack deadline is what fails a key that never appears.
        if (!Array.isArray(data.events)) throw new TypeError('unrecognised Coinbase subscription frame');
        const keys = [];
        for (const event of data.events) {
          const subscriptionsState = event?.subscriptions;
          if (subscriptionsState === null || typeof subscriptionsState !== 'object') continue;
          for (const [channel, products] of Object.entries(subscriptionsState)) {
            if (!Array.isArray(products)) continue;
            for (const product of products) keys.push(`${channel}:${product}`);
          }
        }
        return { kind: 'subscription', keys, full: true, ok: true, detail: '' };
      }

      if (data.channel === 'l2_data') {
        const frame = l2FrameOf(data);
        if (frame !== null) return { kind: 'data', book: true };
        if (namesAnotherProduct(data.events, 'l2_data')) {
          throw new TypeError('unrecognised Coinbase l2_data frame for another product');
        }
        return { kind: 'protocol-error', reason: 'malformed Coinbase orderbook frame' };
      }

      if (data.channel === 'market_trades') {
        if (namesAnotherProduct(data.events, 'market_trades')) {
          throw new TypeError('unrecognised Coinbase market_trades frame for another product');
        }
        if (!isTradeFrame(data)) throw new TypeError('malformed Coinbase trade frame');
        return { kind: 'data', trade: true };
      }

      if (data.channel === 'heartbeats') {
        // The venue's own liveness signal: nothing is answered, and nothing else is read from it.
        return { kind: 'heartbeat', answered: false };
      }

      throw new TypeError(`unrecognised Coinbase frame (channel ${JSON.stringify(data.channel ?? null)})`);
    },

    /**
     * The board changes a frame carries: a snapshot is `{replace:true, levels}` - it is the whole
     * book - and an update is `{replace:false, changes}` where the size is the post-update quantity
     * and 0 removes the level. Trades carry no level changes.
     */
    changesFor(envelope) {
      const frame = l2FrameOf(envelope?.raw);
      if (frame === null) return { replace: false, changes: [] };
      const changes = changesOfFrame(frame);
      return frame.type === 'snapshot' ? { replace: true, levels: changes } : { replace: false, changes };
    },

    /**
     * The sequence rule, applied by reception before a frame is stamped: the sequence_num must move
     * strictly forward, and no further than the coalescing allowance - the venue's counter skips
     * when it coalesces changes into one frame, but a skip beyond the allowance is a suspected loss.
     * A repeat, a backwards step, or an update before any snapshot fails closed, and stays failed
     * until a new connection. A snapshot re-anchors. Trades have no book sequence to judge.
     */
    acceptDepthEvent(raw) {
      const data = asObject(raw);
      if (data === null) return { status: 'malformed', reason: 'not a Coinbase frame' };
      if (data.channel === 'market_trades') return { status: 'applied' };
      if (data.channel !== 'l2_data') {
        return { status: 'malformed', reason: `unexpected Coinbase frame (channel ${JSON.stringify(data.channel ?? null)})` };
      }
      const frame = l2FrameOf(data);
      if (frame === null) return { status: 'malformed', reason: 'malformed Coinbase orderbook frame' };
      if (failed) return { status: 'resync', reason: 'the Coinbase stream is failed closed' };

      if (frame.type === 'snapshot') {
        lastAcceptedSeq = frame.seq;
        return { status: 'applied' };
      }
      if (lastAcceptedSeq === null) {
        failed = true;
        return { status: 'resync', reason: 'a Coinbase update arrived before any snapshot' };
      }
      const delta = frame.seq - lastAcceptedSeq;
      if (delta <= 0) {
        failed = true;
        return { status: 'resync', reason: `the Coinbase sequence_num went backwards or repeated (${lastAcceptedSeq} -> ${frame.seq})` };
      }
      if (delta > tolerance) {
        failed = true;
        return { status: 'resync', reason: `the Coinbase sequence_num skipped ${delta} beyond the coalescing allowance (${tolerance})` };
      }
      lastAcceptedSeq = frame.seq;
      return { status: 'applied' };
    },

    /**
     * The book-side half of the same rule, for the single-process structure that resolves the
     * boundary proof from the adapter. A snapshot re-anchors a fresh stream; an update must move
     * strictly forward within the allowance of the last proven sequence. Reception refuses a broken
     * stream until a new connection, and the book's half persists its refusal the same way - the
     * two sides must not disagree about a stream. The last frame the book proved carries its
     * sequence on the envelope and is adopted before anything is judged, so a non-book frame in
     * between cannot erase what the rule had already proven. Frames with no book sequence connect
     * to nothing and are let through.
     */
    connects({ previous = null, current = null } = {}) {
      const previousSeq = Number.isInteger(previous?.meta?.venue_seq) ? previous.meta.venue_seq : null;
      if (previousSeq !== null && (lastProvenSeq === null || previousSeq > lastProvenSeq)) lastProvenSeq = previousSeq;
      const frame = l2FrameOf(current?.raw ?? current);
      if (frame === null) return true;
      if (provenFailed) return false;
      if (frame.type === 'snapshot') {
        lastProvenSeq = frame.seq;
        return true;
      }
      if (lastProvenSeq === null) {
        provenFailed = true;
        return false;
      }
      const delta = frame.seq - lastProvenSeq;
      if (delta <= 0 || delta > tolerance) {
        provenFailed = true;
        return false;
      }
      lastProvenSeq = frame.seq;
      return true;
    },

    /** The book sequence, for the envelope's `meta.venue_seq`. Only l2 frames have one. */
    venueSeqOf(raw) {
      const frame = l2FrameOf(raw);
      return frame === null ? null : frame.seq;
    },
  };
}

/** The Spot market (BTC-USD): level2, market_trades and heartbeats on the Advanced Trade socket. */
export function createCoinbaseAdapter({ market = 'coinbase_spot', symbol = 'BTC-USD', url = null, restUrl = null, seqSkipTolerance } = {}) {
  return makeCoinbaseAdapter({ market, symbol, url, restUrl, seqSkipTolerance });
}
