/**
 * OKX v5 public WS: the USDT perpetual (BTC-USDT-SWAP) and the Spot market (BTC-USDT).
 *
 * The wire contract here is the official one, checked against the live public stream on the
 * post-8443 endpoint (`wss://ws.okx.com/ws/v5/public` - OKX stops accepting port 8443 on
 * 2026-10-31, and port 443 already serves the same protocol):
 *
 *  - One connection serves both channels. Each topic is asked for in its own `subscribe` request
 *    under its own alphanumeric `id` (the documented id format - a hyphen is refused with 60033).
 *    A successful ack carries the channel's own `arg`, so the key comes from the ack itself; a
 *    refusal (`event:"error"`) echoes the request `id`, so the refusal maps back to the topic it
 *    refused. One refused topic among acknowledged ones is a failed link, not a link that answered
 *    something.
 *  - `books` pushes a full `snapshot` after the subscription (prevSeqId -1), then `update`s whose
 *    `prevSeqId` is the previous `seqId`. The rule: `prevSeqId` must equal the last `seqId` - a
 *    mismatch fails closed into a resync. Two documented exceptions are accepted, and both must
 *    still bridge (`prevSeqId` is the `seqId` just accepted): the empty keep-alive update
 *    (`prevSeqId == seqId`, the seq held), and the one-time maintenance reset (`seqId < prevSeqId`,
 *    after which the normal rule resumes from the lower seq). `checksum` is deprecated and is
 *    not used. An update before any snapshot fails closed; a fresh subscription answers with a
 *    snapshot. A `notice` (64008) is the venue announcing a service upgrade: the connection is
 *    replaced.
 *  - `trades` frames are trades; `liquidation-orders` frames are liquidations. Both are classified
 *    and neither carries level changes - the board only ever moves on book frames. A trade is then
 *    dropped by reception (as it is for every venue here), while a liquidation is stamped and
 *    travels as a frame with an empty diff, so it is recorded. The liquidation channel is
 *    instType-wide (its subscription takes only `instType`): frames for other instruments are
 *    expected traffic and are not ours to carry - they are dropped, not reported as anomalies.
 *  - The keep-alive is the documented text `ping`, sent when the socket has gone quiet (10
 *    seconds, inside the connection's 15 second silence deadline; the venue's own rule is a ping
 *    after under 30 seconds of silence) and answered by a text `pong`. Server-side liveness is the
 *    WebSocket protocol ping, answered by the socket implementation itself.
 *
 * The REST books endpoint is declared as the venue's public fallback location. The adapter itself
 * does not fetch it: the WS snapshot is the subscription's own synchronization path, and a missing
 * one fails closed into a resubscribe rather than a second source of the same book.
 */

import { rawTrade, rawBook, rawLiquidation, collapse } from './raw-shape.mjs';
import { openInterestRecord } from '../oi.mjs';

const OKX_INSTRUMENTS = Object.freeze({
  okx_perp: Object.freeze(['BTC-USDT-SWAP']),
  okx_spot: Object.freeze(['BTC-USDT']),
});

const OKX_WS = 'wss://ws.okx.com/ws/v5/public';
const OKX_REST_BASE = 'https://openapi.okx.com/api/v5/market/books';

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

/** An OKX level is [px, sz, ...]: the first two fields are the ones this receiver interprets. */
function validLevel(level) {
  if (!Array.isArray(level) || level.length < 2) return false;
  const price = finiteNumber(level[0]);
  const size = finiteNumber(level[1]);
  return price !== null && price > 0 && size !== null && size >= 0;
}

function makeOkxAdapter({ market, symbol, instType, liquidation, openInterest = false, url = null, restUrl = null, bookDepth = 400, contractValue = 1 } = {}) {
  const known = OKX_INSTRUMENTS[market];
  if (!Array.isArray(known) || !known.includes(symbol)) {
    throw new TypeError(`unsupported OKX instrument for ${market}: ${symbol}`);
  }

  // One request per topic, each under its own alphanumeric id. The ack names its topic through
  // `arg`; the id is what a refusal echoes, so it is the bridge back to the expected key.
  // Set 8: the perp adds OKX's own `open-interest` (OI), `funding-rate` and `mark-price` channels -
  // v1 polled the same three values over REST (`lib/derivatives-helper.mjs:303-353`); the WS channels
  // are the venue's push form of the same data.
  const subscriptions = [
    { id: 'ob1', key: `books:${symbol}`, arg: { channel: 'books', instId: symbol } },
    { id: 'tr1', key: `trades:${symbol}`, arg: { channel: 'trades', instId: symbol } },
    ...(liquidation ? [{ id: 'lq1', key: `liquidation-orders:${instType}`, arg: { channel: 'liquidation-orders', instType } }] : []),
    ...(openInterest ? [
      { id: 'oi1', key: `open-interest:${symbol}`, arg: { channel: 'open-interest', instId: symbol } },
      { id: 'fr1', key: `funding-rate:${symbol}`, arg: { channel: 'funding-rate', instId: symbol } },
      { id: 'mp1', key: `mark-price:${symbol}`, arg: { channel: 'mark-price', instId: symbol } },
    ] : []),
  ];
  const keyById = new Map(subscriptions.map((entry) => [entry.id, entry.key]));

  // Reception-side continuity (`acceptDepthEvent`) and book-side continuity (`connects`) are kept
  // apart on purpose: in the single-process structure both run over the same frames.
  let lastAcceptedSeq = null;
  let lastProvenSeq = null;
  let failed = false;
  // Set 8: the OI/funding/mark channels arrive separately, so the mark and funding a row is built
  // with are the latest seen - the open-interest frame itself carries only OI. v1 published one row
  // per poll with all three coalesced; this keeps that row whole.
  let mergedMarkPrice = null;
  let mergedFundingRate = null;
  let mergedNextFundingTime = null;

  function resetState() {
    lastAcceptedSeq = null;
    lastProvenSeq = null;
    failed = false;
    mergedMarkPrice = null;
    mergedFundingRate = null;
    mergedNextFundingTime = null;
  }

  /** A well-formed book frame for our instrument, or null: one row with an integer seqId/prevSeqId and valid levels. */
  function bookFrameOf(value) {
    const data = asObject(value);
    if (data === null || data.arg?.channel !== 'books' || data.arg?.instId !== symbol) return null;
    if (data.action !== 'snapshot' && data.action !== 'update') return null;
    const row = Array.isArray(data.data) && data.data.length === 1 ? data.data[0] : null;
    if (row === null || typeof row !== 'object') return null;
    if (!Number.isInteger(row.seqId) || row.seqId < 0) return null;
    if (!Number.isInteger(row.prevSeqId)) return null;
    if (!Array.isArray(row.bids) || !Array.isArray(row.asks)) return null;
    if (!row.bids.every(validLevel) || !row.asks.every(validLevel)) return null;
    return { action: data.action, seqId: row.seqId, prevSeqId: row.prevSeqId, bids: row.bids, asks: row.asks };
  }

  /** The trade frame's identity and numbers. sz is contracts on the swap and base currency on spot. */
  function isTradeFrame(data) {
    if (data.arg?.instId !== symbol || !Array.isArray(data.data) || data.data.length === 0) return false;
    return data.data.every(
      (row) =>
        row !== null &&
        typeof row === 'object' &&
        row.instId === symbol &&
        (row.side === 'buy' || row.side === 'sell') &&
        finiteNumber(row.px) !== null &&
        Number(row.px) > 0 &&
        finiteNumber(row.sz) !== null &&
        Number(row.sz) > 0 &&
        Number.isInteger(Number(row.ts)) &&
        (typeof row.tradeId === 'string' || typeof row.tradeId === 'number'),
    );
  }

  /** The rows of a liquidation frame that belong to our instrument, validated; null when the frame itself is malformed. */
  function ourLiquidationRows(data) {
    if (!Array.isArray(data.data) || data.data.length === 0) return null;
    const ours = [];
    for (const row of data.data) {
      // Every row must name its instrument. Only a row that names another instrument is the
      // expected traffic of an instType-wide channel; a row that names none is the frame changing
      // shape and is not dropped in its company.
      if (row === null || typeof row !== 'object' || typeof row.instId !== 'string' || row.instId.length === 0) {
        return null;
      }
      if (row.instId !== symbol) continue;
      if (!Array.isArray(row.details) || row.details.length === 0) return null;
      for (const detail of row.details) {
        if (
          detail === null ||
          typeof detail !== 'object' ||
          (detail.side !== 'buy' && detail.side !== 'sell') ||
          finiteNumber(detail.sz) === null ||
          Number(detail.sz) <= 0 ||
          finiteNumber(detail.bkPx) === null ||
          !Number.isInteger(Number(detail.ts))
        ) {
          return null;
        }
      }
      ours.push(row);
    }
    return ours;
  }

  /** The level changes a book frame carries, in the v1 contract's shape. */
  function changesOfFrame(frame) {
    const changes = [];
    for (const [side, list] of [
      ['bid', frame.bids],
      ['ask', frame.asks],
    ]) {
      for (const level of list) changes.push({ side, price: Number(level[0]), size: Number(level[1]) });
    }
    return changes;
  }

  return {
    market,
    symbol,
    url: url ?? OKX_WS,
    restUrl: restUrl ?? `${OKX_REST_BASE}?instId=${symbol}&sz=${bookDepth}`,
    stream: 'trades',

    // The proof this venue gives is its sequence pair: prevSeqId must bridge to the last seqId
    // over the range the rule judged. No checksum is claimed - the venue deprecates it.
    boundary: 'sequence',

    ackMode: 'explicit',
    expectedSubscriptions: () => subscriptions.map((entry) => entry.key),

    subscribeMessages: () => subscriptions.map((entry) => JSON.stringify({ id: entry.id, op: 'subscribe', args: [entry.arg] })),

    // The documented keep-alive: a text ping when the socket has gone quiet, answered by a text pong.
    keepAlive: () => ({ noActivityMs: 10_000, payload: () => 'ping' }),

    // A new socket is a new stream: the sequence cursors describe a range that has ended.
    onConnectionOpen() {
      resetState();
    },

    /**
     * Classify one frame. Data kinds are the ones reception understands; a frame that cannot be
     * classified is thrown, which reception counts as an unparsable frame - except another
     * instrument's liquidation, which the instType-wide channel makes expected traffic.
     */
    parse(raw) {
      const text = textOf(raw);
      if (text === 'pong') return { kind: 'heartbeat', answered: true };
      if (text === 'ping') return { kind: 'heartbeat', answered: false };

      const data = asObject(raw);
      if (data === null) throw new TypeError('unrecognised OKX frame: not a JSON object');

      if (data.event === 'subscribe') {
        const channel = data.arg?.channel;
        if (typeof channel !== 'string') throw new TypeError('unrecognised OKX frame: an ack without a channel');
        const key = `${channel}:${data.arg.instId ?? data.arg.instType ?? '*'}`;
        return { kind: 'subscription', key, ok: true, detail: '' };
      }
      if (data.event === 'error') {
        const key = keyById.get(data.id) ?? 'error';
        return { kind: 'subscription', key, ok: false, detail: `${data.code ?? ''} ${data.msg ?? ''}`.trim() };
      }
      if (data.event === 'notice') {
        return { kind: 'shutdown', detail: `${data.code ?? ''} ${data.msg ?? ''}`.trim() };
      }

      const channel = data.arg?.channel;
      if (channel === 'books') {
        if (data.arg?.instId !== symbol) {
          throw new TypeError(`unrecognised OKX books frame for ${JSON.stringify(data.arg?.instId)}`);
        }
        return bookFrameOf(data) !== null
          ? { kind: 'data', book: true }
          : { kind: 'protocol-error', reason: 'malformed OKX orderbook frame' };
      }
      if (channel === 'trades') {
        if (data.arg?.instId !== symbol) {
          throw new TypeError(`unrecognised OKX trades frame for ${JSON.stringify(data.arg?.instId)}`);
        }
        if (!isTradeFrame(data)) throw new TypeError('malformed OKX trade frame');
        return { kind: 'data', trade: true };
      }
      if (channel === 'liquidation-orders') {
        const ours = ourLiquidationRows(data);
        if (ours === null) throw new TypeError('malformed OKX liquidation frame');
        // The channel is instType-wide: another instrument's liquidation is expected traffic and
        // is not ours to carry. It is dropped here rather than stamped under our market.
        return ours.length > 0 ? { kind: 'data', liquidation: true } : null;
      }
      if (openInterest && (channel === 'open-interest' || channel === 'funding-rate' || channel === 'mark-price')) {
        if (data.arg?.instId !== symbol) {
          throw new TypeError(`unrecognised OKX ${channel} frame for ${JSON.stringify(data.arg?.instId)}`);
        }
        return { kind: 'data', auxiliary: true };
      }
      throw new TypeError(`unrecognised OKX frame (channel ${JSON.stringify(channel ?? null)})`);
    },

    /**
     * The board changes a frame carries: a snapshot is `{replace:true, levels}` - it is the whole
     * book - and an update is `{replace:false, changes}` with size 0 removing a level. Trades and
     * liquidations carry no level changes.
     */
    changesFor(envelope) {
      const frame = bookFrameOf(envelope?.raw);
      if (frame === null) return { replace: false, changes: [] };
      const changes = changesOfFrame(frame);
      return frame.action === 'snapshot' ? { replace: true, levels: changes } : { replace: false, changes };
    },

    /**
     * The sequence rule, applied by reception before a frame is stamped: `prevSeqId` must bridge to
     * the last `seqId` - a mismatch (gap, stale, duplicate, a reset whose prevSeqId does not match)
     * fails closed, and stays failed until a new connection. The documented exceptions bridge like
     * everything else: the empty keep-alive update holds the seq (prevSeqId == seqId) and is
     * accepted as a no-op; the maintenance reset arrives as seqId < prevSeqId and re-anchors the
     * stream to the lower seq. A same-seq update that carries levels is not the keep-alive and
     * fails closed. An update before any snapshot fails closed; a snapshot re-anchors, whatever
     * the previous numbering. Frames that are not book frames (trades, liquidations) are accepted
     * untouched - they have no book sequence to judge.
     */
    acceptDepthEvent(raw) {
      const data = asObject(raw);
      if (data === null) return { status: 'malformed', reason: 'not an OKX frame' };
      const channel = data.arg?.channel;
      if (channel === 'trades' || channel === 'liquidation-orders') return { status: 'applied' };
      if (channel !== 'books' || data.arg?.instId !== symbol) {
        return { status: 'malformed', reason: `unexpected OKX frame (channel ${JSON.stringify(channel ?? null)})` };
      }
      const frame = bookFrameOf(data);
      if (frame === null) return { status: 'malformed', reason: 'malformed OKX orderbook frame' };
      if (failed) return { status: 'resync', reason: 'the OKX stream is failed closed' };

      if (frame.action === 'snapshot') {
        lastAcceptedSeq = frame.seqId;
        return { status: 'applied' };
      }
      if (lastAcceptedSeq === null) {
        failed = true;
        return { status: 'resync', reason: 'an OKX update arrived before any snapshot' };
      }
      // Every update - the reset included - must bridge: its prevSeqId is the seqId just accepted.
      if (frame.prevSeqId !== lastAcceptedSeq) {
        failed = true;
        return {
          status: 'resync',
          reason: `the OKX prevSeqId ${frame.prevSeqId} does not bridge the last seqId ${lastAcceptedSeq}`,
        };
      }
      if (frame.seqId < frame.prevSeqId) {
        // The documented maintenance reset: it bridges like any update, then the seq restarts at a
        // lower value and the normal rule resumes from there.
        lastAcceptedSeq = frame.seqId;
        return { status: 'applied' };
      }
      if (frame.seqId === frame.prevSeqId) {
        // A held seq is only the documented keep-alive, and the keep-alive is empty; a same-seq
        // update carrying levels is a shape nothing here can trust.
        if (frame.bids.length === 0 && frame.asks.length === 0) return { status: 'applied' };
        failed = true;
        return { status: 'resync', reason: 'an OKX update held its seqId but carried level changes' };
      }
      lastAcceptedSeq = frame.seqId;
      return { status: 'applied' };
    },

    /**
     * The book-side half of the same rule, for the single-process structure that resolves the
     * boundary proof from the adapter. A snapshot re-anchors; every update - the maintenance reset
     * and the empty keep-alive included - must bridge to the last proven `seqId`, and the reset
     * and keep-alive re-anchor or hold exactly as reception accepts them. A same-seq update that
     * carries levels, a reset that does not bridge, and an update with nothing proven before it
     * are refused. The last frame the book proved carries its seq on the envelope and is adopted
     * before anything is judged, so a non-book frame in between cannot erase what the rule had
     * already proven. Frames with no book sequence connect to nothing and are let through.
     */
    connects({ previous = null, current = null } = {}) {
      const previousSeq = Number.isInteger(previous?.meta?.venue_seq) ? previous.meta.venue_seq : null;
      if (previousSeq !== null && (lastProvenSeq === null || previousSeq > lastProvenSeq)) lastProvenSeq = previousSeq;
      const frame = bookFrameOf(current?.raw ?? current);
      if (frame === null) return true;
      if (frame.action === 'snapshot') {
        lastProvenSeq = frame.seqId;
        return true;
      }
      // Every update - the reset included - must bridge to the last seq the rule proved.
      if (lastProvenSeq === null || frame.prevSeqId !== lastProvenSeq) return false;
      if (frame.seqId < frame.prevSeqId) {
        lastProvenSeq = frame.seqId; // the documented reset: bridges, then the seq restarts lower
        return true;
      }
      if (frame.seqId === frame.prevSeqId) {
        // A held seq connects only as the documented empty keep-alive; a same-seq update with
        // levels is refused.
        return frame.bids.length === 0 && frame.asks.length === 0;
      }
      lastProvenSeq = frame.seqId;
      return true;
    },

    /**
     * Set 7b: classify one received frame as a raw record, an array of records, or null. The payloads
     * are the running v1 store's, measured 2026-10-10 (`okx_spot`/`okx_perp`): an update is
     * `{prev_seq, type:'update', bids, asks, ts, seq}` (the venue's own `prevSeqId` when present, else
     * the last accepted `seqId`), a snapshot is `{snapshot_origin:'ws_sync', type:'snapshot', bids,
     * asks, ts, seq}`. A trade is v1's `{market, price, qty, side, ts, tradeId}` with `qty` in coin
     * (`sz * ctVal`); a liquidation is v1's `{market, exchange, symbol, side, price, qty, notional,
     * raw_type:'liquidation-orders', trade_id, source_ts, ts}`.
     */
    rawEventFor(frame) {
      const data = asObject(frame?.raw);
      if (data === null) return null;
      const channel = data.arg?.channel;
      if (channel === 'trades') {
        if (data.arg?.instId !== symbol || !isTradeFrame(data)) return null;
        const out = [];
        for (const row of data.data) {
          const ts = Number(row.ts);
          if (!Number.isInteger(ts) || ts <= 0) continue;
          out.push(rawTrade({ market, price: Number(row.px), qty: Number(row.sz) * contractValue, side: row.side === 'buy' ? 'buy' : 'sell', ts, tradeId: row.tradeId }));
        }
        return collapse(out);
      }
      if (channel === 'liquidation-orders') {
        const rows = ourLiquidationRows(data);
        if (rows === null || rows.length === 0) return null;
        const out = [];
        for (const row of rows) {
          for (const detail of row.details) {
            const price = Number(detail.fillPx ?? detail.bkPx);
            const qty = Number(detail.sz) * contractValue;
            if (!(price > 0) || !(qty > 0)) continue;
            const sourceTs = Number(detail.ts);
            out.push(rawLiquidation({
              market, exchange: 'okx', symbol: row.instId, side: detail.side === 'buy' ? 'buy' : 'sell',
              price, qty, notional: price * qty, raw_type: 'liquidation-orders', trade_id: null,
              source_ts: Number.isInteger(sourceTs) && sourceTs > 0 ? sourceTs : null, ts: frame.atMs,
            }));
          }
        }
        return collapse(out);
      }
      if (openInterest && channel === 'open-interest') {
        const row = Array.isArray(data.data) && data.data.length === 1 ? data.data[0] : null;
        if (row === null || typeof row !== 'object' || row.instId !== symbol) return null;
        const sourceTs = Number(row.ts);
        if (!Number.isInteger(sourceTs) || sourceTs <= 0) return null;
        const ts = Number.isFinite(frame?.atMs) && frame.atMs > 0 ? Math.floor(frame.atMs) : Date.now();
        return openInterestRecord({
          market,
          sample: {
            open_interest: Number(row.oi),
            oiCcy: finiteNumber(row.oiCcy),
            oiUsd: finiteNumber(row.oiUsd),
            mark_price: mergedMarkPrice,
            funding_rate: mergedFundingRate,
            next_funding_time: mergedNextFundingTime,
            source_ts: sourceTs,
            ts: sourceTs,
          },
          ts,
          nowMs: ts,
        });
      }
      if (openInterest && channel === 'funding-rate') {
        const row = Array.isArray(data.data) ? data.data[0] : null;
        if (row !== null && typeof row === 'object' && row.instId === symbol) {
          mergedFundingRate = finiteNumber(row.fundingRate);
          mergedNextFundingTime = finiteNumber(row.fundingTime);
        }
        return null;
      }
      if (openInterest && channel === 'mark-price') {
        const row = Array.isArray(data.data) ? data.data[0] : null;
        if (row !== null && typeof row === 'object' && row.instId === symbol) {
          mergedMarkPrice = finiteNumber(row.markPx);
        }
        return null;
      }
      if (channel === 'books') {
        const book = bookFrameOf(data);
        if (book === null) return null;
        const bids = book.bids.map((level) => [String(level[0]), String(level[1])]);
        const asks = book.asks.map((level) => [String(level[0]), String(level[1])]);
        const ts = Number(data.data?.[0]?.ts);
        if (!Number.isInteger(ts) || ts <= 0) return null;
        if (book.action === 'snapshot') {
          const payload = { snapshot_origin: 'ws_sync', type: 'snapshot', bids, asks, ts, seq: book.seqId };
          const write = (stream) => rawBook({ market, stream, payload, event_ts_ms: ts, source_event_ts_ms: ts, source_event_time_known: true });
          return [write('book_updates'), write('snapshots')];
        }
        const payload = { prev_seq: Number.isInteger(book.prevSeqId) ? book.prevSeqId : lastAcceptedSeq, type: 'update', bids, asks, ts, seq: book.seqId };
        return rawBook({ market, payload, event_ts_ms: ts, source_event_ts_ms: ts, source_event_time_known: true });
      }
      return null;
    },

    /** The book sequence, for the envelope's `meta.venue_seq`. Only book frames have one. */
    venueSeqOf(raw) {
      const frame = bookFrameOf(raw);
      return frame === null ? null : frame.seqId;
    },
  };
}

/** The USDT perpetual (BTC-USDT-SWAP): books at 400 levels, trades, and liquidations. */
export function createOkxPerpAdapter({ market = 'okx_perp', symbol = 'BTC-USDT-SWAP', bookDepth = 400, url = null, restUrl = null } = {}) {
  // v1 measured the swap contract as 0.01 BTC (`lib/okx-connector.mjs:17`); a trade or liquidation
  // size is contracts, so the raw qty is `sz * ctVal` exactly as v1 emitted it.
  return makeOkxAdapter({ market, symbol, instType: 'SWAP', liquidation: true, openInterest: true, bookDepth, url, restUrl, contractValue: 0.01 });
}

/** The Spot market (BTC-USDT): books at 400 levels and trades. */
export function createOkxSpotAdapter({ market = 'okx_spot', symbol = 'BTC-USDT', bookDepth = 400, url = null, restUrl = null } = {}) {
  return makeOkxAdapter({ market, symbol, instType: 'SPOT', liquidation: false, bookDepth, url, restUrl });
}
