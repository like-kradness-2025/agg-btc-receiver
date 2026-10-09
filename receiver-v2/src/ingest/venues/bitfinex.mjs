/** Bitfinex public aggregated book adapter (SEQ_ALL + OB_CHECKSUM). */

import { rawBook, rawTrade, collapse } from './raw-shape.mjs';

const INFO_RECONNECT = 20051;
const INFO_MAINTENANCE_START = 20060;
const INFO_MAINTENANCE_END = 20061;
const ALREADY_SUBSCRIBED = 10301;
const SEQ_ALL = 65536;
const OB_CHECKSUM = 131072;

const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();
function crc32Signed(text) {
  let crc = 0xffffffff;
  for (let i = 0; i < text.length; i += 1) crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ text.charCodeAt(i)) & 0xff];
  return (crc ^ 0xffffffff) | 0;
}
function textOf(raw) { return typeof raw === 'string' ? raw : raw?.toString?.('utf8') ?? ''; }
function jsonOf(raw) { try { return JSON.parse(textOf(raw)); } catch { return null; } }

export function createBitfinexAdapter({
  market = 'bitfinex_spot', symbol = 'tBTCUSD', bookPrecision = 'P0', bookFrequency = 'F0', bookLength = '25',
  url = 'wss://api-pub.bitfinex.com/ws/2',
} = {}) {
  const stream = 'book';
  const funding = symbol.startsWith('f');
  if (!/^[tf][A-Z0-9]+$/.test(symbol)) throw new TypeError(`unsupported Bitfinex book symbol: ${symbol}`);
  const channelById = new Map();
  const expectedKeys = [`book:${symbol}`, `trades:${symbol}`];
  let tradeChanId = null;
  let lastSeq = null;
  let mirror = { bids: new Map(), asks: new Map() };
  let pending = [];
  let failed = false;
  let sawSnapshot = false;

  function resetState() {
    lastSeq = null;
    mirror = { bids: new Map(), asks: new Map() };
    pending = [];
    failed = false;
    sawSnapshot = false;
    channelById.clear();
    tradeChanId = null;
  }
  function checksumInput(book) {
    const bids = [...book.bids.values()].sort((a, b) => b.price - a.price).slice(0, 25);
    const asks = [...book.asks.values()].sort((a, b) => a.price - b.price).slice(0, 25);
    const parts = [];
    for (let i = 0; i < 25; i += 1) {
      if (bids[i]) parts.push(bids[i].priceText, bids[i].amountText);
      if (asks[i]) parts.push(asks[i].priceText, asks[i].amountText);
    }
    return parts.join(':');
  }
  function levelOf(level) {
    if (!Array.isArray(level) || level.length < (funding ? 4 : 3)) return null;
    const price = Number(level[0]);
    const count = Number(level[funding ? 2 : 1]);
    const amount = Number(level[funding ? 3 : 2]);
    if (!Number.isFinite(price) || !Number.isInteger(count) || count < 0 || !Number.isFinite(amount)) return null;
    return { price, count, amount, priceText: String(level[0]), amountText: String(level[funding ? 3 : 2]) };
  }
  function changesOfData(data) {
    const body = data[1];
    const snapshot = Array.isArray(body) && Array.isArray(body[0]);
    const levels = snapshot ? body : [body];
    const changes = [];
    for (const rawLevel of levels) {
      const level = levelOf(rawLevel);
      if (!level) return null;
      changes.push({
        side: funding ? (level.amount < 0 ? 'bid' : 'ask') : (level.amount > 0 ? 'bid' : 'ask'),
        price: level.price,
        size: level.count === 0 ? 0 : Math.abs(level.amount),
      });
    }
    return { snapshot, levels, changes };
  }
  function candidateFor(frame) {
    const next = { bids: new Map(mirror.bids), asks: new Map(mirror.asks) };
    if (frame.snapshot) { next.bids.clear(); next.asks.clear(); }
    for (const level of frame.levels) {
      const normalized = levelOf(level);
      if (!normalized) return null;
      const side = funding ? (normalized.amount < 0 ? 'bid' : 'ask') : (normalized.amount > 0 ? 'bid' : 'ask');
      const map = side === 'bid' ? next.bids : next.asks;
      if (normalized.count === 0) map.delete(normalized.price);
      else map.set(normalized.price, normalized);
    }
    return next;
  }
  function sequenceOf(data) {
    if (!Array.isArray(data) || !Number.isInteger(data[0])) return null;
    const candidate = data[data.length - 1];
    return Number.isInteger(candidate) ? candidate : null;
  }
  function protocolFrame(data) {
    if (!Array.isArray(data) || !Number.isInteger(data[0])) return null;
    if (data[1] === 'hb') {
      const seq = Number.isInteger(data[2]) ? data[2] : null;
      return { kind: 'heartbeat', answered: false, ...(seq === null ? { sequenced: false } : { seq }) };
    }
    if (!channelById.has(data[0])) return { kind: 'protocol-error', reason: `unknown channel id ${String(data[0])}` };
    if (data[1] === 'cs') {
      if (data.length === 3 && Number.isInteger(data[2])) return { kind: 'checksum', checksum: data[2], seq: null };
      if (data.length === 4 && Number.isInteger(data[2]) && Number.isInteger(data[3])) return { kind: 'checksum', checksum: data[2], seq: data[3] };
      return { kind: 'protocol-error', reason: 'invalid Bitfinex checksum frame' };
    }
    const frame = changesOfData(data);
    if (!frame) return { kind: 'protocol-error', reason: 'invalid Bitfinex book level' };
    const seq = sequenceOf(data);
    if (seq === null) return { kind: 'data', book: true, sequenced: false };
    return { kind: 'data', book: true, seq };
  }

  return {
    market, url, stream, boundary: 'sequence', ackMode: 'explicit',
    expectedSubscriptions: () => [...expectedKeys],
    onConnectionOpen() { resetState(); },
    subscribeMessages: () => [
      JSON.stringify({ event: 'conf', flags: SEQ_ALL + OB_CHECKSUM }),
      JSON.stringify({ event: 'subscribe', channel: 'book', symbol, prec: bookPrecision, freq: bookFrequency, len: bookLength }),
      // Set 8: v1 subscribed Bitfinex `trades` too (`lib/bitfinex-connector.mjs:67-72`) and this
      // receiver now records them; the trade frames are raw-only (the board is book-only).
      JSON.stringify({ event: 'subscribe', channel: 'trades', symbol }),
    ],
    heartbeatMessage: () => JSON.stringify({ event: 'ping' }),
    parse(raw) {
      const data = jsonOf(raw);
      if (!Array.isArray(data)) {
        if (!data || typeof data !== 'object') return null;
        if (data.event === 'subscribed') {
          if (!Number.isInteger(data.chanId) || (data.symbol ?? symbol) !== symbol) return { kind: 'protocol-error', reason: 'invalid Bitfinex subscription acknowledgement' };
          if (data.channel === 'book') {
            channelById.set(data.chanId, `book:${symbol}`);
            return { kind: 'subscription', key: `book:${symbol}`, ok: true };
          }
          if (data.channel === 'trades') {
            tradeChanId = data.chanId;
            return { kind: 'subscription', key: `trades:${symbol}`, ok: true };
          }
          return { kind: 'protocol-error', reason: 'invalid Bitfinex subscription acknowledgement' };
        }
        if (data.event === 'unsubscribed') return { kind: 'subscription', key: `${data.chanId}`, ok: false, detail: 'unsubscribed' };
        if (data.event === 'error') {
          const already = Number(data.code) === ALREADY_SUBSCRIBED;
          return { kind: 'subscription', key: data.chanId ? String(data.chanId) : 'error', ok: already, detail: `${data.code ?? ''} ${data.msg ?? ''}`.trim() };
        }
        if (data.event === 'info') {
          const code = Number(data.code);
          if (code === INFO_RECONNECT) return { kind: 'shutdown', detail: `info ${code}` };
          if (code === INFO_MAINTENANCE_START) return { kind: 'maintenance', detail: `info ${code}`, resume: false };
          if (code === INFO_MAINTENANCE_END) return { kind: 'maintenance', detail: `info ${code}`, resume: true };
          return { kind: 'heartbeat', answered: false };
        }
        if (data.event === 'pong') return { kind: 'heartbeat', answered: true };
        return null;
      }
      // Set 8: a trade-channel frame. Only `tu` (the confirmed, final trade) becomes a record; `te`
      // (immediate, revisable) is dropped exactly as v1 dropped it (`lib/bitfinex-connector.mjs:112-116`),
      // so a trade is never counted twice. A trade snapshot (`[chanId, [[...], ...]]`) is the
      // subscription's own history of the same trades. Every one of them is still a connection
      // message carrying the shared sequence, so all of them have to be classified as data - what
      // becomes a record is decided by `rawEventFor`, not here.
      if (tradeChanId !== null && data[0] === tradeChanId) {
        const body = data[1];
        // A channel heartbeat is a heartbeat whichever channel it rides: it carries the shared
        // sequence too, and the connection hands a sequenced heartbeat to `acceptDepthEvent`.
        if (body === 'hb') return protocolFrame(data);
        if (body === 'tu' || body === 'te' || Array.isArray(body)) return { kind: 'data', trade: true };
        return null;
      }
      return protocolFrame(data);
    },
    changesFor(envelope) {
      const data = jsonOf(envelope?.raw);
      if (!Array.isArray(data) || !Number.isInteger(data[0]) || !channelById.has(data[0])) return { replace: false, changes: [] };
      const frame = changesOfData(data);
      if (!frame) return { replace: false, changes: [] };
      return frame.snapshot ? { replace: true, levels: frame.changes } : { replace: false, changes: frame.changes };
    },
    acceptDepthEvent(raw) {
      const data = jsonOf(raw);
      const parsed = protocolFrame(data);
      if (!parsed || !['data', 'checksum', 'heartbeat'].includes(parsed.kind)) return { status: 'malformed', reason: 'malformed Bitfinex sequenced event' };
      if (parsed.sequenced === false) return { status: 'resync', reason: 'Bitfinex SEQ_ALL sequence missing' };
      const seq = parsed.seq ?? sequenceOf(data);
      if (Number.isInteger(seq)) {
        if (lastSeq !== null && seq !== lastSeq + 1) { failed = true; pending = []; return { status: 'resync', reason: 'Bitfinex sequence gap, duplicate, or reversal', expected: lastSeq + 1, actual: seq }; }
        lastSeq = seq;
      }
      if (failed) return { status: 'resync', reason: 'Bitfinex stream is failed closed' };
      if (parsed.kind === 'heartbeat') return { status: 'heartbeat' };
      if (parsed.kind === 'data') {
        const frame = changesOfData(data);
        const candidate = candidateFor(frame);
        if (!candidate) return { status: 'resync', reason: 'invalid Bitfinex book update' };
        pending.push(String(raw));
        mirror = candidate;
        return { status: 'pending' };
      }
      if (String(parsed.checksum) !== String(crc32Signed(checksumInput(mirror)))) { failed = true; pending = []; return { status: 'resync', reason: 'Bitfinex order book checksum mismatch' }; }
      const released = pending;
      pending = [];
      return { status: 'applied', released };
    },
    connects({ previous = null, current = null } = {}) {
      const currentSeq = sequenceOf(jsonOf(current?.raw ?? current));
      if (!Number.isInteger(currentSeq)) return false;
      const previousSeq = Number.isInteger(previous?.meta?.venue_seq) ? previous.meta.venue_seq : null;
      return previousSeq === null || currentSeq === previousSeq + 1;
    },
    /**
     * Set 7b: classify one received frame as a raw record (or an array, for a snapshot's second
     * write), or null. The payload is the running v1 store's, measured 2026-10-10 (`bitfinex_spot`):
     * `{event_time_source:'local', type, bids, asks, ts, seq:null}` - the venue gives no exchange book
     * time and no sequence, so the time is local and `seq` is null. A board frame whose body is a
     * nested array is the whole book (`type:'snapshot'`); a single `[price, count, amount]` is a
     * diff, and v1 called the first diff before any snapshot a snapshot too. Trades are not part of
     * this adapter (v2 does not subscribe to Bitfinex trades; that is Set 8's catalog item).
     */
    rawEventFor(frame) {
      const data = jsonOf(frame?.raw);
      if (Array.isArray(data) && tradeChanId !== null && data[0] === tradeChanId) {
        const rows = data[1] === 'tu' ? [data[2]] : Array.isArray(data[1]) ? data[1] : [];
        const out = [];
        for (const trade of rows) {
          if (!Array.isArray(trade) || trade.length < 4) continue;
          const price = Number(trade[3]);
          const amount = Number(trade[2]);
          const ts = Number(trade[1]);
          if (!(price > 0) || !Number.isFinite(amount) || !Number.isInteger(ts) || ts <= 0) continue;
          out.push(rawTrade({
            market, price, qty: Math.abs(amount), side: amount < 0 ? 'sell' : 'buy',
            ts, tradeId: String(trade[0]),
          }));
        }
        return collapse(out);
      }
      if (!Array.isArray(data) || !Number.isInteger(data[0]) || !channelById.has(data[0])) return null;
      const parsed = protocolFrame(data);
      if (!parsed || parsed.kind !== 'data') return null;
      const book = changesOfData(data);
      if (!book) return null;
      const bids = [];
      const asks = [];
      for (const level of book.levels) {
        const normalized = levelOf(level);
        if (!normalized) continue;
        const side = funding ? (normalized.amount < 0 ? 'bid' : 'ask') : (normalized.amount > 0 ? 'bid' : 'ask');
        const qty = normalized.count === 0 ? '' : String(Math.abs(normalized.amount));
        (side === 'bid' ? bids : asks).push([String(normalized.price), qty]);
      }
      const type = book.snapshot || !sawSnapshot ? 'snapshot' : 'update';
      if (book.snapshot) sawSnapshot = true;
      const atMs = Number.isFinite(frame?.atMs) && frame.atMs > 0 ? Math.floor(frame.atMs) : Date.now();
      const payload = { event_time_source: 'local', type, bids, asks, ts: atMs, seq: null };
      if (type === 'snapshot') {
        const write = (stream) => rawBook({ market, stream, payload, event_ts_ms: atMs });
        return [write('book_updates'), write('snapshots')];
      }
      return rawBook({ market, payload, event_ts_ms: atMs });
    },
    venueSeqOf(raw) { const data = jsonOf(raw); return sequenceOf(data); },
    /**
     * Set 8: Bitfinex numbers every message on a connection with one sequence, across channels. A
     * trade frame - including the `te` and trade heartbeats that produce no record - therefore has to
     * advance the same counter a book frame does. Without this the next book frame reads as a gap
     * (`expected: 2, actual: 3` after book(1), trade(2), book(3)) and the adapter resyncs on healthy
     * traffic forever.
     */
    acceptAuxFrame(raw) {
      const data = jsonOf(raw);
      if (!Array.isArray(data) || !Number.isInteger(data[0]) || data[0] !== tradeChanId) return null;
      if (failed) return { status: 'resync', reason: 'Bitfinex stream is failed closed' };
      const seq = sequenceOf(data) ?? (Number.isInteger(data[2]) ? data[2] : null);
      if (!Number.isInteger(seq)) return null;
      if (lastSeq !== null && seq !== lastSeq + 1) {
        failed = true;
        pending = [];
        return { status: 'resync', reason: 'Bitfinex sequence gap, duplicate, or reversal', expected: lastSeq + 1, actual: seq };
      }
      lastSeq = seq;
      return { status: 'accepted' };
    },
  };
}
