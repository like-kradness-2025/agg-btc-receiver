// lib/gemini-connector.mjs — Gemini BTC/USD spot connector

import { TradeOnlyConnector } from './trade-only-connector.mjs';

const WS_URL = 'wss://api.gemini.com/v2/marketdata/';
const SYMBOL = 'btcusd';

const toNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const normalizeSide = (trade) => {
  if (trade == null || typeof trade !== 'object') return null;
  if (trade.m != null) return trade.m ? 'sell' : 'buy';
  if (trade.side != null) {
    const v = String(trade.side).toLowerCase();
    if (v === 'buy' || v === 'bid' || v === 'b') return 'buy';
    if (v === 'sell' || v === 'ask' || v === 's') return 'sell';
  }
  if (trade.makerSide != null) {
    const v = String(trade.makerSide).toLowerCase();
    if (v === 'buy' || v === 'bid') return 'sell';
    if (v === 'sell' || v === 'ask') return 'buy';
  }
  return null;
};

const normalizeTs = (value) => {
  // Astra P2: strict type check BEFORE numeric coercion — Number(null)===0,
  // Number('')===0 and Number(false)===0 would fabricate a known ts=0 for a
  // missing timestamp. Only non-blank numbers/numeric strings are accepted;
  // negatives are never valid event clocks. Unknown stays null.
  if (value === null || value === undefined) return null;
  if (typeof value !== 'number' && typeof value !== 'string') return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  const n = Number(value);
  // Issue #12: unparseable source timestamp is unknown (null), never the
  // local receive time. Callers fail-closed on null via _emitTrade.
  if (!Number.isFinite(n) || n < 0) return null;
  if (n > 1e15) return Math.floor(n / 1000);
  if (n > 1e12) return Math.floor(n);
  if (n > 1e9) return Math.floor(n * 1000);
  return Math.floor(n);
};

// Gemini book frames (l2_updates changes) carry no exchange event
// timestamp: ts is the local processing time. Issue #12 marks that
// explicitly so source_event_ts_ms never inherits a local wall clock.
const LOCAL_DEPTH_META = Object.freeze({
  source_event_ts_ms: null,
  source_event_time_known: false,
  event_time_source: 'local',
});

const collectTrades = (data) => {
  if (!data || typeof data !== 'object') return [];
  if (Array.isArray(data.events)) {
    const out = [];
    for (const event of data.events) {
      if (!event || typeof event !== 'object') continue;
      if (Array.isArray(event.trades)) {
        out.push(...event.trades);
      } else if (event.type === 'trade' || event.p != null || event.q != null || event.m != null) {
        out.push(event);
      }
    }
    if (out.length) return out;
  }
  if (Array.isArray(data.trades)) return data.trades;
  if (data.type === 'trade' || data.event === 'trade' || data.m != null || data.p != null || data.q != null) {
    return [data];
  }
  return [];
};

const DEPTH_WS_URL = 'wss://api.gemini.com/v2/marketdata/';

// The depth WS is a separate socket generation from the main WS. It gets its
// own connection id / receive_seq counter so its frames never share — and
// never advance — the main socket's ingress sequence (Astra P2-1).
let depthConnectionSequence = 0;

export class GeminiConnector extends TradeOnlyConnector {
  constructor(config) {
    super(config, {
      market: 'gemini_spot',
      wsUrl: config.wsUrl || WS_URL,
      restUrl: config.restUrl || '',
    });
    this._depthWs = null;
    this._depthSnapshotReceived = false;
    this._depthConnectionId = null;
    this._depthReceiveSeq = 0;
    // Depth socket generation counter (Astra P2): mirrors _wsGeneration on
    // the main socket so callbacks of a superseded depth socket (a frame or
    // error delivered after a reconnect replaced it) bail out instead of
    // recording data under the NEW generation's connection id / receive_seq.
    this._depthWsGeneration = 0;
  }

  subscribe() {
    // Subscribe to l2 on V2 marketdata endpoint (includes trades + depth)
    this._ws.send(JSON.stringify({
      type: 'subscribe',
      subscriptions: [{ name: 'l2', symbols: [SYMBOL] }],
    }));

    // Depth subscription on separate WS (same endpoint, also l2 — redundant but works)
    this._closeDepthWs();
    this._depthSnapshotReceived = false;
    this._connectDepthWs();
  }

  async _connectDepthWs() {
    const WebSocket = await this._getWsImpl();
    if (!WebSocket) {
      this.emit('error', { market: this.market, message: 'depth WS: no WebSocket implementation' });
      return;
    }

    // Fresh depth socket generation: independent connection identity and
    // receive_seq counter, reset on every (re)connect of the depth socket.
    // The generation token guards every callback below — frames or errors
    // from a superseded depth socket are ignored (Astra P2).
    const depthGeneration = ++this._depthWsGeneration;
    this._depthConnectionId = `${this.market}:depth:${process.pid}:${++depthConnectionSequence}`;
    this._depthReceiveSeq = 0;
    this._depthWs = new WebSocket(DEPTH_WS_URL);

    this._depthWs.on('open', () => {
      if (depthGeneration !== this._depthWsGeneration) return; // stale depth socket
      this._depthWs.send(JSON.stringify({
        type: 'subscribe',
        subscriptions: [{ name: 'l2', symbols: [SYMBOL] }],
      }));
    });

    this._depthWs.on('message', (raw) => {
      if (depthGeneration !== this._depthWsGeneration) return; // stale depth socket
      // Issue #12 (Astra P2-1): the depth WS is a separate socket from the
      // main WS, so capture its ingress metadata at this socket's own message
      // boundary with its OWN connection id / receive_seq — this._ingress and
      // the main socket's counter only track the main socket's frames.
      const ingress = this._captureDepthIngress();
      this._lastMsgAt = ingress.recv_ts_ms;
      try {
        const data = JSON.parse(raw.toString());
        if (data.type === 'l2_updates') {
          this._handleDepthUpdate(data, ingress);
        }
      } catch { /* ignore parse errors */ }
    });

    this._depthWs.on('close', () => {
      // Depth WS closed; main WS reconnect cycle will recreate it
    });

    this._depthWs.on('error', (err) => {
      if (depthGeneration !== this._depthWsGeneration) return; // stale depth socket
      this.emit('error', { market: this.market, message: 'depth WS error: ' + err.message });
    });
  }

  /**
   * Capture ingress metadata for one depth-WS frame using the depth socket's
   * OWN connection identity and receive_seq counter — never the main
   * socket's (Astra P2-1). recv_mono_ns still rides the connector-wide
   * monotonic guard so it stays strictly increasing across both sockets.
   * @returns {{recv_ts_ms: number, recv_mono_ns: number|null, connection_id: string|null, receive_seq: number}}
   */
  _captureDepthIngress() {
    return {
      recv_ts_ms: Date.now(),
      recv_mono_ns: this._monoNow(),
      connection_id: this._depthConnectionId ?? null,
      receive_seq: ++this._depthReceiveSeq,
    };
  }

  /**
   * @param {Object} data - parsed l2_updates frame
   * @param {Object|null} [ingress] - ingress metadata captured at the depth
   *   WS message boundary (this socket does not update this._ingress)
   */
  _handleDepthUpdate(data, ingress = null) {
    const ts = Date.now();
    const changes = data.changes || [];

    // Apply each change to the local book
    for (const change of changes) {
      const side = change.side; // 'bid' or 'ask'
      const price = change.price;
      const remaining = change.remaining;
      // Delete level if qty is zero
      const qty = (remaining === '0' || remaining === '0.0') ? '' : remaining;
      this.book.applyDiff(side, price, qty);
    }

    // Emit full book state: first message is snapshot, subsequent are updates
    const snapshot = this.book.toSnapshot(ts);
    const type = this._depthSnapshotReceived ? 'update' : 'snapshot';
    this._depthSnapshotReceived = true;

    this._emitDepth(type, snapshot.bids, snapshot.asks, ts, null, LOCAL_DEPTH_META, ingress);

    // Gemini V2 l2_updates includes a trades array; emit individual trades
    const tradeRows = data.trades || [];
    for (const t of tradeRows) {
      if (!t || typeof t !== 'object') continue;
      const price = toNumber(t.price ?? t.px);
      const qty = toNumber(t.quantity ?? t.qty ?? t.size);
      if (price == null || qty == null) continue;
      // Gemini side is maker side; invert for taker side
      const side = normalizeSide(t);
      // Issue #12: unparseable/missing exchange time is unknown (null) —
      // never fall back to the local processing time.
      // Astra P2: strict type check BEFORE numeric coercion — Number(false),
      // Number('') and Number('   ') are all 0, which would fabricate a known
      // ts:0 / source_event_time_known:true for a missing timestamp. Only
      // non-blank numbers/numeric strings pass (negatives excluded); ISO date
      // strings still resolve via Date.parse; everything else stays null and
      // the trade is dropped fail-closed.
      // Astra R3 (P2 regression): a finite negative — number or numeric
      // string, e.g. '-1' — is rejected in place so it never reaches
      // Date.parse, which would turn '-1' into a known 2001 epoch and emit
      // the trade with a fabricated source time.
      const rawTradeTs = t.timestamp ?? t.ts;
      let tradeTs = null;
      if (rawTradeTs != null) {
        const rawType = typeof rawTradeTs;
        const coercible = (rawType === 'number' || rawType === 'string')
          && (rawType !== 'string' || rawTradeTs.trim() !== '');
        if (coercible) {
          const num = Number(rawTradeTs);
          if (Number.isFinite(num)) {
            if (num >= 0) tradeTs = num;
          } else if (rawType === 'string') {
            // Non-numeric string (e.g. ISO date) may still resolve.
            const parsed = Date.parse(rawTradeTs);
            if (Number.isFinite(parsed) && parsed >= 0) tradeTs = parsed;
          }
        }
      }
      const tradeId = String(t.event_id ?? t.trade_id ?? t.tradeId ?? t.id ?? `${tradeTs}-${price}-${qty}`);
      if (side && tradeTs !== null) {
        this._emitTrade(price, qty, side, tradeTs, tradeId, ingress);
      }
    }
  }

  _closeDepthWs() {
    // Invalidate every callback of the closing socket immediately — a frame
    // that was in flight when the socket was closed must not be processed
    // (or stamped with a newer generation's id/seq) after a reconnect.
    this._depthWsGeneration++;
    if (this._depthWs) {
      try { this._depthWs.close(1000, 'reconnect'); } catch { /* ignore */ }
      this._depthWs = null;
    }
  }

  _resetBook() {
    super._resetBook();
    this._closeDepthWs();
    this._depthSnapshotReceived = false;
  }

  disconnect() {
    this._closeDepthWs();
    super.disconnect();
  }

  _onMessage(data) {
    if (!data || typeof data !== 'object') return;
    if (data.type === 'heartbeat' || data.type === 'subscription_ack' || data.type === 'subscribed') return;

    // l2_updates from V2 marketdata: trades embedded in data.trades
    const trades = collectTrades(data);
    for (const t of trades) {
      const price = toNumber(t.p ?? t.price ?? (Array.isArray(t) ? t[1] : null));
      const qty = toNumber(t.q ?? t.amount ?? t.size ?? (Array.isArray(t) ? t[2] : null));
      const side = normalizeSide(t);
      const ts = normalizeTs(t.E ?? t.timestamp ?? t.time ?? data.timestamp ?? data.time ?? (Array.isArray(t) ? t[3] : null));
      const tradeId = String(t.tid ?? t.trade_id ?? t.tradeId ?? `${ts}-${price}-${qty}`);
      if (price == null || qty == null || !side) continue;
      this._emitTrade(price, qty, side, ts, tradeId);
    }
  }
}
