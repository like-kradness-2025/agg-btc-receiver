// lib/coinbase-connector.mjs — Coinbase spot connector for btc-receiver v3.00
// Uses Coinbase Advanced Trade WebSocket (public, no auth required for level2 / market_trades)

import { BaseConnector } from './base-connector.mjs';
import { FullBook } from './full-book.mjs';

const WS_SNAPSHOT_TIMEOUT_MS = 15000;
// Issue #14: max number of recently emitted trade_ids retained for reconnect
// dedupe. Reconnect market_trades snapshots re-deliver only the trades near
// the disconnect moment, so an order-agnostic bounded recency window (Map
// keeps insertion order) proves "already emitted by a previous connection".
const TRADE_ID_WINDOW_MAX = 8192;

const toPairs = (rows = []) => rows.map(([p, q]) => [String(p), String(q)]);

export class CoinbaseConnector extends BaseConnector {
  constructor(config) {
    super(config, {
      market: 'coinbase_spot',
      wsUrl: config.wsUrl || 'wss://advanced-trade-ws.coinbase.com',
      restUrl: config.restUrl || 'https://api.exchange.coinbase.com/products/BTC-USD/book?level=3',
    });
    /** @type {FullBook} */
    this.book = new FullBook('coinbase_spot', { maxLevels: config.depthLimit ?? 0 });

    // Issue #14: L2 sequence continuity state (per connection generation).
    // `_l2BridgePending` is true until the FIRST l2_data update frame is
    // applied after a snapshot. The snapshot frame's sequence_num and the
    // update stream's sequence_num cannot be proven to share one counter
    // (Coinbase can deliver updates whose seq is already ahead of the
    // snapshot frame's seq), so the snapshot→first-update bridge accepts any
    // strictly-newer seq once. Every LATER update frame must be exactly
    // localSeq + 1; any other jump is a dropped-message gap → fail-closed
    // resync. This is the explicit bridge/steady-state split #14 requires.
    this._l2BridgePending = true;

    // Issue #14 trade idempotency (normalized layer, per process): bounded
    // recency window of emitted trade_ids. Reset only at construction — never
    // on reconnect — so a reconnect snapshot's overlap with the previous
    // connection is provably a duplicate.
    /** @type {Map<string, true>} */ this._tradeIdWindow = new Map();

    this._stats.dedupedTradeCount = 0;
    this._stats.l2SeqGapCount = 0;
    this._stats.l2DupDropCount = 0;
  }

  subscribe() {
    // Advanced Trade WS: send two individual subscribe frames
    // (channel as string, not array — array form caused auth failure)
    this._ws.send(JSON.stringify({
      type: 'subscribe',
      product_ids: ['BTC-USD'],
      channel: 'level2',
    }));
    this._ws.send(JSON.stringify({
      type: 'subscribe',
      product_ids: ['BTC-USD'],
      channel: 'market_trades',
    }));
  }

  _onMessage(data) {
    // Handle error messages (old Exchange endpoint sent level2-auth errors)
    if (data.type === 'error') {
      this.emit('error', { market: this.market, message: `WS error: ${data.message || data.reason || JSON.stringify(data)}` });
      return;
    }
    // Subscription ack
    if (data.channel === 'subscriptions') return;

    if (data.channel === 'l2_data') {
      this._handleDepth(data);
    } else if (data.channel === 'market_trades') {
      this._handleTrade(data);
    }
  }

  _handleDepth(data) {
    const seq = data.sequence_num != null ? data.sequence_num : null;
    // Advanced Trade l2_data has no top-level timestamp: ts is the local
    // processing time (unchanged semantics). Issue #12 marks it explicitly as
    // NOT an exchange event time via source_event_ts_ms:null.
    const ts = Date.now();
    const localTimeMeta = {
      source_event_ts_ms: null,
      source_event_time_known: false,
      event_time_source: 'local',
    };

    const events = data.events || [];
    for (const event of events) {
      if (event.type === 'snapshot') {
        const updates = event.updates || [];
        const bids = [];
        const asks = [];
        for (const u of updates) {
          if (u.side === 'bid') {
            bids.push([String(u.price_level), String(u.new_quantity)]);
          } else if (u.side === 'ask' || u.side === 'offer') {
            asks.push([String(u.price_level), String(u.new_quantity)]);
          }
        }

        if (!this._emitDepth('snapshot', bids, asks, ts, seq, { snapshot_origin: 'ws_sync', ...localTimeMeta })) return;
        this.book.applySnapshot(bids, asks, seq);
        this._notifyWsSnapshotReceived(seq);
        this._firstRunningDepth = true;
        // Issue #14: fresh snapshot = fresh bridge. The first post-snapshot
        // update frame may carry a seq that is not snapshotSeq + 1 (Coinbase
        // delivers updates whose stream position can already be ahead of the
        // snapshot frame); it is accepted once, then continuity is strict.
        this._l2BridgePending = true;
        this._replayRingBufAfterSnapshot();
        this._ringBuf = [];
        return;
      }

      if (event.type === 'update') {
        if (!this._wsSnapshotReceived) {
          this._bufferMsg(data);
          return;
        }
        if (this._state === 'reconnecting') return;

        // Issue #14: fail-closed L2 sequence continuity. Verdict BEFORE any
        // book mutation so a gapped frame can never pollute book state:
        //   dup             → already-applied frame → drop (counted)
        //   gap             → dropped messages between localSeq and seq →
        //                      resync through the same snapshot path
        //   unverifiable    → frame without sequence_num → continuity cannot
        //                      be proven → resync (never fail-open apply)
        const localSeq = this.book._lastSeq;
        const verdict = this._l2Continuity(seq, localSeq);
        if (verdict === 'dup') {
          this._stats.l2DupDropCount++;
          return;
        }
        if (verdict !== 'ok') {
          this._stats.l2SeqGapCount++;
          this._handleSequenceGap(
            verdict === 'gap'
              ? `coinbase l2 sequence gap: ${localSeq} -> ${seq}`
              : 'coinbase l2 update without sequence_num (continuity unverifiable)',
            data);
          return;
        }

        const updates = event.updates || [];
        const bids = [];
        const asks = [];
        for (const u of updates) {
          if (u.side === 'bid') {
            bids.push([String(u.price_level), String(u.new_quantity)]);
          } else if (u.side === 'ask' || u.side === 'offer') {
            asks.push([String(u.price_level), String(u.new_quantity)]);
          }
        }

        if (!this._emitDepth('update', bids, asks, ts, seq, { prev_seq: localSeq, ...localTimeMeta })) return;
        for (const [p, q] of bids) this.book.applyDiff('bid', p, q, seq);
        for (const [p, q] of asks) this.book.applyDiff('ask', p, q, seq);
        if (seq != null) this.book.setLastSeq(seq);
        this._l2BridgePending = false;
      }
    }
  }

  _handleTrade(data) {
    // Advanced Trade market_trades format. Each event is a 'snapshot' (the
    // recent-trade window re-sent on every (re)subscribe) or an 'update'
    // (live stream). Issue #14: snapshot trades are re-delivered across
    // reconnects and must not double-count downstream (CVD).
    const events = data.events || [];
    for (const event of events) {
      const eventType = event.type === 'snapshot' ? 'snapshot' : 'update';
      const trades = event.trades || [];
      for (const t of trades) {
        const parsedTime = Date.parse(t.time);
        const tradeId = String(t.trade_id || '');
        // Idempotency policy (normalized layer): exchange+product scoped
        // identity = (market, trade_id). trade_id strictly increases per
        // product, so a single max cursor proves "already emitted by an
        // earlier connection". Raw/source frames are untouched by this check
        // (canonical raw layer keeps duplicates when it lands, #10/#11); the
        // connector's emitted trade stream is the idempotent normalized view.
        if (tradeId !== '' && this._isDupTradeId(tradeId)) {
          this._stats.dedupedTradeCount++;
          // Issue #19 (Astra P1): a duplicate is still a RECENT emission —
          // refresh its position in the recency window (tail re-insert) so a
          // window overflow can never evict a trade_id that the next
          // reconnect snapshot may re-deliver again. Without the refresh the
          // duplicate branch's early continue leaves the id at the head, it
          // gets evicted on the next overflow, and the following re-delivery
          // is misclassified as new (CVD double count).
          this._rememberTradeId(tradeId);
          continue;
        }
        // Issue #12: Date.parse() must never fall back to receive time —
        // an unparseable exchange time is unknown (null → fail-closed drop).
        const emitted = this._emitTrade(
          parseFloat(t.price),
          parseFloat(t.size),
          t.side === 'SELL' ? 'buy' : 'sell',
          Number.isFinite(parsedTime) ? parsedTime : null,
          tradeId,
          null,
          { trade_event_type: eventType }
        );
        if (emitted && tradeId !== '') this._rememberTradeId(tradeId);
      }
    }
  }

  /** Issue #14: has this normalized trade_id already been emitted? */
  _isDupTradeId(tradeId) {
    return this._tradeIdWindow.has(tradeId);
  }

  /** Issue #14: record an emitted trade_id in the bounded recency window. */
  _rememberTradeId(tradeId) {
    const window = this._tradeIdWindow;
    if (window.has(tradeId)) {
      // Re-insert to the tail: recency, not first-seen order, decides eviction.
      window.delete(tradeId);
    } else if (window.size >= TRADE_ID_WINDOW_MAX) {
      const oldest = window.keys().next().value;
      if (oldest !== undefined) window.delete(oldest);
    }
    window.set(tradeId, true);
  }

  /**
   * Issue #14 continuity verdict for one l2_data update frame.
   * Domain (per official Advanced Trade docs and live-frame shape): the
   * sequence_num on l2_data counts that channel's own frames; market_trades
   * frames are a separate channel and are never part of this comparison.
   * Within one connection the update frames must be consecutive
   * (localSeq + 1); a larger jump means frames were dropped between the two
   * received frames → 'gap'. The single frame directly after a snapshot is
   * the unprovable bridge (snapshot-frame seq vs update-stream seq cannot be
   * proven to share a counter) → any strictly-newer seq is accepted once.
   * @param {number|null} frameSeq
   * @param {number|null} localSeq - book._lastSeq (last APPLIED frame seq)
   * @returns {'ok'|'dup'|'gap'|'unverifiable'}
   */
  _l2Continuity(frameSeq, localSeq) {
    if (frameSeq == null) return 'unverifiable';
    if (localSeq == null) return 'ok'; // no anchor yet: frame becomes the anchor
    if (this._l2BridgePending) return frameSeq > localSeq ? 'ok' : 'dup';
    if (frameSeq === localSeq + 1) return 'ok';
    if (frameSeq <= localSeq) return 'dup';
    return 'gap';
  }

  /** Replay ring buffer l2_data updates after snapshot (Issue #14: the same
   * continuity invariant applies — buffered frames are consecutive stream
   * frames, so any jump between adjacent buffered frames is a socket-level
   * drop and must fail closed instead of silently drifting the book). */
  _replayRingBufAfterSnapshot() {
    const localTimeMeta = {
      source_event_ts_ms: null,
      source_event_time_known: false,
      event_time_source: 'local',
    };
    for (const msg of this._ringBuf) {
      const msgSeq = msg.sequence_num != null ? msg.sequence_num : null;
      const localSeq = this.book._lastSeq;
      const verdict = this._l2Continuity(msgSeq, localSeq);
      if (verdict === 'dup') {
        this._stats.l2DupDropCount++;
        continue;
      }
      if (verdict !== 'ok') {
        this._stats.l2SeqGapCount++;
        this._handleSequenceGap(
          verdict === 'gap'
            ? `coinbase l2 replay sequence gap: ${localSeq} -> ${msgSeq}`
            : 'coinbase l2 replay frame without sequence_num (continuity unverifiable)',
          msg);
        return;
      }
      const events = msg.events || [];
      for (const event of events) {
        if (event.type !== 'update') continue;
        const updates = event.updates || [];
        const bids = [];
        const asks = [];
        for (const u of updates) {
          if (u.side === 'bid') bids.push([String(u.price_level), String(u.new_quantity)]);
          else if (u.side === 'ask' || u.side === 'offer') asks.push([String(u.price_level), String(u.new_quantity)]);
        }
        this._emitDepth('update', bids, asks, Date.now(), msgSeq, { prev_seq: localSeq, ...localTimeMeta }, msg._ingress ?? null);
        for (const u of updates) {
          const qty = u.new_quantity;
          const price = u.price_level;
          if (u.side === 'bid') {
            this.book.applyDiff('bid', String(price), String(qty));
          } else if (u.side === 'ask' || u.side === 'offer') {
            this.book.applyDiff('ask', String(price), String(qty));
          }
        }
      }
      if (msgSeq != null) this.book.setLastSeq(msgSeq);
      this._l2BridgePending = false;
    }
  }

  _resetBook() {
    this._l2BridgePending = true;
    super._resetBook();
  }

  async _syncBook() {
    this._setState('syncing');

    if (this._wsSnapshotReceived) {
      this._finalizeWsSnapshotSync();
      return;
    }

    // Issue #19 (Astra P1): identity of THIS sync attempt. The WS-snapshot
    // waiter resolves the instant the snapshot frame arrives, but snapshot
    // processing continues synchronously AFTER that resolution — a replay
    // sequence gap (or a concurrent reconnect) can clear the book and move
    // the machine to 'reconnecting' before the resumed _syncBook gets to
    // finalize. The connection generation lets the resumed attempt tell
    // "my sync" from a superseding one.
    const syncGeneration = this._wsGeneration;
    this._beginWsSnapshotSync();

    try {
      await this._waitForWsSnapshot(WS_SNAPSHOT_TIMEOUT_MS, 'ws snapshot timeout');
    } catch (err) {
      if (err?.code === 'WS_SNAPSHOT_ABORTED') return;

      let restored = false;
      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          const snapshot = await this._fetchSnapshot();
          // Issue #19 (Astra P1): superseded mid-fetch — a reconnect took
          // over while this REST call was in flight. Never apply this
          // attempt's snapshot into the newer connection's book/sync.
          if (this._wsGeneration !== syncGeneration) return;
          // Coinbase REST returns {bids: [[price, qty, orderCount], ...], asks: [[price, qty, orderCount], ...], sequence}
          if (!this._validateSnapshotLevels(snapshot)) throw new Error('invalid REST depth snapshot');
          const bids = toPairs(snapshot.bids || []);
          const asks = toPairs(snapshot.asks || []);
          // Coinbase Exchange REST and Advanced Trade WS use different
          // sequence domains.  The REST value must not anchor the WS stream;
          // keep this fallback snapshot explicitly unsequenced so the next
          // Advanced Trade snapshot can establish its own cursor.
          const seq = null;
          this.book.applySnapshot(bids, asks, seq);
          this._emitDepth('snapshot', bids, asks, Date.now(), seq, {
            snapshot_origin: 'rest_fallback',
            source_event_ts_ms: null,
            source_event_time_known: false,
            event_time_source: 'rest_snapshot',
          }, this._localCapture());
          this._notifyWsSnapshotReceived(seq);
          restored = true;
          break;
        } catch (restErr) {
          this.emit('error', { market: this.market, message: `sync REST fallback attempt ${attempt} failed: ${restErr.message}` });
        }
      }

      if (!restored) {
        this._failWsSnapshotSync('init sync failed after 3 retries');
        throw new Error(`init sync failed for ${this.market} after 3 retries`);
      }
    }

    // Issue #19 (Astra P1): success is only real when the sync actually
    // completed — a snapshot was received AND nothing derailed the sync
    // while _syncBook was awaiting. The waiter resolves the moment the
    // snapshot frame arrives, but the replay runs synchronously right after
    // it: a buffered-frame sequence gap calls _handleSequenceGap, which
    // clears the book, resets _wsSnapshotReceived and moves to
    // 'reconnecting' with a reconnect armed. The resumed _syncBook must not
    // clobber that back to 'running' — reporting a failed sync as success
    // leaves the connector 'running' with an empty book and no snapshot
    // (same pattern as the PR #17 OKX fix). The generation check covers a
    // REST-fallback restore that landed after a mid-sync reconnect/error
    // took over — leave the state machine to the reconnect/error machinery.
    if (this._state === 'syncing' && this._wsSnapshotReceived && this._wsGeneration === syncGeneration) {
      this._finalizeWsSnapshotSync();
    }
  }

  async _fetchSnapshot() {
    const res = await fetch(this.restUrl, {
      headers: { 'User-Agent': 'btc-receiver/v3.00' },
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) throw new Error(`REST snapshot ${res.status}`);
    return res.json();
  }

}
