/**
 * 同期だけを再試行してよいかの判定 (2026-09-24 / 計画 B、sol 監査の指摘反映)。
 *
 * 目的: 2026-09-23 23:30〜23:38 に `init sync failed after 3 retries` のたび WS を作り直し、
 * binance_perp が約5分停止した。WS が生きていて購読も成立しているなら、WS は捨てずに
 * REST 同期だけを上限つきで再試行する。作り直すのは次の4条件のいずれかのときだけ:
 *   1. ソケットが生きていない
 *   2. バッファが溢れた (順序を保証できない)
 *   3. 購読が成立していない (ack 失敗 or 未 ack)
 *   4. 同期再試行の上限に到達した (WS再接続とは別の上限)
 *
 * 購読の扱い (sol の指摘): subscribe-tracker は現状「観測用」で、全コネクタが購読を記録して
 * いるわけではない。そこで:
 *   - 'unacked' / 'failed' → 成立していないとみなし、WS を作り直す
 *   - 'none-recorded'      → 記録が無いので判断材料にしない (再試行を許可し、記録が無いことをログに残す)
 *
 * 前提条件 (sol P1②): この受信プロセスは「同一ソケットで購読をリセットして再購読」する経路を持たない
 * (再購読は常に新しいソケットで行う)。将来それを入れる場合は、購読リセットの時点で旧同期を失効させ、
 * 再 ack を確認したあとでだけこの再試行を許可すること。
 */

/** REST 同期だけの再試行上限 (WS 再接続の上限 30 とは別物)。 */
export const SYNC_RETRY_MAX = 5;
export const SYNC_RETRY_BASE_MS = 1000;
export const SYNC_RETRY_MAX_MS = 30000;

/**
 * 同じソケットで REST 同期だけを再試行してよいか。
 * @param {{socketOpen:boolean, subscriptionState?:string, bufferOverflow?:boolean,
 *          retryCount?:number, maxRetries?:number}} state
 * @returns {{retry:boolean, reason:string}}
 */
export function canRetrySyncOnSocket(state = {}) {
  const maxRetries = Number.isFinite(state.maxRetries) && state.maxRetries > 0
    ? state.maxRetries : SYNC_RETRY_MAX;
  if (state.socketOpen !== true) return { retry: false, reason: 'socket-closed' };
  if (state.bufferOverflow === true) return { retry: false, reason: 'buffer-overflow' };
  const subs = state.subscriptionState;
  if (subs === 'failed' || subs === 'unacked') return { retry: false, reason: 'subscription-not-established' };
  const count = Number.isFinite(state.retryCount) && state.retryCount > 0 ? state.retryCount : 0;
  if (count >= maxRetries) return { retry: false, reason: 'retry-cap' };
  if (subs === 'none-recorded' || subs === undefined) return { retry: true, reason: 'ok-no-subscription-record' };
  return { retry: true, reason: 'ok' };
}

/**
 * REST 同期だけの待機 (ms)。WS 再接続のバックオフとは別に、素直な指数で上限 30 秒。
 * @param {number} retryCount 1 始まり
 * @param {{baseMs?:number, maxMs?:number, random?:() => number, jitterMs?:number}} [opts]
 */
export function syncRetryDelayMs(retryCount, opts = {}) {
  const baseMs = Number.isFinite(opts.baseMs) && opts.baseMs > 0 ? opts.baseMs : SYNC_RETRY_BASE_MS;
  const maxMs = Number.isFinite(opts.maxMs) && opts.maxMs > 0 ? opts.maxMs : SYNC_RETRY_MAX_MS;
  const jitterMs = Number.isFinite(opts.jitterMs) && opts.jitterMs >= 0 ? opts.jitterMs : 250;
  const random = typeof opts.random === 'function' ? opts.random : Math.random;
  const n = Number.isFinite(retryCount) && retryCount > 0 ? Math.floor(retryCount) : 1;
  const capped = Math.min(baseMs * Math.pow(2, n - 1), maxMs);
  return Math.round(capped + random() * jitterMs);
}

/** subscribe-tracker の1行表現 ('subs=ok' / 'subs=book:failed' / 'subs=trades:no-ack') を状態に変換する。 */
export function subscriptionStateFromText(text) {
  const t = String(text ?? '');
  if (t === '' || t === 'subs=ok') return 'ok';
  if (t.includes(':failed')) return 'failed';
  if (t.includes(':no-ack')) return 'unacked';
  return 'unknown';
}
