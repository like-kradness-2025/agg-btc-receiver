/**
 * 再接続の待ち時間と、試行回数のリセット条件 (2026-09-24)。
 *
 * 実測 (2026-09-23 23:30〜23:38): 同期失敗のたびに WS を作り直すループで
 * binance_perp が約5分止まった。`open` で試行回数を 0 に戻していたため、
 * 指数バックオフと上限 (30回) が実質無効だった（待ちが毎回1〜2秒に戻る）。
 *
 * 方針 (Astra 承認済みの計画 C):
 *  - 試行回数は open で戻さない。**安定稼働を確認して初めて**戻す。
 *  - 連続失敗は上限つき指数バックオフ＋jitter。
 */

/** 安定とみなすまでの時間 (ms)。Astra 計画の例示 (同期成功後60秒)。 */
export const STABILITY_MS = 60000;

/**
 * 次の再接続までの待ち (ms)。attempt は 1 始まり。
 * @param {number} attempt
 * @param {{baseMs?:number, maxMs?:number, jitterMs?:number, random?:() => number}} [opts]
 */
export function backoffDelayMs(attempt, opts = {}) {
  const baseMs = Number.isFinite(opts.baseMs) && opts.baseMs > 0 ? opts.baseMs : 1000;
  const maxMs = Number.isFinite(opts.maxMs) && opts.maxMs > 0 ? opts.maxMs : 30000;
  const jitterMs = Number.isFinite(opts.jitterMs) && opts.jitterMs >= 0 ? opts.jitterMs : 1000;
  const random = typeof opts.random === 'function' ? opts.random : Math.random;
  const n = Number.isFinite(attempt) && attempt > 0 ? Math.floor(attempt) : 1;
  const exponential = baseMs * Math.pow(2, n - 1);
  const capped = Math.min(exponential, maxMs);
  return Math.round(capped + random() * jitterMs);
}

/** 試行回数をリセットしてよいか (安定稼働を確認したか)。 */
export function shouldResetAttempts(stableForMs, opts = {}) {
  const stableMs = Number.isFinite(opts.stableMs) && opts.stableMs > 0 ? opts.stableMs : STABILITY_MS;
  const n = Number(stableForMs);
  if (!Number.isFinite(n)) return false;
  return n >= stableMs;
}
