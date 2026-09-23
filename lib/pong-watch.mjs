/**
 * Pong 途絶の判定 (2026-09-23)。
 *
 * 実測 (24時間の journal): Binance系4市場は切断時に必ず「7回連続 pong 未応答」
 * (lastPong 約35秒前) を示し、Bitfinex / Kraken は 0〜1 回しか未応答にならない。
 * = Binance の停止はデータ停止ではなく経路/ソケットの無音死で、ping/pong の方が
 * データ無音より約20秒早く気づける。
 *
 * 誤再接続を避けるため、判定は保守的にする:
 *  - この接続で pong を1回でも受け取っていること (protocol ping に応答しない venue を除外)
 *  - 未応答が limit 回以上 (既定3 = 15秒。実測の正常時最大は1回なので余裕がある)
 */

/** 未応答の数 (送った ping - 返ってきた pong)。負にはしない。 */
export function unansweredPings({ pingsSent, pongsReceived }) {
  const deficit = Number(pingsSent) - Number(pongsReceived);
  return Number.isFinite(deficit) && deficit > 0 ? deficit : 0;
}

/**
 * この接続を「pong 途絶」と見なして張り直すべきか。
 * @param {{pingsSent:number, pongsReceived:number}} liveness
 * @param {{limit?:number, pongsEverReceived?:boolean}} [opts]
 */
export function isPongStarved(liveness, opts = {}) {
  const limit = Number.isFinite(opts.limit) && opts.limit > 0 ? opts.limit : 3;
  // 一度も pong が返っていない venue では判定しない (protocol ping 非対応の可能性)。
  const everReceived = opts.pongsEverReceived ?? (Number(liveness?.pongsReceived) > 0);
  if (!everReceived) return false;
  return unansweredPings(liveness ?? {}) >= limit;
}
