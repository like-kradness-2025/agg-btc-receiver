/**
 * Pong 途絶の判定 (2026-09-23)。
 *
 * 実測 (journal):
 *  - 本物の停止: Binance系4市場で pings=749 pongs=742 **lastPong=34914ms ago**
 *    → データ無音(30秒)より前に ping への応答が途絶えている = 経路/ソケットの無音死。
 *  - 正常時: lastPong は数秒 (実測 0.3〜6.0秒)。pongs は返り続けている。
 *
 * 判定は「最後の pong からの経過時間」で行う。未応答の“回数”で判定すると、pong が
 * 遅れて返るだけで未応答が2〜3に達し、誤再接続が多発することを実機で確認した
 * (2026-09-23 20:0x〜20:4x に66回。lastPong は 2.8〜6.0秒 = 生きていた)。
 */

/** 既定の途絶しきい値: ping間隔5秒の3回分。実測の正常時最大(約6秒)より十分に大きい。 */
export const DEFAULT_PONG_SILENCE_MS = 15000;

/** 未応答の数 (送った ping - 返ってきた pong)。ログの補助情報としてのみ使う。 */
export function unansweredPings({ pingsSent, pongsReceived }) {
  const deficit = Number(pingsSent) - Number(pongsReceived);
  return Number.isFinite(deficit) && deficit > 0 ? deficit : 0;
}

/**
 * この接続を「pong 途絶」と見なして張り直すべきか。
 * @param {{pongAgeMs:number|null, pongsReceived:number, pingsSent:number}} liveness
 * @param {{maxAgeMs?:number, pongsEverReceived?:boolean}} [opts]
 */
export function isPongStarved(liveness, opts = {}) {
  const raw = opts.maxAgeMs;
  const maxAgeMs = Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_PONG_SILENCE_MS;
  // この接続で pong を一度も受けていない venue は判定しない (protocol ping 非対応の可能性)。
  const everReceived = opts.pongsEverReceived ?? (Number(liveness?.pongsReceived) > 0);
  if (!everReceived) return false;
  // pings を1回も送っていない = 判定材料が無い。
  if (!(Number(liveness?.pingsSent) > 0)) return false;
  const age = liveness?.pongAgeMs;
  if (age === null || age === undefined) return false; // まだ pong が無い
  const n = Number(age);
  if (!Number.isFinite(n)) return false;
  return n >= maxAgeMs;
}
