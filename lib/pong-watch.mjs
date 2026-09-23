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

/**
 * この接続を「pong 途絶」と見なして張り直すべきか。
 *
 * 実測 (2026-09-23 23:38): 接続をまたいだ記録を使うと誤爆する。新しく張った接続
 * (pings=3 pongs=2) に対し、5分前の旧接続の lastPong を当てて「308秒途絶」と判定した。
 * そのため判定材料は「いまの接続」に限定する:
 *   - いまの接続で pong を1回以上受けている (旧接続の pong では判定しない)
 *   - いまの接続がしきい値以上生きている (開いた直後を判定しない)
 *   - 最後の pong からの経過がしきい値以上
 * @param {{pongAgeMs:number|null, pongsReceived:number, pingsSent:number}} liveness
 * @param {{pongsAtConnectionOpen?:number, connectionAgeMs?:number, maxAgeMs?:number}} [opts]
 */
export function isPongStarved(liveness, opts = {}) {
  const raw = opts.maxAgeMs;
  const maxAgeMs = Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_PONG_SILENCE_MS;
  const pongs = Number(liveness?.pongsReceived);
  if (!Number.isFinite(pongs) || pongs <= 0) return false; // 一度も pong が無い venue は判定しない
  if (!(Number(liveness?.pingsSent) > 0)) return false;   // ping を送っていない = 材料が無い
  // いまの接続で pong を受けたか (旧接続の pong を根拠にしない)
  const atOpen = opts.pongsAtConnectionOpen;
  if (Number.isFinite(atOpen) && pongs <= Number(atOpen)) return false;
  // 接続した直後は判定しない
  const connAge = Number(opts.connectionAgeMs);
  if (Number.isFinite(connAge) && connAge < maxAgeMs) return false;
  const age = liveness?.pongAgeMs;
  if (age === null || age === undefined) return false;
  const n = Number(age);
  if (!Number.isFinite(n)) return false;
  return n >= maxAgeMs;
}
