/**
 * チャンネル単位の配信停止の判定 (2026-09-23, Astra 指摘の反映)。
 *
 * 背景: 無音検知は「任意のメッセージ受信」(_lastMsgAt) で更新されるため、heartbeat や
 * 一部チャンネルだけが生きている状態では trades/板の停止を検知できない。各チャンネルの
 * 最終データ時刻から停滞を判定する。
 *
 * 注意: ここは観測のみ。再接続や再購読の判断に使う前に、まず実測で頻度と効果を確かめる。
 */
export function channelSilenceReport({ depthAgeMs = null, tradeAgeMs = null, limitMs, limitText = 'channel_silence' } = {}) {
  if (!Number.isFinite(limitMs)) throw new TypeError('limitMs must be a finite number');
  const stale = [];
  if (depthAgeMs !== null && depthAgeMs > limitMs) stale.push({ channel: 'depth', ageMs: Math.round(depthAgeMs) });
  if (tradeAgeMs !== null && tradeAgeMs > limitMs) stale.push({ channel: 'trades', ageMs: Math.round(tradeAgeMs) });
  const ages = {
    ...(depthAgeMs === null ? {} : { depth_ms: Math.round(depthAgeMs) }),
    ...(tradeAgeMs === null ? {} : { trades_ms: Math.round(tradeAgeMs) }),
  };
  return {
    stale,
    ages,
    staleChannels: stale.map((s) => s.channel),
    text() {
      if (stale.length === 0) return `${limitText}=ok`;
      return `${limitText}=${stale.map((s) => `${s.channel}:${s.ageMs}ms`).join(',')}`;
    },
  };
}

/** 最終データ時刻(ms, 0は未受信)から経過msを出す。未受信は null (= 判定対象外)。 */
export function ageFrom(lastAtMs, nowMs) {
  if (!Number.isFinite(lastAtMs) || lastAtMs <= 0) return null;
  return Math.max(0, nowMs - lastAtMs);
}
