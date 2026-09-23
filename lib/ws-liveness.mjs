/**
 * WS 接続の生存確認 (制御 Ping/Pong) の記録。
 *
 * 目的 (2026-09-23 実測): 板イベントが約30秒止まり、その間の raw が復元できずに欠落する。
 * これが「こちらのソケットが無音で死んでいる」のか「取引所の配信が止まっている」のかを、
 * 次の再接続時に確定させるための観測。**再接続の判断には使わない (挙動を変えない)**。
 *
 * 判定: 板が止まっている間に Pong が返っていれば配信/購読側、
 *       Pong も途絶えていれば接続経路側 (ソケット/Pong のどちらも来ない)。
 */
export function createWsLiveness({ now = () => Date.now() } = {}) {
  let pingsSent = 0;
  let pongsReceived = 0;
  let lastPingAt = null;
  let lastPongAt = null;
  return {
    onPingSent() {
      pingsSent += 1;
      lastPingAt = now();
    },
    onPong() {
      pongsReceived += 1;
      lastPongAt = now();
    },
    get pingsSent() { return pingsSent; },
    get pongsReceived() { return pongsReceived; },
    get lastPingAt() { return lastPingAt; },
    get lastPongAt() { return lastPongAt; },
    /** 最後の Pong からの経過ms。一度も Pong が無ければ null。 */
    pongAgeMs() { return lastPongAt === null ? null : now() - lastPongAt; },
    /** 再接続ログに載せる短い表現。 */
    text() {
      const age = this.pongAgeMs();
      return `pings=${pingsSent} pongs=${pongsReceived} lastPong=${age === null ? 'none' : `${age}ms ago`}`;
    },
  };
}
