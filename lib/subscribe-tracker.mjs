/**
 * 購読の成立追跡 (2026-09-23, Astra 監査の M1/M4 反映)。
 *
 * 背景: 接続は成立しても購読が失敗/未成立だと、そのチャンネルだけ永久に欠ける。
 * Bybit は ack を成功・失敗とも無視し、Bitfinex は error を無視しているため、
 * 「pong は返るが配信は無い」状態を生存と誤認し得る。
 *
 * ここは記録のみ。再購読や再接続の判断に使うのは、旧購読の解除と ack 確認が
 * 実装できてから (M1)。
 */
export function createSubscribeTracker({ now = () => Date.now() } = {}) {
  const entries = new Map(); // channel -> { requestedAt, ackedAt, failedAt, reason }
  const touch = (channel) => {
    if (!entries.has(channel)) entries.set(channel, { requestedAt: null, ackedAt: null, failedAt: null, reason: null });
    return entries.get(channel);
  };
  return {
    /** 購読要求を出した時刻を記録する。 */
    requested(channel) {
      const e = touch(channel);
      e.requestedAt = now();
      e.failedAt = null;
      e.reason = null;
    },
    /** 購読成立 (成功ack) を記録する。 */
    acked(channel) {
      const e = touch(channel);
      e.ackedAt = now();
      e.failedAt = null;
      e.reason = null;
    },
    /** 購読失敗 (error/失敗ack) を記録する。 */
    failed(channel, reason = null) {
      const e = touch(channel);
      e.failedAt = now();
      e.reason = reason;
    },
    /** 期限内に ack が来ていない購読 (未成立の疑い)。 */
    unacked(deadlineMs) {
      const t = now();
      const out = [];
      for (const [channel, e] of entries) {
        if (e.requestedAt === null || e.ackedAt !== null) continue;
        const age = t - e.requestedAt;
        if (age > deadlineMs) out.push({ channel, ageMs: age, failed: e.failedAt !== null, reason: e.reason });
      }
      return out;
    },
    /** 失敗が記録されている購読 (ack で明示的に拒否されたもの)。 */
    failedChannels() {
      const out = [];
      for (const [channel, e] of entries) {
        if (e.failedAt !== null) out.push({ channel, reason: e.reason });
      }
      return out;
    },
    /** ログ/status 用の短い表現 (正常時は ok)。 */
    text(deadlineMs) {
      const bad = [...this.unacked(deadlineMs).map((u) => `${u.channel}:${u.failed ? 'failed' : 'no-ack'}`),
        ...this.failedChannels().map((f) => `${f.channel}:failed`)];
      const uniq = [...new Set(bad)];
      return uniq.length === 0 ? 'subs=ok' : `subs=${uniq.join(',')}`;
    },
    size() { return entries.size; },
  };
}
