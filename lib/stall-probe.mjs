/**
 * stall-probe — プロセスの「一時的なフリーズ（ブロック）」を観測するための計測器。
 *
 * 背景 (2026-09-21): agg-btc-receiver は 24 時間に約 9 回、複数市場が同時に
 * `no message for ~30s` で再接続し、その都度 raw に 30〜37 秒の欠落が出ていた。
 * 別プロセスの疎通監視 (ICMP/TCP/DNS) は全事象で正常だったため回線は原因ではなく、
 * 一方で receiver 自身の `health.jsonl` (1 秒刻み) には最大 19.3 秒の穴があった
 * (4 時間で 30 件・計 175 秒)。つまり **プロセスが数秒〜20 秒ブロックしている**。
 *
 * ただし「どこで止まっているか」は既存ログからは分からない（停止中は無言のため）。
 * この計測器は **挙動を一切変えずに** 次を記録する:
 *   - イベントループの遅延 (サンプリング間隔のずれ)
 *   - 遅延の前後で実行されていた処理のスパン (処理名・所要時間・詳細)
 *   - ゲージ値 (キュー深さ等) の推移
 *
 * 記録は **異常時のみ**（通常時はファイルに一切書かない）。停止中は当然書けないので、
 * 復帰直後の最初のサンプルで「さっき何 ms 止まっていたか」と「その間に実行中だった処理」
 * をまとめて出力する。
 */

const DEFAULT_LAG_THRESHOLD_MS = 1500;
const DEFAULT_SAMPLE_MS = 250;
const DEFAULT_RING_SIZE = 400;
const DEFAULT_RECOVERY_STREAK = 2;

function toIso(ms) {
  return new Date(ms).toISOString();
}

export class StallProbe {
  #label;
  #lagThresholdMs;
  #sampleMs;
  #ringSize;
  #recoveryStreak;
  #minSpanMs;
  #onAnomaly;
  #now;
  #timer = null;
  #expectedAt = 0;
  #lastTickAt = 0;
  #lagSamples = [];
  #spans = [];
  #gauges = [];
  #event = null;
  #quietTicks = 0;
  #stats = { samples: 0, events: 0, maxLagMs: 0, spansRecorded: 0 };

  /**
   * @param {object} options
   * @param {string} options.label            記録に載せる主体名 ('main' / 'worker:A' 等)
   * @param {number} [options.lagThresholdMs] これを超える遅延で異常とみなす
   * @param {number} [options.sampleMs]       サンプリング間隔
   * @param {number} [options.ringSize]       スパン/ゲージの保持件数
   * @param {number} [options.recoveryStreak] 連続何サンプル正常なら復帰とみなすか
   * @param {(record: object) => void} [options.onAnomaly] 記録の受け取り先
   * @param {() => number} [options.now]      時刻源 (テスト用)
   */
  constructor({
    label,
    lagThresholdMs = DEFAULT_LAG_THRESHOLD_MS,
    sampleMs = DEFAULT_SAMPLE_MS,
    ringSize = DEFAULT_RING_SIZE,
    recoveryStreak = DEFAULT_RECOVERY_STREAK,
    minSpanMs = 0,
    onAnomaly = () => {},
    // 既定は単調クロック基準 (NTP の時刻補正で遅延計測が歪まないように)。
    now = () => Math.round(performance.timeOrigin + performance.now()),
  } = {}) {
    if (!label) throw new TypeError('stall probe requires a label');
    this.#label = label;
    this.#lagThresholdMs = Math.max(1, Number(lagThresholdMs));
    this.#sampleMs = Math.max(50, Number(sampleMs));
    this.#ringSize = Math.max(10, Number(ringSize));
    this.#recoveryStreak = Math.max(1, Number(recoveryStreak));
    this.#minSpanMs = Math.max(0, Number(minSpanMs) || 0);
    this.#onAnomaly = onAnomaly;
    this.#now = now;
  }

  get label() { return this.#label; }
  get running() { return this.#timer !== null; }
  stats() {
    return {
      ...this.#stats,
      lagThresholdMs: this.#lagThresholdMs,
      sampleMs: this.#sampleMs,
      spansHeld: this.#spans.length,
      minSpanMs: this.#minSpanMs,
      inEvent: this.#event !== null,
    };
  }

  start() {
    if (this.#timer) return this;
    this.#expectedAt = this.#now() + this.#sampleMs;
    this.#timer = setInterval(() => this.sample(), this.#sampleMs);
    this.#timer.unref?.();
    return this;
  }

  stop() {
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = null;
    return this;
  }

  /** 完了済みスパンを記録する (呼び出し側が開始時刻を持っている場合)。 */
  span(name, startedAtMs, { details = undefined, endedAtMs = undefined } = {}) {
    const end = endedAtMs ?? this.#now();
    const start = Number(startedAtMs);
    const entry = {
      name,
      started_at_ms: start,
      ended_at_ms: end,
      dur_ms: Math.max(0, end - start),
      ...(details === undefined ? {} : { details }),
    };
    if (entry.dur_ms < this.#minSpanMs) return null;
    this.#spans.push(entry);
    if (this.#spans.length > this.#ringSize) this.#spans.shift();
    this.#stats.spansRecorded += 1;
    return entry;
  }

  /** 非同期処理を計測して実行する (結果はそのまま返す)。 */
  async wrap(name, fn, details) {
    const startedAt = this.#now();
    try {
      return await fn();
    } finally {
      this.span(name, startedAt, details === undefined ? {} : { details });
    }
  }

  /** 同期処理を計測して実行する。 */
  wrapSync(name, fn, details) {
    const startedAt = this.#now();
    try {
      return fn();
    } finally {
      this.span(name, startedAt, details === undefined ? {} : { details });
    }
  }

  /**
   * 明示的な観測記録。呼び出し側が「異常」と判断した時だけ呼ぶ
   * (正常時は呼ばない = 記録ゼロの方針を維持する)。
   * 例: writer が閾値以上の append 時間を検出し、その内訳を残したいとき。
   */
  report(record) {
    if (typeof this.#onAnomaly !== 'function') return null;
    const entry = { ts: new Date(this.#now()).toISOString(), label: this.#label, ...record };
    try {
      this.#onAnomaly(entry);
    } catch {
      // 観測の失敗で本体を落とさない。
    }
    return entry;
  }

  /**
   * 高頻度パス用の軽量マーカー。`begin()` の戻り値の `end()` を finally で呼ぶ。
   * minSpanMs 未満はリングに残さないので、メッセージ毎の呼び出しでも安全。
   */
  begin(name, details) {
    const startedAt = this.#now();
    return {
      end: (extra) => this.span(name, startedAt, {
        ...(details === undefined && extra === undefined ? {} : { details: { ...(details ?? {}), ...(extra ?? {}) } }),
      }),
    };
  }

  /** ゲージ値 (キュー深さ等) を記録する。 */
  note(name, value) {
    this.#gauges.push({ name, value, at: this.#now() });
    if (this.#gauges.length > this.#ringSize) this.#gauges.shift();
  }

  /**
   * 1 サンプル分の計測を実行する。setInterval から呼ばれるほか、テストや
   * 明示的な駆動 (停止中の復帰検出) にも使う。
   */
  sample() {
    this.#tick();
  }

  #tick() {
    const now = this.#now();
    const lag = now - this.#expectedAt;
    const drift = now - this.#lastTickAt;
    this.#lastTickAt = now;
    this.#expectedAt = now + this.#sampleMs;
    this.#stats.samples += 1;
    if (lag > this.#stats.maxLagMs) this.#stats.maxLagMs = lag;
    void drift;

    const slow = lag >= this.#lagThresholdMs;
    this.#lagSamples.push({ at: now, lag_ms: lag, slow });
    if (this.#lagSamples.length > this.#ringSize) this.#lagSamples.shift();

    if (slow) {
      this.#quietTicks = 0;
      if (this.#event === null) {
        this.#event = {
          started_at_ms: now - lag,
          detected_at_ms: now,
          max_lag_ms: lag,
          samples: 1,
        };
        this.#stats.events += 1;
        this.#emit('stall', this.#event, now - lag, now);
      } else {
        this.#event.max_lag_ms = Math.max(this.#event.max_lag_ms, lag);
        this.#event.samples += 1;
      }
      return;
    }

    if (this.#event !== null) {
      this.#quietTicks += 1;
      if (this.#quietTicks >= this.#recoveryStreak) {
        const ended = { ...this.#event, ended_at_ms: now, duration_ms: now - this.#event.started_at_ms };
        this.#event = null;
        this.#quietTicks = 0;
        this.#emit('recovered', ended, ended.started_at_ms, now);
      }
    }
  }

  #emit(kind, event, fromMs, toMs) {
    const pad = 2000;
    const windowFrom = fromMs - pad;
    const windowTo = toMs + pad;
    const spans = this.#spans
      .filter((s) => s.ended_at_ms >= windowFrom && s.started_at_ms <= windowTo)
      .map((s) => ({ ...s }));
    const gaugeNames = new Set(
      this.#gauges.filter((g) => g.at >= windowFrom && g.at <= windowTo).map((g) => g.name),
    );
    const gauges = {};
    for (const name of gaugeNames) {
      gauges[name] = this.#gauges
        .filter((g) => g.name === name && g.at >= windowFrom && g.at <= windowTo)
        .map((g) => [toIso(g.at), g.value]);
    }
    const lagSamples = this.#lagSamples
      .filter((s) => s.at >= windowFrom && s.at <= windowTo)
      .map((s) => [toIso(s.at), s.lag_ms]);
    this.#onAnomaly({
      kind,
      label: this.#label,
      ts: toIso(toMs),
      lag_threshold_ms: this.#lagThresholdMs,
      sample_ms: this.#sampleMs,
      ...(kind === 'stall'
        ? { detected_at_ms: event.detected_at_ms, max_lag_ms: event.max_lag_ms }
        : { duration_ms: event.duration_ms, max_lag_ms: event.max_lag_ms }),
      slow_samples: event.samples ?? 1,
      stalled_since: toIso(event.started_at_ms),
      window: { from: toIso(windowFrom), to: toIso(windowTo) },
      lag_samples: lagSamples,
      spans,
      gauges,
    });
  }
}

/**
 * 異常時のみ書くログ (サイズでローテーション)。通常運転ではファイルを作らない。
 */
export function createStallLog({ filePath, maxBytes = 5 * 1024 * 1024, generations = 3, fsModule }) {
  const fs = fsModule;
  return {
    path: filePath,
    write(record) {
      try {
        fs.mkdirSync(filePath.slice(0, filePath.lastIndexOf('/')), { recursive: true });
        let size = 0;
        try { size = fs.statSync(filePath).size; } catch { size = 0; }
        if (size >= maxBytes) {
          for (let i = generations - 1; i >= 1; i -= 1) {
            const from = i === 1 ? filePath : `${filePath}.${i - 1}`;
            const to = `${filePath}.${i}`;
            try { if (fs.existsSync(from)) fs.renameSync(from, to); } catch { /* best effort */ }
          }
        }
        fs.appendFileSync(filePath, `${JSON.stringify(record)}\n`);
        return true;
      } catch {
        return false;
      }
    },
  };
}

export const STALL_PROBE_DEFAULTS = {
  lagThresholdMs: DEFAULT_LAG_THRESHOLD_MS,
  sampleMs: DEFAULT_SAMPLE_MS,
  ringSize: DEFAULT_RING_SIZE,
  recoveryStreak: DEFAULT_RECOVERY_STREAK,
  minSpanMs: 0,
};
