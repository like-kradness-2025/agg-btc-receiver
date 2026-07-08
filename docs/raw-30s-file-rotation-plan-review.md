# 30秒ファイル分割設計 レビュー観点

> 対象: `docs/raw-30s-file-rotation-plan.md`
> 現状実装: `orderflow_monitor.mjs` + `lib/buffered-writer.mjs`

---

## 1. timestamp基準

- 現行案では trade / depth / liquidation は event timestamp 基準、snapshot は serialized write path 内の write 直前時刻基準、日付フォルダは UTC 基準で統一されている。方向はよい。
- いまは次も明記済み:
  - non-numeric / numeric-string / non-finite は drop + log
  - old-drop watermark は **market / kind ごと**
  - old-drop 判定は `window_start_ms <= finalized watermark`
  - **future skew 許容内でも `window_start_ms > current_wall_clock_window_start_ms` は drop**
- これで live ingest と recovery が同じ writable-window ルールになった。
- 実装前に確認すべき点:
  - 各 exchange の実 timestamp 桁 spot check

---

## 2. rename安全性

- `.open` と `.jsonl` を同一ディレクトリに置き、同一ファイルシステム内 finalize を前提にしている。
- 重要なのは **no-clobber finalize primitive** を使う点。
  - 既存 `.jsonl` を上書きしない
  - `EEXIST` は quarantine + log + no watermark advance
- quarantine も **no-clobber** で行う。
  - 既存 quarantine artifact を上書きしない
  - 衝突時は一意 suffix を付ける
- finalize sequence:
  - live: `drain -> close -> no-clobber finalize -> watermark update`
  - recovery: `no-clobber finalize -> watermark update`

---

## 3. late event

- 現行案では late event は current + previous の2窓までだけ許容する。
- older への late event は drop + log で固定済み。
- finalized 済み窓は reopen しない。
- 後段は最新2窓を読まない前提も固定済み。

---

## 4. market直列性

- 現行案は「窓順直列」を保証対象に狭めた。これは妥当。
- 1ファイル内部の strict event-time sort は保証しない、と明記済み。
- さらに、**同じ market / kind の全 mutation を1本の直列キューに通す** 不変条件が追加された。
- 対象 mutation:
  - event write
  - timer-driven finalize
  - startup recovery
  - quarantine 移動
- startup recovery は、その key の最初の live mutation より先に完了するか、同じ直列キューの先頭で処理する。

---

## 5. snapshot扱い

- snapshot は serialized write path 内で bucketing 直前に採る `Date.now()` 基準。
- 監査用に **`write_time_ms`** を保存する。
- source timestamp があれば **同じ正規化関数で整数 ms 化して** `source_time_ms` として別保持する。

---

## 6. UTC日跨ぎ

- UTC 基準は明記済み。
- 境界例も明記済み。
- restart 時の `.open` 探索は market / kind 配下の **再帰走査** に固定された。

---

## 7. 再起動復旧

- startup recovery の順序が plan に追加された。
  1. `.jsonl` 走査で watermark 復元
  2. 再帰走査で `.open` 列挙
  3. `window_start_ms` 昇順で整列
  4. keep / recovery-finalize / quarantine 判定
  5. keep した `.open` を manager へ注入
- recovery finalize は **昇順** で実行し、watermark は **`max`** でしか進めない。
- live ingest と同じ writable-window ルールを使うため、restart 時の future quarantine と整合する。

---

## 8. 小差分実装可能性

- いまの設計なら **小差分でいける**。
- 主変更点は `orderflow_monitor.mjs` に集中できる見込み。
- 主に必要な追加:
  - timestamp normalize 関数
  - window 計算関数
  - market/kind 単位 writer manager
  - per-market/kind serialized queue
  - startup `.open` recovery
  - quarantine path
  - no-clobber finalize/quarantine helper
- broad rewrite は不要。

---

## 現時点の判定

### 主要 blocker
- 前回 Codex が挙げた blocker に対して、文書側の不変条件は追加済み
- 次の判定は、最新 plan を対象にした独立レビューで再確認すべき

### 実装前の残り確認
- 各 exchange timestamp の実桁 spot check
- snapshot row へ `write_time_ms` / `source_time_ms` をどう載せるか
- manager API の最小差分形
