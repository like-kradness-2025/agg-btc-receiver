# Burst Aggregator Implementation Plan

Based on: docs/burst-aggregator-design.md (v3, blocker解消済み)

## 全体の流れ

1. lib/replay-book-state.mjs を抽出
2. scripts/burst-agg.mjs を新規作成
3. cron 切り替え
4. 古いコード削除

---

## Phase 1: lib/replay-book-state.mjs 抽出

今の `scripts/aggregate-live-v3.mjs` から `replayBestBookState()` 関数を lib に切り出す。

**ファイル**: `lib/replay-book-state.mjs`

```js
export function replayBestBookState(bookEvents, fromMs, toMs) {
  // 現状の実装をそのまま移植
  // bookEvents を時系列にソート
  // forward-fill で bestBid/bestAsk を復元
  // bookAtTime(ts) クロージャを返す
}
```

**テスト**: `test/replay-book-state.test.mjs` に既存テストを移す。既存の aggregate-live-v3.test.mjs から該当部分 import に変える。

---

## Phase 2: scripts/burst-agg.mjs 新規作成

新しい集約エントリポイント。責務:
- data/live_v3/trades/… から raw 読み出し
- data/live_v3/book_updates/…, snapshots/… から book 読み出し
- BurstBuilder で burst 形成
- 30s window 単位で処理
- data/burst_agg/summary/… / features/… に出力

**引数** （aggregate-live-v3.mjs から最小限に絞る）:

```
--data <path>         data/live_v3
--out <path>          data/burst_agg
--markets <list>      カンマ区切り
--from <ISO>          開始時刻
--to <ISO>            終了時刻
--book-range-usd <N>  デフォルト 10000
```

**出力の差分** （今と比べて）:

| 今（aggregate-live-v3） | 新（burst-agg） |
|---|---|
| 1s_features（1秒=1行、trade非依存） | なし |
| 30s_book（30秒=1行） | なし |
| なし | summary（30秒=1行、dense） |
| なし | features（30秒=最大30行、sparse） |

**burst builder の跨ぎ処理**:
- window 境界で `burstBuilder.flushAll()` は呼ばない
- window 処理後に burst builder をリセットしない（BurstBuilder は stream を保持し続ける）
- summary の burst 紐付け: burst の `start_ts` がどの window に入るか判定

**book state**:
- 新しく lib/replay-book-state.mjs を使う
- trades と book_updates を時間同期して処理
- windows 処理で book_at_time() を使い open/close の best_bid/ask を取得

---

## Phase 3: cron 切り替え

既存の aggregate-live-v3-2min を burst-agg に切り替える。

**cron コマンド**:
```
node scripts/burst-agg.mjs \
  --data data/live_v3 \
  --out data/burst_agg \
  --markets <全15 market> \
  --from <5 min ago> \
  --to <now> \
  --book-range-usd 10000
```

出力先は `data/burst_agg/summary/` + `data/burst_agg/features/`。run report は `data/burst_agg/runs/`。

---

## Phase 4: クリーンアップ

削除するもの:
- `scripts/aggregate-live-v3.mjs`
- `test/aggregate-live-v3.test.mjs`
- `scripts/cleanup-processed-raw.mjs`
- `cleanup-processed-raw` cron job
- dashboard の集約情報表示
