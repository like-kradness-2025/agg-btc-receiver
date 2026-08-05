# Raw Hot Storage Tier Spec

## 0. 目的

本仕様は、BTC 市場データ receiver の raw hot storage tier を再設計するための仕様書である。

対象は 17 BTC markets のリアルタイム受信データで、以下を満たすことを目的とする。

- WS depth diff を失わない
- full snapshot を recovery checkpoint として保持する
- trade は raw JSONL のまま保持する
- すべてを同一の retention policy で扱う
- 日付パーティションで運用しやすくする

## 1. 結論

### Decision 1: Diff storage format

採用案は **Option C: JSONL on write path + nightly Parquet conversion** である。

理由:

- ingest path が最も単純で、既存の `BufferedWriter` をそのまま活かせる
- 受信直後に書けるため、バッチ待ちによる遅延や crash 時の未 flush 損失を避けられる
- Parquet は閉じた日付パーティションだけを対象に変換できるので、小さな Parquet ファイルを量産しない
- nightly job で row count とファイル整合性を確認してから JSONL を削除できる

非採用理由:

- Option A は実装が簡単だが、長期保持時のストレージ効率が悪い
- Option B は圧縮効率は良いが、書き込み経路が複雑になり、append に向かない

### Decision 2: Snapshot trigger conditions

採用するトリガーは以下である。

1. **Periodic timer**: 10 分ごと
2. **Successful recovery transition**: connector が `running` に戻った直後
   - connector startup after initial sync
   - reconnect/resync after disconnect
   - sequence gap recovery after book reset
3. **Graceful shutdown**: shutdown 前の最終 flush 後に 1 回

補足:

- `reconnecting` になった瞬間には snapshot を書かない
- snapshot は「book が valid になった後」にだけ書く
- gap detection 直後は book が壊れているので、復旧完了後に書く

## 2. ディレクトリ layout

### 2.1 Hot tier

UTC 日付パーティションを使う。

```text
data/raw_hot/
  2026-06-25/
    depth/
      binance_spot.jsonl
      binance_perp.jsonl
      ...
    snapshot/
      binance_spot.jsonl
      binance_perp.jsonl
      ...
    trade/
      binance_spot.jsonl
      binance_perp.jsonl
      ...
```

### 2.2 Archive tier

夜間変換後の Parquet は同じ日付パーティションと stream 分割を維持する。

```text
data/raw_archive/
  2026-06-25/
    depth/
      binance_spot.parquet
    snapshot/
      binance_spot.parquet
    trade/
      binance_spot.parquet
    manifest.json
```

### 2.3 Current day と closed day

- 現在 UTC 日の JSONL は hot tier で open のまま保持する
- 直前の closed day は nightly job の成功まで JSONL を保持する
- nightly job 成功後は closed day の JSONL を削除し、Parquet のみを残す

## 3. File naming conventions

### 3.1 Date partition

- パーティション名は UTC の `YYYY-MM-DD`
- ローカルタイムではなく UTC を使う
- DST の影響を避けるため、日付境界は常に UTC 00:00 とする

### 3.2 File names

- depth diff: `{market}.jsonl`
- snapshot: `{market}.jsonl`
- trade: `{market}.jsonl`
- archive parquet: `{market}.parquet`

### 3.3 Market id

`market` は既存コードの canonical market id をそのまま使う。

例:

- `binance_spot`
- `binance_perp`
- `binance_coinm_perp`
- `binance_perp_btcusdc`
- `bybit_spot`
- `bybit_perp`
- `okx_spot`
- `okx_perp`
- `coinbase_spot`
- `kraken_spot`

## 4. Diff schema

depth diff は WS incremental orderbook update を 1 行 1 JSON で保存する。

### 4.1 Common fields

| field | type | required | notes |
|---|---|---:|---|
| `schemaVersion` | string | yes | 固定値 `1.0` |
| `stream` | string | yes | 固定値 `depth` |
| `type` | string | yes | 固定値 `update` |
| `ts` | number | yes | upstream event time in ms UTC. 無ければ ingest time を入れる |
| `recvTs` | number | yes | receiver が line を生成した時刻の epoch ms |
| `market` | string | yes | canonical market id |
| `exchange` | string | yes | canonical exchange id |
| `seq` | number \| null | yes | feed sequence. 無い feed は `null` |
| `prevSeq` | number \| null | yes | bridge 検証用。無い feed は `null` |
| `bids` | array<[string, string]> | yes | `[price, qty]` の文字列ペア |
| `asks` | array<[string, string]> | yes | `[price, qty]` の文字列ペア |

### 4.2 Example

```json
{
  "schemaVersion": "1.0",
  "stream": "depth",
  "type": "update",
  "ts": 1782369865486,
  "recvTs": 1782369865491,
  "market": "binance_spot",
  "exchange": "binance",
  "seq": 96350319614,
  "prevSeq": 96350319613,
  "bids": [["61731.99", "3.15"]],
  "asks": [["62532.00", "0.5"]]
}
```

### 4.3 Rules

- `bids` / `asks` の価格と数量は文字列で保持する
- 数値への変換は replay / query 側で行う
- delete は diff 内で `qty === "0"` または空文字として表現してよい
- 1 イベントに複数レベル変更が入ってよい
- orderbook の完全 snapshot を diff ファイルに混ぜない

## 5. Snapshot schema

snapshot は full book checkpoint である。

### 5.1 Common fields

| field | type | required | notes |
|---|---|---:|---|
| `schemaVersion` | string | yes | 固定値 `1.0` |
| `stream` | string | yes | 固定値 `snapshot` |
| `reason` | string | yes | `startup`, `periodic`, `reconnect`, `gap_recovery`, `shutdown` |
| `ts` | number | yes | snapshot の作成時刻 ms UTC |
| `recvTs` | number | yes | receiver が line を生成した時刻の epoch ms |
| `market` | string | yes | canonical market id |
| `exchange` | string | yes | canonical exchange id |
| `seq` | number \| null | yes | snapshot 時点の last applied seq |
| `bids` | array<[string, string]> | yes | full bids, sorted best-to-worst |
| `asks` | array<[string, string]> | yes | full asks, sorted best-to-worst |
| `bidLevelCount` | number | yes | bids の件数 |
| `askLevelCount` | number | yes | asks の件数 |

### 5.2 Example

```json
{
  "schemaVersion": "1.0",
  "stream": "snapshot",
  "reason": "periodic",
  "ts": 1782369900000,
  "recvTs": 1782369900004,
  "market": "binance_spot",
  "exchange": "binance",
  "seq": 96350319881,
  "bids": [["61731.99", "3.15"], ["61731.98", "1.20"]],
  "asks": [["62532.00", "0.5"], ["62532.10", "1.00"]],
  "bidLevelCount": 2,
  "askLevelCount": 2
}
```

### 5.3 Rules

- snapshot は full book であること
- partial book や diff のみのデータを snapshot に混ぜない
- `seq` はその snapshot の book state を復元するための最後の sequence
- `reason` は checkpoint の発生理由を示す

## 6. Snapshot trigger conditions

### 6.1 Periodic timer

- 10 分ごとに snapshot を書く
- 基準は UTC の wall clock boundary
- つまり `00`, `10`, `20`, `30`, `40`, `50` 分の 0 秒に揃える

### 6.2 Successful recovery transition

以下の状態遷移で、`running` に入った直後に snapshot を 1 回書く。

- `connecting -> syncing -> running`
- `reconnecting -> syncing -> running`
- `running -> reconnecting -> syncing -> running`
- `running -> error -> syncing -> running`

ここで重要なのは、**成功した復旧のあとだけ snapshot を書く** ことである。

書かないもの:

- `reconnecting` の開始時
- `syncing` 中
- gap を検知した瞬間
- 失敗した retry の途中

### 6.3 Graceful shutdown

- shutdown 開始時にまず writer を flush する
- その後、book が valid な market について最終 snapshot を 1 回書く
- その snapshot の `reason` は `shutdown`
- その後に writer を close する

### 6.4 Exact timing

おすすめの実装タイミングは以下である。

- `running` への state transition を受けた event loop turn の中で即時 snapshot
- periodic snapshot は 10 分 boundary 到達後、次の tick で即時 snapshot
- shutdown snapshot は pending diff/trade flush 完了後、writer close 前に即時 snapshot

## 7. Retention policy

### 7.1 Hot window

- hot JSONL の保持対象は **current UTC day + 直近 1 closed day**（最大 2 日分）
- steady state では、JSONL は実質 0 〜 48 時間のホットデータである
- nightly conversion が遅延した場合でも、closed day の JSONL は archive 成功まで残す

### 7.2 Rotation mechanism

1. UTC 00:00 で writer の出力先を翌日パーティションに切り替える
2. 既存 writer は close し、新しい日付の file を lazily open する
3. closed day は nightly job の対象になる

### 7.3 Archive retention

- Parquet archive は hot tier とは別に管理する
- 初期値は **180 日（約 6 ヶ月）** とし、サイズ状況を見て調整する

## 8. Nightly archive / conversion plan

### 8.1 When

- 毎日 1 回、UTC 00:15 以降に実行する
- 目的は、日付が閉じた partition のみを安全に変換するためである

### 8.2 What

- 前日以前の closed partition の `depth/`, `snapshot/`, `trade/` を Parquet 化する
- current day の partition は触らない

### 8.3 How

推奨手順:

1. `read_json_auto()` で JSONL を読む
2. DuckDB で `COPY ... TO ... (FORMAT PARQUET, COMPRESSION ZSTD)` を実行する
3. row count を JSONL 側と Parquet 側で比較する
4. 必要なら `min(ts)` / `max(ts)` も比較する
5. 変換成功後に manifest を書く
6. manifest が成功なら JSONL を削除する

### 8.4 Manifest

`manifest.json` は最低でも以下を持つ。

- `partitionDate`
- `stream`
- `market`
- `sourcePath`
- `targetPath`
- `rowCount`
- `status`
- `startedAt`
- `finishedAt`

### 8.5 Failure handling

- 変換失敗時は JSONL を残す
- Parquet を壊して JSONL を消さない
- 失敗 partition は次回 nightly で再試行する

## 9. Trade data handling

trade は raw JSONL のまま保持する。

### 9.1 Rules

- trade は aggregate しない
- trade は depth / snapshot と同じ retention policy に従う
- 1 trade event = 1 JSON line
- 複数 trade を 1 WS message で受けた場合は、1 件ずつ line 化する

### 9.2 Required portable fields

trade レコードは connector ごとの正規化済み raw JSONL とするが、少なくとも以下を持つこと。

- `ts`: number
- `market`: string
- `price`: number
- `qty`: number
- `side`: `"buy"` | `"sell"`
- `tradeId`: string | null

### 9.3 Recommended extras

必要なら以下を追加してよい。

- `exchange`: string
- `recvTs`: number
- `symbol`: string
- `sourceTs`: number

ただし、query / replay は必須フィールドだけで動くようにする。

## 10. Replay procedure

### 10.1 Goal

Replay は以下の順で book を復元する。

1. 最新の snapshot を読む
2. その snapshot の `seq` 以降の depth diff を順に適用する
3. trade は別系統として参照する

### 10.2 Market-local replay

market ごとに独立して再生する。cross-market merge は不要である。

### 10.3 Procedure

1. 対象 market と target time を決める
2. target time 以下で最も新しい snapshot を探す
3. その snapshot を初期 book としてロードする
4. 同じ market の depth diff を file order で順に読む
5. `seq` がある feed は sequence continuity を確認する
6. gap があれば replay を停止し、欠落を報告する
7. trade は必要なら別ファイルから同じ time range で読む

### 10.4 Ordering rules

- 同一ファイル内は append order を正とする
- replay は file order を優先する
- `ts` は補助情報であり、同一 market の順序決定は file order を優先する
- `seq` が使える feed では `seq` を補助的な検証に使う

### 10.5 Restart recovery

receiver 再起動時は以下で復元する。

1. 前回の closed day の最終 snapshot を読む
2. current day の snapshot があればそれを優先する
3. 以後の diff を file order で適用する

## 11. Migration plan from current layout

### 11.1 Current layout

現行の raw 保存は以下の flat layout である。

```text
data/live_fairprice/
  book/{market}.jsonl
  book/{market}_update.jsonl
  trades/{market}.jsonl
  fairprice/{market}.jsonl
  ...
```

### 11.2 Migration strategy

**Dual-write は行わない。** 新形式の実装完了と同時に旧形式を停止する。

#### Phase 1: 新形式実装

- 新しい `data/raw_hot/YYYY-MM-DD/{stream}/{market}.jsonl` を追加する
- 旧 layout の writer は維持したまま、新 layout への書込を追加する
- `fairprice_monitor.mjs` 内で新旧両方に書くか、新のみに切り替える

#### Phase 2: Cutover

- 新形式の動作確認完了後、旧 layout (`data/live_fairprice/book/`, `data/live_fairprice/trades/` 等) への書込を停止する
- 既存の旧データは削除してよい
- 1s agg daemon は新 layout から book を読むよう更新する（`scripts/aggregate-1s.mjs` の book path 変更）

#### Phase 3: Verification

- 新旧の row count を比較して欠損がないことを確認する（書込停止前に 5 分程度の dual-write overlap を入れてもよい）
- 代表 market で replay 成功を確認する
- nightly Parquet 変換後の row count も確認する

### 11.3 Backfill caveat

- current day の open file は backfill 途中にまだ増える可能性がある
- そのため、cutover 前に短い dual-write overlap を置くのが安全である

## 12. Implementation notes

- 既存の `BufferedWriter` を hot path で再利用する
- 日付切り替えだけを担当する薄い writer manager を追加する
- JSONL は UTF-8, 1 line = 1 JSON object, trailing LF あり
- Parquet 生成は ingest path から完全に分離する
- 変換失敗時は原本 JSONL を authoritative source とする
