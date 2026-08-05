# 後工程 OrderFlow パイプライン仕様

> **後工程の正本仕様（target、2026-07-19）**
>
> Receiver の正本仕様は [`../SPEC.md`](../SPEC.md)、現行実装との差分は
> [`orderflow_features_spec.md`](orderflow_features_spec.md) を参照する。
> 本書に列挙した特徴量は目標仕様であり、列挙されているだけで実装済みとは扱わない。

## 1. 目的と境界

後工程は、raw-only receiver が保存したイベントを再生し、OrderFlow 分析に必要な
特徴量を、再現可能かつ lookahead のない時系列として生成する。

後工程の責務:

- raw trade から全約定フローと burst 特徴量を生成する
- book snapshot と update を市場固有の規則で再生し、板状態を復元する
- 板イベントを add / cancel / depletion / replenishment として分類する
- 約定と板状態を時刻順に結合し、sweep・吸収・価格インパクトを計算する
- liquidation と外部派生データを、利用可能な市場だけで追加する
- 1秒、30秒、5分の特徴量と品質情報を出力する
- checkpoint、manifest、hash により再実行を冪等にする

後工程がしないこと:

- receiver の受信・再接続・raw 保存を変更しない
- 未取得データを 0 で捏造しない
- 将来リターンなどの教師ラベルを特徴量ファイルに混在させない
- 特徴量生成の完了だけを理由に raw ファイルを即時削除しない

## 2. 入力契約

入力は `data/live_v3/` の30秒 JSONL ブロックである。

| stream | path | 主な内容 |
|---|---|---|
| trades | `trades/<market>/<date>/<HH-MM-SS>.jsonl` | `market, price, qty, side, ts, tradeId` |
| book | `book_updates/<market>/<date>/<HH-MM-SS>.jsonl` | snapshot/update、bids、asks、ts、market固有seq |
| liquidation | `liquidations/<market>/<date>/<HH-MM-SS>.jsonl` | 強制清算イベント |
| health | `health.jsonl` | receiver の接続・受信・欠損診断 |

`agg_trades/`、`snapshots/`、`book_snapshots/` は現行 receiver の正本出力ではない。
古いディレクトリが存在しても、新規処理の必須入力にしてはならない。

注意事項:

- `seq` の意味は connector ごとに異なる。全市場に単調増加を仮定しない
- Kraken の `seq` 相当値は checksum として扱い、連番判定に使用しない
- raw に受信時刻がない場合、通信 latency 特徴量は計算不能であり `null` とする
- 価格・数量の文字列表現は decimal として読んだ後、計算精度を明示して数値化する

## 3. 全体構成

```text
raw trades ───────────► trade flow ───────┐
       └──────────────► burst detector ───┤
                                          │
book snapshot/seed ─┐                      ├─► features_1s
book updates ───────┴─► book replay ──────┤       │
                         ├─ book state ────┤       ├─► features_30s
                         └─ event flow ────┤       └─► features_5min
trades × book state ───► interaction ─────┤
liquidations/aux ──────► crypto/perp ─────┘

future prices ───────────────────────────────► labels（別系統）
```

特徴量は7層で管理する。

1. Trade flow primitives
2. Burst / temporal structure
3. Book state
4. Book event flow / OFI
5. Trade-book interaction
6. Price response / regime
7. Crypto・perpetual・cross-market

すべての層に品質・provenance を付ける。

## 4. 時間契約

### 4.1 基本粒度

- 入力単位: 30秒ブロック
- 基本出力: 1秒足
- rolling window: 5秒、10秒、30秒、60秒を基本とする
- 集約出力: 30秒、5分
- 市場ごとに独立して処理し、異なる市場のイベント順を混在させない

### 4.2 区間

- 1秒 bucket は `[ts, ts + 1000)` の半開区間
- strict-past rolling は `[ts - window, ts)` とし、現在秒を含めない
- 同一 stream・同一 timestamp は connector の順序情報を優先し、なければ
  stable input order を維持する
- 異なる stream の同一 timestamp に因果順を捏造しない。共通 sequence がない場合、
  trade-book join は `book.ts < trade.ts` の直前状態を使い、同時刻 tie を品質情報に記録する
- 秒・30秒・5分境界は UTC epoch で切る
- row の `ts` は window 開始時刻とし、row は window 終了と watermark 確認後にだけ利用可能

### 4.3 確定条件

30秒入力ブロックは次を満たしてから確定処理する。

- ファイルが close 済みで、size が安定している
- JSONL の全行が parse できる
- 対象 block より新しい block または明示 watermark を確認している
- 遅延到着の許容時間を経過している

遅延到着を受けた場合は、対象区間を再計算して同じ partition を原子的に置換する。

## 5. Book seed・再生契約

### 5.1 Seed の取得

Book 特徴量は、次のいずれかの明示的 seed を取得してから有効にする。

1. `book_updates` 内の connector 標準化済み snapshot row
2. 時刻・market・source を記録した外部 REST snapshot

両 side に価格が存在するだけでは seed 完了とみなさない。
seed 前の book 特徴量は 0 ではなく `null` とし、`book_seeded=false` を付ける。

### 5.2 Replay

- snapshot は local book を全置換する
- update は connector 固有の replace/delta semantics に従う
- quantity 0 の削除、partial replace、sequence bridge を connector adapter で処理する
- crossed book、負数量、不正価格、sequence gap を検出する
- gap または checksum 不一致後は book を無効化し、再 seed まで book 特徴量を `null` にする

### 5.3 検証

- snapshot 直後の best bid/ask と depth を期待値照合する
- replay 後の checksum または exchange snapshot と照合する
- 同じ raw と設定から同じ row/hash が生成されることを確認する

## 6. Canonical 出力

新しい正本 namespace は `data/derived/orderflow_features_v1/` とする。
既存 `burst_features_v1` は互換読み取り用とし、意味を変えて上書きしない。

```text
data/derived/orderflow_features_v1/
├── features_1s/<market>/<date>/<HH-MM-SS>.jsonl
├── features_30s/<market>/<date>/<HH-MM-SS>.jsonl
├── features_5min/<market>/<date>/<HH-MM-SS>.jsonl
├── labels_forward/<market>/<date>/<HH-MM-SS>.jsonl
├── manifests/<market>/<date>.jsonl
├── checkpoints/<market>.json
└── quarantine/<stream>/<market>/<date>/
```

1秒 row の envelope:

```json
{
  "schema_version": "orderflow_features_1s_v2",
  "ts": 0,
  "market": "binance_spot",
  "features": {},
  "_quality": {}
}
```

特徴量名・型・単位・window・null 条件・集約 operator は schema registry で固定する。
列追加は schema version を上げる。既存列の意味または単位を同じ version 内で変えない。

## 7. 特徴量仕様

### 7.1 P0: Trade flow primitives

全市場で raw trades だけから生成できる最優先層。

| field | 定義・単位 | no trade |
|---|---|---|
| `trade_count_1s` | 約定 print 数 | 0 |
| `buy_trade_count_1s`, `sell_trade_count_1s` | aggressor side 別 print 数 | 0 |
| `traded_qty_1s` | `Σ qty` | 0 |
| `traded_notional_1s` | `Σ price × qty`、quote currency | 0 |
| `buy_qty_1s`, `sell_qty_1s` | side 別 qty | 0 |
| `buy_notional_1s`, `sell_notional_1s` | side 別 notional | 0 |
| `signed_volume_1s` | `buy_qty - sell_qty` | 0 |
| `signed_notional_1s` | `buy_notional - sell_notional` | 0 |
| `trade_imbalance_qty_1s` | signed volume / traded qty | 0 |
| `trade_imbalance_notional_1s` | signed notional / traded notional | 0 |
| `mean_trade_notional_1s` | print notional の平均 | null |
| `median_trade_notional_1s` | print notional の中央値 | null |
| `max_trade_notional_1s` | 最大 print notional | null |
| `large_trade_count_1s` | versioned market threshold 以上の print 数 | 0 |
| `large_trade_notional_share_1s` | large notional / total notional | 0 |
| `mean_interarrival_ms_1s` | 後側tradeがbucket内にある約定間隔の平均 | null |
| `median_interarrival_ms_1s` | 後側tradeがbucket内にある約定間隔の中央値 | null |
| `p95_interarrival_ms_1s` | 後側tradeがbucket内にある約定間隔の95 percentile | null |
| `side_flip_count_1s` | 後側tradeがbucket内にある aggressor side 反転回数 | 0 |
| `realized_vol_10s`, `realized_vol_60s` | strict-past log return の population std | warmupはnull |

large trade threshold は市場・quote currency ごとの versioned config とし、
row または manifest に threshold version を記録する。
interarrival と side flip は直前秒の最終 trade を state として引き継ぎ、
秒境界にある間隔・反転を落とさない。

### 7.2 P0: Burst / temporal structure

既存 burst 特徴量を維持し、全 trade flow と明確に区別する。

- burst count、total/max notional、max prints、max duration
- buy/sell burst notional、burst delta、burst imbalance
- largest burst share
- same-price count、max length、notional、absorption ratio
- multilevel count、max span ticks/bps、notional
- burst notional / 30秒 traded notional

未実装値を 0 固定で出力してはならない。実装できない列は `null` とし、
`_quality.unavailable_features` に理由を記録する。

### 7.3 P1: Book state

seed 済み full book の各1秒末状態から生成する。

| group | fields |
|---|---|
| top of book | `best_bid_price`, `best_ask_price`, `best_bid_qty`, `best_ask_qty` |
| price | `mid_price`, `spread_abs`, `spread_ticks`, `spread_bps` |
| L1 | `queue_imbalance_l1`, `microprice`, `microprice_deviation_bps` |
| level depth | bid/ask の `depth_l1`, `depth_l5`, `depth_l10`, `depth_l20` |
| bps depth | bid/ask の `depth_5bps`, `10bps`, `25bps`, `50bps`, `100bps` |
| depth imbalance | 各 level/bps band の `(bid-ask)/(bid+ask)` |
| shape | weighted depth、slope、convexity、level gap、wall concentration |

Depth は実際の level を合計する。best level の数量を `$100/$1000 depth` の
代理値として複製してはならない。qty と notional は別列にする。

### 7.4 P2: Book event flow / OFI

連続する book state と raw update からイベント量を生成する。

- bid/ask add qty・notional
- bid/ask cancel qty・notional
- net liquidity flow
- `ofi_l1`, `ofi_l5`, `ofi_l10`
- best queue depletion・replenishment
- queue turnover、cancel/add ratio
- book update count
- spread widen/narrow count
- depth change と imbalance change

replace update は旧 quantity との差分から add/cancel を判定する。
`qty > 0` を無条件に add、`qty = 0` のみを cancel とする実装は禁止する。

### 7.5 P2: Trade-book interaction

各 trade の直前に有効だった book state と結合する。

- at-touch qty/notional
- through-touch qty/notional
- swept level count・sweep notional
- execution slippage bps
- aggressive qty / pre-trade top depth
- aggressive qty / same-side bps depth
- best depletion 後の replenishment latency
- absorption、exhaustion、failed continuation
- signed flow 当たりの過去価格変化
- burst at-touch/through ratio、burst depletion/replenishment

book が未 seed、gap 中、stale の場合は interaction 全体を `null` とする。

### 7.6 P2: Price response / regime

- strict-past return: 1秒、5秒、10秒、30秒、60秒
- realized volatility: 10秒、30秒、60秒
- high-low range、mid-price change
- volume・spread・depth・OFI の rolling z-score
- liquidity regime、volatility regime
- historical impact: 過去に発生した flow に対して観測済みの価格反応

`return_t+1`、`return_t+5` など将来値は特徴量ではない。
必要なら `labels_forward/` に別 row として保存し、学習データ作成時に結合する。

### 7.7 P3: Crypto / perpetual / cross-market

データが実際に取得できる市場だけで生成し、未対応市場は `null` にする。

- liquidation count、qty、notional、buy/sell imbalance、burst
- liquidation / visible depth、liquidation 後の replenishment
- open interest と変化率
- funding rate、premium、basis
- taker buy/sell ratio
- spot/perp flow divergence
- exchange 間の OFI・return divergence
- lead-lag

外部 feed の timestamp、source、取得遅延、欠損率を manifest に記録する。

## 8. 主要計算式

分母が0の imbalance は、イベントが存在しない場合のみ 0 とする。
入力不足・book 無効・warmup は `null` であり、0とは区別する。

```text
signed_volume = Σbuy(qty) - Σsell(qty)
trade_imbalance_qty = signed_volume / Σ(qty)

queue_imbalance_l1 = (bid_qty_1 - ask_qty_1)
                     / (bid_qty_1 + ask_qty_1)

microprice = (best_ask × bid_qty_1 + best_bid × ask_qty_1)
             / (bid_qty_1 + ask_qty_1)

depth_imbalance_band = (bid_depth_band - ask_depth_band)
                       / (bid_depth_band + ask_depth_band)

realized_vol = population_std(log(price_i / price_i-1))
```

Cont-style L1 OFI の1イベント差分:

```text
e_n =
  I(bid_price_n >= bid_price_n-1) × bid_qty_n
- I(bid_price_n <= bid_price_n-1) × bid_qty_n-1
- I(ask_price_n <= ask_price_n-1) × ask_qty_n
+ I(ask_price_n >= ask_price_n-1) × ask_qty_n-1

ofi_l1 = Σ e_n
```

L5/L10 は level ごとの差分を価格距離または level weight 付きで合計し、
weight 定義を schema version に固定する。

## 9. 30秒・5分集約

1秒 row を単純平均せず、列ごとに operator を固定する。

| 種類 | operator |
|---|---|
| count、qty、notional、signed flow、add/cancel | sum |
| spread、imbalance、microprice deviation、depth | time-weighted mean + last |
| max trade、sweep levels、burst size | max |
| interarrival、slippage | count-weighted percentile/mean |
| return | 期間始点と終点から再計算 |
| realized volatility | 元の price series から再計算 |
| ratio | numerator と denominator を先に sum して再計算 |
| quality | worst status、coverage、missing seconds |

- `features_30s`: 30個の1秒 row が揃った時だけ complete
- `features_5min`: 10個の complete 30秒 row が揃った時だけ complete
- 不完全な区間を出す場合は `finalized=false` と missing interval を必ず記録する

## 10. Null・品質契約

原則:

- イベントが0件: count/flow は `0`
- 計算対象が存在しない: mean/max/percentile は `null`
- warmup 不足: rolling 特徴量は `null`
- 未対応 feed: `null`
- book 未 seed・gap・stale: book 系は `null`
- 未実装: `null`。0固定は禁止
- NaN、Infinity は出力禁止

`_quality` の必須項目:

```text
input_complete
coverage_ratio
missing_streams
book_seeded
book_stale
sequence_status
gap_detected
warmup_features
unavailable_features
late_event_count
timestamp_inversion_count
source_paths
source_hashes
config_version
finalized
```

`health.jsonl` の reconnect、drop、queue overflow、flush error を対象時間に結合し、
異常区間を「正常データ」として扱わない。

## 11. 分析・学習での使用契約

分析対象は原則として、`finalized=true`、`input_complete=true` で、
必要な層の品質条件を満たした row に限定する。

```text
trade-only analysis:
  trade品質OK

book-state analysis:
  trade品質OK + book_seeded + !book_stale + !gap_detected

OFI / interaction analysis:
  book-state条件 + sequence_status=valid + 対象featureがavailable

cross-market analysis:
  各marketの条件 + 共通watermark + coverage閾値
```

利用規則:

- `null` は欠損理由とともに mask し、機械的に0補完しない
- 比較する row は同じ schema/config/threshold version に揃える
- quote notional を市場横断で比較するときは、明示した FX/peg rate と時刻で
  共通通貨へ変換し、その rate source を残す
- scaling、winsorization、threshold 学習は train 区間だけで fit する
- 学習・検証・test split は時系列順にし、重なる rolling window の leakage を防ぐ
- prediction time は event window の終了時刻と data availability を基準にする
- cross-market join は wall-clock の見かけではなく watermark 済み availability で揃える
- forward label は `labels_forward` から読み、feature生成時には参照しない

## 12. 冪等性・保持

- source path + source hash + config version + schema version を処理キーにする
- checkpoint は出力の原子的 commit 後に進める
- 再実行は同一 row と同一 content hash を生成する
- parse error、gap、不正 book は quarantine し、処理済みにしない
- raw の削除は retention/archive job の責務とする
- raw 削除条件は Parquet 等への変換、row count、hash、backup/retention policy の
  全条件を満たした場合に限る

## 13. 実装フェーズと合格条件

### Phase 0 — trade-only 完成

- P0 trade primitives と既存 burst を実装
- raw trade count、qty、notional が秒→30秒→5分で保存則を満たす
- 0件/null/warmup、lookahead、再起動、重複をテスト
- 全市場の実データで timestamp、finite、side、total parity を検証

### Phase 1 — full book state

- `book_updates` 内 snapshot または明示 REST seed を読める
- connector 別 replay と sequence/checksum 検証を実装
- seed 前、gap 中、stale 中は book 系が必ず null
- depth は複数 level の実合計であることを fixture と実データで検証

### Phase 2 — OFI と interaction

- add/cancel、OFI、depletion/replenishment を手計算 fixture と一致させる
- trade 直前 book との as-of join を検証
- sweep、slippage、absorption の境界条件を検証
- reconnect/gap をまたいで state を継続しない

### Phase 3 — derivatives / cross-market

- liquidation と外部 feed の capability matrix を確定
- market-local clock と coverage を検証してから cross-market join を有効化
- lead-lag の探索と本番特徴量を分離する

各 Phase は unit test、replay test、24時間以上の実データ validation、
schema/manifest 検証を通過してから complete とする。

## 14. 現行実装からの既知差分

| 項目 | 現状 | 本仕様で必要な対応 |
|---|---|---|
| trade/burst | 1s→30s→5min が動作 | P0全約定特徴量を追加 |
| P0/P1 OrderFlow | 1秒raw-trade列、multi-level depth、1秒OFI/add-cancelを実装。P0は全15 market・24時間検証済み | P1 event-level独立照合、OFI rollup、専用namespace migration |
| book seed | 旧 `snapshots/` 系 index を探索 | `book_updates` snapshot / 明示REST seedへ移行 |
| book replay | canonical parse、snapshot/update replay、1秒strict as-ofを実装 | connector別sequence/checksumと長時間独立照合 |
| depth | seed済みbookの全levelからmid±$100 / mid±$1000を1秒as-of計算 | connector別実データでdepth・checksumを照合 |
| fixed zero | 一部未実装列が0固定 | null + unavailable reasonへ変更 |
| output path | 既存 `burst_features_v1` path + `feature_schema_version`（互換既定） | `tfp.mjs --orderflow` で新規 `orderflow_features_v1` へ段階移行 |

この表が解消されるまでは、P1/P2 の book-aware 値を本番分析の根拠にしない。

## 15. 参考文献

- Cont, Kukanov, Stoikov, [The Price Impact of Order Book Events](https://arxiv.org/abs/1011.6402)
- Gould, Bonart, [Queue Imbalance as a One-Tick-Ahead Price Predictor](https://arxiv.org/abs/1512.03492)
- Stoikov, [The Micro-Price](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2970694)
- Zhang, Zohren, Roberts, [DeepLOB](https://arxiv.org/abs/1808.03668)
