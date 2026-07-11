# TFP Book Contract P0-0

- 文書 ID: `specify-2026-07-11-tfp-book-contract-p0`
- 対象: `agg-btc-receiver`
- 状態: P0-0 実装前契約
- 対象外: 30 秒集約、5 分集約、Receiver の派生処理化
- 関連: `specify-2026-07-11-tradeflow-pipeline.md`、`design-2026-07-10-feature-compression-pipeline.md`

## 1. 目的と既存実装との差分

本書は、TFP が immutable raw の `book_updates` を event-time で replay し、既存 trade-only #1--#12 と同じ 1 秒行へ board MVP 候補を供給するための、実装者が推測してはならない契約を固定する。

Receiver の責務は受信と raw 保存だけである。`lib/orderflow-worker.mjs` は `trades`、`book_updates`、`liquidations` の 3 writer だけを作り、depth event をそのまま `book_updates` に渡す。TFP は Receiver の writer、connector の `FullBook`、または raw ファイルを変更してはならない。

現在の実装に関する明示的な適合差分:

- 現在の raw depth event は `type/bids/asks/ts/seq` の浅い event で、下記の `book_updates envelope` の `schema_version`、`event_ts_ms`、`source`、`prev_seq`、hash provenance を持たない。これは契約未達であり、P0-0 では仕様を満たす入力へ変換できたことにしてはならない。
- `lib/replay-book-state.mjs` は `effective_ts_ms` を前提にして strict `< anchor` を実装している。この境界を本契約の正本とする。
- 現在の connector/`FullBook` は exchange ごとの sequence 検査・再同期を持つが、raw replay 共通の gap quarantine 契約ではない。TFP は connector の online 状態を信頼せず、raw envelope の sequence を再検証する。
- 既存 `features_1s` は #13 を `null`、#14--#22 を `0` とする P1 契約である。book MVP の実値はこの P1 placeholder を上書きせず、別の候補列として出す。P4 の列昇格は別承認とする。

## 2. 境界、責務、writer

### 2.1 Receiver

Receiver が行うのは次だけである。

1. connector から受信する。
2. 受信 event を raw JSONL に append する。
3. `RawRotationWriter` の 30 秒 path/rotation/flush/recovery を行う。

Receiver は snapshot 適用、update の順序付け、gap 判定、feature 計算、quality の補完、quarantine の判断を行わない。受信 event が不完全でも、正規化可能な最小範囲を超えて意味を補ってはならない。

### 2.2 TFP

TFP は market ごとに一つの process/cursor/checkpoint/output writer を持つ single-writer である。同一 `absolute outputRoot + schema_version + market` を lock identity とし、live/backfill/manual/cron は同じ namespace を共有する。lock 取得失敗は `blocked/no-write` とし、自前 PID/mtime stale 判定は禁止する。

TFP の処理順は次で固定する。

1. raw block を read-only で取得し、bytes と SHA-256 を保存する。
2. envelope を parse/validate する。
3. `event_ts_ms`、sequence、file path、line number で deterministic order を作る。
4. snapshot/update を state に適用する。
5. 指定 anchor で strict `< anchor` の state を lookup する。
6. trade burst と board 候補を同じ 1 秒 row に計算する。
7. quality/provenance/hash を出力する。
8. gap/malformed/hash 不整合などで契約を満たせない block は output を commit せず quarantine する。

TFP は raw を edit、rewrite、delete、sort、truncate してはならない。派生内の stable sort は許可するが、元 raw bytes と hash を保持する。

## 3. Raw block と immutable 条件

入力 path は次である。

```text
data/live_v3/trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
data/live_v3/book_updates/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
```

30 秒 block の時間範囲は `[block_start_ms, block_start_ms + 30000)` である。trade の `ts` と book の `event_ts_ms` は block 内にあり、block 境界を含む左閉右開でなければならない。

- 空の既存 file は `valid-empty`。これは観測されたゼロ量であり、quarantine しない。
- file が存在しない場合、live では finalized-through より前なら `verified-missing`、後なら `not-yet-arrived`。backfill では frozen inventory に列挙されているのに無ければ `verified-missing`。
- JSON parse error、末尾 partial line、必須 field 欠落、数値不正、block 範囲外、raw hash 不一致は `malformed` または `hash_mismatch` として quarantine。空 block に置換しない。
- raw block の hash は file bytes の SHA-256（UTF-8 bytes）で、JSON を parse/re-serialize した値の hash ではない。

## 4. `book_updates` envelope v1

book raw の各 JSONL 行は、次の top-level object でなければならない。未知の top-level field は保持してよいが、意味付けしない。

```json
{
  "schema_version": "book_updates_v1",
  "market": "binance_spot",
  "type": "snapshot",
  "event_ts_ms": 1000,
  "seq": 100,
  "prev_seq": null,
  "bids": [["100", "2"]],
  "asks": [["101", "3"]],
  "source": {"exchange": "test", "channel": "book"}
}
```

固定規則:

- `schema_version` は文字列 `book_updates_v1`。
- `market` は path の market と一致。
- `type` は `snapshot` または `update` のみ。`delete` は envelope type として使用しない。削除は update の qty `0` で表す。
- `event_ts_ms` は有限整数。event-time として使い、wall clock は使わない。
- `seq` は null または 0 以上の整数。sequence を持たない source は null とする。推測した sequence は禁止。
- `prev_seq` は null または整数。source が提供する直前 sequence をそのまま保存する。TFP が後から補完しない。
- `bids`、`asks` は `[price, qty]` の配列。price/qty は raw 表現を保持する numeric string。price は正、qty は 0 以上。qty `0` はその price level の削除。
- snapshot は state 全置換。空 side は空配列で明示できる。
- update は指定 level だけを set/delete し、指定されない level を変更しない。
- snapshot の `seq` が nullなら state は seeded になるが、sequence continuity の検査対象にはならない。
- source の timestamp が秒/μs/ns の場合は Receiver で意味を変えず raw 保存し、TFP の入力 adapter が `event_ts_ms` を明示的に生成した envelope だけを受け入れる。曖昧な単位は malformed。

現在の connector が出す `type/bids/asks/ts/seq` は adapter で envelope へ写像するまで raw contract v1 ではない。adapter は `ts` の単位、source、seq semantics を決めて provenance に残すこと。

## 5. Replay state と sequence

state は `seeded`、bid price→qty map、ask price→qty map、`last_seq`、`last_event_ts_ms`、`sequence_status` を持つ。

### 5.1 適用

- `snapshot`: bids/asks map を clear して全置換し、`seeded=true`。seq が non-null なら `last_seq=seq`。
- `update`: `seeded=false` の間は map に適用しても feature state として公開しない。seq continuity が正常で、snapshot 後である場合だけ公開可能。
- qty `>0`: set。qty `=0`: delete。負数、非有限値は malformed。
- best bid は全 bid の最大 price、best ask は全 ask の最小 price。mid は双方が存在するときだけ `(best_bid + best_ask) / 2`。
- crossed book（best bid >= best ask）は state を公開せず `quarantine`。既存 state を都合よく修正しない。

### 5.2 sequence gap

`seq` が null の source では `sequence_status="unsequenced"` とし、gap 判定を行わない。ただし provenance には unsequenced を残す。

`seq` がある source では次を適用する。

- snapshot は seed point。snapshot seq `S` の直後に許される update は `seq=S+1`。source が range を送る場合は `prev_seq=S` かつ `seq>S` で `prev_seq` が直前 `last_seq` と一致すれば許可する。
- update は `prev_seq` が non-null の場合、`prev_seq === last_seq` でなければ gap。`prev_seq` が nullでも、単一 sequence source では `seq === last_seq + 1` でなければ gap。
- `seq <= last_seq` は duplicate/stale。state を変更せず `stale_duplicate` として quality に記録する。block が stale だけなら quarantine しない。
- `seq > last_seq + 1`、`prev_seq !== last_seq`、snapshot 後の最初の update が bridge 不成立の場合は `sequence_gap`。その block の book-derived value は計算せず、block output は quarantine、cursor は進めない。
- gap 後に後続 snapshot が同じ raw inventory 内で現れても、gap 区間を隠して自動回復してはならない。snapshot 自体を別の valid seed として記録するが、block は quarantine のままにする。

## 6. event-time、anchor、horizon

### 6.1 anchor 境界（正本）

book state lookup は **strict `< anchor_ts_ms`** である。`event_ts_ms === anchor_ts_ms` の event はその anchor には含めず、`anchor_ts_ms + 1` 以降の lookup に含める。`<=` を併用してはならない。

1 秒 row の anchor は `second_ts`（`block_start_ms` から 1000 ms 刻み）である。よって row `[second_ts, second_ts+1000)` の trade/burst 集計と、book state at `second_ts` は同じ左閉右開の定義を使う。row 内の event は次の row の book state に影響する。

book の event が row anchor の後にしか無い場合、book 値は未観測であり、前の state を未来へ先読みしてはならない。

### 6.2 finalized horizon と frozen inventory

- live: `finalized_through_ms` が権威。処理対象 block は `block_end_ms <= finalized_through_ms` のものだけ。horizon より先は `not-yet-arrived` であり、EOF でも empty でもない。
- backfill: frozen inventory が列挙した block identity（market、kind、block_start、raw hash）だけを処理対象とする。inventory にない block は推測して生成しない。
- `frozen inventory` または live の finalized-through が示す境界を越えて EOF flush してはならない。
- horizon/inventory 内で file が欠落・不正なら error/quarantine。horizon 外の欠落は blocked/pending とし、synthetic empty shard を作らない。
- 本契約は 1 秒 canonical row と book replay に限る。30 秒 feature、5 分 feature、rollup、inventory format の永続 schema は対象外である。

## 7. null / 0 / unavailable / quarantine

| 表現 | 意味 | 例 |
|---|---|---|
| `null` | 値の概念はあるが、seed 未完了・適用不能・この phase では未計算 | unseeded の best bid、#13 P1 |
| `0` | 観測された量がゼロ、または既存契約で明示された placeholder | qty 0 delete 後の depth、#14/#15--#22 P1 |
| `unavailable` | quality/status の状態名。値を 0 に変換しない | source が unsequenced、prior mid 不在 |
| `quarantine` | 出力を commit してはならない block 状態 | malformed、hash mismatch、sequence gap、crossed book |

unseeded は `best_bid/best_ask/mid=null`、board candidate は null、`book_status="unseeded"`。sequence gap は同じ null ではなく `book_status="quarantine"` で block fail。unavailable は正常な未観測であり、quarantine ではない。trade-only #1--#12 は book の unavailable/gap によって null化しない。既存の #13=`null`、#14--#22=`0` は互換のため維持する。

## 8. quality / provenance / hash

各 1 秒 row の `_quality` は少なくとも次を持つ。

```json
{
  "contract": "tfp_book_contract_v1",
  "book_status": "seeded|unseeded|unsequenced|stale_duplicate|quarantine",
  "sequence_status": "ok|unsequenced|stale_duplicate|gap|malformed",
  "trade_count_this_second": 0,
  "input_block_ids": ["trades/1970-01-01/00-00-00.jsonl", "book_updates/1970-01-01/00-00-00.jsonl"],
  "raw_sha256": {"trades/...": "<sha256>", "book_updates/...": "<sha256>"},
  "raw_sha256_present": true,
  "reason_code": null,
  "line_no": [1, 2],
  "event_payload_sha256": ["<sha256_of_event_json>"],
  "book_event_count_applied": 0,
  "book_event_count_ignored": 0,
  "anchor_rule": "event_ts_ms < anchor_ts_ms",
  "provenance": {"book_source": "raw_book_updates", "adapter": "<name>@<version>"}
}
```

- `raw_sha256` は入力 bytes の hash。派生 JSON hash は `derived_sha256` として別名にする。
- input block id、line number、event sequence、snapshot/update type、source exchange/channel を、quarantine record と manifest に残す。
- hash 欠落は「hash が無いだけの valid」とせず `hash_unavailable` quality とし、P0 commit は禁止する。既存 raw の hash を計算できる reader が責務を持つ。
- quality は値本体の代替ではない。quality が `ok` でない値を 0 に丸めない。

## 9. Trade-only #1--#12 互換

P0-0 の book contract は既存 trade-only の意味を変更しない。

- #1--#11 は既存 `feature-computer-1s.mjs` の burst 集計をそのまま使う。
- #12 `burst_notional_vs_30s_traded_notional` は raw trades の `[second_ts - 30000, second_ts)` の notional を分母にし、分母 `<=0` は既存契約どおり `0`。
- book の unseeded/unavailable/gap は #1--#12 の計算を黙って除外する理由にならない。
- #13 `burst_notional_vs_top_depth` は現 P1 row では `null`、#14 `burst_mid_move_bps_1s` と #15--#22 は現 P1 row では `0`。これら既存位置を board MVP 実値で上書きするのは本仕様の範囲外。

## 10. Board MVP 候補（値、式、責務）

P0-0 は候補の意味と検証ベクトルを固定するが、既存 #13/#14 の昇格や本番出力 wiring は行わない。候補は 1 秒 row に別名で出せる設計とする。

### 10.1 `board_top_depth_ratio`

- `top_depth_levels = 1` 固定。
- `top_depth_notional = best_bid * best_bid_qty + best_ask * best_ask_qty`。
- `board_top_depth_ratio = burst_notional_1s / top_depth_notional`。
- 分母が 0、未seed、best side/qty 不在、crossed、quarantine は `null`。分母が正で burst notional が 0 の観測結果は `0`。
- 責務: replay state が best side と qty を提供し、feature stage が同一 anchor の burst notional を分子にする。depth stage が trade を再計算してはならない。

### 10.2 `board_mid_move_bps`

- `mid(anchor) = (best_bid(anchor)+best_ask(anchor))/2`。
- `prior_mid` は直前 1 秒 anchor の mid。`board_mid_move_bps = (mid(anchor)-prior_mid) / prior_mid * 10000`。
- 現 anchor または prior anchor が unseeded/unavailable、prior mid `<=0`、crossed、quarantine は `null`。変化なしは観測された `0`。
- 責務: replay stage が anchor ごとの mid lookup を提供し、feature stage が差分だけを計算する。event-time の無い wall-clock mid を使わない。

### 10.3 `board_vs_30s`

- 既存 #12 と同じ `burst_notional_1s / traded_notional_raw_[anchor-30000,anchor)`。
- denominator `<=0` は `0`（観測された期間内の約定なし）。trades raw が missing/malformed/hash mismatch なら `0` にせず block quarantine。
- 責務: raw trades reader が分母、burst stage が分子を持つ。book stage は関与しない。

### 10.4 `board_vs_depth`

- `board_vs_depth = burst_notional_1s / top_depth_notional`。`board_top_depth_ratio` と同一式・同一 null 規則で、consumer 用の明示名である。
- 別の depth 定義（数量合計のみ、N-level、percent depth）を暗黙に導入しない。N-level は別仕様で決める。

## 11. malformed と quarantine record

quarantine は `<outputRoot>/quarantine/<market>/<block_start_ms>.json` に no-clobber で保存する。record は `contract_version`、market、block、reason code、raw input paths、raw_sha256（取得できた場合）、line_no、event payload の hash、処理時刻、既存 cursor を持つ。reason code は少なくとも `MALFORMED_ENVELOPE`、`MALFORMED_LEVEL`、`EVENT_OUT_OF_RANGE`、`SEQUENCE_GAP`、`HASH_MISMATCH`、`CROSSED_BOOK`、`MISSING_FINALIZED_INPUT` とする。

quarantine した block は feature shard、manifest commit、checkpoint cursor advancement を生成しない。再処理は raw を修正せず、別の adapter/spec version または正しい frozen inventory で明示的に行う。

## 12. P0-0 検証ベクトルと受入条件

`docs/fixtures/tfp-book-contract-vector-v1.json` が正本の最小 golden vector である。実装者は次を全て再現できなければならない。

1. snapshot 適用後の best bid/ask/qty、mid、top depth を計算できる。
2. update の set/delete を適用できる。
3. anchor と同時刻の event を除外し、anchor より 1 ms 後には含める（strict `<`）。
4. snapshot 前 update は unseeded のまま公開しない。
5. sequence gap は state/output を quarantine し、0 や empty に置換しない。
6. malformed envelope は line と reason を特定して quarantine する。
7. trade-only #1--#12 の #12 分母と board 候補の式を手計算値で一致させる。
8. raw input の bytes hash、provenance、quality status を expected と一致させる。

P0-0 の完了条件は、production code の変更ではなく、仕様と fixture の独立検証可能性である。30 秒/5 分 output の追加、既存 #13/#14 の実値化、Receiver への責務移動は完了条件に含めない。

## 13. P0-0 FAIL 修正 addendum（正本）

sound­ing-board の指摘に対する明示的な補正である。本節と fixture の同名フィールドが、上記の曖昧な記述に優先する。

### 13.1 fixture の trade/features 突合

fixture の trade は `1000ms buy 10` と `1500ms sell 10`。既存 `feature-computer-1s.mjs` / `BurstBuilder` の規約（同一 side、隣接 gap `<=50ms`、bucket overlap `start < end && end >= start`）では、二つは別 burst で、`second_ts=1000` の row にだけ overlap する。従って正しい値は次である。

- `feature_computer_1s_row_at_1000.burst_notional_1s = 20`、`burst_count_1s = 2`、`traded_notional_30s = 30`、`burst_notional_vs_30s = 2/3`。
- `feature_computer_1s_row_at_2000.burst_notional_1s = 0`。2000ms row に 1000/1500ms burst を先読みしない。
- `board_*` は既存 #1--#12 の列ではなく、別名・別責務の候補集計である。候補は row の burst 集計と `book_state_at_anchor` を組み合わせるが、既存列を上書きしない。
- anchor 1000 の book は 500ms snapshot のみ（1000ms event は strict `<` で除外）なので top depth は `100*2+101*3=503`。anchor 2000 は update 後で `101*2+102*3=508`。

### 13.2 deterministic event ordering

全 event の sort key は次の 5 要素をこの順で比較する。

```text
(event_ts_ms ASC,
 type_priority ASC,                 # snapshot=0, update=1
 sequence_or_range_start ASC,       # seq または seq_start; null は最後
 file_path ASC,
 line_no ASC)
```

同一 key も入力順を保持する stable sort とする。`event_ts_ms` が raw line 順で逆転した場合は raw bytes/line_no を変更せず、この key で replay し、`reordered_input=true` と逆転数を provenance に記録する。wall-clock は ordering に使わない。event-time が block 範囲外、非有限、または単位不明なら `malformed` であり sort して救済しない。

### 13.3 adapter の抽象 sequence 契約

adapter が canonical envelope に出す sequence 情報は `seq`、`prev_seq`、任意の `seq_start`/`seq_end`、`bridge_kind` と provenance (`source`, adapter name/version, source field mapping) である。

| source 形 | canonicalize | bridge 条件 |
|---|---|---|
| single seq `S`、prev `P` | `seq=S, prev_seq=P` | `P==last_seq`。prev が無い single-seq source は `S==last_seq+1` のみ許可 |
| range `A..B`、prev `P` | `seq=B, seq_start=A, seq_end=B, prev_seq=P` | `P==last_seq` かつ `A==last_seq+1` |
| snapshot seq `S` | seed `seq=S, bridge_kind=seed` | snapshot は seed。snapshot 後の初回 update は上記 single/range 条件が必要 |
| envelope で表現不能/opaque | seq 系を null のまま | `unsequenced`。source mapping/provenance は必須、推測禁止 |

range の `seq_end` を単一 seq として扱うことは許可するが、`seq_start` を provenance から落としてはならない。gap 後の snapshot は新しい seed として記録できるが、同一 raw inventory 内の既発生 gap と quarantine を消去しない。

### 13.4 状態遷移、commit、cursor（kind/policy 対応）

| state | 判定 | kind/policy | commit | cursor | quarantine |
|:---|---|---:|---|---:|
| `assumed-empty-gap` | trade-only proven absent lookback; exists=false, horizon=true, kind=trade | trade-only | yes | advance | no; zero contribution |
| `valid-empty` | file が存在し parse 可能な空 block | any | yes | advance | no |
| `verified-missing` | authoritative horizon/inventory 内で file が欠落・不正（kind=book_updates または default） | book_updates | no | retain | yes |
| `not-yet-arrived` | horizon 外、または horizon proof が無い | any | no | retain pending | no; `no-horizon-proof` |
| `malformed` | parse/field/range/level 不正 | any | no | retain | yes |
| `hash_mismatch` | raw bytes SHA-256 が inventory/manifest と不一致 | any | no | retain | yes |
| `unsequenced` | source seq が null で推測不能 | any | yes（他条件が valid） | advance | no; book value は unavailable |
| `sequence-gap` | bridge/continuity 不成立 | any | no | retain | yes |
| `unknown-input` | null/undefined/{} が computeBlockOutcome または processBlock に渡された | any | no | retain | no; blocked_reason=unknown-input |

`valid-empty` の zero は観測された空量であり、`verified-missing`/`not-yet-arrived` を空に置換しない。既存 TFP の `ASSUMED_EMPTY_GAP` は、既存 trade-only #12 lookback の proven absent block を zero contribution として commit できる既存意味に限定する。book block の synthetic empty や sequence-gap の自動回復は許可しない。horizon proof が無い EOF は `no-horizon-proof` のまま pending cursor を保持する。quarantine は feature shard、manifest commit、checkpoint cursor advancement を全て禁止する。

### 13.5 fixture provenance と独立 verifier

fixture は raw JSONL bytes の SHA-256、path、line_no、seq/prev/range、snapshot/update、source、adapter version、hash mismatch、frozen inventory を含む。`test/tfp-book-contract-fixture.test.mjs` は fixture を直接読み、手計算の replay/order/state/feature 式だけで検証する。production の replay、BurstDetector、feature computer、pipeline、connector は import しない。

独立 verifier は同時刻 priority、out-of-order、strict anchor、valid-empty、verified-missing、not-yet-arrived、malformed、hash mismatch、unsequenced、sequence gap、crossed book、frozen inventory、quarantine no-commit と raw hash を検証する。これにより fixture の期待値生成と production replay の同一実装依存を避ける。

## 13.6 Frozen inventory と ASSUMED_EMPTY_GAP の契約明確化

### 13.6.1 frozen inventory の kind 分離

既存 TFP trade-only pipeline の frozen inventory は `trades` kind を管理する。P0-0 book contract adapter は `book_updates` kind を別 namespace として扱う。これにより：

- trade-only pipeline と book adapter は同一 raw inventory 構造を共有するが、kind が異なるため相互に干渉しない。
- `frozen_inventory.blocks[].kind` が `book_updates` であることと、trade-only の `trades` が別であることを明示する。
- book_updates の missing/hash mismatch/undeclared block は book adapter の quarantine ロジックが単独で判断する。trade-only inventory の存在を理由に book block の missing を無視しない。
- P0-0 は book adapter の kind 分離と独立検証を固定するが、本番 pipeline 統合（trade-only pipeline への book_updates 処理の組み込み）は完了条件に含めない。「book adapter が別 kind を扱う未実装」を明示し、P0-0 で production 対応済みと誤認させない。

### 13.6.2 ASSUMED_EMPTY_GAP の trade-only 限定

`ASSUMED_EMPTY_GAP` は既存 TFP trade-only の proven absent block（例：#12 lookback 範囲内で block が存在しない場合の zero contribution commit）に限定する。以下の拡張は許可しない。

- book block の sequence gap を ASSUMED_EMPTY_GAP で自動回復しない。
- book block の verified-missing を empty shard に置換しない。
- book_updates の未観測 range に synthetic zero-depth を生成しない。

book の sequence gap は常に quarantine であり、trade-only の ASSUMED_EMPTY_GAP とは異なる commit/cursor 規則に従う。独立 verifier はこの差を実計算で確認する。

## 13.7 Independent verifier computation rules（正本）

以下は `test/tfp-book-contract-fixture.test.mjs` の独立 verifier が実計算すべき rule を明示する。fixture の expected は最終比較のみに使われ、verifier は expected の宣言値を直接読んで判断しない。

### 13.7.1 入力状態判定（input → state）

verifier は `computeBlockOutcome(options)` で以下の入力を組み合わせて状態を導出する。

| input conditions | derived blockState | commit | cursor | quarantine | state/feature公開 |
|---|---|---|---|---|---|---|
| kind=trade, exists=false, horizon=true | assumed-empty-gap | true | advance | no; zero contribution | null |
| exists=false, horizon=true (kind=book_updates または default) | verified-missing | false | retain | yes (MISSING_FINALIZED_INPUT) | null |
| exists=false, horizon=false | not-yet-arrived | false | retain | no (no-horizon-proof) | null |
| exists=true, parse_ok=false | malformed | false | retain | yes | null |
| exists=true, parse_ok=true, events=[] | valid-empty | true | advance | no | 空state |
| null/undefined/{} | unknown-input | false | retain | no; blocked_reason=unknown-input | null |
| exists=true, parse=true, sha256(raw)≠expected | hash_mismatch | false | retain | yes | null |
| exists=true, parse=true, events=[...] | sm 処理結果 | sm 決定 | sm 決定 | sm 決定 | sm 決定 |

gap/malformed/crossed 後は `state=null, feature=null` であり、commit=false だけでは不十分である。`valid-empty` は空 state だが commit/cursor advance は許可する。

### 13.7.2 BookStateMachine range event bridge

range event は以下の条件を全て満たすときのみ bridge 成立とする。

- `prev_seq === last_seq`（直前 sequence との一致）
- `seq_start === last_seq + 1`（連続性）
- `seq_end >= seq_start`（範囲の妥当性）

single seq でも `prev_seq` 有りの場合は `prev_seq === last_seq` かつ `seq === last_seq + 1`。`prev_seq` null の場合は `seq === last_seq + 1` のみ。どちらも不成立は gap。

### 13.7.3 公開 state の null/unavailable

quarantine 状態（gap, malformed, crossed）では：
- `snapshotState()` は `null` を返す（内部 bids/asks map は保持しても公開しない）
- feature（board 候補）は `null`
- これにより consumer が quarantined state を誤って使用するのを防ぐ

stale/duplicate では:
- 適用前後の `best_bid/best_ask/last_seq` が同一であることを assert する
- state は変更されない

### 13.7.4 Computed quality/provenance

verifier は events と state machine から `_quality` object を実計算する。

- `contract`: `"tfp_book_contract_v1"` 固定
- `input_block_ids`: events の `path` の unique set
- `raw_sha256`: `fixture.raw_block` の `{path: sha256}` map（events からではなく fixture の raw block metadata から）
- `provenance.book_source`: `"raw_book_updates"`
- `provenance.adapter`: events の `source.adapter@source.adapter_version` を抽出

quarantine case では `line_no`、`event payload hash`、`reason code` も provenance に含める。

### 13.7.5 Hash mismatch の実計算

- `computedActual = sha256(input.raw_content)` で actual hash を計算する
- `input.expected_sha256` は inventory が持つ正しい hash
- `computedActual !== expected_sha256` → hash_mismatch を導出する
- fixture の `expected.actual_sha256` と computedActual を突合する（固定ゼロ値の宣言比較をしない）

### 13.7.6 Frozen inventory matching（kind 対応）

verifier は以下を独立に計算する。

1. `declared_existing`: inventory に宣言され、raw_input_map に存在し、`raw.kind===block.kind && raw.sha256===block.sha256 && raw.path===block.path` を全て満たす block
2. `declared_missing`: inventory に宣言されたが raw_input_map に存在しない block
3. `hash_mismatch_paths`: inventory に宣言され、raw_input_map に存在するが hash が不一致の block
4. `kind_mismatch_paths`: inventory に宣言され、raw_input_map に存在するが kind が不一致の block
5. `undeclared_present`: raw_input_map に存在するが inventory に宣言されていない block

`kind_mismatch_paths` は kind 不一致を明示的に分離する。上記 2・3・4 のいずれかが空でない場合、`matchFrozenInventory` の返す overall commit=false, cursor=retain, quarantine=true となる。

### 13.7.7 ASSUMED_EMPTY_GAP の差（kind 分離）

trade-only の proven absent lookback は `kind=trade, inside_authoritative_horizon=true, exists=false` を `assumed_empty_gap` として commit=true, cursor=advance, zero contribution で扱う（既存 TFP 互換）。book block の同条件（`kind=book_updates`）は `verified-missing`（horizon=true）として commit=false, quarantine=true。horizon 外の book block は `not-yet-arrived`（horizon=false）として commit=false であり、synthetic empty に置換しない。verifier は両者の差を以下のように実計算する。

- trade scenario: kind=trade, exists=false, horizon=true → commit=true (assumed_empty_gap, zero contribution)
- book scenario: kind=book_updates, exists=false, horizon=false → commit=false (not-yet-arrived, no synthetic empty)
