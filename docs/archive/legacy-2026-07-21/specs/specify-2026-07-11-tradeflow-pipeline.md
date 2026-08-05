# TradeFlow Pipeline（TFP）仕様

## 1. 目的

**TradeFlow Pipeline（TFP）** は、Receiver が保存した不変 raw trade block を市場ごとに直列処理し、burst 形成・1秒特徴量・時間集約を一つの downstream worker で生成する派生処理である。

- Receiver は receive + raw save のみを担当する。
- TFP は raw を変更・削除しない。
- burst は独立サービスではなく TFP 内の論理段階である。
- 30秒・5分などの集約も、必要な場合は同じ TFP worker の後段で行う。

## 2. 名称と用語

- **TFP worker**: market ごとの単一 writer プロセス。
- **Raw block**: `data/live_v3/trades/<market>/<UTC date>/<HH-MM-SS>.jsonl` の30秒入力。
- **Burst stage**: 時系列 trade から burst を形成する内部段階。
- **Feature stage**: burst と raw の情報から30行の1秒 feature shard を作る内部段階。
- **Rollup stage**: 1秒 feature を30秒・5分等へ畳み込む任意の内部段階。初期導入では output を要求しない。
- **Commit stage**: staged shard、intent、checkpoint、committed manifest を耐久化する段階。

## 3. 処理モデル

```text
Receiver -> immutable raw blocks
                 |
                 v
TFP worker (market ごと single writer)
  validate -> burst -> 1s feature -> optional rollup -> atomic commit
```

TFP は一つの process / cursor / checkpoint / market lock を使う。burst 中間ファイルを永続化して別 worker へ渡すことは、外部 consumer・監査要件・独立再計算要件が明示されるまで行わない。

## 4. 安全契約

1. `flock -x -n <output_root>/locks/<market>.lock` を保持できた worker だけが、その market の checkpoint / manifest / output を変更できる。
2. checkpoint が存在する場合、checkpoint cursor は権威的である。通常 CLI `--from` は cursor より後へ skip させてはならない。
3. block の欠損・部分書込み・hash 不一致は空 block と扱わない。finalized horizon の内側なら quarantine、外側なら blocked とする。
4. EOF flush は frozen inventory または30秒境界の `--finalized-through` の証明がある場合だけ許可する。
5. intent / final shard / checkpoint / committed manifest の不整合は fail-closed で quarantine する。存在確認だけで commit 完了とみなさない。
6. checkpoint には open burst continuation state と nextId だけを保存する。closed burst 履歴、prints、same_price_runs は保存しない。
7. in-memory closed burst は feature stage が必要とする時間窓を超えたら prune する。prune 前後で feature 出力は不変でなければならない。
8. commit は shard hash、input hash、checkpoint generation を記録し、同一 composite key を二重 commit しない。

### 4.1 Raw gap の「データなし」ポリシー

raw block が存在しないことは、以後 **市場データなし（valid-empty）** と解釈する。これは入力破損や不正 JSON とは異なる。

- 欠落した block 自体の feature shard は生成しない。TFP は実在する raw block だけを出力する。
- 存在する block N の #12 lookback に必要な直前 block が存在しない場合、その block の寄与をゼロとして計算を続行する。
- block scanner 上で N と次の実在 block N+k の間に gap があっても、k-1 block を data-none として扱い、N を commit して N+k を pending に進める。
- data-none を推測した場合、structured `ASSUMED_EMPTY_GAP` log と manifest record の `assumed_empty_input_blocks` に、推測した block start ms を残す。推測内容を黙って捨てない。
- **空ファイルは valid-empty**。一方、存在するファイルの JSON 不正、timestamp 範囲外、price/qty 不正は従来どおり E007 fail-closed とする。
- `--finalized-through` / frozen inventory は raw gap を error に昇格させない。データ欠損を埋めたように見せる synthetic shard は生成しない。

### 4.2 Block 内 timestamp 逆転の正規化

存在する raw block の trade timestamp が行順で減少しても、各 trade が block 範囲内かつその他の値検証を通る場合は、raw 原本を変更せず、派生処理内で **`ts` 昇順・同一 timestamp は元の行順** の stable sort を行う。

- `E001`–`E003`、E005 は引き続き fail-closed。順序だけを理由に E004 停止しない。
- parse result は `reordered_input`、`timestamp_inversion_count`、raw SHA-256 を返す。
- その block が candidate / pending / lookback として処理される際、structured `ASSUMED_REORDERED_INPUT` log と committed manifest record の `reordered_input`、`timestamp_inversion_count` に記録する。
- 監査 record は元 raw を書換えず、入力 hash と共に正規化の事実だけを記録する。

## 5. 出力契約

- 現フェーズは trade-only feature #1–#12。
- 1 commit は対象30秒に対応する30行の1秒 JSONL shard を生成する。
- #13 は null、#14–#22 は既存の P1 placeholder 契約を維持する。
- Rollup stage を追加しても raw / 1秒 feature の意味・既存 path は変更しない。
- Rollup の永続出力、schema、consumer は別仕様が承認されるまで導入しない。

## 6. 最小実装スコープ

### Phase A: TFP safety completion（今回実装対象）

- P1-2 bounded retention
- committed-state を含む crash recovery
- manifest 破損を fail-closed で保全
- full-state deep clone の削除
- lock / recovery / retention / cursor-skip の回帰テスト

### Phase B: TFP rollup（今回の対象外）

30秒・5分集約の output schema、consumer、再計算方針が確定後に同一 worker の内部 stage として追加する。別サービス化は要求しない。

## 7. 受入基準

Phase A は次をすべて満たすまで未完了。

- 全 unit/golden test が PASS。
- retention を含む長系列テストで closed burst 保持数が境界内。
- checkpoint は 64KiB 以下、pending/open state は各256KiB以下。
- crash point ごとの restart で shard 1個、committed composite key 1個、generation 単調増加。
- committed shard / checkpoint / manifest / hash の不整合は quarantine され、cursor は進まない。
- 95点以上の独立レビュー後だけ、cron停止・隔離 output root・1 market で5分制御試験を行う。
