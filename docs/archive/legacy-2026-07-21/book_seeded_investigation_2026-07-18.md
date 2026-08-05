# book_snapshots seeded=false 原因調査レポート

**日付:** 2026-07-18
**対象:** book_snapshots / features_1s のbook特徴量

---

## 1. 現状データ（直近24h）

### book_snapshots seeded率

| market | seeded | total | 率 |
|--------|--------|-------|-----|
| binance_perp | 30 | 70,890 | 0.04% |
| binance_perp_btcusdc | 0 | 70,950 | 0.0% |
| binance_spot | 120 | 70,980 | 0.2% |
| binance_spot_usdc | 90 | 71,010 | 0.1% |
| bitfinex_spot | 480 | 69,810 | 0.7% |
| bitmex_perp | 150 | 67,110 | 0.2% |
| bitstamp_spot | 30 | 45,240 | 0.07% |
| bybit_perp | 60 | 72,780 | 0.08% |
| bybit_spot | 30 | 73,560 | 0.04% |
| coinbase_spot | 30 | 72,360 | 0.04% |
| crypto_com_spot | 300 | 121,740 | 0.2% |
| hyperliquid_perp | 120 | 123,480 | 0.1% |
| kraken_spot | 30 | 71,940 | 0.04% |
| okx_perp | 60 | 123,690 | 0.05% |
| okx_spot | 60 | 123,690 | 0.05% |
| **TOTAL** | **1,590** | **1,249,230** | **0.1%** |

### features_1s book特徴量 B1-B9 null率

| market | rows | any_null | 率 |
|--------|------|----------|-----|
| binance_perp | 2,130 | 2,100 | 98.6% |
| binance_perp_btcusdc | 2,130 | 2,130 | 100% |
| binance_spot | 2,130 | 2,040 | 95.8% |
| bitfinex_spot | 2,040 | 1,830 | 89.7% |
| **TOTAL** | **31,260** | **30,300** | **96.9%** |

全9特徴量（B1-B9）が一律96.9% null → book未seedと同一パターン。

---

## 2. 根本原因

### REST upsertがWS stateを破壊する

**症状:**
- WS updatesだけで処理 → seeded=True（正常）
- REST upsert後 → best_bid > best_ask（crossed!）→ seeded=False

**再現ログ（binance_spot例）:**

```
Block 20:59:30: 300 WS updates
  BEFORE WS apply: seeded=False bids=0 asks=0 bb=0.0 ba=0.0
  AFTER  WS apply: seeded=True  bids=275 asks=242 bb=64120.0 ba=64120.0  ← 正常
  AFTER  REST upsert: seeded=False bids=5220 asks=5201 bb=64146.0 ba=64120.0 (crossed!)  ← 破壊
  SNAPSHOTS: 0/30 seeded
```

**原因:**
1. REST snapshotは古いprice levelを含む（limit=5000等で広範囲）
2. WSで構築したbest ask（64120.0）より高いbid（64146.0）がRESTに含まれる
3. REST upsertでbid=64146.0が追加 → best_bid > best_ask → crossed
4. crossed状態になると、その後のWS updatesだけでは復活しない
   - 古いlevelが残り続けるため、next blockでもcrossed継続

**影響範囲:**
- 全15 market
- 99.9%のbook_snapshotsがunseeded
- 96.9%のfeatures_1s book特徴量がnull
- 実質的にbookデータが機能していない

---

## 3. Node側（burst-agg.mjs）との比較

Node側は異なるアプローチ:
- `scripts/burst-agg.mjs`はbook eventsを直接replay
- REST upsertは使用せず、snapshotファイルがあればそれをseedとして使用
- crossed bookは`sanitizeBookState()`でnull扱い

→ **Node側はREST upsert問題を回避している**

---

## 4. 修正方針

### 最小修正: REST upsertを「seed only」に変更

**修正案:**
- 既にWSでseededなら → REST upsertを**skip**
- 未seedの場合のみ → RESTで初回seed

**理由:**
- WS stateを破壊しない
- 未seed marketだけRESTで補完
- 最小限のコード変更（`scripts/downstream.py`の1ブロック）

**実装:**
```python
# scripts/downstream.py の process_block() 内
if seed_rest and not br._seeded:  # 未seedの場合のみREST適用
    from lib.downstream.rest_book import fetch_rest_book
    rest_bids, rest_asks = fetch_rest_book(market) or ({}, {})
    if rest_bids or rest_asks:
        merge_update = {
            "bids": [[str(p), str(q)] for p, q in rest_bids.items()],
            "asks": [[str(p), str(q)] for p, q in rest_asks.items()],
            "ts": 0,
        }
        br.apply_json(merge_update)
```

**検証:**
- 既存テスト（63件）がpassすることを確認
- 実データで再処理してseeded率が改善することを確認

---

## 5. 影響と制約

**変更ファイル:**
- `scripts/downstream.py`（1箇所）

**影響:**
- 実運用中的数据が変わる可能性 → **要再処理**
- 既存cursorをリセットして全ブロック再処理が必要

**制約:**
- Receiver/downstream/Gateway/Hermesの再起動は禁止（タスク要件）
- commit/pushも禁止
- 修正はコードベースのみ、反映は別途手動

---

## 6. 代替案（検討のみ）

### 案A: REST levelを「WS範囲内」に制限
- RESTからcurrent mid±$10000のlevelのみ適用
- 古い遠距離levelを除外
- 実装複雑、WS stateとの整合性保証が難しい

### 案B: REST upsertを完全廃止
- WS updatesだけで運用
- 未seed marketは永远にunseededのまま
- 簡単だが機能性低下

### 案C: REST upsertを「replace」に変更
- WS stateを全clearしてRESTで置換
- WSの增量性が失われる → 却下

→ **最小修正案（seed only）を採用**
