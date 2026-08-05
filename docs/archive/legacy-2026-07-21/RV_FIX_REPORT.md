# RV10/RV60 FIX Report — t_08d6e06f

## 問題
- `incremental_features.py:208-209` で `realized_vol_10s/60s` が `None` 固定
- `feature_compiler.py` のRV計算は indentation bug で production pipeline から呼ばれていない
- 実Parquet 15market 309,549行 でRV non-null = 0

## 実装内容

### 1. incremental_features.py のRV実装
- `_compute_realized_vol_from_series()` を追加（line 17-64）
  - Window: `[at_ts - window_ms, at_ts)` — strict past、no lookahead
  - 3 prices / 2 log-returns 未満で `None`（warmup）
  - Binary search で window start を探索
- `IncrementalFeatureComputer.__init__` に `_price_series` state を追加（line 99-100）
  - `(ts_ms, price)` tuple の list、60s window 分のみ保持
  - `process_trade()` で trade ごとに price を追加（line 114-116）
  - `_compute_features_for_second()` で RV10s/RV60s を計算（line 273-278）
  - 計算後に 60s 以前の entry を prune（line 282-292）
- `flush()` で `_price_series` をリセットしない（line 350-351）
  - cross-block で RV rolling window を維持

### 2. feature_compiler.py の整理
- `compute_1s_features()` 内のRV計算を正しい indentation に修正（line 172-173）
  - ただし production path は `IncrementalFeatureComputer` を使用（downstream.py）
  - `compute_1s_features` は batch 処理用に維持

### 3. downstream.py のper-market computer永続化
- `_FEATURE_COMPUTERS: Dict[str, IncrementalFeatureComputer]` を追加（line 66）
- `_get_feature_computer(market, tick_size)` で singleton パターン（line 69-73）
- `process_block()` で computer を再利用（line 175）

### 4. テスト
- `tests/test_orderflow_p0.py`: 15 tests（RV計算、no-lookahead、warmup、constant price、market isolation）
- `tests/test_orderflow_p0_fixtures.py`: 12 tests（手計算値との一致検証）
- 全90 Python tests pass

### 5. Parquet再生成
- 既存 features_1s を全削除せず、`--from-ms` でcursor上書きして再処理
- raw は絶対変更しない
- 11,362 parquet files、185,927 rows を生成

## 検証結果

### テスト
```
Python: 90/90 pass (0.13s)
Node:   793/794 pass (1 pre-existing failure)
npm check: pass
```

### 実Parquet RV検証（15 market）
```
Total rows:    185,927
RV10 non-null: 124,339 (66.9%)
RV60 non-null: 138,062 (74.3%)
```

| Market                | Rows  | RV10nn | RV10% | RV60nn | RV60% |
|-----------------------|-------|--------|-------|--------|-------|
| binance_perp          | 20538 | 20338  | 99.0% | 20340  | 99.0% |
| binance_perp_btcusdc  | 9078  | 2363   | 26.0% | 3742   | 41.2% |
| binance_spot          | 21823 | 21660  | 99.3% | 21662  | 99.3% |
| binance_spot_usdc     | 9121  | 3604   | 39.5% | 5419   | 59.4% |
| bitfinex_spot         | 7564  | 496    | 6.6%  | 1114   | 14.7% |
| bitmex_perp           | 4751  | 153    | 3.2%  | 398    | 8.4%  |
| bitstamp_spot         | 10349 | 490    | 4.7%  | 2630   | 25.4% |
| bybit_perp            | 11464 | 8678   | 75.7% | 9684   | 84.5% |
| bybit_spot            | 9839  | 5548   | 56.4% | 7300   | 74.2% |
| coinbase_spot         | 24743 | 24721  | 99.9% | 24722  | 99.9% |
| crypto_com_spot       | 7966  | 583    | 7.3%  | 1185   | 14.9% |
| hyperliquid_perp      | 12643 | 11138  | 88.1% | 11825  | 93.5% |
| kraken_spot           | 8999  | 1069   | 11.9% | 2911   | 32.3% |
| okx_perp              | 15895 | 15190  | 95.6% | 15323  | 96.4% |
| okx_spot              | 11154 | 8308   | 74.5% | 9807   | 87.9% |

低流動性 market（bitfinex, bitmex, bitstamp, crypto_com）で RV non-null 率が低いのは正常：
- RV は rolling window 内に 3 trades 以上必要
- trade が sparse な market では window 内に 0-2 trades の second が多い
- warmup 後も trade 不足で null になるのは spec 通り

### Schema
- 39 columns（旧33 + B1-B9 book features + P0 6 features）
- `realized_vol_10s`, `realized_vol_60s`: `float64, nullable=True`

### No-lookahead
- Window: `[at_ts - window_ms, at_ts)` — strict past
- `process_trade()` で price を追加するが、RV計算は次second boundary
- Test fixtures で未来trade混入時の動作検証済み

## 変更ファイル
- `lib/downstream/incremental_features.py` (new): RV実装
- `lib/downstream/feature_compiler.py` (modified): indentation修正
- `lib/downstream/config.py` (modified): schema v2 (39 cols)
- `scripts/downstream.py` (modified): per-market computer永続化
- `tests/test_orderflow_p0.py` (new): 15 tests
- `tests/test_orderflow_p0_fixtures.py` (new): 12 tests

## 未確認
- commit/push は task 指示で禁止
- raw data は変更していない（確認済み）
- downstream --watch は停止していない（再生成は one-shot で完了）
