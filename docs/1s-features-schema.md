# 1s Features Schema v1

## Overview

1行 = 1 market × 1 second の十分統計量。

Output: `data/agg/{market}.parquet`（market単位の single Parquet, 追記）

## Columns

### Keys
| Column | Type | Description |
|--------|------|-------------|
| `ts` | BIGINT | epoch ms, floored to second (`ts - ts % 1000`) |
| `market` | VARCHAR | e.g. `binance_perp` |
| `type` | VARCHAR | always `'1s_feature'` |

### Trade Aggregation (from raw trades in this second)
| Column | Type | Description |
|--------|------|-------------|
| `open` | DOUBLE | first trade price |
| `high` | DOUBLE | max trade price |
| `low` | DOUBLE | min trade price |
| `close` | DOUBLE | last trade price |
| `vwap` | DOUBLE | volume-weighted avg price (notional / qty) |
| `trade_count` | BIGINT | number of trades |
| `buy_qty` | DOUBLE | BTC qty bought |
| `sell_qty` | DOUBLE | BTC qty sold |
| `buy_notional` | DOUBLE | USD notional bought |
| `sell_notional` | DOUBLE | USD notional sold |
| `delta_notional` | DOUBLE | buy_notional - sell_notional |
| `buy_small_qty` | DOUBLE | BTC qty bought in trades with USD notional < $1k |
| `buy_medium_qty` | DOUBLE | BTC qty bought in trades with $1k <= USD notional < $10k |
| `buy_large_qty` | DOUBLE | BTC qty bought in trades with USD notional >= $10k |
| `buy_small_count` | BIGINT | count of buy trades with USD notional < $1k |
| `buy_medium_count` | BIGINT | count of buy trades with $1k <= USD notional < $10k |
| `buy_large_count` | BIGINT | count of buy trades with USD notional >= $10k |
| `sell_small_qty` | DOUBLE | BTC qty sold in trades with USD notional < $1k |
| `sell_medium_qty` | DOUBLE | BTC qty sold in trades with $1k <= USD notional < $10k |
| `sell_large_qty` | DOUBLE | BTC qty sold in trades with USD notional >= $10k |
| `sell_small_count` | BIGINT | count of sell trades with USD notional < $1k |
| `sell_medium_count` | BIGINT | count of sell trades with $1k <= USD notional < $10k |
| `sell_large_count` | BIGINT | count of sell trades with USD notional >= $10k |

### Top of Book (boundary state at second open/close)
| Column | Type | Description |
|--------|------|-------------|
| `mid_open` | DOUBLE | mid price at second start |
| `mid_close` | DOUBLE | mid price at second end |
| `spread_bps_open` | DOUBLE | spread in bps at second start |
| `spread_bps_close` | DOUBLE | spread in bps at second end |
| `best_bid_open` | DOUBLE | best bid at second start |
| `best_ask_open` | DOUBLE | best ask at second start |
| `best_bid_close` | DOUBLE | best bid at second end |
| `best_ask_close` | DOUBLE | best ask at second end |

### Book Depth State (notional at bps levels, sampled at second boundary)
| Column | Type | Description |
|--------|------|-------------|
| `bid_1bps` | DOUBLE | bid notional within 1bps of mid at close |
| `ask_1bps` | DOUBLE | ask notional within 1bps of mid at close |
| `bid_5bps` | DOUBLE | bid notional within 5bps |
| `ask_5bps` | DOUBLE | ask notional within 5bps |
| `bid_25bps` | DOUBLE | bid notional within 25bps |
| `ask_25bps` | DOUBLE | ask notional within 25bps |
| `bid_100bps` | DOUBLE | bid notional within 100bps |
| `ask_100bps` | DOUBLE | ask notional within 100bps |

### Book Flow (depth diff events during this second)
| Column | Type | Description |
|--------|------|-------------|
| `bid_add_qty_near` | DOUBLE | BTC qty added to bid side within 5bps |
| `bid_cancel_qty_near` | DOUBLE | BTC qty cancelled on bid side within 5bps |
| `ask_add_qty_near` | DOUBLE | BTC qty added to ask side within 5bps |
| `ask_cancel_qty_near` | DOUBLE | BTC qty cancelled on ask side within 5bps |
| `bid_add_qty_deep` | DOUBLE | BTC qty added to bid side beyond 5bps |
| `bid_cancel_qty_deep` | DOUBLE | BTC qty cancelled on bid side beyond 5bps |
| `ask_add_qty_deep` | DOUBLE | BTC qty added to ask side beyond 5bps |
| `ask_cancel_qty_deep` | DOUBLE | BTC qty cancelled on ask side beyond 5bps |

### Quality (event counts and health)
| Column | Type | Description |
|--------|------|-------------|
| `depth_update_count` | BIGINT | number of depth diff events received this second |
| `trade_event_count` | BIGINT | trade events received (same as trade_count for now) |
| `snapshot_reset_count` | BIGINT | number of book snapshot resets this second |
| `seq_gap_count` | BIGINT | detected sequence gaps this second |
| `stale_ms` | BIGINT | ms since last depth update (>5000 = stale) |
| `missing_flag` | BIGINT | bitmask: 1=no trade, 2=no depth update, 4=no book state |

## Changes from Current agg schema

### Kept (same name/semantics)
- ts, market, type, open, high, low, close, vwap, trade_count
- buy_qty, sell_qty, buy_notional, sell_notional, delta_notional
- buy_small_qty, buy_medium_qty, buy_large_qty
- sell_small_qty, sell_medium_qty, sell_large_qty

### Removed
- `mid_price` → replaced by `mid_open` / `mid_close`
- `spread_bps` → replaced by `spread_bps_open` / `spread_bps_close`
- `bid_depth_5/25/100bps` → now includes `_1bps` too, sampled at close
- `bid_bucketed` / `ask_bucketed` → **removed** ($10bin JSON → too expensive, minimal usage)

### Added
- Book boundary: `mid_open`, `mid_close`, `spread_bps_open`, `spread_bps_close`, `best_bid/ask_open`, `best_bid/ask_close`
- Depth flow: `bid_add/cancel_qty_near/deep`, `ask_add/cancel_qty_near/deep`
- Quality: `depth_update_count`, `trade_event_count`, `snapshot_reset_count`, `seq_gap_count`, `stale_ms`, `missing_flag`

## Storage

- Single `data/1s_features/{YYYY-MM-DD}/{market}.jsonl` per market per day
- Append-only (each row is a JSON object per line)
- DuckDB `read_json_auto()` for reading in aggregate-1s.mjs
- Compaction: not needed (small per-market files, ~3MB/day)
- Aggregate output: `data/agg/{market}.parquet` (same as before)
