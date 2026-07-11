# Golden Fixtures Calculation Rationale

## Parameters (from design-2026-07-10-burst-reducer.md)
- `gap_threshold_ms = 50`
- `max_burst_duration_ms = 5000`
- `tick_size = 0.01` (BTC spot markets)

## P1 Contract Values (Phase 1 trade-only MVP)
- `#12 burst_notional_vs_30s_traded_notional = 0` (zeroLookup — agg_trades not wired in these fixtures)
- `#13 burst_notional_vs_top_depth = null` (book not available in P1)
- `#14 burst_mid_move_bps_1s = 0` (book not available, P1 contract: 0, not null)
- `#15-#22 = 0` (research + monitoring fields, P1 placeholder)

---

## 1. trades-basic.jsonl → expected-features-1s.jsonl

**Input trades (all buy, same price=100):**
```
ts=500,  qty=1  →  notional=100
ts=520,  qty=2  →  notional=200
ts=540,  qty=1  →  notional=100
ts=560,  qty=1  →  notional=100
Total notional = 500
```

**Burst formation** (gap_threshold_ms=50, all adjacent gaps = 20ms ≤ 50):
→ 1 burst, side=buy, price range [100, 100]

**Burst properties:**
- burst_notional = 100+200+100+100 = **500**
- burst_print_count = **4**
- burst_duration_ms = 560-500 = **60**
- min_price = max_price = 100 → distinct_price_count = **1**
- same_price_burst = true (distinct_price_count == 1)
- multilevel_burst = false (distinct_price_count < 2)
- span_ticks = 0

**1s bucket ts=0** [0, 1000): burst_start=500 < 1000 AND burst_end=560 >= 0 → overlaps ✓
All other buckets (ts ≥ 1000): burst_end=560 < 1000 → no overlap.

**Features for ts=0:**
| Feature | Value | Formula |
|---|---|---|
| burst_count_1s | 1 | count of overlapping bursts |
| total_burst_notional_1s | 500 | sum(burst_notional) |
| max_burst_notional_1s | 500 | max(burst_notional) |
| max_burst_prints_1s | 4 | max(print_count) |
| max_burst_duration_ms_1s | 60 | max(duration_ms) |
| buy_burst_notional_1s | 500 | sum(buy burst notional) |
| sell_burst_notional_1s | 0 | sum(sell burst notional) |
| burst_imbalance_ratio_1s | 1.0 | (500-0)/(500+0) |
| largest_burst_share_notional_1s | 1.0 | 500/500 |
| same_price_burst_count_1s | 1 | bursts with distinct_price_count==1 |
| multilevel_burst_count_1s | 0 | bursts with distinct_price_count≥2 |
| burst_notional_vs_30s_traded_notional | 0 | zeroLookup (agg_trades not wired) |

_quality: warmup=false, book_seeded=false, input_block_ids=["trades-basic"]
trade_count_this_second=4 for ts=0, 0 for others.

---

## 2. trades-cross-boundary.jsonl → expected-cross-boundary-1s.jsonl

**Input trades (both buy, same price=100):**
```
ts=29900, qty=1 → notional=100
ts=30100, qty=1 → notional=100
```

**Burst formation** (gap_threshold_ms=50):
- gap = 30100 - 29900 = 200ms > 50 → **2 separate bursts**

**Burst 1:** ts=29900, single print, notional=100, duration=0
  - Overlaps bucket ts=29000 [29000, 30000): 29900 < 30000 ∧ 29900 ≥ 29000 ✓

**Burst 2:** ts=30100, single print, notional=100, duration=0
  - Overlaps bucket ts=30000 [30000, 31000) — in the NEXT 30s block
  - NOT in current block output (which covers ts=0…29000)

**Features for ts=29000:**
| Feature | Value |
|---|---|
| burst_count_1s | 1 |
| total_burst_notional_1s | 100 |
| max_burst_notional_1s | 100 |
| max_burst_prints_1s | 1 |
| max_burst_duration_ms_1s | 0 |
| buy_burst_notional_1s | 100 |
| sell_burst_notional_1s | 0 |
| burst_imbalance_ratio_1s | 1.0 |
| largest_burst_share_notional_1s | 1.0 |
| same_price_burst_count_1s | 1 |
| multilevel_burst_count_1s | 0 |

_quality: warmup=false, book_seeded=false, input_block_ids=["trades-cross-boundary"]
trade_count_this_second=1 for ts=29000, 0 for others.

---

## 3. agg-trades-basic.jsonl

**Input rows (30s window [0, 30000)):**
```
ts=5000,  volume=20, vwap=100 → notional=2000
ts=15000, volume=30, vwap=100 → notional=3000
ts=25000, volume=50, vwap=100 → notional=5000
Total 30s traded notional = 10000
```

When combined with trades-basic.jsonl (burst notional=500):
  `burst_notional_vs_30s_traded_notional = 500 / 10000 = 0.05`
(This computation is not used in expected-features-1s.jsonl which has #12=0.)
