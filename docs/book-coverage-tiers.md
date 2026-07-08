# Book Coverage Tiers

## Purpose

This note fixes the operational interpretation of each market's in-memory order book coverage in `agg-btc-receiver`.

The receiver runs continuously and applies book events into an in-memory `FullBook`, but **continuous WS ingestion does not imply true full-book visibility**. For most exchanges, the receiver can only maintain:

```text
initial snapshot seed
+ subsequent observed diff / snapshot-replace updates
= current visible active book
```

This means some markets are useful for far-book / macro liquidity shape analysis, while others should be treated as near-touch / bounded-depth feeds only.

## Evidence base

This tiering is grounded in:

1. connector implementation review (`lib/*-connector.mjs`)
2. exchange-specific snapshot/depth limits already encoded in the connectors/config
3. 2026-07-04 live 40s measurement from `data/live_market_levels/30s_book/...`

Measured raw level counts during the 40s run:

| market | bid levels | ask levels |
|---|---:|---:|
| binance_spot | 5053 | 5083 |
| binance_spot_usdc | 4996 | 5018 |
| binance_perp | 1657 | 1752 |
| binance_perp_btcusdc | 1095 | 1145 |
| bybit_perp | 1000 | 1000 |
| bybit_spot | 200 | 200 |
| okx_perp | 400 | 400 |
| okx_spot | 400 | 400 |
| kraken_spot | 1031 | 1013 |
| bitstamp_spot | 56 | 51 |
| crypto_com_spot | 10 | 10 |
| bitfinex_spot | 25 | 25 |
| bitmex_perp | 1786 | 1821 |
| coinbase_spot | 14817 | 27551 |
| hyperliquid_perp | 20 | 20 |

## Tier definitions

### Tier A — full-book-like / far-book-usable

Characteristics:
- deep snapshot seed or effectively broad live table stream
- outer levels can accumulate over time
- suitable for macro liquidity shape / far-book bucket analysis
- still not a mathematical guarantee of the exchange's entire hidden/resting book

Markets:
- `coinbase_spot`
- `bitmex_perp`
- `binance_spot`
- `binance_spot_usdc`
- `kraken_spot`

Operational meaning:
- these are the primary candidates for `30s_book` macro analysis
- long runtime improves far-book coverage
- analytics should still tolerate extreme outlier levels (especially Coinbase / BitMEX)

### Tier B — snapshot-limited but useful mid-depth

Characteristics:
- moderate seed depth
- can observe more than touch-only behavior
- far-book coverage exists but is materially constrained by upstream snapshot/channel depth

Markets:
- `binance_perp`
- `binance_perp_btcusdc`
- `bybit_perp`

Operational meaning:
- usable for mid-depth shape / near-to-mid liquidity monitoring
- weaker than Tier A for far-book wall studies
- resnapshot helps with freshness/drift, but does not remove the upstream ceiling

### Tier C — bounded-depth / near-book only

Characteristics:
- exchange channel exposes a hard bounded number of levels
- the receiver can maintain those levels accurately, but cannot see deep far-book structure
- should be interpreted as touch-to-nearby liquidity only

Markets:
- `okx_perp`
- `okx_spot`
- `bybit_spot`
- `bitstamp_spot`
- `bitfinex_spot`
- `crypto_com_spot`
- `hyperliquid_perp`

Operational meaning:
- valid for near-book imbalance, spread-adjacent liquidity, and microstructure monitoring
- invalid to treat as broad macro full-book shape
- `30s_book` on these markets is a bounded-depth macro proxy, not true far-book coverage

## Key interpretation rule

For every market, the in-memory book should be interpreted as:

```text
exchange-visible active L2 state
not
guaranteed complete exchange-wide full depth
```

This is especially important for snapshot-limited venues:
- static far levels outside the initial seed never appear unless later touched by a diff / replace event
- long uptime improves observed coverage only where the venue actually emits outer-level updates

## Implications for downstream analysis

### 1s features

No change. `1s_features` remain the micro / flow / burst layer.

### 30s_book

`30s_book` is the macro liquidity-shape layer, but interpretation must be tier-aware:
- Tier A: broad macro usage is acceptable
- Tier B: mid-depth macro usage only
- Tier C: near-book macro proxy only

### Cross-market comparisons

Do **not** compare `30s_book` breadth across markets as if they had identical exchange visibility.

Correct comparison pattern:
- compare Tier A markets against Tier A markets for far-book studies
- compare Tier C markets only on near-book behavior
- do not infer “market X has less distant liquidity than market Y” solely from shallower visible level counts when the upstream channel depths differ

## Recommended next operational step

Add explicit tier metadata to downstream consumers / charts / reports so every market's `30s_book` is interpreted using its coverage class.

Suggested tag set:

```json
{
  "coinbase_spot": "tier_a_full_book_like",
  "bitmex_perp": "tier_a_full_book_like",
  "binance_spot": "tier_a_full_book_like",
  "binance_spot_usdc": "tier_a_full_book_like",
  "kraken_spot": "tier_a_full_book_like",
  "binance_perp": "tier_b_snapshot_limited_mid_depth",
  "binance_perp_btcusdc": "tier_b_snapshot_limited_mid_depth",
  "bybit_perp": "tier_b_snapshot_limited_mid_depth",
  "okx_perp": "tier_c_bounded_depth_near_book",
  "okx_spot": "tier_c_bounded_depth_near_book",
  "bybit_spot": "tier_c_bounded_depth_near_book",
  "bitstamp_spot": "tier_c_bounded_depth_near_book",
  "bitfinex_spot": "tier_c_bounded_depth_near_book",
  "crypto_com_spot": "tier_c_bounded_depth_near_book",
  "hyperliquid_perp": "tier_c_bounded_depth_near_book"
}
```

## Non-goal

This document does not claim that periodic resnapshot alone can upgrade Tier C venues into true far-book markets. If the venue exposes only bounded depth, the receiver cannot synthesize the unseen outer book.