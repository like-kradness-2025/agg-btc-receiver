"""
Downstream pipeline config: constants, schema, tick sizes.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional

# ── Burst detection parameters ──────────────────────────────────────────
GAP_THRESHOLD_MS: int = 50         # max gap between trades in same burst
MAX_BURST_DURATION_MS: int = 5000  # max duration of a single burst
BLOCK_DURATION_MS: int = 30000     # Receiver 30s block duration
SECOND_MS: int = 1000

# ── Paths ────────────────────────────────────────────────────────────────
LIVE_DATA_DIR: str = "data/live_v3"
DERIVED_DIR: str = "data/derived/burst_features_v1"
FEATURES_1S_DIR: str = "features_1s"
CURSOR_FILE: str = ".cursor.json"

# ── Market tick sizes (bps / tick for span calc) ────────────────────────
MARKET_TICK_SIZE: Dict[str, float] = {
    "binance_spot": 0.01,
    "binance_perp": 1.0,
    "binance_coinm_perp": 1.0,
    "binance_perp_btcusdc": 1.0,
    "binance_spot_usdc": 0.01,
    "bybit_spot": 0.01,
    "bybit_perp": 0.1,
    "okx_spot": 0.01,
    "okx_perp": 0.1,
    "coinbase_spot": 0.01,
    "coinbase_international_perp": 0.1,
    "kraken_spot": 0.01,
    "bitstamp_spot": 0.01,
    "gemini_spot": 0.01,
    "crypto_com_spot": 0.01,
    "bitfinex_spot": 0.01,
    "bitmex_perp": 0.5,
    "hyperliquid_perp": 0.1,
}


def get_tick_size(market: str) -> Optional[float]:
    return MARKET_TICK_SIZE.get(market)


# ── Parquet schema (PyArrow) ────────────────────────────────────────────
import pyarrow as pa

FEATURE_1S_SCHEMA = pa.schema([
    # Envelope
    pa.field("ts", pa.int64(), nullable=False),         # epoch ms, second boundary
    pa.field("market", pa.utf8(), nullable=False),

    # ── Trade-only features #1-#12 ──
    pa.field("burst_count_1s", pa.int32(), nullable=False),
    pa.field("total_burst_notional_1s", pa.float64(), nullable=False),
    pa.field("max_burst_notional_1s", pa.float64(), nullable=False),
    pa.field("max_burst_prints_1s", pa.int32(), nullable=False),
    pa.field("max_burst_duration_ms_1s", pa.int32(), nullable=False),
    pa.field("buy_burst_notional_1s", pa.float64(), nullable=False),
    pa.field("sell_burst_notional_1s", pa.float64(), nullable=False),
    pa.field("burst_imbalance_ratio_1s", pa.float64(), nullable=False),
    pa.field("largest_burst_share_notional_1s", pa.float64(), nullable=False),
    pa.field("same_price_burst_count_1s", pa.int32(), nullable=False),
    pa.field("multilevel_burst_count_1s", pa.int32(), nullable=False),
    pa.field("burst_notional_vs_30s_traded_notional", pa.float64(), nullable=False),

    # ── Book-dependent #13-#14 (P1: nullable=null / 0) ──
    pa.field("burst_notional_vs_top_depth", pa.float64(), nullable=True),
    pa.field("burst_mid_move_bps_1s", pa.float64(), nullable=False),

    # ── Research #15-#21 (P1: 0) ──
    pa.field("same_price_burst_max_len_1s", pa.int32(), nullable=False),
    pa.field("same_price_burst_notional_1s", pa.float64(), nullable=False),
    pa.field("multilevel_burst_max_span_ticks_1s", pa.float64(), nullable=False),
    pa.field("multilevel_burst_max_span_bps_1s", pa.float64(), nullable=False),
    pa.field("multilevel_burst_notional_1s", pa.float64(), nullable=False),
    pa.field("same_price_absorption_ratio_1s", pa.float64(), nullable=False),
    pa.field("burst_delta_notional_1s", pa.float64(), nullable=False),

    # ── Monitoring #22 ──
    pa.field("outlier_trade_flag_1s", pa.int32(), nullable=False),
])


# ── Quality tiers ────────────────────────────────────────────────────────
BOOK_COVERAGE_TIERS = {
    "full": {"binance_spot", "binance_perp", "binance_coinm_perp", "binance_perp_btcusdc",
             "binance_spot_usdc", "bybit_spot", "bybit_perp", "okx_spot", "okx_perp",
             "coinbase_spot", "kraken_spot"},
    "partial": {"crypto_com_spot", "bitfinex_spot", "bitmex_perp", "hyperliquid_perp"},
    "trade_only": {"bitstamp_spot", "gemini_spot", "coinbase_international_perp"},
}
