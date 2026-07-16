"""
REST orderbook snapshot fetcher.

Fetches full orderbook from exchange REST APIs to seed BookReplay state.
Supports markets with configured restUrl in config.v3.json.
"""

import json
import time
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Tuple


# Cache: market → (timestamp, bids_dict, asks_dict)
_rest_cache: Dict[str, Tuple[float, Dict[float, float], Dict[float, float]]] = {}
CACHE_TTL_SECONDS = 30  # refresh every 30s


def _parse_binance(data: dict) -> Tuple[Dict[float, float], Dict[float, float]]:
    bids = {float(p): float(q) for p, q in data.get("bids", []) if float(q) > 0}
    asks = {float(p): float(q) for p, q in data.get("asks", []) if float(q) > 0}
    return bids, asks


def _parse_bybit(data: dict) -> Tuple[Dict[float, float], Dict[float, float]]:
    result = data.get("result", {})
    bids = {float(p): float(q) for p, q in result.get("b", []) if float(q) > 0}
    asks = {float(p): float(q) for p, q in result.get("a", []) if float(q) > 0}
    return bids, asks


def _parse_okx(data: dict) -> Tuple[Dict[float, float], Dict[float, float]]:
    # OKX returns array in data field
    entries = data.get("data", [])
    if not entries:
        return {}, {}
    entry = entries[0]
    bids = {}
    for item in entry.get("bids", []):
        p, q = float(item[0]), float(item[1])
        if q > 0:
            bids[p] = q
    asks = {}
    for item in entry.get("asks", []):
        p, q = float(item[0]), float(item[1])
        if q > 0:
            asks[p] = q
    return bids, asks


def _parse_kraken(data: dict) -> Tuple[Dict[float, float], Dict[float, float]]:
    result = data.get("result", {})
    # Find the pair key (dynamic)
    for pair_key, pair_data in result.items():
        if isinstance(pair_data, dict) and "bids" in pair_data:
            bids = {float(p): float(q) for p, q, *_ in pair_data.get("bids", []) if float(q) > 0}
            asks = {float(p): float(q) for p, q, *_ in pair_data.get("asks", []) if float(q) > 0}
            return bids, asks
    return {}, {}


def _parse_coinbase(data: dict) -> Tuple[Dict[float, float], Dict[float, float]]:
    # level 3: bids/asks are [price, qty, order_id]
    bids = {}
    for item in data.get("bids", []):
        p, q = float(item[0]), float(item[1])
        if q > 0:
            bids[p] = bids.get(p, 0.0) + q
    asks = {}
    for item in data.get("asks", []):
        p, q = float(item[0]), float(item[1])
        if q > 0:
            asks[p] = asks.get(p, 0.0) + q
    return bids, asks


def _parse_bitstamp(data: dict) -> Tuple[Dict[float, float], Dict[float, float]]:
    """Bitstamp: {bids: [["price", "qty"], ...], asks: [...]}"""
    bids = {float(p): float(q) for p, q in data.get("bids", []) if float(q) > 0}
    asks = {float(p): float(q) for p, q in data.get("asks", []) if float(q) > 0}
    return bids, asks


def _parse_crypto_com(data: dict) -> Tuple[Dict[float, float], Dict[float, float]]:
    """Crypto.com: {result: {data: [{bids: [["p","q"],...], asks: [...]}]}}"""
    result = data.get("result", {})
    entries = result.get("data", [])
    if not entries:
        return {}, {}
    entry = entries[0]
    bids = {}
    for item in entry.get("bids", []):
        p, q = float(item[0]), float(item[1])
        if q > 0:
            bids[p] = q
    asks = {}
    for item in entry.get("asks", []):
        p, q = float(item[0]), float(item[1])
        if q > 0:
            asks[p] = q
    return bids, asks


def _parse_bitfinex(data: list) -> Tuple[Dict[float, float], Dict[float, float]]:
    """Bitfinex: [[price, count, qty], ...] — qty>0=bid, qty<0=ask"""
    bids, asks = {}, {}
    for item in data:
        price = float(item[0])
        qty = float(item[2])  # item[1] is count
        if qty > 0:
            bids[price] = qty
        elif qty < 0:
            asks[price] = abs(qty)
    return bids, asks


def _parse_bitmex(data: dict) -> Tuple[Dict[float, float], Dict[float, float]]:
    """BitMEX: [{id, side, price, size}, ...]"""
    bids, asks = {}, {}
    for item in data:
        side = item.get("side", "").upper()
        price = float(item["price"])
        size = float(item.get("size", 0) or item.get("qty", 0))
        if size <= 0:
            continue
        if side == "BUY" or side == "BID":
            bids[price] = bids.get(price, 0.0) + size
        elif side == "SELL" or side == "ASK":
            asks[price] = asks.get(price, 0.0) + size
    return bids, asks


def _parse_hyperliquid(data: dict) -> Tuple[Dict[float, float], Dict[float, float]]:
    """Hyperliquid L2 book: {levels: [[{px, sz}, ...], [{px, sz}, ...]]}"""
    levels = data.get("levels", [])
    if len(levels) < 2:
        return {}, {}
    bids, asks = {}, {}
    for level in levels[0]:  # bids
        p = float(level["px"])
        s = float(level["sz"])
        if s > 0:
            bids[p] = bids.get(p, 0.0) + s
    for level in levels[1]:  # asks
        p = float(level["px"])
        s = float(level["sz"])
        if s > 0:
            asks[p] = asks.get(p, 0.0) + s
    return bids, asks


# Map: market → (rest_url, parser)
# Parsed from config.v3.json at runtime
REST_CONFIG: Dict[str, Tuple[str, callable]] = {}


def _load_rest_config(config_path: str = "config.v3.json"):
    """Load REST endpoint config from receiver config file."""
    global REST_CONFIG
    if REST_CONFIG:
        return

    parsers = {
        "binance_spot": _parse_binance,
        "binance_perp": _parse_binance,
        "binance_perp_btcusdc": _parse_binance,
        "binance_spot_usdc": _parse_binance,
        "binance_coinm_perp": _parse_binance,
        "bybit_perp": _parse_bybit,
        "bybit_spot": _parse_bybit,
        "okx_perp": _parse_okx,
        "okx_spot": _parse_okx,
        "kraken_spot": _parse_kraken,
        "coinbase_spot": _parse_coinbase,
        "bitstamp_spot": _parse_bitstamp,
        "crypto_com_spot": _parse_crypto_com,
        "bitfinex_spot": _parse_bitfinex,
        "bitmex_perp": _parse_bitmex,
        "hyperliquid_perp": _parse_hyperliquid,
    }

    # Hardcode hyperliquid (POST-based API, can't use GET URL)
    HYPERLIQUID_URL = "https://api.hyperliquid.xyz/info"

    try:
        with open(config_path) as f:
            cfg = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return

    for market, mc in cfg.get("markets", {}).items():
        url = mc.get("restUrl", "")
        if url and market in parsers:
            REST_CONFIG[market] = (url, parsers[market])

    # Hyperliquid: add POST-based URL if not in config
    if "hyperliquid_perp" in parsers and "hyperliquid_perp" not in REST_CONFIG:
        REST_CONFIG["hyperliquid_perp"] = (HYPERLIQUID_URL, _parse_hyperliquid)


def fetch_rest_book(market: str, config_path: str = "config.v3.json",
                    force: bool = False) -> Optional[Tuple[Dict[float, float], Dict[float, float]]]:
    """
    Fetch full orderbook from REST API for a market.

    Returns (bids_dict, asks_dict) or None if unavailable/failed.
    Uses cache (30s TTL) to avoid rate limits.
    """
    global _rest_cache

    # Check cache
    if not force and market in _rest_cache:
        cached_at, bids, asks = _rest_cache[market]
        if time.time() - cached_at < CACHE_TTL_SECONDS:
            return bids, asks

    _load_rest_config(config_path)
    if market not in REST_CONFIG:
        return None

    url, parser = REST_CONFIG[market]
    is_post = market == "hyperliquid_perp"

    try:
        if is_post:
            data = json.dumps({"type": "l2Book", "coin": "BTC"}).encode()
            req = urllib.request.Request(url, data=data, headers={
                "User-Agent": "agg-btc-receiver/1.0",
                "Content-Type": "application/json",
            })
        else:
            req = urllib.request.Request(url, headers={
                "User-Agent": "agg-btc-receiver/1.0",
                "Accept": "application/json",
            })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, TimeoutError, OSError) as e:
        return None

    try:
        bids, asks = parser(data)
    except (KeyError, ValueError, TypeError, IndexError):
        return None

    # Update cache
    _rest_cache[market] = (time.time(), bids, asks)
    return bids, asks


def seed_book_replay(book_replay, market: str, config_path: str = "config.v3.json") -> bool:
    """
    Seed a BookReplay instance with REST orderbook snapshot.

    Applies all bid/ask levels to the replay, overwriting previous state.
    Returns True if seeded successfully, False if REST unavailable.
    """
    from .book_replay import BookReplay
    if not isinstance(book_replay, BookReplay):
        return False

    result = fetch_rest_book(market, config_path)
    if result is None:
        return False

    bids, asks = result

    # Reset and apply all levels
    book_replay.reset()
    update = {
        "bids": [[str(p), str(q)] for p, q in bids.items()],
        "asks": [[str(p), str(q)] for p, q in asks.items()],
        "ts": int(time.time() * 1000),
    }
    book_replay.apply_json(update)
    return True
