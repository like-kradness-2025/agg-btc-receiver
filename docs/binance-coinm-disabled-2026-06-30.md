# Binance COIN-M BTCUSD_PERP disabled operationally

Date: 2026-06-30

## Decision

`binance_coinm_perp` is disabled in `config.v3.json`.

```json
"binance_coinm_perp": {
  "enabled": false
}
```

## Reason

This is not a local receiver sync-only issue. Direct Binance COIN-M checks showed that `BTCUSD_PERP` did not provide a usable live order book or live trade stream at the time of verification.

Evidence gathered on 2026-06-30:

- `GET https://dapi.binance.com/dapi/v1/depth?symbol=BTCUSD_PERP&limit=5`
  - HTTP 200
  - `bids: []`, `asks: []`
- `GET https://dapi.binance.com/dapi/v1/ticker/bookTicker?symbol=BTCUSD_PERP`
  - `bidPrice: "0.0"`, `askPrice: "0.0"`, `lastUpdateId: 0`
- `GET https://dapi.binance.com/dapi/v1/trades?symbol=BTCUSD_PERP&limit=5`
  - `[]`
- `GET https://dapi.binance.com/dapi/v1/openInterest?symbol=BTCUSD_PERP`
  - HTTP 400
  - `Symbol is on delivering or delivered or settling or closed or pre-trading.`
- WebSocket probes produced 0 messages within 20-45 seconds:
  - `wss://dstream.binance.com/ws/btcusd_perp@trade`
  - `wss://dstream.binance.com/ws/btcusd_perp@aggTrade`
  - `wss://dstream.binance.com/ws/btcusd_perp@depth@100ms`

Keeping this market enabled caused a 30s reconnect loop and no fresh `1s_features` rows.

## Scope

- Disable only `binance_coinm_perp`.
- Keep `binance_perp_btcusdc` enabled. It recovered after the low-volume snapshot-only sync fix.
- Do not remove historical COIN-M files; old files remain as historical artifacts and should not be interpreted as live.

## Operational baseline

After this change, the active market count is 15 instead of 16.
Charts and reports should say "15 active markets" while this disable is in effect.

## Re-enable criteria

Only re-enable if live probes show all of the following:

1. REST depth returns non-empty bids and asks.
2. REST trades or aggTrades has current timestamps.
3. WebSocket `@depth@100ms` or `@trade` emits live messages.
4. A receiver restart produces fresh `data/1s_features/{date}/binance_coinm_perp.jsonl` rows.
