// Market metadata used at the canonical book boundary.
// Raw events retain exchange-native quantities; canonical envelopes expose BTC.

const BASE = Object.freeze({ sourceUnit: 'BTC', mode: 'identity' });

export const MARKET_REGISTRY = Object.freeze({
  binance_spot: BASE,
  binance_spot_usdc: BASE,
  bybit_spot: BASE,
  okx_spot: BASE,
  coinbase_spot: BASE,
  kraken_spot: BASE,
  bitstamp_spot: BASE,
  crypto_com_spot: BASE,
  bitfinex_spot: BASE,
  binance_perp: BASE,
  binance_perp_btcusdc: BASE,
  bybit_perp: BASE,
  hyperliquid_perp: BASE,
  okx_perp: { sourceUnit: 'contracts', mode: 'contracts_to_btc', contractValueBtc: 0.01 },
  bitmex_perp: { sourceUnit: 'contracts', mode: 'inverse_usd_to_btc', contractValueUsd: 1 },
  binance_coinm_perp: { sourceUnit: 'contracts', mode: 'inverse_usd_to_btc', contractValueUsd: 100 },
});

export const BOOK_QUANTITY_SCHEMA = 'book_qty_v1';

export function getMarketBookSpec(market) {
  return MARKET_REGISTRY[market] || BASE;
}

/** Normalize one exchange-native book quantity to BTC. */
export function normalizeBookQuantity(market, price, quantity) {
  const p = Number(price);
  const q = Number(quantity);
  if (quantity === '' || q === 0) return 0;
  if (!Number.isFinite(p) || p <= 0 || !Number.isFinite(q) || q < 0) return null;

  const spec = getMarketBookSpec(market);
  if (spec.mode === 'contracts_to_btc') return q * spec.contractValueBtc;
  if (spec.mode === 'inverse_usd_to_btc') return q * spec.contractValueUsd / p;
  return q;
}

function formatQuantity(value, original) {
  if (value === 0) return '0';
  if (original !== undefined && value === Number(original)) return String(original);
  return String(value);
}

export function normalizeBookLevels(market, levels) {
  const result = [];
  for (const entry of levels || []) {
    const [price, quantity] = entry;
    const normalized = normalizeBookQuantity(market, price, quantity);
    if (normalized === null) return { valid: false, levels: result };
    result.push([String(price), formatQuantity(normalized, quantity)]);
  }
  return { valid: true, levels: result };
}

export function bookQuantityMetadata(market) {
  const spec = getMarketBookSpec(market);
  return {
    schema: BOOK_QUANTITY_SCHEMA,
    target_unit: 'BTC',
    source_unit: spec.sourceUnit,
    mode: spec.mode,
    ...(spec.contractValueBtc ? { contract_value_btc: spec.contractValueBtc } : {}),
    ...(spec.contractValueUsd ? { contract_value_usd: spec.contractValueUsd } : {}),
  };
}
