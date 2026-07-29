// lib/events.mjs — Shared event type definitions for btc-receiver v3.00

/**
 * @typedef {'init'|'connecting'|'connected'|'syncing'|'running'|'reconnecting'|'error'} ConnectorState
 */

/**
 * @typedef {Object} DepthEvent
 * @property {string} market
 * @property {'snapshot'|'update'|'delete'} type
 * @property {Array<[string, string]>} bids
 * @property {Array<[string, string]>} asks
 * @property {number} ts
 * @property {number} [seq]
 */

/**
 * @typedef {Object} TradeEvent
 * @property {string} market
 * @property {number} price
 * @property {number} qty
 * @property {'buy'|'sell'} side
 * @property {number} ts
 * @property {string} [tradeId]
 */

/**
 * @typedef {Object} ConnectorStats
 * @property {ConnectorState} state
 * @property {number} connectedAt
 * @property {number} lastDepthMsgAt
 * @property {number} lastTradeMsgAt
 * @property {number} depthMsgCount
 * @property {number} tradeMsgCount
 * @property {number} droppedDepthCount
 * @property {number} droppedTradeCount
 * @property {number} droppedLiquidationCount
 * @property {number} reconnectCount
 * @property {number} resyncCount
 * @property {number} lastSeq
 */

export const EVENTS = {
  DEPTH: 'depth',
  TRADE: 'trade',
  ERROR: 'error',
  STATE_CHANGE: 'stateChange',
  HEALTH: 'health',
};
