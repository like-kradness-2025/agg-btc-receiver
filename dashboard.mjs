#!/usr/bin/env node
/**
 * dashboard.mjs — Receiver status dashboard
 *
 * Usage:
 *   node dashboard.mjs [--port 3847] [--data data/live_v3]
 *
 * Serves a single-page dark-theme dashboard showing:
 * - Per-market connection status
 * - Trade/depth message rates
 * - File rotation state
 * - System resource usage
 */

import http from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const PORT = parseInt(process.argv.includes('--port')
  ? process.argv[process.argv.indexOf('--port') + 1] : '3847', 10);
const DATA_DIR = process.argv.includes('--data')
  ? process.argv[process.argv.indexOf('--data') + 1] : 'data/live_v3';
const ROOT_DIR = path.dirname(fileURLToPath(import.meta.url));
const HEALTH_FILE = path.join(DATA_DIR, 'health.jsonl');
const POLL_MS = 2000;
const RATE_HISTORY_FILE = '/tmp/agg-trade-rate-history.jsonl';
const CHART_FILE = '/tmp/agg-chart.png';
const CHART_SCRIPT = path.join(ROOT_DIR, 'scripts', 'chart_rates.py');
const PREFERRED_PYTHON = path.join(os.homedir(), 'btc-tools', '.venv', 'bin', 'python');
const PYTHON_BIN = fs.existsSync(PREFERRED_PYTHON) ? PREFERRED_PYTHON : 'python3';

// ── State ───────────────────────────────────────────────────────────────────
const MAX_HISTORY = 30;
let rawCountsSnapshot = null;
let rawCountsHistory = [];

// ── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Read raw 30s block files for a given market + date, return line counts
 * for both trades and book_updates in the last N files.
 */
function readRawBlockCounts(market, dateDir) {
  const tradesDir = path.join(DATA_DIR, 'trades', market, dateDir);
  const bookDir = path.join(DATA_DIR, 'book_updates', market, dateDir);
  let tradeFiles = []; let bookFiles = [];
  try { tradeFiles = fs.readdirSync(tradesDir).filter(f => f.endsWith('.jsonl')).sort(); } catch {}
  try { bookFiles = fs.readdirSync(bookDir).filter(f => f.endsWith('.jsonl')).sort(); } catch {}
  const len = tradeFiles.length;
  // Keep last 2 blocks: current + previous (for delta calculation)
  const result = { market, tradeLines: {}, bookLines: {}, latestTs: 0 };
  for (const fileset of [['tradeLines', tradeFiles, tradesDir, 'trade'], ['bookLines', bookFiles, bookDir, 'book']]) {
    const [key, files, dir] = fileset;
    for (const f of files.slice(-2)) {
      let count = 0; let maxTs = 0;
      try {
        const content = fs.readFileSync(path.join(dir, f), 'utf-8');
        const lines = content.split('\n').filter(Boolean);
        count = lines.length;
        // Get the latest timestamp from last line
        const lastLine = lines[lines.length - 1];
        if (lastLine.startsWith('{')) {
          const parsed = JSON.parse(lastLine);
          if (parsed.ts > maxTs) maxTs = parsed.ts;
        }
      } catch {}
      result[key][f] = { count, maxTs };
      if (maxTs > result.latestTs) result.latestTs = maxTs;
    }
  }
  return result;
}

function readAllRawCounts() {
  const now = new Date();
  const dateDir = now.toISOString().slice(0, 10); // 2026-07-10
  const tradesTop = path.join(DATA_DIR, 'trades');
  let markets = [];
  try { markets = fs.readdirSync(tradesTop).filter(d => {
    try { return fs.statSync(path.join(tradesTop, d)).isDirectory(); } catch { return false; }
  }); } catch { return { ts: Date.now(), markets: {} }; }

  const out = { ts: Date.now(), markets: {} };
  let globalLatest = 0;
  for (const m of markets) {
    const info = readRawBlockCounts(m, dateDir);
    // Compute rates from the 2 most recent 30s blocks
    const tradeFiles = Object.keys(info.tradeLines).sort();
    const bookFiles = Object.keys(info.bookLines).sort();
    let tradeRate = 0; let depthRate = 0;

    if (tradeFiles.length >= 2) {
      const prev = info.tradeLines[tradeFiles[tradeFiles.length - 2]];
      const curr = info.tradeLines[tradeFiles[tradeFiles.length - 1]];
      tradeRate = Math.max(0, Math.round((curr.count - prev.count) / 30));
    } else if (tradeFiles.length === 1) {
      // Only one file — approximate as its total lines / 30
      tradeRate = Math.max(0, Math.round(info.tradeLines[tradeFiles[0]].count / 30));
    }

    if (bookFiles.length >= 2) {
      const prev = info.bookLines[bookFiles[bookFiles.length - 2]];
      const curr = info.bookLines[bookFiles[bookFiles.length - 1]];
      depthRate = Math.max(0, Math.round((curr.count - prev.count) / 30));
    } else if (bookFiles.length === 1) {
      depthRate = Math.max(0, Math.round(info.bookLines[bookFiles[0]].count / 30));
    }

    out.markets[m] = {
      state: 'running',
      tradeRate,
      depthRate,
      latestTs: info.latestTs,
    };
    if (info.latestTs > globalLatest) globalLatest = info.latestTs;
  }
  out.ts = globalLatest || Date.now();
  return out;
}

function appendRateHistory(snapshot) {
  if (!snapshot) return;
  const lines = [];
  for (const [market, m] of Object.entries(snapshot.markets || {})) {
    lines.push(JSON.stringify({
      ts: snapshot.ts,
      market,
      tradeMsgCount: m.tradeRate,
      depthMsgCount: m.depthRate,
    }));
  }
  if (!lines.length) return;
  try {
    fs.appendFileSync(RATE_HISTORY_FILE, `${lines.join('\n')}\n`);
  } catch (error) {
    console.error('[dashboard] failed to append rate history:', error.message);
  }
}

function generateChartPng() {
  try {
    execFileSync(PYTHON_BIN, [CHART_SCRIPT, '--input', RATE_HISTORY_FILE, '--output', CHART_FILE, '--minutes', '60'], { stdio: 'pipe', timeout: 10000 });
  } catch (e) {
    // chart generation is best-effort background task
  }
}

// 1x1 transparent placeholder PNG (base64)
function createPlaceholderPng() {
  return Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==', 'base64');
}

// Pre-generate chart after startup, then every 5s in background
setTimeout(() => { try { generateChartPng(); } catch {} }, 3000);
setInterval(() => { try { generateChartPng(); } catch {} }, 5000);

async function getFileCounts() {
  const counts = { trades: 0, book_updates: 0, liquidations: 0, open: 0 };
  try {
    const entries = await fsp.readdir(DATA_DIR, { withFileTypes: true });
    for (const e of entries) {
      if (!e.isDirectory() || e.name.startsWith('_') || e.name === 'health.jsonl') continue;
      const kind = e.name;
      if (!(kind in counts)) continue;
      const marketDirs = await fsp.readdir(path.join(DATA_DIR, kind), { withFileTypes: true }).catch(() => []);
      for (const m of marketDirs) {
        if (!m.isDirectory()) continue;
        const dateDirs = await fsp.readdir(path.join(DATA_DIR, kind, m.name), { withFileTypes: true }).catch(() => []);
        for (const d of dateDirs) {
          if (!d.isDirectory()) continue;
          const files = await fsp.readdir(path.join(DATA_DIR, kind, m.name, d.name)).catch(() => []);
          for (const f of files) {
            if (f.endsWith('.jsonl') && !f.endsWith('.open')) counts[kind]++;
            if (f.endsWith('.open')) counts.open++;
          }
        }
      }
    }
  } catch {}
  return counts;
}

function getProcessInfo() {
  const mem = process.memoryUsage();
  return {
    rss: Math.round(mem.rss / 1024 / 1024),
    heap: Math.round(mem.heapUsed / 1024 / 1024),
    uptime: Math.round(process.uptime()),
    loadavg: os.loadavg().map(v => v.toFixed(2)),
    freemem: Math.round(os.freemem() / 1024 / 1024),
    totalmem: Math.round(os.totalmem() / 1024 / 1024),
  };
}

// ── Periodic poll (based on raw file counts) ────────────────────────────────

function refreshRawCounts() {
  const snap = readAllRawCounts();
  if (snap && snap.markets) {
    // Append to rate history for chart
    if (rawCountsSnapshot) {
      appendRateHistory(snap);
    }
    rawCountsSnapshot = snap;
    rawCountsHistory.push(snap);
    if (rawCountsHistory.length > MAX_HISTORY) rawCountsHistory.shift();
  }
}

refreshRawCounts();
setInterval(refreshRawCounts, POLL_MS);

// ── HTTP server ─────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  if (url.pathname === '/api/status') {
    const snap = rawCountsSnapshot;
    const fileCounts = await getFileCounts();
    const proc = getProcessInfo();

    // Total trade/depth rate across all markets
    let totalTradeRate = 0, totalDepthRate = 0;
    const markets = snap?.markets || {};
    for (const [m, info] of Object.entries(markets)) {
      totalTradeRate += info.tradeRate || 0;
      totalDepthRate += info.depthRate || 0;
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      markets,
      totalTradeRate,
      totalDepthRate,
      ts: snap?.ts || Date.now(),
      fileCounts,
      proc,
    }));
    return;
  }

  if (url.pathname === '/api/chart') {
    // Chart is pre-generated every 5 seconds in background
    try {
      const png = await fsp.readFile(CHART_FILE);
      res.writeHead(200, {
        'Content-Type': 'image/png',
        'Cache-Control': 'no-store, no-cache, must-revalidate',
      });
      res.end(png);
    } catch (error) {
      // No chart yet — return placeholder
      res.writeHead(200, { 'Content-Type': 'image/png' });
      res.end(createPlaceholderPng());
    }
    return;
  }

  if (url.pathname === '/' || url.pathname === '/receiver/') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(DASHBOARD_HTML);
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[dashboard] http://localhost:${PORT}`);
});

// ── Dashboard HTML ──────────────────────────────────────────────────────────

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>BTC Receiver — EffiZen</title>
<style>
  :root {
    --bg-deep: #070b16;
    --bg: #0d1225;
    --bg-card: rgba(17, 24, 39, 0.75);
    --bg-card-hover: rgba(25, 35, 55, 0.85);
    --border: rgba(255,255,255,0.07);
    --border-light: rgba(255,255,255,0.12);
    --text: #e6edf3;
    --text-secondary: #8b949e;
    --text-muted: #6e7681;
    --accent-blue: #58a6ff;
    --accent-purple: #a78bfa;
    --accent-green: #3fb950;
    --accent-yellow: #d29922;
    --accent-red: #f85149;
    --accent-cyan: #38bdf8;
    --accent-pink: #f472b6;
    --radius: 14px;
    --radius-sm: 10px;
    --radius-xs: 6px;
    --font: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans JP", Helvetica, Arial, sans-serif;
  }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
  @keyframes fadeIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }
  @keyframes glowPulse {
    0%,100% { box-shadow: 0 0 6px var(--glow-color, var(--accent-green)); }
    50% { box-shadow: 0 0 16px var(--glow-color, var(--accent-green)); }
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  @keyframes slideIn { from { opacity:0; transform:translateX(-6px); } to { opacity:1; transform:translateX(0); } }

  * { box-sizing: border-box; margin: 0; padding: 0; }
  html { font-size: 15px; }
  html, body {
    background: var(--bg-deep);
    color: var(--text);
    font-family: var(--font);
    line-height: 1.5;
    -webkit-font-smoothing: antialiased;
    min-height: 100vh;
  }
  body {
    background:
      radial-gradient(ellipse 600px 400px at 20% 0%, rgba(56,189,248,0.07), transparent),
      radial-gradient(ellipse 500px 500px at 80% 100%, rgba(167,139,250,0.05), transparent),
      linear-gradient(180deg, var(--bg) 0%, var(--bg-deep) 100%);
    background-attachment: fixed;
  }
  .app {
    max-width: 720px;
    margin: 0 auto;
    padding: 18px 16px 40px;
    animation: fadeIn 0.5s ease;
  }

  /* ─── Nav ─── */
  .nav {
    display: flex;
    gap: 4px;
    margin-bottom: 20px;
    padding: 4px;
    background: rgba(255,255,255,0.03);
    border: 1px solid var(--border);
    border-radius: 12px;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
  }
  .nav a {
    flex: 0 0 auto;
    padding: 7px 16px;
    font-size: 13px;
    font-weight: 600;
    color: var(--text-secondary);
    text-decoration: none;
    border-radius: 8px;
    transition: all 0.2s;
    white-space: nowrap;
  }
  .nav a:hover { color: var(--text); background: rgba(255,255,255,0.05); }
  .nav a.active {
    color: var(--text);
    background: rgba(88,166,255,0.15);
    box-shadow: 0 0 10px rgba(88,166,255,0.08);
  }

  /* ─── Header ─── */
  .header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 18px;
  }
  .header-left { display: flex; align-items: center; gap: 12px; }
  .header-logo {
    width: 36px; height: 36px;
    border-radius: 10px;
    background: linear-gradient(135deg, var(--accent-green), var(--accent-blue));
    display: flex; align-items: center; justify-content: center;
    font-size: 16px; font-weight: 800; color: #fff;
    flex-shrink: 0;
  }
  .header-info h1 {
    font-size: 18px;
    font-weight: 700;
    letter-spacing: -0.02em;
    background: linear-gradient(135deg, var(--accent-green), var(--accent-blue));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
  }
  .header-info .sub {
    font-size: 11px;
    color: var(--text-muted);
  }
  .header-right {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .live-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    background: var(--accent-red);
    animation: glowPulse 1.6s ease-in-out infinite;
    --glow-color: var(--accent-red);
  }
  .live-dot.green { background: var(--accent-green); --glow-color: var(--accent-green); }
  .live-dot.yellow { background: var(--accent-yellow); --glow-color: var(--accent-yellow); }
  .live-dot.red { background: var(--accent-red); --glow-color: var(--accent-red); }
  .ts {
    font-size: 13px;
    color: var(--text-muted);
    font-variant-numeric: tabular-nums;
    font-family: "SF Mono", "Fira Code", monospace;
  }

  /* ─── Status Bar ─── */
  .status-bar {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 14px 16px;
    border-radius: var(--radius);
    background: var(--bg-card);
    backdrop-filter: blur(12px);
    border: 1px solid var(--border);
    margin-bottom: 20px;
    transition: border-color 0.4s;
  }
  .status-bar.green { border-color: rgba(63,185,80,0.2); }
  .status-bar.yellow { border-color: rgba(210,153,34,0.2); }
  .status-bar.red { border-color: rgba(248,81,73,0.2); }
  .status-dot {
    width: 10px; height: 10px; border-radius: 50%; flex: 0 0 auto;
    animation: pulse 2s ease-in-out infinite;
  }
  .status-dot.green { background: var(--accent-green); box-shadow: 0 0 10px rgba(63,185,80,0.5); }
  .status-dot.yellow { background: var(--accent-yellow); box-shadow: 0 0 10px rgba(210,153,34,0.5); }
  .status-dot.red { background: var(--accent-red); box-shadow: 0 0 10px rgba(248,81,73,0.5); }
  .status-label { font-size: 14px; font-weight: 600; }
  .status-detail { font-size: 12px; color: var(--text-secondary); margin-left: auto; }

  /* ─── Section Title ─── */
  .section-title {
    font-size: 11px;
    font-weight: 700;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin: 22px 0 10px;
  }

  /* ─── Metrics Grid (2x2) ─── */
  .metrics {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 10px;
    margin-bottom: 20px;
  }
  .metric-card {
    background: var(--bg-card);
    backdrop-filter: blur(12px);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 12px 12px;
    text-align: center;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s;
  }
  .metric-card:hover { border-color: var(--border-light); }
  .metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    border-radius: var(--radius) var(--radius) 0 0;
  }
  .metric-card.blue::before { background: var(--accent-blue); }
  .metric-card.green::before { background: var(--accent-green); }
  .metric-card.yellow::before { background: var(--accent-yellow); }
  .metric-card .icon {
    font-size: 16px;
    margin-bottom: 2px;
    display: block;
    line-height: 1;
  }
  .metric-card .val {
    font-size: 24px;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
    line-height: 1.2;
  }
  .metric-card .lbl {
    font-size: 10px;
    color: var(--text-muted);
    margin-top: 4px;
    letter-spacing: 0.02em;
  }
  .metric-card.blue .val { color: var(--accent-blue); }
  .metric-card.green .val { color: var(--accent-green); }
  .metric-card.yellow .val { color: var(--accent-yellow); }

  /* ─── System Info ─── */
  .sys-row {
    display: flex;
    flex-wrap: wrap;
    gap: 6px 14px;
    padding: 12px 16px;
    background: var(--bg-card);
    backdrop-filter: blur(8px);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    margin-bottom: 20px;
    font-size: 13px;
    color: var(--text-muted);
  }
  .sys-row strong { color: var(--text); font-weight: 600; }

  /* ─── Tab Bar ─── */
  .tab-bar {
    display: flex;
    gap: 4px;
    margin-bottom: 14px;
    padding: 4px;
    background: rgba(255,255,255,0.03);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
  }
  .tab-btn {
    flex: 1;
    padding: 9px 0;
    text-align: center;
    font-size: 13px;
    font-weight: 600;
    border: none;
    border-radius: 8px;
    background: transparent;
    color: var(--text-muted);
    cursor: pointer;
    transition: all 0.2s ease;
  }
  .tab-btn:hover { color: var(--text); background: rgba(255,255,255,0.03); }
  .tab-btn.active {
    background: rgba(88,166,255,0.15);
    color: var(--text);
    box-shadow: 0 0 14px rgba(88,166,255,0.1);
  }
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }

  /* ─── Chart Area ─── */
  .chart-card {
    background: var(--bg-card);
    backdrop-filter: blur(12px);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 10px;
    overflow: hidden;
  }
  .chart-image {
    display: block;
    width: 100%;
    border: 0;
    border-radius: var(--radius-sm);
    min-height: 180px;
    background: rgba(0,0,0,0.2);
  }
  .chart-loading {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 180px;
    color: var(--text-muted);
    font-size: 13px;
    gap: 10px;
  }
  .chart-loading::before {
    content: '';
    width: 18px; height: 18px;
    border: 2px solid var(--border);
    border-top-color: var(--accent-blue);
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
  }

  /* ─── Market List ─── */
  .market-list { display: flex; flex-direction: column; gap: 6px; }
  .market-row {
    display: grid;
    grid-template-columns: 1fr auto auto auto;
    align-items: center;
    gap: 10px;
    padding: 12px 14px;
    background: var(--bg-card);
    backdrop-filter: blur(12px);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    font-size: 13px;
    transition: border-color 0.2s;
    animation: slideIn 0.3s ease;
    animation-fill-mode: backwards;
  }
  .market-row:hover { border-color: var(--border-light); }
  .market-row .name {
    font-weight: 600;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    font-size: 13px;
  }
  .market-row .trade {
    color: var(--accent-blue);
    text-align: right;
    font-variant-numeric: tabular-nums;
    font-weight: 600;
    font-size: 12px;
    padding: 3px 8px;
    background: rgba(88,166,255,0.08);
    border-radius: 5px;
    min-width: 50px;
  }
  .market-row .depth {
    color: var(--accent-green);
    text-align: right;
    font-variant-numeric: tabular-nums;
    font-weight: 600;
    font-size: 12px;
    padding: 3px 8px;
    background: rgba(63,185,80,0.08);
    border-radius: 5px;
    min-width: 50px;
  }
  .market-row .dot {
    width: 9px; height: 9px; border-radius: 50%; justify-self: center;
  }
  .market-row .dot.green { background: var(--accent-green); box-shadow: 0 0 6px rgba(63,185,80,0.5); }
  .market-row .dot.yellow { background: var(--accent-yellow); box-shadow: 0 0 6px rgba(210,153,34,0.5); }
  .market-row .dot.red { background: var(--accent-red); box-shadow: 0 0 6px rgba(248,81,73,0.5); }
  .market-row .label-row { font-size: 10px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.04em; }

  /* ─── Responsive ─── */
  @media (max-width: 480px) {
    .app { padding: 12px 10px; }
    .chart-card { margin: 0 -10px; border-radius: 0; border-left: 0; border-right: 0; }
    .header { flex-wrap: wrap; gap: 6px; }
    .header-right { margin-left: auto; }
  }
</style>
</head>
<body>
<div class="app" id="app">
  <nav class="nav">
    <a href="/">🏠 Home</a>
    <a href="/sentinel/">🛡️ Server Patrol</a>
    <a href="/receiver/" class="active">📡 BTC Receiver</a>
    <a href="/usage/">📊 Usage</a>
    <a href="/kanban/">📋 管理</a>
  </nav>
  <div class="header">
    <div class="header-left">
      <div class="header-logo">B</div>
      <div class="header-info">
        <h1>BTC Receiver</h1>
        <div class="sub">agg-btc-receiver · market data</div>
      </div>
    </div>
    <div class="header-right">
      <span class="live-dot green" id="status-dot"></span>
      <span class="ts" id="ts">--:--:--</span>
    </div>
  </div>
  <div class="status-bar green" id="status-bar">
    <span class="status-dot green" id="status-bar-dot"></span>
    <span class="status-label" id="status-label">接続中...</span>
    <span class="status-detail" id="status-detail"></span>
  </div>
  <div class="section-title">概要</div>
  <div class="metrics">
    <div class="metric-card blue"><span class="icon">💹</span><div class="val" id="m-trades">0</div><div class="lbl">取引/s</div></div>
    <div class="metric-card green"><span class="icon">📊</span><div class="val" id="m-depth">0</div><div class="lbl">板更新/s</div></div>
    <div class="metric-card"><span class="icon">🟢</span><div class="val" id="m-running">0/0</div><div class="lbl">稼働中</div></div>
    <div class="metric-card yellow"><span class="icon">📁</span><div class="val" id="m-files">0</div><div class="lbl">ファイル</div></div>
  </div>
  <div class="sys-row" id="sys-row"></div>
  <div class="section-title">データ</div>
  <div class="tab-bar">
    <button class="tab-btn active" onclick="switchTab('chart')">📊 チャート</button>
    <button class="tab-btn" onclick="switchTab('ranking')">📋 ランキング</button>
  </div>
  <div class="tab-panel active" id="tab-chart">
    <div class="chart-card"><img id="chart-image" src="api/chart?ts=0" class="chart-image" alt="trade rate chart" onerror="this.style.display='none';var n=this.nextElementSibling;if(n)n.style.display='flex'" onload="this.style.display='block';var n=this.nextElementSibling;if(n)n.style.display='none'"><div class="chart-loading" style="display:none">読み込み中...</div></div>
  </div>
  <div class="tab-panel" id="tab-ranking">
    <div class="market-list" id="market-list">
      <div class="market-row">
        <span></span>
        <span class="label-row" style="text-align:right">取引</span>
        <span class="label-row" style="text-align:right">板</span>
        <span></span>
      </div>
    </div>
  </div>
<script>
var nf = new Intl.NumberFormat('en-US');
function fmt(n) { return nf.format(Math.max(0, Math.round(Number(n)||0))); }
function clock(ts) { return ts ? new Date(ts).toLocaleTimeString([],{hour12:false}) : '--:--:--'; }
function tone(s) { return s==='running'?'green':s==='error'?'red':'yellow'; }
function refreshChart() {
  var img = document.getElementById('chart-image');
  if (!img) return;
  img.src = 'api/chart?ts=' + Date.now();
}

function switchTab(name) {
  document.querySelectorAll('.tab-btn').forEach(function(b){ b.classList.remove('active'); });
  document.querySelectorAll('.tab-panel').forEach(function(p){ p.classList.remove('active'); });
  var btn = document.querySelector('.tab-btn[onclick*="' + name + '"]');
  var panel = document.getElementById('tab-' + name);
  if (btn) btn.classList.add('active');
  if (panel) { panel.classList.add('active'); if (name === 'chart') refreshChart(); }
}

function render(data) {
  var markets = data && data.markets || {};
  var fc = data && data.fileCounts || {};
  var proc = data && data.proc || {};
  if (!Object.keys(markets).length) return;
  var mv = Object.values(markets);
  var t = mv.length, run = mv.filter(function(m){return m.state==='running'}).length;
  var tr = data.totalTradeRate || 0;
  var dr = data.totalDepthRate || 0;

  document.getElementById('ts').textContent = clock(data.ts);

  var sd = document.getElementById('status-bar-dot');
  var sl = document.getElementById('status-label');
  var sdet = document.getElementById('status-detail');
  var sbar = document.getElementById('status-bar');
  var ldot = document.getElementById('status-dot');
  if (!run || run < t) {
    sd.className='status-dot yellow'; sbar.className='status-bar yellow';
    sl.textContent='注意'; sdet.textContent=(t-run)+' 停止';
    if(ldot) { ldot.className='live-dot yellow'; }
  } else {
    sd.className='status-dot green'; sbar.className='status-bar green';
    sl.textContent='正常稼働中'; sdet.textContent=t+' 全マーケット稼働';
    if(ldot) { ldot.className='live-dot green'; }
  }

  document.getElementById('m-trades').textContent = fmt(tr);
  document.getElementById('m-depth').textContent = fmt(dr);
  document.getElementById('m-running').textContent = run+'/'+t;
  document.getElementById('m-files').textContent = fmt(fc.trades||0);

  var ups = proc.uptime? (function(s){var d=Math.floor(s/86400),h=Math.floor((s%86400)/3600),m=Math.floor((s%3600)/60);return(d>0?d+'日 ':'')+h+'時間'+m+'分';})(proc.uptime) : '--';
  document.getElementById('sys-row').innerHTML =
    'プロセス <strong>'+fmt(proc.heap||0)+'MB</strong>' +
    ' ／ メモリ <strong>'+fmt(proc.rss||0)+'MB</strong>' +
    ' ／ 稼働時間 <strong>'+ups+'</strong>';

  var list = Object.entries(markets).map(function(e){
    return {name:e[0], state:e[1].state||'unknown', tr:e[1].tradeRate||0, dr:e[1].depthRate||0};
  });
  list.sort(function(a,b){return b.tr!==a.tr?b.tr-a.tr:b.dr!==a.dr?b.dr-a.dr:a.name.localeCompare(b.name);});
  var html = '<div class="market-row"><span></span><span class="label-row" style="text-align:right">取引</span>' +
    '<span class="label-row" style="text-align:right">板</span><span></span></div>';
  list.forEach(function(m){
    html += '<div class="market-row">' +
      '<span class="name">'+m.name.replace(/_/g,' ')+'</span>' +
      '<span class="trade">'+fmt(m.tr)+'</span>' +
      '<span class="depth">'+fmt(m.dr)+'</span>' +
      '<span class="dot '+tone(m.state)+'"></span></div>';
  });
  document.getElementById('market-list').innerHTML = html;
}

(function loop(){
  fetch('api/status').then(function(r){return r.json();}).then(render).catch(function(){});
  setTimeout(loop, 3000);
})();
refreshChart();
setInterval(refreshChart, 10000);
</script>
</body>
</html>`;
