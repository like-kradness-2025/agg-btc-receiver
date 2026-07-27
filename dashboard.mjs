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
import path from 'node:path';
import os from 'node:os';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const PORT = parseInt(process.argv.includes('--port')
  ? process.argv[process.argv.indexOf('--port') + 1] : '3847', 10);
const ROOT_DIR = path.dirname(fileURLToPath(import.meta.url));
const HEALTH_FILE = process.argv.includes('--health')
  ? process.argv[process.argv.indexOf('--health') + 1]
  : path.join('/home/weed420/Tool/agg-btc-receiver', 'data', 'live_db', 'health.jsonl');
const RATE_HISTORY_FILE = '/tmp/agg-trade-rate-history.jsonl';
const POLL_MS = 3000;

// ── State ───────────────────────────────────────────────────────────────────
const MAX_HISTORY = 30;
let rawCountsSnapshot = null;
let rawCountsHistory = [];

// ── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Read latest N rows from health.jsonl and compute per-market rates.
 * Returns same shape as the old readAllRawCounts so the rest of the code works unchanged.
 */
function readHealthSnapshot(count = 2) {
  const out = { ts: Date.now(), markets: {} };
  let lines = [];
  try {
    const content = fs.readFileSync(HEALTH_FILE, 'utf-8');
    lines = content.trim().split('\n').filter(Boolean).slice(-count);
  } catch { return out; }
  if (!lines.length) return out;

  // Parse lines → { ts, state, markets }
  const snapshots = lines.map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  if (!snapshots.length) return out;
  const latest = snapshots[snapshots.length - 1];
  const prev = snapshots.length > 1 ? snapshots[0] : null;

  out.ts = latest.ts;
  for (const [market, info] of Object.entries(latest.markets || {})) {
    const prevInfo = prev?.markets?.[market];
    const depthDiff = (info.depthMsgCount || 0) - (prevInfo?.depthMsgCount || 0);
    const tradeDiff = (info.tradeMsgCount || 0) - (prevInfo?.tradeMsgCount || 0);
    const intervalSec = prev ? (latest.ts - prev.ts) / 1000 : 1;
    out.markets[market] = {
      state: info.state || 'unknown',
      tradeRate: intervalSec > 0 ? Math.round(Math.max(0, tradeDiff) / intervalSec) : 0,
      depthRate: intervalSec > 0 ? Math.round(Math.max(0, depthDiff) / intervalSec) : 0,
      latestTs: info.lastTradeMsgAt || info.lastDepthMsgAt || latest.ts,
      depthMsgCount: info.depthMsgCount || 0,
      tradeMsgCount: info.tradeMsgCount || 0,
      reconnects: info.reconnectCount || 0,
      connectedAt: info.connectedAt || 0,
    };
  }
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
  } catch {}
}

// ── Periodic poll ────────────────────────────────────────────────────────────
function refreshRawCounts() {
  const snap = readHealthSnapshot(2);
  if (snap && snap.markets) {
    if (rawCountsSnapshot) { appendRateHistory(snap); }
    rawCountsSnapshot = snap;
    rawCountsHistory.push(snap);
    if (rawCountsHistory.length > MAX_HISTORY) rawCountsHistory.shift();
  }
}

async function getFileCounts() {
  const counts = { trades: 0, book_updates: 0, liquidations: 0, open: 0 };
  // File counts are no longer derived from raw files; use health snapshot summary
  const snap = rawCountsSnapshot;
  if (snap?.markets) {
    for (const m of Object.values(snap.markets)) {
      counts.trades += m.tradeMsgCount || 0;
      counts.book_updates += m.depthMsgCount || 0;
    }
  }
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

  if (url.pathname === '/api/history') {
    const limit = parseInt(url.searchParams.get('limit')) || 240;
    const lines = [];
    try {
      const content = fs.readFileSync(RATE_HISTORY_FILE, 'utf-8');
      const all = content.trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
      // Keep last N per market, group by ts
      const byMarket = {};
      for (const entry of all) {
        if (!byMarket[entry.market]) byMarket[entry.market] = [];
        byMarket[entry.market].push({ ts: entry.ts * 1000, trade: entry.tradeMsgCount, depth: entry.depthMsgCount });
      }
      for (const [m, pts] of Object.entries(byMarket)) {
        byMarket[m] = pts.slice(-limit);
      }
      lines.push(byMarket);
    } catch {}
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ history: lines.length ? lines[0] : {} }));
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
  // Defer initial poll so the server can accept connections first
  setTimeout(() => { refreshRawCounts(); }, 200);
  setInterval(refreshRawCounts, 5000);
});

// ── Dashboard HTML ──────────────────────────────────────────────────────────

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>BTC Receiver — EffiZen</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4" integrity="sha384-jb8JQMbMoBUzgWatfe6COACi2ljcDdZQ2OxczGA3bGNeWe+6DChMTBJemed7ZnvJ" crossorigin="anonymous"></script>
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
    --accent-orange: #f97316;
    --radius: 14px;
    --radius-sm: 10px;
  }
  @keyframes fadeIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }
  @keyframes glowPulse { 0%,100% { box-shadow:0 0 8px var(--glow-color); } 50% { box-shadow:0 0 18px var(--glow-color); } }
  @keyframes livePulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.35;transform:scale(1.3)} }
  * { box-sizing:border-box; margin:0; padding:0; }
  html { font-size: 15px; }
  html, body { background: var(--bg-deep); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans JP", Helvetica, Arial, sans-serif; line-height: 1.5; -webkit-font-smoothing: antialiased; min-height: 100vh; }
  body { background: radial-gradient(ellipse 600px 400px at 20% 0%, rgba(56,189,248,0.07), transparent), radial-gradient(ellipse 500px 500px at 80% 100%, rgba(167,139,250,0.05), transparent), linear-gradient(180deg, var(--bg) 0%, var(--bg-deep) 100%); background-attachment: fixed; }
  .app { max-width:960px; margin:0 auto; padding:18px 16px 50px; animation:fadeIn 0.5s ease; }
  .nav { display:flex; gap:4px; margin-bottom:20px; padding:4px; background:rgba(255,255,255,0.03); border:1px solid var(--border); border-radius:12px; overflow-x:auto; }
  .nav a { flex:0 0 auto; padding:7px 16px; font-size:13px; font-weight:600; color:var(--text-secondary); text-decoration:none; border-radius:8px; transition:all 0.2s; white-space:nowrap; }
  .nav a:hover { color:var(--text); background:rgba(255,255,255,0.05); }
  .nav a.active { color:var(--text); background:rgba(88,166,255,0.15); box-shadow:0 0 10px rgba(88,166,255,0.08); }
  .header { display:flex; gap:16px; align-items:center; padding:20px 0 16px; }
  .header-icon { width:48px; height:48px; border-radius:14px; background:linear-gradient(135deg,var(--accent-green),var(--accent-cyan)); display:flex; align-items:center; justify-content:center; font-size:22px; flex-shrink:0; box-shadow:0 0 24px rgba(63,185,80,0.15); }
  .header-info h1 { font-size:22px; font-weight:800; letter-spacing:-0.02em; margin-bottom:3px; }
  .header-info .sub { font-size:12px; color:var(--text-muted); display:flex; gap:14px; flex-wrap:wrap; }
  .header-info .sub .dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:4px; animation:livePulse 1.6s ease-in-out infinite; }
  .header-info .sub .dot.green { background:var(--accent-green); box-shadow:0 0 8px rgba(63,185,80,0.7); }
  .header-info .sub .dot.red { background:var(--accent-red); box-shadow:0 0 8px rgba(248,81,73,0.7); }

  /* Metrics row */
  .metrics { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:20px; }
  .metric { background:var(--bg-card); backdrop-filter:blur(12px); border:1px solid var(--border); border-radius:var(--radius); padding:16px; text-align:center; transition:border-color 0.2s; position:relative; overflow:hidden; }
  .metric:hover { border-color:var(--border-light); }
  .metric .icon { font-size:18px; margin-bottom:2px; }
  .metric .val { font-size:28px; font-weight:800; font-variant-numeric:tabular-nums; line-height:1.2; letter-spacing:-0.02em; }
  .metric .val.blue { color:var(--accent-blue); }
  .metric .val.green { color:var(--accent-green); }
  .metric .val.yellow { color:var(--accent-yellow); }
  .metric .val.purple { color:var(--accent-purple); }
  .metric .lbl { font-size:10px; color:var(--text-muted); margin-top:4px; letter-spacing:0.03em; text-transform:uppercase; }

  /* Sections */
  .sect { font-size:11px; font-weight:700; color:var(--text-secondary); letter-spacing:0.06em; margin:24px 0 10px; display:flex; align-items:center; gap:8px; }
  .sect .badge { font-size:9px; padding:1px 7px; border-radius:999px; background:rgba(255,255,255,0.04); color:var(--text-muted); font-weight:600; }

  /* Charts grid */
  .chart-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:20px; }
  .chart-card { background:var(--bg-card); backdrop-filter:blur(12px); border:1px solid var(--border); border-radius:var(--radius); padding:14px; }
  .chart-title { font-size:12px; font-weight:700; margin-bottom:8px; color:var(--text-secondary); display:flex; justify-content:space-between; }
  .chart-wrap { height:160px; position:relative; }
  @media (max-width:640px) { .chart-grid { grid-template-columns:1fr; } }

  /* Market ranking table */
  .mkt-header, .mkt-row { display:grid; grid-template-columns:1fr 80px 80px 80px 20px; gap:8px; align-items:center; padding:10px 14px; font-size:13px; }
  .mkt-header { font-size:10px; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.04em; border-bottom:1px solid var(--border); margin-bottom:4px; }
  .mkt-row { border-radius:var(--radius-sm); background:var(--bg-card); backdrop-filter:blur(8px); border:1px solid var(--border); margin-bottom:4px; transition:all 0.15s; }
  .mkt-row:hover { border-color:var(--border-light); }
  .mkt-row .name { font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .mkt-row .num { font-variant-numeric:tabular-nums; font-weight:600; text-align:right; font-size:12px; }
  .mkt-row .num.trade { color:var(--accent-blue); }
  .mkt-row .num.depth { color:var(--accent-green); }
  .mkt-row .num.rate { color:var(--accent-cyan); }
  .mkt-row .dot { width:10px; height:10px; border-radius:50%; justify-self:center; }
  .mkt-row .dot.green { background:var(--accent-green); box-shadow:0 0 6px rgba(63,185,80,0.5); }
  .mkt-row .dot.yellow { background:var(--accent-yellow); box-shadow:0 0 6px rgba(210,153,34,0.5); }
  .mkt-row .dot.red { background:var(--accent-red); box-shadow:0 0 6px rgba(248,81,73,0.5); }

  /* Status bar */
  .status-bar { display:flex; align-items:center; gap:14px; padding:14px 18px; background:var(--bg-card); backdrop-filter:blur(8px); border:1px solid var(--border); border-radius:var(--radius-sm); margin-bottom:20px; font-size:12px; color:var(--text-muted); }
  .status-bar .dot { width:10px; height:10px; border-radius:50%; flex-shrink:0; }
  .status-bar .dot.green { background:var(--accent-green); box-shadow:0 0 8px rgba(63,185,80,0.5); }
  .status-bar .dot.yellow { background:var(--accent-yellow); box-shadow:0 0 8px rgba(210,153,34,0.5); }
  .status-bar .dot.red { background:var(--accent-red); box-shadow:0 0 8px rgba(248,81,73,0.5); }
  .status-bar .label { font-weight:600; font-size:13px; color:var(--text); }
  .status-bar .detail { margin-left:auto; }

  /* Footer */
  .footer-bar { display:flex; justify-content:space-between; align-items:center; margin-top:24px; padding:12px 16px; background:var(--bg-card); backdrop-filter:blur(8px); border:1px solid var(--border); border-radius:var(--radius); font-size:11px; color:var(--text-muted); }
  .footer-bar strong { color:var(--text-secondary); font-weight:600; }
  .footer-bar .fresh.ok { color:var(--accent-green); }
  .footer-bar .fresh.stale { color:var(--accent-yellow); }

  @media (max-width:600px) {
    .metrics { grid-template-columns:repeat(2,1fr); }
    .app { padding:12px 10px 40px; }
    .mkt-header, .mkt-row { grid-template-columns:1fr 60px 60px 60px 16px; gap:6px; padding:8px 10px; font-size:11px; }
  }
</style>
</head>
<body>
<div class="app">
  <nav class="nav">
    <a href="/">🏠 ホーム</a>
    <a href="/sentinel/">🛡️ サーバー監視</a>
    <a href="/receiver/" class="active">📡 BTC受信</a>
    <a href="/usage/">📊 使用量</a>
    <a href="/kanban/">📋 管理</a>
  </nav>

  <div class="header">
    <div class="header-icon">₿</div>
    <div class="header-info">
      <h1>BTC Receiver</h1>
      <div class="sub">
        <span id="status-badge"><span class="dot green"></span> 稼働中</span>
        <span id="data-age">--:--:--</span>
        <span id="mkt-count">0 markets</span>
      </div>
    </div>
  </div>

  <div class="metrics" id="metrics"></div>

  <div class="status-bar" id="status-bar">
    <span class="dot green" id="status-dot"></span>
    <span class="label" id="status-label">読み込み中...</span>
    <span class="detail" id="status-detail"></span>
  </div>

  <div class="sect">📈 レート推移 <span class="badge" id="chart-points">0点</span></div>
  <div class="chart-grid">
    <div class="chart-card"><div class="chart-title"><span>💹 取引メッセージ/s</span><span style="color:var(--accent-blue);font-weight:400" id="chart-trade-sum">0</span></div><div class="chart-wrap"><canvas id="chart-trade"></canvas></div></div>
    <div class="chart-card"><div class="chart-title"><span>📊 板更新/s</span><span style="color:var(--accent-green);font-weight:400" id="chart-depth-sum">0</span></div><div class="chart-wrap"><canvas id="chart-depth"></canvas></div></div>
  </div>

  <div class="sect">📋 マーケットランキング <span class="badge" id="mkt-running">0/0稼働</span></div>
  <div class="mkt-header">
    <span>マーケット</span>
    <span style="text-align:right">取引/s</span>
    <span style="text-align:right">板/s</span>
    <span style="text-align:right">合計/s</span>
    <span></span>
  </div>
  <div id="market-list"></div>

  <div class="footer-bar">
    <span>🖥️ <strong id="sys-rss">--</strong>MB ／ 💾 <strong id="sys-files">0</strong> files</span>
    <span class="fresh ok" id="data-state">● 最新</span>
  </div>
</div>

<script>
var nf = new Intl.NumberFormat('en-US');
function fmt(n) { return nf.format(Math.max(0, Math.round(Number(n)||0))); }
function fmt1(n) { return Number(n||0).toFixed(1); }
var chartInstances = {};

function renderCharts(data) {
  var history = data && data.history || {};
  var markets = Object.keys(history);
  if (!markets.length) return;
  // Aggregate: sum of all markets per timestamp
  var tsMap = {};
  markets.forEach(function(m){
    (history[m]||[]).forEach(function(pt){
      if (!tsMap[pt.ts]) tsMap[pt.ts] = { trade: 0, depth: 0 };
      tsMap[pt.ts].trade += pt.trade || 0;
      tsMap[pt.ts].depth += pt.depth || 0;
    });
  });
  var sorted = Object.entries(tsMap).sort(function(a,b){ return a[0]-b[0]; });
  var labels = sorted.map(function(e){ return new Date(Number(e[0])).getTime(); });
  var tradeData = sorted.map(function(e){ return e[1].trade; });
  var depthData = sorted.map(function(e){ return e[1].depth; });

  var sumTr = tradeData.reduce(function(a,b){return a+b;}, 0);
  var sumDr = depthData.reduce(function(a,b){return a+b;}, 0);
  document.getElementById('chart-trade-sum').textContent = fmt(sumTr / tradeData.length);
  document.getElementById('chart-depth-sum').textContent = fmt(sumDr / depthData.length);
  document.getElementById('chart-points').textContent = labels.length + '点';

  var specs = [
    { id:'chart-trade', label:'取引/s', data:tradeData, color:'#58a6ff', fill:'rgba(88,166,255,0.08)' },
    { id:'chart-depth', label:'板更新/s', data:depthData, color:'#3fb950', fill:'rgba(63,185,80,0.08)' },
  ];
  specs.forEach(function(spec){
    var canvas = document.getElementById(spec.id);
    if (!canvas) return;
    if (chartInstances[spec.id]) chartInstances[spec.id].destroy();
    chartInstances[spec.id] = new Chart(canvas, {
      type: 'line',
      data: {
        labels: labels,
        datasets: [{
          label: spec.label,
          data: spec.data,
          borderColor: spec.color,
          backgroundColor: spec.fill,
          fill: true,
          tension: 0.15,
          borderWidth: 1.5,
          pointRadius: 0,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: false,
        interaction: { mode:'nearest', intersect:false },
        plugins: { legend: { display:false } },
        scales: {
          x: { type:'linear', ticks:{color:'#6e7681',maxTicksLimit:6,maxRotation:0,font:{size:9}, callback:function(v){ var d=new Date(v); return ('0'+d.getHours()).slice(-2)+':'+('0'+d.getMinutes()).slice(-2); }}, grid:{color:'rgba(255,255,255,0.04)'} },
          y: { beginAtZero:true, ticks:{color:'#8b949e',font:{size:9}}, grid:{color:'rgba(255,255,255,0.04)'} }
        }
      }
    });
  });
}

function render(data) {
  if (!data) return;
  var markets = data.markets || {};
  var fc = data.fileCounts || {};
  var proc = data.proc || {};
  var mv = Object.values(markets);
  var totalM = mv.length;
  var runM = mv.filter(function(m){return m.state==='running';}).length;
  var tr = data.totalTradeRate || 0;
  var dr = data.totalDepthRate || 0;

  document.getElementById('data-age').textContent = '🕐 ' + new Date(data.ts).toLocaleTimeString('ja-JP',{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});
  document.getElementById('mkt-count').textContent = totalM + ' markets';

  var sd = document.getElementById('status-dot');
  var sl = document.getElementById('status-label');
  var sdet = document.getElementById('status-detail');
  if (runM < totalM) {
    sd.className = 'dot yellow';
    sl.textContent = '一部停止';
    sdet.textContent = (totalM - runM) + '/' + totalM + ' 停止中';
    document.getElementById('status-badge').innerHTML = '<span class="dot yellow"></span> 一部停止';
  } else if (totalM === 0) {
    sd.className = 'dot red';
    sl.textContent = 'データなし';
    sdet.textContent = 'マーケット情報がありません';
    document.getElementById('status-badge').innerHTML = '<span class="dot red"></span> 停止中';
  } else {
    sd.className = 'dot green';
    sl.textContent = '正常稼働';
    sdet.textContent = totalM + ' 全マーケット稼働';
    document.getElementById('status-badge').innerHTML = '<span class="dot green"></span> 稼働中';
  }

  document.getElementById('metrics').innerHTML =
    '<div class="metric"><div class="icon">💹</div><div class="val blue">' + fmt(tr) + '</div><div class="lbl">取引/s</div></div>' +
    '<div class="metric"><div class="icon">📊</div><div class="val green">' + fmt(dr) + '</div><div class="lbl">板更新/s</div></div>' +
    '<div class="metric"><div class="icon">🟢</div><div class="val">' + runM + '/' + totalM + '</div><div class="lbl">マーケット稼働</div></div>' +
    '<div class="metric"><div class="icon">📁</div><div class="val purple">' + fmt(fc.trades||0) + '</div><div class="lbl">取引ファイル</div></div>';

  var ups = proc.uptime ? (function(s){var d=Math.floor(s/86400),h=Math.floor((s%86400)/3600),m=Math.floor((s%3600)/60);return(d>0?d+'日 ':'')+h+'時間'+m+'分';})(proc.uptime) : '--';
  document.getElementById('sys-rss').textContent = fmt(proc.heap||0);
  document.getElementById('sys-files').textContent = fmt((fc.trades||0)+(fc.book_updates||0));

  var list = Object.entries(markets).map(function(e){
    return {name:e[0], state:e[1].state||'unknown', tr:e[1].tradeRate||0, dr:e[1].depthRate||0};
  });
  list.sort(function(a,b){return (b.tr+b.dr)!==(a.tr+a.dr)?(b.tr+b.dr)-(a.tr+a.dr):a.name.localeCompare(b.name);});
  var html = '';
  list.forEach(function(m){
    var total = m.tr + m.dr;
    html += '<div class="mkt-row">' +
      '<span class="name">' + m.name.replace(/_/g,' ') + '</span>' +
      '<span class="num trade">' + fmt(m.tr) + '</span>' +
      '<span class="num depth">' + fmt(m.dr) + '</span>' +
      '<span class="num rate">' + fmt(total) + '</span>' +
      '<span class="dot ' + (m.state==='running'?'green':'yellow') + '"></span></div>';
  });
  document.getElementById('market-list').innerHTML = html;
  document.getElementById('mkt-running').textContent = runM + '/' + totalM + '稼働';
  document.getElementById('data-state').textContent = '● 最新 ' + new Date().toLocaleTimeString('ja-JP',{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});
}

// Load both
(function loop(){
  fetch('api/status').then(function(r){return r.json();}).then(render).catch(function(){});
  setTimeout(loop, 3000);
})();
fetch('api/history?limit=120').then(function(r){return r.json();}).then(renderCharts).catch(function(){});
setInterval(function(){ fetch('api/history?limit=120').then(function(r){return r.json();}).then(renderCharts).catch(function(){}); }, 15000);
</script>
</body>
</html>`;
