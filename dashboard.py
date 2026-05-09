"""
Fibbo Closed Trade Dashboard

Lightweight, dependency-free web dashboard for the Fibonacci scalper's
closed trade history. Reads the local SQLite database only.

Usage:
    python dashboard.py
    python dashboard.py --host 0.0.0.0 --port 8090
    python dashboard.py --db data/fib.db
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_DB = ROOT_DIR / "data" / "fib.db"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8090
DEFAULT_REFRESH_SECONDS = 30


def load_dashboard_config() -> dict[str, Any]:
    """Load dashboard defaults from config/settings.yaml when available."""
    settings_path = ROOT_DIR / "config" / "settings.yaml"
    if not settings_path.exists():
        return {}
    try:
        import yaml  # Existing project dependency.
    except Exception:
        return {}
    try:
        with settings_path.open("r", encoding="utf-8") as fh:
            settings = yaml.safe_load(fh) or {}
    except Exception:
        return {}
    dashboard = settings.get("dashboard") or {}
    return dashboard if isinstance(dashboard, dict) else {}


HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Fibbo // Closed Trade Console</title>
  <style>
    :root{
      --bg:#020403;--panel:#050806;--panel2:#07100b;--line:#14251b;--line2:#1d3d2a;
      --text:#d5f6df;--muted:#607565;--green:#16f37a;--green2:#07a84f;--red:#ff3b67;
      --amber:#ffd166;--cyan:#78ffe0;--shadow:0 0 24px rgba(22,243,122,.08)
    }
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(circle at 78% 18%,#082012 0,#020403 34%,#000 100%);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif;font-size:13px;overflow-x:hidden}
    body:before{content:"";position:fixed;inset:0;pointer-events:none;background:linear-gradient(rgba(255,255,255,.025) 1px,transparent 1px);background-size:100% 3px;mix-blend-mode:screen;opacity:.22;z-index:5}
    .mono{font-family:"JetBrains Mono","SFMono-Regular",Consolas,monospace}.wrap{min-height:100vh;padding:10px;display:grid;grid-template-rows:auto 1fr auto;gap:8px}
    header,.footer{border:1px solid var(--line);background:rgba(0,0,0,.72);box-shadow:var(--shadow)}
    header{height:54px;display:grid;grid-template-columns:280px 1fr auto;align-items:center;padding:0 14px;gap:14px}.brand{display:flex;align-items:center;gap:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase}.dot{width:10px;height:10px;border-radius:50%;background:var(--green);box-shadow:0 0 14px var(--green)}.sub{font-size:10px;color:var(--muted);font-weight:600;letter-spacing:.18em}.ticker{white-space:nowrap;overflow:hidden;color:var(--muted);font-size:11px}.ticker span{margin-right:24px}.clock{color:var(--green);font-weight:700}
    .grid{display:grid;grid-template-columns:260px minmax(620px,1fr) 280px;gap:8px}.panel{border:1px solid var(--line);background:linear-gradient(180deg,rgba(7,16,11,.86),rgba(0,0,0,.86));box-shadow:var(--shadow);min-width:0}.panel h3{margin:0;padding:10px 12px;border-bottom:1px solid var(--line);font-size:11px;letter-spacing:.16em;text-transform:uppercase;color:#b9dac0;display:flex;justify-content:space-between}.panel h3 b{color:var(--green);font-weight:700}.pad{padding:10px}
    .filters{display:grid;gap:8px}.input,select{width:100%;background:#020503;border:1px solid var(--line2);color:var(--text);padding:9px 10px;border-radius:2px;outline:none}.input:focus,select:focus{border-color:var(--green);box-shadow:0 0 0 1px rgba(22,243,122,.12)}label{font-size:10px;color:var(--muted);letter-spacing:.12em;text-transform:uppercase}.btn{background:linear-gradient(90deg,#053a1d,#0b7b3e);border:1px solid #13c96d;color:#001f0d;font-weight:900;padding:10px;cursor:pointer;text-transform:uppercase;letter-spacing:.12em}.btn:hover{filter:brightness(1.15)}
    .log{height:calc(100vh - 174px);overflow:hidden;color:#49624f;font-size:10px;line-height:1.55}.log div{display:flex;gap:8px;border-bottom:1px solid rgba(20,37,27,.35);padding:2px 0}.log .ok{color:var(--green)}.log .bad{color:var(--red)}
    .stats{display:grid;grid-template-columns:repeat(6,1fr);gap:8px;margin-bottom:8px}.stat{border:1px solid var(--line);background:rgba(0,0,0,.58);padding:11px;min-height:82px}.stat .k{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.13em}.stat .v{font-size:22px;margin-top:8px;font-weight:800}.pos{color:var(--green)}.neg{color:var(--red)}.flat{color:var(--text)}.tiny{font-size:11px;color:var(--muted);margin-top:4px}
    .main2{display:grid;grid-template-columns:1.4fr .9fr;gap:8px;margin-bottom:8px}.chart{height:270px;position:relative}.bars{height:190px}.canvas{width:100%;height:100%;display:block}.tableWrap{height:calc(100vh - 470px);min-height:300px;overflow:auto}table{width:100%;border-collapse:collapse;font-size:12px}th,td{border-bottom:1px solid rgba(20,37,27,.85);padding:9px 8px;text-align:left;white-space:nowrap}th{position:sticky;top:0;background:#030604;color:#7d9683;font-size:10px;text-transform:uppercase;letter-spacing:.11em;z-index:2}tr:hover td{background:rgba(22,243,122,.045)}.pill{padding:3px 7px;border:1px solid var(--line2);font-size:10px}.pill.long{color:var(--green);border-color:#0a7d41;background:rgba(22,243,122,.08)}.pill.short{color:var(--red);border-color:#913047;background:rgba(255,59,103,.08)}
    .sideStats{display:grid;gap:8px}.rank{display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid rgba(20,37,27,.75);padding:9px 0}.rank .name{font-weight:800}.meter{height:4px;background:#102018;margin-top:6px;overflow:hidden}.meter i{display:block;height:100%;background:linear-gradient(90deg,var(--green2),var(--green))}.orb{height:170px;display:grid;place-items:center;position:relative}.orb:before{content:"";width:118px;height:118px;border-radius:50%;background:radial-gradient(circle,#dfffee 0 1px,transparent 1.8px),radial-gradient(circle at 50% 50%,rgba(22,243,122,.18),transparent 60%);background-size:7px 7px,100% 100%;filter:blur(.2px);box-shadow:0 0 34px rgba(22,243,122,.3);animation:pulse 3s infinite}.orb span{position:absolute;bottom:18px;color:var(--muted);font-size:10px;letter-spacing:.18em}@keyframes pulse{50%{transform:scale(1.04);opacity:.85}}
    .footer{height:30px;display:flex;align-items:center;gap:20px;padding:0 12px;color:var(--muted);font-size:10px}.footer b{color:var(--green)}
    @media(max-width:1100px){.grid{grid-template-columns:1fr}.log{height:150px}.stats{grid-template-columns:repeat(2,1fr)}.main2{grid-template-columns:1fr}header{grid-template-columns:1fr}.ticker,.clock{display:none}.tableWrap{height:auto;max-height:540px}}
  </style>
</head>
<body>
<div class="wrap">
  <header>
    <div><div class="brand"><i class="dot"></i>FIBBO CLOSED TRADES</div><div class="sub">Fibonacci Scalper // History Console</div></div>
    <div class="ticker mono" id="ticker"></div>
    <div class="clock mono" id="clock">--:--:--</div>
  </header>
  <div class="grid">
    <aside class="panel"><h3>Market console <b id="activeCount">0 CLOSED</b></h3><div class="pad filters">
      <div><label>Range</label><select id="range"><option value="all">All history</option><option value="1">Today</option><option value="7">Last 7 days</option><option value="30">Last 30 days</option></select></div>
      <div><label>Symbol</label><input id="symbol" class="input mono" placeholder="e.g. BTC / TA / all" /></div>
      <div><label>Side</label><select id="side"><option value="">Long + Short</option><option>LONG</option><option>SHORT</option></select></div>
      <button class="btn" id="apply">Apply filters</button>
      <div class="log mono" id="log"></div>
    </div></aside>
    <main>
      <section class="stats" id="stats"></section>
      <section class="main2">
        <div class="panel"><h3>Equity curve <b>CUM NET PNL</b></h3><div class="pad chart"><canvas class="canvas" id="equity"></canvas></div></div>
        <div class="panel"><h3>Daily net PnL <b>BAR SCAN</b></h3><div class="pad bars"><canvas class="canvas" id="daily"></canvas></div></div>
      </section>
      <section class="panel"><h3>Closed trade history <b id="updated">SYNCING</b></h3><div class="tableWrap"><table><thead><tr><th>Closed</th><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>Margin</th><th>Lev</th><th>Net PnL</th><th>ROI</th><th>Reason</th><th>Score</th></tr></thead><tbody id="rows"></tbody></table></div></section>
    </main>
    <aside class="panel"><h3>Scanner stats <b>FIB ONLY</b></h3><div class="pad sideStats">
      <div class="orb"><span class="mono">ACTIVE // SCANNING</span></div>
      <div id="sidebox"></div>
      <h3 style="border:0;padding:10px 0 0">Top symbols <b>NET</b></h3><div id="symbols"></div>
      <h3 style="border:0;padding:10px 0 0">Best / Worst <b>EXTREMES</b></h3><div id="extremes"></div>
    </div></aside>
  </div>
  <div class="footer mono"><span><b>DATABASE</b> data/fib.db</span><span><b>STATUS</b> READ ONLY</span><span><b>THEME</b> PUMPRADAR-STYLE TERMINAL</span></div>
</div>
<script>
const $=id=>document.getElementById(id); let state=null;
function fmt(n,d=2){return Number(n||0).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d})}
function money(n){return (n>=0?'+':'-')+'$'+fmt(Math.abs(n))}
function pct(n){return (n>=0?'+':'')+fmt(n)+'%'}
function cls(n){return n>0?'pos':n<0?'neg':'flat'}
function t(ms){if(!ms)return '-'; return new Date(ms).toLocaleString(undefined,{month:'short',day:'2-digit',hour:'2-digit',minute:'2-digit'})}
function stat(k,v,sub,c){return `<div class="stat"><div class="k">${k}</div><div class="v mono ${c||''}">${v}</div><div class="tiny">${sub||''}</div></div>`}
async function load(){
  const q=new URLSearchParams({range:$('range').value,symbol:$('symbol').value,side:$('side').value});
  const res=await fetch('/api/summary?'+q); state=await res.json(); render();
}
function render(){const s=state.summary,tr=state.trades;
 $('activeCount').textContent=`${s.total_trades} CLOSED`; $('updated').textContent=new Date().toLocaleTimeString();
 $('ticker').innerHTML=[`NET ${money(s.net_pnl)}`,`WIN ${fmt(s.win_rate,1)}%`,`PF ${fmt(s.profit_factor,2)}`,`AVG ROI ${pct(s.avg_roi)}`,`FEES $${fmt(s.fees)}`].map(x=>`<span>${x}</span>`).join('');
 $('stats').innerHTML=[stat('Total net',money(s.net_pnl),'after fees',cls(s.net_pnl)),stat('Win rate',fmt(s.win_rate,1)+'%',`${s.wins} wins / ${s.losses} losses`,'pos'),stat('Closed trades',s.total_trades,'filled exits','flat'),stat('Profit factor',fmt(s.profit_factor,2),'gross wins / losses',s.profit_factor>=1?'pos':'neg'),stat('Average ROI',pct(s.avg_roi),'net pnl / margin',cls(s.avg_roi)),stat('Fees paid','$'+fmt(s.fees),'commission drag','flat')].join('');
 $('rows').innerHTML=tr.map(r=>`<tr><td class="mono">${t(r.closed_at)}</td><td><b>${r.symbol}</b></td><td><span class="pill ${r.direction.toLowerCase()}">${r.direction}</span></td><td class="mono">${fmt(r.entry_price,6)}</td><td class="mono">${fmt(r.exit_price,6)}</td><td class="mono">$${fmt(r.margin_used)}</td><td class="mono">${r.leverage}x</td><td class="mono ${cls(r.net_pnl)}">${money(r.net_pnl)}</td><td class="mono ${cls(r.roi_pct)}">${pct(r.roi_pct)}</td><td>${r.close_reason||'-'}</td><td class="mono">${r.confluence_score||0}</td></tr>`).join('');
 $('log').innerHTML=tr.slice(0,80).map(r=>`<div><span>${t(r.closed_at)}</span><span class="${r.net_pnl>=0?'ok':'bad'}">${r.symbol}</span><span>${money(r.net_pnl)}</span></div>`).join('');
 $('sidebox').innerHTML=`<div class="rank"><div><div class="name">LONG</div><div class="tiny">${s.long.count} trades</div></div><div class="mono ${cls(s.long.net)}">${money(s.long.net)}</div></div><div class="rank"><div><div class="name">SHORT</div><div class="tiny">${s.short.count} trades</div></div><div class="mono ${cls(s.short.net)}">${money(s.short.net)}</div></div>`;
 const maxSym=Math.max(...state.symbols.map(x=>Math.abs(x.net_pnl)),1); $('symbols').innerHTML=state.symbols.slice(0,7).map(x=>`<div class="rank"><div style="flex:1"><div class="name">${x.symbol}</div><div class="meter"><i style="width:${Math.max(5,Math.abs(x.net_pnl)/maxSym*100)}%"></i></div></div><div class="mono ${cls(x.net_pnl)}">${money(x.net_pnl)}</div></div>`).join('');
 $('extremes').innerHTML=[state.best,state.worst].filter(Boolean).map(x=>`<div class="rank"><div><div class="name">${x.symbol}</div><div class="tiny">${t(x.closed_at)} // ${x.direction}</div></div><div class="mono ${cls(x.net_pnl)}">${money(x.net_pnl)}</div></div>`).join('');
 drawLine($('equity'),state.equity); drawBars($('daily'),state.daily);
}
function prep(c){const r=c.getBoundingClientRect(),d=window.devicePixelRatio||1;c.width=r.width*d;c.height=r.height*d;const x=c.getContext('2d');x.scale(d,d);x.clearRect(0,0,r.width,r.height);return [x,r.width,r.height]}
function grid(ctx,w,h){ctx.strokeStyle='rgba(22,243,122,.10)';ctx.lineWidth=1;for(let i=1;i<5;i++){ctx.beginPath();ctx.moveTo(0,h*i/5);ctx.lineTo(w,h*i/5);ctx.stroke()}}
function drawLine(c,data){const [ctx,w,h]=prep(c);grid(ctx,w,h); if(!data.length)return; const vals=data.map(x=>x.value),min=Math.min(...vals,0),max=Math.max(...vals,1),pad=12; ctx.strokeStyle='#16f37a';ctx.lineWidth=2;ctx.beginPath();data.forEach((p,i)=>{const x=pad+i*Math.max(1,(w-pad*2)/(data.length-1||1)), y=h-pad-((p.value-min)/(max-min||1))*(h-pad*2); i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.stroke();ctx.lineTo(w-pad,h-pad);ctx.lineTo(pad,h-pad);ctx.fillStyle='rgba(22,243,122,.08)';ctx.fill()}
function drawBars(c,data){const [ctx,w,h]=prep(c);grid(ctx,w,h); if(!data.length)return; const max=Math.max(...data.map(x=>Math.abs(x.net_pnl)),1), bw=w/data.length*.62; data.forEach((p,i)=>{const x=i*w/data.length+(w/data.length-bw)/2, mid=h/2, bh=Math.abs(p.net_pnl)/max*(h*.42); ctx.fillStyle=p.net_pnl>=0?'#16f37a':'#ff3b67'; ctx.fillRect(x,p.net_pnl>=0?mid-bh:mid,bw,bh);}); ctx.strokeStyle='rgba(213,246,223,.25)';ctx.beginPath();ctx.moveTo(0,h/2);ctx.lineTo(w,h/2);ctx.stroke()}
setInterval(()=>{$('clock').textContent=new Date().toLocaleTimeString()},1000);$('apply').onclick=load;window.onresize=()=>state&&render();load();setInterval(load,__REFRESH_MS__);
</script>
</body>
</html>"""


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def parse_closed_after(days: str) -> int | None:
    if days == "all":
        return None
    try:
        d = max(1, int(days))
    except ValueError:
        return None
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return now_ms - d * 24 * 60 * 60 * 1000


def load_dashboard_data(db_path: Path, params: dict[str, list[str]]) -> dict[str, Any]:
    symbol = (params.get("symbol", [""])[0] or "").strip().upper()
    side = (params.get("side", [""])[0] or "").strip().upper()
    range_days = params.get("range", ["all"])[0]
    closed_after = parse_closed_after(range_days)

    where = ["status = 'CLOSED'", "closed_at IS NOT NULL", "closed_at > 0"]
    args: list[Any] = []
    if symbol:
        where.append("UPPER(symbol) LIKE ?")
        args.append(f"%{symbol}%")
    if side in {"LONG", "SHORT"}:
        where.append("direction = ?")
        args.append(side)
    if closed_after is not None:
        where.append("closed_at >= ?")
        args.append(closed_after)

    sql = f"""
        SELECT id, symbol, direction, entry_price, exit_price, margin_used, leverage,
               pnl, fees, net_pnl, confluence_score, quality, regime,
               opened_at, closed_at, close_reason, metadata
        FROM trades
        WHERE {' AND '.join(where)}
        ORDER BY closed_at DESC
    """

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        trades = rows_to_dicts(con.execute(sql, args).fetchall())
    finally:
        con.close()

    for trade in trades:
        margin = trade.get("margin_used") or 0
        trade["roi_pct"] = ((trade.get("net_pnl") or 0) / margin * 100) if margin else 0

    wins = [t for t in trades if (t.get("net_pnl") or 0) > 0]
    losses = [t for t in trades if (t.get("net_pnl") or 0) < 0]
    gross_win = sum(t["net_pnl"] for t in wins)
    gross_loss = abs(sum(t["net_pnl"] for t in losses))
    net_pnl = sum(t.get("net_pnl") or 0 for t in trades)
    fees = sum(t.get("fees") or 0 for t in trades)
    total_margin = sum(t.get("margin_used") or 0 for t in trades)

    by_side = {"LONG": {"count": 0, "net": 0.0}, "SHORT": {"count": 0, "net": 0.0}}
    by_symbol: dict[str, dict[str, Any]] = defaultdict(lambda: {"symbol": "", "count": 0, "net_pnl": 0.0})
    by_day: dict[str, float] = defaultdict(float)
    cumulative = 0.0
    equity = []

    for trade in sorted(trades, key=lambda x: x.get("closed_at") or 0):
        pnl = trade.get("net_pnl") or 0
        cumulative += pnl
        equity.append({"time": trade.get("closed_at"), "value": cumulative})
        day = datetime.fromtimestamp((trade.get("closed_at") or 0) / 1000, timezone.utc).strftime("%Y-%m-%d")
        by_day[day] += pnl
        if trade.get("direction") in by_side:
            by_side[trade["direction"]]["count"] += 1
            by_side[trade["direction"]]["net"] += pnl
        sym = trade.get("symbol") or "UNKNOWN"
        by_symbol[sym]["symbol"] = sym
        by_symbol[sym]["count"] += 1
        by_symbol[sym]["net_pnl"] += pnl

    best = max(trades, key=lambda x: x.get("net_pnl") or 0, default=None)
    worst = min(trades, key=lambda x: x.get("net_pnl") or 0, default=None)

    return {
        "summary": {
            "total_trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": (len(wins) / len(trades) * 100) if trades else 0,
            "net_pnl": net_pnl,
            "fees": fees,
            "profit_factor": (gross_win / gross_loss) if gross_loss else (gross_win if gross_win else 0),
            "avg_roi": (net_pnl / total_margin * 100) if total_margin else 0,
            "long": by_side["LONG"],
            "short": by_side["SHORT"],
        },
        "trades": trades[:500],
        "equity": equity,
        "daily": [{"date": k, "net_pnl": v} for k, v in sorted(by_day.items())],
        "symbols": sorted(by_symbol.values(), key=lambda x: abs(x["net_pnl"]), reverse=True),
        "best": best,
        "worst": worst,
    }


class DashboardHandler(BaseHTTPRequestHandler):
    db_path: Path = DEFAULT_DB
    refresh_seconds: int = DEFAULT_REFRESH_SECONDS

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {self.address_string()} {fmt % args}")

    def send_text(self, body: str, content_type: str = "text/html; charset=utf-8", status: int = 200) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802 - stdlib API
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = HTML.replace("__REFRESH_MS__", str(max(5, self.refresh_seconds) * 1000))
            self.send_text(body)
            return
        if parsed.path == "/api/summary":
            if not self.db_path.exists():
                self.send_text(json.dumps({"error": f"DB not found: {self.db_path}"}), "application/json", 404)
                return
            payload = load_dashboard_data(self.db_path, parse_qs(parsed.query))
            self.send_text(json.dumps(payload, default=str), "application/json")
            return
        self.send_text("Not found", "text/plain", 404)


def parse_args() -> argparse.Namespace:
    cfg = load_dashboard_config()
    parser = argparse.ArgumentParser(description="Fibbo closed trade dashboard")
    parser.add_argument("--host", default=cfg.get("host", DEFAULT_HOST), help=f"Bind host, default: {DEFAULT_HOST}")
    parser.add_argument("--port", type=int, default=cfg.get("port", DEFAULT_PORT), help=f"Bind port, default: {DEFAULT_PORT}")
    parser.add_argument("--db", default=cfg.get("database_path", str(DEFAULT_DB)), help="SQLite DB path, default: data/fib.db")
    parser.add_argument("--refresh", type=int, default=cfg.get("refresh_seconds", DEFAULT_REFRESH_SECONDS), help="Browser refresh interval in seconds")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.is_absolute():
        db_path = ROOT_DIR / db_path
    DashboardHandler.db_path = db_path
    DashboardHandler.refresh_seconds = args.refresh
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"Fibbo dashboard: http://{args.host}:{args.port}")
    print(f"Reading: {db_path}")
    server.serve_forever()


if __name__ == "__main__":
    main()
