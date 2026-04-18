"""
dashboard/app.py — Dexter Pro Strategy Analytics Dashboard

Clean backtest vs real-trade comparison interface.

Usage:
    python -m dashboard.app
    python -m dashboard.app --port 8800
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from aiohttp import web

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent

# Ensure project root is on sys.path for sibling package imports
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DATA = ROOT / "data"

DB_PATHS = {
    "execution": DATA / "ctrader_openapi.db",
    "signals": DATA / "signal_history.db",
    "scalp": DATA / "scalp_signal_history.db",
    "backtest": ROOT / "backtest" / "backtest_results.db",
    "candles": ROOT / "backtest" / "candle_data.db",
}

STATIC_DIR = Path(__file__).resolve().parent

# Known strategy families
FAMILIES_META = {
    "canary":   {"full": "xau_scalp_default", "symbol": "XAUUSD", "status": "LIVE", "risk": 2.50},
    "pb":       {"full": "xau_scalp_pullback_limit", "symbol": "XAUUSD", "status": "LIVE", "risk": 2.50},
    "td":       {"full": "xau_scalp_tick_depth", "symbol": "XAUUSD", "status": "CANARY", "risk": 0.75},
    "fss":      {"full": "xau_scalp_flow_short_sidecar", "symbol": "XAUUSD", "status": "CANARY", "risk": 0.45},
    "mfu":      {"full": "xau_scalp_microtrend_follow_up", "symbol": "XAUUSD", "status": "CANARY", "risk": 0.65},
    "bs":       {"full": "xau_scalp_breakout_stop", "symbol": "XAUUSD", "status": "CANARY", "risk": 0.75},
    "fffs":     {"full": "xau_scalp_failed_fade_follow_stop", "symbol": "XAUUSD", "status": "PROBE", "risk": 0.75},
    "rr":       {"full": "xau_scalp_range_repair", "symbol": "XAUUSD", "status": "PROBE", "risk": 0.75},
    "bwl":      {"full": "btc_weekday_lob_momentum", "symbol": "BTCUSD", "status": "LIVE", "risk": 1.10},
    "ewp":      {"full": "eth_weekday_overlap_probe", "symbol": "ETHUSD", "status": "CANARY", "risk": 0.35},
    "cfs":      {"full": "crypto_flow_short", "symbol": "BTC/ETH", "status": "CANARY", "risk": 0.50},
    "cfb":      {"full": "crypto_flow_buy", "symbol": "BTC/ETH", "status": "CANARY", "risk": 0.50},
    "cwc":      {"full": "crypto_winner_confirmed", "symbol": "BTC/ETH", "status": "CANARY", "risk": 0.50},
    "cbr":      {"full": "crypto_behavioral_retest", "symbol": "BTC/ETH", "status": "CANARY", "risk": 0.50},
}

SESSION_RANGES = {
    "asian":    (0, 7),
    "london":   (7, 12),
    "overlap":  (12, 16),
    "new_york": (16, 21),
    "late_ny":  (21, 24),
}

# In-memory backtest task state
_bt_task: dict[str, Any] = {"status": "idle", "progress": "", "proc": None, "output": ""}


# ── DB Helpers ───────────────────────────────────────────────────────────────

def _connect(name: str) -> Optional[sqlite3.Connection]:
    path = DB_PATHS.get(name)
    if not path or not path.exists():
        return None
    try:
        conn = sqlite3.connect(str(path), timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def _rows(conn, sql, params=()) -> list[dict]:
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    except Exception as e:
        logger.debug("SQL error: %s", e)
        return []


def _parse_family(source: str) -> str:
    """Extract family key from source like 'scalp_xauusd:fss:canary'."""
    parts = str(source or "").split(":")
    if len(parts) >= 2 and parts[1] not in ("canary", "winner"):
        return parts[1]
    if len(parts) >= 1:
        s = parts[0]
        if "scheduled" in s:
            return "scheduled"
        if "xauusd" in s:
            return "canary"
        if "btcusd" in s:
            return "btc"
        if "ethusd" in s:
            return "eth"
    return str(source or "unknown")


def _hour_to_session(hour: int) -> str:
    for name, (start, end) in SESSION_RANGES.items():
        if start <= hour < end:
            return name
    return "off_hours"


# ── Real Trade Queries ───────────────────────────────────────────────────────

def _get_real_deals(symbol: str, from_dt: str, to_dt: str) -> list[dict]:
    """Get closed deals with PnL from ctrader_deals."""
    conn = _connect("execution")
    if not conn:
        return []
    try:
        sql = """
            SELECT deal_id, symbol, direction, volume, execution_price,
                   gross_profit_usd, pnl_usd, outcome, source, lane, execution_utc
            FROM ctrader_deals
            WHERE pnl_usd != 0
        """
        params: list = []
        if symbol and symbol != "ALL":
            sql += " AND symbol = ?"
            params.append(symbol)
        if from_dt:
            sql += " AND execution_utc >= ?"
            params.append(from_dt)
        if to_dt:
            sql += " AND execution_utc <= ?"
            params.append(to_dt)
        sql += " ORDER BY execution_utc DESC"
        return _rows(conn, sql, params)
    finally:
        conn.close()


def _get_real_all_deals(symbol: str, from_dt: str, to_dt: str) -> list[dict]:
    """Get ALL deals (including opens with pnl=0) for trade history display."""
    conn = _connect("execution")
    if not conn:
        return []
    try:
        sql = """
            SELECT deal_id, symbol, direction, volume, execution_price,
                   gross_profit_usd, pnl_usd, outcome, source, lane, execution_utc
            FROM ctrader_deals WHERE 1=1
        """
        params: list = []
        if symbol and symbol != "ALL":
            sql += " AND symbol = ?"
            params.append(symbol)
        if from_dt:
            sql += " AND execution_utc >= ?"
            params.append(from_dt)
        if to_dt:
            sql += " AND execution_utc <= ?"
            params.append(to_dt)
        sql += " ORDER BY execution_utc DESC LIMIT 200"
        return _rows(conn, sql, params)
    finally:
        conn.close()


def _compute_real_stats(deals: list[dict]) -> dict:
    """Compute performance stats from closed deals."""
    if not deals:
        return {
            "total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0,
            "total_pnl_usd": 0, "profit_factor": 0, "max_drawdown_usd": 0,
            "avg_winner_usd": 0, "avg_loser_usd": 0,
            "by_family": {}, "by_session": {}, "by_direction": {},
        }

    winners = [d for d in deals if d["pnl_usd"] > 0]
    losers = [d for d in deals if d["pnl_usd"] < 0]
    total_pnl = sum(d["pnl_usd"] for d in deals)
    gross_profit = sum(d["pnl_usd"] for d in winners)
    gross_loss = abs(sum(d["pnl_usd"] for d in losers))

    # Max drawdown
    equity, peak, max_dd = 0.0, 0.0, 0.0
    for d in sorted(deals, key=lambda x: x["execution_utc"]):
        equity += d["pnl_usd"]
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    # By family
    by_family: dict[str, dict] = {}
    for d in deals:
        fam = _parse_family(d.get("source", ""))
        if fam not in by_family:
            by_family[fam] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
        by_family[fam]["trades"] += 1
        by_family[fam]["pnl"] += d["pnl_usd"]
        if d["pnl_usd"] > 0:
            by_family[fam]["wins"] += 1
        elif d["pnl_usd"] < 0:
            by_family[fam]["losses"] += 1
    for v in by_family.values():
        v["win_rate"] = round(v["wins"] / v["trades"] * 100, 1) if v["trades"] else 0
        v["pnl"] = round(v["pnl"], 2)

    # By session (from execution_utc hour)
    by_session: dict[str, dict] = {}
    for d in deals:
        try:
            hour = int(d["execution_utc"][11:13])
        except (ValueError, TypeError, IndexError):
            hour = 0
        sess = _hour_to_session(hour)
        if sess not in by_session:
            by_session[sess] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
        by_session[sess]["trades"] += 1
        by_session[sess]["pnl"] += d["pnl_usd"]
        if d["pnl_usd"] > 0:
            by_session[sess]["wins"] += 1
        elif d["pnl_usd"] < 0:
            by_session[sess]["losses"] += 1
    for v in by_session.values():
        v["win_rate"] = round(v["wins"] / v["trades"] * 100, 1) if v["trades"] else 0
        v["pnl"] = round(v["pnl"], 2)

    # By direction
    by_direction: dict[str, dict] = {}
    for d in deals:
        dr = d.get("direction", "unknown")
        if dr not in by_direction:
            by_direction[dr] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
        by_direction[dr]["trades"] += 1
        by_direction[dr]["pnl"] += d["pnl_usd"]
        if d["pnl_usd"] > 0:
            by_direction[dr]["wins"] += 1
        elif d["pnl_usd"] < 0:
            by_direction[dr]["losses"] += 1
    for v in by_direction.values():
        v["win_rate"] = round(v["wins"] / v["trades"] * 100, 1) if v["trades"] else 0
        v["pnl"] = round(v["pnl"], 2)

    return {
        "total_trades": len(deals),
        "wins": len(winners),
        "losses": len(losers),
        "win_rate": round(len(winners) / len(deals) * 100, 1),
        "total_pnl_usd": round(total_pnl, 2),
        "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0,
        "max_drawdown_usd": round(max_dd, 2),
        "avg_winner_usd": round(gross_profit / len(winners), 2) if winners else 0,
        "avg_loser_usd": round(-gross_loss / len(losers), 2) if losers else 0,
        "by_family": by_family,
        "by_session": by_session,
        "by_direction": by_direction,
    }


# ── Backtest Queries ─────────────────────────────────────────────────────────

def _get_bt_runs(symbol: str = "") -> list[dict]:
    conn = _connect("backtest")
    if not conn:
        return []
    try:
        sql = "SELECT id, run_name, strategy, total_trades, win_rate, total_pnl_r, max_drawdown, profit_factor, start_date, end_date, created_at FROM backtest_runs ORDER BY id DESC"
        runs = _rows(conn, sql)
        if symbol and symbol != "ALL":
            runs = [r for r in runs if symbol.lower() in r.get("run_name", "").lower()]
        return runs
    finally:
        conn.close()


def _get_bt_report(run_id: int) -> dict:
    conn = _connect("backtest")
    if not conn:
        return {}
    try:
        row = conn.execute("SELECT report_json FROM backtest_runs WHERE id=?", (run_id,)).fetchone()
        if row and row["report_json"]:
            return json.loads(row["report_json"])
        return {}
    finally:
        conn.close()


def _get_coverage() -> list[dict]:
    conn = _connect("candles")
    if not conn:
        return []
    try:
        sql = """
            SELECT symbol, tf, COUNT(*) as bars,
                   MIN(ts) as earliest, MAX(ts) as latest
            FROM candles
            GROUP BY symbol, tf
            ORDER BY symbol, tf
        """
        return _rows(conn, sql)
    finally:
        conn.close()


# ── Comparison Logic ─────────────────────────────────────────────────────────

def _compare(real_stats: dict, bt_report: dict) -> dict:
    """Compare real vs backtest, weighted 70% real / 30% BT.

    Only compares unit-compatible metrics (%, ratios).
    USD vs R-multiple are shown side-by-side but not scored.
    """
    if not real_stats.get("total_trades") or not bt_report.get("total_trades"):
        return {"match_pct": 0, "metrics": [], "verdict": "Insufficient data"}

    metrics = []

    def _add(name, real_val, bt_val, fmt="pct", scored=True):
        if real_val is None or bt_val is None:
            return
        delta = bt_val - real_val
        if scored:
            denom = max(abs(real_val), 0.01)
            deviation = min(abs(delta) / denom, 1.0)
            match = round((1 - deviation) * 100, 0)
        else:
            match = -1  # not scored
        status = "good" if match >= 70 else "fair" if match >= 40 else "poor" if match >= 0 else "info"
        metrics.append({
            "name": name, "real": real_val, "bt": bt_val,
            "delta": round(delta, 2), "match": match, "status": status, "fmt": fmt,
        })

    # Comparable metrics (same units)
    _add("Win Rate %", real_stats.get("win_rate"), bt_report.get("win_rate"))
    _add("Profit Factor", real_stats.get("profit_factor"), bt_report.get("profit_factor"), fmt="num")
    _add("Trades", real_stats.get("total_trades"), bt_report.get("total_trades"), fmt="int", scored=False)

    # Direction breakdown — compare win rates per direction
    real_dirs = real_stats.get("by_direction", {})
    bt_dirs = bt_report.get("by_direction", {})
    for d in ("long", "short"):
        r_wr = real_dirs.get(d, {}).get("win_rate", None)
        b_wr = bt_dirs.get(d, {}).get("win_rate", None)
        if r_wr is not None and b_wr is not None:
            _add(f"{d.title()} WR%", r_wr, b_wr)

    # Session breakdown — compare best sessions
    real_sess = real_stats.get("by_session", {})
    bt_sess = bt_report.get("by_session", {})
    for s in ("london", "overlap", "new_york", "asian"):
        r_wr = real_sess.get(s, {}).get("win_rate", None)
        b_wr = bt_sess.get(s, {}).get("win_rate", None)
        if r_wr is not None and b_wr is not None:
            _add(f"{s.title()} WR%", r_wr, b_wr)

    # Different-unit metrics (shown but not scored)
    _add("Avg Winner", real_stats.get("avg_winner_usd"), bt_report.get("avg_winner_r"), fmt="val", scored=False)
    _add("Avg Loser", real_stats.get("avg_loser_usd"), bt_report.get("avg_loser_r"), fmt="val", scored=False)

    # Overall match = weighted average of scored metrics only
    scored = [m for m in metrics if m["match"] >= 0]
    avg_match = sum(m["match"] for m in scored) / len(scored) if scored else 0

    if avg_match >= 75:
        verdict = "BT closely matches Real — high confidence"
    elif avg_match >= 50:
        verdict = "BT partially matches Real — use with caution"
    else:
        verdict = "BT diverges from Real — investigate causes"

    return {
        "match_pct": round(avg_match),
        "metrics": metrics,
        "verdict": verdict,
        "weight_note": "Real 70% / Backtest 30% — Real data drives decisions",
    }


# ── Pipeline ─────────────────────────────────────────────────────────────────

def _get_pipeline(symbol: str = "") -> list[dict]:
    """Return development pipeline status for all families."""
    pipeline = []
    for key, meta in FAMILIES_META.items():
        if symbol and symbol != "ALL" and meta["symbol"] != symbol:
            continue
        pipeline.append({
            "family": key,
            "full_name": meta["full"],
            "symbol": meta["symbol"],
            "status": meta["status"],
            "risk_usd": meta["risk"],
        })
    return pipeline


# ── Route Handlers ───────────────────────────────────────────────────────────

async def handle_index(request: web.Request) -> web.FileResponse:
    return web.FileResponse(STATIC_DIR / "index.html")


async def handle_overview(request: web.Request) -> web.Response:
    """Main dashboard data: real stats + latest BT + comparison."""
    symbol = request.query.get("symbol", "XAUUSD").upper()
    from_dt = request.query.get("from", "")
    to_dt = request.query.get("to", "")

    # Real trades
    deals = _get_real_deals(symbol, from_dt, to_dt)
    real_stats = _compute_real_stats(deals)

    # Latest BT run for this symbol
    bt_runs = _get_bt_runs(symbol)
    bt_report = {}
    bt_summary = {}
    if bt_runs:
        bt_summary = bt_runs[0]
        bt_report = _get_bt_report(bt_runs[0]["id"])

    # Comparison
    comparison = _compare(real_stats, bt_report)

    # Pipeline
    pipeline = _get_pipeline(symbol)

    # Trade history — only closing deals (with PnL), not opening deals
    all_deals = _get_real_deals(symbol, from_dt, to_dt)

    return web.json_response({
        "symbol": symbol,
        "from": from_dt,
        "to": to_dt,
        "real": real_stats,
        "bt": {
            "summary": bt_summary,
            "report": bt_report,
        },
        "comparison": comparison,
        "pipeline": pipeline,
        "trades": all_deals[:100],
        "bt_runs": bt_runs,
    })


async def handle_bt_runs(request: web.Request) -> web.Response:
    symbol = request.query.get("symbol", "")
    runs = _get_bt_runs(symbol)
    return web.json_response(runs)


async def handle_bt_report(request: web.Request) -> web.Response:
    run_id = int(request.match_info["run_id"])
    report = _get_bt_report(run_id)
    return web.json_response(report)


async def handle_coverage(request: web.Request) -> web.Response:
    return web.json_response(_get_coverage())


async def handle_bt_run(request: web.Request) -> web.Response:
    """Trigger a new backtest run via subprocess."""
    global _bt_task
    if _bt_task["status"] == "running":
        return web.json_response({"error": "Backtest already running"}, status=409)

    body = await request.json()
    symbol = body.get("symbol", "XAUUSD")
    days = body.get("days", 5)
    from_date = body.get("from", "")
    to_date = body.get("to", "")
    source = body.get("source", "ctrader")

    cmd = [sys.executable, "-m", "backtest.run_backtest",
           "--symbol", symbol, "--days", str(days), "--source", source]
    if from_date:
        cmd.extend(["--from", from_date])
    if to_date:
        cmd.extend(["--to", to_date])

    _bt_task = {"status": "running", "progress": "Starting...", "output": "", "proc": None}

    async def _run():
        global _bt_task
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(ROOT),
            )
            _bt_task["proc"] = proc
            output_lines = []
            async for line in proc.stdout:
                text = line.decode("utf-8", errors="replace").strip()
                if text:
                    output_lines.append(text)
                    # Parse progress from output
                    if "Progress:" in text:
                        _bt_task["progress"] = text.split("Progress:")[-1].strip()
                    elif "Ingesting" in text or "Replaying" in text or "Resolving" in text:
                        _bt_task["progress"] = text
            await proc.wait()
            _bt_task["output"] = "\n".join(output_lines)
            _bt_task["status"] = "done" if proc.returncode == 0 else "error"
            _bt_task["progress"] = "Complete" if proc.returncode == 0 else f"Failed (exit {proc.returncode})"
        except Exception as e:
            _bt_task["status"] = "error"
            _bt_task["progress"] = str(e)

    asyncio.ensure_future(_run())
    return web.json_response({"status": "started", "command": " ".join(cmd)})


async def handle_stream_status(request: web.Request) -> web.Response:
    """GET /api/stream/status — streaming service health + data freshness."""
    try:
        from execution.stream_reader import StreamReader
        reader = StreamReader(db_path=str(DB_PATHS["execution"]))
        status = reader.get_stream_status()
        freshness = reader.get_data_freshness()
        margin = reader.get_margin_status()
        recent_exec = reader.get_recent_executions(limit=10)
        return web.json_response({
            "stream": status,
            "freshness": freshness,
            "margin": margin,
            "recent_executions": recent_exec,
        })
    except Exception as e:
        return web.json_response({
            "stream": {"running": False, "connected": False, "error": str(e)},
            "freshness": {},
            "margin": {"has_data": False},
            "recent_executions": [],
        })


async def handle_bt_progress(request: web.Request) -> web.Response:
    return web.json_response({
        "status": _bt_task["status"],
        "progress": _bt_task["progress"],
        "output": _bt_task.get("output", "")[-2000:],
    })


# ── App Factory ──────────────────────────────────────────────────────────────

def create_app() -> web.Application:
    app = web.Application()

    # CORS middleware
    @web.middleware
    async def cors_middleware(request, handler):
        if request.method == "OPTIONS":
            resp = web.Response()
        else:
            resp = await handler(request)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp

    app.middlewares.append(cors_middleware)

    app.router.add_get("/", handle_index)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/bt/runs", handle_bt_runs)
    app.router.add_get("/api/bt/report/{run_id}", handle_bt_report)
    app.router.add_get("/api/bt/coverage", handle_coverage)
    app.router.add_post("/api/bt/run", handle_bt_run)
    app.router.add_get("/api/bt/progress", handle_bt_progress)
    app.router.add_get("/api/stream/status", handle_stream_status)

    return app


def main():
    parser = argparse.ArgumentParser(description="Dexter Pro Analytics Dashboard")
    parser.add_argument("--port", type=int, default=8800, help="Port (default: 8800)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host (default: 0.0.0.0)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    app = create_app()
    print(f"\n  Dexter Pro Analytics Dashboard")
    print(f"  http://localhost:{args.port}\n")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
