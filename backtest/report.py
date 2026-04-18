"""
backtest/report.py — Analytics from a completed backtest run.
Generates win rate, PnL, drawdown, per-session/direction breakdowns.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

from backtest.virtual_executor import TradeResult

logger = logging.getLogger(__name__)


def generate_report(results: List[TradeResult], run_name: str = "") -> dict:
    """Generate comprehensive backtest analytics from trade results.

    Args:
        results: list of TradeResult from VirtualExecutor
        run_name: optional label for this run

    Returns:
        dict with all metrics
    """
    # Filter to only filled trades (exclude expired_no_fill)
    trades = [r for r in results if r.outcome != "expired_no_fill"]
    all_results = results  # keep for fill rate

    if not trades:
        return {
            "run_name": run_name,
            "total_signals": len(all_results),
            "total_trades": 0,
            "fill_rate": 0.0,
            "message": "No trades filled",
        }

    wins = [t for t in trades if t.pnl_r > 0]
    losses = [t for t in trades if t.pnl_r < 0]
    breakeven = [t for t in trades if t.pnl_r == 0]

    total_pnl_r = sum(t.pnl_r for t in trades)
    total_pnl_pips = sum(t.pnl_pips for t in trades)

    gross_profit = sum(t.pnl_r for t in wins) if wins else 0.0
    gross_loss = abs(sum(t.pnl_r for t in losses)) if losses else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

    # Max drawdown (peak-to-trough in cumulative R)
    cumulative = []
    running = 0.0
    for t in trades:
        running += t.pnl_r
        cumulative.append(running)

    max_dd = 0.0
    peak = 0.0
    for c in cumulative:
        if c > peak:
            peak = c
        dd = peak - c
        if dd > max_dd:
            max_dd = dd

    # Per-direction breakdown
    by_direction = _group_stats(trades, key=lambda t: t.direction)

    # Per-outcome breakdown
    outcome_counts = defaultdict(int)
    for t in trades:
        outcome_counts[t.outcome] += 1

    # Per-session breakdown (from signal time hour → session)
    by_session = _group_stats(trades, key=lambda t: _hour_to_session(t.entry_time))

    # Best / worst trade
    best = max(trades, key=lambda t: t.pnl_r)
    worst = min(trades, key=lambda t: t.pnl_r)

    avg_bars = sum(t.bars_held for t in trades) / len(trades)
    avg_winner_r = (sum(t.pnl_r for t in wins) / len(wins)) if wins else 0.0
    avg_loser_r = (sum(t.pnl_r for t in losses) / len(losses)) if losses else 0.0

    report = {
        "run_name": run_name,
        "total_signals": len(all_results),
        "total_trades": len(trades),
        "fill_rate": round(len(trades) / len(all_results) * 100, 1) if all_results else 0.0,
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(breakeven),
        "win_rate": round(len(wins) / len(trades) * 100, 1) if trades else 0.0,
        "total_pnl_r": round(total_pnl_r, 2),
        "total_pnl_pips": round(total_pnl_pips, 1),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown_r": round(max_dd, 2),
        "avg_winner_r": round(avg_winner_r, 2),
        "avg_loser_r": round(avg_loser_r, 2),
        "avg_bars_held": round(avg_bars, 1),
        "best_trade_r": round(best.pnl_r, 2),
        "worst_trade_r": round(worst.pnl_r, 2),
        "outcome_counts": dict(outcome_counts),
        "by_direction": by_direction,
        "by_session": by_session,
    }
    return report


def print_report(report: dict) -> None:
    """Print a formatted backtest report to console."""
    print("\n" + "=" * 60)
    print(f"  BACKTEST REPORT: {report.get('run_name', 'unnamed')}")
    print("=" * 60)

    print(f"\n  Signals: {report.get('total_signals', 0)}  |  "
          f"Filled: {report.get('total_trades', 0)}  |  "
          f"Fill rate: {report.get('fill_rate', 0)}%")

    if report.get("total_trades", 0) == 0:
        print("  No trades to report.")
        return

    print(f"\n  Win/Loss: {report['wins']}W / {report['losses']}L  |  "
          f"Win rate: {report['win_rate']}%")
    print(f"  Total PnL: {report['total_pnl_r']:+.2f}R  |  "
          f"{report['total_pnl_pips']:+.1f} pips")
    print(f"  Profit factor: {report['profit_factor']:.2f}  |  "
          f"Max DD: {report['max_drawdown_r']:.2f}R")
    print(f"  Avg winner: {report['avg_winner_r']:+.2f}R  |  "
          f"Avg loser: {report['avg_loser_r']:+.2f}R")
    print(f"  Best: {report['best_trade_r']:+.2f}R  |  "
          f"Worst: {report['worst_trade_r']:+.2f}R")
    print(f"  Avg bars held: {report['avg_bars_held']:.1f}")

    # Direction breakdown
    if report.get("by_direction"):
        print("\n  --- By Direction ---")
        for d, stats in report["by_direction"].items():
            print(f"  {d:>6}: {stats['trades']}T  "
                  f"{stats['win_rate']}% WR  "
                  f"{stats['pnl_r']:+.2f}R")

    # Session breakdown
    if report.get("by_session"):
        print("\n  --- By Session ---")
        for s, stats in report["by_session"].items():
            print(f"  {s:>10}: {stats['trades']}T  "
                  f"{stats['win_rate']}% WR  "
                  f"{stats['pnl_r']:+.2f}R")

    print("\n" + "=" * 60)


def report_to_json(report: dict) -> str:
    """Serialize report to JSON string."""
    return json.dumps(report, indent=2, default=str)


# ── helpers ─────────────────────────────────────────────────────────────────

def _group_stats(trades: list, key) -> dict:
    """Group trades by a key function and compute stats per group."""
    groups = defaultdict(list)
    for t in trades:
        groups[key(t)].append(t)

    result = {}
    for name, group in sorted(groups.items()):
        wins = [t for t in group if t.pnl_r > 0]
        pnl = sum(t.pnl_r for t in group)
        result[name] = {
            "trades": len(group),
            "wins": len(wins),
            "win_rate": round(len(wins) / len(group) * 100, 1) if group else 0.0,
            "pnl_r": round(pnl, 2),
        }
    return result


def _hour_to_session(ts) -> str:
    """Map a timestamp to a trading session name."""
    if ts is None:
        return "unknown"
    try:
        if hasattr(ts, "hour"):
            h = ts.hour
        else:
            h = datetime.fromisoformat(str(ts)).hour
    except Exception:
        return "unknown"

    # UTC session mapping
    if 0 <= h < 7:
        return "asian"
    elif 7 <= h < 12:
        return "london"
    elif 12 <= h < 16:
        return "overlap"
    elif 16 <= h < 21:
        return "new_york"
    else:
        return "late_ny"
