"""
backtest/run_fibo_backtest.py — Backtest wrapper for FiboAdvanceScanner.

Usage:
  python -m backtest.run_fibo_backtest --days 7
  python -m backtest.run_fibo_backtest --from 2026-04-01 --to 2026-04-08
  python -m backtest.run_fibo_backtest --days 3 --sniper-only
  python -m backtest.run_fibo_backtest --days 3 --scout-only

Replays historical XAUUSD data through the live FiboAdvanceScanner pipeline
using the existing ReplayEngine + VirtualExecutor infrastructure.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from backtest.candle_store import CandleStore
from backtest.replay_engine import ReplayEngine
from backtest.virtual_executor import VirtualExecutor
from backtest.report import generate_report, print_report
from backtest.results_store import ResultsStore

logger = logging.getLogger(__name__)


class _BacktestDatetime(datetime):
    """datetime proxy that returns replay cursor for .now() calls."""
    _bt_cursor: Optional[datetime] = None

    @classmethod
    def now(cls, tz=None):
        if cls._bt_cursor is not None:
            return cls._bt_cursor
        return super().now(tz)

    @classmethod
    def utcnow(cls):
        if cls._bt_cursor is not None:
            return cls._bt_cursor
        return super().utcnow()


def run_fibo_backtest(
    symbol: str = "XAUUSD",
    days: int = 7,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    scan_interval_min: int = 15,
    run_name: Optional[str] = None,
    sniper_only: bool = False,
    scout_only: bool = False,
) -> dict:
    """
    Replay historical XAUUSD through FiboAdvanceScanner.

    Args:
        symbol: Trading symbol (default XAUUSD)
        days: Lookback days (ignored if start_dt/end_dt set)
        start_dt: Replay start (UTC)
        end_dt: Replay end (UTC)
        scan_interval_min: Minutes between scans (default 15)
        run_name: Display name for the run
        sniper_only: Only run Sniper mode
        scout_only: Only run Scout mode
    """
    sym = symbol.upper()
    store = CandleStore()
    results_store = ResultsStore()

    if run_name is None:
        run_name = f"fibo_backtest_{sym}_{days}d"

    # Verify data exists
    _, latest_ts, total = store.coverage(sym, "5m")
    if total == 0:
        print(f"  No data for {sym}. Run with --ingest-only first.")
        return {}

    if end_dt is None:
        end_dt = pd.Timestamp(latest_ts).to_pydatetime()
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
    if start_dt is None:
        start_dt = end_dt - timedelta(days=days)

    # Get M5 bar timestamps for cursor stepping (step by scan_interval)
    df_m5 = store.fetch(sym, "5m", start=start_dt, end=end_dt)
    if df_m5 is None or df_m5.empty:
        print(f"  No M5 data for {sym} {start_dt} -> {end_dt}")
        return {}

    # Step every N bars (scan_interval_min / 5)
    step = max(1, scan_interval_min // 5)
    timestamps = list(df_m5.index)[::step]

    print(f"\n  FiboAdvance Backtest: {sym}")
    print(f"  Period: {timestamps[0]} → {timestamps[-1]}")
    print(f"  Scans: {len(timestamps)} (every {scan_interval_min} min)")

    # ── Monkey-patch infrastructure ────────────────────────────────────────
    originals = {}

    # Patch datetime (only in market.data_fetcher, not globally)
    import market.data_fetcher as _xauusd_mod
    _orig_dt_class = _xauusd_mod.datetime
    _xauusd_mod.datetime = _BacktestDatetime
    originals["__xauusd_datetime"] = (_xauusd_mod, _orig_dt_class)

    # Patch session manager
    from market.data_fetcher import session_manager
    _original_is_open = session_manager.is_xauusd_market_open
    _original_get_info = session_manager.get_session_info
    session_manager.is_xauusd_market_open = lambda *a, **kw: True

    # Import config for session times
    from config import config as _cfg

    def _backtest_session_info():
        cursor_ts = _BacktestDatetime._bt_cursor or datetime.now(timezone.utc)
        hour_min = cursor_ts.strftime("%H:%M")
        active = []
        sessions_cfg = getattr(_cfg, "SESSIONS", {}) or {}
        for name, times in sessions_cfg.items():
            if times.get("start", "") <= hour_min <= times.get("end", ""):
                active.append(name)
        if not active:
            active = ["off_hours"]
        return {
            "utc_time": cursor_ts.strftime("%Y-%m-%d %H:%M UTC"),
            "active_sessions": active,
            "high_volatility": any(s in active for s in ["london", "new_york", "overlap"]),
            "xauusd_market_open": True,
            "fx_weekend_closed": False,
        }

    session_manager.get_session_info = _backtest_session_info
    originals["__session_manager_patch"] = _original_is_open
    originals["__session_info_patch"] = _original_get_info

    # Patch get_current_price
    from market.data_fetcher import xauusd_provider
    _orig_get_price = xauusd_provider.get_current_price

    def _backtest_get_price():
        if _BacktestDatetime._bt_cursor is not None:
            df = store.fetch(sym, "5m", end=_BacktestDatetime._bt_cursor, bars=1)
            if df is not None and not df.empty:
                return float(df["close"].iloc[-1])
        return _orig_get_price()

    xauusd_provider.get_current_price = _backtest_get_price
    originals["__get_price"] = _orig_get_price

    # Patch LiveProfileAutopilot to return empty snapshots (no live data)
    from learning.live_profile_autopilot import LiveProfileAutopilot
    _orig_capture = LiveProfileAutopilot.latest_capture_feature_snapshot
    LiveProfileAutopilot.latest_capture_feature_snapshot = lambda *a, **kw: {}
    originals["__autopilot_patch"] = _orig_capture

    # ── Set up scanner ─────────────────────────────────────────────────────
    from scanners.fibo_advance import FiboAdvanceScanner, _cfg as fibo_cfg

    # If sniper_only/scout_only, temporarily override config
    if scout_only:
        # Disable sniper by raising its min score to impossible level
        original_cfg = fibo_cfg
        import scanners.fibo_advance as fibo_mod

        def _patched_cfg(key, default):
            if key == "FIBO_ADVANCE_MIN_FIBO_SCORE":
                return 999.0  # Impossible — sniper never fires
            return original_cfg(key, default)
        fibo_mod._cfg = _patched_cfg
    elif sniper_only:
        import scanners.fibo_advance as fibo_mod
        original_cfg = fibo_cfg

        def _patched_cfg(key, default):
            if key == "FIBO_SCOUT_ENABLED":
                return False
            return original_cfg(key, default)
        fibo_mod._cfg = _patched_cfg

    scanner = FiboAdvanceScanner()
    engine = ReplayEngine(store, symbol=sym)
    executor = VirtualExecutor(store)

    signals_collected = []

    try:
        with engine:
            for i, ts in enumerate(timestamps):
                engine.set_cursor(ts)
                cursor_dt = ts.to_pydatetime() if isinstance(ts, pd.Timestamp) else ts
                if cursor_dt.tzinfo is None:
                    cursor_dt = cursor_dt.replace(tzinfo=timezone.utc)
                _BacktestDatetime._bt_cursor = cursor_dt

                # Progress
                if (i + 1) % 50 == 0 or i == len(timestamps) - 1:
                    pct = (i + 1) / len(timestamps) * 100
                    print(f"\r  Progress: {pct:.0f}% ({i+1}/{len(timestamps)})", end="", flush=True)

                # Run scan
                try:
                    signal = scanner.scan()
                except Exception as e:
                    logger.debug("Scan error at %s: %s", ts, e)
                    continue

                if signal is None:
                    continue

                signal_dict = {
                    "time": cursor_dt.isoformat(),
                    "direction": signal.direction,
                    "entry": signal.entry,
                    "stop_loss": signal.stop_loss,
                    "tp1": signal.take_profit_1,
                    "tp2": signal.take_profit_2 or 0,
                    "tp3": signal.take_profit_3 or 0,
                    "symbol": sym,
                    "entry_type": signal.entry_type or "limit",
                    "source": "fibo_xauusd",
                    "pattern": signal.pattern,
                    "confidence": signal.confidence,
                }
                signals_collected.append((cursor_dt, signal_dict))

        print(f"\n  Collected {len(signals_collected)} signals")

        # Resolve
        print(f"  Resolving {len(signals_collected)} signals...")
        for signal_time, signal_dict in signals_collected:
            executor.resolve_trade(signal_dict, signal_time)

        # Report
        report = generate_report(executor.results, run_name=run_name)
        report["symbol"] = sym
        report["start_date"] = str(start_dt)
        report["end_date"] = str(end_dt)
        report["scanner"] = "FiboAdvance"
        report["scan_interval_min"] = scan_interval_min

        print_report(report)

        run_id = results_store.save_run(report)
        print(f"  Saved as run #{run_id}")
        return report

    finally:
        # Restore originals
        if "__xauusd_datetime" in originals:
            mod, orig_cls = originals.pop("__xauusd_datetime")
            mod.datetime = orig_cls
        if "__session_manager_patch" in originals:
            session_manager.is_xauusd_market_open = originals.pop("__session_manager_patch")
        if "__session_info_patch" in originals:
            session_manager.get_session_info = originals.pop("__session_info_patch")
        if "__get_price" in originals:
            xauusd_provider.get_current_price = originals.pop("__get_price")
        if "__autopilot_patch" in originals:
            LiveProfileAutopilot.latest_capture_feature_snapshot = originals.pop("__autopilot_patch")

        _BacktestDatetime._bt_cursor = None


def main():
    parser = argparse.ArgumentParser(description="FiboAdvance Backtest")
    parser.add_argument("--symbol", default="XAUUSD", help="Symbol (default: XAUUSD)")
    parser.add_argument("--days", type=int, default=7, help="Days to replay")
    parser.add_argument("--from", dest="from_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--interval", type=int, default=15, help="Scan interval in minutes")
    parser.add_argument("--sniper-only", action="store_true", help="Sniper mode only")
    parser.add_argument("--scout-only", action="store_true", help="Scout mode only")
    parser.add_argument("--ingest-only", action="store_true", help="Only download data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)

    start_dt = datetime.fromisoformat(args.from_date).replace(tzinfo=timezone.utc) if args.from_date else None
    end_dt = datetime.fromisoformat(args.to_date).replace(tzinfo=timezone.utc) if args.to_date else None

    if args.ingest_only:
        store = CandleStore()
        n = store.ingest_from_ctrader(args.symbol, "5m", days=args.days + 2)
        print(f"  Ingested {n} M5 bars")
        for tf in ["15m", "1h", "4h", "1d"]:
            n = store.ingest_from_ctrader(args.symbol, tf, days=args.days + 2)
            print(f"  Ingested {n} {tf} bars")
        return

    run_fibo_backtest(
        symbol=args.symbol,
        days=args.days,
        start_dt=start_dt,
        end_dt=end_dt,
        scan_interval_min=args.interval,
        sniper_only=args.sniper_only,
        scout_only=args.scout_only,
    )


if __name__ == "__main__":
    main()
