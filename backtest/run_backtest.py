"""
backtest/run_backtest.py — CLI entry point for strategy backtesting.

Usage:
  python -m backtest.run_backtest --days 5
  python -m backtest.run_backtest --symbol BTCUSD --source ctrader --days 30
  python -m backtest.run_backtest --from 2026-03-01 --to 2026-03-15
  python -m backtest.run_backtest --days 5 --family xau_scalp_pullback_limit
  python -m backtest.run_backtest --days 5 --sweep SCALPING_M1_TRIGGER_RSI_LONG_MAX=65,70,75
  python -m backtest.run_backtest --days 5 --override SCALPING_XAU_STRUCTURE_TF=15m
  python -m backtest.run_backtest --import-csv xauusd_m5.csv --tf 5m
  python -m backtest.run_backtest --ingest-only --days 5
  python -m backtest.run_backtest --list-runs
  python -m backtest.run_backtest --coverage
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import pandas as pd

from backtest.candle_store import CandleStore
from backtest.replay_engine import ReplayEngine
from backtest.virtual_executor import VirtualExecutor
from backtest.report import generate_report, print_report
from backtest.results_store import ResultsStore

logger = logging.getLogger(__name__)

# Symbols that use xauusd_provider
_XAU_SYMBOLS = {"XAUUSD", "GOLD"}
# Symbols that use crypto_provider
_CRYPTO_SYMBOLS = {"BTCUSD", "ETHUSD"}


def _ingest_all_tfs(store: CandleStore, symbol: str = "XAUUSD", source: str = "ctrader", days: int = 60) -> dict:
    """Ingest all available timeframes from yfinance or cTrader."""
    tfs = ["1m", "5m", "15m", "1h", "4h", "1d"]
    counts = {}
    for tf in tfs:
        if source == "ctrader":
            n = store.ingest_from_ctrader(symbol, tf, days=days)
        else:
            n = store.ingest_from_yfinance(symbol, tf)
        counts[tf] = n
        if n > 0:
            earliest, latest, total = store.coverage(symbol, tf)
            print(f"  {tf}: +{n} bars (total {total}, {earliest} -> {latest})")
        else:
            print(f"  {tf}: no data")
    return counts


def _select_scanner_method(scanner, symbol: str):
    """Return the right scanner method for the given symbol."""
    sym = symbol.upper()
    if sym in _XAU_SYMBOLS or "XAU" in sym:
        return scanner.scan_xauusd
    elif "BTC" in sym:
        return scanner.scan_btc
    elif "ETH" in sym:
        return scanner.scan_eth
    else:
        return scanner.scan_xauusd  # fallback


def _run_single(
    store: CandleStore,
    results_store: ResultsStore,
    symbol: str,
    days: int,
    run_name: str,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    family: Optional[str] = None,
    overrides: Optional[dict] = None,
) -> dict:
    """Run a single backtest pass."""
    from config import config
    from scanners.scalping_scanner import ScalpingScanner

    sym = symbol.upper()

    # Apply config overrides
    originals = {}
    if overrides:
        for key, value in overrides.items():
            if hasattr(config, key):
                originals[key] = getattr(config, key)
                setattr(config, key, value)

    try:
        # Disable market-closed guard and force scalping enabled for backtest
        originals["SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED"] = getattr(config, "SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED", True)
        originals["SCALPING_ENABLED"] = getattr(config, "SCALPING_ENABLED", True)
        config.SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED = False
        config.SCALPING_ENABLED = True

        # ── Backtest clock ────────────────────────────────────────────
        # The scanner uses datetime.now() for session detection, PDH/PDL,
        # liquidity map, and event windows.  In backtest these MUST return
        # the replay cursor time, not the real system clock.
        import scanners.xauusd as _xauusd_mod
        _orig_dt_class = _xauusd_mod.datetime

        class _BacktestDatetime(_orig_dt_class):
            """datetime proxy that returns replay cursor for .now() calls."""
            _bt_cursor = None
            @classmethod
            def now(cls, tz=None):
                if cls._bt_cursor is not None:
                    return cls._bt_cursor
                return _orig_dt_class.now(tz)

        _xauusd_mod.datetime = _BacktestDatetime
        originals["__xauusd_datetime"] = (_xauusd_mod, _orig_dt_class)

        # ── Session manager patches ──────────────────────────────────
        from market.data_fetcher import session_manager, SessionManager
        _original_is_open = session_manager.is_xauusd_market_open
        _original_is_holiday = session_manager.is_xauusd_holiday
        _original_get_info = session_manager.get_session_info
        session_manager.is_xauusd_market_open = lambda *a, **kw: True
        session_manager.is_xauusd_holiday = lambda *a, **kw: False
        originals["__session_manager_patch"] = _original_is_open
        originals["__session_holiday_patch"] = _original_is_holiday

        def _backtest_session_info():
            """Build session info from cursor time, not real clock."""
            cursor_ts = _BacktestDatetime._bt_cursor or datetime.now(timezone.utc)
            hour_min = cursor_ts.strftime("%H:%M")
            active = []
            for name, times in config.SESSIONS.items():
                if times["start"] <= hour_min <= times["end"]:
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
        originals["__session_info_patch"] = _original_get_info

        # ── Suppress EcoCalendar HTTP calls during backtest ──────────
        try:
            from market.economic_calendar import economic_calendar as _eco_cal
            originals["__eco_fetch"] = _eco_cal.fetch_events
            _eco_cal.fetch_events = lambda *a, **kw: []
        except (ImportError, AttributeError):
            pass

        # ── Patch get_current_price to use historical data ───────────
        from market.data_fetcher import xauusd_provider
        _orig_get_price = xauusd_provider.get_current_price
        def _backtest_get_price():
            """Return last close from replay cursor instead of live price."""
            if _BacktestDatetime._bt_cursor is not None:
                df = store.fetch(sym, "5m", end=_BacktestDatetime._bt_cursor, bars=1)
                if df is not None and not df.empty:
                    return float(df["close"].iloc[-1])
            return _orig_get_price()
        xauusd_provider.get_current_price = _backtest_get_price
        originals["__get_price"] = _orig_get_price

        # Determine replay window
        if end_dt is None:
            end_dt = datetime.now(timezone.utc)
            _, latest_ts, bar_count = store.coverage(sym, "5m")
            if latest_ts:
                end_dt = pd.Timestamp(latest_ts).to_pydatetime()
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
        if start_dt is None:
            start_dt = end_dt - timedelta(days=days)

        # Get M5 bar timestamps for cursor stepping
        df_m5 = store.fetch(sym, "5m", start=start_dt, end=end_dt)
        if df_m5 is None or df_m5.empty:
            print(f"  No M5 data available for {sym} {start_dt} -> {end_dt}")
            print("  Run with --ingest-only first to download data.")
            return {}

        timestamps = list(df_m5.index)
        print(f"\n  Replaying {len(timestamps)} M5 bars for {sym}")
        print(f"  From {timestamps[0]} to {timestamps[-1]}")

        # Set up replay engine and scanner
        engine = ReplayEngine(store, symbol=sym)
        executor = VirtualExecutor(store)
        scanner = ScalpingScanner()
        scan_method = _select_scanner_method(scanner, sym)

        signals_collected = []

        with engine:
            for i, ts in enumerate(timestamps):
                engine.set_cursor(ts)
                # Sync backtest clock so scanner datetime.now() returns cursor
                cursor_dt = ts.to_pydatetime() if isinstance(ts, pd.Timestamp) else ts
                if cursor_dt.tzinfo is None:
                    cursor_dt = cursor_dt.replace(tzinfo=timezone.utc)
                _BacktestDatetime._bt_cursor = cursor_dt

                # Progress indicator every 100 bars
                if (i + 1) % 100 == 0 or i == len(timestamps) - 1:
                    pct = (i + 1) / len(timestamps) * 100
                    print(f"\r  Progress: {pct:.0f}% ({i+1}/{len(timestamps)} bars)", end="", flush=True)

                # Run scanner
                try:
                    result = scan_method(require_enabled=False)
                except Exception as e:
                    logger.debug("[Backtest] Scanner error at %s: %s", ts, e)
                    continue

                if result.signal is None:
                    continue

                sig = result.signal
                signal_dict = {
                    "direction": sig.direction,
                    "entry": sig.entry,
                    "stop_loss": sig.stop_loss,
                    "tp1": sig.take_profit_1,
                    "tp2": getattr(sig, "take_profit_2", 0) or 0,
                    "tp3": getattr(sig, "take_profit_3", 0) or 0,
                    "symbol": sym,
                    "entry_type": getattr(sig, "entry_type", "limit"),
                    "source": getattr(result, "source", ""),
                    "pattern": getattr(sig, "pattern", ""),
                    "confidence": getattr(sig, "confidence", 0),
                }

                # Family filter
                if family:
                    source = str(signal_dict.get("source", "")).lower()
                    if family.lower() not in source:
                        continue

                signals_collected.append((ts, signal_dict))

        print()  # newline after progress

        # Resolve all signals
        print(f"  Resolving {len(signals_collected)} signals...")
        for signal_time, signal_dict in signals_collected:
            if isinstance(signal_time, pd.Timestamp):
                signal_time = signal_time.to_pydatetime()
            if signal_time.tzinfo is None:
                signal_time = signal_time.replace(tzinfo=timezone.utc)
            executor.resolve_trade(signal_dict, signal_time)

        # Generate report
        report = generate_report(executor.results, run_name=run_name)
        report["symbol"] = sym
        report["start_date"] = str(start_dt)
        report["end_date"] = str(end_dt)
        if overrides:
            report["overrides"] = overrides
        if family:
            report["strategy"] = family

        print_report(report)

        # Save to results store
        run_id = results_store.save_run(report, params=overrides)
        print(f"  Saved as run #{run_id}")

        return report

    finally:
        # Restore backtest datetime patch
        if "__xauusd_datetime" in originals:
            mod, orig_cls = originals.pop("__xauusd_datetime")
            mod.datetime = orig_cls
        # Restore session manager patches
        if "__session_manager_patch" in originals:
            from market.data_fetcher import session_manager
            session_manager.is_xauusd_market_open = originals.pop("__session_manager_patch")
        if "__session_holiday_patch" in originals:
            from market.data_fetcher import session_manager
            session_manager.is_xauusd_holiday = originals.pop("__session_holiday_patch")
        if "__session_info_patch" in originals:
            from market.data_fetcher import session_manager
            session_manager.get_session_info = originals.pop("__session_info_patch")
        # Restore get_current_price
        if "__get_price" in originals:
            from market.data_fetcher import xauusd_provider
            xauusd_provider.get_current_price = originals.pop("__get_price")
        # Restore EcoCalendar
        if "__eco_fetch" in originals:
            try:
                from market.economic_calendar import economic_calendar as _eco_cal
                _eco_cal.fetch_events = originals.pop("__eco_fetch")
            except (ImportError, AttributeError):
                originals.pop("__eco_fetch", None)
        # Reset backtest clock
        try:
            _BacktestDatetime._bt_cursor = None
        except NameError:
            pass
        # Restore config
        for key, value in originals.items():
            setattr(config, key, value)


def _run_crypto_sweep(
    store: CandleStore,
    results_store: ResultsStore,
    symbol: str,
    days: int,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
) -> None:
    """Sweep crypto parameters to find optimal config.

    Tests combinations of min_confidence, M1 RSI bounds, and structure TF.
    Ranks results by profit factor > 1.0, then win rate, then drawdown.
    """
    import itertools

    conf_key = f"SCALPING_{'BTC' if 'BTC' in symbol else 'ETH'}_MIN_CONFIDENCE"

    sweep_matrix = {
        conf_key: [60, 63, 65, 68, 70],
        "SCALPING_CRYPTO_M1_RSI_LONG_MIN": [45, 48, 50, 52],
        "SCALPING_CRYPTO_M1_RSI_SHORT_MAX": [48, 50, 52, 55],
    }

    keys = list(sweep_matrix.keys())
    value_lists = [sweep_matrix[k] for k in keys]
    combos = list(itertools.product(*value_lists))
    total = len(combos)

    print(f"\n{'='*60}")
    print(f"  Crypto Parameter Sweep: {symbol}")
    print(f"  {total} combinations across {len(keys)} parameters")
    print(f"{'='*60}")

    all_results: List[dict] = []

    for idx, combo in enumerate(combos, 1):
        overrides = dict(zip(keys, combo))
        label = " | ".join(f"{k.split('_')[-1]}={v}" for k, v in overrides.items())
        name = f"crypto_sweep_{symbol}_{idx}"

        print(f"\n  [{idx}/{total}] {label}")

        try:
            report = _run_single(
                store, results_store, symbol, days, name,
                start_dt=start_dt, end_dt=end_dt, overrides=overrides,
            )
            if report:
                report["sweep_overrides"] = overrides
                all_results.append(report)
        except Exception as e:
            logger.warning("[CryptoSweep] Run %d failed: %s", idx, e)
            continue

    if not all_results:
        print("\n  No successful runs.")
        return

    # Rank by: profit_factor > 1.0, then win_rate, then least max_drawdown
    ranked = sorted(
        all_results,
        key=lambda r: (
            1 if r.get("profit_factor", 0) > 1.0 else 0,
            r.get("win_rate", 0),
            -abs(r.get("max_drawdown_r", 0)),
        ),
        reverse=True,
    )

    print(f"\n{'='*60}")
    print(f"  CRYPTO SWEEP RESULTS — {symbol} ({len(ranked)} runs)")
    print(f"{'='*60}")
    print(f"  {'#':>3}  {'Trades':>6}  {'WR%':>5}  {'PnL R':>7}  {'PF':>5}  {'MaxDD':>6}  Config")
    print(f"  {'-'*70}")

    for i, r in enumerate(ranked[:15], 1):
        ov = r.get("sweep_overrides", {})
        cfg_str = " ".join(f"{k.split('_')[-1]}={v}" for k, v in ov.items())
        print(f"  {i:>3}  {r.get('total_trades', 0):>6}  "
              f"{r.get('win_rate', 0):>5.1f}  {r.get('total_pnl_r', 0):>+7.2f}  "
              f"{r.get('profit_factor', 0):>5.2f}  {r.get('max_drawdown_r', 0):>+6.2f}  {cfg_str}")

    best = ranked[0]
    print(f"\n  RECOMMENDED CONFIG for {symbol}:")
    for k, v in best.get("sweep_overrides", {}).items():
        print(f"    {k}={v}")


def main():
    parser = argparse.ArgumentParser(description="Dexter Pro Strategy Backtester")
    parser.add_argument("--symbol", type=str, default="XAUUSD",
                        help="Symbol to backtest: XAUUSD (default), BTCUSD, ETHUSD")
    parser.add_argument("--days", type=int, default=5, help="Days of data to backtest (default: 5)")
    parser.add_argument("--from", type=str, default=None, dest="from_date",
                        help="Start date (YYYY-MM-DD), overrides --days")
    parser.add_argument("--to", type=str, default=None, dest="to_date",
                        help="End date (YYYY-MM-DD), default: latest available data")
    parser.add_argument("--family", type=str, default=None, help="Filter to specific strategy family")
    parser.add_argument("--sweep", type=str, default=None,
                        help="Parameter sweep, e.g. SCALPING_M1_TRIGGER_RSI_LONG_MAX=65,70,75")
    parser.add_argument("--override", type=str, action="append", default=[],
                        help="Config override, e.g. SCALPING_XAU_STRUCTURE_TF=15m (repeatable)")
    parser.add_argument("--import-csv", type=str, default=None, help="Import CSV file")
    parser.add_argument("--tf", type=str, default="5m", help="Timeframe for CSV import (default: 5m)")
    parser.add_argument("--ingest-only", action="store_true", help="Only download/store data, don't run backtest")
    parser.add_argument("--list-runs", action="store_true", help="List previous backtest runs")
    parser.add_argument("--coverage", action="store_true", help="Show data coverage")
    parser.add_argument("--run-name", type=str, default=None, help="Name for this backtest run")
    parser.add_argument("--db", type=str, default=None, help="Custom candle DB path")
    parser.add_argument("--source", type=str, choices=["yfinance", "ctrader"], default="ctrader",
                        help="Data source: ctrader (default, OpenAPI ~60 days M5) or yfinance (5 days M5)")
    parser.add_argument("--sweep-crypto", action="store_true",
                        help="Auto-sweep crypto params: min_confidence, M1 RSI, multi_tf_bonus")

    args = parser.parse_args()
    symbol = args.symbol.upper()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    store = CandleStore(db_path=args.db)
    results_store = ResultsStore()

    try:
        # List runs mode
        if args.list_runs:
            runs = results_store.list_runs()
            if not runs:
                print("No backtest runs found.")
                return
            print(f"\n{'ID':>4}  {'Name':<30}  {'Trades':>6}  {'WR%':>5}  {'PnL R':>7}  {'PF':>5}  {'Created'}")
            print("-" * 90)
            for r in runs:
                print(f"{r['id']:>4}  {r['run_name']:<30}  {r['total_trades']:>6}  "
                      f"{r['win_rate']:>5.1f}  {r['total_pnl_r']:>+7.2f}  "
                      f"{r['profit_factor']:>5.2f}  {r['created_at'][:19]}")
            return

        # Coverage mode
        if args.coverage:
            symbols_to_check = [symbol] if symbol != "ALL" else ["XAUUSD", "BTCUSD", "ETHUSD"]
            for sym in symbols_to_check:
                print(f"\nData coverage for {sym}:")
                for tf in ["1m", "5m", "15m", "1h", "4h", "1d"]:
                    earliest, latest, count = store.coverage(sym, tf)
                    if count > 0:
                        print(f"  {tf:>4}: {count:>6} bars  ({earliest} -> {latest})")
                    else:
                        print(f"  {tf:>4}: no data")
            return

        # CSV import
        if args.import_csv:
            print(f"Importing CSV: {args.import_csv} as {symbol}/{args.tf}")
            n = store.ingest_from_csv(args.import_csv, symbol, args.tf)
            print(f"  Imported {n} bars")
            if args.ingest_only:
                return

        # Ingest data
        source_label = "cTrader OpenAPI" if args.source == "ctrader" else "yfinance"
        print(f"Ingesting {symbol} data from {source_label}...")
        _ingest_all_tfs(store, symbol=symbol, source=args.source, days=args.days)

        if args.ingest_only:
            print("\nIngest complete. Use --coverage to check data.")
            return

        # Parse date range
        start_dt = None
        end_dt = None
        if args.from_date:
            start_dt = datetime.strptime(args.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if args.to_date:
            end_dt = datetime.strptime(args.to_date, "%Y-%m-%d").replace(hour=23, minute=59, tzinfo=timezone.utc)

        # Parse overrides
        overrides = {}
        for ov in args.override:
            if "=" in ov:
                key, value = ov.split("=", 1)
                overrides[key.strip()] = value.strip()

        # Sweep mode
        if args.sweep:
            if "=" not in args.sweep:
                print("Invalid sweep format. Use: KEY=val1,val2,val3")
                return
            sweep_key, sweep_vals = args.sweep.split("=", 1)
            values = [v.strip() for v in sweep_vals.split(",")]
            print(f"\nSweeping {sweep_key} over {values}")

            for val in values:
                sweep_overrides = dict(overrides)
                try:
                    typed_val = float(val)
                    if typed_val == int(typed_val):
                        typed_val = int(typed_val)
                except ValueError:
                    typed_val = val
                sweep_overrides[sweep_key] = typed_val

                name = args.run_name or f"sweep_{symbol}_{sweep_key}={val}"
                print(f"\n{'='*60}")
                print(f"  Sweep: {sweep_key} = {val}")
                _run_single(store, results_store, symbol, args.days, name,
                            start_dt=start_dt, end_dt=end_dt,
                            family=args.family, overrides=sweep_overrides)
            return

        # Crypto sweep mode
        if args.sweep_crypto:
            if symbol not in _CRYPTO_SYMBOLS:
                print(f"--sweep-crypto requires --symbol BTCUSD or ETHUSD, got {symbol}")
                return
            _run_crypto_sweep(store, results_store, symbol, args.days,
                              start_dt=start_dt, end_dt=end_dt)
            return

        # Single run
        name = args.run_name or f"{symbol}_{args.days}d"
        if args.from_date:
            name = args.run_name or f"{symbol}_{args.from_date}_to_{args.to_date or 'now'}"
        if args.family:
            name += f"_{args.family}"
        _run_single(store, results_store, symbol, args.days, name,
                    start_dt=start_dt, end_dt=end_dt,
                    family=args.family, overrides=overrides or None)

    finally:
        store.close()
        results_store.close()


if __name__ == "__main__":
    main()
