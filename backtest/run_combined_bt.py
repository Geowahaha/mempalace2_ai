"""
backtest/run_combined_bt.py — Combined multi-family conductor backtest for XAUUSD.

Simulates the full OpportunityFollow chain across all sidecar families in a single
replay pass. Families tested:

  1. behavioral_v2  = xau_scalp_pullback_limit    (always active, base family)
  2. FSS            = xau_scalp_flow_short_sidecar (bear regime + flow gate + short)
  3. FLS            = xau_scalp_flow_long_sidecar  (bull regime + flow gate + long)
  4. FFFS           = xau_scalp_failed_fade_follow_stop (bull regime, direction=short + conf≥72)
  5. RR             = xau_scalp_range_repair        (ranging regime, any direction)

Usage:
  python -m backtest.run_combined_bt --from 2026-03-19
  python -m backtest.run_combined_bt --from 2026-03-19 --to 2026-03-27
  python -m backtest.run_combined_bt --from 2026-03-19 --to 2026-03-27 --symbol XAUUSD
  python -m backtest.run_combined_bt --from 2026-03-19 --db /path/to/candle.db
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd

from backtest.candle_store import CandleStore
from backtest.replay_engine import ReplayEngine
from backtest.virtual_executor import VirtualExecutor, TradeResult
from backtest.results_store import ResultsStore

logger = logging.getLogger(__name__)

# ── Family definitions ────────────────────────────────────────────────────────

FAMILIES = ["behavioral_v2", "fss", "fls", "fffs", "rr"]

FAMILY_META = {
    "behavioral_v2": {"label": "behavioral_v2", "regime": "all",     "source": "scalp_xauusd:behavioral_v2:canary"},
    "fss":           {"label": "FSS",            "regime": "bear",    "source": "scalp_xauusd:fss:canary"},
    "fls":           {"label": "FLS",            "regime": "bull",    "source": "scalp_xauusd:fls:canary"},
    "fffs":          {"label": "FFFS",           "regime": "bear",    "source": "scalp_xauusd:fffs:canary"},
    "rr":            {"label": "RR",             "regime": "ranging", "source": "scalp_xauusd:rr:canary"},
}


# ── Regime detection ─────────────────────────────────────────────────────────

def _detect_regime(store: CandleStore, cursor_dt: datetime, symbol: str = "XAUUSD") -> str:
    """Detect H1 regime at cursor_dt using last 20 H1 bars.

    Returns: "bull" | "bear" | "ranging" | "unknown"
    """
    end = cursor_dt
    start = cursor_dt - timedelta(hours=25)  # extra margin
    df = store.fetch(symbol, "1h", start=start, end=end, bars=20)
    if df is None or len(df) < 6:
        return "unknown"

    closes = df["close"].values
    slope = (closes[-1] - closes[-6]) / closes[-6] * 100.0
    if slope > 0.20:
        return "bull"
    if slope < -0.20:
        return "bear"
    return "ranging"


# ── Flow gate ─────────────────────────────────────────────────────────────────

def _passes_flow_gate(signal, direction_required: str, min_conf: float = 65.0) -> bool:
    """Check flow conditions for FSS/FLS sidecar entry.

    In live system delta_proxy/bar_volume_proxy come from tick microstructure
    which is not available in candle-based BT. Gates used here:
      - direction match
      - confidence >= min_conf (default 65, lowered from 69 to unlock 80+ band for FLS)
    Live system additionally gates on delta_proxy>=0.08 and bar_vol>=0.38.
    """
    confidence = float(getattr(signal, "confidence", 0.0) or 0.0)
    direction = str(getattr(signal, "direction", "") or "").lower()
    if direction != direction_required:
        return False
    if confidence < min_conf:
        return False
    return True


# ── Signal dict builder ───────────────────────────────────────────────────────

def _build_signal_dict(sig, sym: str, source: str, signal_time: Optional[datetime] = None, regime: str = "") -> dict:
    """Convert a scanner Signal object into a VirtualExecutor-compatible dict."""
    return {
        "direction": str(getattr(sig, "direction", "") or ""),
        "entry": float(getattr(sig, "entry", 0) or 0),
        "stop_loss": float(getattr(sig, "stop_loss", 0) or 0),
        "tp1": float(getattr(sig, "take_profit_1", 0) or 0),
        "tp2": float(getattr(sig, "take_profit_2", 0) or 0),
        "tp3": float(getattr(sig, "take_profit_3", 0) or 0),
        "symbol": sym,
        "entry_type": str(getattr(sig, "entry_type", "limit") or "limit"),
        "source": source,
        "pattern": str(getattr(sig, "pattern", "") or ""),
        "confidence": float(getattr(sig, "confidence", 0) or 0),
        # ── BT metadata (not used by executor — for reporting only) ────────
        "_signal_time": signal_time.isoformat() if signal_time else "",
        "_regime": regime,
    }


# ── Per-family stats calculator ────────────────────────────────────────────────

def _calc_family_stats(results: List[TradeResult]) -> dict:
    """Calculate key metrics for one family's trade results."""
    trades = [r for r in results if r.outcome != "expired_no_fill"]
    if not trades:
        return {
            "trades": 0, "wins": 0, "losses": 0,
            "win_rate": 0.0, "pnl_r": 0.0,
            "profit_factor": 0.0, "max_dd_r": 0.0,
        }

    wins = [t for t in trades if t.pnl_r > 0]
    losses = [t for t in trades if t.pnl_r < 0]
    pnl_r = sum(t.pnl_r for t in trades)
    gross_profit = sum(t.pnl_r for t in wins) if wins else 0.0
    gross_loss = abs(sum(t.pnl_r for t in losses)) if losses else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

    # Peak-to-trough drawdown in cumulative R
    running = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in trades:
        running += t.pnl_r
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    return {
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(trades) * 100, 1) if trades else 0.0,
        "pnl_r": round(pnl_r, 2),
        "profit_factor": round(profit_factor, 2),
        "max_dd_r": round(max_dd, 2),
    }


# ── Session label helper ──────────────────────────────────────────────────────

def _get_session_label(dt: datetime) -> str:
    """Return the primary session for a UTC datetime using config.SESSIONS ranges.

    Priority order: overlap > london > new_york > asian > off_hours
    (overlap is subset of london AND new_york — must be checked first)
    """
    hour_min = dt.strftime("%H:%M")
    # config.SESSIONS: {"asian": {"start":"00:00","end":"08:00"}, ...}
    try:
        from config import config
        sessions_cfg = config.SESSIONS
    except Exception:
        sessions_cfg = {
            "asian":    {"start": "00:00", "end": "08:00"},
            "london":   {"start": "07:00", "end": "16:00"},
            "new_york": {"start": "12:00", "end": "21:00"},
            "overlap":  {"start": "12:00", "end": "16:00"},
        }

    active = [
        name for name, times in sessions_cfg.items()
        if times["start"] <= hour_min <= times["end"]
    ]
    # Priority: overlap > london > new_york > asian > off_hours
    for preferred in ("overlap", "london", "new_york", "asian"):
        if preferred in active:
            return preferred
    return "off_hours"


# ── Detailed per-family breakdown ─────────────────────────────────────────────

def _detailed_family_breakdown(
    family_key: str,
    results: List["TradeResult"],
    daily_regime_map: Dict[str, str],
) -> None:
    """Print per-session / per-direction / per-confidence-band / per-day breakdown."""
    trades = [r for r in results if r.outcome != "expired_no_fill"]
    label = FAMILY_META[family_key]["label"]
    if not trades:
        print(f"\n--- {label} : no resolved trades ---")
        return

    print(f"\n{'=' * 56}")
    print(f"  DETAIL: {label}  ({len(trades)} trades)")
    print(f"{'=' * 56}")

    def _mini_stats(subset):
        if not subset:
            return 0, 0.0, 0.0, 0.0
        wins = [t for t in subset if t.pnl_r > 0]
        losses = [t for t in subset if t.pnl_r < 0]
        pnl = sum(t.pnl_r for t in subset)
        gp = sum(t.pnl_r for t in wins) if wins else 0.0
        gl = abs(sum(t.pnl_r for t in losses)) if losses else 0.0
        pf = gp / gl if gl > 0 else float("inf")
        wr = len(wins) / len(subset) * 100 if subset else 0.0
        return len(subset), wr, pnl, pf

    mini_hdr = f"  {'Label':<14}  {'Trades':>6}  {'WR%':>5}  {'PnL(R)':>8}  {'PF':>5}"
    mini_sep = "  " + "-" * 46

    # ── By direction ─────────────────────────────────────────────────────
    print("\n  By direction:")
    print(mini_hdr)
    print(mini_sep)
    for dirn in ("long", "short"):
        sub = [t for t in trades if t.direction == dirn]
        n, wr, pnl, pf = _mini_stats(sub)
        pf_s = f"{pf:.2f}" if pf != float("inf") else " inf"
        print(f"  {dirn:<14}  {n:>6}  {wr:>5.1f}%  {pnl:>+8.2f}  {pf_s:>5}")

    # ── By session ───────────────────────────────────────────────────────
    print("\n  By session (UTC):")
    print(mini_hdr)
    print(mini_sep)
    session_order = ["overlap", "london", "new_york", "asian", "off_hours"]
    session_buckets: Dict[str, list] = {s: [] for s in session_order}
    for t in trades:
        sig_time_str = t.signal.get("_signal_time", "") if isinstance(t.signal, dict) else ""
        try:
            dt = datetime.fromisoformat(sig_time_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            dt = t.entry_time if isinstance(t.entry_time, datetime) else datetime.now(timezone.utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        sess = _get_session_label(dt)
        session_buckets.setdefault(sess, []).append(t)

    for sess in session_order:
        sub = session_buckets.get(sess, [])
        n, wr, pnl, pf = _mini_stats(sub)
        if n == 0:
            continue
        pf_s = f"{pf:.2f}" if pf != float("inf") else " inf"
        print(f"  {sess:<14}  {n:>6}  {wr:>5.1f}%  {pnl:>+8.2f}  {pf_s:>5}")

    # ── By confidence band ────────────────────────────────────────────────
    print("\n  By confidence band:")
    print(mini_hdr)
    print(mini_sep)
    bands = [("90+", 90, 101), ("80-89", 80, 90), ("70-79", 70, 80), ("60-69", 60, 70), ("<60", 0, 60)]
    for band_label, lo, hi in bands:
        sub = [
            t for t in trades
            if lo <= float((t.signal.get("confidence") if isinstance(t.signal, dict) else 0) or 0) < hi
        ]
        n, wr, pnl, pf = _mini_stats(sub)
        if n == 0:
            continue
        pf_s = f"{pf:.2f}" if pf != float("inf") else " inf"
        print(f"  {band_label:<14}  {n:>6}  {wr:>5.1f}%  {pnl:>+8.2f}  {pf_s:>5}")

    # ── By day ────────────────────────────────────────────────────────────
    print("\n  By day:")
    day_hdr = f"  {'Day':<12}  {'Regime':<8}  {'Trades':>6}  {'WR%':>5}  {'PnL(R)':>8}"
    print(day_hdr)
    print("  " + "-" * 44)
    day_buckets: Dict[str, list] = defaultdict(list)
    for t in trades:
        et = t.entry_time
        if isinstance(et, pd.Timestamp):
            et = et.to_pydatetime()
        day_key = et.strftime("%Y-%m-%d") if et else "unknown"
        day_buckets[day_key].append(t)

    for day_key in sorted(day_buckets.keys()):
        sub = day_buckets[day_key]
        n, wr, pnl, _ = _mini_stats(sub)
        regime_label = daily_regime_map.get(day_key, "?").upper()[:6]
        print(f"  {day_key:<12}  {regime_label:<8}  {n:>6}  {wr:>5.1f}%  {pnl:>+8.2f}")

    # ── Session × direction cross-tab ─────────────────────────────────────
    print("\n  Session × direction cross-tab:")
    cross_hdr = f"  {'Session/Dir':<20}  {'Trades':>6}  {'WR%':>5}  {'PnL(R)':>8}  {'PF':>5}"
    print(cross_hdr)
    print("  " + "-" * 54)
    for sess in session_order:
        for dirn in ("long", "short"):
            sub = [t for t in session_buckets.get(sess, []) if t.direction == dirn]
            n, wr, pnl, pf = _mini_stats(sub)
            if n == 0:
                continue
            pf_s = f"{pf:.2f}" if pf != float("inf") else " inf"
            label_s = f"{sess}/{dirn}"
            flag = "  [WEAK]" if (n >= 5 and wr < 50.0) else ("  [OK]" if n >= 10 and wr >= 65.0 else "")
            print(f"  {label_s:<20}  {n:>6}  {wr:>5.1f}%  {pnl:>+8.2f}  {pf_s:>5}{flag}")

    # ── Confidence band × direction cross-tab ─────────────────────────────
    print("\n  Confidence × direction cross-tab:")
    print(cross_hdr)
    print("  " + "-" * 54)
    for band_label, lo, hi in bands:
        for dirn in ("long", "short"):
            sub = [
                t for t in trades
                if dirn == t.direction
                and lo <= float((t.signal.get("confidence") if isinstance(t.signal, dict) else 0) or 0) < hi
            ]
            n, wr, pnl, pf = _mini_stats(sub)
            if n == 0:
                continue
            pf_s = f"{pf:.2f}" if pf != float("inf") else " inf"
            label_cd = f"{band_label}/{dirn}"
            flag = "  [WEAK]" if (n >= 5 and wr < 50.0) else ("  [STAR]" if n >= 10 and wr >= 70.0 else "")
            print(f"  {label_cd:<20}  {n:>6}  {wr:>5.1f}%  {pnl:>+8.2f}  {pf_s:>5}{flag}")

    # ── Outcome distribution ──────────────────────────────────────────────
    print("\n  Outcome distribution:")
    outcome_counts: Dict[str, int] = defaultdict(int)
    for t in trades:
        outcome_counts[t.outcome] += 1
    total_n = len(trades)
    for outcome_label in ("tp3_hit", "tp2_hit", "tp1_hit", "sl_hit", "expired"):
        cnt = outcome_counts.get(outcome_label, 0)
        if cnt == 0:
            continue
        pct = cnt / total_n * 100
        print(f"    {outcome_label:<20}  {cnt:>4}  ({pct:>5.1f}%)")


# ── Promotion recommendations ─────────────────────────────────────────────────

def _print_promotion_recommendations(family_stats: Dict[str, dict]) -> None:
    """Print actionable promote / tune / review verdict per family."""
    # Thresholds
    PROMOTE_WR = 65.0
    PROMOTE_PF = 2.0
    PROMOTE_N  = 20
    TUNE_WR    = 55.0
    TUNE_PF    = 1.5

    print()
    print("=" * 60)
    print("  PROMOTION RECOMMENDATIONS")
    print("=" * 60)
    hdr = f"  {'Family':<16}  {'Trades':>6}  {'WR%':>5}  {'PF':>5}  {'Verdict'}"
    print(hdr)
    print("  " + "-" * 54)

    for fam_key in FAMILIES:
        meta = FAMILY_META[fam_key]
        s = family_stats.get(fam_key, {})
        n   = s.get("trades", 0)
        wr  = s.get("win_rate", 0.0)
        pf  = s.get("profit_factor", 0.0)
        label = meta["label"]

        if n == 0:
            verdict = "NO SIGNAL — gate redesign needed"
        elif wr >= PROMOTE_WR and pf >= PROMOTE_PF and n >= PROMOTE_N:
            if n >= 50:
                verdict = "PROMOTE (strong: WR/PF/n all pass)"
            else:
                verdict = "PROMOTE-CANDIDATE (WR+PF pass, n low — monitor)"
        elif wr >= PROMOTE_WR and n >= PROMOTE_N:
            verdict = "TUNE PF (WR good, PF marginal)"
        elif pf >= PROMOTE_PF and n >= PROMOTE_N:
            verdict = "TUNE WR (PF good, WR marginal)"
        elif wr >= TUNE_WR and pf >= TUNE_PF:
            verdict = "TUNE (borderline — adjust risk params)"
        else:
            verdict = "REVIEW / DISABLE"

        pf_s = f"{pf:.2f}" if pf != float("inf") else "  inf"
        print(f"  {label:<16}  {n:>6}  {wr:>5.1f}%  {pf_s:>5}  {verdict}")

    print()


# ── Deduplication for TOTAL row ───────────────────────────────────────────────

def _dedup_results(family_results: Dict[str, List[TradeResult]]) -> List[TradeResult]:
    """Build a deduplicated total list.

    Only keeps one result per (signal_time, entry, direction) triplet to avoid
    double-counting when the same base signal feeds multiple families.
    behavioral_v2 is the canonical base; FSS/FLS/FFFS/RR contribute only when
    they triggered AND differ from the base outcome key.
    """
    seen: set = set()
    combined: List[TradeResult] = []

    # behavioral_v2 first, then sidecars
    priority_order = ["behavioral_v2", "fss", "fls", "fffs", "rr"]

    for fam in priority_order:
        for r in family_results.get(fam, []):
            key = (round(float(r.entry_price), 2), str(r.direction), str(r.entry_time))
            if key not in seen:
                seen.add(key)
                combined.append(r)

    return combined


# ── Report printer ─────────────────────────────────────────────────────────────

def _print_combined_report(
    family_stats: Dict[str, dict],
    total_stats: dict,
    regime_counts: Dict[str, int],
    daily_map: Dict[str, Dict[str, int]],
    start_dt: datetime,
    end_dt: datetime,
    symbol: str,
    family_results: Optional[Dict[str, list]] = None,
) -> None:
    """Print the combined backtest report to stdout."""
    total_bars = sum(regime_counts.values())
    days_str = f"{(end_dt - start_dt).days} days"

    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    print()
    print("=" * 60)
    print(f"  COMBINED BT: {symbol}  {start_str} -> {end_str}  ({days_str})")
    print("=" * 60)
    print()

    hdr = f"{'Family':<16}  {'Regime':<8}  {'Trades':>6}  {'WR%':>5}  {'PnL(R)':>8}  {'PF':>5}  {'MaxDD(R)':>8}"
    sep = "-" * len(hdr)
    print(hdr)
    print(sep)

    for fam_key in FAMILIES:
        meta = FAMILY_META[fam_key]
        s = family_stats.get(fam_key, {})
        label = meta["label"]
        regime = meta["regime"]
        trades = s.get("trades", 0)
        wr = s.get("win_rate", 0.0)
        pnl = s.get("pnl_r", 0.0)
        pf = s.get("profit_factor", 0.0)
        mdd = s.get("max_dd_r", 0.0)
        pf_str = f"{pf:.2f}" if pf != float("inf") else " inf"
        print(f"{label:<16}  {regime:<8}  {trades:>6}  {wr:>5.1f}%  {pnl:>+8.2f}  {pf_str:>5}  {-mdd:>+8.2f}")

    print(sep)
    t = total_stats
    trades_t = t.get("trades", 0)
    wr_t = t.get("win_rate", 0.0)
    pnl_t = t.get("pnl_r", 0.0)
    pf_t = t.get("profit_factor", 0.0)
    mdd_t = t.get("max_dd_r", 0.0)
    pf_t_str = f"{pf_t:.2f}" if pf_t != float("inf") else " inf"
    print(f"{'TOTAL (no dup)':<16}  {'all':<8}  {trades_t:>6}  {wr_t:>5.1f}%  {pnl_t:>+8.2f}  {pf_t_str:>5}  {-mdd_t:>+8.2f}")
    print()

    # Regime distribution
    print("Regime distribution (by H1):")
    for regime_label, key in [("BULL", "bull"), ("BEAR", "bear"), ("RANGING", "ranging"), ("UNKNOWN", "unknown")]:
        count = regime_counts.get(key, 0)
        pct = (count / total_bars * 100) if total_bars > 0 else 0.0
        if key == "unknown":
            print(f"  {regime_label:<8}: {count:>4} bars")
        else:
            print(f"  {regime_label:<8}: {count:>4} bars ({pct:.1f}%)")

    print()
    print("Daily regime map:")
    for day_str in sorted(daily_map.keys()):
        day_data = daily_map[day_str]
        regime = day_data.get("regime", "unknown").upper()
        b2 = day_data.get("behavioral_v2", 0)
        fss_n = day_data.get("fss", 0)
        fls_n = day_data.get("fls", 0)
        fffs_n = day_data.get("fffs", 0)
        rr_n = day_data.get("rr", 0)
        print(f"  {day_str}  {regime:<8}  behavioral={b2}  FSS={fss_n}  FLS={fls_n}  FFFS={fffs_n}  RR={rr_n}")
    print()

    # ── Detailed per-family breakdowns ─────────────────────────────────
    if family_results:
        # Build day -> regime map for breakdown labels
        day_regime_map = {day: info.get("regime", "?") for day, info in daily_map.items()}
        for fam_key in FAMILIES:
            _detailed_family_breakdown(fam_key, family_results.get(fam_key, []), day_regime_map)
        print()

    # ── Promotion recommendations ──────────────────────────────────────
    _print_promotion_recommendations(family_stats)


# ── Main combined backtest runner ──────────────────────────────────────────────

def run_combined(
    store: CandleStore,
    results_store: ResultsStore,
    symbol: str = "XAUUSD",
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    days: int = 9,
) -> Dict[str, dict]:
    """Run the combined multi-family backtest.

    Returns dict of {family_key: stats_dict} plus "total" key.
    """
    from config import config
    from scanners.scalping_scanner import ScalpingScanner

    sym = symbol.upper()
    originals: dict = {}

    try:
        # ── Config patches ─────────────────────────────────────────────────
        originals["SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED"] = getattr(config, "SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED", True)
        originals["SCALPING_ENABLED"] = getattr(config, "SCALPING_ENABLED", True)
        config.SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED = False
        config.SCALPING_ENABLED = True

        # ── Backtest clock patch ───────────────────────────────────────────
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

        # ── Session manager patches ────────────────────────────────────────
        from market.data_fetcher import session_manager
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

        # ── Suppress EcoCalendar HTTP calls ────────────────────────────────
        try:
            from market.economic_calendar import economic_calendar as _eco_cal
            originals["__eco_fetch"] = _eco_cal.fetch_events
            _eco_cal.fetch_events = lambda *a, **kw: []
        except (ImportError, AttributeError):
            pass

        # ── Patch get_current_price to use historical data ─────────────────
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

        # ── Determine replay window ────────────────────────────────────────
        if end_dt is None:
            _, latest_ts, _ = store.coverage(sym, "5m")
            if latest_ts:
                end_dt = pd.Timestamp(latest_ts).to_pydatetime()
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
            else:
                end_dt = datetime.now(timezone.utc)
        if start_dt is None:
            start_dt = end_dt - timedelta(days=days)

        # ── Fetch M5 bar index ─────────────────────────────────────────────
        df_m5 = store.fetch(sym, "5m", start=start_dt, end=end_dt)
        if df_m5 is None or df_m5.empty:
            print(f"  No M5 data for {sym} {start_dt} -> {end_dt}")
            print("  Run: python -m backtest.run_backtest --ingest-only --days 30")
            return {}

        timestamps = list(df_m5.index)
        print(f"\n  Replaying {len(timestamps)} M5 bars for {sym}")
        print(f"  From: {timestamps[0]}")
        print(f"  To:   {timestamps[-1]}")
        print()

        # ── Set up scanner and replay engine ──────────────────────────────
        engine = ReplayEngine(store, symbol=sym)
        scanner = ScalpingScanner()

        # ── One VirtualExecutor per family ────────────────────────────────
        executors: Dict[str, VirtualExecutor] = {fam: VirtualExecutor(store) for fam in FAMILIES}

        # ── Collection pass — one list per family ─────────────────────────
        # Each entry: (cursor_dt, signal_dict)
        collected: Dict[str, List[Tuple[datetime, dict]]] = {fam: [] for fam in FAMILIES}

        # Regime tracking
        regime_counts: Dict[str, int] = defaultdict(int)
        # Daily map: {day_str: {regime: str, family: count}}
        daily_map: Dict[str, Dict] = defaultdict(lambda: defaultdict(int))

        with engine:
            for i, ts in enumerate(timestamps):
                engine.set_cursor(ts)
                cursor_dt = ts.to_pydatetime() if isinstance(ts, pd.Timestamp) else ts
                if cursor_dt.tzinfo is None:
                    cursor_dt = cursor_dt.replace(tzinfo=timezone.utc)
                _BacktestDatetime._bt_cursor = cursor_dt

                # Progress indicator every 100 bars
                if (i + 1) % 100 == 0 or i == len(timestamps) - 1:
                    pct = (i + 1) / len(timestamps) * 100
                    print(f"\r  Progress: {pct:.0f}% ({i+1}/{len(timestamps)} bars)", end="", flush=True)

                # Detect H1 regime for this bar
                regime = _detect_regime(store, cursor_dt, sym)
                regime_counts[regime] += 1

                day_str = cursor_dt.strftime("%Y-%m-%d")
                daily_map[day_str]["regime"] = regime  # last bar's regime wins for display

                # Run scanner
                try:
                    result = scanner.scan_xauusd(require_enabled=False)
                except Exception as e:
                    logger.debug("[CombinedBT] Scanner error at %s: %s", ts, e)
                    continue

                if result is None or result.status != "ready" or result.signal is None:
                    continue

                sig = result.signal
                direction = str(getattr(sig, "direction", "") or "").lower()
                confidence = float(getattr(sig, "confidence", 0.0) or 0.0)

                # Session label for this bar (used by session-gated families)
                bar_session = _get_session_label(cursor_dt)

                # ── Family 1: behavioral_v2 — all valid signals (no session/dir gate)
                sd_base = _build_signal_dict(sig, sym, FAMILY_META["behavioral_v2"]["source"], cursor_dt, regime)
                collected["behavioral_v2"].append((cursor_dt, sd_base))
                daily_map[day_str]["behavioral_v2"] += 1

                # ── Family 2: FSS — bear + short + conf≥65 + overlap/london only ──
                _eu_sessions = {"overlap", "london"}
                if regime == "bear" and _passes_flow_gate(sig, "short") and bar_session in _eu_sessions:
                    sd_fss = _build_signal_dict(sig, sym, FAMILY_META["fss"]["source"], cursor_dt, regime)
                    collected["fss"].append((cursor_dt, sd_fss))
                    daily_map[day_str]["fss"] += 1

                # ── Family 3: FLS — bull + long + conf≥65 (lowered from 69→65) ──
                if regime == "bull" and _passes_flow_gate(sig, "long", min_conf=65.0):
                    sd_fls = _build_signal_dict(sig, sym, FAMILY_META["fls"]["source"], cursor_dt, regime)
                    collected["fls"].append((cursor_dt, sd_fls))
                    daily_map[day_str]["fls"] += 1

                # ── Family 4: FFFS — bear + short + conf≥72 + overlap/london only ──
                # High-confidence bear reinforcement — EU sessions only (same as FSS)
                if regime == "bear" and direction == "short" and confidence >= 72.0 and bar_session in _eu_sessions:
                    sd_fffs = _build_signal_dict(sig, sym, FAMILY_META["fffs"]["source"], cursor_dt, regime)
                    collected["fffs"].append((cursor_dt, sd_fffs))
                    daily_map[day_str]["fffs"] += 1

                # ── Family 5: RR — ranging + long-only + not overlap ──────────────
                # Short direction WR=11.5% (catastrophic) → long-only gate added
                if regime == "ranging" and direction == "long" and bar_session != "overlap":
                    sd_rr = _build_signal_dict(sig, sym, FAMILY_META["rr"]["source"], cursor_dt, regime)
                    collected["rr"].append((cursor_dt, sd_rr))
                    daily_map[day_str]["rr"] += 1

        print()  # newline after progress

        # ── Resolution pass — resolve each family independently ────────────
        for fam in FAMILIES:
            fam_signals = collected[fam]
            print(f"  Resolving {len(fam_signals):>4} signals for {FAMILY_META[fam]['label']}...")
            for signal_time, signal_dict in fam_signals:
                if isinstance(signal_time, pd.Timestamp):
                    signal_time = signal_time.to_pydatetime()
                if signal_time.tzinfo is None:
                    signal_time = signal_time.replace(tzinfo=timezone.utc)
                executors[fam].resolve_trade(signal_dict, signal_time)

        # ── Compute per-family stats ────────────────────────────────────────
        family_stats: Dict[str, dict] = {}
        for fam in FAMILIES:
            family_stats[fam] = _calc_family_stats(executors[fam].results)

        # ── Total (deduplicated) ───────────────────────────────────────────
        family_results_map = {fam: executors[fam].results for fam in FAMILIES}
        dedup = _dedup_results(family_results_map)
        total_stats = _calc_family_stats(dedup)

        # ── Save each family to ResultsStore ──────────────────────────────
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")
        for fam in FAMILIES:
            s = family_stats[fam]
            run_name = f"combined_bt_{sym}_{start_str}_{end_str}_{fam}"
            report = {
                "run_name": run_name,
                "symbol": sym,
                "start_date": str(start_dt),
                "end_date": str(end_dt),
                "strategy": FAMILY_META[fam]["source"],
                "total_signals": len(collected[fam]),
                "total_trades": s.get("trades", 0),
                "fill_rate": (
                    round(s.get("trades", 0) / len(collected[fam]) * 100, 1)
                    if collected[fam] else 0.0
                ),
                "wins": s.get("wins", 0),
                "losses": s.get("losses", 0),
                "win_rate": s.get("win_rate", 0.0),
                "total_pnl_r": s.get("pnl_r", 0.0),
                "profit_factor": s.get("profit_factor", 0.0),
                "max_drawdown_r": s.get("max_dd_r", 0.0),
            }
            try:
                run_id = results_store.save_run(report)
                logger.debug("[CombinedBT] Saved %s as run #%d", run_name, run_id)
            except Exception as e:
                logger.warning("[CombinedBT] Could not save %s: %s", run_name, e)

        # ── Print report ───────────────────────────────────────────────────
        _print_combined_report(
            family_stats=family_stats,
            total_stats=total_stats,
            regime_counts=dict(regime_counts),
            daily_map={k: dict(v) for k, v in daily_map.items()},
            start_dt=start_dt,
            end_dt=end_dt,
            symbol=sym,
            family_results={fam: executors[fam].results for fam in FAMILIES},
        )

        return {**family_stats, "total": total_stats}

    finally:
        # ── Restore all patches ─────────────────────────────────────────────
        if "__xauusd_datetime" in originals:
            mod, orig_cls = originals.pop("__xauusd_datetime")
            mod.datetime = orig_cls

        if "__session_manager_patch" in originals:
            try:
                from market.data_fetcher import session_manager
                session_manager.is_xauusd_market_open = originals.pop("__session_manager_patch")
            except Exception:
                originals.pop("__session_manager_patch", None)

        if "__session_holiday_patch" in originals:
            try:
                from market.data_fetcher import session_manager
                session_manager.is_xauusd_holiday = originals.pop("__session_holiday_patch")
            except Exception:
                originals.pop("__session_holiday_patch", None)

        if "__session_info_patch" in originals:
            try:
                from market.data_fetcher import session_manager
                session_manager.get_session_info = originals.pop("__session_info_patch")
            except Exception:
                originals.pop("__session_info_patch", None)

        if "__get_price" in originals:
            try:
                from market.data_fetcher import xauusd_provider
                xauusd_provider.get_current_price = originals.pop("__get_price")
            except Exception:
                originals.pop("__get_price", None)

        if "__eco_fetch" in originals:
            try:
                from market.economic_calendar import economic_calendar as _eco_cal
                _eco_cal.fetch_events = originals.pop("__eco_fetch")
            except (ImportError, AttributeError):
                originals.pop("__eco_fetch", None)

        try:
            _BacktestDatetime._bt_cursor = None
        except NameError:
            pass

        # Restore config flags
        for key, value in originals.items():
            try:
                from config import config
                setattr(config, key, value)
            except Exception:
                pass


# ── CLI entry point ────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dexter Pro — Combined Multi-Family XAUUSD Backtester"
    )
    parser.add_argument(
        "--from", dest="from_date", type=str, required=True,
        help="Start date YYYY-MM-DD (required)",
    )
    parser.add_argument(
        "--to", dest="to_date", type=str, default=None,
        help="End date YYYY-MM-DD (default: latest available bar)",
    )
    parser.add_argument(
        "--symbol", type=str, default="XAUUSD",
        help="Symbol (default: XAUUSD)",
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Custom candle DB path (optional)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    # Suppress verbose scanner/provider INFO logs — only show BT progress
    for _noisy in ("scanners", "market", "analysis", "agent", "learning", "execution"):
        logging.getLogger(_noisy).setLevel(logging.ERROR)

    start_dt = datetime.strptime(args.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = None
    if args.to_date:
        end_dt = datetime.strptime(args.to_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        )

    store = CandleStore(db_path=args.db)
    results_store = ResultsStore()

    try:
        run_combined(
            store=store,
            results_store=results_store,
            symbol=args.symbol.upper(),
            start_dt=start_dt,
            end_dt=end_dt,
        )
    finally:
        store.close()
        results_store.close()


if __name__ == "__main__":
    main()
