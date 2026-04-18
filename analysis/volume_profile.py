"""
analysis/volume_profile.py

Session Volume Profile from M1 bar data (stream_trendbars).
Computes POC, Value Area (VA), High/Low Volume Nodes (HVN/LVN) per session.

Uses cTrader streaming M1 bars stored in ctrader_openapi.db → stream_trendbars table.
Pure Python — no external dependencies.
"""
from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Session boundaries in UTC (approximate, not DST-adjusted for simplicity)
SESSION_WINDOWS_UTC = {
    "asian":  (0, 8),    # 00:00-08:00 UTC
    "london": (7, 15),   # 07:00-15:00 UTC (overlap 07-08)
    "ny":     (13, 21),  # 13:00-21:00 UTC (overlap 13-15)
    "full":   (0, 24),   # full day
}

DEFAULT_TICK_SIZE = 0.01       # XAUUSD minimum price increment
DEFAULT_VA_PCT = 0.70          # 70% of volume = value area
DEFAULT_HVN_PERCENTILE = 80    # top 20% volume = HVN
DEFAULT_LVN_PERCENTILE = 20    # bottom 20% volume = LVN

# Per-symbol tick sizes and default bucket multipliers
SYMBOL_TICK_CONFIG = {
    "XAUUSD": {"tick_size": 0.01, "bucket_ticks": 10},    # $0.10 per bucket
    "BTCUSD": {"tick_size": 1.0, "bucket_ticks": 50},     # $50 per bucket
    "ETHUSD": {"tick_size": 0.01, "bucket_ticks": 500},   # $5 per bucket
}


def get_tick_config(symbol: str) -> dict:
    """Get tick_size and default bucket_ticks for a symbol."""
    return dict(SYMBOL_TICK_CONFIG.get(symbol.upper(), {"tick_size": DEFAULT_TICK_SIZE, "bucket_ticks": 10}))


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


# ---------------------------------------------------------------------------
# 1. Query M1 bars from stream_trendbars
# ---------------------------------------------------------------------------

def query_m1_bars(
    conn: sqlite3.Connection,
    *,
    symbol: str = "XAUUSD",
    hours_back: int = 24,
    session: str = "full",
) -> list[dict]:
    """Query M1 bars from stream_trendbars for the given lookback window.

    Returns list of dicts: {ts_ms, ts_utc, open, high, low, close, volume}
    """
    now_utc = datetime.now(timezone.utc)
    cutoff_ms = int((now_utc - timedelta(hours=max(1, hours_back))).timestamp() * 1000)

    try:
        rows = conn.execute(
            """
            SELECT ts_ms, ts_utc, open, high, low, close, volume
            FROM stream_trendbars
            WHERE symbol = ? AND tf = 'M1' AND ts_ms >= ?
            ORDER BY ts_ms ASC
            """,
            (symbol, cutoff_ms),
        ).fetchall()
    except Exception as e:
        logger.debug("[VolumeProfile] query error: %s", e)
        return []

    # Filter by session window if not "full"
    session_start, session_end = SESSION_WINDOWS_UTC.get(session, (0, 24))
    result = []
    for row in rows:
        bar = {
            "ts_ms": int(row["ts_ms"]),
            "ts_utc": str(row["ts_utc"] or ""),
            "open": _safe_float(row["open"]),
            "high": _safe_float(row["high"]),
            "low": _safe_float(row["low"]),
            "close": _safe_float(row["close"]),
            "volume": max(0, int(row["volume"] or 0)),
        }
        if session != "full":
            try:
                bar_hour = datetime.fromtimestamp(bar["ts_ms"] / 1000, tz=timezone.utc).hour
            except Exception:
                bar_hour = 0
            if session_start <= session_end:
                if not (session_start <= bar_hour < session_end):
                    continue
            else:
                if not (bar_hour >= session_start or bar_hour < session_end):
                    continue
        result.append(bar)
    return result


# ---------------------------------------------------------------------------
# 2. Build price-level volume distribution
# ---------------------------------------------------------------------------

def build_price_volume_distribution(
    bars: list[dict],
    *,
    tick_size: float = DEFAULT_TICK_SIZE,
    bucket_ticks: int = 10,
) -> dict[float, float]:
    """Aggregate bar data into price-level volume buckets.

    Each bar's volume is distributed across [low, high] range proportionally.
    Bucket size = tick_size * bucket_ticks (e.g., 0.01 * 10 = $0.10 per bucket for XAUUSD).

    Returns {price_level: total_volume} dict.
    """
    if not bars:
        return {}

    bucket_size = max(tick_size, tick_size * max(1, bucket_ticks))
    distribution: dict[float, float] = defaultdict(float)

    for bar in bars:
        bar_high = bar.get("high", 0.0)
        bar_low = bar.get("low", 0.0)
        bar_close = bar.get("close", 0.0)
        bar_volume = max(1, bar.get("volume", 1))  # use 1 as min to avoid zero-contribution bars

        if bar_high <= 0 or bar_low <= 0:
            continue

        # Snap to bucket boundaries
        low_bucket = round(bar_low - (bar_low % bucket_size), 2)
        high_bucket = round(bar_high - (bar_high % bucket_size) + bucket_size, 2)

        # Count buckets the bar spans
        n_buckets = max(1, int(round((high_bucket - low_bucket) / bucket_size)))

        # Distribute volume — weight close bucket higher (TPO-like weighting)
        close_bucket = round(bar_close - (bar_close % bucket_size), 2)
        base_vol = bar_volume / (n_buckets + 1)  # +1 for close bonus

        price = low_bucket
        while price <= high_bucket:
            contribution = base_vol
            if abs(price - close_bucket) < bucket_size * 0.5:
                contribution = base_vol * 2  # close bucket gets double weight
            distribution[round(price, 2)] += contribution
            price = round(price + bucket_size, 2)

    return dict(distribution)


# ---------------------------------------------------------------------------
# 3. Compute Volume Profile metrics
# ---------------------------------------------------------------------------

def compute_volume_profile(
    distribution: dict[float, float],
    *,
    va_pct: float = DEFAULT_VA_PCT,
    hvn_percentile: int = DEFAULT_HVN_PERCENTILE,
    lvn_percentile: int = DEFAULT_LVN_PERCENTILE,
) -> dict:
    """Compute POC, VA, HVN, LVN from price-volume distribution.

    Returns {
        poc: float,                    # Point of Control (highest volume price)
        va_high: float,                # Value Area High
        va_low: float,                 # Value Area Low
        hvn_levels: list[float],       # High Volume Nodes
        lvn_levels: list[float],       # Low Volume Nodes
        total_volume: float,
        n_buckets: int,
        profile: list[{price, volume, pct}]  # full profile for visualization
    }
    """
    if not distribution:
        return {
            "poc": 0.0, "va_high": 0.0, "va_low": 0.0,
            "hvn_levels": [], "lvn_levels": [],
            "total_volume": 0.0, "n_buckets": 0, "profile": [],
        }

    # Sort by price
    sorted_levels = sorted(distribution.items(), key=lambda x: x[0])
    total_volume = sum(v for _, v in sorted_levels)
    if total_volume <= 0:
        return {
            "poc": 0.0, "va_high": 0.0, "va_low": 0.0,
            "hvn_levels": [], "lvn_levels": [],
            "total_volume": 0.0, "n_buckets": len(sorted_levels), "profile": [],
        }

    # POC = price level with highest volume
    poc_price = max(sorted_levels, key=lambda x: x[1])[0]

    # Value Area = smallest price range containing va_pct of total volume
    # Start from POC and expand outward
    sorted_by_vol = sorted(sorted_levels, key=lambda x: x[1], reverse=True)
    va_volume_target = total_volume * max(0.5, min(0.95, va_pct))
    va_levels = set()
    cumulative = 0.0
    for price, vol in sorted_by_vol:
        va_levels.add(price)
        cumulative += vol
        if cumulative >= va_volume_target:
            break

    va_prices = sorted(va_levels)
    va_high = va_prices[-1] if va_prices else poc_price
    va_low = va_prices[0] if va_prices else poc_price

    # HVN / LVN based on volume percentiles
    volumes = sorted([v for _, v in sorted_levels])
    hvn_threshold = volumes[min(len(volumes) - 1, int(len(volumes) * hvn_percentile / 100))]
    lvn_threshold = volumes[min(len(volumes) - 1, int(len(volumes) * lvn_percentile / 100))]

    hvn_levels = sorted([p for p, v in sorted_levels if v >= hvn_threshold])
    lvn_levels = sorted([p for p, v in sorted_levels if v <= lvn_threshold and v > 0])

    # Build profile for visualization/export
    profile = [
        {
            "price": round(p, 2),
            "volume": round(v, 1),
            "pct": round(v / total_volume * 100, 2) if total_volume > 0 else 0.0,
        }
        for p, v in sorted_levels
    ]

    return {
        "poc": round(poc_price, 2),
        "va_high": round(va_high, 2),
        "va_low": round(va_low, 2),
        "hvn_levels": [round(p, 2) for p in hvn_levels[:10]],  # cap at 10
        "lvn_levels": [round(p, 2) for p in lvn_levels[:10]],
        "total_volume": round(total_volume, 1),
        "n_buckets": len(sorted_levels),
        "profile": profile,
    }


# ---------------------------------------------------------------------------
# 4. Entry confirmation helpers
# ---------------------------------------------------------------------------

def check_entry_vs_profile(
    entry_price: float,
    direction: str,
    vp: dict,
    *,
    tick_size: float = DEFAULT_TICK_SIZE,
    bucket_ticks: int = 10,
) -> dict:
    """Check an entry price against the volume profile for confirmation signals.

    Returns {
        poc_distance: float,          # distance from POC in price units
        in_value_area: bool,          # price inside VA
        near_poc: bool,               # within 2 buckets of POC
        near_hvn: bool,               # within 2 buckets of any HVN
        near_lvn: bool,               # within 2 buckets of any LVN
        vp_confirmation: str,         # 'strong' | 'moderate' | 'weak' | 'neutral'
        vp_reason: str,
    }
    """
    poc = float(vp.get("poc", 0.0) or 0.0)
    va_high = float(vp.get("va_high", 0.0) or 0.0)
    va_low = float(vp.get("va_low", 0.0) or 0.0)
    hvn = list(vp.get("hvn_levels") or [])
    lvn = list(vp.get("lvn_levels") or [])

    if poc <= 0 or entry_price <= 0:
        return {
            "poc_distance": 0.0, "in_value_area": False,
            "near_poc": False, "near_hvn": False, "near_lvn": False,
            "vp_confirmation": "neutral", "vp_reason": "no_profile",
        }

    bucket_range = tick_size * max(1, bucket_ticks) * 2  # 2 buckets tolerance
    poc_distance = abs(entry_price - poc)
    in_va = va_low <= entry_price <= va_high
    near_poc = poc_distance <= bucket_range
    near_hvn = any(abs(entry_price - h) <= bucket_range for h in hvn)
    near_lvn = any(abs(entry_price - l) <= bucket_range for l in lvn)

    # Confirmation logic
    reasons = []
    score = 0

    if direction == "long":
        # Long at POC/HVN rejection = strong (buying at support)
        if near_poc or near_hvn:
            score += 2
            reasons.append("long_at_volume_support")
        # Long at LVN = weak (thin liquidity, can slice through)
        if near_lvn:
            score -= 1
            reasons.append("long_at_thin_liquidity")
        # Long above VA = momentum (breakout from value)
        if entry_price > va_high:
            score += 1
            reasons.append("long_above_va_breakout")
    else:
        # Short at POC/HVN rejection = strong (selling at resistance)
        if near_poc or near_hvn:
            score += 2
            reasons.append("short_at_volume_resistance")
        if near_lvn:
            score -= 1
            reasons.append("short_at_thin_liquidity")
        if entry_price < va_low:
            score += 1
            reasons.append("short_below_va_breakdown")

    if in_va and not near_poc:
        reasons.append("inside_value_area")

    if score >= 2:
        confirmation = "strong"
    elif score >= 1:
        confirmation = "moderate"
    elif score <= -1:
        confirmation = "weak"
    else:
        confirmation = "neutral"

    return {
        "poc_distance": round(poc_distance, 2),
        "in_value_area": in_va,
        "near_poc": near_poc,
        "near_hvn": near_hvn,
        "near_lvn": near_lvn,
        "vp_confirmation": confirmation,
        "vp_reason": "|".join(reasons) if reasons else "neutral",
    }


# ---------------------------------------------------------------------------
# 5. SL/TP placement helpers
# ---------------------------------------------------------------------------

def suggest_sl_from_profile(
    entry_price: float,
    direction: str,
    vp: dict,
    *,
    atr_sl: float = 0.0,
    min_distance: float = 0.50,
) -> dict:
    """Suggest SL placement based on HVN levels (institutional support/resistance).

    Combines VP structural levels with ATR-based SL:
    - If HVN is closer to entry than ATR SL → use HVN (tighter, structural)
    - If HVN is farther than ATR SL → use ATR (respect the math)
    - Always respects min_distance

    Returns {suggested_sl, source, hvn_used, atr_sl, improvement_pct}
    """
    hvn = sorted(vp.get("hvn_levels") or [])
    poc = float(vp.get("poc", 0.0) or 0.0)

    if not hvn or entry_price <= 0:
        return {
            "suggested_sl": round(atr_sl, 2) if atr_sl > 0 else 0.0,
            "source": "atr_only",
            "hvn_used": 0.0,
            "atr_sl": round(atr_sl, 2),
            "improvement_pct": 0.0,
        }

    # Find nearest HVN beyond entry in the adverse direction
    candidates = []
    for h in hvn:
        if direction == "long" and h < entry_price - min_distance:
            candidates.append(h)
        elif direction == "short" and h > entry_price + min_distance:
            candidates.append(h)

    if not candidates:
        return {
            "suggested_sl": round(atr_sl, 2) if atr_sl > 0 else 0.0,
            "source": "atr_only",
            "hvn_used": 0.0,
            "atr_sl": round(atr_sl, 2),
            "improvement_pct": 0.0,
        }

    # Pick closest HVN to entry (tightest valid SL)
    if direction == "long":
        hvn_sl = max(candidates)  # highest HVN below entry
        hvn_distance = entry_price - hvn_sl
    else:
        hvn_sl = min(candidates)  # lowest HVN above entry
        hvn_distance = hvn_sl - entry_price

    if atr_sl > 0:
        atr_distance = abs(entry_price - atr_sl)
        if hvn_distance < atr_distance:
            improvement = round((atr_distance - hvn_distance) / atr_distance * 100, 1)
            return {
                "suggested_sl": round(hvn_sl, 2),
                "source": "hvn_structural",
                "hvn_used": round(hvn_sl, 2),
                "atr_sl": round(atr_sl, 2),
                "improvement_pct": improvement,
            }

    return {
        "suggested_sl": round(atr_sl, 2) if atr_sl > 0 else round(hvn_sl, 2),
        "source": "atr_preferred" if atr_sl > 0 else "hvn_structural",
        "hvn_used": round(hvn_sl, 2),
        "atr_sl": round(atr_sl, 2),
        "improvement_pct": 0.0,
    }


def suggest_tp_from_profile(
    entry_price: float,
    direction: str,
    vp: dict,
    *,
    min_rr: float = 1.5,
    risk_distance: float = 0.0,
) -> dict:
    """Suggest TP target at LVN levels (thin liquidity = price slices through easily).

    LVN beyond entry in profitable direction = natural TP target.

    Returns {suggested_tp, source, lvn_used, min_rr_tp}
    """
    lvn = sorted(vp.get("lvn_levels") or [])

    min_rr_tp = 0.0
    if risk_distance > 0:
        if direction == "long":
            min_rr_tp = round(entry_price + risk_distance * min_rr, 2)
        else:
            min_rr_tp = round(entry_price - risk_distance * min_rr, 2)

    if not lvn or entry_price <= 0:
        return {
            "suggested_tp": min_rr_tp,
            "source": "rr_only",
            "lvn_used": 0.0,
            "min_rr_tp": min_rr_tp,
        }

    # Find LVN beyond entry in profitable direction, past min RR
    candidates = []
    for l in lvn:
        if direction == "long" and l > entry_price:
            if risk_distance <= 0 or l >= min_rr_tp:
                candidates.append(l)
        elif direction == "short" and l < entry_price:
            if risk_distance <= 0 or l <= min_rr_tp:
                candidates.append(l)

    if not candidates:
        return {
            "suggested_tp": min_rr_tp,
            "source": "rr_only",
            "lvn_used": 0.0,
            "min_rr_tp": min_rr_tp,
        }

    # Pick closest LVN (first TP target)
    if direction == "long":
        lvn_tp = min(candidates)
    else:
        lvn_tp = max(candidates)

    return {
        "suggested_tp": round(lvn_tp, 2),
        "source": "lvn_structural",
        "lvn_used": round(lvn_tp, 2),
        "min_rr_tp": min_rr_tp,
    }


# ---------------------------------------------------------------------------
# 6. Full session VP builder (convenience)
# ---------------------------------------------------------------------------

def build_session_volume_profile(
    conn: sqlite3.Connection,
    *,
    symbol: str = "XAUUSD",
    hours_back: int = 24,
    session: str = "full",
    tick_size: float = DEFAULT_TICK_SIZE,
    bucket_ticks: int = 10,
    va_pct: float = DEFAULT_VA_PCT,
) -> dict:
    """Full pipeline: query bars → build distribution → compute VP.

    Returns {ok, vp: {poc, va_high, va_low, hvn_levels, lvn_levels, ...}, bars_used, session}
    """
    bars = query_m1_bars(conn, symbol=symbol, hours_back=hours_back, session=session)
    if not bars:
        return {
            "ok": False,
            "status": "no_bars",
            "vp": {},
            "bars_used": 0,
            "session": session,
        }

    distribution = build_price_volume_distribution(bars, tick_size=tick_size, bucket_ticks=bucket_ticks)
    vp = compute_volume_profile(distribution, va_pct=va_pct)

    return {
        "ok": True,
        "vp": vp,
        "bars_used": len(bars),
        "session": session,
        "symbol": symbol,
        "hours_back": hours_back,
    }
