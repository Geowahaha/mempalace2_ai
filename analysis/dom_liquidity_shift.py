"""
analysis/dom_liquidity_shift.py

DOM Liquidity Shift Detector for Position Manager active defense.
Detects liquidity draining/building from consecutive depth snapshots
stored in ctrader_depth_quotes table.

Integrates with existing active defense score in ctrader_executor.py.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


# ---------------------------------------------------------------------------
# 1. Query depth snapshots (consecutive capture runs)
# ---------------------------------------------------------------------------

def query_recent_depth_snapshots(
    conn: sqlite3.Connection,
    *,
    symbol: str = "XAUUSD",
    lookback_min: int = 30,
    max_runs: int = 6,
) -> list[dict]:
    """Query depth quotes grouped by capture run, recent first.

    Returns list of snapshots: [{run_id, event_utc, bid_levels: [{price, size}], ask_levels: [...]}]
    """
    cutoff_ts = (datetime.now(timezone.utc) - timedelta(minutes=max(1, lookback_min))).timestamp()

    try:
        # Get distinct runs within lookback
        runs = conn.execute(
            """
            SELECT DISTINCT run_id, MAX(event_ts) as max_ts
            FROM ctrader_depth_quotes
            WHERE symbol = ? AND event_ts >= ?
            GROUP BY run_id
            ORDER BY max_ts DESC
            LIMIT ?
            """,
            (symbol, cutoff_ts, max_runs),
        ).fetchall()
    except Exception as e:
        logger.debug("[DOMLiquidity] query runs error: %s", e)
        return []

    if not runs:
        return []

    snapshots = []
    for run_row in runs:
        run_id = str(run_row["run_id"])
        try:
            depth_rows = conn.execute(
                """
                SELECT side, price, size, level_index, event_utc, event_ts
                FROM ctrader_depth_quotes
                WHERE run_id = ? AND symbol = ?
                ORDER BY side, level_index
                """,
                (run_id, symbol),
            ).fetchall()
        except Exception:
            continue

        bid_levels = []
        ask_levels = []
        event_utc = ""
        for dr in depth_rows:
            level = {
                "price": _safe_float(dr["price"]),
                "size": _safe_float(dr["size"]),
                "level_index": int(dr["level_index"] or 0),
            }
            event_utc = str(dr["event_utc"] or "")
            if str(dr["side"] or "").lower() == "bid":
                bid_levels.append(level)
            else:
                ask_levels.append(level)

        snapshots.append({
            "run_id": run_id,
            "event_utc": event_utc,
            "bid_levels": sorted(bid_levels, key=lambda x: x["price"], reverse=True),
            "ask_levels": sorted(ask_levels, key=lambda x: x["price"]),
            "bid_total_size": sum(l["size"] for l in bid_levels),
            "ask_total_size": sum(l["size"] for l in ask_levels),
        })

    return snapshots


# ---------------------------------------------------------------------------
# 2. Compute liquidity shift metrics
# ---------------------------------------------------------------------------

def compute_liquidity_shift(
    snapshots: list[dict],
) -> dict:
    """Compare consecutive depth snapshots to detect liquidity changes.

    Returns {
        bid_size_change_pct: float,    # % change in total bid size (negative = draining)
        ask_size_change_pct: float,    # % change in total ask size
        bid_wall_shift: str,           # 'building' | 'stable' | 'draining'
        ask_wall_shift: str,
        imbalance_trend: float,        # positive = bids strengthening vs asks
        liquidity_score: int,          # -2 to +2 (negative = adverse for longs)
        n_snapshots: int,
    }
    """
    if len(snapshots) < 2:
        return {
            "bid_size_change_pct": 0.0,
            "ask_size_change_pct": 0.0,
            "bid_wall_shift": "unknown",
            "ask_wall_shift": "unknown",
            "imbalance_trend": 0.0,
            "liquidity_score": 0,
            "n_snapshots": len(snapshots),
        }

    # Snapshots are newest-first, so [0] = latest, [-1] = oldest
    latest = snapshots[0]
    earliest = snapshots[-1]

    latest_bid = _safe_float(latest.get("bid_total_size"), 0.0)
    latest_ask = _safe_float(latest.get("ask_total_size"), 0.0)
    earliest_bid = _safe_float(earliest.get("bid_total_size"), 0.0)
    earliest_ask = _safe_float(earliest.get("ask_total_size"), 0.0)

    # Bid/ask size change
    bid_change_pct = ((latest_bid - earliest_bid) / earliest_bid * 100) if earliest_bid > 0 else 0.0
    ask_change_pct = ((latest_ask - earliest_ask) / earliest_ask * 100) if earliest_ask > 0 else 0.0

    # Wall shift classification
    def _classify_shift(change_pct: float) -> str:
        if change_pct > 15:
            return "building"
        elif change_pct < -15:
            return "draining"
        return "stable"

    bid_wall_shift = _classify_shift(bid_change_pct)
    ask_wall_shift = _classify_shift(ask_change_pct)

    # Imbalance trend across all snapshots
    imbalances = []
    for snap in snapshots:
        bid_sz = _safe_float(snap.get("bid_total_size"), 0.0)
        ask_sz = _safe_float(snap.get("ask_total_size"), 0.0)
        total = bid_sz + ask_sz
        if total > 0:
            imbalances.append((bid_sz - ask_sz) / total)
        else:
            imbalances.append(0.0)

    # Trend = latest imbalance vs earliest (positive = bids strengthening)
    imbalance_trend = imbalances[0] - imbalances[-1] if len(imbalances) >= 2 else 0.0

    # Liquidity score: [-2, +2] where negative = bearish shift
    score = 0
    if bid_wall_shift == "building":
        score += 1
    elif bid_wall_shift == "draining":
        score -= 1
    if ask_wall_shift == "draining":
        score += 1  # asks draining = less resistance = bullish
    elif ask_wall_shift == "building":
        score -= 1  # asks building = more resistance = bearish
    # Clamp
    score = max(-2, min(2, score))

    return {
        "bid_size_change_pct": round(bid_change_pct, 2),
        "ask_size_change_pct": round(ask_change_pct, 2),
        "bid_wall_shift": bid_wall_shift,
        "ask_wall_shift": ask_wall_shift,
        "imbalance_trend": round(imbalance_trend, 4),
        "liquidity_score": score,
        "n_snapshots": len(snapshots),
    }


# ---------------------------------------------------------------------------
# 3. Adverse liquidity detection (for position manager)
# ---------------------------------------------------------------------------

def detect_adverse_liquidity(
    shift: dict,
    direction: str,
) -> dict:
    """Determine if liquidity shift is adverse to the current position direction.

    For longs: bid draining + ask building = adverse (support disappearing, resistance building)
    For shorts: ask draining + bid building = adverse (resistance disappearing, support building against us)

    Returns {
        is_adverse: bool,
        severity: str,              # 'none' | 'mild' | 'moderate' | 'severe'
        adverse_score: int,         # 0-3 (higher = more adverse)
        recommendation: str,        # 'hold' | 'tighten_stop' | 'close_partial' | 'close_full'
        details: dict,
    }
    """
    bid_shift = str(shift.get("bid_wall_shift", "stable") or "stable")
    ask_shift = str(shift.get("ask_wall_shift", "stable") or "stable")
    liq_score = int(shift.get("liquidity_score", 0) or 0)
    imbalance_trend = float(shift.get("imbalance_trend", 0.0) or 0.0)

    adverse_score = 0
    reasons = []

    if direction == "long":
        # Adverse for longs: bid draining (support gone), ask building (resistance up)
        if bid_shift == "draining":
            adverse_score += 1
            reasons.append("bid_support_draining")
        if ask_shift == "building":
            adverse_score += 1
            reasons.append("ask_resistance_building")
        if imbalance_trend < -0.10:
            adverse_score += 1
            reasons.append("imbalance_turning_bearish")
    else:
        # Adverse for shorts: ask draining (resistance gone), bid building (support up)
        if ask_shift == "draining":
            adverse_score += 1
            reasons.append("ask_resistance_draining")
        if bid_shift == "building":
            adverse_score += 1
            reasons.append("bid_support_building")
        if imbalance_trend > 0.10:
            adverse_score += 1
            reasons.append("imbalance_turning_bullish")

    is_adverse = adverse_score >= 1
    if adverse_score >= 3:
        severity = "severe"
        recommendation = "close_full"
    elif adverse_score >= 2:
        severity = "moderate"
        recommendation = "tighten_stop"
    elif adverse_score >= 1:
        severity = "mild"
        recommendation = "tighten_stop"
    else:
        severity = "none"
        recommendation = "hold"

    return {
        "is_adverse": is_adverse,
        "severity": severity,
        "adverse_score": adverse_score,
        "recommendation": recommendation,
        "reasons": reasons,
        "details": {
            "bid_shift": bid_shift,
            "ask_shift": ask_shift,
            "liquidity_score": liq_score,
            "imbalance_trend": round(imbalance_trend, 4),
        },
    }


# ---------------------------------------------------------------------------
# 3b. Favorable liquidity detection (for TP extension)
# ---------------------------------------------------------------------------

def detect_favorable_liquidity(
    shift: dict,
    direction: str,
) -> dict:
    """Determine if liquidity shift is favorable — supports holding/extending TP.

    Mirror of detect_adverse_liquidity:
    For longs: bid building + ask draining = favorable (support growing, resistance fading)
    For shorts: ask building + bid draining = favorable (resistance growing, support fading)

    Returns {
        is_favorable: bool,
        strength: str,              # 'none' | 'mild' | 'moderate' | 'strong'
        favorable_score: int,       # 0-3 (higher = more favorable)
        recommendation: str,        # 'hold' | 'extend_tp' | 'trail_wide'
        reasons: list[str],
        details: dict,
    }
    """
    bid_shift = str(shift.get("bid_wall_shift", "stable") or "stable")
    ask_shift = str(shift.get("ask_wall_shift", "stable") or "stable")
    liq_score = int(shift.get("liquidity_score", 0) or 0)
    imbalance_trend = float(shift.get("imbalance_trend", 0.0) or 0.0)

    favorable_score = 0
    reasons = []

    if direction == "long":
        if bid_shift == "building":
            favorable_score += 1
            reasons.append("bid_support_building")
        if ask_shift == "draining":
            favorable_score += 1
            reasons.append("ask_resistance_draining")
        if imbalance_trend > 0.10:
            favorable_score += 1
            reasons.append("imbalance_turning_bullish")
    else:
        if ask_shift == "building":
            favorable_score += 1
            reasons.append("ask_resistance_building")
        if bid_shift == "draining":
            favorable_score += 1
            reasons.append("bid_support_draining")
        if imbalance_trend < -0.10:
            favorable_score += 1
            reasons.append("imbalance_turning_bearish")

    is_favorable = favorable_score >= 2
    if favorable_score >= 3:
        strength = "strong"
        recommendation = "trail_wide"
    elif favorable_score >= 2:
        strength = "moderate"
        recommendation = "extend_tp"
    elif favorable_score >= 1:
        strength = "mild"
        recommendation = "hold"
    else:
        strength = "none"
        recommendation = "hold"

    return {
        "is_favorable": is_favorable,
        "strength": strength,
        "favorable_score": favorable_score,
        "recommendation": recommendation,
        "reasons": reasons,
        "details": {
            "bid_shift": bid_shift,
            "ask_shift": ask_shift,
            "liquidity_score": liq_score,
            "imbalance_trend": round(imbalance_trend, 4),
        },
    }


# ---------------------------------------------------------------------------
# 4. Full liquidity shift analysis (convenience)
# ---------------------------------------------------------------------------

def analyze_dom_liquidity(
    conn: sqlite3.Connection,
    *,
    symbol: str = "XAUUSD",
    direction: str = "long",
    lookback_min: int = 30,
    max_runs: int = 6,
) -> dict:
    """Full pipeline: query depth → compute shift → detect adverse.

    Returns {ok, shift, adverse, snapshots_used}
    """
    snapshots = query_recent_depth_snapshots(
        conn, symbol=symbol, lookback_min=lookback_min, max_runs=max_runs
    )
    if len(snapshots) < 2:
        return {
            "ok": False,
            "status": "insufficient_depth_data",
            "shift": {},
            "adverse": {"is_adverse": False, "severity": "none", "recommendation": "hold"},
            "snapshots_used": len(snapshots),
        }

    shift = compute_liquidity_shift(snapshots)
    adverse = detect_adverse_liquidity(shift, direction)
    favorable = detect_favorable_liquidity(shift, direction)

    return {
        "ok": True,
        "shift": shift,
        "adverse": adverse,
        "favorable": favorable,
        "snapshots_used": len(snapshots),
        "symbol": symbol,
        "direction": direction,
    }
