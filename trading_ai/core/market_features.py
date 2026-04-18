from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

SessionName = Literal["ASIA", "LONDON", "NY"]
TrendName = Literal["UP", "DOWN", "RANGE"]
VolName = Literal["LOW", "MEDIUM", "HIGH"]


def _utc_hour(ts_unix: float) -> int:
    return int(datetime.fromtimestamp(ts_unix, tz=timezone.utc).hour)


def session_from_ts(ts_unix: float) -> SessionName:
    """
    UTC session buckets for day-trading context (FX/metals style).
    ASIA 00–08, LONDON 08–13, NY 13–22, late 22–00 → ASIA.
    """
    h = _utc_hour(ts_unix)
    if 0 <= h < 8:
        return "ASIA"
    if 8 <= h < 13:
        return "LONDON"
    if 13 <= h < 22:
        return "NY"
    return "ASIA"


def _pct_returns(closes: List[float]) -> List[float]:
    out: List[float] = []
    for i in range(1, len(closes)):
        a, b = closes[i - 1], closes[i]
        if a and abs(a) > 1e-12:
            out.append((b - a) / abs(a))
    return out


def _stdev(xs: List[float]) -> float:
    if len(xs) < 2:
        return 0.0
    mean = sum(xs) / float(len(xs))
    var = sum((x - mean) ** 2 for x in xs) / float(len(xs))
    return math.sqrt(max(0.0, var))


def _trend_from_closes(closes: List[float], *, sensitivity: float = 0.00025) -> TrendName:
    if len(closes) < 5:
        return "RANGE"
    window = closes[-20:] if len(closes) >= 20 else closes
    ret = (window[-1] - window[0]) / max(abs(window[0]), 1e-12)
    if ret > sensitivity:
        return "UP"
    if ret < -sensitivity:
        return "DOWN"
    return "RANGE"


def _volatility_from_closes_and_spread(
    closes: List[float],
    *,
    mid: float,
    spread: float,
) -> VolName:
    rets = _pct_returns(closes[-32:] if len(closes) > 32 else closes)
    if len(rets) >= 3:
        var = sum(r * r for r in rets) / len(rets)
        st = math.sqrt(var)
        if st < 0.00008:
            tier: VolName = "LOW"
        elif st < 0.00022:
            tier = "MEDIUM"
        else:
            tier = "HIGH"
    else:
        spread_pct = (spread / mid) if mid else 0.0
        if spread_pct < 0.00005:
            tier = "LOW"
        elif spread_pct < 0.00012:
            tier = "MEDIUM"
        else:
            tier = "HIGH"
    return tier


def _range_width(xs: List[float]) -> float:
    if len(xs) < 2:
        return 0.0
    return max(xs) - min(xs)


def _momentum_pct(closes: List[float], lookback: int) -> float:
    if len(closes) <= lookback:
        return 0.0
    base = closes[-(lookback + 1)]
    if abs(base) <= 1e-12:
        return 0.0
    return (closes[-1] - base) / abs(base)


def _structure_flags(closes: List[float]) -> Dict[str, bool]:
    if len(closes) < 6:
        return {"higher_high": False, "lower_low": False, "consolidation": True}
    recent = closes[-5:]
    prior = closes[-10:-5] if len(closes) >= 10 else closes[:-5]
    hh = closes[-1] >= max(recent[:-1], default=closes[-1])
    ll = closes[-1] <= min(recent[:-1], default=closes[-1])
    wr, wp = _range_width(recent), _range_width(prior) if len(prior) >= 2 else _range_width(recent)
    consolidation = wp > 1e-12 and wr <= 0.55 * wp
    return {"higher_high": hh, "lower_low": ll, "consolidation": bool(consolidation)}


def extract_features(market_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a stable feature dict for memory keys and prompting.

    `market_data` should include at least: mid, spread, ts_unix, symbol.
    Optional:
      * `price_history`: list of recent mids (strongly recommended)
      * `bars`: list of {open,high,low,close} for richer structure
    """
    mid = float(market_data.get("mid") or 0.0)
    spread = float(market_data.get("spread") or 0.0)
    ts = float(market_data.get("ts_unix") or 0.0)
    symbol = str(market_data.get("symbol") or "")

    bars = market_data.get("bars")
    closes: List[float]
    if isinstance(bars, list) and bars:
        closes = []
        for b in bars:
            if isinstance(b, dict) and "close" in b:
                closes.append(float(b["close"]))
            elif isinstance(b, (list, tuple)) and len(b) >= 4:
                closes.append(float(b[3]))
    else:
        ph = market_data.get("price_history")
        if isinstance(ph, list) and ph:
            closes = [float(x) for x in ph if x is not None]
        else:
            closes = [mid] if mid else []

    if mid and (not closes or closes[-1] != mid):
        closes = closes + [mid]

    session = session_from_ts(ts if ts > 0 else time.time())
    trend = _trend_from_closes(closes)
    vol = _volatility_from_closes_and_spread(closes, mid=mid, spread=spread)
    structure = _structure_flags(closes)
    recent_window = closes[-20:] if len(closes) >= 20 else closes
    recent_rets = _pct_returns(closes[-32:] if len(closes) > 32 else closes)
    realized_vol = _stdev(recent_rets)
    momentum_5 = _momentum_pct(closes, 5)
    momentum_20 = _momentum_pct(closes, 20)
    range_width_pct = (_range_width(recent_window) / max(abs(mid), 1e-12)) if mid else 0.0
    trend_strength = abs(momentum_20) / max(realized_vol, 1e-9)
    recent_high = max(recent_window) if recent_window else mid
    recent_low = min(recent_window) if recent_window else mid

    return {
        "symbol": symbol,
        "session": session,
        "trend_direction": trend,
        "volatility": vol,
        "structure": structure,
        "sample_closes_len": len(closes),
        "spread_pct": (spread / mid) if mid else 0.0,
        "momentum_5": momentum_5,
        "momentum_20": momentum_20,
        "realized_volatility": realized_vol,
        "range_width_pct": range_width_pct,
        "trend_strength": trend_strength,
        "distance_from_recent_high_pct": ((recent_high - mid) / max(abs(mid), 1e-12)) if mid else 0.0,
        "distance_from_recent_low_pct": ((mid - recent_low) / max(abs(mid), 1e-12)) if mid else 0.0,
    }


def infer_setup_tag(features: Dict[str, Any], proposed_action: str) -> str:
    """Classify setup for memory tagging: trend_follow | reversal | breakout."""
    act = str(proposed_action or "HOLD").upper()
    trend = features.get("trend_direction", "RANGE")
    st = features.get("structure") or {}
    consolidation = bool(st.get("consolidation"))
    if consolidation and act in ("BUY", "SELL"):
        return "breakout"
    if trend == "DOWN" and act == "BUY":
        return "reversal"
    if trend == "UP" and act == "SELL":
        return "reversal"
    if trend == "UP" and act == "BUY":
        return "trend_follow"
    if trend == "DOWN" and act == "SELL":
        return "trend_follow"
    return "trend_follow"
