"""
execution/entry_confirmation.py
Provides pre-execution gates for MT5 trades based on M5 candle confirmation
and HTF/LTF convergence logic.
"""
import logging
from dataclasses import dataclass
from typing import Optional

from config import config
from learning.symbol_neural_brain import _classify_symbol

logger = logging.getLogger(__name__)


@dataclass
class M5ConfirmationResult:
    ok: bool
    status: str
    reason: str
    skipped_due_to_distance: bool = False


@dataclass
class HTFLTFConvergenceResult:
    ok: bool
    status: str
    reason: str
    size_mult: float = 1.0
    h1_trend: str = "neutral"
    m5_struct: str = "neutral"


def _candle_is_bullish(c) -> bool:
    return c["close"] > c["open"]


def _candle_is_bearish(c) -> bool:
    return c["close"] < c["open"]


def _ema(prices: list[float], period: int) -> float:
    if not prices:
        return 0.0
    if len(prices) == 1:
        return prices[0]
    alpha = 2.0 / (period + 1.0)
    ema = prices[0]
    for p in prices[1:]:
        ema = (p - ema) * alpha + ema
    return ema


def _check_family_enabled(family: str, apply_to: str) -> bool:
    if not apply_to:
        return False
    if apply_to.strip().lower() == "all":
        return True
    return family in [f.strip().lower() for f in apply_to.split(",") if f.strip()]


def check_m5_confirmation(
    mt5_bridge, broker_symbol: str, direction: str, entry_price: float, atr: float
) -> M5ConfirmationResult:
    """
    Check if the last M5 candles confirm the entry direction.
    - If price is > MT5_M5_CONFIRM_MAX_ATR_DIST from entry, skip check (assume stale).
    - LONG: PASS if last M5 is bullish or current is >50% bullish body.
             FAIL if last 2 M5 are bearish.
    - SHORT: mirror logic.
    """
    is_enabled = getattr(config, "MT5_M5_CONFIRM_ENABLED", False)
    if not is_enabled:
        return M5ConfirmationResult(True, "disabled", "M5 confirmation disabled")

    family = _classify_symbol(broker_symbol)
    apply_to = str(getattr(config, "MT5_M5_CONFIRM_APPLY_TO", "fx,gold"))
    if not _check_family_enabled(family, apply_to):
        return M5ConfirmationResult(True, "ignored_family", f"M5 confirm not applied to {family}")

    # Safety bounds
    entry = max(1e-12, entry_price)
    atr = max(1e-12, atr)

    # Get current tick to check distance
    tick = mt5_bridge.symbol_info_tick(broker_symbol)
    if not tick:
        return M5ConfirmationResult(True, "error", "could not get tick, bypassing check")

    is_long = direction.lower() == "long"
    current_price = getattr(tick, "ask", 0.0) if is_long else getattr(tick, "bid", 0.0)

    max_dist_atr = float(getattr(config, "MT5_M5_CONFIRM_MAX_ATR_DIST", 1.0))
    dist_atr = abs(current_price - entry) / atr

    if dist_atr > max_dist_atr:
        return M5ConfirmationResult(
            True, "stale_entry", f"price {dist_atr:.2f} ATR away from entry (max {max_dist_atr:.1f}), bypassing",
            skipped_due_to_distance=True
        )

    # Fetch last 3 M5 candles (0=current unfinished, 1=last closed, 2=prev closed)
    try:
        rates = mt5_bridge.copy_rates_from_pos(broker_symbol, mt5_bridge.TIMEFRAME_M5, 0, 3)
    except Exception as e:
        logger.debug(f"[EntryConfirm] copy_rates_from error: {e}")
        return M5ConfirmationResult(True, "error", f"API error: {e}, bypassing")

    if rates is None or len(rates) < 3:
        return M5ConfirmationResult(True, "error", "insufficient M5 candles, bypassing")

    c0 = rates[-1]  # current
    c1 = rates[-2]  # last closed
    c2 = rates[-3]  # prev closed

    if is_long:
        if _candle_is_bearish(c1) and _candle_is_bearish(c2):
            return M5ConfirmationResult(False, "blocked", "last 2 M5 candles are bearish (rejection)")
        
        if _candle_is_bullish(c1):
            return M5ConfirmationResult(True, "confirmed", "last closed M5 is bullish")
        
        # Current candle forming bullish body
        body = c0["close"] - c0["open"]
        full = c0["high"] - c0["low"]
        if full > 0 and (body / full) > 0.5:
             return M5ConfirmationResult(True, "confirmed", "current M5 forming bullish >50% body")
             
        # Strict mode check
        strict = getattr(config, "MT5_M5_CONFIRM_STRICT", False)
        if strict:
            return M5ConfirmationResult(False, "blocked", "M5 not explicitly bullish (strict mode)")

        return M5ConfirmationResult(True, "neutral", "M5 neutral, passed (non-strict)")

    else:
        # SHORT
        if _candle_is_bullish(c1) and _candle_is_bullish(c2):
            return M5ConfirmationResult(False, "blocked", "last 2 M5 candles are bullish (rejection)")
            
        if _candle_is_bearish(c1):
            return M5ConfirmationResult(True, "confirmed", "last closed M5 is bearish")
            
        # Current candle forming bearish body
        body = c0["open"] - c0["close"]
        full = c0["high"] - c0["low"]
        if full > 0 and (body / full) > 0.5:
             return M5ConfirmationResult(True, "confirmed", "current M5 forming bearish >50% body")

        strict = getattr(config, "MT5_M5_CONFIRM_STRICT", False)
        if strict:
            return M5ConfirmationResult(False, "blocked", "M5 not explicitly bearish (strict mode)")

        return M5ConfirmationResult(True, "neutral", "M5 neutral, passed (non-strict)")


def check_htf_ltf_convergence(
    mt5_bridge, broker_symbol: str, direction: str
) -> HTFLTFConvergenceResult:
    """
    Check H1 trend vs M5 microstructure convergence.
    - H1: last 2 closes trend
    - M5: current price vs M5 EMA20
    """
    is_enabled = getattr(config, "MT5_HTF_LTF_FILTER_ENABLED", False)
    if not is_enabled:
        return HTFLTFConvergenceResult(True, "disabled", "HTF/LTF filter disabled")

    family = _classify_symbol(broker_symbol)
    apply_to = str(getattr(config, "MT5_HTF_LTF_FILTER_APPLY_TO", "fx,gold"))
    if not _check_family_enabled(family, apply_to):
        return HTFLTFConvergenceResult(True, "ignored_family", f"HTF/LTF filter not applied to {family}")

    is_long = direction.lower() == "long"
    penalty = float(getattr(config, "MT5_HTF_LTF_SOFT_SIZE_PENALTY", 0.8))
    hard_block = bool(getattr(config, "MT5_HTF_LTF_HARD_BLOCK", False))

    res = HTFLTFConvergenceResult(True, "checking", "", 1.0)

    try:
        # H1 trend
        h1_rates = mt5_bridge.copy_rates_from_pos(broker_symbol, mt5_bridge.TIMEFRAME_H1, 0, 3)
        if h1_rates is not None and len(h1_rates) >= 3:
            h1c1 = h1_rates[-2]["close"]
            h1c2 = h1_rates[-3]["close"]
            if h1c1 > h1c2:
                res.h1_trend = "bullish"
            elif h1c1 < h1c2:
                res.h1_trend = "bearish"
                
        # M5 structure
        m5_rates = mt5_bridge.copy_rates_from_pos(broker_symbol, mt5_bridge.TIMEFRAME_M5, 0, 20)
        tick = mt5_bridge.symbol_info_tick(broker_symbol)
        
        if m5_rates is not None and len(m5_rates) >= 20 and tick is not None:
            closes = [r["close"] for r in m5_rates]
            ema20 = _ema(closes, 20)
            current_price = getattr(tick, "ask", closes[-1]) if is_long else getattr(tick, "bid", closes[-1])
            res.m5_struct = "bullish" if current_price > ema20 else "bearish"
            
    except Exception as e:
        logger.debug(f"[EntryConfirm] HTF/LTF API error: {e}")
        res.status = "error"
        res.reason = f"API error: {e}, bypassing"
        return res

    # Evaluate convergence
    if res.h1_trend == "neutral" or res.m5_struct == "neutral":
        res.status = "neutral"
        res.reason = "insufficient data to determine convergence"
        return res

    h1_aligns = (is_long and res.h1_trend == "bullish") or (not is_long and res.h1_trend == "bearish")
    m5_aligns = (is_long and res.m5_struct == "bullish") or (not is_long and res.m5_struct == "bearish")

    if h1_aligns and m5_aligns:
        res.status = "converged"
        res.reason = f"H1 {res.h1_trend} + M5 {res.m5_struct} fully aligned"
        res.size_mult = 1.0
        return res

    if not h1_aligns and not m5_aligns:
        # Fully counter-trend
        msg = f"H1 is {res.h1_trend}, M5 is {res.m5_struct} - fully against {direction} signal"
        if hard_block:
            res.ok = False
            res.status = "blocked"
            res.reason = msg
        else:
            res.status = "soft_warn"
            res.reason = msg
            res.size_mult = penalty
        return res

    # Partial alignment (1 out of 2)
    res.status = "soft_warn"
    res.reason = f"Partial alignment: H1 {res.h1_trend}, M5 {res.m5_struct}"
    res.size_mult = penalty
    return res
