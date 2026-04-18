"""
backtest/virtual_executor.py — Bar-by-bar TP/SL resolution for backtesting.
No broker connection needed. Reuses _check_hit logic from SignalSimulator.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import pandas as pd

from backtest.candle_store import CandleStore

logger = logging.getLogger(__name__)

# Default resolution timeframe — use M5 bars to check TP/SL
RESOLUTION_TF = "5m"
# Max bars to walk forward before marking trade as expired
DEFAULT_MAX_BARS = 500  # ~42 hours of M5


class TradeResult:
    """Result of a resolved backtest trade."""
    __slots__ = (
        "signal", "outcome", "entry_price", "exit_price", "exit_time",
        "pnl_pips", "pnl_r", "bars_held", "direction", "symbol",
        "entry_time", "entry_type",
    )

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def to_dict(self) -> dict:
        return {s: getattr(self, s, None) for s in self.__slots__}


class VirtualExecutor:
    """Resolves trade signals against historical candle data."""

    def __init__(self, store: CandleStore):
        self.store = store
        self.results: List[TradeResult] = []

    def resolve_trade(
        self,
        signal: dict,
        signal_time: datetime,
        max_bars: int = DEFAULT_MAX_BARS,
        resolution_tf: str = RESOLUTION_TF,
    ) -> Optional[TradeResult]:
        """Walk forward bar-by-bar from signal_time, resolve TP/SL.

        Args:
            signal: dict with keys: direction, entry, stop_loss, tp1, tp2, tp3,
                    symbol, entry_type, source
            signal_time: when the signal was generated
            max_bars: max bars to walk before declaring expired
            resolution_tf: timeframe to use for resolution (default 5m)

        Returns:
            TradeResult or None if no data available
        """
        direction = str(signal.get("direction", "")).lower()
        entry = float(signal.get("entry", 0))
        sl = float(signal.get("stop_loss", 0))
        tp1 = float(signal.get("tp1", 0))
        tp2 = float(signal.get("tp2", 0) or 0)
        tp3 = float(signal.get("tp3", 0) or 0)
        symbol = str(signal.get("symbol", "XAUUSD")).upper()
        entry_type = str(signal.get("entry_type", "limit")).lower()

        if entry <= 0 or sl <= 0 or tp1 <= 0:
            logger.debug("[VirtualExec] Invalid signal levels: entry=%s sl=%s tp1=%s", entry, sl, tp1)
            return None

        # Fetch bars starting after signal_time
        end_window = signal_time + timedelta(minutes=max_bars * 5)
        df = self.store.fetch(symbol, resolution_tf, start=signal_time, end=end_window)
        if df is None or df.empty:
            return None

        # Phase 1: For stop/limit entries, check if entry is triggered
        entry_triggered = False
        entry_bar_idx = 0

        if entry_type == "market":
            entry_triggered = True
            entry_bar_idx = 0
        else:
            for i, (ts, bar) in enumerate(df.iterrows()):
                high = float(bar["high"])
                low = float(bar["low"])
                if entry_type == "buy_stop" and direction == "long":
                    if high >= entry:
                        entry_triggered = True
                        entry_bar_idx = i
                        break
                elif entry_type == "sell_stop" and direction == "short":
                    if low <= entry:
                        entry_triggered = True
                        entry_bar_idx = i
                        break
                elif entry_type == "limit":
                    if direction == "long" and low <= entry:
                        entry_triggered = True
                        entry_bar_idx = i
                        break
                    elif direction == "short" and high >= entry:
                        entry_triggered = True
                        entry_bar_idx = i
                        break

        if not entry_triggered:
            result = TradeResult(
                signal=signal, outcome="expired_no_fill",
                entry_price=entry, exit_price=0.0,
                exit_time=df.index[-1] if len(df) > 0 else signal_time,
                pnl_pips=0.0, pnl_r=0.0,
                bars_held=len(df), direction=direction,
                symbol=symbol, entry_time=signal_time,
                entry_type=entry_type,
            )
            self.results.append(result)
            return result

        # Phase 2: Walk bars from entry to find TP/SL hit
        entry_time = df.index[entry_bar_idx]
        resolution_bars = df.iloc[entry_bar_idx:]

        for i, (ts, bar) in enumerate(resolution_bars.iterrows()):
            high = float(bar["high"])
            low = float(bar["low"])

            # Check SL first (conservative — SL checked before TP on same bar)
            if direction == "long":
                if sl > 0 and low <= sl:
                    outcome = "sl_hit"
                    exit_price = sl
                elif tp3 > 0 and high >= tp3:
                    outcome = "tp3_hit"
                    exit_price = tp3
                elif tp2 > 0 and high >= tp2:
                    outcome = "tp2_hit"
                    exit_price = tp2
                elif tp1 > 0 and high >= tp1:
                    outcome = "tp1_hit"
                    exit_price = tp1
                else:
                    continue
            elif direction == "short":
                if sl > 0 and high >= sl:
                    outcome = "sl_hit"
                    exit_price = sl
                elif tp3 > 0 and low <= tp3:
                    outcome = "tp3_hit"
                    exit_price = tp3
                elif tp2 > 0 and low <= tp2:
                    outcome = "tp2_hit"
                    exit_price = tp2
                elif tp1 > 0 and low <= tp1:
                    outcome = "tp1_hit"
                    exit_price = tp1
                else:
                    continue
            else:
                continue

            pnl_pips = self._calc_pips(direction, entry, exit_price, symbol)
            pnl_r = self._calc_r_multiple(direction, entry, exit_price, sl)

            result = TradeResult(
                signal=signal, outcome=outcome,
                entry_price=entry, exit_price=exit_price,
                exit_time=ts, pnl_pips=pnl_pips, pnl_r=pnl_r,
                bars_held=i, direction=direction,
                symbol=symbol, entry_time=entry_time,
                entry_type=entry_type,
            )
            self.results.append(result)
            return result

        # Expired — max bars reached without TP/SL
        last_close = float(resolution_bars.iloc[-1]["close"]) if len(resolution_bars) > 0 else entry
        pnl_pips = self._calc_pips(direction, entry, last_close, symbol)
        pnl_r = self._calc_r_multiple(direction, entry, last_close, sl)

        result = TradeResult(
            signal=signal, outcome="expired",
            entry_price=entry, exit_price=last_close,
            exit_time=resolution_bars.index[-1] if len(resolution_bars) > 0 else signal_time,
            pnl_pips=pnl_pips, pnl_r=pnl_r,
            bars_held=len(resolution_bars), direction=direction,
            symbol=symbol, entry_time=entry_time,
            entry_type=entry_type,
        )
        self.results.append(result)
        return result

    # ── PnL helpers (from SignalSimulator) ──────────────────────────────────

    @staticmethod
    def _calc_pips(direction: str, entry: float, exit_price: float, symbol: str) -> float:
        """Calculate pips. For XAUUSD 1 pip = $0.01."""
        diff = exit_price - entry if direction == "long" else entry - exit_price
        sym = str(symbol or "").upper()
        if "XAU" in sym or "GOLD" in sym:
            return round(diff / 0.01, 1)
        return round(diff, 4)

    @staticmethod
    def _calc_r_multiple(direction: str, entry: float, exit_price: float, stop_loss: float) -> float:
        """Calculate R-multiple, bounded to [-3, 6]."""
        try:
            risk = abs(entry - stop_loss)
            if risk <= 1e-12:
                return 0.0
            pnl = (exit_price - entry) if direction == "long" else (entry - exit_price)
            r = pnl / risk
            return max(-3.0, min(6.0, float(r)))
        except Exception:
            return 0.0
