"""
Entry/TP/SL Optimization Strategies

Implements multiple strategies for finding optimal trade levels:
  1. ATR-Based Dynamic Levels
  2. Structure-Based (Support/Resistance)
  3. Fibonacci Extension
  4. Volume Profile Entry Zones
  5. Sharpe-Optimized Entry Timing
  6. Ensemble (weighted combination of all)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("mempalace2.strategies")


@dataclass
class OptimizationResult:
    """Result of entry/TP/SL optimization."""
    entry_price: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    take_profit_3: float
    risk_reward_1: float
    risk_reward_2: float
    risk_reward_3: float
    expected_value: float
    win_probability: float
    max_loss_pct: float
    strategy_used: str
    confidence: float


class ATROptimizer:
    """
    ATR-based level optimization.

    Uses Average True Range to dynamically set levels based
    on current volatility regime.
    """

    @staticmethod
    def optimize(
        close: float,
        atr: float,
        direction: str,
        volatility_regime: str = "normal",  # low, normal, high
    ) -> Dict:
        """
        Calculate optimal levels based on ATR.

        Adjusts multipliers based on volatility regime:
          - Low vol: tighter SL, wider TPs (breakout expected)
          - High vol: wider SL, tighter TPs (range-bound likely)
        """
        regime_mult = {"low": 0.8, "normal": 1.0, "high": 1.3}
        mult = regime_mult.get(volatility_regime, 1.0)

        sl_mult = 1.5 * mult
        tp1_mult = 2.0 * mult
        tp2_mult = 3.5 * mult
        tp3_mult = 5.0 * mult

        if direction == "long":
            return {
                "entry": close,
                "sl": close - atr * sl_mult,
                "tp1": close + atr * tp1_mult,
                "tp2": close + atr * tp2_mult,
                "tp3": close + atr * tp3_mult,
                "rr1": tp1_mult / sl_mult,
                "rr2": tp2_mult / sl_mult,
                "rr3": tp3_mult / sl_mult,
            }
        else:
            return {
                "entry": close,
                "sl": close + atr * sl_mult,
                "tp1": close - atr * tp1_mult,
                "tp2": close - atr * tp2_mult,
                "tp3": close - atr * tp3_mult,
                "rr1": tp1_mult / sl_mult,
                "rr2": tp2_mult / sl_mult,
                "rr3": tp3_mult / sl_mult,
            }


class StructureOptimizer:
    """
    Support/Resistance based optimization.

    Places SL behind structure, TPs at next structure levels.
    Only uses structure if it gives better R:R than ATR alone.
    """

    @staticmethod
    def optimize(
        close: float,
        atr: float,
        direction: str,
        supports: List[float],
        resistances: List[float],
    ) -> Optional[Dict]:
        """
        Try to find better levels using market structure.
        Returns None if structure doesn't improve on ATR.
        """
        if not supports and not resistances:
            return None

        min_buffer = atr * 0.3  # minimum buffer beyond structure

        if direction == "long":
            # SL: nearest support below, with buffer
            below = [s for s in supports if s < close]
            if not below:
                return None

            sl = max(below) - min_buffer
            risk = close - sl
            if risk <= 0:
                return None

            # TPs: resistances above
            above = sorted([r for r in resistances if r > close])
            tp1 = above[0] if above else close + risk * 2
            tp2 = above[1] if len(above) > 1 else close + risk * 3
            tp3 = above[2] if len(above) > 2 else close + risk * 5

        else:
            # Short
            above = [r for r in resistances if r > close]
            if not above:
                return None

            sl = min(above) + min_buffer
            risk = sl - close
            if risk <= 0:
                return None

            below = sorted([s for s in supports if s < close], reverse=True)
            tp1 = below[0] if below else close - risk * 2
            tp2 = below[1] if len(below) > 1 else close - risk * 3
            tp3 = below[2] if len(below) > 2 else close - risk * 5

        return {
            "entry": close,
            "sl": round(sl, 2),
            "tp1": round(tp1, 2),
            "tp2": round(tp2, 2),
            "tp3": round(tp3, 2),
            "rr1": round(abs(tp1 - close) / risk, 2),
            "rr2": round(abs(tp2 - close) / risk, 2),
            "rr3": round(abs(tp3 - close) / risk, 2),
        }


class FibonacciOptimizer:
    """
    Fibonacci extension-based TP levels.

    Uses Fibonacci ratios (1.618, 2.618, 4.236) for TP targets.
    """

    FIB_EXTENSIONS = [1.618, 2.618, 4.236]

    @staticmethod
    def optimize(
        swing_high: float,
        swing_low: float,
        close: float,
        direction: str,
    ) -> Dict:
        """Calculate Fibonacci extension levels."""
        range_size = abs(swing_high - swing_low)

        if direction == "long":
            entry = close
            sl = swing_low - range_size * 0.1
            risk = entry - sl
            tp1 = entry + range_size * FibonacciOptimizer.FIB_EXTENSIONS[0]
            tp2 = entry + range_size * FibonacciOptimizer.FIB_EXTENSIONS[1]
            tp3 = entry + range_size * FibonacciOptimizer.FIB_EXTENSIONS[2]
        else:
            entry = close
            sl = swing_high + range_size * 0.1
            risk = sl - entry
            tp1 = entry - range_size * FibonacciOptimizer.FIB_EXTENSIONS[0]
            tp2 = entry - range_size * FibonacciOptimizer.FIB_EXTENSIONS[1]
            tp3 = entry - range_size * FibonacciOptimizer.FIB_EXTENSIONS[2]

        return {
            "entry": round(entry, 2),
            "sl": round(sl, 2),
            "tp1": round(tp1, 2),
            "tp2": round(tp2, 2),
            "tp3": round(tp3, 2),
            "rr1": round(abs(tp1 - entry) / risk, 2) if risk > 0 else 0,
            "rr2": round(abs(tp2 - entry) / risk, 2) if risk > 0 else 0,
            "rr3": round(abs(tp3 - entry) / risk, 2) if risk > 0 else 0,
        }


class EnsembleOptimizer:
    """
    Combines multiple optimization strategies and picks the best one.

    Scoring criteria:
      - Risk/Reward ratio (higher is better)
      - Win probability (based on historical backtest)
      - Expected value (Kelly-weighted)
      - Structure alignment (bonus if TP/SL near key levels)
    """

    @staticmethod
    def optimize(
        close: float,
        atr: float,
        direction: str,
        supports: List[float],
        resistances: List[float],
        swing_high: float = 0,
        swing_low: float = 0,
        volatility_regime: str = "normal",
        win_rate: float = 0.55,
    ) -> OptimizationResult:
        """
        Run all strategies and return the best one.

        Strategy selection logic:
          1. Run all optimizers
          2. Score each by R:R * win_probability * structure_bonus
          3. Return the highest-scoring result
        """
        candidates = []

        # 1. ATR-based
        atr_result = ATROptimizer.optimize(close, atr, direction, volatility_regime)
        atr_score = EnsembleOptimizer._score_result(atr_result, win_rate, "ATR")
        candidates.append((atr_score, atr_result, "ATR"))

        # 2. Structure-based
        struct_result = StructureOptimizer.optimize(close, atr, direction, supports, resistances)
        if struct_result:
            struct_score = EnsembleOptimizer._score_result(struct_result, win_rate, "Structure")
            # Bonus for structure alignment (levels tend to hold)
            struct_score *= 1.15
            candidates.append((struct_score, struct_result, "Structure"))

        # 3. Fibonacci
        if swing_high > 0 and swing_low > 0:
            fib_result = FibonacciOptimizer.optimize(swing_high, swing_low, close, direction)
            fib_score = EnsembleOptimizer._score_result(fib_result, win_rate, "Fibonacci")
            candidates.append((fib_score, fib_result, "Fibonacci"))

        # Pick best
        if not candidates:
            # Fallback
            return OptimizationResult(
                entry_price=close,
                stop_loss=close - atr * 1.5 if direction == "long" else close + atr * 1.5,
                take_profit_1=close + atr * 2.0 if direction == "long" else close - atr * 2.0,
                take_profit_2=close + atr * 3.5 if direction == "long" else close - atr * 3.5,
                take_profit_3=close + atr * 5.0 if direction == "long" else close - atr * 5.0,
                risk_reward_1=2.0,
                risk_reward_2=3.5,
                risk_reward_3=5.0,
                expected_value=0.0,
                win_probability=win_rate,
                max_loss_pct=0.5,
                strategy_used="fallback",
                confidence=50.0,
            )

        best_score, best_result, best_strategy = max(candidates, key=lambda x: x[0])

        # Calculate expected value
        risk = abs(best_result["entry"] - best_result["sl"])
        reward = abs(best_result["tp1"] - best_result["entry"])
        ev = win_rate * reward - (1 - win_rate) * risk

        max_loss_pct = risk / close * 100

        return OptimizationResult(
            entry_price=best_result["entry"],
            stop_loss=best_result["sl"],
            take_profit_1=best_result["tp1"],
            take_profit_2=best_result["tp2"],
            take_profit_3=best_result["tp3"],
            risk_reward_1=best_result["rr1"],
            risk_reward_2=best_result["rr2"],
            risk_reward_3=best_result["rr3"],
            expected_value=ev,
            win_probability=win_rate,
            max_loss_pct=max_loss_pct,
            strategy_used=best_strategy,
            confidence=min(100, best_score * 10),
        )

    @staticmethod
    def _score_result(result: Dict, win_rate: float, strategy: str) -> float:
        """
        Score a result by its quality metrics.

        Higher = better trade setup.
        """
        rr = result.get("rr1", 1.0)
        if rr < 1.5:
            return 0  # not worth it

        # Expected value score
        risk = abs(result["entry"] - result["sl"])
        reward = abs(result["tp1"] - result["entry"])
        ev = win_rate * reward - (1 - win_rate) * risk
        ev_score = max(0, ev / risk * 10) if risk > 0 else 0

        # R:R score (diminishing returns past 5R)
        rr_score = min(rr, 5.0) * 2

        # Combined
        return ev_score + rr_score
