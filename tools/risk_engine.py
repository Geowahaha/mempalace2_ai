"""
Risk Engine Tool — Core risk management for position sizing and trade validation.

Implements:
  - Kelly Criterion for optimal position sizing
  - ATR-based dynamic Stop Loss / Take Profit
  - Portfolio heat calculation
  - Risk/Reward ratio enforcement
  - Sharpe-adjusted entry optimization
  - Correlation risk management
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np

from core.state import TradeSignal, PortfolioState
from tools.base import Tool, ToolCategory, ToolResult

logger = logging.getLogger("mempalace2.tools.risk")


@dataclass
class RiskAssessment:
    """Complete risk assessment for a potential trade."""
    approved: bool = False
    rejection_reason: str = ""

    # Position sizing
    kelly_pct: float = 0.0
    position_size_pct: float = 0.0
    position_value: float = 0.0
    quantity: float = 0.0

    # Risk metrics
    risk_per_trade_pct: float = 0.0
    risk_reward_ratio: float = 0.0
    expected_value: float = 0.0
    sharpe_entry_score: float = 0.0

    # Portfolio impact
    portfolio_heat_after: float = 0.0
    correlated_exposure: float = 0.0
    margin_required: float = 0.0


class RiskEngineTool(Tool):
    """
    Validates and sizes trades for optimal risk management.

    Parameters:
      signal: TradeSignal with entry, SL, TP levels
      portfolio: Current portfolio state
      win_rate: Historical win rate (0-1)
      avg_win: Average winning trade (%)
      avg_loss: Average losing trade (%)
    """

    name = "risk_engine"
    category = ToolCategory.RISK
    description = "Risk assessment and position sizing"
    is_read_only = True
    is_safe = True

    def __init__(self, config=None):
        self.config = config

    def validate_input(self, signal=None, portfolio=None, **kwargs) -> Optional[str]:
        if signal is None:
            return "signal (TradeSignal) is required"
        if signal.entry_price <= 0:
            return "signal.entry_price must be positive"
        if signal.stop_loss <= 0:
            return "signal.stop_loss must be positive"
        if signal.take_profit_1 <= 0:
            return "signal.take_profit_1 must be positive"
        return None

    async def execute(
        self,
        signal: TradeSignal = None,
        portfolio: PortfolioState = None,
        win_rate: float = 0.55,
        avg_win: float = 2.5,
        avg_loss: float = 1.2,
        **kwargs,
    ) -> ToolResult:
        """Run full risk assessment on a trade signal."""
        if portfolio is None:
            from core.state import get_state
            portfolio = get_state().portfolio

        config = self.config
        assessment = RiskAssessment()

        # 1. Calculate Risk/Reward ratio
        risk = abs(signal.entry_price - signal.stop_loss)
        reward = abs(signal.take_profit_1 - signal.entry_price)
        if risk <= 0:
            assessment.rejection_reason = "Stop loss equals entry price"
            return ToolResult.ok(assessment=assessment)

        assessment.risk_reward_ratio = reward / risk

        # 2. Check minimum R:R
        min_rr = config.min_risk_reward if config else 2.0
        if assessment.risk_reward_ratio < min_rr:
            assessment.rejection_reason = (
                f"R:R {assessment.risk_reward_ratio:.2f} < minimum {min_rr}"
            )
            return ToolResult.ok(assessment=assessment)

        # 3. Kelly Criterion position sizing
        assessment.kelly_pct = self._kelly_criterion(
            win_rate, assessment.risk_reward_ratio, config
        )

        # 4. Apply position size limits
        max_pos_pct = config.max_position_pct if config else 5.0
        assessment.position_size_pct = min(assessment.kelly_pct, max_pos_pct)

        # 5. Calculate position value
        assessment.position_value = portfolio.total_equity * (assessment.position_size_pct / 100)
        assessment.quantity = assessment.position_value / signal.entry_price

        # 6. Risk per trade
        assessment.risk_per_trade_pct = (risk / signal.entry_price) * assessment.position_size_pct

        # 7. Expected value (Kelly-weighted)
        assessment.expected_value = (
            win_rate * (reward / signal.entry_price) * assessment.position_size_pct
            - (1 - win_rate) * (risk / signal.entry_price) * assessment.position_size_pct
        )

        # 8. Sharpe-adjusted entry score
        assessment.sharpe_entry_score = self._sharpe_entry_score(signal)

        # 9. Portfolio heat check
        max_heat = config.max_portfolio_risk_pct if config else 6.0
        assessment.portfolio_heat_after = portfolio.total_risk_pct + assessment.risk_per_trade_pct

        if assessment.portfolio_heat_after > max_heat:
            assessment.rejection_reason = (
                f"Portfolio heat {assessment.portfolio_heat_after:.1f}% "
                f"exceeds max {max_heat}%"
            )
            return ToolResult.ok(assessment=assessment)

        # 10. Max open trades check
        max_trades = config.max_open_trades if config else 5
        if portfolio.open_positions >= max_trades:
            assessment.rejection_reason = (
                f"Max open trades ({max_trades}) reached"
            )
            return ToolResult.ok(assessment=assessment)

        # 11. Daily loss circuit breaker
        max_daily = config.max_daily_loss_pct if config else 3.0
        daily_loss_pct = abs(min(0, portfolio.daily_pnl)) / portfolio.total_equity * 100
        if daily_loss_pct >= max_daily:
            assessment.rejection_reason = f"Daily loss limit reached ({daily_loss_pct:.1f}%)"
            return ToolResult.ok(assessment=assessment)

        # ✅ All checks passed
        assessment.approved = True
        logger.info(
            f"Risk APPROVED: {signal.symbol} {signal.direction} "
            f"R:R={assessment.risk_reward_ratio:.2f} "
            f"Size={assessment.position_size_pct:.1f}% "
            f"EV={assessment.expected_value:.3f}%"
        )

        return ToolResult.ok(assessment=assessment)

    def _kelly_criterion(
        self, win_rate: float, risk_reward: float, config=None
    ) -> float:
        """
        Kelly Criterion: f* = (p * b - q) / b
        Where:
          p = probability of winning
          b = ratio of win to loss (risk/reward)
          q = 1 - p
        """
        p = win_rate
        q = 1 - p
        b = risk_reward

        kelly = (p * b - q) / b if b > 0 else 0
        kelly = max(0, kelly)  # never negative

        # Apply fractional Kelly (conservative)
        fraction = config.kelly_fraction if config else 0.25
        return kelly * fraction * 100  # as percentage

    def _sharpe_entry_score(self, signal: TradeSignal) -> float:
        """
        Score how good the entry is relative to recent volatility.
        Higher score = better risk-adjusted entry.
        """
        if signal.atr <= 0:
            return 50.0  # neutral if no ATR data

        # Distance from entry to key levels (in ATR units)
        entry = signal.entry_price

        # How close is entry to support (for longs) or resistance (for shorts)?
        if signal.direction == "long":
            support_dist = abs(entry - signal.support_level) / signal.atr if signal.support_level > 0 else 2.0
            # Closer to support = better entry
            support_score = max(0, 100 - support_dist * 30)
            # Below EMA = better entry for trend
            trend_bonus = 20 if signal.trend_strength > 0 else 0
        else:
            support_dist = abs(entry - signal.resistance_level) / signal.atr if signal.resistance_level > 0 else 2.0
            support_score = max(0, 100 - support_dist * 30)
            trend_bonus = 20 if signal.trend_strength < 0 else 0

        return min(100, support_score + trend_bonus)

    def calculate_optimal_tpsl(
        self,
        entry_price: float,
        direction: str,
        atr: float,
        supports: List[float],
        resistances: List[float],
        config=None,
    ) -> Dict:
        """
        Calculate optimal TP/SL levels based on ATR and market structure.

        Returns dict with:
          stop_loss: Optimal SL price
          take_profit_1: Conservative TP
          take_profit_2: Moderate TP
          take_profit_3: Aggressive TP
          risk_reward_1/2/3: R:R for each TP
        """
        atr_mult_sl = config.atr_multiplier_sl if config else 1.5
        atr_mult_tp = config.atr_multiplier_tp if config else 3.0

        if direction == "long":
            # SL: ATR-based below entry, respecting nearest support
            sl_atr = entry_price - atr * atr_mult_sl
            sl_support = max([s for s in supports if s < entry_price], default=sl_atr)
            stop_loss = max(sl_atr, sl_support - atr * 0.3)  # just below support

            # TPs: ATR multiples + next resistance levels
            tp1_atr = entry_price + atr * atr_mult_tp
            tp2_atr = entry_price + atr * atr_mult_tp * 2
            tp3_atr = entry_price + atr * atr_mult_tp * 3

            above = sorted([r for r in resistances if r > entry_price])
            tp1 = min(above[0], tp1_atr) if above else tp1_atr
            tp2 = min(above[1], tp2_atr) if len(above) > 1 else tp2_atr
            tp3 = tp3_atr
        else:
            # Short: inverted logic
            sl_atr = entry_price + atr * atr_mult_sl
            sl_resistance = min([r for r in resistances if r > entry_price], default=sl_atr)
            stop_loss = min(sl_atr, sl_resistance + atr * 0.3)

            tp1_atr = entry_price - atr * atr_mult_tp
            tp2_atr = entry_price - atr * atr_mult_tp * 2
            tp3_atr = entry_price - atr * atr_mult_tp * 3

            below = sorted([s for s in supports if s < entry_price], reverse=True)
            tp1 = max(below[0], tp1_atr) if below else tp1_atr
            tp2 = max(below[1], tp2_atr) if len(below) > 1 else tp2_atr
            tp3 = tp3_atr

        risk = abs(entry_price - stop_loss)

        return {
            "stop_loss": round(stop_loss, 2),
            "take_profit_1": round(tp1, 2),
            "take_profit_2": round(tp2, 2),
            "take_profit_3": round(tp3, 2),
            "risk_reward_1": round(abs(tp1 - entry_price) / risk, 2) if risk > 0 else 0,
            "risk_reward_2": round(abs(tp2 - entry_price) / risk, 2) if risk > 0 else 0,
            "risk_reward_3": round(abs(tp3 - entry_price) / risk, 2) if risk > 0 else 0,
        }
