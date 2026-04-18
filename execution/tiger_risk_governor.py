"""
execution/tiger_risk_governor.py - Tiger Risk Governor
Phase-based risk management for the $15 → $1M growth path.

Dynamically adjusts:
1. Risk % per trade based on account equity phase
2. Max positions based on account size
3. Daily loss circuit breaker per phase
4. Lot sizing with compounding acceleration
"""
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RiskPhase:
    """Defines risk parameters for a specific account equity phase."""
    name: str
    min_equity: float       # minimum equity for this phase
    max_equity: float       # maximum equity (exclusive)
    risk_pct: float         # % of equity to risk per trade
    max_positions: int      # max open positions allowed
    daily_loss_pct: float   # max daily loss % before circuit breaker
    lot_floor: float        # minimum lot size (broker minimum)
    lot_cap: float          # maximum lot size per trade
    description: str        # human readable


# Tiger growth phases: ultra-conservative → conservative → normal → aggressive
TIGER_PHASES = [
    RiskPhase(
        name="seedling",
        min_equity=0.0,
        max_equity=50.0,
        risk_pct=0.5,          # Risk only 0.5% per trade ($15 = $0.075 risk)
        max_positions=2,
        daily_loss_pct=3.0,    # Stop after 3% daily loss
        lot_floor=0.01,
        lot_cap=0.02,
        description="$0-$50: Seedling phase — protect the seed capital"
    ),
    RiskPhase(
        name="sprout",
        min_equity=50.0,
        max_equity=200.0,
        risk_pct=0.75,
        max_positions=3,
        daily_loss_pct=3.5,
        lot_floor=0.01,
        lot_cap=0.05,
        description="$50-$200: Sprout phase — cautious growth"
    ),
    RiskPhase(
        name="sapling",
        min_equity=200.0,
        max_equity=1000.0,
        risk_pct=1.0,
        max_positions=4,
        daily_loss_pct=4.0,
        lot_floor=0.01,
        lot_cap=0.10,
        description="$200-$1K: Sapling phase — normal risk"
    ),
    RiskPhase(
        name="tree",
        min_equity=1000.0,
        max_equity=5000.0,
        risk_pct=1.25,
        max_positions=5,
        daily_loss_pct=4.0,
        lot_floor=0.01,
        lot_cap=0.50,
        description="$1K-$5K: Tree phase — compound acceleration"
    ),
    RiskPhase(
        name="forest",
        min_equity=5000.0,
        max_equity=50000.0,
        risk_pct=1.5,
        max_positions=6,
        daily_loss_pct=3.5,
        lot_floor=0.05,
        lot_cap=2.00,
        description="$5K-$50K: Forest phase — strong growth"
    ),
    RiskPhase(
        name="titan",
        min_equity=50000.0,
        max_equity=float("inf"),
        risk_pct=1.0,          # Reduce risk % as account grows large
        max_positions=8,
        daily_loss_pct=3.0,
        lot_floor=0.10,
        lot_cap=5.00,
        description="$50K+: Titan phase — protect the empire"
    ),
]


class TigerRiskGovernor:
    """
    Tiger Risk Governor: Phase-based risk management engine.
    
    Usage:
        governor = TigerRiskGovernor()
        phase = governor.get_phase(equity=15.0)
        lot_size = governor.calculate_lot_size(
            equity=15.0,
            risk_distance_pips=50,
            pip_value=0.10,   # per mini lot
        )
    """

    def __init__(self):
        self._daily_pnl: float = 0.0      # accumulated daily P&L in $
        self._daily_start_equity: float = 0.0
        self._trades_today: int = 0
        self._circuit_breaker_active: bool = False

    def get_phase(self, equity: float) -> RiskPhase:
        """Get the current risk phase for the given equity."""
        equity = max(0.0, float(equity))
        for phase in TIGER_PHASES:
            if phase.min_equity <= equity < phase.max_equity:
                return phase
        return TIGER_PHASES[-1]  # titan phase fallback

    def calculate_lot_size(
        self,
        equity: float,
        risk_distance_pips: float,
        pip_value: float = 0.10,
        *,
        confidence: float = 75.0,
        sl_liquidity_mapped: bool = False,
    ) -> tuple[float, dict]:
        """
        Calculate the optimal lot size for a trade.
        
        Args:
            equity: Current account equity in USD
            risk_distance_pips: Distance from entry to SL in pips
            pip_value: Dollar value per pip per standard mini lot (0.10)
            confidence: Signal confidence (0-100)
            sl_liquidity_mapped: True if SL uses anti-sweep placement
            
        Returns:
            (lot_size, metadata_dict)
        """
        phase = self.get_phase(equity)
        
        meta = {
            "phase": phase.name,
            "equity": round(equity, 2),
            "risk_pct": phase.risk_pct,
            "phase_description": phase.description,
        }

        if self._circuit_breaker_active:
            meta["circuit_breaker"] = True
            meta["reason"] = "daily loss circuit breaker active"
            return phase.lot_floor, meta

        # Calculate risk amount in $
        risk_amount = equity * (phase.risk_pct / 100.0)
        
        # Confidence adjustment: scale risk with confidence
        # 60% conf = 0.75x risk, 80% conf = 1.0x, 90%+ = 1.1x
        if confidence >= 90:
            conf_mult = 1.10
        elif confidence >= 80:
            conf_mult = 1.0
        elif confidence >= 70:
            conf_mult = 0.90
        else:
            conf_mult = 0.75
        
        # Anti-sweep bonus: if SL is behind liquidity, slightly larger size
        # because the SL is better protected
        liq_mult = 1.05 if sl_liquidity_mapped else 1.0
        
        risk_amount *= conf_mult * liq_mult
        
        # Calculate lot size: risk_amount / (risk_pips * pip_value_per_lot)
        if risk_distance_pips <= 0 or pip_value <= 0:
            meta["reason"] = "invalid risk distance or pip value"
            return phase.lot_floor, meta
        
        dollar_risk_per_lot = risk_distance_pips * pip_value
        lot_size = risk_amount / dollar_risk_per_lot
        
        # Clamp to phase limits
        lot_size = max(phase.lot_floor, min(phase.lot_cap, lot_size))
        
        # Round to valid lot step (typically 0.01)
        lot_size = round(lot_size, 2)
        lot_size = max(phase.lot_floor, lot_size)
        
        meta.update({
            "risk_amount": round(risk_amount, 4),
            "conf_mult": conf_mult,
            "liq_mult": liq_mult,
            "risk_pips": round(risk_distance_pips, 2),
            "lot_calculated": lot_size,
            "lot_floor": phase.lot_floor,
            "lot_cap": phase.lot_cap,
        })
        
        return lot_size, meta

    def check_circuit_breaker(self, equity: float, daily_pnl: float) -> tuple[bool, str]:
        """
        Check if daily loss circuit breaker should activate.
        
        Returns (is_ok, reason).
        """
        phase = self.get_phase(equity)
        start_equity = self._daily_start_equity if self._daily_start_equity > 0 else equity
        
        if start_equity <= 0:
            return True, "ok"
        
        daily_loss_pct = abs(min(0, daily_pnl)) / start_equity * 100.0
        
        if daily_loss_pct >= phase.daily_loss_pct:
            self._circuit_breaker_active = True
            reason = (
                f"CIRCUIT BREAKER: {daily_loss_pct:.1f}% daily loss >= "
                f"{phase.daily_loss_pct:.1f}% limit ({phase.name} phase)"
            )
            logger.warning("[TigerRisk] %s", reason)
            return False, reason
        
        return True, "ok"

    def check_position_limit(self, equity: float, current_positions: int) -> tuple[bool, str]:
        """
        Check if we can open another position.
        
        Returns (is_ok, reason).
        """
        phase = self.get_phase(equity)
        
        if current_positions >= phase.max_positions:
            reason = (
                f"Position limit: {current_positions}/{phase.max_positions} "
                f"({phase.name} phase)"
            )
            return False, reason
        
        return True, "ok"

    def reset_daily(self, current_equity: float):
        """Reset daily tracking (call at start of new trading day)."""
        self._daily_pnl = 0.0
        self._daily_start_equity = float(current_equity)
        self._trades_today = 0
        self._circuit_breaker_active = False
        logger.info(
            "[TigerRisk] Daily reset: equity=$%.2f, phase=%s",
            current_equity,
            self.get_phase(current_equity).name,
        )

    def record_trade_pnl(self, pnl: float):
        """Record a closed trade's P&L for daily tracking."""
        self._daily_pnl += float(pnl)
        self._trades_today += 1

    def status(self, equity: float) -> dict:
        """Get current risk governor status."""
        phase = self.get_phase(equity)
        return {
            "phase": phase.name,
            "phase_description": phase.description,
            "equity": round(equity, 2),
            "risk_pct": phase.risk_pct,
            "max_positions": phase.max_positions,
            "daily_loss_limit_pct": phase.daily_loss_pct,
            "lot_floor": phase.lot_floor,
            "lot_cap": phase.lot_cap,
            "daily_pnl": round(self._daily_pnl, 2),
            "trades_today": self._trades_today,
            "circuit_breaker_active": self._circuit_breaker_active,
        }


# Singleton
tiger_risk_governor = TigerRiskGovernor()
