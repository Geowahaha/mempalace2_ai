from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, List, Optional

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


class TradeScore(IntEnum):
    LOSS = -1
    NEUTRAL = 0
    WIN = 1


def evaluate_outcome(
    pnl: float,
    *,
    notional: float,
    neutral_rel_threshold: float,
) -> TradeScore:
    """Map realized PnL to reinforcement-style score using relative return vs notional."""
    if notional <= 0.0:
        return TradeScore.NEUTRAL
    rel = pnl / notional
    if rel > neutral_rel_threshold:
        return TradeScore.WIN
    if rel < -neutral_rel_threshold:
        return TradeScore.LOSS
    return TradeScore.NEUTRAL


@dataclass(slots=True)
class RiskManager:
    max_trades_per_session: int
    max_consecutive_losses: int
    neutral_rel_threshold: float

    trades_executed: int = 0
    consecutive_losses: int = 0
    halted: bool = False
    halt_reason: Optional[str] = None
    recent_scores: List[TradeScore] = field(default_factory=list)

    def can_trade(self) -> bool:
        if self.halted:
            return False
        if self.trades_executed >= self.max_trades_per_session:
            self._halt("max_trades_per_session reached")
            return False
        if self.consecutive_losses >= self.max_consecutive_losses:
            self._halt("max_consecutive_losses reached")
            return False
        return True

    def _halt(self, reason: str) -> None:
        if not self.halted:
            self.halted = True
            self.halt_reason = reason
            log.error("Risk halt: %s", reason)

    def on_trade_result(self, score: TradeScore, *, pnl: Optional[float] = None) -> None:
        """Update counters after a closed trade is scored."""
        self.trades_executed += 1
        self.recent_scores.append(score)
        if score == TradeScore.LOSS:
            self.consecutive_losses += 1
        elif score == TradeScore.WIN:
            self.consecutive_losses = 0
        # NEUTRAL does not extend loss streak
        log.info(
            "Risk stats: trades=%s consecutive_losses=%s last_score=%s pnl=%s",
            self.trades_executed,
            self.consecutive_losses,
            score,
            pnl,
        )

    def reset_session(self) -> None:
        self.trades_executed = 0
        self.consecutive_losses = 0
        self.halted = False
        self.halt_reason = None
        self.recent_scores.clear()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "trades_executed": int(self.trades_executed),
            "consecutive_losses": int(self.consecutive_losses),
            "halted": bool(self.halted),
            "halt_reason": self.halt_reason,
            "recent_scores": [int(s) for s in self.recent_scores],
        }

    def restore(self, payload: Dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        self.trades_executed = int(payload.get("trades_executed") or 0)
        self.consecutive_losses = int(payload.get("consecutive_losses") or 0)
        self.halted = bool(payload.get("halted", False))
        self.halt_reason = str(payload.get("halt_reason") or "") or None
        self.recent_scores = []
        for raw in list(payload.get("recent_scores") or []):
            try:
                self.recent_scores.append(TradeScore(int(raw)))
            except (TypeError, ValueError):
                continue
