from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


@dataclass(slots=True)
class PerformanceTracker:
    """Rolling stats from closed trades (PnL in price space × volume — same unit as loop)."""

    pnls: List[float] = field(default_factory=list)
    equity: List[float] = field(default_factory=list)
    wins: int = 0
    losses: int = 0
    neutrals: int = 0

    def __post_init__(self) -> None:
        if not self.equity:
            self.equity = [0.0]

    def record_close(self, pnl: float, *, score: int) -> None:
        self.pnls.append(float(pnl))
        last = self.equity[-1]
        nxt = last + float(pnl)
        self.equity.append(nxt)
        if score > 0:
            self.wins += 1
        elif score < 0:
            self.losses += 1
        else:
            self.neutrals += 1
        log.info(
            "Performance: equity=%.5f win_rate=%.1f%% trades=%s",
            nxt,
            self.win_rate * 100.0,
            len(self.pnls),
        )

    @property
    def closed_count(self) -> int:
        return len(self.pnls)

    @property
    def win_rate(self) -> float:
        n = self.wins + self.losses
        if n <= 0:
            return 0.0
        return self.wins / n

    @property
    def avg_profit(self) -> float:
        if not self.pnls:
            return 0.0
        return sum(self.pnls) / len(self.pnls)

    @property
    def max_drawdown(self) -> float:
        """Largest peak-to-trough drop in cumulative equity (positive magnitude)."""
        curve = self.equity
        if len(curve) < 2:
            return 0.0
        peak = curve[0]
        max_dd = 0.0
        for x in curve:
            if x > peak:
                peak = x
            dd = peak - x
            if dd > max_dd:
                max_dd = dd
        return max_dd

    def summary(self) -> dict:
        return {
            "closed_trades": self.closed_count,
            "win_rate": round(self.win_rate, 4),
            "avg_profit": round(self.avg_profit, 6),
            "max_drawdown": round(self.max_drawdown, 6),
            "wins": self.wins,
            "losses": self.losses,
            "neutrals": self.neutrals,
            "equity_last": round(self.equity[-1], 6) if self.equity else 0.0,
        }
