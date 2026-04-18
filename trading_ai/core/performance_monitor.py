from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, TYPE_CHECKING

from trading_ai.utils.logger import get_logger

if TYPE_CHECKING:
    from trading_ai.core.agent import Decision
    from trading_ai.core.strategy_evolution import StrategyRegistry

log = get_logger(__name__)


def _max_drawdown(equity: List[float]) -> float:
    if len(equity) < 2:
        return 0.0
    peak = equity[0]
    max_dd = 0.0
    for x in equity:
        if x > peak:
            peak = x
        dd = peak - x
        if dd > max_dd:
            max_dd = dd
    return max_dd


@dataclass
class PerformanceMonitor:
    """
    Measurement-only layer for human tuning: win rate, profit factor, drawdown,
    selectivity (filters starving entries), strategy survival, correlation load.
    """

    log_interval_cycles: int = 50
    alert_max_drawdown: float = 0.0
    """If > 0, log WARNING when max drawdown (same units as PnL equity) exceeds this."""
    alert_selectivity_min: float = 0.03
    """Warn when opens/LLM-intents falls below this (after enough intents)."""
    alert_min_llm_intents: int = 40
    """Minimum LLM BUY/SELL calls before selectivity warning."""

    # --- Counters ---
    loop_cycles: int = 0
    llm_trade_intents: int = 0
    position_opens: int = 0
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    neutrals: int = 0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    total_profit: float = 0.0
    equity_curve: List[float] = field(default_factory=lambda: [0.0])
    penalty_samples: List[float] = field(default_factory=list)
    _last_strategy_snapshot_keys: int = 0
    _last_strategy_active: int = 0

    def update_on_signal(self, decision: "Decision") -> None:
        """
        Call once per loop with LLM output *before* hard/pattern/fusion vetoes.
        """
        act = str(decision.action).upper()
        if act in ("BUY", "SELL"):
            self.llm_trade_intents += 1

    def note_correlation_penalty(self, penalty: float) -> None:
        """Record penalty mass from fusion (0..1 typical)."""
        p = float(penalty)
        if p > 1e-9:
            self.penalty_samples.append(p)
            if len(self.penalty_samples) > 500:
                self.penalty_samples = self.penalty_samples[-500:]

    def update_after_execution(self, final_action: str, *, opened: bool) -> None:
        """Call after execute_trade when you know if a new position opened."""
        act = str(final_action).upper()
        if act in ("BUY", "SELL") and opened:
            self.position_opens += 1

    def update_on_trade(self, pnl: float, score: int) -> None:
        """Call on position close (realized PnL)."""
        p = float(pnl)
        self.total_trades += 1
        self.total_profit += p
        last = self.equity_curve[-1]
        self.equity_curve.append(last + p)
        if score > 0:
            self.wins += 1
            self.gross_profit += p
        elif score < 0:
            self.losses += 1
            self.gross_loss += abs(p)
        else:
            self.neutrals += 1

    def update_on_strategy(self, registry: "StrategyRegistry") -> None:
        """Refresh strategy survival stats from registry snapshot."""
        snap = registry.snapshot()
        n = len(snap)
        active = sum(1 for st in snap.values() if isinstance(st, dict) and bool(st.get("active", True)))
        self._last_strategy_snapshot_keys = n
        self._last_strategy_active = active

    def tick_cycle_end(self) -> None:
        """Call once at end of each main loop iteration."""
        self.loop_cycles += 1

    @property
    def win_rate(self) -> float:
        decided = self.wins + self.losses
        if decided <= 0:
            return 0.0
        return self.wins / float(decided)

    @property
    def profit_factor(self) -> float:
        if self.gross_loss < 1e-12:
            return float("inf") if self.gross_profit > 1e-12 else 0.0
        return self.gross_profit / self.gross_loss

    @property
    def max_drawdown(self) -> float:
        return _max_drawdown(self.equity_curve)

    @property
    def selectivity_ratio(self) -> float:
        """Share of LLM BUY/SELL intents that resulted in an open (approximate pipeline)."""
        if self.llm_trade_intents <= 0:
            return 0.0
        return self.position_opens / float(self.llm_trade_intents)

    @property
    def strategy_survival_rate(self) -> float:
        if self._last_strategy_snapshot_keys <= 0:
            return 0.0
        return self._last_strategy_active / float(self._last_strategy_snapshot_keys)

    @property
    def avg_correlation_penalty(self) -> float:
        if not self.penalty_samples:
            return 0.0
        return sum(self.penalty_samples) / len(self.penalty_samples)

    def build_summary(self) -> Dict[str, Any]:
        pf = self.profit_factor
        pf_out: Any
        if pf == float("inf"):
            pf_out = "inf"
        else:
            pf_out = round(pf, 4)
        return {
            "loop_cycles": self.loop_cycles,
            "llm_trade_intents": self.llm_trade_intents,
            "position_opens": self.position_opens,
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "neutrals": self.neutrals,
            "win_rate": round(self.win_rate, 4),
            "profit_factor": pf_out,
            "total_profit": round(self.total_profit, 6),
            "max_drawdown": round(self.max_drawdown, 6),
            "equity_last": round(self.equity_curve[-1], 6) if self.equity_curve else 0.0,
            "selectivity_ratio": round(self.selectivity_ratio, 4),
            "strategy_survival_rate": round(self.strategy_survival_rate, 4),
            "strategy_keys": self._last_strategy_snapshot_keys,
            "strategy_active": self._last_strategy_active,
            "avg_correlation_penalty": round(self.avg_correlation_penalty, 4),
            "penalty_observations": len(self.penalty_samples),
        }

    def maybe_log_summary_and_alerts(self) -> None:
        interval = max(1, int(self.log_interval_cycles))
        if self.loop_cycles <= 0 or self.loop_cycles % interval != 0:
            return
        s = self.build_summary()
        lines = [
            "[PERFORMANCE SUMMARY]",
            f"  cycles={s['loop_cycles']} llm_intents={s['llm_trade_intents']} opens={s['position_opens']} closes={s['total_trades']}",
            f"  win_rate={s['win_rate']} profit_factor={s['profit_factor']} total_profit={s['total_profit']}",
            f"  max_drawdown={s['max_drawdown']} equity_last={s['equity_last']}",
            f"  selectivity_ratio={s['selectivity_ratio']} survival_rate={s['strategy_survival_rate']} (keys {s['strategy_active']} / {s['strategy_keys']})",
            f"  avg_correlation_penalty={s['avg_correlation_penalty']} (n={s['penalty_observations']})",
        ]
        log.info("\n".join(lines))
        self._maybe_tuning_hint(s)

    def _maybe_tuning_hint(self, s: Dict[str, Any]) -> None:
        hints: List[str] = []
        if self.alert_max_drawdown > 0 and float(s["max_drawdown"]) > self.alert_max_drawdown:
            log.warning(
                "[PERFORMANCE ALERT] max_drawdown=%s exceeds threshold=%s — review risk and filters",
                s["max_drawdown"],
                self.alert_max_drawdown,
            )
            hints.append("drawdown")
        if (
            self.llm_trade_intents >= self.alert_min_llm_intents
            and float(s["selectivity_ratio"]) < self.alert_selectivity_min
        ):
            log.warning(
                "[PERFORMANCE ALERT] selectivity_ratio=%s below %s after %s LLM intents — possible over-filtering",
                s["selectivity_ratio"],
                self.alert_selectivity_min,
                self.llm_trade_intents,
            )
            hints.append("selectivity")
        if float(s["avg_correlation_penalty"]) > 0.45 and s["penalty_observations"] >= 20:
            log.info(
                "[PERFORMANCE TUNING HINT] High avg correlation penalty — strategies may be redundant; check STRATEGY keys overlap"
            )

    def brief_line(self) -> str:
        s = self.build_summary()
        return (
            f"mon: wr={s['win_rate']} pf={s['profit_factor']} dd={s['max_drawdown']} "
            f"sel={s['selectivity_ratio']} surv={s['strategy_survival_rate']} pen={s['avg_correlation_penalty']}"
        )
