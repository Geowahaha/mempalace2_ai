"""
learning/mt5_backtester.py
MT5 history sync + backtest helper for Dexter Pro.
"""
from __future__ import annotations

from learning.neural_brain import neural_brain


class MT5Backtester:
    def run(self, days: int = 30, sync_days: int = 120) -> dict:
        sync = neural_brain.sync_outcomes_from_mt5(days=sync_days)
        feedback = neural_brain.sync_signal_outcomes_from_market(days=sync_days)
        report = neural_brain.backtest_report(days=days)
        report["sync"] = sync
        report["feedback"] = feedback
        report["model"] = neural_brain.model_status()
        return report


mt5_backtester = MT5Backtester()
