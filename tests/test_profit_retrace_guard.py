from __future__ import annotations

import unittest

from config import Config
from execution.ctrader_executor import CTraderExecutor


class ProfitRetraceGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig = {
            "CTRADER_PM_PROFIT_RETRACE_GUARD_ENABLED": Config.CTRADER_PM_PROFIT_RETRACE_GUARD_ENABLED,
            "CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_AGE_MIN": Config.CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_AGE_MIN,
            "CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_PEAK_R": Config.CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_PEAK_R,
            "CTRADER_PM_PROFIT_RETRACE_GUARD_EXIT_RETRACE_R": Config.CTRADER_PM_PROFIT_RETRACE_GUARD_EXIT_RETRACE_R,
            "CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_BAR_VOLUME_PROXY": Config.CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_BAR_VOLUME_PROXY,
            "CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_ABS_MID_DRIFT_PCT": Config.CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_ABS_MID_DRIFT_PCT,
            "CTRADER_PM_PROFIT_RETRACE_SWEEP_RECOVERY_ENABLED": Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_RECOVERY_ENABLED,
            "CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_REJECTION_RATIO": Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_REJECTION_RATIO,
            "CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_BAR_VOLUME_PROXY": Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_BAR_VOLUME_PROXY,
            "CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DELTA_PROXY": Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DELTA_PROXY,
            "CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DEPTH_IMBALANCE": Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DEPTH_IMBALANCE,
            "CTRADER_PM_PROFIT_RETRACE_SWEEP_LOCK_R": Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_LOCK_R,
            "CTRADER_PM_CORRECTIVE_FAMILIES": Config.CTRADER_PM_CORRECTIVE_FAMILIES,
            "CTRADER_PM_IMPULSE_FAMILIES": Config.CTRADER_PM_IMPULSE_FAMILIES,
        }
        Config.CTRADER_PM_PROFIT_RETRACE_GUARD_ENABLED = True
        Config.CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_AGE_MIN = 1.0
        Config.CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_PEAK_R = 0.3
        Config.CTRADER_PM_PROFIT_RETRACE_GUARD_EXIT_RETRACE_R = 0.2
        Config.CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_BAR_VOLUME_PROXY = 0.22
        Config.CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_ABS_MID_DRIFT_PCT = 0.004
        Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_RECOVERY_ENABLED = True
        Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_REJECTION_RATIO = 0.28
        Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_BAR_VOLUME_PROXY = 0.3
        Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DELTA_PROXY = 0.08
        Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DEPTH_IMBALANCE = 0.06
        Config.CTRADER_PM_PROFIT_RETRACE_SWEEP_LOCK_R = 0.05
        Config.CTRADER_PM_CORRECTIVE_FAMILIES = "xau_scalp_pullback_limit"
        Config.CTRADER_PM_IMPULSE_FAMILIES = "xau_scalp_breakout_stop"

    def tearDown(self) -> None:
        for key, value in self._orig.items():
            setattr(Config, key, value)

    @staticmethod
    def _make_executor(features: dict) -> CTraderExecutor:
        executor = object.__new__(CTraderExecutor)
        executor._position_peak_r = {}

        def _snapshot(*, symbol: str, direction: str, confidence: float = 0.0):
            _ = symbol, direction, confidence
            return {"ok": True, "features": dict(features)}

        executor._latest_capture_snapshot = _snapshot  # type: ignore[attr-defined]
        return executor

    def test_corrective_sweep_recovery_tightens_instead_of_close(self) -> None:
        features = {
            "day_type": "trend",
            "bar_volume_proxy": 0.42,
            "mid_drift_pct": 0.001,
            "delta_proxy": 0.12,
            "depth_imbalance": 0.08,
            "rejection_ratio": 0.35,
        }
        ex = self._make_executor(features)
        ex._position_peak_r[1001] = 0.85
        plan = ex._profit_retrace_guard_plan(
            source="scalp_xauusd:pb:canary",
            symbol="XAUUSD",
            direction="long",
            position_id=1001,
            entry=4860.0,
            stop_loss=4854.0,
            current_price=4863.0,
            confidence=74.0,
            age_min=6.0,
            r_now=0.50,
        )
        self.assertTrue(plan.get("active"))
        self.assertEqual(plan.get("action"), "tighten")
        self.assertEqual(plan.get("reason"), "profit_retrace_guard_corrective_sweep_tighten")

    def test_corrective_weak_market_closes(self) -> None:
        features = {
            "day_type": "range",
            "bar_volume_proxy": 0.10,
            "mid_drift_pct": 0.001,
            "delta_proxy": 0.01,
            "depth_imbalance": 0.00,
            "rejection_ratio": 0.12,
        }
        ex = self._make_executor(features)
        ex._position_peak_r[1002] = 0.70
        plan = ex._profit_retrace_guard_plan(
            source="scalp_xauusd:pb:canary",
            symbol="XAUUSD",
            direction="long",
            position_id=1002,
            entry=4860.0,
            stop_loss=4854.0,
            current_price=4861.0,
            confidence=72.0,
            age_min=6.0,
            r_now=0.40,
        )
        self.assertTrue(plan.get("active"))
        self.assertEqual(plan.get("action"), "close")
        self.assertEqual(plan.get("reason"), "profit_retrace_guard_close")


if __name__ == "__main__":
    unittest.main()
