from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from trading_ai.core.execution import MarketSnapshot, OpenPosition
from trading_ai.core.position_manager import (
    assess_entry_candidate,
    evaluate_entry_hold_override,
    evaluate_open_position,
    write_monitor_snapshot,
)
from trading_ai.core.skillbook import SkillMatch


class PositionManagerTests(unittest.TestCase):
    def _settings(self) -> SimpleNamespace:
        return SimpleNamespace(
            position_manager_max_hold_minutes=240,
            position_manager_min_expected_move_pct=0.00035,
            position_manager_tp_vol_multiplier=1.35,
            position_manager_sl_vol_multiplier=0.95,
            position_manager_trail_trigger_fraction=0.55,
            position_manager_risk_close_threshold=0.64,
            min_trade_confidence=0.65,
            entry_override_enabled=True,
            entry_override_min_opportunity=0.67,
            entry_override_max_risk=0.55,
            entry_override_min_edge=0.16,
            entry_override_confidence=0.67,
        )

    def test_assess_entry_candidate_prefers_aligned_setup(self) -> None:
        match = SkillMatch(
            skill_key="UP*HIGH*NY_trend_follow",
            score=6.4,
            title="Aligned lane",
            summary="Strong continuation lane.",
            use_when=[],
            avoid_when=[],
            guardrails=[],
            confidence_rules=[],
            stats={"trades_seen": 4, "wins": 3, "losses": 1, "win_rate": 0.75, "risk_adjusted_score": 0.45},
            triggers={},
            file_path="",
            fit_reasons=["strategy_key"],
        )
        aligned = assess_entry_candidate(
            action="BUY",
            features={
                "trend_direction": "UP",
                "structure": {"consolidation": False},
                "realized_volatility": 0.0002,
                "momentum_5": 0.0008,
                "momentum_20": 0.0012,
                "spread_pct": 0.00005,
            },
            decision={"reason": "test"},
            matches=[match],
            strategy_state={"trades": 4, "wins": 3, "shadow_trades": 1, "shadow_wins": 1, "shadow_losses": 0},
            pattern_analysis={"per_setup_tag": {"trend_follow": {"win_rate": 0.72}}},
        )
        self.assertGreater(aligned["opportunity_score"], aligned["risk_score"])

    def test_evaluate_open_position_can_take_profit(self) -> None:
        plan = evaluate_open_position(
            position=OpenPosition(
                order_id="1",
                symbol="XAUUSD",
                side="BUY",
                volume=0.01,
                entry_price=100.0,
                position_id="p1",
                opened_ts=0.0,
            ),
            market=MarketSnapshot(
                symbol="XAUUSD",
                bid=100.30,
                ask=100.32,
                mid=100.31,
                spread=0.02,
                ts_unix=60.0,
                extra={},
            ),
            features={
                "trend_direction": "UP",
                "structure": {"consolidation": False},
                "realized_volatility": 0.0002,
                "momentum_5": 0.001,
                "momentum_20": 0.0015,
                "spread_pct": 0.00005,
            },
            close_context={"setup_tag": "trend_follow", "strategy_key": "UP*HIGH*NY_trend_follow"},
            matches=[
                SkillMatch(
                    skill_key="UP*HIGH*NY_trend_follow",
                    score=6.8,
                    title="Strong lane",
                    summary="",
                    use_when=[],
                    avoid_when=[],
                    guardrails=[],
                    confidence_rules=[],
                    stats={"trades_seen": 5, "wins": 4, "losses": 1, "win_rate": 0.8, "risk_adjusted_score": 0.6},
                    triggers={},
                    file_path="",
                    fit_reasons=["strategy_key"],
                )
            ],
            strategy_state={"trades": 5, "wins": 4, "shadow_trades": 1, "shadow_wins": 1, "shadow_losses": 0},
            pattern_analysis={"per_setup_tag": {"trend_follow": {"win_rate": 0.8}}},
            settings=self._settings(),
        )
        self.assertEqual(plan.action, "CLOSE")
        self.assertIn("take_profit", plan.reason)

    def test_write_monitor_snapshot_writes_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot_path = Path(tmp) / "latest.json"
            history_path = Path(tmp) / "history.ndjson"
            write_monitor_snapshot(snapshot_path, history_path, {"ok": True, "value": 1})
            self.assertTrue(snapshot_path.is_file())
            self.assertTrue(history_path.is_file())
            self.assertIn('"ok": true', snapshot_path.read_text(encoding="utf-8").lower())

    def test_hold_override_promotes_strong_aligned_setup(self) -> None:
        assessment = {
            "action": "BUY",
            "opportunity_score": 0.7046,
            "risk_score": 0.4837,
            "edge_score": 0.5,
        }
        result = evaluate_entry_hold_override(
            anticipated_action="BUY",
            anticipated_assessment=assessment,
            decision_action="HOLD",
            decision_reason="The market structure is clear but the team brief is cautious.",
            matches=[],
            risk_state={"can_trade": True},
            room_guard=None,
            settings=self._settings(),
        )
        self.assertTrue(result["eligible"])
        self.assertGreaterEqual(result["confidence"], 0.67)

    def test_hold_override_respects_guarded_hold_reasons(self) -> None:
        assessment = {
            "action": "SELL",
            "opportunity_score": 0.71,
            "risk_score": 0.42,
            "edge_score": 0.29,
        }
        result = evaluate_entry_hold_override(
            anticipated_action="SELL",
            anticipated_assessment=assessment,
            decision_action="HOLD",
            decision_reason="pre_llm_hard_filter:structure_consolidation",
            matches=[],
            risk_state={"can_trade": True},
            room_guard=None,
            settings=self._settings(),
        )
        self.assertFalse(result["eligible"])
        self.assertEqual(result["blocked_reason"], "hard_filter")

    def test_hold_override_allows_impulse_breakout_during_soft_hard_filter(self) -> None:
        assessment = {
            "action": "BUY",
            "opportunity_score": 0.70,
            "risk_score": 0.61,
            "edge_score": 0.17,
            "impulse_support": 0.86,
        }
        result = evaluate_entry_hold_override(
            anticipated_action="BUY",
            anticipated_assessment=assessment,
            decision_action="HOLD",
            decision_reason="pre_llm_hard_filter:trend_RANGE",
            matches=[],
            risk_state={"can_trade": True},
            room_guard=None,
            settings=self._settings(),
        )
        self.assertTrue(result["eligible"])
        self.assertEqual(result.get("hard_filter_override"), "impulse_support")

    def test_evaluate_open_position_closes_profit_reversal_in_weak_market(self) -> None:
        close_context = {
            "setup_tag": "trend_follow",
            "strategy_key": "UP*LOW*ASIA_trend_follow",
            "pm_peak_return_pct": 0.0040,
        }
        plan = evaluate_open_position(
            position=OpenPosition(
                order_id="2",
                symbol="XAUUSD",
                side="BUY",
                volume=0.01,
                entry_price=100.0,
                position_id="p2",
                opened_ts=0.0,
            ),
            market=MarketSnapshot(
                symbol="XAUUSD",
                bid=99.99,
                ask=100.01,
                mid=100.00,
                spread=0.02,
                ts_unix=3600.0,
                extra={},
            ),
            features={
                "session": "ASIA",
                "trend_direction": "RANGE",
                "volatility": "LOW",
                "structure": {"consolidation": True, "higher_high": False, "lower_low": True},
                "realized_volatility": 0.00005,
                "momentum_5": -0.0003,
                "momentum_20": -0.0002,
                "spread_pct": 0.0002,
            },
            close_context=close_context,
            matches=[],
            strategy_state={"trades": 2, "wins": 1},
            pattern_analysis={"per_setup_tag": {"trend_follow": {"win_rate": 0.5}}},
            settings=self._settings(),
        )
        self.assertEqual(plan.action, "CLOSE")
        self.assertIn("profit_reversal", plan.reason)


if __name__ == "__main__":
    unittest.main()
