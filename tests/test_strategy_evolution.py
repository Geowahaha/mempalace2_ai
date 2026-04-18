from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from trading_ai.core.strategy_evolution import StrategyRegistry


class StrategyEvolutionTests(unittest.TestCase):
    def test_shadow_probe_can_unlock_probationary_lane(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            registry = StrategyRegistry(Path(tmp) / "strategy_registry.json")
            key = "UP*HIGH*LONDON_trend_follow"
            registry.record_shadow_probe(key, pnl=0.4, score=1)
            registry.record_shadow_probe(key, pnl=0.2, score=1)

            stats = registry.get_stats(key)
            self.assertIsNotNone(stats)
            self.assertEqual(stats.shadow_trades, 2)
            self.assertEqual(stats.pending_recommendation, "promote_from_shadow")
            self.assertTrue(registry.is_strategy_allowed(key))
            self.assertGreater(registry.get_strategy_boost(key), 0.0)

    def test_negative_skill_feedback_quarantines_lane(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            registry = StrategyRegistry(Path(tmp) / "strategy_registry.json")
            key = "DOWN*HIGH*ASIA_trend_follow"
            registry.sync_skill_feedback(
                key,
                risk_adjusted_score=-0.45,
                trades_seen=3,
                win_rate=0.0,
            )

            stats = registry.get_stats(key)
            self.assertIsNotNone(stats)
            self.assertEqual(stats.pending_recommendation, "quarantine")
            self.assertFalse(registry.is_strategy_allowed(key))


if __name__ == "__main__":
    unittest.main()
