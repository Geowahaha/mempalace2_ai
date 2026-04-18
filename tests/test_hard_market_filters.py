from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from trading_ai.config import Settings
from trading_ai.core.strategy import RiskManager, TradeScore
from trading_ai.main import _hard_market_filters, _maybe_soften_hard_filter_veto


class HardMarketFilterTests(unittest.TestCase):
    def _settings(self) -> Settings:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text("HARD_FILTER_MIN_CLOSES=0\n", encoding="utf-8")
            return Settings(_env_file=(str(env_path),))

    @staticmethod
    def _risk() -> RiskManager:
        return RiskManager(
            max_trades_per_session=50,
            max_consecutive_losses=5,
            neutral_rel_threshold=0.0001,
        )

    def test_consolidation_blocks_when_no_impulse(self) -> None:
        settings = self._settings()
        features = {
            "trend_direction": "DOWN",
            "volatility": "HIGH",
            "sample_closes_len": 96,
            "momentum_5": -0.00015,
            "momentum_20": -0.00090,
            "realized_volatility": 0.00028,
            "spread_pct": 0.00008,
            "distance_from_recent_low_pct": 0.00020,
            "distance_from_recent_high_pct": 0.00110,
            "structure": {
                "consolidation": True,
                "higher_high": False,
                "lower_low": False,
            },
            "session": "NY",
        }
        veto = _hard_market_filters(features, self._risk(), settings, "SELL")
        self.assertEqual(veto, "structure_consolidation")

    def test_consolidation_allows_strong_impulse_breakout(self) -> None:
        settings = self._settings()
        features = {
            "trend_direction": "DOWN",
            "volatility": "HIGH",
            "sample_closes_len": 96,
            "momentum_5": -0.0042,
            "momentum_20": -0.0025,
            "realized_volatility": 0.00020,
            "spread_pct": 0.00008,
            "distance_from_recent_low_pct": 0.00020,
            "distance_from_recent_high_pct": 0.00120,
            "structure": {
                "consolidation": True,
                "higher_high": False,
                "lower_low": False,
            },
            "session": "NY",
        }
        veto = _hard_market_filters(features, self._risk(), settings, "SELL")
        self.assertIsNone(veto)

    def test_adaptive_hard_filter_relaxes_supportive_lane(self) -> None:
        settings = self._settings()
        features = {
            "trend_direction": "DOWN",
            "volatility": "HIGH",
            "spread_pct": 0.00008,
            "session": "NY",
        }
        veto, meta = _maybe_soften_hard_filter_veto(
            veto="structure_consolidation",
            action="SELL",
            features=features,
            assessment={
                "opportunity_score": 0.71,
                "risk_score": 0.46,
                "impulse_support": 0.83,
            },
            strategy_key="DOWN*HIGH*NY_trend_follow",
            weekly_lane_profile={
                "mempalace_strategy_lanes": {
                    "DOWN*HIGH*NY_trend_follow": {
                        "classification": "opportunity",
                        "trades": 4,
                        "wins": 2,
                        "losses": 1,
                        "win_rate": 0.50,
                        "loss_rate": 0.25,
                        "missed_opportunities": 3,
                        "prevented_bad": 1,
                        "shadow_blocked_wins": 1,
                        "shadow_blocked_losses": 0,
                    }
                }
            },
            risk=self._risk(),
            settings=settings,
        )
        self.assertIsNone(veto)
        self.assertTrue(meta.get("applied"))

    def test_adaptive_hard_filter_keeps_bad_lane_blocked(self) -> None:
        settings = self._settings()
        features = {
            "trend_direction": "DOWN",
            "volatility": "HIGH",
            "spread_pct": 0.00008,
            "session": "NY",
        }
        veto, meta = _maybe_soften_hard_filter_veto(
            veto="trend_RANGE",
            action="SELL",
            features=features,
            assessment={
                "opportunity_score": 0.73,
                "risk_score": 0.41,
                "impulse_support": 0.87,
            },
            strategy_key="DOWN*HIGH*NY_trend_follow",
            weekly_lane_profile={
                "mempalace_strategy_lanes": {
                    "DOWN*HIGH*NY_trend_follow": {
                        "classification": "bad",
                        "trades": 7,
                        "wins": 2,
                        "losses": 5,
                        "win_rate": 0.2857,
                        "loss_rate": 0.7143,
                        "missed_opportunities": 0,
                        "prevented_bad": 3,
                        "shadow_blocked_wins": 0,
                        "shadow_blocked_losses": 2,
                    }
                }
            },
            risk=self._risk(),
            settings=settings,
        )
        self.assertEqual(veto, "trend_RANGE")
        self.assertFalse(meta.get("applied"))

    def test_adaptive_hard_filter_allows_bad_lane_when_support_dominates(self) -> None:
        settings = self._settings()
        features = {
            "trend_direction": "DOWN",
            "volatility": "HIGH",
            "spread_pct": 0.00008,
            "session": "NY",
        }
        veto, meta = _maybe_soften_hard_filter_veto(
            veto="structure_consolidation",
            action="SELL",
            features=features,
            assessment={
                "opportunity_score": 0.74,
                "risk_score": 0.44,
                "impulse_support": 0.88,
            },
            strategy_key="DOWN*HIGH*NY_trend_follow",
            weekly_lane_profile={
                "mempalace_strategy_lanes": {
                    "DOWN*HIGH*NY_trend_follow": {
                        "classification": "bad",
                        "trades": 8,
                        "wins": 2,
                        "losses": 3,
                        "win_rate": 0.25,
                        "loss_rate": 0.375,
                        "missed_opportunities": 7,
                        "prevented_bad": 0,
                        "shadow_blocked_wins": 0,
                        "shadow_blocked_losses": 0,
                    }
                }
            },
            risk=self._risk(),
            settings=settings,
        )
        self.assertIsNone(veto)
        self.assertTrue(meta.get("applied"))

    def test_adaptive_hard_filter_blocks_on_recent_negative_edge(self) -> None:
        settings = self._settings()
        risk = self._risk()
        risk.recent_scores = [
            TradeScore.LOSS,
            TradeScore.LOSS,
            TradeScore.NEUTRAL,
            TradeScore.LOSS,
        ]
        features = {
            "trend_direction": "DOWN",
            "volatility": "HIGH",
            "spread_pct": 0.00008,
            "session": "NY",
        }
        veto, meta = _maybe_soften_hard_filter_veto(
            veto="structure_consolidation",
            action="SELL",
            features=features,
            assessment={
                "opportunity_score": 0.74,
                "risk_score": 0.44,
                "impulse_support": 0.88,
            },
            strategy_key="DOWN*HIGH*NY_trend_follow",
            weekly_lane_profile={
                "mempalace_strategy_lanes": {
                    "DOWN*HIGH*NY_trend_follow": {
                        "classification": "opportunity",
                        "trades": 6,
                        "wins": 3,
                        "losses": 2,
                        "win_rate": 0.5,
                        "loss_rate": 0.3333,
                        "missed_opportunities": 5,
                        "prevented_bad": 1,
                        "shadow_blocked_wins": 1,
                        "shadow_blocked_losses": 0,
                    }
                }
            },
            risk=risk,
            settings=settings,
        )
        self.assertEqual(veto, "structure_consolidation")
        self.assertFalse(meta.get("applied"))
        self.assertIn("recent_edge_negative", str(meta.get("blocked_reason") or ""))

    def test_adaptive_hard_filter_uses_recent_positive_edge_bonus(self) -> None:
        settings = self._settings()
        risk = self._risk()
        risk.recent_scores = [
            TradeScore.WIN,
            TradeScore.WIN,
            TradeScore.NEUTRAL,
            TradeScore.WIN,
        ]
        features = {
            "trend_direction": "DOWN",
            "volatility": "HIGH",
            "spread_pct": 0.00008,
            "session": "NY",
        }
        veto, meta = _maybe_soften_hard_filter_veto(
            veto="trend_RANGE",
            action="SELL",
            features=features,
            assessment={
                "opportunity_score": 0.72,
                "risk_score": 0.46,
                "impulse_support": 0.81,
            },
            strategy_key="DOWN*HIGH*NY_trend_follow",
            weekly_lane_profile={
                "mempalace_strategy_lanes": {
                    "DOWN*HIGH*NY_trend_follow": {
                        "classification": "neutral",
                        "trades": 4,
                        "wins": 2,
                        "losses": 1,
                        "win_rate": 0.5,
                        "loss_rate": 0.25,
                        "missed_opportunities": 3,
                        "prevented_bad": 3,
                        "shadow_blocked_wins": 0,
                        "shadow_blocked_losses": 0,
                    }
                }
            },
            risk=risk,
            settings=settings,
        )
        self.assertIsNone(veto)
        self.assertTrue(meta.get("applied"))
        self.assertEqual(int(meta.get("support_edge_required") or 0), 0)


if __name__ == "__main__":
    unittest.main()
