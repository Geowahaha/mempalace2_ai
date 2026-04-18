from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from trading_ai.config import Settings
from trading_ai.core.strategy import RiskManager
from trading_ai.main import _hard_market_filters


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


if __name__ == "__main__":
    unittest.main()
