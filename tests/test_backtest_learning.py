from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from trading_ai.config import Settings
from trading_ai.core.backtest_learning import BacktestLearningSupervisor, build_adaptive_policy_from_backtest


class BacktestLearningPolicyTests(unittest.TestCase):
    @staticmethod
    def _baseline() -> dict[str, float | int]:
        return {
            "hard_filter_adaptive_min_trades": 4,
            "hard_filter_adaptive_support_edge_min": 1,
            "hard_filter_adaptive_min_opportunity": 0.66,
            "hard_filter_adaptive_max_risk": 0.58,
            "hard_filter_adaptive_min_edge": 0.10,
            "hard_filter_adaptive_min_impulse_support": 0.66,
            "hard_filter_adaptive_max_loss_rate": 0.62,
            "hard_filter_adaptive_recent_window": 8,
            "hard_filter_adaptive_recent_min_samples": 4,
            "hard_filter_adaptive_recent_neg_edge_block": -0.25,
            "hard_filter_adaptive_recent_pos_edge_bonus": 0.20,
        }

    def test_relax_mode_when_quality_passes_and_block_rate_high(self) -> None:
        report = {
            "performance": {
                "closed_trades": 40,
                "win_rate": 0.64,
                "avg_profit": 0.08,
                "max_drawdown": 0.45,
            },
            "decisions": {"BUY": 30, "SELL": 20, "HOLD": 10},
            "diagnostics": {
                "blocker_buckets": {
                    "pre_llm_hard_filter:trend_RANGE": 35,
                    "pre_llm_hard_filter:structure_consolidation": 8,
                }
            },
        }
        baseline = self._baseline()
        policy = build_adaptive_policy_from_backtest(
            report=report,
            baseline=baseline,
            min_closed_trades=12,
            min_win_rate=0.5,
            min_avg_profit=0.0,
            max_drawdown=1.2,
            max_shift=0.06,
            apply_enabled=True,
        )
        self.assertEqual(policy.get("mode"), "relax")
        self.assertTrue((policy.get("quality_gate") or {}).get("passed"))
        self.assertGreater((policy.get("metrics") or {}).get("hard_filter_block_ratio") or 0.0, 0.35)
        self.assertLess(
            (policy.get("recommended") or {}).get("hard_filter_adaptive_min_opportunity"),
            baseline["hard_filter_adaptive_min_opportunity"],
        )
        self.assertIn("hard_filter_adaptive_min_opportunity", dict(policy.get("effective_overrides") or {}))

    def test_tighten_mode_when_quality_gate_fails(self) -> None:
        report = {
            "performance": {
                "closed_trades": 5,
                "win_rate": 0.2,
                "avg_profit": -0.03,
                "max_drawdown": 2.4,
            },
            "decisions": {"BUY": 15, "SELL": 10, "HOLD": 5},
            "diagnostics": {"blocker_buckets": {}},
        }
        baseline = self._baseline()
        policy = build_adaptive_policy_from_backtest(
            report=report,
            baseline=baseline,
            min_closed_trades=12,
            min_win_rate=0.5,
            min_avg_profit=0.0,
            max_drawdown=1.2,
            max_shift=0.06,
            apply_enabled=True,
        )
        self.assertEqual(policy.get("mode"), "tighten")
        self.assertFalse((policy.get("quality_gate") or {}).get("passed"))
        self.assertGreater(
            (policy.get("recommended") or {}).get("hard_filter_adaptive_min_opportunity"),
            baseline["hard_filter_adaptive_min_opportunity"],
        )
        self.assertLess(
            (policy.get("recommended") or {}).get("hard_filter_adaptive_max_risk"),
            baseline["hard_filter_adaptive_max_risk"],
        )
        self.assertIn("hard_filter_adaptive_max_risk", dict(policy.get("effective_overrides") or {}))

    def test_apply_disabled_produces_no_effective_overrides(self) -> None:
        report = {
            "performance": {
                "closed_trades": 30,
                "win_rate": 0.6,
                "avg_profit": 0.04,
                "max_drawdown": 0.7,
            },
            "decisions": {"BUY": 20, "SELL": 20, "HOLD": 10},
            "diagnostics": {"blocker_buckets": {"pre_llm_hard_filter:trend_RANGE": 25}},
        }
        policy = build_adaptive_policy_from_backtest(
            report=report,
            baseline=self._baseline(),
            min_closed_trades=12,
            min_win_rate=0.5,
            min_avg_profit=0.0,
            max_drawdown=1.2,
            max_shift=0.06,
            apply_enabled=False,
        )
        self.assertEqual(policy.get("mode"), "relax")
        self.assertEqual(dict(policy.get("effective_overrides") or {}), {})

    def test_supervisor_falls_back_to_repo_root_when_candle_data_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = (root / "data").resolve()
            data_dir.mkdir(parents=True, exist_ok=True)
            repo_root = (root / "repo").resolve()
            (repo_root / "backtest").mkdir(parents=True, exist_ok=True)
            (repo_root / "backtest" / "candle_data.db").write_text("", encoding="utf-8")
            env_path = root / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        f"DATA_DIR={data_dir}",
                        "BACKTEST_LEARNING_ENABLED=true",
                        "BACKTEST_LEARNING_SOURCE_POLICY=real_first",
                        f"BACKTEST_LEARNING_DEXTER_ROOT={root / 'missing_dexter'}",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            settings = Settings(_env_file=(str(env_path),))
            supervisor = BacktestLearningSupervisor(settings=settings)
            supervisor._repo_root = repo_root
            self.assertEqual(supervisor._resolve_backtest_root(), repo_root)

    def test_supervisor_resolve_root_raises_when_no_source_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = (root / "data").resolve()
            data_dir.mkdir(parents=True, exist_ok=True)
            repo_root = (root / "repo").resolve()
            repo_root.mkdir(parents=True, exist_ok=True)
            env_path = root / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        f"DATA_DIR={data_dir}",
                        "BACKTEST_LEARNING_ENABLED=true",
                        "BACKTEST_LEARNING_SOURCE_POLICY=real_only",
                        f"BACKTEST_LEARNING_DEXTER_ROOT={root / 'missing_dexter'}",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            settings = Settings(_env_file=(str(env_path),))
            supervisor = BacktestLearningSupervisor(settings=settings)
            supervisor._repo_root = repo_root
            with self.assertRaises(RuntimeError):
                supervisor._resolve_backtest_root()


if __name__ == "__main__":
    unittest.main()
