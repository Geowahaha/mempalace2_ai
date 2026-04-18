from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from trading_ai.config import Settings


class ConfigTests(unittest.TestCase):
    def test_package_env_overrides_legacy_root_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root_env = Path(tmp) / ".env"
            package_env = Path(tmp) / "trading_ai.env"
            root_env.write_text(
                "\n".join(
                    [
                        "CTRADER_OPENAPI_ACCESS_TOKEN=legacy_root_token",
                        "CTRADER_OPENAPI_REFRESH_TOKEN=legacy_root_refresh",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            package_env.write_text(
                "\n".join(
                    [
                        "DRY_RUN=false",
                        "LIVE_EXECUTION_ENABLED=true",
                        "CTRADER_OPENAPI_ACCESS_TOKEN=package_token",
                        "CTRADER_OPENAPI_REFRESH_TOKEN=package_refresh",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            settings = Settings(_env_file=(str(root_env), str(package_env)))

            self.assertFalse(settings.dry_run)
            self.assertTrue(settings.live_execution_enabled)
            self.assertEqual(settings.ctrader_access_token, "package_token")
            self.assertEqual(settings.ctrader_refresh_token, "package_refresh")

    def test_local_learning_overrides_are_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "LLM_PROVIDER=local",
                        "LOCAL_MODEL_NAME=qwen2.5:1.5b",
                        "LOCAL_THINK=false",
                        "LOCAL_KEEP_ALIVE=10m",
                        "SELF_IMPROVEMENT_MODEL_NAME=gemma4:e2b",
                        "SELF_IMPROVEMENT_TIMEOUT_SEC=180",
                        "SELF_IMPROVEMENT_MAX_TOKENS=256",
                        "SELF_IMPROVEMENT_LOCAL_NUM_CTX=512",
                        "SELF_IMPROVEMENT_LOCAL_KEEP_ALIVE=0s",
                        "SELF_IMPROVEMENT_LOCAL_THINK=false",
                        "LLM_FAILOVER_FAILURE_THRESHOLD=3",
                        "LLM_FAILOVER_COOLDOWN_SEC=45",
                        "LLM_FAILOVER_RUNTIME_PATH=./data/tencent_failover_runtime.json",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            settings = Settings(_env_file=(str(env_path),))

            self.assertEqual(settings.local_model, "qwen2.5:1.5b")
            self.assertFalse(settings.local_think)
            self.assertEqual(settings.local_keep_alive, "10m")
            self.assertEqual(settings.self_improvement_model_name, "gemma4:e2b")
            self.assertEqual(settings.self_improvement_timeout_sec, 180.0)
            self.assertEqual(settings.self_improvement_max_tokens, 256)
            self.assertEqual(settings.self_improvement_local_num_ctx, 512)
            self.assertEqual(settings.self_improvement_local_keep_alive, "0s")
            self.assertFalse(settings.self_improvement_local_think)
            self.assertEqual(settings.llm_failover_failure_threshold, 3)
            self.assertEqual(settings.llm_failover_cooldown_sec, 45.0)
            self.assertEqual(settings.llm_failover_runtime_path.name, "tencent_failover_runtime.json")

    def test_performance_stage_debug_thresholds_accept_low_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "PERFORMANCE_STAGE_WARN_MS=1",
                        "PERFORMANCE_CYCLE_WARN_MS=1",
                        "PERFORMANCE_STAGE_LOG_EVERY_CYCLE=true",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            settings = Settings(_env_file=(str(env_path),))
            self.assertEqual(settings.performance_stage_warn_ms, 1.0)
            self.assertEqual(settings.performance_cycle_warn_ms, 1.0)
            self.assertTrue(settings.performance_stage_log_every_cycle)

    def test_hard_filter_adaptive_and_quote_soft_stale_settings_load(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "HARD_FILTER_ADAPTIVE_ENABLED=true",
                        "HARD_FILTER_ADAPTIVE_MIN_TRADES=4",
                        "HARD_FILTER_ADAPTIVE_SUPPORT_EDGE_MIN=2",
                        "HARD_FILTER_ADAPTIVE_MIN_OPPORTUNITY=0.7",
                        "HARD_FILTER_ADAPTIVE_MAX_RISK=0.52",
                        "HARD_FILTER_ADAPTIVE_MIN_EDGE=0.12",
                        "HARD_FILTER_ADAPTIVE_MIN_IMPULSE_SUPPORT=0.71",
                        "HARD_FILTER_ADAPTIVE_MAX_LOSS_RATE=0.58",
                        "HARD_FILTER_ADAPTIVE_RECENT_WINDOW=10",
                        "HARD_FILTER_ADAPTIVE_RECENT_MIN_SAMPLES=5",
                        "HARD_FILTER_ADAPTIVE_RECENT_NEG_EDGE_BLOCK=-0.3",
                        "HARD_FILTER_ADAPTIVE_RECENT_POS_EDGE_BONUS=0.25",
                        "CTRADER_QUOTE_SOFT_STALE_TTL_SEC=25",
                        "CTRADER_QUOTE_BACKGROUND_REFRESH_ENABLED=true",
                        "CTRADER_CAPTURE_MAX_EVENTS=12",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            settings = Settings(_env_file=(str(env_path),))
            self.assertTrue(settings.hard_filter_adaptive_enabled)
            self.assertEqual(settings.hard_filter_adaptive_min_trades, 4)
            self.assertEqual(settings.hard_filter_adaptive_support_edge_min, 2)
            self.assertEqual(settings.hard_filter_adaptive_min_opportunity, 0.7)
            self.assertEqual(settings.hard_filter_adaptive_max_risk, 0.52)
            self.assertEqual(settings.hard_filter_adaptive_min_edge, 0.12)
            self.assertEqual(settings.hard_filter_adaptive_min_impulse_support, 0.71)
            self.assertEqual(settings.hard_filter_adaptive_max_loss_rate, 0.58)
            self.assertEqual(settings.hard_filter_adaptive_recent_window, 10)
            self.assertEqual(settings.hard_filter_adaptive_recent_min_samples, 5)
            self.assertEqual(settings.hard_filter_adaptive_recent_neg_edge_block, -0.3)
            self.assertEqual(settings.hard_filter_adaptive_recent_pos_edge_bonus, 0.25)
            self.assertEqual(settings.ctrader_quote_soft_stale_ttl_sec, 25.0)
            self.assertTrue(settings.ctrader_quote_background_refresh_enabled)
            self.assertEqual(settings.ctrader_capture_max_events, 12)

    def test_backtest_learning_settings_load_and_paths_normalize(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = (root / "runtime_data").resolve()
            dexter_root = (root / "dexter_source").resolve()
            env_path = root / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        f"DATA_DIR={data_dir}",
                        "BACKTEST_LEARNING_ENABLED=true",
                        "BACKTEST_LEARNING_INTERVAL_SEC=3600",
                        "BACKTEST_LEARNING_FAILURE_BACKOFF_SEC=600",
                        "BACKTEST_LEARNING_LOOKBACK_DAYS=14",
                        "BACKTEST_LEARNING_END_OFFSET_DAYS=1",
                        "BACKTEST_LEARNING_TIMEZONE=Asia/Bangkok",
                        "BACKTEST_LEARNING_SYMBOL=BTCUSD",
                        "BACKTEST_LEARNING_FALLBACK_SYMBOL=ETHUSD",
                        "BACKTEST_LEARNING_TIMEFRAME=15m",
                        "BACKTEST_LEARNING_SOURCE_POLICY=real_first",
                        f"BACKTEST_LEARNING_DEXTER_ROOT={dexter_root}",
                        "BACKTEST_LEARNING_OUTPUT_ROOT=./backtests_auto",
                        "BACKTEST_LEARNING_TIMEOUT_SEC=1500",
                        "BACKTEST_LEARNING_POLICY_APPLY_ENABLED=true",
                        "BACKTEST_LEARNING_POLICY_MAX_SHIFT=0.08",
                        "BACKTEST_LEARNING_MIN_CLOSED_TRADES=16",
                        "BACKTEST_LEARNING_MIN_WIN_RATE=0.56",
                        "BACKTEST_LEARNING_MIN_AVG_PROFIT=0.02",
                        "BACKTEST_LEARNING_MAX_DRAWDOWN=1.1",
                        "BACKTEST_LEARNING_POLICY_PATH=./backtest_learning_policy.json",
                        "BACKTEST_LEARNING_STATE_PATH=./backtest_learning_state.json",
                        "BACKTEST_LEARNING_SUMMARY_PATH=./backtest_learning_summary.md",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            settings = Settings(_env_file=(str(env_path),))

            self.assertTrue(settings.backtest_learning_enabled)
            self.assertEqual(settings.backtest_learning_interval_sec, 3600)
            self.assertEqual(settings.backtest_learning_failure_backoff_sec, 600)
            self.assertEqual(settings.backtest_learning_lookback_days, 14)
            self.assertEqual(settings.backtest_learning_end_offset_days, 1)
            self.assertEqual(settings.backtest_learning_timezone, "Asia/Bangkok")
            self.assertEqual(settings.backtest_learning_symbol, "BTCUSD")
            self.assertEqual(settings.backtest_learning_fallback_symbol, "ETHUSD")
            self.assertEqual(settings.backtest_learning_timeframe, "15m")
            self.assertEqual(settings.backtest_learning_source_policy, "real_first")
            self.assertEqual(settings.backtest_learning_timeout_sec, 1500)
            self.assertEqual(settings.backtest_learning_policy_max_shift, 0.08)
            self.assertEqual(settings.backtest_learning_min_closed_trades, 16)
            self.assertEqual(settings.backtest_learning_min_win_rate, 0.56)
            self.assertEqual(settings.backtest_learning_min_avg_profit, 0.02)
            self.assertEqual(settings.backtest_learning_max_drawdown, 1.1)
            self.assertEqual(settings.backtest_learning_dexter_root, dexter_root)
            self.assertEqual(settings.backtest_learning_output_root, (data_dir / "backtests_auto").resolve())
            self.assertEqual(
                settings.backtest_learning_policy_path,
                (data_dir / "backtest_learning_policy.json").resolve(),
            )
            self.assertEqual(
                settings.backtest_learning_state_path,
                (data_dir / "backtest_learning_state.json").resolve(),
            )
            self.assertEqual(
                settings.backtest_learning_summary_path,
                (data_dir / "backtest_learning_summary.md").resolve(),
            )


if __name__ == "__main__":
    unittest.main()
