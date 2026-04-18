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


if __name__ == "__main__":
    unittest.main()
