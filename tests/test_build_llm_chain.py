from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from trading_ai.config import Settings
from trading_ai.integrations.failover import FailoverProvider
from trading_ai.integrations.ollama import OllamaProvider
from trading_ai.main import build_llm


class BuildLlmChainTests(unittest.TestCase):
    def _settings_from_env(self, lines: list[str]) -> Settings:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return Settings(_env_file=(str(env_path),))

    def test_local_single_model_returns_direct_provider(self) -> None:
        settings = self._settings_from_env(
            [
                "LLM_PROVIDER=local",
                "LOCAL_MODEL_NAME=qwen2.5:1.5b",
                "LOCAL_FALLBACK_MODELS=",
            ]
        )
        with (
            patch("trading_ai.main.list_ollama_models", return_value=["qwen2.5:1.5b"]),
            patch(
                "trading_ai.main.select_available_models",
                return_value=(["qwen2.5:1.5b"], [], False),
            ),
        ):
            provider = build_llm(settings)
        self.assertIsInstance(provider, OllamaProvider)
        self.assertNotIsInstance(provider, FailoverProvider)

    def test_local_multi_model_returns_failover_provider(self) -> None:
        settings = self._settings_from_env(
            [
                "LLM_PROVIDER=local",
                "LOCAL_MODEL_NAME=qwen2.5:1.5b",
                "LOCAL_FALLBACK_MODELS=qwen2.5:0.5b",
            ]
        )
        with (
            patch("trading_ai.main.list_ollama_models", return_value=["qwen2.5:1.5b", "qwen2.5:0.5b"]),
            patch(
                "trading_ai.main.select_available_models",
                return_value=(["qwen2.5:1.5b", "qwen2.5:0.5b"], [], False),
            ),
        ):
            provider = build_llm(settings)
        self.assertIsInstance(provider, FailoverProvider)


if __name__ == "__main__":
    unittest.main()
