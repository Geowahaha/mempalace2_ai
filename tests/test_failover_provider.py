from __future__ import annotations

import asyncio
import time
import unittest

from trading_ai.integrations.failover import (
    FailoverProvider,
    clear_failover_runtime_registry,
    failover_runtime_snapshot,
)


class _AlwaysFailProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def complete_json(self, **_: object):  # type: ignore[override]
        self.calls += 1
        raise RuntimeError("boom")


class _ConstantProvider:
    def __init__(self, payload: dict[str, object]) -> None:
        self.calls = 0
        self.payload = payload

    async def complete_json(self, **_: object):  # type: ignore[override]
        self.calls += 1
        return dict(self.payload)


class FailoverProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_failover_runtime_registry()

    def test_failover_records_failure_then_recovery(self) -> None:
        fail = _AlwaysFailProvider()
        ok = _ConstantProvider({"action": "HOLD", "confidence": 0.2, "reason": "ok"})
        provider = FailoverProvider(
            [("primary", fail), ("backup", ok)],
            name="unit-primary-chain",
            failure_threshold=2,
            cooldown_sec=30,
        )

        result = asyncio.run(provider.complete_json(system="s", user="u"))

        self.assertEqual(result["action"], "HOLD")
        snapshot = provider.snapshot()
        candidates = {item["label"]: item for item in snapshot["candidates"]}
        self.assertEqual(candidates["primary"]["failures"], 1)
        self.assertEqual(candidates["backup"]["successes"], 1)
        registry = failover_runtime_snapshot()
        self.assertIn("unit-primary-chain", registry)

    def test_circuit_breaker_skips_open_candidate(self) -> None:
        fail = _AlwaysFailProvider()
        ok = _ConstantProvider({"action": "HOLD", "confidence": 0.5, "reason": "fallback"})
        provider = FailoverProvider(
            [("broken", fail), ("healthy", ok)],
            name="unit-circuit-chain",
            failure_threshold=1,
            cooldown_sec=120,
        )

        asyncio.run(provider.complete_json(system="s", user="u"))
        asyncio.run(provider.complete_json(system="s", user="u"))

        self.assertEqual(fail.calls, 1, "Second request should skip broken candidate due open circuit")
        snapshot = provider.snapshot()
        candidates = {item["label"]: item for item in snapshot["candidates"]}
        self.assertGreaterEqual(candidates["broken"]["skipped_circuit_open"], 1)
        self.assertTrue(candidates["broken"]["circuit_open"])
        self.assertEqual(candidates["healthy"]["successes"], 2)

    def test_failure_cooldown_scales_with_consecutive_failures(self) -> None:
        fail = _AlwaysFailProvider()
        provider = FailoverProvider(
            [("broken", fail)],
            name="unit-dynamic-cooldown",
            failure_threshold=1,
            cooldown_sec=10,
        )

        first_open_until = provider._mark_failure("broken", RuntimeError("boom"), 1.0)
        second_open_until = provider._mark_failure("broken", RuntimeError("boom"), 1.0)

        first_wait = first_open_until - time.time()
        second_wait = second_open_until - time.time()
        self.assertGreater(second_wait, first_wait + 8.0)

    def test_model_not_found_uses_long_cooldown_override(self) -> None:
        fail = _AlwaysFailProvider()
        provider = FailoverProvider(
            [("broken", fail)],
            name="unit-not-found-cooldown",
            failure_threshold=1,
            cooldown_sec=20,
        )

        open_until = provider._mark_failure(
            "broken",
            RuntimeError("Ollama HTTP 404: {\"error\":\"model 'foo' not found\"}"),
            1.0,
            cooldown_override_sec=600.0,
        )
        remaining = open_until - time.time()
        self.assertGreaterEqual(remaining, 590.0)


if __name__ == "__main__":
    unittest.main()
