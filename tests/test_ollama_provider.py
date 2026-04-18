from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import patch

from trading_ai.integrations.ollama import OllamaProvider


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class OllamaProviderTests(unittest.TestCase):
    def test_provider_normalizes_v1_base_url_and_builds_payload(self) -> None:
        captured = {}

        def fake_urlopen(request, timeout):
            captured["url"] = request.full_url
            captured["timeout"] = timeout
            captured["body"] = json.loads(request.data.decode("utf-8"))
            return _FakeResponse(
                {
                    "message": {
                        "content": "```json\n{\"action\":\"HOLD\",\"confidence\":0.42,\"reason\":\"ok\"}\n```"
                    },
                    "done_reason": "stop",
                }
            )

        provider = OllamaProvider(
            api_base_url="http://127.0.0.1:11434/v1",
            model="gemma4:e2b",
            timeout_sec=12,
            max_retries=1,
            max_tokens=120,
            num_ctx=512,
            keep_alive="0s",
            think=False,
        )

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            result = asyncio.run(
                provider.complete_json(system="system prompt", user="user prompt", temperature=0.15)
            )

        self.assertEqual(captured["url"], "http://127.0.0.1:11434/api/chat")
        self.assertEqual(captured["timeout"], 12.0)
        self.assertEqual(captured["body"]["model"], "gemma4:e2b")
        self.assertEqual(captured["body"]["format"], "json")
        self.assertFalse(captured["body"]["think"])
        self.assertEqual(captured["body"]["keep_alive"], "0s")
        self.assertEqual(captured["body"]["options"]["num_ctx"], 512)
        self.assertEqual(captured["body"]["options"]["num_predict"], 120)
        self.assertAlmostEqual(captured["body"]["options"]["temperature"], 0.15, places=6)
        self.assertEqual(result["action"], "HOLD")
        self.assertAlmostEqual(result["confidence"], 0.42, places=6)

    def test_provider_sends_schema_format_when_requested(self) -> None:
        captured = {}

        def fake_urlopen(request, timeout):
            captured["body"] = json.loads(request.data.decode("utf-8"))
            return _FakeResponse(
                {
                    "message": {"content": "{\"skill_key\":\"lane\",\"title\":\"t\",\"summary\":\"s\",\"use_when\":[],\"avoid_when\":[],\"guardrails\":[],\"confidence_rules\":[],\"team_notes\":{\"strategist\":[],\"risk_guardian\":[],\"execution\":[],\"learning\":[]}}"},
                    "done_reason": "stop",
                }
            )

        provider = OllamaProvider(
            api_base_url="http://127.0.0.1:11434",
            model="gemma4:e2b",
            timeout_sec=12,
            max_retries=1,
            max_tokens=120,
            think=False,
        )
        schema = {
            "type": "object",
            "properties": {"skill_key": {"type": "string"}},
            "required": ["skill_key"],
        }

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            result = asyncio.run(
                provider.complete_json(system="system", user="user", json_schema=schema)
            )

        self.assertEqual(captured["body"]["format"], schema)
        self.assertEqual(result["skill_key"], "lane")

    def test_provider_raises_on_thinking_only_output(self) -> None:
        def fake_urlopen(request, timeout):
            return _FakeResponse(
                {
                    "message": {"content": "", "thinking": "hidden chain"},
                    "done_reason": "length",
                }
            )

        provider = OllamaProvider(
            api_base_url="http://127.0.0.1:11434",
            model="gemma4:e2b",
            timeout_sec=12,
            max_retries=1,
            max_tokens=120,
            think=False,
        )

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(provider.complete_json(system="system", user="user"))

        self.assertIn("thinking_only", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
