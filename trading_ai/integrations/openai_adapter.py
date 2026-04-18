from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional

from openai import APIError, AsyncOpenAI, RateLimitError

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


def _extract_json_object(content: str) -> Dict[str, Any]:
    text = (content or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start : end + 1])
        raise


class OpenAIProvider:
    """OpenAI or any OpenAI-compatible HTTP API (local vLLM, LiteLLM, etc.)."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: Optional[str] = None,
        timeout_sec: float = 120.0,
        max_retries: int = 4,
        max_tokens: int = 256,
    ) -> None:
        self._client = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout_sec,
        )
        self._model = model
        self._max_retries = max(1, int(max_retries))
        self._max_tokens = max(32, int(max_tokens))

    async def complete_json(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        json_schema: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """
        Request JSON object response. Retries on rate limits / transient API errors.
        """
        delay_sec = 1.0
        last_exc: Optional[Exception] = None
        for attempt in range(self._max_retries):
            try:
                resp = await self._client.chat.completions.create(
                    model=self._model,
                    temperature=temperature,
                    max_tokens=self._max_tokens,
                    response_format=(
                        {
                            "type": "json_schema",
                            "json_schema": {
                                "name": "structured_response",
                                "schema": json_schema,
                            },
                        }
                        if json_schema
                        else {"type": "json_object"}
                    ),
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                )
                content = resp.choices[0].message.content or "{}"
                try:
                    return _extract_json_object(content)
                except json.JSONDecodeError:
                    log.error("LLM returned non-JSON: %s", content[:500])
                    raise
            except (RateLimitError, APIError) as exc:
                status_code = getattr(exc, "status_code", None)
                if status_code is not None and int(status_code) < 500 and int(status_code) != 429:
                    log.warning("LLM non-retryable API error: %s", exc)
                    raise
                last_exc = exc
                log.warning(
                    "LLM transient error (attempt %s/%s): %s - sleeping %.1fs",
                    attempt + 1,
                    self._max_retries,
                    exc,
                    delay_sec,
                )
                if attempt + 1 >= self._max_retries:
                    break
                await asyncio.sleep(delay_sec)
                delay_sec = min(delay_sec * 2.0, 15.0)
            except Exception as exc:
                log.warning("LLM call failed: %s", exc)
                raise
        assert last_exc is not None
        raise last_exc
