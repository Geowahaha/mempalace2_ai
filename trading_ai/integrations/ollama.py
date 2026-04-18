from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

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


def _normalize_ollama_base_url(base_url: Optional[str]) -> str:
    root = str(base_url or "").strip().rstrip("/")
    if not root:
        return "http://127.0.0.1:11434"
    if root.lower().endswith("/v1"):
        root = root[:-3].rstrip("/")
    return root or "http://127.0.0.1:11434"


class OllamaProvider:
    """Native Ollama chat API provider with thinking and context controls."""

    def __init__(
        self,
        *,
        api_base_url: str,
        model: str,
        timeout_sec: float = 120.0,
        max_retries: int = 4,
        max_tokens: int = 256,
        num_ctx: Optional[int] = None,
        keep_alive: Optional[str] = None,
        think: Optional[bool] = False,
    ) -> None:
        self._api_url = _normalize_ollama_base_url(api_base_url) + "/api/chat"
        self._model = str(model).strip()
        self._timeout_sec = max(5.0, float(timeout_sec))
        self._max_retries = max(1, int(max_retries))
        self._max_tokens = max(32, int(max_tokens))
        self._num_ctx = int(num_ctx) if num_ctx else None
        self._keep_alive = str(keep_alive).strip() if keep_alive is not None else None
        self._think = think

    def _build_payload(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        json_schema: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        options: Dict[str, Any] = {
            "temperature": float(temperature),
            "num_predict": self._max_tokens,
        }
        if self._num_ctx is not None:
            options["num_ctx"] = int(self._num_ctx)

        payload: Dict[str, Any] = {
            "model": self._model,
            "stream": False,
            "format": json_schema if json_schema is not None else "json",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "options": options,
        }
        if self._keep_alive:
            payload["keep_alive"] = self._keep_alive
        if self._think is not None:
            payload["think"] = bool(self._think)
        return payload

    def _post_json(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._api_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout_sec) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"Ollama HTTP {exc.code}: {detail or exc.reason}") from exc
        except urllib.error.URLError as exc:
            reason = getattr(exc, "reason", exc)
            raise RuntimeError(f"Ollama request failed: {reason}") from exc
        try:
            return json.loads(raw or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Ollama returned invalid JSON payload: {raw[:500]}") from exc

    async def complete_json(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        json_schema: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        delay_sec = 1.0
        last_exc: Optional[Exception] = None
        payload = self._build_payload(
            system=system,
            user=user,
            temperature=temperature,
            json_schema=json_schema,
        )
        for attempt in range(self._max_retries):
            try:
                response = await asyncio.to_thread(self._post_json, payload)
                message = dict(response.get("message") or {})
                content = str(message.get("content") or "").strip()
                if not content:
                    thinking = str(message.get("thinking") or "").strip()
                    done_reason = str(response.get("done_reason") or "unknown").strip()
                    if thinking:
                        raise RuntimeError(
                            f"Ollama returned thinking_only output for {self._model} "
                            f"(done_reason={done_reason})"
                        )
                    raise RuntimeError(
                        f"Ollama returned empty content for {self._model} "
                        f"(done_reason={done_reason})"
                    )
                try:
                    return _extract_json_object(content)
                except json.JSONDecodeError:
                    log.error("Ollama returned non-JSON: %s", content[:500])
                    raise
            except Exception as exc:
                message = str(exc)
                if "HTTP 400" in message or "HTTP 404" in message or "HTTP 422" in message:
                    log.warning("Ollama non-retryable API error: %s", exc)
                    raise
                last_exc = exc
                log.warning(
                    "Ollama transient error (attempt %s/%s) model=%s: %s - sleeping %.1fs",
                    attempt + 1,
                    self._max_retries,
                    self._model,
                    exc,
                    delay_sec,
                )
                if attempt + 1 >= self._max_retries:
                    break
                await asyncio.sleep(delay_sec)
                delay_sec = min(delay_sec * 2.0, 15.0)
        assert last_exc is not None
        raise last_exc
