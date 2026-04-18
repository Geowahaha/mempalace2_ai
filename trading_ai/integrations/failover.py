from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


class FailoverProvider:
    """Try multiple LLM backends in order until one returns valid JSON."""

    def __init__(self, providers: Iterable[Tuple[str, Any]]) -> None:
        self._providers: List[Tuple[str, Any]] = list(providers)
        if not self._providers:
            raise ValueError("FailoverProvider requires at least one provider")

    async def complete_json(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        json_schema: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        errors: List[str] = []
        for label, provider in self._providers:
            try:
                result = await provider.complete_json(
                    system=system,
                    user=user,
                    temperature=temperature,
                    json_schema=json_schema,
                )
                if errors:
                    log.info("LLM failover recovered via %s after %s prior errors", label, len(errors))
                return result
            except Exception as exc:
                msg = f"{label}: {type(exc).__name__}: {exc}"
                errors.append(msg)
                log.warning("LLM candidate failed: %s", msg)
        raise RuntimeError("All LLM candidates failed | " + " | ".join(errors))
