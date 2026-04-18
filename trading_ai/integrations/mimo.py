from __future__ import annotations

from trading_ai.integrations.openai_adapter import OpenAIProvider


class MiMoProvider(OpenAIProvider):
    """
    MiMo (or other OpenAI-compatible endpoints) — same wire protocol as OpenAI chat.completions.

    Configure MIMO_BASE_URL and MIMO_API_KEY in the environment.
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str,
        timeout_sec: float = 120.0,
        max_retries: int = 4,
        max_tokens: int = 256,
    ) -> None:
        super().__init__(
            api_key=api_key,
            model=model,
            base_url=base_url,
            timeout_sec=timeout_sec,
            max_retries=max_retries,
            max_tokens=max_tokens,
        )
