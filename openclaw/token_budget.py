"""
openclaw/token_budget.py — Monthly token budget tracker

Tracks Qwen API token usage per model per month.
Prevents unexpected charges by switching to free fallbacks (Groq)
when approaching the free-tier quota.

State: data/runtime/token_budget.json
Reset: automatic on month change
Alert: Telegram at 80% usage
Block: auto-switch provider at 95%
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_BUDGET_FILE = Path(__file__).parent.parent / "data" / "runtime" / "token_budget.json"

# Default monthly budgets (tokens) — 90% of Alibaba free tier
_DEFAULT_BUDGETS = {
    "qwen-plus":  900_000,   # free tier: ~1M/month
    "qwen-turbo": 900_000,   # free tier: ~1M/month
}

_ALERT_THRESHOLD = 0.80   # notify at 80%
_BLOCK_THRESHOLD = 0.95   # switch provider at 95%


def _load() -> dict:
    try:
        if _BUDGET_FILE.exists():
            return json.loads(_BUDGET_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save(state: dict) -> None:
    try:
        _BUDGET_FILE.parent.mkdir(parents=True, exist_ok=True)
        _BUDGET_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.debug("[token_budget] save error: %s", exc)


def _current_month() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _get_budget(model: str) -> int:
    """Get configured monthly budget for a model."""
    try:
        from config import config
        key = f"QWEN_{model.upper().replace('-', '_')}_MONTHLY_BUDGET"
        val = getattr(config, key, None)
        if val is not None:
            return int(val)
    except Exception:
        pass
    return _DEFAULT_BUDGETS.get(model, 900_000)


def record_usage(model: str, input_tokens: int, output_tokens: int) -> None:
    """
    Record token usage after a successful API call.
    Sends Telegram alert if approaching budget.
    """
    state = _load()
    month = _current_month()

    # Reset if new month
    if state.get("month") != month:
        state = {"month": month, "models": {}}

    models = state.setdefault("models", {})
    entry = models.setdefault(model, {"input_tokens": 0, "output_tokens": 0, "calls": 0})
    entry["input_tokens"] += input_tokens
    entry["output_tokens"] += output_tokens
    entry["calls"] += 1
    total = entry["input_tokens"] + entry["output_tokens"]

    budget = _get_budget(model)
    pct = total / budget if budget else 0

    logger.debug("[token_budget] %s: %d/%d (%.1f%%)", model, total, budget, pct * 100)

    # Save state
    state["last_updated"] = datetime.now(timezone.utc).isoformat()
    _save(state)

    # Telegram alert at 80%
    if pct >= _ALERT_THRESHOLD and not state.get(f"alerted_{month}_{model}_80"):
        state[f"alerted_{month}_{model}_80"] = True
        _save(state)
        _send_alert(
            f"⚠️ *Qwen quota {int(pct*100)}%*\n"
            f"Model: `{model}`\n"
            f"Used: {total:,} / {budget:,} tokens\n"
            f"Auto-switch to Groq at 95%.",
        )


def is_blocked(model: str) -> bool:
    """Returns True if this model's budget is at/above block threshold → use fallback."""
    state = _load()
    month = _current_month()
    if state.get("month") != month:
        return False  # new month, reset
    entry = state.get("models", {}).get(model, {})
    total = entry.get("input_tokens", 0) + entry.get("output_tokens", 0)
    budget = _get_budget(model)
    return (total / budget) >= _BLOCK_THRESHOLD if budget else False


def get_status() -> dict:
    """Return current budget status for all tracked models."""
    state = _load()
    month = _current_month()
    if state.get("month") != month:
        return {"month": month, "models": {}}
    result = {"month": month, "models": {}}
    for model, entry in state.get("models", {}).items():
        budget = _get_budget(model)
        total = entry.get("input_tokens", 0) + entry.get("output_tokens", 0)
        result["models"][model] = {
            "used_tokens": total,
            "budget_tokens": budget,
            "pct": round(total / budget * 100, 1) if budget else 0,
            "calls": entry.get("calls", 0),
        }
    return result


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: 1 token ≈ 4 chars (English), 2 chars (Thai/Chinese)."""
    # Simple heuristic
    return max(1, len(text) // 3)


def _send_alert(text: str) -> None:
    try:
        from config import config
        from notifier.admin_bot import admin_bot
        chat_id = str(getattr(config, "TELEGRAM_CHAT_ID", "") or "").strip()
        if chat_id and chat_id.lstrip("-").isdigit():
            admin_bot._send_text(int(chat_id), text, parse_mode="Markdown")
    except Exception as exc:
        logger.debug("[token_budget] alert send error: %s", exc)
