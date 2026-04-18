from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List

from trading_ai.config import load_settings, memory_persist_path
from trading_ai.core.memory import MemoryEngine, MemoryNote
from trading_ai.core.strategy_evolution import StrategyRegistry
from trading_ai.integrations.mimo import MiMoProvider
from trading_ai.main import build_skillbook
from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


def build_memory() -> MemoryEngine:
    settings = load_settings()
    return MemoryEngine(
        persist_path=memory_persist_path(settings),
        collection_name=settings.memory_collection,
        score_weight=settings.memory_score_weight,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the daily MiMo analyst over MemPalace trading memory")
    p.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Print the analyst output without storing hall_advice notes.",
    )
    return p.parse_args()


async def run_daily_analyst(*, dry_run: bool = False) -> Dict[str, Any]:
    settings = load_settings()
    if not settings.mimo_api_key:
        raise RuntimeError("MIMO_API_KEY or MIMO_API is required for daily analyst mode")

    memory = build_memory()
    packet = memory.build_daily_analyst_packet()
    registry = StrategyRegistry(Path(settings.strategy_registry_path))
    packet["strategy_promotions"] = registry.promotion_snapshot()
    packet["skills"] = build_skillbook(settings).list_skills(limit=20)

    provider = MiMoProvider(
        api_key=settings.mimo_api_key,
        model=settings.mimo_model,
        base_url=settings.mimo_base_url,
        timeout_sec=settings.llm_timeout_sec,
        max_retries=1,
    )
    system = (
        "You are a trading systems analyst. Review the memory packet and return JSON only. "
        "Do not give discretionary live trading orders. "
        "You only propose lane promotion, lane demotion, risk-rule adjustments, and recurring execution issues. "
        "Every recommendation must cite concrete rooms or lanes from the packet."
    )
    user = json.dumps(
        {
            "task": "Review the daily trading memory and propose improvements.",
            "required_schema": {
                "summary": "short string",
                "promote_to_shadow": [{"lane": "string", "reason": "string"}],
                "promote_to_live": [{"lane": "string", "reason": "string"}],
                "demote_to_lab": [{"lane": "string", "reason": "string"}],
                "risk_rules": ["string"],
                "execution_issues": ["string"],
                "rooms_to_watch": ["string"],
            },
            "packet": packet,
        },
        ensure_ascii=False,
    )
    result = await provider.complete_json(system=system, user=user, temperature=0.1)
    if dry_run:
        return result

    for key, rec in (
        ("promote_to_shadow", "promote_to_shadow"),
        ("promote_to_live", "promote_to_live"),
        ("demote_to_lab", "demote_to_lab"),
    ):
        for row in list(result.get(key) or []):
            lane = str((row or {}).get("lane") or "").strip()
            if lane:
                registry.set_pending_recommendation(lane, rec)

    summary = str(result.get("summary") or "MiMo daily analyst review")
    tags: List[str] = ["mimo", "daily-analyst"]
    for key in ("promote_to_shadow", "promote_to_live", "demote_to_lab"):
        for row in list(result.get(key) or [])[:5]:
            lane = str((row or {}).get("lane") or "").strip()
            if lane:
                tags.append(lane)

    memory.store_note(
        MemoryNote(
            title="MiMo daily analyst review",
            content=json.dumps(result, ensure_ascii=False, indent=2),
            wing="research",
            hall="hall_advice",
            room="daily-analyst-review",
            note_type="analyst_review",
            hall_type="hall_advice",
            source="mimo_daily_analyst",
            importance=0.92,
            tags=tags,
        )
    )
    memory.store_note(
        MemoryNote(
            title="MiMo daily analyst summary",
            content=summary,
            wing="research",
            hall="hall_advice",
            room="daily-analyst-summary",
            note_type="analyst_summary",
            hall_type="hall_advice",
            source="mimo_daily_analyst",
            importance=0.88,
            tags=tags,
        )
    )
    return result


def main() -> None:
    args = parse_args()
    result = asyncio.run(run_daily_analyst(dry_run=bool(args.dry_run)))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
