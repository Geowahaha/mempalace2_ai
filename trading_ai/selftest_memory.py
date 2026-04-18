from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict

from fastapi.testclient import TestClient

from trading_ai.config import load_settings, memory_persist_path
from trading_ai.core.memory import MemoryEngine, MemoryNote, MemoryRecord
from trading_ai.core.strategy_evolution import StrategyRegistry


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _store_trade(
    memory: MemoryEngine,
    registry: StrategyRegistry,
    *,
    strategy_key: str,
    symbol: str,
    session: str,
    trend: str,
    volatility: str,
    setup_tag: str,
    action: str,
    confidence: float,
    score: int,
    pnl: float,
    count: int,
) -> None:
    for i in range(count):
        memory.store_memory(
            MemoryRecord(
                market={"symbol": symbol, "bid": 3300.0 + i, "ask": 3300.5 + i},
                features={
                    "symbol": symbol,
                    "session": session,
                    "trend_direction": trend,
                    "volatility": volatility,
                },
                decision={
                    "action": action,
                    "confidence": confidence,
                    "reason": f"selftest {strategy_key} sample {i}",
                },
                result={"pnl": pnl},
                score=score,
                setup_tag=setup_tag,
                strategy_key=strategy_key,
                wing=f"symbol:{symbol.lower()}",
                hall="hall_events",
                room=strategy_key,
                journal=f"Self-test journal for {strategy_key} sample {i}",
                tags=["selftest", setup_tag],
            )
        )
        registry.update_strategy(strategy_key, {"pnl": pnl, "score": score})


def run_selftest() -> Dict[str, Any]:
    tmpdir = Path(tempfile.gettempdir()) / "mempalace_memory_selftest"
    if tmpdir.exists():
        shutil.rmtree(tmpdir)
    tmpdir.mkdir(parents=True, exist_ok=True)

    old_data_dir = os.environ.get("DATA_DIR")
    old_registry = os.environ.get("STRATEGY_REGISTRY_PATH")
    os.environ["DATA_DIR"] = str(tmpdir)
    os.environ["STRATEGY_REGISTRY_PATH"] = str(tmpdir / "strategy_registry.json")
    try:
        settings = load_settings()
        memory = MemoryEngine(
            persist_path=memory_persist_path(settings),
            collection_name=settings.memory_collection,
            score_weight=settings.memory_score_weight,
        )
        registry = StrategyRegistry(Path(settings.strategy_registry_path))

        winner = "UP*HIGH*NY_breakout"
        danger = "RANGE*LOW*ASIA_trend_follow"
        opportunity = "UP*MEDIUM*LONDON_pullback"

        _store_trade(
            memory,
            registry,
            strategy_key=winner,
            symbol="XAUUSD",
            session="NY",
            trend="UP",
            volatility="HIGH",
            setup_tag="breakout",
            action="BUY",
            confidence=0.86,
            score=1,
            pnl=21.0,
            count=5,
        )
        _store_trade(
            memory,
            registry,
            strategy_key=danger,
            symbol="XAUUSD",
            session="ASIA",
            trend="RANGE",
            volatility="LOW",
            setup_tag="trend_follow",
            action="BUY",
            confidence=0.9,
            score=-1,
            pnl=-14.0,
            count=4,
        )
        _store_trade(
            memory,
            registry,
            strategy_key=opportunity,
            symbol="XAUUSD",
            session="LONDON",
            trend="UP",
            volatility="MEDIUM",
            setup_tag="pullback",
            action="BUY",
            confidence=0.52,
            score=1,
            pnl=9.0,
            count=3,
        )

        registry.set_lane_stage(winner, "candidate")
        registry.set_lane_stage(danger, "live")
        registry.set_lane_stage(opportunity, "shadow")

        memory.store_note(
            MemoryNote(
                title="Execution self-test slippage note",
                content="Synthetic execution note for self-test. This must appear in execution wing.",
                wing="execution",
                hall="hall_preferences",
                room="rollover-slippage",
                note_type="execution_note",
                hall_type="hall_preferences",
                symbol="XAUUSD",
                importance=0.8,
                source="selftest",
                tags=["selftest", "execution"],
            )
        )
        memory.store_note(
            MemoryNote(
                title="Research self-test winner lane",
                content="Synthetic research note for self-test. This must appear in wake-up context.",
                wing="research",
                hall="hall_discoveries",
                room=winner,
                note_type="research_note",
                hall_type="hall_discoveries",
                symbol="XAUUSD",
                session="NY",
                strategy_key=winner,
                importance=0.88,
                source="selftest",
                tags=["selftest", "research"],
            )
        )

        intelligence = memory.get_memory_intelligence()
        winner_names = {str(item.get("name")) for item in intelligence.get("winner_rooms") or []}
        danger_names = {str(item.get("name")) for item in intelligence.get("danger_rooms") or []}
        anti_names = {str(item.get("name")) for item in intelligence.get("anti_pattern_rooms") or []}
        opportunity_names = {str(item.get("name")) for item in intelligence.get("opportunity_rooms") or []}
        recommendations = {
            str(item.get("strategy_key")): str(item.get("recommendation"))
            for item in intelligence.get("promotion_pipeline") or []
        }

        _assert(winner in winner_names, "winner room missing")
        _assert(danger in danger_names, "danger room missing")
        _assert(danger in anti_names, "anti-pattern room missing")
        _assert(opportunity in opportunity_names, "opportunity room missing")
        _assert(recommendations.get(winner) == "promote_to_shadow", "winner promotion hint missing")

        guard = memory.get_room_guardrail(
            symbol="XAUUSD",
            session="ASIA",
            setup_tag="trend_follow",
            trend_direction="RANGE",
            volatility="LOW",
            strategy_key=danger,
        )
        _assert(bool(guard.get("blocked")), "anti-pattern guard did not block")

        wakeup = memory.build_wake_up_context(symbol="XAUUSD", session="NY", top_k=3, note_top_k=3)
        _assert("L1 palace notes" in wakeup, "wake-up context missing palace notes")
        _assert("Synthetic research note" in wakeup, "research note missing from wake-up context")

        registry.sync_promotion_hints(intelligence.get("promotion_pipeline") or [])
        snapshot = registry.promotion_snapshot()
        _assert(
            any(row["strategy_key"] == winner and row["pending_recommendation"] for row in snapshot),
            "promotion snapshot missing pending recommendation",
        )

        from trading_ai.api import app, get_memory, get_settings

        get_settings.cache_clear()
        get_memory.cache_clear()
        client = TestClient(app)
        _assert(client.get("/memory/intelligence").status_code == 200, "API intelligence failed")
        _assert(client.get("/memory/analyst-packet").status_code == 200, "API analyst packet failed")
        _assert(client.get("/strategy/promotions").status_code == 200, "API promotions failed")

        return {
            "ok": True,
            "tmpdir": str(tmpdir),
            "memory_count": memory.count(),
            "winner_rooms": sorted(winner_names),
            "danger_rooms": sorted(danger_names),
            "anti_pattern_rooms": sorted(anti_names),
            "opportunity_rooms": sorted(opportunity_names),
            "promotion_pipeline": intelligence.get("promotion_pipeline") or [],
            "guard_blocked": guard.get("blocked"),
            "api_checked": True,
        }
    finally:
        if old_data_dir is None:
            os.environ.pop("DATA_DIR", None)
        else:
            os.environ["DATA_DIR"] = old_data_dir
        if old_registry is None:
            os.environ.pop("STRATEGY_REGISTRY_PATH", None)
        else:
            os.environ["STRATEGY_REGISTRY_PATH"] = old_registry


def main() -> None:
    print(json.dumps(run_selftest(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
