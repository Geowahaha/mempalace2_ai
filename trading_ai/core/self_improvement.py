from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional, Protocol

from trading_ai.core.memory import MemoryEngine, MemoryNote
from trading_ai.core.skillbook import SkillBook
from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


class SupportsCompleteJson(Protocol):
    async def complete_json(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        json_schema: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]: ...


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _outcome_label(score: int) -> str:
    if score > 0:
        return "win"
    if score < 0:
        return "loss"
    return "neutral"


def _review_has_required_fields(review: Dict[str, Any]) -> bool:
    if not isinstance(review, dict):
        return False
    for key in (
        "skill_key",
        "title",
        "summary",
        "use_when",
        "avoid_when",
        "guardrails",
        "confidence_rules",
        "team_notes",
    ):
        if key not in review:
            return False
    team_notes = review.get("team_notes")
    if not isinstance(team_notes, dict):
        return False
    for key in ("strategist", "risk_guardian", "execution", "learning"):
        if key not in team_notes:
            return False
    return True


class SelfImprovementEngine:
    """
    Distills closed-trade lessons into reusable procedural skills.

    This is the Hermes-inspired observe -> critique -> distill -> reuse loop for Mempalac.
    """

    def __init__(
        self,
        *,
        skillbook: SkillBook,
        memory: Optional[MemoryEngine] = None,
        llm: Optional[SupportsCompleteJson] = None,
        enabled: bool = True,
        store_notes: bool = True,
    ) -> None:
        self._skillbook = skillbook
        self._memory = memory
        self._llm = llm
        self._enabled = bool(enabled)
        self._store_notes = bool(store_notes)

    def _build_packet(
        self,
        *,
        close_context: Dict[str, Any],
        close_result: Dict[str, Any],
        score: int,
        strategy_state: Optional[Dict[str, Any]],
        room_guard: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        features = dict(close_context.get("features") or {})
        decision = dict(close_context.get("decision") or {})
        result = dict(close_result or {})
        return {
            "market": dict(close_context.get("market") or {}),
            "features": features,
            "decision": decision,
            "result": result,
            "score": int(score),
            "outcome_label": _outcome_label(int(score)),
            "setup_tag": str(close_context.get("setup_tag") or ""),
            "strategy_key": str(close_context.get("strategy_key") or ""),
            "room": str(close_context.get("strategy_key") or close_context.get("setup_tag") or ""),
            "active_skill_keys": list(close_context.get("active_skill_keys") or []),
            "team_brief": dict(close_context.get("team_brief") or {}),
            "strategy_state": dict(strategy_state or {}),
            "room_guard": dict(room_guard or {}),
            "created_ts": _safe_float(close_context.get("created_ts") or time.time(), time.time()),
            "symbol": str((close_context.get("market") or {}).get("symbol") or ""),
            "session": str(features.get("session") or ""),
            "trend_direction": str(features.get("trend_direction") or ""),
            "volatility": str(features.get("volatility") or ""),
            "action": str(decision.get("action") or ""),
            "confidence": _safe_float(decision.get("confidence")),
            "reason": str(decision.get("reason") or ""),
            "pnl": _safe_float(result.get("pnl")),
            "entry_price": _safe_float(result.get("entry_price")),
            "exit_price": _safe_float(result.get("exit_price")),
        }

    async def _llm_review(self, packet: Dict[str, Any]) -> Dict[str, Any]:
        assert self._llm is not None
        system = (
            "You are the Hermes-style self-improvement layer for an autonomous trading system. "
            "Review one closed trade, critique the decision path, and distill exactly one reusable procedural skill. "
            "Optimize for risk-adjusted expectancy, not aggression. "
            "Penalize overconfident losses, reward repeatable structure, and prefer explicit guardrails over vague advice. "
            "Do not claim guaranteed profits. Return JSON only."
        )
        user = json.dumps(
            {
                "task": "Distill a reusable trading skill from a closed trade.",
                "required_schema": {
                    "skill_key": "string",
                    "title": "string",
                    "summary": "string",
                    "use_when": ["string"],
                    "avoid_when": ["string"],
                    "guardrails": ["string"],
                    "confidence_rules": ["string"],
                    "team_notes": {
                        "strategist": ["string"],
                        "risk_guardian": ["string"],
                        "execution": ["string"],
                        "learning": ["string"],
                    },
                },
                "packet": packet,
            },
            ensure_ascii=False,
        )
        schema = {
            "type": "object",
            "properties": {
                "skill_key": {"type": "string"},
                "title": {"type": "string"},
                "summary": {"type": "string"},
                "use_when": {"type": "array", "items": {"type": "string"}},
                "avoid_when": {"type": "array", "items": {"type": "string"}},
                "guardrails": {"type": "array", "items": {"type": "string"}},
                "confidence_rules": {"type": "array", "items": {"type": "string"}},
                "team_notes": {
                    "type": "object",
                    "properties": {
                        "strategist": {"type": "array", "items": {"type": "string"}},
                        "risk_guardian": {"type": "array", "items": {"type": "string"}},
                        "execution": {"type": "array", "items": {"type": "string"}},
                        "learning": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["strategist", "risk_guardian", "execution", "learning"],
                    "additionalProperties": False,
                },
            },
            "required": [
                "skill_key",
                "title",
                "summary",
                "use_when",
                "avoid_when",
                "guardrails",
                "confidence_rules",
                "team_notes",
            ],
            "additionalProperties": True,
        }
        return await self._llm.complete_json(
            system=system,
            user=user,
            temperature=0.0,
            json_schema=schema,
        )

    def _heuristic_review(self, packet: Dict[str, Any]) -> Dict[str, Any]:
        outcome = str(packet.get("outcome_label") or "neutral")
        trend = str(packet.get("trend_direction") or "RANGE")
        volatility = str(packet.get("volatility") or "MEDIUM")
        session = str(packet.get("session") or "unknown-session")
        strategy_key = str(packet.get("strategy_key") or packet.get("setup_tag") or "general-learning")
        action = str(packet.get("action") or "HOLD")
        pnl = _safe_float((packet.get("result") or {}).get("pnl"))
        confidence = _safe_float(packet.get("confidence"))
        structure = dict((packet.get("features") or {}).get("structure") or {})

        use_when = []
        avoid_when = []
        guardrails = []
        confidence_rules = []
        strategist_notes = []
        risk_notes = []
        execution_notes = []
        learning_notes = []

        aligned = (trend == "UP" and action == "BUY") or (trend == "DOWN" and action == "SELL")
        if aligned:
            use_when.append("Action and trend direction are aligned.")
            strategist_notes.append("Prefer directional continuation when action agrees with the prevailing trend.")
        if not bool(structure.get("consolidation")):
            use_when.append("Structure is not consolidating.")
            execution_notes.append("Cleaner structure reduces false breakout noise.")
        if outcome == "win":
            use_when.append(f"Context repeated a profitable lane in {session} with {volatility} volatility.")
        if outcome == "loss":
            avoid_when.append(f"Do not treat {strategy_key} as strong edge yet in {session}.")
        if confidence >= 0.75 and outcome == "loss":
            avoid_when.append("Avoid high-conviction entries without stronger confirmation.")
            confidence_rules.append("Raise the evidence threshold before allowing full-size entries.")
            risk_notes.append("Overconfident losses should tighten the entry gate, not loosen it.")
        if confidence < 0.55 and outcome == "win":
            confidence_rules.append("Allow small probe entries when the same context repeats with clean structure.")
            learning_notes.append("This lane may be under-rated; study for shadow promotion rather than jumping to live.")
        if volatility == "LOW":
            guardrails.append("Keep HOLD bias when volatility compresses.")
            risk_notes.append("Low-volatility regimes deserve stricter selectivity.")
        if trend == "RANGE":
            guardrails.append("Do not force directional trades in range-bound conditions.")
        if bool(structure.get("consolidation")):
            guardrails.append("Require breakout confirmation before acting out of consolidation.")
        if outcome == "win":
            learning_notes.append("Promote only after repeated wins with acceptable drawdown.")
        elif outcome == "loss":
            learning_notes.append("Retain the lesson, but do not expand size until the lane repairs its expectancy.")
        else:
            learning_notes.append("Neutral outcome: keep collecting evidence before changing policy.")

        summary = (
            f"{outcome.upper()} on {strategy_key} during {session}: action={action} trend={trend} "
            f"volatility={volatility} confidence={confidence:.3f} pnl={pnl:.6f}."
        )
        return {
            "skill_key": strategy_key,
            "title": f"{outcome.title()} lesson for {strategy_key}",
            "summary": summary,
            "use_when": use_when,
            "avoid_when": avoid_when,
            "guardrails": guardrails,
            "confidence_rules": confidence_rules,
            "team_notes": {
                "strategist": strategist_notes,
                "risk_guardian": risk_notes,
                "execution": execution_notes,
                "learning": learning_notes,
            },
            "source": "heuristic_self_improvement",
        }

    def _store_skill_note(self, packet: Dict[str, Any], skill: Dict[str, Any]) -> None:
        if self._memory is None or not self._store_notes:
            return
        room = str(skill.get("skill_key") or packet.get("strategy_key") or "general-learning")
        try:
            self._memory.store_note(
                MemoryNote(
                    title=f"Agent skill updated: {skill.get('title')}",
                    content=self._skillbook.render_note_excerpt(skill),
                    wing="research",
                    hall="hall_discoveries",
                    room=room,
                    note_type="agent_skill",
                    hall_type="hall_discoveries",
                    symbol=str(packet.get("symbol") or ""),
                    session=str(packet.get("session") or ""),
                    setup_tag=str(packet.get("setup_tag") or ""),
                    strategy_key=str(packet.get("strategy_key") or ""),
                    importance=0.84,
                    source="self_improvement",
                    tags=["agent-skill", room, str(packet.get("outcome_label") or "neutral")],
                )
            )
        except Exception as exc:
            log.warning("SelfImprovement: failed to store skill note %s: %s", room, exc)

    async def learn_from_closed_trade(
        self,
        *,
        close_context: Dict[str, Any],
        close_result: Dict[str, Any],
        score: int,
        strategy_state: Optional[Dict[str, Any]] = None,
        room_guard: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self._enabled:
            return None
        packet = self._build_packet(
            close_context=close_context,
            close_result=close_result,
            score=score,
            strategy_state=strategy_state,
            room_guard=room_guard,
        )
        review: Dict[str, Any]
        if self._llm is not None:
            try:
                review = await self._llm_review(packet)
                if not _review_has_required_fields(review):
                    raise ValueError(
                        "self_improvement_invalid_schema:"
                        + ",".join(sorted(str(k) for k in review.keys()))
                    )
            except Exception as exc:
                log.warning("SelfImprovement: LLM review failed, fallback to heuristics: %s", exc)
                review = self._heuristic_review(packet)
        else:
            review = self._heuristic_review(packet)

        skill = self._skillbook.upsert_from_review(review=review, evidence=packet)
        self._store_skill_note(packet, skill)
        log.info(
            "SelfImprovement: updated skill=%s outcome=%s trades=%s edge=%.3f",
            skill.get("skill_key"),
            packet.get("outcome_label"),
            int(((skill.get("stats") or {}).get("trades_seen")) or 0),
            _safe_float((skill.get("stats") or {}).get("risk_adjusted_score")),
        )
        return skill
