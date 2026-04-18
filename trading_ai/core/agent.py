from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, runtime_checkable

from trading_ai.config import Settings
from trading_ai.core.execution import Action, MarketSnapshot
from trading_ai.core.memory import RecallHit
from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


@dataclass(slots=True)
class Decision:
    action: Action
    confidence: float
    reason: str
    raw: Dict[str, Any]

    @staticmethod
    def from_llm_payload(data: Dict[str, Any]) -> "Decision":
        if isinstance(data, list):
            if data and isinstance(data[0], dict):
                data = dict(data[0])
            else:
                data = {}
        elif not isinstance(data, dict):
            data = {}
        action_raw = str(data.get("action", "HOLD")).upper()
        action = action_raw
        if action not in ("BUY", "SELL", "HOLD"):
            data["_invalid_action"] = action_raw
            action = "HOLD"
        raw_conf = data.get("confidence", 0.0)
        if isinstance(raw_conf, str):
            token = raw_conf.strip().lower()
            alias = {
                "low": 0.35,
                "medium": 0.65,
                "med": 0.65,
                "high": 0.85,
            }
            if token.endswith("%"):
                try:
                    raw_conf = float(token[:-1].strip()) / 100.0
                except ValueError:
                    raw_conf = alias.get(token, raw_conf)
            else:
                raw_conf = alias.get(token, raw_conf)
        try:
            conf = float(raw_conf)
        except (TypeError, ValueError):
            conf = 0.0
        if conf > 1.0 and conf <= 100.0:
            conf = conf / 100.0
        conf = max(0.0, min(1.0, conf))
        reason = str(data.get("reason", "")).strip() or "no_reason"
        return Decision(action=action, confidence=conf, reason=reason, raw=data)


@runtime_checkable
class LLMClient(Protocol):
    async def complete_json(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        json_schema: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]: ...


def apply_confidence_floor(decision: Decision, floor: float) -> Decision:
    """Force HOLD when confidence is below minimum threshold."""
    if decision.action == "HOLD":
        return decision
    if decision.confidence < floor:
        r = dict(decision.raw)
        r["_confidence_gate"] = f"below_{floor}"
        return Decision(
            action="HOLD",
            confidence=decision.confidence,
            reason=f"low_confidence_floor({decision.confidence:.3f}<{floor})",
            raw=r,
        )
    return decision


class TradingAgent:
    """Structured features + similar-trade recall + risk + pattern intelligence → JSON decision."""

    def __init__(
        self,
        llm: LLMClient,
        settings: Settings,
    ) -> None:
        self._llm = llm
        self._settings = settings

    def _render_top_similar(self, hits: List[RecallHit], limit: int = 5) -> str:
        lines: List[str] = []
        for i, h in enumerate(hits[:limit], start=1):
            excerpt = h.document[:320].replace("\n", " ")
            lines.append(
                f"{i}. rank={h.weighted_score:.3f} sim={h.similarity:.3f} meta={h.metadata}\n   {excerpt}"
            )
        return "\n".join(lines) if lines else "(no matching structured memories)"

    def _build_user_payload(
        self,
        market: MarketSnapshot,
        *,
        features: Dict[str, Any],
        similar_trades: List[RecallHit],
        risk_state: Dict[str, Any],
        pattern_analysis: Dict[str, Any],
        wake_up_context: str,
        skill_context: str,
        team_brief: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "instruction": (
                "Act as a professional day trader with strict risk management. "
                "Only trade when high-probability conditions exist: clear directional bias, "
                "favorable session/volatility context, supportive similar past outcomes, "
                "pattern_analysis that does not contradict the trade, and procedural skills that support the setup. "
                "If pattern win_rate is weak or sample_size is thin for the contemplated setup, lower confidence first "
                "instead of defaulting to HOLD when trend, volatility, and structure are otherwise unusually clear. "
                "If memory is sparse or no close-matching trades exist yet, you may still take a small bootstrap trade "
                "when market structure, trend, and session alignment are unusually clear. "
                "Do not choose HOLD only because procedural_skill_context is sparse. "
                "Output a single JSON object only."
            ),
            "decision_rules": [
                "Rule priority is risk block, hard market block, directional opportunity, memory/pattern modifier.",
                "If risk_state.can_trade is false, action must be HOLD.",
                "If team_brief.risk_guardian reports blocked=true or can_trade=false, action must be HOLD.",
                "If trend_direction is RANGE, volatility is LOW, or structure.consolidation is true, action must be HOLD.",
                "If trend_direction is UP, volatility is MEDIUM or HIGH, and structure.consolidation is false, BUY may be valid when memory and pattern data do not contradict it.",
                "If trend_direction is DOWN, volatility is MEDIUM or HIGH, and structure.consolidation is false, SELL may be valid when memory and pattern data do not contradict it.",
                "Never choose BUY against a DOWN trend. Never choose SELL against an UP trend.",
                "Sparse skills, sparse memory, or thin pattern sample alone are not contradictions; they should usually reduce confidence instead of forcing HOLD.",
                "Lower confidence when memory is sparse, pattern sample_size is thin, top_similar_trades are mixed, or skill_context contains avoid_when / guardrails that match the current market.",
                "Treat team_brief as a four-seat council: strategist, memory_librarian, risk_guardian, evolution_coach. If they materially disagree, prefer HOLD.",
            ],
            "output_contract": (
                "Return exactly one JSON object. "
                "action must be exactly one string from this set: BUY, SELL, HOLD. "
                "confidence must be a numeric float between 0 and 1, not a percent string. "
                "reason must be a short explanation. "
                "Do not copy the schema text or output BUY|SELL|HOLD as the action."
            ),
            "valid_actions": ["BUY", "SELL", "HOLD"],
            "required_json_keys": ["action", "confidence", "reason"],
            "current_market": market.as_prompt_dict(),
            "structured_features": features,
            "memory_wakeup_context": wake_up_context,
            "procedural_skill_context": skill_context,
            "team_brief": team_brief,
            "top_similar_trades": self._render_top_similar(similar_trades, limit=5),
            "risk_state": risk_state,
            "pattern_analysis": pattern_analysis,
        }

    def _decision_json_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {"type": "string", "enum": ["BUY", "SELL", "HOLD"]},
                "confidence": {"type": "number"},
                "reason": {"type": "string"},
            },
            "required": ["action", "confidence", "reason"],
            "additionalProperties": True,
        }

    def _heuristic_fallback_decision(
        self,
        *,
        features: Dict[str, Any],
        similar_trades: List[RecallHit],
        risk_state: Dict[str, Any],
        pattern_analysis: Dict[str, Any],
        error: Exception,
    ) -> Decision:
        trend = str(features.get("trend_direction") or "RANGE").upper()
        volatility = str(features.get("volatility") or "LOW").upper()
        structure = dict(features.get("structure") or {})
        spread_pct = float(features.get("spread_pct") or 0.0)
        sample_len = int(features.get("sample_closes_len") or 0)
        similar_bias = {"BUY": 0, "SELL": 0}

        for hit in similar_trades[:5]:
            try:
                body = json.loads(hit.document)
            except Exception:
                continue
            decision = body.get("decision") or {}
            action = str(decision.get("action") or "").upper()
            score = int(float(body.get("score", 0) or 0))
            if action in similar_bias and score > 0:
                similar_bias[action] += 1

        if not bool(risk_state.get("can_trade", True)):
            return Decision(
                action="HOLD",
                confidence=0.0,
                reason=f"heuristic_fallback:risk_block llm_error={type(error).__name__}",
                raw={"fallback": True},
            )
        if trend not in ("UP", "DOWN"):
            return Decision(
                action="HOLD",
                confidence=0.0,
                reason=f"heuristic_fallback:trend_{trend.lower()} llm_error={type(error).__name__}",
                raw={"fallback": True},
            )
        if volatility == "LOW":
            return Decision(
                action="HOLD",
                confidence=0.0,
                reason=f"heuristic_fallback:volatility_low llm_error={type(error).__name__}",
                raw={"fallback": True},
            )
        if bool(structure.get("consolidation")):
            return Decision(
                action="HOLD",
                confidence=0.0,
                reason=f"heuristic_fallback:consolidation llm_error={type(error).__name__}",
                raw={"fallback": True},
            )
        if sample_len < max(6, int(self._settings.hard_filter_min_closes or 0)):
            return Decision(
                action="HOLD",
                confidence=0.0,
                reason=(
                    f"heuristic_fallback:warmup sample_len={sample_len} "
                    f"need>={max(6, int(self._settings.hard_filter_min_closes or 0))}"
                ),
                raw={"fallback": True},
            )

        action = "BUY" if trend == "UP" else "SELL"
        if action == "BUY" and similar_bias["SELL"] > similar_bias["BUY"] + 1:
            return Decision(
                action="HOLD",
                confidence=0.0,
                reason="heuristic_fallback:similar_memory_sell_bias",
                raw={"fallback": True, "similar_bias": similar_bias},
            )
        if action == "SELL" and similar_bias["BUY"] > similar_bias["SELL"] + 1:
            return Decision(
                action="HOLD",
                confidence=0.0,
                reason="heuristic_fallback:similar_memory_buy_bias",
                raw={"fallback": True, "similar_bias": similar_bias},
            )

        trend_follow = (
            dict(pattern_analysis.get("per_setup_tag") or {}).get("trend_follow") or {}
        )
        sample_size = int(trend_follow.get("sample_size") or 0)
        win_rate = trend_follow.get("win_rate")
        if (
            sample_size >= self._settings.pattern_min_sample_size
            and win_rate is not None
            and float(win_rate) < self._settings.pattern_min_win_rate
        ):
            return Decision(
                action="HOLD",
                confidence=0.0,
                reason=(
                    "heuristic_fallback:pattern_gate "
                    f"wr={float(win_rate):.3f} n={sample_size}"
                ),
                raw={"fallback": True},
            )

        confidence = 0.68 if volatility == "MEDIUM" else 0.73
        if spread_pct > 0.00012:
            confidence -= 0.08
        if action == "BUY" and bool(structure.get("higher_high")):
            confidence += 0.04
        if action == "SELL" and bool(structure.get("lower_low")):
            confidence += 0.04
        if sample_size >= self._settings.pattern_boost_min_sample and win_rate is not None:
            if float(win_rate) >= self._settings.pattern_boost_min_win_rate:
                confidence += 0.05
            else:
                confidence -= 0.03
        if similar_bias[action] >= 2:
            confidence += 0.03
        confidence = max(0.0, min(0.95, confidence))
        decision = Decision(
            action=action,
            confidence=confidence,
            reason=(
                "heuristic_fallback:"
                f"trend={trend} vol={volatility} spread_pct={spread_pct:.6f} "
                f"pattern_n={sample_size} pattern_wr={win_rate} similar_bias={similar_bias}"
            ),
            raw={"fallback": True, "similar_bias": similar_bias},
        )
        return apply_confidence_floor(decision, self._settings.min_trade_confidence)

    async def decide(
        self,
        market: MarketSnapshot,
        features: Dict[str, Any],
        *,
        similar_trades: List[RecallHit],
        risk_state: Dict[str, Any],
        pattern_analysis: Dict[str, Any],
        wake_up_context: str,
        skill_context: str,
        team_brief: Dict[str, Any],
    ) -> Decision:
        system = (
            "You are a deterministic trading classifier and policy head for a data-driven execution system. "
            "Apply the decision_rules exactly, then use memory_wakeup_context, top_similar_trades, "
            "procedural_skill_context, team_brief, risk_state, and pattern_analysis as confidence modifiers. "
            "Cold start is allowed for unusually clear directional structure with controlled risk; sparse memory alone is not a HOLD signal. "
            "Return JSON only with keys action, confidence, reason. "
            "Valid actions are exactly BUY, SELL, or HOLD. "
            "Never output 'BUY|SELL|HOLD' as a literal action."
        )
        user = json.dumps(
            self._build_user_payload(
                market,
                features=features,
                similar_trades=similar_trades,
                risk_state=risk_state,
                pattern_analysis=pattern_analysis,
                wake_up_context=wake_up_context,
                skill_context=skill_context,
                team_brief=team_brief,
            ),
            ensure_ascii=False,
            indent=2,
        )
        try:
            raw = await self._llm.complete_json(
                system=system,
                user=user,
                temperature=0.15,
                json_schema=self._decision_json_schema(),
            )
            decision = Decision.from_llm_payload(raw)
            if decision.raw.get("_invalid_action"):
                raise ValueError(f"llm_invalid_action:{decision.raw.get('_invalid_action')}")
            decision = apply_confidence_floor(decision, self._settings.min_trade_confidence)
            log.info("Agent: %s conf=%.3f reason=%s", decision.action, decision.confidence, decision.reason[:200])
            return decision
        except Exception as exc:
            if self._settings.llm_fallback_enabled:
                log.warning("LLM decision failed, heuristic fallback: %s", exc)
                return self._heuristic_fallback_decision(
                    features=features,
                    similar_trades=similar_trades,
                    risk_state=risk_state,
                    pattern_analysis=pattern_analysis,
                    error=exc,
                )
            log.exception("LLM decision failed, HOLD: %s", exc)
            return Decision(
                action="HOLD",
                confidence=0.0,
                reason=f"llm_error:{exc}",
                raw={},
            )


def format_matched_trades_log(hits: List[RecallHit]) -> str:
    parts: List[str] = []
    for h in hits[:5]:
        parts.append(f"id={h.id} w={h.weighted_score:.2f} {h.metadata}")
    return "; ".join(parts) if parts else "none"
