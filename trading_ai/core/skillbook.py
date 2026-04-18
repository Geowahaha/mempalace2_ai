from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


def _slug(value: Any, *, default: str = "skill") -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    out: List[str] = []
    last_dash = False
    for ch in raw:
        if ch.isalnum():
            out.append(ch)
            last_dash = False
        elif not last_dash:
            out.append("-")
            last_dash = True
    return "".join(out).strip("-") or default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_lines(value: Any) -> List[str]:
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, Iterable):
        out: List[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                out.append(text)
        return out
    return []


def _dedupe_keep_order(items: Sequence[str], *, limit: int = 12) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _iso_utc(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(ts)))


@dataclass(slots=True)
class SkillMatch:
    skill_key: str
    score: float
    title: str
    summary: str
    use_when: List[str]
    avoid_when: List[str]
    guardrails: List[str]
    confidence_rules: List[str]
    stats: Dict[str, Any]
    triggers: Dict[str, Any]
    file_path: str
    fit_reasons: List[str]


class SkillBook:
    """
    Hermes-inspired procedural memory for recurring trading situations.

    Skills are stored twice:
    1. An index JSON used for fast recall and score updates.
    2. A markdown document per skill for human / agent inspection.
    """

    def __init__(
        self,
        *,
        root_dir: Path,
        index_path: Path,
        max_evidence: int = 8,
    ) -> None:
        self._root_dir = Path(root_dir)
        self._index_path = Path(index_path)
        self._max_evidence = max(2, int(max_evidence))
        self._index_mtime = 0.0
        self._root_dir.mkdir(parents=True, exist_ok=True)
        self._index_path.parent.mkdir(parents=True, exist_ok=True)
        self._skills: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if not self._index_path.is_file():
            return
        try:
            raw = json.loads(self._index_path.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("SkillBook: failed to load %s: %s", self._index_path, exc)
            return
        if not isinstance(raw, dict):
            return
        for skill_key, blob in raw.items():
            if isinstance(blob, dict):
                self._skills[str(skill_key)] = blob
        try:
            self._index_mtime = self._index_path.stat().st_mtime
        except OSError:
            self._index_mtime = 0.0
        log.info("SkillBook: loaded %s skills from %s", len(self._skills), self._index_path)

    def _maybe_reload(self) -> None:
        if not self._index_path.is_file():
            return
        try:
            current_mtime = self._index_path.stat().st_mtime
        except OSError:
            return
        if current_mtime <= self._index_mtime:
            return
        self._skills = {}
        self._load()

    def _persist(self) -> None:
        tmp = self._index_path.with_suffix(self._index_path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(self._skills, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        tmp.replace(self._index_path)
        try:
            self._index_mtime = self._index_path.stat().st_mtime
        except OSError:
            self._index_mtime = 0.0

    def _skill_path(self, skill_key: str) -> Path:
        return self._root_dir / f"{_slug(skill_key)}.md"

    def _compute_stats(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        trades_seen = max(0, _safe_int(stats.get("trades_seen")))
        wins = max(0, _safe_int(stats.get("wins")))
        losses = max(0, _safe_int(stats.get("losses")))
        neutrals = max(0, _safe_int(stats.get("neutrals")))
        total_pnl = _safe_float(stats.get("total_pnl"))
        confidence_total = _safe_float(stats.get("_confidence_total"))
        avg_confidence = (confidence_total / float(trades_seen)) if trades_seen else 0.0
        avg_pnl = (total_pnl / float(trades_seen)) if trades_seen else 0.0
        win_rate = (wins / float(trades_seen)) if trades_seen else 0.0
        loss_rate = (losses / float(trades_seen)) if trades_seen else 0.0
        overconfident_losses = max(0, _safe_int(stats.get("overconfident_losses")))
        underconfident_wins = max(0, _safe_int(stats.get("underconfident_wins")))
        trade_count = float(trades_seen) if trades_seen else 1.0
        risk_penalty = (overconfident_losses / trade_count) * 0.35 + (loss_rate * 0.2)
        opportunity_bonus = (underconfident_wins / trade_count) * 0.1
        risk_adjusted_score = (win_rate - loss_rate) + avg_pnl - risk_penalty + opportunity_bonus
        out = dict(stats)
        out.update(
            {
                "trades_seen": trades_seen,
                "wins": wins,
                "losses": losses,
                "neutrals": neutrals,
                "total_pnl": round(total_pnl, 6),
                "avg_pnl": round(avg_pnl, 6),
                "avg_confidence": round(avg_confidence, 4),
                "win_rate": round(win_rate, 4),
                "loss_rate": round(loss_rate, 4),
                "overconfident_losses": overconfident_losses,
                "underconfident_wins": underconfident_wins,
                "risk_adjusted_score": round(risk_adjusted_score, 6),
                "_confidence_total": confidence_total,
            }
        )
        return out

    def _render_skill_markdown(self, skill: Dict[str, Any]) -> str:
        stats = dict(skill.get("stats") or {})
        triggers = dict(skill.get("triggers") or {})
        evidence = list(skill.get("evidence") or [])
        lines = [
            "---",
            f"skill_key: {skill.get('skill_key')}",
            f"title: {skill.get('title')}",
            f"updated_at: {_iso_utc(_safe_float(stats.get('last_updated_ts') or time.time()))}",
            f"strategy_keys: {json.dumps(triggers.get('strategy_keys') or [], ensure_ascii=False)}",
            f"sessions: {json.dumps(triggers.get('sessions') or [], ensure_ascii=False)}",
            f"symbols: {json.dumps(triggers.get('symbols') or [], ensure_ascii=False)}",
            "---",
            "",
            f"# {skill.get('title')}",
            "",
            "## Summary",
            str(skill.get("summary") or "No summary yet."),
            "",
            "## Use When",
        ]
        for item in list(skill.get("use_when") or [])[:8]:
            lines.append(f"- {item}")
        if not list(skill.get("use_when") or []):
            lines.append("- No confirmed reuse pattern yet.")
        lines.extend(["", "## Avoid When"])
        for item in list(skill.get("avoid_when") or [])[:8]:
            lines.append(f"- {item}")
        if not list(skill.get("avoid_when") or []):
            lines.append("- No explicit avoid conditions recorded yet.")
        lines.extend(["", "## Guardrails"])
        for item in list(skill.get("guardrails") or [])[:8]:
            lines.append(f"- {item}")
        if not list(skill.get("guardrails") or []):
            lines.append("- Keep standard session risk controls active.")
        lines.extend(["", "## Confidence Rules"])
        for item in list(skill.get("confidence_rules") or [])[:8]:
            lines.append(f"- {item}")
        if not list(skill.get("confidence_rules") or []):
            lines.append("- Confidence policy unchanged.")
        lines.extend(
            [
                "",
                "## Stats",
                f"- trades_seen: {stats.get('trades_seen', 0)}",
                f"- wins: {stats.get('wins', 0)}",
                f"- losses: {stats.get('losses', 0)}",
                f"- neutrals: {stats.get('neutrals', 0)}",
                f"- total_pnl: {stats.get('total_pnl', 0.0)}",
                f"- avg_pnl: {stats.get('avg_pnl', 0.0)}",
                f"- win_rate: {stats.get('win_rate', 0.0)}",
                f"- avg_confidence: {stats.get('avg_confidence', 0.0)}",
                f"- risk_adjusted_score: {stats.get('risk_adjusted_score', 0.0)}",
                "",
                "## Recent Evidence",
            ]
        )
        if evidence:
            for row in evidence[: self._max_evidence]:
                lines.append(
                    "- "
                    f"{_iso_utc(_safe_float(row.get('created_ts') or time.time()))} "
                    f"outcome={row.get('outcome_label')} pnl={row.get('pnl')} "
                    f"confidence={row.get('confidence')} session={row.get('session')} "
                    f"reason={row.get('reason')}"
                )
        else:
            lines.append("- No evidence samples yet.")
        return "\n".join(lines) + "\n"

    def _write_markdown(self, skill: Dict[str, Any]) -> None:
        path = self._skill_path(str(skill.get("skill_key") or "skill"))
        path.write_text(self._render_skill_markdown(skill), encoding="utf-8")
        skill["file_path"] = str(path)

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        self._maybe_reload()
        return {key: json.loads(json.dumps(value)) for key, value in self._skills.items()}

    def list_skills(
        self,
        *,
        symbol: Optional[str] = None,
        session: Optional[str] = None,
        strategy_key: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        self._maybe_reload()
        out: List[Dict[str, Any]] = []
        for skill in self._skills.values():
            triggers = dict(skill.get("triggers") or {})
            if symbol and symbol not in list(triggers.get("symbols") or []):
                continue
            if session and session not in list(triggers.get("sessions") or []):
                continue
            if strategy_key and strategy_key not in list(triggers.get("strategy_keys") or []):
                continue
            out.append(json.loads(json.dumps(skill)))
        out.sort(
            key=lambda item: (
                -_safe_float((item.get("stats") or {}).get("risk_adjusted_score")),
                -_safe_float((item.get("stats") or {}).get("last_updated_ts")),
                str(item.get("skill_key") or ""),
            )
        )
        return out[: max(1, min(int(limit), 200))]

    def recall(
        self,
        *,
        symbol: str,
        session: str = "",
        setup_tag: str = "",
        strategy_key: str = "",
        room: str = "",
        trend_direction: str = "",
        volatility: str = "",
        action: str = "",
        top_k: int = 3,
    ) -> List[SkillMatch]:
        self._maybe_reload()
        matches: List[SkillMatch] = []
        for skill_key, skill in self._skills.items():
            triggers = dict(skill.get("triggers") or {})
            stats = dict(skill.get("stats") or {})
            score = 0.0
            fit_reasons: List[str] = []

            strategy_keys = list(triggers.get("strategy_keys") or [])
            rooms = list(triggers.get("rooms") or [])
            symbols = list(triggers.get("symbols") or [])
            sessions = list(triggers.get("sessions") or [])
            setup_tags = list(triggers.get("setup_tags") or [])
            trends = list(triggers.get("trend_directions") or [])
            vols = list(triggers.get("volatilities") or [])
            actions = list(triggers.get("actions") or [])

            if strategy_key and strategy_key in strategy_keys:
                score += 6.0
                fit_reasons.append("strategy_key")
            if room and room in rooms:
                score += 4.5
                fit_reasons.append("room")
            if symbol and symbol in symbols:
                score += 2.0
                fit_reasons.append("symbol")
            if session and session in sessions:
                score += 1.5
                fit_reasons.append("session")
            if setup_tag and setup_tag in setup_tags:
                score += 1.5
                fit_reasons.append("setup_tag")
            if trend_direction and trend_direction in trends:
                score += 1.25
                fit_reasons.append("trend")
            if volatility and volatility in vols:
                score += 1.0
                fit_reasons.append("volatility")
            if action and action in actions:
                score += 0.75
                fit_reasons.append("action")

            score += max(-1.0, min(1.0, _safe_float(stats.get("risk_adjusted_score"))))
            if score <= 0.0:
                continue

            matches.append(
                SkillMatch(
                    skill_key=skill_key,
                    score=round(score, 4),
                    title=str(skill.get("title") or skill_key),
                    summary=str(skill.get("summary") or ""),
                    use_when=list(skill.get("use_when") or []),
                    avoid_when=list(skill.get("avoid_when") or []),
                    guardrails=list(skill.get("guardrails") or []),
                    confidence_rules=list(skill.get("confidence_rules") or []),
                    stats=stats,
                    triggers=triggers,
                    file_path=str(skill.get("file_path") or ""),
                    fit_reasons=fit_reasons,
                )
            )

        matches.sort(
            key=lambda item: (
                -float(item.score),
                -_safe_float(item.stats.get("risk_adjusted_score")),
                -_safe_float(item.stats.get("last_updated_ts")),
                item.skill_key,
            )
        )
        return matches[: max(1, min(int(top_k), 12))]

    def render_prompt_context(self, matches: Sequence[SkillMatch]) -> str:
        if not matches:
            return "No procedural skill documents match the current market context yet."
        lines = ["Procedural skillbook context"]
        for idx, match in enumerate(matches[:6], start=1):
            stats = match.stats
            lines.append(
                f"{idx}. title={match.title} skill_key={match.skill_key} fit={match.score:.2f} "
                f"edge={_safe_float(stats.get('risk_adjusted_score')):.3f} "
                f"trades={_safe_int(stats.get('trades_seen'))} "
                f"win_rate={_safe_float(stats.get('win_rate')):.3f}"
            )
            lines.append(f"   summary={match.summary or 'n/a'}")
            if match.use_when:
                lines.append(f"   use_when={'; '.join(match.use_when[:2])}")
            if match.avoid_when:
                lines.append(f"   avoid_when={'; '.join(match.avoid_when[:2])}")
            if match.guardrails:
                lines.append(f"   guardrails={'; '.join(match.guardrails[:2])}")
            if match.confidence_rules:
                lines.append(f"   confidence_rules={'; '.join(match.confidence_rules[:2])}")
        return "\n".join(lines)

    def render_note_excerpt(self, skill: Dict[str, Any]) -> str:
        parts = [str(skill.get("summary") or "").strip()]
        for label, key in (
            ("Use when", "use_when"),
            ("Avoid when", "avoid_when"),
            ("Guardrails", "guardrails"),
            ("Confidence", "confidence_rules"),
        ):
            values = list(skill.get(key) or [])
            if values:
                parts.append(f"{label}: " + "; ".join(values[:3]))
        return "\n".join(part for part in parts if part)

    def upsert_from_review(
        self,
        *,
        review: Dict[str, Any],
        evidence: Dict[str, Any],
    ) -> Dict[str, Any]:
        skill_key = str(
            review.get("skill_key")
            or evidence.get("strategy_key")
            or evidence.get("room")
            or evidence.get("setup_tag")
            or "general-learning"
        ).strip()
        skill_key = skill_key or "general-learning"
        existing = dict(self._skills.get(skill_key) or {})
        stats = dict(existing.get("stats") or {})

        score = _safe_int(evidence.get("score"))
        confidence = _safe_float(evidence.get("confidence"))
        pnl = _safe_float(evidence.get("pnl"))
        now = _safe_float(evidence.get("created_ts") or time.time(), time.time())

        stats["trades_seen"] = _safe_int(stats.get("trades_seen")) + 1
        if score > 0:
            stats["wins"] = _safe_int(stats.get("wins")) + 1
        elif score < 0:
            stats["losses"] = _safe_int(stats.get("losses")) + 1
        else:
            stats["neutrals"] = _safe_int(stats.get("neutrals")) + 1
        stats["total_pnl"] = _safe_float(stats.get("total_pnl")) + pnl
        stats["_confidence_total"] = _safe_float(stats.get("_confidence_total")) + confidence
        stats["last_score"] = score
        stats["last_updated_ts"] = now
        if score < 0 and confidence >= 0.75:
            stats["overconfident_losses"] = _safe_int(stats.get("overconfident_losses")) + 1
        if score > 0 and confidence < 0.55:
            stats["underconfident_wins"] = _safe_int(stats.get("underconfident_wins")) + 1
        stats = self._compute_stats(stats)

        evidence_row = {
            "created_ts": now,
            "outcome_label": str(evidence.get("outcome_label") or "neutral"),
            "pnl": round(pnl, 6),
            "confidence": round(confidence, 4),
            "session": str(evidence.get("session") or ""),
            "reason": str(evidence.get("reason") or "")[:220],
            "action": str(evidence.get("action") or ""),
            "symbol": str(evidence.get("symbol") or ""),
        }
        evidence_rows = list(existing.get("evidence") or [])
        evidence_rows.insert(0, evidence_row)
        evidence_rows = evidence_rows[: self._max_evidence]

        existing_team_notes = dict(existing.get("team_notes") or {})
        review_team_notes = dict(review.get("team_notes") or {})
        merged_team_notes: Dict[str, List[str]] = {}
        for key in ("strategist", "risk_guardian", "execution", "learning"):
            merged_team_notes[key] = _dedupe_keep_order(
                _coerce_lines(existing_team_notes.get(key))
                + _coerce_lines(review_team_notes.get(key)),
                limit=8,
            )

        triggers = dict(existing.get("triggers") or {})
        triggers["symbols"] = _dedupe_keep_order(
            list(triggers.get("symbols") or []) + [str(evidence.get("symbol") or "")],
            limit=8,
        )
        triggers["sessions"] = _dedupe_keep_order(
            list(triggers.get("sessions") or []) + [str(evidence.get("session") or "")],
            limit=8,
        )
        triggers["setup_tags"] = _dedupe_keep_order(
            list(triggers.get("setup_tags") or []) + [str(evidence.get("setup_tag") or "")],
            limit=8,
        )
        triggers["strategy_keys"] = _dedupe_keep_order(
            list(triggers.get("strategy_keys") or []) + [str(evidence.get("strategy_key") or "")],
            limit=8,
        )
        triggers["rooms"] = _dedupe_keep_order(
            list(triggers.get("rooms") or []) + [str(evidence.get("room") or "")],
            limit=8,
        )
        triggers["trend_directions"] = _dedupe_keep_order(
            list(triggers.get("trend_directions") or []) + [str(evidence.get("trend_direction") or "")],
            limit=6,
        )
        triggers["volatilities"] = _dedupe_keep_order(
            list(triggers.get("volatilities") or []) + [str(evidence.get("volatility") or "")],
            limit=6,
        )
        triggers["actions"] = _dedupe_keep_order(
            list(triggers.get("actions") or []) + [str(evidence.get("action") or "")],
            limit=6,
        )

        skill = {
            "skill_key": skill_key,
            "title": str(review.get("title") or existing.get("title") or f"Skill {skill_key}"),
            "summary": str(review.get("summary") or existing.get("summary") or "").strip(),
            "use_when": _dedupe_keep_order(
                list(existing.get("use_when") or []) + _coerce_lines(review.get("use_when")),
                limit=10,
            ),
            "avoid_when": _dedupe_keep_order(
                list(existing.get("avoid_when") or []) + _coerce_lines(review.get("avoid_when")),
                limit=10,
            ),
            "guardrails": _dedupe_keep_order(
                list(existing.get("guardrails") or []) + _coerce_lines(review.get("guardrails")),
                limit=10,
            ),
            "confidence_rules": _dedupe_keep_order(
                list(existing.get("confidence_rules") or [])
                + _coerce_lines(review.get("confidence_rules")),
                limit=10,
            ),
            "team_notes": merged_team_notes,
            "triggers": triggers,
            "stats": stats,
            "evidence": evidence_rows,
            "source": str(review.get("source") or "self_improvement"),
        }
        self._write_markdown(skill)
        self._skills[skill_key] = skill
        self._persist()
        return json.loads(json.dumps(skill))


def build_team_brief(
    *,
    features: Dict[str, Any],
    risk_state: Dict[str, Any],
    pattern_analysis: Dict[str, Any],
    matches: Sequence[SkillMatch],
    strategy_state: Optional[Dict[str, Any]] = None,
    room_guard: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    structure = dict(features.get("structure") or {})
    strategist = (
        f"trend={features.get('trend_direction')} volatility={features.get('volatility')} "
        f"session={features.get('session')} consolidation={structure.get('consolidation')} "
        f"higher_high={structure.get('higher_high')} lower_low={structure.get('lower_low')}"
    )
    if matches:
        top = matches[0]
        memory_librarian = (
            f"top_skill={top.skill_key} fit={top.score:.2f} edge={_safe_float(top.stats.get('risk_adjusted_score')):.3f}; "
            f"summary={top.summary or 'n/a'}"
        )
        if top.avoid_when:
            memory_librarian += f"; avoid_when={'; '.join(top.avoid_when[:2])}"
        if top.use_when:
            memory_librarian += f"; use_when={'; '.join(top.use_when[:2])}"
    else:
        memory_librarian = "No strong skill match; rely on structure, patterns, and risk."

    risk_bits = [f"can_trade={risk_state.get('can_trade', True)}"]
    if room_guard:
        risk_bits.append(
            f"room={room_guard.get('room')} blocked={room_guard.get('blocked')} caution={room_guard.get('caution')}"
        )
    matched_pattern = pattern_analysis.get("matched_pattern")
    if matched_pattern:
        risk_bits.append(
            f"pattern={matched_pattern} win_rate={pattern_analysis.get('win_rate')} sample={pattern_analysis.get('sample_size')}"
        )
    risk_guardian = "; ".join(str(bit) for bit in risk_bits if bit)

    evolution_bits: List[str] = []
    if strategy_state:
        evolution_bits.append(f"lane_stage={strategy_state.get('lane_stage')}")
        evolution_bits.append(f"pending={strategy_state.get('pending_recommendation') or 'none'}")
        evolution_bits.append(f"trades={strategy_state.get('trades')}")
        evolution_bits.append(f"wins={strategy_state.get('wins')}")
        evolution_bits.append(f"losses={strategy_state.get('losses')}")
        evolution_bits.append(f"total_profit={strategy_state.get('total_profit')}")
    evolution = "; ".join(evolution_bits) if evolution_bits else "No evolution stats for this lane yet."

    return {
        "strategist": strategist,
        "memory_librarian": memory_librarian,
        "risk_guardian": risk_guardian,
        "evolution_coach": evolution,
    }
