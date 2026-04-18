from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from trading_ai.core.correlation_engine import apply_correlation_to_fusion_weight
from trading_ai.core.memory import RecallHit

RegimeName = Literal["trending_up", "trending_down", "mean_reverting", "vol_spike", "neutral"]


@dataclass(slots=True)
class PortfolioVote:
    """One scored opinion for conflict resolution / fusion."""

    source: str
    action: Literal["BUY", "SELL", "HOLD"]
    strength: float
    """0..1 — confidence-like mass for this vote."""
    weight: float
    """Relative importance vs other sources (from config)."""


@dataclass(slots=True)
class FusionResult:
    action: Literal["BUY", "SELL", "HOLD"]
    confidence: float
    reason_detail: str
    regime: RegimeName
    buy_mass: float
    sell_mass: float
    hold_mass: float
    diag: Dict[str, Any] = field(default_factory=dict)


def classify_regime(features: Dict[str, Any], *, spread_pct: Optional[float] = None) -> RegimeName:
    """
    Coarse regime for weight adjustment (XAU-friendly: trend vs chop vs spike).
    Uses the same feature keys as `extract_features`.
    """
    trend = str(features.get("trend_direction") or "RANGE").upper()
    vol = str(features.get("volatility") or "MEDIUM").upper()
    st = features.get("structure") or {}
    consolidation = bool(st.get("consolidation"))

    sp = float(spread_pct if spread_pct is not None else features.get("spread_pct") or 0.0)
    if vol == "HIGH" and sp > 0.00018:
        return "vol_spike"
    if trend in ("UP", "DOWN") and not consolidation:
        return "trending_up" if trend == "UP" else "trending_down"
    if consolidation or trend == "RANGE":
        return "mean_reverting"
    return "neutral"


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def vote_from_memory_hits(hits: List[RecallHit]) -> Optional[PortfolioVote]:
    """
    Aggregate similar-trade memories into one directional vote.
    Winning outcomes reinforce their entry side; losses push the opposite (soft).
    """
    if not hits:
        return None
    buy_mass = 0.0
    sell_mass = 0.0
    for h in hits:
        meta = h.metadata or {}
        act = str(meta.get("action") or "").upper()
        if act not in ("BUY", "SELL"):
            continue
        try:
            outcome = float(meta.get("outcome_score", 0.0))
        except (TypeError, ValueError):
            outcome = 0.0
        w = max(0.0, float(h.weighted_score))
        align = 0.55 + 0.45 * _clip01((outcome + 1.0) / 2.0)
        if outcome < 0:
            align = 0.35
        elif outcome == 0:
            align = 0.45
        mass = w * align
        if act == "BUY":
            buy_mass += mass
        else:
            sell_mass += mass
    total = buy_mass + sell_mass
    if total <= 1e-9:
        return None
    if buy_mass > sell_mass * 1.15:
        return PortfolioVote(
            source="memory",
            action="BUY",
            strength=_clip01(buy_mass / total),
            weight=1.0,
        )
    if sell_mass > buy_mass * 1.15:
        return PortfolioVote(
            source="memory",
            action="SELL",
            strength=_clip01(sell_mass / total),
            weight=1.0,
        )
    return PortfolioVote(
        source="memory",
        action="HOLD",
        strength=_clip01(1.0 - abs(buy_mass - sell_mass) / total),
        weight=1.0,
    )


def vote_from_structure(features: Dict[str, Any]) -> PortfolioVote:
    """Cheap trend/structure prior — not a second model."""
    trend = str(features.get("trend_direction") or "RANGE").upper()
    st = features.get("structure") or {}
    consolidation = bool(st.get("consolidation"))

    if consolidation:
        return PortfolioVote(
            source="structure",
            action="HOLD",
            strength=0.4,
            weight=1.0,
        )
    if trend == "UP":
        return PortfolioVote(source="structure", action="BUY", strength=0.42, weight=1.0)
    if trend == "DOWN":
        return PortfolioVote(source="structure", action="SELL", strength=0.42, weight=1.0)
    return PortfolioVote(source="structure", action="HOLD", strength=0.35, weight=1.0)


def _regime_multipliers(regime: RegimeName) -> Dict[str, float]:
    """Scale vote weights by regime so chop/spike does not over-trust trend."""
    if regime == "mean_reverting":
        return {"llm": 1.0, "memory": 1.05, "structure": 0.55}
    if regime == "vol_spike":
        return {"llm": 1.0, "memory": 0.95, "structure": 0.45}
    if regime in ("trending_up", "trending_down"):
        return {"llm": 1.0, "memory": 1.0, "structure": 1.08}
    return {"llm": 1.0, "memory": 1.0, "structure": 0.95}


def build_portfolio_votes(
    *,
    llm_action: Literal["BUY", "SELL", "HOLD"],
    llm_confidence: float,
    features: Dict[str, Any],
    similar_hits: List[RecallHit],
    cfg_weights: Dict[str, float],
) -> List[PortfolioVote]:
    w_llm = max(0.0, float(cfg_weights.get("llm", 1.0)))
    w_mem = max(0.0, float(cfg_weights.get("memory", 0.55)))
    w_str = max(0.0, float(cfg_weights.get("structure", 0.35)))

    votes: List[PortfolioVote] = [
        PortfolioVote(
            source="llm",
            action=llm_action,
            strength=_clip01(float(llm_confidence)),
            weight=w_llm,
        )
    ]
    mv = vote_from_memory_hits(similar_hits)
    if mv:
        mv = PortfolioVote(
            source=mv.source,
            action=mv.action,
            strength=mv.strength,
            weight=w_mem,
        )
        votes.append(mv)
    sv = vote_from_structure(features)
    sv = PortfolioVote(
        source=sv.source,
        action=sv.action,
        strength=sv.strength,
        weight=w_str * sv.weight,
    )
    votes.append(sv)
    return votes


def _effective_mass(v: PortfolioVote, regime: RegimeName) -> tuple[float, float, float]:
    mul = _regime_multipliers(regime)
    key = v.source
    if key not in mul:
        key = "llm"
    m = max(0.0, v.weight) * mul.get(key, 1.0)
    eff = m * max(0.0, v.strength)
    if v.action == "BUY":
        return eff, 0.0, 0.0
    if v.action == "SELL":
        return 0.0, eff, 0.0
    return 0.0, 0.0, eff


def fuse_portfolio_votes(
    votes: List[PortfolioVote],
    *,
    regime: RegimeName,
    tie_margin: float,
    llm_anchor_confidence: float,
    llm_original_action: Literal["BUY", "SELL", "HOLD"],
    llm_original_confidence: float,
    correlation_penalty: float = 0.0,
    diversity_bonus: float = 0.0,
) -> FusionResult:
    """
    Weighted directional pool: resolve BUY vs SELL conflict; near-tie → HOLD.
    Optional anchor: very confident LLM cannot be direction-flipped (only tightened to HOLD).
    """
    buy_m = sell_m = hold_m = 0.0
    by_src: Dict[str, Any] = {}
    for v in votes:
        b, s, h = _effective_mass(v, regime)
        buy_m += b
        sell_m += s
        hold_m += h
        by_src[v.source] = {"action": v.action, "strength": v.strength, "eff_buy": b, "eff_sell": s}

    denom = buy_m + sell_m + 1e-9
    margin = max(0.0, float(tie_margin))

    reason = "tie_hold"
    action: Literal["BUY", "SELL", "HOLD"] = "HOLD"
    conf_out = 0.0

    if buy_m > sell_m * (1.0 + margin):
        action = "BUY"
        reason = "buy_wins_pool"
        conf_out = _clip01(buy_m / denom)
    elif sell_m > buy_m * (1.0 + margin):
        action = "SELL"
        reason = "sell_wins_pool"
        conf_out = _clip01(sell_m / denom)
    else:
        tmax = max(buy_m, sell_m, hold_m * 0.5, 1e-9)
        conf_out = _clip01(1.0 - abs(buy_m - sell_m) / (tmax * 2.0))

    anchored = False
    if (
        llm_anchor_confidence > 0
        and llm_original_confidence >= llm_anchor_confidence
        and llm_original_action in ("BUY", "SELL")
        and action != llm_original_action
        and action in ("BUY", "SELL")
    ):
        action = llm_original_action
        conf_out = _clip01(
            0.5 * conf_out + 0.5 * llm_original_confidence,
        )
        reason = f"llm_anchor_keep_{llm_original_action.lower()}"
        anchored = True

    pen = max(0.0, float(correlation_penalty))
    div = max(0.0, float(diversity_bonus))
    fusion_w = apply_correlation_to_fusion_weight(
        1.0,
        penalty=pen,
        diversity_bonus=div,
    )
    conf_out = _clip01(conf_out * fusion_w)

    corr_diag = {
        "penalty": round(pen, 4),
        "diversity_bonus": round(div, 4),
        "fusion_weight": round(fusion_w, 4),
    }

    diag = {
        "regime": regime,
        "buy_mass": round(buy_m, 5),
        "sell_mass": round(sell_m, 5),
        "hold_mass": round(hold_m, 5),
        "votes": by_src,
        "llm_anchored": anchored,
        "correlation": corr_diag,
    }
    return FusionResult(
        action=action,
        confidence=max(conf_out, 0.0),
        reason_detail=reason,
        regime=regime,
        buy_mass=buy_m,
        sell_mass=sell_m,
        hold_mass=hold_m,
        diag=diag,
    )


def parse_recall_actions_for_diag(hits: List[RecallHit]) -> List[Dict[str, Any]]:
    """Compact log payload (avoid huge documents)."""
    out: List[Dict[str, Any]] = []
    for h in hits[:6]:
        meta = dict(h.metadata or {})
        out.append(
            {
                "sim": round(h.similarity, 4),
                "ws": round(h.weighted_score, 4),
                "action": meta.get("action"),
                "outcome_score": meta.get("outcome_score"),
            }
        )
    return out
