from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from trading_ai.core.execution import MarketSnapshot, OpenPosition


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


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _iso_utc(ts_unix: float) -> str:
    return datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _entry_skill_stats(matches: List[Any], strategy_state: Optional[Dict[str, Any]]) -> Dict[str, float]:
    top_stats = dict(matches[0].stats) if matches else {}
    shadow_trades = _safe_float((strategy_state or {}).get("shadow_trades"))
    shadow_wins = _safe_float((strategy_state or {}).get("shadow_wins"))
    shadow_losses = _safe_float((strategy_state or {}).get("shadow_losses"))
    return {
        "trades_seen": float(_safe_int(top_stats.get("trades_seen")) or _safe_int((strategy_state or {}).get("trades"))),
        "win_rate": _safe_float(top_stats.get("win_rate"), _safe_float((strategy_state or {}).get("wins")) / max(1.0, float(_safe_int((strategy_state or {}).get("trades"))))),
        "edge": _safe_float(top_stats.get("risk_adjusted_score")),
        "shadow_bias": (shadow_wins - shadow_losses) / max(1.0, shadow_trades),
    }


def assess_entry_candidate(
    *,
    action: str,
    features: Dict[str, Any],
    decision: Optional[Dict[str, Any]],
    matches: List[Any],
    strategy_state: Optional[Dict[str, Any]],
    pattern_analysis: Dict[str, Any],
) -> Dict[str, Any]:
    side = str(action or "HOLD").upper()
    if side not in ("BUY", "SELL"):
        return {
            "action": side,
            "opportunity_score": 0.0,
            "risk_score": 1.0,
            "edge_score": 0.0,
            "summary": "No directional opportunity assessed.",
        }

    skill = _entry_skill_stats(matches, strategy_state)
    trend = str(features.get("trend_direction") or "RANGE").upper()
    vol = _safe_float(features.get("realized_volatility"))
    m5 = _safe_float(features.get("momentum_5"))
    m20 = _safe_float(features.get("momentum_20"))
    spread_pct = _safe_float(features.get("spread_pct"))
    setup_tag = str((strategy_state or {}).get("setup_tag") or "trend_follow")
    pattern_row = dict((pattern_analysis.get("per_setup_tag") or {}).get(setup_tag) or {})
    pattern_wr = _safe_float(pattern_row.get("win_rate"), 0.5) if pattern_row.get("win_rate") is not None else 0.5
    aligned = 1.0 if (trend == "UP" and side == "BUY") or (trend == "DOWN" and side == "SELL") else -1.0 if trend in {"UP", "DOWN"} else 0.0
    signed_momentum = (m5 * 0.65 + m20 * 0.35) * (1.0 if side == "BUY" else -1.0)
    edge_score = _clamp(0.5 + (skill["edge"] * 0.22) + (skill["shadow_bias"] * 0.10), 0.0, 1.0)
    vol_base = max(vol * 12.0, spread_pct * 8.0, 1e-6)
    directional_thrust = signed_momentum / vol_base
    impulse_support = _clamp(
        0.50
        + (0.35 * directional_thrust)
        + (0.10 * max(0.0, aligned))
        - (0.15 * (1.0 if bool((features.get("structure") or {}).get("consolidation")) else 0.0)),
        0.0,
        1.0,
    )
    opportunity = _clamp(
        0.30
        + (0.25 * pattern_wr)
        + (0.18 * edge_score)
        + (0.16 * _clamp(0.5 + signed_momentum / max(vol * 18.0, 1e-6), 0.0, 1.0))
        + (0.08 * max(0.0, aligned))
        - (0.08 * min(1.0, spread_pct / 0.00045)),
        0.0,
        1.0,
    )
    risk = _clamp(
        0.28
        + (0.22 * (1.0 - pattern_wr))
        + (0.18 * (1.0 - edge_score))
        + (0.16 * (1.0 if bool((features.get("structure") or {}).get("consolidation")) else 0.0))
        + (0.08 * (1.0 if aligned < 0 else 0.0))
        + (0.08 * min(1.0, spread_pct / 0.00045)),
        0.0,
        1.0,
    )
    return {
        "action": side,
        "opportunity_score": round(opportunity, 4),
        "risk_score": round(risk, 4),
        "edge_score": round(edge_score, 4),
        "impulse_support": round(impulse_support, 4),
        "pattern_win_rate": round(pattern_wr, 4),
        "skill_edge": round(skill["edge"], 4),
        "summary": (
            f"{side} opp={opportunity:.3f} risk={risk:.3f} pattern_wr={pattern_wr:.3f} "
            f"edge={skill['edge']:.3f} m5={m5:.5f} m20={m20:.5f}"
        ),
        "decision_reason": str((decision or {}).get("reason") or ""),
    }


def evaluate_entry_hold_override(
    *,
    anticipated_action: str,
    anticipated_assessment: Dict[str, Any],
    decision_action: str,
    decision_reason: str,
    matches: List[Any],
    risk_state: Dict[str, Any],
    room_guard: Optional[Dict[str, Any]],
    settings: Any,
) -> Dict[str, Any]:
    side = str(anticipated_action or "HOLD").upper()
    decision_side = str(decision_action or "HOLD").upper()
    assessment = dict(anticipated_assessment or {})
    opportunity = _safe_float(assessment.get("opportunity_score"))
    risk = _safe_float(assessment.get("risk_score"), 1.0)
    edge = opportunity - risk
    decision_reason_text = str(decision_reason or "")
    result = {
        "eligible": False,
        "action": side,
        "opportunity_score": round(opportunity, 4),
        "risk_score": round(risk, 4),
        "edge_score": round(edge, 4),
        "blocked_reason": "unknown",
    }
    min_opportunity = _safe_float(getattr(settings, "entry_override_min_opportunity", 0.67), 0.67)
    max_risk = _safe_float(getattr(settings, "entry_override_max_risk", 0.55), 0.55)
    min_edge = _safe_float(getattr(settings, "entry_override_min_edge", 0.16), 0.16)

    if not bool(getattr(settings, "entry_override_enabled", True)):
        result["blocked_reason"] = "disabled"
        return result
    if decision_side != "HOLD":
        result["blocked_reason"] = "decision_not_hold"
        return result
    if side not in ("BUY", "SELL"):
        result["blocked_reason"] = "anticipated_hold"
        return result
    if not bool((risk_state or {}).get("can_trade", True)):
        result["blocked_reason"] = "risk_blocked"
        return result
    if room_guard and bool(room_guard.get("blocked")):
        result["blocked_reason"] = "room_guard_blocked"
        return result
    if decision_reason_text.startswith("pre_llm_hard_filter:") or "|hard_filter:" in decision_reason_text:
        impulse_support = _safe_float(assessment.get("impulse_support"))
        softenable_tokens = ("trend_RANGE", "structure_consolidation", "volatility_LOW")
        hard_filter_softenable = any(token in decision_reason_text for token in softenable_tokens)
        hard_filter_impulse_ok = (
            hard_filter_softenable
            and side in ("BUY", "SELL")
            and impulse_support >= 0.72
            and opportunity >= max(0.58, min_opportunity - 0.08)
            and edge >= max(0.08, min_edge - 0.08)
            and risk <= min(0.95, max_risk + 0.12)
        )
        if not hard_filter_impulse_ok:
            result["blocked_reason"] = "hard_filter"
            return result
        result["hard_filter_override"] = "impulse_support"
    if (
        "|memory_guard:anti_pattern:" in decision_reason_text
        or "|skill_block:" in decision_reason_text
        or "|pattern_block:" in decision_reason_text
        or "|strategy_disabled:" in decision_reason_text
        or decision_reason_text == "risk_block_session"
    ):
        result["blocked_reason"] = "guarded_hold"
        return result

    top = matches[0] if matches else None
    if top is not None:
        stats = dict(getattr(top, "stats", {}) or {})
        trades_seen = _safe_int(stats.get("trades_seen"))
        wins = _safe_int(stats.get("wins"))
        losses = _safe_int(stats.get("losses"))
        edge_score = _safe_float(stats.get("risk_adjusted_score"))
        if trades_seen >= 3 and losses > wins and edge_score <= -0.2:
            result["blocked_reason"] = "negative_skill_edge"
            return result

    hard_filter_override = bool(result.get("hard_filter_override"))
    effective_max_risk = min(0.95, max_risk + 0.12) if hard_filter_override else max_risk
    effective_min_edge = max(0.08, min_edge - 0.08) if hard_filter_override else min_edge

    if opportunity < min_opportunity:
        result["blocked_reason"] = "opportunity_too_low"
        return result
    if risk > effective_max_risk:
        result["blocked_reason"] = "risk_too_high"
        return result
    if edge < effective_min_edge:
        result["blocked_reason"] = "edge_too_low"
        return result

    result.update(
        {
            "eligible": True,
            "blocked_reason": "",
            "confidence": round(
                max(
                    _safe_float(getattr(settings, "min_trade_confidence", 0.65), 0.65),
                    _safe_float(getattr(settings, "entry_override_confidence", 0.67), 0.67),
                ),
                4,
            ),
        }
    )
    return result


@dataclass(slots=True)
class PositionPlan:
    action: str
    reason: str
    strategy_key: str
    setup_tag: str
    entry_price: float
    exit_basis_price: float
    take_profit_price: float
    stop_loss_price: float
    trailing_stop_price: float
    expected_move_pct: float
    unrealized_pnl: float
    unrealized_return_pct: float
    opportunity_score: float
    risk_score: float
    hold_score: float
    elapsed_minutes: float
    max_hold_minutes: float
    metadata: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def evaluate_open_position(
    *,
    position: OpenPosition,
    market: MarketSnapshot,
    features: Dict[str, Any],
    close_context: Dict[str, Any],
    matches: List[Any],
    strategy_state: Optional[Dict[str, Any]],
    pattern_analysis: Dict[str, Any],
    settings: Any,
) -> PositionPlan:
    side = str(position.side).upper()
    sign = 1.0 if side == "BUY" else -1.0
    exit_basis = float(market.bid if side == "BUY" else market.ask)
    entry = float(position.entry_price)
    if entry <= 0.0 or exit_basis <= 0.0:
        return PositionPlan(
            action="HOLD",
            reason="position_manager_bad_quote_hold",
            strategy_key=str(close_context.get("strategy_key") or ""),
            setup_tag=str(close_context.get("setup_tag") or "trend_follow"),
            entry_price=entry,
            exit_basis_price=exit_basis,
            take_profit_price=entry,
            stop_loss_price=entry,
            trailing_stop_price=entry,
            expected_move_pct=0.0,
            unrealized_pnl=0.0,
            unrealized_return_pct=0.0,
            opportunity_score=0.0,
            risk_score=1.0,
            hold_score=0.0,
            elapsed_minutes=0.0,
            max_hold_minutes=float(settings.position_manager_max_hold_minutes),
            metadata={"symbol": position.symbol, "side": side, "volume": float(position.volume)},
        )
    unrealized_return_pct = sign * ((exit_basis - entry) / max(abs(entry), 1e-12))
    unrealized_pnl = sign * (exit_basis - entry) * float(position.volume)

    skill = _entry_skill_stats(matches, strategy_state)
    vol = max(_safe_float(features.get("realized_volatility")), _safe_float(features.get("spread_pct")) * 2.5)
    m5 = _safe_float(features.get("momentum_5"))
    m20 = _safe_float(features.get("momentum_20"))
    spread_pct = _safe_float(features.get("spread_pct"))
    signed_momentum = (m5 * 0.65 + m20 * 0.35) * sign
    trend = str(features.get("trend_direction") or "RANGE").upper()
    volatility_label = str(features.get("volatility") or "").upper()
    session = str(features.get("session") or "").upper()
    structure = dict(features.get("structure") or {})
    is_consolidating = bool(structure.get("consolidation"))
    aligned = 1.0 if (trend == "UP" and side == "BUY") or (trend == "DOWN" and side == "SELL") else -1.0 if trend in {"UP", "DOWN"} else 0.0
    setup_tag = str(close_context.get("setup_tag") or "trend_follow")
    pattern_row = dict((pattern_analysis.get("per_setup_tag") or {}).get(setup_tag) or {})
    pattern_wr = _safe_float(pattern_row.get("win_rate"), 0.5) if pattern_row.get("win_rate") is not None else 0.5
    edge = skill["edge"]
    edge_score = _clamp(0.5 + (edge * 0.22) + (skill["shadow_bias"] * 0.10), 0.0, 1.0)
    elapsed_minutes = max(0.0, (float(market.ts_unix) - float(position.opened_ts)) / 60.0)
    hold_decay = _clamp(elapsed_minutes / max(1.0, float(settings.position_manager_max_hold_minutes)), 0.0, 1.5)
    is_weak_market = volatility_label == "LOW" or trend == "RANGE" or is_consolidating or session == "ASIA"
    vol_base = max(vol * 12.0, spread_pct * 8.0, 1e-6)
    impulse_strength = signed_momentum / vol_base
    impulse_threshold = 2.2 if session == "ASIA" else 1.6
    impulse_continuation = aligned > 0 and impulse_strength >= impulse_threshold and not is_consolidating

    expected_move_pct = max(
        float(settings.position_manager_min_expected_move_pct),
        vol * (1.15 + max(-0.20, min(0.35, edge * 0.12))) * (1.0 + max(-0.12, min(0.22, signed_momentum * 18.0))),
        spread_pct * 7.0,
    )
    tp_vol_multiplier = float(settings.position_manager_tp_vol_multiplier)
    if impulse_continuation:
        tp_vol_multiplier *= 1.25
    elif is_weak_market:
        tp_vol_multiplier *= 0.88
    tp_pct = expected_move_pct * tp_vol_multiplier * (1.0 + max(0.0, edge) * 0.06)
    sl_vol_multiplier = float(settings.position_manager_sl_vol_multiplier)
    if impulse_continuation:
        sl_vol_multiplier *= 1.18
    elif is_weak_market:
        sl_vol_multiplier *= 0.90
    sl_pct = max(
        spread_pct * 5.0,
        expected_move_pct * sl_vol_multiplier * (1.0 + max(0.0, -signed_momentum) * 0.08),
    )

    if side == "BUY":
        tp_price = entry * (1.0 + tp_pct)
        sl_price = entry * (1.0 - sl_pct)
    else:
        tp_price = entry * (1.0 - tp_pct)
        sl_price = entry * (1.0 + sl_pct)

    trail_price = sl_price
    trail_lock_fraction = 0.45
    if is_weak_market and not impulse_continuation:
        trail_lock_fraction = 0.72
    elif impulse_continuation:
        trail_lock_fraction = 0.36
    if unrealized_return_pct > tp_pct * float(settings.position_manager_trail_trigger_fraction):
        locked_pct = max(spread_pct * 3.5, unrealized_return_pct * trail_lock_fraction)
        trail_price = entry * (1.0 + locked_pct) if side == "BUY" else entry * (1.0 - locked_pct)

    stored_peak_return = _safe_float(close_context.get("pm_peak_return_pct"), 0.0)
    peak_return = max(stored_peak_return, unrealized_return_pct, 0.0)
    close_context["pm_peak_return_pct"] = round(peak_return, 10)
    close_context["pm_last_return_pct"] = round(unrealized_return_pct, 10)
    stored_peak_pnl = _safe_float(close_context.get("pm_peak_unrealized_pnl"), float("-inf"))
    if unrealized_pnl > stored_peak_pnl:
        close_context["pm_peak_unrealized_pnl"] = round(unrealized_pnl, 10)

    opportunity = _clamp(
        0.24
        + (0.24 * pattern_wr)
        + (0.18 * edge_score)
        + (0.16 * _clamp(0.5 + signed_momentum / max(vol * 18.0, 1e-6), 0.0, 1.0))
        + (0.10 * max(0.0, aligned))
        + (0.10 * _clamp(unrealized_return_pct / max(tp_pct, 1e-9), 0.0, 1.0))
        - (0.10 * min(1.0, hold_decay)),
        0.0,
        1.0,
    )
    risk = _clamp(
        0.26
        + (0.20 * (1.0 - pattern_wr))
        + (0.16 * (1.0 - edge_score))
        + (0.12 * (1.0 if bool((features.get("structure") or {}).get("consolidation")) else 0.0))
        + (0.10 * (1.0 if aligned < 0 else 0.0))
        + (0.10 * _clamp(-unrealized_return_pct / max(sl_pct, 1e-9), 0.0, 1.0))
        + (0.06 * min(1.0, spread_pct / 0.00045))
        + (0.10 * max(0.0, hold_decay - 0.75)),
        0.0,
        1.0,
    )
    hold_score = _clamp(0.5 + opportunity - risk, 0.0, 1.0)
    soft_risk_threshold = max(0.45, float(settings.position_manager_risk_close_threshold))
    catastrophic_sl_price = (
        entry * (1.0 - (sl_pct * (1.75 if impulse_continuation else 1.45)))
        if side == "BUY"
        else entry * (1.0 + (sl_pct * (1.75 if impulse_continuation else 1.45)))
    )
    profit_lock_activation = max(
        spread_pct * 4.0,
        expected_move_pct * (0.55 if is_weak_market else 0.80),
    )
    profit_giveback_limit = max(
        spread_pct * 3.0,
        peak_return * (0.35 if is_weak_market else 0.55),
    )
    profit_giveback_pct = max(0.0, peak_return - max(unrealized_return_pct, 0.0))

    action = "HOLD"
    reason = "position_manager_hold"
    weak_market_take_profit_ready = (
        is_weak_market
        and unrealized_return_pct >= max(spread_pct * 6.0, expected_move_pct * 0.55)
        and signed_momentum <= max(vol * 0.12, spread_pct * 1.5)
    )
    if side == "BUY" and exit_basis >= tp_price:
        action, reason = "CLOSE", "position_manager_take_profit"
    elif side == "SELL" and exit_basis <= tp_price:
        action, reason = "CLOSE", "position_manager_take_profit"
    elif weak_market_take_profit_ready:
        action, reason = "CLOSE", "position_manager_weak_market_take_profit"
    elif side == "BUY" and exit_basis <= catastrophic_sl_price:
        action, reason = "CLOSE", "position_manager_catastrophic_stop_loss"
    elif side == "SELL" and exit_basis >= catastrophic_sl_price:
        action, reason = "CLOSE", "position_manager_catastrophic_stop_loss"
    elif (
        peak_return >= profit_lock_activation
        and unrealized_return_pct <= 0.0
        and (is_weak_market or aligned <= 0 or signed_momentum < 0.0)
    ):
        action, reason = "CLOSE", "position_manager_profit_reversal"
    elif (
        peak_return >= profit_lock_activation
        and profit_giveback_pct >= profit_giveback_limit
        and (is_weak_market or signed_momentum < 0.0 or risk >= soft_risk_threshold * 0.90)
    ):
        action, reason = "CLOSE", "position_manager_profit_giveback"
    elif side == "BUY" and exit_basis <= sl_price:
        if impulse_continuation and opportunity >= 0.62 and risk < soft_risk_threshold and unrealized_return_pct > -(sl_pct * 0.55):
            action, reason = "HOLD", "position_manager_hold_impulse_soft_stop"
        else:
            action, reason = "CLOSE", "position_manager_stop_loss"
    elif side == "SELL" and exit_basis >= sl_price:
        if impulse_continuation and opportunity >= 0.62 and risk < soft_risk_threshold and unrealized_return_pct > -(sl_pct * 0.55):
            action, reason = "HOLD", "position_manager_hold_impulse_soft_stop"
        else:
            action, reason = "CLOSE", "position_manager_stop_loss"
    elif side == "BUY" and exit_basis <= trail_price and unrealized_return_pct > 0:
        action, reason = "CLOSE", "position_manager_trailing_protect"
    elif side == "SELL" and exit_basis >= trail_price and unrealized_return_pct > 0:
        action, reason = "CLOSE", "position_manager_trailing_protect"
    elif risk >= float(settings.position_manager_risk_close_threshold) and opportunity < 0.55:
        action, reason = "CLOSE", "position_manager_risk_dominates"
    elif elapsed_minutes >= float(settings.position_manager_max_hold_minutes) and opportunity < 0.60:
        action, reason = "CLOSE", "position_manager_time_decay"

    return PositionPlan(
        action=action,
        reason=reason,
        strategy_key=str(close_context.get("strategy_key") or ""),
        setup_tag=setup_tag,
        entry_price=entry,
        exit_basis_price=exit_basis,
        take_profit_price=tp_price,
        stop_loss_price=sl_price,
        trailing_stop_price=trail_price,
        expected_move_pct=expected_move_pct,
        unrealized_pnl=unrealized_pnl,
        unrealized_return_pct=unrealized_return_pct,
        opportunity_score=opportunity,
        risk_score=risk,
        hold_score=hold_score,
        elapsed_minutes=elapsed_minutes,
        max_hold_minutes=float(settings.position_manager_max_hold_minutes),
        metadata={
            "symbol": position.symbol,
            "side": side,
            "volume": float(position.volume),
            "pattern_win_rate": round(pattern_wr, 4),
            "skill_edge": round(edge, 4),
            "signed_momentum": round(signed_momentum, 6),
            "impulse_strength": round(impulse_strength, 6),
            "impulse_continuation": bool(impulse_continuation),
            "weak_market": bool(is_weak_market),
            "spread_pct": round(spread_pct, 6),
            "trend": trend,
            "aligned": aligned,
            "session": session,
            "volatility": volatility_label,
            "peak_return_pct": round(peak_return, 6),
            "profit_giveback_pct": round(profit_giveback_pct, 6),
            "opened_utc": _iso_utc(float(position.opened_ts)),
        },
    )


def write_monitor_snapshot(snapshot_path: Path, history_path: Path, payload: Dict[str, Any]) -> None:
    snapshot_path = Path(snapshot_path)
    history_path = Path(history_path)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with history_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
