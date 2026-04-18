"""
learning/live_profile_autopilot.py

Data-driven live profile controls for winner-focused trading:
1) Audit missed opportunities from neural gate decisions.
2) Auto-apply safe live profile changes with minimum-sample gates.
3) Roll back canary changes if post-apply live performance degrades.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import statistics
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from config import config


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None = None) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_to_ms(raw: str) -> int:
    text = str(raw or "").strip()
    if not text:
        return 0
    try:
        src = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if src.tzinfo is None:
            src = src.replace(tzinfo=timezone.utc)
        return int(src.astimezone(timezone.utc).timestamp() * 1000)
    except Exception:
        return 0


def _ms_to_iso(ms: int) -> str:
    try:
        return _iso(datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc))
    except Exception:
        return _iso()


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _norm_symbol(raw: str) -> str:
    return str(raw or "").strip().upper()


def _norm_source(raw: str) -> str:
    return str(raw or "").strip().lower()


def _norm_signature(raw: str) -> str:
    tokens = [
        str(part or "").strip().lower().replace(" ", "_")
        for part in str(raw or "").split(",")
        if str(part or "").strip()
    ]
    return ",".join(tokens)


def _confidence_band(conf: float) -> str:
    value = _safe_float(conf, 0.0)
    if value < 70.0:
        return "<70"
    if value < 75.0:
        return "70-74.9"
    if value < 80.0:
        return "75-79.9"
    return "80+"


def _confidence_floor_for_band(band: str) -> Optional[float]:
    b = str(band or "").strip()
    if b in {"<70", "70-74.9"}:
        return 70.0
    if b == "75-79.9":
        return 75.0
    if b == "80+":
        return 80.0
    return None


def _prob_gap_band(gap: Optional[float]) -> str:
    if gap is None:
        return "none"
    g = max(0.0, float(gap))
    if g < 0.005:
        return "<0.005"
    if g < 0.010:
        return "0.005-0.0099"
    if g < 0.020:
        return "0.010-0.0199"
    if g < 0.030:
        return "0.020-0.0299"
    return ">=0.03"


def _quantile(values: list[float], q: float) -> Optional[float]:
    if not values:
        return None
    arr = sorted(float(v) for v in values)
    qv = max(0.0, min(1.0, float(q)))
    if len(arr) == 1:
        return arr[0]
    pos = (len(arr) - 1) * qv
    lo = int(pos)
    hi = min(lo + 1, len(arr) - 1)
    frac = pos - lo
    return arr[lo] * (1.0 - frac) + arr[hi] * frac


def _new_bucket() -> dict:
    return {"resolved": 0, "wins": 0, "losses": 0, "pnl_usd": 0.0}


def _update_bucket(bucket: dict, pnl: float, outcome: int) -> None:
    if int(outcome) not in (0, 1):
        return
    bucket["resolved"] = int(bucket.get("resolved", 0) or 0) + 1
    if int(outcome) == 1:
        bucket["wins"] = int(bucket.get("wins", 0) or 0) + 1
    else:
        bucket["losses"] = int(bucket.get("losses", 0) or 0) + 1
    bucket["pnl_usd"] = float(bucket.get("pnl_usd", 0.0) or 0.0) + float(pnl)


def _finalize_bucket(bucket: dict) -> dict:
    resolved = int(bucket.get("resolved", 0) or 0)
    wins = int(bucket.get("wins", 0) or 0)
    pnl = float(bucket.get("pnl_usd", 0.0) or 0.0)
    return {
        "resolved": resolved,
        "wins": wins,
        "losses": int(bucket.get("losses", 0) or 0),
        "win_rate": round((wins / resolved), 4) if resolved > 0 else 0.0,
        "pnl_usd": round(pnl, 4),
        "avg_pnl_usd": round((pnl / resolved), 4) if resolved > 0 else 0.0,
    }


def _merge_bucket(left: dict, right: dict) -> dict:
    out = _new_bucket()
    for src in (left or {}, right or {}):
        out["resolved"] += int(src.get("resolved", 0) or 0)
        out["wins"] += int(src.get("wins", 0) or 0)
        out["losses"] += int(src.get("losses", 0) or 0)
        out["pnl_usd"] += float(src.get("pnl_usd", 0.0) or 0.0)
    return out


def _subtract_bucket(total: dict, part: dict) -> dict:
    out = _new_bucket()
    out["resolved"] = max(0, int((total or {}).get("resolved", 0) or 0) - int((part or {}).get("resolved", 0) or 0))
    out["wins"] = max(0, int((total or {}).get("wins", 0) or 0) - int((part or {}).get("wins", 0) or 0))
    out["losses"] = max(0, int((total or {}).get("losses", 0) or 0) - int((part or {}).get("losses", 0) or 0))
    out["pnl_usd"] = float((total or {}).get("pnl_usd", 0.0) or 0.0) - float((part or {}).get("pnl_usd", 0.0) or 0.0)
    return out


def _safe_json_dict(raw: str) -> dict:
    try:
        payload = json.loads(str(raw or "{}") or "{}")
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(row[1] or "").strip().lower() for row in list(rows or []) if str(row[1] or "").strip()}
    except Exception:
        return set()


def _extract_request_context(request_json: str) -> dict:
    payload = _safe_json_dict(request_json)
    root = dict(payload)
    nested_payload = payload.get("payload")
    if isinstance(nested_payload, dict):
        payload = dict(nested_payload)
    raw_scores = dict(payload.get("raw_scores", {}) or {})
    session = (
        payload.get("session")
        or root.get("session")
        or raw_scores.get("session")
        or raw_scores.get("session_zone")
        or raw_scores.get("scalp_profile_session")
        or raw_scores.get("kill_zone")
        or ""
    )
    timeframe = payload.get("timeframe") or root.get("timeframe") or raw_scores.get("timeframe") or ""
    entry_type = payload.get("entry_type") or root.get("entry_type") or raw_scores.get("entry_type") or ""
    pattern = payload.get("pattern") or root.get("pattern") or raw_scores.get("pattern") or ""
    return {
        "session": _norm_signature(session) or "unknown",
        "timeframe": str(timeframe or "").strip().lower().replace(" ", "") or "unknown",
        "entry_type": str(entry_type or "").strip().lower() or "unknown",
        "pattern": str(pattern or "").strip() or "unknown",
        "raw_scores": raw_scores,
        "payload": payload,
        "root": root,
    }


def _actual_entry_from_deal(raw_payload: dict) -> float:
    close_detail = dict((raw_payload or {}).get("raw", {}) or {}).get("closePositionDetail")
    if isinstance(close_detail, dict):
        try:
            return float(close_detail.get("entryPrice") or 0.0)
        except Exception:
            pass
    try:
        return float((raw_payload or {}).get("entry_price") or 0.0)
    except Exception:
        return 0.0


def _hold_minutes(created_utc: str, closed_utc: str) -> float | None:
    created_ms = _iso_to_ms(created_utc)
    closed_ms = _iso_to_ms(closed_utc)
    if created_ms <= 0 or closed_ms <= 0 or closed_ms < created_ms:
        return None
    return round((closed_ms - created_ms) / 60000.0, 3)


def _external_prior_summary(prior: dict) -> str:
    if not isinstance(prior, dict) or not prior:
        return ""
    models = ",".join(str(x or "").strip() for x in list(prior.get("prior_models") or []) if str(x or "").strip())
    transfer = str(prior.get("transfer_confidence") or "").strip().lower()
    try:
        router_bonus = float(prior.get("router_bonus", 0.0) or 0.0)
    except Exception:
        router_bonus = 0.0
    try:
        uncertainty_adj = float(prior.get("uncertainty_adjustment", 0.0) or 0.0)
    except Exception:
        uncertainty_adj = 0.0
    feature_alignment = ",".join(
        str(x or "").strip() for x in list(prior.get("feature_alignment") or [])[:3] if str(x or "").strip()
    )
    parts = []
    if models:
        parts.append(models)
    if transfer:
        parts.append(f"transfer={transfer}")
    if abs(router_bonus) > 0:
        parts.append(f"router={router_bonus:+.2f}")
    if abs(uncertainty_adj) > 0:
        parts.append(f"uncert={uncertainty_adj:+.2f}")
    if feature_alignment:
        parts.append(f"features={feature_alignment}")
    return " | ".join(parts)


def _chart_state_router_summary(row: dict) -> str:
    if not isinstance(row, dict) or not row:
        return ""
    state_label = str(row.get("state_label") or "").strip().lower()
    session = str(row.get("session") or "").strip()
    timeframe = str(row.get("timeframe") or "").strip()
    plan = str(row.get("follow_up_plan") or "").strip().lower()
    resolved = int(((row.get("stats") or {}).get("resolved", 0) or 0))
    score = _safe_float(row.get("state_score"), 0.0)
    parts = []
    if state_label:
        parts.append(state_label)
    if session:
        parts.append(session)
    if timeframe:
        parts.append(timeframe)
    if resolved > 0:
        parts.append(f"resolved={resolved}")
    if score > 0:
        parts.append(f"score={round(score, 2)}")
    if plan:
        parts.append(plan)
    return " | ".join(parts)


def _avg(values: list[float]) -> float:
    arr = [float(v) for v in list(values or [])]
    return (sum(arr) / len(arr)) if arr else 0.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(float(low), min(float(high), float(value)))


def _pattern_family(pattern: str) -> str:
    text = str(pattern or "").strip()
    if not text:
        return "unknown"
    primary = str(text.split("|", 1)[0] or "").strip().lower()
    if not primary:
        return "unknown"
    alias = {
        "scalp_flow_force": "scalp_flow",
        "pullback_limit": "pullback_limit",
        "failed_fade_follow_stop": "failed_fade_follow_stop",
        "choch_entry": "choch_entry",
        "ob_bounce": "ob_bounce",
    }
    return alias.get(primary, primary)


def _value_bucket(value: float, *, low: float, high: float, labels: tuple[str, str, str]) -> str:
    v = _safe_float(value, 0.0)
    if v <= float(low):
        return str(labels[0])
    if v >= float(high):
        return str(labels[2])
    return str(labels[1])


def _direction_token(direction: str) -> str:
    side = str(direction or "").strip().lower()
    if side in {"buy", "long"}:
        return "long"
    if side in {"sell", "short"}:
        return "short"
    return "unknown"


def _chart_follow_up_plan(state_label: str, direction: str) -> str:
    state = str(state_label or "").strip().lower()
    side = _direction_token(direction)
    if state == "continuation_drive":
        return "follow_with_shallow_retest_or_break_stop" if side in {"long", "short"} else ""
    if state == "pullback_absorption":
        return "follow_with_limit_on_micro_pullback" if side in {"long", "short"} else ""
    if state == "repricing_transition":
        return "wait_pause_then_follow_with_stop" if side in {"long", "short"} else ""
    if state == "breakout_drive":
        return "follow_with_stop_after_micro_consolidation" if side in {"long", "short"} else ""
    if state == "failed_fade_risk":
        return "avoid_fade_flip_to_follow_stop"
    if state == "reversal_exhaustion":
        return "wait_reversal_confirmation_then_probe"
    return ""


def _classify_chart_state(direction: str, request_ctx: dict, capture_features: Optional[dict] = None) -> dict:
    ctx = dict(request_ctx or {})
    raw_scores = dict(ctx.get("raw_scores") or {})
    features = dict(capture_features or {})
    side = _direction_token(direction)
    dir_sign = 1.0 if side == "long" else -1.0 if side == "short" else 0.0
    delta_proxy = _safe_float(features.get("delta_proxy"), 0.0)
    imbalance = _safe_float(features.get("depth_imbalance"), 0.0)
    rejection = _safe_float(features.get("rejection_ratio"), 0.0)
    bar_volume = _safe_float(features.get("bar_volume_proxy"), 0.0)
    spread_expansion = _safe_float(features.get("spread_expansion"), 1.0)
    day_type = str(features.get("day_type") or "trend").strip().lower() or "trend"
    h1_trend = str(
        raw_scores.get("scalp_force_trend_h1")
        or raw_scores.get("trend_h1")
        or raw_scores.get("h1_trend")
        or "unknown"
    ).strip().lower() or "unknown"
    try:
        m5_momentum = _safe_float(
            raw_scores.get("m5_momentum")
            or raw_scores.get("scalp_m5_momentum")
            or raw_scores.get("momentum_m5"),
            0.0,
        )
    except Exception:
        m5_momentum = 0.0
    delta_aligned = dir_sign * delta_proxy
    imbalance_aligned = dir_sign * imbalance
    momentum_aligned = dir_sign * m5_momentum
    volume_bucket = _value_bucket(bar_volume, low=0.25, high=0.60, labels=("low", "medium", "high"))
    rejection_bucket = _value_bucket(rejection, low=0.18, high=0.40, labels=("low", "medium", "high"))
    imbalance_bucket = _value_bucket(imbalance_aligned, low=-0.015, high=0.025, labels=("against", "mixed", "aligned"))
    delta_bucket = _value_bucket(delta_aligned, low=-0.04, high=0.08, labels=("against", "mixed", "aligned"))
    spread_bucket = _value_bucket(spread_expansion, low=1.03, high=1.12, labels=("calm", "expanding", "wide"))
    pattern_family = _pattern_family(ctx.get("pattern") or "")
    continuation_bias = delta_aligned + (imbalance_aligned * 1.25) + max(0.0, momentum_aligned * 0.02)
    state_label = "range_probe"
    if day_type == "panic_spread":
        state_label = "panic_dislocation"
    elif continuation_bias >= 0.14 and rejection <= 0.18 and bar_volume >= 0.45:
        state_label = "continuation_drive"
    elif day_type == "fast_expansion" and continuation_bias >= 0.08 and rejection <= 0.24 and bar_volume >= 0.40:
        state_label = "breakout_drive"
    elif continuation_bias >= 0.03 and rejection <= 0.34 and bar_volume >= 0.25:
        state_label = "pullback_absorption"
    elif day_type == "repricing" and continuation_bias >= 0.02 and rejection <= 0.28:
        state_label = "repricing_transition"
    elif continuation_bias <= -0.06 and rejection <= 0.22 and bar_volume >= 0.30:
        state_label = "failed_fade_risk"
    elif rejection >= 0.40 and abs(delta_proxy) <= 0.08:
        state_label = "reversal_exhaustion"
    return {
        "state_label": state_label,
        "pattern_family": pattern_family,
        "day_type": day_type,
        "h1_trend": h1_trend,
        "delta_bucket": delta_bucket,
        "imbalance_bucket": imbalance_bucket,
        "rejection_bucket": rejection_bucket,
        "volume_bucket": volume_bucket,
        "spread_bucket": spread_bucket,
        "continuation_bias": round(continuation_bias, 4),
        "follow_up_plan": _chart_follow_up_plan(state_label, side),
    }


def classify_xau_day_type(features: dict) -> dict:
    feat = dict(features or {})
    spread_avg_pct = _safe_float(feat.get("spread_avg_pct"), 0.0)
    spread_expansion = _safe_float(feat.get("spread_expansion"), 1.0)
    drift_abs = abs(_safe_float(feat.get("mid_drift_pct"), 0.0))
    delta_abs = abs(_safe_float(feat.get("delta_proxy"), 0.0))
    rejection_ratio = _safe_float(feat.get("rejection_ratio"), 0.0)
    bar_volume_proxy = _safe_float(feat.get("bar_volume_proxy"), 0.0)
    reasons: list[str] = []

    panic_spread_expansion = max(
        1.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_PANIC_SPREAD_EXPANSION", 1.16) or 1.16),
    )
    panic_spread_pct = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_PANIC_SPREAD_PCT", 0.0025) or 0.0025),
    )
    panic_drift = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_PANIC_MIN_ABS_DRIFT_PCT", 0.008) or 0.008),
    )
    panic_delta = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_PANIC_MIN_ABS_DELTA_PROXY", 0.14) or 0.14),
    )
    panic_rejection = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_PANIC_MAX_REJECTION", 0.18) or 0.18),
    )
    panic_bar_volume = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_PANIC_MIN_BAR_VOLUME_PROXY", 0.55) or 0.55),
    )
    fast_expansion_min = max(
        1.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_SPREAD_EXPANSION", 1.12) or 1.12),
    )
    fast_expansion_max_spread_pct = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MAX_SPREAD_PCT", 0.0022) or 0.0022),
    )
    fast_expansion_drift = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_ABS_DRIFT_PCT", 0.0025) or 0.0025),
    )
    fast_expansion_delta = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_ABS_DELTA_PROXY", 0.10) or 0.10),
    )
    fast_expansion_bar_volume = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_BAR_VOLUME_PROXY", 0.45) or 0.45),
    )
    fast_expansion_rejection = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_REJECTION", 0.20) or 0.20),
    )
    repricing_drift = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_ABS_DRIFT_PCT", 0.010) or 0.010),
    )
    repricing_delta = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_ABS_DELTA_PROXY", 0.08) or 0.08),
    )
    repricing_bar_volume = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_BAR_VOLUME_PROXY", 0.40) or 0.40),
    )
    repricing_rejection = max(
        0.0,
        float(getattr(config, "XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_REJECTION", 0.10) or 0.10),
    )

    panic_from_expansion = (
        spread_expansion >= panic_spread_expansion
        and drift_abs >= panic_drift
        and delta_abs >= panic_delta
        and rejection_ratio <= panic_rejection
        and bar_volume_proxy >= panic_bar_volume
    )
    if spread_avg_pct >= panic_spread_pct or panic_from_expansion:
        if spread_avg_pct >= panic_spread_pct:
            reasons.append("spread_avg_pct")
        if panic_from_expansion:
            reasons.extend(["spread_expansion", "one_way_liquidity_stress"])
        return {
            "day_type": "panic_spread",
            "reasons": reasons,
            "metrics": {
                "spread_avg_pct": round(spread_avg_pct, 6),
                "spread_expansion": round(spread_expansion, 4),
                "mid_drift_pct_abs": round(drift_abs, 6),
                "delta_proxy_abs": round(delta_abs, 4),
                "rejection_ratio": round(rejection_ratio, 4),
                "bar_volume_proxy": round(bar_volume_proxy, 4),
            },
        }

    if (
        spread_expansion >= fast_expansion_min
        and spread_avg_pct <= fast_expansion_max_spread_pct
        and drift_abs >= fast_expansion_drift
        and delta_abs >= fast_expansion_delta
        and bar_volume_proxy >= fast_expansion_bar_volume
        and rejection_ratio >= fast_expansion_rejection
    ):
        return {
            "day_type": "fast_expansion",
            "reasons": ["spread_expansion_fast", "tradable_liquidity_expansion"],
            "metrics": {
                "spread_avg_pct": round(spread_avg_pct, 6),
                "spread_expansion": round(spread_expansion, 4),
                "mid_drift_pct_abs": round(drift_abs, 6),
                "delta_proxy_abs": round(delta_abs, 4),
                "rejection_ratio": round(rejection_ratio, 4),
                "bar_volume_proxy": round(bar_volume_proxy, 4),
            },
        }

    if (
        drift_abs >= repricing_drift
        and delta_abs >= repricing_delta
        and bar_volume_proxy >= repricing_bar_volume
        and rejection_ratio >= repricing_rejection
    ):
        return {
            "day_type": "repricing",
            "reasons": ["drift_delta_bar_volume_repricing"],
            "metrics": {
                "spread_avg_pct": round(spread_avg_pct, 6),
                "spread_expansion": round(spread_expansion, 4),
                "mid_drift_pct_abs": round(drift_abs, 6),
                "delta_proxy_abs": round(delta_abs, 4),
                "rejection_ratio": round(rejection_ratio, 4),
                "bar_volume_proxy": round(bar_volume_proxy, 4),
            },
        }

    return {
        "day_type": "trend",
        "reasons": ["default_trend"],
        "metrics": {
            "spread_avg_pct": round(spread_avg_pct, 6),
            "spread_expansion": round(spread_expansion, 4),
            "mid_drift_pct_abs": round(drift_abs, 6),
            "delta_proxy_abs": round(delta_abs, 4),
            "rejection_ratio": round(rejection_ratio, 4),
            "bar_volume_proxy": round(bar_volume_proxy, 4),
        },
    }


def summarize_market_capture(spots_rows: list, depth_rows: list) -> dict:
    spots = list(spots_rows or [])
    depth = list(depth_rows or [])
    bids = [float(row["bid"] or 0.0) for row in spots if _safe_float(row["bid"], 0.0) > 0.0]
    asks = [float(row["ask"] or 0.0) for row in spots if _safe_float(row["ask"], 0.0) > 0.0]
    spread_pcts = [float(row["spread_pct"] or 0.0) for row in spots if _safe_float(row["spread_pct"], 0.0) > 0.0]
    mids = [((bids[idx] + asks[idx]) / 2.0) for idx in range(min(len(bids), len(asks)))]
    spot_ts = [int(_safe_float(row["event_ts"], 0.0)) for row in spots if int(_safe_float(row["event_ts"], 0.0)) > 0]
    spread_last = spread_pcts[-1] if spread_pcts else 0.0
    spread_med = statistics.median(spread_pcts) if spread_pcts else 0.0
    spread_expansion = (spread_last / spread_med) if spread_med > 0 else 1.0
    drift_pct = 0.0
    if len(mids) >= 2 and mids[0] > 0:
        drift_pct = ((mids[-1] - mids[0]) / mids[0]) * 100.0
    move_deltas = [(mids[idx] - mids[idx - 1]) for idx in range(1, len(mids))]
    up_move_abs = sum(max(0.0, float(delta)) for delta in move_deltas)
    down_move_abs = sum(max(0.0, -1.0 * float(delta)) for delta in move_deltas)
    total_move_abs = up_move_abs + down_move_abs
    delta_proxy = ((up_move_abs - down_move_abs) / total_move_abs) if total_move_abs > 0 else 0.0
    nonzero_deltas = [float(delta) for delta in move_deltas if abs(float(delta)) > 1e-9]
    tick_up_ratio = (
        sum(1 for delta in nonzero_deltas if float(delta) > 0.0) / len(nonzero_deltas)
        if nonzero_deltas
        else 0.0
    )
    rejection_ratio = 0.0
    if len(mids) >= 3:
        impulse = max(abs(mid - mids[0]) for mid in mids)
        end_move = abs(mids[-1] - mids[0])
        rejection_ratio = 0.0 if impulse <= 0 else max(0.0, 1.0 - (end_move / impulse))
    latest_depth: dict[tuple[str, int], float] = {}
    early_depth: dict[tuple[str, int], float] = {}
    late_depth: dict[tuple[str, int], float] = {}
    depth_ts = [int(_safe_float(row["event_ts"], 0.0)) for row in depth if int(_safe_float(row["event_ts"], 0.0)) > 0]
    split_ts = 0
    if depth_ts:
        split_ts = min(depth_ts) + int((max(depth_ts) - min(depth_ts)) / 2)
    for row in depth:
        side = str(row["side"] or "").strip().lower()
        level = int(_safe_float(row["level_index"], 0))
        size = float(row["size"] or 0.0)
        if side not in {"bid", "ask"} or level < 0:
            continue
        key = (side, level)
        latest_depth[key] = size
        if split_ts > 0:
            event_ts = int(_safe_float(row["event_ts"], 0.0))
            if event_ts <= split_ts:
                early_depth[key] = size
            else:
                late_depth[key] = size
    latest_bid = sum(val for (side, _lvl), val in latest_depth.items() if side == "bid")
    latest_ask = sum(val for (side, _lvl), val in latest_depth.items() if side == "ask")
    depth_imbalance = ((latest_bid - latest_ask) / (latest_bid + latest_ask)) if (latest_bid + latest_ask) > 0 else 0.0
    early_bid = sum(val for (side, _lvl), val in early_depth.items() if side == "bid")
    early_ask = sum(val for (side, _lvl), val in early_depth.items() if side == "ask")
    late_bid = sum(val for (side, _lvl), val in late_depth.items() if side == "bid")
    late_ask = sum(val for (side, _lvl), val in late_depth.items() if side == "ask")
    early_imbalance = ((early_bid - early_ask) / (early_bid + early_ask)) if (early_bid + early_ask) > 0 else 0.0
    late_imbalance = ((late_bid - late_ask) / (late_bid + late_ask)) if (late_bid + late_ask) > 0 else 0.0
    refill_shift = late_imbalance - early_imbalance
    depth_by_ts: dict[int, dict[str, float]] = {}
    for row in depth:
        event_ts = int(_safe_float(row["event_ts"], 0.0))
        side = str(row["side"] or "").strip().lower()
        size = float(row["size"] or 0.0)
        if event_ts <= 0 or side not in {"bid", "ask"}:
            continue
        bucket = depth_by_ts.setdefault(event_ts, {"bid": 0.0, "ask": 0.0})
        bucket[side] += size
    depth_points = [(ts, vals["bid"], vals["ask"]) for ts, vals in sorted(depth_by_ts.items())]
    total_depth_series = [float(bid + ask) for _ts, bid, ask in depth_points if (bid + ask) > 0]
    avg_total_depth = _avg(total_depth_series)
    total_turnover = 0.0
    for idx in range(1, len(depth_points)):
        prev_bid = float(depth_points[idx - 1][1])
        prev_ask = float(depth_points[idx - 1][2])
        cur_bid = float(depth_points[idx][1])
        cur_ask = float(depth_points[idx][2])
        total_turnover += abs(cur_bid - prev_bid) + abs(cur_ask - prev_ask)
    depth_turnover_ratio = (
        total_turnover / (avg_total_depth * max(1, len(depth_points) - 1))
        if avg_total_depth > 0 and len(depth_points) >= 2
        else 0.0
    )
    first_ts_candidates = [ts for ts in (spot_ts[:1] + depth_ts[:1]) if int(ts) > 0]
    last_ts_candidates = [ts for ts in ((spot_ts[-1:] if spot_ts else []) + (depth_ts[-1:] if depth_ts else [])) if int(ts) > 0]
    duration_sec = 0.0
    if first_ts_candidates and last_ts_candidates:
        duration_sec = max(0.0, (max(last_ts_candidates) - min(first_ts_candidates)) / 1000.0)
    spot_rate = (len(spots) / max(duration_sec, 1.0)) if spots else 0.0
    depth_rate = (len(depth) / max(duration_sec, 1.0)) if depth else 0.0
    spot_rate_norm = min(2.0, spot_rate / 1.2) if spot_rate > 0 else 0.0
    depth_rate_norm = min(2.0, depth_rate / 12.0) if depth_rate > 0 else 0.0
    depth_turnover_norm = min(2.0, depth_turnover_ratio * 4.0) if depth_turnover_ratio > 0 else 0.0
    bar_volume_proxy = _clamp((spot_rate_norm + depth_rate_norm + depth_turnover_norm) / 6.0, 0.0, 1.0)
    out = {
        "spots_count": len(spots),
        "depth_count": len(depth),
        "spread_avg_pct": _avg(spread_pcts),
        "spread_last_pct": spread_last,
        "spread_expansion": spread_expansion,
        "mid_drift_pct": drift_pct,
        "delta_proxy": delta_proxy,
        "tick_up_ratio": tick_up_ratio,
        "rejection_ratio": rejection_ratio,
        "depth_imbalance": depth_imbalance,
        "depth_refill_shift": refill_shift,
        "depth_turnover_ratio": depth_turnover_ratio,
        "bar_volume_proxy": bar_volume_proxy,
        "duration_sec": round(duration_sec, 4),
        "mid_open": mids[0] if mids else 0.0,
        "mid_close": mids[-1] if mids else 0.0,
    }
    # Deep microstructure features (entry sharpness analytics)
    try:
        from analysis.entry_sharpness import compute_deep_features
        deep = compute_deep_features(mids=mids, move_deltas=move_deltas, spot_ts=spot_ts, spread_pcts=spread_pcts, depth_points=depth_points)
        out.update(deep)
    except Exception:
        pass
    day_type = classify_xau_day_type(out)
    out["day_type"] = str(day_type.get("day_type") or "trend")
    out["day_type_reasons"] = list(day_type.get("reasons") or [])
    return out


def evaluate_xau_tick_depth_filter(features: dict, direction: str, *, confidence: float = 0.0) -> dict:
    side = str(direction or "").strip().lower()
    feat = dict(features or {})
    reasons: list[str] = []
    score = 0
    min_score = max(1, int(getattr(config, "XAU_TICK_DEPTH_FILTER_MIN_GATE_SCORE", 3) or 3))
    min_spots = max(1, int(getattr(config, "XAU_TICK_DEPTH_FILTER_MIN_SPOTS", 6) or 6))
    min_depth = max(0, int(getattr(config, "XAU_TICK_DEPTH_FILTER_MIN_DEPTH_QUOTES", 40) or 40))
    spread_avg_pct = _safe_float(feat.get("spread_avg_pct"), 0.0)
    spread_expansion = _safe_float(feat.get("spread_expansion"), 9.9)
    imbalance = _safe_float(feat.get("depth_imbalance"), 0.0)
    refill_shift = _safe_float(feat.get("depth_refill_shift"), 0.0)
    rejection_ratio = _safe_float(feat.get("rejection_ratio"), 1.0)
    delta_proxy = _safe_float(feat.get("delta_proxy"), 0.0)
    bar_volume_proxy = _safe_float(feat.get("bar_volume_proxy"), 0.0)
    spots_count = int(_safe_float(feat.get("spots_count"), 0))
    depth_count = int(_safe_float(feat.get("depth_count"), 0))
    day_type_meta = classify_xau_day_type(feat)
    day_type = str(day_type_meta.get("day_type") or feat.get("day_type") or "trend").strip().lower() or "trend"
    if spots_count >= min_spots:
        score += 1
    else:
        reasons.append(f"spots<{min_spots}")
    if depth_count >= min_depth:
        score += 1
    else:
        reasons.append(f"depth<{min_depth}")
    if spread_avg_pct <= float(getattr(config, "XAU_TICK_DEPTH_FILTER_MAX_SPREAD_PCT", 0.0022) or 0.0022):
        score += 1
    else:
        reasons.append("spread_too_wide")
    if spread_expansion <= float(getattr(config, "XAU_TICK_DEPTH_FILTER_MAX_SPREAD_EXPANSION", 1.12) or 1.12):
        score += 1
    else:
        reasons.append("spread_expanding")
    if bar_volume_proxy >= float(getattr(config, "XAU_TICK_DEPTH_FILTER_MIN_BAR_VOLUME_PROXY", 0.35) or 0.35):
        score += 1
    else:
        reasons.append("bar_volume_too_low")
    if side == "long":
        if imbalance <= float(getattr(config, "XAU_TICK_DEPTH_FILTER_LONG_MAX_IMBALANCE", -0.01) or -0.01):
            score += 1
        else:
            reasons.append("long_imbalance_not_supportive")
        if refill_shift <= float(getattr(config, "XAU_TICK_DEPTH_FILTER_LONG_MAX_REFILL_SHIFT", -0.03) or -0.03):
            score += 1
        else:
            reasons.append("long_refill_not_supportive")
        if delta_proxy <= float(getattr(config, "XAU_TICK_DEPTH_FILTER_LONG_MAX_DELTA_PROXY", -0.01) or -0.01):
            score += 1
        else:
            reasons.append("long_delta_not_supportive")
    elif side == "short":
        if imbalance >= float(getattr(config, "XAU_TICK_DEPTH_FILTER_SHORT_MIN_IMBALANCE", 0.005) or 0.005):
            score += 1
        else:
            reasons.append("short_imbalance_not_supportive")
        if rejection_ratio <= float(getattr(config, "XAU_TICK_DEPTH_FILTER_SHORT_MAX_REJECTION", 0.20) or 0.20):
            score += 1
        else:
            reasons.append("short_rejection_too_high")
        if delta_proxy >= float(getattr(config, "XAU_TICK_DEPTH_FILTER_SHORT_MIN_DELTA_PROXY", 0.01) or 0.01):
            score += 1
        else:
            reasons.append("short_delta_not_supportive")
    if float(confidence or 0.0) >= 72.0:
        score += 1
    panic_block = bool(getattr(config, "XAU_TICK_DEPTH_FILTER_PANIC_SPREAD_BLOCK", True))
    if day_type == "panic_spread" and panic_block:
        reasons.append("panic_spread_day_block")
    gate_pass = bool(score >= min_score)
    sample_delta = max(0, int(getattr(config, "XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_SCORE_DELTA", 1) or 1))
    sample_min_confidence = float(getattr(config, "XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_CONFIDENCE", 73.0) or 73.0)
    sample_max_spread_expansion = float(getattr(config, "XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MAX_SPREAD_EXPANSION", 1.05) or 1.05)
    sample_min_bar_volume_proxy = float(getattr(config, "XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_BAR_VOLUME_PROXY", 0.45) or 0.45)
    if day_type == "repricing":
        sample_delta += max(
            0,
            int(getattr(config, "XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_SCORE_BONUS", 1) or 1),
        )
        sample_min_confidence += float(
            getattr(config, "XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MIN_CONFIDENCE_DELTA", -1.0) or -1.0
        )
        sample_max_spread_expansion *= max(
            0.5,
            float(getattr(config, "XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MAX_SPREAD_EXPANSION_MULT", 1.05) or 1.05),
        )
        sample_min_bar_volume_proxy *= max(
            0.5,
            float(getattr(config, "XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MIN_BAR_VOLUME_MULT", 0.95) or 0.95),
        )
    if day_type == "fast_expansion":
        sample_delta += max(
            0,
            int(getattr(config, "XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_SCORE_BONUS", 1) or 1),
        )
        sample_min_confidence += float(
            getattr(config, "XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MIN_CONFIDENCE_DELTA", -0.5) or -0.5
        )
        sample_max_spread_expansion *= max(
            0.75,
            float(getattr(config, "XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MAX_SPREAD_EXPANSION_MULT", 1.35) or 1.35),
        )
        sample_min_bar_volume_proxy *= max(
            0.5,
            float(getattr(config, "XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MIN_BAR_VOLUME_MULT", 1.0) or 1.0),
        )
    sample_threshold = max(1, int(min_score) - sample_delta)
    sample_pass = False
    sample_mode = ""
    if (
        (not gate_pass)
        and not (day_type == "panic_spread" and panic_block)
        and bool(getattr(config, "XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_ENABLED", True))
        and score >= sample_threshold
        and float(confidence or 0.0) >= sample_min_confidence
        and spots_count >= min_spots
        and depth_count >= min_depth
        and spread_avg_pct <= float(getattr(config, "XAU_TICK_DEPTH_FILTER_MAX_SPREAD_PCT", 0.0022) or 0.0022)
        and spread_expansion <= sample_max_spread_expansion
        and bar_volume_proxy >= sample_min_bar_volume_proxy
    ):
        sample_pass = True
        if day_type == "repricing":
            sample_mode = "repricing_near_pass"
        elif day_type == "fast_expansion":
            sample_mode = "fast_expansion_near_pass"
        else:
            sample_mode = "near_pass"
        reasons = list(reasons) + ["canary_sample_near_pass"]
    if day_type == "panic_spread" and panic_block:
        gate_pass = False
        sample_pass = False
        sample_mode = ""
    normalized = _clamp(score / max(1, min_score + 3), 0.0, 1.0)
    return {
        "pass": gate_pass,
        "canary_sample_pass": sample_pass,
        "sample_mode": sample_mode,
        "day_type": day_type,
        "day_type_reasons": list(day_type_meta.get("reasons") or []),
        "sample_risk_multiplier": round(
            float(getattr(config, "XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_RISK_MULTIPLIER", 0.70) or 0.70),
            4,
        ) if sample_pass else 1.0,
        "score": int(score),
        "score_ratio": round(float(normalized), 4),
        "reasons": reasons,
        "features": {
            "spots_count": spots_count,
            "depth_count": depth_count,
            "spread_avg_pct": round(spread_avg_pct, 6),
            "spread_expansion": round(spread_expansion, 4),
            "depth_imbalance": round(imbalance, 4),
            "depth_refill_shift": round(refill_shift, 4),
            "rejection_ratio": round(rejection_ratio, 4),
            "delta_proxy": round(delta_proxy, 4),
            "bar_volume_proxy": round(bar_volume_proxy, 4),
            "tick_up_ratio": round(_safe_float(feat.get("tick_up_ratio"), 0.0), 4),
            "depth_turnover_ratio": round(_safe_float(feat.get("depth_turnover_ratio"), 0.0), 4),
            "mid_drift_pct": round(_safe_float(feat.get("mid_drift_pct"), 0.0), 6),
            "day_type": day_type,
        },
    }


def _price_crossed_target_from_fill(direction: str, actual_entry: float, planned_tp: float) -> bool:
    if actual_entry <= 0 or planned_tp <= 0:
        return False
    side = str(direction or "").strip().lower()
    if side == "long":
        return actual_entry >= planned_tp
    if side == "short":
        return actual_entry <= planned_tp
    return False


def _stop_invalid_from_fill(direction: str, actual_entry: float, planned_sl: float) -> bool:
    if actual_entry <= 0 or planned_sl <= 0:
        return False
    side = str(direction or "").strip().lower()
    if side == "long":
        return planned_sl >= actual_entry
    if side == "short":
        return planned_sl <= actual_entry
    return False


def _classify_trade_abnormality(
    direction: str,
    actual_entry: float,
    planned_sl: float,
    planned_tp: float,
    execution_meta=None,
) -> dict:
    flags: list[str] = []
    if _price_crossed_target_from_fill(direction, actual_entry, planned_tp):
        flags.append("repair_like_target_crossed")
    if _stop_invalid_from_fill(direction, actual_entry, planned_sl):
        flags.append("invalid_stop_from_fill")
    meta_obj = execution_meta if isinstance(execution_meta, dict) else _safe_json_dict(str(execution_meta or "{}"))
    try:
        meta_text = json.dumps(meta_obj, ensure_ascii=True, sort_keys=True).lower()
    except Exception:
        meta_text = str(execution_meta or "").strip().lower()
    if "repair_missing_sl" in meta_text:
        flags.append("manager_repaired_missing_sl")
    if "invalid_tp" in meta_text or "close_at_planned_target" in meta_text:
        flags.append("manager_invalid_tp_recovery")
    uniq_flags = sorted(set(str(x or "").strip() for x in list(flags or []) if str(x or "").strip()))
    return {
        "flags": uniq_flags,
        "exclude_from_learning": bool(uniq_flags),
        "repair_like": "repair_like_target_crossed" in uniq_flags,
    }


def _max_drawdown_usd(pnls: list[float]) -> float:
    if not pnls:
        return 0.0
    peak = 0.0
    equity = 0.0
    max_dd = 0.0
    for pnl in list(pnls or []):
        equity += float(pnl or 0.0)
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return round(abs(max_dd), 4)


def _stddev(values: list[float]) -> float:
    vals = [float(v) for v in list(values or [])]
    if len(vals) < 2:
        return 0.0
    try:
        return float(statistics.pstdev(vals))
    except Exception:
        return 0.0


def _sharpe_like(pnls: list[float]) -> Optional[float]:
    vals = [float(v) for v in list(pnls or [])]
    if len(vals) < 2:
        return None
    std = _stddev(vals)
    if std <= 1e-9:
        return None
    mean = sum(vals) / max(len(vals), 1)
    return float(mean / std) * math.sqrt(float(len(vals)))


def _deflated_sharpe_proxy(pnls: list[float], *, trials: int = 1) -> Optional[float]:
    sharpe = _sharpe_like(pnls)
    if sharpe is None:
        return None
    n = max(1, len(list(pnls or [])))
    k = max(1, int(trials or 1))
    penalty = math.sqrt(max(math.log(float(k) + 1.0), 0.0) / float(n))
    return round(float(sharpe) - float(penalty), 4)


class LiveProfileAutopilot:
    def __init__(
        self,
        *,
        report_dir: Optional[str] = None,
        runtime_dir: Optional[str] = None,
        env_local_path: Optional[str] = None,
        neural_gate_db_path: Optional[str] = None,
        mt5_db_path: Optional[str] = None,
        ctrader_db_path: Optional[str] = None,
    ):
        base_dir = Path(__file__).resolve().parent.parent
        data_dir = base_dir / "data"
        self.report_dir = Path(report_dir or (data_dir / "reports"))
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir = Path(runtime_dir or (data_dir / "runtime"))
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.env_local_path = Path(env_local_path or (base_dir / ".env.local"))
        ng_cfg = str(getattr(config, "NEURAL_GATE_LEARNING_DB_PATH", "") or "").strip()
        mt5_cfg = str(getattr(config, "MT5_AUTOPILOT_DB_PATH", "") or "").strip()
        ctrader_cfg = str(getattr(config, "CTRADER_DB_PATH", "") or "").strip()
        self.neural_gate_db_path = Path(neural_gate_db_path or ng_cfg or (data_dir / "neural_gate_learning.db"))
        self.mt5_db_path = Path(mt5_db_path or mt5_cfg or (data_dir / "mt5_autopilot.db"))
        self.ctrader_db_path = Path(ctrader_db_path or ctrader_cfg or (data_dir / "ctrader_openapi.db"))
        self.signal_learning_db_path = data_dir / "signal_learning.db"
        self.state_path = self.runtime_dir / "auto_live_profile_state.json"
        self.canary_audit_state_path = self.runtime_dir / "canary_post_trade_audit_state.json"
        self.ct_only_watch_state_path = self.runtime_dir / "ct_only_watch_state.json"
        self.xau_direct_lane_tune_state_path = self.runtime_dir / "xau_direct_lane_tune_state.json"
        self.parameter_trial_state_path = self.runtime_dir / "parameter_trials.json"
        self._last_family_collect_summary = {"rows_seen": 0, "excluded_abnormal_rows": 0}

    def _connect_neural(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.neural_gate_db_path), timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _connect_signal_learning(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.signal_learning_db_path), timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _connect_mt5(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.mt5_db_path), timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _connect_ctrader(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.ctrader_db_path), timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def _report_path(self, name: str) -> Path:
        return self.report_dir / f"{name}.json"

    @staticmethod
    def _version_info() -> dict:
        return {
            "ctrader_profile_version": str(getattr(config, "CTRADER_PROFILE_VERSION", "ctrader_profile_v1") or "ctrader_profile_v1"),
            "mission_stack_version": str(getattr(config, "MISSION_STACK_VERSION", "mission_stack_v1") or "mission_stack_v1"),
            "strategy_generator_version": 1,
        }

    @staticmethod
    def _execution_scope() -> dict:
        ctrader_live = bool(
            getattr(config, "CTRADER_ENABLED", False)
            and getattr(config, "CTRADER_AUTOTRADE_ENABLED", False)
            and (not getattr(config, "CTRADER_DRY_RUN", True))
        )
        mt5_live_xau = bool(getattr(config, "MT5_ENABLED", False) and getattr(config, "MT5_EXECUTE_XAUUSD", False))
        mt5_live_crypto = bool(getattr(config, "MT5_ENABLED", False) and getattr(config, "MT5_EXECUTE_CRYPTO", False))
        return {
            "backend_focus": "ctrader_only" if ctrader_live and (not mt5_live_xau) and (not mt5_live_crypto) else "mixed",
            "ctrader_live_enabled": ctrader_live,
            "mt5_live_xau_enabled": mt5_live_xau,
            "mt5_live_crypto_enabled": mt5_live_crypto,
            "persistent_canary_ctrader_enabled": bool(getattr(config, "PERSISTENT_CANARY_CTRADER_ENABLED", False)),
            "persistent_canary_mt5_enabled": bool(getattr(config, "PERSISTENT_CANARY_MT5_ENABLED", False)),
        }

    @staticmethod
    def _load_json(path: Path) -> dict:
        try:
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return {}

    @staticmethod
    def _normalized_text_list(items) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for raw in list(items or []):
            text = str(raw or "").strip()
            if not text:
                continue
            lowered = text.lower()
            if lowered in {"unknown", "none", "null", "n/a"}:
                continue
            if text not in seen:
                seen.add(text)
                out.append(text)
        return out

    @staticmethod
    def _present_context_value(value, *, lower: bool = False) -> str:
        text = str(value or "").strip()
        if not text or text.lower() in {"unknown", "none", "null", "n/a"}:
            return ""
        return text.lower() if lower else text

    @staticmethod
    def _extract_feature_context(features_json: str) -> dict:
        feat = {}
        try:
            raw = json.loads(str(features_json or "{}") or "{}")
            feat = raw if isinstance(raw, dict) else {}
        except Exception:
            feat = {}
        raw_scores = dict(feat.get("raw_scores", {}) or {})
        session = (
            feat.get("session_zone")
            or raw_scores.get("session_zone")
            or raw_scores.get("session")
            or raw_scores.get("signal_session")
            or raw_scores.get("scalp_profile_session")
            or raw_scores.get("kill_zone")
            or ""
        )
        pattern = feat.get("pattern") or raw_scores.get("pattern") or ""
        return {
            "session": _norm_signature(str(session or "")) or "unknown",
            "pattern": str(pattern or "").strip() or "unknown",
        }

    @staticmethod
    def _load_market_capture_features(conn: sqlite3.Connection, *, symbol: str, execution_meta_json: str) -> dict:
        meta = _safe_json_dict(str(execution_meta_json or "{}"))
        run_id = str((meta.get("market_capture") or {}).get("run_id") or "").strip()
        token = _norm_symbol(symbol)
        if not run_id or not token:
            return {}
        try:
            spots = conn.execute(
                """
                SELECT bid, ask, spread, spread_pct, event_ts
                  FROM ctrader_spot_ticks
                 WHERE run_id = ?
                   AND symbol = ?
                 ORDER BY event_ts ASC, id ASC
                """,
                (run_id, token),
            ).fetchall()
            depth = conn.execute(
                """
                SELECT side, size, price, level_index, event_ts
                  FROM ctrader_depth_quotes
                 WHERE run_id = ?
                   AND symbol = ?
                 ORDER BY event_ts ASC, id ASC
                """,
                (run_id, token),
            ).fetchall()
        except Exception:
            return {}
        if not spots and not depth:
            return {}
        return summarize_market_capture(spots, depth)

    def build_missed_opportunity_audit_report(self, *, days: int = 14) -> dict:
        lookback_days = max(1, int(days or 14))
        since_dt = _utc_now() - timedelta(days=lookback_days)
        since_iso = _iso(since_dt)
        min_sample = max(2, int(getattr(config, "AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 6) or 6))
        min_wr = float(getattr(config, "AUTO_APPLY_LIVE_PROFILE_MIN_WIN_RATE", 0.60) or 0.60)
        min_pnl = float(getattr(config, "AUTO_APPLY_LIVE_PROFILE_MIN_PNL_USD", 0.0) or 0.0)
        current_canary_conf = float(getattr(config, "NEURAL_GATE_CANARY_MIN_CONFIDENCE", 72.0) or 72.0)
        current_canary_floor = float(getattr(config, "NEURAL_GATE_CANARY_FIXED_ALLOW_LOW", 0.0) or 0.0)
        out = {
            "ok": False,
            "days": lookback_days,
            "since_utc": since_iso,
            "generated_at": _iso(_utc_now()),
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "neural_gate_db_path": str(self.neural_gate_db_path),
            "current_policy": {
                "canary_min_confidence": round(current_canary_conf, 3),
                "canary_fixed_allow_low": round(current_canary_floor, 3),
            },
            "summary": {"missed_rows": 0, "allow_rows": 0, "missed_positive_groups": 0},
            "symbols": [],
            "recommendations": [],
            "error": "",
        }
        if not self.neural_gate_db_path.exists():
            out["error"] = "neural_gate_db_not_found"
            return out

        missed_rows = []
        allow_baseline: dict[tuple[str, str], dict] = {}
        try:
            with closing(self._connect_neural()) as conn:
                missed_rows = conn.execute(
                    """
                    SELECT created_at, source, signal_symbol, broker_symbol, confidence, neural_prob, min_prob,
                           decision, decision_reason, outcome_type, outcome, pnl_usd, features_json
                      FROM neural_gate_decisions
                     WHERE created_at >= ?
                       AND resolved=1
                       AND outcome IN (0, 1)
                       AND decision IN ('neural_block', 'other_skip')
                     ORDER BY created_at ASC
                    """,
                    (since_iso,),
                ).fetchall()
                allow_rows = conn.execute(
                    """
                    SELECT source, signal_symbol, broker_symbol, outcome, pnl_usd
                      FROM neural_gate_decisions
                     WHERE created_at >= ?
                       AND resolved=1
                       AND outcome IN (0, 1)
                       AND decision='allow'
                     ORDER BY created_at ASC
                    """,
                    (since_iso,),
                ).fetchall()
        except Exception as e:
            out["error"] = f"query_error:{e}"
            return out

        out["summary"]["missed_rows"] = len(missed_rows)
        out["summary"]["allow_rows"] = len(allow_rows)
        for row in list(allow_rows or []):
            sym = _norm_symbol(row["signal_symbol"] or row["broker_symbol"] or "")
            src = _norm_source(row["source"])
            bucket = allow_baseline.setdefault((sym, src), _new_bucket())
            _update_bucket(bucket, _safe_float(row["pnl_usd"], 0.0), _safe_int(row["outcome"], -1))

        groups: dict[tuple[str, str, str, str, str], dict] = {}
        symbol_rollup: dict[str, dict] = {}
        for row in list(missed_rows or []):
            sym = _norm_symbol(row["signal_symbol"] or row["broker_symbol"] or "")
            src = _norm_source(row["source"])
            decision = _norm_source(row["decision"])
            reason = _norm_source(row["decision_reason"])
            outcome_type = _norm_source(row["outcome_type"])
            pnl = _safe_float(row["pnl_usd"], 0.0)
            outcome = _safe_int(row["outcome"], -1)
            conf = _safe_float(row["confidence"], 0.0)
            neural_prob_f = None if row["neural_prob"] is None else _safe_float(row["neural_prob"], 0.0)
            min_prob_f = None if row["min_prob"] is None else _safe_float(row["min_prob"], 0.0)
            prob_gap = None
            if (neural_prob_f is not None) and (min_prob_f is not None):
                prob_gap = max(0.0, float(min_prob_f) - float(neural_prob_f))
            ctx = self._extract_feature_context(str(row["features_json"] or ""))
            group_key = (sym, src, decision, reason, outcome_type)
            rec = groups.setdefault(
                group_key,
                {
                    "symbol": sym,
                    "source": src,
                    "decision": decision,
                    "decision_reason": reason,
                    "outcome_type": outcome_type,
                    "bucket": _new_bucket(),
                    "conf_bands": {},
                    "prob_gap_bands": {},
                    "sessions": {},
                    "patterns": {},
                    "neural_probs": [],
                    "prob_gaps": [],
                    "winning_neural_probs": [],
                },
            )
            _update_bucket(rec["bucket"], pnl, outcome)
            _update_bucket(rec["conf_bands"].setdefault(_confidence_band(conf), _new_bucket()), pnl, outcome)
            _update_bucket(rec["prob_gap_bands"].setdefault(_prob_gap_band(prob_gap), _new_bucket()), pnl, outcome)
            _update_bucket(rec["sessions"].setdefault(str(ctx["session"]), _new_bucket()), pnl, outcome)
            _update_bucket(rec["patterns"].setdefault(str(ctx["pattern"]), _new_bucket()), pnl, outcome)
            if neural_prob_f is not None:
                rec["neural_probs"].append(float(neural_prob_f))
            if prob_gap is not None:
                rec["prob_gaps"].append(float(prob_gap))
            if int(outcome) == 1 and neural_prob_f is not None:
                rec["winning_neural_probs"].append(float(neural_prob_f))
            sym_roll = symbol_rollup.setdefault(sym, {"missed": _new_bucket(), "allow": _new_bucket()})
            _update_bucket(sym_roll["missed"], pnl, outcome)

        for (sym, _src), bucket in allow_baseline.items():
            sym_roll = symbol_rollup.setdefault(sym, {"missed": _new_bucket(), "allow": _new_bucket()})
            sym_roll["allow"] = _merge_bucket(sym_roll.get("allow") or _new_bucket(), bucket)

        groups_out = []
        recommendations = []
        for rec in groups.values():
            stat = _finalize_bucket(rec["bucket"])
            conf_bands = [
                {"band": band, **_finalize_bucket(bucket)}
                for band, bucket in (rec.get("conf_bands") or {}).items()
                if int(bucket.get("resolved", 0) or 0) > 0
            ]
            prob_gap_bands = [
                {"band": band, **_finalize_bucket(bucket)}
                for band, bucket in (rec.get("prob_gap_bands") or {}).items()
                if int(bucket.get("resolved", 0) or 0) > 0
            ]
            sessions = [
                {"session": name, **_finalize_bucket(bucket)}
                for name, bucket in (rec.get("sessions") or {}).items()
                if int(bucket.get("resolved", 0) or 0) > 0
            ]
            patterns = [
                {"pattern": name, **_finalize_bucket(bucket)}
                for name, bucket in (rec.get("patterns") or {}).items()
                if int(bucket.get("resolved", 0) or 0) > 0
            ]
            conf_bands.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)
            prob_gap_bands.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)
            sessions.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)
            patterns.sort(key=lambda x: (float(x.get("pnl_usd", 0.0)), float(x.get("win_rate", 0.0)), int(x.get("resolved", 0))), reverse=True)
            baseline = _finalize_bucket(allow_baseline.get((rec["symbol"], rec["source"])) or _new_bucket())
            candidate_band = next(
                (
                    band_row for band_row in conf_bands
                    if int(band_row.get("resolved", 0) or 0) >= max(3, min_sample // 2)
                    and float(band_row.get("win_rate", 0.0) or 0.0) >= min_wr
                    and float(band_row.get("pnl_usd", 0.0) or 0.0) > 0.0
                ),
                None,
            )
            proposed_canary_min_conf = None
            if candidate_band is not None:
                floor = _confidence_floor_for_band(str(candidate_band.get("band", "") or ""))
                if floor is not None and float(floor) < current_canary_conf:
                    proposed_canary_min_conf = float(floor)
            proposed_canary_allow_low = None
            if rec["winning_neural_probs"]:
                q25 = _quantile(list(rec["winning_neural_probs"]), 0.25)
                if q25 is not None:
                    suggested = round(max(0.53, float(q25) - 0.01), 3)
                    if current_canary_floor > 0.0 and suggested < current_canary_floor:
                        proposed_canary_allow_low = suggested

            action = "collect_more_sample"
            why = "insufficient_edge"
            if rec["decision"] == "neural_block" and rec["decision_reason"] == "below_neural_min_prob":
                if int(stat.get("resolved", 0) or 0) >= min_sample and float(stat.get("win_rate", 0.0) or 0.0) >= min_wr and float(stat.get("pnl_usd", 0.0) or 0.0) > min_pnl:
                    action = "canary_reinstate"
                    why = "blocked_counterfactual_edge_positive"
                elif int(stat.get("resolved", 0) or 0) >= min_sample and (float(stat.get("win_rate", 0.0) or 0.0) < 0.45 or float(stat.get("pnl_usd", 0.0) or 0.0) <= 0.0):
                    action = "keep_blocked"
                    why = "blocked_counterfactual_edge_negative"
            elif int(stat.get("resolved", 0) or 0) >= min_sample and float(stat.get("win_rate", 0.0) or 0.0) >= min_wr and float(stat.get("pnl_usd", 0.0) or 0.0) > min_pnl:
                action = "review_guard"
                why = "non_neural_skip_counterfactual_edge_positive"

            group_row = {
                "symbol": rec["symbol"],
                "source": rec["source"],
                "decision": rec["decision"],
                "decision_reason": rec["decision_reason"],
                "outcome_type": rec["outcome_type"],
                "stats": stat,
                "baseline_allow": baseline,
                "top_conf_bands": conf_bands[:4],
                "top_prob_gap_bands": prob_gap_bands[:4],
                "top_sessions": sessions[:4],
                "top_patterns": patterns[:4],
                "neural_prob_quantiles": {
                    "p25": round(_quantile(list(rec["neural_probs"]), 0.25), 4) if rec["neural_probs"] else None,
                    "median": round(_quantile(list(rec["neural_probs"]), 0.50), 4) if rec["neural_probs"] else None,
                    "p75": round(_quantile(list(rec["neural_probs"]), 0.75), 4) if rec["neural_probs"] else None,
                },
                "prob_gap_quantiles": {
                    "p25": round(_quantile(list(rec["prob_gaps"]), 0.25), 4) if rec["prob_gaps"] else None,
                    "median": round(_quantile(list(rec["prob_gaps"]), 0.50), 4) if rec["prob_gaps"] else None,
                    "p75": round(_quantile(list(rec["prob_gaps"]), 0.75), 4) if rec["prob_gaps"] else None,
                },
                "recommendation": {
                    "action": action,
                    "why": why,
                    "minimum_sample_gate": min_sample,
                    "proposed_canary_min_confidence": proposed_canary_min_conf,
                    "proposed_canary_allow_low": proposed_canary_allow_low,
                    "current_canary_min_confidence": round(current_canary_conf, 3),
                    "current_canary_allow_low": round(current_canary_floor, 3),
                },
            }
            groups_out.append(group_row)
            if action in {"canary_reinstate", "review_guard"}:
                recommendations.append(
                    {
                        "symbol": rec["symbol"],
                        "source": rec["source"],
                        "decision": rec["decision"],
                        "decision_reason": rec["decision_reason"],
                        "action": action,
                        "resolved": int(stat.get("resolved", 0) or 0),
                        "win_rate": float(stat.get("win_rate", 0.0) or 0.0),
                        "pnl_usd": float(stat.get("pnl_usd", 0.0) or 0.0),
                        "proposed_canary_min_confidence": proposed_canary_min_conf,
                        "proposed_canary_allow_low": proposed_canary_allow_low,
                        "top_conf_band": str((conf_bands[0] or {}).get("band", "")) if conf_bands else "",
                    }
                )

        groups_out.sort(key=lambda item: (float(((item.get("stats") or {}).get("pnl_usd", 0.0) or 0.0)), float(((item.get("stats") or {}).get("win_rate", 0.0) or 0.0)), int(((item.get("stats") or {}).get("resolved", 0) or 0))), reverse=True)
        recommendations.sort(key=lambda item: (float(item.get("pnl_usd", 0.0) or 0.0), float(item.get("win_rate", 0.0) or 0.0), int(item.get("resolved", 0) or 0)), reverse=True)
        for sym in sorted(symbol_rollup.keys()):
            roll = symbol_rollup.get(sym) or {}
            out["symbols"].append(
                {
                    "symbol": sym,
                    "missed": _finalize_bucket(roll.get("missed") or _new_bucket()),
                    "allow": _finalize_bucket(roll.get("allow") or _new_bucket()),
                    "groups": [g for g in groups_out if str(g.get("symbol", "")) == sym][:5],
                }
            )
        out["symbols"].sort(key=lambda item: (float(((item.get("missed") or {}).get("pnl_usd", 0.0) or 0.0)), float(((item.get("missed") or {}).get("win_rate", 0.0) or 0.0))), reverse=True)
        out["ok"] = True
        out["summary"]["missed_positive_groups"] = len(recommendations)
        out["recommendations"] = recommendations
        return out

    @staticmethod
    def _lane_base_source(source: str, suffix: str) -> str:
        src = _norm_source(source)
        suffix_norm = str(suffix or "").strip().lower()
        if suffix_norm and src.endswith(suffix_norm):
            return src[: -len(suffix_norm)]
        return src

    @staticmethod
    def _load_named_state(path: Path) -> dict:
        try:
            if path.exists():
                payload = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    return payload
        except Exception:
            return {}
        return {}

    @staticmethod
    def _save_named_state(path: Path, state: dict) -> None:
        payload = dict(state or {})
        payload["updated_at"] = _iso(_utc_now())
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _save_report_snapshot(self, name: str, payload: dict) -> None:
        try:
            self._report_path(str(name)).write_text(
                json.dumps(dict(payload or {}), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def latest_capture_feature_snapshot(self, *, symbol: str, lookback_sec: Optional[int] = None, direction: str = "", confidence: float = 0.0) -> dict:
        token = _norm_symbol(symbol)
        if not token or not self.ctrader_db_path.exists():
            return {"ok": False, "status": "db_missing", "symbol": token}
        max_age = max(30, int(lookback_sec if lookback_sec is not None else getattr(config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240) or 240))
        since_iso = _iso(_utc_now() - timedelta(seconds=max_age))
        try:
            with closing(self._connect_ctrader()) as conn:
                row = conn.execute(
                    """
                    SELECT run_id, MAX(event_utc) AS last_event_utc
                      FROM ctrader_spot_ticks
                     WHERE symbol = ?
                       AND event_utc >= ?
                       AND COALESCE(run_id, '') <> ''
                    """,
                    (token, since_iso),
                ).fetchone()
                run_id = str((row["run_id"] if row is not None else "") or "").strip()
                if not run_id:
                    return {"ok": False, "status": "capture_missing", "symbol": token, "lookback_sec": max_age}
                spots = conn.execute(
                    """
                    SELECT bid, ask, spread, spread_pct, event_ts
                      FROM ctrader_spot_ticks
                     WHERE run_id = ?
                       AND symbol = ?
                     ORDER BY event_ts ASC, id ASC
                    """,
                    (run_id, token),
                ).fetchall()
                depth = conn.execute(
                    """
                    SELECT side, size, price, level_index, event_ts
                      FROM ctrader_depth_quotes
                     WHERE run_id = ?
                       AND symbol = ?
                     ORDER BY event_ts ASC, id ASC
                    """,
                    (run_id, token),
                ).fetchall()
            features = summarize_market_capture(spots, depth)
            gate = evaluate_xau_tick_depth_filter(features, direction, confidence=confidence) if token == "XAUUSD" and direction else {}
            return {
                "ok": True,
                "status": "ok",
                "symbol": token,
                "run_id": run_id,
                "lookback_sec": max_age,
                "last_event_utc": str((row["last_event_utc"] if row is not None else "") or ""),
                "features": features,
                "gate": gate,
            }
        except Exception as e:
            return {"ok": False, "status": "capture_query_error", "symbol": token, "error": str(e)}

    def build_recent_win_cluster_memory_report(self, *, hours: Optional[int] = None) -> dict:
        lookback_hours = max(1, int(hours or getattr(config, "RECENT_WIN_CLUSTER_LOOKBACK_HOURS", 8) or 8))
        min_resolved = max(1, int(getattr(config, "RECENT_WIN_CLUSTER_MIN_RESOLVED", 2) or 2))
        max_hold_min = max(1, int(getattr(config, "RECENT_WIN_CLUSTER_MAX_HOLD_MIN", 45) or 45))
        since_iso = _iso(_utc_now() - timedelta(hours=lookback_hours))
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "hours": lookback_hours,
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "summary": {
                "rows": 0,
                "wins": 0,
                "clusters": 0,
                "eligible_clusters": 0,
                "excluded_repair_like_rows": 0,
                "excluded_abnormal_rows": 0,
            },
            "clusters": [],
            "top_by_symbol": {},
            "error": "",
        }
        if not self.ctrader_db_path.exists():
            out["ok"] = False
            out["error"] = "ctrader_db_not_found"
            return out

        grouped: dict[tuple[str, str, str, str, str, str, str, str, str], dict] = {}
        try:
            with closing(self._connect_ctrader()) as conn:
                deal_cols = _table_columns(conn, "ctrader_deals")
                journal_cols = _table_columns(conn, "execution_journal")
                raw_json_expr = "d.raw_json AS deal_raw_json" if "raw_json" in deal_cols else "'{}' AS deal_raw_json"
                direction_expr = "j.direction AS direction" if "direction" in journal_cols else "'' AS direction"
                entry_expr = "j.entry AS entry" if "entry" in journal_cols else "0.0 AS entry"
                sl_expr = "j.stop_loss AS stop_loss" if "stop_loss" in journal_cols else "0.0 AS stop_loss"
                tp_expr = "j.take_profit AS take_profit" if "take_profit" in journal_cols else "0.0 AS take_profit"
                meta_expr = "j.execution_meta_json AS execution_meta_json" if "execution_meta_json" in journal_cols else "'{}' AS execution_meta_json"
                rows = conn.execute(
                    f"""
                    SELECT d.execution_utc,
                           d.outcome,
                           d.pnl_usd,
                           j.created_utc,
                           j.source,
                           j.symbol,
                           {direction_expr},
                           j.confidence,
                           {entry_expr},
                           {sl_expr},
                           {tp_expr},
                           j.entry_type,
                           j.request_json,
                           {meta_expr},
                           {raw_json_expr}
                      FROM ctrader_deals d
                      JOIN execution_journal j ON j.id = d.journal_id
                     WHERE d.execution_utc >= ?
                       AND d.outcome IN (0,1)
                       AND COALESCE(d.pnl_usd, 0) > 0
                       AND j.symbol IN ('XAUUSD','BTCUSD','ETHUSD')
                     ORDER BY d.execution_utc ASC, d.deal_id ASC
                    """,
                    (since_iso,),
                ).fetchall()
                out["summary"]["rows"] = len(rows)
                for row in list(rows or []):
                    request_ctx = _extract_request_context(str(row["request_json"] or "{}"))
                    raw_scores = dict(request_ctx.get("raw_scores") or {})
                    symbol = _norm_symbol(row["symbol"] or "")
                    source = _norm_source(row["source"] or "")
                    family = self._strategy_family_for_source(symbol, source)
                    direction = str(row["direction"] or (request_ctx.get("payload") or {}).get("direction") or "").strip().lower()
                    direction = "long" if direction in {"buy", "long"} else "short" if direction in {"sell", "short"} else direction or "unknown"
                    confidence = _safe_float(row["confidence"], 0.0)
                    deal_raw = _safe_json_dict(str(row["deal_raw_json"] or "{}"))
                    actual_entry = _actual_entry_from_deal(deal_raw)
                    planned_sl = _safe_float(row["stop_loss"], 0.0)
                    planned_tp = _safe_float(row["take_profit"], 0.0)
                    abnormal = _classify_trade_abnormality(
                        direction,
                        actual_entry,
                        planned_sl,
                        planned_tp,
                        execution_meta=str(row["execution_meta_json"] or "{}"),
                    )
                    if bool(abnormal.get("repair_like")):
                        out["summary"]["excluded_repair_like_rows"] += 1
                    if bool(abnormal.get("exclude_from_learning")):
                        out["summary"]["excluded_abnormal_rows"] += 1
                        continue
                    session = str(request_ctx.get("session") or "unknown")
                    timeframe = str(request_ctx.get("timeframe") or "unknown")
                    entry_type = str((row["entry_type"] or request_ctx.get("entry_type") or "unknown")).strip().lower()
                    pattern = str(request_ctx.get("pattern") or "unknown")
                    h1_trend = str(raw_scores.get("scalp_force_trend_h1") or raw_scores.get("trend_h1") or "unknown").strip().lower() or "unknown"
                    winner_scope = str(raw_scores.get("winner_logic_scope") or "unknown").strip().lower() or "unknown"
                    winner_regime = str(raw_scores.get("winner_logic_regime") or "unknown").strip().lower() or "unknown"
                    key = (
                        symbol,
                        family,
                        direction,
                        session,
                        timeframe,
                        entry_type,
                        pattern,
                        h1_trend,
                        _confidence_band(confidence),
                    )
                    bucket = grouped.setdefault(
                        key,
                        {
                            "symbol": symbol,
                            "family": family,
                            "direction": direction,
                            "session": session,
                            "timeframe": timeframe,
                            "entry_type": entry_type,
                            "pattern": pattern,
                            "h1_trend": h1_trend,
                            "confidence_band": _confidence_band(confidence),
                            "winner_scope": winner_scope,
                            "winner_regime": winner_regime,
                            "sources": set(),
                            "resolved": 0,
                            "wins": 0,
                            "losses": 0,
                            "pnl_usd": 0.0,
                            "confidence_values": [],
                            "hold_minutes": [],
                        },
                    )
                    bucket["resolved"] += 1
                    bucket["wins"] += 1
                    bucket["pnl_usd"] += _safe_float(row["pnl_usd"], 0.0)
                    bucket["sources"].add(source)
                    bucket["confidence_values"].append(confidence)
                    hold_min = _hold_minutes(str(row["created_utc"] or ""), str(row["execution_utc"] or ""))
                    if hold_min is not None:
                        bucket["hold_minutes"].append(hold_min)

            clusters: list[dict] = []
            for bucket in list(grouped.values()):
                resolved = int(bucket.get("resolved", 0) or 0)
                pnl = _safe_float(bucket.get("pnl_usd"), 0.0)
                avg_hold = None
                hold_list = list(bucket.get("hold_minutes") or [])
                if hold_list:
                    avg_hold = sum(hold_list) / len(hold_list)
                avg_conf = sum(list(bucket.get("confidence_values") or [])) / max(1, len(list(bucket.get("confidence_values") or [])))
                win_rate = 1.0 if resolved > 0 else 0.0
                stable_hold = avg_hold is None or avg_hold <= float(max_hold_min)
                memory_score = (
                    (win_rate * 4.0)
                    + min(4.0, pnl / 4.0)
                    + min(2.0, resolved * 0.40)
                    + max(0.0, (avg_conf - 72.0) / 4.0)
                    - (0.0 if stable_hold else 2.5)
                )
                clusters.append(
                    {
                        "symbol": str(bucket.get("symbol") or ""),
                        "family": str(bucket.get("family") or ""),
                        "direction": str(bucket.get("direction") or ""),
                        "session": str(bucket.get("session") or ""),
                        "timeframe": str(bucket.get("timeframe") or ""),
                        "entry_type": str(bucket.get("entry_type") or ""),
                        "pattern": str(bucket.get("pattern") or ""),
                        "h1_trend": str(bucket.get("h1_trend") or ""),
                        "confidence_band": str(bucket.get("confidence_band") or ""),
                        "winner_scope": str(bucket.get("winner_scope") or ""),
                        "winner_regime": str(bucket.get("winner_regime") or ""),
                        "sources": sorted(list(bucket.get("sources") or set())),
                        "resolved": resolved,
                        "wins": int(bucket.get("wins", 0) or 0),
                        "losses": int(bucket.get("losses", 0) or 0),
                        "win_rate": round(win_rate, 4),
                        "pnl_usd": round(pnl, 4),
                        "avg_pnl_usd": round((pnl / resolved), 4) if resolved > 0 else 0.0,
                        "avg_confidence": round(avg_conf, 2),
                        "avg_hold_min": (None if avg_hold is None else round(avg_hold, 2)),
                        "stable_hold": bool(stable_hold),
                        "memory_score": round(float(memory_score), 4),
                        "memory_eligible": bool(resolved >= min_resolved and pnl > 0 and stable_hold),
                    }
                )

            clusters.sort(
                key=lambda item: (
                    1 if bool(item.get("memory_eligible")) else 0,
                    float(item.get("memory_score") or 0.0),
                    float(item.get("pnl_usd") or 0.0),
                    int(item.get("resolved") or 0),
                ),
                reverse=True,
            )
            out["clusters"] = clusters
            out["summary"]["clusters"] = len(clusters)
            out["summary"]["wins"] = sum(int(x.get("wins", 0) or 0) for x in list(clusters or []))
            out["summary"]["eligible_clusters"] = sum(1 for x in list(clusters or []) if bool(x.get("memory_eligible")))
            top_by_symbol: dict[str, dict] = {}
            for row in list(clusters or []):
                sym = _norm_symbol(row.get("symbol"))
                if sym and sym not in top_by_symbol and bool(row.get("memory_eligible")):
                    top_by_symbol[sym] = dict(row)
            out["top_by_symbol"] = top_by_symbol
        except Exception as exc:
            out["ok"] = False
            out["error"] = str(exc)
        self._save_report_snapshot("recent_win_cluster_memory_report", out)
        return out

    def build_winner_memory_library_report(self, *, days: Optional[int] = None) -> dict:
        lookback_days = max(1, int(days or getattr(config, "WINNER_MEMORY_LIBRARY_LOOKBACK_DAYS", 21) or 21))
        min_resolved = max(1, int(getattr(config, "WINNER_MEMORY_LIBRARY_MIN_RESOLVED", 3) or 3))
        min_win_rate = max(0.0, min(1.0, float(getattr(config, "WINNER_MEMORY_LIBRARY_MIN_WIN_RATE", 0.60) or 0.60)))
        since_iso = _iso(_utc_now() - timedelta(days=lookback_days))
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "days": lookback_days,
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "summary": {
                "rows": 0,
                "situations": 0,
                "library_eligible": 0,
                "market_beating": 0,
                "excluded_repair_like_rows": 0,
                "excluded_abnormal_rows": 0,
            },
            "symbol_baselines": {},
            "family_baselines": [],
            "situations": [],
            "top_by_symbol": {},
            "error": "",
        }
        if not self.ctrader_db_path.exists():
            out["ok"] = False
            out["error"] = "ctrader_db_not_found"
            return out

        grouped: dict[tuple[str, str, str, str, str, str, str, str, str], dict] = {}
        symbol_totals: dict[str, dict] = {}
        family_totals: dict[tuple[str, str], dict] = {}
        try:
            with closing(self._connect_ctrader()) as conn:
                deal_cols = _table_columns(conn, "ctrader_deals")
                journal_cols = _table_columns(conn, "execution_journal")
                raw_json_expr = "d.raw_json AS deal_raw_json" if "raw_json" in deal_cols else "'{}' AS deal_raw_json"
                direction_expr = "j.direction AS direction" if "direction" in journal_cols else "'' AS direction"
                entry_expr = "j.entry AS entry" if "entry" in journal_cols else "0.0 AS entry"
                sl_expr = "j.stop_loss AS stop_loss" if "stop_loss" in journal_cols else "0.0 AS stop_loss"
                tp_expr = "j.take_profit AS take_profit" if "take_profit" in journal_cols else "0.0 AS take_profit"
                meta_expr = "j.execution_meta_json AS execution_meta_json" if "execution_meta_json" in journal_cols else "'{}' AS execution_meta_json"
                rows = conn.execute(
                    f"""
                    SELECT d.execution_utc,
                           d.outcome,
                           d.pnl_usd,
                           j.created_utc,
                           j.source,
                           j.symbol,
                           {direction_expr},
                           j.confidence,
                           {entry_expr},
                           {sl_expr},
                           {tp_expr},
                           j.entry_type,
                           j.request_json,
                           {meta_expr},
                           {raw_json_expr}
                      FROM ctrader_deals d
                      JOIN execution_journal j ON j.id = d.journal_id
                     WHERE d.execution_utc >= ?
                       AND d.outcome IN (0,1)
                       AND d.has_close_detail = 1
                       AND j.symbol IN ('XAUUSD','BTCUSD','ETHUSD')
                     ORDER BY d.execution_utc ASC, d.deal_id ASC
                    """,
                    (since_iso,),
                ).fetchall()
                out["summary"]["rows"] = len(rows)
                for row in list(rows or []):
                    request_ctx = _extract_request_context(str(row["request_json"] or "{}"))
                    raw_scores = dict(request_ctx.get("raw_scores") or {})
                    symbol = _norm_symbol(row["symbol"] or "")
                    source = _norm_source(row["source"] or "")
                    family = self._strategy_family_for_source(symbol, source)
                    if (not symbol) or (not family):
                        continue
                    direction = str(row["direction"] or (request_ctx.get("payload") or {}).get("direction") or "").strip().lower()
                    direction = "long" if direction in {"buy", "long"} else "short" if direction in {"sell", "short"} else direction or "unknown"
                    confidence = _safe_float(row["confidence"], 0.0)
                    deal_raw = _safe_json_dict(str(row["deal_raw_json"] or "{}"))
                    actual_entry = _actual_entry_from_deal(deal_raw)
                    planned_sl = _safe_float(row["stop_loss"], 0.0)
                    planned_tp = _safe_float(row["take_profit"], 0.0)
                    abnormal = _classify_trade_abnormality(
                        direction,
                        actual_entry,
                        planned_sl,
                        planned_tp,
                        execution_meta=str(row["execution_meta_json"] or "{}"),
                    )
                    if bool(abnormal.get("repair_like")):
                        out["summary"]["excluded_repair_like_rows"] += 1
                    if bool(abnormal.get("exclude_from_learning")):
                        out["summary"]["excluded_abnormal_rows"] += 1
                        continue
                    session = str(request_ctx.get("session") or "unknown")
                    timeframe = str(request_ctx.get("timeframe") or "unknown")
                    entry_type = str((row["entry_type"] or request_ctx.get("entry_type") or "unknown")).strip().lower()
                    pattern = str(request_ctx.get("pattern") or "unknown")
                    h1_trend = str(raw_scores.get("scalp_force_trend_h1") or raw_scores.get("trend_h1") or "unknown").strip().lower() or "unknown"
                    winner_scope = str(raw_scores.get("winner_logic_scope") or "unknown").strip().lower() or "unknown"
                    winner_regime = str(raw_scores.get("winner_logic_regime") or "unknown").strip().lower() or "unknown"
                    key = (
                        symbol,
                        family,
                        direction,
                        session,
                        timeframe,
                        entry_type,
                        pattern,
                        h1_trend,
                        _confidence_band(confidence),
                    )
                    bucket = grouped.setdefault(
                        key,
                        {
                            "symbol": symbol,
                            "family": family,
                            "direction": direction,
                            "session": session,
                            "timeframe": timeframe,
                            "entry_type": entry_type,
                            "pattern": pattern,
                            "h1_trend": h1_trend,
                            "confidence_band": _confidence_band(confidence),
                            "winner_scope": winner_scope,
                            "winner_regime": winner_regime,
                            "sources": set(),
                            "bucket": _new_bucket(),
                            "confidence_values": [],
                            "hold_minutes": [],
                        },
                    )
                    outcome = _safe_int(row["outcome"], -1)
                    pnl = _safe_float(row["pnl_usd"], 0.0)
                    _update_bucket(bucket["bucket"], pnl, outcome)
                    bucket["sources"].add(source)
                    bucket["confidence_values"].append(confidence)
                    hold_min = _hold_minutes(str(row["created_utc"] or ""), str(row["execution_utc"] or ""))
                    if hold_min is not None:
                        bucket["hold_minutes"].append(hold_min)
                    _update_bucket(symbol_totals.setdefault(symbol, _new_bucket()), pnl, outcome)
                    _update_bucket(family_totals.setdefault((symbol, family), _new_bucket()), pnl, outcome)

            out["symbol_baselines"] = {
                sym: _finalize_bucket(bucket)
                for sym, bucket in sorted(symbol_totals.items(), key=lambda item: item[0])
            }
            out["family_baselines"] = [
                {
                    "symbol": sym,
                    "family": fam,
                    "stats": _finalize_bucket(bucket),
                }
                for (sym, fam), bucket in sorted(family_totals.items(), key=lambda item: (item[0][0], item[0][1]))
            ]

            situations: list[dict] = []
            for bucket in list(grouped.values()):
                stats = _finalize_bucket(bucket.get("bucket") or _new_bucket())
                resolved = int(stats.get("resolved", 0) or 0)
                symbol = str(bucket.get("symbol") or "")
                family = str(bucket.get("family") or "")
                peer_symbol = _finalize_bucket(_subtract_bucket(symbol_totals.get(symbol) or _new_bucket(), bucket.get("bucket") or _new_bucket()))
                peer_family = _finalize_bucket(_subtract_bucket(family_totals.get((symbol, family)) or _new_bucket(), bucket.get("bucket") or _new_bucket()))
                avg_conf = sum(list(bucket.get("confidence_values") or [])) / max(1, len(list(bucket.get("confidence_values") or [])))
                hold_list = list(bucket.get("hold_minutes") or [])
                avg_hold = (sum(hold_list) / len(hold_list)) if hold_list else None
                win_rate_edge_symbol = float(stats.get("win_rate", 0.0) or 0.0) - float(peer_symbol.get("win_rate", 0.0) or 0.0)
                avg_pnl_edge_symbol = float(stats.get("avg_pnl_usd", 0.0) or 0.0) - float(peer_symbol.get("avg_pnl_usd", 0.0) or 0.0)
                win_rate_edge_family = float(stats.get("win_rate", 0.0) or 0.0) - float(peer_family.get("win_rate", 0.0) or 0.0)
                avg_pnl_edge_family = float(stats.get("avg_pnl_usd", 0.0) or 0.0) - float(peer_family.get("avg_pnl_usd", 0.0) or 0.0)
                library_eligible = (
                    resolved >= min_resolved
                    and float(stats.get("win_rate", 0.0) or 0.0) >= min_win_rate
                    and float(stats.get("pnl_usd", 0.0) or 0.0) > 0.0
                )
                beats_symbol = (
                    int(peer_symbol.get("resolved", 0) or 0) <= 0
                    or (win_rate_edge_symbol >= 0.0 and avg_pnl_edge_symbol >= 0.0)
                )
                beats_family = (
                    int(peer_family.get("resolved", 0) or 0) <= 0
                    or (win_rate_edge_family >= 0.0 and avg_pnl_edge_family >= 0.0)
                )
                market_beating = bool(library_eligible and beats_symbol and beats_family)
                memory_score = (
                    (float(stats.get("win_rate", 0.0) or 0.0) * 20.0)
                    + min(12.0, float(stats.get("pnl_usd", 0.0) or 0.0) / 4.0)
                    + min(8.0, resolved * 0.50)
                    + (win_rate_edge_symbol * 25.0)
                    + (avg_pnl_edge_symbol * 2.0)
                )
                situations.append(
                    {
                        "symbol": symbol,
                        "family": family,
                        "direction": str(bucket.get("direction") or ""),
                        "session": str(bucket.get("session") or ""),
                        "timeframe": str(bucket.get("timeframe") or ""),
                        "entry_type": str(bucket.get("entry_type") or ""),
                        "pattern": str(bucket.get("pattern") or ""),
                        "h1_trend": str(bucket.get("h1_trend") or ""),
                        "confidence_band": str(bucket.get("confidence_band") or ""),
                        "winner_scope": str(bucket.get("winner_scope") or ""),
                        "winner_regime": str(bucket.get("winner_regime") or ""),
                        "sources": sorted(list(bucket.get("sources") or set())),
                        "stats": stats,
                        "avg_confidence": round(avg_conf, 2),
                        "avg_hold_min": None if avg_hold is None else round(avg_hold, 2),
                        "peer_symbol_stats": peer_symbol,
                        "peer_family_stats": peer_family,
                        "win_rate_edge_vs_symbol": round(win_rate_edge_symbol, 4),
                        "avg_pnl_edge_vs_symbol": round(avg_pnl_edge_symbol, 4),
                        "win_rate_edge_vs_family": round(win_rate_edge_family, 4),
                        "avg_pnl_edge_vs_family": round(avg_pnl_edge_family, 4),
                        "library_eligible": bool(library_eligible),
                        "market_beating": bool(market_beating),
                        "memory_score": round(float(memory_score), 4),
                    }
                )

            situations.sort(
                key=lambda item: (
                    1 if bool(item.get("market_beating")) else 0,
                    1 if bool(item.get("library_eligible")) else 0,
                    float(item.get("memory_score") or 0.0),
                    float(((item.get("stats") or {}).get("pnl_usd", 0.0) or 0.0)),
                    int(((item.get("stats") or {}).get("resolved", 0) or 0)),
                ),
                reverse=True,
            )
            out["situations"] = situations
            out["summary"]["situations"] = len(situations)
            out["summary"]["library_eligible"] = sum(1 for row in list(situations or []) if bool(row.get("library_eligible")))
            out["summary"]["market_beating"] = sum(1 for row in list(situations or []) if bool(row.get("market_beating")))
            top_by_symbol: dict[str, dict] = {}
            for row in list(situations or []):
                sym = _norm_symbol(row.get("symbol"))
                if sym and sym not in top_by_symbol and bool(row.get("market_beating")):
                    top_by_symbol[sym] = dict(row)
            for row in list(situations or []):
                sym = _norm_symbol(row.get("symbol"))
                if sym and sym not in top_by_symbol and bool(row.get("library_eligible")):
                    top_by_symbol[sym] = dict(row)
            out["top_by_symbol"] = top_by_symbol
        except Exception as exc:
            out["ok"] = False
            out["error"] = str(exc)
        self._save_report_snapshot("winner_memory_library_report", out)
        return out

    def build_chart_state_memory_report(self, *, days: int = 21) -> dict:
        lookback_days = max(1, int(days or 21))
        since_iso = _iso(_utc_now() - timedelta(days=lookback_days))
        min_resolved = max(2, int(getattr(config, "CHART_STATE_MEMORY_MIN_RESOLVED", 2) or 2))
        follow_up_min = max(1, int(getattr(config, "CHART_STATE_MEMORY_FOLLOW_UP_MIN_RESOLVED", 1) or 1))
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "days": lookback_days,
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "summary": {
                "rows": 0,
                "states": 0,
                "profitable_states": 0,
                "follow_up_candidates": 0,
                "excluded_repair_like_rows": 0,
                "excluded_abnormal_rows": 0,
            },
            "symbol_baselines": {},
            "states": [],
            "top_by_symbol": {},
            "recommendations": [],
            "error": "",
        }
        if not self.ctrader_db_path.exists():
            out["ok"] = False
            out["error"] = "ctrader_db_not_found"
            return out

        symbol_totals: dict[str, dict] = {}
        grouped: dict[tuple[str, str, str, str, str, str, str, str], dict] = {}
        try:
            with closing(self._connect_ctrader()) as conn:
                deal_cols = _table_columns(conn, "ctrader_deals")
                journal_cols = _table_columns(conn, "execution_journal")
                raw_json_expr = "d.raw_json AS deal_raw_json" if "raw_json" in deal_cols else "'{}' AS deal_raw_json"
                direction_expr = "j.direction AS direction" if "direction" in journal_cols else "'' AS direction"
                entry_expr = "j.entry AS entry" if "entry" in journal_cols else "0.0 AS entry"
                sl_expr = "j.stop_loss AS stop_loss" if "stop_loss" in journal_cols else "0.0 AS stop_loss"
                tp_expr = "j.take_profit AS take_profit" if "take_profit" in journal_cols else "0.0 AS take_profit"
                meta_expr = "j.execution_meta_json AS execution_meta_json" if "execution_meta_json" in journal_cols else "'{}' AS execution_meta_json"
                rows = conn.execute(
                    f"""
                    SELECT d.execution_utc,
                           d.outcome,
                           d.pnl_usd,
                           j.created_utc,
                           j.source,
                           j.symbol,
                           {direction_expr},
                           j.confidence,
                           {entry_expr},
                           {sl_expr},
                           {tp_expr},
                           j.entry_type,
                           j.request_json,
                           {meta_expr},
                           {raw_json_expr}
                      FROM ctrader_deals d
                      JOIN execution_journal j ON j.id = d.journal_id
                     WHERE d.execution_utc >= ?
                       AND d.outcome IN (0,1)
                       AND d.has_close_detail = 1
                       AND j.symbol IN ('XAUUSD','BTCUSD','ETHUSD')
                     ORDER BY d.execution_utc ASC, d.deal_id ASC
                    """,
                    (since_iso,),
                ).fetchall()
                out["summary"]["rows"] = len(rows)
                for row in list(rows or []):
                    request_ctx = _extract_request_context(str(row["request_json"] or "{}"))
                    raw_scores = dict(request_ctx.get("raw_scores") or {})
                    symbol = _norm_symbol(row["symbol"] or "")
                    source = _norm_source(row["source"] or "")
                    family = self._strategy_family_for_source(symbol, source)
                    if (not symbol) or (not family):
                        continue
                    direction = _direction_token(
                        str(row["direction"] or (request_ctx.get("payload") or {}).get("direction") or "")
                    )
                    confidence = _safe_float(row["confidence"], 0.0)
                    deal_raw = _safe_json_dict(str(row["deal_raw_json"] or "{}"))
                    actual_entry = _actual_entry_from_deal(deal_raw)
                    abnormal = _classify_trade_abnormality(
                        direction,
                        actual_entry,
                        _safe_float(row["stop_loss"], 0.0),
                        _safe_float(row["take_profit"], 0.0),
                        execution_meta=str(row["execution_meta_json"] or "{}"),
                    )
                    if bool(abnormal.get("repair_like")):
                        out["summary"]["excluded_repair_like_rows"] += 1
                    if bool(abnormal.get("exclude_from_learning")):
                        out["summary"]["excluded_abnormal_rows"] += 1
                        continue
                    capture_features = self._load_market_capture_features(
                        conn,
                        symbol=symbol,
                        execution_meta_json=str(row["execution_meta_json"] or "{}"),
                    )
                    chart_state = _classify_chart_state(direction, request_ctx, capture_features=capture_features)
                    state_label = str(chart_state.get("state_label") or "range_probe")
                    session = str(request_ctx.get("session") or "unknown")
                    timeframe = str(request_ctx.get("timeframe") or "unknown")
                    h1_trend = str(chart_state.get("h1_trend") or "unknown")
                    day_type = str(chart_state.get("day_type") or "trend")
                    conf_band = _confidence_band(confidence)
                    key = (
                        symbol,
                        state_label,
                        direction,
                        session,
                        timeframe,
                        day_type,
                        h1_trend,
                        conf_band,
                    )
                    bucket = grouped.setdefault(
                        key,
                        {
                            "symbol": symbol,
                            "state_label": state_label,
                            "direction": direction,
                            "session": session,
                            "timeframe": timeframe,
                            "day_type": day_type,
                            "h1_trend": h1_trend,
                            "confidence_band": conf_band,
                            "pattern_families": {},
                            "family_buckets": {},
                            "entry_types": {},
                            "sources": set(),
                            "bucket": _new_bucket(),
                            "hold_minutes": [],
                            "continuation_bias_values": [],
                            "rejection_bucket_counts": {},
                            "volume_bucket_counts": {},
                            "spread_bucket_counts": {},
                            "imbalance_bucket_counts": {},
                            "delta_bucket_counts": {},
                            "follow_up_plan": str(chart_state.get("follow_up_plan") or ""),
                        },
                    )
                    outcome = _safe_int(row["outcome"], -1)
                    pnl = _safe_float(row["pnl_usd"], 0.0)
                    _update_bucket(bucket["bucket"], pnl, outcome)
                    _update_bucket(symbol_totals.setdefault(symbol, _new_bucket()), pnl, outcome)
                    family_bucket = bucket["family_buckets"].setdefault(family, _new_bucket())
                    _update_bucket(family_bucket, pnl, outcome)
                    entry_type = str((row["entry_type"] or request_ctx.get("entry_type") or "unknown")).strip().lower() or "unknown"
                    bucket["entry_types"][entry_type] = int(bucket["entry_types"].get(entry_type, 0) or 0) + 1
                    pattern_family = str(chart_state.get("pattern_family") or "unknown")
                    bucket["pattern_families"][pattern_family] = int(bucket["pattern_families"].get(pattern_family, 0) or 0) + 1
                    bucket["sources"].add(source)
                    hold_min = _hold_minutes(str(row["created_utc"] or ""), str(row["execution_utc"] or ""))
                    if hold_min is not None:
                        bucket["hold_minutes"].append(hold_min)
                    bucket["continuation_bias_values"].append(_safe_float(chart_state.get("continuation_bias"), 0.0))
                    for field_name, counter_name in (
                        ("rejection_bucket", "rejection_bucket_counts"),
                        ("volume_bucket", "volume_bucket_counts"),
                        ("spread_bucket", "spread_bucket_counts"),
                        ("imbalance_bucket", "imbalance_bucket_counts"),
                        ("delta_bucket", "delta_bucket_counts"),
                    ):
                        token = str(chart_state.get(field_name) or "").strip().lower()
                        if token:
                            bucket[counter_name][token] = int(bucket[counter_name].get(token, 0) or 0) + 1

            out["symbol_baselines"] = {
                sym: _finalize_bucket(bucket)
                for sym, bucket in sorted(symbol_totals.items(), key=lambda item: item[0])
            }

            states: list[dict] = []
            for bucket in list(grouped.values()):
                stats = _finalize_bucket(bucket.get("bucket") or _new_bucket())
                resolved = int(stats.get("resolved", 0) or 0)
                symbol = str(bucket.get("symbol") or "")
                symbol_bucket = symbol_totals.get(symbol) or _new_bucket()
                peer_symbol = _finalize_bucket(_subtract_bucket(symbol_bucket, bucket.get("bucket") or _new_bucket()))
                family_rows = []
                for family_name, family_bucket in sorted(
                    dict(bucket.get("family_buckets") or {}).items(),
                    key=lambda item: (
                        float(_finalize_bucket(item[1]).get("pnl_usd", 0.0) or 0.0),
                        float(_finalize_bucket(item[1]).get("win_rate", 0.0) or 0.0),
                        int(_finalize_bucket(item[1]).get("resolved", 0) or 0),
                    ),
                    reverse=True,
                ):
                    family_stats = _finalize_bucket(family_bucket)
                    family_rows.append({"family": str(family_name or ""), "stats": family_stats})
                best_family = dict(family_rows[0]) if family_rows else {}
                avg_hold = _avg(list(bucket.get("hold_minutes") or [])) if list(bucket.get("hold_minutes") or []) else None
                continuation_bias = _avg(list(bucket.get("continuation_bias_values") or []))
                win_rate_edge = float(stats.get("win_rate", 0.0) or 0.0) - float(peer_symbol.get("win_rate", 0.0) or 0.0)
                avg_pnl_edge = float(stats.get("avg_pnl_usd", 0.0) or 0.0) - float(peer_symbol.get("avg_pnl_usd", 0.0) or 0.0)
                state_score = (
                    (float(stats.get("win_rate", 0.0) or 0.0) * 18.0)
                    + min(10.0, float(stats.get("pnl_usd", 0.0) or 0.0) / 4.0)
                    + min(8.0, resolved * 0.65)
                    + (continuation_bias * 10.0)
                    + (win_rate_edge * 18.0)
                    + (avg_pnl_edge * 2.0)
                )
                follow_up_candidate = bool(
                    resolved >= follow_up_min
                    and float(stats.get("pnl_usd", 0.0) or 0.0) > 0.0
                    and float(stats.get("win_rate", 0.0) or 0.0) >= 0.57
                    and str(bucket.get("state_label") or "") in {
                        "continuation_drive",
                        "pullback_absorption",
                        "repricing_transition",
                        "breakout_drive",
                    }
                )
                profitable_state = bool(
                    resolved >= min_resolved
                    and float(stats.get("pnl_usd", 0.0) or 0.0) > 0.0
                    and float(stats.get("win_rate", 0.0) or 0.0) >= max(0.0, float(peer_symbol.get("win_rate", 0.0) or 0.0))
                )
                state_row = {
                    "symbol": symbol,
                    "state_label": str(bucket.get("state_label") or ""),
                    "direction": str(bucket.get("direction") or ""),
                    "session": str(bucket.get("session") or ""),
                    "timeframe": str(bucket.get("timeframe") or ""),
                    "day_type": str(bucket.get("day_type") or ""),
                    "h1_trend": str(bucket.get("h1_trend") or ""),
                    "confidence_band": str(bucket.get("confidence_band") or ""),
                    "stats": stats,
                    "peer_symbol_stats": peer_symbol,
                    "win_rate_edge_vs_symbol": round(win_rate_edge, 4),
                    "avg_pnl_edge_vs_symbol": round(avg_pnl_edge, 4),
                    "avg_hold_min": None if avg_hold is None else round(avg_hold, 2),
                    "continuation_bias": round(continuation_bias, 4),
                    "pattern_families": dict(sorted(dict(bucket.get("pattern_families") or {}).items(), key=lambda item: int(item[1]), reverse=True)),
                    "entry_types": dict(sorted(dict(bucket.get("entry_types") or {}).items(), key=lambda item: int(item[1]), reverse=True)),
                    "sources": sorted(list(bucket.get("sources") or set())),
                    "rejection_buckets": dict(sorted(dict(bucket.get("rejection_bucket_counts") or {}).items(), key=lambda item: int(item[1]), reverse=True)),
                    "volume_buckets": dict(sorted(dict(bucket.get("volume_bucket_counts") or {}).items(), key=lambda item: int(item[1]), reverse=True)),
                    "spread_buckets": dict(sorted(dict(bucket.get("spread_bucket_counts") or {}).items(), key=lambda item: int(item[1]), reverse=True)),
                    "imbalance_buckets": dict(sorted(dict(bucket.get("imbalance_bucket_counts") or {}).items(), key=lambda item: int(item[1]), reverse=True)),
                    "delta_buckets": dict(sorted(dict(bucket.get("delta_bucket_counts") or {}).items(), key=lambda item: int(item[1]), reverse=True)),
                    "family_ranking": family_rows[:4],
                    "best_family": dict(best_family),
                    "follow_up_candidate": follow_up_candidate,
                    "follow_up_plan": str(bucket.get("follow_up_plan") or ""),
                    "profitable_state": profitable_state,
                    "state_score": round(float(state_score), 4),
                }
                if follow_up_candidate:
                    out["recommendations"].append(
                        {
                            "symbol": symbol,
                            "state_label": str(bucket.get("state_label") or ""),
                            "direction": str(bucket.get("direction") or ""),
                            "session": str(bucket.get("session") or ""),
                            "timeframe": str(bucket.get("timeframe") or ""),
                            "best_family": str((best_family.get("family") or "")),
                            "follow_up_plan": str(bucket.get("follow_up_plan") or ""),
                            "resolved": resolved,
                            "win_rate": float(stats.get("win_rate", 0.0) or 0.0),
                            "pnl_usd": float(stats.get("pnl_usd", 0.0) or 0.0),
                        }
                    )
                states.append(state_row)

            states.sort(
                key=lambda item: (
                    1 if bool(item.get("follow_up_candidate")) else 0,
                    1 if bool(item.get("profitable_state")) else 0,
                    float(item.get("state_score", 0.0) or 0.0),
                    float(((item.get("stats") or {}).get("pnl_usd", 0.0) or 0.0)),
                    int(((item.get("stats") or {}).get("resolved", 0) or 0)),
                ),
                reverse=True,
            )
            out["states"] = states
            out["summary"]["states"] = len(states)
            out["summary"]["profitable_states"] = sum(1 for row in list(states or []) if bool(row.get("profitable_state")))
            out["summary"]["follow_up_candidates"] = sum(1 for row in list(states or []) if bool(row.get("follow_up_candidate")))
            top_by_symbol: dict[str, dict] = {}
            for row in list(states or []):
                sym = _norm_symbol(row.get("symbol"))
                if sym and sym not in top_by_symbol and bool(row.get("follow_up_candidate")):
                    top_by_symbol[sym] = dict(row)
            for row in list(states or []):
                sym = _norm_symbol(row.get("symbol"))
                if sym and sym not in top_by_symbol and bool(row.get("profitable_state")):
                    top_by_symbol[sym] = dict(row)
            out["top_by_symbol"] = top_by_symbol
        except Exception as exc:
            out["ok"] = False
            out["error"] = str(exc)
        self._save_report_snapshot("chart_state_memory_report", out)
        return out

    def _build_chart_state_router_index(self, chart_state_memory: dict | None = None) -> dict[tuple[str, str], dict]:
        payload = dict(chart_state_memory or {})
        rows = list(payload.get("states") or [])
        min_resolved = max(1, int(getattr(config, "CHART_STATE_ROUTER_MIN_RESOLVED", 3) or 3))
        follow_up_only = bool(getattr(config, "CHART_STATE_ROUTER_FOLLOW_UP_ONLY", True))
        out: dict[tuple[str, str], dict] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            if follow_up_only and not bool(row.get("follow_up_candidate")):
                continue
            stats = dict(row.get("stats") or {})
            if int(stats.get("resolved", 0) or 0) < min_resolved:
                continue
            best_family = str(((row.get("best_family") or {}).get("family") or "")).strip().lower()
            sym = _norm_symbol(row.get("symbol"))
            if not sym or not best_family:
                continue
            key = (sym, best_family)
            current = out.get(key) or {}
            if (
                not current
                or float(row.get("state_score", 0.0) or 0.0) > float(current.get("state_score", 0.0) or 0.0)
            ):
                out[key] = {
                    "symbol": sym,
                    "family": best_family,
                    "state_label": str(row.get("state_label") or ""),
                    "session": str(row.get("session") or ""),
                    "timeframe": str(row.get("timeframe") or ""),
                    "day_type": str(row.get("day_type") or ""),
                    "direction": str(row.get("direction") or ""),
                    "h1_trend": str(row.get("h1_trend") or ""),
                    "confidence_band": str(row.get("confidence_band") or ""),
                    "state_score": round(float(row.get("state_score", 0.0) or 0.0), 4),
                    "stats": stats,
                    "follow_up_candidate": bool(row.get("follow_up_candidate")),
                    "follow_up_plan": str(row.get("follow_up_plan") or ""),
                    "summary": _chart_state_router_summary(row),
                }
        return out

    def _collect_family_closed_rows(self, *, days: int = 21) -> list[dict]:
        lookback_days = max(1, int(days or 21))
        since_iso = _iso(_utc_now() - timedelta(days=lookback_days))
        scope = self._execution_scope()
        backend_focus = str(scope.get("backend_focus") or "mixed")
        summary = {"rows_seen": 0, "excluded_abnormal_rows": 0}
        out: list[dict] = []
        if self.ctrader_db_path.exists():
            try:
                with closing(self._connect_ctrader()) as conn:
                    deal_cols = _table_columns(conn, "ctrader_deals")
                    journal_cols = _table_columns(conn, "execution_journal")
                    direction_expr = "j.direction AS direction" if "direction" in journal_cols else "'' AS direction"
                    entry_expr = "j.entry AS entry" if "entry" in journal_cols else "0.0 AS entry"
                    sl_expr = "j.stop_loss AS stop_loss" if "stop_loss" in journal_cols else "0.0 AS stop_loss"
                    tp_expr = "j.take_profit AS take_profit" if "take_profit" in journal_cols else "0.0 AS take_profit"
                    meta_expr = "j.execution_meta_json AS execution_meta_json" if "execution_meta_json" in journal_cols else "'{}' AS execution_meta_json"
                    raw_json_expr = "d.raw_json AS deal_raw_json" if "raw_json" in deal_cols else "'{}' AS deal_raw_json"
                    rows = conn.execute(
                        f"""
                        SELECT d.execution_utc, d.source AS deal_source, d.symbol, d.pnl_usd, d.outcome,
                               j.source AS journal_source, {direction_expr}, j.confidence, {entry_expr},
                               {sl_expr}, {tp_expr}, j.entry_type, j.request_json,
                               j.created_utc, {meta_expr}, {raw_json_expr}
                          FROM ctrader_deals d
                          LEFT JOIN execution_journal j ON j.id = d.journal_id
                         WHERE d.execution_utc >= ?
                           AND d.has_close_detail = 1
                           AND d.journal_id IS NOT NULL
                           AND d.outcome IN (0, 1)
                         ORDER BY d.execution_utc ASC, d.deal_id ASC
                        """,
                        (since_iso,),
                    ).fetchall()
                    summary["rows_seen"] += len(list(rows or []))
                    for row in list(rows or []):
                        symbol = _norm_symbol(row["symbol"] or "")
                        source = _norm_source(row["journal_source"] or row["deal_source"] or "")
                        family = self._strategy_family_for_source(symbol, source)
                        if (not symbol) or (not source) or (not family):
                            continue
                        ctx = _extract_request_context(str(row["request_json"] or "{}"))
                        direction = str(row["direction"] or (ctx.get("payload") or {}).get("direction") or "").strip().lower()
                        direction = "long" if direction in {"buy", "long"} else "short" if direction in {"sell", "short"} else direction or "unknown"
                        abnormal = _classify_trade_abnormality(
                            direction,
                            _actual_entry_from_deal(_safe_json_dict(str(row["deal_raw_json"] or "{}"))),
                            _safe_float(row["stop_loss"], 0.0),
                            _safe_float(row["take_profit"], 0.0),
                            execution_meta=str(row["execution_meta_json"] or "{}"),
                        )
                        if bool(abnormal.get("exclude_from_learning")):
                            summary["excluded_abnormal_rows"] += 1
                            continue
                        confidence = _safe_float(
                            row["confidence"],
                            _safe_float((ctx.get("payload") or {}).get("confidence"), 0.0),
                        )
                        out.append(
                            {
                                "backend": "ctrader",
                                "closed_utc": str(row["execution_utc"] or ""),
                                "created_utc": str(row["created_utc"] or row["execution_utc"] or ""),
                                "symbol": symbol,
                                "source": source,
                                "family": family,
                                "confidence": confidence,
                                "outcome": _safe_int(row["outcome"], -1),
                                "pnl_usd": _safe_float(row["pnl_usd"], 0.0),
                                "session": str(ctx.get("session") or "unknown"),
                                "timeframe": str(ctx.get("timeframe") or "unknown"),
                                "entry_type": str((row["entry_type"] or ctx.get("entry_type") or "unknown")).strip().lower(),
                                "pattern": str(ctx.get("pattern") or "unknown"),
                                "abnormal_flags": list(abnormal.get("flags") or []),
                            }
                        )
            except Exception:
                pass
        if backend_focus != "ctrader_only" and self.mt5_db_path.exists():
            try:
                with closing(self._connect_mt5()) as conn:
                    rows = conn.execute(
                        """
                        SELECT created_at, source, signal_symbol, broker_symbol, outcome, pnl
                          FROM mt5_execution_journal
                         WHERE created_at >= ?
                           AND resolved = 1
                           AND outcome IN (0, 1)
                         ORDER BY created_at ASC, id ASC
                        """,
                        (since_iso,),
                    ).fetchall()
                    for row in list(rows or []):
                        symbol = _norm_symbol(row["signal_symbol"] or row["broker_symbol"] or "")
                        source = _norm_source(row["source"] or "")
                        family = self._strategy_family_for_source(symbol, source)
                        if (not symbol) or (not source) or (not family):
                            continue
                        out.append(
                            {
                                "backend": "mt5",
                                "closed_utc": str(row["created_at"] or ""),
                                "created_utc": str(row["created_at"] or ""),
                                "symbol": symbol,
                                "source": source,
                                "family": family,
                                "confidence": 0.0,
                                "outcome": _safe_int(row["outcome"], -1),
                                "pnl_usd": _safe_float(row["pnl"], 0.0),
                                "session": "unknown",
                                "timeframe": "unknown",
                                "entry_type": "unknown",
                                "pattern": "unknown",
                            }
                        )
            except Exception:
                pass
        out.sort(key=lambda item: str(item.get("closed_utc") or ""))
        self._last_family_collect_summary = summary
        return out

    def build_family_calibration_report(self, *, days: int = 21) -> dict:
        lookback_days = max(1, int(days or 21))
        rows = self._collect_family_closed_rows(days=lookback_days)
        prior_strength = max(0.0, float(getattr(config, "FAMILY_CALIBRATION_PRIOR_STRENGTH", 6.0) or 6.0))
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "days": lookback_days,
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "summary": {
                "rows": len(rows),
                "families": 0,
                "symbols": 0,
                "excluded_abnormal_rows": int((self._last_family_collect_summary or {}).get("excluded_abnormal_rows", 0) or 0),
            },
            "families": [],
            "recommendations": [],
            "error": "",
        }
        grouped: dict[tuple[str, str], dict] = {}
        for row in list(rows or []):
            key = (_norm_symbol(row.get("symbol")), str(row.get("family") or ""))
            bucket = grouped.setdefault(
                key,
                {
                    "symbol": key[0],
                    "family": key[1],
                    "sources": set(),
                    "backends": set(),
                    "pnls": [],
                    "confidence": [],
                    "overall": _new_bucket(),
                    "bands": {},
                    "sessions": {},
                    "timeframes": {},
                    "entry_types": {},
                    "window_scores": [],
                },
            )
            pnl = _safe_float(row.get("pnl_usd"), 0.0)
            outcome = _safe_int(row.get("outcome"), -1)
            conf = max(0.0, _safe_float(row.get("confidence"), 0.0))
            session = str(row.get("session") or "unknown")
            timeframe = str(row.get("timeframe") or "unknown")
            entry_type = str(row.get("entry_type") or "unknown")
            _update_bucket(bucket["overall"], pnl, outcome)
            _update_bucket(bucket["bands"].setdefault(_confidence_band(conf), _new_bucket()), pnl, outcome)
            _update_bucket(bucket["sessions"].setdefault(session, _new_bucket()), pnl, outcome)
            _update_bucket(bucket["timeframes"].setdefault(timeframe, _new_bucket()), pnl, outcome)
            _update_bucket(bucket["entry_types"].setdefault(entry_type, _new_bucket()), pnl, outcome)
            bucket["pnls"].append(pnl)
            if conf > 0:
                bucket["confidence"].append(conf)
            bucket["sources"].add(str(row.get("source") or ""))
            bucket["backends"].add(str(row.get("backend") or ""))

        family_rows: list[dict] = []
        recommendations: list[dict] = []
        for (_sym, _family), bucket in sorted(grouped.items()):
            overall = _finalize_bucket(bucket.get("overall") or _new_bucket())
            resolved = int(overall.get("resolved", 0) or 0)
            prior_wr = float(overall.get("win_rate", 0.0) or 0.0)
            band_rows = []
            calibration_error_sum = 0.0
            brier_sum = 0.0
            for band, stat_bucket in sorted(dict(bucket.get("bands") or {}).items()):
                stat = _finalize_bucket(stat_bucket)
                band_resolved = int(stat.get("resolved", 0) or 0)
                conf_floor = _confidence_floor_for_band(band)
                avg_conf = conf_floor if conf_floor is not None else 70.0
                empirical_wr = float(stat.get("win_rate", 0.0) or 0.0)
                calibrated_wr = (
                    ((float(stat.get("wins", 0) or 0) + (prior_wr * prior_strength)) / float(band_resolved + prior_strength))
                    if band_resolved > 0
                    else prior_wr
                )
                gap = empirical_wr - (avg_conf / 100.0)
                calibration_error_sum += abs(gap) * band_resolved
                # Approximate Brier using band confidence when exact point probabilities are unavailable.
                brier_component = (
                    empirical_wr * ((1.0 - (avg_conf / 100.0)) ** 2)
                    + (1.0 - empirical_wr) * (((avg_conf / 100.0) - 0.0) ** 2)
                )
                brier_sum += float(brier_component) * band_resolved
                band_rows.append(
                    {
                        "band": band,
                        "resolved": band_resolved,
                        "wins": int(stat.get("wins", 0) or 0),
                        "losses": int(stat.get("losses", 0) or 0),
                        "win_rate": round(empirical_wr, 4),
                        "pnl_usd": round(float(stat.get("pnl_usd", 0.0) or 0.0), 4),
                        "avg_confidence": round(float(avg_conf), 2),
                        "calibrated_win_rate": round(float(calibrated_wr), 4),
                        "confidence_gap": round(float(gap), 4),
                    }
                )
            ece = (calibration_error_sum / resolved) if resolved > 0 else 0.0
            brier = (brier_sum / resolved) if resolved > 0 else 0.0
            window_scores = [
                self._score_bucket_for_ranking(v)
                for v in list((bucket.get("sessions") or {}).values())
            ]
            window_scores = [float(v) for v in window_scores if v is not None]
            score_dispersion = _stddev(window_scores)
            dd = _max_drawdown_usd(list(bucket.get("pnls") or []))
            dsr = _deflated_sharpe_proxy(list(bucket.get("pnls") or []), trials=max(1, len(grouped)))
            uncertainty = round(
                min(
                    1.0,
                    (1.0 / math.sqrt(max(resolved, 1)))
                    + min(0.6, float(ece))
                    + min(0.4, float(score_dispersion) / 100.0),
                ),
                4,
            )
            row = {
                "symbol": str(bucket.get("symbol") or ""),
                "family": str(bucket.get("family") or ""),
                "sources": sorted(list(bucket.get("sources") or set())),
                "backends": sorted(list(bucket.get("backends") or set())),
                "overall": overall,
                "calibration_error": round(float(ece), 4),
                "brier_score": round(float(brier), 4),
                "avg_confidence": round(sum(list(bucket.get("confidence") or [])) / max(len(list(bucket.get("confidence") or [])), 1), 2) if list(bucket.get("confidence") or []) else 0.0,
                "calibrated_win_rate": round(((float(overall.get("wins", 0) or 0) + (prior_wr * prior_strength)) / float(resolved + prior_strength)) if resolved > 0 else prior_wr, 4),
                "max_drawdown_usd": dd,
                "deflated_sharpe_proxy": dsr,
                "uncertainty_score": uncertainty,
                "sessions": [
                    {"session": key, **_finalize_bucket(val)}
                    for key, val in sorted(dict(bucket.get("sessions") or {}).items(), key=lambda item: float(_finalize_bucket(item[1]).get("pnl_usd", 0.0)), reverse=True)
                ][:6],
                "timeframes": [
                    {"timeframe": key, **_finalize_bucket(val)}
                    for key, val in sorted(dict(bucket.get("timeframes") or {}).items(), key=lambda item: float(_finalize_bucket(item[1]).get("pnl_usd", 0.0)), reverse=True)
                ][:6],
                "entry_types": [
                    {"entry_type": key, **_finalize_bucket(val)}
                    for key, val in sorted(dict(bucket.get("entry_types") or {}).items(), key=lambda item: float(_finalize_bucket(item[1]).get("pnl_usd", 0.0)), reverse=True)
                ][:6],
                "confidence_bands": band_rows,
            }
            if row["symbol"] == "XAUUSD" and row["family"] == "xau_scalp_pullback_limit" and resolved >= 12 and float(overall.get("pnl_usd", 0.0) or 0.0) > 0 and float(overall.get("win_rate", 0.0) or 0.0) >= 0.60:
                recommendations.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_pullback_limit",
                        "action": "promote_primary_family",
                        "resolved": resolved,
                        "win_rate": round(float(overall.get("win_rate", 0.0) or 0.0), 4),
                        "pnl_usd": round(float(overall.get("pnl_usd", 0.0) or 0.0), 4),
                        "uncertainty_score": uncertainty,
                        "proposed_primary_family": "xau_scalp_pullback_limit",
                        "proposed_active_families": "xau_scalp_pullback_limit,xau_scalp_breakout_stop",
                    }
                )
            if row["symbol"] == "XAUUSD" and row["family"] == "xau_scalp_breakout_stop" and resolved >= 10 and float(overall.get("win_rate", 0.0) or 0.0) >= 0.50 and float(overall.get("pnl_usd", 0.0) or 0.0) <= 0.0:
                recommendations.append(
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_breakout_stop",
                        "action": "repair_breakout_stop",
                        "resolved": resolved,
                        "win_rate": round(float(overall.get("win_rate", 0.0) or 0.0), 4),
                        "pnl_usd": round(float(overall.get("pnl_usd", 0.0) or 0.0), 4),
                        "uncertainty_score": uncertainty,
                    }
                )
            family_rows.append(row)

        family_rows.sort(
            key=lambda item: (
                float(((item.get("overall") or {}).get("pnl_usd", 0.0) or 0.0)),
                float(((item.get("overall") or {}).get("win_rate", 0.0) or 0.0)),
            ),
            reverse=True,
        )
        out["families"] = family_rows
        out["recommendations"] = recommendations
        out["summary"]["families"] = len(family_rows)
        out["summary"]["symbols"] = len({_norm_symbol(x.get("symbol")) for x in family_rows if _norm_symbol(x.get("symbol"))})
        self._save_report_snapshot("family_calibration_report", out)
        return out

    def build_ctrader_tick_depth_replay_report(self, *, days: int = 7) -> dict:
        lookback_days = max(1, int(days or 7))
        since_iso = _iso(_utc_now() - timedelta(days=lookback_days))
        window_sec = max(10, int(getattr(config, "CTRADER_TICK_DEPTH_REPLAY_LAB_REPLAY_WINDOW_SEC", 45) or 45))
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "days": lookback_days,
            "replay_window_sec": window_sec,
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "summary": {"orders": 0, "orders_with_capture": 0, "families": 0, "excluded_abnormal_rows": 0},
            "families": [],
            "recommendations": [],
            "error": "",
        }
        if not self.ctrader_db_path.exists():
            out["ok"] = False
            out["error"] = "ctrader_db_not_found"
            return out
        family_rollups: dict[tuple[str, str], dict] = {}
        try:
            with closing(self._connect_ctrader()) as conn:
                order_rows = conn.execute(
                    """
                    SELECT j.id, j.created_utc, j.source, j.symbol, j.direction, j.entry_type, j.stop_loss, j.take_profit,
                           j.execution_meta_json, d.outcome, d.pnl_usd, d.raw_json AS deal_raw_json
                      FROM execution_journal j
                      JOIN ctrader_deals d ON d.journal_id = j.id
                     WHERE d.execution_utc >= ?
                       AND d.has_close_detail = 1
                       AND d.outcome IN (0, 1)
                       AND j.symbol IN ('XAUUSD','BTCUSD','ETHUSD')
                     ORDER BY j.created_utc ASC, j.id ASC
                    """,
                    (since_iso,),
                ).fetchall()
                out["summary"]["orders"] = len(order_rows)
                for row in list(order_rows or []):
                    symbol = _norm_symbol(row["symbol"] or "")
                    source = _norm_source(row["source"] or "")
                    family = self._strategy_family_for_source(symbol, source)
                    if not family:
                        continue
                    abnormal = _classify_trade_abnormality(
                        str(row["direction"] or ""),
                        _actual_entry_from_deal(_safe_json_dict(str(row["deal_raw_json"] or "{}"))),
                        _safe_float(row["stop_loss"], 0.0),
                        _safe_float(row["take_profit"], 0.0),
                        execution_meta=str(row["execution_meta_json"] or "{}"),
                    )
                    if bool(abnormal.get("exclude_from_learning")):
                        out["summary"]["excluded_abnormal_rows"] += 1
                        continue
                    created_ms = _iso_to_ms(str(row["created_utc"] or ""))
                    if created_ms <= 0:
                        continue
                    from_iso = _ms_to_iso(created_ms)
                    to_iso = _ms_to_iso(created_ms + (window_sec * 1000))
                    spots = conn.execute(
                        """
                        SELECT bid, ask, spread, spread_pct, event_ts
                          FROM ctrader_spot_ticks
                         WHERE symbol = ?
                           AND event_utc >= ?
                           AND event_utc <= ?
                         ORDER BY event_utc ASC, id ASC
                        """,
                        (symbol, from_iso, to_iso),
                    ).fetchall()
                    depth = conn.execute(
                        """
                        SELECT side, size, price, level_index
                          FROM ctrader_depth_quotes
                         WHERE symbol = ?
                           AND event_utc >= ?
                           AND event_utc <= ?
                         ORDER BY event_utc ASC, id ASC
                        """,
                        (symbol, from_iso, to_iso),
                    ).fetchall()
                    key = (symbol, family)
                    roll = family_rollups.setdefault(
                        key,
                        {
                            "symbol": symbol,
                            "family": family,
                            "orders": 0,
                            "orders_with_capture": 0,
                            "wins": 0,
                            "losses": 0,
                            "pnl_usd": 0.0,
                            "spread_pcts": [],
                            "tick_drifts": [],
                            "depth_imbalances": [],
                            "entry_types": {},
                        },
                    )
                    roll["orders"] += 1
                    outcome = _safe_int(row["outcome"], -1)
                    if outcome == 1:
                        roll["wins"] += 1
                    elif outcome == 0:
                        roll["losses"] += 1
                    roll["pnl_usd"] += _safe_float(row["pnl_usd"], 0.0)
                    entry_type = str(row["entry_type"] or "unknown").strip().lower() or "unknown"
                    roll["entry_types"][entry_type] = int(roll["entry_types"].get(entry_type, 0) or 0) + 1
                    if spots or depth:
                        roll["orders_with_capture"] += 1
                        out["summary"]["orders_with_capture"] += 1
                    if spots:
                        spread_vals = [_safe_float(x["spread_pct"], 0.0) for x in list(spots or [])]
                        mids = []
                        for x in list(spots or []):
                            bid = _safe_float(x["bid"], 0.0)
                            ask = _safe_float(x["ask"], 0.0)
                            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
                            if mid > 0:
                                mids.append(mid)
                        if spread_vals:
                            roll["spread_pcts"].append(sum(spread_vals) / max(len(spread_vals), 1))
                        if len(mids) >= 2 and mids[0] > 0:
                            roll["tick_drifts"].append(((mids[-1] - mids[0]) / mids[0]) * 100.0)
                    if depth:
                        bid_size = sum(_safe_float(x["size"], 0.0) for x in list(depth or []) if str(x["side"] or "").lower() == "bid")
                        ask_size = sum(_safe_float(x["size"], 0.0) for x in list(depth or []) if str(x["side"] or "").lower() == "ask")
                        total_size = bid_size + ask_size
                        if total_size > 0:
                            roll["depth_imbalances"].append((bid_size - ask_size) / total_size)
        except Exception as e:
            out["ok"] = False
            out["error"] = f"tick_depth_replay_query_error:{e}"
            return out

        families = []
        recommendations = []
        for (_sym, _family), roll in sorted(family_rollups.items()):
            resolved = int(roll.get("orders", 0) or 0)
            wins = int(roll.get("wins", 0) or 0)
            row = {
                "symbol": str(roll.get("symbol") or ""),
                "family": str(roll.get("family") or ""),
                "orders": resolved,
                "orders_with_capture": int(roll.get("orders_with_capture", 0) or 0),
                "win_rate": round((wins / resolved), 4) if resolved > 0 else 0.0,
                "pnl_usd": round(float(roll.get("pnl_usd", 0.0) or 0.0), 4),
                "avg_spread_pct": round(sum(roll.get("spread_pcts") or []) / max(len(roll.get("spread_pcts") or []), 1), 5) if list(roll.get("spread_pcts") or []) else 0.0,
                "avg_tick_drift_pct": round(sum(roll.get("tick_drifts") or []) / max(len(roll.get("tick_drifts") or []), 1), 5) if list(roll.get("tick_drifts") or []) else 0.0,
                "avg_depth_imbalance": round(sum(roll.get("depth_imbalances") or []) / max(len(roll.get("depth_imbalances") or []), 1), 5) if list(roll.get("depth_imbalances") or []) else 0.0,
                "entry_types": dict(sorted(dict(roll.get("entry_types") or {}).items(), key=lambda item: int(item[1]), reverse=True)),
            }
            if row["symbol"] == "XAUUSD" and row["family"] == "xau_scalp_pullback_limit" and row["pnl_usd"] > 0 and row["win_rate"] >= 0.60:
                recommendations.append(
                    {
                        "symbol": row["symbol"],
                        "family": row["family"],
                        "action": "prefer_pullback_low_spread",
                        "avg_spread_pct": row["avg_spread_pct"],
                        "avg_depth_imbalance": row["avg_depth_imbalance"],
                    }
                )
            if row["symbol"] == "XAUUSD" and row["family"] == "xau_scalp_breakout_stop" and row["win_rate"] >= 0.50 and row["pnl_usd"] <= 0:
                recommendations.append(
                    {
                        "symbol": row["symbol"],
                        "family": row["family"],
                        "action": "tighten_breakout_stop_trigger",
                        "avg_spread_pct": row["avg_spread_pct"],
                        "avg_depth_imbalance": row["avg_depth_imbalance"],
                    }
                )
            families.append(row)
        families.sort(key=lambda item: (float(item.get("pnl_usd", 0.0) or 0.0), float(item.get("win_rate", 0.0) or 0.0)), reverse=True)
        out["families"] = families
        out["recommendations"] = recommendations
        out["summary"]["families"] = len(families)
        self._save_report_snapshot("ctrader_tick_depth_replay_report", out)
        return out

    def build_xau_tick_depth_filter_report(self, *, days: int = 7) -> dict:
        lookback_days = max(1, int(days or 7))
        since_iso = _iso(_utc_now() - timedelta(days=lookback_days))
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "days": lookback_days,
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "summary": {
                "rows": 0,
                "passing_rows": 0,
                "accepted_rows": 0,
                "families": 0,
                "excluded_abnormal_rows": 0,
                "missed_positive_rows": 0,
                "missed_positive_families": 0,
            },
            "families": [],
            "missed_opportunities": [],
            "recommendations": [],
            "error": "",
        }
        if not self.ctrader_db_path.exists():
            out["ok"] = False
            out["error"] = "ctrader_db_not_found"
            return out
        family_rollups: dict[tuple[str, str], dict] = {}
        try:
            with closing(self._connect_ctrader()) as conn:
                rows = conn.execute(
                    """
                    SELECT j.id, j.source, j.symbol, j.direction, j.confidence, j.entry_type,
                           j.stop_loss, j.take_profit, j.execution_meta_json,
                           d.outcome, d.pnl_usd, d.raw_json AS deal_raw_json
                      FROM execution_journal j
                      JOIN ctrader_deals d ON d.journal_id = j.id
                     WHERE d.execution_utc >= ?
                       AND d.has_close_detail = 1
                       AND d.outcome IN (0, 1)
                       AND j.symbol = 'XAUUSD'
                       AND json_extract(j.execution_meta_json, '$.market_capture.run_id') IS NOT NULL
                     ORDER BY d.execution_utc ASC, d.deal_id ASC
                    """,
                    (since_iso,),
                ).fetchall()
                for row in list(rows or []):
                    source = _norm_source(row["source"] or "")
                    family = self._strategy_family_for_source("XAUUSD", source)
                    if not family:
                        continue
                    abnormal = _classify_trade_abnormality(
                        str(row["direction"] or ""),
                        _actual_entry_from_deal(_safe_json_dict(str(row["deal_raw_json"] or "{}"))),
                        _safe_float(row["stop_loss"], 0.0),
                        _safe_float(row["take_profit"], 0.0),
                        execution_meta=str(row["execution_meta_json"] or "{}"),
                    )
                    if bool(abnormal.get("exclude_from_learning")):
                        out["summary"]["excluded_abnormal_rows"] += 1
                        continue
                    run_id = str(_safe_json_dict(str(row["execution_meta_json"] or "{}")).get("market_capture", {}).get("run_id", "") or "").strip()
                    if not run_id:
                        continue
                    spots = conn.execute(
                        """
                        SELECT bid, ask, spread, spread_pct, event_ts
                          FROM ctrader_spot_ticks
                         WHERE run_id = ?
                           AND symbol = 'XAUUSD'
                         ORDER BY event_ts ASC, id ASC
                        """,
                        (run_id,),
                    ).fetchall()
                    depth = conn.execute(
                        """
                        SELECT side, size, price, level_index, event_ts
                          FROM ctrader_depth_quotes
                         WHERE run_id = ?
                           AND symbol = 'XAUUSD'
                         ORDER BY event_ts ASC, id ASC
                        """,
                        (run_id,),
                    ).fetchall()
                    features = summarize_market_capture(spots, depth)
                    gate = evaluate_xau_tick_depth_filter(
                        features,
                        str(row["direction"] or ""),
                        confidence=_safe_float(row["confidence"], 0.0),
                    )
                    key = ("XAUUSD", family)
                    roll = family_rollups.setdefault(
                        key,
                        {
                            "symbol": "XAUUSD",
                            "family": family,
                            "baseline": _new_bucket(),
                            "filtered": _new_bucket(),
                            "accepted": _new_bucket(),
                            "missed_positive": _new_bucket(),
                            "missed_reason_counts": {},
                            "missed_day_types": {},
                            "feature_rows": [],
                        },
                    )
                    outcome = _safe_int(row["outcome"], -1)
                    pnl = _safe_float(row["pnl_usd"], 0.0)
                    _update_bucket(roll["baseline"], pnl, outcome)
                    out["summary"]["rows"] += 1
                    gate_pass = bool(gate.get("pass"))
                    accepted = bool(gate_pass or gate.get("canary_sample_pass"))
                    if gate_pass:
                        _update_bucket(roll["filtered"], pnl, outcome)
                        out["summary"]["passing_rows"] += 1
                    if accepted:
                        _update_bucket(roll["accepted"], pnl, outcome)
                        out["summary"]["accepted_rows"] += 1
                    elif outcome == 1 and pnl > 0.0:
                        _update_bucket(roll["missed_positive"], pnl, outcome)
                        out["summary"]["missed_positive_rows"] += 1
                        day_type = str((gate.get("features") or {}).get("day_type") or "").strip().lower() or "trend"
                        roll["missed_day_types"][day_type] = int(roll["missed_day_types"].get(day_type, 0) or 0) + 1
                        for reason in list(gate.get("reasons") or []):
                            token = str(reason or "").strip()
                            if token:
                                roll["missed_reason_counts"][token] = int(roll["missed_reason_counts"].get(token, 0) or 0) + 1
                    roll["feature_rows"].append(
                        {
                            "outcome": outcome,
                            "pnl_usd": pnl,
                            "gate_pass": gate_pass,
                            "gate_accepted": accepted,
                            "canary_sample_pass": bool(gate.get("canary_sample_pass")),
                            "gate_score": int(gate.get("score", 0) or 0),
                            **dict(gate.get("features") or {}),
                        }
                    )
        except Exception as e:
            out["ok"] = False
            out["error"] = f"xau_tick_depth_filter_query_error:{e}"
            return out

        families: list[dict] = []
        missed_opportunities: list[dict] = []
        recommendations: list[dict] = []
        for (_sym, _family), roll in sorted(family_rollups.items()):
            base = _finalize_bucket(roll["baseline"])
            filt = _finalize_bucket(roll["filtered"])
            accepted = _finalize_bucket(roll["accepted"])
            missed_positive = _finalize_bucket(roll["missed_positive"])
            improvement_wr = round(float(filt.get("win_rate", 0.0) or 0.0) - float(base.get("win_rate", 0.0) or 0.0), 4)
            improvement_avg = round(float(filt.get("avg_pnl_usd", 0.0) or 0.0) - float(base.get("avg_pnl_usd", 0.0) or 0.0), 4)
            rows = list(roll.get("feature_rows") or [])
            pass_rows = [row for row in rows if bool(row.get("gate_pass"))]
            reason_counts = dict(sorted((roll.get("missed_reason_counts") or {}).items(), key=lambda item: int(item[1] or 0), reverse=True))
            day_type_counts = dict(sorted((roll.get("missed_day_types") or {}).items(), key=lambda item: int(item[1] or 0), reverse=True))
            family_row = {
                "symbol": "XAUUSD",
                "family": str(roll.get("family") or ""),
                "baseline": base,
                "filtered": filt,
                "accepted": accepted,
                "filter_pass_rate": round((len(pass_rows) / len(rows)), 4) if rows else 0.0,
                "accepted_rate": round((int(accepted.get("resolved", 0) or 0) / len(rows)), 4) if rows else 0.0,
                "win_rate_improvement": improvement_wr,
                "avg_pnl_improvement": improvement_avg,
                "avg_gate_score": round(_avg([_safe_float(row.get("gate_score"), 0.0) for row in rows]), 4),
                "avg_filtered_spread_pct": round(_avg([_safe_float(row.get("spread_avg_pct"), 0.0) for row in pass_rows]), 6) if pass_rows else 0.0,
                "avg_filtered_imbalance": round(_avg([_safe_float(row.get("depth_imbalance"), 0.0) for row in pass_rows]), 4) if pass_rows else 0.0,
                "avg_filtered_refill_shift": round(_avg([_safe_float(row.get("depth_refill_shift"), 0.0) for row in pass_rows]), 4) if pass_rows else 0.0,
                "avg_filtered_rejection": round(_avg([_safe_float(row.get("rejection_ratio"), 0.0) for row in pass_rows]), 4) if pass_rows else 0.0,
                "missed_positive": missed_positive,
                "missed_positive_top_reasons": [{"reason": key, "count": int(val or 0)} for key, val in list(reason_counts.items())[:4]],
                "missed_positive_day_types": day_type_counts,
                "pros": [],
                "cons": [],
            }
            if improvement_wr > 0.05:
                family_row["pros"].append(f"win_rate+{improvement_wr:.2f}")
            if improvement_avg > 0.1:
                family_row["pros"].append(f"avg_pnl+{improvement_avg:.2f}")
            if family_row["filter_pass_rate"] < 0.25:
                family_row["cons"].append("sample_drop_high")
            if int(filt.get("resolved", 0) or 0) < 4:
                family_row["cons"].append("filtered_sample_small")
            if float(filt.get("pnl_usd", 0.0) or 0.0) <= 0.0:
                family_row["cons"].append("filtered_pnl_not_positive")
            if int(missed_positive.get("resolved", 0) or 0) > 0:
                family_row["cons"].append("missed_positive_rows_present")
            if (
                family_row["family"] in {"xau_scalp_pullback_limit", "xau_scalp_microtrend"}
                and int(filt.get("resolved", 0) or 0) >= 6
                and improvement_wr > 0.08
                and improvement_avg > 0.05
            ):
                recommendations.append(
                    {
                        "symbol": "XAUUSD",
                        "family": family_row["family"],
                        "action": "deploy_tick_depth_filter_canary",
                        "win_rate_improvement": improvement_wr,
                        "avg_pnl_improvement": improvement_avg,
                        "filter_pass_rate": family_row["filter_pass_rate"],
                    }
                )
            if int(missed_positive.get("resolved", 0) or 0) > 0:
                out["summary"]["missed_positive_families"] += 1
                missed_row = {
                    "symbol": "XAUUSD",
                    "family": family_row["family"],
                    "missed_positive": missed_positive,
                    "day_types": day_type_counts,
                    "top_reasons": [{"reason": key, "count": int(val or 0)} for key, val in list(reason_counts.items())[:4]],
                }
                missed_opportunities.append(missed_row)
                if (
                    family_row["family"] == "xau_scalp_tick_depth_filter"
                    and int(missed_positive.get("resolved", 0) or 0) >= 2
                    and float(missed_positive.get("pnl_usd", 0.0) or 0.0) > 0.0
                ):
                    recommendations.append(
                        {
                            "symbol": "XAUUSD",
                            "family": family_row["family"],
                            "action": "review_fast_expansion_sample_threshold",
                            "missed_positive_resolved": int(missed_positive.get("resolved", 0) or 0),
                            "missed_positive_pnl_usd": float(missed_positive.get("pnl_usd", 0.0) or 0.0),
                            "top_day_type": next(iter(day_type_counts.keys()), ""),
                            "top_reason": next(iter(reason_counts.keys()), ""),
                        }
                    )
            families.append(family_row)
        families.sort(
            key=lambda item: (
                float(item.get("win_rate_improvement", 0.0) or 0.0),
                float(((item.get("filtered") or {}).get("pnl_usd", 0.0) or 0.0)),
            ),
            reverse=True,
        )
        missed_opportunities.sort(
            key=lambda item: (
                float(((item.get("missed_positive") or {}).get("pnl_usd", 0.0) or 0.0)),
                int(((item.get("missed_positive") or {}).get("resolved", 0) or 0)),
            ),
            reverse=True,
        )
        out["families"] = families
        out["missed_opportunities"] = missed_opportunities
        out["recommendations"] = recommendations
        out["summary"]["families"] = len(families)
        self._save_report_snapshot("xau_tick_depth_filter_report", out)
        return out

    def build_ct_only_experiment_report(self, *, hours: Optional[int] = None) -> dict:
        lookback_hours = max(1, int(hours or getattr(config, "CT_ONLY_EXPERIMENT_REPORT_LOOKBACK_HOURS", 18) or 18))
        since_iso = _iso(_utc_now() - timedelta(hours=lookback_hours))
        fallback_hours = max(72, lookback_hours)
        external_prior_report = dict(
            self.build_external_model_prior_library_report()
            or self._load_json(self._report_path("external_model_prior_library_report"))
            or {}
        )
        prior_map = {
            str(row.get("family") or "").strip().lower(): dict(row)
            for row in list(external_prior_report.get("families") or [])
            if str(row.get("family") or "").strip()
        }
        tracked_sources = {
            "scalp_btcusd:bwl:canary": {"symbol": "BTCUSD", "family": "btc_weekday_lob_momentum"},
            "scalp_ethusd:ewp:canary": {"symbol": "ETHUSD", "family": "eth_weekday_overlap_probe"},
            "scalp_xauusd:td:canary": {"symbol": "XAUUSD", "family": "xau_scalp_tick_depth_filter"},
            "scalp_xauusd:pb:canary": {"symbol": "XAUUSD", "family": "xau_scalp_pullback_limit"},
            "scalp_xauusd:ff:canary": {"symbol": "XAUUSD", "family": "xau_scalp_failed_fade_follow_stop"},
            "scalp_xauusd:mfu:canary": {"symbol": "XAUUSD", "family": "xau_scalp_microtrend_follow_up"},
            "scalp_xauusd:fss:canary": {"symbol": "XAUUSD", "family": "xau_scalp_flow_short_sidecar"},
            "scalp_xauusd:rr:canary": {"symbol": "XAUUSD", "family": "xau_scalp_range_repair"},
            "scalp_xauusd:canary": {"symbol": "XAUUSD", "family": "xau_scalp_microtrend"},
        }
        source_placeholders = ", ".join("?" for _ in tracked_sources)
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "hours": lookback_hours,
            "since_utc": since_iso,
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "summary": {
                "tracked_sources": len(tracked_sources),
                "execution_rows": 0,
                "closed_rows": 0,
                "xau_td_resolved": 0,
                "xau_pb_resolved": 0,
                "xau_ff_resolved": 0,
                "xau_mfu_resolved": 0,
                "xau_fss_resolved": 0,
                "xau_rr_resolved": 0,
                "btc_bwl_resolved": 0,
                "eth_ewp_resolved": 0,
                "prior_backed_sources": 0,
            },
            "sources": [],
            "comparisons": {},
            "gate_analysis": {},
            "recommendations": [],
            "error": "",
        }
        if not self.ctrader_db_path.exists():
            out["ok"] = False
            out["error"] = "ctrader_db_not_found"
            return out
        source_rows: dict[str, dict] = {
            src: {
                "source": src,
                "symbol": meta["symbol"],
                "family": meta["family"],
                "execution": {},
                "closed_total": _new_bucket(),
                "latest_exec_utc": "",
                "latest_close_utc": "",
            }
            for src, meta in tracked_sources.items()
        }
        try:
            with closing(self._connect_ctrader()) as conn:
                journal_cols = _table_columns(conn, "execution_journal")
                def _load_source_rows(query_since_iso: str) -> tuple[int, int]:
                    exec_count = 0
                    close_count = 0
                    for item in list(source_rows.values()):
                        item["execution"] = {}
                        item["closed_total"] = _new_bucket()
                        item["latest_exec_utc"] = ""
                        item["latest_close_utc"] = ""
                    if "status" in journal_cols:
                        exec_rows = conn.execute(
                            f"""
                            SELECT source, status, MAX(created_utc) AS latest_exec_utc, COUNT(*) AS n
                              FROM execution_journal
                             WHERE created_utc >= ?
                               AND source IN ({source_placeholders})
                             GROUP BY source, status
                            """,
                            (query_since_iso, *tracked_sources.keys()),
                        ).fetchall()
                        for row in list(exec_rows or []):
                            src = _norm_source(row["source"])
                            item = source_rows.get(src)
                            if not item:
                                continue
                            status = str(row["status"] or "").strip().lower()
                            item["execution"][status] = int(item["execution"].get(status, 0) or 0) + int(row["n"] or 0)
                            latest_exec = str(row["latest_exec_utc"] or "").strip()
                            if latest_exec and latest_exec > str(item.get("latest_exec_utc") or ""):
                                item["latest_exec_utc"] = latest_exec
                            exec_count += int(row["n"] or 0)
                    close_rows = conn.execute(
                        f"""
                        SELECT j.source, d.outcome, d.pnl_usd, MAX(d.execution_utc) AS latest_close_utc, COUNT(*) AS n
                          FROM ctrader_deals d
                          JOIN execution_journal j ON j.id = d.journal_id
                         WHERE d.execution_utc >= ?
                           AND d.has_close_detail = 1
                           AND d.outcome IN (0, 1)
                           AND j.source IN ({source_placeholders})
                         GROUP BY j.source, d.outcome, d.pnl_usd, d.deal_id
                        """,
                        (query_since_iso, *tracked_sources.keys()),
                    ).fetchall()
                    for row in list(close_rows or []):
                        src = _norm_source(row["source"])
                        item = source_rows.get(src)
                        if not item:
                            continue
                        pnl = _safe_float(row["pnl_usd"], 0.0)
                        outcome = _safe_int(row["outcome"], -1)
                        _update_bucket(item["closed_total"], pnl, outcome)
                        latest_close = str(row["latest_close_utc"] or "").strip()
                        if latest_close and latest_close > str(item.get("latest_close_utc") or ""):
                            item["latest_close_utc"] = latest_close
                        close_count += 1
                    return exec_count, close_count

                exec_count, close_count = _load_source_rows(since_iso)
                if close_count <= 0 and fallback_hours > lookback_hours:
                    fallback_since_iso = _iso(_utc_now() - timedelta(hours=fallback_hours))
                    exec_count, close_count = _load_source_rows(fallback_since_iso)
                    out["hours"] = fallback_hours
                    out["since_utc"] = fallback_since_iso
                    out.setdefault("summary", {})["fallback_window_used"] = True
                else:
                    out.setdefault("summary", {})["fallback_window_used"] = False
                out["summary"]["execution_rows"] = int(exec_count)
                out["summary"]["closed_rows"] = int(close_count)
        except Exception as e:
            out["ok"] = False
            out["error"] = f"ct_only_experiment_query_error:{e}"
            return out

        sources = []
        for src, item in source_rows.items():
            row = dict(item)
            row["closed_total"] = _finalize_bucket(item["closed_total"])
            row["execution"] = dict(sorted((item.get("execution") or {}).items()))
            prior = dict(prior_map.get(str(row.get("family") or "").strip().lower()) or {})
            row["external_prior"] = prior
            row["external_prior_summary"] = _external_prior_summary(prior)
            row["external_prior_models"] = list(prior.get("prior_models") or [])
            if prior:
                out["summary"]["prior_backed_sources"] += 1
            sources.append(row)
        sources.sort(
            key=lambda item: (
                float(((item.get("closed_total") or {}).get("pnl_usd", 0.0) or 0.0)),
                float(((item.get("closed_total") or {}).get("win_rate", 0.0) or 0.0)),
                int(((item.get("closed_total") or {}).get("resolved", 0) or 0)),
            ),
            reverse=True,
        )
        out["sources"] = sources

        def _get_source(name: str) -> dict:
            return next((dict(x) for x in sources if str(x.get("source") or "") == name), {})

        xau_td = _get_source("scalp_xauusd:td:canary")
        xau_pb = _get_source("scalp_xauusd:pb:canary")
        xau_ff = _get_source("scalp_xauusd:ff:canary")
        xau_mfu = _get_source("scalp_xauusd:mfu:canary")
        xau_fss = _get_source("scalp_xauusd:fss:canary")
        xau_rr = _get_source("scalp_xauusd:rr:canary")
        xau_microtrend = _get_source("scalp_xauusd:canary")
        btc_bwl = _get_source("scalp_btcusd:bwl:canary")
        eth_ewp = _get_source("scalp_ethusd:ewp:canary")
        out["summary"]["xau_td_resolved"] = int(((xau_td.get("closed_total") or {}).get("resolved", 0) or 0))
        out["summary"]["xau_pb_resolved"] = int(((xau_pb.get("closed_total") or {}).get("resolved", 0) or 0))
        out["summary"]["xau_ff_resolved"] = int(((xau_ff.get("closed_total") or {}).get("resolved", 0) or 0))
        out["summary"]["xau_mfu_resolved"] = int(((xau_mfu.get("closed_total") or {}).get("resolved", 0) or 0))
        out["summary"]["xau_fss_resolved"] = int(((xau_fss.get("closed_total") or {}).get("resolved", 0) or 0))
        out["summary"]["xau_rr_resolved"] = int(((xau_rr.get("closed_total") or {}).get("resolved", 0) or 0))
        out["summary"]["btc_bwl_resolved"] = int(((btc_bwl.get("closed_total") or {}).get("resolved", 0) or 0))
        out["summary"]["eth_ewp_resolved"] = int(((eth_ewp.get("closed_total") or {}).get("resolved", 0) or 0))

        btc_gate = {
            "symbol": "BTCUSD",
            "family": "btc_weekday_lob_momentum",
            "lookback_hours": lookback_hours,
            "analysis_window_hours": lookback_hours,
            "fallback_used": False,
            "recent_base_rows": 0,
            "strict_reference": {"base_rows": 0, "passes": 0, "blockers": {}},
            "runtime_policy": {"base_rows": 0, "passes": 0, "blockers": {}},
            "runtime_added_passes": [],
            "runtime_added_pass_count": 0,
        }
        if self.signal_learning_db_path.exists():
            try:
                with closing(self._connect_signal_learning()) as conn:
                    signal_cols = _table_columns(conn, "signal_events")
                    if signal_cols and "source" in signal_cols:
                        def _query_gate_rows(hours_window: int):
                            return conn.execute(
                                """
                                SELECT created_at, direction, confidence, pattern, session, timeframe, extra_json
                                  FROM signal_events
                                 WHERE created_at >= ?
                                   AND source = 'scalp_btcusd'
                                 ORDER BY created_at DESC
                                """,
                                (_iso(_utc_now() - timedelta(hours=int(hours_window))),),
                            ).fetchall()

                        gate_rows = _query_gate_rows(lookback_hours)
                        btc_gate["recent_base_rows"] = len(list(gate_rows or []))
                        if btc_gate["recent_base_rows"] < 3:
                            gate_rows = _query_gate_rows(max(72, lookback_hours))
                            btc_gate["analysis_window_hours"] = max(72, lookback_hours)
                            btc_gate["fallback_used"] = True

                        def _bump(counter: dict, keys: list[str]) -> None:
                            for key in keys:
                                counter[key] = int(counter.get(key, 0) or 0) + 1

                        def _btc_eval(row, *, runtime: bool) -> tuple[bool, list[str], dict]:
                            payload = _safe_json_dict(str(row["extra_json"] or "{}"))
                            raw_scores = dict(payload.get("raw_scores", {}) or {})
                            direction = str(row["direction"] or "").strip().lower()
                            confidence = _safe_float(row["confidence"], 0.0)
                            pattern = str((row["pattern"] or payload.get("pattern") or raw_scores.get("pattern") or "")).strip().lower()
                            session_sig = _norm_signature(
                                row["session"]
                                or payload.get("session")
                                or raw_scores.get("session")
                                or raw_scores.get("session_zone")
                                or ""
                            )
                            timeframe = str((row["timeframe"] or payload.get("timeframe") or raw_scores.get("timeframe") or "")).strip().lower().replace(" ", "")
                            entry_type = str(
                                payload.get("entry_type")
                                or raw_scores.get("entry_type")
                                or raw_scores.get("scalp_m1_entry_order_type")
                                or raw_scores.get("scalp_m1_entry_type")
                                or "market"
                            ).strip().lower()
                            winner_regime = str(raw_scores.get("crypto_winner_logic_regime") or raw_scores.get("winner_logic_regime") or "").strip().lower()
                            neural_prob = _safe_float(raw_scores.get("neural_probability"), 0.0)
                            blockers: list[str] = []
                            relaxed_reason = ""
                            if direction != "long":
                                blockers.append("direction")
                            if session_sig not in {"new_york", "london,new_york,overlap"}:
                                blockers.append("session")
                            if timeframe != "5m+1m":
                                blockers.append("timeframe")
                            if not (70.0 <= confidence <= 74.9):
                                blockers.append("confidence")
                            if pattern not in {"ob_bounce", "choch_entry"}:
                                blockers.append("pattern")
                            winner_ok = winner_regime == "strong"
                            if runtime and not winner_ok:
                                if (
                                    bool(getattr(config, "BTC_WEEKDAY_LOB_ALLOW_NEUTRAL_OB_BOUNCE", True))
                                    and pattern == "ob_bounce"
                                    and winner_regime == "neutral"
                                    and confidence >= float(getattr(config, "BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_CONFIDENCE", 72.8) or 72.8)
                                    and neural_prob >= float(getattr(config, "BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_NEURAL_PROB", 0.65) or 0.65)
                                ):
                                    winner_ok = True
                                    relaxed_reason = "neutral_ob_bounce"
                            if not winner_ok:
                                blockers.append("winner")
                            if pattern == "choch_entry" and entry_type != "limit":
                                choch_market_ok = False
                                if runtime:
                                    choch_market_ok = (
                                        bool(getattr(config, "BTC_WEEKDAY_LOB_CHOCH_MARKET_TO_LIMIT_ENABLED", True))
                                        and entry_type == "market"
                                        and winner_regime == "strong"
                                        and confidence <= float(getattr(config, "BTC_WEEKDAY_LOB_CHOCH_MARKET_MAX_CONFIDENCE", 72.2) or 72.2)
                                        and neural_prob >= float(getattr(config, "BTC_WEEKDAY_LOB_CHOCH_MARKET_MIN_NEURAL_PROB", 0.63) or 0.63)
                                    )
                                    if choch_market_ok:
                                        relaxed_reason = "choch_market_to_limit"
                                if not choch_market_ok:
                                    blockers.append("choch_limit")
                            return (len(blockers) == 0), blockers, {
                                "created_at": str(row["created_at"] or ""),
                                "direction": direction,
                                "confidence": round(confidence, 2),
                                "pattern": pattern,
                                "session": session_sig,
                                "timeframe": timeframe,
                                "winner_regime": winner_regime,
                                "entry_type": entry_type,
                                "neural_probability": round(neural_prob, 4),
                                "relaxed_reason": relaxed_reason,
                            }

                        for row in list(gate_rows or []):
                            strict_ok, strict_blockers, strict_meta = _btc_eval(row, runtime=False)
                            runtime_ok, runtime_blockers, runtime_meta = _btc_eval(row, runtime=True)
                            btc_gate["strict_reference"]["base_rows"] += 1
                            btc_gate["runtime_policy"]["base_rows"] += 1
                            if strict_ok:
                                btc_gate["strict_reference"]["passes"] += 1
                            else:
                                _bump(btc_gate["strict_reference"]["blockers"], strict_blockers)
                            if runtime_ok:
                                btc_gate["runtime_policy"]["passes"] += 1
                            else:
                                _bump(btc_gate["runtime_policy"]["blockers"], runtime_blockers)
                            if runtime_ok and not strict_ok:
                                btc_gate["runtime_added_passes"].append(runtime_meta)
                        btc_gate["runtime_added_pass_count"] = len(list(btc_gate.get("runtime_added_passes") or []))
            except Exception:
                pass
        out["gate_analysis"]["btc_weekday_lob_momentum"] = btc_gate

        compare_min_resolved = max(1, int(getattr(config, "XAU_TD_VS_PB_COMPARE_MIN_RESOLVED", 4) or 4))
        td_stats = dict(xau_td.get("closed_total") or {})
        pb_stats = dict(xau_pb.get("closed_total") or {})
        xau_td_filter_report = dict(
            self._load_json(self._report_path("xau_tick_depth_filter_report"))
            or {}
        )
        td_filter_family = next(
            (
                dict(row)
                for row in list(xau_td_filter_report.get("families") or [])
                if str(row.get("family") or "").strip().lower() == "xau_scalp_tick_depth_filter"
            ),
            {},
        )
        leader = ""
        recommendation = "collect_more_sample"
        if int(td_stats.get("resolved", 0) or 0) >= compare_min_resolved and (
            float(td_stats.get("pnl_usd", 0.0) or 0.0) > float(pb_stats.get("pnl_usd", 0.0) or 0.0)
            or float(td_stats.get("win_rate", 0.0) or 0.0) > float(pb_stats.get("win_rate", 0.0) or 0.0)
        ):
            leader = "xau_scalp_tick_depth_filter"
            recommendation = "td_outperforming_pb_keep_collecting"
        elif int(pb_stats.get("resolved", 0) or 0) >= compare_min_resolved:
            leader = "xau_scalp_pullback_limit"
            recommendation = "pb_outperforming_td_keep_pb_primary"
        out["comparisons"]["xau_td_vs_pb_live"] = {
            "td": td_stats,
            "pb": pb_stats,
            "leader": leader,
            "recommendation": recommendation,
        }
        out["comparisons"]["xau_td_regret_opportunities"] = {
            "missed_positive": dict(td_filter_family.get("missed_positive") or {}),
            "day_types": dict(td_filter_family.get("missed_positive_day_types") or {}),
            "top_reasons": list(td_filter_family.get("missed_positive_top_reasons") or []),
            "accepted": dict(td_filter_family.get("accepted") or {}),
            "recommendation": (
                "review_fast_expansion_thresholds"
                if int(((td_filter_family.get("missed_positive") or {}).get("resolved", 0) or 0)) >= 2
                else "collect_td_sample"
            ),
        }
        ff_stats = dict(xau_ff.get("closed_total") or {})
        ff_execution = dict(xau_ff.get("execution") or {})
        ff_launches = sum(int(val or 0) for val in ff_execution.values())
        ff_resolved = int(ff_stats.get("resolved", 0) or 0)
        ff_wins = int(ff_stats.get("wins", 0) or 0)
        out["comparisons"]["xau_ff_effectiveness"] = {
            "ff": ff_stats,
            "launches": int(ff_launches),
            "close_rate": round((ff_resolved / ff_launches), 4) if ff_launches > 0 else 0.0,
            "reversal_rescue_rate": round((ff_wins / ff_launches), 4) if ff_launches > 0 else 0.0,
            "recommendation": (
                "ff_reducing_sl_before_reversal_keep_collecting"
                if ff_resolved >= compare_min_resolved and float(ff_stats.get("pnl_usd", 0.0) or 0.0) > 0.0
                else ("ff_launched_wait_for_close" if ff_launches > 0 and ff_resolved == 0 else "collect_ff_sample")
            ),
        }
        mfu_stats = dict(xau_mfu.get("closed_total") or {})
        microtrend_stats = dict(xau_microtrend.get("closed_total") or {})
        mfu_leader = ""
        mfu_recommendation = "collect_mfu_sample"
        if int(mfu_stats.get("resolved", 0) or 0) > 0:
            if (
                float(mfu_stats.get("pnl_usd", 0.0) or 0.0) >= float(microtrend_stats.get("pnl_usd", 0.0) or 0.0)
                and float(mfu_stats.get("win_rate", 0.0) or 0.0) >= float(microtrend_stats.get("win_rate", 0.0) or 0.0)
            ):
                mfu_leader = "xau_scalp_microtrend_follow_up"
                mfu_recommendation = "mfu_follow_up_state_improving"
            else:
                mfu_leader = "xau_scalp_microtrend"
                mfu_recommendation = "mfu_collect_more_sample"
        out["comparisons"]["xau_mfu_vs_broad_microtrend_live"] = {
            "mfu": mfu_stats,
            "broad_microtrend": microtrend_stats,
            "leader": mfu_leader,
            "recommendation": mfu_recommendation,
        }
        fss_stats = dict(xau_fss.get("closed_total") or {})
        fss_leader = ""
        fss_recommendation = "collect_fss_sample"
        if int(fss_stats.get("resolved", 0) or 0) > 0:
            if (
                float(fss_stats.get("pnl_usd", 0.0) or 0.0) >= float(microtrend_stats.get("pnl_usd", 0.0) or 0.0)
                and float(fss_stats.get("win_rate", 0.0) or 0.0) >= float(microtrend_stats.get("win_rate", 0.0) or 0.0)
            ):
                fss_leader = "xau_scalp_flow_short_sidecar"
                fss_recommendation = "fss_short_continuation_improving"
            else:
                fss_leader = "xau_scalp_microtrend"
                fss_recommendation = "fss_collect_more_sample"
        out["comparisons"]["xau_fss_vs_broad_microtrend_live"] = {
            "fss": fss_stats,
            "broad_microtrend": microtrend_stats,
            "leader": fss_leader,
            "recommendation": fss_recommendation,
        }

        btc_min_resolved = max(1, int(getattr(config, "BTC_WEEKDAY_LOB_PROMOTION_MIN_RESOLVED", 4) or 4))
        btc_min_wr = float(getattr(config, "BTC_WEEKDAY_LOB_PROMOTION_MIN_WIN_RATE", 0.55) or 0.55)
        btc_min_pnl = float(getattr(config, "BTC_WEEKDAY_LOB_PROMOTION_MIN_PNL_USD", 1.0) or 1.0)
        btc_stats = dict(btc_bwl.get("closed_total") or {})
        if (
            int(btc_stats.get("resolved", 0) or 0) >= btc_min_resolved
            and float(btc_stats.get("win_rate", 0.0) or 0.0) >= btc_min_wr
            and float(btc_stats.get("pnl_usd", 0.0) or 0.0) >= btc_min_pnl
        ):
            out["recommendations"].append(
                {
                    "symbol": "BTCUSD",
                    "family": "btc_weekday_lob_momentum",
                    "action": "promote_btc_weekday_lob_narrow_live",
                    "resolved": int(btc_stats.get("resolved", 0) or 0),
                    "win_rate": float(btc_stats.get("win_rate", 0.0) or 0.0),
                    "pnl_usd": float(btc_stats.get("pnl_usd", 0.0) or 0.0),
                }
            )
        elif int(btc_stats.get("resolved", 0) or 0) > 0:
            out["recommendations"].append(
                {
                    "symbol": "BTCUSD",
                    "family": "btc_weekday_lob_momentum",
                    "action": "collect_btc_weekday_lob_sample",
                    "resolved": int(btc_stats.get("resolved", 0) or 0),
                    "win_rate": float(btc_stats.get("win_rate", 0.0) or 0.0),
                    "pnl_usd": float(btc_stats.get("pnl_usd", 0.0) or 0.0),
                }
            )
        elif int((btc_gate.get("runtime_policy") or {}).get("passes", 0) or 0) > 0:
            out["recommendations"].append(
                {
                    "symbol": "BTCUSD",
                    "family": "btc_weekday_lob_momentum",
                    "action": "observe_runtime_bwl_candidates",
                    "runtime_passes": int((btc_gate.get("runtime_policy") or {}).get("passes", 0) or 0),
                    "strict_passes": int((btc_gate.get("strict_reference") or {}).get("passes", 0) or 0),
                    "runtime_added_pass_count": int(btc_gate.get("runtime_added_pass_count", 0) or 0),
                }
            )
        out["recommendations"].append(
            {
                "symbol": "XAUUSD",
                "family": str(out["comparisons"]["xau_td_vs_pb_live"].get("leader") or ""),
                "action": str(out["comparisons"]["xau_td_vs_pb_live"].get("recommendation") or ""),
                "td_resolved": int(td_stats.get("resolved", 0) or 0),
                "pb_resolved": int(pb_stats.get("resolved", 0) or 0),
            }
        )
        out["recommendations"].append(
            {
                "symbol": "XAUUSD",
                "family": "xau_scalp_failed_fade_follow_stop",
                "action": str(out["comparisons"]["xau_ff_effectiveness"].get("recommendation") or ""),
                "ff_resolved": int(ff_stats.get("resolved", 0) or 0),
                "ff_launches": int(ff_launches),
            }
        )
        out["recommendations"].append(
            {
                "symbol": "XAUUSD",
                "family": "xau_scalp_microtrend_follow_up",
                "action": str(out["comparisons"]["xau_mfu_vs_broad_microtrend_live"].get("recommendation") or ""),
                "mfu_resolved": int(mfu_stats.get("resolved", 0) or 0),
                "broad_microtrend_resolved": int(microtrend_stats.get("resolved", 0) or 0),
            }
        )
        out["recommendations"].append(
            {
                "symbol": "XAUUSD",
                "family": "xau_scalp_flow_short_sidecar",
                "action": str(out["comparisons"]["xau_fss_vs_broad_microtrend_live"].get("recommendation") or ""),
                "fss_resolved": int(fss_stats.get("resolved", 0) or 0),
                "broad_microtrend_resolved": int(microtrend_stats.get("resolved", 0) or 0),
            }
        )
        self._save_report_snapshot("ct_only_experiment_report", out)
        return out

    def build_ct_only_watch_report(self) -> dict:
        experiment = dict(
            self._load_json(self._report_path("ct_only_experiment_report"))
            or self.build_ct_only_experiment_report()
            or {}
        )
        trading_manager = dict(self._load_json(self._report_path("trading_manager_report")) or {})
        trading_manager_state = self._load_named_state(self.runtime_dir / "trading_manager_state.json")
        state = self._load_named_state(self.ct_only_watch_state_path)

        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "state_path": str(self.ct_only_watch_state_path),
            "report_refs": {
                "ct_only_experiment_report": str(self._report_path("ct_only_experiment_report")),
                "trading_manager_report": str(self._report_path("trading_manager_report")),
                "trading_manager_state": str(self.runtime_dir / "trading_manager_state.json"),
            },
            "summary": {
                "td_first_execution_detected": False,
                "td_first_resolved_detected": False,
                "ff_first_execution_detected": False,
                "ff_first_resolved_detected": False,
                "pb_demotion_applied": False,
            },
            "milestones": {},
            "error": "",
        }
        if not self.ctrader_db_path.exists():
            out["ok"] = False
            out["error"] = "ctrader_db_not_found"
            return out

        def _source_report_row(source_name: str) -> dict:
            target = _norm_source(source_name)
            return next(
                (
                    dict(row)
                    for row in list(experiment.get("sources") or [])
                    if _norm_source((row or {}).get("source")) == target
                ),
                {},
            )

        def _source_firsts(source_name: str) -> dict:
            target = _norm_source(source_name)
            exec_info = {"count": 0, "first_utc": "", "latest_utc": ""}
            close_info = {"count": 0, "first_utc": "", "latest_utc": ""}
            try:
                with closing(self._connect_ctrader()) as conn:
                    row = conn.execute(
                        """
                        SELECT COUNT(*) AS n, MIN(created_utc) AS first_utc, MAX(created_utc) AS latest_utc
                          FROM execution_journal
                         WHERE source = ?
                        """,
                        (target,),
                    ).fetchone()
                    if row is not None:
                        exec_info = {
                            "count": int(row["n"] or 0),
                            "first_utc": str(row["first_utc"] or "").strip(),
                            "latest_utc": str(row["latest_utc"] or "").strip(),
                        }
                    row = conn.execute(
                        """
                        SELECT COUNT(*) AS n, MIN(d.execution_utc) AS first_utc, MAX(d.execution_utc) AS latest_utc
                          FROM ctrader_deals d
                          JOIN execution_journal j ON j.id = d.journal_id
                         WHERE j.source = ?
                           AND d.has_close_detail = 1
                           AND d.outcome IN (0, 1)
                        """,
                        (target,),
                    ).fetchone()
                    if row is not None:
                        close_info = {
                            "count": int(row["n"] or 0),
                            "first_utc": str(row["first_utc"] or "").strip(),
                            "latest_utc": str(row["latest_utc"] or "").strip(),
                        }
            except Exception as e:
                out["ok"] = False
                out["error"] = f"watch_query_error:{e}"
            return {"execution": exec_info, "resolved": close_info}

        def _mark_first(slot: str, *, detected: bool, event_utc: str, extra: dict | None = None) -> dict:
            current = dict((state.get("milestones") or {}).get(slot) or {})
            payload = {
                "detected": bool(detected),
                "event_utc": str(event_utc or "").strip(),
                "first_seen_at": str(current.get("first_seen_at") or ""),
                "first_event_utc": str(current.get("first_event_utc") or ""),
            }
            if detected:
                if not payload["first_seen_at"]:
                    payload["first_seen_at"] = _iso(_utc_now())
                if not payload["first_event_utc"]:
                    payload["first_event_utc"] = str(event_utc or "").strip() or payload["first_seen_at"]
            if isinstance(extra, dict):
                for key, value in extra.items():
                    payload[str(key)] = value
            return payload

        td_report_row = _source_report_row("scalp_xauusd:td:canary")
        td_firsts = _source_firsts("scalp_xauusd:td:canary")
        ff_report_row = _source_report_row("scalp_xauusd:ff:canary")
        ff_firsts = _source_firsts("scalp_xauusd:ff:canary")

        xau_row = next(
            (
                dict(row)
                for row in list(trading_manager.get("symbols") or [])
                if _norm_symbol((row or {}).get("symbol")) == "XAUUSD"
            ),
            {},
        )
        routing_apply = dict(trading_manager.get("family_routing_apply") or {})
        routing_state = dict(trading_manager_state.get("xau_family_routing") or {})
        routing_recommendation = dict(xau_row.get("family_routing_recommendations") or {})
        xau_findings_text = " ".join(str(x or "") for x in list(xau_row.get("manager_findings") or []))
        promoted_families = {str(x or "").strip().lower() for x in list(routing_recommendation.get("promoted_families") or []) if str(x or "").strip()}
        support_mode_hint = str(routing_recommendation.get("support_mode") or "").strip().lower()
        pb_demotion_detected = False
        pb_demotion_event_utc = ""
        pb_demotion_reason = ""
        pb_demotion_support_mode = ""
        pb_demotion_mode = ""
        if (
            str(routing_state.get("status") or "").strip().lower() == "active"
            and str(routing_state.get("mode") or "").strip().lower() == "scheduled_dominant_demote_pb"
        ):
            pb_demotion_detected = True
            pb_demotion_event_utc = str(routing_state.get("applied_at") or "").strip()
            pb_demotion_reason = str(routing_state.get("reason") or "").strip()
            pb_demotion_mode = str(routing_state.get("mode") or "").strip()
            pb_demotion_support_mode = str(routing_state.get("support_mode") or routing_recommendation.get("support_mode") or "").strip()
        elif (
            str(routing_apply.get("status") or "").strip().lower() in {"applied", "already_active"}
            and (
                str(routing_recommendation.get("mode") or "").strip().lower() == "scheduled_dominant_demote_pb"
                or ("pb demotion" in xau_findings_text.lower())
                or (
                    support_mode_hint == "calibration_fallback"
                    and "xau_scalp_tick_depth_filter" in promoted_families
                )
            )
        ):
            pb_demotion_detected = True
            pb_demotion_event_utc = str((routing_state.get("applied_at") or routing_apply.get("generated_at") or out["generated_at"]) or "").strip()
            pb_demotion_reason = str(routing_apply.get("reason") or routing_recommendation.get("reason") or "").strip()
            pb_demotion_support_mode = str(routing_recommendation.get("support_mode") or "").strip()
            pb_demotion_mode = str(routing_recommendation.get("mode") or "manager_override").strip()

        milestones = {
            "td_first_execution": _mark_first(
                "td_first_execution",
                detected=int((td_firsts.get("execution") or {}).get("count", 0) or 0) > 0,
                event_utc=str((td_firsts.get("execution") or {}).get("first_utc") or ""),
                extra={
                    "source": "scalp_xauusd:td:canary",
                    "count": int((td_firsts.get("execution") or {}).get("count", 0) or 0),
                    "latest_event_utc": str((td_firsts.get("execution") or {}).get("latest_utc") or ""),
                    "latest_report_exec_utc": str(td_report_row.get("latest_exec_utc") or ""),
                },
            ),
            "td_first_resolved": _mark_first(
                "td_first_resolved",
                detected=int((td_firsts.get("resolved") or {}).get("count", 0) or 0) > 0,
                event_utc=str((td_firsts.get("resolved") or {}).get("first_utc") or ""),
                extra={
                    "source": "scalp_xauusd:td:canary",
                    "count": int((td_firsts.get("resolved") or {}).get("count", 0) or 0),
                    "latest_event_utc": str((td_firsts.get("resolved") or {}).get("latest_utc") or ""),
                },
            ),
            "ff_first_execution": _mark_first(
                "ff_first_execution",
                detected=int((ff_firsts.get("execution") or {}).get("count", 0) or 0) > 0,
                event_utc=str((ff_firsts.get("execution") or {}).get("first_utc") or ""),
                extra={
                    "source": "scalp_xauusd:ff:canary",
                    "count": int((ff_firsts.get("execution") or {}).get("count", 0) or 0),
                    "latest_event_utc": str((ff_firsts.get("execution") or {}).get("latest_utc") or ""),
                    "latest_report_exec_utc": str(ff_report_row.get("latest_exec_utc") or ""),
                },
            ),
            "ff_first_resolved": _mark_first(
                "ff_first_resolved",
                detected=int((ff_firsts.get("resolved") or {}).get("count", 0) or 0) > 0,
                event_utc=str((ff_firsts.get("resolved") or {}).get("first_utc") or ""),
                extra={
                    "source": "scalp_xauusd:ff:canary",
                    "count": int((ff_firsts.get("resolved") or {}).get("count", 0) or 0),
                    "latest_event_utc": str((ff_firsts.get("resolved") or {}).get("latest_utc") or ""),
                },
            ),
            "pb_demotion_applied": _mark_first(
                "pb_demotion_applied",
                detected=pb_demotion_detected,
                event_utc=pb_demotion_event_utc,
                extra={
                    "mode": pb_demotion_mode,
                    "support_mode": pb_demotion_support_mode,
                    "reason": pb_demotion_reason,
                },
            ),
        }

        state["milestones"] = milestones
        self._save_named_state(self.ct_only_watch_state_path, state)

        out["milestones"] = milestones
        out["summary"]["td_first_execution_detected"] = bool(milestones["td_first_execution"].get("detected"))
        out["summary"]["td_first_resolved_detected"] = bool(milestones["td_first_resolved"].get("detected"))
        out["summary"]["ff_first_execution_detected"] = bool(milestones["ff_first_execution"].get("detected"))
        out["summary"]["ff_first_resolved_detected"] = bool(milestones["ff_first_resolved"].get("detected"))
        out["summary"]["pb_demotion_applied"] = bool(milestones["pb_demotion_applied"].get("detected"))
        self._save_report_snapshot("ct_only_watch_report", out)
        return out

    def build_canary_post_trade_audit_report(self, *, days: int = 14) -> dict:
        lookback_days = max(1, int(days or 14))
        since_dt = _utc_now() - timedelta(days=lookback_days)
        since_iso = _iso(since_dt)
        milestones = list(config.get_canary_post_trade_audit_milestones() or [3, 5])
        min_milestone = min(milestones) if milestones else 3
        out = {
            "ok": False,
            "days": lookback_days,
            "since_utc": since_iso,
            "generated_at": _iso(_utc_now()),
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "state_path": str(self.canary_audit_state_path),
            "milestones": milestones,
            "summary": {
                "groups": 0,
                "total_canary_closed": 0,
                "review_ready": False,
                "btc_eth_canary_closed": 0,
                "btc_eth_focus_ready": False,
                "excluded_abnormal_rows": 0,
            },
            "rows": [],
            "symbols": [],
            "focus_queue": [],
            "milestone_events": [],
            "error": "",
        }

        lane_rows: dict[tuple[str, str, str], dict] = {}
        same_backend_controls: dict[tuple[str, str, str], dict] = {}
        cross_backend_controls: dict[tuple[str, str], dict] = {}
        if not self.mt5_db_path.exists() and not self.ctrader_db_path.exists():
            out["error"] = "no_execution_dbs"
            return out

        def ensure_row(backend: str, symbol: str, base_source: str) -> dict:
            key = (str(backend), _norm_symbol(symbol), _norm_source(base_source))
            row = lane_rows.get(key)
            if row is None:
                row = {
                    "backend": str(backend),
                    "symbol": _norm_symbol(symbol),
                    "base_source": _norm_source(base_source),
                    "canary": _new_bucket(),
                    "main": _new_bucket(),
                    "winner": _new_bucket(),
                    "latest_close_utc": "",
                }
                lane_rows[key] = row
            return row

        def ensure_same_backend_control(backend: str, symbol: str, base_source: str) -> dict:
            key = (str(backend), _norm_symbol(symbol), _norm_source(base_source))
            row = same_backend_controls.get(key)
            if row is None:
                row = {"main": _new_bucket(), "winner": _new_bucket()}
                same_backend_controls[key] = row
            return row

        def ensure_cross_backend_control(symbol: str, base_source: str) -> dict:
            key = (_norm_symbol(symbol), _norm_source(base_source))
            row = cross_backend_controls.get(key)
            if row is None:
                row = {"main": _new_bucket(), "winner": _new_bucket(), "backends": set(), "latest_close_utc": ""}
                cross_backend_controls[key] = row
            return row

        def record_control(backend: str, symbol: str, source: str, pnl: float, outcome: int, close_utc: str) -> None:
            src = _norm_source(source)
            sym = _norm_symbol(symbol)
            if (not src) or (not sym) or (":canary" in src) or (":bypass" in src):
                return
            bucket_name = "winner" if src.endswith(":winner") else "main"
            base = self._lane_base_source(src, ":winner") if bucket_name == "winner" else src
            same = ensure_same_backend_control(backend, sym, base)
            cross = ensure_cross_backend_control(sym, base)
            _update_bucket(same[bucket_name], pnl, outcome)
            _update_bucket(cross[bucket_name], pnl, outcome)
            cross["backends"].add(str(backend))
            stamp = str(close_utc or "").strip()
            if stamp and stamp > str(cross.get("latest_close_utc") or ""):
                cross["latest_close_utc"] = stamp

        def mark_latest(row: dict, close_utc: str) -> None:
            stamp = str(close_utc or "").strip()
            if stamp and stamp > str(row.get("latest_close_utc") or ""):
                row["latest_close_utc"] = stamp

        try:
            if self.mt5_db_path.exists():
                with closing(self._connect_mt5()) as conn:
                    rows = conn.execute(
                        """
                        SELECT created_at, source, COALESCE(signal_symbol, broker_symbol, '') AS symbol, outcome, pnl
                          FROM mt5_execution_journal
                         WHERE created_at >= ?
                           AND resolved=1
                           AND outcome IN (0, 1)
                         ORDER BY id ASC
                        """,
                        (since_iso,),
                    ).fetchall()
                    for raw in list(rows or []):
                        src = _norm_source(raw["source"])
                        sym = _norm_symbol(raw["symbol"])
                        if not src or not sym:
                            continue
                        pnl = _safe_float(raw["pnl"], 0.0)
                        outcome = _safe_int(raw["outcome"], -1)
                        close_utc = str(raw["created_at"] or "")
                        if src.endswith(":canary"):
                            base = self._lane_base_source(src, ":canary")
                            row = ensure_row("mt5", sym, base)
                            _update_bucket(row["canary"], pnl, outcome)
                            mark_latest(row, close_utc)
                        else:
                            record_control("mt5", sym, src, pnl, outcome, close_utc)

            if self.ctrader_db_path.exists():
                with closing(self._connect_ctrader()) as conn:
                    rows = conn.execute(
                        """
                        SELECT d.execution_utc, d.source, COALESCE(d.symbol, '') AS symbol, d.outcome, d.pnl_usd,
                               j.direction, j.stop_loss, j.take_profit, j.execution_meta_json, d.raw_json AS deal_raw_json
                          FROM ctrader_deals d
                          LEFT JOIN execution_journal j ON j.id = d.journal_id
                         WHERE d.execution_utc >= ?
                           AND d.has_close_detail = 1
                           AND d.journal_id IS NOT NULL
                           AND d.outcome IN (0, 1)
                         ORDER BY d.deal_id ASC
                        """,
                        (since_iso,),
                    ).fetchall()
                    for raw in list(rows or []):
                        src = _norm_source(raw["source"])
                        sym = _norm_symbol(raw["symbol"])
                        if not src or not sym:
                            continue
                        abnormal = _classify_trade_abnormality(
                            str(raw["direction"] or ""),
                            _actual_entry_from_deal(_safe_json_dict(str(raw["deal_raw_json"] or "{}"))),
                            _safe_float(raw["stop_loss"], 0.0),
                            _safe_float(raw["take_profit"], 0.0),
                            execution_meta=str(raw["execution_meta_json"] or "{}"),
                        )
                        if bool(abnormal.get("exclude_from_learning")):
                            out["summary"]["excluded_abnormal_rows"] = int(out["summary"].get("excluded_abnormal_rows", 0) or 0) + 1
                            continue
                        pnl = _safe_float(raw["pnl_usd"], 0.0)
                        outcome = _safe_int(raw["outcome"], -1)
                        close_utc = str(raw["execution_utc"] or "")
                        if src.endswith(":canary"):
                            base = self._lane_base_source(src, ":canary")
                            row = ensure_row("ctrader", sym, base)
                            _update_bucket(row["canary"], pnl, outcome)
                            mark_latest(row, close_utc)
                        else:
                            record_control("ctrader", sym, src, pnl, outcome, close_utc)
        except Exception as e:
            out["error"] = f"query_error:{e}"
            return out

        state = self._load_named_state(self.canary_audit_state_path)
        max_seen = dict(state.get("max_resolved") or {})
        milestone_events = []
        symbol_rollup: dict[str, dict] = {}
        rows_out: list[dict] = []

        for key in sorted(lane_rows.keys()):
            row = dict(lane_rows[key] or {})
            canary = _finalize_bucket(row.get("canary") or _new_bucket())
            if int(canary.get("resolved", 0) or 0) <= 0:
                continue
            same_ctrl = dict(same_backend_controls.get((row["backend"], row["symbol"], row["base_source"])) or {})
            cross_ctrl = dict(cross_backend_controls.get((row["symbol"], row["base_source"])) or {})
            main = _finalize_bucket(same_ctrl.get("main") or _new_bucket())
            winner = _finalize_bucket(same_ctrl.get("winner") or _new_bucket())
            control = _finalize_bucket(_merge_bucket(same_ctrl.get("main") or _new_bucket(), same_ctrl.get("winner") or _new_bucket()))
            cross_main = _finalize_bucket(cross_ctrl.get("main") or _new_bucket())
            cross_winner = _finalize_bucket(cross_ctrl.get("winner") or _new_bucket())
            cross_control_total = _finalize_bucket(_merge_bucket(cross_ctrl.get("main") or _new_bucket(), cross_ctrl.get("winner") or _new_bucket()))
            lane_key = f"{row['backend']}|{row['symbol']}|{row['base_source']}"
            prev = _safe_int(max_seen.get(lane_key), 0)
            current = int(canary.get("resolved", 0) or 0)
            for milestone in milestones:
                if prev < int(milestone) <= current:
                    milestone_events.append(
                        {
                            "backend": row["backend"],
                            "symbol": row["symbol"],
                            "base_source": row["base_source"],
                            "milestone": int(milestone),
                            "resolved": current,
                            "latest_close_utc": str(row.get("latest_close_utc") or ""),
                        }
                    )
            max_seen[lane_key] = max(prev, current)

            delta_wr = round(float(canary.get("win_rate", 0.0) or 0.0) - float(control.get("win_rate", 0.0) or 0.0), 4)
            delta_pnl = round(float(canary.get("pnl_usd", 0.0) or 0.0) - float(control.get("pnl_usd", 0.0) or 0.0), 4)
            cross_delta_wr = round(float(canary.get("win_rate", 0.0) or 0.0) - float(cross_control_total.get("win_rate", 0.0) or 0.0), 4)
            cross_delta_pnl = round(float(canary.get("pnl_usd", 0.0) or 0.0) - float(cross_control_total.get("pnl_usd", 0.0) or 0.0), 4)
            row_out = {
                "backend": row["backend"],
                "symbol": row["symbol"],
                "base_source": row["base_source"],
                "latest_close_utc": str(row.get("latest_close_utc") or ""),
                "canary": canary,
                "main": main,
                "winner": winner,
                "control_total": control,
                "control_cross_backend_main": cross_main,
                "control_cross_backend_winner": cross_winner,
                "control_cross_backend_total": cross_control_total,
                "control_cross_backend_backends": sorted(list(cross_ctrl.get("backends") or set())),
                "deltas_vs_control": {
                    "win_rate": delta_wr,
                    "pnl_usd": delta_pnl,
                    "resolved": int(canary.get("resolved", 0) or 0) - int(control.get("resolved", 0) or 0),
                },
                "deltas_vs_cross_backend_control": {
                    "win_rate": cross_delta_wr,
                    "pnl_usd": cross_delta_pnl,
                    "resolved": int(canary.get("resolved", 0) or 0) - int(cross_control_total.get("resolved", 0) or 0),
                },
                "comparison_basis": "cross_backend" if int(cross_control_total.get("resolved", 0) or 0) > 0 else "same_backend",
                "status": (
                    "review_ready"
                    if int(canary.get("resolved", 0) or 0) >= min_milestone
                    else "waiting_sample"
                ),
            }
            rows_out.append(row_out)

            sym_roll = symbol_rollup.setdefault(
                row["symbol"],
                {"canary": _new_bucket(), "control": _new_bucket(), "cross_control": _new_bucket(), "backends": set(), "sources": set(), "cross_sources": set()},
            )
            sym_roll["canary"] = _merge_bucket(sym_roll.get("canary") or _new_bucket(), row.get("canary") or _new_bucket())
            sym_roll["control"] = _merge_bucket(sym_roll.get("control") or _new_bucket(), _merge_bucket(same_ctrl.get("main") or _new_bucket(), same_ctrl.get("winner") or _new_bucket()))
            src_key = str(row.get("base_source") or "")
            if src_key not in set(sym_roll.get("cross_sources") or set()):
                sym_roll["cross_control"] = _merge_bucket(sym_roll.get("cross_control") or _new_bucket(), _merge_bucket(cross_ctrl.get("main") or _new_bucket(), cross_ctrl.get("winner") or _new_bucket()))
                sym_roll.setdefault("cross_sources", set()).add(src_key)
            sym_roll["backends"].add(row["backend"])
            sym_roll["sources"].add(row["base_source"])

        rows_out.sort(
            key=lambda item: (
                int(((item.get("canary") or {}).get("resolved", 0) or 0)),
                float(((item.get("canary") or {}).get("pnl_usd", 0.0) or 0.0)),
                float(((item.get("canary") or {}).get("win_rate", 0.0) or 0.0)),
            ),
            reverse=True,
        )
        symbols_out = []
        for sym, roll in sorted(symbol_rollup.items()):
            canary = _finalize_bucket(roll.get("canary") or _new_bucket())
            control = _finalize_bucket(roll.get("control") or _new_bucket())
            cross_control = _finalize_bucket(roll.get("cross_control") or _new_bucket())
            symbols_out.append(
                {
                    "symbol": sym,
                    "canary_total": canary,
                    "control_total": control,
                    "control_cross_backend_total": cross_control,
                    "backends": sorted(list(roll.get("backends") or set())),
                    "sources": sorted(list(roll.get("sources") or set())),
                    "delta_vs_control": {
                        "win_rate": round(float(canary.get("win_rate", 0.0) or 0.0) - float(control.get("win_rate", 0.0) or 0.0), 4),
                        "pnl_usd": round(float(canary.get("pnl_usd", 0.0) or 0.0) - float(control.get("pnl_usd", 0.0) or 0.0), 4),
                    },
                    "delta_vs_cross_backend_control": {
                        "win_rate": round(float(canary.get("win_rate", 0.0) or 0.0) - float(cross_control.get("win_rate", 0.0) or 0.0), 4),
                        "pnl_usd": round(float(canary.get("pnl_usd", 0.0) or 0.0) - float(cross_control.get("pnl_usd", 0.0) or 0.0), 4),
                    },
                }
            )
        symbols_out.sort(
            key=lambda item: (
                int(((item.get("canary_total") or {}).get("resolved", 0) or 0)),
                float(((item.get("canary_total") or {}).get("pnl_usd", 0.0) or 0.0)),
            ),
            reverse=True,
        )

        state["version"] = 1
        state["max_resolved"] = max_seen
        self._save_named_state(self.canary_audit_state_path, state)

        total_canary_closed = sum(int(((row.get("canary") or {}).get("resolved", 0) or 0)) for row in rows_out)
        btc_eth_closed = sum(
            int(((row.get("canary") or {}).get("resolved", 0) or 0))
            for row in rows_out
            if str(row.get("symbol") or "") in {"BTCUSD", "ETHUSD"}
        )
        out["ok"] = True
        out["summary"] = {
            "groups": len(rows_out),
            "total_canary_closed": total_canary_closed,
            "review_ready": bool(total_canary_closed >= min_milestone),
            "btc_eth_canary_closed": btc_eth_closed,
            "btc_eth_focus_ready": bool(btc_eth_closed >= min_milestone),
        }
        out["rows"] = rows_out
        out["symbols"] = symbols_out
        out["focus_queue"] = rows_out[:6]
        out["milestone_events"] = milestone_events
        out["recommendations"] = self._build_canary_tuning_recommendations(symbols_out)
        out["status"] = "review_ready" if bool(total_canary_closed >= min_milestone) else "waiting_sample"
        return out

    def _current_value(self, key: str) -> str:
        try:
            if self.env_local_path.exists():
                text = self.env_local_path.read_text(encoding="utf-8")
                prefix = f"{str(key)}="
                for line in text.splitlines():
                    s = str(line or "").strip()
                    if not s or s.startswith("#"):
                        continue
                    if s.upper().startswith(prefix.upper()):
                        return s.split("=", 1)[1] if "=" in s else ""
        except Exception:
            pass
        if key in os.environ:
            return str(os.environ.get(key, ""))
        if hasattr(config, key):
            value = getattr(config, key)
            if isinstance(value, bool):
                return "1" if value else "0"
            return str(value)
        return ""

    @staticmethod
    def _coerce_config_value(key: str, value: str):
        current = getattr(config, key, None)
        if isinstance(current, bool):
            return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(current, int) and not isinstance(current, bool):
            return int(float(value))
        if isinstance(current, float):
            return float(value)
        return str(value)

    def _apply_runtime_value(self, key: str, value: str) -> None:
        os.environ[str(key)] = str(value)
        if hasattr(config, str(key)):
            try:
                setattr(config, str(key), self._coerce_config_value(str(key), str(value)))
            except Exception:
                setattr(config, str(key), str(value))

    @staticmethod
    def _upsert_env_key(path: Path, key: str, value: str) -> dict:
        out = {"ok": False, "path": str(path), "key": str(key), "updated": False, "created": False, "line": f"{key}={value}", "backup_path": "", "error": ""}
        try:
            old = path.read_text(encoding="utf-8") if path.exists() else ""
            newline = "\r\n" if "\r\n" in old else "\n"
            lines = old.splitlines()
            prefix = f"{key}="
            replaced = False
            for i, line in enumerate(lines):
                s = str(line or "").strip()
                if not s or s.startswith("#"):
                    continue
                if s.upper().startswith(prefix.upper()):
                    lines[i] = f"{key}={value}"
                    replaced = True
                    break
            if not replaced:
                if lines and str(lines[-1]).strip():
                    lines.append("")
                lines.append(f"{key}={value}")
            new_text = newline.join(lines)
            if new_text and not new_text.endswith(newline):
                new_text += newline
            changed = (new_text != old)
            if changed:
                if old:
                    backup_dir = path.parent / "data" / "env_backups"
                    backup_dir.mkdir(parents=True, exist_ok=True)
                    ts = _utc_now().strftime("%Y%m%d_%H%M%S")
                    backup_path = backup_dir / f"{path.name}.{ts}.bak"
                    backup_path.write_text(old, encoding="utf-8")
                    out["backup_path"] = str(backup_path)
                    keep = max(5, int(getattr(config, "AUTO_APPLY_LIVE_PROFILE_ENV_BACKUP_KEEP", 20) or 20))
                    backups = sorted(backup_dir.glob(f"{path.name}.*.bak"), key=lambda p: p.name)
                    for drop in backups[: max(0, len(backups) - keep)]:
                        try:
                            drop.unlink()
                        except Exception:
                            pass
                path.write_text(new_text, encoding="utf-8")
            out["ok"] = True
            out["updated"] = bool(changed)
            out["created"] = bool((not replaced) and changed)
            return out
        except Exception as e:
            out["error"] = str(e)
            return out

    def _load_state(self) -> dict:
        state = self._load_json(self.state_path)
        if not isinstance(state, dict):
            state = {}
        state.setdefault("version", 1)
        state.setdefault("history", [])
        state.setdefault("active_bundle", None)
        return state

    def _save_state(self, state: dict) -> None:
        payload = dict(state or {})
        payload["updated_at"] = _iso(_utc_now())
        self.state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _lower_csv(values: set[str]) -> str:
        return ",".join(sorted({str(v or "").strip().lower() for v in (values or set()) if str(v or "").strip()}))

    @staticmethod
    def _signature_csv(values: list[str]) -> str:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in list(values or []):
            item = str(value or "").strip()
            if not item or item in seen:
                continue
            seen.add(item)
            ordered.append(item)
        return "|".join(ordered)

    def _evaluate_live_performance_since(self, *, since_iso: str, affected_symbols: set[str], affected_sources: set[str]) -> dict:
        mt5_bucket = _new_bucket()
        ctrader_bucket = _new_bucket()
        if self.mt5_db_path.exists():
            try:
                with closing(self._connect_mt5()) as conn:
                    rows = conn.execute(
                        """
                        SELECT source, signal_symbol, broker_symbol, outcome, pnl
                          FROM mt5_execution_journal
                         WHERE created_at >= ?
                           AND resolved=1
                           AND outcome IN (0, 1)
                         ORDER BY id ASC
                        """,
                        (since_iso,),
                    ).fetchall()
                    for row in list(rows or []):
                        sym = _norm_symbol(row["signal_symbol"] or row["broker_symbol"] or "")
                        src = _norm_source(row["source"])
                        if affected_symbols and sym not in affected_symbols and src not in affected_sources:
                            continue
                        _update_bucket(mt5_bucket, _safe_float(row["pnl"], 0.0), _safe_int(row["outcome"], -1))
            except Exception:
                pass
        if self.ctrader_db_path.exists():
            try:
                with closing(self._connect_ctrader()) as conn:
                    rows = conn.execute(
                        """
                        SELECT source, symbol, outcome, pnl_usd
                          FROM ctrader_deals
                         WHERE execution_utc >= ?
                           AND has_close_detail = 1
                           AND journal_id IS NOT NULL
                           AND outcome IN (0, 1)
                         ORDER BY deal_id ASC
                        """,
                        (since_iso,),
                    ).fetchall()
                    for row in list(rows or []):
                        sym = _norm_symbol(row["symbol"])
                        src = _norm_source(row["source"])
                        if affected_symbols and sym not in affected_symbols and src not in affected_sources:
                            continue
                        _update_bucket(ctrader_bucket, _safe_float(row["pnl_usd"], 0.0), _safe_int(row["outcome"], -1))
            except Exception:
                pass
        return {"since_utc": since_iso, "mt5": _finalize_bucket(mt5_bucket), "ctrader": _finalize_bucket(ctrader_bucket), "total": _finalize_bucket(_merge_bucket(mt5_bucket, ctrader_bucket))}

    def _managed_ctrader_sources(self) -> set[str]:
        return {"scalp_btcusd", "scalp_btcusd:winner", "scalp_ethusd", "scalp_ethusd:winner", "scalp_xauusd", "xauusd_scheduled", "xauusd_scheduled:winner"}

    def _build_canary_tuning_recommendations(self, symbols: list[dict]) -> list[dict]:
        min_sample = max(2, int(getattr(config, "AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 6) or 6))
        min_wr = float(getattr(config, "AUTO_APPLY_LIVE_PROFILE_MIN_WIN_RATE", 0.60) or 0.60)
        min_pnl = float(getattr(config, "AUTO_APPLY_LIVE_PROFILE_MIN_PNL_USD", 0.0) or 0.0)
        out: list[dict] = []
        for row in list(symbols or []):
            sym = _norm_symbol(row.get("symbol"))
            stats = dict(row.get("canary_total") or {})
            control = dict(row.get("control_cross_backend_total") or row.get("control_total") or {})
            resolved = int(stats.get("resolved", 0) or 0)
            wr = float(stats.get("win_rate", 0.0) or 0.0)
            pnl = float(stats.get("pnl_usd", 0.0) or 0.0)
            control_resolved = int(control.get("resolved", 0) or 0)
            control_wr = float(control.get("win_rate", 0.0) or 0.0)
            control_pnl = float(control.get("pnl_usd", 0.0) or 0.0)
            if resolved < min_sample:
                continue

            key = ""
            current = None
            proposed = None
            action = ""
            reason = ""
            min_floor = 0.0
            max_cap = 100.0

            if sym == "XAUUSD":
                key = "NEURAL_GATE_CANARY_MIN_CONFIDENCE"
                current = float(getattr(config, key, 72.0) or 72.0)
                min_floor = float(getattr(config, "AUTO_APPLY_XAU_CANARY_CONFIDENCE_MIN", 68.0) or 68.0)
                max_cap = float(getattr(config, "AUTO_APPLY_XAU_CANARY_CONFIDENCE_MAX", 80.0) or 80.0)
                if control_resolved >= min_sample:
                    if (wr >= control_wr + 0.03) and (pnl >= control_pnl):
                        proposed = max(min_floor, current - 1.0)
                        action = "loosen_xau_canary"
                        reason = "xau_canary_outperform_cross_backend_control"
                    elif (wr <= max(0.0, control_wr - 0.03)) or (pnl < control_pnl):
                        proposed = min(max_cap, current + 1.0)
                        action = "tighten_xau_canary"
                        reason = "xau_canary_underperform_cross_backend_control"
                elif pnl > min_pnl and wr >= min_wr:
                    proposed = max(min_floor, current - 1.0)
                    action = "loosen_xau_canary"
                    reason = "xau_canary_edge_positive"
                elif pnl <= min_pnl or wr < max(0.48, min_wr - 0.08):
                    proposed = min(max_cap, current + 1.0)
                    action = "tighten_xau_canary"
                    reason = "xau_canary_edge_negative"
            elif sym == "BTCUSD":
                key = "SCALPING_BTC_MIN_CONFIDENCE_WEEKEND"
                current = float(getattr(config, key, 75.0) or 75.0)
                min_floor = float(getattr(config, "AUTO_APPLY_BTC_WEEKEND_CONFIDENCE_MIN", 70.0) or 70.0)
                max_cap = float(getattr(config, "AUTO_APPLY_BTC_WEEKEND_CONFIDENCE_MAX", 82.0) or 82.0)
                if control_resolved >= min_sample:
                    if (wr >= control_wr + 0.03) and (pnl >= control_pnl):
                        proposed = max(min_floor, current - 1.0)
                        action = "loosen_btc_weekend_conf"
                        reason = "btc_canary_outperform_cross_backend_control"
                    elif (wr <= max(0.0, control_wr - 0.03)) or (pnl < control_pnl):
                        proposed = min(max_cap, current + 1.0)
                        action = "tighten_btc_weekend_conf"
                        reason = "btc_canary_underperform_cross_backend_control"
                elif pnl > min_pnl and wr >= max(min_wr, 0.55):
                    proposed = max(min_floor, current - 1.0)
                    action = "loosen_btc_weekend_conf"
                    reason = "btc_canary_edge_positive"
                elif pnl <= min_pnl or wr < 0.50:
                    proposed = min(max_cap, current + 1.0)
                    action = "tighten_btc_weekend_conf"
                    reason = "btc_canary_edge_negative"
            elif sym == "ETHUSD":
                key = "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND"
                current = float(getattr(config, key, 76.0) or 76.0)
                min_floor = float(getattr(config, "AUTO_APPLY_ETH_WEEKEND_CONFIDENCE_MIN", 72.0) or 72.0)
                max_cap = float(getattr(config, "AUTO_APPLY_ETH_WEEKEND_CONFIDENCE_MAX", 84.0) or 84.0)
                if control_resolved >= min_sample:
                    if (wr >= control_wr + 0.03) and (pnl >= control_pnl):
                        proposed = max(min_floor, current - 1.0)
                        action = "loosen_eth_weekend_conf"
                        reason = "eth_canary_outperform_cross_backend_control"
                    elif (wr <= max(0.0, control_wr - 0.03)) or (pnl < control_pnl):
                        proposed = min(max_cap, current + 1.0)
                        action = "tighten_eth_weekend_conf"
                        reason = "eth_canary_underperform_cross_backend_control"
                elif pnl > min_pnl and wr >= max(min_wr, 0.58):
                    proposed = max(min_floor, current - 1.0)
                    action = "loosen_eth_weekend_conf"
                    reason = "eth_canary_edge_positive"
                elif pnl <= min_pnl or wr < 0.52:
                    proposed = min(max_cap, current + 1.0)
                    action = "tighten_eth_weekend_conf"
                    reason = "eth_canary_edge_negative"

            if not key or current is None or proposed is None:
                continue
            if round(float(proposed), 4) == round(float(current), 4):
                continue
            out.append(
                {
                    "symbol": sym,
                    "key": key,
                    "current": round(float(current), 4),
                    "proposed": round(float(proposed), 4),
                    "action": action,
                    "reason": reason,
                    "resolved": resolved,
                    "win_rate": round(wr, 4),
                    "pnl_usd": round(pnl, 4),
                    "control_resolved": control_resolved,
                    "control_win_rate": round(control_wr, 4),
                    "control_pnl_usd": round(control_pnl, 4),
                }
            )
        out.sort(key=lambda item: (abs(float(item.get("pnl_usd", 0.0) or 0.0)), int(item.get("resolved", 0) or 0)), reverse=True)
        return out

    @staticmethod
    def _score_bucket_for_ranking(bucket: dict, *, model_bucket: bool = False) -> Optional[float]:
        stat = _finalize_bucket(bucket or _new_bucket())
        resolved = int(stat.get("resolved", 0) or 0)
        if resolved <= 0:
            return None
        wr = float(stat.get("win_rate", 0.0) or 0.0)
        avg = float(stat.get("avg_pnl_usd", 0.0) or 0.0)
        pnl = float(stat.get("pnl_usd", 0.0) or 0.0)
        wr_factor = (wr - 0.50) * (70.0 if model_bucket else 100.0)
        avg_factor = avg * (2.0 if model_bucket else 4.0)
        sample_bonus = min(resolved, 24) * (0.18 if model_bucket else 0.45)
        pnl_bonus = 2.5 if pnl > 0 else (-2.5 if pnl < 0 else 0.0)
        return round(wr_factor + avg_factor + sample_bonus + pnl_bonus, 4)

    @staticmethod
    def _window_weights(days_windows: list[int]) -> dict[int, float]:
        ordered = [int(v) for v in list(days_windows or []) if int(v) > 0]
        if not ordered:
            ordered = [3, 7, 14]
        ordered = sorted(list(dict.fromkeys(ordered)))
        raw: dict[int, float] = {}
        remaining = len(ordered)
        for idx, days in enumerate(ordered):
            raw[int(days)] = float(remaining - idx)
        total = sum(raw.values()) or 1.0
        return {int(days): round(float(weight) / float(total), 4) for days, weight in raw.items()}

    def _strategy_family_for_source(self, symbol: str, source: str) -> str:
        sym = _norm_symbol(symbol)
        src = self._lane_base_source(self._lane_base_source(source, ":canary"), ":winner")
        family_aliases = {
            "pb": "xau_scalp_pullback_limit",
            "bs": "xau_scalp_breakout_stop",
            "td": "xau_scalp_tick_depth_filter",
            "ff": "xau_scalp_failed_fade_follow_stop",
            "mfu": "xau_scalp_microtrend_follow_up",
            "fss": "xau_scalp_flow_short_sidecar",
            "rr": "xau_scalp_range_repair",
            "bwl": "btc_weekday_lob_momentum",
            "ewp": "eth_weekday_overlap_probe",
            "xau_scalp_pullback_limit": "xau_scalp_pullback_limit",
            "xau_scalp_breakout_stop": "xau_scalp_breakout_stop",
            "xau_scalp_tick_depth_filter": "xau_scalp_tick_depth_filter",
            "xau_scalp_failed_fade_follow_stop": "xau_scalp_failed_fade_follow_stop",
            "xau_scalp_microtrend_follow_up": "xau_scalp_microtrend_follow_up",
            "xau_scalp_flow_short_sidecar": "xau_scalp_flow_short_sidecar",
            "xau_scalp_range_repair": "xau_scalp_range_repair",
            "btc_weekday_lob_momentum": "btc_weekday_lob_momentum",
            "eth_weekday_overlap_probe": "eth_weekday_overlap_probe",
        }
        src_parts = [part.strip().lower() for part in str(src or "").split(":") if part.strip()]
        if len(src_parts) >= 2:
            explicit_family = family_aliases.get(src_parts[1], "")
            if explicit_family:
                return explicit_family
            src = str(src_parts[0] or "")
        if sym == "XAUUSD":
            if src == "xauusd_scheduled":
                return "xau_scheduled_trend"
            if src == "scalp_xauusd":
                return "xau_scalp_microtrend"
        if sym == "BTCUSD" and src == "scalp_btcusd":
            return "btc_weekend_winner"
        if sym == "ETHUSD" and src == "scalp_ethusd":
            return "eth_weekend_winner"
        return ""

    def _ctrader_context_from_journal_row(
        self,
        journal_row: sqlite3.Row | dict | None,
        *,
        fallback_source: str = "",
        fallback_symbol: str = "",
    ) -> dict:
        row = dict(journal_row or {})
        source = _norm_source(row.get("source") or fallback_source)
        symbol = _norm_symbol(row.get("symbol") or fallback_symbol)
        request_json = str(row.get("request_json") or "{}")
        payload = _safe_json_dict(request_json)
        req_ctx = _extract_request_context(request_json)
        raw_scores = dict(req_ctx.get("raw_scores") or {})
        mtf_snapshot = dict(
            payload.get("xau_multi_tf_snapshot")
            or raw_scores.get("xau_multi_tf_snapshot")
            or {}
        )
        family = self._present_context_value(
            raw_scores.get("strategy_family")
            or raw_scores.get("family")
            or self._strategy_family_for_source(symbol, source),
            lower=True,
        )
        reasons = self._normalized_text_list(payload.get("reasons") or raw_scores.get("reasons") or [])
        warnings = self._normalized_text_list(payload.get("warnings") or raw_scores.get("warnings") or [])
        strict_alignment = self._present_context_value(
            mtf_snapshot.get("strict_alignment") or mtf_snapshot.get("alignment"),
            lower=True,
        )
        aligned_side = self._present_context_value(
            payload.get("xau_mtf_aligned_side")
            or raw_scores.get("xau_mtf_aligned_side")
            or mtf_snapshot.get("strict_aligned_side")
            or mtf_snapshot.get("aligned_side"),
            lower=True,
        )
        return {
            "source": source,
            "symbol": symbol,
            "family": family,
            "strategy_family": family,
            "entry_type": self._present_context_value(
                payload.get("entry_type") or row.get("entry_type") or req_ctx.get("entry_type"),
                lower=True,
            ),
            "session": self._present_context_value(req_ctx.get("session"), lower=True),
            "timeframe": self._present_context_value(req_ctx.get("timeframe"), lower=True),
            "pattern": self._present_context_value(req_ctx.get("pattern")),
            "reasons": reasons,
            "warnings": warnings,
            "strict_alignment": strict_alignment,
            "xau_mtf_aligned_side": aligned_side,
            "xau_multi_tf_snapshot": mtf_snapshot if mtf_snapshot else {},
        }

    def build_ctrader_data_integrity_report(
        self,
        *,
        days: Optional[int] = None,
        repair: Optional[bool] = None,
    ) -> dict:
        lookback_days = max(1, int(days or getattr(config, "CTRADER_DATA_INTEGRITY_REPORT_LOOKBACK_DAYS", 120) or 120))
        repair_on_run = bool(
            getattr(config, "CTRADER_DATA_INTEGRITY_REPORT_REPAIR_ON_RUN", True)
            if repair is None
            else repair
        )
        out = {
            "ok": False,
            "generated_at": _iso(_utc_now()),
            "days": lookback_days,
            "repair_on_run": repair_on_run,
            "db_path": str(self.ctrader_db_path),
            "summary": {
                "journal_rows": 0,
                "deal_rows": 0,
                "journal_missing_family": 0,
                "journal_missing_reasons": 0,
                "journal_missing_warnings": 0,
                "journal_missing_pattern": 0,
                "journal_missing_session": 0,
                "journal_missing_timeframe": 0,
                "journal_missing_mtf": 0,
                "journal_missing_request_payload": 0,
                "journalless_deals": 0,
                "deal_rows_missing_context_before": 0,
                "deal_rows_repaired": 0,
                "deal_rows_remaining_missing": 0,
                "open_positions_missing_sl": 0,
                "open_positions_missing_tp": 0,
                "open_orders_missing_sl": 0,
                "open_orders_missing_tp": 0,
            },
            "journal_coverage": {},
            "deal_coverage_before": {},
            "deal_coverage_after": {},
            "examples": {
                "journal_missing": [],
                "deal_repaired": [],
                "deal_remaining": [],
                "protection_gaps": [],
            },
            "error": "",
        }
        if not self.ctrader_db_path.exists():
            out["error"] = "ctrader_db_missing"
            self._save_report_snapshot("ctrader_data_integrity_report", out)
            return out

        since_iso = _iso(_utc_now() - timedelta(days=lookback_days))
        journal_context: dict[int, dict] = {}
        journal_field_counts = {
            "family": 0,
            "reasons": 0,
            "warnings": 0,
            "pattern": 0,
            "session": 0,
            "timeframe": 0,
            "xau_multi_tf_snapshot": 0,
        }
        deal_before_counts = {
            "family": 0,
            "strategy_family": 0,
            "entry_type": 0,
            "session": 0,
            "timeframe": 0,
            "pattern": 0,
            "reasons": 0,
            "warnings": 0,
            "strict_alignment": 0,
            "xau_multi_tf_snapshot": 0,
        }
        deal_after_counts = dict(deal_before_counts)

        def _field_present(payload: dict, key: str) -> bool:
            value = (payload or {}).get(key)
            if isinstance(value, list):
                return bool(self._normalized_text_list(value))
            if isinstance(value, dict):
                return bool(value)
            return bool(self._present_context_value(value))

        try:
            with self._connect_ctrader() as conn:
                journal_rows = list(
                    conn.execute(
                        """
                        SELECT id, created_utc, source, symbol, entry_type, request_json
                          FROM execution_journal
                         WHERE created_utc >= ?
                         ORDER BY created_utc DESC, id DESC
                        """,
                        (since_iso,),
                    ).fetchall()
                )
                deal_rows = list(
                    conn.execute(
                        """
                        SELECT deal_id, source, symbol, journal_id, raw_json, has_close_detail, execution_utc
                          FROM ctrader_deals
                         WHERE execution_utc >= ?
                         ORDER BY execution_utc DESC, deal_id DESC
                        """,
                        (since_iso,),
                    ).fetchall()
                )
                out["summary"]["journal_rows"] = len(journal_rows)
                out["summary"]["deal_rows"] = len(deal_rows)

                for row in list(journal_rows or []):
                    jid = int(row["id"] or 0)
                    ctx = self._ctrader_context_from_journal_row(row)
                    journal_context[jid] = ctx
                    request_payload = _safe_json_dict(str(row["request_json"] or "{}"))
                    if not request_payload:
                        out["summary"]["journal_missing_request_payload"] += 1
                    for key in list(journal_field_counts.keys()):
                        if _field_present(ctx, key):
                            journal_field_counts[key] += 1
                    missing = []
                    if not _field_present(ctx, "family"):
                        out["summary"]["journal_missing_family"] += 1
                        missing.append("family")
                    if not _field_present(ctx, "reasons"):
                        out["summary"]["journal_missing_reasons"] += 1
                        missing.append("reasons")
                    if not _field_present(ctx, "warnings"):
                        out["summary"]["journal_missing_warnings"] += 1
                        missing.append("warnings")
                    if not _field_present(ctx, "pattern"):
                        out["summary"]["journal_missing_pattern"] += 1
                        missing.append("pattern")
                    if not _field_present(ctx, "session"):
                        out["summary"]["journal_missing_session"] += 1
                        missing.append("session")
                    if not _field_present(ctx, "timeframe"):
                        out["summary"]["journal_missing_timeframe"] += 1
                        missing.append("timeframe")
                    if not _field_present(ctx, "xau_multi_tf_snapshot"):
                        out["summary"]["journal_missing_mtf"] += 1
                        missing.append("xau_multi_tf_snapshot")
                    if missing and len(list(out["examples"]["journal_missing"])) < 8:
                        out["examples"]["journal_missing"].append(
                            {
                                "journal_id": jid,
                                "created_utc": str(row["created_utc"] or ""),
                                "source": str(row["source"] or ""),
                                "symbol": str(row["symbol"] or ""),
                                "missing": missing,
                            }
                        )

                updates: list[tuple[str, int]] = []
                for row in list(deal_rows or []):
                    raw = _safe_json_dict(str(row["raw_json"] or "{}"))
                    deal_id = int(row["deal_id"] or 0)
                    journal_id = int(row["journal_id"] or 0)
                    source = _norm_source(row["source"] or raw.get("source") or "")
                    symbol = _norm_symbol(row["symbol"] or raw.get("symbol") or "")
                    if journal_id <= 0:
                        out["summary"]["journalless_deals"] += 1
                    ctx = journal_context.get(journal_id) or self._ctrader_context_from_journal_row(
                        {},
                        fallback_source=source,
                        fallback_symbol=symbol,
                    )
                    if not source:
                        source = str(ctx.get("source") or "")
                    if not symbol:
                        symbol = str(ctx.get("symbol") or "")

                    for key in list(deal_before_counts.keys()):
                        if _field_present(raw, key):
                            deal_before_counts[key] += 1

                    enriched = dict(raw)
                    desired = {
                        "deal_attribution_version": int(raw.get("deal_attribution_version") or 2),
                        "source": str(raw.get("source") or source or ctx.get("source") or "").strip().lower(),
                        "symbol": str(raw.get("symbol") or symbol or ctx.get("symbol") or "").strip().upper(),
                        "family": str(raw.get("family") or ctx.get("family") or "").strip().lower(),
                        "strategy_family": str(raw.get("strategy_family") or ctx.get("strategy_family") or ctx.get("family") or "").strip().lower(),
                        "entry_type": str(raw.get("entry_type") or ctx.get("entry_type") or "").strip().lower(),
                        "session": str(raw.get("session") or ctx.get("session") or "").strip().lower(),
                        "timeframe": str(raw.get("timeframe") or ctx.get("timeframe") or "").strip().lower(),
                        "pattern": str(raw.get("pattern") or ctx.get("pattern") or "").strip(),
                        "strict_alignment": str(raw.get("strict_alignment") or ctx.get("strict_alignment") or "").strip().lower(),
                        "xau_mtf_aligned_side": str(raw.get("xau_mtf_aligned_side") or ctx.get("xau_mtf_aligned_side") or "").strip().lower(),
                        "xau_multi_tf_snapshot": dict(raw.get("xau_multi_tf_snapshot") or ctx.get("xau_multi_tf_snapshot") or {}),
                        "reasons": list(raw.get("reasons") or ctx.get("reasons") or []),
                        "warnings": list(raw.get("warnings") or ctx.get("warnings") or []),
                    }
                    changed_keys: list[str] = []
                    for key, value in desired.items():
                        if key in {"reasons", "warnings"}:
                            value = self._normalized_text_list(value)
                            if value and not self._normalized_text_list(enriched.get(key) or []):
                                enriched[key] = value
                                changed_keys.append(key)
                            continue
                        if isinstance(value, dict):
                            if value and not bool(enriched.get(key)):
                                enriched[key] = value
                                changed_keys.append(key)
                            continue
                        token = self._present_context_value(value, lower=key in {"source", "family", "strategy_family", "entry_type", "session", "timeframe", "strict_alignment", "xau_mtf_aligned_side"})
                        if token and not _field_present(enriched, key):
                            enriched[key] = token
                            changed_keys.append(key)

                    before_missing = [
                        key for key in deal_before_counts.keys()
                        if not _field_present(raw, key)
                    ]
                    if before_missing:
                        out["summary"]["deal_rows_missing_context_before"] += 1
                    if changed_keys and repair_on_run:
                        updates.append((json.dumps(enriched, ensure_ascii=False, separators=(",", ":")), deal_id))
                        out["summary"]["deal_rows_repaired"] += 1
                        if len(list(out["examples"]["deal_repaired"])) < 8:
                            out["examples"]["deal_repaired"].append(
                                {
                                    "deal_id": deal_id,
                                    "journal_id": journal_id,
                                    "source": source,
                                    "filled_keys": changed_keys,
                                    "had_close_detail": bool(int(row["has_close_detail"] or 0)),
                                }
                            )
                    for key in list(deal_after_counts.keys()):
                        if _field_present(enriched, key):
                            deal_after_counts[key] += 1
                    after_missing = [
                        key for key in deal_after_counts.keys()
                        if not _field_present(enriched, key)
                    ]
                    if after_missing:
                        out["summary"]["deal_rows_remaining_missing"] += 1
                        if len(list(out["examples"]["deal_remaining"])) < 8:
                            out["examples"]["deal_remaining"].append(
                                {
                                    "deal_id": deal_id,
                                    "journal_id": journal_id,
                                    "source": source,
                                    "remaining_missing": after_missing,
                                    "had_close_detail": bool(int(row["has_close_detail"] or 0)),
                                }
                            )

                if updates:
                    conn.executemany("UPDATE ctrader_deals SET raw_json=? WHERE deal_id=?", updates)

                if _table_columns(conn, "ctrader_positions"):
                    row = conn.execute(
                        """
                        SELECT
                            SUM(CASE WHEN is_open=1 AND COALESCE(stop_loss,0)<=0 THEN 1 ELSE 0 END) AS missing_sl,
                            SUM(CASE WHEN is_open=1 AND COALESCE(take_profit,0)<=0 THEN 1 ELSE 0 END) AS missing_tp
                          FROM ctrader_positions
                        """
                    ).fetchone()
                    out["summary"]["open_positions_missing_sl"] = int((row["missing_sl"] if row is not None else 0) or 0)
                    out["summary"]["open_positions_missing_tp"] = int((row["missing_tp"] if row is not None else 0) or 0)
                if _table_columns(conn, "ctrader_orders"):
                    row = conn.execute(
                        """
                        SELECT
                            SUM(CASE WHEN is_open=1 AND COALESCE(stop_loss,0)<=0 THEN 1 ELSE 0 END) AS missing_sl,
                            SUM(CASE WHEN is_open=1 AND COALESCE(take_profit,0)<=0 THEN 1 ELSE 0 END) AS missing_tp
                          FROM ctrader_orders
                        """
                    ).fetchone()
                    out["summary"]["open_orders_missing_sl"] = int((row["missing_sl"] if row is not None else 0) or 0)
                    out["summary"]["open_orders_missing_tp"] = int((row["missing_tp"] if row is not None else 0) or 0)
                protection_gaps = []
                if int(out["summary"]["open_positions_missing_sl"] or 0) > 0:
                    protection_gaps.append("open_positions_missing_sl")
                if int(out["summary"]["open_positions_missing_tp"] or 0) > 0:
                    protection_gaps.append("open_positions_missing_tp")
                if int(out["summary"]["open_orders_missing_sl"] or 0) > 0:
                    protection_gaps.append("open_orders_missing_sl")
                if int(out["summary"]["open_orders_missing_tp"] or 0) > 0:
                    protection_gaps.append("open_orders_missing_tp")
                out["examples"]["protection_gaps"] = protection_gaps
        except Exception as e:
            out["error"] = f"db_query_error:{e}"
            self._save_report_snapshot("ctrader_data_integrity_report", out)
            return out

        total_journal = max(1, int(out["summary"]["journal_rows"] or 0))
        total_deals = max(1, int(out["summary"]["deal_rows"] or 0))
        out["journal_coverage"] = {
            key: {
                "rows": int(count or 0),
                "ratio": round(float(count or 0) / float(total_journal), 4),
            }
            for key, count in sorted(journal_field_counts.items())
        }
        out["deal_coverage_before"] = {
            key: {
                "rows": int(count or 0),
                "ratio": round(float(count or 0) / float(total_deals), 4),
            }
            for key, count in sorted(deal_before_counts.items())
        }
        out["deal_coverage_after"] = {
            key: {
                "rows": int(count or 0),
                "ratio": round(float(count or 0) / float(total_deals), 4),
            }
            for key, count in sorted(deal_after_counts.items())
        }
        out["ok"] = True
        self._save_report_snapshot("ctrader_data_integrity_report", out)
        return out

    def build_external_model_prior_library_report(self) -> dict:
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "models": [
                {
                    "model": "DeepLOB",
                    "source_url": "https://arxiv.org/abs/1808.03668",
                    "core_prior": "Short-horizon order-book state improves entry timing when spread/depth structure is stable.",
                    "feature_priors": [
                        "spread_expansion",
                        "depth_imbalance",
                        "rejection_after_impulse",
                        "continuation_after_refill",
                    ],
                },
                {
                    "model": "BDLOB",
                    "source_url": "https://arxiv.org/abs/1811.10041",
                    "core_prior": "Bayesian uncertainty should reduce trade frequency and position size when evidence is sparse or unstable.",
                    "feature_priors": [
                        "family_specific_uncertainty",
                        "confidence_calibration",
                        "sample_aware_penalty",
                    ],
                },
                {
                    "model": "FinRL_Podracer",
                    "source_url": "https://arxiv.org/abs/2111.05188",
                    "core_prior": "Keep a fast generational search loop, walk-forward ranking, and staged promotion instead of single-model lock-in.",
                    "feature_priors": [
                        "ensemble_family_search",
                        "walk_forward_ranking",
                        "canary_promote_demote",
                    ],
                },
                {
                    "model": "HLOB",
                    "source_url": "https://arxiv.org/abs/2501.09610",
                    "core_prior": "Hierarchical order-book routing improves short-horizon directional decisions by switching experts by local market state.",
                    "feature_priors": [
                        "session_router",
                        "state_gated_entry",
                        "imbalance_bias",
                    ],
                },
                {
                    "model": "EarnHFT",
                    "source_url": "https://arxiv.org/abs/2411.06389",
                    "core_prior": "Separate market-state labeling from trading policy and keep execution policies narrow per regime.",
                    "feature_priors": [
                        "regime_router",
                        "specialized_policy_per_state",
                        "sample_aware_exploration",
                    ],
                },
                {
                    "model": "DeepFolio",
                    "source_url": "https://arxiv.org/abs/2401.03382",
                    "core_prior": "Use multi-step allocation priors and cross-asset context to avoid over-committing weak crypto setups.",
                    "feature_priors": [
                        "cross_asset_context",
                        "portfolio_penalty",
                        "uncertainty_aware_sizing",
                    ],
                },
            ],
            "families": [],
        }
        families = [
            {
                "symbol": "XAUUSD",
                "family": "xau_scalp_tick_depth_filter",
                "prior_models": ["DeepLOB", "BDLOB"],
                "router_bonus": 2.4,
                "transfer_confidence": "high",
                "uncertainty_adjustment": -0.05,
                "feature_alignment": ["spread_expansion", "depth_imbalance", "continuation_after_refill"],
                "notes": "Experimental microstructure family; use only as isolated canary until live capture proves edge.",
            },
            {
                "symbol": "XAUUSD",
                "family": "xau_scalp_pullback_limit",
                "prior_models": ["DeepLOB", "BDLOB"],
                "router_bonus": 1.4,
                "transfer_confidence": "high",
                "uncertainty_adjustment": -0.03,
                "feature_alignment": ["depth_imbalance", "continuation_after_refill"],
                "notes": "Pullback entries benefit from stable spread and supportive depth skew before limit fill.",
            },
            {
                "symbol": "XAUUSD",
                "family": "xau_scalp_breakout_stop",
                "prior_models": ["DeepLOB"],
                "router_bonus": 0.7,
                "transfer_confidence": "medium",
                "uncertainty_adjustment": -0.01,
                "feature_alignment": ["spread_expansion", "rejection_after_impulse"],
                "notes": "Breakout stops need cleaner breakout structure; keep small until replay confirms trigger quality.",
            },
            {
                "symbol": "XAUUSD",
                "family": "xau_scalp_failed_fade_follow_stop",
                "prior_models": ["DeepLOB", "EarnHFT", "BDLOB"],
                "router_bonus": 1.0,
                "transfer_confidence": "medium",
                "uncertainty_adjustment": -0.02,
                "feature_alignment": ["failed_fade_flip", "state_gated_entry", "uncertainty_aware_sizing"],
                "notes": "Follow-stop rescue family should only flip after strong opposite flow confirms a failed fade; keep isolated until reversal rescue rate is proven live.",
            },
            {
                "symbol": "XAUUSD",
                "family": "xau_scalp_microtrend_follow_up",
                "prior_models": ["DeepLOB", "EarnHFT", "BDLOB"],
                "router_bonus": 1.55,
                "transfer_confidence": "high",
                "uncertainty_adjustment": -0.04,
                "feature_alignment": ["state_gated_entry", "specialized_policy_per_state", "continuation_after_refill"],
                "notes": "State-locked microtrend follow-up should only engage when chart-state memory shows continuation or repricing follow-through with live capture confirmation.",
            },
            {
                "symbol": "XAUUSD",
                "family": "xau_scalp_flow_short_sidecar",
                "prior_models": ["DeepLOB", "EarnHFT", "BDLOB"],
                "router_bonus": 1.65,
                "transfer_confidence": "high",
                "uncertainty_adjustment": -0.04,
                "feature_alignment": ["state_gated_entry", "short_continuation", "continuation_after_refill", "uncertainty_aware_sizing"],
                "notes": "Narrow XAU short continuation sidecar for New York flow states; keep canary-only and compare against broad microtrend before promotion.",
            },
            {
                "symbol": "XAUUSD",
                "family": "xau_scheduled_trend",
                "prior_models": ["FinRL_Podracer"],
                "router_bonus": 1.2,
                "transfer_confidence": "high",
                "uncertainty_adjustment": -0.04,
                "feature_alignment": ["walk_forward_ranking", "ensemble_family_search"],
                "notes": "Trend family fits staged promotion and lower uncertainty when 1h context is stable.",
            },
            {
                "symbol": "BTCUSD",
                "family": "btc_weekend_winner",
                "prior_models": ["FinRL_Podracer", "BDLOB"],
                "router_bonus": 0.9,
                "transfer_confidence": "medium",
                "uncertainty_adjustment": -0.02,
                "feature_alignment": ["sample_aware_penalty", "walk_forward_ranking"],
                "notes": "Keep weekend winner narrow and sample-aware until more live evidence accumulates.",
            },
            {
                "symbol": "BTCUSD",
                "family": "btc_weekday_lob_momentum",
                "prior_models": ["HLOB", "EarnHFT", "DeepFolio"],
                "router_bonus": 1.3,
                "transfer_confidence": "high",
                "uncertainty_adjustment": -0.03,
                "feature_alignment": ["session_router", "state_gated_entry", "cross_asset_context"],
                "notes": "Weekday BTC model should stay long-biased, overlap/new-york only, and use regime-gated entry like HLOB/EarnHFT style specialists.",
            },
            {
                "symbol": "ETHUSD",
                "family": "eth_weekend_winner",
                "prior_models": ["FinRL_Podracer", "BDLOB"],
                "router_bonus": 0.4,
                "transfer_confidence": "low",
                "uncertainty_adjustment": 0.02,
                "feature_alignment": ["sample_aware_penalty"],
                "notes": "ETH still sparse; priors should increase caution rather than encourage promotion.",
            },
            {
                "symbol": "ETHUSD",
                "family": "eth_weekday_overlap_probe",
                "prior_models": ["EarnHFT", "DeepFolio"],
                "router_bonus": 0.35,
                "transfer_confidence": "low",
                "uncertainty_adjustment": 0.05,
                "feature_alignment": ["regime_router", "sample_aware_exploration", "uncertainty_aware_sizing"],
                "notes": "ETH weekday probe should collect data only in overlap with strong regime and very small size until local edge is proven.",
            },
        ]
        out["families"] = families
        out["summary"] = {
            "models": len(out["models"]),
            "families": len(families),
            "high_transfer_families": len([row for row in families if str(row.get("transfer_confidence") or "") == "high"]),
        }
        self._save_report_snapshot("external_model_prior_library_report", out)
        return out

    def _family_specific_uncertainty_score(self, *, family: str, base_uncertainty: float, replay_row: dict | None = None, prior_row: dict | None = None) -> float:
        score = float(base_uncertainty)
        if not bool(getattr(config, "FAMILY_UNCERTAINTY_ROUTER_ENABLED", True)):
            return _clamp(score, 0.0, 1.0)
        replay = dict(replay_row or {})
        prior = dict(prior_row or {})
        capture_min = max(1, int(getattr(config, "FAMILY_UNCERTAINTY_CAPTURE_MIN_SAMPLE", 10) or 10))
        capture_count = int(replay.get("orders_with_capture", 0) or 0)
        wr = float(replay.get("win_rate", 0.0) or 0.0)
        pnl = float(replay.get("pnl_usd", 0.0) or 0.0)
        pos_bonus = float(getattr(config, "FAMILY_UNCERTAINTY_POSITIVE_BONUS", 0.04) or 0.04)
        neg_penalty = float(getattr(config, "FAMILY_UNCERTAINTY_NEGATIVE_PENALTY", 0.08) or 0.08)
        prior_bonus = float(getattr(config, "FAMILY_UNCERTAINTY_PRIOR_BONUS", 0.03) or 0.03)
        if capture_count >= capture_min:
            score -= pos_bonus * 0.5
        else:
            score += pos_bonus * 0.5
        if capture_count >= capture_min and pnl > 0.0 and wr >= 0.55:
            score -= pos_bonus
        elif capture_count >= max(4, int(capture_min / 2)) and pnl <= 0.0:
            score += neg_penalty
        transfer = str(prior.get("transfer_confidence") or "").strip().lower()
        if transfer == "high":
            score -= prior_bonus
        elif transfer == "low":
            score += prior_bonus
        if family == "xau_scalp_breakout_stop":
            score += 0.04
        if family == "xau_scalp_tick_depth_filter" and capture_count < capture_min:
            score += 0.06
        if family == "btc_weekday_lob_momentum":
            score -= 0.02
        if family == "eth_weekday_overlap_probe":
            score += 0.08
        return round(_clamp(score, 0.0, 1.0), 4)

    def _build_walk_forward_family_report(self, *, days_windows: list[int]) -> dict:
        windows = sorted(list(dict.fromkeys([max(1, int(v)) for v in list(days_windows or []) if int(v) > 0]))) or [3, 7, 14]
        now = _utc_now()
        max_days = max(windows)
        since_iso = _iso(now - timedelta(days=max_days))
        thresholds = {int(days): _iso(now - timedelta(days=int(days))) for days in windows}
        weights = self._window_weights(windows)
        scope = self._execution_scope()
        backend_focus = str(scope.get("backend_focus") or "mixed")
        include_mt5 = backend_focus != "ctrader_only"
        purge_hours = max(0, int(getattr(config, "STRATEGY_PROMOTION_PURGE_HOURS", 6) or 6))
        purge_cutoff = _iso(now - timedelta(hours=purge_hours)) if purge_hours > 0 else ""
        family_rows: dict[tuple[str, str], dict] = {}

        def ensure_row(sym: str, family: str) -> dict:
            key = (sym, family)
            row = family_rows.get(key)
            if row is None:
                row = {
                    "symbol": sym,
                    "family": family,
                    "live_total": _new_bucket(),
                    "canary_total": _new_bucket(),
                    "total": _new_bucket(),
                    "purged_total": _new_bucket(),
                    "windows": {
                        int(days): {
                            "live": _new_bucket(),
                            "canary": _new_bucket(),
                            "total": _new_bucket(),
                        }
                        for days in windows
                    },
                    "sources": set(),
                    "backends": set(),
                    "pnls": [],
                    "purged_pnls": [],
                }
                family_rows[key] = row
            return row

        def record_row(ts: str, source: str, symbol: str, pnl: float, outcome: int, *, backend: str) -> None:
            sym = _norm_symbol(symbol)
            src = _norm_source(source)
            family = self._strategy_family_for_source(sym, src)
            if (not sym) or (not src) or (not family) or int(outcome) not in (0, 1):
                return
            row = ensure_row(sym, family)
            is_canary = src.endswith(":canary")
            target = row["canary_total"] if is_canary else row["live_total"]
            _update_bucket(target, pnl, outcome)
            _update_bucket(row["total"], pnl, outcome)
            row["sources"].add(src)
            row["backends"].add(str(backend))
            ts_norm = str(ts or "")
            row["pnls"].append(float(pnl))
            if purge_cutoff and ts_norm <= purge_cutoff:
                _update_bucket(row["purged_total"], pnl, outcome)
                row["purged_pnls"].append(float(pnl))
            for days, threshold in thresholds.items():
                if ts_norm >= threshold:
                    window = row["windows"][int(days)]
                    _update_bucket(window["canary"] if is_canary else window["live"], pnl, outcome)
                    _update_bucket(window["total"], pnl, outcome)

        events: list[dict] = []
        try:
            if include_mt5 and self.mt5_db_path.exists():
                with closing(self._connect_mt5()) as conn:
                    rows = conn.execute(
                        """
                        SELECT created_at, source, signal_symbol, broker_symbol, outcome, pnl
                          FROM mt5_execution_journal
                         WHERE created_at >= ?
                           AND resolved=1
                           AND outcome IN (0, 1)
                        ORDER BY id ASC
                        """,
                        (since_iso,),
                    ).fetchall()
                    for row in list(rows or []):
                        events.append(
                            {
                                "ts": str(row["created_at"] or ""),
                                "source": str(row["source"] or ""),
                                "symbol": str(row["signal_symbol"] or row["broker_symbol"] or ""),
                                "pnl": _safe_float(row["pnl"], 0.0),
                                "outcome": _safe_int(row["outcome"], -1),
                                "backend": "mt5",
                            }
                        )
            if self.ctrader_db_path.exists():
                with closing(self._connect_ctrader()) as conn:
                    rows = conn.execute(
                        """
                        SELECT execution_utc, source, symbol, outcome, pnl_usd
                          FROM ctrader_deals
                         WHERE execution_utc >= ?
                           AND has_close_detail = 1
                           AND journal_id IS NOT NULL
                           AND outcome IN (0, 1)
                        ORDER BY deal_id ASC
                        """,
                        (since_iso,),
                    ).fetchall()
                    for row in list(rows or []):
                        events.append(
                            {
                                "ts": str(row["execution_utc"] or ""),
                                "source": str(row["source"] or ""),
                                "symbol": str(row["symbol"] or ""),
                                "pnl": _safe_float(row["pnl_usd"], 0.0),
                                "outcome": _safe_int(row["outcome"], -1),
                                "backend": "ctrader",
                            }
                        )
        except Exception as e:
            return {"ok": False, "error": f"walk_forward_query_error:{e}", "windows_days": windows, "backend_focus": backend_focus, "rows": []}

        events.sort(key=lambda item: str(item.get("ts") or ""))
        for event in list(events or []):
            record_row(
                str(event.get("ts") or ""),
                str(event.get("source") or ""),
                str(event.get("symbol") or ""),
                _safe_float(event.get("pnl"), 0.0),
                _safe_int(event.get("outcome"), -1),
                backend=str(event.get("backend") or ""),
            )

        rows_out: list[dict] = []
        for key in sorted(family_rows.keys()):
            row = family_rows[key]
            windows_out = []
            weighted_score = 0.0
            total_weight = 0.0
            for days in windows:
                window = row["windows"][int(days)]
                live = _finalize_bucket(window["live"])
                canary = _finalize_bucket(window["canary"])
                total = _finalize_bucket(window["total"])
                live_score = self._score_bucket_for_ranking(window["live"])
                canary_score = self._score_bucket_for_ranking(window["canary"])
                total_score = self._score_bucket_for_ranking(window["total"])
                observed_score = live_score
                if observed_score is None:
                    observed_score = total_score
                if observed_score is None:
                    observed_score = canary_score
                weight = float(weights.get(int(days), 0.0) or 0.0)
                if observed_score is not None and weight > 0.0:
                    weighted_score += float(observed_score) * weight
                    total_weight += weight
                windows_out.append(
                    {
                        "days": int(days),
                        "weight": round(weight, 4),
                        "live": live,
                        "canary": canary,
                        "total": total,
                        "score_live": None if live_score is None else round(float(live_score), 4),
                        "score_canary": None if canary_score is None else round(float(canary_score), 4),
                        "score_total": None if total_score is None else round(float(total_score), 4),
                        "observed_score": None if observed_score is None else round(float(observed_score), 4),
                    }
                )
            purged_score = self._score_bucket_for_ranking(row.get("purged_total") or _new_bucket())
            dd = _max_drawdown_usd(list(row.get("pnls") or []))
            dsr = _deflated_sharpe_proxy(list(row.get("pnls") or []), trials=max(1, len(family_rows)))
            rows_out.append(
                {
                    "symbol": row["symbol"],
                    "family": row["family"],
                    "sources": sorted(list(row["sources"] or set())),
                    "backends": sorted(list(row["backends"] or set())),
                    "live_total": _finalize_bucket(row["live_total"]),
                    "canary_total": _finalize_bucket(row["canary_total"]),
                    "total": _finalize_bucket(row["total"]),
                    "purged_total": _finalize_bucket(row["purged_total"]),
                    "windows": windows_out,
                    "observed_walk_forward_score": round((weighted_score / total_weight), 4) if total_weight > 0.0 else None,
                    "purged_observed_walk_forward_score": None if purged_score is None else round(float(purged_score), 4),
                    "max_drawdown_usd": dd,
                    "deflated_sharpe_proxy": dsr,
                }
            )
        rows_out.sort(
            key=lambda item: (
                float(item.get("observed_walk_forward_score") or -9999.0),
                int(((item.get("total") or {}).get("resolved", 0) or 0)),
                float(((item.get("total") or {}).get("pnl_usd", 0.0) or 0.0)),
            ),
            reverse=True,
        )
        return {"ok": True, "generated_at": _iso(now), "windows_days": windows, "backend_focus": backend_focus, "rows": rows_out}

    @staticmethod
    def _top_rows(rows: list[dict], *, limit: int = 3) -> list[dict]:
        out: list[dict] = []
        for row in list(rows or []):
            if len(out) >= max(0, int(limit)):
                break
            out.append(dict(row))
        return out

    def _build_strategy_specs_for_symbol(
        self,
        *,
        symbol: str,
        winner_row: dict,
        crypto_row: dict,
        canary_row: dict,
        audit_recommendations: list[dict],
    ) -> list[dict]:
        sym = _norm_symbol(symbol)
        specs: list[dict] = []
        winner_mode = str((winner_row or {}).get("recommended_live_mode") or "").strip().lower()
        entry_bias = str((winner_row or {}).get("entry_bias") or "").strip().lower()
        top_model_sessions = list((winner_row or {}).get("top_model_sessions") or [])
        top_model_bands = list((winner_row or {}).get("top_model_conf_bands") or [])
        top_weekend_sessions = list((crypto_row or {}).get("top_weekend_sessions") or [])
        top_weekday_sessions = list((crypto_row or {}).get("top_weekday_sessions") or [])
        top_weekend_bands = list((crypto_row or {}).get("top_weekend_conf_bands") or [])
        canary_total = dict((canary_row or {}).get("canary_total") or {})
        baseline = dict((canary_row or {}).get("control_cross_backend_total") or (canary_row or {}).get("control_total") or {})
        audit_row = next((dict(x) for x in list(audit_recommendations or []) if _norm_symbol(x.get("symbol")) == sym), {})
        common = {
            "symbol": sym,
            "generator_version": 1,
            "winner_mode": winner_mode,
            "entry_bias": entry_bias,
            "baseline": baseline,
            "canary_total": canary_total,
            "audit_signal": audit_row,
        }

        if sym == "XAUUSD":
            sessions = [str(x.get("session") or "").strip() for x in top_model_sessions if str(x.get("session") or "").strip()][:2]
            specs.append(
                {
                    **common,
                    "strategy_id": "xau_scheduled_trend_v1",
                    "family": "xau_scheduled_trend",
                    "evidence_family": "xau_scheduled_trend",
                    "priority": 1,
                    "promotion_capable": True,
                    "execution_ready": True,
                    "rationale": "Promote scheduled winner path when XAU scalp family underperforms baseline control.",
                    "selector_inputs": {
                        "top_sessions": sessions,
                        "top_conf_bands": self._top_rows(top_model_bands, limit=2),
                        "winner_mode": winner_mode,
                    },
                    "proposed_overrides": {
                        "MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE": f"{max(78.0, float(getattr(config, 'MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE', 78.0) or 78.0)):.1f}".rstrip("0").rstrip("."),
                        "MT5_XAU_SCHEDULED_LIVE_SESSIONS": self._signature_csv(sessions or list(config.get_mt5_xau_scheduled_live_sessions() or set())),
                    },
                }
            )
            specs.append(
                {
                    **common,
                    "strategy_id": "xau_scalp_microtrend_repair_v1",
                    "family": "xau_scalp_microtrend",
                    "evidence_family": "xau_scalp_microtrend",
                    "priority": 2,
                    "promotion_capable": True,
                    "execution_ready": True,
                    "rationale": "Repair XAU scalp family by tightening confidence and reducing canary volume until live edge stabilizes.",
                    "selector_inputs": {
                        "top_conf_bands": self._top_rows(top_model_bands, limit=3),
                        "canary_resolved": int(canary_total.get("resolved", 0) or 0),
                        "winner_mode": winner_mode,
                    },
                    "proposed_overrides": {
                        "NEURAL_GATE_CANARY_MIN_CONFIDENCE": f"{min(float(getattr(config, 'AUTO_APPLY_XAU_CANARY_CONFIDENCE_MAX', 80.0) or 80.0), float(getattr(config, 'NEURAL_GATE_CANARY_MIN_CONFIDENCE', 72.0) or 72.0) + 1.0):.1f}".rstrip('0').rstrip('.'),
                        "PERSISTENT_CANARY_MT5_VOLUME_MULTIPLIER": f"{max(0.10, min(0.20, float(getattr(config, 'PERSISTENT_CANARY_MT5_VOLUME_MULTIPLIER', 0.20) or 0.20))):.2f}".rstrip('0').rstrip('.'),
                    },
                }
            )
            specs.append(
                {
                    **common,
                    "strategy_id": "xau_scalp_pullback_limit_v1",
                    "family": "xau_scalp_pullback_limit",
                    "evidence_family": "xau_scalp_pullback_limit",
                    "priority": 3,
                    "promotion_capable": True,
                    "execution_ready": True,
                    "rationale": "Deploy XAU pullback limit canary when missed-opportunity evidence suggests direction was right but entry quality was poor.",
                    "selector_inputs": {
                        "audit_action": str(audit_row.get("action") or ""),
                        "audit_reason": str(audit_row.get("reason") or ""),
                    },
                    "proposed_overrides": {
                        "NEURAL_GATE_CANARY_FIXED_ALLOW_LOW": f"{_safe_float(audit_row.get('proposed_canary_allow_low'), getattr(config, 'NEURAL_GATE_CANARY_FIXED_ALLOW_LOW', 0.0) or 0.0):.3f}".rstrip('0').rstrip('.'),
                        "CTRADER_XAU_PRIMARY_FAMILY": "xau_scalp_pullback_limit",
                        "CTRADER_XAU_ACTIVE_FAMILIES": "xau_scalp_pullback_limit,xau_scalp_breakout_stop",
                    },
                }
            )
            specs.append(
                {
                    **common,
                    "strategy_id": "xau_scalp_breakout_stop_v1",
                    "family": "xau_scalp_breakout_stop",
                    "evidence_family": "xau_scalp_breakout_stop",
                    "priority": 4,
                    "promotion_capable": True,
                    "execution_ready": True,
                    "rationale": "Deploy XAU breakout stop canary to catch continuation moves when pullback entry is not reached but momentum extends.",
                    "selector_inputs": {
                        "top_conf_bands": self._top_rows(top_model_bands, limit=2),
                        "audit_action": str(audit_row.get("action") or ""),
                    },
                    "proposed_overrides": {
                        "PERSISTENT_CANARY_STRATEGY_FAMILIES": "xau_scalp_pullback_limit,xau_scalp_breakout_stop",
                        "CTRADER_XAU_ACTIVE_FAMILIES": "xau_scalp_pullback_limit,xau_scalp_breakout_stop",
                    },
                }
            )
            specs.append(
                {
                    **common,
                    "strategy_id": "xau_scalp_tick_depth_filter_v1",
                    "family": "xau_scalp_tick_depth_filter",
                    "evidence_family": "xau_scalp_pullback_limit",
                    "priority": 5,
                    "promotion_capable": False,
                    "execution_ready": True,
                    "experimental": True,
                    "rationale": "Isolated XAU experimental family that reuses base scalp signals only when recent cTrader tick/depth capture matches replay-supported microstructure.",
                    "selector_inputs": {
                        "capture_symbol": "XAUUSD",
                        "lookback_sec": int(getattr(config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240) or 240),
                    },
                    "proposed_overrides": {
                        "PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES": "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop,xau_scalp_microtrend_follow_up,xau_scalp_flow_short_sidecar",
                    },
                }
            )
            specs.append(
                {
                    **common,
                    "strategy_id": "xau_scalp_microtrend_follow_up_v1",
                    "family": "xau_scalp_microtrend_follow_up",
                    "evidence_family": "xau_scalp_microtrend",
                    "priority": 6,
                    "promotion_capable": False,
                    "execution_ready": True,
                    "experimental": True,
                    "rationale": "State-locked XAU follow-up sidecar that engages only when chart-state memory identifies profitable continuation/repricing follow-through for microtrend execution.",
                    "selector_inputs": {
                        "capture_symbol": "XAUUSD",
                        "state_memory_required": True,
                        "follow_up_only": True,
                    },
                    "proposed_overrides": {
                        "PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES": "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop,xau_scalp_microtrend_follow_up,xau_scalp_flow_short_sidecar",
                    },
                }
            )
            specs.append(
                {
                    **common,
                    "strategy_id": "xau_scalp_flow_short_sidecar_v1",
                    "family": "xau_scalp_flow_short_sidecar",
                    "evidence_family": "xau_scalp_microtrend",
                    "priority": 7,
                    "promotion_capable": False,
                    "execution_ready": True,
                    "experimental": True,
                    "rationale": "Narrow XAU short continuation sidecar that captures SCALP_FLOW_FORCE follow-through when chart-state memory marks continuation_drive or repricing_transition as profitable.",
                    "selector_inputs": {
                        "capture_symbol": "XAUUSD",
                        "state_memory_required": True,
                        "flow_pattern_required": "SCALP_FLOW_FORCE",
                        "follow_up_only": True,
                    },
                    "proposed_overrides": {
                        "PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES": "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop,xau_scalp_microtrend_follow_up,xau_scalp_flow_short_sidecar",
                    },
                }
            )

        if sym == "BTCUSD":
            recommended = dict((crypto_row or {}).get("recommended_weekend_profile") or {})
            sessions = list(recommended.get("allowed_sessions") or []) or [str(x.get("session") or "").strip() for x in top_weekend_sessions if str(x.get("session") or "").strip()][:2]
            specs.append(
                {
                    **common,
                    "strategy_id": "btcusd_weekend_winner_v1",
                    "family": "btc_weekend_winner",
                    "evidence_family": "btc_weekend_winner",
                    "priority": 1,
                    "promotion_capable": True,
                    "execution_ready": True,
                    "rationale": "Use BTC weekend winner profile only after it beats live baseline with enough sample.",
                    "selector_inputs": {
                        "top_weekend_sessions": self._top_rows(top_weekend_sessions, limit=3),
                        "top_weekend_conf_bands": self._top_rows(top_weekend_bands, limit=3),
                        "winner_mode": winner_mode,
                    },
                    "proposed_overrides": {
                        "SCALPING_BTC_MIN_CONFIDENCE_WEEKEND": f"{float(recommended.get('min_confidence', getattr(config, 'SCALPING_BTC_MIN_CONFIDENCE_WEEKEND', 75.0) or 75.0)):.1f}".rstrip('0').rstrip('.'),
                        "SCALPING_BTC_ALLOWED_SESSIONS_WEEKEND": self._signature_csv(list(sessions or [])),
                    },
                }
            )
            specs.append(
                {
                    **common,
                    "strategy_id": "btcusd_overlap_momentum_v1",
                    "family": "btc_weekend_overlap_momentum",
                    "evidence_family": "btc_weekend_winner",
                    "priority": 2,
                    "promotion_capable": True,
                    "execution_ready": True,
                    "rationale": "Alternative BTC family that widens into overlap only if walk-forward beats baseline.",
                    "selector_inputs": {
                        "top_weekday_sessions": self._top_rows(top_weekday_sessions, limit=3),
                        "winner_mode": winner_mode,
                    },
                    "proposed_overrides": {
                        "SCALPING_BTC_ALLOWED_SESSIONS_WEEKEND": self._signature_csv(["new_york", "london,new_york,overlap"]),
                        "SCALPING_BTC_MIN_CONFIDENCE_WEEKEND": f"{max(70.0, float(recommended.get('min_confidence', getattr(config, 'SCALPING_BTC_MIN_CONFIDENCE_WEEKEND', 75.0) or 75.0)) - 1.0):.1f}".rstrip('0').rstrip('.'),
                    },
                }
            )
            specs.append(
                {
                    **common,
                    "strategy_id": "btcusd_weekday_lob_momentum_v1",
                    "family": "btc_weekday_lob_momentum",
                    "evidence_family": "btc_weekend_winner",
                    "priority": 3,
                    "promotion_capable": True,
                    "execution_ready": True,
                    "experimental": True,
                    "rationale": "Experimental BTC weekday model copied from crypto-specific HLOB/EarnHFT style priors: long-biased, overlap/new-york only, and regime-gated.",
                    "selector_inputs": {
                        "top_weekday_sessions": self._top_rows(top_weekday_sessions, limit=3),
                        "winner_mode": winner_mode,
                    },
                    "proposed_overrides": {
                        "PERSISTENT_CANARY_STRATEGY_FAMILIES": "xau_scalp_pullback_limit,btc_weekday_lob_momentum",
                        "PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES": "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop,xau_scalp_microtrend_follow_up,xau_scalp_flow_short_sidecar,eth_weekday_overlap_probe",
                        "BTC_WEEKDAY_LOB_CTRADER_RISK_USD": f"{float(getattr(config, 'BTC_WEEKDAY_LOB_NARROW_LIVE_RISK_USD', 1.1) or 1.1):.2f}".rstrip('0').rstrip('.'),
                    },
                }
            )

        if sym == "ETHUSD":
            recommended = dict((crypto_row or {}).get("recommended_weekend_profile") or {})
            sessions = list(recommended.get("allowed_sessions") or []) or [str(x.get("session") or "").strip() for x in top_weekend_sessions if str(x.get("session") or "").strip()][:2]
            specs.append(
                {
                    **common,
                    "strategy_id": "ethusd_weekend_winner_v1",
                    "family": "eth_weekend_winner",
                    "evidence_family": "eth_weekend_winner",
                    "priority": 1,
                    "promotion_capable": True,
                    "execution_ready": True,
                    "rationale": "Keep ETH on narrow weekend winner profile until real-sample edge proves itself.",
                    "selector_inputs": {
                        "top_weekend_sessions": self._top_rows(top_weekend_sessions, limit=3),
                        "top_weekend_conf_bands": self._top_rows(top_weekend_bands, limit=3),
                        "winner_mode": winner_mode,
                    },
                    "proposed_overrides": {
                        "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND": f"{float(recommended.get('min_confidence', getattr(config, 'SCALPING_ETH_MIN_CONFIDENCE_WEEKEND', 76.0) or 76.0)):.1f}".rstrip('0').rstrip('.'),
                        "SCALPING_ETH_ALLOWED_SESSIONS_WEEKEND": self._signature_csv(list(sessions or [])),
                    },
                }
            )
            specs.append(
                {
                    **common,
                    "strategy_id": "ethusd_overlap_filter_v1",
                    "family": "eth_weekend_overlap_filter",
                    "evidence_family": "eth_weekend_winner",
                    "priority": 2,
                    "promotion_capable": True,
                    "execution_ready": True,
                    "rationale": "ETH filter variant keeps only overlap-aligned sessions and slightly higher confidence.",
                    "selector_inputs": {
                        "top_weekday_sessions": self._top_rows(top_weekday_sessions, limit=3),
                        "winner_mode": winner_mode,
                    },
                    "proposed_overrides": {
                        "SCALPING_ETH_ALLOWED_SESSIONS_WEEKEND": self._signature_csv(["london,new_york,overlap"]),
                        "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND": f"{min(float(getattr(config, 'AUTO_APPLY_ETH_WEEKEND_CONFIDENCE_MAX', 84.0) or 84.0), float(recommended.get('min_confidence', getattr(config, 'SCALPING_ETH_MIN_CONFIDENCE_WEEKEND', 76.0) or 76.0)) + 1.0):.1f}".rstrip('0').rstrip('.'),
                    },
                }
            )
            specs.append(
                {
                    **common,
                    "strategy_id": "ethusd_weekday_overlap_probe_v1",
                    "family": "eth_weekday_overlap_probe",
                    "evidence_family": "eth_weekend_winner",
                    "priority": 3,
                    "promotion_capable": False,
                    "execution_ready": True,
                    "experimental": True,
                    "rationale": "Experimental ETH weekday probe inspired by EarnHFT-style regime routing; overlap only and tiny size to collect weekday evidence safely.",
                    "selector_inputs": {
                        "top_weekday_sessions": self._top_rows(top_weekday_sessions, limit=3),
                        "winner_mode": winner_mode,
                    },
                    "proposed_overrides": {
                        "PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES": "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop,xau_scalp_microtrend_follow_up,xau_scalp_flow_short_sidecar,btc_weekday_lob_momentum,eth_weekday_overlap_probe",
                    },
                }
            )
        return specs

    def _evaluate_strategy_promotion_gate(self, spec: dict, ranking_row: dict | None, baseline: dict, calibration_row: dict | None = None) -> dict:
        min_sample = max(2, int(getattr(config, "STRATEGY_PROMOTION_MIN_SAMPLE", 8) or 8))
        wr_edge = float(getattr(config, "STRATEGY_PROMOTION_MIN_WIN_RATE_EDGE", 0.02) or 0.02)
        pnl_edge = float(getattr(config, "STRATEGY_PROMOTION_MIN_PNL_EDGE_USD", 0.0) or 0.0)
        score_edge = float(getattr(config, "STRATEGY_PROMOTION_MIN_SCORE_EDGE", 0.5) or 0.5)
        require_positive = bool(getattr(config, "STRATEGY_PROMOTION_REQUIRE_POSITIVE_SCORE", True))
        min_dsr = float(getattr(config, "STRATEGY_PROMOTION_MIN_DSR", 0.0) or 0.0)
        max_dd = max(0.0, float(getattr(config, "STRATEGY_PROMOTION_MAX_DD_USD", 35.0) or 35.0))
        staged_max_uncertainty = min(
            1.0,
            max(0.0, float(getattr(config, "STRATEGY_PROMOTION_STAGED_MAX_UNCERTAINTY", 0.60) or 0.60)),
        )
        ranking = dict(ranking_row or {})
        calibration = dict(calibration_row or {})
        observed = dict(ranking.get("total") or {})
        if int(observed.get("resolved", 0) or 0) <= 0:
            observed = dict(ranking.get("canary_total") or {})
        baseline_stat = _finalize_bucket(baseline or _new_bucket())
        candidate_resolved = int(observed.get("resolved", 0) or 0)
        candidate_wr = float(observed.get("win_rate", 0.0) or 0.0)
        candidate_pnl = float(observed.get("pnl_usd", 0.0) or 0.0)
        baseline_wr = float(baseline_stat.get("win_rate", 0.0) or 0.0)
        baseline_pnl = float(baseline_stat.get("pnl_usd", 0.0) or 0.0)
        candidate_score = ranking.get("observed_walk_forward_score")
        purged_score = ranking.get("purged_observed_walk_forward_score")
        baseline_score = self._score_bucket_for_ranking(baseline_stat)
        candidate_dsr = calibration.get("deflated_sharpe_proxy", ranking.get("deflated_sharpe_proxy"))
        candidate_dd = calibration.get("max_drawdown_usd", ranking.get("max_drawdown_usd"))
        calibrated_wr = calibration.get("calibrated_win_rate")
        uncertainty_score = _safe_float(calibration.get("uncertainty_score"), 1.0)
        staged_canary_primary = False
        if purged_score is None:
            family = str(spec.get("family") or "")
            proposed_overrides = dict(spec.get("proposed_overrides") or {})
            staged_canary_primary = bool(
                self._execution_scope().get("backend_focus") == "ctrader_only"
                and family == "xau_scalp_pullback_limit"
                and "CTRADER_XAU_PRIMARY_FAMILY" in proposed_overrides
                and candidate_resolved >= max(12, min_sample * 2)
                and candidate_wr >= baseline_wr + wr_edge
                and candidate_pnl > 0.0
                and candidate_score is not None
                and float(candidate_score) > 0.0
                and candidate_dsr is not None
                and float(candidate_dsr) >= float(min_dsr)
                and candidate_dd is not None
                and float(candidate_dd) <= float(max_dd)
                and calibrated_wr is not None
                and float(calibrated_wr) >= baseline_wr + wr_edge
                and float(uncertainty_score) <= staged_max_uncertainty
            )
        blockers: list[str] = []
        if not bool(spec.get("promotion_capable", False)):
            blockers.append("promotion_capable=false")
        if not bool(spec.get("execution_ready", False)):
            blockers.append("execution_ready=false")
        if candidate_resolved < min_sample:
            blockers.append(f"min_sample:{candidate_resolved}<{min_sample}")
        if int(baseline_stat.get("resolved", 0) or 0) < min_sample:
            blockers.append(f"baseline_sample:{int(baseline_stat.get('resolved', 0) or 0)}<{min_sample}")
        if candidate_wr < baseline_wr + wr_edge:
            blockers.append(f"win_rate_edge:{round(candidate_wr,4)}<{round(baseline_wr + wr_edge,4)}")
        if candidate_pnl < baseline_pnl + pnl_edge:
            blockers.append(f"pnl_edge:{round(candidate_pnl,4)}<{round(baseline_pnl + pnl_edge,4)}")
        if candidate_score is None or baseline_score is None or float(candidate_score) < float(baseline_score) + float(score_edge):
            blockers.append(
                f"score_edge:{'none' if candidate_score is None else round(float(candidate_score),4)}<"
                f"{'none' if baseline_score is None else round(float(baseline_score) + float(score_edge),4)}"
            )
        if (not staged_canary_primary) and (purged_score is None or float(purged_score) <= 0.0):
            blockers.append(f"purged_score:{purged_score}")
        if candidate_dsr is None or float(candidate_dsr) < float(min_dsr):
            blockers.append(f"dsr:{candidate_dsr}<{round(float(min_dsr),4)}")
        if candidate_dd is not None and float(candidate_dd) > float(max_dd):
            blockers.append(f"max_dd:{round(float(candidate_dd),4)}>{round(float(max_dd),4)}")
        if calibrated_wr is not None and float(calibrated_wr) < baseline_wr + wr_edge:
            blockers.append(f"calibrated_wr_edge:{round(float(calibrated_wr),4)}<{round(baseline_wr + wr_edge,4)}")
        if require_positive and (candidate_score is None or float(candidate_score) <= 0.0):
            blockers.append(f"positive_score:{candidate_score}")
        return {
            "eligible": len(blockers) == 0,
            "candidate_resolved": candidate_resolved,
            "candidate_win_rate": round(candidate_wr, 4),
            "candidate_pnl_usd": round(candidate_pnl, 4),
            "baseline_resolved": int(baseline_stat.get("resolved", 0) or 0),
            "baseline_win_rate": round(baseline_wr, 4),
            "baseline_pnl_usd": round(baseline_pnl, 4),
            "candidate_score": None if candidate_score is None else round(float(candidate_score), 4),
            "purged_candidate_score": None if purged_score is None else round(float(purged_score), 4),
            "baseline_score": None if baseline_score is None else round(float(baseline_score), 4),
            "candidate_dsr": None if candidate_dsr is None else round(float(candidate_dsr), 4),
            "candidate_max_drawdown_usd": None if candidate_dd is None else round(float(candidate_dd), 4),
            "calibrated_win_rate": None if calibrated_wr is None else round(float(calibrated_wr), 4),
            "staged_canary_primary": bool(staged_canary_primary),
            "blockers": blockers,
        }

    def _select_meta_policy(self, *, candidates: list[dict]) -> dict:
        symbols: dict[str, list[dict]] = {}
        for row in list(candidates or []):
            symbols.setdefault(_norm_symbol(row.get("symbol")), []).append(dict(row))
        policies = []
        for sym, rows in sorted(symbols.items()):
            ordered = sorted(
                list(rows or []),
                key=lambda item: (
                    1 if bool(((item.get("promotion_gate") or {}).get("eligible"))) else 0,
                    float(item.get("router_score") or -9999.0),
                    float(item.get("walk_forward_score") or -9999.0),
                    -float(item.get("uncertainty_score") or 9999.0),
                    -int(item.get("priority", 99) or 99),
                ),
                reverse=True,
            )
            primary = dict(ordered[0] if ordered else {})
            backup = dict(ordered[1] if len(ordered) > 1 else {})
            family = str(primary.get("family") or "")
            regime = "sample_collection"
            if family == "xau_scheduled_trend":
                regime = "trend_priority"
            elif family == "xau_scalp_pullback_limit":
                regime = "pullback_priority"
            elif family == "xau_scalp_breakout_stop":
                regime = "breakout_continuation"
            elif family == "xau_scalp_tick_depth_filter":
                regime = "microstructure_filter"
            elif family == "xau_scalp_microtrend_follow_up":
                regime = "state_follow_up"
            elif family == "xau_scalp_flow_short_sidecar":
                regime = "flow_short_follow_up"
            elif family == "xau_scalp_microtrend":
                regime = "microtrend_repair"
            elif family == "btc_weekend_winner":
                regime = "weekend_winner"
            elif family == "btc_weekend_overlap_momentum":
                regime = "overlap_expansion"
            elif family == "btc_weekday_lob_momentum":
                regime = "weekday_lob_router"
            elif family == "eth_weekend_winner":
                regime = "weekend_winner"
            elif family == "eth_weekend_overlap_filter":
                regime = "overlap_filter"
            elif family == "eth_weekday_overlap_probe":
                regime = "weekday_probe"
            policies.append(
                {
                    "symbol": sym,
                    "selected_strategy_id": str(primary.get("strategy_id") or ""),
                    "selected_family": family,
                    "selected_regime": regime,
                    "selected_status": str(primary.get("status") or ""),
                    "promotion_ready": bool(((primary.get("promotion_gate") or {}).get("eligible"))),
                    "router_score": primary.get("router_score"),
                    "uncertainty_score": primary.get("uncertainty_score"),
                    "walk_forward_score": primary.get("walk_forward_score"),
                    "backup_strategy_id": str(backup.get("strategy_id") or ""),
                    "backup_family": str(backup.get("family") or ""),
                    "selector_reason": str(primary.get("rationale") or ""),
                }
            )
        return {"generated_at": _iso(_utc_now()), "symbols": policies}

    def build_strategy_lab_report(
        self,
        *,
        winner_report: Optional[dict] = None,
        crypto_report: Optional[dict] = None,
        canary_report: Optional[dict] = None,
        audit_report: Optional[dict] = None,
        chart_state_memory: Optional[dict] = None,
    ) -> dict:
        winner = dict(winner_report or self._load_json(self._report_path("winner_mission_report")) or {})
        crypto = dict(crypto_report or self._load_json(self._report_path("crypto_weekend_scorecard")) or {})
        canary = dict(canary_report or self._load_json(self._report_path("canary_post_trade_audit_report")) or {})
        audit = dict(audit_report or self._load_json(self._report_path("missed_opportunity_audit_report")) or {})
        generator_enabled = bool(getattr(config, "STRATEGY_GENERATOR_ENABLED", True))
        windows_days = list(config.get_strategy_walk_forward_windows_days() or [3, 7, 14])
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "generator_enabled": generator_enabled,
            "generator_version": 1,
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "search_space": {
                "xau_canary_confidence": {
                    "current": float(getattr(config, "NEURAL_GATE_CANARY_MIN_CONFIDENCE", 72.0) or 72.0),
                    "min": float(getattr(config, "AUTO_APPLY_XAU_CANARY_CONFIDENCE_MIN", 68.0) or 68.0),
                    "max": float(getattr(config, "AUTO_APPLY_XAU_CANARY_CONFIDENCE_MAX", 80.0) or 80.0),
                },
                "btc_weekend_confidence": {
                    "current": float(getattr(config, "SCALPING_BTC_MIN_CONFIDENCE_WEEKEND", 75.0) or 75.0),
                    "min": float(getattr(config, "AUTO_APPLY_BTC_WEEKEND_CONFIDENCE_MIN", 70.0) or 70.0),
                    "max": float(getattr(config, "AUTO_APPLY_BTC_WEEKEND_CONFIDENCE_MAX", 82.0) or 82.0),
                },
                "eth_weekend_confidence": {
                    "current": float(getattr(config, "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND", 76.0) or 76.0),
                    "min": float(getattr(config, "AUTO_APPLY_ETH_WEEKEND_CONFIDENCE_MIN", 72.0) or 72.0),
                    "max": float(getattr(config, "AUTO_APPLY_ETH_WEEKEND_CONFIDENCE_MAX", 84.0) or 84.0),
                },
                "strategy_families": [
                    "xau_scheduled_trend",
                    "xau_scalp_microtrend",
                    "xau_scalp_microtrend_follow_up",
                    "xau_scalp_flow_short_sidecar",
                    "xau_scalp_pullback_limit",
                    "xau_scalp_breakout_stop",
                    "xau_scalp_tick_depth_filter",
                    "btc_weekend_winner",
                    "btc_weekend_overlap_momentum",
                    "btc_weekday_lob_momentum",
                    "eth_weekend_winner",
                    "eth_weekend_overlap_filter",
                    "eth_weekday_overlap_probe",
                ],
                "walk_forward_windows_days": list(windows_days),
            },
            "candidates": [],
            "strategy_specs": [],
            "walk_forward_ranking": [],
            "family_calibration": {},
            "recent_win_memory": {},
            "winner_memory_library": {},
            "chart_state_memory": {},
            "external_model_prior_library": {},
            "xau_tick_depth_filter": {},
            "meta_policy": {"generated_at": _iso(_utc_now()), "symbols": []},
            "promotable_candidates": [],
            "blocked_candidates": [],
            "summary": {"spec_count": 0, "promotable_count": 0, "blocked_count": 0},
            "error": "",
        }
        if not generator_enabled:
            out["summary"]["disabled"] = True
            return out
        canary_symbols = {str(x.get("symbol") or ""): dict(x) for x in list((canary or {}).get("symbols") or [])}
        winner_symbols = {str(x.get("symbol") or ""): dict(x) for x in list((winner or {}).get("symbols") or [])}
        crypto_symbols = {str(x.get("symbol") or ""): dict(x) for x in list((crypto or {}).get("symbols") or [])}
        for rec in list((crypto or {}).get("recommendations") or []):
            sym = str(rec.get("symbol") or "")
            if not sym or sym in crypto_symbols:
                continue
            crypto_symbols[sym] = {
                "symbol": sym,
                "recommended_weekend_profile": {
                    "min_confidence": rec.get("recommended_min_confidence"),
                    "allowed_sessions": list(rec.get("recommended_sessions") or []),
                },
            }
        audit_recommendations = list((audit or {}).get("recommendations") or [])
        candidates: list[dict] = []
        for sym in ("XAUUSD", "BTCUSD", "ETHUSD"):
            candidates.extend(
                self._build_strategy_specs_for_symbol(
                    symbol=sym,
                    winner_row=winner_symbols.get(sym) or {},
                    crypto_row=crypto_symbols.get(sym) or {},
                    canary_row=canary_symbols.get(sym) or {},
                    audit_recommendations=audit_recommendations,
                )
            )

        walk_forward = self._build_walk_forward_family_report(days_windows=windows_days)
        out["walk_forward_ranking"] = list((walk_forward or {}).get("rows") or [])
        calibration_report = self.build_family_calibration_report(
            days=max(
                max(int(v) for v in list(windows_days or [14])),
                int(getattr(config, "FAMILY_CALIBRATION_REPORT_LOOKBACK_DAYS", 21) or 21),
            )
        )
        out["family_calibration"] = calibration_report
        recent_win_memory = self.build_recent_win_cluster_memory_report()
        out["recent_win_memory"] = recent_win_memory
        winner_memory_library = self.build_winner_memory_library_report()
        out["winner_memory_library"] = winner_memory_library
        chart_state_memory = dict(
            chart_state_memory
            or self.build_chart_state_memory_report(
                days=max(
                    max(int(v) for v in list(windows_days or [14])),
                    int(getattr(config, "CHART_STATE_MEMORY_LOOKBACK_DAYS", 21) or 21),
                )
            )
            or {}
        )
        out["chart_state_memory"] = chart_state_memory
        external_priors = self.build_external_model_prior_library_report()
        out["external_model_prior_library"] = external_priors
        xau_tick_depth_filter = self.build_xau_tick_depth_filter_report(
            days=max(1, int(getattr(config, "CTRADER_TICK_DEPTH_REPLAY_LAB_LOOKBACK_DAYS", 7) or 7))
        )
        out["xau_tick_depth_filter"] = xau_tick_depth_filter
        ranking_index = {
            (_norm_symbol(row.get("symbol")), str(row.get("family") or "")): dict(row)
            for row in list((walk_forward or {}).get("rows") or [])
        }
        calibration_index = {
            (_norm_symbol(row.get("symbol")), str(row.get("family") or "")): dict(row)
            for row in list((calibration_report or {}).get("families") or [])
        }
        replay_index = {
            (_norm_symbol(row.get("symbol")), str(row.get("family") or "")): dict(row)
            for row in list((xau_tick_depth_filter or {}).get("families") or [])
        }
        memory_index = {
            (_norm_symbol(row.get("symbol")), str(row.get("family") or "")): dict(row)
            for row in list((recent_win_memory or {}).get("clusters") or [])
            if bool(row.get("memory_eligible"))
        }
        winner_memory_index = {
            (_norm_symbol(row.get("symbol")), str(row.get("family") or "")): dict(row)
            for row in list((winner_memory_library or {}).get("situations") or [])
            if bool(row.get("market_beating"))
        }
        chart_state_index = self._build_chart_state_router_index(chart_state_memory)
        prior_index = {
            (_norm_symbol(row.get("symbol")), str(row.get("family") or "")): dict(row)
            for row in list((external_priors or {}).get("families") or [])
        }
        enriched: list[dict] = []
        promotable: list[dict] = []
        blocked: list[dict] = []
        generator_min_sample = max(1, int(getattr(config, "STRATEGY_GENERATOR_MIN_SAMPLE", 6) or 6))
        uncertainty_enabled = bool(getattr(config, "UNCERTAINTY_ROUTER_ENABLED", True))
        uncertainty_penalty_mult = float(getattr(config, "UNCERTAINTY_ROUTER_PENALTY_MULT", 18.0) or 18.0)
        chart_state_bonus_enabled = bool(getattr(config, "CHART_STATE_ROUTER_BONUS_ENABLED", True))
        chart_state_bonus_cap = float(getattr(config, "CHART_STATE_ROUTER_BONUS_CAP", 4.0) or 4.0)
        chart_state_bonus_mult = float(getattr(config, "CHART_STATE_ROUTER_BONUS_MULT", 0.12) or 0.12)

        for spec in list(candidates or []):
            row = dict(spec or {})
            sym = _norm_symbol(row.get("symbol"))
            evidence_family = str(row.get("evidence_family") or row.get("family") or "")
            family = str(row.get("family") or "")
            ranking_row = ranking_index.get((sym, family)) or ranking_index.get((sym, evidence_family)) or {}
            calibration_row = calibration_index.get((sym, family)) or calibration_index.get((sym, evidence_family)) or {}
            replay_row = replay_index.get((sym, family)) or replay_index.get((sym, evidence_family)) or {}
            prior_row = prior_index.get((sym, family)) or prior_index.get((sym, evidence_family)) or {}
            if sym == "XAUUSD":
                model_bucket = dict(((winner_symbols.get(sym) or {}).get("model") or {}))
            else:
                model_bucket = dict(((crypto_symbols.get(sym) or {}).get("model") or (winner_symbols.get(sym) or {}).get("model") or {}))
            model_score = self._score_bucket_for_ranking(model_bucket, model_bucket=True)
            observed_score = ranking_row.get("observed_walk_forward_score")
            canary_score = self._score_bucket_for_ranking((canary_symbols.get(sym) or {}).get("canary_total") or _new_bucket())
            calibrated_wr = calibration_row.get("calibrated_win_rate")
            calibration_error = _safe_float(calibration_row.get("calibration_error"), 0.0)
            raw_uncertainty = _safe_float(calibration_row.get("uncertainty_score"), 1.0 if uncertainty_enabled else 0.0)
            uncertainty_score = self._family_specific_uncertainty_score(
                family=family,
                base_uncertainty=raw_uncertainty,
                replay_row=replay_row,
                prior_row=prior_row,
            ) if uncertainty_enabled else 0.0
            score_parts = []
            if observed_score is not None:
                score_parts.append((0.60, float(observed_score)))
            if canary_score is not None:
                score_parts.append((0.20, float(canary_score)))
            if model_score is not None:
                score_parts.append((0.20, float(model_score)))
            walk_forward_score = None
            if score_parts:
                denom = sum(weight for weight, _val in score_parts) or 1.0
                walk_forward_score = round(sum(weight * val for weight, val in score_parts) / denom, 4)
            baseline_score = self._score_bucket_for_ranking(row.get("baseline") or _new_bucket())
            calibration_bonus = ((float(calibrated_wr) - 0.50) * 60.0) if calibrated_wr is not None else 0.0
            calibration_penalty = float(calibration_error) * 30.0
            memory_row = memory_index.get((sym, family)) or memory_index.get((sym, evidence_family)) or {}
            memory_bonus_cap = float(getattr(config, "RECENT_WIN_CLUSTER_ROUTER_BONUS_CAP", 6.0) or 6.0)
            memory_bonus_mult = float(getattr(config, "RECENT_WIN_CLUSTER_ROUTER_BONUS_MULT", 1.0) or 1.0)
            memory_bonus = min(memory_bonus_cap, max(0.0, _safe_float(memory_row.get("memory_score"), 0.0) * memory_bonus_mult)) if memory_row else 0.0
            prior_bonus = 0.0
            if bool(getattr(config, "EXTERNAL_MODEL_PRIOR_ROUTER_ENABLED", True)) and prior_row:
                prior_bonus = min(
                    float(getattr(config, "EXTERNAL_MODEL_PRIOR_ROUTER_BONUS_CAP", 3.0) or 3.0),
                    max(0.0, _safe_float(prior_row.get("router_bonus"), 0.0) * float(getattr(config, "EXTERNAL_MODEL_PRIOR_ROUTER_MULT", 1.0) or 1.0)),
                )
            chart_state_row = (
                chart_state_index.get((sym, family))
                or chart_state_index.get((sym, evidence_family))
                or {}
            )
            chart_state_bonus = 0.0
            if chart_state_bonus_enabled and chart_state_row:
                chart_state_bonus = min(
                    chart_state_bonus_cap,
                    max(0.0, _safe_float(chart_state_row.get("state_score"), 0.0) * chart_state_bonus_mult),
                )
            router_score = None
            if walk_forward_score is not None:
                router_score = float(walk_forward_score) + float(calibration_bonus) - float(calibration_penalty)
                router_score += float(memory_bonus)
                router_score += float(prior_bonus)
                router_score += float(chart_state_bonus)
                if uncertainty_enabled:
                    router_score -= float(uncertainty_score) * float(uncertainty_penalty_mult)
                router_score = round(router_score, 4)
            row["family_evidence"] = {
                "observed_total": dict(ranking_row.get("total") or {}),
                "observed_live": dict(ranking_row.get("live_total") or {}),
                "observed_canary": dict(ranking_row.get("canary_total") or {}),
                "purged_total": dict(ranking_row.get("purged_total") or {}),
                "windows": list(ranking_row.get("windows") or []),
                "sources": list(ranking_row.get("sources") or []),
                "backends": list(ranking_row.get("backends") or []),
            }
            row["calibration"] = {
                "calibrated_win_rate": calibrated_wr,
                "calibration_error": calibration_row.get("calibration_error"),
                "brier_score": calibration_row.get("brier_score"),
                "deflated_sharpe_proxy": calibration_row.get("deflated_sharpe_proxy", ranking_row.get("deflated_sharpe_proxy")),
                "max_drawdown_usd": calibration_row.get("max_drawdown_usd", ranking_row.get("max_drawdown_usd")),
                "uncertainty_score": uncertainty_score,
                "raw_uncertainty_score": raw_uncertainty,
            }
            row["recent_win_memory"] = dict(memory_row or {})
            row["recent_win_memory_bonus"] = round(float(memory_bonus), 4)
            row["winner_memory_library"] = dict(winner_memory_index.get((sym, family)) or winner_memory_index.get((sym, evidence_family)) or {})
            row["chart_state_router"] = dict(chart_state_row or {})
            row["chart_state_router_bonus"] = round(float(chart_state_bonus), 4)
            row["external_model_prior"] = dict(prior_row or {})
            row["external_model_prior_bonus"] = round(float(prior_bonus), 4)
            row["xau_tick_depth_filter"] = dict(replay_row or {})
            calibration_for_gate = dict(calibration_row or {})
            calibration_for_gate["uncertainty_score"] = uncertainty_score
            promotion_gate = self._evaluate_strategy_promotion_gate(row, ranking_row, row.get("baseline") or {}, calibration_row=calibration_for_gate)
            status = "candidate"
            if int(((ranking_row.get("total") or {}).get("resolved", 0) or 0)) < generator_min_sample:
                status = "sample_collection"
            if bool(row.get("experimental")):
                status = "experimental"
            if bool(promotion_gate.get("eligible")):
                status = "promotable"
            elif list(promotion_gate.get("blockers") or []):
                status = "blocked"
            row["walk_forward_score"] = walk_forward_score
            row["router_score"] = router_score
            row["uncertainty_score"] = round(float(uncertainty_score), 4)
            row["observed_walk_forward_score"] = observed_score
            row["model_score"] = None if model_score is None else round(float(model_score), 4)
            row["canary_score"] = None if canary_score is None else round(float(canary_score), 4)
            row["baseline_score"] = None if baseline_score is None else round(float(baseline_score), 4)
            row["promotion_gate"] = promotion_gate
            row["status"] = status
            enriched.append(row)
            if status == "promotable":
                promotable.append(dict(row))
            if status == "blocked":
                blocked.append(dict(row))

        enriched.sort(
            key=lambda item: (
                1 if str(item.get("status") or "") == "promotable" else 0,
                float(item.get("router_score") or -9999.0),
                float(item.get("walk_forward_score") or -9999.0),
                -int(item.get("priority", 99) or 99),
            ),
            reverse=True,
        )
        out["candidates"] = enriched
        out["strategy_specs"] = enriched
        out["promotable_candidates"] = promotable
        out["blocked_candidates"] = blocked
        out["meta_policy"] = self._select_meta_policy(candidates=enriched)
        out["summary"] = {
            "spec_count": len(enriched),
            "promotable_count": len(promotable),
            "blocked_count": len(blocked),
            "families_ranked": len(list((walk_forward or {}).get("rows") or [])),
            "calibrated_families": len(list((calibration_report or {}).get("families") or [])),
            "winner_memory_situations": len(list((winner_memory_library or {}).get("situations") or [])),
            "winner_memory_market_beating": int(((winner_memory_library or {}).get("summary") or {}).get("market_beating", 0) or 0),
            "chart_state_memory_states": len(list((chart_state_memory or {}).get("states") or [])),
            "chart_state_follow_up_candidates": int(((chart_state_memory or {}).get("summary") or {}).get("follow_up_candidates", 0) or 0),
        }
        self._save_report_snapshot("strategy_lab_report", out)
        return out

    def build_mission_progress_report(
        self,
        *,
        winner_report: Optional[dict] = None,
        crypto_report: Optional[dict] = None,
        audit_report: Optional[dict] = None,
        canary_report: Optional[dict] = None,
        auto_apply_report: Optional[dict] = None,
        strategy_lab_report: Optional[dict] = None,
    ) -> dict:
        winner = dict(winner_report or self._load_json(self._report_path("winner_mission_report")) or {})
        crypto = dict(crypto_report or self._load_json(self._report_path("crypto_weekend_scorecard")) or {})
        audit = dict(audit_report or self._load_json(self._report_path("missed_opportunity_audit_report")) or {})
        canary = dict(canary_report or self._load_json(self._report_path("canary_post_trade_audit_report")) or {})
        auto_apply = dict(auto_apply_report or self._load_json(self._report_path("auto_apply_live_profile_report")) or {})
        strategy_lab = dict(strategy_lab_report or self._load_json(self._report_path("strategy_lab_report")) or {})
        winner_memory_library = dict((strategy_lab or {}).get("winner_memory_library") or self._load_json(self._report_path("winner_memory_library_report")) or {})
        chart_state_memory = dict((strategy_lab or {}).get("chart_state_memory") or self._load_json(self._report_path("chart_state_memory_report")) or {})
        ct_experiment = dict(self.build_ct_only_experiment_report() or self._load_json(self._report_path("ct_only_experiment_report")) or {})
        state = self._load_state()
        active = dict(state.get("active_bundle") or {})
        out = {
            "ok": True,
            "generated_at": _iso(_utc_now()),
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "targets": {
                "min_sample": int(getattr(config, "AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 6) or 6),
                "target_win_rate": float(getattr(config, "AUTO_APPLY_LIVE_PROFILE_MIN_WIN_RATE", 0.60) or 0.60),
            },
            "summary": {
                "active_bundle_status": str(active.get("status") or "none"),
                "active_bundle_id": str(active.get("id") or ""),
                "canary_closed_total": int(((canary.get("summary") or {}).get("total_canary_closed", 0) or 0)),
                "missed_opportunity_positive_groups": int(((audit.get("summary") or {}).get("missed_positive_groups", 0) or 0)),
                "strategy_candidates": len(list((strategy_lab or {}).get("candidates") or [])),
                "promotable_candidates": len(list((strategy_lab or {}).get("promotable_candidates") or [])),
                "winner_memory_market_beating": int(((winner_memory_library.get("summary") or {}).get("market_beating", 0) or 0)),
                "chart_state_follow_up_candidates": int(((chart_state_memory.get("summary") or {}).get("follow_up_candidates", 0) or 0)),
                "ct_only_experiment_closed_rows": int(((ct_experiment.get("summary") or {}).get("closed_rows", 0) or 0)),
            },
            "symbols": [],
            "dashboard": {
                "meta_policy": dict((strategy_lab or {}).get("meta_policy") or {}),
                "promotion_queue": list((strategy_lab or {}).get("promotable_candidates") or [])[:5],
                "walk_forward_leaderboard": list((strategy_lab or {}).get("walk_forward_ranking") or [])[:8],
                "recent_win_memory": list(((strategy_lab or {}).get("recent_win_memory") or {}).get("clusters") or [])[:5],
                "winner_memory_library": list((winner_memory_library or {}).get("situations") or [])[:5],
                "chart_state_memory": list((chart_state_memory or {}).get("states") or [])[:5],
                "ct_only_experiment": dict(ct_experiment or {}),
            },
            "recent_changes": list(state.get("history") or [])[-3:],
            "report_refs": {
                "winner_mission_report": str(self._report_path("winner_mission_report")),
                "crypto_weekend_scorecard": str(self._report_path("crypto_weekend_scorecard")),
                "missed_opportunity_audit_report": str(self._report_path("missed_opportunity_audit_report")),
                "canary_post_trade_audit_report": str(self._report_path("canary_post_trade_audit_report")),
                "auto_apply_live_profile_report": str(self._report_path("auto_apply_live_profile_report")),
                "strategy_lab_report": str(self._report_path("strategy_lab_report")),
                "family_calibration_report": str(self._report_path("family_calibration_report")),
                "recent_win_cluster_memory_report": str(self._report_path("recent_win_cluster_memory_report")),
                "winner_memory_library_report": str(self._report_path("winner_memory_library_report")),
                "chart_state_memory_report": str(self._report_path("chart_state_memory_report")),
                "ctrader_tick_depth_replay_report": str(self._report_path("ctrader_tick_depth_replay_report")),
                "ct_only_experiment_report": str(self._report_path("ct_only_experiment_report")),
            },
        }
        winner_symbols = {str(x.get("symbol") or ""): dict(x) for x in list((winner or {}).get("symbols") or [])}
        crypto_recs = {str(x.get("symbol") or ""): dict(x) for x in list((crypto or {}).get("recommendations") or [])}
        canary_symbols = {str(x.get("symbol") or ""): dict(x) for x in list((canary or {}).get("symbols") or [])}
        strategy_candidates = {}
        for row in list((strategy_lab or {}).get("candidates") or []):
            strategy_candidates.setdefault(str(row.get("symbol") or ""), []).append(dict(row))
        meta_policy_symbols = {
            str(row.get("symbol") or ""): dict(row)
            for row in list(((strategy_lab or {}).get("meta_policy") or {}).get("symbols") or [])
        }
        recent_memory_clusters = list(((strategy_lab or {}).get("recent_win_memory") or {}).get("clusters") or [])
        winner_memory_top_by_symbol = dict((winner_memory_library or {}).get("top_by_symbol") or {})
        chart_state_top_by_symbol = dict((chart_state_memory or {}).get("top_by_symbol") or {})
        experiment_sources = {
            str(item.get("symbol") or ""): dict(item)
            for item in list((ct_experiment or {}).get("sources") or [])
            if str(item.get("symbol") or "").strip()
        }
        xau_td_vs_pb = dict(((ct_experiment or {}).get("comparisons") or {}).get("xau_td_vs_pb_live") or {})

        for sym in ("XAUUSD", "BTCUSD", "ETHUSD"):
            canary_row = canary_symbols.get(sym) or {}
            canary_total = dict(canary_row.get("canary_total") or {})
            control_total = dict(canary_row.get("control_cross_backend_total") or canary_row.get("control_total") or {})
            wr = float(canary_total.get("win_rate", 0.0) or 0.0)
            resolved = int(canary_total.get("resolved", 0) or 0)
            target_wr = float(out["targets"]["target_win_rate"])
            leaderboard = sorted(
                list(strategy_candidates.get(sym) or []),
                key=lambda item: (
                    1 if bool(((item.get("promotion_gate") or {}).get("eligible"))) else 0,
                    float(item.get("walk_forward_score") or -9999.0),
                    -int(item.get("priority", 99) or 99),
                ),
                reverse=True,
            )
            asian_long_memory = next(
                (
                    dict(item)
                    for item in recent_memory_clusters
                    if _norm_symbol(item.get("symbol")) == sym
                    and str(item.get("session") or "") == "asian"
                    and str(item.get("direction") or "") == "long"
                    and bool(item.get("memory_eligible"))
                ),
                {},
            )
            out["symbols"].append(
                {
                    "symbol": sym,
                    "winner_mode": str((winner_symbols.get(sym) or {}).get("recommended_live_mode") or ""),
                    "canary_total": canary_total,
                    "control_total": control_total,
                    "wr_gap_to_target": round(wr - target_wr, 4) if resolved > 0 else None,
                    "sample_gap_to_target": max(0, int(out["targets"]["min_sample"]) - resolved),
                    "crypto_profile": crypto_recs.get(sym) or {},
                    "latest_auto_apply_status": str((auto_apply or {}).get("status") or ""),
                    "candidate_strategies": list(strategy_candidates.get(sym) or []),
                    "leaderboard": leaderboard[:3],
                    "selected_meta_policy": meta_policy_symbols.get(sym) or {},
                    "promotion_ready": bool(((meta_policy_symbols.get(sym) or {}).get("promotion_ready"))),
                    "selected_family": str((meta_policy_symbols.get(sym) or {}).get("selected_family") or ""),
                    "selected_regime": str((meta_policy_symbols.get(sym) or {}).get("selected_regime") or ""),
                    "recent_win_memory": dict((((strategy_lab or {}).get("recent_win_memory") or {}).get("top_by_symbol") or {}).get(sym) or {}),
                    "winner_memory_library": dict(winner_memory_top_by_symbol.get(sym) or {}),
                    "chart_state_memory": dict(chart_state_top_by_symbol.get(sym) or {}),
                    "asian_long_memory": asian_long_memory,
                    "ct_only_experiment_source": dict(experiment_sources.get(sym) or {}),
                    "xau_td_vs_pb_live": dict(xau_td_vs_pb) if sym == "XAUUSD" else {},
                }
            )
        self._save_report_snapshot("mission_progress_report", out)
        return out

    def build_xau_direct_lane_report(self, *, hours: Optional[int] = None) -> dict:
        lookback_hours = max(
            1,
            int(
                hours
                or getattr(config, "XAU_DIRECT_LANE_REPORT_LOOKBACK_HOURS", 72)
                or getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_LOOKBACK_HOURS", 24)
                or 72
            ),
        )
        out = {
            "ok": False,
            "generated_at": _iso(_utc_now()),
            "hours": lookback_hours,
            "db_path": str(self.ctrader_db_path),
            "summary": {},
            "sources": {},
            "directions": {},
            "strict_alignment": {},
            "latest": [],
            "error": "",
        }
        if not self.ctrader_db_path.exists():
            out["error"] = "ctrader_db_missing"
            self._save_report_snapshot("xau_direct_lane_report", out)
            return out

        since_iso = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
        tracked_sources = ("scalp_xauusd", "scalp_xauusd:winner")

        def _bucket() -> dict:
            return {
                "sent": 0,
                "filled": 0,
                "resolved": 0,
                "wins": 0,
                "losses": 0,
                "pnl_usd": 0.0,
                "avg_confidence": 0.0,
                "_conf_sum": 0.0,
                "_conf_n": 0,
                "_reasons": {},
            }

        def _record_reason(bucket: dict, reason: str) -> None:
            token = str(reason or "").strip()
            if not token:
                return
            reasons = bucket.setdefault("_reasons", {})
            reasons[token] = int(reasons.get(token, 0) or 0) + 1

        def _finalize(bucket: dict) -> dict:
            out_bucket = {k: v for k, v in dict(bucket or {}).items() if not str(k).startswith("_")}
            sent = max(0, int(out_bucket.get("sent", 0) or 0))
            filled = max(0, int(out_bucket.get("filled", 0) or 0))
            resolved = max(0, int(out_bucket.get("resolved", 0) or 0))
            wins = max(0, int(out_bucket.get("wins", 0) or 0))
            losses = max(0, int(out_bucket.get("losses", 0) or 0))
            conf_n = max(0, int(bucket.get("_conf_n", 0) or 0))
            conf_sum = float(bucket.get("_conf_sum", 0.0) or 0.0)
            out_bucket["pnl_usd"] = round(float(out_bucket.get("pnl_usd", 0.0) or 0.0), 2)
            out_bucket["fill_rate"] = round((float(filled) / float(sent)), 4) if sent > 0 else 0.0
            out_bucket["win_rate"] = round((float(wins) / float(resolved)), 4) if resolved > 0 else 0.0
            out_bucket["avg_confidence"] = round((conf_sum / float(conf_n)), 2) if conf_n > 0 else 0.0
            reasons = sorted(
                list((bucket.get("_reasons") or {}).items()),
                key=lambda item: (-int(item[1] or 0), str(item[0])),
            )
            out_bucket["top_reasons"] = [
                {"reason": str(reason), "count": int(count or 0)}
                for reason, count in reasons[:3]
            ]
            return out_bucket

        def _safe_json(text: str) -> dict:
            try:
                payload = json.loads(text or "{}")
                return payload if isinstance(payload, dict) else {}
            except Exception:
                return {}

        summary_bucket = _bucket()
        source_buckets = {"main": _bucket(), "winner": _bucket()}
        direction_buckets = {"long": _bucket(), "short": _bucket()}
        alignment_buckets = {
            "aligned_bullish": _bucket(),
            "aligned_bearish": _bucket(),
            "mixed": _bucket(),
            "unknown": _bucket(),
        }
        journal_meta: dict[int, dict] = {}

        try:
            with self._connect_ctrader() as conn:
                journal_cols = _table_columns(conn, "execution_journal")
                deal_cols = _table_columns(conn, "ctrader_deals")
                order_col = "order_id" if "order_id" in journal_cols else "NULL AS order_id"
                position_col = "position_id" if "position_id" in journal_cols else "NULL AS position_id"
                message_col = "message" if "message" in journal_cols else "'' AS message"
                deal_direction_col = "direction" if "direction" in deal_cols else "'' AS direction"
                journal_rows = list(
                    conn.execute(
                        f"""
                        SELECT id, created_utc, source, direction, confidence, status, {message_col}, {order_col},
                               {position_col}, request_json, execution_meta_json
                          FROM execution_journal
                         WHERE created_utc >= ?
                           AND UPPER(COALESCE(symbol,''))='XAUUSD'
                           AND LOWER(COALESCE(source,'')) IN (?, ?)
                         ORDER BY created_utc DESC, id DESC
                        """,
                        (since_iso, tracked_sources[0], tracked_sources[1]),
                    ).fetchall()
                )
                deal_rows = list(
                    conn.execute(
                        f"""
                        SELECT journal_id, source, {deal_direction_col}, pnl_usd, outcome
                          FROM ctrader_deals
                         WHERE execution_utc >= ?
                           AND has_close_detail=1
                           AND UPPER(COALESCE(symbol,''))='XAUUSD'
                           AND LOWER(COALESCE(source,'')) IN (?, ?)
                           AND journal_id IS NOT NULL
                         ORDER BY execution_utc DESC, deal_id DESC
                        """,
                        (since_iso, tracked_sources[0], tracked_sources[1]),
                    ).fetchall()
                )
        except Exception as e:
            out["error"] = f"db_query_error:{e}"
            self._save_report_snapshot("xau_direct_lane_report", out)
            return out

        for row in journal_rows:
            source_token = _norm_source(row["source"])
            lane_key = "winner" if source_token.endswith(":winner") else "main"
            direction = str(row["direction"] or "").strip().lower() or "unknown"
            bucket_targets = [summary_bucket, source_buckets.setdefault(lane_key, _bucket())]
            if direction in direction_buckets:
                bucket_targets.append(direction_buckets[direction])
            try:
                conf = float(row["confidence"] or 0.0)
            except Exception:
                conf = 0.0
            request_payload = _safe_json(str(row["request_json"] or "{}"))
            raw = dict(request_payload.get("raw_scores") or {})
            mtf = dict(raw.get("xau_multi_tf_snapshot") or {})
            strict_alignment = str(mtf.get("strict_alignment") or mtf.get("alignment") or "unknown").strip().lower() or "unknown"
            if strict_alignment not in alignment_buckets:
                strict_alignment = "mixed" if strict_alignment else "unknown"
            bucket_targets.append(alignment_buckets[strict_alignment])
            for bucket in bucket_targets:
                bucket["sent"] += 1
                bucket["_conf_sum"] += conf
                bucket["_conf_n"] += 1
            status = str(row["status"] or "").strip().lower()
            filled = bool(
                int(row["order_id"] or 0) > 0
                or int(row["position_id"] or 0) > 0
                or status in {"accepted", "filled", "reconciled_open", "closed"}
            )
            if filled:
                for bucket in bucket_targets:
                    bucket["filled"] += 1
            elif status not in {"", "accepted"}:
                for bucket in bucket_targets:
                    _record_reason(bucket, str(row["message"] or status))
            journal_meta[int(row["id"] or 0)] = {
                "lane_key": lane_key,
                "direction": direction,
                "strict_alignment": strict_alignment,
                "created_utc": str(row["created_utc"] or ""),
                "confidence": round(conf, 2),
                "source": str(row["source"] or ""),
                "status": status,
                "message": str(row["message"] or ""),
            }

        for row in deal_rows:
            journal_id = int(row["journal_id"] or 0)
            meta = dict(journal_meta.get(journal_id) or {})
            lane_key = str(meta.get("lane_key") or ("winner" if _norm_source(row["source"]).endswith(":winner") else "main"))
            direction = str(meta.get("direction") or row["direction"] or "").strip().lower()
            strict_alignment = str(meta.get("strict_alignment") or "unknown")
            bucket_targets = [summary_bucket, source_buckets.setdefault(lane_key, _bucket())]
            if direction in direction_buckets:
                bucket_targets.append(direction_buckets[direction])
            bucket_targets.append(alignment_buckets.setdefault(strict_alignment, _bucket()))
            pnl = float(row["pnl_usd"] or 0.0)
            outcome = row["outcome"]
            for bucket in bucket_targets:
                bucket["resolved"] += 1
                bucket["pnl_usd"] += pnl
                if outcome == 1 or pnl > 0:
                    bucket["wins"] += 1
                elif outcome == 0 or pnl < 0:
                    bucket["losses"] += 1

        out["summary"] = _finalize(summary_bucket)
        out["sources"] = {key: _finalize(bucket) for key, bucket in source_buckets.items()}
        out["directions"] = {key: _finalize(bucket) for key, bucket in direction_buckets.items()}
        out["strict_alignment"] = {key: _finalize(bucket) for key, bucket in alignment_buckets.items()}
        out["latest"] = list(journal_meta.values())[:5]
        out["ok"] = True
        self._save_report_snapshot("xau_direct_lane_report", out)
        return out

    def auto_tune_xau_direct_lane(
        self,
        *,
        report: Optional[dict] = None,
        persist_env: Optional[bool] = None,
    ) -> dict:
        enabled = bool(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_ENABLED", False))
        persist = bool(getattr(config, "AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV", True)) if persist_env is None else bool(persist_env)
        out = {
            "ok": False,
            "enabled": enabled,
            "persist_env": persist,
            "generated_at": _iso(_utc_now()),
            "state_path": str(self.xau_direct_lane_tune_state_path),
            "status": "disabled" if not enabled else "ready",
            "changes": {},
            "applied_changes": {},
            "summary": {},
            "reasons": [],
            "error": "",
        }
        def _save_tune_state(payload: dict) -> None:
            state = self._load_named_state(self.xau_direct_lane_tune_state_path)
            history = list(state.get("history") or [])
            history.append(
                {
                    "generated_at": str(payload.get("generated_at") or _iso(_utc_now())),
                    "status": str(payload.get("status") or ""),
                    "changes": dict(payload.get("changes") or {}),
                    "summary": dict(payload.get("summary") or {}),
                    "reasons": list(payload.get("reasons") or []),
                }
            )
            state["history"] = history[-20:]
            state["last_action"] = history[-1]
            self._save_named_state(self.xau_direct_lane_tune_state_path, state)
        if not enabled:
            _save_tune_state(out)
            self._save_report_snapshot("xau_direct_lane_auto_tune_report", out)
            return out

        lane_report = dict(
            report
            or self._load_json(self._report_path("xau_direct_lane_report"))
            or self.build_xau_direct_lane_report(
                hours=max(1, int(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_LOOKBACK_HOURS", 24) or 24))
            )
            or {}
        )
        if not bool(lane_report.get("ok")):
            out["status"] = "missing_report"
            out["error"] = str(lane_report.get("error") or "xau_direct_lane_report_unavailable")
            _save_tune_state(out)
            self._save_report_snapshot("xau_direct_lane_auto_tune_report", out)
            return out

        summary = dict(lane_report.get("summary") or {})
        out["summary"] = summary
        resolved = int(summary.get("resolved", 0) or 0)
        pnl_usd = float(summary.get("pnl_usd", 0.0) or 0.0)
        win_rate = float(summary.get("win_rate", 0.0) or 0.0)
        fill_rate = float(summary.get("fill_rate", 0.0) or 0.0)
        min_resolved = max(1, int(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_MIN_RESOLVED", 4) or 4))
        shadow_min = max(1, int(getattr(config, "XAU_SHADOW_BACKTEST_MIN_SAMPLE", 5) or 5))
        use_shadow = False
        shadow_data: dict = {}
        if resolved < min_resolved:
            # Attempt to supplement with shadow backtest outcomes
            if bool(getattr(config, "XAU_SHADOW_BACKTEST_ENABLED", True)):
                try:
                    shadow_data = dict(self._get_shadow_outcomes(lookback_hours=72) or {})
                except Exception:
                    shadow_data = {}
            shadow_resolved = int(shadow_data.get("resolved", 0) or 0)
            if shadow_resolved >= shadow_min:
                use_shadow = True
                resolved = shadow_resolved
                win_rate = float(shadow_data.get("win_rate", 0.0) or 0.0)
                pnl_usd = 0.0  # no real PnL from shadow; use win_rate only
                out["reasons"].append(f"shadow_evidence:resolved={shadow_resolved}")
            else:
                out["ok"] = True
                out["status"] = "insufficient_sample"
                out["reasons"].append(
                    f"resolved<{min_resolved} shadow_resolved={shadow_resolved}<{shadow_min}"
                )
                _save_tune_state(out)
                self._save_report_snapshot("xau_direct_lane_auto_tune_report", out)
                return out

        current_min = float(getattr(config, "MT5_SCALP_XAU_LIVE_CONF_MIN", 72.0) or 72.0)
        current_max = float(getattr(config, "MT5_SCALP_XAU_LIVE_CONF_MAX", 75.0) or 75.0)
        step = max(0.1, float(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_CONF_STEP", 0.5) or 0.5))
        min_floor = float(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_MIN_CONF_FLOOR", 70.0) or 70.0)
        max_ceil = max(min_floor + 1.0, float(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_MAX_CONF_CEIL", 78.0) or 78.0))
        tighten_wr = float(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_WIN_RATE", 0.42) or 0.42)
        tighten_pnl = float(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_PNL_USD", -4.0) or -4.0)
        loosen_wr = float(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_LOOSEN_MIN_WIN_RATE", 0.60) or 0.60)
        loosen_pnl = float(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_LOOSEN_MIN_PNL_USD", 4.0) or 4.0)
        target_fill = float(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_TARGET_FILL_RATE", 0.55) or 0.55)

        new_min = current_min
        new_max = current_max
        if use_shadow:
            # Shadow mode: no real PnL, use win_rate only for tighten/loosen decision
            tighten = bool(win_rate <= tighten_wr)
            loosen = bool(win_rate >= loosen_wr)
        else:
            tighten = bool(win_rate <= tighten_wr or pnl_usd <= tighten_pnl)
            loosen = bool(win_rate >= loosen_wr and pnl_usd >= loosen_pnl and fill_rate < target_fill)
        status_tag = "shadow_" if use_shadow else ""
        if tighten:
            new_min = min(max_ceil - 1.0, max(min_floor, current_min + step))
            new_max = max(new_min + 1.0, min(max_ceil, current_max - step))
            out["status"] = f"{status_tag}tightened"
            out["reasons"].append(f"wr={win_rate:.2f} pnl={pnl_usd:.2f}")
        elif loosen:
            new_min = max(min_floor, current_min - step)
            new_max = min(max_ceil, max(current_max + step, new_min + 1.0))
            out["status"] = f"{status_tag}loosened"
            out["reasons"].append(f"wr={win_rate:.2f} pnl={pnl_usd:.2f} fill={fill_rate:.2f}")
        else:
            out["ok"] = True
            out["status"] = "hold"
            out["reasons"].append("performance_in_band")
            _save_tune_state(out)
            self._save_report_snapshot("xau_direct_lane_auto_tune_report", out)
            return out

        changes = {}
        if round(new_min, 4) != round(current_min, 4):
            changes["MT5_SCALP_XAU_LIVE_CONF_MIN"] = f"{new_min:.2f}"
        if round(new_max, 4) != round(current_max, 4):
            changes["MT5_SCALP_XAU_LIVE_CONF_MAX"] = f"{new_max:.2f}"
        out["changes"] = dict(changes)
        if not changes:
            out["ok"] = True
            out["status"] = "no_effective_change"
            _save_tune_state(out)
            self._save_report_snapshot("xau_direct_lane_auto_tune_report", out)
            return out

        # ── Parameter Trial Sandbox gate ────────────────────────────────────
        # Instead of applying directly, propose a trial POC to be validated by BT first.
        if bool(getattr(config, "XAU_DIRECT_LANE_TRIAL_ENABLED", True)):
            trial_reason = " | ".join(str(r) for r in list(out.get("reasons") or []))
            trial_ids = []
            for param, proposed_value in changes.items():
                try:
                    current_val = str(getattr(config, param, "") or "")
                    tid = self._propose_parameter_trial(
                        param=param,
                        current_value=current_val,
                        proposed_value=str(proposed_value),
                        direction=str(out.get("status") or "").replace("shadow_", ""),
                        reason=trial_reason,
                        source="auto_tune_xau_direct_lane",
                    )
                    trial_ids.append(tid)
                except Exception:
                    pass
            out["ok"] = True
            out["status"] = f"trial_proposed:{','.join(trial_ids)}" if trial_ids else "trial_propose_failed"
            out["trial_ids"] = trial_ids
            _save_tune_state(out)
            self._save_report_snapshot("xau_direct_lane_auto_tune_report", out)
            return out

        # Fallback: apply directly (trial sandbox disabled)
        applied = {}
        for key, value in changes.items():
            self._apply_runtime_value(str(key), str(value))
            applied[str(key)] = self._upsert_env_key(self.env_local_path, str(key), str(value)) if persist else {
                "ok": True,
                "updated": False,
                "reason": "persist_disabled",
            }
        out["ok"] = True
        out["applied_changes"] = applied
        _save_tune_state(out)
        self._save_report_snapshot("xau_direct_lane_auto_tune_report", out)
        return out

    # ── BTC direct lane auto-tune ────────────────────────────────────────────

    def auto_tune_btc_direct_lane(self) -> dict:
        """Self-tune BTC_FSS_MIN_CONFIDENCE and BTC_FLS_MIN_CONFIDENCE from live fills.

        Priority: live fills (ctrader_deals) > shadow journal > insufficient_sample.
        All tuning proposals go through PTS (Parameter Trial Sandbox) — never applied directly.
        """
        enabled = bool(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_ENABLED", True))
        out: dict = {
            "ok": False,
            "enabled": enabled,
            "generated_at": _iso(_utc_now()),
            "families": {},
            "status": "disabled" if not enabled else "ready",
            "error": "",
        }
        if not enabled:
            self._save_report_snapshot("btc_direct_lane_auto_tune_report", out)
            return out
        if not self.ctrader_db_path.exists():
            out["error"] = "ctrader_db_missing"
            self._save_report_snapshot("btc_direct_lane_auto_tune_report", out)
            return out

        lookback_hours = max(24, int(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_LOOKBACK_HOURS", 48) or 48))
        min_resolved = max(1, int(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_MIN_RESOLVED", 3) or 3))
        step = max(0.1, float(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_CONF_STEP", 0.5) or 0.5))
        min_floor = float(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_MIN_CONF_FLOOR", 63.0) or 63.0)
        max_ceil = max(min_floor + 1.0, float(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_MAX_CONF_CEIL", 74.0) or 74.0))
        tighten_wr = float(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_WIN_RATE", 0.42) or 0.42)
        loosen_wr = float(getattr(config, "BTC_DIRECT_LANE_AUTO_TUNE_LOOSEN_MIN_WIN_RATE", 0.62) or 0.62)
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Sources: scalp_btcusd:bfss:canary, scalp_btcusd:bfls:canary, scalp_btcusd:brr:canary
        family_config: list[tuple[str, str, str]] = [
            ("bfss", "BTC_FSS_MIN_CONFIDENCE", "scalp_btcusd:bfss:canary"),
            ("bfls", "BTC_FLS_MIN_CONFIDENCE", "scalp_btcusd:bfls:canary"),
            ("brr", "BTC_RANGE_REPAIR_MIN_CONFIDENCE", "scalp_btcusd:brr:canary"),
        ]

        try:
            with self._connect_ctrader() as conn:
                deal_cols = _table_columns(conn, "ctrader_deals")
                direction_col = "direction" if "direction" in deal_cols else "'' AS direction"
                all_deals = conn.execute(
                    f"""
                    SELECT LOWER(COALESCE(source,'')) AS source, pnl_usd, outcome, {direction_col}
                      FROM ctrader_deals
                     WHERE execution_utc >= ?
                       AND UPPER(COALESCE(symbol,'')) = 'BTCUSD'
                     ORDER BY execution_utc DESC
                    """,
                    (since_iso,),
                ).fetchall()
        except Exception as exc:
            out["error"] = f"db_query_error:{exc}"
            self._save_report_snapshot("btc_direct_lane_auto_tune_report", out)
            return out

        any_action = False
        for alias, conf_key, source_token in family_config:
            family_out: dict = {"alias": alias, "param": conf_key, "source": source_token, "status": "hold"}
            fills = [r for r in all_deals if str(r[0] or "").startswith(source_token.split(":")[0]) and alias in str(r[0] or "")]
            resolved = len(fills)
            wins = sum(1 for r in fills if (int(r[2] or -1) == 1 or float(r[1] or 0.0) > 0))
            losses = sum(1 for r in fills if (int(r[2] or -1) == 0 or float(r[1] or 0.0) < 0))
            pnl = round(sum(float(r[1] or 0.0) for r in fills), 2)
            win_rate = round(wins / resolved, 4) if resolved > 0 else 0.0

            family_out.update({"resolved": resolved, "wins": wins, "losses": losses, "pnl_usd": pnl, "win_rate": win_rate})

            if resolved < min_resolved:
                family_out["status"] = f"insufficient_sample:{resolved}<{min_resolved}"
                out["families"][alias] = family_out
                continue

            current_conf = float(getattr(config, conf_key, 67.0) or 67.0)
            tighten = bool(win_rate <= tighten_wr or pnl < -3.0)
            loosen = bool(win_rate >= loosen_wr and pnl >= 2.0)

            if tighten:
                new_conf = round(min(max_ceil - 1.0, max(min_floor, current_conf + step)), 2)
                direction = "tightened"
            elif loosen:
                new_conf = round(max(min_floor, current_conf - step), 2)
                direction = "loosened"
            else:
                family_out["status"] = "hold"
                out["families"][alias] = family_out
                continue

            if abs(round(new_conf, 4) - round(current_conf, 4)) < 0.05:
                family_out["status"] = "no_effective_change"
                out["families"][alias] = family_out
                continue

            family_out["proposed_value"] = f"{new_conf:.2f}"
            family_out["current_value"] = f"{current_conf:.2f}"
            family_out["direction"] = direction
            reason = f"wr={win_rate:.2f} pnl={pnl:.2f} n={resolved}"

            if bool(getattr(config, "XAU_DIRECT_LANE_TRIAL_ENABLED", True)):
                try:
                    tid = self._propose_parameter_trial(
                        param=conf_key,
                        current_value=f"{current_conf:.2f}",
                        proposed_value=f"{new_conf:.2f}",
                        direction=direction,
                        reason=f"btc_{alias}:{reason}",
                        source="auto_tune_btc_direct_lane",
                    )
                    family_out["status"] = f"trial_proposed:{tid}"
                    any_action = True
                except Exception:
                    family_out["status"] = "trial_propose_failed"
            else:
                self._apply_runtime_value(conf_key, f"{new_conf:.2f}")
                self._upsert_env_key(self.env_local_path, conf_key, f"{new_conf:.2f}")
                family_out["status"] = f"{direction}:applied_direct"
                any_action = True

            out["families"][alias] = family_out

        out["ok"] = True
        out["status"] = "ran" if any_action else "hold_all"
        self._save_report_snapshot("btc_direct_lane_auto_tune_report", out)
        return out

    # ── Parameter Trial Sandbox ──────────────────────────────────────────────

    def _load_trials(self) -> list:
        state = self._load_named_state(self.parameter_trial_state_path)
        return list(state.get("trials") or [])

    def _save_trials(self, trials: list) -> None:
        self._save_named_state(self.parameter_trial_state_path, {"trials": trials})

    def _propose_parameter_trial(
        self,
        *,
        param: str,
        current_value: str,
        proposed_value: str,
        direction: str,
        reason: str,
        source: str,
    ) -> str:
        """Create a new trial record. Returns trial ID. Skips if duplicate pending or awaiting approval."""
        trials = self._load_trials()
        # De-duplicate: skip if same param already in-flight or awaiting admin approval
        for t in trials:
            if str(t.get("param") or "") == param and str(t.get("status") or "") in (
                "pending_bt", "bt_running", "bt_passed", "waiting_approval"
            ):
                return str(t.get("id") or "")
        # Enforce max pending cap
        pending = [t for t in trials if str(t.get("status") or "") in ("pending_bt", "bt_running")]
        max_pending = max(1, int(getattr(config, "XAU_DIRECT_LANE_TRIAL_MAX_PENDING", 3) or 3))
        if len(pending) >= max_pending:
            return "cap_reached"
        trial_id = f"pts_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{param[-8:]}"
        trial: dict = {
            "id": trial_id,
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": str(source or ""),
            "status": "pending_bt",
            "param": str(param),
            "current_value": str(current_value),
            "proposed_value": str(proposed_value),
            "direction": str(direction or ""),
            "reason": str(reason or ""),
            "bt_result": None,
            "bt_completed_at": None,
            "notified_at": None,
            "applied_at": None,
        }
        trials.append(trial)
        # Prune applied/rejected beyond last 20
        archived = [t for t in trials if str(t.get("status") or "") in ("applied", "rejected")]
        active = [t for t in trials if str(t.get("status") or "") not in ("applied", "rejected")]
        self._save_trials(active + archived[-20:])
        return trial_id

    def run_parameter_trial_bt(self) -> dict:
        """Run BT for all pending trials. Returns summary of results."""
        out: dict = {"ok": False, "checked": 0, "passed": 0, "failed": 0, "skipped": 0, "trials": []}
        if not bool(getattr(config, "XAU_DIRECT_LANE_TRIAL_ENABLED", True)):
            out["status"] = "disabled"
            return out
        trials = self._load_trials()
        pending = [t for t in trials if str(t.get("status") or "") == "pending_bt"]
        if not pending:
            out["ok"] = True
            out["status"] = "no_pending"
            return out
        lookback_hours = max(24, int(getattr(config, "XAU_DIRECT_LANE_TRIAL_BT_LOOKBACK_HOURS", 72) or 72))
        min_incremental = max(1, int(getattr(config, "XAU_DIRECT_LANE_TRIAL_BT_MIN_INCREMENTAL", 3) or 3))
        min_wr = max(0.3, float(getattr(config, "XAU_DIRECT_LANE_TRIAL_BT_MIN_WIN_RATE", 0.55) or 0.55))
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Load shadow journal once for all trials
        shadow_rows: list = []
        try:
            with self._connect_ctrader() as conn:
                shadow_rows = conn.execute(
                    """
                    SELECT confidence, shadow_outcome, block_reason
                      FROM xau_shadow_journal
                     WHERE signal_utc >= ?
                       AND symbol = 'XAUUSD'
                       AND shadow_outcome IS NOT NULL
                       AND shadow_outcome != 'expired'
                    """,
                    (since_iso,),
                ).fetchall()
        except Exception as exc:
            out["error"] = f"shadow_query_error:{exc}"
            return out
        for trial in pending:
            trial["status"] = "bt_running"
            tid = str(trial.get("id") or "")
            param = str(trial.get("param") or "")
            direction = str(trial.get("direction") or "")
            bt_result: dict = {"trial_id": tid, "param": param, "verdict": "insufficient_data"}
            # XAU params: shadow journal incremental band BT
            _XAU_SHADOW_PARAMS = {
                "MT5_SCALP_XAU_LIVE_CONF_MIN", "MT5_SCALP_XAU_LIVE_CONF_MAX",
                "XAU_DIRECT_LANE_MIN_CONFIDENCE", "XAU_TDF_MIN_CONFIDENCE",
                "XAU_MFU_MIN_CONFIDENCE", "XAU_FLOW_SHORT_SIDECAR_MIN_CONFIDENCE",
                "XAU_FFFS_MIN_CONFIDENCE", "XAU_RANGE_REPAIR_MIN_CONFIDENCE",
            }
            # BTC/ETH params: ctrader_deals overall WR (no confidence band available)
            _CRYPTO_LIVE_PARAMS: dict[str, str] = {
                "CTRADER_BTC_WINNER_MIN_CONFIDENCE": "scalp_btcusd:canary",
                "BTC_WEEKDAY_LOB_MIN_CONFIDENCE": "scalp_btcusd:canary",
                "BTC_FSS_MIN_CONFIDENCE": "scalp_btcusd:cfs:canary",
                "BTC_FLS_MIN_CONFIDENCE": "scalp_btcusd:fls:canary",
                "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND": "scalp_ethusd:canary",
                "ETH_WEEKDAY_PROBE_MIN_CONFIDENCE": "scalp_ethusd:canary",
            }
            if param in _XAU_SHADOW_PARAMS:
                try:
                    current_val = float(trial.get("current_value") or 0.0)
                    proposed_val = float(trial.get("proposed_value") or 0.0)
                except Exception:
                    bt_result["verdict"] = "invalid_values"
                    trial["bt_result"] = bt_result
                    trial["status"] = "bt_failed"
                    trial["bt_completed_at"] = now_iso
                    out["failed"] += 1
                    out["trials"].append(bt_result)
                    continue
                lo = min(current_val, proposed_val)
                hi = max(current_val, proposed_val)
                # Incremental signals: confidence in the delta band
                incremental = [r for r in shadow_rows if lo <= float(r[0] or 0.0) < hi]
                inc_wins = sum(1 for r in incremental if str(r[1] or "") == "tp_hit")
                inc_resolved = len(incremental)
                inc_wr = round(inc_wins / inc_resolved, 4) if inc_resolved > 0 else 0.0
                # Baseline: signals above current threshold (already allowed)
                baseline = [r for r in shadow_rows if float(r[0] or 0.0) >= hi]
                base_wins = sum(1 for r in baseline if str(r[1] or "") == "tp_hit")
                base_resolved = len(baseline)
                base_wr = round(base_wins / base_resolved, 4) if base_resolved > 0 else 0.0
                bt_result.update({
                    "current_value": current_val,
                    "proposed_value": proposed_val,
                    "direction": direction,
                    "incremental_resolved": inc_resolved,
                    "incremental_wins": inc_wins,
                    "incremental_win_rate": inc_wr,
                    "baseline_resolved": base_resolved,
                    "baseline_win_rate": base_wr,
                    "min_win_rate_required": min_wr,
                    "min_incremental_required": min_incremental,
                })
                if inc_resolved < min_incremental:
                    bt_result["verdict"] = "insufficient_data"
                    bt_result["verdict_reason"] = f"incremental_resolved={inc_resolved}<{min_incremental}"
                    trial["status"] = "pending_bt"  # keep pending — not enough data yet
                    out["skipped"] += 1
                elif direction in ("loosened", "loosen") and inc_wr >= min_wr:
                    bt_result["verdict"] = "pass"
                    bt_result["verdict_reason"] = f"incremental_wr={inc_wr:.2f}>={min_wr:.2f} | n={inc_resolved}"
                    trial["status"] = "bt_passed"
                    out["passed"] += 1
                elif direction in ("tightened", "tighten"):
                    # For tightening: incremental signals have bad WR (we're blocking losers)
                    inc_sl_rate = round(1.0 - inc_wr, 4)
                    if inc_sl_rate >= 0.45:
                        bt_result["verdict"] = "pass"
                        bt_result["verdict_reason"] = f"incremental_sl_rate={inc_sl_rate:.2f} (losers confirmed) | n={inc_resolved}"
                        trial["status"] = "bt_passed"
                        out["passed"] += 1
                    else:
                        bt_result["verdict"] = "fail"
                        bt_result["verdict_reason"] = f"incremental_wr={inc_wr:.2f} — tightening would block winners | n={inc_resolved}"
                        trial["status"] = "bt_failed"
                        out["failed"] += 1
                else:
                    bt_result["verdict"] = "fail"
                    bt_result["verdict_reason"] = f"incremental_wr={inc_wr:.2f}<{min_wr:.2f} | n={inc_resolved}"
                    trial["status"] = "bt_failed"
                    out["failed"] += 1
            elif param in _CRYPTO_LIVE_PARAMS:
                # BTC/ETH: use ctrader_deals overall WR (no confidence band stored)
                source_token = _CRYPTO_LIVE_PARAMS[param]
                try:
                    with self._connect_ctrader() as conn:
                        crypto_rows = conn.execute(
                            """
                            SELECT outcome, pnl_usd FROM ctrader_deals
                            WHERE source = ? AND outcome IS NOT NULL
                            ORDER BY rowid DESC LIMIT 30
                            """,
                            (source_token,),
                        ).fetchall()
                    resolved = len(crypto_rows)
                    wins = sum(1 for r in crypto_rows if int(r[0] or 0) == 1)
                    overall_wr = round(wins / resolved, 4) if resolved > 0 else 0.0
                    sl_rate = round(1.0 - overall_wr, 4)
                    bt_result.update({
                        "source": source_token,
                        "resolved": resolved,
                        "wins": wins,
                        "overall_win_rate": overall_wr,
                        "sl_rate": sl_rate,
                        "min_resolved_required": min_incremental,
                    })
                    if resolved < min_incremental:
                        bt_result["verdict"] = "insufficient_data"
                        bt_result["verdict_reason"] = f"resolved={resolved}<{min_incremental}"
                        trial["status"] = "pending_bt"
                        out["skipped"] += 1
                    elif direction in ("loosened", "loosen") and overall_wr >= min_wr:
                        bt_result["verdict"] = "pass"
                        bt_result["verdict_reason"] = f"overall_wr={overall_wr:.2f}>={min_wr:.2f} n={resolved}"
                        trial["status"] = "bt_passed"
                        out["passed"] += 1
                    elif direction in ("tightened", "tighten") and sl_rate >= 0.45:
                        bt_result["verdict"] = "pass"
                        bt_result["verdict_reason"] = f"sl_rate={sl_rate:.2f}>=0.45 (losers confirmed) n={resolved}"
                        trial["status"] = "bt_passed"
                        out["passed"] += 1
                    else:
                        bt_result["verdict"] = "fail"
                        bt_result["verdict_reason"] = (
                            f"overall_wr={overall_wr:.2f} direction={direction} "
                            f"(need loosen>={min_wr:.2f} or tighten sl>=0.45) n={resolved}"
                        )
                        trial["status"] = "bt_failed"
                        out["failed"] += 1
                except Exception as exc:
                    bt_result["verdict"] = "error"
                    bt_result["verdict_reason"] = str(exc)
                    trial["status"] = "bt_failed"
                    out["failed"] += 1
            else:
                bt_result["verdict"] = "unsupported_param"
                trial["status"] = "bt_failed"
                out["failed"] += 1
            trial["bt_result"] = bt_result
            if trial["status"] != "pending_bt":
                trial["bt_completed_at"] = now_iso
            out["checked"] += 1
            out["trials"].append(bt_result)
        self._save_trials(trials)
        out["ok"] = True
        self._save_report_snapshot("parameter_trial_bt_report", out)
        return out

    def get_pending_trial_notifications(self) -> list:
        """Return trials that passed BT and have not yet been notified."""
        trials = self._load_trials()
        return [t for t in trials if str(t.get("status") or "") == "bt_passed" and not t.get("notified_at")]

    def mark_trial_notified(self, trial_id: str) -> None:
        trials = self._load_trials()
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for t in trials:
            if str(t.get("id") or "") == trial_id:
                t["notified_at"] = now_iso
        self._save_trials(trials)

    def apply_trial(self, trial_id: str, *, persist: bool = True) -> dict:
        """Apply a passed trial to live config. Returns result dict."""
        trials = self._load_trials()
        trial = next((t for t in trials if str(t.get("id") or "") == trial_id), None)
        if not trial:
            return {"ok": False, "error": f"trial_not_found:{trial_id}"}
        if str(trial.get("status") or "") != "bt_passed":
            return {"ok": False, "error": f"trial_status_not_bt_passed:{trial.get('status')}"}
        param = str(trial.get("param") or "")
        proposed_value = str(trial.get("proposed_value") or "")
        if not param or not proposed_value:
            return {"ok": False, "error": "missing_param_or_value"}
        self._apply_runtime_value(param, proposed_value)
        env_result = self._upsert_env_key(self.env_local_path, param, proposed_value) if persist else {"ok": True, "updated": False, "reason": "persist_disabled"}
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        trial["status"] = "applied"
        trial["applied_at"] = now_iso
        self._save_trials(trials)
        return {"ok": True, "trial_id": trial_id, "param": param, "value": proposed_value, "env": env_result}

    # ── Shadow backtest ──────────────────────────────────────────────────────

    def _get_shadow_outcomes(self, *, lookback_hours: int = 72) -> dict:
        """Return aggregated shadow journal stats for auto-tune consumption."""
        out: dict = {
            "ok": False,
            "total": 0,
            "resolved": 0,
            "wins": 0,
            "losses": 0,
            "expired": 0,
            "win_rate": 0.0,
            "top_block_reasons": [],
            "by_block_reason": {},
        }
        if not self.ctrader_db_path.exists():
            return out
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        try:
            with self._connect_ctrader() as conn:
                rows = conn.execute(
                    """
                    SELECT shadow_outcome, block_reason
                      FROM xau_shadow_journal
                     WHERE signal_utc >= ?
                       AND symbol = 'XAUUSD'
                    """,
                    (since_iso,),
                ).fetchall()
        except Exception:
            return out
        reason_buckets: dict = {}
        for row in rows:
            outcome = str(row[0] or "").strip().lower() if row[0] else ""
            reason = str(row[1] or "unknown").strip()
            out["total"] += 1
            bucket = reason_buckets.setdefault(reason, {"resolved": 0, "wins": 0, "losses": 0})
            if outcome == "tp_hit":
                out["resolved"] += 1
                out["wins"] += 1
                bucket["resolved"] += 1
                bucket["wins"] += 1
            elif outcome == "sl_hit":
                out["resolved"] += 1
                out["losses"] += 1
                bucket["resolved"] += 1
                bucket["losses"] += 1
            elif outcome == "expired":
                out["expired"] += 1
        resolved = out["resolved"]
        out["win_rate"] = round(out["wins"] / resolved, 4) if resolved > 0 else 0.0
        out["by_block_reason"] = {
            r: {
                "resolved": b["resolved"],
                "wins": b["wins"],
                "losses": b["losses"],
                "win_rate": round(b["wins"] / b["resolved"], 4) if b["resolved"] > 0 else 0.0,
            }
            for r, b in reason_buckets.items()
        }
        top = sorted(reason_buckets.items(), key=lambda x: -x[1]["resolved"])
        out["top_block_reasons"] = [r for r, _ in top[:5]]
        out["ok"] = True
        return out

    def run_xau_shadow_backtest(self) -> dict:
        """Resolve pending shadow journal signals against candle_data.db.

        For each unresolved XAUUSD shadow signal, walks 1m candles forward
        in time and records whether TP1 or SL was hit first within the
        configured resolve window.
        """
        out: dict = {
            "ok": False,
            "total": 0,
            "pending": 0,
            "newly_resolved": 0,
            "skipped_no_candles": 0,
            "error": "",
        }
        if not bool(getattr(config, "XAU_SHADOW_BACKTEST_ENABLED", True)):
            out["status"] = "disabled"
            return out
        candle_db_path = Path(__file__).resolve().parent.parent / "backtest" / "candle_data.db"
        use_tick_fallback = not candle_db_path.exists()
        if not self.ctrader_db_path.exists():
            out["error"] = "ctrader_db_missing"
            return out
        resolve_hours = max(1.0, float(getattr(config, "XAU_SHADOW_BACKTEST_RESOLVE_HOURS", 4.0) or 4.0))
        cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=resolve_hours + 0.5)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        try:
            with self._connect_ctrader() as cconn:
                cconn.execute("PRAGMA journal_mode=WAL")
                cconn.execute("""
                    CREATE TABLE IF NOT EXISTS xau_shadow_journal (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        signal_utc TEXT NOT NULL,
                        symbol TEXT NOT NULL DEFAULT 'XAUUSD',
                        direction TEXT NOT NULL,
                        confidence REAL NOT NULL DEFAULT 0.0,
                        entry REAL NOT NULL DEFAULT 0.0,
                        stop_loss REAL NOT NULL DEFAULT 0.0,
                        take_profit_1 REAL NOT NULL DEFAULT 0.0,
                        take_profit_2 REAL NOT NULL DEFAULT 0.0,
                        take_profit_3 REAL NOT NULL DEFAULT 0.0,
                        block_reason TEXT NOT NULL DEFAULT '',
                        raw_scores_json TEXT NOT NULL DEFAULT '{}',
                        shadow_outcome TEXT,
                        resolved_utc TEXT,
                        shadow_pnl_rr REAL
                    )
                """)
                pending_rows = cconn.execute(
                    """
                    SELECT id, signal_utc, direction, entry, stop_loss, take_profit_1
                      FROM xau_shadow_journal
                     WHERE shadow_outcome IS NULL
                       AND signal_utc <= ?
                       AND symbol = 'XAUUSD'
                     ORDER BY signal_utc ASC
                     LIMIT 200
                    """,
                    (cutoff_iso,),
                ).fetchall()
                out["total"] = int(
                    (cconn.execute("SELECT COUNT(*) FROM xau_shadow_journal WHERE symbol='XAUUSD'").fetchone() or (0,))[0]
                )
                out["pending"] = len(pending_rows)
        except Exception as exc:
            out["error"] = f"db_query_error:{exc}"
            return out

        if not pending_rows:
            out["ok"] = True
            return out

        newly_resolved = 0
        skipped = 0
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        def _fetch_bars(signal_utc_str: str, end_utc_str: str) -> list:
            """Return list of (high, low) tuples covering the resolve window.
            Primary: candle_data.db 1m bars. Fallback: 1m buckets from spot_ticks."""
            if not use_tick_fallback:
                try:
                    cconn2 = sqlite3.connect(str(candle_db_path), timeout=15)
                    rows = cconn2.execute(
                        "SELECT high, low FROM candles WHERE symbol='XAUUSD' AND tf='1m' AND ts >= ? AND ts <= ? ORDER BY ts ASC",
                        (signal_utc_str, end_utc_str),
                    ).fetchall()
                    cconn2.close()
                    return [(float(r[0] or 0), float(r[1] or 0)) for r in rows]
                except Exception:
                    return []
            # Tick fallback: build 1m buckets from ctrader_spot_ticks mid prices
            try:
                tick_rows = self._connect_ctrader().execute(
                    "SELECT bid, ask FROM ctrader_spot_ticks WHERE symbol='XAUUSD' AND event_utc >= ? AND event_utc <= ? ORDER BY event_utc ASC",
                    (signal_utc_str, end_utc_str),
                ).fetchall()
            except Exception:
                return []
            if not tick_rows:
                return []
            # Aggregate into pseudo-1m buckets of ~12 ticks each (sparse but sufficient)
            bucket_size = max(1, len(tick_rows) // max(1, int(resolve_hours * 60)))
            bars: list = []
            i = 0
            while i < len(tick_rows):
                chunk = tick_rows[i: i + bucket_size]
                mids = [(float(r[0] or 0) + float(r[1] or 0)) / 2.0 for r in chunk if r[0] and r[1]]
                if mids:
                    bars.append((max(mids), min(mids)))
                i += bucket_size
            return bars

        try:
            with self._connect_ctrader() as cconn:
                for row in pending_rows:
                    row_id = int(row[0])
                    signal_utc = str(row[1] or "")
                    direction = str(row[2] or "").strip().lower()
                    entry = float(row[3] or 0.0)
                    sl = float(row[4] or 0.0)
                    tp1 = float(row[5] or 0.0)
                    if entry <= 0 or sl <= 0 or tp1 <= 0:
                        cconn.execute(
                            "UPDATE xau_shadow_journal SET shadow_outcome=?, resolved_utc=? WHERE id=?",
                            ("expired", now_iso, row_id),
                        )
                        newly_resolved += 1
                        continue
                    risk = abs(entry - sl)
                    if risk < 1e-6:
                        cconn.execute(
                            "UPDATE xau_shadow_journal SET shadow_outcome=?, resolved_utc=? WHERE id=?",
                            ("expired", now_iso, row_id),
                        )
                        newly_resolved += 1
                        continue
                    end_utc = (
                        datetime.strptime(signal_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                        + timedelta(hours=resolve_hours)
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                    bars = _fetch_bars(signal_utc, end_utc)
                    if not bars:
                        skipped += 1
                        continue
                    outcome = "expired"
                    pnl_rr = None
                    for high, low in bars:
                        if direction == "long":
                            tp_hit = high >= tp1
                            sl_hit = low <= sl
                        else:
                            tp_hit = low <= tp1
                            sl_hit = high >= sl
                        if tp_hit and sl_hit:
                            outcome = "tp_hit"
                            pnl_rr = round(abs(tp1 - entry) / risk, 4)
                            break
                        elif tp_hit:
                            outcome = "tp_hit"
                            pnl_rr = round(abs(tp1 - entry) / risk, 4)
                            break
                        elif sl_hit:
                            outcome = "sl_hit"
                            pnl_rr = round(-1.0, 4)
                            break
                    cconn.execute(
                        "UPDATE xau_shadow_journal SET shadow_outcome=?, resolved_utc=?, shadow_pnl_rr=? WHERE id=?",
                        (outcome, now_iso, pnl_rr, row_id),
                    )
                    newly_resolved += 1
                cconn.commit()
        except Exception as exc:
            out["error"] = f"resolve_loop_error:{exc}"

        out["ok"] = True
        out["newly_resolved"] = newly_resolved
        out["skipped_no_candles"] = skipped
        out["pending"] = max(0, out["pending"] - newly_resolved)
        self._save_report_snapshot("xau_shadow_backtest_report", out)
        return out

    def _build_candidate_changes(
        self,
        *,
        winner_report: dict,
        crypto_report: dict,
        audit_report: dict,
        canary_report: Optional[dict] = None,
        strategy_lab_report: Optional[dict] = None,
        experiment_report: Optional[dict] = None,
    ) -> dict:
        min_sample = max(2, int(getattr(config, "AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 6) or 6))
        changes: dict[str, str] = {}
        skipped: list[dict] = []
        reasons: list[str] = []
        affected_symbols: set[str] = set()
        affected_sources: set[str] = set()
        strategy_promotions: list[dict] = []
        managed_sources = self._managed_ctrader_sources()
        current_allowed_sources = set(config.get_ctrader_allowed_sources() or set())
        current_store_sources = set(config.get_ctrader_store_feed_sources() or set())
        preserved_allowed = {s for s in current_allowed_sources if s not in managed_sources}
        preserved_store = {s for s in current_store_sources if s not in managed_sources}
        desired_managed_sources: set[str] = set()
        xau_ctrader_direct_enabled = bool(getattr(config, "PERSISTENT_CANARY_CTRADER_ENABLED", True))
        xau_direct_allowed_sources = {
            str(src or "").strip().lower()
            for src in list((config.get_persistent_canary_direct_allowed_sources() or set()))
            if str(src or "").strip()
        }
        xau_keep_scalp_direct = xau_ctrader_direct_enabled and ("scalp_xauusd" in xau_direct_allowed_sources)
        xau_keep_scheduled_direct = xau_ctrader_direct_enabled and ("xauusd_scheduled" in xau_direct_allowed_sources)

        for rec in list((winner_report or {}).get("recommendations") or []):
            sym = _norm_symbol(rec.get("symbol"))
            mode = str(rec.get("recommended_live_mode") or "").strip().lower()
            model_row = next((row for row in list((winner_report or {}).get("symbols") or []) if _norm_symbol(row.get("symbol")) == sym), {})
            model_samples = int(((model_row.get("model") or {}).get("resolved", 0) or 0))
            if model_samples < min_sample:
                skipped.append({"key": f"winner:{sym}", "reason": f"min_sample_gate:{model_samples}<{min_sample}"})
                continue
            if sym == "BTCUSD":
                if mode.startswith("winner"):
                    desired_managed_sources.add("scalp_btcusd:winner")
                    affected_symbols.add(sym)
                    affected_sources.add("scalp_btcusd:winner")
                    reasons.append("BTC winner report => cTrader winner-only")
                elif mode == "collect_sample":
                    desired_managed_sources.add("scalp_btcusd")
            elif sym == "ETHUSD":
                if mode.startswith("winner"):
                    desired_managed_sources.add("scalp_ethusd:winner")
                    affected_symbols.add(sym)
                    affected_sources.add("scalp_ethusd:winner")
                    reasons.append("ETH winner report => cTrader winner-only")
                elif mode == "collect_sample":
                    desired_managed_sources.add("scalp_ethusd")
            elif sym == "XAUUSD":
                if mode in {"scheduled_winner_only", "scheduled_winner_plus_safe_scalp"}:
                    desired_managed_sources.add("xauusd_scheduled:winner")
                    desired_managed_sources.add("scalp_xauusd:winner")
                    affected_symbols.add(sym)
                    affected_sources.add("xauusd_scheduled:winner")
                    reasons.append(f"XAU winner report => {mode}")
                if xau_keep_scheduled_direct:
                    desired_managed_sources.add("xauusd_scheduled")
                    affected_sources.add("xauusd_scheduled")
                if mode == "scheduled_winner_plus_safe_scalp" or xau_keep_scalp_direct:
                    desired_managed_sources.add("scalp_xauusd")
                    affected_sources.add("scalp_xauusd")

        allowed_csv = self._lower_csv(preserved_allowed | desired_managed_sources)
        store_csv = self._lower_csv(preserved_store | desired_managed_sources)
        if allowed_csv and allowed_csv != self._current_value("CTRADER_ALLOWED_SOURCES"):
            changes["CTRADER_ALLOWED_SOURCES"] = allowed_csv
        if store_csv and store_csv != self._current_value("CTRADER_STORE_FEED_SOURCES"):
            changes["CTRADER_STORE_FEED_SOURCES"] = store_csv

        for rec in list((crypto_report or {}).get("recommendations") or []):
            sym = _norm_symbol(rec.get("symbol"))
            if sym not in {"BTCUSD", "ETHUSD"}:
                continue
            source = str(rec.get("profile_source") or "").strip().lower()
            weekend_resolved = int(rec.get("weekend_resolved", 0) or 0)
            if source != "weekend" and weekend_resolved < min_sample:
                skipped.append({"key": f"crypto_profile:{sym}", "reason": f"min_sample_gate:{weekend_resolved}<{min_sample}"})
                continue
            conf = float(rec.get("recommended_min_confidence", 0.0) or 0.0)
            sessions = [str(v or "").strip() for v in list(rec.get("recommended_sessions") or []) if str(v or "").strip()]
            if sym == "BTCUSD":
                if conf > 0.0 and str(conf) != self._current_value("SCALPING_BTC_MIN_CONFIDENCE_WEEKEND"):
                    changes["SCALPING_BTC_MIN_CONFIDENCE_WEEKEND"] = f"{conf:.1f}".rstrip("0").rstrip(".")
                if sessions:
                    csv = self._signature_csv(sessions)
                    if csv != self._current_value("SCALPING_BTC_ALLOWED_SESSIONS_WEEKEND"):
                        changes["SCALPING_BTC_ALLOWED_SESSIONS_WEEKEND"] = csv
                affected_symbols.add(sym)
            elif sym == "ETHUSD":
                if conf > 0.0 and str(conf) != self._current_value("SCALPING_ETH_MIN_CONFIDENCE_WEEKEND"):
                    changes["SCALPING_ETH_MIN_CONFIDENCE_WEEKEND"] = f"{conf:.1f}".rstrip("0").rstrip(".")
                if sessions:
                    csv = self._signature_csv(sessions)
                    if csv != self._current_value("SCALPING_ETH_ALLOWED_SESSIONS_WEEKEND"):
                        changes["SCALPING_ETH_ALLOWED_SESSIONS_WEEKEND"] = csv
                affected_symbols.add(sym)

        for rec in list((audit_report or {}).get("recommendations") or []):
            sym = _norm_symbol(rec.get("symbol"))
            src = _norm_source(rec.get("source"))
            action = str(rec.get("action") or "").strip().lower()
            resolved = int(rec.get("resolved", 0) or 0)
            if sym != "XAUUSD" or src != "scalp_xauusd" or action != "canary_reinstate":
                continue
            if resolved < min_sample:
                skipped.append({"key": "xau_canary", "reason": f"min_sample_gate:{resolved}<{min_sample}"})
                continue
            proposed_conf = rec.get("proposed_canary_min_confidence")
            if proposed_conf is not None:
                proposed_conf_f = float(proposed_conf)
                current_conf = float(getattr(config, "NEURAL_GATE_CANARY_MIN_CONFIDENCE", 72.0) or 72.0)
                if proposed_conf_f < current_conf:
                    changes["NEURAL_GATE_CANARY_MIN_CONFIDENCE"] = f"{proposed_conf_f:.1f}".rstrip("0").rstrip(".")
                    affected_symbols.add("XAUUSD")
                    affected_sources.add("scalp_xauusd")
                    reasons.append("XAU missed-op audit => lower canary min confidence")
            proposed_floor = rec.get("proposed_canary_allow_low")
            if proposed_floor is not None:
                proposed_floor_f = float(proposed_floor)
                current_floor = float(getattr(config, "NEURAL_GATE_CANARY_FIXED_ALLOW_LOW", 0.0) or 0.0)
                if current_floor > 0.0 and proposed_floor_f < current_floor:
                    changes["NEURAL_GATE_CANARY_FIXED_ALLOW_LOW"] = f"{proposed_floor_f:.3f}".rstrip("0").rstrip(".")
                    affected_symbols.add("XAUUSD")
                    affected_sources.add("scalp_xauusd")
                    reasons.append("XAU missed-op audit => widen canary low floor")

        for rec in list((canary_report or {}).get("recommendations") or []):
            sym = _norm_symbol(rec.get("symbol"))
            key = str(rec.get("key") or "").strip()
            resolved = int(rec.get("resolved", 0) or 0)
            proposed = rec.get("proposed")
            if not key or proposed is None:
                continue
            if resolved < min_sample:
                skipped.append({"key": f"canary:{sym}:{key}", "reason": f"min_sample_gate:{resolved}<{min_sample}"})
                continue
            current_raw = self._current_value(key)
            current_val = _safe_float(current_raw, None) if str(current_raw).strip() else None
            proposed_f = float(proposed)
            if current_val is not None and round(current_val, 4) == round(proposed_f, 4):
                continue
            if key.startswith("SCALPING_") or key.startswith("NEURAL_GATE_") or key.startswith("MT5_"):
                changes[key] = f"{proposed_f:.3f}".rstrip("0").rstrip(".")
                affected_symbols.add(sym)
                if sym == "XAUUSD":
                    affected_sources.add("scalp_xauusd")
                elif sym == "BTCUSD":
                    affected_sources.add("scalp_btcusd")
                elif sym == "ETHUSD":
                    affected_sources.add("scalp_ethusd")
                reasons.append(f"{sym} canary audit => {str(rec.get('action') or 'tune')}")

        for rec in list((experiment_report or {}).get("recommendations") or []):
            sym = _norm_symbol(rec.get("symbol"))
            family = str(rec.get("family") or "").strip().lower()
            action = str(rec.get("action") or "").strip().lower()
            if sym == "BTCUSD" and family == "btc_weekday_lob_momentum" and action == "promote_btc_weekday_lob_narrow_live":
                standard_families = set(getattr(config, "get_persistent_canary_strategy_families", lambda: set())() or set())
                experimental_families = set(getattr(config, "get_persistent_canary_experimental_families", lambda: set())() or set())
                if bool(getattr(config, "XAU_FLOW_SHORT_SIDECAR_ENABLED", True)):
                    experimental_families.add("xau_scalp_flow_short_sidecar")
                standard_families.add("btc_weekday_lob_momentum")
                experimental_families.discard("btc_weekday_lob_momentum")
                std_csv = self._lower_csv(standard_families)
                exp_csv = self._lower_csv(experimental_families)
                if std_csv != self._current_value("PERSISTENT_CANARY_STRATEGY_FAMILIES"):
                    changes["PERSISTENT_CANARY_STRATEGY_FAMILIES"] = std_csv
                if exp_csv != self._current_value("PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES"):
                    changes["PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES"] = exp_csv
                target_risk = float(getattr(config, "BTC_WEEKDAY_LOB_NARROW_LIVE_RISK_USD", 1.1) or 1.1)
                current_risk = float(getattr(config, "BTC_WEEKDAY_LOB_CTRADER_RISK_USD", 0.9) or 0.9)
                if target_risk > current_risk:
                    changes["BTC_WEEKDAY_LOB_CTRADER_RISK_USD"] = f"{target_risk:.2f}".rstrip("0").rstrip(".")
                affected_symbols.add(sym)
                affected_sources.add("scalp_btcusd:bwl:canary")
                reasons.append("BTC weekday LOB experiment => promote to narrow live lane")

        if bool(getattr(config, "STRATEGY_PROMOTION_ENABLED", True)):
            for row in list((strategy_lab_report or {}).get("promotable_candidates") or []):
                sym = _norm_symbol(row.get("symbol"))
                strategy_id = str(row.get("strategy_id") or "").strip()
                gate = dict(row.get("promotion_gate") or {})
                if (not sym) or (not strategy_id) or (not bool(gate.get("eligible"))):
                    continue
                if not bool(row.get("execution_ready", False)):
                    skipped.append({"key": f"strategy:{sym}:{strategy_id}", "reason": "execution_not_ready"})
                    continue
                overrides = dict(row.get("proposed_overrides") or {})
                applied_any = False
                for key, value in overrides.items():
                    cfg_key = str(key or "").strip()
                    if not cfg_key:
                        continue
                    rendered = str(value)
                    if rendered == self._current_value(cfg_key):
                        continue
                    changes[cfg_key] = rendered
                    applied_any = True
                if applied_any:
                    strategy_promotions.append(
                        {
                            "symbol": sym,
                            "strategy_id": strategy_id,
                            "family": str(row.get("family") or ""),
                            "walk_forward_score": row.get("walk_forward_score"),
                            "promotion_gate": gate,
                        }
                    )
                    affected_symbols.add(sym)
                    if sym == "XAUUSD":
                        affected_sources.update({"xauusd_scheduled:winner", "scalp_xauusd"})
                    elif sym == "BTCUSD":
                        affected_sources.add("scalp_btcusd:winner")
                    elif sym == "ETHUSD":
                        affected_sources.add("scalp_ethusd:winner")
                    reasons.append(f"{sym} strategy promotion => {strategy_id}")

        return {
            "changes": changes,
            "skipped": skipped,
            "reasons": reasons,
            "affected_symbols": sorted(list(affected_symbols)),
            "affected_sources": sorted(list(affected_sources)),
            "strategy_promotions": strategy_promotions,
        }

    def auto_apply_live_profile(
        self,
        *,
        winner_report: Optional[dict] = None,
        crypto_report: Optional[dict] = None,
        audit_report: Optional[dict] = None,
        canary_report: Optional[dict] = None,
        strategy_lab_report: Optional[dict] = None,
        experiment_report: Optional[dict] = None,
        persist_env: Optional[bool] = None,
    ) -> dict:
        enabled = bool(getattr(config, "AUTO_APPLY_LIVE_PROFILE_ENABLED", False))
        persist = bool(getattr(config, "AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV", True)) if persist_env is None else bool(persist_env)
        rollback_min_resolved = max(1, int(getattr(config, "AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MIN_RESOLVED", 4) or 4))
        rollback_max_net_loss = float(getattr(config, "AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MAX_NET_LOSS_USD", -20.0) or -20.0)
        rollback_min_wr = float(getattr(config, "AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MIN_WIN_RATE", 0.40) or 0.40)
        out = {"ok": False, "enabled": enabled, "persist_env": persist, "generated_at": _iso(_utc_now()), "versions": self._version_info(), "execution_scope": self._execution_scope(), "state_path": str(self.state_path), "status": "disabled" if not enabled else "ready", "rollback": {}, "candidate_changes": {}, "applied_changes": {}, "strategy_promotions": [], "skipped": [], "reasons": [], "error": ""}
        if not enabled:
            self._save_report_snapshot("auto_apply_live_profile_report", out)
            return out

        state = self._load_state()
        active = dict(state.get("active_bundle") or {})
        if active and str(active.get("status") or "") == "active":
            eval_payload = self._evaluate_live_performance_since(
                since_iso=str(active.get("applied_at") or ""),
                affected_symbols={_norm_symbol(v) for v in list(active.get("affected_symbols") or []) if _norm_symbol(v)},
                affected_sources={_norm_source(v) for v in list(active.get("affected_sources") or []) if _norm_source(v)},
            )
            total = dict(eval_payload.get("total") or {})
            resolved = int(total.get("resolved", 0) or 0)
            net = float(total.get("pnl_usd", 0.0) or 0.0)
            wr = float(total.get("win_rate", 0.0) or 0.0)
            rollback_needed = bool(resolved >= rollback_min_resolved and (net <= rollback_max_net_loss or wr < rollback_min_wr))
            if rollback_needed:
                reverted = {}
                for key, payload in dict(active.get("changes") or {}).items():
                    old_value = str((payload or {}).get("old", "") or "")
                    self._apply_runtime_value(str(key), old_value)
                    reverted[str(key)] = self._upsert_env_key(self.env_local_path, str(key), old_value) if persist else {"ok": True, "updated": False, "reason": "persist_disabled"}
                active["status"] = "rolled_back"
                active["rolled_back_at"] = _iso(_utc_now())
                active["evaluation"] = eval_payload
                active["reverted"] = reverted
                state.setdefault("history", []).append(dict(active))
                state["active_bundle"] = None
                self._save_state(state)
                out["rollback"] = {"status": "rolled_back", "evaluation": eval_payload, "reverted": reverted}
            elif resolved >= rollback_min_resolved:
                active["status"] = "stable"
                active["stabilized_at"] = _iso(_utc_now())
                active["evaluation"] = eval_payload
                state.setdefault("history", []).append(dict(active))
                state["active_bundle"] = None
                self._save_state(state)
                out["rollback"] = {"status": "stable", "evaluation": eval_payload}
            else:
                max_wait_min = float(getattr(config, "AUTO_APPLY_LIVE_PROFILE_MAX_WAIT_MIN", 20.0) or 20.0)
                applied_at_str = str(active.get("applied_at") or "")
                timed_out = False
                if applied_at_str:
                    try:
                        from datetime import timezone as _tz
                        applied_dt = datetime.fromisoformat(applied_at_str.replace("Z", "+00:00"))
                        waited_min = (_utc_now() - applied_dt.replace(tzinfo=_tz.utc) if applied_dt.tzinfo is None else _utc_now() - applied_dt).total_seconds() / 60.0
                        timed_out = waited_min >= max_wait_min
                    except Exception:
                        pass
                if timed_out:
                    active["status"] = "timed_out"
                    active["timed_out_at"] = _iso(_utc_now())
                    active["evaluation"] = eval_payload
                    state.setdefault("history", []).append(dict(active))
                    state["active_bundle"] = None
                    self._save_state(state)
                    out["rollback"] = {"status": "timed_out", "evaluation": eval_payload}
                else:
                    active["evaluation"] = eval_payload
                    state["active_bundle"] = active
                    self._save_state(state)
                    out["rollback"] = {"status": "waiting_sample", "evaluation": eval_payload}
                    out["ok"] = True
                    out["status"] = "waiting_active_canary"
                    self._save_report_snapshot("auto_apply_live_profile_report", out)
                    return out
        else:
            out["rollback"] = {"status": "none"}

        winner = dict(winner_report or self._load_json(self._report_path("winner_mission_report")) or {})
        crypto = dict(crypto_report or self._load_json(self._report_path("crypto_weekend_scorecard")) or {})
        audit = dict(audit_report or self._load_json(self._report_path("missed_opportunity_audit_report")) or {})
        canary = dict(canary_report or self._load_json(self._report_path("canary_post_trade_audit_report")) or {})
        experiment = dict(experiment_report or self.build_ct_only_experiment_report() or self._load_json(self._report_path("ct_only_experiment_report")) or {})
        strategy_lab = dict(
            strategy_lab_report
            or self.build_strategy_lab_report(
                winner_report=winner,
                crypto_report=crypto,
                canary_report=canary,
                audit_report=audit,
            )
            or self._load_json(self._report_path("strategy_lab_report"))
            or {}
        )
        plan = self._build_candidate_changes(
            winner_report=winner,
            crypto_report=crypto,
            audit_report=audit,
            canary_report=canary,
            strategy_lab_report=strategy_lab,
            experiment_report=experiment,
        )
        out["candidate_changes"] = dict(plan.get("changes") or {})
        out["skipped"] = list(plan.get("skipped") or [])
        out["reasons"] = list(plan.get("reasons") or [])
        out["strategy_promotions"] = list(plan.get("strategy_promotions") or [])
        if not out["candidate_changes"]:
            out["ok"] = True
            out["status"] = "no_changes"
            self._save_report_snapshot("auto_apply_live_profile_report", out)
            return out

        applied = {}
        change_bundle = {}
        for key, new_value in dict(out["candidate_changes"]).items():
            old_value = self._current_value(key)
            if str(old_value) == str(new_value):
                continue
            self._apply_runtime_value(str(key), str(new_value))
            applied[str(key)] = self._upsert_env_key(self.env_local_path, str(key), str(new_value)) if persist else {"ok": True, "updated": False, "reason": "persist_disabled"}
            change_bundle[str(key)] = {"old": str(old_value), "new": str(new_value)}

        if not change_bundle:
            out["ok"] = True
            out["status"] = "no_effective_changes"
            out["applied_changes"] = applied
            self._save_report_snapshot("auto_apply_live_profile_report", out)
            return out

        bundle = {
            "id": _utc_now().strftime("%Y%m%d_%H%M%S"),
            "status": "active",
            "applied_at": _iso(_utc_now()),
            "versions": self._version_info(),
            "execution_scope": self._execution_scope(),
            "changes": change_bundle,
            "affected_symbols": list(plan.get("affected_symbols") or []),
            "affected_sources": list(plan.get("affected_sources") or []),
            "reasons": list(plan.get("reasons") or []),
            "strategy_promotions": list(plan.get("strategy_promotions") or []),
            "report_refs": {
                "winner_mission_report": str(self._report_path("winner_mission_report")),
                "crypto_weekend_scorecard": str(self._report_path("crypto_weekend_scorecard")),
                "missed_opportunity_audit_report": str(self._report_path("missed_opportunity_audit_report")),
                "canary_post_trade_audit_report": str(self._report_path("canary_post_trade_audit_report")),
                "strategy_lab_report": str(self._report_path("strategy_lab_report")),
                "ct_only_experiment_report": str(self._report_path("ct_only_experiment_report")),
            },
        }
        state["active_bundle"] = bundle
        self._save_state(state)
        out["ok"] = True
        out["status"] = "applied"
        out["applied_changes"] = applied
        out["active_bundle"] = bundle
        self._save_report_snapshot("auto_apply_live_profile_report", out)
        return out


live_profile_autopilot = LiveProfileAutopilot()
