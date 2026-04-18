"""
learning/sharpness_feedback.py

Self-improving feedback loop for Entry Sharpness Score:
1) Sharpness ↔ Outcome Correlation — which dimensions predict wins vs losses
2) Weight Auto-Calibration — adjust dimension weights toward predictive power
3) Family Performance Decay Detector — rolling win-rate decay → risk reduction / alert

All query functions take a sqlite3 Connection; no global state.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SHARPNESS_DIMENSIONS = [
    "momentum_quality",
    "flow_persistence",
    "absorption_quality",
    "price_stability",
    "positioning_quality",
]

DIMENSION_TO_WEIGHT_KEY = {
    "momentum_quality": "XAU_ENTRY_SHARPNESS_W_MOMENTUM",
    "flow_persistence": "XAU_ENTRY_SHARPNESS_W_FLOW",
    "absorption_quality": "XAU_ENTRY_SHARPNESS_W_ABSORPTION",
    "price_stability": "XAU_ENTRY_SHARPNESS_W_STABILITY",
    "positioning_quality": "XAU_ENTRY_SHARPNESS_W_POSITIONING",
}

_WEIGHT_KEY_DEFAULTS = {
    "XAU_ENTRY_SHARPNESS_W_MOMENTUM": 1.0,
    "XAU_ENTRY_SHARPNESS_W_FLOW": 1.0,
    "XAU_ENTRY_SHARPNESS_W_ABSORPTION": 1.0,
    "XAU_ENTRY_SHARPNESS_W_STABILITY": 1.0,
    "XAU_ENTRY_SHARPNESS_W_POSITIONING": 1.0,
}


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime] = None) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# 1. Extract sharpness data from resolved trades
# ---------------------------------------------------------------------------

def _extract_sharpness_from_meta(request_json_str: str) -> Optional[dict]:
    """Parse request_json and extract sharpness dict from xau_openapi_entry_router."""
    try:
        req = json.loads(request_json_str or "{}")
    except Exception:
        return None
    raw_scores = req.get("raw_scores") or {}
    router = raw_scores.get("xau_openapi_entry_router") or {}
    sharpness = router.get("sharpness") or {}
    if not sharpness or "sharpness_score" not in sharpness:
        return None
    return dict(sharpness)


def query_resolved_trades_with_sharpness(
    conn: sqlite3.Connection,
    *,
    days: int = 14,
    symbol: str = "XAUUSD",
) -> list[dict]:
    """Query ctrader_deals + execution_journal for resolved trades with sharpness data.

    Returns list of dicts: {outcome, pnl_usd, direction, source, sharpness: {...}, ...}
    """
    cutoff = (_utc_now() - __import__("datetime").timedelta(days=max(1, days))).strftime("%Y-%m-%d %H:%M:%S")
    try:
        rows = conn.execute(
            """
            SELECT d.outcome, d.pnl_usd, d.execution_utc,
                   j.direction, j.source, j.symbol, j.confidence,
                   j.request_json
            FROM ctrader_deals d
            JOIN execution_journal j ON j.id = d.journal_id
            WHERE d.has_close_detail = 1
              AND d.outcome IN (0, 1)
              AND d.execution_utc >= ?
              AND j.symbol = ?
            ORDER BY d.execution_utc DESC
            """,
            (cutoff, symbol),
        ).fetchall()
    except Exception as e:
        logger.debug("[SharpnessFeedback] query error: %s", e)
        return []

    results = []
    for row in rows:
        sharpness = _extract_sharpness_from_meta(row["request_json"])
        if not sharpness:
            continue
        results.append({
            "outcome": int(row["outcome"]),
            "pnl_usd": _safe_float(row["pnl_usd"]),
            "direction": str(row["direction"] or ""),
            "source": str(row["source"] or ""),
            "confidence": _safe_float(row["confidence"]),
            "execution_utc": str(row["execution_utc"] or ""),
            "sharpness_score": int(sharpness.get("sharpness_score", 50) or 50),
            "sharpness_band": str(sharpness.get("sharpness_band", "normal") or "normal"),
            "momentum_quality": _safe_float(sharpness.get("momentum_quality")),
            "flow_persistence": _safe_float(sharpness.get("flow_persistence")),
            "absorption_quality": _safe_float(sharpness.get("absorption_quality")),
            "price_stability": _safe_float(sharpness.get("price_stability")),
            "positioning_quality": _safe_float(sharpness.get("positioning_quality")),
            "sharpness_reasons": list(sharpness.get("sharpness_reasons") or []),
        })
    return results


# ---------------------------------------------------------------------------
# 2. Sharpness ↔ Outcome Correlation
# ---------------------------------------------------------------------------

def _point_biserial_correlation(continuous: list[float], binary: list[int]) -> float:
    """Compute point-biserial correlation between a continuous variable and binary outcome.

    Returns r in [-1, 1]. Positive = higher values correlate with wins.
    Returns 0.0 when insufficient data or zero variance.
    """
    if len(continuous) != len(binary) or len(continuous) < 4:
        return 0.0
    group_0 = [c for c, b in zip(continuous, binary) if b == 0]
    group_1 = [c for c, b in zip(continuous, binary) if b == 1]
    if not group_0 or not group_1:
        return 0.0
    n = len(continuous)
    n0 = len(group_0)
    n1 = len(group_1)
    mean_0 = sum(group_0) / n0
    mean_1 = sum(group_1) / n1
    try:
        sd = statistics.stdev(continuous)
    except Exception:
        return 0.0
    if sd < 1e-12:
        return 0.0
    r = ((mean_1 - mean_0) / sd) * ((n0 * n1) / (n * n)) ** 0.5
    return max(-1.0, min(1.0, r))


def compute_sharpness_correlation(trades: list[dict]) -> dict:
    """Compute per-dimension correlation with outcome.

    Returns dict with:
    - per_dimension: {dim_name: {r, mean_win, mean_loss, n}} for each dimension
    - composite: {r, mean_win, mean_loss} for sharpness_score
    - band_win_rates: {band: {wins, losses, win_rate, avg_pnl}}
    - n_trades, n_wins, n_losses, overall_win_rate
    """
    if not trades:
        return {"n_trades": 0, "per_dimension": {}, "composite": {}, "band_win_rates": {}}

    outcomes = [t["outcome"] for t in trades]
    n_trades = len(trades)
    n_wins = sum(outcomes)
    n_losses = n_trades - n_wins

    # Composite score correlation
    scores = [float(t["sharpness_score"]) for t in trades]
    composite_r = _point_biserial_correlation(scores, outcomes)
    win_scores = [s for s, o in zip(scores, outcomes) if o == 1]
    loss_scores = [s for s, o in zip(scores, outcomes) if o == 0]

    # Per-dimension correlation
    per_dim = {}
    for dim in SHARPNESS_DIMENSIONS:
        values = [float(t.get(dim, 0.0)) for t in trades]
        r = _point_biserial_correlation(values, outcomes)
        win_vals = [v for v, o in zip(values, outcomes) if o == 1]
        loss_vals = [v for v, o in zip(values, outcomes) if o == 0]
        per_dim[dim] = {
            "r": round(r, 4),
            "mean_win": round(sum(win_vals) / len(win_vals), 2) if win_vals else 0.0,
            "mean_loss": round(sum(loss_vals) / len(loss_vals), 2) if loss_vals else 0.0,
            "n": len(values),
        }

    # Band-level win rates
    band_stats: dict[str, dict] = {}
    for t in trades:
        band = t.get("sharpness_band", "normal")
        if band not in band_stats:
            band_stats[band] = {"wins": 0, "losses": 0, "pnl_sum": 0.0}
        if t["outcome"] == 1:
            band_stats[band]["wins"] += 1
        else:
            band_stats[band]["losses"] += 1
        band_stats[band]["pnl_sum"] += t.get("pnl_usd", 0.0)
    band_win_rates = {}
    for band, s in band_stats.items():
        total = s["wins"] + s["losses"]
        band_win_rates[band] = {
            "wins": s["wins"],
            "losses": s["losses"],
            "win_rate": round(s["wins"] / total, 4) if total > 0 else 0.0,
            "avg_pnl": round(s["pnl_sum"] / total, 4) if total > 0 else 0.0,
            "total": total,
        }

    return {
        "n_trades": n_trades,
        "n_wins": n_wins,
        "n_losses": n_losses,
        "overall_win_rate": round(n_wins / n_trades, 4) if n_trades > 0 else 0.0,
        "composite": {
            "r": round(composite_r, 4),
            "mean_win": round(sum(win_scores) / len(win_scores), 2) if win_scores else 0.0,
            "mean_loss": round(sum(loss_scores) / len(loss_scores), 2) if loss_scores else 0.0,
        },
        "per_dimension": per_dim,
        "band_win_rates": band_win_rates,
    }


# ---------------------------------------------------------------------------
# 3. Weight Auto-Calibration Recommendations
# ---------------------------------------------------------------------------

def compute_weight_recommendations(
    correlation: dict,
    *,
    current_weights: Optional[dict] = None,
    max_step: float = 0.15,
    min_weight: float = 0.5,
    max_weight: float = 2.0,
    min_trades: int = 10,
) -> dict:
    """Generate weight adjustment recommendations based on correlation data.

    Dimensions with strong positive r → increase weight.
    Dimensions with negative r → decrease weight.
    Step size scales with |r|, capped at max_step per cycle.

    Returns {recommendations: [{dim, current, recommended, r, reason}], apply: bool}
    """
    per_dim = correlation.get("per_dimension") or {}
    n_trades = int(correlation.get("n_trades", 0) or 0)
    cw = dict(current_weights or _WEIGHT_KEY_DEFAULTS)

    if n_trades < min_trades:
        return {
            "apply": False,
            "reason": f"insufficient_trades:{n_trades}<{min_trades}",
            "recommendations": [],
        }

    recommendations = []
    for dim in SHARPNESS_DIMENSIONS:
        dim_data = per_dim.get(dim) or {}
        r = float(dim_data.get("r", 0.0) or 0.0)
        config_key = DIMENSION_TO_WEIGHT_KEY[dim]
        current_w = _safe_float(cw.get(config_key), 1.0)

        # Scale step by |r|, capped at max_step
        step = min(max_step, abs(r) * 0.5)
        if abs(r) < 0.05:
            # Too weak to act on
            recommendations.append({
                "dimension": dim,
                "config_key": config_key,
                "current": round(current_w, 3),
                "recommended": round(current_w, 3),
                "r": r,
                "action": "hold",
                "reason": f"weak_signal:r={r:.4f}",
            })
            continue

        if r > 0:
            new_w = min(max_weight, current_w + step)
            action = "increase"
            reason = f"positive_correlation:r={r:.4f}"
        else:
            new_w = max(min_weight, current_w - step)
            action = "decrease"
            reason = f"negative_correlation:r={r:.4f}"

        recommendations.append({
            "dimension": dim,
            "config_key": config_key,
            "current": round(current_w, 3),
            "recommended": round(new_w, 3),
            "r": r,
            "action": action,
            "reason": reason,
        })

    has_changes = any(rec["action"] != "hold" for rec in recommendations)
    return {
        "apply": has_changes,
        "reason": "actionable_correlations" if has_changes else "no_actionable_signals",
        "recommendations": recommendations,
        "n_trades": n_trades,
    }


# ---------------------------------------------------------------------------
# 4. Family Performance Decay Detector
# ---------------------------------------------------------------------------

def detect_family_decay(
    conn: sqlite3.Connection,
    *,
    recent_trades: int = 20,
    baseline_trades: int = 60,
    decay_threshold: float = 0.15,
    min_recent: int = 6,
    symbol: str = "XAUUSD",
) -> dict:
    """Detect families whose recent win rate has decayed significantly.

    Compares last `recent_trades` vs previous `baseline_trades` win rate.
    If drop > decay_threshold → flag for risk reduction.

    Returns {families: [{family, recent_wr, baseline_wr, decay, action, ...}], alerts: [...]}
    """
    try:
        rows = conn.execute(
            """
            SELECT j.source, d.outcome, d.pnl_usd, d.execution_utc
            FROM ctrader_deals d
            JOIN execution_journal j ON j.id = d.journal_id
            WHERE d.has_close_detail = 1
              AND d.outcome IN (0, 1)
              AND j.symbol = ?
            ORDER BY d.execution_utc DESC
            """,
            (symbol,),
        ).fetchall()
    except Exception as e:
        logger.debug("[FamilyDecay] query error: %s", e)
        return {"families": [], "alerts": []}

    # Group by family (extracted from source)
    family_trades: dict[str, list[dict]] = {}
    for row in rows:
        source = str(row["source"] or "")
        # Extract family from source like "scalp_xauusd:xau_scalp_pullback_limit:canary"
        parts = source.split(":")
        family = parts[1] if len(parts) >= 2 else parts[0] if parts else source
        if family not in family_trades:
            family_trades[family] = []
        family_trades[family].append({
            "outcome": int(row["outcome"]),
            "pnl_usd": _safe_float(row["pnl_usd"]),
        })

    families = []
    alerts = []
    for family, trades in family_trades.items():
        n_total = len(trades)
        if n_total < min_recent:
            continue
        # Trades are ordered DESC (newest first)
        recent = trades[:min(recent_trades, n_total)]
        baseline_start = min(recent_trades, n_total)
        baseline_end = min(baseline_start + baseline_trades, n_total)
        baseline = trades[baseline_start:baseline_end]

        recent_wins = sum(1 for t in recent if t["outcome"] == 1)
        recent_wr = recent_wins / len(recent) if recent else 0.0
        recent_pnl = sum(t["pnl_usd"] for t in recent)

        if len(baseline) < min_recent:
            # Not enough baseline data to compare
            families.append({
                "family": family,
                "recent_trades": len(recent),
                "recent_win_rate": round(recent_wr, 4),
                "recent_pnl": round(recent_pnl, 2),
                "baseline_trades": len(baseline),
                "baseline_win_rate": 0.0,
                "decay": 0.0,
                "action": "monitor",
                "reason": "insufficient_baseline",
            })
            continue

        baseline_wins = sum(1 for t in baseline if t["outcome"] == 1)
        baseline_wr = baseline_wins / len(baseline)
        decay = baseline_wr - recent_wr

        if decay >= decay_threshold and recent_wr < 0.45:
            action = "reduce_risk"
            reason = f"decay:{decay:.2%}_below_45%_wr"
        elif decay >= decay_threshold:
            action = "alert"
            reason = f"decay:{decay:.2%}"
        elif recent_wr < 0.30 and len(recent) >= min_recent:
            action = "pause"
            reason = f"critical_wr:{recent_wr:.2%}"
        else:
            action = "ok"
            reason = ""

        entry = {
            "family": family,
            "recent_trades": len(recent),
            "recent_win_rate": round(recent_wr, 4),
            "recent_pnl": round(recent_pnl, 2),
            "baseline_trades": len(baseline),
            "baseline_win_rate": round(baseline_wr, 4),
            "decay": round(decay, 4),
            "action": action,
            "reason": reason,
        }
        families.append(entry)
        if action in ("reduce_risk", "pause", "alert"):
            alerts.append(entry)

    return {
        "families": sorted(families, key=lambda f: f.get("decay", 0.0), reverse=True),
        "alerts": alerts,
        "checked_utc": _iso(),
    }


# ---------------------------------------------------------------------------
# 5. Full Sharpness Feedback Report (combines all 3)
# ---------------------------------------------------------------------------

def build_sharpness_feedback_report(
    conn: sqlite3.Connection,
    *,
    days: int = 14,
    symbol: str = "XAUUSD",
    current_weights: Optional[dict] = None,
    min_trades_for_calibration: int = 10,
    decay_recent_trades: int = 20,
    decay_baseline_trades: int = 60,
    decay_threshold: float = 0.15,
) -> dict:
    """Build complete sharpness feedback report.

    Returns {correlation, calibration, family_decay, summary, checked_utc}
    """
    trades = query_resolved_trades_with_sharpness(conn, days=days, symbol=symbol)
    correlation = compute_sharpness_correlation(trades)
    calibration = compute_weight_recommendations(
        correlation,
        current_weights=current_weights,
        min_trades=min_trades_for_calibration,
    )
    family_decay = detect_family_decay(
        conn,
        recent_trades=decay_recent_trades,
        baseline_trades=decay_baseline_trades,
        decay_threshold=decay_threshold,
        symbol=symbol,
    )

    # Summary
    composite_r = float((correlation.get("composite") or {}).get("r", 0.0) or 0.0)
    n_alerts = len(family_decay.get("alerts") or [])
    calibrate_ready = bool(calibration.get("apply"))

    summary = {
        "n_trades_with_sharpness": len(trades),
        "composite_r": round(composite_r, 4),
        "calibration_ready": calibrate_ready,
        "n_decay_alerts": n_alerts,
        "strongest_dimension": "",
        "weakest_dimension": "",
    }

    per_dim = correlation.get("per_dimension") or {}
    if per_dim:
        sorted_dims = sorted(per_dim.items(), key=lambda x: x[1].get("r", 0.0), reverse=True)
        summary["strongest_dimension"] = sorted_dims[0][0] if sorted_dims else ""
        summary["weakest_dimension"] = sorted_dims[-1][0] if sorted_dims else ""

    return {
        "ok": True,
        "checked_utc": _iso(),
        "summary": summary,
        "correlation": correlation,
        "calibration": calibration,
        "family_decay": family_decay,
    }


def format_sharpness_feedback_text(report: dict) -> str:
    """Format sharpness feedback report as Telegram-friendly text."""
    summary = report.get("summary") or {}
    corr = report.get("correlation") or {}
    cal = report.get("calibration") or {}
    decay = report.get("family_decay") or {}

    lines = ["\U0001f4ca Sharpness Feedback Report"]
    n = summary.get("n_trades_with_sharpness", 0)
    if n == 0:
        lines.append("No trades with sharpness data yet.")
        return "\n".join(lines)

    comp = corr.get("composite") or {}
    lines.append(f"Trades: {n} | WR: {corr.get('overall_win_rate', 0):.0%}")
    lines.append(f"Score r={comp.get('r', 0):.3f} (win avg={comp.get('mean_win', 0):.0f}, loss avg={comp.get('mean_loss', 0):.0f})")

    # Band breakdown
    bands = corr.get("band_win_rates") or {}
    if bands:
        band_parts = []
        for band in ("knife", "caution", "normal", "sharp"):
            b = bands.get(band)
            if b and b.get("total", 0) > 0:
                band_parts.append(f"{band}:{b['win_rate']:.0%}({b['total']})")
        if band_parts:
            lines.append("Bands: " + " | ".join(band_parts))

    # Dimension ranking
    per_dim = corr.get("per_dimension") or {}
    if per_dim:
        sorted_dims = sorted(per_dim.items(), key=lambda x: x[1].get("r", 0.0), reverse=True)
        dim_parts = [f"{d[0].split('_')[0]}={d[1]['r']:+.3f}" for d in sorted_dims]
        lines.append("Dims: " + " ".join(dim_parts))

    # Calibration
    if cal.get("apply"):
        recs = cal.get("recommendations") or []
        changes = [r for r in recs if r.get("action") != "hold"]
        if changes:
            lines.append(f"\U0001f527 Calibration: {len(changes)} weight adjustments ready")

    # Decay alerts
    alerts = decay.get("alerts") or []
    if alerts:
        lines.append(f"\U000026a0 Decay: {len(alerts)} families flagged")
        for a in alerts[:3]:
            lines.append(f"  {a['family']}: WR {a['recent_win_rate']:.0%} (was {a['baseline_win_rate']:.0%}) → {a['action']}")

    return "\n".join(lines)
