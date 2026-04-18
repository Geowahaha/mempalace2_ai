"""
openclaw/agents/performance_agent.py — Performance Analysis Agent

Reads live_profile_autopilot build_* reports, scores every family by
win rate + PnL + drawdown + calibration error. Tags top/bottom performers.
Outputs findings for the Optimization Agent to reason over.
"""

import logging
from typing import Any

from openclaw.agents.base import AgentResult, BaseAgent

logger = logging.getLogger(__name__)

# Minimum trades to consider a family "meaningful" data
_MIN_RESOLVED = 4

# Score weights
_W_WIN_RATE = 0.40
_W_PNL = 0.30
_W_DRAWDOWN = 0.15
_W_CALIBRATION = 0.15


def _score_family(fam: dict) -> float:
    """Composite 0–1 score for a single family row."""
    overall = fam.get("overall") or {}
    resolved = int(overall.get("resolved", 0) or 0)
    if resolved < _MIN_RESOLVED:
        return 0.5  # neutral — not enough data

    win_rate = float(overall.get("win_rate", 0.0) or 0.0)
    pnl_usd = float(overall.get("pnl_usd", 0.0) or 0.0)
    max_dd = float(fam.get("max_drawdown_usd", 0.0) or 0.0)
    cal_error = float(fam.get("calibration_error", 0.0) or 0.0)
    uncertainty = float(fam.get("uncertainty_score", 1.0) or 1.0)

    # Normalize win rate: 0.4=0.0 → 0.7=1.0
    wr_score = max(0.0, min(1.0, (win_rate - 0.40) / 0.30))

    # Normalize pnl: clamp to [-50, +50] → [0, 1]
    pnl_score = max(0.0, min(1.0, (pnl_usd + 50.0) / 100.0))

    # Drawdown score: 0 DD = 1.0, -50 = 0.0
    dd_score = max(0.0, min(1.0, 1.0 + (max_dd / 50.0)))

    # Calibration: 0 ECE = 1.0, 0.5 ECE = 0.0
    cal_score = max(0.0, min(1.0, 1.0 - (cal_error * 2.0)))

    composite = (
        wr_score * _W_WIN_RATE
        + pnl_score * _W_PNL
        + dd_score * _W_DRAWDOWN
        + cal_score * _W_CALIBRATION
    )
    # Penalize high uncertainty
    composite *= max(0.4, 1.0 - (uncertainty * 0.4))
    return round(composite, 4)


class PerformanceAgent(BaseAgent):
    """
    Analyses calibration + canary reports to score all families.
    Findings returned:
      - family_scores: list sorted by score desc
      - top_performers: families with score >= 0.65
      - bottom_performers: families with score < 0.35 and resolved >= MIN_RESOLVED
      - canary_summary: total closed, win rate, PnL
      - winner_memory_summary: market-beating setups count
    """

    name = "performance_agent"

    def run(self, context: dict) -> AgentResult:
        try:
            from learning.live_profile_autopilot import live_profile_autopilot as lpa
        except Exception as exc:
            return self._error(f"import lpa: {exc}")

        try:
            cal_report = lpa.build_family_calibration_report(days=21)
        except Exception as exc:
            return self._error(f"build_family_calibration_report: {exc}")

        families_raw = list(cal_report.get("families") or [])
        if not families_raw:
            return self._skip("no family data in calibration report")

        # Score every family
        scored = []
        for fam in families_raw:
            score = _score_family(fam)
            overall = fam.get("overall") or {}
            scored.append({
                "symbol": fam.get("symbol", ""),
                "family": fam.get("family", ""),
                "score": score,
                "resolved": int(overall.get("resolved", 0) or 0),
                "win_rate": round(float(overall.get("win_rate", 0.0) or 0.0), 4),
                "pnl_usd": round(float(overall.get("pnl_usd", 0.0) or 0.0), 2),
                "max_drawdown_usd": round(float(fam.get("max_drawdown_usd", 0.0) or 0.0), 2),
                "calibration_error": round(float(fam.get("calibration_error", 0.0) or 0.0), 4),
                "uncertainty_score": round(float(fam.get("uncertainty_score", 1.0) or 1.0), 4),
                "deflated_sharpe": round(float(fam.get("deflated_sharpe_proxy", 0.0) or 0.0), 3),
            })

        scored.sort(key=lambda x: x["score"], reverse=True)

        top = [f for f in scored if f["score"] >= 0.65 and f["resolved"] >= _MIN_RESOLVED]
        bottom = [f for f in scored if f["score"] < 0.35 and f["resolved"] >= _MIN_RESOLVED]

        for f in scored:
            logger.info("[performance_agent] %s r=%d wr=%.2f pnl=%.1f score=%.3f",
                        f["family"], f["resolved"], f["win_rate"], f["pnl_usd"], f["score"])

        # Canary summary
        canary_summary: dict = {}
        try:
            canary_report = lpa.build_canary_post_trade_audit_report(days=14)
            canary_summary = dict((canary_report.get("summary") or {}).copy())
        except Exception as exc:
            logger.debug("[performance_agent] canary report skip: %s", exc)

        # Winner memory summary
        winner_summary: dict = {}
        try:
            winner_report = lpa.build_winner_memory_library_report(days=21)
            winner_summary = dict((winner_report.get("summary") or {}).copy())
        except Exception as exc:
            logger.debug("[performance_agent] winner memory skip: %s", exc)

        findings = {
            "family_scores": scored,
            "top_performers": top,
            "bottom_performers": bottom,
            "total_families_scored": len(scored),
            "canary_summary": canary_summary,
            "winner_memory_summary": winner_summary,
        }

        confidence = min(1.0, len([f for f in scored if f["resolved"] >= _MIN_RESOLVED]) / max(1, len(scored)))
        return self._ok(findings=findings, confidence=round(confidence, 3))
