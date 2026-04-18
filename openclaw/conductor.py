"""
openclaw/conductor.py — Conductor Agent (Master Orchestrator)

Runs all specialist agents on a timer, collects findings/proposals,
routes proposals to PTS, and sends a unified Telegram summary.

Called from scheduler.py every CONDUCTOR_INTERVAL_MIN (default 30 min).

Cycle order:
  1. RiskGuardAgent    — safety first, may emit emergency actions
  2. PerformanceAgent  — score all families
  3. RegimeAgent       — classify market regime (receives bottom performers)
  4. OpportunityFollow — activates FSS/FLS sidecar when behavioral v2 WR is high
  5. OptimizationAgent — AI proposes parameter changes (receives perf + regime)
  6. Telegram summary  — only if any agent produced proposals or alerts

Design:
  - Each agent gets _safe_run() — a single agent crash never stops the cycle
  - Findings flow forward: each agent receives previous agents' output in context
  - Emergency risk actions are reported immediately, regardless of other results
  - OpportunityFollow writes directly to trading_manager_state.json (safe path)
"""

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Conductor:
    """Master orchestrator. Instantiate once; call run_cycle() on each tick."""

    def __init__(self) -> None:
        from openclaw.agents.risk_guard_agent import RiskGuardAgent
        from openclaw.agents.performance_agent import PerformanceAgent
        from openclaw.agents.regime_agent import RegimeAgent
        from openclaw.agents.optimization_agent import OptimizationAgent

        self._risk_guard = RiskGuardAgent()
        self._performance = PerformanceAgent()
        self._regime = RegimeAgent()
        self._optimization = OptimizationAgent()

    def run_cycle(self) -> dict:
        """
        Execute one full conductor cycle.
        Returns a summary dict with all agent results.
        """
        started_at = _utc_now_iso()
        logger.info("[conductor] cycle start %s", started_at)

        results: dict[str, Any] = {}
        context: dict = {}

        # ── 1. Risk Guard (safety first, emergency actions bypass PTS) ────────
        rg_result = self._risk_guard._safe_run(context)
        results["risk_guard"] = _serialise(rg_result)

        risk_findings = rg_result.findings if rg_result.is_ok() else {}
        emergency_actions = list(risk_findings.get("emergency_actions_taken") or [])

        # Notify immediately on emergency
        if emergency_actions:
            self._notify_emergency(emergency_actions)

        context["risk_findings"] = risk_findings

        # ── 2. Performance Agent ─────────────────────────────────────────────
        perf_result = self._performance._safe_run(context)
        results["performance"] = _serialise(perf_result)

        perf_findings = perf_result.findings if perf_result.is_ok() else {}
        context["performance_findings"] = perf_findings
        # Pass bottom performers to regime agent for family exclusion
        context["bottom_performers"] = list(perf_findings.get("bottom_performers") or [])

        # ── 3. Regime Agent ──────────────────────────────────────────────────
        regime_result = self._regime._safe_run(context)
        results["regime"] = _serialise(regime_result)

        regime_findings = regime_result.findings if regime_result.is_ok() else {}
        context["regime_findings"] = regime_findings

        # ── 4. Opportunity Follow ────────────────────────────────────────────
        # When behavioral v2 (xau_scalp_pullback_limit) WR is high → activate
        # FSS/FFFS opportunity sidecar so the next scan cycle captures follow
        # entries. Only fires when no shock/cluster guard is active.
        shock_active = bool(risk_findings.get("shock_active"))
        cluster_guard = bool(risk_findings.get("cluster_guard_active"))

        follow_result = self._run_opportunity_follow(
            perf_findings=perf_findings,
            regime_findings=regime_findings,
            shock_active=shock_active,
            cluster_guard=cluster_guard,
        )
        results["opportunity_follow"] = follow_result
        context["opportunity_follow"] = follow_result

        # ── 5. Optimization Agent ────────────────────────────────────────────
        # Skip if shock active or cluster guard on — never propose changes during crisis
        if shock_active or cluster_guard:
            logger.info("[conductor] optimization skipped — shock=%s cluster_guard=%s", shock_active, cluster_guard)
            results["optimization"] = {"status": "skip", "reason": "shock_or_cluster_guard_active"}
        else:
            opt_result = self._optimization._safe_run(context)
            results["optimization"] = _serialise(opt_result)

        # ── 6. OpenClaw version guard (rate-limited to every 4h internally) ────
        try:
            from openclaw.version_guard import check_and_notify as _vg_check
            _vg_check()
        except Exception as exc:
            logger.debug("[conductor] version_guard skip: %s", exc)

        # ── 7. Compose Telegram summary ──────────────────────────────────────
        finished_at = _utc_now_iso()
        summary = self._build_summary(results, started_at, finished_at)

        if self._should_notify(results):
            self._send_telegram(summary)

        logger.info(
            "[conductor] cycle done — risk=%s perf=%s regime=%s opt=%s",
            results.get("risk_guard", {}).get("status"),
            results.get("performance", {}).get("status"),
            results.get("regime", {}).get("status"),
            results.get("optimization", {}).get("status"),
        )

        return {
            "ok": True,
            "started_at": started_at,
            "finished_at": finished_at,
            "results": results,
            "summary": summary,
        }

    # ── helpers ──────────────────────────────────────────────────────────────

    def _run_opportunity_follow(
        self,
        perf_findings: dict,
        regime_findings: dict,
        shock_active: bool,
        cluster_guard: bool,
    ) -> dict:
        """
        Activate opportunity sidecar (FSS + FFFS) when behavioral v2 WR is high.
        Writes directly to trading_manager_state.json — same safe path as RiskGuard.
        Only activates if CONDUCTOR_OPPORTUNITY_FOLLOW_ENABLED=1 (default=1).
        Auto-expires after CONDUCTOR_FOLLOW_EXPIRE_MIN minutes (default=90).
        """
        try:
            from config import config
            if not bool(getattr(config, "CONDUCTOR_OPPORTUNITY_FOLLOW_ENABLED", True)):
                return {"status": "disabled"}
            if shock_active or cluster_guard:
                return {"status": "skip", "reason": "shock_or_cluster_guard"}

            min_wr = float(getattr(config, "CONDUCTOR_FOLLOW_MIN_BEHAVIORAL_WR", 0.60) or 0.60)
            min_resolved = int(getattr(config, "CONDUCTOR_FOLLOW_MIN_RESOLVED", 4) or 4)
            expire_min = int(getattr(config, "CONDUCTOR_FOLLOW_EXPIRE_MIN", 90) or 90)

            # Find behavioral v2 (xau_scalp_pullback_limit) in family scores
            family_scores = list(perf_findings.get("family_scores") or [])
            behavioral = next(
                (f for f in family_scores if f.get("family") == "xau_scalp_pullback_limit"),
                None,
            )
            if not behavioral:
                return {"status": "skip", "reason": "no_behavioral_data"}

            resolved = int(behavioral.get("resolved", 0) or 0)
            wr = float(behavioral.get("win_rate", 0.0) or 0.0)
            pnl = float(behavioral.get("pnl_usd", 0.0) or 0.0)
            regime = str(regime_findings.get("regime", "") or "")

            if resolved < min_resolved:
                return {"status": "skip", "reason": f"resolved={resolved}<{min_resolved}"}

            # Determine which follow families to activate based on regime direction
            follow_families: list[str] = []
            if wr >= min_wr and pnl >= 0:
                # Trending bear: FSS (short, trend-aligned) + FFFS (high-conf bear reinforcement) + range repair
                if "bear" in regime or "ranging" in regime:
                    follow_families = ["xau_scalp_flow_short_sidecar", "xau_scalp_failed_fade_follow_stop", "xau_scalp_range_repair"]
                # Trending bull: FLS (long, trend-aligned)
                elif "bull" in regime or regime in ("trending_bull", "crypto_weekend"):
                    follow_families = ["xau_scalp_flow_long_sidecar"]
                else:
                    follow_families = ["xau_scalp_flow_long_sidecar"]

            if not follow_families:
                # Deactivate if WR has dropped below threshold
                deactivated = self._deactivate_opportunity_sidecar()
                return {"status": "deactivated" if deactivated else "inactive", "wr": wr, "resolved": resolved}

            # Activate sidecar
            activated = self._activate_opportunity_sidecar(
                families=follow_families,
                reason=f"behavioral_v2 WR={wr:.0%} n={resolved} PnL={pnl:.1f}",
                expire_min=expire_min,
            )
            result = {
                "status": "activated" if activated else "already_active",
                "families": follow_families,
                "behavioral_wr": round(wr, 3),
                "behavioral_resolved": resolved,
                "pnl_usd": round(pnl, 2),
                "regime": regime,
            }
            if activated:
                logger.info(
                    "[conductor] opportunity follow activated: families=%s wr=%.0f%% n=%d",
                    follow_families, wr * 100, resolved,
                )
            return result

        except Exception as exc:
            logger.debug("[conductor] opportunity follow error: %s", exc)
            return {"status": "error", "error": str(exc)}

    def _activate_opportunity_sidecar(self, families: list, reason: str, expire_min: int) -> bool:
        """Write xau_opportunity_sidecar active state to trading_manager_state.json."""
        try:
            from config import config
            from pathlib import Path
            import json
            runtime_dir = Path(getattr(config, "RUNTIME_DIR", "data/runtime"))
            state_path = runtime_dir / "trading_manager_state.json"
            try:
                with open(state_path, "r", encoding="utf-8") as f:
                    state = json.load(f)
            except Exception:
                state = {}
            from datetime import timedelta
            now_iso = _utc_now_iso()
            expire_iso = (
                datetime.now(timezone.utc) + timedelta(minutes=expire_min)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            existing = dict(state.get("xau_opportunity_sidecar") or {})
            if str(existing.get("status") or "") == "active":
                return False  # already active, no change
            state["xau_opportunity_sidecar"] = {
                "status": "active",
                "activated_at": now_iso,
                "expires_at": expire_iso,
                "mode": "behavioral_follow",
                "families": families,
                "reason": reason,
                "source": "conductor_opportunity_follow",
            }
            with open(state_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            return True
        except Exception as exc:
            logger.debug("[conductor] activate sidecar error: %s", exc)
            return False

    def _deactivate_opportunity_sidecar(self) -> bool:
        """Clear expired or low-WR opportunity sidecar from trading_manager_state.json."""
        try:
            from config import config
            from pathlib import Path
            import json
            runtime_dir = Path(getattr(config, "RUNTIME_DIR", "data/runtime"))
            state_path = runtime_dir / "trading_manager_state.json"
            try:
                with open(state_path, "r", encoding="utf-8") as f:
                    state = json.load(f)
            except Exception:
                return False
            existing = dict(state.get("xau_opportunity_sidecar") or {})
            if str(existing.get("status") or "") != "active":
                return False
            if str(existing.get("source") or "") != "conductor_opportunity_follow":
                return False  # don't touch manually-set sidecars
            state["xau_opportunity_sidecar"] = {"status": "inactive", "cleared_at": _utc_now_iso()}
            with open(state_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            return True
        except Exception as exc:
            logger.debug("[conductor] deactivate sidecar error: %s", exc)
            return False

    def _should_notify(self, results: dict) -> bool:
        """Send Telegram only if there's something worth reporting."""
        opt = results.get("optimization") or {}
        rg = results.get("risk_guard") or {}
        follow = results.get("opportunity_follow") or {}

        has_proposals = bool((opt.get("findings") or {}).get("proposals_routed"))
        has_emergency = bool((rg.get("findings") or {}).get("emergency_actions_taken"))
        has_bottom = bool((results.get("performance") or {}).get("findings", {}).get("bottom_performers"))
        has_follow = str(follow.get("status") or "") == "activated"

        return has_proposals or has_emergency or has_bottom or has_follow

    def _build_summary(self, results: dict, started_at: str, finished_at: str) -> str:
        lines: list[str] = []
        lines.append(f"[Conductor] Cycle {started_at}")
        lines.append("")

        # Risk Guard
        rg = results.get("risk_guard") or {}
        rg_findings = rg.get("findings") or {}
        emergencies = list(rg_findings.get("emergency_actions_taken") or [])
        if emergencies:
            lines.append("RISK GUARD — EMERGENCY")
            for ea in emergencies:
                lines.append(f"  {ea.get('action')} — {ea.get('reason', ea.get('source', ''))}")
            lines.append("")
        else:
            cluster_alerts = list(rg_findings.get("cluster_alerts") or [])
            dd_alert = bool(rg_findings.get("drawdown_velocity_alert"))
            open_pos = int(rg_findings.get("open_positions", 0) or 0)
            rg_tag = "WARN" if (cluster_alerts or dd_alert) else "OK"
            lines.append(f"Risk Guard [{rg_tag}] positions={open_pos}")
            if cluster_alerts:
                for ca in cluster_alerts[:3]:
                    lines.append(f"  WARN {ca.get('source')} x{ca.get('loss_count')} losses ({ca.get('total_pnl', 0):.1f} USD)")
            if dd_alert:
                lines.append(f"  WARN Drawdown velocity: {rg_findings.get('drawdown_usd_per_hour', 0):.1f} USD/hr")

        # Performance
        perf = results.get("performance") or {}
        perf_findings = perf.get("findings") or {}
        top = list(perf_findings.get("top_performers") or [])
        bottom = list(perf_findings.get("bottom_performers") or [])
        if top or bottom:
            lines.append("")
            lines.append("Performance")
            for f in top[:3]:
                lines.append(f"  TOP {f.get('family')} WR={f.get('win_rate', 0):.1%} PnL={f.get('pnl_usd', 0):.1f}")
            for f in bottom[:3]:
                lines.append(f"  LOW {f.get('family')} WR={f.get('win_rate', 0):.1%} PnL={f.get('pnl_usd', 0):.1f}")

        # Regime
        regime = results.get("regime") or {}
        regime_findings = regime.get("findings") or {}
        if regime_findings:
            lines.append("")
            regime_label = str(regime_findings.get("regime", "unknown"))
            session = str(regime_findings.get("session", ""))
            rec = list(regime_findings.get("recommended_families") or [])
            lines.append(f"Regime: {regime_label} ({session})")
            if rec:
                lines.append(f"  Active: {', '.join(rec[:4])}")

        # Opportunity Follow
        follow = results.get("opportunity_follow") or {}
        if str(follow.get("status") or "") == "activated":
            families = list(follow.get("families") or [])
            wr = float(follow.get("behavioral_wr", 0.0) or 0.0)
            lines.append("")
            lines.append(f"Follow Activated — behavioral WR={wr:.0%}")
            for fam in families[:3]:
                lines.append(f"  + {fam}")

        # Optimization
        opt = results.get("optimization") or {}
        opt_findings = opt.get("findings") or {}
        routed = list(opt_findings.get("proposals_routed") or [])
        if routed:
            lines.append("")
            lines.append(f"Optimization — {len(routed)} trial(s) proposed via PTS")
            for p in routed[:4]:
                lines.append(f"  trial {p.get('trial_id','?')[:8]}... {p.get('param')} -> {p.get('proposed_value')}")
            lines.append("Use /trials to review")

        return "\n".join(lines)

    def _send_telegram(self, text: str) -> None:
        """Send conductor summary — admin_bot (sync) primary, notifier fallback."""
        sent = False
        try:
            from config import config
            from notifier.admin_bot import admin_bot
            chat_id = str(getattr(config, "TELEGRAM_CHAT_ID", "") or "").strip()
            if chat_id and chat_id.lstrip("-").isdigit():
                admin_bot._send_text(int(chat_id), text)
                sent = True
        except Exception as exc:
            logger.debug("[conductor] admin_bot send error: %s", exc)
        if not sent:
            try:
                from notifier.telegram_bot import notifier
                notifier._send(text, parse_mode=None, feature="conductor")
            except Exception as exc:
                logger.debug("[conductor] notifier send error: %s", exc)

    def _notify_emergency(self, emergency_actions: list) -> None:
        lines = ["[RISK GUARD] EMERGENCY"]
        for ea in emergency_actions:
            lines.append(f"  {ea.get('action')}: {ea.get('reason', ea.get('source', ''))}")
        self._send_telegram("\n".join(lines))


def _serialise(result: Any) -> dict:
    """Convert AgentResult to plain dict for logging/JSON."""
    if result is None:
        return {"status": "none"}
    return {
        "agent_name": getattr(result, "agent_name", ""),
        "status": getattr(result, "status", ""),
        "confidence": getattr(result, "confidence", 0.0),
        "findings": dict(getattr(result, "findings", {}) or {}),
        "proposals": list(getattr(result, "proposals", []) or []),
        "error": getattr(result, "error", ""),
        "generated_at": getattr(result, "generated_at", ""),
    }


# Module-level singleton — created on first import
_conductor: Conductor | None = None


def get_conductor() -> Conductor:
    global _conductor
    if _conductor is None:
        _conductor = Conductor()
    return _conductor


def run_conductor_cycle() -> dict:
    """Entry point called from scheduler.py."""
    return get_conductor().run_cycle()
