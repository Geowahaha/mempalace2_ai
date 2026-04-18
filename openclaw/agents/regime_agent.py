"""
openclaw/agents/regime_agent.py — Regime Context Agent

Classifies current market regime from:
  - trading_manager_state.json (shock, micro_regime, execution_directive, cluster_loss)
  - Most recent XAU scanner snapshot (trend direction, volatility proxy)
  - Session timing (london/ny/asian/off-hours)

Outputs regime label + which families are optimal for current conditions.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from openclaw.agents.base import AgentResult, BaseAgent

logger = logging.getLogger(__name__)

# Regime labels
REGIME_TRENDING_BULL = "trending_bull"
REGIME_TRENDING_BEAR = "trending_bear"
REGIME_RANGING = "ranging"
REGIME_VOLATILE_EXPANSION = "volatile_expansion"
REGIME_NEWS_SHOCK = "news_shock"
REGIME_OFF_HOURS = "off_hours"
REGIME_CRYPTO_WEEKEND = "crypto_weekend"  # Saturday/Sunday: XAU closed, crypto 24/7

# Per-regime family recommendations (canary & primary)
_REGIME_FAMILY_MAP: dict[str, list[str]] = {
    REGIME_TRENDING_BULL: [
        "xau_scalp_pullback_limit",
        "xau_scalp_microtrend_follow_up",
        "btc_weekday_lob_momentum",
        "eth_weekday_overlap_probe",
    ],
    REGIME_TRENDING_BEAR: [
        "xau_scalp_pullback_limit",
        "xau_scalp_flow_short_sidecar",
        "xau_scalp_failed_fade_follow_stop",
        "btc_weekday_lob_momentum",
    ],
    REGIME_RANGING: [
        "xau_scalp_range_repair",
        "xau_scalp_tick_depth_filter",
        "xau_scalp_pullback_limit",
    ],
    REGIME_VOLATILE_EXPANSION: [
        "xau_scalp_tick_depth_filter",  # MRD-gated, handles expansion safely
        "xau_scalp_failed_fade_follow_stop",
    ],
    REGIME_NEWS_SHOCK: [],  # shock mode — no new entries recommended
    REGIME_OFF_HOURS: [
        "xau_scalp_pullback_limit",  # scheduled only
    ],
    REGIME_CRYPTO_WEEKEND: [
        "btc_weekday_lob_momentum",   # BTC primary canary
        "eth_weekday_overlap_probe",  # ETH probe
        # BTC canary sub-families (always active on weekend)
        "xau_scalp_flow_short_sidecar",   # bfss maps to BTC FSS
        "xau_scalp_failed_fade_follow_stop",  # bfls maps to BTC FLS
        "xau_scalp_range_repair",         # brr maps to BTC RR
    ],
}


def _load_json_safe(path: Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


class RegimeAgent(BaseAgent):
    """
    Reads live runtime state to classify current market regime.
    Findings:
      - regime: one of the REGIME_* constants
      - regime_evidence: dict of raw signals used
      - recommended_families: list[str]
      - session: london | ny | asian | off_hours
      - shock_active: bool
      - cluster_loss_active: bool
    """

    name = "regime_agent"

    def run(self, context: dict) -> AgentResult:
        try:
            from config import config
        except Exception as exc:
            return self._error(f"import config: {exc}")

        # ── load trading manager state ──────────────────────────────────────
        try:
            runtime_dir = Path(getattr(config, "RUNTIME_DIR", "data/runtime"))
            state_path = runtime_dir / "trading_manager_state.json"
            tm_state = _load_json_safe(state_path)
        except Exception as exc:
            tm_state = {}
            logger.debug("[regime_agent] tm_state load error: %s", exc)

        shock_profile = dict(tm_state.get("xau_shock_profile") or {})
        shock_active = str(shock_profile.get("status", "inactive")).lower() == "active"

        cluster_guard = dict(tm_state.get("xau_cluster_loss_guard") or {})
        cluster_loss_active = str(cluster_guard.get("status", "inactive")).lower() == "active"

        micro_regime = dict(tm_state.get("xau_micro_regime") or {})
        micro_label = str(micro_regime.get("mode", "")).lower()

        exec_directive = dict(tm_state.get("xau_execution_directive") or {})
        exec_status = str(exec_directive.get("status", "inactive")).lower()
        exec_mode = str(exec_directive.get("mode", "")).lower()

        # ── determine session ───────────────────────────────────────────────
        session = self._current_session()

        # ── classify regime ─────────────────────────────────────────────────
        evidence: dict = {
            "shock_active": shock_active,
            "cluster_loss_active": cluster_loss_active,
            "micro_regime_mode": micro_label,
            "exec_directive_status": exec_status,
            "exec_directive_mode": exec_mode,
            "session": session,
        }

        # Priority order: shock > weekend_crypto > cluster_loss > micro_regime > directive > session
        if shock_active:
            regime = REGIME_NEWS_SHOCK
        elif session == "crypto_weekend":
            # Weekend: XAU market closed, focus exclusively on BTC/ETH
            # cluster_loss still blocks if crypto families are losing
            regime = REGIME_RANGING if cluster_loss_active else REGIME_CRYPTO_WEEKEND
        elif cluster_loss_active:
            regime = REGIME_RANGING  # conservative — wait for cluster to clear
        elif "recovery" in micro_label or "expansion" in micro_label:
            regime = REGIME_VOLATILE_EXPANSION
        elif exec_status == "active" and "bull" in exec_mode:
            regime = REGIME_TRENDING_BULL
        elif exec_status == "active" and ("bear" in exec_mode or "short" in exec_mode):
            regime = REGIME_TRENDING_BEAR
        elif session == "off_hours":
            regime = REGIME_OFF_HOURS
        else:
            # Default: use session heuristic
            regime = REGIME_RANGING if session == "asian" else REGIME_TRENDING_BULL

        recommended = list(_REGIME_FAMILY_MAP.get(regime, []))

        # Exclude bottom performers passed from context
        bottom_families = [f.get("family", "") for f in (context.get("bottom_performers") or [])]
        if bottom_families:
            recommended = [f for f in recommended if f not in bottom_families]

        findings = {
            "regime": regime,
            "regime_evidence": evidence,
            "recommended_families": recommended,
            "session": session,
            "shock_active": shock_active,
            "cluster_loss_active": cluster_loss_active,
        }

        confidence = 0.85 if shock_active or cluster_loss_active else 0.60
        return self._ok(findings=findings, confidence=confidence)

    # ── helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _current_session() -> str:
        now_utc = datetime.now(timezone.utc)
        hour = now_utc.hour
        weekday = now_utc.weekday()  # 0=Monday

        # Weekend: XAU closed, but BTC/ETH 24/7
        if weekday >= 5:
            return "crypto_weekend"

        # London: 07:00–17:00 UTC
        if 7 <= hour < 17:
            return "london"

        # NY overlap: 12:00–17:00 UTC (subset of london above)
        # NY solo: 17:00–21:00 UTC
        if 17 <= hour < 21:
            return "ny"

        # Asian: 00:00–07:00 UTC
        if 0 <= hour < 7:
            return "asian"

        # After NY close
        return "off_hours"
