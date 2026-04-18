"""
openclaw/agents/optimization_agent.py — Parameter Optimization Agent

Uses AI (Gemini primary, Ollama fallback) to reason over performance + regime
findings and propose specific parameter changes. All proposals are routed
through the PTS safety gate (never applied directly).

Proposal types supported:
  - confidence_threshold: tighten/loosen per-family min confidence
  - risk_per_trade: adjust CTRADER_RISK for a family
  - (future) session_filter: enable/disable family for a session
"""

import json
import logging
import urllib.request
from typing import Any

from openclaw.agents.base import AgentResult, BaseAgent

logger = logging.getLogger(__name__)

# Only touch confidence params — risk params need explicit user approval
# Family names come from live_profile_autopilot._collect_family_closed_rows()
_TUNABLE_PARAMS: dict[str, str] = {
    # XAU families
    "xau_scalp_pullback_limit": "XAU_DIRECT_LANE_MIN_CONFIDENCE",
    "xau_scalp_tick_depth_filter": "XAU_TDF_MIN_CONFIDENCE",
    "xau_scalp_microtrend_follow_up": "XAU_MFU_MIN_CONFIDENCE",
    "xau_scalp_flow_short_sidecar": "XAU_FLOW_SHORT_SIDECAR_MIN_CONFIDENCE",
    "xau_scalp_failed_fade_follow_stop": "XAU_FFFS_MIN_CONFIDENCE",
    "xau_scalp_range_repair": "XAU_RANGE_REPAIR_MIN_CONFIDENCE",
    # BTC families — real calibration names
    "btc_weekend_winner": "CTRADER_BTC_WINNER_MIN_CONFIDENCE",       # source: scalp_btcusd:canary
    "btc_weekday_lob_momentum": "BTC_WEEKDAY_LOB_MIN_CONFIDENCE",    # weekday LOB
    # ETH families — real calibration names
    "eth_weekend_winner": "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND",     # source: scalp_ethusd:canary
    "eth_weekday_overlap_probe": "ETH_WEEKDAY_PROBE_MIN_CONFIDENCE", # weekday probe
}

# Confidence value ceilings per param (from config AUTO_APPLY_*_MAX)
_PARAM_CEILINGS: dict[str, str] = {
    "CTRADER_BTC_WINNER_MIN_CONFIDENCE": "AUTO_APPLY_BTC_WEEKEND_CONFIDENCE_MAX",
    "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND": "AUTO_APPLY_ETH_WEEKEND_CONFIDENCE_MAX",
    "XAU_DIRECT_LANE_MIN_CONFIDENCE": "XAU_DIRECT_LANE_TRIAL_MAX_CONF_CEIL",
}

# Weekend-only crypto families
_CRYPTO_FAMILIES = {"btc_weekend_winner", "eth_weekend_winner", "btc_weekday_lob_momentum", "eth_weekday_overlap_probe"}

_SYSTEM_PROMPT = """You are Dexter Pro's Parameter Optimization Agent — an expert autonomous
trading system optimizer. You receive structured performance data and market regime context,
then propose SPECIFIC, CONSERVATIVE parameter adjustments.

Rules:
1. Only propose changes when data is clear (resolved >= 5 trades).
2. Step size for confidence: ±0.5 maximum per cycle.
3. Never propose loosening if win_rate < 0.50 with >= 5 trades.
4. Never propose tightening if win_rate > 0.65 with >= 5 trades.
5. Prefer "skip" over uncertain changes.
6. Output ONLY valid JSON — no commentary.

Output format:
{
  "proposals": [
    {
      "param": "XAU_DIRECT_LANE_MIN_CONFIDENCE",
      "family": "xau_scalp_pullback_limit",
      "current_value": 71.5,
      "proposed_value": 71.0,
      "direction": "loosen",
      "reason": "win_rate=0.67 with 8 resolved trades, regime=trending_bull"
    }
  ],
  "skip_reason": ""
}"""


def _build_prompt(perf_findings: dict, regime_findings: dict) -> str:
    from datetime import datetime, timezone
    family_scores = list(perf_findings.get("family_scores") or [])
    regime = str(regime_findings.get("regime", "unknown"))
    recommended = list(regime_findings.get("recommended_families") or [])
    bottom = [f.get("family", "") for f in (perf_findings.get("bottom_performers") or [])]
    top = [f.get("family", "") for f in (perf_findings.get("top_performers") or [])]
    is_weekend = datetime.now(timezone.utc).weekday() >= 5

    # On weekends prioritize crypto families; weekdays include all
    if is_weekend:
        tunable_scores = [
            f for f in family_scores
            if f.get("family") in _TUNABLE_PARAMS
            and f.get("resolved", 0) >= 3  # lower bar on weekend — fewer fills
        ]
        focus_note = (
            "WEEKEND MODE: XAU market is closed. Focus ONLY on BTC and ETH families. "
            "BTC/ETH trade 24/7. Even 3+ resolved trades are sufficient for weekend tuning."
        )
    else:
        tunable_scores = [
            f for f in family_scores
            if f.get("family") in _TUNABLE_PARAMS and f.get("resolved", 0) >= 5
        ]
        focus_note = "Weekday mode: XAU + BTC/ETH all active."

    context = {
        "regime": regime,
        "is_weekend": is_weekend,
        "focus_note": focus_note,
        "recommended_families": recommended,
        "top_performers": top,
        "bottom_performers": bottom,
        "family_performance": tunable_scores,
        "tunable_params": _TUNABLE_PARAMS,
    }

    return (
        "Analyze this Dexter Pro performance snapshot and propose parameter adjustments:\n\n"
        + json.dumps(context, indent=2)
        + "\n\nRespond with JSON only."
    )


class OptimizationAgent(BaseAgent):
    """
    Receives findings from PerformanceAgent + RegimeAgent.
    Uses Gemini (or Ollama fallback) to reason over the data.
    Routes all proposals through PTS via _propose_parameter_trial().

    Findings:
      - ai_proposals_raw: raw JSON from AI
      - proposals_routed: list of trial IDs created in PTS
      - proposals_skipped: list of proposals rejected by local validation
    """

    name = "optimization_agent"

    def run(self, context: dict) -> AgentResult:
        perf_findings = context.get("performance_findings") or {}
        regime_findings = context.get("regime_findings") or {}
        skip_reason = ""  # may be set by AI path

        if not perf_findings:
            return self._skip("no performance findings in context")

        # ── build AI prompt ─────────────────────────────────────────────────
        prompt = _build_prompt(perf_findings, regime_findings)

        # Log what tunable data we have before AI call
        family_scores = perf_findings.get("family_scores") or []
        from datetime import datetime, timezone
        is_weekend = datetime.now(timezone.utc).weekday() >= 5
        min_resolved = 3 if is_weekend else 5
        tunable_count = sum(1 for f in family_scores if f.get("family") in _TUNABLE_PARAMS and f.get("resolved", 0) >= min_resolved)
        logger.info("[optimization_agent] tunable families with data: %d/%d (weekend=%s)", tunable_count, len(family_scores), is_weekend)

        if tunable_count == 0:
            return self._skip(f"no tunable families with >= {min_resolved} resolved trades")

        # ── call AI ─────────────────────────────────────────────────────────
        ai_raw = self._call_ai(prompt)

        if ai_raw:
            # ── parse AI JSON ────────────────────────────────────────────────
            try:
                ai_json = self._extract_json(ai_raw)
                ai_proposals = list(ai_json.get("proposals") or [])
                skip_reason = str(ai_json.get("skip_reason") or "").strip()
                logger.info("[optimization_agent] AI proposals: %d | skip_reason: %s", len(ai_proposals), skip_reason or "none")
                if not ai_proposals:
                    return self._skip(skip_reason or "AI proposed no changes")
            except Exception as exc:
                logger.warning("[optimization_agent] AI JSON parse error: %s | falling back to rules", exc)
                ai_proposals = self._rule_based_proposals(perf_findings, regime_findings)
        else:
            logger.warning("[optimization_agent] All AI providers failed — using rule-based proposals")
            ai_proposals = self._rule_based_proposals(perf_findings, regime_findings)

        if not ai_proposals:
            return self._skip("no proposals from AI or rules")

        # ── validate + route through PTS ─────────────────────────────────────
        try:
            from learning.live_profile_autopilot import live_profile_autopilot as lpa
            from config import config
        except Exception as exc:
            return self._error(f"import error: {exc}")

        routed: list[dict] = []
        skipped: list[dict] = []

        for prop in ai_proposals:
            try:
                param = str(prop.get("param", "")).strip()
                current_value = prop.get("current_value")
                proposed_value = prop.get("proposed_value")
                direction = str(prop.get("direction", "")).strip()
                reason = str(prop.get("reason", "ai_optimization_agent")).strip()

                if not param or proposed_value is None:
                    skipped.append({**prop, "skip_reason": "missing param or proposed_value"})
                    continue

                # Safety: step size check
                try:
                    if abs(float(proposed_value) - float(current_value or 0)) > 1.5:
                        skipped.append({**prop, "skip_reason": "step_too_large"})
                        continue
                except Exception:
                    pass

                # Safety: ceiling check
                ceiling_key = _PARAM_CEILINGS.get(param)
                if ceiling_key:
                    try:
                        ceiling = float(getattr(config, ceiling_key, 82.0) or 82.0)
                        if float(proposed_value) > ceiling:
                            skipped.append({**prop, "skip_reason": f"above_ceiling_{ceiling}"})
                            continue
                    except Exception:
                        pass

                # Get real current value from config
                real_current = getattr(config, param, None)
                if real_current is None:
                    skipped.append({**prop, "skip_reason": f"param_not_in_config: {param}"})
                    continue

                tid = lpa._propose_parameter_trial(
                    param=param,
                    current_value=real_current,
                    proposed_value=proposed_value,
                    direction=direction,
                    reason=f"[opt_agent] {reason}",
                    source="optimization_agent",
                )
                if tid:
                    routed.append({"trial_id": tid, "param": param, "proposed_value": proposed_value})
                    logger.info("[optimization_agent] trial proposed: %s → %s=%s", tid, param, proposed_value)
                else:
                    skipped.append({**prop, "skip_reason": "pts_rejected_duplicate_or_cap"})

            except Exception as exc:
                skipped.append({**prop, "skip_reason": str(exc)})

        findings = {
            "ai_proposals_raw": ai_proposals,
            "proposals_routed": routed,
            "proposals_skipped": skipped,
            "ai_skip_reason": skip_reason,
        }

        confidence = min(1.0, len(routed) / max(1, len(ai_proposals)))
        return self._ok(findings=findings, proposals=routed, confidence=confidence)

    # ── AI call ──────────────────────────────────────────────────────────────

    def _call_ai(self, prompt: str) -> str:
        """Call AI — tries OpenClaw gateway → Groq → OpenRouter → Gemini in order. Returns empty on all failures."""
        from agent.brain import DexterBrain
        from config import config

        messages = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ]
        brain = DexterBrain()

        # ── OpenClaw local gateway (2026.3.24 OpenAI-compat endpoint) ────
        # Fastest path: local gateway at port 18789 handles provider failover.
        # Only active if OPENCLAW_GATEWAY_URL is set (e.g. http://localhost:18789)
        gateway_url = str(getattr(config, "OPENCLAW_GATEWAY_URL", "") or "").strip().rstrip("/")
        if gateway_url:
            try:
                import urllib.request, json as _json
                payload = _json.dumps({
                    "model": "auto",
                    "messages": messages,
                    "max_tokens": 800,
                    "temperature": 0.1,
                }).encode()
                req = urllib.request.Request(
                    f"{gateway_url}/v1/chat/completions",
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=12) as resp:
                    data = _json.loads(resp.read())
                content = data["choices"][0]["message"]["content"]
                if content:
                    logger.info("[optimization_agent] OpenClaw gateway OK: %d chars", len(content))
                    return str(content).strip()
            except Exception as exc:
                logger.debug("[optimization_agent] OpenClaw gateway skip: %s", exc)

        # ── Qwen-Plus DashScope (conductor-grade — check budget first) ──────────
        qwen_key = str(getattr(config, "QWEN_API_KEY", "") or "").strip()
        if qwen_key:
            try:
                from openclaw.token_budget import is_blocked as _budget_blocked, record_usage as _record
                qwen_model_direct = str(getattr(config, "QWEN_MODEL", "") or "qwen-plus")
                if _budget_blocked(qwen_model_direct):
                    logger.info("[optimization_agent] Qwen/%s budget at 95%% — skipping to Groq", qwen_model_direct)
                else:
                    qwen_base = str(getattr(config, "QWEN_BASE_URL", "") or "https://dashscope-intl.aliyuncs.com/compatible-mode/v1").rstrip("/")
                    payload = json.dumps({
                        "model": qwen_model_direct,
                        "messages": messages,
                        "max_tokens": 800,
                        "temperature": 0.1,
                    }).encode()
                    req = urllib.request.Request(
                        f"{qwen_base}/chat/completions",
                        data=payload,
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {qwen_key}"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=15) as resp:
                        data = json.loads(resp.read())
                    content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
                    usage = data.get("usage") or {}
                    if content:
                        _record(qwen_model_direct, usage.get("prompt_tokens", 1100), usage.get("completion_tokens", 400))
                        logger.info("[optimization_agent] Qwen/%s OK: %d chars", qwen_model_direct, len(content))
                        return str(content).strip()
            except Exception as exc:
                logger.warning("[optimization_agent] Qwen/DashScope error: %s", exc)

        # ── Qwen via OpenRouter (fallback — free, no extra key) ───────────────
        or_key = str(getattr(config, "OPENROUTER_API_KEY", "") or "").strip()
        if or_key:
            try:
                payload = json.dumps({
                    "model": "qwen/qwq-32b:free",
                    "messages": messages,
                    "max_tokens": 800,
                    "temperature": 0.1,
                }).encode()
                req = urllib.request.Request(
                    "https://openrouter.ai/api/v1/chat/completions",
                    data=payload,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {or_key}",
                        "HTTP-Referer": "https://github.com/dexter-pro",
                        "X-Title": "Dexter Pro Optimization Agent",
                    },
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=20) as resp:
                    data = json.loads(resp.read())
                content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
                if content:
                    logger.info("[optimization_agent] Qwen/OpenRouter OK: %d chars", len(content))
                    return str(content).strip()
            except Exception as exc:
                logger.warning("[optimization_agent] Qwen/OpenRouter error: %s", exc)

        # ── Groq QwQ-32B (reasoning model, free, no quota) ───────────────────
        # QwQ is Qwen's reasoning model — better than llama for structured JSON.
        # Falls back to llama if QwQ is unavailable/rate-limited.
        groq_key = str(getattr(config, "GROQ_API_KEY", "") or "")
        logger.debug("[optimization_agent] Groq key present: %s", bool(groq_key))
        if groq_key:
            # Try QwQ-32B first (reasoning model)
            for groq_model in ["qwen-qwq-32b", "deepseek-r1-distill-llama-70b", "llama-3.3-70b-versatile"]:
                try:
                    payload = json.dumps({
                        "model": groq_model,
                        "messages": messages,
                        "max_tokens": 1200,
                        "temperature": 0.1,
                    }).encode()
                    req = urllib.request.Request(
                        "https://api.groq.com/openai/v1/chat/completions",
                        data=payload,
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {groq_key}"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=25) as resp:
                        data = json.loads(resp.read())
                    content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
                    if content:
                        logger.info("[optimization_agent] Groq/%s OK: %d chars", groq_model, len(content))
                        return str(content).strip()
                except Exception as exc:
                    logger.debug("[optimization_agent] Groq/%s skip: %s", groq_model, exc)
                    continue

        # ── OpenRouter (free models) ──────────────────────────────────────
        if config.OPENROUTER_API_KEY:
            try:
                result = brain._chat_openai_compat(messages=messages, provider="openrouter", max_tokens=800, temperature=0.1)
                if result:
                    logger.info("[optimization_agent] OpenRouter OK: %d chars", len(result))
                    return str(result).strip()
            except Exception as exc:
                logger.warning("[optimization_agent] OpenRouter error: %s", exc)

        # ── Gemini native (billing required) ─────────────────────────────
        if config.has_gemini_key():
            try:
                result = brain._chat_gemini_native(messages=messages, max_tokens=800, temperature=0.1)
                if result:
                    logger.info("[optimization_agent] Gemini OK: %d chars", len(result))
                    return str(result).strip()
            except Exception as exc:
                logger.warning("[optimization_agent] Gemini error: %s", exc)

        return ""

    @staticmethod
    def _rule_based_proposals(perf_findings: dict, regime_findings: dict) -> list[dict]:
        """
        Deterministic confidence tuning when AI is unavailable.
        Rules mirror auto_tune_xau_direct_lane logic:
          WR < 0.42 + resolved >= 3 → TIGHTEN (+0.5 conf)
          WR >= 0.62 + resolved >= 3 → LOOSEN (-0.5 conf)
        """
        from datetime import datetime, timezone
        from config import config
        is_weekend = datetime.now(timezone.utc).weekday() >= 5
        min_resolved = 3 if is_weekend else 5
        tighten_wr = float(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_WIN_RATE", 0.42))
        loosen_wr = float(getattr(config, "XAU_DIRECT_LANE_AUTO_TUNE_LOOSEN_MIN_WIN_RATE", 0.62))

        family_scores = list(perf_findings.get("family_scores") or [])
        proposals = []

        for fam in family_scores:
            family_name = str(fam.get("family", ""))
            resolved = int(fam.get("resolved", 0) or 0)
            wr = float(fam.get("win_rate", 0.0) or 0.0)

            if resolved < min_resolved:
                continue
            if family_name not in _TUNABLE_PARAMS:
                continue

            param = _TUNABLE_PARAMS[family_name]
            current = getattr(config, param, None)
            if current is None:
                continue

            current_f = float(current)
            if wr < tighten_wr:
                proposed = round(current_f + 0.5, 1)
                reason = f"rule: wr={wr:.2f}<{tighten_wr} resolved={resolved}"
                proposals.append({"param": param, "family": family_name, "current_value": current_f,
                                   "proposed_value": proposed, "direction": "tighten", "reason": reason})
            elif wr >= loosen_wr:
                proposed = round(current_f - 0.5, 1)
                reason = f"rule: wr={wr:.2f}>={loosen_wr} resolved={resolved}"
                proposals.append({"param": param, "family": family_name, "current_value": current_f,
                                   "proposed_value": proposed, "direction": "loosen", "reason": reason})

        if proposals:
            logger.info("[optimization_agent] rule-based proposals: %d", len(proposals))
        return proposals

    @staticmethod
    def _extract_json(text: str) -> dict:
        """Extract first JSON object from AI response."""
        import re
        # Strip markdown code fences
        cleaned = re.sub(r"```(?:json)?", "", text).strip().strip("`").strip()
        # Find JSON block
        start = cleaned.find("{")
        end = cleaned.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(cleaned[start:end])
        return json.loads(cleaned)
