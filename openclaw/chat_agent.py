"""
openclaw/chat_agent.py — Conversational AI interface for Dexter Pro

Answers free-form questions about the live trading system via Telegram /ask.
Builds real-time context from: family performance, regime, open positions,
PTS trials, trading manager state, and config thresholds.

Usage (Telegram):
  /ask วันนี้ระบบเป็นยังไงบ้าง?
  /ask which families are performing well?
  /ask should I approve the ETH trial?
  /ask explain the current XAU regime
"""

import json
import logging
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are Dexter Pro's AI Trading Assistant — the embedded intelligence
of an autonomous multi-strategy trading system running live 24/7.

Your role: answer the owner's questions about the trading system clearly and concisely.
You have access to real-time system state, family performance, open positions, and more.

Rules:
- Be direct and actionable. No fluff.
- Use numbers when available (WR %, PnL, confidence).
- If asked in Thai, answer in Thai. If English, answer in English.
- Flag anything that needs the owner's attention.
- You can recommend actions (/approve, /reject, /update_openclaw etc.) when relevant.
- This is real money — be honest about risks and uncertainties.
- Max response: 400 words. Keep it focused.
"""


def _collect_context() -> dict:
    """Gather live system state for AI context."""
    ctx: dict = {}

    # ── Family performance (current month, matches cTrader statement) ─────────
    # Query ctrader_deals directly (not via positions JOIN) so orphan deals
    # — positions opened while streamer was offline — are included correctly.
    try:
        import sqlite3
        db_path = Path(__file__).parent.parent / "data" / "ctrader_openapi.db"
        if db_path.exists():
            with sqlite3.connect(str(db_path), timeout=5) as conn:
                conn.row_factory = sqlite3.Row
                # Current week start (Monday 00:00 UTC) — matches cTrader "Current week" statement
                from datetime import timedelta
                now_utc = datetime.now(timezone.utc)
                week_start = (now_utc - timedelta(days=now_utc.weekday())).strftime("%Y-%m-%dT00:00:00Z")
                rows = conn.execute("""
                    SELECT d.source as family, d.lane,
                           COUNT(DISTINCT d.position_id) as resolved,
                           ROUND(SUM(d.pnl_usd), 2) as total_pnl_usd,
                           ROUND(AVG(CASE WHEN d.pnl_usd > 0 THEN 1.0 ELSE 0.0 END), 2) as win_rate
                    FROM ctrader_deals d
                    WHERE d.outcome IN (0, 1)
                      AND d.source != '' AND d.source IS NOT NULL
                      AND d.execution_utc >= ?
                    GROUP BY d.source, d.lane
                    ORDER BY total_pnl_usd DESC
                """, (week_start,)).fetchall()
                ctx["family_performance"] = [dict(r) for r in rows]
                ctx["family_period"] = f"current week (from {week_start[:10]})"
    except Exception as exc:
        logger.debug("[chat_agent] family scores from DB: %s", exc)

    # ── PTS trials ────────────────────────────────────────────────────────────
    try:
        from learning.live_profile_autopilot import live_profile_autopilot as lpa
        trials = lpa._load_trials() or []
        pending = [
            {
                "id": t.get("id"),
                "param": t.get("param"),
                "current": t.get("current_value"),
                "proposed": t.get("proposed_value"),
                "direction": t.get("direction"),
                "status": t.get("status"),
                "reason": str(t.get("reason", ""))[:80],
            }
            for t in trials
            if str(t.get("status", "")) in ("bt_passed", "waiting_approval", "pending_bt", "bt_running")
        ]
        ctx["pts_pending"] = pending
    except Exception as exc:
        logger.debug("[chat_agent] pts trials: %s", exc)

    # ── Trading manager state ─────────────────────────────────────────────────
    try:
        tm_path = Path(__file__).parent.parent / "data" / "runtime" / "trading_manager_state.json"
        if tm_path.exists():
            tm = json.loads(tm_path.read_text(encoding="utf-8"))
            ctx["trading_manager"] = {
                "xau_directive": tm.get("xau_execution_directive", {}).get("bias", "none"),
                "shock_mode": tm.get("xau_shock_mode", {}).get("active", False),
                "cluster_guard": tm.get("xau_cluster_loss_guard", {}).get("active", False),
                "opportunity_sidecar": tm.get("xau_opportunity_sidecar", {}).get("status", "idle"),
            }
    except Exception as exc:
        logger.debug("[chat_agent] trading manager: %s", exc)

    # ── Open positions (cTrader DB) ───────────────────────────────────────────
    try:
        import sqlite3
        db_path = Path(__file__).parent.parent / "data" / "ctrader_openapi.db"
        if db_path.exists():
            with sqlite3.connect(str(db_path), timeout=5) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT symbol, side, entry_price, volume, pnl FROM positions "
                    "WHERE status='open' ORDER BY opened_at DESC LIMIT 10"
                ).fetchall()
                ctx["open_positions"] = [dict(r) for r in rows]
    except Exception as exc:
        logger.debug("[chat_agent] open positions: %s", exc)

    # ── Config thresholds (key ones) ─────────────────────────────────────────
    try:
        from config import config
        ctx["config_snapshot"] = {
            "xau_direct_lane_conf": getattr(config, "XAU_DIRECT_LANE_MIN_CONFIDENCE", "?"),
            "eth_weekend_conf": getattr(config, "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND", "?"),
            "btc_winner_conf": getattr(config, "CTRADER_BTC_WINNER_MIN_CONFIDENCE", "?"),
            "ctrader_risk_usd": getattr(config, "CTRADER_RISK", "?"),
            "qwen_model": getattr(config, "QWEN_MODEL", "?"),
        }
    except Exception as exc:
        logger.debug("[chat_agent] config: %s", exc)

    # ── OpenClaw version ──────────────────────────────────────────────────────
    try:
        from openclaw.version_guard import get_state as vg_state
        vg = vg_state()
        ctx["openclaw"] = {
            "installed": vg.get("installed_version", "unknown"),
            "latest": vg.get("latest_version", "unknown"),
            "update_available": vg.get("update_available", False),
        }
    except Exception:
        pass

    ctx["utc_now"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ctx["weekday"] = datetime.now(timezone.utc).strftime("%A")
    return ctx


def ask(question: str) -> str:
    """
    Answer a free-form question about the trading system.
    Returns AI response string, or error message.
    """
    context = _collect_context()

    user_msg = (
        f"System snapshot (UTC {context.get('utc_now', '?')}, {context.get('weekday', '?')}):\n"
        + json.dumps(context, indent=2, ensure_ascii=False)
        + f"\n\nOwner question: {question}"
    )

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
    ]

    return _call_ai(messages) or "⚠️ AI ไม่ตอบกลับ — ลอง /ask อีกครั้งหรือตรวจสอบ logs"


def _call_ai(messages: list[dict]) -> Optional[str]:
    """
    Provider chain for /ask: uses qwen-turbo (fast, cheap) not qwen-plus.
    qwen-plus is reserved for conductor/optimization (needs structured JSON).
    Falls back to Groq (free, unlimited) when turbo budget runs low.
    """
    from config import config

    # ── Qwen-Turbo DashScope (/ask uses turbo — fast, cheap, good enough) ────
    qwen_key = str(getattr(config, "QWEN_API_KEY", "") or "").strip()
    if qwen_key:
        try:
            from openclaw.token_budget import is_blocked as _budget_blocked, record_usage as _record
            turbo_model = "qwen-turbo"
            if not _budget_blocked(turbo_model):
                qwen_base = str(getattr(config, "QWEN_BASE_URL", "") or "https://dashscope-intl.aliyuncs.com/compatible-mode/v1").rstrip("/")
                result, usage = _http_chat_with_usage(
                    url=f"{qwen_base}/chat/completions",
                    model=turbo_model,
                    messages=messages,
                    api_key=qwen_key,
                    timeout=20,
                    provider="Qwen-Turbo/DashScope",
                )
                if result:
                    _record(turbo_model, usage.get("prompt_tokens", 800), usage.get("completion_tokens", 300))
                    return result
            else:
                logger.info("[chat_agent] qwen-turbo budget at 95%% → Groq")
        except Exception as exc:
            logger.warning("[chat_agent] Qwen-Turbo error: %s", exc)

    # ── OpenClaw gateway ─────────────────────────────────────────────────────
    gateway_url = str(getattr(config, "OPENCLAW_GATEWAY_URL", "") or "").strip().rstrip("/")
    if gateway_url:
        result = _http_chat(
            url=f"{gateway_url}/v1/chat/completions",
            model="auto",
            messages=messages,
            api_key="",
            timeout=15,
            provider="OpenClaw/gateway",
        )
        if result:
            return result

    # ── Groq — try QwQ-32B first (reasoning), then llama fallback ────────────
    groq_key = str(getattr(config, "GROQ_API_KEY", "") or "").strip()
    if groq_key:
        for groq_model in ["qwen-qwq-32b", "llama-3.3-70b-versatile"]:
            result, _ = _http_chat_with_usage(
                url="https://api.groq.com/openai/v1/chat/completions",
                model=groq_model,
                messages=messages,
                api_key=groq_key,
                timeout=20,
                provider=f"Groq/{groq_model}",
            )
            if result:
                return result

    # ── OpenRouter Qwen fallback ─────────────────────────────────────────────
    or_key = str(getattr(config, "OPENROUTER_API_KEY", "") or "").strip()
    if or_key:
        result = _http_chat(
            url="https://openrouter.ai/api/v1/chat/completions",
            model="qwen/qwq-32b:free",
            messages=messages,
            api_key=or_key,
            timeout=25,
            provider="Qwen/OpenRouter",
            extra_headers={"HTTP-Referer": "https://github.com/dexter-pro", "X-Title": "Dexter Pro"},
        )
        if result:
            return result

    # ── Gemini ───────────────────────────────────────────────────────────────
    try:
        from agent.brain import DexterBrain
        brain = DexterBrain()
        if config.has_gemini_key():
            result = brain._chat_gemini_native(messages=messages, max_tokens=800, temperature=0.2)
            if result:
                logger.info("[chat_agent] Gemini OK")
                return str(result).strip()
    except Exception as exc:
        logger.warning("[chat_agent] Gemini error: %s", exc)

    return None


def _http_chat(
    url: str,
    model: str,
    messages: list[dict],
    api_key: str,
    timeout: int = 20,
    provider: str = "",
    extra_headers: Optional[dict] = None,
) -> Optional[str]:
    """Generic OpenAI-compat HTTP call (no usage tracking)."""
    result, _ = _http_chat_with_usage(url, model, messages, api_key, timeout, provider, extra_headers)
    return result


def _http_chat_with_usage(
    url: str,
    model: str,
    messages: list[dict],
    api_key: str,
    timeout: int = 20,
    provider: str = "",
    extra_headers: Optional[dict] = None,
) -> tuple[Optional[str], dict]:
    """Generic OpenAI-compat HTTP call — returns (content, usage_dict)."""
    try:
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        if extra_headers:
            headers.update(extra_headers)
        payload = json.dumps({
            "model": model,
            "messages": messages,
            "max_tokens": 800,
            "temperature": 0.2,
        }).encode()
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
        usage = data.get("usage") or {}
        if content:
            logger.info("[chat_agent] %s OK: %d chars", provider, len(content))
            return str(content).strip(), usage
    except Exception as exc:
        logger.warning("[chat_agent] %s error: %s", provider, exc)
    return None, {}
