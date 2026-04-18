from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from trading_ai.config import LLMProviderName, Settings, load_settings, memory_persist_path
from trading_ai.core.agent import (
    Decision,
    TradingAgent,
    apply_confidence_floor,
    format_matched_trades_log,
)
from trading_ai.core.execution import (
    Broker,
    CloseDetail,
    ExecutionService,
    MarketSnapshot,
    OpenPosition,
    PaperBroker,
)
from trading_ai.core.market_features import extract_features, infer_setup_tag
from trading_ai.core.memory import MemoryEngine, MemoryNote, MemoryRecord
from trading_ai.core.patterns import (
    PatternBook,
    apply_pattern_confidence_boost,
    build_pattern_analysis_for_prompt,
    parse_memory_document_to_row,
    passes_pattern_execution_gate,
    score_pattern,
)
from trading_ai.core.performance import PerformanceTracker
from trading_ai.core.performance_monitor import PerformanceMonitor
from trading_ai.core.position_manager import (
    assess_entry_candidate,
    evaluate_entry_hold_override,
    evaluate_open_position,
    write_monitor_snapshot,
)
from trading_ai.core.self_improvement import SelfImprovementEngine
from trading_ai.core.runtime_state import (
    load_runtime_positions_state,
    load_shadow_runtime_positions_state,
    save_runtime_state,
)
from trading_ai.core.skillbook import SkillBook, SkillMatch, build_team_brief
from trading_ai.core.weekly_lane_learning import (
    build_weekly_lane_profile,
    save_weekly_lane_profile,
)
from trading_ai.core.portfolio_intelligence import (
    build_portfolio_votes,
    classify_regime,
    fuse_portfolio_votes,
    parse_recall_actions_for_diag,
)
from trading_ai.core.correlation_engine import CorrelationEngine, active_strategy_keys_from_registry
from trading_ai.core.strategy import RiskManager, evaluate_outcome
from trading_ai.core.strategy_evolution import StrategyRegistry, build_strategy_key
from trading_ai.integrations.ctrader import CTraderBroker, CTraderConfig
from trading_ai.integrations.ctrader_dexter_worker import CTraderDexterWorkerBroker
from trading_ai.integrations.failover import FailoverProvider, failover_runtime_snapshot
from trading_ai.integrations.mimo import MiMoProvider
from trading_ai.integrations.ollama import OllamaProvider
from trading_ai.integrations.openai_adapter import OpenAIProvider
from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


def _split_model_csv(raw: str) -> List[str]:
    out: List[str] = []
    for chunk in str(raw or "").split(","):
        model = str(chunk).strip()
        if model and model not in out:
            out.append(model)
    return out


def _unique_models(*groups: List[str]) -> List[str]:
    out: List[str] = []
    for group in groups:
        for model in group:
            item = str(model or "").strip()
            if item and item not in out:
                out.append(item)
    return out


def _build_openai_chain(
    *,
    api_key: str,
    base_url: Optional[str],
    models: List[str],
    timeout_sec: float,
    max_retries: int,
    max_tokens: int,
    label_prefix: str,
    failover_name: str,
    failure_threshold: int,
    cooldown_sec: float,
):
    providers = [
        (
            f"{label_prefix}:{model}",
            OpenAIProvider(
                api_key=api_key,
                model=model,
                base_url=base_url,
                timeout_sec=timeout_sec,
                max_retries=max_retries,
                max_tokens=max_tokens,
            ),
        )
        for model in models
    ]
    log.info("%s failover chain: %s", label_prefix.upper(), [label for label, _ in providers])
    return (
        FailoverProvider(
            providers,
            name=failover_name,
            failure_threshold=failure_threshold,
            cooldown_sec=cooldown_sec,
        )
        if len(providers) > 1
        else providers[0][1]
    )


def _build_local_chain(
    settings: Settings,
    *,
    models: List[str],
    timeout_sec: float,
    max_tokens: int,
    num_ctx: Optional[int],
    keep_alive: Optional[str],
    think: Optional[bool],
    label_prefix: str,
    failover_name: str,
    failure_threshold: int,
    cooldown_sec: float,
):
    providers = [
        (
            f"{label_prefix}:{model}",
            OllamaProvider(
                api_base_url=settings.local_base_url,
                model=model,
                timeout_sec=timeout_sec,
                max_retries=settings.llm_max_retries,
                max_tokens=max_tokens,
                num_ctx=num_ctx,
                keep_alive=keep_alive,
                think=think,
            ),
        )
        for model in models
    ]
    log.info("%s failover chain: %s", label_prefix.upper(), [label for label, _ in providers])
    return (
        FailoverProvider(
            providers,
            name=failover_name,
            failure_threshold=failure_threshold,
            cooldown_sec=cooldown_sec,
        )
        if len(providers) > 1
        else providers[0][1]
    )


def build_llm(settings: Settings):
    if settings.llm_provider is LLMProviderName.OPENAI:
        key = settings.openai_api_key or ""
        if not key:
            raise RuntimeError("OPENAI_API_KEY is required when LLM_PROVIDER=openai")
        models = _unique_models(
            [str(settings.openai_model).strip()],
            _split_model_csv(settings.openai_fallback_models),
        )
        return _build_openai_chain(
            api_key=key,
            base_url=settings.openai_base_url,
            models=models,
            timeout_sec=settings.llm_timeout_sec,
            max_retries=settings.llm_max_retries,
            max_tokens=settings.llm_max_tokens,
            label_prefix="openai",
            failover_name="primary-openai",
            failure_threshold=settings.llm_failover_failure_threshold,
            cooldown_sec=settings.llm_failover_cooldown_sec,
        )
    if settings.llm_provider is LLMProviderName.MIMO:
        key = settings.mimo_api_key or ""
        if not key:
            raise RuntimeError("MIMO_API_KEY is required when LLM_PROVIDER=mimo")
        return MiMoProvider(
            api_key=key,
            model=settings.mimo_model,
            base_url=settings.mimo_base_url,
            timeout_sec=settings.llm_timeout_sec,
            max_retries=settings.llm_max_retries,
            max_tokens=settings.llm_max_tokens,
        )
    if settings.llm_provider is LLMProviderName.LOCAL:
        models = _unique_models(
            [str(settings.local_model).strip()],
            _split_model_csv(settings.local_fallback_models),
        )
        return _build_local_chain(
            settings,
            models=models,
            timeout_sec=settings.llm_timeout_sec,
            max_tokens=settings.llm_max_tokens,
            num_ctx=settings.local_num_ctx,
            keep_alive=settings.local_keep_alive,
            think=settings.local_think,
            label_prefix="local",
            failover_name="primary-local",
            failure_threshold=settings.llm_failover_failure_threshold,
            cooldown_sec=settings.llm_failover_cooldown_sec,
        )
    raise RuntimeError(f"Unsupported LLM provider: {settings.llm_provider}")


def build_self_improvement_llm(settings: Settings, primary_llm):
    model_override = str(settings.self_improvement_model_name or "").strip()
    if not model_override:
        return primary_llm

    timeout_sec = float(settings.self_improvement_timeout_sec or max(settings.llm_timeout_sec, 120.0))
    max_tokens = int(settings.self_improvement_max_tokens or max(settings.llm_max_tokens, 256))

    if settings.llm_provider is LLMProviderName.OPENAI:
        key = settings.openai_api_key or ""
        if not key:
            return primary_llm
        models = _unique_models(
            [model_override],
            [str(settings.openai_model).strip()],
            _split_model_csv(settings.openai_fallback_models),
        )
        return _build_openai_chain(
            api_key=key,
            base_url=settings.openai_base_url,
            models=models,
            timeout_sec=timeout_sec,
            max_retries=settings.llm_max_retries,
            max_tokens=max_tokens,
            label_prefix="self-improvement-openai",
            failover_name="self-improvement-openai",
            failure_threshold=settings.llm_failover_failure_threshold,
            cooldown_sec=settings.llm_failover_cooldown_sec,
        )

    if settings.llm_provider is LLMProviderName.LOCAL:
        models = _unique_models(
            [model_override],
            [str(settings.local_model).strip()],
            _split_model_csv(settings.local_fallback_models),
        )
        return _build_local_chain(
            settings,
            models=models,
            timeout_sec=timeout_sec,
            max_tokens=max_tokens,
            num_ctx=settings.self_improvement_local_num_ctx,
            keep_alive=settings.self_improvement_local_keep_alive,
            think=settings.self_improvement_local_think,
            label_prefix="self-improvement-local",
            failover_name="self-improvement-local",
            failure_threshold=settings.llm_failover_failure_threshold,
            cooldown_sec=settings.llm_failover_cooldown_sec,
        )

    if settings.llm_provider is LLMProviderName.MIMO:
        key = settings.mimo_api_key or ""
        if not key:
            return primary_llm
        return MiMoProvider(
            api_key=key,
            model=model_override,
            base_url=settings.mimo_base_url,
            timeout_sec=timeout_sec,
            max_retries=settings.llm_max_retries,
            max_tokens=max_tokens,
        )

    return primary_llm


def _safe_int_account(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    try:
        return int(float(str(raw).strip()))
    except ValueError:
        return None


def _enforce_live_safety(settings: Settings) -> None:
    if settings.dry_run:
        return
    if not settings.live_execution_enabled:
        raise RuntimeError(
            "Refusing live execution: set LIVE_EXECUTION_ENABLED=true explicitly after validating "
            "the Mempalac runtime on this VM."
        )
    if settings.ctrader_dexter_worker and settings.ctrader_quote_source == "paper":
        raise RuntimeError(
            "Refusing live execution with CTRADER_QUOTE_SOURCE=paper. "
            "Use CTRADER_QUOTE_SOURCE=auto or dexter_capture so live learning uses real broker quotes."
        )


def _export_decision_to_dexter_family(
    *,
    settings: Settings,
    decision: Decision,
    features: Dict[str, Any],
    strategy_key: str,
) -> None:
    if not settings.dexter_family_export_enabled:
        return
    try:
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        action = str(decision.action or "").strip().upper()
        direction = "long" if action == "BUY" else "short" if action == "SELL" else ""
        raw_conf = float(decision.confidence or 0.0)
        confidence = raw_conf * 100.0 if raw_conf <= 1.0 else raw_conf
        confidence = max(0.0, min(100.0, confidence))
        symbol = str(features.get("symbol") or settings.symbol or "").strip().upper()
        payload: Dict[str, Any] = {
            "updated_at": now_utc,
            "action": action,
            "symbol": symbol,
            "base_source": str(settings.dexter_family_export_base_source or "scalp_xauusd").strip().lower(),
            "family": str(settings.dexter_family_export_family or "xau_scalp_mempalace_lane").strip().lower(),
            "strategy_id": str(settings.dexter_family_export_strategy_id or "xau_scalp_mempalace_lane_v1").strip(),
            "confidence": round(confidence, 3),
            "reason": str(decision.reason or ""),
            "strategy_key": str(strategy_key or ""),
            "session": str(features.get("session") or ""),
            "timeframe": str(features.get("timeframe") or ""),
            "signal_id": f"mempalace-{int(datetime.now(timezone.utc).timestamp())}",
            "producer": "mempalace_loop",
        }
        if direction:
            payload["direction"] = direction
        path = Path(settings.dexter_family_export_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(path)
    except Exception as exc:
        log.warning("Dexter family export failed: %s", exc)


def build_broker(settings: Settings) -> Broker:
    quote = PaperBroker(settings.symbol)
    if settings.ctrader_dexter_worker and settings.ctrader_account_id:
        try:
            return CTraderDexterWorkerBroker(settings, quote_broker=quote)
        except Exception as exc:
            if settings.live_execution_enabled and not settings.dry_run:
                raise RuntimeError(f"CTraderDexterWorkerBroker unavailable: {exc}") from exc
            log.exception("CTraderDexterWorkerBroker unavailable: %s - PaperBroker", exc)
            return quote
    if settings.ctrader_enabled and settings.ctrader_client_id and settings.ctrader_client_secret:
        if settings.live_execution_enabled and not settings.dry_run:
            raise RuntimeError(
                "Native integrations/ctrader.py is still a stub transport. "
                "Use CTRADER_DEXTER_WORKER=1 for real execution or keep DRY_RUN=true."
            )
        cfg = CTraderConfig(
            client_id=settings.ctrader_client_id,
            client_secret=settings.ctrader_client_secret,
            access_token=settings.ctrader_access_token,
            refresh_token=settings.ctrader_refresh_token,
            redirect_uri=settings.ctrader_redirect_uri,
            demo=settings.ctrader_demo,
            account_id=_safe_int_account(settings.ctrader_account_id),
            account_login=settings.ctrader_account_login,
        )
        return CTraderBroker(cfg)
    if settings.live_execution_enabled and not settings.dry_run:
        raise RuntimeError(
            "Live execution requested but no live-capable broker is configured. "
            "Enable CTRADER_DEXTER_WORKER with CTRADER_ACCOUNT_ID and Dexter worker access."
        )
    log.info("cTrader disabled or incomplete credentials - using PaperBroker")
    return quote


def build_memory(settings: Settings) -> MemoryEngine:
    return MemoryEngine(
        persist_path=memory_persist_path(settings),
        collection_name=settings.memory_collection,
        score_weight=settings.memory_score_weight,
    )


def build_skillbook(settings: Settings) -> SkillBook:
    return SkillBook(
        root_dir=Path(settings.skillbook_dir),
        index_path=Path(settings.skillbook_index_path),
        max_evidence=settings.skillbook_max_evidence,
    )


def _persist_llm_failover_snapshot(settings: Settings) -> None:
    snapshots = failover_runtime_snapshot()
    if not snapshots:
        return
    payload = {
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "provider": settings.llm_provider.value,
        "snapshots": snapshots,
    }
    out_path = Path(settings.llm_failover_runtime_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(out_path)
    except Exception as exc:
        log.warning("Failed to persist LLM failover snapshot: %s", exc)


async def smoke_ctrader_worker(settings: Settings) -> None:
    """Single BUY via Dexter `ctrader_execute_once` - verifies broker wiring without the LLM loop."""
    broker = build_broker(settings)
    if not isinstance(broker, CTraderDexterWorkerBroker):
        log.error(
            "smoke-worker needs CTraderDexterWorkerBroker. "
            "Set CTRADER_DEXTER_WORKER=1 and CTRADER_ACCOUNT_ID; fix CTRADER_WORKER_SCRIPT if needed. "
            "Got broker type: %s",
            type(broker).__name__,
        )
        raise SystemExit(2)
    execution = ExecutionService(broker)
    log.info(
        "smoke-worker: BUY %s volume=%s dry_run=%s",
        settings.symbol,
        settings.default_volume,
        settings.dry_run,
    )
    outcome = await execution.execute_trade(
        symbol=settings.symbol,
        action="BUY",
        volume=float(settings.default_volume),
        decision_reason="mempalac_smoke_worker",
        dry_run=settings.dry_run,
    )
    tr = outcome.trade
    log.info(
        "smoke-worker done executed=%s dry_run=%s order_id=%s message=%s",
        tr.executed,
        tr.dry_run,
        tr.order_id,
        tr.message,
    )
    if tr.raw_response is not None:
        log.info("smoke-worker raw_response=%s", json.dumps(tr.raw_response, ensure_ascii=True)[:4000])
    if not settings.dry_run and not tr.executed:
        raise SystemExit(1)


def _journal_structured(
    market: MarketSnapshot,
    features: Dict[str, Any],
    decision: Decision,
    settings: Settings,
    setup_tag: str,
    *,
    strategy_key: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> str:
    payload = {
        "market": market.as_prompt_dict(),
        "features": features,
        "decision": {
            "action": decision.action,
            "confidence": decision.confidence,
            "reason": decision.reason,
        },
        "setup_tag": setup_tag,
        "strategy_key": strategy_key,
        "runtime": {"dry_run": settings.dry_run, "provider": str(settings.llm_provider)},
        "extra": extra or {},
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _hydrate_pattern_book(memory: MemoryEngine, book: PatternBook) -> None:
    rows: List[Dict[str, Any]] = []
    for item in memory.list_all_structured_experiences():
        r = parse_memory_document_to_row(item["document"], item["metadata"])
        if r:
            rows.append(r)
    book.hydrate_from_rows(rows)


def _hard_market_filters(
    features: Dict[str, Any],
    risk: RiskManager,
    settings: Settings,
    action: str = "",
) -> Optional[str]:
    def _is_impulse_breakout(target_action: str) -> bool:
        side = str(target_action or "").upper()
        if side not in ("BUY", "SELL"):
            return False
        sign = 1.0 if side == "BUY" else -1.0
        m5 = sign * float(features.get("momentum_5") or 0.0)
        m20 = sign * float(features.get("momentum_20") or 0.0)
        vol = float(features.get("realized_volatility") or 0.0)
        spread_pct = float(features.get("spread_pct") or 0.0)
        session = str(features.get("session") or "").upper()
        structure = dict(features.get("structure") or {})
        breakout_distance = (
            float(features.get("distance_from_recent_high_pct") or 1.0)
            if side == "BUY"
            else float(features.get("distance_from_recent_low_pct") or 1.0)
        )
        contra_structure = bool(structure.get("lower_low")) if side == "BUY" else bool(structure.get("higher_high"))
        vol_base = max(vol * 12.0, spread_pct * 8.0, 1e-6)
        thrust = m5 / vol_base
        continuation = m20 / vol_base
        thrust_threshold = 2.2 if session == "ASIA" else 1.6
        breakout_limit = max(0.00075, spread_pct * 8.0)
        return (
            m5 > 0.0
            and thrust >= thrust_threshold
            and continuation > -0.35
            and breakout_distance <= breakout_limit
            and not contra_structure
        )

    def _is_corrective_countertrend(target_action: str) -> bool:
        side = str(target_action or "").upper()
        if side not in ("BUY", "SELL"):
            return False
        trend = str(features.get("trend_direction") or "").upper()
        structure = dict(features.get("structure") or {})
        spread_pct = float(features.get("spread_pct") or 0.0)
        sign = 1.0 if side == "BUY" else -1.0
        m5 = sign * float(features.get("momentum_5") or 0.0)
        m20 = sign * float(features.get("momentum_20") or 0.0)
        contra_trend = trend == ("DOWN" if side == "BUY" else "UP")
        contra_structure = bool(structure.get("lower_low")) if side == "BUY" else bool(structure.get("higher_high"))
        fast_threshold = max(0.00008, spread_pct * 2.5)
        return contra_trend or (m5 < -fast_threshold and (m20 < 0.0 or contra_structure))

    sample_len = int(features.get("sample_closes_len") or 0)
    trend = str(features.get("trend_direction") or "")
    normalized_action = str(action or "").upper()
    impulse_breakout = _is_impulse_breakout(normalized_action)
    if normalized_action in {"BUY", "SELL"} and _is_corrective_countertrend(normalized_action):
        return f"corrective_wave_{normalized_action.lower()}"
    if action == "BUY" and trend == "DOWN":
        return "action_trend_mismatch_BUY_vs_DOWN"
    if action == "SELL" and trend == "UP":
        return "action_trend_mismatch_SELL_vs_UP"
    if settings.hard_filter_min_closes > 0 and sample_len < settings.hard_filter_min_closes:
        return None
    if str(features.get("volatility")) == "LOW" and not impulse_breakout:
        return "volatility_LOW"
    if str(features.get("trend_direction")) == "RANGE" and not impulse_breakout:
        return "trend_RANGE"
    if bool((features.get("structure") or {}).get("consolidation")) and not impulse_breakout:
        return "structure_consolidation"
    if risk.consecutive_losses >= settings.entry_loss_streak_block:
        return f"loss_streak_{risk.consecutive_losses}>={settings.entry_loss_streak_block}"
    return None


def _reason_bucket(reason: str) -> str:
    text = str(reason or "").strip()
    if not text:
        return "unknown"
    if text.startswith("pre_llm_hard_filter:"):
        return f"pre_llm_hard_filter:{text.split(':', 1)[1].split('|', 1)[0].split()[0]}"
    if text.startswith("low_confidence_floor("):
        return "low_confidence_floor"
    for marker, bucket in (
        ("|pattern_block:", "pattern_block"),
        ("|strategy_disabled:", "strategy_disabled"),
        ("|memory_guard:anti_pattern:", "memory_guard_anti_pattern"),
        ("|memory_guard:", "memory_guard"),
        ("|exposure_cap:", "exposure_cap"),
        ("|hard_filter:", "hard_filter"),
        ("|loss_streak_soft_gate:", "loss_streak_soft_gate"),
        ("skill_promotion:", "skill_promotion"),
        ("|skill_block:", "skill_block"),
        ("|skill_caution:", "skill_caution"),
        ("|skill_support:", "skill_support"),
        ("|pattern_soft_gate:", "pattern_soft_gate"),
        ("|strategy_soft_gate:", "strategy_soft_gate"),
    ):
        if marker in text or text.startswith(marker):
            return bucket
    if text == "risk_block_session":
        return "risk_block_session"
    if text.startswith("heuristic_fallback:"):
        bucket = text.split("|", 1)[0]
        bucket = bucket.split(" llm_error", 1)[0]
        bucket = bucket.split(" sample_len", 1)[0]
        return bucket
    return text[:96]


def _trade_raw_error_code(raw_response: Optional[Dict[str, Any]]) -> str:
    payload = dict(raw_response or {})
    exec_meta = dict(payload.get("execution_meta") or {})
    code = str(exec_meta.get("error_code") or payload.get("error_code") or "").strip().upper()
    return code


def _trade_failure_detail(trade_message: str, raw_response: Optional[Dict[str, Any]]) -> str:
    payload = dict(raw_response or {})
    broker_message = str(payload.get("message") or "").strip()
    detail = str(trade_message or "").strip() or broker_message or "trade_not_executed"
    if detail.lower() in {"rejected", "error", "worker_failure", "no_worker_json"} and broker_message:
        detail = broker_message
    error_code = _trade_raw_error_code(raw_response)
    if error_code and error_code not in detail.upper():
        detail = f"{error_code}: {detail}"
    return detail[:220]


def _should_store_execution_failure_note(trade_message: str, raw_response: Optional[Dict[str, Any]]) -> bool:
    error_code = _trade_raw_error_code(raw_response)
    if error_code == "MARKET_CLOSED":
        return False
    if str(trade_message or "").strip().lower() in {"skip_same_side_open", "hold"}:
        return False
    detail = _trade_failure_detail(trade_message, raw_response).lower()
    if "market is closed" in detail:
        return False
    if error_code == "INVALID_REQUEST" and "comment is too long" in detail:
        return False
    return True


def _apply_skill_feedback(
    decision: Decision,
    *,
    anticipated_action: str,
    matches: List[SkillMatch],
    min_trade_confidence: float,
) -> tuple[Decision, Dict[str, Any]]:
    if anticipated_action not in ("BUY", "SELL") or not matches:
        return decision, {"applied": False}

    top = matches[0]
    stats = dict(top.stats)
    trades_seen = int(stats.get("trades_seen") or 0)
    wins = int(stats.get("wins") or 0)
    losses = int(stats.get("losses") or 0)
    win_rate = float(stats.get("win_rate") or 0.0)
    edge = float(stats.get("risk_adjusted_score") or 0.0)
    feedback = {
        "applied": False,
        "skill_key": top.skill_key,
        "fit": top.score,
        "edge": edge,
        "trades_seen": trades_seen,
        "win_rate": win_rate,
    }

    raw = dict(decision.raw)
    raw["skill_feedback"] = feedback

    if decision.action in ("BUY", "SELL"):
        if trades_seen >= 3 and losses >= wins + 2 and edge < -0.35 and top.score >= 4.0:
            feedback.update({"applied": True, "type": "block"})
            raw["skill_feedback"] = feedback
            return (
                Decision(
                    action="HOLD",
                    confidence=decision.confidence,
                    reason=f"{decision.reason}|skill_block:{top.skill_key}",
                    raw=raw,
                ),
                feedback,
            )

        delta = 0.0
        if trades_seen >= 2 and win_rate >= 0.55 and edge > 0.05:
            delta += min(0.08, 0.03 + (0.01 * min(trades_seen, 4)))
        elif trades_seen >= 2 and losses > wins and edge < -0.10:
            delta -= min(0.10, 0.04 + (0.02 * min(losses - wins, 3)))
        elif trades_seen == 1 and edge > 0.20:
            delta += 0.02
        elif trades_seen == 1 and edge < -0.90:
            delta -= 0.02

        if abs(delta) > 1e-9:
            feedback.update(
                {
                    "applied": True,
                    "type": "support" if delta > 0 else "caution",
                    "delta": round(delta, 4),
                }
            )
            raw["skill_feedback"] = feedback
            adjusted = Decision(
                action=decision.action,
                confidence=max(0.0, min(0.95, float(decision.confidence) + delta)),
                reason=f"{decision.reason}|{'skill_support' if delta > 0 else 'skill_caution'}:{top.skill_key}",
                raw=raw,
            )
            return apply_confidence_floor(adjusted, min_trade_confidence), feedback

        return Decision(
            action=decision.action,
            confidence=decision.confidence,
            reason=decision.reason,
            raw=raw,
        ), feedback

    promotable_hold = decision.reason.startswith("low_confidence_floor(") or "similar_memory_" in decision.reason
    if promotable_hold and trades_seen >= 3 and top.score >= 6.0 and win_rate >= 0.66 and edge > 0.20:
        feedback.update({"applied": True, "type": "promotion"})
        raw["skill_feedback"] = feedback
        promoted_conf = min(0.95, max(min_trade_confidence, 0.67 + min(0.06, 0.01 * trades_seen)))
        return (
            Decision(
                action=anticipated_action,
                confidence=promoted_conf,
                reason=f"skill_promotion:{top.skill_key}|{decision.reason}",
                raw=raw,
            ),
            feedback,
        )

    return Decision(
        action=decision.action,
        confidence=decision.confidence,
        reason=decision.reason,
        raw=raw,
    ), feedback


def _is_new_lane(strategy_state: Optional[Dict[str, Any]], settings: Settings) -> bool:
    trades = int((strategy_state or {}).get("trades") or 0)
    return trades < int(settings.soft_gate_new_lane_max_trades)


def _strategy_state_payload(registry: StrategyRegistry, strategy_key: str) -> Optional[Dict[str, Any]]:
    if not strategy_key:
        return None
    stats = registry.get_stats(strategy_key)
    if stats is None:
        return None
    return {
        "trades": stats.trades,
        "wins": stats.wins,
        "losses": stats.losses,
        "total_profit": stats.total_profit,
        "score": stats.score,
        "ranking_score": stats.ranking_score,
        "active": stats.active,
        "lane_stage": stats.lane_stage,
        "pending_recommendation": stats.pending_recommendation,
        "shadow_trades": stats.shadow_trades,
        "shadow_wins": stats.shadow_wins,
        "shadow_losses": stats.shadow_losses,
        "shadow_total_profit": stats.shadow_total_profit,
    }


def _sync_registry_from_skill(registry: StrategyRegistry, skill: Optional[Dict[str, Any]]) -> None:
    if not skill:
        return
    stats = dict(skill.get("stats") or {})
    registry.sync_skill_feedback(
        str(skill.get("skill_key") or ""),
        risk_adjusted_score=float(stats.get("risk_adjusted_score") or 0.0),
        trades_seen=int(stats.get("trades_seen") or 0),
        win_rate=float(stats.get("win_rate") or 0.0),
    )


def _soften_pattern_block(
    decision: Decision,
    *,
    pat_reason: str,
    strategy_state: Optional[Dict[str, Any]],
    matches: List[SkillMatch],
    settings: Settings,
) -> tuple[Decision, bool]:
    if not settings.soft_gate_new_lane_enabled:
        return decision, False
    pending = str((strategy_state or {}).get("pending_recommendation") or "")
    allow = _is_new_lane(strategy_state, settings) or pending in {"probation_boost", "promote_from_shadow"}
    if not allow and matches:
        top = matches[0]
        allow = float(top.stats.get("risk_adjusted_score") or 0.0) > 0.15 and int(top.stats.get("trades_seen") or 0) >= 1
    if not allow:
        return decision, False
    if not (str(pat_reason).startswith("pattern_low_sample:") or str(pat_reason).startswith("pattern_unknown")):
        return decision, False
    raw = dict(decision.raw)
    raw["soft_gate"] = {"type": "pattern", "reason": pat_reason}
    softened = Decision(
        action=decision.action,
        confidence=max(0.0, min(0.95, float(decision.confidence) - float(settings.soft_gate_confidence_penalty))),
        reason=f"{decision.reason}|pattern_soft_gate:{pat_reason}",
        raw=raw,
    )
    return apply_confidence_floor(softened, float(settings.soft_gate_min_confidence)), True


def _soften_strategy_block(
    decision: Decision,
    *,
    strategy_key: str,
    strategy_state: Optional[Dict[str, Any]],
    matches: List[SkillMatch],
    settings: Settings,
) -> tuple[Decision, bool]:
    if not settings.soft_gate_new_lane_enabled:
        return decision, False
    pending = str((strategy_state or {}).get("pending_recommendation") or "")
    if pending in {"quarantine", "quarantine_shadow"}:
        return decision, False
    allow = _is_new_lane(strategy_state, settings) or pending in {"probation_boost", "promote_from_shadow"}
    if not allow and matches:
        top = matches[0]
        allow = float(top.stats.get("risk_adjusted_score") or 0.0) > 0.15 and int(top.stats.get("trades_seen") or 0) >= 1
    if not allow:
        return decision, False
    raw = dict(decision.raw)
    raw["soft_gate"] = {"type": "strategy", "strategy_key": strategy_key}
    softened = Decision(
        action=decision.action,
        confidence=max(0.0, min(0.95, float(decision.confidence) - float(settings.soft_gate_confidence_penalty))),
        reason=f"{decision.reason}|strategy_soft_gate:{strategy_key}",
        raw=raw,
    )
    return apply_confidence_floor(softened, float(settings.soft_gate_min_confidence)), True


def _loss_streak_override_payload(
    *,
    veto: Optional[str],
    anticipated_action: str,
    strategy_key: str,
    strategy_state: Optional[Dict[str, Any]],
    matches: List[SkillMatch],
    settings: Settings,
) -> Optional[Dict[str, Any]]:
    if not settings.loss_streak_override_enabled:
        return None
    if anticipated_action not in ("BUY", "SELL"):
        return None
    text = str(veto or "")
    if not text.startswith("loss_streak_"):
        return None

    state = dict(strategy_state or {})
    pending = str(state.get("pending_recommendation") or "")
    shadow_trades = int(state.get("shadow_trades") or 0)
    shadow_wins = int(state.get("shadow_wins") or 0)
    shadow_total_profit = float(state.get("shadow_total_profit") or 0.0)
    shadow_wr = (shadow_wins / float(shadow_trades)) if shadow_trades else 0.0

    top = matches[0] if matches else None
    stats = dict(top.stats) if top else {}
    trades_seen = int(stats.get("trades_seen") or 0)
    skill_wr = float(stats.get("win_rate") or 0.0)
    skill_edge = float(stats.get("risk_adjusted_score") or 0.0)
    skill_fit = float(top.score) if top else 0.0
    skill_key = str(top.skill_key) if top else strategy_key

    min_shadow_trades = int(settings.loss_streak_override_min_shadow_trades)
    min_shadow_wr = float(settings.loss_streak_override_min_shadow_win_rate)
    min_skill_trades = int(settings.loss_streak_override_min_skill_trades)
    min_skill_edge = float(settings.loss_streak_override_min_skill_edge)
    min_skill_wr = max(0.55, min_shadow_wr)

    if (
        pending == "promote_from_shadow"
        and shadow_trades >= min_shadow_trades
        and shadow_wr >= min_shadow_wr
        and shadow_total_profit > 0.0
    ):
        return {
            "applied": True,
            "type": "promote_from_shadow",
            "strategy_key": strategy_key,
            "skill_key": skill_key,
            "shadow_trades": shadow_trades,
            "shadow_win_rate": round(shadow_wr, 4),
            "shadow_total_profit": round(shadow_total_profit, 6),
        }

    if (
        pending == "probation_boost"
        and shadow_trades >= max(1, min_shadow_trades - 1)
        and shadow_wr >= min_shadow_wr
        and shadow_total_profit >= 0.0
    ):
        return {
            "applied": True,
            "type": "probation_boost",
            "strategy_key": strategy_key,
            "skill_key": skill_key,
            "shadow_trades": shadow_trades,
            "shadow_win_rate": round(shadow_wr, 4),
            "shadow_total_profit": round(shadow_total_profit, 6),
        }

    if (
        top is not None
        and trades_seen >= min_skill_trades
        and skill_wr >= min_skill_wr
        and skill_edge >= min_skill_edge
        and skill_fit >= 4.0
    ):
        return {
            "applied": True,
            "type": "skill_edge",
            "strategy_key": strategy_key,
            "skill_key": skill_key,
            "skill_trades_seen": trades_seen,
            "skill_win_rate": round(skill_wr, 4),
            "skill_edge": round(skill_edge, 6),
        }

    return None


def _apply_loss_streak_soft_gate(
    decision: Decision,
    *,
    override: Optional[Dict[str, Any]],
    settings: Settings,
    min_trade_confidence: float,
) -> Decision:
    if not override or decision.action not in ("BUY", "SELL"):
        return decision
    raw = dict(decision.raw)
    raw["loss_streak_override"] = dict(override)
    marker = f"{override.get('type')}:{override.get('skill_key') or override.get('strategy_key') or 'unknown'}"
    softened = Decision(
        action=decision.action,
        confidence=max(
            0.0,
            min(0.95, float(decision.confidence) - float(settings.loss_streak_override_confidence_penalty)),
        ),
        reason=f"{decision.reason}|loss_streak_soft_gate:{marker}",
        raw=raw,
    )
    return apply_confidence_floor(softened, min_trade_confidence)


def _requested_trade_volume(decision: Decision, settings: Settings) -> float:
    reason = str(decision.reason or "")
    if reason.startswith("weekly_lane_probe_override:") or "|weekly_lane_probe_override:" in reason:
        requested = float(settings.default_volume) * float(settings.weekly_lane_probe_volume_fraction)
        floor = max(1e-4, float(settings.risk_min_order_lot))
        return min(float(settings.default_volume), max(floor, requested))
    if (
        reason.startswith("entry_override:")
        or
        "|pattern_soft_gate:" in reason
        or "|strategy_soft_gate:" in reason
        or "|loss_streak_soft_gate:" in reason
    ):
        requested = float(settings.default_volume) * float(settings.probation_trade_volume_fraction)
        floor = max(1e-4, float(settings.risk_min_order_lot))
        return min(float(settings.default_volume), max(floor, requested))
    return float(settings.default_volume)


def _eligible_shadow_probe_bucket(bucket: str) -> bool:
    text = str(bucket or "")
    if text.startswith("pre_llm_hard_filter:loss_streak_"):
        return True
    return text in {"pattern_block", "strategy_disabled", "low_confidence_floor"}


def _shadow_probe_volume(settings: Settings) -> float:
    requested = float(settings.default_volume) * float(settings.shadow_probe_volume_fraction)
    return max(1e-4, requested)


def _shadow_probe_market_ok(market: MarketSnapshot, features: Dict[str, Any]) -> bool:
    if float(market.bid) <= 0.0 or float(market.ask) <= 0.0 or float(market.mid) <= 0.0:
        return False
    return float(features.get("spread_pct") or 0.0) < 0.001


def _is_weekly_lane_block_reason(reason: str) -> bool:
    text = str(reason or "")
    if not text:
        return False
    if text.startswith("pre_llm_hard_filter:loss_streak_"):
        return True
    markers = (
        "|pattern_block:",
        "|strategy_disabled:",
        "|strategy_soft_gate:",
        "|memory_guard:",
        "low_confidence_floor(",
    )
    return any(marker in text for marker in markers)


def _weekly_lane_probe_market_ok(features: Dict[str, Any]) -> bool:
    if str(features.get("volatility") or "").upper() == "LOW":
        return False
    if str(features.get("trend_direction") or "").upper() == "RANGE":
        return False
    if bool((features.get("structure") or {}).get("consolidation")):
        return False
    return float(features.get("spread_pct") or 0.0) < 0.0015


async def _estimate_account_equity(broker: Broker, settings: Settings) -> float:
    getter = getattr(broker, "get_account_equity", None)
    if callable(getter):
        try:
            equity = await asyncio.to_thread(getter)
            if equity is not None and float(equity) > 0:
                return float(equity)
        except Exception as exc:
            log.warning("Risk cap equity probe failed: %s", exc)
    return float(settings.risk_equity_fallback_usd)


async def _cap_trade_volume_for_exposure(
    *,
    broker: Broker,
    execution: ExecutionService,
    settings: Settings,
    symbol: str,
    action: str,
    requested_volume: float,
    confidence: float,
) -> tuple[float, str]:
    if action not in ("BUY", "SELL"):
        return 0.0, "not_entry"

    side_positions = [p for p in execution.positions_for(symbol) if p.side == action]
    if side_positions and not settings.pyramiding_enabled:
        return 0.0, "pyramiding_disabled"
    if side_positions and confidence < settings.pyramid_add_min_confidence:
        return 0.0, f"pyramid_confidence_low:{confidence:.3f}<{settings.pyramid_add_min_confidence:.3f}"
    if len(side_positions) >= settings.pyramid_max_positions_per_side:
        return 0.0, f"pyramid_position_cap:{len(side_positions)}>={settings.pyramid_max_positions_per_side}"

    equity = await _estimate_account_equity(broker, settings)
    equity_cap = max(0.0, equity / 1000.0 * float(settings.risk_max_lot_per_1000_equity))
    hard_cap = float(settings.risk_max_total_lot_per_symbol)
    cap = min(hard_cap, equity_cap) if hard_cap > 0 else equity_cap
    current = execution.total_volume(symbol, action) if side_positions else 0.0
    remaining = max(0.0, cap - current)
    min_lot = max(0.0, float(settings.risk_min_order_lot))

    if remaining + 1e-12 < min_lot:
        return 0.0, f"exposure_cap_full:current={current:.4f}:cap={cap:.4f}:equity={equity:.2f}"

    allowed = min(float(requested_volume), remaining)
    if allowed + 1e-12 < min_lot:
        return 0.0, f"order_below_min_lot:allowed={allowed:.4f}:min={min_lot:.4f}"
    if allowed < float(requested_volume):
        return allowed, f"volume_capped:{requested_volume:.4f}->{allowed:.4f}:cap={cap:.4f}:equity={equity:.2f}"
    return float(requested_volume), f"volume_ok:current={current:.4f}:cap={cap:.4f}:equity={equity:.2f}"


async def _reconcile_open_positions_from_broker(
    broker: Broker,
    settings: Settings,
) -> tuple[list[OpenPosition], bool]:
    runner = getattr(broker, "_run_worker", None)
    if not callable(runner):
        return [], False
    account_id = _safe_int_account(settings.ctrader_account_id)
    if not account_id:
        return [], False
    payload = {
        "account_id": int(account_id),
        "symbol": settings.symbol,
        "lookback_hours": 72,
        "max_rows": 100,
    }
    try:
        data = await asyncio.to_thread(runner, "reconcile", payload)
    except Exception as exc:
        log.warning("Startup broker reconcile failed: %s", exc)
        return [], False
    if not bool(data.get("ok")):
        log.warning(
            "Startup broker reconcile returned status=%s message=%s",
            data.get("status"),
            str(data.get("message") or "")[:220],
        )
        return [], False
    scale = max(1, int(settings.ctrader_worker_volume_scale))

    # Dexter worker order volume uses DEFAULT_VOLUME * scale, but cTrader reconcile
    # returns open-position volume in a centi-unit of that worker payload.
    # Example seen in production demo:
    #   requested DEFAULT_VOLUME=0.01 -> worker volume=1 -> reconcile raw volume=100
    # Convert back into Mempalace lot-sized units so exposure caps and pyramiding
    # continue to operate on the same unit used at order entry time.
    broker_to_lot_divisor = float(scale * 100)
    positions: list[OpenPosition] = []
    for row in list(data.get("positions") or []):
        try:
            symbol = str(row.get("symbol") or "").upper().strip()
            if symbol != settings.symbol.upper():
                continue
            direction = str(row.get("direction") or "").strip().lower()
            side = "BUY" if direction == "long" else "SELL" if direction == "short" else ""
            if not side:
                continue
            raw_volume = float(row.get("volume") or 0.0)
            if raw_volume <= 0:
                continue
            opened_ms = float(row.get("open_timestamp_ms") or row.get("updated_timestamp_ms") or 0.0)
            positions.append(
                OpenPosition(
                    order_id=str(row.get("position_id") or row.get("order_id") or f"reconcile_{len(positions)+1}"),
                    symbol=symbol,
                    side=side,  # type: ignore[arg-type]
                    volume=raw_volume / broker_to_lot_divisor,
                    entry_price=float(row.get("entry_price") or 0.0),
                    position_id=str(row.get("position_id") or "") or None,
                    opened_ts=(opened_ms / 1000.0) if opened_ms > 0 else 0.0,
                )
            )
        except Exception as exc:
            log.warning("Skipping startup reconciled position row=%s err=%s", row, exc)
    return positions, True


def _seed_price_history_from_monitor(
    history_path: Path,
    *,
    symbol: str,
    limit: int,
    max_age_sec: Optional[float] = None,
    now_ts: Optional[float] = None,
) -> list[float]:
    file_path = Path(history_path)
    if limit <= 0 or not file_path.is_file():
        return []

    mids: deque[float] = deque(maxlen=max(1, int(limit)))
    latest_ts = 0.0
    cutoff_ts = 0.0
    if max_age_sec is not None and float(max_age_sec) > 0.0:
        cutoff_ts = float(now_ts if now_ts is not None else datetime.now(tz=timezone.utc).timestamp()) - float(max_age_sec)
    try:
        with file_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                text = line.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    continue
                market = dict(payload.get("market") or {})
                row_symbol = str(market.get("symbol") or "").upper().strip()
                if row_symbol and row_symbol != str(symbol).upper().strip():
                    continue
                row_ts = 0.0
                try:
                    row_ts = float(market.get("ts_unix") or 0.0)
                except (TypeError, ValueError):
                    row_ts = 0.0
                if row_ts <= 0.0:
                    updated_utc = str(payload.get("updated_utc") or "").strip()
                    if updated_utc:
                        try:
                            row_ts = datetime.fromisoformat(updated_utc.replace("Z", "+00:00")).timestamp()
                        except ValueError:
                            row_ts = 0.0
                if row_ts > 0.0:
                    latest_ts = max(latest_ts, row_ts)
                if cutoff_ts > 0.0 and row_ts > 0.0 and row_ts < cutoff_ts:
                    continue
                try:
                    mid = float(market.get("mid") or 0.0)
                except (TypeError, ValueError):
                    continue
                if mid > 0.0:
                    mids.append(mid)
    except OSError as exc:
        log.warning("Monitor history seed load failed %s: %s", file_path, exc)
        return []
    if cutoff_ts > 0.0 and latest_ts > 0.0 and latest_ts < cutoff_ts:
        age_sec = max(0.0, (float(now_ts if now_ts is not None else datetime.now(tz=timezone.utc).timestamp()) - latest_ts))
        log.warning(
            "Skipping stale monitor seed from %s age=%.1fs max_age=%.1fs",
            file_path,
            age_sec,
            float(max_age_sec or 0.0),
        )
        return []
    return list(mids)


def _startup_monitor_seed_max_age_sec(settings: Settings) -> float:
    window_sec = max(1.0, float(settings.loop_interval_sec)) * max(1, int(settings.price_history_max))
    return max(300.0, min(7200.0, window_sec * 2.0))


async def _seed_price_history_from_broker(broker: Broker, settings: Settings) -> list[float]:
    if not isinstance(broker, CTraderDexterWorkerBroker):
        return []
    try:
        closes = await broker.get_recent_closes(
            settings.symbol,
            count=settings.price_history_max,
            timeframe="1m",
        )
    except Exception as exc:
        log.warning("Broker trendbar seed failed for %s: %s", settings.symbol, exc)
        return []
    return list(closes[-settings.price_history_max :]) if closes else []


def _context_key_for_position(position: OpenPosition) -> tuple[str, str, str, str]:
    return (
        str(position.position_id or "").strip(),
        str(position.order_id or "").strip(),
        str(position.symbol or "").upper().strip(),
        str(position.side or "").upper().strip(),
    )


def _resolve_startup_positions(
    *,
    restored_positions: list[OpenPosition],
    restored_contexts: list[Dict[str, Any]],
    broker_positions: list[OpenPosition],
    broker_reconcile_ok: bool,
) -> tuple[list[OpenPosition], list[Dict[str, Any]], str]:
    """
    Prefer broker truth when a live reconcile is available.

    Returns:
      positions, contexts, source_label
    """

    if broker_reconcile_ok:
        if not broker_positions:
            return [], [], "broker_empty"
        context_by_key: dict[tuple[str, str, str, str], Dict[str, Any]] = {}
        for idx, restored in enumerate(restored_positions):
            if idx < len(restored_contexts):
                context_by_key[_context_key_for_position(restored)] = dict(restored_contexts[idx])
        broker_contexts: list[Dict[str, Any]] = []
        for position in broker_positions:
            ctx = context_by_key.get(_context_key_for_position(position))
            if ctx is not None:
                broker_contexts.append(ctx)
        return broker_positions, broker_contexts, "broker"
    return list(restored_positions), list(restored_contexts), "runtime"


async def learning_loop(settings: Settings) -> None:
    memory = build_memory(settings)
    broker = build_broker(settings)
    execution = ExecutionService(broker)
    shadow_execution = ExecutionService(broker)
    llm = build_llm(settings)
    agent = TradingAgent(llm, settings)
    skillbook = build_skillbook(settings)
    learning_llm = build_self_improvement_llm(settings, llm)
    self_improvement = SelfImprovementEngine(
        skillbook=skillbook,
        memory=memory,
        llm=learning_llm,
        enabled=settings.self_improvement_enabled,
        store_notes=settings.self_improvement_store_notes,
    )
    risk = RiskManager(
        max_trades_per_session=settings.max_trades_per_session,
        max_consecutive_losses=settings.max_consecutive_losses,
        neutral_rel_threshold=settings.neutral_pnl_threshold,
    )
    perf = PerformanceTracker()
    price_history: List[float] = []
    pattern_book = PatternBook()
    _hydrate_pattern_book(memory, pattern_book)

    registry = StrategyRegistry(Path(settings.strategy_registry_path))
    if len(registry.snapshot()) == 0 and memory.count() > 0:
        hist: List[Dict[str, Any]] = []
        for item in memory.list_all_structured_experiences():
            r = parse_memory_document_to_row(item["document"], item["metadata"])
            if r:
                hist.append(r)
        if hist:
            registry.hydrate_from_closed_trades(hist)

    correlation: Optional[CorrelationEngine] = None
    if settings.correlation_engine_enabled:
        correlation = CorrelationEngine(
            Path(settings.strategy_correlation_path),
            max_len=settings.correlation_max_history,
            min_samples_matrix=settings.correlation_min_samples,
            penalty_mid_threshold=settings.correlation_penalty_mid_threshold,
            penalty_high_threshold=settings.correlation_penalty_high_threshold,
            penalty_mid=settings.correlation_penalty_mid,
            penalty_high=settings.correlation_penalty_high,
            max_penalty=settings.correlation_max_penalty,
            diversity_threshold=settings.correlation_diversity_threshold,
            diversity_bonus=settings.correlation_diversity_bonus,
        )

    perf_mon = PerformanceMonitor(
        log_interval_cycles=settings.performance_log_interval,
        alert_max_drawdown=settings.performance_alert_max_drawdown,
        alert_selectivity_min=settings.performance_alert_selectivity_min,
        alert_min_llm_intents=settings.performance_alert_min_llm_intents,
    )

    open_contexts: List[Dict[str, Any]] = []
    shadow_open_contexts: List[Dict[str, Any]] = []
    restored_positions, restored_contexts, restored_risk = load_runtime_positions_state(settings.runtime_state_path)
    broker_positions: list[OpenPosition] = []
    broker_reconcile_ok = False
    if restored_positions or (settings.live_execution_enabled and not settings.dry_run):
        broker_positions, broker_reconcile_ok = await _reconcile_open_positions_from_broker(broker, settings)

    startup_positions, startup_contexts, startup_source = _resolve_startup_positions(
        restored_positions=restored_positions,
        restored_contexts=restored_contexts,
        broker_positions=broker_positions,
        broker_reconcile_ok=broker_reconcile_ok,
    )

    if startup_positions:
        execution.restore_positions(startup_positions)
        open_contexts = list(startup_contexts)
        while open_contexts and len(open_contexts) > len(startup_positions):
            open_contexts.pop(0)
        while open_contexts and len(open_contexts) < len(startup_positions):
            open_contexts.insert(0, dict(open_contexts[0]))
        latest = startup_positions[-1]
        if startup_source == "broker":
            log.warning(
                "Reconciled %s open position(s) from broker at startup; latest=%s %s volume=%s position_id=%s",
                len(startup_positions),
                latest.side,
                latest.symbol,
                latest.volume,
                latest.position_id,
            )
            if restored_positions and len(startup_positions) != len(restored_positions):
                log.warning(
                    "Startup reconcile replaced runtime state positions: runtime=%s broker=%s matched_contexts=%s",
                    len(restored_positions),
                    len(startup_positions),
                    len(open_contexts),
                )
        else:
            log.warning(
                "Restored %s open position(s) from runtime state; latest=%s %s volume=%s position_id=%s",
                len(startup_positions),
                latest.side,
                latest.symbol,
                latest.volume,
                latest.position_id,
            )
    elif restored_positions and startup_source == "broker_empty":
        log.warning(
            "Dropped %s runtime-state open position(s) because broker reconcile returned no live positions",
            len(restored_positions),
        )
    restored_shadow_positions, restored_shadow_contexts = load_shadow_runtime_positions_state(settings.runtime_state_path)
    if restored_shadow_positions:
        shadow_execution.restore_positions(restored_shadow_positions)
        shadow_open_contexts = list(restored_shadow_contexts)
        while shadow_open_contexts and len(shadow_open_contexts) > len(restored_shadow_positions):
            shadow_open_contexts.pop(0)
        while shadow_open_contexts and len(shadow_open_contexts) < len(restored_shadow_positions):
            shadow_open_contexts.insert(0, dict(shadow_open_contexts[0]))
        latest_shadow = restored_shadow_positions[-1]
        log.warning(
            "Restored %s shadow probe position(s); latest=%s %s volume=%s position_id=%s",
            len(restored_shadow_positions),
            latest_shadow.side,
            latest_shadow.symbol,
            latest_shadow.volume,
            latest_shadow.position_id,
        )
    if restored_risk:
        risk.restore(restored_risk)
        log.info("Restored runtime risk state: %s", risk.snapshot())

    seeded_history = await _seed_price_history_from_broker(broker, settings)
    if seeded_history:
        price_history = list(seeded_history[-settings.price_history_max :])
        log.info(
            "Seeded %s prior mid prices from cTrader trendbars for %s",
            len(price_history),
            settings.symbol,
        )
    else:
        seeded_history = _seed_price_history_from_monitor(
            settings.position_monitor_history_path,
            symbol=settings.symbol,
            limit=settings.price_history_max,
            max_age_sec=_startup_monitor_seed_max_age_sec(settings),
        )
        if seeded_history:
            price_history = list(seeded_history[-settings.price_history_max :])
            log.info(
                "Seeded %s prior mid prices from recent monitor history %s",
                len(price_history),
                settings.position_monitor_history_path,
            )

    weekly_lane_profile: Dict[str, Any] = {}
    weekly_lane_last_refresh_ts = 0.0

    async def refresh_weekly_lane_profile(*, force: bool = False) -> None:
        nonlocal weekly_lane_profile, weekly_lane_last_refresh_ts
        if not settings.weekly_lane_learning_enabled:
            return
        now_ts = time.time()
        if not force and (now_ts - weekly_lane_last_refresh_ts) < max(15, int(settings.weekly_lane_refresh_sec)):
            return
        try:
            profile = await asyncio.to_thread(
                build_weekly_lane_profile,
                memory=memory,
                symbol=settings.symbol,
                monitor_history_path=Path(settings.position_monitor_history_path),
                dexter_db_path=(
                    Path(settings.weekly_lane_dexter_db_path)
                    if settings.weekly_lane_dexter_db_path is not None
                    else None
                ),
                min_trades=int(settings.weekly_lane_min_trades),
                good_win_rate=float(settings.weekly_lane_good_win_rate),
                bad_loss_rate=float(settings.weekly_lane_bad_loss_rate),
                bad_pnl_threshold=float(settings.weekly_lane_bad_pnl_threshold),
                lookahead_steps=int(settings.weekly_lane_monitor_lookahead_steps),
                move_threshold_pct=float(settings.weekly_lane_monitor_move_threshold_pct),
            )
            weekly_lane_profile = dict(profile or {})
            save_weekly_lane_profile(Path(settings.weekly_lane_profile_path), weekly_lane_profile)
            weekly_lane_last_refresh_ts = now_ts
            summary = dict(weekly_lane_profile.get("summary") or {})
            log.info(
                "Weekly lane profile refreshed symbol=%s trades=%s strategy_lanes=%s promote=%s block=%s probe=%s dexter_error=%s",
                settings.symbol,
                summary.get("mempalace_trade_count"),
                summary.get("mempalace_strategy_lanes"),
                len(list(weekly_lane_profile.get("promote_lanes") or [])),
                len(list(weekly_lane_profile.get("block_lanes") or [])),
                len(list(weekly_lane_profile.get("probe_lanes") or [])),
                summary.get("dexter_source_error") or "-",
            )
        except Exception as exc:
            log.warning("Weekly lane profile refresh failed: %s", exc)

    def persist_runtime_state() -> None:
        save_runtime_state(
            settings.runtime_state_path,
            open_position=execution.open_position_for(settings.symbol),
            open_context=open_contexts[-1] if open_contexts else None,
            open_positions=execution.positions_for(settings.symbol),
            open_contexts=open_contexts,
            shadow_open_position=shadow_execution.open_position_for(settings.symbol),
            shadow_open_context=shadow_open_contexts[-1] if shadow_open_contexts else None,
            shadow_open_positions=shadow_execution.positions_for(settings.symbol),
            shadow_open_contexts=shadow_open_contexts,
            risk=risk,
        )

    async def process_closed_trade(close: CloseDetail, close_context: Optional[Dict[str, Any]], *, close_reason: str) -> None:
        notional = close.notional_approx()
        tscore = evaluate_outcome(
            close.pnl,
            notional=notional,
            neutral_rel_threshold=settings.neutral_pnl_threshold,
        )
        score_int = int(tscore)

        if close_context is not None:
            sk_close = str(close_context.get("strategy_key") or "")
            record = MemoryRecord(
                market=dict(close_context["market"]),
                features=dict(close_context["features"]),
                decision=dict(close_context["decision"]),
                result={
                    "pnl": close.pnl,
                    "exit_price": close.exit_price,
                    "entry_price": close.entry_price,
                },
                score=score_int,
                setup_tag=str(close_context["setup_tag"]),
                strategy_key=sk_close,
                journal=str(close_context["journal"]),
                tags=list(close_context.get("tags") or []),
            )
            memory.store_memory(
                record,
                extra_metadata={
                    "trade_score": score_int,
                    "strategy_key": sk_close,
                },
            )
            closed_confidence = float((close_context.get("decision") or {}).get("confidence") or 0.0)
            current_room = str(
                sk_close
                or build_strategy_key(
                    dict(close_context["features"]),
                    str(close_context["setup_tag"]),
                )
            )
            if score_int < 0 and closed_confidence >= 0.75:
                memory.store_note(
                    MemoryNote(
                        title="Overconfident loss",
                        content=(
                            f"Loss recorded in room={current_room} with confidence={closed_confidence:.3f} "
                            f"pnl={float(close.pnl):.6f}. Treat as anti-pattern candidate until more evidence arrives."
                        ),
                        wing=f"symbol:{str(settings.symbol).lower()}",
                        hall="hall_discoveries",
                        room=current_room,
                        note_type="anti_pattern_candidate",
                        hall_type="hall_discoveries",
                        symbol=settings.symbol,
                        session=str(close_context["features"].get("session") or ""),
                        setup_tag=str(close_context["setup_tag"]),
                        strategy_key=sk_close,
                        importance=0.9,
                        source="trade_close",
                        tags=["anti-pattern", "overconfident-loss", current_room],
                    )
                )
            elif score_int > 0 and closed_confidence < 0.55:
                memory.store_note(
                    MemoryNote(
                        title="Underconfident win",
                        content=(
                            f"Win recorded in room={current_room} with confidence={closed_confidence:.3f} "
                            f"pnl={float(close.pnl):.6f}. This may be an opportunity room worth promoting."
                        ),
                        wing=f"symbol:{str(settings.symbol).lower()}",
                        hall="hall_discoveries",
                        room=current_room,
                        note_type="opportunity_candidate",
                        hall_type="hall_discoveries",
                        symbol=settings.symbol,
                        session=str(close_context["features"].get("session") or ""),
                        setup_tag=str(close_context["setup_tag"]),
                        strategy_key=sk_close,
                        importance=0.78,
                        source="trade_close",
                        tags=["opportunity", "underconfident-win", current_room],
                    )
                )
            if sk_close:
                registry.update_strategy(
                    sk_close,
                    {"pnl": float(close.pnl), "score": score_int},
                )
                if settings.correlation_engine_enabled and correlation is not None:
                    correlation.update_pnl(sk_close, float(close.pnl))
            updated_stats = registry.get_stats(sk_close) if sk_close else None
            updated_state: Optional[Dict[str, Any]] = None
            if updated_stats is not None:
                updated_state = {
                    "trades": updated_stats.trades,
                    "wins": updated_stats.wins,
                    "losses": updated_stats.losses,
                    "total_profit": updated_stats.total_profit,
                    "score": updated_stats.score,
                    "ranking_score": updated_stats.ranking_score,
                    "active": updated_stats.active,
                    "lane_stage": updated_stats.lane_stage,
                    "pending_recommendation": updated_stats.pending_recommendation,
                }
            close_room_guard = (
                memory.get_room_guardrail(
                    symbol=settings.symbol,
                    session=str(close_context["features"].get("session") or ""),
                    setup_tag=str(close_context["setup_tag"]),
                    trend_direction=str(close_context["features"].get("trend_direction") or ""),
                    volatility=str(close_context["features"].get("volatility") or ""),
                    strategy_key=sk_close,
                )
                if settings.memory_room_guard_enabled
                else None
            )
            close_result = {
                "pnl": float(close.pnl),
                "entry_price": float(close.entry_price),
                "exit_price": float(close.exit_price),
                "side": close.side,
                "close_reason": close_reason,
            }
            try:
                learned_skill = await self_improvement.learn_from_closed_trade(
                    close_context=close_context,
                    close_result=close_result,
                    score=score_int,
                    strategy_state=updated_state,
                    room_guard=close_room_guard,
                )
                _sync_registry_from_skill(registry, learned_skill)
            except Exception as exc:
                log.warning("Self improvement step failed for %s: %s", sk_close or current_room, exc)
            pattern_book.append_closed_trade(
                features=dict(close_context["features"]),
                setup_tag=str(close_context["setup_tag"]),
                score=score_int,
                pnl=float(close.pnl),
            )
        else:
            memory.store_note(
                MemoryNote(
                    title="Close without context",
                    content=(
                        f"Closed {close.side} {close.symbol} without matching open context. "
                        f"entry={close.entry_price:.5f} exit={close.exit_price:.5f} pnl={float(close.pnl):.6f}"
                    ),
                    wing="execution",
                    hall="hall_events",
                    room=f"close-without-context:{str(settings.symbol).lower()}",
                    note_type="runtime_gap",
                    hall_type="hall_events",
                    symbol=settings.symbol,
                    importance=0.7,
                    source="learning_loop",
                    tags=["runtime-gap", "close-without-context", close.side.lower()],
                )
            )

        risk.on_trade_result(tscore, pnl=close.pnl)
        perf.record_close(close.pnl, score=score_int)
        if settings.performance_monitor_enabled:
            perf_mon.update_on_trade(float(close.pnl), score_int)

    async def process_shadow_close(close: CloseDetail, close_context: Optional[Dict[str, Any]], *, close_reason: str) -> None:
        if close_context is None:
            log.warning(
                "Shadow probe close without context side=%s entry=%.5f exit=%.5f pnl=%.6f",
                close.side,
                close.entry_price,
                close.exit_price,
                float(close.pnl),
            )
            return

        strategy_key = str(close_context.get("strategy_key") or "")
        setup_tag = str(close_context.get("setup_tag") or "")
        features = dict(close_context.get("features") or {})
        decision = dict(close_context.get("decision") or {})
        blocker_bucket = str(close_context.get("probe_blocker_bucket") or "unknown")
        blocker_reason = str(close_context.get("probe_blocker_reason") or "")
        pnl = float(close.pnl)
        notional = close.notional_approx()
        tscore = evaluate_outcome(
            pnl,
            notional=notional,
            neutral_rel_threshold=settings.neutral_pnl_threshold,
        )
        score_int = int(tscore)

        memory.store_memory(
            MemoryRecord(
                market=dict(close_context.get("market") or {}),
                features=features,
                decision=decision,
                result={
                    "pnl": pnl,
                    "entry_price": float(close.entry_price),
                    "exit_price": float(close.exit_price),
                },
                score=score_int,
                setup_tag=setup_tag,
                strategy_key=strategy_key,
                journal=str(close_context.get("journal") or ""),
                tags=list(close_context.get("tags") or []) + ["shadow-probe"],
            ),
            extra_metadata={
                "trade_score": score_int,
                "strategy_key": strategy_key,
                "memory_type": "shadow_probe",
                "probe_blocker_bucket": blocker_bucket,
            },
        )
        pattern_book.append_closed_trade(
            features=features,
            setup_tag=setup_tag,
            score=score_int,
            pnl=pnl,
        )
        if strategy_key:
            registry.record_shadow_probe(strategy_key, pnl=pnl, score=score_int)

        note_title = "Blocked opportunity confirmed" if score_int > 0 else "Blocker confirmed"
        note_type = "opportunity_candidate" if score_int > 0 else "anti_pattern_candidate"
        note_tags = ["shadow-probe", blocker_bucket, "blocked-opportunity" if score_int > 0 else "blocked-loss"]
        memory.store_note(
            MemoryNote(
                title=note_title,
                content=(
                    f"shadow_probe blocker={blocker_bucket} strategy={strategy_key or setup_tag} "
                    f"side={close.side} pnl={pnl:.6f} close_reason={close_reason}. original_blocker={blocker_reason}"
                ),
                wing=f"symbol:{str(settings.symbol).lower()}",
                hall="hall_discoveries",
                room=str(strategy_key or build_strategy_key(features, setup_tag)),
                note_type=note_type,
                hall_type="hall_discoveries",
                symbol=settings.symbol,
                session=str(features.get("session") or ""),
                setup_tag=setup_tag,
                strategy_key=strategy_key,
                importance=0.84 if score_int > 0 else 0.78,
                source="shadow_probe",
                tags=note_tags,
            )
        )

        strategy_state = _strategy_state_payload(registry, strategy_key)
        room_guard = (
            memory.get_room_guardrail(
                symbol=settings.symbol,
                session=str(features.get("session") or ""),
                setup_tag=setup_tag,
                trend_direction=str(features.get("trend_direction") or ""),
                volatility=str(features.get("volatility") or ""),
                strategy_key=strategy_key,
            )
            if settings.memory_room_guard_enabled
            else None
        )
        try:
            learned_skill = await self_improvement.learn_from_closed_trade(
                close_context=close_context,
                close_result={
                    "pnl": pnl,
                    "entry_price": float(close.entry_price),
                    "exit_price": float(close.exit_price),
                    "close_reason": close_reason,
                    "side": close.side,
                    "volume": float(close.volume),
                    "shadow_probe": True,
                    "probe_blocker_bucket": blocker_bucket,
                },
                score=score_int,
                strategy_state=strategy_state,
                room_guard=room_guard,
            )
            _sync_registry_from_skill(registry, learned_skill)
        except Exception as exc:
            log.warning(
                "Shadow self improvement step failed for %s blocker=%s: %s",
                strategy_key or setup_tag,
                blocker_bucket,
                exc,
            )

    await refresh_weekly_lane_profile(force=True)
    persist_runtime_state()
    log.info(
        "Learning loop started instance=%s symbol=%s dry_run=%s live_execution=%s memory_count=%s patterns=%s strategies=%s",
        settings.instance_name,
        settings.symbol,
        settings.dry_run,
        settings.live_execution_enabled,
        memory.count(),
        len(pattern_book.patterns_dict()),
        len(registry.snapshot()),
    )

    while True:
        try:
            if risk.halted:
                log.error("Stopped: %s", risk.halt_reason)
                await asyncio.sleep(settings.loop_interval_sec)
                continue

            await refresh_weekly_lane_profile()

            if (
                settings.strategy_evolution_v2_enabled
                and settings.strategy_aging_enabled
            ):
                registry.apply_aging(settings.strategy_aging_factor)

            if correlation is not None:
                correlation.start_cycle()

            market = await execution.get_market_data(settings.symbol)
            price_history.append(float(market.mid))
            if len(price_history) > settings.price_history_max:
                price_history = price_history[-settings.price_history_max :]

            md: Dict[str, Any] = {**market.as_prompt_dict(), "price_history": list(price_history)}
            features = extract_features(md)

            similar = memory.recall_similar_trades(
                features,
                symbol=settings.symbol,
                top_k=settings.similar_trades_top_k,
            )

            risk_state = {
                "can_trade": risk.can_trade(),
                "halted": risk.halted,
                "consecutive_losses": risk.consecutive_losses,
                "max_consecutive_losses_halt": settings.max_consecutive_losses,
                "entry_loss_streak_block": settings.entry_loss_streak_block,
                "trades_executed_session": risk.trades_executed,
                "min_confidence_required": settings.min_trade_confidence,
            }

            patterns_live = pattern_book.patterns_dict()
            pattern_analysis = build_pattern_analysis_for_prompt(features, patterns_live)
            anticipated_action = (
                "BUY"
                if str(features.get("trend_direction") or "").upper() == "UP"
                else "SELL"
                if str(features.get("trend_direction") or "").upper() == "DOWN"
                else "HOLD"
            )
            if anticipated_action == "HOLD":
                pre_buy_veto = _hard_market_filters(features, risk, settings, "BUY")
                pre_sell_veto = _hard_market_filters(features, risk, settings, "SELL")
                momentum_5 = float(features.get("momentum_5") or 0.0)
                if pre_buy_veto is None and pre_sell_veto is None:
                    anticipated_action = "BUY" if momentum_5 >= 0.0 else "SELL"
                elif pre_buy_veto is None:
                    anticipated_action = "BUY"
                elif pre_sell_veto is None:
                    anticipated_action = "SELL"
            anticipated_setup = infer_setup_tag(features, anticipated_action)
            anticipated_strategy_key = build_strategy_key(features, anticipated_setup)
            anticipated_room_guard: Optional[Dict[str, Any]] = None
            if settings.memory_room_guard_enabled:
                anticipated_room_guard = memory.get_room_guardrail(
                    symbol=settings.symbol,
                    session=str(features.get("session") or ""),
                    setup_tag=anticipated_setup,
                    trend_direction=str(features.get("trend_direction") or ""),
                    volatility=str(features.get("volatility") or ""),
                    strategy_key=anticipated_strategy_key,
                )
            anticipated_stats = registry.get_stats(anticipated_strategy_key)
            anticipated_state: Optional[Dict[str, Any]] = None
            if anticipated_stats is not None:
                anticipated_state = {
                    "trades": anticipated_stats.trades,
                    "wins": anticipated_stats.wins,
                    "losses": anticipated_stats.losses,
                    "total_profit": anticipated_stats.total_profit,
                    "score": anticipated_stats.score,
                    "ranking_score": anticipated_stats.ranking_score,
                    "active": anticipated_stats.active,
                    "lane_stage": anticipated_stats.lane_stage,
                    "pending_recommendation": anticipated_stats.pending_recommendation,
                }
            skill_matches = skillbook.recall(
                symbol=settings.symbol,
                session=str(features.get("session") or ""),
                setup_tag=anticipated_setup,
                strategy_key=anticipated_strategy_key,
                room=anticipated_strategy_key,
                trend_direction=str(features.get("trend_direction") or ""),
                volatility=str(features.get("volatility") or ""),
                action=anticipated_action,
                top_k=settings.skill_recall_top_k,
            )
            active_skill_keys = [match.skill_key for match in skill_matches]
            skill_context = skillbook.render_prompt_context(skill_matches)
            team_brief = (
                build_team_brief(
                    features=features,
                    risk_state=risk_state,
                    pattern_analysis=pattern_analysis,
                    matches=skill_matches,
                    strategy_state=anticipated_state,
                    room_guard=anticipated_room_guard,
                )
                if settings.agent_team_enabled
                else {}
            )
            anticipated_assessment = assess_entry_candidate(
                action=anticipated_action,
                features=features,
                decision={"reason": f"anticipated:{anticipated_action}"},
                matches=skill_matches,
                strategy_state=anticipated_state,
                pattern_analysis=pattern_analysis,
            )

            if settings.position_manager_enabled and execution.positions_for(settings.symbol):
                managed_positions = execution.positions_for(settings.symbol)
                managed_plans = []
                for idx, position in enumerate(managed_positions):
                    close_context = open_contexts[idx] if idx < len(open_contexts) else None
                    if close_context is None:
                        continue
                    ctx_matches = skillbook.recall(
                        symbol=settings.symbol,
                        session=str(features.get("session") or ""),
                        setup_tag=str(close_context.get("setup_tag") or ""),
                        strategy_key=str(close_context.get("strategy_key") or ""),
                        room=str(close_context.get("strategy_key") or ""),
                        trend_direction=str(features.get("trend_direction") or ""),
                        volatility=str(features.get("volatility") or ""),
                        action=str((close_context.get("decision") or {}).get("action") or ""),
                        top_k=settings.skill_recall_top_k,
                    )
                    plan = evaluate_open_position(
                        position=position,
                        market=market,
                        features=features,
                        close_context=close_context,
                        matches=ctx_matches,
                        strategy_state=_strategy_state_payload(registry, str(close_context.get("strategy_key") or "")),
                        pattern_analysis=pattern_analysis,
                        settings=settings,
                    )
                    managed_plans.append(plan)
                close_plans = [plan for plan in managed_plans if plan.action == "CLOSE"]
                if close_plans:
                    close_reason = close_plans[0].reason
                    manager_closes = await execution.close_positions(
                        symbol=settings.symbol,
                        reason=close_reason,
                        dry_run=settings.dry_run,
                    )
                    managed_contexts = list(open_contexts[: len(manager_closes)])
                    for idx, close in enumerate(manager_closes):
                        close_context = managed_contexts[idx] if idx < len(managed_contexts) else None
                        await process_closed_trade(close, close_context, close_reason=close_reason)
                    if manager_closes:
                        open_contexts = open_contexts[len(manager_closes) :]
                        persist_runtime_state()
                        log.info(
                            "PositionManager closed %s %s position(s) reason=%s",
                            len(manager_closes),
                            settings.symbol,
                            close_reason,
                        )

            if settings.position_manager_enabled and shadow_execution.positions_for(settings.symbol):
                shadow_positions = shadow_execution.positions_for(settings.symbol)
                shadow_plans = []
                for idx, position in enumerate(shadow_positions):
                    close_context = shadow_open_contexts[idx] if idx < len(shadow_open_contexts) else None
                    if close_context is None:
                        continue
                    ctx_matches = skillbook.recall(
                        symbol=settings.symbol,
                        session=str(features.get("session") or ""),
                        setup_tag=str(close_context.get("setup_tag") or ""),
                        strategy_key=str(close_context.get("strategy_key") or ""),
                        room=str(close_context.get("strategy_key") or ""),
                        trend_direction=str(features.get("trend_direction") or ""),
                        volatility=str(features.get("volatility") or ""),
                        action=str((close_context.get("decision") or {}).get("action") or ""),
                        top_k=settings.skill_recall_top_k,
                    )
                    shadow_plans.append(
                        evaluate_open_position(
                            position=position,
                            market=market,
                            features=features,
                            close_context=close_context,
                            matches=ctx_matches,
                            strategy_state=_strategy_state_payload(registry, str(close_context.get("strategy_key") or "")),
                            pattern_analysis=pattern_analysis,
                            settings=settings,
                        )
                    )
                if any(plan.action == "CLOSE" for plan in shadow_plans):
                    close_reason = next(plan.reason for plan in shadow_plans if plan.action == "CLOSE")
                    shadow_closes = await shadow_execution.close_positions(
                        symbol=settings.symbol,
                        reason=close_reason,
                        dry_run=True,
                    )
                    shadow_contexts = list(shadow_open_contexts[: len(shadow_closes)])
                    for idx, close in enumerate(shadow_closes):
                        close_context = shadow_contexts[idx] if idx < len(shadow_contexts) else None
                        await process_shadow_close(close, close_context, close_reason=close_reason)
                    if shadow_closes:
                        shadow_open_contexts = shadow_open_contexts[len(shadow_closes) :]
                        persist_runtime_state()
                        log.info(
                            "PositionManager closed %s shadow probe(s) reason=%s",
                            len(shadow_closes),
                            close_reason,
                        )

            probe_candidate_decision: Optional[Decision] = None
            probe_candidate_bucket = ""
            probe_candidate_reason = ""
            pre_llm_veto = _hard_market_filters(
                features,
                risk,
                settings,
                anticipated_action if anticipated_action in ("BUY", "SELL") else "",
            )
            loss_streak_override = _loss_streak_override_payload(
                veto=pre_llm_veto,
                anticipated_action=anticipated_action,
                strategy_key=anticipated_strategy_key,
                strategy_state=anticipated_state,
                matches=skill_matches,
                settings=settings,
            )
            if pre_llm_veto and not loss_streak_override:
                decision = Decision(
                    action="HOLD",
                    confidence=0.0,
                    reason=f"pre_llm_hard_filter:{pre_llm_veto}",
                    raw={"pre_llm_hard_filter": pre_llm_veto},
                )
                if settings.shadow_probe_enabled and str(pre_llm_veto).startswith("loss_streak_"):
                    probe_candidate_decision = agent._heuristic_fallback_decision(
                        similar_trades=similar,
                        features=features,
                        risk_state=risk_state,
                        pattern_analysis=pattern_analysis,
                        error=RuntimeError("shadow_probe_loss_streak"),
                    )
                    if settings.self_improvement_enabled:
                        probe_candidate_decision, _ = _apply_skill_feedback(
                            probe_candidate_decision,
                            anticipated_action=anticipated_action,
                            matches=skill_matches,
                            min_trade_confidence=settings.shadow_probe_min_confidence,
                        )
                    probe_candidate_bucket = _reason_bucket(decision.reason)
                    probe_candidate_reason = decision.reason
            else:
                wake_up_context = memory.build_wake_up_context(
                    symbol=settings.symbol,
                    session=str(features.get("session") or "") or None,
                    top_k=settings.memory_wakeup_top_k,
                    note_top_k=settings.memory_note_top_k,
                )
                decision = await agent.decide(
                    market,
                    features,
                    similar_trades=similar,
                    risk_state=risk_state,
                    pattern_analysis=pattern_analysis,
                    wake_up_context=wake_up_context,
                    skill_context=skill_context,
                    team_brief=team_brief,
                )
                if settings.self_improvement_enabled:
                    decision, _ = _apply_skill_feedback(
                        decision,
                        anticipated_action=anticipated_action,
                        matches=skill_matches,
                        min_trade_confidence=settings.min_trade_confidence,
                    )
                decision = _apply_loss_streak_soft_gate(
                    decision,
                    override=loss_streak_override,
                    settings=settings,
                    min_trade_confidence=float(settings.soft_gate_min_confidence),
                )
                entry_override = evaluate_entry_hold_override(
                    anticipated_action=anticipated_action,
                    anticipated_assessment=anticipated_assessment,
                    decision_action=decision.action,
                    decision_reason=decision.reason,
                    matches=skill_matches,
                    risk_state=risk_state,
                    room_guard=anticipated_room_guard,
                    settings=settings,
                )
                if entry_override.get("eligible"):
                    override_confidence = float(entry_override.get("confidence") or settings.entry_override_confidence)
                    override_reason = (
                        "entry_override:"
                        f"opp={float(entry_override.get('opportunity_score') or 0.0):.3f}:"
                        f"risk={float(entry_override.get('risk_score') or 0.0):.3f}:"
                        f"edge={float(entry_override.get('edge_score') or 0.0):.3f}|"
                        f"{decision.reason}"
                    )
                    decision = Decision(
                        action=anticipated_action,
                        confidence=override_confidence,
                        reason=override_reason,
                        raw={**dict(decision.raw), "entry_override": dict(entry_override)},
                    )
                    log.info(
                        "Entry override promoted HOLD -> %s conf=%.3f opp=%.3f risk=%.3f edge=%.3f",
                        decision.action,
                        decision.confidence,
                        float(entry_override.get("opportunity_score") or 0.0),
                        float(entry_override.get("risk_score") or 0.0),
                        float(entry_override.get("edge_score") or 0.0),
                    )
            if settings.performance_monitor_enabled:
                perf_mon.update_on_signal(decision)

            veto = _hard_market_filters(features, risk, settings, decision.action)
            if loss_streak_override and str(veto).startswith("loss_streak_"):
                veto = None
            if veto and not decision.reason.startswith("pre_llm_hard_filter:"):
                decision = Decision(
                    action="HOLD",
                    confidence=0.0,
                    reason=f"{decision.reason}|hard_filter:{veto}",
                    raw=dict(decision.raw),
                )
                decision = apply_confidence_floor(decision, settings.min_trade_confidence)

            if (
                settings.portfolio_intelligence_enabled
                and decision.action in ("BUY", "SELL")
            ):
                regime = classify_regime(features)
                llm_a = decision.action
                llm_c = float(decision.confidence)
                votes = build_portfolio_votes(
                    llm_action=llm_a,
                    llm_confidence=llm_c,
                    features=features,
                    similar_hits=similar,
                    cfg_weights={
                        "llm": settings.portfolio_weight_llm,
                        "memory": settings.portfolio_weight_memory,
                        "structure": settings.portfolio_weight_structure,
                    },
                )
                pen_f = 0.0
                bon_f = 0.0
                corr_pen_meta: Dict[str, Any] = {}
                corr_div_meta: Dict[str, Any] = {}
                matrix_snapshot: Dict[str, float] = {}
                if settings.correlation_engine_enabled and correlation is not None:
                    cand_sk = build_strategy_key(
                        features,
                        infer_setup_tag(features, llm_a),
                    )
                    matrix_snapshot = correlation.get_correlation_matrix_cached()
                    act_keys = active_strategy_keys_from_registry(registry.snapshot(), min_trades=1)
                    pen_f, corr_pen_meta = correlation.get_correlation_penalty(
                        cand_sk,
                        act_keys,
                        matrix=matrix_snapshot,
                    )
                    bon_f, corr_div_meta = correlation.get_diversity_bonus(
                        cand_sk,
                        act_keys,
                        matrix=matrix_snapshot,
                    )
                    log.info(
                        "Correlation L5: key=%s penalty=%.3f bonus=%.3f top_pairs=%s",
                        cand_sk,
                        pen_f,
                        bon_f,
                        correlation.top_correlation_pairs(matrix_snapshot, limit=8),
                    )
                    if settings.performance_monitor_enabled:
                        perf_mon.note_correlation_penalty(pen_f)

                fused = fuse_portfolio_votes(
                    votes,
                    regime=regime,
                    tie_margin=settings.portfolio_tie_margin,
                    llm_anchor_confidence=settings.portfolio_llm_anchor_confidence,
                    llm_original_action=llm_a,
                    llm_original_confidence=llm_c,
                    correlation_penalty=pen_f,
                    diversity_bonus=bon_f,
                )
                fused.diag["recall_digest"] = parse_recall_actions_for_diag(similar)
                if settings.correlation_engine_enabled:
                    fused.diag["correlation_penalty_detail"] = corr_pen_meta
                    fused.diag["correlation_diversity_detail"] = corr_div_meta

                conf_cap = min(0.95, fused.confidence) if fused.action != "HOLD" else fused.confidence
                if fused.action != decision.action or abs(conf_cap - float(decision.confidence)) > 1e-5:
                    reason = f"{decision.reason}|portfolio:{fused.reason_detail}|regime={fused.regime}"
                    if pen_f > 0 or bon_f > 0:
                        reason += f"|corr_p={pen_f:.2f}_b={bon_f:.2f}"
                    decision = Decision(
                        action=fused.action,
                        confidence=conf_cap,
                        reason=reason,
                        raw={**dict(decision.raw), "portfolio_fusion": fused.diag},
                    )
                    decision = apply_confidence_floor(decision, settings.min_trade_confidence)
                log.info(
                    "Portfolio L4+L5: regime=%s result=%s conf=%.3f masses buy=%.4f sell=%.4f corr=%s",
                    regime,
                    decision.action,
                    decision.confidence,
                    fused.buy_mass,
                    fused.sell_mass,
                    fused.diag.get("correlation"),
                )

            base_decision = Decision(
                action=decision.action,
                confidence=decision.confidence,
                reason=decision.reason,
                raw=dict(decision.raw),
            )
            matched_log = format_matched_trades_log(similar)
            setup_eval = infer_setup_tag(features, decision.action) if decision.action in ("BUY", "SELL") else ""
            if decision.action in ("BUY", "SELL"):
                ok_pat, pat_reason, pat_stat = passes_pattern_execution_gate(
                    features,
                    patterns_live,
                    setup_eval,
                    min_win_rate=settings.pattern_min_win_rate,
                    min_sample_size=settings.pattern_min_sample_size,
                    strict_unknown=settings.pattern_gate_strict,
                )
                ps = score_pattern(features, patterns_live, setup_eval)
                conf_before_pat = decision.confidence
                if not ok_pat:
                    softened, softened_applied = _soften_pattern_block(
                        decision,
                        pat_reason=pat_reason,
                        strategy_state=_strategy_state_payload(registry, build_strategy_key(features, setup_eval)),
                        matches=skill_matches,
                        settings=settings,
                    )
                    if softened_applied:
                        decision = softened
                        log.info(
                            "pattern SOFT gate: key=%s matched=%s reason=%s conf=%.3f",
                            ps.matched_key,
                            ps.matched,
                            pat_reason,
                            decision.confidence,
                        )
                    else:
                        decision = Decision(
                            action="HOLD",
                            confidence=decision.confidence,
                            reason=f"{decision.reason}|pattern_block:{pat_reason}",
                            raw=dict(decision.raw),
                        )
                        log.info(
                            "pattern HARD HOLD: key=%s matched=%s win_rate=%s n=%s reason=%s",
                            ps.matched_key,
                            ps.matched,
                            (pat_stat or {}).get("win_rate"),
                            (pat_stat or {}).get("count"),
                            pat_reason,
                        )
                else:
                    new_conf, boosted = apply_pattern_confidence_boost(
                        decision.confidence,
                        pat_stat or {},
                        boost_min_win_rate=settings.pattern_boost_min_win_rate,
                        boost_min_sample=settings.pattern_boost_min_sample,
                        delta=settings.pattern_confidence_boost_delta,
                        cap=settings.pattern_confidence_cap,
                    )
                    if boosted:
                        decision = Decision(
                            action=decision.action,
                            confidence=new_conf,
                            reason=f"{decision.reason}|pattern_boost",
                            raw=dict(decision.raw),
                        )
                        decision = apply_confidence_floor(decision, settings.min_trade_confidence)
                    log.info(
                        "pattern OK: key=%s win_rate=%s n=%s model_boost=%.4f prob=%.3f | conf %.3f -> %.3f (boosted=%s)",
                        ps.matched_key,
                        (pat_stat or {}).get("win_rate"),
                        (pat_stat or {}).get("count"),
                        ps.confidence_boost,
                        ps.success_probability,
                        conf_before_pat,
                        decision.confidence,
                        boosted,
                    )

                if decision.action in ("BUY", "SELL"):
                    evolution_key = build_strategy_key(features, setup_eval)
                    snap = registry.snapshot()
                    log.info(
                        "strategy evolution: key=%s allowed=%s stats=%s",
                        evolution_key,
                        registry.is_strategy_allowed(evolution_key),
                        snap.get(evolution_key),
                    )
                    if not registry.is_strategy_allowed(evolution_key):
                        softened, softened_applied = _soften_strategy_block(
                            decision,
                            strategy_key=evolution_key,
                            strategy_state=_strategy_state_payload(registry, evolution_key),
                            matches=skill_matches,
                            settings=settings,
                        )
                        if softened_applied:
                            decision = softened
                            log.info(
                                "StrategyRegistry soft gate: key=%s conf=%.3f pending=%s",
                                evolution_key,
                                decision.confidence,
                                (_strategy_state_payload(registry, evolution_key) or {}).get("pending_recommendation"),
                            )
                        else:
                            decision = Decision(
                                action="HOLD",
                                confidence=decision.confidence,
                                reason=f"{decision.reason}|strategy_disabled:{evolution_key}",
                                raw=dict(decision.raw),
                            )
                            log.warning(
                                "StrategyRegistry: key suppressed (low win rate / negative pnl) - HOLD %s",
                                evolution_key,
                            )
                    else:
                        conf_before_evo = decision.confidence
                        evo_boost = registry.get_strategy_boost(evolution_key)
                        if evo_boost > 0.0:
                            new_c = min(0.95, decision.confidence + evo_boost)
                            decision = Decision(
                                action=decision.action,
                                confidence=new_c,
                                reason=f"{decision.reason}|evolution_boost",
                                raw=dict(decision.raw),
                            )
                            decision = apply_confidence_floor(decision, settings.min_trade_confidence)
                            log.info(
                                "StrategyRegistry boost: key=%s +%.2f | conf %.3f -> %.3f",
                                evolution_key,
                                evo_boost,
                                conf_before_evo,
                                decision.confidence,
                            )

                    if (
                        settings.strategy_evolution_v2_enabled
                        and decision.action in ("BUY", "SELL")
                    ):
                        ok_rank, rank_reason = registry.passes_global_rank(
                            evolution_key,
                            top_n=settings.strategy_global_top_n,
                            exploration_max_trades=settings.strategy_exploration_max_trades,
                        )
                        if not ok_rank:
                            decision = Decision(
                                action="HOLD",
                                confidence=decision.confidence,
                                reason=f"{decision.reason}|global_rank:{rank_reason}",
                                raw=dict(decision.raw),
                            )
                            log.info(
                                "StrategyEvolution v2: global HOLD key=%s reason=%s top=%s explore_max=%s",
                                evolution_key,
                                rank_reason,
                                settings.strategy_global_top_n,
                                settings.strategy_exploration_max_trades,
                            )
                        else:
                            tops = registry.get_top_strategies(
                                min(5, settings.strategy_global_top_n),
                                active_only=True,
                                min_trades=1,
                            )
                            log.info(
                                "StrategyEvolution v2: rank OK key=%s (%s) top_sample=%s",
                                evolution_key,
                                rank_reason,
                                [(k, round(s.ranking_score, 5)) for k, s in tops],
                            )

                    if settings.memory_room_guard_enabled and decision.action in ("BUY", "SELL"):
                        room_guard = memory.get_room_guardrail(
                            symbol=settings.symbol,
                            session=str(features.get("session") or ""),
                            setup_tag=setup_eval,
                            trend_direction=str(features.get("trend_direction") or ""),
                            volatility=str(features.get("volatility") or ""),
                            strategy_key=evolution_key,
                        )
                        raw = dict(decision.raw)
                        raw["memory_room_guard"] = room_guard
                        if room_guard.get("blocked") and settings.memory_room_guard_block_anti:
                            decision = Decision(
                                action="HOLD",
                                confidence=decision.confidence,
                                reason=f"{decision.reason}|memory_guard:anti_pattern:{room_guard.get('room')}",
                                raw=raw,
                            )
                        else:
                            delta = float(room_guard.get("confidence_delta") or 0.0)
                            if delta != 0.0:
                                decision = Decision(
                                    action=decision.action,
                                    confidence=max(0.0, min(0.95, decision.confidence + delta)),
                                    reason=f"{decision.reason}|memory_guard:{room_guard.get('room')}",
                                    raw=raw,
                                )
                                decision = apply_confidence_floor(decision, settings.min_trade_confidence)
                        log.info(
                            "Memory room guard: room=%s blocked=%s caution=%s delta=%.3f notes=%s",
                            room_guard.get("room"),
                            room_guard.get("blocked"),
                            room_guard.get("caution"),
                            float(room_guard.get("confidence_delta") or 0.0),
                            len(list(room_guard.get("supporting_notes") or [])),
                        )

            if settings.weekly_lane_learning_enabled and weekly_lane_profile:
                decision_lane_key = (
                    build_strategy_key(features, infer_setup_tag(features, decision.action))
                    if decision.action in ("BUY", "SELL")
                    else anticipated_strategy_key
                )
                lane_payload = dict(
                    (weekly_lane_profile.get("mempalace_strategy_lanes") or {}).get(decision_lane_key) or {}
                )
                if lane_payload:
                    lane_class = str(lane_payload.get("classification") or "")
                    lane_recommendation = str(lane_payload.get("recommendation") or "")
                    lane_support = int(lane_payload.get("missed_opportunities") or 0) + int(
                        lane_payload.get("shadow_blocked_wins") or 0
                    )
                    lane_caution = int(lane_payload.get("prevented_bad") or 0) + int(
                        lane_payload.get("shadow_blocked_losses") or 0
                    )
                    lane_meta = {
                        "lane_key": decision_lane_key,
                        "classification": lane_class,
                        "recommendation": lane_recommendation,
                        "trades": int(lane_payload.get("trades") or 0),
                        "wins": int(lane_payload.get("wins") or 0),
                        "losses": int(lane_payload.get("losses") or 0),
                        "win_rate": float(lane_payload.get("win_rate") or 0.0),
                        "pnl_sum": float(lane_payload.get("pnl_sum") or 0.0),
                        "blocked_events": int(lane_payload.get("blocked_events") or 0),
                        "missed_opportunities": int(lane_payload.get("missed_opportunities") or 0),
                        "prevented_bad": int(lane_payload.get("prevented_bad") or 0),
                        "shadow_blocked_wins": int(lane_payload.get("shadow_blocked_wins") or 0),
                        "shadow_blocked_losses": int(lane_payload.get("shadow_blocked_losses") or 0),
                    }
                    raw = dict(decision.raw)
                    raw["weekly_lane_learning"] = lane_meta

                    if decision.action in ("BUY", "SELL"):
                        if lane_class == "bad" and settings.weekly_lane_block_bad_lanes:
                            log.info(
                                "Weekly lane guard HOLD lane=%s trades=%s wr=%.3f pnl=%.6f",
                                decision_lane_key,
                                lane_meta["trades"],
                                lane_meta["win_rate"],
                                lane_meta["pnl_sum"],
                            )
                            decision = Decision(
                                action="HOLD",
                                confidence=0.0,
                                reason=f"{decision.reason}|weekly_lane_bad:{decision_lane_key}",
                                raw=raw,
                            )
                        else:
                            conf_delta = 0.0
                            if lane_class == "good":
                                conf_delta += float(settings.weekly_lane_confidence_boost)
                            elif lane_class == "bad":
                                conf_delta -= float(settings.weekly_lane_confidence_penalty)
                            if conf_delta != 0.0:
                                prior_conf = float(decision.confidence)
                                decision = Decision(
                                    action=decision.action,
                                    confidence=max(0.0, min(0.95, prior_conf + conf_delta)),
                                    reason=f"{decision.reason}|weekly_lane:{lane_class}:{decision_lane_key}",
                                    raw=raw,
                                )
                                decision = apply_confidence_floor(decision, settings.min_trade_confidence)
                                log.info(
                                    "Weekly lane confidence adjust lane=%s class=%s conf=%.3f->%.3f",
                                    decision_lane_key,
                                    lane_class,
                                    prior_conf,
                                    decision.confidence,
                                )
                    elif (
                        decision.action == "HOLD"
                        and settings.weekly_lane_probe_override_enabled
                        and anticipated_action in ("BUY", "SELL")
                    ):
                        if (
                            _is_weekly_lane_block_reason(decision.reason)
                            and _weekly_lane_probe_market_ok(features)
                            and lane_support >= int(settings.weekly_lane_probe_min_support)
                            and lane_support > lane_caution
                        ):
                            probe_confidence = max(
                                float(settings.min_trade_confidence),
                                min(0.95, float(settings.weekly_lane_probe_override_confidence)),
                            )
                            decision = Decision(
                                action=anticipated_action,
                                confidence=probe_confidence,
                                reason=f"weekly_lane_probe_override:{decision_lane_key}|{decision.reason}",
                                raw=raw,
                            )
                            log.info(
                                "Weekly lane probe override lane=%s support=%s caution=%s action=%s conf=%.3f",
                                decision_lane_key,
                                lane_support,
                                lane_caution,
                                anticipated_action,
                                decision.confidence,
                            )

            log.info(
                "features=%s | decision=%s | conf=%.3f | reason=%s | matched=%s | skills=%s | pa=%s",
                json.dumps(features, ensure_ascii=False, sort_keys=True),
                decision.action,
                decision.confidence,
                decision.reason,
                matched_log,
                active_skill_keys,
                json.dumps(
                    {
                        "matched_pattern": pattern_analysis.get("matched_pattern"),
                        "win_rate": pattern_analysis.get("win_rate"),
                        "sample_size": pattern_analysis.get("sample_size"),
                    },
                    ensure_ascii=False,
                ),
            )

            if not risk.can_trade() and decision.action in ("BUY", "SELL"):
                log.warning("Risk block - overriding %s to HOLD", decision.action)
                decision = Decision(
                    action="HOLD",
                    confidence=0.0,
                    reason="risk_block_session",
                    raw=decision.raw,
                )

            trade_volume = _requested_trade_volume(decision, settings)
            if (
                decision.action in ("BUY", "SELL")
                and settings.strategy_evolution_v2_enabled
                and settings.strategy_capital_weighting_enabled
            ):
                st_exec = infer_setup_tag(features, decision.action)
                sk_exec = build_strategy_key(features, st_exec)
                mult = registry.get_position_size_multiplier(
                    sk_exec,
                    pool=settings.strategy_capital_pool,
                    clamp_min=settings.strategy_capital_mult_min,
                    clamp_max=settings.strategy_capital_mult_max,
                )
                trade_volume = max(1e-9, float(settings.default_volume) * mult)
                log.info(
                    "StrategyEvolution v2: volume key=%s mult=%.4f base=%s -> vol=%.6f",
                    sk_exec,
                    mult,
                    settings.default_volume,
                    trade_volume,
                )
            if decision.action in ("BUY", "SELL"):
                if not _shadow_probe_market_ok(market, features):
                    decision = Decision(
                        action="HOLD",
                        confidence=0.0,
                        reason=f"{decision.reason}|market_quote_invalid",
                        raw=dict(decision.raw),
                    )
                    trade_volume = 0.0
                else:
                    capped_volume, cap_reason = await _cap_trade_volume_for_exposure(
                        broker=broker,
                        execution=execution,
                        settings=settings,
                        symbol=settings.symbol,
                        action=decision.action,
                        requested_volume=trade_volume,
                        confidence=float(decision.confidence),
                    )
                    if capped_volume <= 0:
                        log.warning(
                            "Exposure cap - overriding %s to HOLD: %s",
                            decision.action,
                            cap_reason,
                        )
                        decision = Decision(
                            action="HOLD",
                            confidence=0.0,
                            reason=f"{decision.reason}|exposure_cap:{cap_reason}",
                            raw={**dict(decision.raw), "exposure_cap": cap_reason},
                        )
                        trade_volume = 0.0
                    else:
                        if abs(capped_volume - trade_volume) > 1e-9:
                            log.info(
                                "Exposure cap adjusted volume %s %s: %.4f -> %.4f (%s)",
                                settings.symbol,
                                decision.action,
                                trade_volume,
                                capped_volume,
                                cap_reason,
                            )
                        trade_volume = capped_volume

            final_bucket = _reason_bucket(decision.reason)
            _export_decision_to_dexter_family(
                settings=settings,
                decision=decision,
                features=features,
                strategy_key=anticipated_strategy_key,
            )
            outcome = await execution.execute_trade(
                symbol=settings.symbol,
                action=decision.action,
                volume=trade_volume,
                decision_reason=decision.reason,
                dry_run=settings.dry_run,
            )

            closed_positions = list(outcome.closes or ([] if outcome.close is None else [outcome.close]))
            if closed_positions:
                closed_contexts = list(open_contexts[: len(closed_positions)])
                if len(closed_contexts) < len(closed_positions):
                    log.warning(
                        "Close context mismatch: closes=%s contexts=%s for %s",
                        len(closed_positions),
                        len(closed_contexts),
                        settings.symbol,
                    )
                for idx, close in enumerate(closed_positions):
                    close_context = closed_contexts[idx] if idx < len(closed_contexts) else None
                    await process_closed_trade(close, close_context, close_reason="flip_signal")

                open_contexts = open_contexts[len(closed_positions) :]
                persist_runtime_state()

            opened = (
                decision.action in ("BUY", "SELL")
                and outcome.trade.message not in ("hold", "skip_same_side_open")
            )
            if (
                decision.action in ("BUY", "SELL")
                and not settings.dry_run
                and not outcome.trade.executed
            ):
                trade_error_code = _trade_raw_error_code(outcome.trade.raw_response)
                trade_failure_detail = _trade_failure_detail(
                    outcome.trade.message,
                    outcome.trade.raw_response,
                )
                raw_trade_json = (
                    json.dumps(outcome.trade.raw_response, ensure_ascii=True)[:2500]
                    if outcome.trade.raw_response is not None
                    else "{}"
                )
                log.warning(
                    "Trade not executed action=%s symbol=%s error_code=%s message=%s raw=%s",
                    decision.action,
                    settings.symbol,
                    trade_error_code or "-",
                    trade_failure_detail,
                    raw_trade_json,
                )
                if _should_store_execution_failure_note(outcome.trade.message, outcome.trade.raw_response):
                    failure_room = f"execution:{decision.action.lower()}:{settings.symbol.lower()}"
                    memory.store_note(
                        MemoryNote(
                            title="Execution failure",
                            content=(
                                f"Failed to execute {decision.action} for {settings.symbol}. "
                                f"message={trade_failure_detail} reason={decision.reason}"
                            ),
                            wing="execution",
                            hall="hall_events",
                            room=failure_room,
                            note_type="execution_failure",
                            hall_type="hall_events",
                            symbol=settings.symbol,
                            session=str(features.get("session") or ""),
                            setup_tag=setup_eval,
                            strategy_key=build_strategy_key(features, setup_eval) if setup_eval else "",
                            importance=0.82,
                            source="execution_service",
                            tags=["execution-failure", settings.symbol, decision.action.lower()],
                        )
                    )
                else:
                    log.info(
                        "Skipped execution failure memory note for action=%s symbol=%s error_code=%s",
                        decision.action,
                        settings.symbol,
                        trade_error_code or "-",
                    )
            if settings.performance_monitor_enabled:
                perf_mon.update_after_execution(
                    decision.action,
                    opened=bool(opened and (outcome.trade.executed or settings.dry_run)),
                )
            if opened and (outcome.trade.executed or settings.dry_run):
                tag = infer_setup_tag(features, decision.action)
                sk_open = build_strategy_key(features, tag)
                open_contexts.append(
                    {
                        "market": market.as_prompt_dict(),
                        "features": dict(features),
                        "decision": {
                            "action": decision.action,
                            "confidence": decision.confidence,
                            "reason": decision.reason,
                        },
                        "setup_tag": tag,
                        "strategy_key": sk_open,
                        "created_ts": float(getattr(market, "ts_unix", 0.0) or 0.0),
                        "active_skill_keys": list(active_skill_keys),
                        "skill_context": skill_context,
                        "team_brief": dict(team_brief),
                        "journal": _journal_structured(
                            market,
                            features,
                            decision,
                            settings,
                            tag,
                            strategy_key=sk_open,
                        ),
                        "tags": [settings.symbol, tag, str(features.get("session", "")), sk_open],
                    }
                )
                persist_runtime_state()

            if settings.shadow_probe_enabled and decision.action == "HOLD":
                shadow_candidate = probe_candidate_decision
                shadow_bucket = probe_candidate_bucket
                shadow_reason = probe_candidate_reason
                if shadow_candidate is None and _eligible_shadow_probe_bucket(final_bucket):
                    shadow_candidate, _ = _apply_skill_feedback(
                        base_decision,
                        anticipated_action=anticipated_action,
                        matches=skill_matches,
                        min_trade_confidence=settings.shadow_probe_min_confidence,
                    )
                    if shadow_candidate.action in ("BUY", "SELL"):
                        shadow_bucket = final_bucket
                        shadow_reason = decision.reason
                    else:
                        shadow_candidate = None
                if (
                    shadow_candidate is not None
                    and shadow_candidate.action in ("BUY", "SELL")
                    and float(shadow_candidate.confidence) >= float(settings.shadow_probe_min_confidence)
                    and _shadow_probe_market_ok(market, features)
                ):
                    existing_shadow = shadow_execution.open_position_for(settings.symbol)
                    if existing_shadow is None or existing_shadow.side != shadow_candidate.action:
                        shadow_outcome = await shadow_execution.execute_trade(
                            symbol=settings.symbol,
                            action=shadow_candidate.action,
                            volume=_shadow_probe_volume(settings),
                            decision_reason=f"shadow_probe:{shadow_bucket}:{shadow_reason}",
                            dry_run=True,
                        )
                        shadow_closed_positions = list(
                            shadow_outcome.closes or ([] if shadow_outcome.close is None else [shadow_outcome.close])
                        )
                        if shadow_closed_positions:
                            closed_contexts = list(shadow_open_contexts[: len(shadow_closed_positions)])
                            if len(closed_contexts) < len(shadow_closed_positions):
                                log.warning(
                                    "Shadow close context mismatch: closes=%s contexts=%s for %s",
                                    len(shadow_closed_positions),
                                    len(closed_contexts),
                                    settings.symbol,
                                )
                            for idx, close in enumerate(shadow_closed_positions):
                                close_context = closed_contexts[idx] if idx < len(closed_contexts) else None
                                await process_shadow_close(
                                    close,
                                    close_context,
                                    close_reason="shadow_flip_signal",
                                )
                            shadow_open_contexts = shadow_open_contexts[len(shadow_closed_positions) :]

                        shadow_tag = infer_setup_tag(features, shadow_candidate.action)
                        shadow_key = build_strategy_key(features, shadow_tag)
                        shadow_open_contexts.append(
                            {
                                "market": market.as_prompt_dict(),
                                "features": dict(features),
                                "decision": {
                                    "action": shadow_candidate.action,
                                    "confidence": shadow_candidate.confidence,
                                    "reason": shadow_candidate.reason,
                                },
                                "setup_tag": shadow_tag,
                                "strategy_key": shadow_key,
                                "created_ts": float(getattr(market, "ts_unix", 0.0) or 0.0),
                                "probe_blocker_bucket": shadow_bucket,
                                "probe_blocker_reason": shadow_reason,
                                "active_skill_keys": list(active_skill_keys),
                                "skill_context": skill_context,
                                "team_brief": dict(team_brief),
                                "journal": _journal_structured(
                                    market,
                                    features,
                                    shadow_candidate,
                                    settings,
                                    shadow_tag,
                                    strategy_key=shadow_key,
                                    extra={
                                        "probe_blocker_bucket": shadow_bucket,
                                        "probe_blocker_reason": shadow_reason,
                                    },
                                ),
                                "tags": [
                                    settings.symbol,
                                    shadow_tag,
                                    str(features.get("session", "")),
                                    shadow_key,
                                    "shadow-probe",
                                ],
                            }
                        )
                        persist_runtime_state()
                        log.info(
                            "Shadow probe opened %s %s conf=%.3f blocker=%s reason=%s",
                            settings.symbol,
                            shadow_candidate.action,
                            shadow_candidate.confidence,
                            shadow_bucket,
                            shadow_reason,
                        )

            if settings.position_manager_enabled:
                final_assessment = assess_entry_candidate(
                    action=decision.action,
                    features=features,
                    decision={"reason": decision.reason, "confidence": decision.confidence},
                    matches=skill_matches,
                    strategy_state=anticipated_state,
                    pattern_analysis=pattern_analysis,
                )
                monitor_open_positions: List[Dict[str, Any]] = []
                for idx, position in enumerate(execution.positions_for(settings.symbol)):
                    close_context = open_contexts[idx] if idx < len(open_contexts) else None
                    if close_context is None:
                        continue
                    ctx_matches = skillbook.recall(
                        symbol=settings.symbol,
                        session=str(features.get("session") or ""),
                        setup_tag=str(close_context.get("setup_tag") or ""),
                        strategy_key=str(close_context.get("strategy_key") or ""),
                        room=str(close_context.get("strategy_key") or ""),
                        trend_direction=str(features.get("trend_direction") or ""),
                        volatility=str(features.get("volatility") or ""),
                        action=str((close_context.get("decision") or {}).get("action") or ""),
                        top_k=settings.skill_recall_top_k,
                    )
                    monitor_open_positions.append(
                        evaluate_open_position(
                            position=position,
                            market=market,
                            features=features,
                            close_context=close_context,
                            matches=ctx_matches,
                            strategy_state=_strategy_state_payload(registry, str(close_context.get("strategy_key") or "")),
                            pattern_analysis=pattern_analysis,
                            settings=settings,
                        ).as_dict()
                    )
                def _local_iso_utc(ts: float) -> str:
                    import datetime
                    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                
                write_monitor_snapshot(
                    Path(settings.position_monitor_path),
                    Path(settings.position_monitor_history_path),
                    {
                        "updated_utc": _local_iso_utc(float(getattr(market, "ts_unix", 0.0) or 0.0)),
                        "symbol": settings.symbol,
                        "market": market.as_prompt_dict(),
                        "features": features,
                        "anticipated_action": anticipated_action,
                        "anticipated_strategy_key": anticipated_strategy_key,
                        "strategy_state": anticipated_state,
                        "loss_streak_override": loss_streak_override,
                        "skill_keys": active_skill_keys,
                        "final_decision": {
                            "action": decision.action,
                            "confidence": decision.confidence,
                            "reason": decision.reason,
                        },
                        "entry_assessment": {
                            "anticipated": anticipated_assessment,
                            "final": final_assessment,
                        },
                        "open_positions": monitor_open_positions,
                    },
                )

            if memory.count() > 0:
                registry.sync_promotion_hints(
                    list((memory.get_memory_intelligence() or {}).get("promotion_pipeline") or [])
                )

            log.info("performance snapshot: %s", perf.summary())
            if settings.performance_monitor_enabled:
                perf_mon.update_on_strategy(registry)
                perf_mon.tick_cycle_end()
                perf_mon.maybe_log_summary_and_alerts()

            persist_runtime_state()
        except Exception as exc:
            log.exception("Loop cycle failed: %s", exc)
            try:
                memory.store_note(
                    MemoryNote(
                        title="Loop cycle failure",
                        content=str(exc),
                        wing="execution",
                        hall="hall_events",
                        room="loop-cycle-failure",
                        note_type="runtime_failure",
                        hall_type="hall_events",
                        symbol=settings.symbol,
                        importance=0.75,
                        source="learning_loop",
                        tags=["runtime-failure", type(exc).__name__],
                    )
                )
            except Exception:
                log.exception("Failed to persist runtime failure note")
            persist_runtime_state()
        _persist_llm_failover_snapshot(settings)
        await asyncio.sleep(settings.loop_interval_sec)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mempalac autonomous trading AI engine")
    p.add_argument(
        "--interval",
        type=float,
        default=None,
        help="Override LOOP_INTERVAL_SEC",
    )
    p.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Force dry_run on/off for this process",
    )
    p.add_argument(
        "--smoke-worker",
        action="store_true",
        help="Send one BUY through Dexter ctrader_execute_once.py then exit (no LLM loop)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings()
    if args.interval is not None:
        settings.loop_interval_sec = args.interval
    if args.dry_run is not None:
        settings.dry_run = args.dry_run
    _enforce_live_safety(settings)
    if args.smoke_worker:
        asyncio.run(smoke_ctrader_worker(settings))
        return
    asyncio.run(learning_loop(settings))


if __name__ == "__main__":
    main()
