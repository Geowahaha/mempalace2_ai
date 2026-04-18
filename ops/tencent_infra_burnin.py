from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from trading_ai.config import LLMProviderName, Settings, load_settings
from trading_ai.integrations.failover import failover_runtime_snapshot
from trading_ai.main import build_broker, build_llm, build_self_improvement_llm


_DECISION_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {"type": "string"},
        "confidence": {"type": "number"},
        "reason": {"type": "string"},
    },
    "required": ["action", "confidence", "reason"],
    "additionalProperties": True,
}


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(float(v) for v in values)
    if len(sorted_vals) == 1:
        return round(sorted_vals[0], 3)
    pos = max(0.0, min(1.0, float(p))) * (len(sorted_vals) - 1)
    low = int(pos)
    high = min(low + 1, len(sorted_vals) - 1)
    if low == high:
        return round(sorted_vals[low], 3)
    ratio = pos - low
    value = sorted_vals[low] + (sorted_vals[high] - sorted_vals[low]) * ratio
    return round(value, 3)


async def _probe_llm(
    *,
    label: str,
    client: Any,
    symbol: str,
    rounds: int,
    temperature: float,
) -> Dict[str, Any]:
    latencies_ms: List[float] = []
    failures: List[str] = []
    actions: List[str] = []
    confidences: List[float] = []
    sample: Dict[str, Any] = {}
    for idx in range(max(1, int(rounds))):
        started = time.perf_counter()
        try:
            payload = await client.complete_json(
                system=(
                    "Return exactly one JSON object with action, confidence, and reason. "
                    "Use HOLD when uncertain."
                ),
                user=json.dumps(
                    {
                        "symbol": symbol,
                        "session": "london",
                        "trend": "UP",
                        "volatility": "MEDIUM",
                        "task": "Burn-in probe",
                        "round": idx + 1,
                    },
                    ensure_ascii=False,
                ),
                temperature=float(temperature),
                json_schema=_DECISION_SCHEMA,
            )
            latency = (time.perf_counter() - started) * 1000.0
            latencies_ms.append(latency)
            action = str(payload.get("action") or "").strip().upper()
            confidence = payload.get("confidence")
            if action not in {"BUY", "SELL", "HOLD"}:
                action = "INVALID"
            actions.append(action)
            try:
                confidences.append(float(confidence))
            except Exception:
                pass
            sample = {
                "action": action,
                "confidence": payload.get("confidence"),
                "reason": str(payload.get("reason", ""))[:200],
            }
        except Exception as exc:
            latency = (time.perf_counter() - started) * 1000.0
            latencies_ms.append(latency)
            failures.append(f"{type(exc).__name__}: {exc}")

    ok_calls = max(0, len(latencies_ms) - len(failures))
    action_breakdown = {
        "BUY": int(sum(1 for item in actions if item == "BUY")),
        "SELL": int(sum(1 for item in actions if item == "SELL")),
        "HOLD": int(sum(1 for item in actions if item == "HOLD")),
        "INVALID": int(sum(1 for item in actions if item == "INVALID")),
    }
    total_actions = max(1, int(len(actions)))
    hold_rate = float(action_breakdown.get("HOLD", 0)) / float(total_actions)
    rejection_rate = float(len(failures)) / float(max(1, int(rounds)))
    avg_conf = (sum(confidences) / float(len(confidences))) if confidences else 0.0
    return {
        "label": label,
        "rounds": len(latencies_ms),
        "successes": ok_calls,
        "failures": len(failures),
        "failure_messages": failures[:6],
        "action_breakdown": action_breakdown,
        "hold_rate": round(hold_rate, 4),
        "trade_intent_rate": round(
            float(action_breakdown.get("BUY", 0) + action_breakdown.get("SELL", 0))
            / float(total_actions),
            4,
        ),
        "rejection_rate": round(rejection_rate, 4),
        "avg_confidence": round(avg_conf, 4),
        "latency_ms": {
            "p50": _percentile(latencies_ms, 0.50),
            "p95": _percentile(latencies_ms, 0.95),
            "max": round(max(latencies_ms), 3) if latencies_ms else 0.0,
            "avg": round(sum(latencies_ms) / len(latencies_ms), 3) if latencies_ms else 0.0,
        },
        "sample_response": sample,
    }


async def _probe_broker(settings: Settings) -> Dict[str, Any]:
    try:
        broker = build_broker(settings)
    except Exception as exc:
        return {
            "ok": False,
            "status": "init_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }

    if not hasattr(broker, "_run_worker"):
        return {
            "ok": False,
            "status": "unsupported",
            "broker_type": type(broker).__name__,
        }

    if not settings.ctrader_account_id:
        return {
            "ok": False,
            "status": "missing_account_id",
            "broker_type": type(broker).__name__,
        }

    try:
        payload = {"account_id": int(settings.ctrader_account_id)}
    except Exception:
        return {
            "ok": False,
            "status": "invalid_account_id",
            "broker_type": type(broker).__name__,
            "account_id": str(settings.ctrader_account_id),
        }

    try:
        data = await asyncio.to_thread(broker._run_worker, "health", payload)
        inner = dict(data or {})
        inner_ok = bool(inner.get("ok"))
        inner_status = str(inner.get("status") or "")
        if not inner_ok or inner_status in {"account_auth_failed", "auth_failed"}:
            return {
                "ok": False,
                "status": "worker_health_failed",
                "error": str(inner.get("message") or inner_status or "unknown"),
                "data": inner,
            }
        return {"ok": True, "status": "ok", "data": inner}
    except Exception as exc:
        return {
            "ok": False,
            "status": "worker_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }


def _build_recommendations(report: Dict[str, Any]) -> List[str]:
    recs: List[str] = []
    has_failover_snapshot = bool(report.get("failover_snapshot"))
    probes = list(report.get("llm_probes") or [])
    for probe in probes:
        label = str(probe.get("label") or "llm")
        failures = int(probe.get("failures") or 0)
        p95 = float(((probe.get("latency_ms") or {}).get("p95") or 0.0))
        hold_rate = float(probe.get("hold_rate") or 0.0)
        if failures > 0:
            if has_failover_snapshot:
                recs.append(
                    f"{label}: observed failures={failures}; keep fallback chain enabled and inspect failover snapshot."
                )
            else:
                recs.append(
                    f"{label}: observed failures={failures}; configure LOCAL_FALLBACK_MODELS/OPENAI_FALLBACK_MODELS for multi-model failover."
                )
        if p95 > 3500:
            recs.append(
                f"{label}: p95 latency {p95:.0f}ms is high; reduce max tokens or move heavier model to self-improvement lane."
            )
        if hold_rate > 0.85:
            recs.append(
                f"{label}: hold_rate {hold_rate:.2f} is high; review hard filters/confidence floor to reduce missed opportunities."
            )
    broker = dict(report.get("broker_health") or {})
    if broker and not bool(broker.get("ok")):
        detail = str(broker.get("error") or broker.get("status") or "unknown")
        inner = dict(broker.get("data") or {})
        inner_status = str(inner.get("status") or "").strip()
        if inner_status:
            detail = f"{detail} ({inner_status})"
        recs.append(f"broker: {detail}; fix cTrader auth/account before enabling live execution.")
    if not recs:
        recs.append("burn-in checks passed; current Tencent profile is suitable for next live signal.")
    return recs


async def _run(args: argparse.Namespace) -> Dict[str, Any]:
    settings = load_settings()
    primary_llm = build_llm(settings)
    symbol = str(args.symbol or settings.symbol).strip().upper() or "XAUUSD"
    if settings.llm_provider is LLMProviderName.LOCAL:
        active_model = settings.local_model
    elif settings.llm_provider is LLMProviderName.OPENAI:
        active_model = settings.openai_model
    else:
        active_model = settings.mimo_model

    probes: List[Dict[str, Any]] = []
    probes.append(
        await _probe_llm(
            label="primary",
            client=primary_llm,
            symbol=symbol,
            rounds=args.rounds,
            temperature=args.temperature,
        )
    )

    if args.include_self_improvement:
        review_llm = build_self_improvement_llm(settings, primary_llm)
        if review_llm is not primary_llm:
            probes.append(
                await _probe_llm(
                    label="self_improvement",
                    client=review_llm,
                    symbol=symbol,
                    rounds=max(2, int(args.rounds // 2) or 2),
                    temperature=max(0.0, float(args.temperature) * 0.5),
                )
            )

    broker_health: Dict[str, Any] = {}
    if args.with_broker_health:
        broker_health = await _probe_broker(settings)

    report: Dict[str, Any] = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "instance_name": settings.instance_name,
        "symbol": symbol,
        "llm_provider": settings.llm_provider.value,
        "llm_model": active_model,
        "loop_interval_sec": settings.loop_interval_sec,
        "dry_run": settings.dry_run,
        "live_execution_enabled": settings.live_execution_enabled,
        "llm_probes": probes,
        "failover_snapshot": failover_runtime_snapshot(),
        "broker_health": broker_health,
    }
    report["recommendations"] = _build_recommendations(report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tencent infra burn-in: probe LLM failover chain and optional cTrader worker health."
    )
    parser.add_argument("--rounds", type=int, default=6, help="LLM probe rounds for primary chain.")
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.1,
        help="Probe temperature for JSON output stability test.",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="",
        help="Override probe symbol (ex: BTCUSD for weekend burn-in).",
    )
    parser.add_argument(
        "--include-self-improvement",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Probe dedicated Hermes/self-improvement model chain when configured.",
    )
    parser.add_argument(
        "--with-broker-health",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also run broker worker health probe (requires CTRADER account env).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Optional path to write burn-in JSON report.",
    )
    parser.add_argument(
        "--fail-on-error",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Exit code 1 if any LLM probe failures or broker health probe fails.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = asyncio.run(_run(args))
    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)

    if args.output:
        out_path = Path(str(args.output)).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text + "\n", encoding="utf-8")

    if args.fail_on_error:
        llm_failures = sum(int(item.get("failures") or 0) for item in list(report.get("llm_probes") or []))
        broker = dict(report.get("broker_health") or {})
        broker_failed = bool(broker) and not bool(broker.get("ok"))
        if llm_failures > 0 or broker_failed:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
