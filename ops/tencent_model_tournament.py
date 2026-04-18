from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from trading_ai.config import LLMProviderName, load_settings


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _discover_ollama_models() -> List[str]:
    try:
        proc = subprocess.run(
            ["ollama", "list"],
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=12,
            check=False,
        )
    except Exception:
        return []
    if proc.returncode != 0:
        return []
    rows = [line.strip() for line in str(proc.stdout or "").splitlines() if line.strip()]
    if not rows:
        return []
    out: List[str] = []
    for row in rows[1:]:
        token = str(row.split()[0] if row.split() else "").strip()
        if token and token not in out:
            out.append(token)
    return out


def _default_profiles() -> List[Dict[str, Any]]:
    settings = load_settings()
    profiles: List[Dict[str, Any]] = []
    if settings.llm_provider is not LLMProviderName.LOCAL:
        profiles.append(
            {
                "name": "current",
                "env": {
                    "LLM_PROVIDER": settings.llm_provider.value,
                },
            }
        )
        return profiles

    installed = _discover_ollama_models()
    installed_set = {str(item).strip() for item in installed}

    def _exists(model: str) -> bool:
        if not installed_set:
            return True
        return str(model).strip() in installed_set

    def _add(name: str, env: Dict[str, str]) -> None:
        model_name = str(env.get("LOCAL_MODEL_NAME") or "").strip()
        if model_name and not _exists(model_name):
            return
        fallback_csv = str(env.get("LOCAL_FALLBACK_MODELS") or "").strip()
        if fallback_csv and installed_set:
            keep = [item.strip() for item in fallback_csv.split(",") if item.strip() in installed_set]
            env = dict(env)
            env["LOCAL_FALLBACK_MODELS"] = ",".join(keep)
        profiles.append({"name": name, "env": env})

    _add(
        "current",
        {
            "LLM_PROVIDER": "local",
            "LOCAL_MODEL_NAME": str(settings.local_model),
            "LOCAL_FALLBACK_MODELS": str(settings.local_fallback_models or ""),
            "SELF_IMPROVEMENT_MODEL_NAME": str(settings.self_improvement_model_name or ""),
            "LLM_MAX_TOKENS": str(settings.llm_max_tokens),
            "LLM_TIMEOUT_SEC": str(settings.llm_timeout_sec),
            "MIN_TRADE_CONFIDENCE": str(settings.min_trade_confidence),
        },
    )

    # Fast lane candidates (low-latency primary)
    _add(
        "gemma3_fast",
        {
            "LLM_PROVIDER": "local",
            "LOCAL_MODEL_NAME": "gemma3:1b-it-qat",
            "LOCAL_FALLBACK_MODELS": "qwen2.5:0.5b,qwen2.5:1.5b",
            "SELF_IMPROVEMENT_MODEL_NAME": "qwen2.5:1.5b",
            "LLM_MAX_TOKENS": "80",
            "LLM_TIMEOUT_SEC": "15",
            "MIN_TRADE_CONFIDENCE": "0.60",
        },
    )
    _add(
        "qwen05_ultra_fast",
        {
            "LLM_PROVIDER": "local",
            "LOCAL_MODEL_NAME": "qwen2.5:0.5b",
            "LOCAL_FALLBACK_MODELS": "gemma3:1b-it-qat,qwen2.5:1.5b",
            "SELF_IMPROVEMENT_MODEL_NAME": "qwen2.5:1.5b",
            "LLM_MAX_TOKENS": "72",
            "LLM_TIMEOUT_SEC": "12",
            "MIN_TRADE_CONFIDENCE": "0.60",
        },
    )
    _add(
        "qwen15_balanced",
        {
            "LLM_PROVIDER": "local",
            "LOCAL_MODEL_NAME": "qwen2.5:1.5b",
            "LOCAL_FALLBACK_MODELS": "gemma3:1b-it-qat,qwen2.5:0.5b",
            "SELF_IMPROVEMENT_MODEL_NAME": "qwen2.5:1.5b",
            "LLM_MAX_TOKENS": "96",
            "LLM_TIMEOUT_SEC": "18",
            "MIN_TRADE_CONFIDENCE": "0.60",
        },
    )

    gemma4_model = next((m for m in installed if "gemma4" in m.lower()), "")
    if gemma4_model:
        _add(
            "gemma4_decision",
            {
                "LLM_PROVIDER": "local",
                "LOCAL_MODEL_NAME": gemma4_model,
                "LOCAL_FALLBACK_MODELS": "gemma3:1b-it-qat,qwen2.5:1.5b",
                "SELF_IMPROVEMENT_MODEL_NAME": gemma4_model,
                "LLM_MAX_TOKENS": "96",
                "LLM_TIMEOUT_SEC": "18",
                "MIN_TRADE_CONFIDENCE": "0.60",
            },
        )
        _add(
            "gemma3_with_gemma4_learning",
            {
                "LLM_PROVIDER": "local",
                "LOCAL_MODEL_NAME": "gemma3:1b-it-qat",
                "LOCAL_FALLBACK_MODELS": "qwen2.5:1.5b,qwen2.5:0.5b",
                "SELF_IMPROVEMENT_MODEL_NAME": gemma4_model,
                "LLM_MAX_TOKENS": "80",
                "LLM_TIMEOUT_SEC": "15",
                "MIN_TRADE_CONFIDENCE": "0.60",
            },
        )
    return profiles


def _load_profiles(path: Optional[str]) -> List[Dict[str, Any]]:
    if not path:
        return _default_profiles()
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    out: List[Dict[str, Any]] = []
    for idx, item in enumerate(list(payload or []), start=1):
        if not isinstance(item, dict):
            continue
        env = dict(item.get("env") or {})
        name = str(item.get("name") or f"profile_{idx}").strip() or f"profile_{idx}"
        out.append({"name": name, "env": {str(k): str(v) for k, v in env.items()}})
    return out


def _run_burnin(
    *,
    symbol: str,
    rounds: int,
    temperature: float,
    include_self_improvement: bool,
    with_broker_health: bool,
    timeout_sec: int,
    env_overrides: Dict[str, str],
) -> Dict[str, Any]:
    env = dict(os.environ)
    env.update({str(k): str(v) for k, v in dict(env_overrides or {}).items()})
    with tempfile.NamedTemporaryFile(prefix="mempalace_tournament_", suffix=".json", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    cmd: List[str] = [
        sys.executable,
        str(_REPO_ROOT / "ops" / "tencent_infra_burnin.py"),
        "--symbol",
        symbol,
        "--rounds",
        str(max(1, int(rounds))),
        "--temperature",
        str(float(temperature)),
        "--output",
        str(tmp_path),
    ]
    cmd.extend(
        [
            "--include-self-improvement" if include_self_improvement else "--no-include-self-improvement",
            "--with-broker-health" if with_broker_health else "--no-with-broker-health",
        ]
    )

    proc = subprocess.run(
        cmd,
        cwd=str(_REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=max(20, int(timeout_sec)),
        check=False,
    )
    output_text = str(proc.stdout or "")
    report_text = ""
    if tmp_path.exists():
        try:
            report_text = tmp_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            report_text = ""
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
    if not report_text.strip():
        report_text = output_text
    report = json.loads(report_text or "{}")
    return {
        "returncode": int(proc.returncode),
        "stdout": output_text,
        "stderr": str(proc.stderr or ""),
        "report": report,
    }


def _score_candidate(
    *,
    report: Dict[str, Any],
    target_hold_rate: float,
    weight_latency: float,
    weight_hold: float,
    weight_reject: float,
) -> Dict[str, Any]:
    probes = list(report.get("llm_probes") or [])
    primary = next((item for item in probes if str(item.get("label") or "") == "primary"), {}) or {}
    p95 = _safe_float(((primary.get("latency_ms") or {}).get("p95")), 99999.0)
    hold_rate = _safe_float(primary.get("hold_rate"), 1.0)
    rejection_rate = _safe_float(primary.get("rejection_rate"), 1.0)
    trade_intent_rate = _safe_float(primary.get("trade_intent_rate"), 0.0)
    avg_confidence = _safe_float(primary.get("avg_confidence"), 0.0)

    latency_penalty = min(2.0, max(0.0, p95) / 8000.0)
    hold_gap = abs(max(0.0, min(1.0, hold_rate)) - max(0.0, min(1.0, target_hold_rate)))
    hold_penalty = min(2.0, hold_gap / max(0.05, target_hold_rate))
    reject_penalty = min(2.0, max(0.0, rejection_rate) / 0.20)

    broker = dict(report.get("broker_health") or {})
    broker_ok = bool(broker.get("ok", True))
    broker_penalty = 1.0 if not broker_ok else 0.0

    score = (
        float(weight_latency) * latency_penalty
        + float(weight_hold) * hold_penalty
        + float(weight_reject) * reject_penalty
        + broker_penalty
    )
    return {
        "score": round(score, 6),
        "p95_ms": round(p95, 3),
        "hold_rate": round(hold_rate, 4),
        "trade_intent_rate": round(trade_intent_rate, 4),
        "rejection_rate": round(rejection_rate, 4),
        "avg_confidence": round(avg_confidence, 4),
        "broker_ok": broker_ok,
        "broker_status": str((broker.get("status") or "") if broker else ""),
    }


def _env_patch_from_candidate(candidate: Dict[str, Any]) -> Dict[str, str]:
    env = {str(k): str(v) for k, v in dict(candidate.get("env") or {}).items()}
    keep_keys = {
        "LLM_PROVIDER",
        "LOCAL_MODEL_NAME",
        "LOCAL_FALLBACK_MODELS",
        "SELF_IMPROVEMENT_MODEL_NAME",
        "SELF_IMPROVEMENT_MAX_TOKENS",
        "SELF_IMPROVEMENT_TIMEOUT_SEC",
        "LLM_MAX_TOKENS",
        "LLM_TIMEOUT_SEC",
        "MIN_TRADE_CONFIDENCE",
    }
    return {k: v for k, v in env.items() if k in keep_keys and str(v).strip()}


def _apply_env_patch(env_path: Path, patch: Dict[str, str]) -> None:
    env_path = Path(env_path)
    env_path.parent.mkdir(parents=True, exist_ok=True)
    lines = env_path.read_text(encoding="utf-8", errors="replace").splitlines() if env_path.exists() else []
    keys = list(patch.keys())
    kept: List[str] = []
    for line in lines:
        text = str(line or "")
        stripped = text.strip()
        if "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in patch:
                continue
        kept.append(text)
    if kept and kept[-1].strip():
        kept.append("")
    for key in keys:
        kept.append(f"{key}={patch[key]}")
    env_path.write_text("\n".join(kept).rstrip() + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run automatic Tencent model tournament and select best Mempalace profile."
    )
    parser.add_argument("--symbol", default="", help="Probe symbol (default from settings).")
    parser.add_argument("--rounds", type=int, default=4, help="Burn-in rounds per profile.")
    parser.add_argument("--temperature", type=float, default=0.1, help="Burn-in probe temperature.")
    parser.add_argument(
        "--profiles-file",
        default="",
        help="Optional JSON file with profile list [{name, env:{...}}].",
    )
    parser.add_argument(
        "--include-self-improvement",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include dedicated self-improvement lane probe in each profile burn-in.",
    )
    parser.add_argument(
        "--with-broker-health",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include cTrader worker health check in each profile burn-in.",
    )
    parser.add_argument("--target-hold-rate", type=float, default=0.55, help="Target hold-rate for balanced aggressiveness.")
    parser.add_argument("--weight-latency", type=float, default=0.50, help="Latency weight in score.")
    parser.add_argument("--weight-hold", type=float, default=0.30, help="Hold-rate weight in score.")
    parser.add_argument("--weight-reject", type=float, default=0.20, help="Rejection-rate weight in score.")
    parser.add_argument("--timeout-sec", type=int, default=240, help="Timeout per profile burn-in run.")
    parser.add_argument(
        "--apply",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Apply winner env patch to trading_ai/.env automatically.",
    )
    parser.add_argument(
        "--env-path",
        default="trading_ai/.env",
        help="Env file path used when --apply is enabled.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional JSON report output path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings()
    symbol = str(args.symbol or settings.symbol or "XAUUSD").strip().upper()
    profiles = _load_profiles(str(args.profiles_file or "").strip() or None)
    if not profiles:
        raise SystemExit("No tournament profiles available.")

    rows: List[Dict[str, Any]] = []
    for profile in profiles:
        name = str(profile.get("name") or "unnamed").strip() or "unnamed"
        env_overrides = {str(k): str(v) for k, v in dict(profile.get("env") or {}).items()}
        run = _run_burnin(
            symbol=symbol,
            rounds=max(1, int(args.rounds)),
            temperature=float(args.temperature),
            include_self_improvement=bool(args.include_self_improvement),
            with_broker_health=bool(args.with_broker_health),
            timeout_sec=max(20, int(args.timeout_sec)),
            env_overrides=env_overrides,
        )
        report = dict(run.get("report") or {})
        summary = _score_candidate(
            report=report,
            target_hold_rate=float(args.target_hold_rate),
            weight_latency=float(args.weight_latency),
            weight_hold=float(args.weight_hold),
            weight_reject=float(args.weight_reject),
        )
        rows.append(
            {
                "name": name,
                "env": env_overrides,
                "returncode": int(run.get("returncode") or 0),
                "summary": summary,
                "llm_model": str(report.get("llm_model") or ""),
                "recommendations": list(report.get("recommendations") or []),
                "report": report,
                "stdout_tail": str(run.get("stdout") or "").strip()[-2000:],
                "stderr_tail": str(run.get("stderr") or "").strip()[-2000:],
            }
        )

    rows.sort(key=lambda item: (float((item.get("summary") or {}).get("score") or 9999.0), str(item.get("name") or "")))
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx

    winner = rows[0]
    winner_patch = _env_patch_from_candidate(winner)
    applied = False
    if args.apply and winner_patch:
        _apply_env_patch((_REPO_ROOT / str(args.env_path)).resolve(), winner_patch)
        applied = True

    payload = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbol": symbol,
        "rounds": max(1, int(args.rounds)),
        "profiles_tested": len(rows),
        "scoring": {
            "target_hold_rate": float(args.target_hold_rate),
            "weights": {
                "latency": float(args.weight_latency),
                "hold_rate": float(args.weight_hold),
                "rejection_rate": float(args.weight_reject),
            },
            "formula": "lower_score_is_better; score = w_latency*latency_penalty + w_hold*hold_penalty + w_reject*rejection_penalty + broker_penalty",
        },
        "winner": {
            "name": str(winner.get("name") or ""),
            "rank": int(winner.get("rank") or 1),
            "summary": dict(winner.get("summary") or {}),
            "llm_model": str(winner.get("llm_model") or ""),
            "env_patch": winner_patch,
        },
        "applied": bool(applied),
        "rows": rows,
    }

    text = json.dumps(payload, ensure_ascii=False, indent=2)
    print(text)
    if str(args.output or "").strip():
        output_path = Path(str(args.output)).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
