from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from trading_ai.config import Settings
from trading_ai.utils.logger import get_logger

log = get_logger(__name__)
UTC = timezone.utc

_POLICY_KEYS = {
    "hard_filter_adaptive_min_trades",
    "hard_filter_adaptive_support_edge_min",
    "hard_filter_adaptive_min_opportunity",
    "hard_filter_adaptive_max_risk",
    "hard_filter_adaptive_min_edge",
    "hard_filter_adaptive_min_impulse_support",
    "hard_filter_adaptive_max_loss_rate",
    "hard_filter_adaptive_recent_window",
    "hard_filter_adaptive_recent_min_samples",
    "hard_filter_adaptive_recent_neg_edge_block",
    "hard_filter_adaptive_recent_pos_edge_bonus",
}


def _iso_utc(ts: Optional[float] = None) -> str:
    now = datetime.fromtimestamp(float(ts if ts is not None else time.time()), tz=UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")


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


def _clamp(value: float, low: float, high: float) -> float:
    if value < low:
        return low
    if value > high:
        return high
    return value


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        if not path.is_file():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        return dict(payload) if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_report_json(raw_stdout: str) -> Dict[str, Any]:
    text = str(raw_stdout or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
        return dict(payload) if isinstance(payload, dict) else {}
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            payload = json.loads(text[start : end + 1])
            return dict(payload) if isinstance(payload, dict) else {}
        except Exception:
            return {}
    return {}


def adaptive_hard_filter_baseline(settings: Settings) -> Dict[str, Any]:
    return {
        "hard_filter_adaptive_min_trades": int(settings.hard_filter_adaptive_min_trades),
        "hard_filter_adaptive_support_edge_min": int(settings.hard_filter_adaptive_support_edge_min),
        "hard_filter_adaptive_min_opportunity": float(settings.hard_filter_adaptive_min_opportunity),
        "hard_filter_adaptive_max_risk": float(settings.hard_filter_adaptive_max_risk),
        "hard_filter_adaptive_min_edge": float(settings.hard_filter_adaptive_min_edge),
        "hard_filter_adaptive_min_impulse_support": float(settings.hard_filter_adaptive_min_impulse_support),
        "hard_filter_adaptive_max_loss_rate": float(settings.hard_filter_adaptive_max_loss_rate),
        "hard_filter_adaptive_recent_window": int(settings.hard_filter_adaptive_recent_window),
        "hard_filter_adaptive_recent_min_samples": int(settings.hard_filter_adaptive_recent_min_samples),
        "hard_filter_adaptive_recent_neg_edge_block": float(settings.hard_filter_adaptive_recent_neg_edge_block),
        "hard_filter_adaptive_recent_pos_edge_bonus": float(settings.hard_filter_adaptive_recent_pos_edge_bonus),
    }


def build_adaptive_policy_from_backtest(
    *,
    report: Dict[str, Any],
    baseline: Dict[str, Any],
    min_closed_trades: int,
    min_win_rate: float,
    min_avg_profit: float,
    max_drawdown: float,
    max_shift: float,
    apply_enabled: bool,
) -> Dict[str, Any]:
    performance = dict(report.get("performance") or {})
    decisions = dict(report.get("decisions") or {})
    diagnostics = dict(report.get("diagnostics") or {})
    blocker_buckets = dict(diagnostics.get("blocker_buckets") or {})

    closed_trades = _safe_int(performance.get("closed_trades"), 0)
    win_rate = _safe_float(performance.get("win_rate"), 0.0)
    avg_profit = _safe_float(performance.get("avg_profit"), 0.0)
    max_dd_observed = _safe_float(performance.get("max_drawdown"), 0.0)

    total_decisions = max(1, sum(max(0, _safe_int(value, 0)) for value in decisions.values()))
    blocked_hard = sum(
        max(0, _safe_int(value, 0))
        for key, value in blocker_buckets.items()
        if str(key).startswith("pre_llm_hard_filter:")
    )
    blocked_ratio = blocked_hard / float(total_decisions)

    quality_gate_passed = (
        closed_trades >= int(min_closed_trades)
        and win_rate >= float(min_win_rate)
        and avg_profit >= float(min_avg_profit)
        and max_dd_observed <= float(max_drawdown)
    )

    recommended = dict(baseline)
    mode = "hold"
    notes: list[str] = []
    shift_cap = max(0.0, float(max_shift))
    shift = 0.0

    if quality_gate_passed and blocked_ratio >= 0.35:
        mode = "relax"
        relax_signal = ((blocked_ratio - 0.35) / 0.45) + max(0.0, win_rate - float(min_win_rate))
        relax_strength = _clamp(relax_signal, 0.0, 1.0)
        shift = shift_cap * relax_strength
        notes.append(f"relax_due_blocked_ratio={blocked_ratio:.3f}")
        notes.append(f"relax_strength={relax_strength:.3f}")

        recommended["hard_filter_adaptive_min_trades"] = max(
            2,
            int(_safe_int(baseline.get("hard_filter_adaptive_min_trades"), 3) - (1 if blocked_ratio >= 0.55 else 0)),
        )
        recommended["hard_filter_adaptive_support_edge_min"] = max(
            0,
            int(_safe_int(baseline.get("hard_filter_adaptive_support_edge_min"), 1) - (1 if blocked_ratio >= 0.60 else 0)),
        )
        recommended["hard_filter_adaptive_min_opportunity"] = _clamp(
            _safe_float(baseline.get("hard_filter_adaptive_min_opportunity"), 0.66) - shift,
            0.50,
            0.90,
        )
        recommended["hard_filter_adaptive_max_risk"] = _clamp(
            _safe_float(baseline.get("hard_filter_adaptive_max_risk"), 0.58) + (shift * 0.7),
            0.35,
            0.80,
        )
        recommended["hard_filter_adaptive_min_edge"] = _clamp(
            _safe_float(baseline.get("hard_filter_adaptive_min_edge"), 0.10) - (shift * 0.5),
            0.02,
            0.35,
        )
        recommended["hard_filter_adaptive_min_impulse_support"] = _clamp(
            _safe_float(baseline.get("hard_filter_adaptive_min_impulse_support"), 0.66) - (shift * 0.4),
            0.50,
            0.90,
        )
        recommended["hard_filter_adaptive_max_loss_rate"] = _clamp(
            _safe_float(baseline.get("hard_filter_adaptive_max_loss_rate"), 0.62) + (shift * 0.5),
            0.45,
            0.90,
        )
    elif not quality_gate_passed:
        mode = "tighten"
        win_gap = max(0.0, float(min_win_rate) - win_rate) / max(0.01, float(min_win_rate))
        profit_gap = 1.0 if avg_profit < float(min_avg_profit) else 0.0
        dd_gap = max(0.0, max_dd_observed - float(max_drawdown)) / max(0.01, float(max_drawdown))
        tighten_strength = _clamp(max(win_gap, profit_gap, dd_gap, 0.35), 0.0, 1.0)
        shift = shift_cap * tighten_strength
        notes.append("tighten_due_quality_gate")
        notes.append(f"tighten_strength={tighten_strength:.3f}")

        recommended["hard_filter_adaptive_min_trades"] = min(
            12,
            int(_safe_int(baseline.get("hard_filter_adaptive_min_trades"), 3) + 1),
        )
        recommended["hard_filter_adaptive_support_edge_min"] = min(
            4,
            int(_safe_int(baseline.get("hard_filter_adaptive_support_edge_min"), 1) + 1),
        )
        recommended["hard_filter_adaptive_min_opportunity"] = _clamp(
            _safe_float(baseline.get("hard_filter_adaptive_min_opportunity"), 0.66) + shift,
            0.50,
            0.92,
        )
        recommended["hard_filter_adaptive_max_risk"] = _clamp(
            _safe_float(baseline.get("hard_filter_adaptive_max_risk"), 0.58) - (shift * 0.6),
            0.30,
            0.80,
        )
        recommended["hard_filter_adaptive_min_edge"] = _clamp(
            _safe_float(baseline.get("hard_filter_adaptive_min_edge"), 0.10) + (shift * 0.5),
            0.02,
            0.35,
        )
        recommended["hard_filter_adaptive_min_impulse_support"] = _clamp(
            _safe_float(baseline.get("hard_filter_adaptive_min_impulse_support"), 0.66) + (shift * 0.4),
            0.50,
            0.92,
        )
        recommended["hard_filter_adaptive_max_loss_rate"] = _clamp(
            _safe_float(baseline.get("hard_filter_adaptive_max_loss_rate"), 0.62) - (shift * 0.5),
            0.35,
            0.95,
        )
    else:
        notes.append("quality_ok_but_blocked_ratio_low")

    effective_overrides: Dict[str, Any] = {}
    if apply_enabled and mode != "hold":
        for key in sorted(_POLICY_KEYS):
            before = baseline.get(key)
            after = recommended.get(key, before)
            if before is None:
                effective_overrides[key] = after
                continue
            if isinstance(before, int):
                if int(after) != int(before):
                    effective_overrides[key] = int(after)
            else:
                if abs(_safe_float(after) - _safe_float(before)) > 1e-9:
                    effective_overrides[key] = float(after)

    return {
        "generated_utc": _iso_utc(),
        "mode": mode,
        "quality_gate": {
            "passed": bool(quality_gate_passed),
            "min_closed_trades": int(min_closed_trades),
            "min_win_rate": float(min_win_rate),
            "min_avg_profit": float(min_avg_profit),
            "max_drawdown": float(max_drawdown),
            "closed_trades": closed_trades,
            "win_rate": round(win_rate, 4),
            "avg_profit": round(avg_profit, 6),
            "max_drawdown_observed": round(max_dd_observed, 6),
        },
        "metrics": {
            "total_decisions": int(total_decisions),
            "hard_filter_blocks": int(blocked_hard),
            "hard_filter_block_ratio": round(blocked_ratio, 4),
            "shift_applied": round(shift, 6),
        },
        "baseline": dict(baseline),
        "recommended": dict(recommended),
        "effective_overrides": dict(effective_overrides),
        "policy_apply_enabled": bool(apply_enabled),
        "notes": notes,
    }


class BacktestLearningSupervisor:
    """
    Periodic Hermes-style research lane:
    run backtest -> evaluate quality -> update adaptive hard-filter policy.
    """

    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings
        self._repo_root = Path(__file__).resolve().parents[2]
        self._state_path = Path(settings.backtest_learning_state_path)
        self._policy_path = Path(settings.backtest_learning_policy_path)
        self._summary_path = Path(settings.backtest_learning_summary_path)
        self._interval_sec = max(300.0, float(settings.backtest_learning_interval_sec))
        self._failure_backoff_sec = max(60.0, float(settings.backtest_learning_failure_backoff_sec))
        self._running = False
        self._last_attempt_ts = 0.0
        self._last_success = False
        self._overrides: Dict[str, Any] = {}
        self._load_state()
        self._load_policy()

    def _load_state(self) -> None:
        state = _read_json(self._state_path)
        self._last_attempt_ts = _safe_float(state.get("last_attempt_ts"), 0.0)
        self._last_success = bool(state.get("last_success", False))

    def _load_policy(self) -> None:
        policy = _read_json(self._policy_path)
        raw = dict(policy.get("effective_overrides") or {})
        self._overrides = {k: raw[k] for k in raw if k in _POLICY_KEYS}

    def _persist_state(self, *, status: str, error: str = "") -> None:
        payload = {
            "updated_utc": _iso_utc(),
            "status": str(status),
            "error": str(error or ""),
            "last_attempt_ts": float(self._last_attempt_ts),
            "last_success": bool(self._last_success),
            "running": bool(self._running),
        }
        _write_json(self._state_path, payload)

    def should_run(self, now_ts: Optional[float] = None) -> bool:
        if not bool(self._settings.backtest_learning_enabled):
            return False
        if self._running:
            return False
        now = float(now_ts if now_ts is not None else time.time())
        if self._last_attempt_ts <= 0.0:
            return True
        spacing = self._interval_sec if self._last_success else min(self._interval_sec, self._failure_backoff_sec)
        return (now - self._last_attempt_ts) >= spacing

    def current_overrides(self) -> Dict[str, Any]:
        return dict(self._overrides)

    async def trigger_if_due(self) -> Optional[Dict[str, Any]]:
        if not self.should_run():
            return None
        self._running = True
        self._last_attempt_ts = time.time()
        self._persist_state(status="running")
        try:
            policy = await asyncio.to_thread(self._run_once_sync)
            self._last_success = True
            self._overrides = {
                k: v
                for k, v in dict(policy.get("effective_overrides") or {}).items()
                if k in _POLICY_KEYS
            }
            self._persist_state(status="ok")
            return policy
        except Exception as exc:
            self._last_success = False
            self._persist_state(status="error", error=str(exc))
            log.warning("BacktestLearning: cycle failed: %s", exc)
            return None
        finally:
            self._running = False

    def _compute_window(self) -> tuple[str, str, str]:
        tz_name = str(self._settings.backtest_learning_timezone or "Asia/Bangkok")
        try:
            local_tz = ZoneInfo(tz_name)
        except Exception:
            local_tz = UTC
            tz_name = "UTC"
        end_offset = max(0, int(self._settings.backtest_learning_end_offset_days))
        lookback_days = max(2, int(self._settings.backtest_learning_lookback_days))
        today_local = datetime.now(local_tz).date()
        end_day = today_local - timedelta(days=end_offset)
        if end_day >= today_local:
            end_day = today_local - timedelta(days=1)
        start_day = end_day - timedelta(days=lookback_days - 1)
        return start_day.isoformat(), end_day.isoformat(), tz_name

    def _run_backtest_subprocess(self, *, start_day: str, end_day: str, tz_name: str) -> Dict[str, Any]:
        dexter_root = Path(self._settings.backtest_learning_dexter_root)
        if not dexter_root.exists():
            raise RuntimeError(f"backtest_dexter_root_missing:{dexter_root}")
        output_root = Path(self._settings.backtest_learning_output_root)
        output_root.mkdir(parents=True, exist_ok=True)

        cmd = [
            sys.executable,
            "-m",
            "trading_ai.backtest",
            "--start",
            str(start_day),
            "--end",
            str(end_day),
            "--timezone",
            str(tz_name),
            "--symbol",
            str(self._settings.symbol),
            "--timeframe",
            str(self._settings.backtest_learning_timeframe),
            "--dexter-root",
            str(dexter_root),
            "--output-root",
            str(output_root),
            "--source-policy",
            str(self._settings.backtest_learning_source_policy),
            "--enable-learning",
        ]
        proc = subprocess.run(
            cmd,
            cwd=str(self._repo_root),
            capture_output=True,
            text=True,
            timeout=max(120, int(self._settings.backtest_learning_timeout_sec)),
            check=False,
        )
        if int(proc.returncode) != 0:
            stderr = str(proc.stderr or "").strip().splitlines()
            tail = " | ".join(stderr[-8:]) if stderr else "no_stderr"
            raise RuntimeError(f"backtest_failed:code={proc.returncode}:{tail}")
        report = _extract_report_json(str(proc.stdout or ""))
        if not report:
            raise RuntimeError("backtest_report_parse_failed")
        return report

    def _write_summary(self, policy: Dict[str, Any]) -> None:
        gate = dict(policy.get("quality_gate") or {})
        metrics = dict(policy.get("metrics") or {})
        overrides = dict(policy.get("effective_overrides") or {})
        lines = [
            "# Backtest Learning Policy",
            "",
            f"- Generated UTC: `{policy.get('generated_utc')}`",
            f"- Mode: `{policy.get('mode')}`",
            f"- Quality gate passed: `{gate.get('passed')}`",
            f"- Closed trades: `{gate.get('closed_trades')}`",
            f"- Win rate: `{gate.get('win_rate')}`",
            f"- Avg profit: `{gate.get('avg_profit')}`",
            f"- Max drawdown: `{gate.get('max_drawdown_observed')}`",
            f"- Hard-filter block ratio: `{metrics.get('hard_filter_block_ratio')}`",
            "",
            "## Effective Overrides",
        ]
        if overrides:
            for key in sorted(overrides):
                lines.append(f"- `{key}` = `{overrides[key]}`")
        else:
            lines.append("- none")
        notes = list(policy.get("notes") or [])
        lines.append("")
        lines.append("## Notes")
        if notes:
            for item in notes:
                lines.append(f"- {item}")
        else:
            lines.append("- none")
        self._summary_path.parent.mkdir(parents=True, exist_ok=True)
        self._summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _run_once_sync(self) -> Dict[str, Any]:
        start_day, end_day, tz_name = self._compute_window()
        report = self._run_backtest_subprocess(start_day=start_day, end_day=end_day, tz_name=tz_name)
        baseline = adaptive_hard_filter_baseline(self._settings)
        policy = build_adaptive_policy_from_backtest(
            report=report,
            baseline=baseline,
            min_closed_trades=int(self._settings.backtest_learning_min_closed_trades),
            min_win_rate=float(self._settings.backtest_learning_min_win_rate),
            min_avg_profit=float(self._settings.backtest_learning_min_avg_profit),
            max_drawdown=float(self._settings.backtest_learning_max_drawdown),
            max_shift=float(self._settings.backtest_learning_policy_max_shift),
            apply_enabled=bool(self._settings.backtest_learning_policy_apply_enabled),
        )
        policy["run_window"] = {
            "start_day": start_day,
            "end_day": end_day,
            "timezone": tz_name,
            "symbol": str(self._settings.symbol),
            "timeframe": str(self._settings.backtest_learning_timeframe),
            "source_policy": str(self._settings.backtest_learning_source_policy),
            "dexter_root": str(self._settings.backtest_learning_dexter_root),
            "output_root": str(self._settings.backtest_learning_output_root),
        }
        report_mode = str(report.get("mode") or "")
        run_id = str(report.get("run_id") or "")
        if run_id:
            policy["run_id"] = run_id
        if report_mode:
            policy["backtest_mode"] = report_mode
        _write_json(self._policy_path, policy)
        self._write_summary(policy)
        log.info(
            "BacktestLearning: mode=%s quality=%s closed=%s win_rate=%.3f blocked_ratio=%.3f overrides=%s",
            str(policy.get("mode") or "hold"),
            bool(((policy.get("quality_gate") or {}).get("passed"))),
            int(((policy.get("quality_gate") or {}).get("closed_trades") or 0)),
            float(((policy.get("quality_gate") or {}).get("win_rate") or 0.0)),
            float(((policy.get("metrics") or {}).get("hard_filter_block_ratio") or 0.0)),
            len(dict(policy.get("effective_overrides") or {})),
        )
        return policy
