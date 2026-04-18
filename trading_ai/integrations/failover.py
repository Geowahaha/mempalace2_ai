from __future__ import annotations

import time
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Dict, Iterable, List, Tuple

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)

_REGISTRY_LOCK = RLock()
_FAILOVER_REGISTRY: Dict[str, "FailoverProvider"] = {}


def failover_runtime_snapshot() -> Dict[str, Any]:
    """Thread-safe snapshot of all registered failover chains."""
    with _REGISTRY_LOCK:
        return {
            name: provider.snapshot()
            for name, provider in _FAILOVER_REGISTRY.items()
        }


def clear_failover_runtime_registry() -> None:
    """Testing utility to reset global failover telemetry."""
    with _REGISTRY_LOCK:
        _FAILOVER_REGISTRY.clear()


class FailoverProvider:
    """Try multiple LLM backends in order until one returns valid JSON."""

    def __init__(
        self,
        providers: Iterable[Tuple[str, Any]],
        *,
        name: str | None = None,
        failure_threshold: int = 2,
        cooldown_sec: float = 20.0,
    ) -> None:
        self._providers: List[Tuple[str, Any]] = list(providers)
        if not self._providers:
            raise ValueError("FailoverProvider requires at least one provider")
        self._name = str(name or f"failover-{id(self)}").strip()
        self._failure_threshold = max(1, int(failure_threshold))
        self._cooldown_sec = max(0.0, float(cooldown_sec))
        self._lock = RLock()
        self._stats: Dict[str, Dict[str, Any]] = {}
        now_ts = time.time()
        for label, _ in self._providers:
            self._stats[str(label)] = {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "skipped_circuit_open": 0,
                "consecutive_failures": 0,
                "last_error": "",
                "last_error_ts": 0.0,
                "last_success_ts": 0.0,
                "last_latency_ms": 0.0,
                "latency_total_ms": 0.0,
                "circuit_open_until_ts": 0.0,
                "created_ts": now_ts,
            }
        with _REGISTRY_LOCK:
            _FAILOVER_REGISTRY[self._name] = self

    def _is_circuit_open(self, label: str, now_ts: float) -> bool:
        with self._lock:
            stat = self._stats.get(label)
            if not stat:
                return False
            return float(stat.get("circuit_open_until_ts") or 0.0) > now_ts

    def _mark_circuit_skip(self, label: str) -> None:
        with self._lock:
            stat = self._stats.get(label)
            if not stat:
                return
            stat["skipped_circuit_open"] = int(stat.get("skipped_circuit_open") or 0) + 1

    def _circuit_wait_sec(self, label: str, now_ts: float) -> float:
        with self._lock:
            stat = self._stats.get(label)
            if not stat:
                return 0.0
            open_until = float(stat.get("circuit_open_until_ts") or 0.0)
            return max(0.0, open_until - now_ts)

    def _mark_success(self, label: str, latency_ms: float) -> None:
        now_ts = time.time()
        with self._lock:
            stat = self._stats.get(label)
            if not stat:
                return
            stat["attempts"] = int(stat.get("attempts") or 0) + 1
            stat["successes"] = int(stat.get("successes") or 0) + 1
            stat["consecutive_failures"] = 0
            stat["last_success_ts"] = now_ts
            stat["last_latency_ms"] = float(latency_ms)
            stat["latency_total_ms"] = float(stat.get("latency_total_ms") or 0.0) + float(latency_ms)
            stat["circuit_open_until_ts"] = 0.0

    def _mark_failure(self, label: str, exc: Exception, latency_ms: float) -> float:
        now_ts = time.time()
        with self._lock:
            stat = self._stats.get(label)
            if not stat:
                return 0.0
            stat["attempts"] = int(stat.get("attempts") or 0) + 1
            stat["failures"] = int(stat.get("failures") or 0) + 1
            stat["consecutive_failures"] = int(stat.get("consecutive_failures") or 0) + 1
            stat["last_error"] = f"{type(exc).__name__}: {exc}"
            stat["last_error_ts"] = now_ts
            stat["last_latency_ms"] = float(latency_ms)
            stat["latency_total_ms"] = float(stat.get("latency_total_ms") or 0.0) + float(latency_ms)
            if int(stat["consecutive_failures"]) >= self._failure_threshold:
                stat["circuit_open_until_ts"] = now_ts + self._cooldown_sec
            return float(stat.get("circuit_open_until_ts") or 0.0)

    def snapshot(self) -> Dict[str, Any]:
        now_ts = time.time()
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._lock:
            rows: List[Dict[str, Any]] = []
            for label, _ in self._providers:
                stat = dict(self._stats.get(label) or {})
                attempts = int(stat.get("attempts") or 0)
                failures = int(stat.get("failures") or 0)
                successes = int(stat.get("successes") or 0)
                open_until = float(stat.get("circuit_open_until_ts") or 0.0)
                rows.append(
                    {
                        "label": label,
                        "attempts": attempts,
                        "successes": successes,
                        "failures": failures,
                        "success_rate": round((successes / attempts), 4) if attempts else 0.0,
                        "failure_rate": round((failures / attempts), 4) if attempts else 0.0,
                        "avg_latency_ms": round(
                            float(stat.get("latency_total_ms") or 0.0) / attempts, 3
                        )
                        if attempts
                        else 0.0,
                        "last_latency_ms": round(float(stat.get("last_latency_ms") or 0.0), 3),
                        "consecutive_failures": int(stat.get("consecutive_failures") or 0),
                        "skipped_circuit_open": int(stat.get("skipped_circuit_open") or 0),
                        "circuit_open": open_until > now_ts,
                        "circuit_open_for_sec": round(max(0.0, open_until - now_ts), 3),
                        "last_error": str(stat.get("last_error") or ""),
                        "last_error_ts": float(stat.get("last_error_ts") or 0.0),
                        "last_success_ts": float(stat.get("last_success_ts") or 0.0),
                    }
                )
        return {
            "name": self._name,
            "generated_utc": now_utc,
            "failure_threshold": self._failure_threshold,
            "cooldown_sec": self._cooldown_sec,
            "candidates": rows,
        }

    async def complete_json(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        json_schema: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        errors: List[str] = []
        cycle_start_ts = time.time()
        for label, provider in self._providers:
            if self._is_circuit_open(label, cycle_start_ts):
                self._mark_circuit_skip(label)
                wait_sec = self._circuit_wait_sec(label, cycle_start_ts)
                msg = f"{label}: circuit_open({wait_sec:.1f}s)"
                errors.append(msg)
                log.warning("LLM candidate skipped (open circuit): %s", msg)
                continue

            started = time.perf_counter()
            try:
                result = await provider.complete_json(
                    system=system,
                    user=user,
                    temperature=temperature,
                    json_schema=json_schema,
                )
                latency_ms = (time.perf_counter() - started) * 1000.0
                self._mark_success(label, latency_ms)
                if errors:
                    log.info("LLM failover recovered via %s after %s prior errors", label, len(errors))
                return result
            except Exception as exc:
                latency_ms = (time.perf_counter() - started) * 1000.0
                open_until_ts = self._mark_failure(label, exc, latency_ms)
                msg = f"{label}: {type(exc).__name__}: {exc}"
                errors.append(msg)
                log.warning("LLM candidate failed: %s", msg)
                if open_until_ts > time.time():
                    log.warning(
                        "LLM candidate circuit opened: %s cooldown=%.1fs threshold=%s",
                        label,
                        self._cooldown_sec,
                        self._failure_threshold,
                    )
        raise RuntimeError("All LLM candidates failed | " + " | ".join(errors))
