from __future__ import annotations

import asyncio
import csv
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

from trading_ai.config import Settings
from trading_ai.core.execution import (
    Action,
    Broker,
    CloseResult,
    MarketSnapshot,
    OpenPosition,
    TradeResult,
)
from trading_ai.utils.logger import get_logger

log = get_logger(__name__)

_DEFAULT_WORKER_CANDIDATES = (
    Path(r"D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed\ops\ctrader_execute_once.py"),
)
_AUTH_FAILURE_STATUSES = {"app_auth_failed", "account_auth_failed"}
_TOKEN_ENV_KEYS = {
    "access_token": (
        "CTRADER_OPENAPI_ACCESS_TOKEN",
        "CTRADER_ACCESS_TOKEN",
        "OpenAPI_Access_token_API_key3",
        "OpenAPI_Access_token_API_key2",
        "OpenAPI_Access_token_API_key",
        "new_Accesstoken",
        "new_Access_token",
    ),
    "refresh_token": (
        "CTRADER_OPENAPI_REFRESH_TOKEN",
        "CTRADER_REFRESH_TOKEN",
        "OpenAPI_Refresh_token_API_key3",
        "OpenAPI_Refresh_token_API_key2",
        "OpenAPI_Refresh_token_API_key",
        "new_Refresh_token",
    ),
}


def _resolve_worker_script(settings: Settings) -> Path:
    if settings.ctrader_worker_script:
        p = Path(settings.ctrader_worker_script)
        if p.is_file():
            return p.resolve()
    for cand in _DEFAULT_WORKER_CANDIDATES:
        if cand.is_file():
            return cand.resolve()
    raise FileNotFoundError(
        "Set CTRADER_WORKER_SCRIPT to dexter_pro_v3_fixed/ops/ctrader_execute_once.py"
    )


def _resolve_worker_python(settings: Settings, dexter_root: Path) -> str:
    """Prefer Dexter repo venv (Twisted + ctrader Open API); Mempalac venv usually lacks them."""
    raw_python = str(settings.ctrader_worker_python or "").strip().strip('"').strip("'")
    if raw_python:
        p = Path(raw_python).expanduser()
        if p.is_file():
            return str(p)
        rel = dexter_root / p
        if rel.is_file():
            return str(rel)
    search_roots = [
        dexter_root,
        Path(__file__).resolve().parent.parent,
        Path(__file__).resolve().parent,
    ]
    rel_candidates = (
        Path(".venv") / "bin" / "python",
        Path("venv") / "bin" / "python",
        Path(".venv") / "Scripts" / "python.exe",
        Path("venv") / "Scripts" / "python.exe",
    )
    for root in search_roots:
        for rel in rel_candidates:
            cand = root / rel
            if cand.is_file():
                return str(cand)
    for rel in rel_candidates:
        cand = dexter_root / rel
        if cand.is_file():
            return str(cand)
    return str(Path(sys.executable))


def _to_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out <= 0:
        return None
    return out


def _normalize_symbol(value: Any) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _extract_price(data: Dict[str, Any], *paths: tuple[str, ...]) -> Optional[float]:
    for path in paths:
        cur: Any = data
        ok = True
        for part in path:
            if not isinstance(cur, dict) or part not in cur:
                ok = False
                break
            cur = cur[part]
        if ok:
            price = _to_float(cur)
            if price is not None:
                return price
    return None


def _compact_broker_comment(value: Any, *, limit: int = 24) -> str:
    text = str(value or "").strip()
    if not text:
        return "mempalac"
    head = text.split("|", 1)[0].strip()
    for sep in (":", "("):
        if sep in head:
            head = head.split(sep, 1)[0].strip()
            break
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in head)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    cleaned = cleaned.strip("_")
    if not cleaned:
        cleaned = "mempalac"
    return cleaned[: max(4, int(limit))]


def _worker_mode_allows_retry(mode: str) -> bool:
    return mode in {"health", "reconcile", "capture_market", "accounts", "get_trendbars"}


def _worker_retry_attempts(mode: str) -> int:
    token = str(mode or "").strip().lower()
    if not _worker_mode_allows_retry(token):
        return 1
    if token == "capture_market":
        # Keep market quote path snappy. get_market_data has fallback handling.
        return 2
    if token in {"get_trendbars", "health"}:
        return 3
    return 5


def _worker_retry_sleep_sec(mode: str, attempt_number: int) -> float:
    token = str(mode or "").strip().lower()
    step = max(1, int(attempt_number))
    if token == "capture_market":
        return min(0.9, 0.25 * step)
    return 1.0 + step


def _worker_result_is_transient(result: Dict[str, Any]) -> bool:
    status = str(result.get("status") or "").strip().lower()
    message = str(result.get("message") or "").strip().lower()
    error_code = str(result.get("error_code") or "").strip().lower()
    if status in {"no_worker_json", "worker_failure", "worker_invalid_result"}:
        return True
    if error_code == "cant_route_request" or "cannot route request" in message:
        return False
    if status in _AUTH_FAILURE_STATUSES:
        return False
    if status == "error" and "deferred" in message:
        return True
    return False


def _worker_result_is_auth_failure(result: Dict[str, Any]) -> bool:
    status = str(result.get("status") or "").strip().lower()
    return status in _AUTH_FAILURE_STATUSES


class CTraderDexterWorkerBroker(Broker):
    """
    Live/demo orders via Dexter's one-shot Open API worker (protobuf + Twisted).

    Dexter remains a separate, read-only dependency:
    - Mempalac shells out to Dexter's worker script.
    - No shared imports or in-process reactor state.
    - Quotes can be fetched from `capture_market` so learning uses live/demo prices
      instead of synthetic PaperBroker quotes.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        quote_broker: Broker,
    ) -> None:
        self._settings = settings
        self._quote = quote_broker
        self._script = _resolve_worker_script(settings)
        dexter_root = self._script.parent.parent
        self._python = _resolve_worker_python(settings, dexter_root)
        self._dexter_root = dexter_root
        self._quote_cache: Dict[str, MarketSnapshot] = {}
        self._reference_quote_cache: Dict[str, MarketSnapshot] = {}
        self._quote_refresh_tasks: Dict[str, asyncio.Task[Any]] = {}
        self._project_root = Path(__file__).resolve().parents[2]
        self._package_root = Path(__file__).resolve().parents[1]
        self._env_paths = (
            self._project_root / ".env",
            self._package_root / ".env",
        )
        self._token_state_path = self._dexter_root / "data" / "runtime" / "ctrader_token_state.json"
        if self._python == str(Path(sys.executable).resolve()) and not settings.ctrader_worker_python:
            log.warning(
                "CTRADER_WORKER_PYTHON not set and no .venv\\Scripts\\python.exe under %s "
                "(install Twisted in Mempalac venv or point CTRADER_WORKER_PYTHON at Dexter Python)",
                dexter_root,
            )
        raw_id = str(settings.ctrader_account_id or "").strip()
        self._account_id = int(float(raw_id)) if raw_id else 0
        if self._account_id <= 0:
            raise ValueError(
                "CTRADER_ACCOUNT_ID must be the numeric ctidTraderAccountId from cTrader Open API "
                "(find it via Dexter health/accounts or broker UI export)."
            )

    def _read_env_tokens(self, path: Path) -> Dict[str, str]:
        if not path.is_file():
            return {}
        tokens: Dict[str, str] = {}
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return {}
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("#") or "=" not in stripped:
                continue
            key, _, value = stripped.partition("=")
            norm_key = key.strip()
            norm_value = value.strip()
            if not norm_value:
                continue
            for token_name, aliases in _TOKEN_ENV_KEYS.items():
                if norm_key in aliases and token_name not in tokens:
                    tokens[token_name] = norm_value
        return tokens

    def _sync_tokens_from_disk(self) -> Optional[Dict[str, Any]]:
        sources: list[Dict[str, Any]] = []
        for priority, path in enumerate(self._env_paths):
            if not path.is_file():
                continue
            tokens = self._read_env_tokens(path)
            if not tokens:
                continue
            try:
                mtime = path.stat().st_mtime
            except OSError:
                mtime = 0.0
            sources.append(
                {
                    "source": str(path),
                    "mtime": mtime,
                    "priority": priority,
                    "access_token": tokens.get("access_token", ""),
                    "refresh_token": tokens.get("refresh_token", ""),
                }
            )
        if self._token_state_path.is_file():
            try:
                payload = json.loads(self._token_state_path.read_text(encoding="utf-8"))
                mtime = self._token_state_path.stat().st_mtime
            except (OSError, json.JSONDecodeError):
                payload = {}
                mtime = 0.0
            access_token = str(payload.get("access_token") or "").strip()
            refresh_token = str(payload.get("refresh_token") or "").strip()
            if access_token or refresh_token:
                sources.append(
                    {
                        "source": str(self._token_state_path),
                        "mtime": mtime,
                        "priority": 10,
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                    }
                )
        if not sources:
            return None

        sources.sort(key=lambda item: (float(item.get("mtime") or 0.0), int(item.get("priority") or 0)), reverse=True)
        latest = sources[0]
        changed = False
        access_token = str(latest.get("access_token") or "").strip()
        refresh_token = str(latest.get("refresh_token") or "").strip()
        if access_token and access_token != str(self._settings.ctrader_access_token or "").strip():
            self._settings.ctrader_access_token = access_token
            changed = True
        if refresh_token and refresh_token != str(self._settings.ctrader_refresh_token or "").strip():
            self._settings.ctrader_refresh_token = refresh_token
            changed = True
        if not changed:
            return None
        return {
            "source": latest.get("source"),
            "mtime": latest.get("mtime"),
            "has_access_token": bool(access_token),
            "has_refresh_token": bool(refresh_token),
        }

    def _run_worker(self, mode: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        attempts = _worker_retry_attempts(mode)
        last_result: Dict[str, Any] = {
            "ok": False,
            "status": "worker_not_run",
            "message": f"worker never launched for mode={mode}",
        }
        auth_retry_used = False
        attempt = 0
        while attempt < attempts:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".json",
                delete=False,
            ) as tmp:
                json.dump(payload, tmp, ensure_ascii=True)
                path = tmp.name
            try:
                worker_env = os.environ.copy()
                if self._settings.ctrader_client_id:
                    value = str(self._settings.ctrader_client_id)
                    worker_env["CTRADER_OPENAPI_CLIENT_ID"] = value
                    worker_env["CTRADER_CLIENT_ID"] = value
                    worker_env["OpenAPI_ClientID"] = value
                if self._settings.ctrader_client_secret:
                    value = str(self._settings.ctrader_client_secret)
                    worker_env["CTRADER_OPENAPI_CLIENT_SECRET"] = value
                    worker_env["CTRADER_CLIENT_SECRET"] = value
                    worker_env["OpenAPI_Secreat"] = value
                    worker_env["OpenAPI_Secret"] = value
                if self._settings.ctrader_access_token:
                    value = str(self._settings.ctrader_access_token)
                    worker_env["CTRADER_OPENAPI_ACCESS_TOKEN"] = value
                    worker_env["CTRADER_ACCESS_TOKEN"] = value
                    worker_env["OpenAPI_Access_token_API_key"] = value
                    worker_env["OpenAPI_Access_token_API_key3"] = value
                if self._settings.ctrader_refresh_token:
                    value = str(self._settings.ctrader_refresh_token)
                    worker_env["CTRADER_OPENAPI_REFRESH_TOKEN"] = value
                    worker_env["CTRADER_REFRESH_TOKEN"] = value
                    worker_env["OpenAPI_Refresh_token_API_key"] = value
                    worker_env["OpenAPI_Refresh_token_API_key3"] = value
                if self._settings.ctrader_redirect_uri:
                    value = str(self._settings.ctrader_redirect_uri)
                    worker_env["CTRADER_OPENAPI_REDIRECT_URI"] = value
                    worker_env["OpenAPI_Redirect_URI"] = value
                worker_env["CTRADER_USE_DEMO"] = "1" if self._settings.ctrader_demo else "0"
                if self._settings.ctrader_account_id:
                    worker_env["CTRADER_ACCOUNT_ID"] = str(self._settings.ctrader_account_id)
                if self._settings.ctrader_account_login:
                    worker_env["CTRADER_ACCOUNT_LOGIN"] = str(self._settings.ctrader_account_login)
                cmd = [
                    self._python,
                    str(self._script),
                    "--mode",
                    mode,
                    "--payload-file",
                    path,
                ]
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=int(self._settings.ctrader_worker_timeout_sec),
                    cwd=str(self._dexter_root),
                    env=worker_env,
                )
                out = (proc.stdout or "").strip()
                err = (proc.stderr or "").strip()
                if proc.returncode != 0:
                    log.warning(
                        "cTrader worker mode=%s rc=%s stderr_tail=%s",
                        mode,
                        proc.returncode,
                        err[-1500:],
                    )
                json_lines = [ln for ln in out.splitlines() if ln.strip().startswith("{")]
                if not json_lines:
                    last_result = {
                        "ok": False,
                        "status": "no_worker_json",
                        "message": (out or err or "empty worker output")[-800:],
                        "mode": mode,
                    }
                else:
                    last_result = json.loads(json_lines[-1])
            finally:
                try:
                    os.unlink(path)
                except OSError:
                    pass

            if _worker_result_is_auth_failure(last_result) and not auth_retry_used:
                sync_meta = self._sync_tokens_from_disk()
                if sync_meta is not None:
                    auth_retry_used = True
                    log.warning(
                        "cTrader worker auth failure mode=%s status=%s; reloaded tokens from %s and retrying once",
                        mode,
                        last_result.get("status"),
                        sync_meta.get("source"),
                    )
                    continue

            if not _worker_mode_allows_retry(mode):
                return last_result
            if last_result.get("ok") or not _worker_result_is_transient(last_result):
                return last_result
            attempt += 1
            if attempt < attempts:
                sleep_sec = _worker_retry_sleep_sec(mode, attempt)
                log.warning(
                    "Transient cTrader worker failure mode=%s attempt=%s/%s status=%s message=%s; retrying in %.1fs",
                    mode,
                    attempt,
                    attempts,
                    last_result.get("status"),
                    str(last_result.get("message") or "")[:220],
                    sleep_sec,
                )
                time.sleep(sleep_sec)
        return last_result

    def _quote_source(self) -> str:
        src = str(self._settings.ctrader_quote_source or "auto").strip().lower()
        if src == "auto":
            return "dexter_capture"
        return src

    def _fetch_stooq_xauusd_price(self) -> Optional[float]:
        req = Request(
            "https://stooq.com/q/l/?s=xauusd&f=sd2t2ohlcvn&h&e=csv",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        try:
            with urlopen(req, timeout=float(self._settings.ctrader_reference_quote_timeout_sec)) as resp:
                body = resp.read(4096).decode("utf-8", errors="replace")
        except (OSError, URLError, TimeoutError) as exc:
            log.warning("Dexter-style reference quote fetch failed for XAUUSD: %s", exc)
            return None
        rows = list(csv.DictReader(body.splitlines()))
        if not rows:
            return None
        row = rows[0]
        price = _to_float(row.get("Close"))
        if price is None:
            return None
        return price

    def _reference_quote_snapshot(self, symbol: str, *, reason: str = "") -> Optional[MarketSnapshot]:
        if not bool(self._settings.ctrader_reference_quote_fallback_enabled):
            return None
        token = str(symbol).upper().replace(" ", "")
        normalized = _normalize_symbol(token)
        cached = self._reference_quote_cache.get(token)
        now = time.time()
        cache_ttl = max(5.0, float(self._settings.ctrader_quote_cache_ttl_sec))
        if cached and (now - cached.ts_unix) <= cache_ttl:
            return cached

        price: Optional[float] = None
        source = ""
        if normalized == "XAUUSD":
            price = self._fetch_stooq_xauusd_price()
            source = "stooq_xauusd_csv"
        if price is None:
            return None

        spread = float(self._settings.ctrader_reference_quote_spread)
        bid = max(float(price) - spread / 2.0, 0.0)
        ask = float(price) + spread / 2.0
        snap = MarketSnapshot(
            symbol=token,
            bid=bid,
            ask=ask,
            mid=float(price),
            spread=max(ask - bid, 0.0),
            ts_unix=now,
            extra={
                "venue": "dexter_reference_quote",
                "reference_source": source,
                "fallback_reason": str(reason or "")[:220],
                "broker_quote_available": False,
            },
        )
        self._reference_quote_cache[token] = snap
        self._quote_cache[token] = snap
        return snap

    def _allow_paper_fallback(self) -> bool:
        if self._quote_source() == "paper":
            return True
        return not self._settings.live_execution_enabled

    def get_account_equity(self) -> Optional[float]:
        """Best-effort balance/equity probe for exposure caps."""
        data = self._run_worker("health", {"account_id": int(self._account_id)})
        if not data.get("ok"):
            log.warning(
                "Account equity probe failed status=%s message=%s",
                data.get("status"),
                str(data.get("message") or "")[:220],
            )
            return None
        for key in ("equity", "balance", "balance_usd"):
            value = _to_float(data.get(key))
            if value is not None:
                return value
        return None

    def _extract_latest_snapshot(self, symbol: str, data: Dict[str, Any]) -> Optional[MarketSnapshot]:
        token = str(symbol).upper().replace(" ", "")
        normalized_token = _normalize_symbol(token)
        spots = list(data.get("spots") or [])
        matched = [
            row
            for row in spots
            if str(row.get("symbol") or "").upper().replace(" ", "") == token
            or _normalize_symbol(row.get("symbol")) == normalized_token
        ]
        if not matched and len(spots) > 0:
            resolved = list(data.get("resolved_symbols") or [])
            resolved_names = {_normalize_symbol(row.get("symbol")) for row in resolved if isinstance(row, dict)}
            spot_names = {_normalize_symbol(row.get("symbol")) for row in spots if isinstance(row, dict)}
            if normalized_token in resolved_names or len(spot_names) == 1:
                matched = spots
        if not matched:
            return None
        latest = max(matched, key=lambda row: float(row.get("event_ts") or 0.0))
        bid = _to_float(latest.get("bid"))
        ask = _to_float(latest.get("ask"))
        mid = _to_float(latest.get("mid"))
        if mid is None:
            if bid is not None and ask is not None:
                mid = (bid + ask) / 2.0
            else:
                return None
        spread = _to_float(latest.get("spread"))
        if spread is None and bid is not None and ask is not None:
            spread = max(ask - bid, 0.0)
        if bid is None and ask is not None and spread is not None:
            bid = ask - spread
        if ask is None and bid is not None and spread is not None:
            ask = bid + spread
        if bid is None or ask is None:
            return None
        fetched_at_ts = time.time()
        source_event_ts = _to_float(latest.get("event_ts"))
        snap = MarketSnapshot(
            symbol=token,
            bid=float(bid),
            ask=float(ask),
            mid=float(mid),
            spread=float(spread or max(ask - bid, 0.0)),
            ts_unix=float(fetched_at_ts),
            extra={
                "venue": "ctrader_dexter_capture",
                "environment": data.get("environment"),
                "capture_status": data.get("status"),
                "source_event_ts": float(source_event_ts) if source_event_ts is not None else None,
            },
        )
        self._quote_cache[token] = snap
        return snap

    def _capture_payload(self, token: str, *, duration_sec: Optional[int] = None) -> Dict[str, Any]:
        capture_duration = int(
            duration_sec
            if duration_sec is not None
            else int(self._settings.ctrader_capture_duration_sec)
        )
        capture_duration = max(1, min(15, capture_duration))
        max_events = max(4, min(120, int(getattr(self._settings, "ctrader_capture_max_events", 30))))
        return {
            "account_id": int(self._account_id),
            "symbols": [token],
            "include_depth": False,
            "duration_sec": capture_duration,
            "max_events": max_events,
        }

    async def _refresh_quote_in_background(self, token: str, *, reason: str) -> None:
        payload = self._capture_payload(token)
        try:
            data = await asyncio.to_thread(self._run_worker, "capture_market", payload)
            if self._extract_latest_snapshot(token, data) is not None:
                return
            log.warning(
                "Background quote refresh failed for %s: %s (reason=%s)",
                token,
                str(data.get("message") or data.get("status") or "capture_market failed")[:220],
                reason[:120],
            )
        except Exception as exc:
            log.warning(
                "Background quote refresh exception for %s: %s (reason=%s)",
                token,
                str(exc)[:220],
                reason[:120],
            )

    def _schedule_quote_refresh(self, token: str, *, reason: str) -> None:
        if not bool(getattr(self._settings, "ctrader_quote_background_refresh_enabled", True)):
            return
        existing = self._quote_refresh_tasks.get(token)
        if existing is not None and not existing.done():
            return
        try:
            task = asyncio.create_task(self._refresh_quote_in_background(token, reason=reason))
        except RuntimeError:
            return
        self._quote_refresh_tasks[token] = task

        def _cleanup(done_task: asyncio.Task[Any]) -> None:
            tracked = self._quote_refresh_tasks.get(token)
            if tracked is done_task:
                self._quote_refresh_tasks.pop(token, None)

        task.add_done_callback(_cleanup)

    def _effective_soft_stale_ttl_sec(self, cache_ttl: float) -> float:
        configured = float(getattr(self._settings, "ctrader_quote_soft_stale_ttl_sec", cache_ttl))
        loop_interval = max(0.0, float(getattr(self._settings, "loop_interval_sec", 0.0)))
        # Keep soft-stale long enough to cover one full loop tick, so slow capture path
        # does not block every cycle when LOOP_INTERVAL_SEC is above cache TTL.
        loop_floor = loop_interval + 2.0 if loop_interval > 0.0 else cache_ttl
        return max(cache_ttl, configured, loop_floor)

    async def get_market_data(self, symbol: str) -> MarketSnapshot:
        token = str(symbol).upper().replace(" ", "")
        quote_source = self._quote_source()
        if quote_source == "paper":
            return await self._quote.get_market_data(token)
        if quote_source == "dexter_reference":
            snap = await asyncio.to_thread(
                self._reference_quote_snapshot,
                token,
                reason="CTRADER_QUOTE_SOURCE=dexter_reference",
            )
            if snap is not None:
                return snap
            if not self._allow_paper_fallback():
                raise RuntimeError(f"Dexter-style reference quote failed for {token}")
            log.warning("Falling back to PaperBroker quotes for %s: reference quote unavailable", token)
            return await self._quote.get_market_data(token)

        cached = self._quote_cache.get(token)
        cache_ttl = float(self._settings.ctrader_quote_cache_ttl_sec)
        soft_stale_ttl = self._effective_soft_stale_ttl_sec(cache_ttl)
        refresh_enabled = bool(getattr(self._settings, "ctrader_quote_background_refresh_enabled", True))
        now = time.time()
        if cached:
            age = now - cached.ts_unix
            if age <= cache_ttl:
                if refresh_enabled and age >= max(0.5, cache_ttl * 0.6):
                    self._schedule_quote_refresh(token, reason=f"proactive_cache_age age={age:.1f}s")
                return cached
            inflight = self._quote_refresh_tasks.get(token)
            if (
                refresh_enabled
                and inflight is not None
                and not inflight.done()
                and age <= max(soft_stale_ttl * 3.0, cache_ttl * 6.0, 120.0)
            ):
                return MarketSnapshot(
                    symbol=cached.symbol,
                    bid=cached.bid,
                    ask=cached.ask,
                    mid=cached.mid,
                    spread=cached.spread,
                    ts_unix=cached.ts_unix,
                    extra={
                        **dict(cached.extra),
                        "soft_stale_cache": True,
                        "soft_stale_age_sec": round(age, 1),
                        "refresh_inflight": True,
                    },
                )
            if (
                age <= soft_stale_ttl
                and refresh_enabled
            ):
                self._schedule_quote_refresh(token, reason=f"soft_stale_cache age={age:.1f}s")
                return MarketSnapshot(
                    symbol=cached.symbol,
                    bid=cached.bid,
                    ask=cached.ask,
                    mid=cached.mid,
                    spread=cached.spread,
                    ts_unix=cached.ts_unix,
                    extra={
                        **dict(cached.extra),
                        "soft_stale_cache": True,
                        "soft_stale_age_sec": round(age, 1),
                    },
                )

        payload = self._capture_payload(token)
        data = await asyncio.to_thread(self._run_worker, "capture_market", payload)
        snap = self._extract_latest_snapshot(token, data)
        if snap is not None:
            return snap
        last_message = str(data.get("message") or data.get("status") or "capture_market failed")

        message = last_message
        snap = await asyncio.to_thread(self._reference_quote_snapshot, token, reason=message)
        if snap is not None:
            log.warning(
                "Using Dexter-style reference quote for %s after capture failure: %s",
                token,
                message[:220],
            )
            return snap

        if cached:
            age = now - cached.ts_unix
            stale_grace = max(cache_ttl * 30.0, 120.0)
            if age <= stale_grace:
                log.warning(
                    "Using stale cached quote for %s age=%.1fs after capture failure: %s",
                    token,
                    age,
                    last_message,
                )
                return MarketSnapshot(
                    symbol=cached.symbol,
                    bid=cached.bid,
                    ask=cached.ask,
                    mid=cached.mid,
                    spread=cached.spread,
                    ts_unix=cached.ts_unix,
                    extra={
                        **dict(cached.extra),
                        "stale_cache": True,
                        "stale_age_sec": round(age, 1),
                        "capture_error": last_message[:220],
                    },
                )

        if not self._allow_paper_fallback():
            raise RuntimeError(f"Live quote capture failed for {token}: {message}")
        log.warning("Falling back to PaperBroker quotes for %s: %s", token, message)
        return await self._quote.get_market_data(token)

    async def get_recent_closes(
        self,
        symbol: str,
        *,
        count: int,
        timeframe: str = "1m",
    ) -> list[float]:
        token = str(symbol).upper().replace(" ", "")
        bar_count = max(1, min(int(count), 5000))
        payload = {
            "account_id": int(self._account_id),
            "symbol": token,
            "timeframe": str(timeframe or "1m").strip().lower(),
            "count": bar_count,
        }
        data = await asyncio.to_thread(self._run_worker, "get_trendbars", payload)
        if not data.get("ok"):
            log.warning(
                "Trendbar seed failed for %s timeframe=%s status=%s message=%s",
                token,
                payload["timeframe"],
                data.get("status"),
                str(data.get("message") or "")[:220],
            )
            return []

        closes: list[float] = []
        for row in list(data.get("bars") or []):
            if not isinstance(row, dict):
                continue
            close = _to_float(row.get("close"))
            if close is not None:
                closes.append(float(close))
        if not closes:
            log.warning(
                "Trendbar seed returned no usable closes for %s timeframe=%s status=%s",
                token,
                payload["timeframe"],
                data.get("status"),
            )
            return []
        return closes[-bar_count:]

    def _extract_trade_entry_price(self, data: Dict[str, Any], side: Action) -> Optional[float]:
        raw_exec = dict((data.get("execution_meta") or {}).get("raw_execution") or {})
        return _extract_price(
            {"root": data, "raw": raw_exec},
            ("raw", "deal", "executionPrice"),
            ("raw", "deal", "price"),
            ("raw", "position", "price"),
            ("raw", "order", "limitPrice"),
            ("root", "entry_price"),
        )

    async def execute_trade(
        self,
        *,
        symbol: str,
        side: Action,
        volume: float,
        decision_reason: str,
        dry_run: bool,
    ) -> TradeResult:
        token = str(symbol).upper().replace(" ", "")
        if side == "HOLD":
            raise ValueError("HOLD should not reach broker")
        if dry_run:
            m = await self.get_market_data(token)
            entry = m.ask if side == "BUY" else m.bid
            return TradeResult(
                order_id=f"dry_{uuid.uuid4()}",
                symbol=token,
                side=side,
                volume=volume,
                entry_price=entry,
                executed=False,
                dry_run=True,
                message="dry_run",
                position_id=f"dry_{uuid.uuid4().hex[:12]}",
                raw_response={"reason": decision_reason},
            )

        scale = max(1, int(self._settings.ctrader_worker_volume_scale))
        fixed_vol = max(1, min(10_000_000, int(round(float(volume) * scale))))
        direction = "long" if side == "BUY" else "short"
        oid_tag = f"{self._settings.instance_name}_{uuid.uuid4().hex[:12]}"
        payload: Dict[str, Any] = {
            "account_id": int(self._account_id),
            "symbol": token,
            "direction": direction,
            "order_type": "market",
            "fixed_volume": fixed_vol,
            "label": oid_tag,
            "client_order_id": oid_tag,
            "comment": _compact_broker_comment(decision_reason),
        }

        data = await asyncio.to_thread(self._run_worker, "execute", payload)
        ok = bool(data.get("ok"))
        oid = data.get("order_id") or data.get("deal_id") or uuid.uuid4()
        position_id = data.get("position_id")
        entry = self._extract_trade_entry_price(data, side)
        if entry is None:
            m = await self.get_market_data(token)
            entry = m.ask if side == "BUY" else m.bid
        return TradeResult(
            order_id=str(oid),
            symbol=token,
            side=side,
            volume=volume,
            entry_price=float(entry),
            executed=ok,
            dry_run=False,
            message=str(data.get("message") or data.get("status") or "ctrader_worker")[:220],
            position_id=str(position_id) if position_id else None,
            raw_response={**data, "worker_payload": payload},
        )

    async def _resolve_live_position_snapshot(self, position: OpenPosition) -> Optional[Dict[str, Any]]:
        payload = {"account_id": int(self._account_id), "lookback_hours": 24, "max_rows": 50}
        data = await asyncio.to_thread(self._run_worker, "reconcile", payload)
        if not data.get("ok"):
            if position.position_id:
                return {
                    "position_id": str(position.position_id),
                    "volume": None,
                }
            return None
        direction = "long" if position.side == "BUY" else "short"
        matches = [
            row
            for row in list(data.get("positions") or [])
            if str(row.get("symbol") or "").upper() == position.symbol.upper()
            and str(row.get("direction") or "").lower() == direction
            and (
                not position.position_id
                or str(row.get("position_id") or "") == str(position.position_id)
            )
        ]
        if not matches and position.position_id:
            matches = [
                row
                for row in list(data.get("positions") or [])
                if str(row.get("position_id") or "") == str(position.position_id)
            ]
        if not matches:
            if position.position_id:
                return {
                    "position_id": str(position.position_id),
                    "volume": None,
                }
            return None
        latest = max(matches, key=lambda row: float(row.get("updated_timestamp_ms") or row.get("open_timestamp_ms") or 0))
        return dict(latest)

    async def close_position(
        self,
        *,
        symbol: str,
        position: OpenPosition,
        reason: str,
        dry_run: bool,
    ) -> CloseResult:
        token = str(symbol).upper().replace(" ", "")
        if dry_run:
            m = await self.get_market_data(token)
            exit_price = m.bid if position.side == "BUY" else m.ask
            return CloseResult(
                symbol=token,
                side=position.side,
                volume=position.volume,
                exit_price=exit_price,
                closed=True,
                dry_run=True,
                message="dry_run_close",
                position_id=position.position_id,
                raw_response={"reason": reason},
            )

        snapshot = await self._resolve_live_position_snapshot(position)
        live_position_id = str(snapshot.get("position_id") or "") if snapshot else ""
        if not live_position_id:
            return CloseResult(
                symbol=token,
                side=position.side,
                volume=position.volume,
                exit_price=position.entry_price,
                closed=False,
                dry_run=False,
                message="missing_position_id_for_close",
                position_id=position.position_id,
            )

        scale = max(1, int(self._settings.ctrader_worker_volume_scale))
        live_volume = None
        if snapshot:
            for key in ("volume", "close_volume", "filled_volume", "fixed_volume", "quantity"):
                live_volume = _to_float(snapshot.get(key))
                if live_volume is not None:
                    break
        payload = {
            "account_id": int(self._account_id),
            "position_id": int(live_position_id),
            "volume": max(
                1,
                min(
                    10_000_000,
                    int(round(live_volume if live_volume is not None else float(position.volume) * scale)),
                ),
            ),
        }
        data = await asyncio.to_thread(self._run_worker, "close", payload)
        exit_price = _extract_price(
            {"root": data, "raw": dict((data.get("execution_meta") or {}).get("raw_execution") or {})},
            ("raw", "deal", "executionPrice"),
            ("raw", "deal", "price"),
            ("raw", "position", "price"),
        )
        if exit_price is None:
            m = await self.get_market_data(token)
            exit_price = m.bid if position.side == "BUY" else m.ask
        return CloseResult(
            symbol=token,
            side=position.side,
            volume=position.volume,
            exit_price=float(exit_price),
            closed=bool(data.get("ok")),
            dry_run=False,
            message=str(data.get("message") or data.get("status") or "close")[:220],
            position_id=live_position_id,
            raw_response={**data, "worker_payload": payload, "reason": reason},
        )
