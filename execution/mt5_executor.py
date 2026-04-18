"""
execution/mt5_executor.py
MT5 execution bridge for Dexter Pro via RPyC server on Windows host.

Design goals:
- Safe by default: disabled unless MT5_ENABLED=1, dry-run by default.
- Broker-aware symbol resolution: only executes symbols tradable at the connected MT5 broker.
- Strict position limits and duplicate-direction prevention.
"""
import logging
import json
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from config import config
from learning.mt5_adaptive_trade_planner import mt5_adaptive_trade_planner

try:
    from execution.tiger_risk_governor import tiger_risk_governor
except Exception:
    tiger_risk_governor = None

try:
    from api.signal_store import signal_store
except Exception:
    signal_store = None

try:
    import rpyc
except Exception:  # pragma: no cover - handled in runtime checks
    rpyc = None

logger = logging.getLogger(__name__)

MT5_RETCODE_HINTS = {
    10014: "invalid volume for this symbol/account",
    10016: "invalid stops (SL/TP too close or wrong side)",
    10018: "market closed",
    10019: "not enough money",
    10021: "price changed/off quotes",
    10027: "autotrading blocked by terminal settings",
    10030: "invalid filling mode for broker symbol",
}


def _retcode_detail(retcode: int) -> str:
    hint = MT5_RETCODE_HINTS.get(int(retcode))
    if hint:
        return f"order rejected retcode={retcode} ({hint})"
    return f"order rejected retcode={retcode}"


def _fmt_mt5_last_error(err) -> str:
    try:
        if isinstance(err, (list, tuple)):
            parts = [str(x).strip() for x in list(err) if str(x).strip()]
            if parts:
                return " | ".join(parts)
        text = str(err or "").strip()
        if text:
            return text
    except Exception:
        pass
    return "unknown"


def _normalize_symbol_key(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (symbol or "").upper())


def _mt5_attr(obj, name: str, default=None):
    """Read attribute from MT5 namedtuple/netref or dict-like fallback."""
    try:
        if isinstance(obj, dict):
            return obj.get(name, default)
    except Exception:
        pass
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


@dataclass
class MT5ExecutionResult:
    ok: bool
    status: str
    message: str
    signal_symbol: str = ""
    broker_symbol: str = ""
    dry_run: bool = False
    retcode: Optional[int] = None
    ticket: Optional[int] = None
    position_id: Optional[int] = None
    volume: Optional[float] = None
    execution_meta: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "status": self.status,
            "message": self.message,
            "signal_symbol": self.signal_symbol,
            "broker_symbol": self.broker_symbol,
            "dry_run": self.dry_run,
            "retcode": self.retcode,
            "ticket": self.ticket,
            "position_id": self.position_id,
            "volume": self.volume,
            "execution_meta": dict(self.execution_meta or {}),
        }


class MT5Executor:
    def __init__(self):
        self._conn = None
        self._mt5 = None
        self._lock = threading.Lock()
        self._symbols_cache: list[str] = []
        self._symbols_cache_ts = 0.0
        self._symbols_ttl_sec = 300.0
        self._market_closed_cache: dict[str, float] = {}
        self._symbol_map = config.get_mt5_symbol_map()
        self._allow_symbols = config.get_mt5_allow_symbols()
        self._block_symbols = config.get_mt5_block_symbols()
        self._micro_lock = threading.Lock()
        default_micro_path = Path(__file__).resolve().parent.parent / "data" / "mt5_micro_whitelist.json"
        self._micro_whitelist_path = Path(getattr(config, "MT5_MICRO_WHITELIST_PATH", "") or default_micro_path)
        self._micro_whitelist_path.parent.mkdir(parents=True, exist_ok=True)
        self._micro_store = {"version": 1, "accounts": {}}
        self._load_micro_store()

    @property
    def enabled(self) -> bool:
        return bool(config.MT5_ENABLED)

    @property
    def dry_run(self) -> bool:
        return bool(config.MT5_DRY_RUN)

    @property
    def available(self) -> bool:
        return rpyc is not None

    def set_runtime_allow_symbols(self, symbols) -> dict:
        """
        Replace runtime allowlist without requiring process restart.
        Symbols are normalized to uppercase and de-duplicated.
        """
        vals = set()
        try:
            for raw in (list(symbols or []) if symbols is not None else []):
                s = str(raw or "").strip().upper()
                if s:
                    vals.add(s)
        except Exception:
            vals = set()
        self._allow_symbols = vals
        return {
            "ok": True,
            "allow_count": len(self._allow_symbols),
            "allow_symbols": sorted(self._allow_symbols),
        }

    def _disconnect(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = None
        self._mt5 = None

    def _micro_learner_enabled(self) -> bool:
        return bool(getattr(config, "MT5_MICRO_MODE_ENABLED", False)) and bool(
            getattr(config, "MT5_MICRO_WHITELIST_LEARNER_ENABLED", True)
        )

    def _load_micro_store(self) -> None:
        path = self._micro_whitelist_path
        if not path.exists():
            return
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("accounts"), dict):
                self._micro_store = {"version": int(raw.get("version", 1) or 1), "accounts": dict(raw.get("accounts") or {})}
        except Exception as e:
            logger.warning("[MT5] micro whitelist load failed: %s", e)

    def _save_micro_store(self) -> None:
        try:
            tmp = self._micro_whitelist_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._micro_store, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")
            tmp.replace(self._micro_whitelist_path)
        except Exception as e:
            logger.warning("[MT5] micro whitelist save failed: %s", e)

    @staticmethod
    def _micro_status_ttl_sec(status: str) -> int:
        s = str(status or "").lower()
        if s == "allow":
            return 6 * 3600
        if s in {"deny_margin", "micro_margin"}:
            return 2 * 3600
        if s in {"deny_spread", "micro_spread"}:
            return 5 * 60
        if s in {"deny_contract", "unmapped"}:
            return 24 * 3600
        return 3600

    @staticmethod
    def _safe_float(v, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return float(default)

    def _micro_balance_bucket(self, balance_like: float) -> float:
        bucket = max(0.25, self._safe_float(getattr(config, "MT5_MICRO_BALANCE_BUCKET_USD", 2.0), 2.0))
        x = max(0.0, self._safe_float(balance_like, 0.0))
        steps = int(x // bucket)
        return round(steps * bucket, 2)

    def _micro_account_bucket_ctx(self, account_info) -> dict:
        login = int(_mt5_attr(account_info, "login", 0) or 0)
        server = str(_mt5_attr(account_info, "server", "") or "")
        balance = self._safe_float(_mt5_attr(account_info, "balance", 0.0), 0.0)
        equity = self._safe_float(_mt5_attr(account_info, "equity", balance), balance)
        free_margin = self._safe_float(_mt5_attr(account_info, "margin_free", 0.0), 0.0)
        basis = max(balance, equity, free_margin)
        bucket = self._micro_balance_bucket(basis)
        key = f"{server}|{login}|bal{bucket:.2f}"
        return {
            "key": key,
            "login": login,
            "server": server,
            "balance": round(balance, 4),
            "equity": round(equity, 4),
            "free_margin": round(free_margin, 4),
            "balance_bucket": bucket,
            "ts": time.time(),
        }

    def _micro_store_get_symbol_row(self, account_key: str, broker_symbol: str) -> Optional[dict]:
        try:
            return (
                self._micro_store.get("accounts", {})
                .get(str(account_key), {})
                .get("symbols", {})
                .get(str(broker_symbol).upper())
            )
        except Exception:
            return None

    def _micro_cached_deny(self, account_ctx: dict, broker_symbol: str, *, source: str = "", signal_symbol: str = "") -> Optional[dict]:
        if not self._micro_learner_enabled():
            return None
        key = str((account_ctx or {}).get("key") or "")
        if not key:
            return None
        now = time.time()
        hard_cap = max(300, int(getattr(config, "MT5_MICRO_WHITELIST_TTL_HOURS", 24)) * 3600)
        with self._micro_lock:
            row = dict(self._micro_store_get_symbol_row(key, broker_symbol) or {})
        if not row:
            return None
        status = str(row.get("status") or "").lower()
        if status not in {"deny_margin", "deny_spread", "deny_contract", "unmapped"}:
            return None
        if status == "deny_margin":
            try:
                cached_pct = float(row.get("margin_budget_pct", 0.0) or 0.0)
            except Exception:
                cached_pct = 0.0
            # Backward-compatible cache rows may not include margin_budget_pct yet.
            # Only invalidate deny_margin when we have a concrete cached budget.
            if cached_pct > 0.0:
                try:
                    current_pct, _ = self._mt5_margin_budget_pct_for_signal(
                        None,
                        source,
                        broker_symbol=str(broker_symbol or ""),
                        signal_symbol=str(signal_symbol or ""),
                    )
                except Exception:
                    current_pct = 0.0
                if current_pct > max(0.0, cached_pct) + 1e-9:
                    return None
        updated = float(row.get("updated_ts", 0.0) or 0.0)
        age = max(0.0, now - updated) if updated > 0 else 9e9
        ttl = min(hard_cap, self._micro_status_ttl_sec(status))
        if age > ttl:
            return None
        row["age_sec"] = age
        return row

    def _micro_record_symbol_observation(
        self,
        account_ctx: Optional[dict],
        broker_symbol: str,
        *,
        signal_symbol: str = "",
        status: str,
        reason: str = "",
        spread_pct: Optional[float] = None,
        margin_required: Optional[float] = None,
        min_lot_margin: Optional[float] = None,
        margin_budget_pct: Optional[float] = None,
    ) -> None:
        if not self._micro_learner_enabled():
            return
        key = str((account_ctx or {}).get("key") or "")
        if not key:
            return
        sym = str(broker_symbol or "").upper()
        if not sym:
            return
        now = time.time()
        with self._micro_lock:
            accounts = self._micro_store.setdefault("accounts", {})
            acct = accounts.setdefault(
                key,
                {
                    "meta": {},
                    "symbols": {},
                },
            )
            meta = acct.setdefault("meta", {})
            meta.update(
                {
                    "login": int((account_ctx or {}).get("login", 0) or 0),
                    "server": str((account_ctx or {}).get("server", "") or ""),
                    "balance": self._safe_float((account_ctx or {}).get("balance", 0.0), 0.0),
                    "equity": self._safe_float((account_ctx or {}).get("equity", 0.0), 0.0),
                    "free_margin": self._safe_float((account_ctx or {}).get("free_margin", 0.0), 0.0),
                    "balance_bucket": self._safe_float((account_ctx or {}).get("balance_bucket", 0.0), 0.0),
                    "updated_ts": now,
                }
            )
            rows = acct.setdefault("symbols", {})
            row = dict(rows.get(sym) or {})
            row["broker_symbol"] = sym
            if signal_symbol:
                row["signal_symbol"] = str(signal_symbol).upper()
            row["status"] = str(status or "").lower()
            row["last_reason"] = str(reason or "")[:220]
            row["updated_ts"] = now
            row["ttl_sec"] = self._micro_status_ttl_sec(row["status"])
            row["hits"] = int(row.get("hits", 0) or 0) + 1
            if row["status"] == "allow":
                row["allow_count"] = int(row.get("allow_count", 0) or 0) + 1
            elif row["status"] == "deny_margin":
                row["deny_margin_count"] = int(row.get("deny_margin_count", 0) or 0) + 1
            elif row["status"] == "deny_spread":
                row["deny_spread_count"] = int(row.get("deny_spread_count", 0) or 0) + 1
            elif row["status"] == "deny_contract":
                row["deny_contract_count"] = int(row.get("deny_contract_count", 0) or 0) + 1
            elif row["status"] == "unmapped":
                row["unmapped_count"] = int(row.get("unmapped_count", 0) or 0) + 1
            if spread_pct is not None:
                row["last_spread_pct"] = round(self._safe_float(spread_pct, 0.0), 6)
            if margin_required is not None:
                row["last_margin_required"] = round(self._safe_float(margin_required, 0.0), 4)
            if min_lot_margin is not None:
                row["last_min_lot_margin"] = round(self._safe_float(min_lot_margin, 0.0), 4)
            if margin_budget_pct is not None:
                row["margin_budget_pct"] = round(self._safe_float(margin_budget_pct, 0.0), 4)
            rows[sym] = row
            self._save_micro_store()

    def micro_whitelist_status(self, account_info=None) -> dict:
        out = {
            "enabled": self._micro_learner_enabled(),
            "path": str(self._micro_whitelist_path),
            "account_key": "",
            "balance_bucket": None,
            "total_symbols": 0,
            "allowed": 0,
            "denied": 0,
            "top_allowed": [],
            "top_denied": [],
        }
        if not out["enabled"]:
            return out
        acct = account_info
        if acct is None:
            try:
                acct = self._mt5.account_info() if self._mt5 is not None else None
            except Exception:
                acct = None
        if acct is None:
            return out
        ctx = self._micro_account_bucket_ctx(acct)
        out["account_key"] = str(ctx.get("key") or "")
        out["balance_bucket"] = ctx.get("balance_bucket")
        with self._micro_lock:
            rows = dict(
                (
                    self._micro_store.get("accounts", {})
                    .get(str(out["account_key"]), {})
                    .get("symbols", {})
                )
                or {}
            )
        out["total_symbols"] = len(rows)
        allowed = []
        denied = []
        for sym, rec in rows.items():
            r = dict(rec or {})
            item = {"symbol": sym, "status": str(r.get("status") or ""), "hits": int(r.get("hits", 0) or 0)}
            if item["status"] == "allow":
                allowed.append(item)
            elif item["status"].startswith("deny") or item["status"] == "unmapped":
                denied.append(item)
        allowed.sort(key=lambda x: (x["hits"], x["symbol"]), reverse=True)
        denied.sort(key=lambda x: (x["hits"], x["symbol"]), reverse=True)
        out["allowed"] = len(allowed)
        out["denied"] = len(denied)
        out["top_allowed"] = allowed[:5]
        out["top_denied"] = denied[:5]
        return out

    def _ensure_connection(self) -> tuple[bool, str]:
        if not self.enabled:
            return False, "MT5 disabled"
        if not self.available:
            return False, "rpyc not installed"

        with self._lock:
            if self._conn is not None and self._mt5 is not None:
                try:
                    _ = self._conn.root
                    return True, "connected"
                except Exception:
                    self._disconnect()

            try:
                self._conn = rpyc.connect(
                    config.MT5_HOST,
                    int(config.MT5_PORT),
                    config={"allow_pickle": True},
                )
                self._mt5 = self._conn.root.get_mt5()
                if not self._mt5.initialize():
                    err = self._mt5.last_error()
                    self._disconnect()
                    return False, f"initialize failed: {err}"
                return True, "connected"
            except Exception as e:
                self._disconnect()
                return False, f"connect failed: {e}"

    def _get_symbols(self, force_refresh: bool = False) -> list[str]:
        ok, _ = self._ensure_connection()
        if not ok:
            return []
        now = time.time()
        if (
            (not force_refresh)
            and self._symbols_cache
            and (now - self._symbols_cache_ts) < self._symbols_ttl_sec
        ):
            return self._symbols_cache
        try:
            symbols = self._mt5.symbols_get()
            names = [str(s.name) for s in (symbols or []) if getattr(s, "name", None)]
            self._symbols_cache = names
            self._symbols_cache_ts = now
            return names
        except Exception:
            return self._symbols_cache

    def _candidate_symbols(self, signal_symbol: str) -> list[str]:
        src = (signal_symbol or "").strip().upper()
        if not src:
            return []

        candidates = [src]
        if src == "XAUUSD":
            candidates.extend(["GOLD", "XAUUSDM", "XAUUSD.M"])

        # Stock/CFD venue suffix hints (helps map scanner tickers like AAPL -> AAPL.NAS).
        stock_venue_suffixes = [".NAS", ".NYSE", ".AMEX"]
        yahoo_suffix_map = {
            "L": [".LSE", ".LON", ".UK"],
            "DE": [".XETRA", ".DE"],
            "PA": [".EPA", ".PAR", ".FR"],
            "HK": [".HKEX", ".HK"],
            "T": [".TYO", ".JP", ".T"],
            "BK": [".SET", ".BK"],
            "SI": [".SGX", ".SI"],
            "NS": [".NSE", ".NS"],
            "BO": [".BSE", ".BO"],
            "AX": [".ASX", ".AX"],
        }

        if "/" in src:
            base, quote = src.split("/", 1)
            candidates.extend([f"{base}{quote}", base])
            if quote == "USDT":
                candidates.extend([f"{base}USD", f"{base}USDT"])

        if "." in src:
            base, suffix = src.split(".", 1)
            candidates.append(base)
            for venue_sfx in yahoo_suffix_map.get(suffix, []):
                candidates.append(f"{base}{venue_sfx}")
        if "-" in src:
            candidates.append(src.replace("-", ""))

        # Plain ticker fallback: try common US CFD suffixes.
        if "/" not in src and "." not in src and len(src) >= 1:
            for venue_sfx in stock_venue_suffixes:
                candidates.append(f"{src}{venue_sfx}")

        expanded = []
        for c in candidates:
            expanded.append(c)
            for suffix in ("m", ".m", "_m", "i", ".i", "-pro", ".pro"):
                expanded.append(f"{c}{suffix}".upper())

        deduped = []
        seen = set()
        for c in expanded:
            u = c.upper()
            if u not in seen:
                seen.add(u)
                deduped.append(u)
        return deduped

    def resolve_symbol(self, signal_symbol: str) -> Optional[str]:
        symbols = self._get_symbols()
        if not symbols:
            return None

        symbols_upper = {s.upper(): s for s in symbols}
        signal_key = (signal_symbol or "").strip().upper()

        mapped = self._symbol_map.get(signal_key)
        if mapped and mapped in symbols_upper:
            return symbols_upper[mapped]

        for c in self._candidate_symbols(signal_key):
            if c in symbols_upper:
                return symbols_upper[c]

        # Fallback normalization matching (handles broker suffixes/prefixes).
        norm_index: dict[str, list[str]] = {}
        for s in symbols:
            norm_index.setdefault(_normalize_symbol_key(s), []).append(s)
        for c in self._candidate_symbols(signal_key):
            options = norm_index.get(_normalize_symbol_key(c), [])
            if options:
                # Prefer the shortest symbol for deterministic matching.
                return sorted(options, key=len)[0]

        return None

    def filter_tradable_signal_symbols(self, signal_symbols: list[str]) -> dict:
        """
        Bulk-resolve signal symbols against broker symbols efficiently.
        Returns:
          {
            "ok": bool,
            "connected": bool,
            "total": int,
            "tradable": [signal_symbol...],
            "unmapped": [signal_symbol...],
            "resolved_map": {signal_symbol: broker_symbol}
          }
        """
        out = {
            "ok": False,
            "connected": False,
            "total": 0,
            "tradable": [],
            "unmapped": [],
            "resolved_map": {},
            "error": "",
        }
        seen: set[str] = set()
        symbols_in: list[str] = []
        for raw in list(signal_symbols or []):
            s = str(raw or "").strip().upper()
            if not s or s in seen:
                continue
            seen.add(s)
            symbols_in.append(s)
        out["total"] = len(symbols_in)
        if not symbols_in:
            out["ok"] = True
            return out
        if not self.enabled:
            out["error"] = "MT5 disabled"
            return out
        ok, msg = self._ensure_connection()
        if not ok:
            out["error"] = str(msg or "mt5 not connected")
            return out
        out["connected"] = True

        broker_symbols = self._get_symbols()
        if not broker_symbols:
            out["error"] = "no broker symbols loaded"
            return out

        symbols_upper = {s.upper(): s for s in broker_symbols}
        norm_index: dict[str, list[str]] = {}
        for s in broker_symbols:
            norm_index.setdefault(_normalize_symbol_key(s), []).append(s)

        def _resolve_fast(signal_key: str) -> Optional[str]:
            mapped = self._symbol_map.get(signal_key)
            if mapped and mapped in symbols_upper:
                return symbols_upper[mapped]
            for c in self._candidate_symbols(signal_key):
                if c in symbols_upper:
                    return symbols_upper[c]
            for c in self._candidate_symbols(signal_key):
                opts = norm_index.get(_normalize_symbol_key(c), [])
                if opts:
                    return sorted(opts, key=len)[0]
            return None

        for sig in symbols_in:
            brk = _resolve_fast(sig)
            if brk:
                out["tradable"].append(sig)
                out["resolved_map"][sig] = brk
            else:
                out["unmapped"].append(sig)

        out["ok"] = True
        return out

    def _normalize_volume(self, raw_volume: float, symbol_info) -> float:
        vol = float(raw_volume)
        min_v = float(getattr(symbol_info, "volume_min", vol) or vol)
        max_v = float(getattr(symbol_info, "volume_max", vol) or vol)
        step = float(getattr(symbol_info, "volume_step", 0.01) or 0.01)
        vol = min(max(vol, min_v), max_v)
        steps = max(0, round((vol - min_v) / step))
        vol = min_v + (steps * step)
        return round(vol, 8)

    @staticmethod
    def _volume_grid(symbol_info) -> tuple[float, float, float]:
        min_v = float(getattr(symbol_info, "volume_min", 0.01) or 0.01)
        max_v = float(getattr(symbol_info, "volume_max", min_v) or min_v)
        step = float(getattr(symbol_info, "volume_step", 0.01) or 0.01)
        if step <= 0:
            step = 0.01
        return min_v, max_v, step

    def _fit_volume_by_margin_budget(
        self,
        *,
        order_type,
        broker_symbol: str,
        symbol_info,
        price: float,
        desired_volume: float,
        free_margin: float,
        max_usage_pct: float,
        min_left: float,
    ) -> tuple[Optional[float], Optional[float], str]:
        """
        Fit volume to margin budget for tiny accounts.
        Returns (volume, margin_required, reason). volume=None means skip.
        """
        desired = self._normalize_volume(float(desired_volume), symbol_info)
        min_v, _max_v, step = self._volume_grid(symbol_info)
        min_v = self._normalize_volume(min_v, symbol_info)
        max_allowed_margin = free_margin * max(0.0, min(1.0, max_usage_pct / 100.0))
        budget = min(max_allowed_margin, max(0.0, free_margin - float(min_left)))
        if budget <= 0:
            return None, None, (
                f"margin guard: no usable margin budget (free={free_margin:.2f}, "
                f"max_usage={max_usage_pct:.0f}%, min_free_left={min_left:.2f})"
            )

        def _calc(vol: float) -> Optional[float]:
            try:
                m = self._mt5.order_calc_margin(order_type, broker_symbol, float(vol), float(price))
                if m is None:
                    return None
                return float(m)
            except Exception:
                return None

        m_desired = _calc(desired)
        if m_desired is None:
            return None, None, "order_calc_margin unavailable"
        if (m_desired <= budget) and ((free_margin - m_desired) >= float(min_left)):
            return desired, m_desired, "ok"

        # Broker min lot itself may already be too expensive for the account.
        m_min = _calc(min_v)
        if m_min is None:
            return None, None, "order_calc_margin unavailable"
        if (m_min > budget) or ((free_margin - m_min) < float(min_left)):
            return None, m_min, (
                f"margin guard: broker min lot {min_v:.4f} requires {m_min:.2f} > budget {budget:.2f} "
                f"(free margin {free_margin:.2f}, keep >= {min_left:.2f})"
            )

        # Auto-downsize to highest affordable volume on the broker's volume grid.
        approx = desired
        if m_desired > 0:
            approx = desired * (budget / m_desired)
        vol = self._normalize_volume(max(min_v, min(desired, approx)), symbol_info)
        # Walk down grid if still above budget (or rounding pushed it up).
        loops = 0
        m_now = _calc(vol)
        while (
            (m_now is None or m_now > budget or (free_margin - m_now) < float(min_left))
            and vol > min_v
            and loops < 1000
        ):
            vol = self._normalize_volume(max(min_v, vol - step), symbol_info)
            m_now = _calc(vol)
            loops += 1

        if m_now is None:
            return None, None, "order_calc_margin unavailable"
        if (m_now > budget) or ((free_margin - m_now) < float(min_left)):
            return None, m_now, (
                f"margin guard: no affordable volume on grid (min {min_v:.4f}, desired {desired:.4f}, "
                f"budget {budget:.2f}, free {free_margin:.2f})"
            )

        note = "ok"
        if abs(vol - desired) > 1e-12:
            note = (
                f"auto-downsized volume {desired:.4f}->{vol:.4f} "
                f"to fit margin budget {budget:.2f} (required {m_now:.2f})"
            )
        return vol, m_now, note

    def _history_deals_get_robust(self, start_utc: datetime, end_utc: datetime) -> tuple[list, str]:
        """
        Bridge-compatible history deals fetch.
        Some RPyC bridges only support unix timestamps (int) and return None for datetime args.
        """
        attempts = [
            ("aware_dt", start_utc, end_utc),
            ("naive_dt", start_utc.replace(tzinfo=None), end_utc.replace(tzinfo=None)),
            ("ts_int", int(start_utc.timestamp()), int(end_utc.timestamp())),
            ("ts_float", float(start_utc.timestamp()), float(end_utc.timestamp())),
        ]
        last_err = ""
        for mode, a, b in attempts:
            try:
                res = self._mt5.history_deals_get(a, b)
            except Exception as e:
                last_err = f"{mode}:{e}"
                continue
            if res is None:
                last_err = f"{mode}:None"
                continue
            try:
                return list(res or []), mode
            except Exception:
                try:
                    return list(tuple(res) or []), mode
                except Exception as e:
                    last_err = f"{mode}:iter:{e}"
                    continue
        raise RuntimeError(f"history_deals_get unavailable ({last_err or 'no supported signature'})")

    def _price_round(self, value: float, symbol_info) -> float:
        digits = int(getattr(symbol_info, "digits", 5) or 5)
        return round(float(value), digits)

    def _pick_filling_mode(self, symbol_info):
        # Bitmask hints: 1=FOK, 2=IOC, else RETURN.
        fm = int(getattr(symbol_info, "filling_mode", 0) or 0)
        if fm & 2:
            return self._mt5.ORDER_FILLING_IOC
        if fm & 1:
            return self._mt5.ORDER_FILLING_FOK
        return self._mt5.ORDER_FILLING_RETURN

    @staticmethod
    def _comment_token(value: str, fallback: str, limit: int) -> str:
        tok = re.sub(r"[^A-Za-z0-9]+", "", str(value or "").strip()).upper()
        if not tok:
            tok = str(fallback or "").strip().upper()
        return tok[: max(1, int(limit))]

    def _build_order_comment(self, signal, source: str, signal_symbol: str) -> str:
        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
        run_no = 0
        try:
            run_no = int(raw_scores.get("signal_run_no", 0) or 0)
        except Exception:
            run_no = 0
        run_id = str(raw_scores.get("signal_run_id", "") or "").strip()

        src_text = str(source or "")
        if ":bypass" in src_text.lower():
            src_text = "bypass"
        elif ":canary" in src_text.lower():
            src_text = "canary"
        trace_text = f"R{run_no:06d}" if run_no > 0 else str(run_id or "R0")

        prefix_tok = self._comment_token(getattr(config, "MT5_COMMENT_PREFIX", "DEX"), "DEX", 12)
        trace_tok = self._comment_token(trace_text, "R0", 7)
        if trace_tok and not trace_tok.startswith("R"):
            trace_tok = f"R{trace_tok}"[:7]
        if str(src_text).strip().lower() == "bypass":
            source_tok = "BYPASS"
        else:
            source_budget = max(3, 31 - (len(prefix_tok) + 6 + len(trace_tok) + 3))
            source_tok = self._comment_token(src_text, "SRC", min(9, source_budget))
        symbol_budget = max(3, 31 - (len(prefix_tok) + len(source_tok) + len(trace_tok) + 3))
        symbol_tok = self._comment_token(signal_symbol, "SYM", symbol_budget)
        # Some MT5 Python bridges/brokers reject comments with punctuation even inside 31 chars.
        return f"{prefix_tok}{source_tok}{symbol_tok}{trace_tok}"[:31]

    def _is_bot_position_like(self, pos_obj) -> bool:
        try:
            pmagic = int(getattr(pos_obj, "magic", 0) or 0)
            if pmagic == int(config.MT5_MAGIC):
                return True
        except Exception:
            pass
        try:
            c = str(getattr(pos_obj, "comment", "") or "")
            pref = str(getattr(config, "MT5_COMMENT_PREFIX", "DEX") or "").strip()
            if pref and c.startswith(pref + ":"):
                return True
            if pref and ":" in c:
                head = str(c.split(":", 1)[0] or "").strip().upper()
                if head and str(pref).upper().startswith(head):
                    return True
        except Exception:
            pass
        return False

    def _resolve_filled_position_id(self, broker_symbol: str, is_long: bool, comment: str, magic_override: Optional[int] = None) -> Optional[int]:
        """
        Resolve live position ticket after a successful fill.
        This improves post-trade outcome syncing against MT5 history.
        """
        try:
            positions = self._mt5.positions_get(symbol=broker_symbol) or []
            side_type = int(self._mt5.ORDER_TYPE_BUY if is_long else self._mt5.ORDER_TYPE_SELL)
            magic = int(magic_override if magic_override is not None else config.MT5_MAGIC)
            candidates = []
            for p in positions:
                try:
                    ptype = int(getattr(p, "type", -1))
                    if ptype != side_type:
                        continue
                    pmagic = int(getattr(p, "magic", 0) or 0)
                    if pmagic != magic:
                        continue
                    pcomment = str(getattr(p, "comment", "") or "")
                    if comment and pcomment and (pcomment != comment):
                        continue
                    ticket = int(getattr(p, "ticket", 0) or 0)
                    ts = int(getattr(p, "time_msc", 0) or getattr(p, "time", 0) or 0)
                    if ticket > 0:
                        candidates.append((ts, ticket))
                except Exception:
                    continue
            if not candidates:
                return None
            candidates.sort(reverse=True)
            return int(candidates[0][1])
        except Exception:
            return None

    def _position_limits_ok(self, broker_symbol: str, direction: str, *, ignore_open_positions: bool = False) -> tuple[bool, str]:
        if bool(ignore_open_positions):
            return True, "bypass_ignore_open_positions"
        try:
            all_positions_raw = self._mt5.positions_get() or []
            sym_positions_raw = self._mt5.positions_get(symbol=broker_symbol) or []
            bot_only_limits = bool(getattr(config, "MT5_POSITION_LIMITS_BOT_ONLY", False))

            all_positions = list(all_positions_raw)
            sym_positions = list(sym_positions_raw)
            if bot_only_limits:
                all_positions = [p for p in all_positions_raw if self._is_bot_position_like(p)]
                sym_positions = [p for p in sym_positions_raw if self._is_bot_position_like(p)]

            max_open_positions = int(config.MT5_MAX_OPEN_POSITIONS)
            max_per_symbol = int(config.MT5_MAX_POSITIONS_PER_SYMBOL)
            if bool(getattr(config, "MT5_MICRO_MODE_ENABLED", False)) and bool(getattr(config, "MT5_MICRO_SINGLE_POSITION_ONLY", True)):
                max_open_positions = min(max_open_positions, 1)
                max_per_symbol = min(max_per_symbol, 1)
                # Micro account guard: enforce single live position globally,
                # independent from bot-only position filtering.
                if len(all_positions_raw) >= 1:
                    return False, "micro mode: single open position only"

            if len(all_positions) >= max_open_positions:
                if bot_only_limits:
                    return False, f"max bot open positions reached ({len(all_positions)}/{max_open_positions})"
                return False, f"max open positions reached ({len(all_positions)}/{max_open_positions})"

            if len(sym_positions) >= max_per_symbol:
                if bot_only_limits:
                    return False, f"max bot positions for {broker_symbol} reached ({len(sym_positions)}/{max_per_symbol})"
                return False, f"max positions for {broker_symbol} reached ({len(sym_positions)}/{max_per_symbol})"

            if config.MT5_AVOID_DUPLICATE_DIRECTION:
                want_buy = direction == "long"
                buy_type = int(self._mt5.ORDER_TYPE_BUY)
                sell_type = int(self._mt5.ORDER_TYPE_SELL)
                for p in sym_positions:
                    ptype = int(getattr(p, "type", -1))
                    if want_buy and ptype == buy_type:
                        return False, "duplicate long position exists"
                    if (not want_buy) and ptype == sell_type:
                        return False, "duplicate short position exists"
        except Exception as e:
            return False, f"position check failed: {e}"
        return True, "ok"

    def _mt5_min_conf_for_signal(self, signal, source: str = "") -> tuple[float, str]:
        base = float(getattr(config, "MT5_MIN_SIGNAL_CONFIDENCE", 75) or 75)
        reason = "global"
        src = str(source or "").strip().lower()
        if src == "fx":
            fx_min = float(getattr(config, "MT5_MIN_SIGNAL_CONFIDENCE_FX", base) or base)
            base = fx_min
            reason = "fx_default"
        try:
            sym = str(getattr(signal, "symbol", "") or "").strip().upper()
            overrides = config.get_mt5_min_conf_symbol_overrides()
            if sym and sym in overrides:
                base = float(overrides[sym])
                reason = f"symbol_override:{sym}"
                return float(base), reason
            # Also honor overrides keyed by MT5 broker symbol (e.g. ETHUSD for signal ETH/USDT).
            if sym:
                mapped = self.resolve_symbol(sym)
                mapped_u = str(mapped or "").strip().upper()
                if mapped_u and mapped_u in overrides:
                    base = float(overrides[mapped_u])
                    reason = f"symbol_override_mapped:{mapped_u}<-{sym}"
        except Exception:
            pass
        return float(base), reason

    def _symbol_override_candidates(self, signal=None, *, signal_symbol: str = "", broker_symbol: str = "") -> list[str]:
        candidates: list[str] = []

        def _add(v: str):
            s = str(v or "").strip().upper()
            if s and s not in candidates:
                candidates.append(s)

        _add(broker_symbol)
        _add(signal_symbol)
        try:
            _add(str(getattr(signal, "symbol", "") or ""))
        except Exception:
            pass

        if not broker_symbol:
            try:
                base_sig = next((c for c in candidates if c), "")
                mapped = self.resolve_symbol(base_sig) if base_sig else ""
                _add(str(mapped or ""))
            except Exception:
                pass
        return candidates

    @staticmethod
    def _lookup_symbol_float_override(candidates: list[str], overrides: dict[str, float]) -> tuple[Optional[float], str]:
        for idx, c in enumerate(candidates):
            if not c or c not in overrides:
                continue
            try:
                val = float(overrides[c])
            except Exception:
                continue
            if idx == 0:
                return val, f"symbol_override:{c}"
            base_sym = candidates[0] if candidates else ""
            return val, f"symbol_override_mapped:{c}<-{base_sym}"
        return None, ""

    def _get_neural_prob(self, signal, source: str) -> "Optional[float]":
        """
        Retrieve neural win probability using full fallback chain:
          1. Per-symbol model (e.g. XAUUSD, AAPL.NAS) via SymbolNeuralBrain
          2. Per-family model (e.g. _family_gold, _family_stock) via SymbolNeuralBrain
          3. Global model via NeuralBrain (original shared model)
        Returns None if neural brain is disabled or any error occurs.
        """
        try:
            from learning.symbol_neural_brain import symbol_neural_brain
            prob, model_source, quality = symbol_neural_brain.predict_for_signal_with_quality(
                signal,
                source=str(source or ""),
                enforce_quality=True,
            )
            if prob is not None:
                logger.debug(
                    "[NeuralBrain] _get_neural_prob prob=%.3f source=%s quality=%s",
                    prob,
                    model_source,
                    str((quality or {}).get("reason", "n/a")),
                )
                return float(prob)
        except Exception:
            pass
        # Fallback: global NeuralBrain (existing shared model)
        try:
            from learning.neural_brain import neural_brain
            prob = neural_brain.predict_probability(signal, source=str(source or ""))
            return float(prob) if prob is not None else None
        except Exception:
            return None

    def _maybe_apply_fx_confidence_soft_filter(self, signal, source: str, min_conf: float) -> tuple[bool, dict]:

        info = {
            "applied": False,
            "hard_block": False,
            "reason": "n/a",
            "size_multiplier": 1.0,
            "penalty_ratio": 0.0,
            "band_floor": None,
            "band_ceiling": float(min_conf),
            "learned_band_applied": False,
            "learned_band_reason": "",
            "learned_band_samples": 0,
        }
        src = str(source or "").strip().lower()
        if src not in ("fx", "crypto"):
            info["reason"] = "not_fx_or_crypto"
            return False, info
        if src == "fx":
            sf_label = "FX"
            enabled = bool(getattr(config, "MT5_FX_CONF_SOFT_FILTER_ENABLED", False))
            band_pts_cfg = float(getattr(config, "MT5_FX_CONF_SOFT_FILTER_BAND_PTS", 6.0) or 6.0)
            max_penalty_cfg = float(getattr(config, "MT5_FX_CONF_SOFT_FILTER_MAX_SIZE_PENALTY", 0.35) or 0.35)
            learned_enabled = bool(getattr(config, "MT5_FX_CONF_SOFT_FILTER_LEARNED_BAND_ENABLED", False))
            band_overrides = config.get_mt5_fx_conf_soft_filter_band_pts_symbol_overrides()
            pen_overrides = config.get_mt5_fx_conf_soft_filter_max_penalty_symbol_overrides()
        else:
            sf_label = "CRYPTO"
            enabled = bool(getattr(config, "MT5_CRYPTO_CONF_SOFT_FILTER_ENABLED", False))
            band_pts_cfg = float(getattr(config, "MT5_CRYPTO_CONF_SOFT_FILTER_BAND_PTS", 4.0) or 4.0)
            max_penalty_cfg = float(getattr(config, "MT5_CRYPTO_CONF_SOFT_FILTER_MAX_SIZE_PENALTY", 0.25) or 0.25)
            learned_enabled = False
            band_overrides = config.get_mt5_crypto_conf_soft_filter_band_pts_symbol_overrides()
            pen_overrides = config.get_mt5_crypto_conf_soft_filter_max_penalty_symbol_overrides()
        if not enabled:
            info["reason"] = "disabled"
            return False, info
        conf = float(getattr(signal, "confidence", 0.0) or 0.0)
        info["confidence"] = round(conf, 3)
        if conf >= float(min_conf):
            info["reason"] = "above_min"
            return False, info
        candidates = self._symbol_override_candidates(signal)
        try:
            band_override, band_reason = self._lookup_symbol_float_override(candidates, band_overrides)
            if band_override is not None:
                band_pts_cfg = float(band_override)
                info["band_pts_override_reason"] = band_reason
            pen_override, pen_reason = self._lookup_symbol_float_override(candidates, pen_overrides)
            if pen_override is not None:
                max_penalty_cfg = float(pen_override)
                info["max_penalty_override_reason"] = pen_reason
        except Exception:
            pass
        band_pts = max(0.5, float(band_pts_cfg))
        floor = float(min_conf) - band_pts
        ceil = float(min_conf)
        sym = str(getattr(signal, "symbol", "") or "").strip().upper()
        if sym and learned_enabled:
            try:
                from learning.mt5_autopilot_core import mt5_autopilot_core  # lazy import to avoid circular import
                learned = mt5_autopilot_core.fx_learned_confidence_soft_floor(
                    sym,
                    min_conf=float(min_conf),
                    base_band_pts=float(band_pts),
                )
                info["learned_band_reason"] = str(learned.get("reason", ""))
                info["learned_band_samples"] = int(learned.get("samples", 0) or 0)
                if bool(learned.get("applied")):
                    floor = float(learned.get("soft_floor", floor) or floor)
                    ceil = float(learned.get("soft_ceiling", ceil) or ceil)
                    info["learned_band_applied"] = True
            except Exception as e:
                info["learned_band_reason"] = f"exception:{e}"
        if ceil <= floor:
            floor = float(min_conf) - band_pts
            ceil = float(min_conf)
        info["band_floor"] = round(float(floor), 3)
        info["band_ceiling"] = round(float(ceil), 3)
        if conf < float(floor):
            info["hard_block"] = True
            info["reason"] = f"below_soft_band:{float(floor):.1f}"
            return False, info
        span = max(0.1, float(ceil) - float(floor))
        ratio = max(0.0, min(1.0, (float(ceil) - conf) / span))
        max_penalty = max(0.0, min(0.80, float(max_penalty_cfg)))
        penalty_ratio = round(float(max_penalty) * ratio, 4)
        size_multiplier = max(0.20, round(1.0 - penalty_ratio, 4))
        info.update({
            "applied": True,
            "reason": "soft_size_penalty",
            "penalty_ratio": penalty_ratio,
            "size_multiplier": size_multiplier,
        })
        try:
            warnings = list(getattr(signal, "warnings", []) or [])
            warnings.append(
                f"{sf_label} confidence soft-filter: conf {conf:.1f}/{float(min_conf):.1f}, size x{size_multiplier:.2f}"
            )
            signal.warnings = warnings[-8:]
        except Exception:
            pass
        logger.info(
            "[MT5] %s confidence soft filter %s conf=%.1f min=%.1f size x%.2f band=[%.1f,%.1f]%s",
            sf_label,
            str(getattr(signal, 'symbol', '') or '-'),
            conf,
            float(min_conf),
            float(size_multiplier),
            float(floor),
            float(ceil),
            (f" learned(n={int(info.get('learned_band_samples',0) or 0)})" if info.get('learned_band_applied') else ''),
        )
        return True, info

    def _mt5_margin_budget_pct_for_signal(self, signal=None, source: str = "", *, broker_symbol: str = "", signal_symbol: str = "") -> tuple[float, str]:
        base = float(getattr(config, "MT5_MAX_MARGIN_USAGE_PCT", 35.0) or 35.0)
        reason = "global"
        src = str(source or "").strip().lower()
        if src == "fx":
            fx_pct = float(getattr(config, "MT5_MAX_MARGIN_USAGE_PCT_FX", base) or base)
            base = fx_pct
            reason = "fx_default"
        try:
            overrides = config.get_mt5_margin_usage_pct_symbol_overrides()
        except Exception:
            overrides = {}
        candidates = []
        if broker_symbol:
            candidates.append(str(broker_symbol).strip().upper())
        if signal_symbol:
            candidates.append(str(signal_symbol).strip().upper())
        try:
            sym = str(getattr(signal, "symbol", "") or "").strip().upper()
            if sym:
                candidates.append(sym)
        except Exception:
            pass
        for c in candidates:
            if c and c in overrides:
                try:
                    return float(overrides[c]), f"symbol_override:{c}"
                except Exception:
                    continue
        return float(base), reason
    def execute_signal(self, signal, source: str = "", *, volume_multiplier: Optional[float] = None) -> MT5ExecutionResult:
        sig_symbol = str(getattr(signal, "symbol", "") or "")
        if not self.enabled:
            return MT5ExecutionResult(False, "disabled", "MT5 disabled", signal_symbol=sig_symbol)
        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
        entry_type_raw = str(getattr(signal, "entry_type", "") or "").strip().lower()
        planned_entry_original = _safe_float(getattr(signal, "entry", 0.0), 0.0)
        is_limit_entry_requested = (entry_type_raw in {"limit", "patience"}) and bool(getattr(config, "MT5_LIMIT_ENTRY_ENABLED", True))
        is_stop_entry_requested = (entry_type_raw in {"buy_stop", "sell_stop", "stop"}) and bool(getattr(config, "MT5_PENDING_ENTRY_ENABLED", True))
        is_pending_entry_requested = bool(is_limit_entry_requested or is_stop_entry_requested)
        limit_adaptive_exits_only = bool(getattr(config, "MT5_LIMIT_ADAPTIVE_EXITS_SIZE_ONLY", True))
        allow_limit_market_fallback = bool(getattr(config, "MT5_LIMIT_ENTRY_ALLOW_MARKET_FALLBACK", False))
        fb_override = raw_scores.get("mt5_limit_allow_market_fallback", None)
        if fb_override is not None:
            if isinstance(fb_override, str):
                v = str(fb_override).strip().lower()
                if v in {"1", "true", "yes", "on"}:
                    allow_limit_market_fallback = True
                elif v in {"0", "false", "no", "off"}:
                    allow_limit_market_fallback = False
            else:
                allow_limit_market_fallback = bool(fb_override)
        bypass_mode = bool(raw_scores.get("mt5_bypass_test_enabled", False)) and bool(getattr(config, "MT5_BYPASS_TEST_ENABLED", False))
        bypass_confidence = bool(raw_scores.get("mt5_bypass_skip_confidence", False)) and bool(getattr(config, "MT5_BYPASS_TEST_ENABLED", False))
        generic_ignore_open_positions = bool(raw_scores.get("mt5_ignore_open_positions", False))
        bypass_ignore_open_positions = (
            bool(raw_scores.get("mt5_bypass_ignore_open_positions", getattr(config, "MT5_BYPASS_TEST_IGNORE_OPEN_POSITIONS", False)))
            and bool(bypass_mode)
        ) or generic_ignore_open_positions
        try:
            bypass_magic_offset = int(raw_scores.get("mt5_bypass_magic_offset", getattr(config, "MT5_BYPASS_TEST_MAGIC_OFFSET", 500)) or 0)
        except Exception:
            bypass_magic_offset = int(getattr(config, "MT5_BYPASS_TEST_MAGIC_OFFSET", 500) or 0)
        try:
            generic_magic_offset = int(raw_scores.get("mt5_magic_offset", 0) or 0)
        except Exception:
            generic_magic_offset = 0
        exec_magic = int(config.MT5_MAGIC)
        if bypass_mode and bypass_magic_offset:
            exec_magic = int(exec_magic + bypass_magic_offset)
        elif generic_magic_offset:
            exec_magic = int(exec_magic + generic_magic_offset)
        min_conf, min_conf_reason = self._mt5_min_conf_for_signal(signal, source)
        conf_soft_applied, conf_soft_info = self._maybe_apply_fx_confidence_soft_filter(signal, source, min_conf)
        try:
            raw_scores["mt5_min_conf_threshold"] = round(float(min_conf), 3)
            raw_scores["mt5_min_conf_reason"] = str(min_conf_reason or "")
            raw_scores["mt5_bypass_confidence"] = bool(bypass_confidence)
            raw_scores["mt5_bypass_mode"] = bool(bypass_mode)
            raw_scores["mt5_bypass_ignore_open_positions"] = bool(bypass_ignore_open_positions)
            raw_scores["mt5_bypass_magic"] = int(exec_magic)
            raw_scores["mt5_ignore_open_positions"] = bool(generic_ignore_open_positions)
            raw_scores["mt5_magic_offset"] = int(generic_magic_offset)
            raw_scores["mt5_conf_soft_filter_source"] = str(source or "")
            raw_scores["mt5_conf_soft_filter_applied"] = bool(conf_soft_info.get("applied"))
            raw_scores["mt5_conf_soft_filter_reason"] = str(conf_soft_info.get("reason", ""))
            raw_scores["mt5_conf_soft_filter_size_mult"] = float(conf_soft_info.get("size_multiplier", 1.0) or 1.0)
            raw_scores["mt5_conf_soft_filter_band_floor"] = conf_soft_info.get("band_floor")
            raw_scores["mt5_conf_soft_filter_band_ceiling"] = conf_soft_info.get("band_ceiling")
            raw_scores["mt5_conf_soft_filter_learned"] = bool(conf_soft_info.get("learned_band_applied"))
            raw_scores["mt5_fx_conf_soft_filter_applied"] = bool(conf_soft_info.get("applied"))
            raw_scores["mt5_fx_conf_soft_filter_reason"] = str(conf_soft_info.get("reason", ""))
            raw_scores["mt5_fx_conf_soft_filter_size_mult"] = float(conf_soft_info.get("size_multiplier", 1.0) or 1.0)
            raw_scores["mt5_fx_conf_soft_filter_band_floor"] = conf_soft_info.get("band_floor")
            raw_scores["mt5_fx_conf_soft_filter_band_ceiling"] = conf_soft_info.get("band_ceiling")
            raw_scores["mt5_fx_conf_soft_filter_learned"] = bool(conf_soft_info.get("learned_band_applied"))
            raw_scores["mt5_entry_type"] = entry_type_raw or "market"
            raw_scores["mt5_limit_entry_requested"] = bool(is_limit_entry_requested)
            raw_scores["mt5_stop_entry_requested"] = bool(is_stop_entry_requested)
            raw_scores["mt5_pending_entry_requested"] = bool(is_pending_entry_requested)
            raw_scores["mt5_limit_adaptive_exits_size_only"] = bool(limit_adaptive_exits_only)
            raw_scores["mt5_limit_allow_market_fallback"] = bool(allow_limit_market_fallback)
            if planned_entry_original > 0:
                raw_scores["mt5_planned_entry_price"] = round(float(planned_entry_original), 8)
            signal.raw_scores = raw_scores
        except Exception:
            pass
        if (
            float(getattr(signal, "confidence", 0) or 0) < float(min_conf)
            and (not bool(conf_soft_info.get("applied")))
            and (not bypass_confidence)
        ):
            extra = ""
            if conf_soft_info.get("reason"):
                extra = f" [{str(conf_soft_info.get('reason'))}]"
            return MT5ExecutionResult(
                False,
                "skipped",
                f"below MT5 confidence threshold ({float(min_conf):.1f}) ({min_conf_reason}){extra}",
                signal_symbol=sig_symbol,
            )
        if (
            float(getattr(signal, "confidence", 0) or 0) < float(min_conf)
            and (not bool(conf_soft_info.get("applied")))
            and bool(bypass_confidence)
        ):
            try:
                raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                raw_scores["mt5_conf_bypass_applied"] = True
                signal.raw_scores = raw_scores
            except Exception:
                pass

        ok, state = self._ensure_connection()
        if not ok:
            return MT5ExecutionResult(False, "error", state, signal_symbol=sig_symbol)

        broker_symbol = self.resolve_symbol(sig_symbol)
        if not broker_symbol:
            return MT5ExecutionResult(False, "unmapped", "symbol not tradable at broker", signal_symbol=sig_symbol)

        up_broker = broker_symbol.upper()
        up_signal = sig_symbol.upper()
        if self._allow_symbols and (up_broker not in self._allow_symbols) and (up_signal not in self._allow_symbols):
            return MT5ExecutionResult(False, "blocked", "symbol not in MT5 allowlist", sig_symbol, broker_symbol)
        if (up_broker in self._block_symbols) or (up_signal in self._block_symbols):
            return MT5ExecutionResult(False, "blocked", "symbol in MT5 blocklist", sig_symbol, broker_symbol)

        try:
            adaptive_meta = None
            adaptive_size_mult = 1.0
            adaptive_plan_applied = False
            conf_soft_size_mult = float((conf_soft_info or {}).get("size_multiplier", 1.0) or 1.0)
            htf_ltf_size_mult = 1.0

            def _attach_meta(res: MT5ExecutionResult) -> MT5ExecutionResult:
                try:
                    if adaptive_meta:
                        res.execution_meta = dict(adaptive_meta)
                except Exception:
                    pass
                return res

            if not self._mt5.symbol_select(broker_symbol, True):
                return MT5ExecutionResult(False, "error", "symbol_select failed", sig_symbol, broker_symbol)

            symbol_info = self._mt5.symbol_info(broker_symbol)
            tick = self._mt5.symbol_info_tick(broker_symbol)
            if symbol_info is None or tick is None:
                return MT5ExecutionResult(False, "error", "symbol info/tick unavailable", sig_symbol, broker_symbol)

            direction = str(getattr(signal, "direction", "") or "").lower()
            if direction not in ("long", "short"):
                return MT5ExecutionResult(False, "error", "invalid signal direction", sig_symbol, broker_symbol)

            allowed, reason = self._position_limits_ok(
                broker_symbol,
                direction,
                ignore_open_positions=bool(bypass_ignore_open_positions),
            )
            if not allowed:
                return MT5ExecutionResult(False, "skipped", reason, sig_symbol, broker_symbol)

            is_long = direction == "long"
            
            # --- Entry Confirmation & HTF/LTF Gates ---
            from execution.entry_confirmation import check_m5_confirmation, check_htf_ltf_convergence
            
            entry_price = float(getattr(signal, "entry", 0.0))
            atr = float(getattr(signal, "atr", 0.0))
            
            # 1. M5 Confirmation
            if entry_price > 0 and atr > 0:
                m5_res = check_m5_confirmation(self._mt5, broker_symbol, direction, entry_price, atr)
                if not m5_res.ok:
                    return MT5ExecutionResult(
                        False, "m5_blocked", f"M5 confirmation failed: {m5_res.reason}", sig_symbol, broker_symbol
                    )
                if m5_res.status != "disabled" and not m5_res.skipped_due_to_distance:
                    logger.info(f"[EntryConfirm] {broker_symbol} M5 gate: {m5_res.status} ({m5_res.reason})")
                    
            # 2. HTF/LTF Convergence
            conv_res = check_htf_ltf_convergence(self._mt5, broker_symbol, direction)
            if not conv_res.ok:
                return MT5ExecutionResult(
                    False, "htf_blocked", f"HTF/LTF filter blocked: {conv_res.reason}", sig_symbol, broker_symbol
                )
            if conv_res.status != "disabled":
                if conv_res.status != "neutral":
                    logger.info(f"[HTFFilter] {broker_symbol} convergence: {conv_res.status} ({conv_res.reason})")
                htf_ltf_size_mult = conv_res.size_mult

            order_type = self._mt5.ORDER_TYPE_BUY if is_long else self._mt5.ORDER_TYPE_SELL
            account = self._mt5.account_info()
            if account is None:
                return MT5ExecutionResult(False, "error", "account_info unavailable", sig_symbol, broker_symbol)
            free_margin = float(getattr(account, "margin_free", 0.0) or 0.0)
            if free_margin <= 0:
                return MT5ExecutionResult(False, "skipped", "no free margin", sig_symbol, broker_symbol)
            micro_ctx = self._micro_account_bucket_ctx(account) if self._micro_learner_enabled() else None
            cached_deny = self._micro_cached_deny(micro_ctx, broker_symbol, source=source, signal_symbol=sig_symbol)
            if cached_deny:
                deny_status = str(cached_deny.get("status") or "deny").lower()
                age_m = (float(cached_deny.get("age_sec", 0.0) or 0.0) / 60.0) if cached_deny else 0.0
                return MT5ExecutionResult(
                    False,
                    "micro_filtered",
                    f"micro whitelist cache: {deny_status} (age={age_m:.1f}m) {str(cached_deny.get('last_reason') or '')}".strip(),
                    sig_symbol,
                    broker_symbol,
                )

            ask = float(getattr(tick, "ask", 0.0) or 0.0)
            bid = float(getattr(tick, "bid", 0.0) or 0.0)
            spread_now = max(0.0, ask - bid) if (ask > 0 and bid > 0) else 0.0
            mid_now = ((ask + bid) / 2.0) if (ask > 0 and bid > 0) else max(ask, bid, 0.0)
            spread_pct_now = ((spread_now / mid_now) * 100.0) if mid_now > 0 else 0.0
            confidence_now = float(getattr(signal, "confidence", 0.0) or 0.0)
            atr_now = abs(float(getattr(signal, "atr", 0.0) or 0.0))
            price = float(ask if is_long else bid)
            
            if price <= 0.0:
                self._market_closed_cache[broker_symbol] = time.time()
                return MT5ExecutionResult(False, "market_closed", f"market closed / off quotes (price 0.0)", sig_symbol, broker_symbol, retcode=10018)

            sl = self._price_round(float(signal.stop_loss), symbol_info)
            tp = self._price_round(float(signal.take_profit_2), symbol_info)
            price = self._price_round(price, symbol_info)
            requested_entry_price = self._price_round(float(planned_entry_original if planned_entry_original > 0 else price), symbol_info)

            if bool(getattr(config, "MT5_MICRO_MODE_ENABLED", False)):
                mid = (ask + bid) / 2.0 if (ask > 0 and bid > 0) else max(ask, bid, price, 0.0)
                spread = max(0.0, ask - bid) if (ask > 0 and bid > 0) else 0.0
                spread_pct = ((spread / mid) * 100.0) if mid > 0 else 0.0
                max_spread_pct = max(0.0, float(getattr(config, "MT5_MICRO_MAX_SPREAD_PCT", 0.15)))
                if spread_pct > max_spread_pct:
                    self._micro_record_symbol_observation(
                        micro_ctx,
                        broker_symbol,
                        signal_symbol=sig_symbol,
                        status="deny_spread",
                        reason=f"spread {spread_pct:.4f}% > {max_spread_pct:.4f}%",
                        spread_pct=spread_pct,
                    )
                    return MT5ExecutionResult(
                        False,
                        "micro_filtered",
                        f"micro spread filter: {spread_pct:.4f}% > max {max_spread_pct:.4f}% (spread={spread:.5f})",
                        sig_symbol,
                        broker_symbol,
                    )

            # Adaptive execution planner (bounded): adjust RR/SL/TP/size using
            # symbol behavior stats + volatility + spread + session/confidence.
            try:
                adaptive_enabled = bool(getattr(config, "MT5_ADAPTIVE_EXECUTION_ENABLED", True))
                allow_limit_adaptive = bool(limit_adaptive_exits_only and is_limit_entry_requested)
                if adaptive_enabled and ((not is_pending_entry_requested) or allow_limit_adaptive):
                    acct_login = int(getattr(account, "login", 0) or 0)
                    st2 = self.status()
                    acct_server = str(st2.get("account_server", "") or "")
                    account_key = f"{acct_server}|{acct_login}" if acct_login and acct_server else ""
                    plan_exec_price = float(requested_entry_price) if is_pending_entry_requested else float(price)
                    plan = mt5_adaptive_trade_planner.plan_execution(
                        signal=signal,
                        account_key=account_key,
                        broker_symbol=broker_symbol,
                        execution_price=plan_exec_price,
                        bid=float(bid),
                        ask=float(ask),
                        point=float(getattr(symbol_info, "point", 0.0) or 0.0),
                        source=str(source or ""),
                        neural_prob=self._get_neural_prob(signal, source),
                    )
                    if plan and bool(plan.ok):
                        adaptive_size_mult = max(0.25, float(plan.size_multiplier or 1.0))
                        adaptive_meta = plan.to_dict()
                        adaptive_meta["source"] = str(source or "")
                        plan_factors = dict(getattr(plan, "factors", {}) or {})
                        if bool(plan_factors.get("direction_blocked")):
                            block_reason = str(
                                plan_factors.get("direction_block_reason")
                                or "adaptive directional guard blocked this setup"
                            )
                            return _attach_meta(
                                MT5ExecutionResult(False, "skipped", block_reason, sig_symbol, broker_symbol)
                            )
                        if bool(plan.applied):
                            adaptive_plan_applied = True
                            # Mutate execution-facing signal fields so journaling/notifications reflect actual plan used.
                            signal.entry = float(requested_entry_price) if is_pending_entry_requested else float(plan.entry or price)
                            signal.stop_loss = float(plan.stop_loss or signal.stop_loss)
                            if getattr(plan, "take_profit_1", None) is not None:
                                signal.take_profit_1 = float(plan.take_profit_1)
                            if getattr(plan, "take_profit_2", None) is not None:
                                signal.take_profit_2 = float(plan.take_profit_2)
                            if getattr(plan, "take_profit_3", None) is not None:
                                signal.take_profit_3 = float(plan.take_profit_3)
                            if getattr(plan, "rr_target", None) is not None:
                                signal.risk_reward = float(plan.rr_target)
                            sl = self._price_round(float(signal.stop_loss), symbol_info)
                            tp = self._price_round(float(signal.take_profit_2), symbol_info)
                            price = self._price_round(float(signal.entry), symbol_info)
                            raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                            raw_scores["mt5_adaptive_execution"] = {
                                "rr_base": plan.rr_base,
                                "rr_target": plan.rr_target,
                                "stop_scale": plan.stop_scale,
                                "size_multiplier": plan.size_multiplier,
                                "factors": dict(plan.factors or {}),
                            }
                            raw_scores["mt5_adaptive_mode"] = "limit_exits_size_only" if is_limit_entry_requested else "full"
                            signal.raw_scores = raw_scores
                            warnings = list(getattr(signal, "warnings", []) or [])
                            warnings.append(
                                f"MT5 adaptive exec: RR {float(plan.rr_base or 0):.2f}->{float(plan.rr_target or 0):.2f}, "
                                f"SLx{float(plan.stop_scale or 1):.2f}, sizex{float(plan.size_multiplier or 1):.2f}"
                            )
                            signal.warnings = warnings[-8:]
                            logger.info(
                                "[MT5AdaptiveExec] %s -> %s RR %.2f->%.2f SLx%.2f sizex%.2f samples=%s spread=%.4f%% atr=%.4f%%",
                                sig_symbol,
                                broker_symbol,
                                float(plan.rr_base or 0.0),
                                float(plan.rr_target or 0.0),
                                float(plan.stop_scale or 1.0),
                                float(plan.size_multiplier or 1.0),
                                (plan.factors or {}).get("samples", 0),
                                float((plan.factors or {}).get("spread_pct", 0.0) or 0.0),
                                float((plan.factors or {}).get("atr_pct", 0.0) or 0.0),
                            )
                elif is_pending_entry_requested:
                    logger.debug("[MT5AdaptiveExec] skipped for limit entry (exits/size mode disabled) %s", sig_symbol)
            except Exception as e:
                logger.debug("[MT5AdaptiveExec] execute integration error: %s", e, exc_info=True)

            point = float(getattr(symbol_info, "point", 0.0) or 0.0)
            stops_level = int(getattr(symbol_info, "trade_stops_level", 0) or 0)
            min_gap = max(point, point * max(1, stops_level)) if point > 0 else 0
            stops_ref_price = float(requested_entry_price) if bool(is_pending_entry_requested) else float(price)
            if is_long:
                if not (sl < stops_ref_price - min_gap and tp > stops_ref_price + min_gap):
                    return _attach_meta(MT5ExecutionResult(False, "invalid_stops", "long stops invalid for current price", sig_symbol, broker_symbol))
            else:
                if not (sl > stops_ref_price + min_gap and tp < stops_ref_price - min_gap):
                    return _attach_meta(MT5ExecutionResult(False, "invalid_stops", "short stops invalid for current price", sig_symbol, broker_symbol))

            # ── Tiger Risk Governor: Quality-Based Lot Sizing ─────────────
            tiger_meta = {}
            tiger_lot = None
            if tiger_risk_governor is not None:
                try:
                    equity = float(getattr(account, "equity", 0.0) or 0.0)
                    confidence = float(getattr(signal, "confidence", 0.0) or 0.0)
                    sl_mapped = bool(getattr(signal, "sl_liquidity_mapped", False))

                    # Calculate SL distance in pips
                    signal_sl = float(getattr(signal, "stop_loss", 0.0) or 0.0)
                    risk_dist = abs(price - signal_sl) if signal_sl > 0 else 0.0
                    pip_size = float(point * 10) if point > 0 else 0.0001
                    risk_pips = (risk_dist / pip_size) if pip_size > 0 else 50.0
                    risk_pips = max(5.0, risk_pips)  # floor 5 pips

                    # Pip value estimate (per micro lot)
                    pip_value = 0.10  # default for FX micro
                    if "JPY" in up_broker:
                        pip_value = 0.08
                    elif any(x in up_broker for x in ["XAU", "GOLD"]):
                        pip_value = 0.10
                    elif any(x in up_broker for x in ["BTC", "ETH", "CRYPTO"]):
                        pip_value = 0.01

                    tiger_lot, tiger_meta = tiger_risk_governor.calculate_lot_size(
                        equity=equity,
                        risk_distance_pips=risk_pips,
                        pip_value=pip_value,
                        confidence=confidence,
                        sl_liquidity_mapped=sl_mapped,
                    )
                    # Check position limit
                    current_pos = len(self._mt5.positions_get() or ())
                    pos_ok, pos_reason = tiger_risk_governor.check_position_limit(equity, current_pos)
                    if not pos_ok:
                        return _attach_meta(MT5ExecutionResult(
                            False, "tiger_limit", pos_reason, sig_symbol, broker_symbol
                        ))

                    # Check circuit breaker
                    daily_pnl = float(getattr(account, "profit", 0.0) or 0.0)
                    cb_ok, cb_reason = tiger_risk_governor.check_circuit_breaker(equity, daily_pnl)
                    if not cb_ok:
                        return _attach_meta(MT5ExecutionResult(
                            False, "tiger_circuit_breaker", cb_reason, sig_symbol, broker_symbol
                        ))

                    logger.info(
                        "[MT5Tiger] %s lot=%.2f phase=%s equity=$%.2f risk_pips=%.1f conf=%.1f sl_mapped=%s",
                        broker_symbol, tiger_lot, tiger_meta.get('phase', '?'),
                        equity, risk_pips, confidence, sl_mapped,
                    )
                except Exception as e:
                    logger.debug("[MT5Tiger] governor integration error: %s", e, exc_info=True)

            vm_in = 1.0 if volume_multiplier is None else max(0.05, float(volume_multiplier))
            vm = max(0.05, float(vm_in) * float(adaptive_size_mult or 1.0) * float(conf_soft_size_mult or 1.0) * float(htf_ltf_size_mult))

            if tiger_lot is not None and tiger_lot > 0:
                # Use Tiger Risk Governor lot as base, still apply adaptive multipliers
                desired_volume = self._normalize_volume(float(tiger_lot) * float(adaptive_size_mult or 1.0) * float(htf_ltf_size_mult), symbol_info)
            else:
                # Fallback: original static lot sizing
                desired_volume = self._normalize_volume(float(config.MT5_LOT_SIZE) * vm, symbol_info)

            volume = desired_volume
            if volume <= 0:
                return _attach_meta(MT5ExecutionResult(False, "error", "volume normalized to zero", sig_symbol, broker_symbol))

            # Margin safety guard (critical for tiny accounts / high leverage).
            max_usage_pct, max_usage_reason = self._mt5_margin_budget_pct_for_signal(
                signal,
                source,
                broker_symbol=broker_symbol,
                signal_symbol=sig_symbol,
            )
            min_left = float(config.MT5_MIN_FREE_MARGIN_AFTER_TRADE)
            fitted_volume, margin_required, fit_reason = self._fit_volume_by_margin_budget(
                order_type=order_type,
                broker_symbol=broker_symbol,
                symbol_info=symbol_info,
                price=float(price),
                desired_volume=float(desired_volume),
                free_margin=float(free_margin),
                max_usage_pct=float(max_usage_pct),
                min_left=float(min_left),
            )
            fit_reason_text = str(fit_reason or "")
            if str(max_usage_reason or "") != "global":
                fit_reason_text = f"{fit_reason_text} (margin_budget {float(max_usage_pct):.1f}% {max_usage_reason})".strip()

            if fitted_volume is None:
                status = "skipped"
                if bool(getattr(config, "MT5_MICRO_MODE_ENABLED", False)):
                    status = "micro_filtered"
                fit_reason_l = str(fit_reason or "").lower()
                obs_status = "deny_margin"
                if "order_calc_margin unavailable" in fit_reason_l:
                    obs_status = "deny_contract"
                self._micro_record_symbol_observation(
                    micro_ctx,
                    broker_symbol,
                    signal_symbol=sig_symbol,
                    status=obs_status,
                    reason=fit_reason_text,
                    margin_required=margin_required,
                    min_lot_margin=margin_required,
                    margin_budget_pct=max_usage_pct,
                )
                return _attach_meta(MT5ExecutionResult(False, status, fit_reason_text, sig_symbol, broker_symbol))
            volume = float(fitted_volume)
            self._micro_record_symbol_observation(
                micro_ctx,
                broker_symbol,
                signal_symbol=sig_symbol,
                status="allow",
                reason=("affordable" if (fit_reason == "ok") else fit_reason_text),
                spread_pct=(spread_pct if bool(getattr(config, "MT5_MICRO_MODE_ENABLED", False)) else None),
                margin_required=margin_required,
                margin_budget_pct=max_usage_pct,
            )

            comment = self._build_order_comment(signal=signal, source=source, signal_symbol=up_signal)
            if fit_reason and fit_reason != "ok":
                logger.info("[MT5] %s", fit_reason)

            now = time.time()
            if broker_symbol in self._market_closed_cache:
                if now - self._market_closed_cache[broker_symbol] < 300:
                    return _attach_meta(MT5ExecutionResult(False, "market_closed", f"{broker_symbol} market closed (cached)", sig_symbol, broker_symbol))

            is_limit_entry = bool(is_limit_entry_requested)
            is_stop_entry = bool(is_stop_entry_requested)
            if is_pending_entry_requested:
                price = float(requested_entry_price)

            def _limit_fallback_guard(market_price: float) -> tuple[bool, str, dict]:
                meta = {
                    "market_price": float(market_price),
                "requested_entry_price": float(requested_entry_price),
                "spread_pct": float(spread_pct_now),
                "confidence": float(confidence_now),
            }
                if not bool(allow_limit_market_fallback):
                    return False, "fallback_disabled", meta
                min_conf_fb = max(0.0, float(getattr(config, "MT5_LIMIT_FALLBACK_MIN_CONFIDENCE", 82.0) or 82.0))
                max_spread_fb = max(0.0, float(getattr(config, "MT5_LIMIT_FALLBACK_MAX_SPREAD_PCT", 0.03) or 0.03))
                max_slip_atr = max(0.0, float(getattr(config, "MT5_LIMIT_FALLBACK_MAX_SLIPPAGE_ATR", 0.20) or 0.20))
                slip_abs = abs(float(market_price) - float(requested_entry_price))
                slip_atr = (slip_abs / float(atr_now)) if float(atr_now) > 0 else 9_999.0
                meta["slippage_abs"] = float(slip_abs)
                meta["slippage_atr"] = float(slip_atr)
                if float(confidence_now) < float(min_conf_fb):
                    return False, f"confidence<{min_conf_fb:.1f}", meta
                if float(spread_pct_now) > float(max_spread_fb):
                    return False, f"spread>{max_spread_fb:.4f}%", meta
                if float(atr_now) <= 0:
                    return False, "atr_unavailable", meta
                if float(slip_atr) > float(max_slip_atr):
                    return False, f"slippage_atr>{max_slip_atr:.3f}", meta
                return True, "ok", meta
            
            action = self._mt5.TRADE_ACTION_PENDING if is_pending_entry_requested else self._mt5.TRADE_ACTION_DEAL

            if is_limit_entry:
                order_type = self._mt5.ORDER_TYPE_BUY_LIMIT if is_long else self._mt5.ORDER_TYPE_SELL_LIMIT
            elif is_stop_entry:
                order_type = self._mt5.ORDER_TYPE_BUY_STOP if is_long else self._mt5.ORDER_TYPE_SELL_STOP
            else:
                order_type = self._mt5.ORDER_TYPE_BUY if is_long else self._mt5.ORDER_TYPE_SELL

            # Pending entry validation against current market.
            if is_limit_entry:
                if is_long and price <= ask:
                    pass # price is valid limit
                elif not is_long and price >= bid:
                    pass # price is valid limit
                else:
                    market_px = self._price_round(float(ask if is_long else bid), symbol_info)
                    allow_fb, fb_reason, fb_meta = _limit_fallback_guard(float(market_px))
                    try:
                        rs_fb = dict(getattr(signal, "raw_scores", {}) or {})
                        rs_fb["mt5_limit_fallback_guard_reason"] = str(fb_reason)
                        rs_fb["mt5_limit_fallback_guard"] = dict(fb_meta or {})
                        signal.raw_scores = rs_fb
                    except Exception:
                        pass
                    if not allow_fb:
                        return _attach_meta(
                            MT5ExecutionResult(
                                False,
                                "skipped",
                                (
                                    f"strict limit: entry {price} crossed market (bid={bid}, ask={ask}) "
                                    f"| fallback_guard:{fb_reason}"
                                ),
                                sig_symbol,
                                broker_symbol,
                            )
                        )
                    # Guarded fallback mode: preserve cadence only when quality conditions are met.
                    logger.warning(
                        "[MT5] limit entry %s invalid vs market (bid=%s ask=%s); guarded fallback -> market (%s)",
                        price,
                        bid,
                        ask,
                        fb_reason,
                    )
                    action = self._mt5.TRADE_ACTION_DEAL
                    order_type = self._mt5.ORDER_TYPE_BUY if is_long else self._mt5.ORDER_TYPE_SELL
                    price = float(market_px)
                    try:
                        rs_fb = dict(getattr(signal, "raw_scores", {}) or {})
                        rs_fb["mt5_limit_fallback_market"] = True
                        rs_fb["mt5_limit_fallback_reason"] = str(fb_reason)
                        signal.raw_scores = rs_fb
                    except Exception:
                        pass
            elif is_stop_entry:
                if is_long and price >= ask:
                    pass
                elif (not is_long) and price <= bid:
                    pass
                else:
                    return _attach_meta(
                        MT5ExecutionResult(
                            False,
                            "skipped",
                            f"strict stop: entry {price} not beyond market (bid={bid}, ask={ask})",
                            sig_symbol,
                            broker_symbol,
                        )
                    )

            # --- Phase 6: High-Precision Exits (Dynamic TP Padding) ---
            if bool(getattr(config, "MT5_EXIT_DYNAMIC_TP_SPREAD_PAD", True)) and (not adaptive_plan_applied):
                # Calculate real-time spread + estimated commission padding
                real_spread = max(0.0, ask - bid)
                comm_pips = float(getattr(config, "MT5_EXIT_DYNAMIC_TP_COMM_PIPS", 0.5))
                # pip value multiplier based on digits
                pip_mult = 10.0 if "JPY" in broker_symbol else 10000.0 if "XAU" not in broker_symbol else 10.0
                if "BTC" in up_broker or "ETH" in up_broker or "CRYPTO" in up_broker:
                    pip_mult = 1.0
                comm_pad = (comm_pips / pip_mult) if pip_mult > 0 else 0.0
                
                total_pad = real_spread + comm_pad
                if total_pad > 0.0:
                    original_tp = tp
                    if is_long:
                        tp = self._price_round(tp + total_pad, symbol_info)
                    else:
                        tp = self._price_round(tp - total_pad, symbol_info)
                    logger.info("[MT5-Padding] %s Padding TP by %.5f (spread: %.5f, comm: %.5f). Original TP: %.5f -> New TP: %.5f", broker_symbol, total_pad, real_spread, comm_pad, original_tp, tp)

            try:
                rs_req = dict(getattr(signal, "raw_scores", {}) or {})
                rs_req["mt5_req_action"] = int(action)
                rs_req["mt5_order_type"] = int(order_type)
                rs_req["mt5_req_price"] = round(float(price), 8)
                rs_req["mt5_req_spread_pct"] = round(float(spread_pct_now), 6)
                rs_req["mt5_entry_mode"] = "limit" if bool(is_limit_entry) else ("stop" if bool(is_stop_entry) else "market")
                signal.raw_scores = rs_req
            except Exception:
                pass

            request = {
                "action": action,
                "symbol": broker_symbol,
                "volume": volume,
                "type": order_type,
                "price": price,
                "sl": sl,
                "tp": tp,
                "deviation": int(config.MT5_DEVIATION),
                "magic": int(exec_magic),
                "comment": comment,
                "type_time": self._mt5.ORDER_TIME_SPECIFIED if is_pending_entry_requested else self._mt5.ORDER_TIME_GTC,
                "type_filling": self._pick_filling_mode(symbol_info),
            }

            if is_pending_entry_requested:
                # Set expiration time for Pending Orders
                expiration_mins = int(getattr(config, "MT5_LIMIT_TIMEOUT_MINS", 60))
                request["expiration"] = int(time.time() + (expiration_mins * 60))


            if self.dry_run:
                return _attach_meta(MT5ExecutionResult(
                    True,
                    "dry_run",
                    f"dry-run order prepared: {request}",
                    signal_symbol=sig_symbol,
                    broker_symbol=broker_symbol,
                    dry_run=True,
                    volume=float(volume),
                ))

            with self._lock:
                if hasattr(self._conn.root, "exposed_order_send"):
                    result = self._conn.root.exposed_order_send(request)
                else:
                    result = self._mt5.order_send(request)

                if result is None:
                    try:
                        if hasattr(self._conn.root, "exposed_last_error"):
                            err = self._conn.root.exposed_last_error()
                        else:
                            err = self._mt5.last_error()
                    except Exception:
                        err = None
                    req_ctx = {
                        "action": int(request.get("action", 0) or 0),
                        "type": int(request.get("type", 0) or 0),
                        "volume": float(request.get("volume", 0.0) or 0.0),
                        "price": float(request.get("price", 0.0) or 0.0),
                        "sl": float(request.get("sl", 0.0) or 0.0),
                        "tp": float(request.get("tp", 0.0) or 0.0),
                        "type_filling": int(request.get("type_filling", 0) or 0),
                    }
                    return _attach_meta(
                        MT5ExecutionResult(
                            False,
                            "error",
                            f"order_send returned None | last_error={_fmt_mt5_last_error(err)} | request={json.dumps(req_ctx, ensure_ascii=True, separators=(',', ':'))}",
                            sig_symbol,
                            broker_symbol,
                        )
                    )

            retcode = int(getattr(result, "retcode", -1))

            # For scalp-like limit entries, some brokers reject pending requests with
            # transient price/expiration validation errors. Retry once as market order
            # so cadence does not break.
            if is_limit_entry and retcode in {10015, 10022}:
                market_px = self._price_round(float(ask if is_long else bid), symbol_info)
                allow_fb, fb_reason, fb_meta = _limit_fallback_guard(float(market_px))
                try:
                    rs_fb = dict(getattr(signal, "raw_scores", {}) or {})
                    rs_fb["mt5_limit_fallback_guard_reason"] = str(fb_reason)
                    rs_fb["mt5_limit_fallback_guard"] = dict(fb_meta or {})
                    signal.raw_scores = rs_fb
                except Exception:
                    pass
                if not allow_fb:
                    return _attach_meta(
                        MT5ExecutionResult(
                            False,
                            "skipped",
                            f"limit fallback blocked after retcode={retcode}: {fb_reason}",
                            sig_symbol,
                            broker_symbol,
                        )
                    )
                try:
                    logger.warning(
                        "[MT5] limit order rejected retcode=%s for %s; guarded fallback retry -> market (%s)",
                        retcode,
                        broker_symbol,
                        fb_reason,
                    )
                    fallback_req = dict(request)
                    fallback_req["action"] = self._mt5.TRADE_ACTION_DEAL
                    fallback_req["type"] = self._mt5.ORDER_TYPE_BUY if is_long else self._mt5.ORDER_TYPE_SELL
                    fallback_req["price"] = float(market_px)
                    fallback_req["type_time"] = self._mt5.ORDER_TIME_GTC
                    fallback_req.pop("expiration", None)
                    with self._lock:
                        if hasattr(self._conn.root, "exposed_order_send"):
                            fallback_result = self._conn.root.exposed_order_send(fallback_req)
                        else:
                            fallback_result = self._mt5.order_send(fallback_req)
                    if fallback_result is not None:
                        result = fallback_result
                        retcode = int(getattr(result, "retcode", -1))
                        try:
                            rs_fb = dict(getattr(signal, "raw_scores", {}) or {})
                            rs_fb["mt5_limit_fallback_market"] = True
                            rs_fb["mt5_limit_fallback_reason"] = str(fb_reason)
                            signal.raw_scores = rs_fb
                        except Exception:
                            pass
                except Exception as e:
                    logger.warning("[MT5] fallback market retry failed: %s", e)

            if retcode == 10018 or retcode == getattr(self._mt5, "TRADE_RETCODE_MARKET_CLOSED", 10018):
                self._market_closed_cache[broker_symbol] = time.time()
                return _attach_meta(MT5ExecutionResult(False, "market_closed", f"market closed retcode={retcode}", sig_symbol, broker_symbol, retcode=retcode))

            done_codes = {
                int(getattr(self._mt5, "TRADE_RETCODE_DONE", 10009)),
                int(getattr(self._mt5, "TRADE_RETCODE_PLACED", 10008)),
                int(getattr(self._mt5, "TRADE_RETCODE_DONE_PARTIAL", 10010)),
            }
            done_fill_codes = {
                int(getattr(self._mt5, "TRADE_RETCODE_DONE", 10009)),
                int(getattr(self._mt5, "TRADE_RETCODE_DONE_PARTIAL", 10010)),
            }
            ticket = getattr(result, "order", None) or getattr(result, "deal", None)
            if retcode in done_codes:
                position_id = self._resolve_filled_position_id(
                    broker_symbol,
                    is_long,
                    comment,
                    magic_override=exec_magic,
                )
                try:
                    rs_done = dict(getattr(signal, "raw_scores", {}) or {})
                    rs_done["mt5_retcode"] = int(retcode)
                    rs_done["mt5_ticket"] = int(ticket) if ticket else None
                    rs_done["mt5_request_price"] = round(float(request.get("price", 0.0) or 0.0), 8)
                    fill_price = _safe_float(getattr(result, "price", 0.0), 0.0)
                    if int(retcode) in done_fill_codes and fill_price > 0:
                        rs_done["mt5_actual_fill_price"] = round(float(fill_price), 8)
                        if planned_entry_original > 0:
                            delta = float(fill_price) - float(planned_entry_original)
                            rs_done["mt5_fill_vs_planned"] = round(delta, 8)
                            rs_done["mt5_fill_vs_planned_pct"] = round((delta / float(planned_entry_original)) * 100.0, 6)
                    elif is_limit_entry:
                        rs_done["mt5_pending_order_price"] = round(float(request.get("price", 0.0) or 0.0), 8)
                    signal.raw_scores = rs_done
                except Exception:
                    pass

                # ── Tiger: Record signal in Signal Store ─────────────────
                try:
                    if signal_store is not None:
                        signal_store.store_signal(
                            signal,
                            source=str(source or ""),
                            mt5_ticket=int(ticket) if ticket else None,
                            mt5_executed=True,
                            execution_status="filled",
                        )
                except Exception as e:
                    logger.debug("[MT5Tiger] signal_store error: %s", e)

                result_obj = MT5ExecutionResult(
                    True,
                    "filled",
                    f"order accepted retcode={retcode}",
                    signal_symbol=sig_symbol,
                    broker_symbol=broker_symbol,
                    retcode=retcode,
                    ticket=int(ticket) if ticket else None,
                    position_id=position_id,
                    volume=float(volume),
                )
                # Attach Tiger metadata
                if tiger_meta:
                    try:
                        meta = getattr(result_obj, 'execution_meta', {}) or {}
                        meta['tiger_risk'] = tiger_meta
                        result_obj.execution_meta = meta
                    except Exception:
                        pass
                return _attach_meta(result_obj)
            return _attach_meta(MT5ExecutionResult(
                False,
                "rejected",
                _retcode_detail(retcode),
                signal_symbol=sig_symbol,
                broker_symbol=broker_symbol,
                retcode=retcode,
            ))
        except Exception as e:
            return _attach_meta(MT5ExecutionResult(False, "error", f"execution exception: {e}", sig_symbol, broker_symbol))

    @staticmethod
    def _preview_scenario_key(scenario: str) -> str:
        s = str(scenario or "").strip().lower().replace("_", "-")
        if s in {"", "base", "balanced", "normal", "default"}:
            return "balanced"
        if s in {"conservative", "safe", "micro-safe"}:
            return "conservative"
        if s in {"aggressive", "aggro", "fast"}:
            return "aggressive"
        return "balanced"

    @staticmethod
    def _preview_scenario_params(scenario: str) -> dict:
        s = MT5Executor._preview_scenario_key(scenario)
        if s == "conservative":
            return {"scenario": s, "rr_mult": 0.92, "stop_scale_mult": 1.06, "size_mult": 0.82}
        if s == "aggressive":
            return {"scenario": s, "rr_mult": 1.10, "stop_scale_mult": 0.96, "size_mult": 1.12}
        return {"scenario": "balanced", "rr_mult": 1.00, "stop_scale_mult": 1.00, "size_mult": 1.00}

    def _apply_preview_scenario_to_adaptive_plan(
        self,
        *,
        scenario: str,
        adaptive_plan,
        direction: str,
        point: float,
        price: float,
        rr_floor: float,
        rr_cap: float,
        stop_scale_lo: float,
        stop_scale_hi: float,
        size_lo: float,
        size_hi: float,
    ):
        if adaptive_plan is None or not bool(getattr(adaptive_plan, "ok", False)):
            return adaptive_plan
        params = self._preview_scenario_params(scenario)
        if params["scenario"] == "balanced":
            try:
                factors = dict(getattr(adaptive_plan, "factors", {}) or {})
                factors["scenario"] = "balanced"
                adaptive_plan.factors = factors
            except Exception:
                pass
            return adaptive_plan
        try:
            rr_base = _safe_float(getattr(adaptive_plan, "rr_base", 0.0), 0.0)
            rr_target = _safe_float(getattr(adaptive_plan, "rr_target", rr_base), rr_base)
            stop_scale = _safe_float(getattr(adaptive_plan, "stop_scale", 1.0), 1.0)
            size_mult = _safe_float(getattr(adaptive_plan, "size_multiplier", 1.0), 1.0)
            entry = _safe_float(getattr(adaptive_plan, "entry", price), price)
            sl = _safe_float(getattr(adaptive_plan, "stop_loss", 0.0), 0.0)
            if entry <= 0 or sl <= 0:
                return adaptive_plan

            base_risk = abs(entry - sl) / max(0.01, stop_scale)
            rr_target = max(float(rr_floor), min(float(rr_cap), rr_target * float(params["rr_mult"])))
            rr_target = round(float(rr_target), 2)
            stop_scale = max(float(stop_scale_lo), min(float(stop_scale_hi), stop_scale * float(params["stop_scale_mult"])))
            size_mult = max(float(size_lo), min(float(size_hi), size_mult * float(params["size_mult"])))
            new_risk = max(float(point or 0.0) * 2.0, float(base_risk) * float(stop_scale))
            is_long = str(direction or "").lower() == "long"
            if is_long:
                sl2 = entry - new_risk
                tp1 = entry + new_risk
                tp2 = entry + new_risk * rr_target
                tp3 = entry + new_risk * max(rr_target + 1.0, 3.0)
            else:
                sl2 = entry + new_risk
                tp1 = entry - new_risk
                tp2 = entry - new_risk * rr_target
                tp3 = entry - new_risk * max(rr_target + 1.0, 3.0)
            adaptive_plan.stop_loss = float(sl2)
            adaptive_plan.take_profit_1 = float(tp1)
            adaptive_plan.take_profit_2 = float(tp2)
            adaptive_plan.take_profit_3 = float(tp3)
            adaptive_plan.rr_target = float(rr_target)
            adaptive_plan.stop_scale = round(float(stop_scale), 4)
            adaptive_plan.size_multiplier = round(float(size_mult), 4)
            adaptive_plan.applied = True
            adaptive_plan.reason = f"adaptive_{params['scenario']}"
            factors = dict(getattr(adaptive_plan, "factors", {}) or {})
            factors["scenario"] = str(params["scenario"])
            factors["whatif_rr_mult"] = float(params["rr_mult"])
            factors["whatif_stop_scale_mult"] = float(params["stop_scale_mult"])
            factors["whatif_size_mult"] = float(params["size_mult"])
            adaptive_plan.factors = factors
        except Exception:
            return adaptive_plan
        return adaptive_plan

    def preview_adaptive_execution(self, signal, source: str = "", *, volume_multiplier: Optional[float] = None, scenario: str = "") -> dict:
        """
        Preview the MT5 execution plan (adaptive RR/SL/TP/size + margin fit) without sending any order.
        Used by /mt5_plan and operator review flows.
        """
        sig_symbol = str(getattr(signal, "symbol", "") or "")
        out = {
            "ok": False,
            "enabled": self.enabled,
            "connected": False,
            "scenario": self._preview_scenario_key(scenario),
            "signal_symbol": sig_symbol,
            "broker_symbol": "",
            "status": "",
            "reason": "",
            "base": {},
            "adaptive": {},
            "execution": {},
            "margin": {},
            "account": {},
            "error": "",
        }
        if not self.enabled:
            out["status"] = "disabled"
            out["reason"] = "MT5 disabled"
            return out
        min_conf, min_conf_reason = self._mt5_min_conf_for_signal(signal, source)
        conf_soft_applied, conf_soft_info = self._maybe_apply_fx_confidence_soft_filter(signal, source, min_conf)
        out["min_confidence"] = float(min_conf)
        out["min_confidence_reason"] = str(min_conf_reason)
        out["conf_soft_filter"] = dict(conf_soft_info or {})
        if float(getattr(signal, "confidence", 0) or 0) < float(min_conf) and (not bool(conf_soft_info.get("applied"))):
            out["status"] = "below_confidence"
            extra = f" [{conf_soft_info.get('reason')}]" if conf_soft_info.get("reason") else ""
            out["reason"] = f"below MT5 confidence threshold ({float(min_conf):.1f}) ({min_conf_reason}){extra}"
            return out

        ok, state = self._ensure_connection()
        if not ok:
            out["status"] = "error"
            out["reason"] = str(state)
            out["error"] = str(state)
            return out
        out["connected"] = True

        broker_symbol = self.resolve_symbol(sig_symbol)
        out["broker_symbol"] = str(broker_symbol or "")
        if not broker_symbol:
            out["status"] = "unmapped"
            out["reason"] = "symbol not tradable at broker"
            return out

        try:
            if not self._mt5.symbol_select(broker_symbol, True):
                out["status"] = "error"
                out["reason"] = "symbol_select failed"
                return out
            symbol_info = self._mt5.symbol_info(broker_symbol)
            tick = self._mt5.symbol_info_tick(broker_symbol)
            account = self._mt5.account_info()
            if symbol_info is None or tick is None or account is None:
                out["status"] = "error"
                out["reason"] = "symbol info/tick/account unavailable"
                return out

            direction = str(getattr(signal, "direction", "") or "").lower()
            if direction not in ("long", "short"):
                out["status"] = "error"
                out["reason"] = "invalid signal direction"
                return out
            is_long = direction == "long"
            order_type = self._mt5.ORDER_TYPE_BUY if is_long else self._mt5.ORDER_TYPE_SELL
            ask = float(getattr(tick, "ask", 0.0) or 0.0)
            bid = float(getattr(tick, "bid", 0.0) or 0.0)
            price = self._price_round(float(ask if is_long else bid), symbol_info)
            point = float(getattr(symbol_info, "point", 0.0) or 0.0)
            sl0 = self._price_round(float(getattr(signal, "stop_loss", 0.0) or 0.0), symbol_info)
            tp20 = self._price_round(float(getattr(signal, "take_profit_2", 0.0) or 0.0), symbol_info)
            rr0 = _safe_float(getattr(signal, "risk_reward", 0.0), 0.0)
            mid = (ask + bid) / 2.0 if (ask > 0 and bid > 0) else float(price or 0.0)
            spread = max(0.0, ask - bid) if (ask > 0 and bid > 0) else 0.0
            spread_pct = ((spread / mid) * 100.0) if mid > 0 else 0.0

            st = self.status()
            account_key = ""
            try:
                acct_login = int(getattr(account, "login", 0) or 0)
                acct_server = str(st.get("account_server", "") or "")
                if acct_login and acct_server:
                    account_key = f"{acct_server}|{acct_login}"
            except Exception:
                account_key = ""

            adaptive_plan = mt5_adaptive_trade_planner.plan_execution(
                signal=signal,
                account_key=account_key,
                broker_symbol=broker_symbol,
                execution_price=float(price),
                bid=float(bid),
                ask=float(ask),
                point=float(point),
                source=str(source or ""),
                neural_prob=self._get_neural_prob(signal, source),
            )
            adaptive_plan = self._apply_preview_scenario_to_adaptive_plan(
                scenario=scenario,
                adaptive_plan=adaptive_plan,
                direction=direction,
                point=float(point),
                price=float(price),
                rr_floor=float(getattr(config, "MT5_ADAPTIVE_EXECUTION_RR_MIN", 1.2)),
                rr_cap=float(getattr(config, "MT5_ADAPTIVE_EXECUTION_RR_MAX", 2.8)),
                stop_scale_lo=float(getattr(config, "MT5_ADAPTIVE_EXECUTION_STOP_SCALE_MIN", 0.85)),
                stop_scale_hi=float(getattr(config, "MT5_ADAPTIVE_EXECUTION_STOP_SCALE_MAX", 1.35)),
                size_lo=float(getattr(config, "MT5_ADAPTIVE_EXECUTION_SIZE_MIN", 0.70)),
                size_hi=float(getattr(config, "MT5_ADAPTIVE_EXECUTION_SIZE_MAX", 1.10)),
            )

            final_entry = float(price)
            final_sl = float(sl0)
            final_tp1 = _safe_float(getattr(signal, "take_profit_1", 0.0), 0.0)
            final_tp2 = _safe_float(tp20, 0.0)
            final_tp3 = _safe_float(getattr(signal, "take_profit_3", 0.0), 0.0)
            rr_target = float(rr0)
            adaptive_size_mult = 1.0
            adaptive_meta = {}
            if adaptive_plan and bool(adaptive_plan.ok):
                adaptive_size_mult = max(0.25, float(adaptive_plan.size_multiplier or 1.0))
                adaptive_meta = adaptive_plan.to_dict()
                plan_factors = dict(getattr(adaptive_plan, "factors", {}) or {})
                if bool(plan_factors.get("direction_blocked")):
                    out["direction_blocked"] = True
                    out["direction_block_reason"] = str(
                        plan_factors.get("direction_block_reason")
                        or "adaptive directional guard would block this setup"
                    )
                if bool(adaptive_plan.applied):
                    final_entry = float(adaptive_plan.entry or final_entry)
                    final_sl = float(adaptive_plan.stop_loss or final_sl)
                    final_tp1 = float(adaptive_plan.take_profit_1 or final_tp1)
                    final_tp2 = float(adaptive_plan.take_profit_2 or final_tp2)
                    final_tp3 = float(adaptive_plan.take_profit_3 or final_tp3)
                    rr_target = float(adaptive_plan.rr_target or rr_target)
            final_entry = self._price_round(final_entry, symbol_info)
            final_sl = self._price_round(final_sl, symbol_info)
            final_tp2 = self._price_round(final_tp2, symbol_info) if final_tp2 > 0 else final_tp2
            if final_tp1 > 0:
                final_tp1 = self._price_round(final_tp1, symbol_info)
            if final_tp3 > 0:
                final_tp3 = self._price_round(final_tp3, symbol_info)

            vm_in = 1.0 if volume_multiplier is None else max(0.05, float(volume_multiplier))
            vm_final = max(0.05, float(vm_in) * float(adaptive_size_mult) * float((conf_soft_info or {}).get("size_multiplier", 1.0) or 1.0))
            desired_volume = self._normalize_volume(float(config.MT5_LOT_SIZE) * vm_final, symbol_info)
            free_margin = float(getattr(account, "margin_free", 0.0) or 0.0)
            margin_budget_pct, margin_budget_reason = self._mt5_margin_budget_pct_for_signal(
                signal,
                source,
                broker_symbol=broker_symbol,
                signal_symbol=sig_symbol,
            )
            fitted_volume, margin_required, fit_reason = self._fit_volume_by_margin_budget(
                order_type=order_type,
                broker_symbol=broker_symbol,
                symbol_info=symbol_info,
                price=float(final_entry),
                desired_volume=float(desired_volume),
                free_margin=float(free_margin),
                max_usage_pct=float(margin_budget_pct),
                min_left=float(config.MT5_MIN_FREE_MARGIN_AFTER_TRADE),
            )

            out["base"] = {
                "entry": float(price),
                "stop_loss": float(sl0),
                "take_profit_2": float(tp20),
                "risk_reward": float(rr0),
                "lot_size": float(config.MT5_LOT_SIZE),
            }
            out["adaptive"] = adaptive_meta or {
                "ok": False,
                "applied": False,
                "reason": "planner_unavailable",
            }
            out["adaptive"]["scenario"] = out.get("scenario")
            out["execution"] = {
                "direction": direction,
                "entry": float(final_entry),
                "stop_loss": float(final_sl),
                "take_profit_1": (float(final_tp1) if final_tp1 else None),
                "take_profit_2": (float(final_tp2) if final_tp2 else None),
                "take_profit_3": (float(final_tp3) if final_tp3 else None),
                "risk_reward": float(rr_target),
                "volume_multiplier_input": float(vm_in),
                "volume_multiplier_final": float(vm_final),
                "desired_volume": (float(desired_volume) if desired_volume is not None else None),
                "fitted_volume": (float(fitted_volume) if fitted_volume is not None else None),
            }
            out["margin"] = {
                "free_margin": float(free_margin),
                "budget_pct": float(margin_budget_pct),
                "budget_reason": str(margin_budget_reason),
                "required": (None if margin_required is None else float(margin_required)),
                "fit_reason": str(fit_reason or ""),
            }
            out["account"] = {
                "account_key": account_key,
                "login": int(getattr(account, "login", 0) or 0),
                "server": str(st.get("account_server", "") or ""),
                "balance": _safe_float(getattr(account, "balance", 0.0), 0.0),
                "equity": _safe_float(getattr(account, "equity", 0.0), 0.0),
                "free_margin": float(free_margin),
            }
            out["market"] = {
                "bid": float(bid),
                "ask": float(ask),
                "spread": float(spread),
                "spread_pct": float(spread_pct),
                "point": float(point),
            }
            out["ok"] = True
            out["status"] = "ok" if fitted_volume is not None else "margin_filtered"
            out["reason"] = str(fit_reason or "ok")
            return out
        except Exception as e:
            out["status"] = "error"
            out["reason"] = f"preview exception: {e}"
            out["error"] = str(e)
            return out

    def modify_position_sltp(
        self,
        *,
        broker_symbol: str,
        position_ticket: int,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        source: str = "autopilot",
    ) -> MT5ExecutionResult:
        """Modify SL/TP of an open position (TRADE_ACTION_SLTP)."""
        if not self.enabled:
            return MT5ExecutionResult(False, "disabled", "MT5 disabled", broker_symbol=broker_symbol)
        ok, state = self._ensure_connection()
        if not ok:
            return MT5ExecutionResult(False, "error", state, broker_symbol=broker_symbol)
        try:
            if not self._mt5.symbol_select(broker_symbol, True):
                return MT5ExecutionResult(False, "error", "symbol_select failed", broker_symbol=broker_symbol)
            symbol_info = self._mt5.symbol_info(broker_symbol)
            if symbol_info is None:
                return MT5ExecutionResult(False, "error", "symbol info unavailable", broker_symbol=broker_symbol)

            now = time.time()
            if broker_symbol in self._market_closed_cache:
                if now - self._market_closed_cache[broker_symbol] < 300:
                    return MT5ExecutionResult(False, "market_closed", f"{broker_symbol} market closed (cached)", broker_symbol=broker_symbol, ticket=int(position_ticket))

            req = {
                "action": getattr(self._mt5, "TRADE_ACTION_SLTP", 6),
                "symbol": broker_symbol,
                "position": int(position_ticket),
                "magic": int(config.MT5_MAGIC),
                "comment": f"{config.MT5_COMMENT_PREFIX}:pm:{source}"[:31],
            }
            if sl is not None:
                req["sl"] = self._price_round(float(sl), symbol_info)
            if tp is not None:
                req["tp"] = self._price_round(float(tp), symbol_info)
            if self.dry_run:
                return MT5ExecutionResult(
                    True,
                    "dry_run_modify",
                    f"dry-run modify sltp: {req}",
                    broker_symbol=broker_symbol,
                    ticket=int(position_ticket),
                )
            with self._lock:
                if hasattr(self._conn.root, "exposed_order_send"):
                    res = self._conn.root.exposed_order_send(req)
                else:
                    res = self._mt5.order_send(req)
                if res is None:
                    try:
                        if hasattr(self._conn.root, "exposed_last_error"):
                            err = self._conn.root.exposed_last_error()
                        else:
                            err = self._mt5.last_error()
                    except Exception:
                        err = None
                    return MT5ExecutionResult(
                        False,
                        "error",
                        f"order_send returned None | last_error={_fmt_mt5_last_error(err)}",
                        broker_symbol=broker_symbol,
                        ticket=int(position_ticket),
                    )
            
            retcode = int(getattr(res, "retcode", -1))
            
            if retcode == 10018 or retcode == getattr(self._mt5, "TRADE_RETCODE_MARKET_CLOSED", 10018):
                self._market_closed_cache[broker_symbol] = time.time()
                return MT5ExecutionResult(False, "market_closed", f"market closed retcode={retcode}", broker_symbol=broker_symbol, ticket=int(position_ticket), retcode=retcode)

            done_codes = {
                int(getattr(self._mt5, "TRADE_RETCODE_DONE", 10009)),
                int(getattr(self._mt5, "TRADE_RETCODE_PLACED", 10008)),
                int(getattr(self._mt5, "TRADE_RETCODE_DONE_PARTIAL", 10010)),
            }
            if retcode in done_codes:
                return MT5ExecutionResult(True, "modified", f"sltp updated retcode={retcode}", broker_symbol=broker_symbol, ticket=int(position_ticket), retcode=retcode)
            return MT5ExecutionResult(False, "rejected", _retcode_detail(retcode), broker_symbol=broker_symbol, ticket=int(position_ticket), retcode=retcode)
        except Exception as e:
            return MT5ExecutionResult(False, "error", f"modify exception: {e}", broker_symbol=broker_symbol, ticket=int(position_ticket))

    def close_position_partial(
        self,
        *,
        broker_symbol: str,
        position_ticket: int,
        position_type: str,
        position_volume: float,
        close_volume: float,
        source: str = "autopilot",
    ) -> MT5ExecutionResult:
        """Close part/all of a live position by opposite market deal."""
        if not self.enabled:
            return MT5ExecutionResult(False, "disabled", "MT5 disabled", broker_symbol=broker_symbol)
        ok, state = self._ensure_connection()
        if not ok:
            return MT5ExecutionResult(False, "error", state, broker_symbol=broker_symbol)
        try:
            if not self._mt5.symbol_select(broker_symbol, True):
                return MT5ExecutionResult(False, "error", "symbol_select failed", broker_symbol=broker_symbol)
            symbol_info = self._mt5.symbol_info(broker_symbol)
            tick = self._mt5.symbol_info_tick(broker_symbol)
            if symbol_info is None or tick is None:
                return MT5ExecutionResult(False, "error", "symbol info/tick unavailable", broker_symbol=broker_symbol)

            pos_vol = max(0.0, float(position_volume or 0.0))
            cv = max(0.0, float(close_volume or 0.0))
            if pos_vol <= 0 or cv <= 0:
                return MT5ExecutionResult(False, "error", "invalid close volume", broker_symbol=broker_symbol, ticket=int(position_ticket))
            cv = min(cv, pos_vol)
            cv = self._normalize_volume(cv, symbol_info)
            if cv <= 0:
                return MT5ExecutionResult(False, "error", "close volume normalized to zero", broker_symbol=broker_symbol, ticket=int(position_ticket))

            now = time.time()
            if broker_symbol in self._market_closed_cache:
                if now - self._market_closed_cache[broker_symbol] < 300:
                    return MT5ExecutionResult(False, "market_closed", f"{broker_symbol} market closed (cached)", broker_symbol=broker_symbol, ticket=int(position_ticket))

            # Determine position direction by querying live MT5 position (most reliable).
            # The 'position_type' string arg is a hint only and may be a raw int like "0".
            # POSITION_TYPE_BUY == 0, POSITION_TYPE_SELL == 1.
            is_buy_position = None
            try:
                live_positions = self._mt5.positions_get(ticket=int(position_ticket)) or []
                if live_positions:
                    live_pos_type = int(getattr(live_positions[0], "type", -1))
                    is_buy_position = (live_pos_type == int(getattr(self._mt5, "POSITION_TYPE_BUY", 0)))
            except Exception:
                pass
            if is_buy_position is None:
                # Fallback to string hint if live lookup fails
                is_buy_position = str(position_type or "").lower() in {"buy", "long", "0", "0.0"}
            order_type = self._mt5.ORDER_TYPE_SELL if is_buy_position else self._mt5.ORDER_TYPE_BUY
            price = float(getattr(tick, "bid" if is_buy_position else "ask", 0.0) or 0.0)
            
            # If tick price is 0.0, the market is closed or off-quotes. Sending 0.0 yields 10013 Invalid Request.
            if price <= 0.0:
                self._market_closed_cache[broker_symbol] = time.time()
                return MT5ExecutionResult(False, "market_closed", f"market closed / off quotes (price 0.0)", broker_symbol=broker_symbol, ticket=int(position_ticket), retcode=10018)

            price = self._price_round(price, symbol_info)
            req = {
                "action": self._mt5.TRADE_ACTION_DEAL,
                "symbol": broker_symbol,
                "position": int(position_ticket),
                "volume": float(cv),
                "type": order_type,
                "price": price,
                "deviation": int(config.MT5_DEVIATION),
                "magic": int(config.MT5_MAGIC),
                "comment": f"{config.MT5_COMMENT_PREFIX}:pc:{source}"[:31],
                "type_time": self._mt5.ORDER_TIME_GTC,
                "type_filling": self._pick_filling_mode(symbol_info),
            }
            if self.dry_run:
                return MT5ExecutionResult(
                    True,
                    "dry_run_partial_close",
                    f"dry-run partial close: {req}",
                    broker_symbol=broker_symbol,
                    ticket=int(position_ticket),
                    volume=float(cv),
                    dry_run=True,
                )
            def _send_req(rq: dict):
                with self._lock:
                    if hasattr(self._conn.root, "exposed_order_send"):
                        return self._conn.root.exposed_order_send(rq)
                    return self._mt5.order_send(rq)

            def _parse_res(res_obj):
                if res_obj is None:
                    return None, -1, ""
                rc = int(getattr(res_obj, "retcode", -1))
                bcomment = str(_mt5_attr(res_obj, "comment", "") or "").strip()
                return res_obj, rc, bcomment

            done_codes = {
                int(getattr(self._mt5, "TRADE_RETCODE_DONE", 10009)),
                int(getattr(self._mt5, "TRADE_RETCODE_PLACED", 10008)),
                int(getattr(self._mt5, "TRADE_RETCODE_DONE_PARTIAL", 10010)),
            }
            invalid_req_codes = {
                10013,  # invalid request
                int(getattr(self._mt5, "TRADE_RETCODE_INVALID_FILL", 10030)),
            }

            res, retcode, broker_comment = _parse_res(_send_req(req))
            if res is None:
                try:
                    if hasattr(self._conn.root, "exposed_last_error"):
                        err = self._conn.root.exposed_last_error()
                    else:
                        err = self._mt5.last_error()
                except Exception:
                    err = None
                return MT5ExecutionResult(
                    False,
                    "error",
                    f"order_send returned None | last_error={_fmt_mt5_last_error(err)}",
                    broker_symbol=broker_symbol,
                    ticket=int(position_ticket),
                )
            
            if retcode == 10018 or retcode == getattr(self._mt5, "TRADE_RETCODE_MARKET_CLOSED", 10018):
                self._market_closed_cache[broker_symbol] = time.time()
                return MT5ExecutionResult(False, "market_closed", f"market closed retcode={retcode}", broker_symbol=broker_symbol, ticket=int(position_ticket), retcode=retcode, volume=None)

            if retcode == 10013:
                logger.error(f"[MT5 DEEP DEBUG] 10013 Invalid Request. Request payload was: {req} | MT5 tick price: {price} | Order Type: {order_type} | Filling: {req.get('type_filling')}")

            if retcode in done_codes:
                return MT5ExecutionResult(True, "partial_closed", f"partial close accepted retcode={retcode}", broker_symbol=broker_symbol, ticket=int(position_ticket), retcode=retcode, volume=float(cv))

            # Some brokers reject close DEAL requests with extra fields or a specific filling mode.
            # Retry with a minimal request and alternate filling modes before failing.
            if retcode in invalid_req_codes:
                fill_candidates = []
                for cand in [req.get("type_filling"), getattr(self._mt5, "ORDER_FILLING_IOC", None), getattr(self._mt5, "ORDER_FILLING_FOK", None), getattr(self._mt5, "ORDER_FILLING_RETURN", None)]:
                    if cand is None:
                        continue
                    try:
                        iv = int(cand)
                    except Exception:
                        continue
                    if iv not in fill_candidates:
                        fill_candidates.append(iv)
                for tf in fill_candidates:
                    alt = {
                        "action": req["action"],
                        "symbol": req["symbol"],
                        "position": req["position"],
                        "volume": req["volume"],
                        "type": req["type"],
                        "price": req["price"],
                        "magic": req.get("magic", int(config.MT5_MAGIC)),
                        "type_filling": tf,
                    }
                    if "deviation" in req:
                        alt["deviation"] = req["deviation"]
                    if "comment" in req:
                        alt["comment"] = req["comment"]
                    if "type_time" in req:
                        alt["type_time"] = req["type_time"]
                    res2, rc2, bcomment2 = _parse_res(_send_req(alt))
                    if res2 is None:
                        continue
                    if rc2 in done_codes:
                        logger.info("[MT5] partial-close retry succeeded %s ticket=%s retcode=%s fill=%s", broker_symbol, int(position_ticket), rc2, tf)
                        return MT5ExecutionResult(True, "partial_closed", f"partial close accepted retcode={rc2}", broker_symbol=broker_symbol, ticket=int(position_ticket), retcode=rc2, volume=float(cv))
                    # Keep the latest reject if it is more specific.
                    res, retcode, broker_comment = res2, rc2, bcomment2
                    if rc2 not in invalid_req_codes:
                        break

            msg = _retcode_detail(retcode)
            if broker_comment and broker_comment.lower() not in msg.lower():
                msg = f"{msg} [{broker_comment}]"
            return MT5ExecutionResult(False, "rejected", msg, broker_symbol=broker_symbol, ticket=int(position_ticket), retcode=retcode, volume=None)
        except Exception as e:
            return MT5ExecutionResult(False, "error", f"partial close exception: {e}", broker_symbol=broker_symbol, ticket=int(position_ticket))

    def status(self) -> dict:
        base = {
            "enabled": self.enabled,
            "dry_run": self.dry_run,
            "micro_mode": bool(getattr(config, "MT5_MICRO_MODE_ENABLED", False)),
            "micro_single_position_only": bool(getattr(config, "MT5_MICRO_SINGLE_POSITION_ONLY", True)),
            "position_limits_bot_only": bool(getattr(config, "MT5_POSITION_LIMITS_BOT_ONLY", False)),
            "max_open_positions": int(getattr(config, "MT5_MAX_OPEN_POSITIONS", 5)),
            "max_positions_per_symbol": int(getattr(config, "MT5_MAX_POSITIONS_PER_SYMBOL", 1)),
            "micro_max_spread_pct": float(getattr(config, "MT5_MICRO_MAX_SPREAD_PCT", 0.15)),
            "micro_learner_enabled": self._micro_learner_enabled(),
            "available": self.available,
            "host": config.MT5_HOST,
            "port": int(config.MT5_PORT),
            "connected": False,
            "account_login": None,
            "account_server": None,
            "balance": None,
            "equity": None,
            "margin_free": None,
            "currency": None,
            "leverage": None,
            "symbols": len(self._symbols_cache),
            "micro_whitelist_total": 0,
            "micro_whitelist_allowed": 0,
            "micro_whitelist_denied": 0,
            "micro_balance_bucket": None,
            "error": "",
        }
        if not self.enabled:
            return base

        ok, message = self._ensure_connection()
        if not ok:
            base["error"] = message
            return base

        base["connected"] = True
        try:
            acct = self._mt5.account_info()
            if acct is not None:
                base["account_login"] = int(getattr(acct, "login", 0))
                base["account_server"] = str(getattr(acct, "server", ""))
                base["balance"] = self._safe_float(getattr(acct, "balance", None), 0.0)
                base["equity"] = self._safe_float(getattr(acct, "equity", None), 0.0)
                base["margin_free"] = self._safe_float(getattr(acct, "margin_free", None), 0.0)
                base["currency"] = str(getattr(acct, "currency", "") or "")
                try:
                    base["leverage"] = int(getattr(acct, "leverage", 0) or 0)
                except Exception:
                    base["leverage"] = None
                micro = self.micro_whitelist_status(acct)
                base["micro_whitelist_total"] = int(micro.get("total_symbols", 0) or 0)
                base["micro_whitelist_allowed"] = int(micro.get("allowed", 0) or 0)
                base["micro_whitelist_denied"] = int(micro.get("denied", 0) or 0)
                base["micro_balance_bucket"] = micro.get("balance_bucket")
        except Exception:
            pass
        base["symbols"] = len(self._get_symbols())
        return base

    @staticmethod
    def _position_type_label(mt5_obj, ptype: int) -> str:
        try:
            if int(ptype) == int(getattr(mt5_obj, "POSITION_TYPE_BUY", 0)):
                return "buy"
            if int(ptype) == int(getattr(mt5_obj, "POSITION_TYPE_SELL", 1)):
                return "sell"
        except Exception:
            pass
        return str(ptype)

    @staticmethod
    def _order_type_label(mt5_obj, otype: int) -> str:
        labels = {
            int(getattr(mt5_obj, "ORDER_TYPE_BUY", 0)): "buy",
            int(getattr(mt5_obj, "ORDER_TYPE_SELL", 1)): "sell",
            int(getattr(mt5_obj, "ORDER_TYPE_BUY_LIMIT", 2)): "buy_limit",
            int(getattr(mt5_obj, "ORDER_TYPE_SELL_LIMIT", 3)): "sell_limit",
            int(getattr(mt5_obj, "ORDER_TYPE_BUY_STOP", 4)): "buy_stop",
            int(getattr(mt5_obj, "ORDER_TYPE_SELL_STOP", 5)): "sell_stop",
            int(getattr(mt5_obj, "ORDER_TYPE_BUY_STOP_LIMIT", 6)): "buy_stop_limit",
            int(getattr(mt5_obj, "ORDER_TYPE_SELL_STOP_LIMIT", 7)): "sell_stop_limit",
        }
        return labels.get(int(otype), str(otype))

    @staticmethod
    def _deal_entry_is_exit(mt5_obj, entry: int) -> bool:
        try:
            exit_codes = {
                int(getattr(mt5_obj, "DEAL_ENTRY_OUT", 1)),
                int(getattr(mt5_obj, "DEAL_ENTRY_OUT_BY", 3)),
                int(getattr(mt5_obj, "DEAL_ENTRY_INOUT", 2)),
            }
            return int(entry) in exit_codes
        except Exception:
            return False

    @staticmethod
    def _deal_reason_label(mt5_obj, reason: int) -> str:
        try:
            mapping = {
                int(getattr(mt5_obj, "DEAL_REASON_TP", -1)): "TP",
                int(getattr(mt5_obj, "DEAL_REASON_SL", -1)): "SL",
                int(getattr(mt5_obj, "DEAL_REASON_SO", -1)): "STOP_OUT",
                int(getattr(mt5_obj, "DEAL_REASON_CLIENT", -1)): "MANUAL",
                int(getattr(mt5_obj, "DEAL_REASON_MOBILE", -1)): "MANUAL",
                int(getattr(mt5_obj, "DEAL_REASON_WEB", -1)): "MANUAL",
                int(getattr(mt5_obj, "DEAL_REASON_EXPERT", -1)): "EA",
            }
            return mapping.get(int(reason), str(int(reason)))
        except Exception:
            return str(reason)

    @staticmethod
    def _deal_type_label(mt5_obj, dtype: int) -> str:
        try:
            mapping = {
                int(getattr(mt5_obj, "DEAL_TYPE_BUY", 0)): "buy",
                int(getattr(mt5_obj, "DEAL_TYPE_SELL", 1)): "sell",
            }
            return mapping.get(int(dtype), str(int(dtype)))
        except Exception:
            return str(dtype)

    def open_positions_snapshot(self, signal_symbol: str = "", limit: int = 20) -> dict:
        """
        Snapshot live MT5 open positions and pending orders.
        If signal_symbol is provided, attempts broker-symbol resolution first.
        """
        out = {
            "enabled": self.enabled,
            "available": self.available,
            "connected": False,
            "requested_symbol": str(signal_symbol or "").upper(),
            "resolved_symbol": "",
            "positions": [],
            "orders": [],
            "error": "",
            "account_login": None,
            "account_server": None,
            "free_margin": None,
        }
        if not self.enabled:
            out["error"] = "MT5 disabled"
            return out

        ok, msg = self._ensure_connection()
        if not ok:
            out["error"] = msg
            return out
        out["connected"] = True

        try:
            acct = self._mt5.account_info()
            if acct is not None:
                out["account_login"] = int(getattr(acct, "login", 0) or 0)
                out["account_server"] = str(getattr(acct, "server", "") or "")
                out["free_margin"] = float(getattr(acct, "margin_free", 0.0) or 0.0)
        except Exception:
            pass

        req_symbol = out["requested_symbol"]
        broker_symbol = ""
        if req_symbol:
            broker_symbol = self.resolve_symbol(req_symbol) or ""
            if broker_symbol:
                out["resolved_symbol"] = broker_symbol

        try:
            raw_positions = (
                (self._mt5.positions_get(symbol=broker_symbol) if broker_symbol else self._mt5.positions_get())
                or []
            )
        except Exception as e:
            raw_positions = []
            out["error"] = f"positions_get failed: {e}"

        try:
            raw_orders = (
                (self._mt5.orders_get(symbol=broker_symbol) if broker_symbol else self._mt5.orders_get())
                or []
            )
        except Exception:
            raw_orders = []

        if req_symbol and (not broker_symbol):
            # Fallback normalized matching against all positions/orders.
            req_key = _normalize_symbol_key(req_symbol)
            raw_positions = [
                p for p in raw_positions
                if _normalize_symbol_key(str(getattr(p, "symbol", "") or "")) == req_key
            ]
            raw_orders = [
                o for o in raw_orders
                if _normalize_symbol_key(str(getattr(o, "symbol", "") or "")) == req_key
            ]

        max_n = max(1, int(limit))
        tick_cache: dict[str, tuple[float, float, float, float]] = {}

        def _tick_stats(symbol_name: str) -> tuple[float, float, float, float]:
            key = str(symbol_name or "")
            if not key:
                return 0.0, 0.0, 0.0, 0.0
            if key in tick_cache:
                return tick_cache[key]
            ask = bid = spread = spread_pct = 0.0
            try:
                t = self._mt5.symbol_info_tick(key)
                if t is not None:
                    ask = float(getattr(t, "ask", 0.0) or 0.0)
                    bid = float(getattr(t, "bid", 0.0) or 0.0)
                    if ask > 0 and bid > 0:
                        spread = max(0.0, ask - bid)
                        mid = (ask + bid) / 2.0
                        spread_pct = ((spread / mid) * 100.0) if mid > 0 else 0.0
            except Exception:
                pass
            tick_cache[key] = (bid, ask, spread, spread_pct)
            return tick_cache[key]

        for p in list(raw_positions)[:max_n]:
            try:
                sym = str(getattr(p, "symbol", "") or "")
                bid, ask, spread, spread_pct = _tick_stats(sym)
                out["positions"].append(
                    {
                        "ticket": int(getattr(p, "ticket", 0) or 0),
                        "symbol": sym,
                        "type": self._position_type_label(self._mt5, int(getattr(p, "type", -1) or -1)),
                        "volume": float(getattr(p, "volume", 0.0) or 0.0),
                        "price_open": float(getattr(p, "price_open", 0.0) or 0.0),
                        "price_current": float(getattr(p, "price_current", 0.0) or 0.0),
                        "sl": float(getattr(p, "sl", 0.0) or 0.0),
                        "tp": float(getattr(p, "tp", 0.0) or 0.0),
                        "profit": float(getattr(p, "profit", 0.0) or 0.0),
                        "bid": bid,
                        "ask": ask,
                        "spread": spread,
                        "spread_pct": spread_pct,
                        "time": int(getattr(p, "time", 0) or 0),
                        "time_msc": int(getattr(p, "time_msc", 0) or 0),
                        "magic": int(getattr(p, "magic", 0) or 0),
                        "comment": str(getattr(p, "comment", "") or ""),
                    }
                )
            except Exception:
                continue

        for o in list(raw_orders)[:max_n]:
            try:
                out["orders"].append(
                    {
                        "ticket": int(getattr(o, "ticket", 0) or 0),
                        "symbol": str(getattr(o, "symbol", "") or ""),
                        "type": self._order_type_label(self._mt5, int(getattr(o, "type", -1) or -1)),
                        "volume": float(getattr(o, "volume_current", getattr(o, "volume_initial", 0.0)) or 0.0),
                        "price_open": float(getattr(o, "price_open", 0.0) or 0.0),
                        "sl": float(getattr(o, "sl", 0.0) or 0.0),
                        "tp": float(getattr(o, "tp", 0.0) or 0.0),
                        "magic": int(getattr(o, "magic", 0) or 0),
                        "comment": str(getattr(o, "comment", "") or ""),
                    }
                )
            except Exception:
                continue

        return out

    def closed_trades_snapshot(self, signal_symbol: str = "", hours: int = 24, limit: int = 10) -> dict:
        """
        Snapshot recently closed MT5 trades using history deals.
        Aggregates exit deals by position_id and infers close reason (TP/SL/etc) when available.
        """
        out = {
            "enabled": self.enabled,
            "available": self.available,
            "connected": False,
            "requested_symbol": str(signal_symbol or "").upper(),
            "resolved_symbol": "",
            "hours": max(1, int(hours or 24)),
            "closed_trades": [],
            "error": "",
            "account_login": None,
            "account_server": None,
        }
        ok, msg = self._ensure_connection()
        if not ok:
            out["error"] = msg
            return out
        out["connected"] = True

        try:
            acct = self._mt5.account_info()
            if acct is not None:
                out["account_login"] = int(getattr(acct, "login", 0) or 0)
                out["account_server"] = str(getattr(acct, "server", "") or "")
        except Exception:
            pass

        req_symbol = out["requested_symbol"]
        broker_symbol = ""
        if req_symbol:
            broker_symbol = self.resolve_symbol(req_symbol) or ""
            if broker_symbol:
                out["resolved_symbol"] = broker_symbol

        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=out["hours"])
        try:
            raw_deals, query_mode = self._history_deals_get_robust(start, now)
            out["history_query_mode"] = query_mode
        except Exception as e:
            out["error"] = f"history_deals_get failed: {e}"
            return out

        req_key = _normalize_symbol_key(req_symbol) if req_symbol else ""
        broker_key = _normalize_symbol_key(broker_symbol) if broker_symbol else ""
        grouped: dict[int, dict] = {}

        for d in raw_deals:
            try:
                entry_raw = _mt5_attr(d, "entry", None)
                entry = int(entry_raw) if entry_raw is not None else -1
                if not self._deal_entry_is_exit(self._mt5, entry):
                    continue
                symbol = str(_mt5_attr(d, "symbol", "") or "")
                if req_key:
                    sym_key = _normalize_symbol_key(symbol)
                    if sym_key not in {req_key, broker_key}:
                        continue
                close_ts = int(_mt5_attr(d, "time", 0) or 0)
                position_id = int(_mt5_attr(d, "position_id", 0) or 0)
                deal_ticket = int(_mt5_attr(d, "ticket", 0) or 0)
                group_id = position_id if position_id > 0 else (deal_ticket if deal_ticket > 0 else close_ts)
                deal_profit = float(_mt5_attr(d, "profit", 0.0) or 0.0)
                deal_swap = float(_mt5_attr(d, "swap", 0.0) or 0.0)
                deal_commission = float(_mt5_attr(d, "commission", 0.0) or 0.0)
                pnl = deal_profit + deal_swap + deal_commission
                rec = grouped.setdefault(
                    int(group_id),
                    {
                        "position_id": int(position_id) if position_id > 0 else None,
                        "symbol": symbol,
                        "pnl": 0.0,
                        "profit": 0.0,
                        "swap": 0.0,
                        "commission": 0.0,
                        "close_time": close_ts,
                        "close_price": float(_mt5_attr(d, "price", 0.0) or 0.0),
                        "volume": float(_mt5_attr(d, "volume", 0.0) or 0.0),
                        "reason": self._deal_reason_label(
                            self._mt5,
                            (int(_mt5_attr(d, "reason", -1)) if _mt5_attr(d, "reason", None) is not None else -1),
                        ),
                        "deal_type": self._deal_type_label(
                            self._mt5,
                            (int(_mt5_attr(d, "type", -1)) if _mt5_attr(d, "type", None) is not None else -1),
                        ),
                        "comment": str(_mt5_attr(d, "comment", "") or ""),
                        "deals": 0,
                    },
                )
                rec["pnl"] = float(rec.get("pnl", 0.0) or 0.0) + pnl
                rec["profit"] = float(rec.get("profit", 0.0) or 0.0) + deal_profit
                rec["swap"] = float(rec.get("swap", 0.0) or 0.0) + deal_swap
                rec["commission"] = float(rec.get("commission", 0.0) or 0.0) + deal_commission
                rec["deals"] = int(rec.get("deals", 0) or 0) + 1
                if close_ts >= int(rec.get("close_time", 0) or 0):
                    rec["close_time"] = close_ts
                    rec["close_price"] = float(_mt5_attr(d, "price", 0.0) or 0.0)
                    reason_raw = _mt5_attr(d, "reason", None)
                    rec["reason"] = self._deal_reason_label(self._mt5, int(reason_raw) if reason_raw is not None else -1)
                    dtype_raw = _mt5_attr(d, "type", None)
                    rec["deal_type"] = self._deal_type_label(self._mt5, int(dtype_raw) if dtype_raw is not None else -1)
                    rec["volume"] = float(_mt5_attr(d, "volume", 0.0) or 0.0)
                    rec["comment"] = str(_mt5_attr(d, "comment", "") or "")
            except Exception:
                continue

        rows = sorted(grouped.values(), key=lambda x: int(x.get("close_time", 0) or 0), reverse=True)
        for rec in rows[: max(1, int(limit or 10))]:
            try:
                ts = int(rec.get("close_time", 0) or 0)
                rec = dict(rec)
                rec["closed_at_utc"] = (
                    datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    if ts > 0 else ""
                )
                out["closed_trades"].append(rec)
            except Exception:
                continue
        return out

    def list_symbols(self, limit: int = 200) -> list[str]:
        symbols = self._get_symbols()
        if limit and limit > 0:
            return symbols[:limit]
        return symbols

    @staticmethod
    def _affordable_symbol_category(symbol: str) -> str:
        sym = str(symbol or "").upper()
        if not sym:
            return ""
        crypto_bases = {
            "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "BNB", "LTC", "BCH",
            "DOT", "LINK", "TRX", "UNI", "ATOM", "POL", "HBAR", "PEPE", "SHIB", "PAXG",
        }
        fx_ccy = {"EUR", "GBP", "USD", "AUD", "NZD", "CAD", "CHF", "JPY"}
        index_names = {
            "US500", "USTEC", "US30", "US2000", "JP225", "UK100", "AUS200", "HK50", "CHINA50", "STOXX50", "F40", "DE40",
        }
        if sym in index_names or any(sym.startswith(x) for x in index_names):
            return "index"
        if sym.startswith(("XAU", "XAG", "XPT", "XPD")):
            return "metal"
        if sym.endswith("USD") and sym[:-3] in crypto_bases:
            return "crypto"
        if len(sym) >= 6 and sym[:3] in fx_ccy and sym[3:6] in fx_ccy:
            return "fx"
        return ""

    def affordable_symbols_snapshot(self, category: str = "all", limit: int = 20, only_ok: bool = False) -> dict:
        """
        Real-time snapshot of broker symbols that are affordable at minimum lot size
        under current micro-account constraints (free margin, margin-usage budget,
        min free margin after trade, spread guard) plus current symbol allow/block policy.
        """
        cat = str(category or "all").strip().lower()
        if cat in {"forex"}:
            cat = "fx"
        if cat in {"metals"}:
            cat = "metal"
        if cat in {"indices"}:
            cat = "index"
        if cat not in {"all", "crypto", "fx", "metal", "index"}:
            cat = "all"

        top_n = max(1, min(100, int(limit or 20)))
        out = {
            "enabled": self.enabled,
            "available": self.available,
            "connected": False,
            "category": cat,
            "limit": top_n,
            "only_ok": bool(only_ok),
            "account_login": None,
            "account_server": None,
            "currency": None,
            "balance": None,
            "equity": None,
            "free_margin": None,
            "margin_budget_pct": float(getattr(config, "MT5_MAX_MARGIN_USAGE_PCT", 35.0)),
            "margin_budget_reason": "global",
            "allowed_margin": None,
            "min_free_margin_after_trade": float(getattr(config, "MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1.0)),
            "micro_max_spread_pct": float(getattr(config, "MT5_MICRO_MAX_SPREAD_PCT", 0.15)),
            "symbol_policy": {
                "allow_count": len(self._allow_symbols),
                "block_count": len(self._block_symbols),
                "allowlist_active": bool(self._allow_symbols),
            },
            "summary": {},
            "rows": [],
            "error": "",
        }
        if not self.enabled:
            out["error"] = "MT5 disabled"
            return out
        ok, msg = self._ensure_connection()
        if not ok:
            out["error"] = msg
            return out
        out["connected"] = True

        acct = None
        try:
            acct = self._mt5.account_info()
        except Exception:
            acct = None
        if acct is None:
            out["error"] = "account_info unavailable"
            return out

        try:
            out["account_login"] = int(getattr(acct, "login", 0) or 0)
            out["account_server"] = str(getattr(acct, "server", "") or "")
            out["currency"] = str(getattr(acct, "currency", "") or "")
            out["balance"] = self._safe_float(getattr(acct, "balance", None), 0.0)
            out["equity"] = self._safe_float(getattr(acct, "equity", None), 0.0)
            out["free_margin"] = self._safe_float(getattr(acct, "margin_free", None), 0.0)
        except Exception:
            pass
        free_margin = self._safe_float(out.get("free_margin"), 0.0)
        default_budget_pct, default_budget_reason = self._mt5_margin_budget_pct_for_signal(
            None,
            (cat if cat in {"fx", "crypto", "metal", "index"} else ""),
            signal_symbol="",
        )
        out["margin_budget_pct"] = float(default_budget_pct)
        out["margin_budget_reason"] = str(default_budget_reason)
        budget_pct = max(1.0, self._safe_float(out.get("margin_budget_pct"), 35.0))
        allowed_margin = max(0.0, free_margin * (budget_pct / 100.0))
        out["allowed_margin"] = round(allowed_margin, 4)
        min_free_after = max(0.0, self._safe_float(out.get("min_free_margin_after_trade"), 1.0))
        max_spread_pct = max(0.0, self._safe_float(out.get("micro_max_spread_pct"), 0.15))

        try:
            broker_symbols = list(self._get_symbols(force_refresh=True) or [])
        except Exception:
            broker_symbols = []
        if not broker_symbols:
            out["error"] = "no broker symbols loaded"
            return out

        rows = []
        checked = 0
        recognized = 0
        scan_cap = 1500 if cat == "all" else 800
        for sym in broker_symbols:
            su = str(sym or "").upper()
            kind = self._affordable_symbol_category(su)
            if not kind:
                continue
            if cat != "all" and kind != cat:
                continue
            recognized += 1
            if checked >= scan_cap:
                break
            checked += 1
            try:
                # Ensure symbol is selected so tick/margin calc is available.
                try:
                    self._mt5.symbol_select(su, True)
                except Exception:
                    pass
                info = self._mt5.symbol_info(su)
                tick = self._mt5.symbol_info_tick(su)
                if info is None or tick is None:
                    continue

                vol_min = float(getattr(info, "volume_min", 0.0) or 0.0)
                vol_step = float(getattr(info, "volume_step", 0.0) or 0.0)
                ask = float(getattr(tick, "ask", 0.0) or 0.0)
                bid = float(getattr(tick, "bid", 0.0) or 0.0)
                if vol_min <= 0 or ask <= 0 or bid <= 0:
                    continue
                mid = (ask + bid) / 2.0
                spread_pct = ((ask - bid) / mid * 100.0) if mid > 0 else None
                trade_mode = int(getattr(info, "trade_mode", 0) or 0)
                visible = bool(getattr(info, "visible", True))

                order_type_buy = int(getattr(self._mt5, "ORDER_TYPE_BUY", 0))
                order_type_sell = int(getattr(self._mt5, "ORDER_TYPE_SELL", 1))
                margin_req = None
                try:
                    margin_req = self._mt5.order_calc_margin(order_type_buy, su, float(vol_min), float(ask))
                    margin_req = (None if margin_req is None else float(margin_req))
                except Exception:
                    margin_req = None
                if margin_req is None:
                    try:
                        margin_req = self._mt5.order_calc_margin(order_type_sell, su, float(vol_min), float(bid))
                        margin_req = (None if margin_req is None else float(margin_req))
                    except Exception:
                        margin_req = None

                row_budget_pct, row_budget_reason = self._mt5_margin_budget_pct_for_signal(
                    None,
                    kind,
                    broker_symbol=su,
                    signal_symbol=su,
                )
                row_budget_pct = max(1.0, float(row_budget_pct or budget_pct))
                row_allowed_margin = max(0.0, free_margin * (row_budget_pct / 100.0))
                spread_ok = (spread_pct is not None and float(spread_pct) <= float(max_spread_pct))
                margin_ok = (
                    (margin_req is not None)
                    and (float(margin_req) <= float(row_allowed_margin))
                    and ((free_margin - float(margin_req)) >= float(min_free_after))
                )
                allowlist_active = bool(self._allow_symbols)
                in_allow = (not allowlist_active) or (su in self._allow_symbols)
                in_block = su in self._block_symbols
                policy_ok = bool(in_allow and not in_block)
                affordable_market = bool(margin_ok and spread_ok)
                final_ok = bool(affordable_market and policy_ok)
                if margin_req is None:
                    status = "no_margin_calc"
                elif not margin_ok:
                    status = "deny_margin"
                elif not spread_ok:
                    status = "deny_spread"
                elif not policy_ok:
                    status = "deny_policy"
                else:
                    status = "ok"

                rows.append({
                    "symbol": su,
                    "category": kind,
                    "trade_mode": trade_mode,
                    "visible": visible,
                    "vol_min": round(vol_min, 8),
                    "vol_step": round(vol_step, 8),
                    "ask": round(ask, 8),
                    "bid": round(bid, 8),
                    "spread_pct": (None if spread_pct is None else round(float(spread_pct), 6)),
                    "margin_min_lot": (None if margin_req is None else round(float(margin_req), 4)),
                    "margin_budget_pct": round(float(row_budget_pct), 2),
                    "margin_budget_reason": str(row_budget_reason),
                    "allowed_margin": round(float(row_allowed_margin), 4),
                    "margin_ok": margin_ok,
                    "spread_ok": spread_ok,
                    "policy_ok": policy_ok,
                    "allowlist_hit": bool(in_allow),
                    "blocklisted": bool(in_block),
                    "affordable_market": affordable_market,
                    "affordable_now": final_ok,
                    "status": status,
                })
            except Exception:
                continue

        rows.sort(
            key=lambda r: (
                1 if r.get("affordable_now") else 0,
                1 if r.get("affordable_market") else 0,
                1 if r.get("margin_ok") else 0,
                1 if r.get("spread_ok") else 0,
                -(0 if r.get("margin_min_lot") is None else 1),
                -(0 if r.get("spread_pct") is None else 1),
                float(r.get("margin_min_lot") or 1e9),
                float(r.get("spread_pct") or 1e9),
                str(r.get("symbol") or ""),
            ),
            reverse=False,
        )
        # Move best rows to top by re-sorting with ascending metrics after booleans.
        rows.sort(
            key=lambda r: (
                not bool(r.get("affordable_now")),
                not bool(r.get("affordable_market")),
                not bool(r.get("margin_ok")),
                not bool(r.get("spread_ok")),
                float(r.get("margin_min_lot") or 1e9),
                float(r.get("spread_pct") or 1e9),
                str(r.get("symbol") or ""),
            )
        )

        by_category: dict[str, dict] = {}
        for r in rows:
            c = str(r.get("category") or "other")
            b = by_category.setdefault(c, {"total": 0, "ok": 0, "market_ok": 0, "margin_ok": 0, "spread_ok": 0})
            b["total"] += 1
            b["ok"] += (1 if r.get("affordable_now") else 0)
            b["market_ok"] += (1 if r.get("affordable_market") else 0)
            b["margin_ok"] += (1 if r.get("margin_ok") else 0)
            b["spread_ok"] += (1 if r.get("spread_ok") else 0)

        out["summary"] = {
            "broker_symbols": len(broker_symbols),
            "recognized_candidates": recognized,
            "checked": checked,
            "rows": len(rows),
            "ok_now": sum(1 for r in rows if r.get("affordable_now")),
            "market_ok": sum(1 for r in rows if r.get("affordable_market")),
            "margin_ok": sum(1 for r in rows if r.get("margin_ok")),
            "spread_ok": sum(1 for r in rows if r.get("spread_ok")),
            "by_category": by_category,
            "scan_cap": scan_cap,
        }
        if bool(only_ok):
            rows = [r for r in rows if r.get("affordable_now")]
        out["rows"] = rows[:top_n]
        return out

    def build_bootstrap_candidates(self, scope: str = "quick") -> list[str]:
        """
        Build candidate signal symbols for MT5 symbol-map bootstrap.
        quick: XAUUSD + priority crypto pairs.
        all: quick + stock universe symbols.
        """
        mode = (scope or "quick").strip().lower()
        candidates: list[str] = ["XAUUSD"]
        candidates.extend(list(getattr(config, "PRIORITY_PAIRS", []) or []))

        if mode == "all":
            try:
                from market.stock_universe import get_all_stocks
                candidates.extend(get_all_stocks())
            except Exception as e:
                logger.warning("[MT5] bootstrap(all) stock-universe load failed: %s", e)

        deduped: list[str] = []
        seen: set[str] = set()
        for raw in candidates:
            sym = str(raw or "").strip().upper()
            if not sym or sym in seen:
                continue
            seen.add(sym)
            deduped.append(sym)
        return deduped

    def suggest_symbol_map(
        self,
        signal_symbols: Optional[list[str]] = None,
        scope: str = "quick",
    ) -> dict:
        """
        Suggest MT5_SYMBOL_MAP entries by resolving candidate signal symbols
        against current broker symbols.
        """
        mode = (scope or "quick").strip().lower()
        if mode not in ("quick", "all"):
            mode = "quick"

        ok, state = self._ensure_connection()
        if not ok:
            return {
                "connected": False,
                "error": state,
                "scope": mode,
                "broker_symbols": 0,
                "total_candidates": 0,
                "resolved_count": 0,
                "passthrough": [],
                "suggested_map": {},
                "unresolved": [],
                "env_value": "",
                "env_line": "MT5_SYMBOL_MAP=",
            }

        broker_symbols = self._get_symbols(force_refresh=True)
        if not broker_symbols:
            return {
                "connected": True,
                "error": "no broker symbols loaded",
                "scope": mode,
                "broker_symbols": 0,
                "total_candidates": 0,
                "resolved_count": 0,
                "passthrough": [],
                "suggested_map": {},
                "unresolved": [],
                "env_value": "",
                "env_line": "MT5_SYMBOL_MAP=",
            }

        if signal_symbols is None:
            symbols = self.build_bootstrap_candidates(scope=mode)
        else:
            symbols = []
            seen: set[str] = set()
            for raw in signal_symbols:
                sym = str(raw or "").strip().upper()
                if not sym or sym in seen:
                    continue
                seen.add(sym)
                symbols.append(sym)

        passthrough: list[str] = []
        unresolved: list[str] = []
        suggested_map: dict[str, str] = {}
        for sig in symbols:
            resolved = self.resolve_symbol(sig)
            if not resolved:
                unresolved.append(sig)
                continue
            if resolved.upper() == sig.upper():
                passthrough.append(sig)
            else:
                suggested_map[sig] = resolved

        ordered_map = {k: suggested_map[k] for k in sorted(suggested_map.keys())}
        env_value = ",".join(f"{k}={v}" for k, v in ordered_map.items())
        resolved_count = len(passthrough) + len(ordered_map)
        return {
            "connected": True,
            "error": "",
            "scope": mode,
            "broker_symbols": len(broker_symbols),
            "total_candidates": len(symbols),
            "resolved_count": resolved_count,
            "passthrough": sorted(passthrough),
            "suggested_map": ordered_map,
            "unresolved": sorted(unresolved),
            "env_value": env_value,
            "env_line": f"MT5_SYMBOL_MAP={env_value}",
        }


mt5_executor = MT5Executor()
