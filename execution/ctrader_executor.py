"""
execution/ctrader_executor.py

cTrader OpenAPI execution bridge for Dexter Pro.

Design goals:
- Separate process execution so Twisted/OpenAPI lifecycle does not interfere with MT5.
- Safe by default: opt-in, dry-run first, explicit symbol/source allowlists.
- Keep a dedicated execution journal for analysis and seller/store operations.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from config import config

logger = logging.getLogger(__name__)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _normalize_symbol_key(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(symbol or "").upper())


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_to_ms(value: str) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _looks_like_fixture_signal(symbol: str, entry: float, stop_loss: float, take_profit: float) -> bool:
    sym = str(symbol or "").strip().upper()
    if sym not in {"XAUUSD", "ETHUSD", "BTCUSD"}:
        return False
    return (
        abs(_safe_float(entry, 0.0) - 100.0) < 1e-9
        and abs(_safe_float(stop_loss, 0.0) - 99.0) < 1e-9
        and abs(_safe_float(take_profit, 0.0) - 101.0) < 1e-9
    )


def _looks_like_test_pattern(pattern: str) -> bool:
    tokens = {
        str(token or "").strip().upper()
        for token in re.split(r"[^A-Z0-9]+", str(pattern or "").upper())
        if str(token or "").strip()
    }
    return bool(tokens.intersection({"TEST", "FIXTURE", "PYTEST", "UNITTEST"}))


@dataclass
class CTraderExecutionResult:
    ok: bool
    status: str
    message: str
    signal_symbol: str = ""
    broker_symbol: str = ""
    dry_run: bool = False
    account_id: Optional[int] = None
    order_id: Optional[int] = None
    position_id: Optional[int] = None
    deal_id: Optional[int] = None
    volume: Optional[float] = None
    execution_meta: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "ok": bool(self.ok),
            "status": str(self.status or ""),
            "message": str(self.message or ""),
            "signal_symbol": str(self.signal_symbol or ""),
            "broker_symbol": str(self.broker_symbol or ""),
            "dry_run": bool(self.dry_run),
            "account_id": self.account_id,
            "order_id": self.order_id,
            "position_id": self.position_id,
            "deal_id": self.deal_id,
            "volume": self.volume,
            "execution_meta": dict(self.execution_meta or {}),
        }


class CTraderExecutor:
    def __init__(self):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        db_cfg = str(getattr(config, "CTRADER_DB_PATH", "") or "").strip()
        self.db_path = Path(db_cfg) if db_cfg else (data_dir / "ctrader_openapi.db")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.worker_path = Path(__file__).resolve().parent.parent / "ops" / "ctrader_execute_once.py"
        self.trading_manager_state_path = self.db_path.parent / "runtime" / "trading_manager_state.json"
        self.trading_team_state_path = self.db_path.parent / "runtime" / "trading_team_state.json"
        self._autopilot = None
        self._position_peak_r: dict[int, float] = {}
        self._init_db()

    @property
    def enabled(self) -> bool:
        return bool(getattr(config, "CTRADER_ENABLED", False))

    @property
    def autotrade_enabled(self) -> bool:
        return bool(getattr(config, "CTRADER_AUTOTRADE_ENABLED", False))

    @property
    def dry_run(self) -> bool:
        return bool(getattr(config, "CTRADER_DRY_RUN", True))

    @property
    def sdk_available(self) -> bool:
        try:
            import ctrader_open_api  # noqa: F401
            return True
        except Exception:
            return False

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_ts REAL NOT NULL,
                    created_utc TEXT NOT NULL,
                    source TEXT DEFAULT '',
                    lane TEXT DEFAULT '',
                    symbol TEXT DEFAULT '',
                    direction TEXT DEFAULT '',
                    confidence REAL DEFAULT 0,
                    entry REAL DEFAULT 0,
                    stop_loss REAL DEFAULT 0,
                    take_profit REAL DEFAULT 0,
                    entry_type TEXT DEFAULT '',
                    dry_run INTEGER DEFAULT 0,
                    account_id INTEGER,
                    broker_symbol TEXT DEFAULT '',
                    volume REAL DEFAULT 0,
                    status TEXT DEFAULT '',
                    message TEXT DEFAULT '',
                    order_id INTEGER,
                    position_id INTEGER,
                    deal_id INTEGER,
                    signal_run_id TEXT DEFAULT '',
                    signal_run_no INTEGER DEFAULT 0,
                    request_json TEXT DEFAULT '{}',
                    response_json TEXT DEFAULT '{}',
                    execution_meta_json TEXT DEFAULT '{}'
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_journal_ts ON execution_journal(created_ts DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_journal_source ON execution_journal(source)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_journal_symbol ON execution_journal(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_journal_status ON execution_journal(status)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ctrader_positions (
                    position_id INTEGER PRIMARY KEY,
                    account_id INTEGER,
                    source TEXT DEFAULT '',
                    lane TEXT DEFAULT '',
                    symbol TEXT DEFAULT '',
                    broker_symbol TEXT DEFAULT '',
                    direction TEXT DEFAULT '',
                    volume REAL DEFAULT 0,
                    entry_price REAL DEFAULT 0,
                    stop_loss REAL DEFAULT 0,
                    take_profit REAL DEFAULT 0,
                    label TEXT DEFAULT '',
                    comment TEXT DEFAULT '',
                    signal_run_id TEXT DEFAULT '',
                    signal_run_no INTEGER DEFAULT 0,
                    journal_id INTEGER,
                    is_open INTEGER DEFAULT 1,
                    status TEXT DEFAULT '',
                    first_seen_utc TEXT DEFAULT '',
                    last_seen_utc TEXT DEFAULT '',
                    raw_json TEXT DEFAULT '{}'
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_positions_symbol ON ctrader_positions(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_positions_source ON ctrader_positions(source)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ctrader_deals (
                    deal_id INTEGER PRIMARY KEY,
                    account_id INTEGER,
                    position_id INTEGER,
                    order_id INTEGER,
                    source TEXT DEFAULT '',
                    lane TEXT DEFAULT '',
                    symbol TEXT DEFAULT '',
                    broker_symbol TEXT DEFAULT '',
                    direction TEXT DEFAULT '',
                    volume REAL DEFAULT 0,
                    execution_price REAL DEFAULT 0,
                    gross_profit_usd REAL DEFAULT 0,
                    swap_usd REAL DEFAULT 0,
                    commission_usd REAL DEFAULT 0,
                    pnl_conversion_fee_usd REAL DEFAULT 0,
                    pnl_usd REAL DEFAULT 0,
                    outcome INTEGER,
                    has_close_detail INTEGER DEFAULT 0,
                    signal_run_id TEXT DEFAULT '',
                    signal_run_no INTEGER DEFAULT 0,
                    journal_id INTEGER,
                    execution_utc TEXT DEFAULT '',
                    raw_json TEXT DEFAULT '{}'
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_deals_symbol ON ctrader_deals(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_deals_source ON ctrader_deals(source)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_deals_exec_utc ON ctrader_deals(execution_utc DESC)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ctrader_orders (
                    order_id INTEGER PRIMARY KEY,
                    account_id INTEGER,
                    source TEXT DEFAULT '',
                    lane TEXT DEFAULT '',
                    symbol TEXT DEFAULT '',
                    broker_symbol TEXT DEFAULT '',
                    direction TEXT DEFAULT '',
                    volume REAL DEFAULT 0,
                    entry_price REAL DEFAULT 0,
                    stop_loss REAL DEFAULT 0,
                    take_profit REAL DEFAULT 0,
                    order_type TEXT DEFAULT '',
                    order_status TEXT DEFAULT '',
                    label TEXT DEFAULT '',
                    comment TEXT DEFAULT '',
                    client_order_id TEXT DEFAULT '',
                    signal_run_id TEXT DEFAULT '',
                    signal_run_no INTEGER DEFAULT 0,
                    journal_id INTEGER,
                    is_open INTEGER DEFAULT 1,
                    first_seen_utc TEXT DEFAULT '',
                    last_seen_utc TEXT DEFAULT '',
                    raw_json TEXT DEFAULT '{}'
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_orders_symbol ON ctrader_orders(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_orders_source ON ctrader_orders(source)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_orders_open ON ctrader_orders(is_open, last_seen_utc DESC)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ctrader_capture_runs (
                    run_id TEXT PRIMARY KEY,
                    created_ts REAL NOT NULL,
                    created_utc TEXT NOT NULL,
                    account_id INTEGER,
                    environment TEXT DEFAULT '',
                    symbols_json TEXT DEFAULT '[]',
                    duration_sec INTEGER DEFAULT 0,
                    include_depth INTEGER DEFAULT 1,
                    spot_events INTEGER DEFAULT 0,
                    depth_events INTEGER DEFAULT 0,
                    status TEXT DEFAULT '',
                    message TEXT DEFAULT '',
                    raw_json TEXT DEFAULT '{}'
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_capture_runs_ts ON ctrader_capture_runs(created_ts DESC)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ctrader_spot_ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT DEFAULT '',
                    account_id INTEGER,
                    symbol_id INTEGER,
                    symbol TEXT DEFAULT '',
                    bid REAL DEFAULT 0,
                    ask REAL DEFAULT 0,
                    spread REAL DEFAULT 0,
                    spread_pct REAL DEFAULT 0,
                    event_utc TEXT DEFAULT '',
                    event_ts REAL DEFAULT 0,
                    raw_json TEXT DEFAULT '{}'
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_spot_ticks_symbol_ts ON ctrader_spot_ticks(symbol, event_utc DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_spot_ticks_run ON ctrader_spot_ticks(run_id)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ctrader_depth_quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT DEFAULT '',
                    account_id INTEGER,
                    symbol_id INTEGER,
                    symbol TEXT DEFAULT '',
                    quote_id INTEGER,
                    side TEXT DEFAULT '',
                    price REAL DEFAULT 0,
                    size REAL DEFAULT 0,
                    level_index INTEGER DEFAULT 0,
                    event_utc TEXT DEFAULT '',
                    event_ts REAL DEFAULT 0,
                    raw_json TEXT DEFAULT '{}'
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_depth_quotes_symbol_ts ON ctrader_depth_quotes(symbol, event_utc DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ctrader_depth_quotes_run ON ctrader_depth_quotes(run_id)")

    def _configured_account_id(self) -> tuple[Optional[int], str]:
        raw_login = str(getattr(config, "CTRADER_ACCOUNT_LOGIN", "") or "").strip()
        if raw_login:
            row = getattr(config, "find_ctrader_account", lambda *_args, **_kwargs: None)(raw_login, use_demo=getattr(config, "CTRADER_USE_DEMO", False))
            if isinstance(row, dict):
                try:
                    return int(row.get("accountId")), "env:CTRADER_ACCOUNT_LOGIN"
                except Exception:
                    pass
        raw = str(getattr(config, "CTRADER_ACCOUNT_ID", "") or "").strip()
        if raw:
            row = getattr(config, "find_ctrader_account", lambda *_args, **_kwargs: None)(raw)
            if isinstance(row, dict):
                try:
                    matched_id = int(row.get("accountId"))
                    reason = "env:CTRADER_ACCOUNT_ID"
                    if str(row.get("accountNumber", "") or "") == raw or str(row.get("traderLogin", "") or "") == raw:
                        reason = "env:CTRADER_ACCOUNT_ID:login_resolved"
                    return matched_id, reason
                except Exception:
                    pass
            try:
                return int(raw), "env:CTRADER_ACCOUNT_ID:raw"
            except Exception:
                pass
        row = getattr(config, "find_ctrader_account", lambda *_args, **_kwargs: None)("", use_demo=getattr(config, "CTRADER_USE_DEMO", False))
        if isinstance(row, dict):
            try:
                account_id = int(row.get("accountId"))
            except Exception:
                account_id = 0
            if account_id > 0:
                is_demo = not bool(row.get("live", False))
                reason = "env:Ctrader_accounts"
                if str(row.get("depositCurrency", "")).upper() == "USD":
                    reason = "env:Ctrader_accounts:usd_demo_active" if is_demo else "env:Ctrader_accounts:usd_live_active"
                elif is_demo:
                    reason = "env:Ctrader_accounts:demo_active"
                return account_id, reason
        return None, "missing"

    def _source_allowed(self, source: str) -> bool:
        allowed = set(getattr(config, "get_ctrader_allowed_sources", lambda: set())() or set())
        token = str(source or "").strip().lower()
        if bool(getattr(config, "PERSISTENT_CANARY_ENABLED", False)) and token.endswith(":canary"):
            # Direct canary lanes must also respect the narrower direct-source allowlist.
            if token.count(":") == 1:
                base_direct = token.rsplit(":", 1)[0].strip().lower()
                direct_allowed = set(getattr(config, "get_persistent_canary_direct_allowed_sources", lambda: set())() or set())
                if direct_allowed and ("*" not in direct_allowed) and ("all" not in direct_allowed) and (base_direct not in direct_allowed):
                    return False
            base = token.rsplit(":", 1)[0].strip().lower()
            root = base.split(":", 1)[0].strip().lower()
            canary_allowed = set(getattr(config, "get_persistent_canary_allowed_sources", lambda: set())() or set())
            if base and (base in canary_allowed or root in canary_allowed):
                return True
        if not allowed:
            return True
        return token in allowed

    @staticmethod
    def _source_direction_matches(specs: set[tuple[str, str]], *, source: str, direction: str) -> bool:
        src = str(source or "").strip().lower()
        side = str(direction or "").strip().lower()
        if side == "buy":
            side = "long"
        elif side == "sell":
            side = "short"
        for spec_source, spec_direction in set(specs or set()):
            s = str(spec_source or "").strip().lower()
            d = str(spec_direction or "*").strip().lower() or "*"
            if s not in {"*", "all"} and s != src:
                continue
            if d in {"*", "all"} or d == side:
                return True
        return False

    def _source_direction_governance_guard(self, *, source: str, symbol: str, direction: str) -> tuple[bool, str, dict]:
        src = str(source or "").strip().lower()
        side = str(direction or "").strip().lower()
        if side == "buy":
            side = "long"
        elif side == "sell":
            side = "short"
        if not src or side not in {"long", "short"}:
            return True, "", {"source": src, "direction": side}
        protected = set(getattr(config, "get_ctrader_protected_source_directions", lambda: set())() or set())
        meta = {
            "source": src,
            "symbol": str(symbol or "").strip().upper(),
            "direction": side,
            "protected": self._source_direction_matches(protected, source=src, direction=side),
        }
        if bool(meta["protected"]):
            return True, "", meta
        if not bool(getattr(config, "CTRADER_SOURCE_DIRECTION_QUARANTINE_ENABLED", True)):
            return True, "", {**meta, "enabled": False}
        quarantined = set(getattr(config, "get_ctrader_quarantined_source_directions", lambda: set())() or set())
        blocked = self._source_direction_matches(quarantined, source=src, direction=side)
        meta.update({"enabled": True, "quarantined": bool(blocked)})
        if blocked:
            return False, f"source_direction_quarantined:{src}:{side}", meta
        return True, "", meta

    def _symbol_allowed(self, symbol: str) -> bool:
        allowed = set(getattr(config, "get_ctrader_allowed_symbols", lambda: set())() or set())
        token = str(symbol or "").strip().upper()
        if bool(getattr(config, "PERSISTENT_CANARY_ENABLED", False)):
            canary_allowed = set(getattr(config, "get_persistent_canary_allowed_symbols", lambda: set())() or set())
            if token and token in canary_allowed:
                return True
        if not allowed:
            return True
        return token in allowed

    @staticmethod
    def _signal_trace_meta(signal) -> dict:
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        run_no = 0
        try:
            run_no = int(raw.get("signal_run_no", 0) or 0)
        except Exception:
            run_no = 0
        return {
            "run_no": int(run_no),
            "run_id": str(raw.get("signal_run_id", "") or ""),
        }

    @staticmethod
    def _source_lane(source: str) -> str:
        token = str(source or "").lower()
        if ":canary" in token:
            return "canary"
        if ":bypass" in token:
            return "bypass"
        if ":winner" in token:
            return "winner"
        return "main"

    @staticmethod
    def _parse_label_meta(label: str = "", comment: str = "") -> dict:
        raw_label = str(label or "").strip()
        raw_comment = str(comment or "").strip()
        out = {
            "symbol": "",
            "source": "",
            "lane": "",
            "run_no": 0,
            "run_id": "",
        }
        if raw_label.lower().startswith("dexter:"):
            parts = raw_label.split(":")
            if len(parts) >= 4:
                out["symbol"] = str(parts[1] or "").strip().upper()
                out["source"] = str(parts[2] or "").strip().lower()
                try:
                    out["run_no"] = int(parts[3] or 0)
                except Exception:
                    out["run_no"] = 0
        if raw_comment.lower().startswith("dexter|"):
            parts = raw_comment.split("|")
            if len(parts) >= 3:
                comment_source = str(parts[1] or "").strip().lower()
                comment_symbol = str(parts[2] or "").strip().upper()
                if comment_source:
                    out["source"] = comment_source
                if comment_symbol:
                    out["symbol"] = comment_symbol
        out["lane"] = self_lane = CTraderExecutor._source_lane(out["source"])
        if self_lane == "main" and ":winner" in raw_label.lower():
            out["lane"] = "winner"
        return out

    @staticmethod
    def _ms_to_iso(ms_value: int) -> str:
        ts = _safe_int(ms_value, 0)
        if ts <= 0:
            return _utc_now_iso()
        try:
            return datetime.fromtimestamp(ts / 1000.0, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return _utc_now_iso()

    @staticmethod
    def _order_status_token(raw_status: str) -> str:
        token = str(raw_status or "").strip().upper()
        if token.startswith("ORDER_STATUS_"):
            token = token[len("ORDER_STATUS_"):]
        return token.lower()

    def _normalize_order(self, order: dict) -> dict:
        row = dict(order or {})
        trade = dict(row.get("tradeData") or {})
        label = str(trade.get("label", "") or "")
        comment = str(trade.get("comment", "") or "")
        meta = self._parse_label_meta(label, comment)
        source = str(meta.get("source") or "").strip().lower()
        symbol = str(meta.get("symbol") or "").strip().upper()
        order_type = str(row.get("orderType", "") or "").strip().lower()
        status = self._order_status_token(str(row.get("orderStatus", "") or ""))
        side = str(trade.get("tradeSide", "") or "").strip().upper()
        direction = "long" if side == "BUY" else ("short" if side == "SELL" else "")
        entry = 0.0
        if order_type == "limit":
            entry = _safe_float(row.get("limitPrice"), 0.0)
        elif order_type == "stop":
            entry = _safe_float(row.get("stopPrice"), 0.0)
        created_ms = _safe_int(trade.get("openTimestamp"), 0)
        if created_ms <= 0:
            created_ms = _safe_int(row.get("utcLastUpdateTimestamp"), 0)
        return {
            "order_id": _safe_int(row.get("orderId"), 0),
            "source": source,
            "lane": str(meta.get("lane") or self._source_lane(source)),
            "symbol": symbol,
            "broker_symbol": symbol,
            "direction": direction,
            "volume": _safe_float(trade.get("volume"), 0.0),
            "entry_price": entry,
            "stop_loss": _safe_float(row.get("stopLoss"), 0.0),
            "take_profit": _safe_float(row.get("takeProfit"), 0.0),
            "order_type": order_type,
            "order_status": status,
            "label": label,
            "comment": comment,
            "client_order_id": str(row.get("clientOrderId", "") or ""),
            "signal_run_id": str(meta.get("run_id", "") or ""),
            "signal_run_no": int(meta.get("run_no", 0) or 0),
            "created_utc": self._ms_to_iso(created_ms),
            "created_ts": round(created_ms / 1000.0, 3) if created_ms > 0 else time.time(),
            "raw_json": json.dumps(row, ensure_ascii=True, separators=(",", ":")),
        }

    @staticmethod
    def _source_family(source: str) -> str:
        token = str(source or "").strip().lower()
        if not token:
            return ""
        if bool(getattr(config, "DEXTER_MEMPALACE_FAMILY_LANE_ENABLED", False)):
            mem_tokens = set(getattr(config, "get_dexter_mempalace_source_tokens", lambda: set())() or set())
            if mem_tokens and any(mt in token for mt in mem_tokens):
                fam = str(getattr(config, "DEXTER_MEMPALACE_FAMILY_NAME", "xau_scalp_mempalace_lane") or "").strip().lower()
                if fam:
                    return fam
        if ":rr:" in token or "range_repair" in token:
            return "xau_scalp_range_repair"
        if ":td:" in token or "tick_depth_filter" in token:
            return "xau_scalp_tick_depth_filter"
        if ":mfu:" in token or "microtrend_follow_up" in token:
            return "xau_scalp_microtrend_follow_up"
        if ":fss:" in token or "flow_short_sidecar" in token:
            return "xau_scalp_flow_short_sidecar"
        if ":pb:" in token or "pullback_limit" in token:
            return "xau_scalp_pullback_limit"
        if ":bs:" in token or "breakout_stop" in token:
            return "xau_scalp_breakout_stop"
        if token.startswith("xauusd_scheduled"):
            return "xau_scheduled_trend"
        if ":ff:" in token or "failed_fade_follow_stop" in token:
            return "xau_scalp_failed_fade_follow_stop"
        if token.startswith("fibo_xauusd") or "xau_fibo_advance" in token:
            return "xau_fibo_advance"
        if token.startswith("scalp_xauusd"):
            return "xau_scalp_microtrend"
        if token.startswith("scalp_btcusd"):
            return "btc_weekend_winner"
        if token.startswith("scalp_ethusd"):
            return "eth_weekend_winner"
        return ""

    @classmethod
    def _xau_order_care_desk(cls, source: str) -> str:
        family = cls._source_family(source)
        if family == "xau_scalp_range_repair":
            return "range_repair"
        if family == "xau_scalp_flow_short_sidecar":
            return "fss_confirmation"
        if str(source or "").strip().lower().startswith(("scalp_xauusd", "xauusd_scheduled")):
            return "limit_retest"
        return ""

    @staticmethod
    def _negative_execution_statuses() -> set[str]:
        return {
            "disabled",
            "unavailable",
            "dry_run",
            "rejected",
            "error",
            "worker_error",
            "auth_failed",
            "account_auth_failed",
            "timeout",
            "invalid",
            "blocked",
            "skipped",
            "filtered",
        }

    @staticmethod
    def _normalized_text_list(items) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for raw in list(items or []):
            text = str(raw or "").strip()
            if text and text not in seen:
                seen.add(text)
                out.append(text)
        return out

    @staticmethod
    def _xau_alignment_label(mtf_snapshot: dict) -> str:
        snap = dict(mtf_snapshot or {})
        token = str(snap.get("strict_alignment") or snap.get("alignment_label") or "").strip().lower()
        if token:
            return token
        aligned_side = str(snap.get("strict_aligned_side") or snap.get("aligned_side") or "").strip().lower()
        if aligned_side == "long":
            return "aligned_bullish"
        if aligned_side == "short":
            return "aligned_bearish"
        return "mixed" if snap else ""

    def _build_deal_attribution_payload(
        self,
        *,
        deal: dict,
        source: str,
        lane: str,
        symbol: str,
        direction: str,
        journal_row: Optional[sqlite3.Row],
    ) -> dict:
        payload = dict(deal or {})
        journal_obj = dict(journal_row) if journal_row is not None else {}
        req = self._safe_json_load(str(journal_obj.get("request_json", "") or ""))
        execution_meta = self._safe_json_load(str(journal_obj.get("execution_meta_json", "") or ""))
        raw_scores = dict(req.get("raw_scores") or {})
        mtf_snapshot = dict(req.get("xau_multi_tf_snapshot") or raw_scores.get("xau_multi_tf_snapshot") or {})
        market_capture = dict(execution_meta.get("market_capture") or {})
        market_features = dict(market_capture.get("features") or {})
        family = str(
            req.get("family")
            or raw_scores.get("strategy_family")
            or raw_scores.get("family")
            or payload.get("family")
            or self._source_family(source)
            or ""
        ).strip().lower()
        aligned_side = str(
            req.get("xau_mtf_aligned_side")
            or raw_scores.get("xau_mtf_aligned_side")
            or mtf_snapshot.get("strict_aligned_side")
            or mtf_snapshot.get("aligned_side")
            or ""
        ).strip().lower()
        payload.update(
            {
                "deal_attribution_version": 1,
                "source": str(source or "").strip().lower(),
                "lane": str(lane or "").strip().lower(),
                "symbol": str(symbol or payload.get("symbol") or "").strip().upper(),
                "direction": str(direction or payload.get("direction") or "").strip().lower(),
                "family": family,
                "strategy_family": str(raw_scores.get("strategy_family") or family or "").strip().lower(),
                "strategy_id": str(raw_scores.get("strategy_id") or req.get("strategy_id") or "").strip(),
                "session": str(
                    req.get("session")
                    or raw_scores.get("session")
                    or raw_scores.get("session_zone")
                    or raw_scores.get("signal_session")
                    or ""
                ).strip().lower(),
                "entry_type": str(req.get("entry_type") or journal_obj.get("entry_type") or raw_scores.get("entry_type") or "").strip().lower(),
                "pattern": str(req.get("pattern") or raw_scores.get("pattern") or "").strip(),
                "timeframe": str(req.get("timeframe") or raw_scores.get("timeframe") or "").strip().lower(),
                "confidence": round(
                    _safe_float(
                        req.get("confidence", journal_obj.get("confidence", payload.get("confidence", 0.0))),
                        0.0,
                    ),
                    4,
                ),
                "reasons": self._normalized_text_list(req.get("reasons") or raw_scores.get("reasons") or []),
                "warnings": self._normalized_text_list(req.get("warnings") or []),
                "xau_multi_tf_snapshot": mtf_snapshot,
                "xau_mtf_aligned_side": aligned_side,
                "strict_alignment": self._xau_alignment_label(mtf_snapshot) or "unknown",
                "winner_logic_regime": str(
                    raw_scores.get("winner_logic_regime")
                    or raw_scores.get("crypto_winner_logic_regime")
                    or ""
                ).strip().lower(),
                "market_capture_features": {
                    key: market_features.get(key)
                    for key in (
                        "day_type",
                        "chart_state",
                        "spread_expansion",
                        "depth_imbalance",
                        "depth_refill_shift",
                        "delta_proxy",
                        "bar_volume_proxy",
                        "rejection_ratio",
                    )
                    if key in market_features
                },
            }
        )
        return payload

    @staticmethod
    def _worker_price_symbol(symbol: str) -> str:
        token = str(symbol or "").strip().upper()
        if token == "ETHUSD":
            return "ETH/USDT"
        if token == "BTCUSD":
            return "BTC/USDT"
        return token

    def _load_trading_manager_state(self) -> dict:
        try:
            if self.trading_manager_state_path.exists():
                payload = json.loads(self.trading_manager_state_path.read_text(encoding="utf-8"))
                return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
        return {}

    def _load_trading_team_state(self) -> dict:
        try:
            if self.trading_team_state_path.exists():
                payload = json.loads(self.trading_team_state_path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    return {}
                if not any(
                    key in payload
                    for key in (
                        "status",
                        "symbols",
                        "opportunity_feed",
                        "xau_family_routing",
                        "xau_parallel_families",
                        "xau_order_care",
                    )
                ):
                    return {}
                return payload
        except Exception:
            return {}
        return {}

    def _load_routing_runtime_state(self) -> dict:
        if bool(getattr(config, "TRADING_TEAM_ENABLED", True)):
            team_state = self._load_trading_team_state()
            if team_state:
                return team_state
        return self._load_trading_manager_state()

    def _xau_parallel_family_state(self) -> dict:
        state = dict((self._load_routing_runtime_state() or {}).get("xau_parallel_families") or {})
        if str(state.get("status") or "") != "active":
            return {}
        return state

    def _xau_hedge_transition_state(self) -> dict:
        state = dict((self._load_routing_runtime_state() or {}).get("xau_hedge_transition") or {})
        if str(state.get("status") or "") != "active":
            return {}
        return state

    def _xau_opportunity_bypass_state(self) -> dict:
        state = dict((self._load_routing_runtime_state() or {}).get("xau_opportunity_bypass") or {})
        if str(state.get("status") or "") != "active":
            return {}
        return state

    def _xau_cluster_loss_guard_state(self) -> dict:
        state = dict((self._load_routing_runtime_state() or {}).get("xau_cluster_loss_guard") or {})
        if str(state.get("status") or "") != "active":
            return {}
        return state

    def _xau_order_care_state(self, *, symbol: str, source: str) -> dict:
        if not self._is_xau_symbol(symbol):
            return {}
        state = dict((self._load_routing_runtime_state() or {}).get("xau_order_care") or {})
        if str(state.get("status") or "") != "active":
            return {}
        source_token = str(source or "").strip().lower()
        desk_name = self._xau_order_care_desk(source_token)
        desks = dict(state.get("desks") or {})
        if desk_name:
            desk_state = dict(desks.get(desk_name) or {})
            if str(desk_state.get("status") or "active").strip().lower() in {"", "active"}:
                desk_allowed = {
                    str(token or "").strip().lower()
                    for token in list(desk_state.get("allowed_sources") or [])
                    if str(token or "").strip()
                }
                if (not desk_allowed) or source_token in desk_allowed:
                    merged = dict(state)
                    merged["desk"] = desk_name
                    merged["mode"] = str(desk_state.get("mode") or state.get("mode") or "")
                    merged["allowed_sources"] = list(desk_state.get("allowed_sources") or state.get("allowed_sources") or [])
                    merged["overrides"] = dict(desk_state.get("overrides") or state.get("overrides") or {})
                    return merged
        allowed = {
            str(token or "").strip().lower()
            for token in list(state.get("allowed_sources") or [])
            if str(token or "").strip()
        }
        if allowed and source_token not in allowed:
            return {}
        return state

    @staticmethod
    def _csv_lower_set(raw: str) -> set[str]:
        return {
            str(part or "").strip().lower()
            for part in str(raw or "").split(",")
            if str(part or "").strip()
        }

    @staticmethod
    def _journal_event_ts(row: sqlite3.Row | dict) -> float:
        item = dict(row or {})
        created_ts = _safe_float(item.get("created_ts"), 0.0)
        status = str(item.get("status") or "").strip().lower()
        message = str(item.get("message") or "").strip().lower()
        execution_meta = CTraderExecutor._safe_json_load(str(item.get("execution_meta_json") or ""))
        if status == "closed":
            closed = dict(execution_meta.get("closed") or {})
            closed_ts = _iso_to_ms(str(closed.get("execution_utc") or "")) / 1000.0
            if closed_ts > 0:
                return closed_ts
        if status == "canceled" and "stale_ttl:" in message and created_ts > 0:
            match = re.search(r"stale_ttl:(\d+)m", message)
            ttl_min = _safe_int(match.group(1), 0) if match else 0
            if ttl_min > 0:
                return created_ts + float(ttl_min * 60)
        return created_ts

    @staticmethod
    def _payload_price_match(left: dict, right: dict, *, tolerance: float) -> bool:
        tol = max(0.0, _safe_float(tolerance, 0.0))
        for key in ("entry", "stop_loss", "take_profit"):
            left_value = _safe_float(left.get(key), 0.0)
            right_value = _safe_float(right.get(key), 0.0)
            if left_value <= 0 or right_value <= 0:
                return False
            if abs(left_value - right_value) > tol:
                return False
        return True

    def _xau_short_limit_pause_state(self, *, source: str, payload: dict) -> dict:
        symbol = str(payload.get("symbol") or payload.get("market_symbol") or "").strip().upper()
        direction = str(payload.get("direction") or "").strip().lower()
        entry_type = str(payload.get("entry_type") or "").strip().lower()
        family = self._source_family(str(source or payload.get("source") or ""))
        if not bool(getattr(config, "CTRADER_XAU_SHORT_LIMIT_PAUSE_ENABLED", True)):
            return {"active": False, "reason": "disabled"}
        if not self._is_xau_symbol(symbol):
            return {"active": False, "reason": "symbol_filtered"}
        if direction != "short" or entry_type not in {"limit", "patience"}:
            return {"active": False, "reason": "entry_style_filtered"}
        pause_families = self._csv_lower_set(getattr(config, "CTRADER_XAU_SHORT_LIMIT_PAUSE_FAMILIES", ""))
        if pause_families and family not in pause_families:
            return {"active": False, "reason": "family_filtered"}

        now_ts = time.time()
        pause_min = max(1, int(getattr(config, "CTRADER_XAU_SHORT_LIMIT_PAUSE_MIN", 20) or 20))
        lookback_min = max(
            pause_min + max(5, int(getattr(config, "CTRADER_PENDING_ORDER_TTL_XAU_SCALP_MIN", 45) or 45)),
            int(getattr(config, "CTRADER_XAU_SHORT_LIMIT_PAUSE_LOOKBACK_MIN", 95) or 95),
        )
        cutoff_ts = now_ts - float(lookback_min * 60)
        best: dict = {}
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT created_ts, source, status, message, direction, entry_type, signal_run_id, request_json, execution_meta_json
                  FROM execution_journal
                 WHERE UPPER(COALESCE(symbol,''))=?
                   AND created_ts>=?
                 ORDER BY id DESC
                 LIMIT 120
                """,
                (symbol, cutoff_ts),
            ).fetchall()
        grouped: dict[str, list[dict]] = {}
        for row in list(rows or []):
            req = self._safe_json_load(str(row["request_json"] or ""))
            source_token = str(row["source"] or req.get("source") or "").strip().lower()
            run_id = str(row["signal_run_id"] or req.get("signal_run_id") or "").strip()
            if not source_token or not run_id:
                continue
            row_family = self._source_family(source_token)
            status = str(row["status"] or "").strip().lower()
            row_entry_type = str(row["entry_type"] or req.get("entry_type") or "").strip().lower()
            row_direction = str(row["direction"] or req.get("direction") or "").strip().lower()
            execution_meta = self._safe_json_load(str(row["execution_meta_json"] or ""))
            closed = dict(execution_meta.get("closed") or {})
            pnl = _safe_float(closed.get("pnl_usd"), 0.0)
            event_ts = self._journal_event_ts(row)
            grouped.setdefault(run_id, []).append(
                {
                    "run_id": run_id,
                    "source": source_token,
                    "family": row_family,
                    "status": status,
                    "message": str(row["message"] or ""),
                    "direction": row_direction,
                    "entry_type": row_entry_type,
                    "pnl_usd": pnl,
                    "event_ts": event_ts,
                }
            )

        for run_id, items in grouped.items():
            support_items = [
                item for item in items
                if item["family"] == "xau_scalp_flow_short_sidecar"
                and (
                    (item["status"] == "closed" and float(item["pnl_usd"]) > 0.0)
                    or (item["status"] == "canceled" and "stale_ttl:" in str(item["message"] or "").lower())
                )
            ]
            fail_items = [
                item for item in items
                if item["family"] in pause_families
                and item["direction"] == "short"
                and item["entry_type"] in {"limit", "patience"}
                and item["status"] == "closed"
                and float(item["pnl_usd"]) < 0.0
            ]
            if not support_items or not fail_items:
                continue
            trigger_ts = max(float(item.get("event_ts", 0.0) or 0.0) for item in support_items + fail_items)
            remain_sec = (pause_min * 60.0) - max(0.0, now_ts - trigger_ts)
            if remain_sec <= 0:
                continue
            if (not best) or trigger_ts > float(best.get("trigger_ts", 0.0) or 0.0):
                support_state = "fss_win" if any(item["status"] == "closed" and float(item["pnl_usd"]) > 0.0 for item in support_items) else "fss_stale_cancel"
                best = {
                    "active": True,
                    "reason": "family_disagreement_limit_pause",
                    "trigger_ts": round(trigger_ts, 3),
                    "remaining_sec": round(remain_sec, 1),
                    "remaining_min": round(remain_sec / 60.0, 1),
                    "trigger_run_id": run_id,
                    "support_state": support_state,
                    "failed_sources": sorted({str(item.get("source") or "") for item in fail_items}),
                }
        if best:
            return best
        return {"active": False, "reason": "no_recent_family_disagreement"}

    def _apply_xau_same_run_pair_risk_cap(self, *, source: str, payload: dict) -> dict:
        symbol = str(payload.get("symbol") or payload.get("market_symbol") or "").strip().upper()
        run_id = str(payload.get("signal_run_id") or payload.get("client_order_id") or "").strip()
        direction = str(payload.get("direction") or "").strip().lower()
        family = self._source_family(str(source or payload.get("source") or ""))
        if not bool(getattr(config, "CTRADER_XAU_PAIR_RISK_CAP_ENABLED", True)):
            return {"active": False, "reason": "disabled"}
        if not self._is_xau_symbol(symbol):
            return {"active": False, "reason": "symbol_filtered"}
        pair_families = self._csv_lower_set(getattr(config, "CTRADER_XAU_PAIR_RISK_CAP_FAMILIES", ""))
        if pair_families and family not in pair_families:
            return {"active": False, "reason": "family_filtered"}
        if not run_id:
            return {"active": False, "reason": "run_id_missing"}

        current_risk = max(0.0, _safe_float(payload.get("risk_usd"), 0.0))
        if current_risk <= 0.0:
            return {"active": False, "reason": "risk_missing"}
        tolerance = max(0.0, float(getattr(config, "CTRADER_XAU_PAIR_RISK_PRICE_TOLERANCE", 0.05) or 0.05))
        max_total = max(0.10, float(getattr(config, "CTRADER_XAU_PAIR_RISK_MAX_USD", 3.0) or 3.0))
        min_risk = max(0.05, float(getattr(config, "CTRADER_XAU_PAIR_RISK_MIN_USD", 0.15) or 0.15))

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT source, status, request_json
                  FROM execution_journal
                 WHERE UPPER(COALESCE(symbol,''))=?
                   AND COALESCE(signal_run_id,'')=?
                 ORDER BY id DESC
                 LIMIT 20
                """,
                (symbol, run_id),
            ).fetchall()

        negative_statuses = self._negative_execution_statuses() | {"closed", "canceled", "expired"}
        matched_rows: list[dict] = []
        existing_total = 0.0
        for row in list(rows or []):
            status = str(row["status"] or "").strip().lower()
            if status in negative_statuses:
                continue
            req = self._safe_json_load(str(row["request_json"] or ""))
            row_source = str(row["source"] or req.get("source") or "").strip().lower()
            row_family = self._source_family(row_source)
            if pair_families and row_family not in pair_families:
                continue
            if str(req.get("direction") or "").strip().lower() != direction:
                continue
            if not self._payload_price_match(payload, req, tolerance=tolerance):
                continue
            row_risk = max(0.0, _safe_float(req.get("risk_usd"), 0.0))
            if row_risk <= 0.0:
                continue
            existing_total += row_risk
            matched_rows.append(
                {
                    "source": row_source,
                    "family": row_family,
                    "risk_usd": round(row_risk, 4),
                }
            )
        if not matched_rows:
            return {"active": False, "reason": "no_same_run_pair"}

        remaining = max_total - existing_total
        if remaining >= current_risk:
            return {
                "active": False,
                "reason": "within_cap",
                "existing_risk_usd": round(existing_total, 4),
                "current_risk_usd": round(current_risk, 4),
            }
        if remaining < min_risk:
            return {
                "active": True,
                "blocked": True,
                "reason": "same_run_pair_risk_cap_exhausted",
                "existing_risk_usd": round(existing_total, 4),
                "requested_risk_usd": round(current_risk, 4),
                "max_total_risk_usd": round(max_total, 4),
                "matched_rows": matched_rows,
            }

        new_risk = round(min(current_risk, remaining), 4)
        payload["risk_usd"] = new_risk
        raw = dict(payload.get("raw_scores") or {})
        raw["xau_pair_risk_cap_applied"] = True
        raw["xau_pair_risk_existing_risk_usd"] = round(existing_total, 4)
        raw["xau_pair_risk_requested_usd"] = round(current_risk, 4)
        raw["xau_pair_risk_final_usd"] = round(new_risk, 4)
        raw["xau_pair_risk_max_usd"] = round(max_total, 4)
        raw["xau_pair_risk_signal_run_id"] = run_id
        raw["xau_pair_risk_matched_sources"] = [str(item.get("source") or "") for item in matched_rows]
        payload["raw_scores"] = raw
        reasons = self._normalized_text_list(payload.get("reasons"))
        warnings = self._normalized_text_list(payload.get("warnings"))
        reasons.append(f"Pair-risk cap active: same run {run_id} capped to {new_risk:.2f}$")
        warnings.append(f"Pair-risk cap trimmed from {current_risk:.2f}$ to {new_risk:.2f}$")
        payload["reasons"] = self._normalized_text_list(reasons)
        payload["warnings"] = self._normalized_text_list(warnings)
        return {
            "active": True,
            "blocked": False,
            "reason": "same_run_pair_risk_trimmed",
            "existing_risk_usd": round(existing_total, 4),
            "requested_risk_usd": round(current_risk, 4),
            "final_risk_usd": round(new_risk, 4),
            "max_total_risk_usd": round(max_total, 4),
            "matched_rows": matched_rows,
        }

    def _xau_family_runtime_allowlist(self) -> set[str]:
        families: set[str] = set()
        runtime_state = self._load_routing_runtime_state() or {}
        family_routing_state = dict((runtime_state or {}).get("xau_family_routing") or {})
        family_routing_mode = str(family_routing_state.get("mode") or "").strip().lower()
        families.update(str(f or "").strip().lower() for f in list(getattr(config, "get_ctrader_xau_active_families", lambda: set())() or set()) if str(f or "").strip())
        families.update(str(f or "").strip().lower() for f in list(getattr(config, "get_persistent_canary_experimental_families", lambda: set())() or set()) if str(f or "").strip())
        families.update(str(f or "").strip().lower() for f in list(getattr(config, "get_ctrader_pending_order_dynamic_reprice_families", lambda: set())() or set()) if str(f or "").strip())
        families.update(str(f or "").strip().lower() for f in list(getattr(config, "get_ctrader_pending_order_follow_stop_families", lambda: set())() or set()) if str(f or "").strip())
        primary = ""
        if family_routing_mode != "swarm_support_all":
            primary = str(
                family_routing_state.get("primary_family")
                or getattr(config, "CTRADER_XAU_PRIMARY_FAMILY", "")
                or ""
            ).strip().lower()
        if primary:
            families.add(primary)
        families.update(str(f or "").strip().lower() for f in list(family_routing_state.get("active_families") or []) if str(f or "").strip())
        parallel_state = dict((runtime_state or {}).get("xau_parallel_families") or {})
        if str(parallel_state.get("status") or "") != "active":
            parallel_state = {}
        families.update(str(f or "").strip().lower() for f in list(parallel_state.get("allowed_families") or []) if str(f or "").strip())
        hedge_state = dict((runtime_state or {}).get("xau_hedge_transition") or {})
        if str(hedge_state.get("status") or "") != "active":
            hedge_state = {}
        families.update(str(f or "").strip().lower() for f in list(hedge_state.get("allowed_families") or []) if str(f or "").strip())
        bypass_state = dict((runtime_state or {}).get("xau_opportunity_bypass") or {})
        if str(bypass_state.get("status") or "") != "active":
            bypass_state = {}
        families.update(str(f or "").strip().lower() for f in list(bypass_state.get("allowed_families") or []) if str(f or "").strip())
        return {fam for fam in families if fam}

    def _active_group_exposure_snapshot(
        self,
        *,
        symbol: str,
        families: set[str],
        recent_sec: Optional[int] = None,
        exclude_order_id: int = 0,
        exclude_position_id: int = 0,
    ) -> dict:
        symbol_u = str(symbol or "").strip().upper()
        if not symbol_u or not families:
            return {"active_total": 0, "active_long": 0, "active_short": 0}
        recent_window = max(0, int(recent_sec if recent_sec is not None else getattr(config, "CTRADER_DIRECTION_GUARD_RECENT_SEC", 900) or 900))
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            pos_rows = conn.execute(
                """
                SELECT position_id, direction, source
                  FROM ctrader_positions
                 WHERE is_open=1
                   AND UPPER(COALESCE(symbol,''))=?
                """,
                (symbol_u,),
            ).fetchall()
            ord_rows = conn.execute(
                """
                SELECT order_id, direction, source
                  FROM ctrader_orders
                 WHERE is_open=1
                   AND UPPER(COALESCE(symbol,''))=?
                """,
                (symbol_u,),
            ).fetchall()
            recent_rows = []
            closed_journal_ids: set[int] = set()
            if recent_window > 0:
                closed_rows = conn.execute(
                    """
                    SELECT DISTINCT journal_id
                      FROM ctrader_deals
                     WHERE has_close_detail=1
                       AND journal_id IS NOT NULL
                       AND UPPER(COALESCE(symbol,''))=?
                    """,
                    (symbol_u,),
                ).fetchall()
                closed_journal_ids = {int(row["journal_id"] or 0) for row in closed_rows if int(row["journal_id"] or 0) > 0}
                cutoff_ts = time.time() - float(recent_window)
                recent_rows = conn.execute(
                    """
                    SELECT id, order_id, position_id, direction, source
                      FROM execution_journal
                     WHERE created_ts >= ?
                       AND UPPER(COALESCE(symbol,''))=?
                       AND LOWER(COALESCE(status,'')) IN ('accepted','filled','reconciled_open')
                    """,
                    (cutoff_ts, symbol_u),
                ).fetchall()
        existing_position_ids = {int(row["position_id"] or 0) for row in pos_rows if int(row["position_id"] or 0) > 0}
        existing_order_ids = {int(row["order_id"] or 0) for row in ord_rows if int(row["order_id"] or 0) > 0}
        long_count = 0
        short_count = 0
        for row in pos_rows:
            position_id = int(row["position_id"] or 0)
            if exclude_position_id > 0 and position_id == exclude_position_id:
                continue
            if self._source_family(str(row["source"] or "")) not in families:
                continue
            side = str(row["direction"] or "").strip().lower()
            if side == "long":
                long_count += 1
            elif side == "short":
                short_count += 1
        for row in ord_rows:
            order_id = int(row["order_id"] or 0)
            if exclude_order_id > 0 and order_id == exclude_order_id:
                continue
            if self._source_family(str(row["source"] or "")) not in families:
                continue
            side = str(row["direction"] or "").strip().lower()
            if side == "long":
                long_count += 1
            elif side == "short":
                short_count += 1
        for row in recent_rows:
            journal_id = int(row["id"] or 0)
            order_id = int(row["order_id"] or 0)
            position_id = int(row["position_id"] or 0)
            if journal_id in closed_journal_ids:
                continue
            if exclude_order_id > 0 and order_id == exclude_order_id:
                continue
            if exclude_position_id > 0 and position_id == exclude_position_id:
                continue
            if order_id > 0 and order_id in existing_order_ids:
                continue
            if position_id > 0 and position_id in existing_position_ids:
                continue
            if self._source_family(str(row["source"] or "")) not in families:
                continue
            side = str(row["direction"] or "").strip().lower()
            if side == "long":
                long_count += 1
            elif side == "short":
                short_count += 1
        return {"active_total": long_count + short_count, "active_long": long_count, "active_short": short_count}

    def _reference_price(self, symbol: str) -> float:
        token = str(symbol or "").strip().upper()
        try:
            if token == "XAUUSD":
                from market.data_fetcher import xauusd_provider

                return _safe_float(xauusd_provider.get_current_price(), 0.0)
            if token in {"ETHUSD", "BTCUSD"}:
                from market.data_fetcher import crypto_provider

                return _safe_float(crypto_provider.get_current_price(self._worker_price_symbol(token)), 0.0)
        except Exception:
            return 0.0
        return 0.0

    def _price_sanity_guard(self, signal, *, source: str = "") -> tuple[bool, str, dict]:
        if not bool(getattr(config, "CTRADER_PRICE_SANITY_ENABLED", True)):
            return True, "", {}
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        entry = _safe_float(getattr(signal, "entry", 0.0), 0.0)
        stop_loss = _safe_float(getattr(signal, "stop_loss", 0.0), 0.0)
        take_profit = self._take_profit_for_signal(signal)
        ref = self._reference_price(symbol)
        if ref <= 0 or entry <= 0:
            return True, "", {"reference_price": ref}
        max_dev = float(getattr(config, "CTRADER_PRICE_SANITY_FX_MAX_DEVIATION_PCT", 0.03) or 0.03)
        if symbol in {"ETHUSD", "BTCUSD"}:
            max_dev = float(getattr(config, "CTRADER_PRICE_SANITY_CRYPTO_MAX_DEVIATION_PCT", 0.35) or 0.35)
        elif symbol == "XAUUSD":
            max_dev = float(getattr(config, "CTRADER_PRICE_SANITY_XAU_MAX_DEVIATION_PCT", 0.08) or 0.08)
        entry_dev = abs(entry - ref) / max(ref, 1e-8)
        sl_dev = abs(stop_loss - ref) / max(ref, 1e-8) if stop_loss > 0 else 0.0
        tp_dev = abs(take_profit - ref) / max(ref, 1e-8) if take_profit > 0 else 0.0
        meta = {
            "reference_price": round(ref, 6),
            "entry_dev_pct": round(entry_dev * 100.0, 3),
            "sl_dev_pct": round(sl_dev * 100.0, 3),
            "tp_dev_pct": round(tp_dev * 100.0, 3),
            "max_dev_pct": round(max_dev * 100.0, 3),
            "source": str(source or ""),
        }
        if max(entry_dev, sl_dev, tp_dev) > max_dev:
            return False, (
                f"price_sanity_failed: ref={ref:.4f} "
                f"entry_dev={entry_dev*100.0:.2f}% tp_dev={tp_dev*100.0:.2f}% "
                f"max={max_dev*100.0:.2f}%"
            ), meta
        return True, "", meta

    def _market_entry_drift_guard(self, signal, *, source: str = "") -> tuple[bool, str, dict]:
        if not bool(getattr(config, "CTRADER_MARKET_ENTRY_DRIFT_GUARD_ENABLED", True)):
            return True, "", {"enabled": False}
        entry_type = str(getattr(signal, "entry_type", "market") or "market").strip().lower()
        if entry_type in {"limit", "patience", "buy_stop", "sell_stop", "stop"}:
            return True, "", {"enabled": True, "applied": False, "reason": "non_market_entry"}
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        entry = _safe_float(getattr(signal, "entry", 0.0), 0.0)
        ref = self._reference_price(symbol)
        if ref <= 0 or entry <= 0:
            return True, "", {"enabled": True, "applied": False, "reference_price": ref}
        max_dev = max(0.0, float(getattr(config, "CTRADER_MARKET_ENTRY_MAX_DRIFT_PCT", 0.12) or 0.12))
        overrides = dict(getattr(config, "get_ctrader_market_entry_max_drift_symbol_overrides", lambda: {})() or {})
        if symbol in overrides:
            max_dev = max(0.0, _safe_float(overrides.get(symbol), max_dev))
        if max_dev <= 0:
            return True, "", {"enabled": True, "applied": False, "reason": "threshold_zero"}
        drift = abs(entry - ref) / max(ref, 1e-8)
        meta = {
            "enabled": True,
            "applied": True,
            "source": str(source or ""),
            "reference_price": round(ref, 6),
            "entry_dev_pct": round(drift * 100.0, 4),
            "max_dev_pct": round(max_dev * 100.0, 4),
        }
        if drift > max_dev:
            return False, (
                f"market_entry_drift_failed: ref={ref:.4f} "
                f"entry_dev={drift*100.0:.3f}% max={max_dev*100.0:.3f}%"
            ), meta
        return True, "", meta

    def _apply_openapi_exec_feature_pack(self, signal, *, source: str, symbol: str) -> None:
        """Attach recent OpenAPI SQLite microstructure stats to signal.raw_scores (pre-dispatch)."""
        sym = str(symbol or "").strip().upper()
        if not sym:
            return
        lookback = max(5, int(getattr(config, "CTRADER_EXEC_FEATURE_LOOKBACK_SEC", 32) or 32))
        max_ticks = max(4, int(getattr(config, "CTRADER_EXEC_FEATURE_MAX_TICKS", 24) or 24))
        cutoff = time.time() - float(lookback)
        pack: dict = {
            "symbol": sym,
            "source": str(source or ""),
            "lookback_sec": lookback,
            "tick_count": 0,
            "quote_age_sec": None,
            "spread_pct_median": None,
            "bid_velocity_pct": None,
            "depth_bid_sz_l1": None,
            "depth_ask_sz_l1": None,
            "depth_imbalance_l1": None,
            "db_path": str(self.db_path),
        }
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            ticks = conn.execute(
                """
                SELECT bid, ask, spread_pct, event_ts
                  FROM ctrader_spot_ticks
                 WHERE UPPER(COALESCE(symbol,''))=?
                   AND event_ts >= ?
                 ORDER BY event_ts DESC
                 LIMIT ?
                """,
                (sym, cutoff, max_ticks),
            ).fetchall()
            if ticks:
                spreads = sorted(_safe_float(r["spread_pct"], 0.0) for r in ticks if _safe_float(r["spread_pct"], 0.0) > 0)
                if spreads:
                    pack["spread_pct_median"] = round(float(spreads[len(spreads) // 2]), 6)
                ts_last = _safe_float(ticks[0]["event_ts"], 0.0)
                if ts_last > 0:
                    pack["quote_age_sec"] = round(max(0.0, time.time() - ts_last), 3)
                bids = [_safe_float(r["bid"], 0.0) for r in reversed(ticks)]
                if len(bids) >= 2:
                    first, last_b = bids[0], bids[-1]
                    mid = last_b if last_b > 0 else (first if first > 0 else 0.0)
                    if mid > 0 and first > 0:
                        pack["bid_velocity_pct"] = round((last_b - first) / mid * 100.0, 6)
            pack["tick_count"] = len(ticks)
            drows = conn.execute(
                """
                SELECT side, SUM(size) AS sz
                  FROM ctrader_depth_quotes
                 WHERE UPPER(COALESCE(symbol,''))=?
                   AND event_ts >= ?
                   AND level_index=0
                 GROUP BY side
                """,
                (sym, cutoff),
            ).fetchall()
        bid_sz = 0.0
        ask_sz = 0.0
        for r in drows:
            side = str(r["side"] or "").strip().lower()
            s = _safe_float(r["sz"], 0.0)
            if side == "bid":
                bid_sz = s
            elif side in {"ask", "sell"}:
                ask_sz = s
        if bid_sz > 0 or ask_sz > 0:
            pack["depth_bid_sz_l1"] = round(bid_sz, 6)
            pack["depth_ask_sz_l1"] = round(ask_sz, 6)
            pack["depth_imbalance_l1"] = round((bid_sz - ask_sz) / max(bid_sz + ask_sz, 1e-9), 6)
        try:
            raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
            raw_scores["openapi_exec_features"] = pack
            signal.raw_scores = raw_scores
        except Exception:
            pass

    def _take_profit_for_signal(self, signal) -> float:
        level = max(1, min(3, int(getattr(config, "CTRADER_TP_LEVEL", 1) or 1)))
        mapping = {
            1: _safe_float(getattr(signal, "take_profit_1", 0.0), 0.0),
            2: _safe_float(getattr(signal, "take_profit_2", 0.0), 0.0),
            3: _safe_float(getattr(signal, "take_profit_3", 0.0), 0.0),
        }
        tp = mapping.get(level, 0.0)
        if tp > 0:
            return tp
        for alt in (1, 2, 3):
            if mapping.get(alt, 0.0) > 0:
                return mapping[alt]
        return 0.0

    @staticmethod
    def _target_valid_for_position(direction: str, entry_price: float, take_profit: float) -> bool:
        entry = _safe_float(entry_price, 0.0)
        tp = _safe_float(take_profit, 0.0)
        side = str(direction or "").strip().lower()
        if entry <= 0 or tp <= 0 or side not in {"long", "short"}:
            return False
        if side == "long":
            return tp > entry
        return tp < entry

    @staticmethod
    def _target_more_favorable(direction: str, entry_price: float, candidate_take_profit: float, baseline_take_profit: float) -> bool:
        entry = _safe_float(entry_price, 0.0)
        candidate = _safe_float(candidate_take_profit, 0.0)
        baseline = _safe_float(baseline_take_profit, 0.0)
        side = str(direction or "").strip().lower()
        if entry <= 0 or side not in {"long", "short"}:
            return False
        if side == "long":
            return candidate > entry and baseline > entry and candidate > baseline
        return candidate > 0 and baseline > 0 and candidate < baseline < entry

    @staticmethod
    def _stop_valid_for_position(direction: str, entry_price: float, stop_loss: float) -> bool:
        entry = _safe_float(entry_price, 0.0)
        sl = _safe_float(stop_loss, 0.0)
        side = str(direction or "").strip().lower()
        if entry <= 0 or sl <= 0 or side not in {"long", "short"}:
            return False
        if side == "long":
            return sl < entry
        return sl > entry

    @staticmethod
    def _stop_valid_for_management(direction: str, current_price: float, stop_loss: float) -> bool:
        px = _safe_float(current_price, 0.0)
        sl = _safe_float(stop_loss, 0.0)
        side = str(direction or "").strip().lower()
        if px <= 0 or sl <= 0 or side not in {"long", "short"}:
            return False
        if side == "long":
            return sl < px
        return sl > px

    @staticmethod
    def _price_crossed_target(direction: str, price: float, target: float) -> bool:
        px = _safe_float(price, 0.0)
        tp = _safe_float(target, 0.0)
        side = str(direction or "").strip().lower()
        if px <= 0 or tp <= 0 or side not in {"long", "short"}:
            return False
        if side == "long":
            return px >= tp
        return px <= tp

    @staticmethod
    def _r_multiple(direction: str, entry_price: float, stop_loss: float, current_price: float) -> Optional[float]:
        side = str(direction or "").strip().lower()
        entry = _safe_float(entry_price, 0.0)
        sl = _safe_float(stop_loss, 0.0)
        px = _safe_float(current_price, 0.0)
        risk = abs(entry - sl)
        if side not in {"long", "short"} or entry <= 0 or px <= 0 or risk <= 0:
            return None
        if side == "long":
            return (px - entry) / risk
        return (entry - px) / risk

    def _family_trade_mode(self, source: str) -> str:
        family = self._source_family(source)
        if not family:
            return "neutral"
        impulse = set(getattr(config, "get_ctrader_pm_impulse_families", lambda: set())() or set())
        corrective = set(getattr(config, "get_ctrader_pm_corrective_families", lambda: set())() or set())
        if family in impulse:
            return "impulse"
        if family in corrective:
            return "corrective"
        return "neutral"

    def _profit_retrace_guard_plan(
        self,
        *,
        source: str,
        symbol: str,
        direction: str,
        position_id: int,
        entry: float,
        stop_loss: float,
        current_price: float,
        confidence: float,
        age_min: float,
        r_now: Optional[float],
    ) -> dict:
        if not bool(getattr(config, "CTRADER_PM_PROFIT_RETRACE_GUARD_ENABLED", True)):
            return {"active": False, "reason": "disabled"}
        if r_now is None:
            return {"active": False, "reason": "missing_r"}
        if int(position_id or 0) <= 0:
            return {"active": False, "reason": "position_missing"}
        min_age = max(0.0, float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_AGE_MIN", 4.0) or 4.0))
        if float(age_min) < min_age:
            return {"active": False, "reason": "too_young"}
        min_peak_r = max(0.0, float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_GUARD_MIN_PEAK_R", 0.30) or 0.30))
        retrace_trigger = max(0.01, float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_GUARD_EXIT_RETRACE_R", 0.22) or 0.22))
        peak_prev = float(self._position_peak_r.get(int(position_id), float(r_now)) or float(r_now))
        peak_now = max(peak_prev, float(r_now))
        self._position_peak_r[int(position_id)] = peak_now
        if peak_now < min_peak_r:
            return {
                "active": False,
                "reason": "peak_below_min",
                "details": {"peak_r": round(peak_now, 4), "min_peak_r": round(min_peak_r, 4)},
            }
        retrace_r = peak_now - float(r_now)
        if retrace_r < retrace_trigger:
            return {
                "active": False,
                "reason": "retrace_small",
                "details": {"peak_r": round(peak_now, 4), "r_now": round(float(r_now), 4), "retrace_r": round(retrace_r, 4)},
            }

        snapshot = self._latest_capture_snapshot(symbol=symbol, direction=direction, confidence=confidence)
        features = dict(snapshot.get("features") or {}) if isinstance(snapshot, dict) else {}
        day_type = str(features.get("day_type") or "").strip().lower()
        bar_volume_proxy = max(0.0, _safe_float(features.get("bar_volume_proxy"), 0.0))
        mid_drift_pct = _safe_float(features.get("mid_drift_pct"), 0.0)
        delta_proxy = _safe_float(features.get("delta_proxy"), 0.0)
        depth_imbalance = _safe_float(features.get("depth_imbalance"), 0.0)
        rejection_ratio = max(0.0, min(1.0, _safe_float(features.get("rejection_ratio"), 0.0)))
        max_weak_volume = max(
            0.01,
            float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_BAR_VOLUME_PROXY", 0.22) or 0.22),
        )
        max_weak_abs_drift = max(
            0.0001,
            float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_GUARD_WEAK_MAX_ABS_MID_DRIFT_PCT", 0.004) or 0.004),
        )
        weak_market = (
            bar_volume_proxy <= max_weak_volume
            and abs(mid_drift_pct) <= max_weak_abs_drift
        ) or (day_type in {"range", "rotation", "consolidation"})
        sweep_recovery_enabled = bool(
            getattr(config, "CTRADER_PM_PROFIT_RETRACE_SWEEP_RECOVERY_ENABLED", True)
        )
        sweep_min_rejection = max(
            0.0,
            float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_REJECTION_RATIO", 0.28) or 0.28),
        )
        sweep_min_volume = max(
            0.0,
            float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_BAR_VOLUME_PROXY", 0.30) or 0.30),
        )
        sweep_min_delta = max(
            0.0,
            float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DELTA_PROXY", 0.08) or 0.08),
        )
        sweep_min_imbalance = max(
            0.0,
            float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_SWEEP_MIN_DEPTH_IMBALANCE", 0.06) or 0.06),
        )
        supportive_delta = (
            (direction == "long" and delta_proxy >= sweep_min_delta)
            or (direction == "short" and delta_proxy <= (-1.0 * sweep_min_delta))
        )
        supportive_depth = (
            (direction == "long" and depth_imbalance >= sweep_min_imbalance)
            or (direction == "short" and depth_imbalance <= (-1.0 * sweep_min_imbalance))
        )
        sweep_recovery = bool(
            sweep_recovery_enabled
            and rejection_ratio >= sweep_min_rejection
            and bar_volume_proxy >= sweep_min_volume
            and (supportive_delta or supportive_depth)
        )
        family_mode = self._family_trade_mode(source)
        details = {
            "family_mode": family_mode,
            "peak_r": round(peak_now, 4),
            "r_now": round(float(r_now), 4),
            "retrace_r": round(retrace_r, 4),
            "weak_market": bool(weak_market),
            "bar_volume_proxy": round(bar_volume_proxy, 4),
            "mid_drift_pct": round(mid_drift_pct, 6),
            "delta_proxy": round(delta_proxy, 4),
            "depth_imbalance": round(depth_imbalance, 4),
            "rejection_ratio": round(rejection_ratio, 4),
            "sweep_recovery": bool(sweep_recovery),
            "day_type": day_type,
        }
        if family_mode == "impulse":
            impulse_delta = max(
                0.0,
                float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_GUARD_IMPULSE_BYPASS_MIN_DELTA_PROXY", 0.12) or 0.12),
            )
            impulse_volume = max(
                0.01,
                float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_GUARD_IMPULSE_BYPASS_MIN_BAR_VOLUME_PROXY", 0.30) or 0.30),
            )
            continuation_alive = (
                (direction == "long" and delta_proxy >= impulse_delta)
                or (direction == "short" and delta_proxy <= (-1.0 * impulse_delta))
            ) and bar_volume_proxy >= impulse_volume
            details["continuation_alive"] = bool(continuation_alive)
            if continuation_alive:
                return {"active": False, "reason": "impulse_continuation_alive", "details": details}
            risk = abs(entry - stop_loss)
            if risk <= 0:
                return {"active": False, "reason": "invalid_risk", "details": details}
            lock_r = max(
                0.0,
                float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_GUARD_IMPULSE_LOCK_R", 0.08) or 0.08),
            )
            new_sl = entry + (risk * lock_r) if direction == "long" else entry - (risk * lock_r)
            if not self._stop_valid_for_management(direction, current_price, new_sl):
                return {"active": False, "reason": "invalid_impulse_lock_stop", "details": details}
            improves = (new_sl > stop_loss) if direction == "long" else (new_sl < stop_loss)
            if not improves:
                return {"active": False, "reason": "impulse_lock_not_improving", "details": details}
            return {
                "active": True,
                "action": "tighten",
                "reason": "profit_retrace_guard_impulse_tighten",
                "new_stop_loss": round(new_sl, 4),
                "details": details,
            }

        if family_mode == "corrective":
            risk = abs(entry - stop_loss)
            if risk <= 0:
                return {"active": False, "reason": "invalid_risk", "details": details}
            lock_r = max(
                0.0,
                float(getattr(config, "CTRADER_PM_PROFIT_RETRACE_SWEEP_LOCK_R", 0.05) or 0.05),
            )
            new_sl = entry + (risk * lock_r) if direction == "long" else entry - (risk * lock_r)
            if sweep_recovery and self._stop_valid_for_management(direction, current_price, new_sl):
                improves = (new_sl > stop_loss) if direction == "long" else (new_sl < stop_loss)
                if improves:
                    return {
                        "active": True,
                        "action": "tighten",
                        "reason": "profit_retrace_guard_corrective_sweep_tighten",
                        "new_stop_loss": round(new_sl, 4),
                        "details": details,
                    }
            if weak_market:
                return {
                    "active": True,
                    "action": "close",
                    "reason": "profit_retrace_guard_close",
                    "details": details,
                }
            if self._stop_valid_for_management(direction, current_price, new_sl):
                improves = (new_sl > stop_loss) if direction == "long" else (new_sl < stop_loss)
                if improves:
                    return {
                        "active": True,
                        "action": "tighten",
                        "reason": "profit_retrace_guard_corrective_tighten",
                        "new_stop_loss": round(new_sl, 4),
                        "details": details,
                    }
        if weak_market:
            return {
                "active": True,
                "action": "close",
                "reason": "profit_retrace_guard_close",
                "details": details,
            }
        return {"active": False, "reason": "market_not_weak", "details": details}

    @staticmethod
    def _planned_rr(journal_row: Optional[sqlite3.Row]) -> float:
        if journal_row is None:
            return 0.0
        entry = _safe_float(journal_row["entry"], 0.0)
        sl = _safe_float(journal_row["stop_loss"], 0.0)
        tp = _safe_float(journal_row["take_profit"], 0.0)
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        if risk <= 0 or reward <= 0:
            return 0.0
        return reward / risk

    @staticmethod
    def _planned_risk(journal_row: Optional[sqlite3.Row], *, entry_price: float = 0.0, stop_loss: float = 0.0) -> float:
        if journal_row is not None:
            entry = _safe_float(journal_row["entry"], 0.0)
            sl = _safe_float(journal_row["stop_loss"], 0.0)
            risk = abs(entry - sl)
            if risk > 0:
                return risk
        return abs(_safe_float(entry_price, 0.0) - _safe_float(stop_loss, 0.0))

    def _active_exposure_snapshot(
        self,
        *,
        symbol: str,
        source: str = "",
        recent_sec: Optional[int] = None,
        exclude_order_id: int = 0,
        exclude_position_id: int = 0,
    ) -> dict:
        symbol_u = str(symbol or "").strip().upper()
        target_family = self._source_family(source)
        if not symbol_u:
            return {
                "positions_total": 0,
                "positions_long": 0,
                "positions_short": 0,
                "pending_orders_total": 0,
                "pending_orders_long": 0,
                "pending_orders_short": 0,
                "recent_active_total": 0,
                "recent_active_long": 0,
                "recent_active_short": 0,
                "active_total": 0,
                "active_long": 0,
                "active_short": 0,
                "family": target_family,
                "family_positions_total": 0,
                "family_positions_long": 0,
                "family_positions_short": 0,
                "family_pending_orders_total": 0,
                "family_pending_orders_long": 0,
                "family_pending_orders_short": 0,
                "family_recent_active_total": 0,
                "family_recent_active_long": 0,
                "family_recent_active_short": 0,
                "family_active_total": 0,
                "family_active_long": 0,
                "family_active_short": 0,
            }
        recent_window = max(0, int(recent_sec if recent_sec is not None else getattr(config, "CTRADER_DIRECTION_GUARD_RECENT_SEC", 900) or 900))
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            pos_rows = conn.execute(
                """
                SELECT position_id, direction, source
                  FROM ctrader_positions
                 WHERE is_open=1
                   AND UPPER(COALESCE(symbol,''))=?
                """,
                (symbol_u,),
            ).fetchall()
            order_rows = conn.execute(
                """
                SELECT order_id, direction, source
                  FROM ctrader_orders
                 WHERE is_open=1
                   AND UPPER(COALESCE(symbol,''))=?
                """,
                (symbol_u,),
            ).fetchall()
            closed_journal_ids: set[int] = set()
            if recent_window > 0:
                closed_rows = conn.execute(
                    """
                    SELECT DISTINCT journal_id
                      FROM ctrader_deals
                     WHERE has_close_detail=1
                       AND journal_id IS NOT NULL
                       AND UPPER(COALESCE(symbol,''))=?
                    """,
                    (symbol_u,),
                ).fetchall()
                closed_journal_ids = {int(row["journal_id"] or 0) for row in closed_rows if int(row["journal_id"] or 0) > 0}
            existing_position_ids = {int(row["position_id"] or 0) for row in pos_rows if int(row["position_id"] or 0) > 0}
            existing_order_ids = {int(row["order_id"] or 0) for row in order_rows if int(row["order_id"] or 0) > 0}
            recent_rows: list[sqlite3.Row] = []
            if bool(getattr(config, "CTRADER_DIRECTION_GUARD_INCLUDE_RECENT_JOURNAL", True)) and recent_window > 0:
                cutoff_ts = time.time() - float(recent_window)
                recent_rows = conn.execute(
                    """
                    SELECT id, order_id, position_id, direction, status, source
                      FROM execution_journal
                     WHERE created_ts >= ?
                       AND UPPER(COALESCE(symbol,''))=?
                       AND LOWER(COALESCE(status,'')) IN ('accepted','filled','reconciled_open')
                    """,
                    (cutoff_ts, symbol_u),
                ).fetchall()
        pos_long = 0
        pos_short = 0
        fam_pos_long = 0
        fam_pos_short = 0
        for row in pos_rows:
            position_id = int(row["position_id"] or 0)
            if exclude_position_id > 0 and position_id == exclude_position_id:
                continue
            side = str(row["direction"] or "").strip().lower()
            row_family = self._source_family(str(row["source"] or ""))
            if side == "long":
                pos_long += 1
                if target_family and row_family == target_family:
                    fam_pos_long += 1
            elif side == "short":
                pos_short += 1
                if target_family and row_family == target_family:
                    fam_pos_short += 1
        ord_long = 0
        ord_short = 0
        fam_ord_long = 0
        fam_ord_short = 0
        if bool(getattr(config, "CTRADER_DIRECTION_GUARD_INCLUDE_PENDING_ORDERS", True)):
            for row in order_rows:
                order_id = int(row["order_id"] or 0)
                if exclude_order_id > 0 and order_id == exclude_order_id:
                    continue
                side = str(row["direction"] or "").strip().lower()
                row_family = self._source_family(str(row["source"] or ""))
                if side == "long":
                    ord_long += 1
                    if target_family and row_family == target_family:
                        fam_ord_long += 1
                elif side == "short":
                    ord_short += 1
                    if target_family and row_family == target_family:
                        fam_ord_short += 1
        recent_long = 0
        recent_short = 0
        fam_recent_long = 0
        fam_recent_short = 0
        for row in recent_rows:
            journal_id = int(row["id"] or 0)
            order_id = int(row["order_id"] or 0)
            position_id = int(row["position_id"] or 0)
            if journal_id in closed_journal_ids:
                continue
            if exclude_order_id > 0 and order_id == exclude_order_id:
                continue
            if exclude_position_id > 0 and position_id == exclude_position_id:
                continue
            if order_id > 0 and order_id in existing_order_ids:
                continue
            if position_id > 0 and position_id in existing_position_ids:
                continue
            side = str(row["direction"] or "").strip().lower()
            row_family = self._source_family(str(row["source"] or ""))
            if side == "long":
                recent_long += 1
                if target_family and row_family == target_family:
                    fam_recent_long += 1
            elif side == "short":
                recent_short += 1
                if target_family and row_family == target_family:
                    fam_recent_short += 1
        return {
            "positions_total": pos_long + pos_short,
            "positions_long": pos_long,
            "positions_short": pos_short,
            "pending_orders_total": ord_long + ord_short,
            "pending_orders_long": ord_long,
            "pending_orders_short": ord_short,
            "recent_active_total": recent_long + recent_short,
            "recent_active_long": recent_long,
            "recent_active_short": recent_short,
            "active_total": pos_long + pos_short + ord_long + ord_short + recent_long + recent_short,
            "active_long": pos_long + ord_long + recent_long,
            "active_short": pos_short + ord_short + recent_short,
            "family": target_family,
            "family_positions_total": fam_pos_long + fam_pos_short,
            "family_positions_long": fam_pos_long,
            "family_positions_short": fam_pos_short,
            "family_pending_orders_total": fam_ord_long + fam_ord_short,
            "family_pending_orders_long": fam_ord_long,
            "family_pending_orders_short": fam_ord_short,
            "family_recent_active_total": fam_recent_long + fam_recent_short,
            "family_recent_active_long": fam_recent_long,
            "family_recent_active_short": fam_recent_short,
            "family_active_total": fam_pos_long + fam_pos_short + fam_ord_long + fam_ord_short + fam_recent_long + fam_recent_short,
            "family_active_long": fam_pos_long + fam_ord_long + fam_recent_long,
            "family_active_short": fam_pos_short + fam_ord_short + fam_recent_short,
        }

    def _position_direction_guard(self, *, symbol: str, direction: str, source: str = "") -> tuple[bool, str, dict]:
        if not bool(getattr(config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True)):
            return True, "", {"enabled": False}
        symbol_u = str(symbol or "").strip().upper()
        side = str(direction or "").strip().lower()
        family = self._source_family(source)
        if not symbol_u or side not in {"long", "short"}:
            return True, "", {"enabled": True, "applied": False}
        snapshot = self._active_exposure_snapshot(symbol=symbol_u, source=source)
        same_dir = int(snapshot["active_long"] if side == "long" else snapshot["active_short"])
        opposite_dir = int(snapshot["active_short"] if side == "long" else snapshot["active_long"])
        total = int(snapshot["active_total"])
        pending_same = int(snapshot["pending_orders_long"] if side == "long" else snapshot["pending_orders_short"])
        pending_total = int(snapshot["pending_orders_total"])
        family_same_dir = int(snapshot["family_active_long"] if side == "long" else snapshot["family_active_short"])
        family_pending_same = int(snapshot["family_pending_orders_long"] if side == "long" else snapshot["family_pending_orders_short"])
        meta = {
            "enabled": True,
            "symbol": symbol_u,
            "source": str(source or ""),
            "family": family,
            "same_direction_open": same_dir,
            "opposite_direction_open": opposite_dir,
            "total_open": total,
            "max_positions_per_symbol": max(1, int(getattr(config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 3) or 3)),
            "max_positions_per_direction": max(1, int(getattr(config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2) or 2)),
            "max_pending_orders_per_symbol": max(1, int(getattr(config, "CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL", 2) or 2)),
            "max_pending_orders_per_direction": max(1, int(getattr(config, "CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION", 1) or 1)),
            "max_active_per_family_symbol": max(1, int(getattr(config, "CTRADER_MAX_ACTIVE_PER_FAMILY_SYMBOL", 1) or 1)),
            "max_active_per_family_direction": max(1, int(getattr(config, "CTRADER_MAX_ACTIVE_PER_FAMILY_DIRECTION", 1) or 1)),
            "max_pending_orders_per_family_symbol": max(1, int(getattr(config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_SYMBOL", 1) or 1)),
            "max_pending_orders_per_family_direction": max(1, int(getattr(config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_DIRECTION", 1) or 1)),
            "positions_total": int(snapshot["positions_total"]),
            "pending_orders_total": pending_total,
            "recent_active_total": int(snapshot["recent_active_total"]),
            "family_active_total": int(snapshot["family_active_total"]),
            "family_pending_orders_total": int(snapshot["family_pending_orders_total"]),
        }
        parallel_state = {}
        if symbol_u == "XAUUSD":
            runtime_state = self._load_routing_runtime_state() or {}
            parallel_state = self._xau_parallel_family_state()
            if parallel_state:
                allowed_parallel_families = {
                    str(part or "").strip().lower()
                    for part in list(parallel_state.get("allowed_families") or [])
                    if str(part or "").strip()
                }
                xau_feed = dict((((runtime_state.get("opportunity_feed") or {}).get("symbols") or {}).get("XAUUSD") or {}))
                support_all_families = {
                    str(part or "").strip().lower()
                    for part in list(xau_feed.get("support_all_families") or [])
                    if str(part or "").strip()
                }
                swarm_family_count = max(len(allowed_parallel_families), len(support_all_families))
                max_same = max(
                    meta["max_positions_per_direction"],
                    int(parallel_state.get("max_same_direction_families", getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_MAX_SAME_DIRECTION", 3)) or getattr(config, "TRADING_MANAGER_XAU_PARALLEL_FAMILIES_MAX_SAME_DIRECTION", 3)),
                )
                family_routing_mode = str(((runtime_state.get("xau_family_routing") or {}).get("mode") or "")).strip().lower()
                if family_routing_mode == "swarm_support_all" and swarm_family_count > 0:
                    max_same = max(max_same, swarm_family_count)
                max_same = min(max_same, meta["max_positions_per_symbol"])
                meta["max_positions_per_direction"] = max(1, int(max_same))
                meta["max_pending_orders_per_direction"] = min(
                    max(1, int(max_same)),
                    meta["max_pending_orders_per_symbol"],
                )
                meta["parallel_state"] = dict(parallel_state)
        hedge_state = {}
        hedge_allowed = False
        hedge_group = {"active_total": 0, "active_long": 0, "active_short": 0}
        bypass_state = {}
        bypass_allowed = False
        bypass_group = {"active_total": 0, "active_long": 0, "active_short": 0}
        cluster_guard_state = {}
        if symbol_u == "XAUUSD":
            hedge_state = self._xau_hedge_transition_state()
            hedge_allowed_families = {str(part or "").strip().lower() for part in list(hedge_state.get("allowed_families") or []) if str(part or "").strip()}
            hedge_allowed = bool(hedge_state and family and family in hedge_allowed_families and opposite_dir > 0)
            if hedge_allowed:
                hedge_group = self._active_group_exposure_snapshot(
                    symbol=symbol_u,
                    families=hedge_allowed_families,
                )
                meta["hedge_state"] = dict(hedge_state)
                meta["hedge_group"] = dict(hedge_group)
            bypass_state = self._xau_opportunity_bypass_state()
            bypass_allowed_families = {str(part or "").strip().lower() for part in list(bypass_state.get("allowed_families") or []) if str(part or "").strip()}
            bypass_allowed = bool(bypass_state and family and family in bypass_allowed_families and opposite_dir > 0)
            if bypass_allowed:
                bypass_group = self._active_group_exposure_snapshot(
                    symbol=symbol_u,
                    families=bypass_allowed_families,
                )
                meta["opportunity_bypass_state"] = dict(bypass_state)
                meta["opportunity_bypass_group"] = dict(bypass_group)
            cluster_guard_state = self._xau_cluster_loss_guard_state()
            if cluster_guard_state:
                meta["cluster_loss_guard_state"] = dict(cluster_guard_state)
                blocked_direction = str(cluster_guard_state.get("blocked_direction") or "").strip().lower()
                if blocked_direction in {"long", "short"} and blocked_direction == side and not hedge_allowed and not bypass_allowed:
                    losses = int(cluster_guard_state.get("losses", 0) or 0)
                    return False, f"cluster_loss_guard:{symbol_u}:{side}:{losses}", meta
        if bool(getattr(config, "CTRADER_BLOCK_OPPOSITE_DIRECTION", True)) and opposite_dir > 0:
            if not hedge_allowed and not bypass_allowed:
                return False, f"opposite_direction_open:{symbol_u}:{opposite_dir}", meta
            max_hedge = max(1, int(hedge_state.get("max_per_symbol", getattr(config, "TRADING_MANAGER_XAU_HEDGE_LANE_MAX_PER_SYMBOL", 1)) or getattr(config, "TRADING_MANAGER_XAU_HEDGE_LANE_MAX_PER_SYMBOL", 1)))
            max_bypass = max(1, int(bypass_state.get("max_per_symbol", getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MAX_PER_SYMBOL", 2)) or getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MAX_PER_SYMBOL", 2)))
            if hedge_allowed and int(hedge_group.get("active_total", 0) or 0) >= max_hedge:
                return False, f"hedge_cap:{symbol_u}:{int(hedge_group.get('active_total', 0) or 0)}", meta
            if bypass_allowed and int(bypass_group.get("active_total", 0) or 0) >= max_bypass:
                return False, f"opportunity_bypass_cap:{symbol_u}:{int(bypass_group.get('active_total', 0) or 0)}", meta
        if family and int(snapshot["family_pending_orders_total"]) >= meta["max_pending_orders_per_family_symbol"]:
            return False, f"family_pending_order_cap:{symbol_u}:{family}:{int(snapshot['family_pending_orders_total'])}", meta
        if family and family_pending_same >= meta["max_pending_orders_per_family_direction"]:
            return False, f"family_pending_direction_cap:{symbol_u}:{family}:{family_pending_same}", meta
        if pending_total >= meta["max_pending_orders_per_symbol"]:
            return False, f"pending_order_cap:{symbol_u}:{pending_total}", meta
        if pending_same >= meta["max_pending_orders_per_direction"]:
            return False, f"pending_direction_cap:{symbol_u}:{pending_same}", meta
        if family and int(snapshot["family_active_total"]) >= meta["max_active_per_family_symbol"]:
            return False, f"family_position_cap:{symbol_u}:{family}:{int(snapshot['family_active_total'])}", meta
        if family and family_same_dir >= meta["max_active_per_family_direction"]:
            return False, f"family_direction_cap:{symbol_u}:{family}:{family_same_dir}", meta
        if total >= meta["max_positions_per_symbol"]:
            return False, f"symbol_position_cap:{symbol_u}:{total}", meta
        if same_dir >= meta["max_positions_per_direction"]:
            return False, f"direction_position_cap:{symbol_u}:{same_dir}", meta
        return True, "", meta

    def _repair_rr_for_source(self, source: str, planned_rr: float) -> float:
        token = str(source or "").strip().lower()
        base_rr = _safe_float(planned_rr, 0.0)
        if base_rr <= 0:
            base_rr = _safe_float(getattr(config, "CTRADER_PM_INVALID_TP_REPAIR_R", 0.60), 0.60)
        if ":bs:" in token or "breakout_stop" in token:
            cap = _safe_float(getattr(config, "CTRADER_PM_BREAKOUT_REPAIR_TP_R", 0.55), 0.55)
            return max(0.20, min(base_rr, cap))
        if ":pb:" in token or "pullback_limit" in token:
            floor = _safe_float(getattr(config, "CTRADER_PM_PULLBACK_REPAIR_TP_R", 0.75), 0.75)
            return max(0.25, max(base_rr, floor))
        return max(0.20, min(base_rr, 1.20))

    @staticmethod
    def _is_scheduled_canary_source(source: str) -> bool:
        return str(source or "").strip().lower() == "xauusd_scheduled:canary"

    @staticmethod
    def _is_xau_symbol(symbol: str) -> bool:
        return str(symbol or "").strip().upper() == "XAUUSD"

    @staticmethod
    def _xau_active_defense_allowed_sources() -> set[str]:
        raw = str(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_ALLOWED_SOURCES", "") or "").strip().lower()
        if not raw:
            return set()
        return {token.strip() for token in raw.split(",") if token.strip()}

    def _is_xau_active_defense_source(self, *, symbol: str, source: str) -> bool:
        if not self._is_xau_symbol(symbol):
            return False
        if not bool(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_ENABLED", True)):
            return False
        token = str(source or "").strip().lower()
        allowed = self._xau_active_defense_allowed_sources()
        if not allowed:
            return token.startswith("scalp_xauusd:") or self._is_scheduled_canary_source(token)
        return token in allowed

    def _xau_post_fill_stop_clamp_plan(
        self,
        *,
        symbol: str,
        direction: str,
        planned_entry: float,
        planned_stop_loss: float,
        live_entry: float,
        live_stop_loss: float,
        current_price: float = 0.0,
    ) -> dict:
        if not self._is_xau_symbol(symbol):
            return {"active": False}
        if not bool(getattr(config, "CTRADER_PM_XAU_POST_FILL_STOP_CLAMP_ENABLED", True)):
            return {"active": False}
        side = str(direction or "").strip().lower()
        planned_entry_f = _safe_float(planned_entry, 0.0)
        planned_stop_f = _safe_float(planned_stop_loss, 0.0)
        live_entry_f = _safe_float(live_entry, 0.0)
        live_stop_f = _safe_float(live_stop_loss, 0.0)
        px = _safe_float(current_price, 0.0)
        if side not in {"long", "short"}:
            return {"active": False, "reason": "direction_invalid"}
        if planned_entry_f <= 0 or planned_stop_f <= 0 or live_entry_f <= 0:
            return {"active": False, "reason": "planned_or_live_entry_missing"}
        if not self._stop_valid_for_position(side, live_entry_f, live_stop_f):
            return {"active": False, "reason": "live_stop_invalid"}

        planned_risk = abs(planned_entry_f - planned_stop_f)
        live_risk = abs(live_entry_f - live_stop_f)
        if planned_risk <= 0 or live_risk <= 0:
            return {"active": False, "reason": "risk_missing"}

        max_mult = max(1.0, float(getattr(config, "CTRADER_PM_XAU_POST_FILL_STOP_MAX_RISK_MULT", 1.15) or 1.15))
        max_risk = planned_risk * max_mult
        if live_risk <= max_risk:
            return {"active": False, "reason": "risk_within_limit"}

        new_sl = live_entry_f - max_risk if side == "long" else live_entry_f + max_risk
        if not self._stop_valid_for_position(side, live_entry_f, new_sl):
            return {"active": False, "reason": "clamped_stop_invalid"}
        improves = (new_sl > live_stop_f) if side == "long" else (new_sl < live_stop_f)
        if not improves:
            return {"active": False, "reason": "clamp_not_improving"}

        breached = False
        if px > 0:
            breached = (side == "long" and px <= new_sl) or (side == "short" and px >= new_sl)
        details = {
            "planned_risk": round(planned_risk, 4),
            "live_risk": round(live_risk, 4),
            "max_risk": round(max_risk, 4),
            "risk_mult": round(live_risk / max(planned_risk, 1e-9), 4),
            "planned_entry": round(planned_entry_f, 4),
            "live_entry": round(live_entry_f, 4),
            "planned_stop_loss": round(planned_stop_f, 4),
            "live_stop_loss": round(live_stop_f, 4),
        }
        return {
            "active": True,
            "action": "close" if breached else "tighten",
            "reason": "xau_post_fill_stop_clamp",
            "new_stop_loss": round(new_sl, 4),
            "details": details,
        }

    def _xau_profit_extension_plan(
        self,
        *,
        source: str,
        symbol: str,
        direction: str,
        entry: float,
        stop_loss: float,
        planned_tp: float,
        current_tp: float,
        current_price: float,
        confidence: float,
        age_min: float,
        r_now: Optional[float],
    ) -> dict:
        if not self._is_xau_symbol(symbol):
            return {"active": False}
        order_care_state = self._xau_order_care_state(symbol=symbol, source=source)
        if not order_care_state:
            if not bool(getattr(config, "CTRADER_PM_XAU_EXTENSION_ALLOW_WITHOUT_ORDER_CARE", True)):
                return {"active": False, "reason": "order_care_inactive"}
            order_care_state = {"overrides": {}}
        if not self._target_valid_for_position(direction, entry, current_tp):
            return {"active": False, "reason": "invalid_current_target"}
        if not self._price_crossed_target(direction, current_price, current_tp):
            return {"active": False, "reason": "target_not_crossed"}
        if stop_loss <= 0 or entry <= 0:
            return {"active": False, "reason": "invalid_entry_or_stop"}
        risk = abs(entry - stop_loss)
        if risk <= 0:
            return {"active": False, "reason": "invalid_risk"}

        order_care_overrides = dict(order_care_state.get("overrides") or {})
        min_age_min = max(
            0.0,
            float(
                order_care_overrides.get(
                    "extension_min_age_min",
                    getattr(config, "CTRADER_PM_XAU_EXTENSION_MIN_AGE_MIN", 0.15) or 0.15,
                )
                or getattr(config, "CTRADER_PM_XAU_EXTENSION_MIN_AGE_MIN", 0.15)
                or 0.15
            ),
        )
        if age_min < min_age_min:
            return {"active": False, "reason": "too_young"}
        _ext_conf_default = float(getattr(config, "CTRADER_PM_XAU_EXTENSION_MIN_CONFIDENCE", 70.0) or 70.0)
        min_confidence = float(order_care_overrides.get("extension_min_confidence", _ext_conf_default) or _ext_conf_default)
        if confidence < min_confidence:
            return {"active": False, "reason": "confidence_below_extend"}
        min_r = max(0.0, float(order_care_overrides.get("extension_min_r", 0.35) or 0.35))
        if (r_now is not None) and float(r_now) < min_r:
            return {"active": False, "reason": "profit_below_extend_r"}

        current_target_r = abs(current_tp - entry) / risk
        baseline_tp = planned_tp if self._target_valid_for_position(direction, entry, planned_tp) else current_tp
        baseline_target_r = abs(baseline_tp - entry) / risk
        if current_target_r + 1e-9 < baseline_target_r:
            return {"active": False, "reason": "target_trimmed"}
        if self._target_more_favorable(direction, entry, current_tp, baseline_tp):
            return {"active": False, "reason": "already_extended"}

        snapshot = self._latest_capture_snapshot(symbol=symbol, direction=direction, confidence=confidence)
        features = dict(snapshot.get("features") or {})
        if not bool(snapshot.get("ok")) or not features:
            return {"active": False, "reason": str(snapshot.get("status") or "no_capture")}

        day_type = str(features.get("day_type") or "trend").strip().lower() or "trend"
        delta_proxy = _safe_float(features.get("delta_proxy"), 0.0)
        imbalance = _safe_float(features.get("depth_imbalance"), 0.0)
        drift_pct = _safe_float(features.get("mid_drift_pct"), 0.0)
        rejection_ratio = max(0.0, min(1.0, _safe_float(features.get("rejection_ratio"), 0.0)))
        bar_volume_proxy = max(0.0, _safe_float(features.get("bar_volume_proxy"), 0.0))

        if direction == "long":
            supportive_delta = max(0.0, delta_proxy)
            supportive_imbalance = max(0.0, imbalance)
            supportive_drift = max(0.0, drift_pct)
        else:
            supportive_delta = max(0.0, -1.0 * delta_proxy)
            supportive_imbalance = max(0.0, -1.0 * imbalance)
            supportive_drift = max(0.0, -1.0 * drift_pct)

        min_bar_volume = float(order_care_overrides.get("extension_min_bar_volume_proxy", 0.40) or 0.40)
        min_supportive_delta = float(order_care_overrides.get("extension_min_supportive_delta", 0.12) or 0.12)
        min_supportive_imbalance = float(order_care_overrides.get("extension_min_supportive_imbalance", 0.10) or 0.10)
        min_supportive_drift = float(order_care_overrides.get("extension_min_supportive_drift_pct", 0.010) or 0.010)
        max_rejection = float(order_care_overrides.get("extension_max_rejection", 0.16) or 0.16)
        extension_score = max(1, int(order_care_overrides.get("extension_score", 5) or 5))

        score = 0
        reasons: list[str] = []
        if bar_volume_proxy >= min_bar_volume:
            score += 1
        else:
            reasons.append("volume_light")
        if supportive_delta >= min_supportive_delta:
            score += 1
        else:
            reasons.append("delta_not_supportive")
        if supportive_imbalance >= min_supportive_imbalance:
            score += 1
        else:
            reasons.append("imbalance_not_supportive")
        if supportive_drift >= min_supportive_drift:
            score += 1
        else:
            reasons.append("drift_not_supportive")
        if rejection_ratio <= max_rejection:
            score += 1
        else:
            reasons.append("rejection_too_high")
        if day_type in {"trend", "fast_expansion", "repricing"}:
            score += 1
        else:
            reasons.append("day_type_not_supportive")

        # DOM favorable liquidity boost (+1 score when DOM supports direction)
        dom_favorable_details = {}
        if bool(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_ENABLED", True)):
            try:
                from analysis.dom_liquidity_shift import analyze_dom_liquidity
                import sqlite3 as _sqlite3_ext
                _ext_db = Path(__file__).resolve().parent.parent / "data" / "ctrader_openapi.db"
                if _ext_db.exists():
                    with _sqlite3_ext.connect(str(_ext_db), timeout=5) as _ext_conn:
                        _ext_conn.row_factory = _sqlite3_ext.Row
                        dom_result = analyze_dom_liquidity(_ext_conn, symbol=symbol, direction=direction, lookback_min=max(5, int(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_LOOKBACK_MIN", 30) or 30)), max_runs=max(2, int(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_MAX_RUNS", 6) or 6)))
                    if bool(dom_result.get("ok")):
                        favorable = dict(dom_result.get("favorable") or {})
                        fav_score = int(favorable.get("favorable_score", 0) or 0)
                        if fav_score >= 2:
                            score += 1
                            reasons.append("dom_liquidity_favorable")
                        dom_favorable_details = {"dom_favorable_score": fav_score, "dom_strength": str(favorable.get("strength", "") or ""), "dom_recommendation": str(favorable.get("recommendation", "") or "")}
            except Exception:
                pass

        details = {
            "run_id": str(snapshot.get("run_id") or ""),
            "day_type": day_type,
            "score": int(score),
            "current_target_r": round(current_target_r, 4),
            "baseline_target_r": round(baseline_target_r, 4),
            "bar_volume_proxy": round(bar_volume_proxy, 4),
            "supportive_delta": round(supportive_delta, 4),
            "supportive_imbalance": round(supportive_imbalance, 4),
            "supportive_drift_pct": round(supportive_drift, 5),
            "rejection_ratio": round(rejection_ratio, 4),
            "reasons": list(reasons),
        }
        if dom_favorable_details:
            details["dom_liquidity"] = dom_favorable_details
        if order_care_state:
            details["order_care_mode"] = str(order_care_state.get("mode") or "")
        if score < extension_score:
            return {"active": False, "reason": "score_below_extend", "details": details}

        base_tp_r = max(0.25, float(order_care_overrides.get("extension_tp_r", 1.10) or 1.10))
        base_step_r = max(0.10, float(order_care_overrides.get("extension_step_r", 0.25) or 0.25))
        lock_r = max(0.01, float(order_care_overrides.get("extension_lock_r", 0.18) or 0.18))

        # ── Momentum-adaptive step_r ──────────────────────────────────────
        # Assess momentum strength from snapshot features
        momentum_favorable = 0
        if bar_volume_proxy >= 0.50:
            momentum_favorable += 1
        if supportive_delta >= 0.15:
            momentum_favorable += 1
        if supportive_imbalance >= 0.12:
            momentum_favorable += 1
        if supportive_drift >= 0.015:
            momentum_favorable += 1
        if rejection_ratio <= 0.12:
            momentum_favorable += 1

        if momentum_favorable >= 4:
            step_r = base_step_r + 0.10  # strong momentum → bigger extension
            momentum_label = "strong"
        elif momentum_favorable >= 2:
            step_r = base_step_r         # moderate → default
            momentum_label = "moderate"
        else:
            step_r = max(0.10, base_step_r - 0.10)  # weak momentum → smaller extension
            momentum_label = "weak"

        details["momentum_adaptive"] = {
            "favorable_count": momentum_favorable,
            "momentum_label": momentum_label,
            "step_r": round(step_r, 2),
            "base_step_r": round(base_step_r, 2),
        }
        target_r = max(base_tp_r, current_target_r + step_r)
        new_tp = entry + (risk * target_r) if direction == "long" else entry - (risk * target_r)
        if not self._target_valid_for_position(direction, entry, new_tp):
            return {"active": False, "reason": "invalid_extension_target", "details": details}
        if not self._target_more_favorable(direction, entry, new_tp, current_tp):
            return {"active": False, "reason": "extension_not_improving_target", "details": details}
        if self._price_crossed_target(direction, current_price, new_tp):
            return {"active": False, "reason": "extension_target_already_crossed", "details": details}

        candidate_sl = entry + (risk * lock_r) if direction == "long" else entry - (risk * lock_r)
        if direction == "long":
            new_sl = max(stop_loss, candidate_sl)
        else:
            new_sl = min(stop_loss, candidate_sl)
        if not self._stop_valid_for_position(direction, entry, new_sl):
            new_sl = stop_loss

        details["target_r"] = round(target_r, 4)
        return {
            "active": True,
            "action": "extend",
            "reason": "xau_profit_extension",
            "new_stop_loss": round(new_sl, 4),
            "new_take_profit": round(new_tp, 4),
            "details": details,
        }

    def _xau_active_defense_plan(
        self,
        *,
        source: str,
        symbol: str,
        direction: str,
        entry: float,
        stop_loss: float,
        target_tp: float,
        current_price: float,
        confidence: float,
        age_min: float,
        r_now: Optional[float],
    ) -> dict:
        if not self._is_xau_active_defense_source(symbol=symbol, source=source):
            return {"active": False}
        order_care_state = self._xau_order_care_state(symbol=symbol, source=source)
        order_care_overrides = dict(order_care_state.get("overrides") or {})
        min_age_min = float(order_care_overrides.get("min_age_min", getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_AGE_MIN", 2.0) or 2.0) or 2.0)
        if age_min < min_age_min:
            return {"active": False, "reason": "too_young"}
        snapshot = self._latest_capture_snapshot(symbol=symbol, direction=direction, confidence=confidence)
        features = dict(snapshot.get("features") or {})
        if not bool(snapshot.get("ok")) or not features:
            return {"active": False, "reason": str(snapshot.get("status") or "no_capture")}

        day_type = str(features.get("day_type") or "trend").strip().lower() or "trend"
        delta_proxy = _safe_float(features.get("delta_proxy"), 0.0)
        imbalance = _safe_float(features.get("depth_imbalance"), 0.0)
        drift_pct = _safe_float(features.get("mid_drift_pct"), 0.0)
        rejection_ratio = max(0.0, min(1.0, _safe_float(features.get("rejection_ratio"), 0.0)))
        bar_volume_proxy = max(0.0, _safe_float(features.get("bar_volume_proxy"), 0.0))

        if direction == "long":
            adverse_delta = max(0.0, -1.0 * delta_proxy)
            adverse_imbalance = max(0.0, -1.0 * imbalance)
            adverse_drift = max(0.0, -1.0 * drift_pct)
        else:
            adverse_delta = max(0.0, delta_proxy)
            adverse_imbalance = max(0.0, imbalance)
            adverse_drift = max(0.0, drift_pct)

        score = 0
        reasons: list[str] = []
        if bar_volume_proxy >= float(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_BAR_VOLUME_PROXY", 0.32) or 0.32):
            score += 1
        else:
            reasons.append("volume_light")
        if adverse_delta >= float(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_ADVERSE_DELTA", 0.10) or 0.10):
            score += 1
        else:
            reasons.append("delta_not_adverse")
        if adverse_imbalance >= float(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_ADVERSE_IMBALANCE", 0.08) or 0.08):
            score += 1
        else:
            reasons.append("imbalance_not_adverse")
        if adverse_drift >= float(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_ADVERSE_DRIFT_PCT", 0.010) or 0.010):
            score += 1
        else:
            reasons.append("drift_not_adverse")
        if rejection_ratio <= float(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_MAX_REJECTION", 0.20) or 0.20):
            score += 1
        else:
            reasons.append("rejection_too_high")
        if day_type in {"repricing", "fast_expansion", "panic_spread"}:
            score += 1

        # DOM liquidity shift (Tier 2 enhancement)
        dom_shift_details = {}
        if bool(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_ENABLED", True)):
            try:
                from analysis.dom_liquidity_shift import analyze_dom_liquidity
                import sqlite3 as _sqlite3_dom
                _dom_db = Path(__file__).resolve().parent.parent / "data" / "ctrader_openapi.db"
                if _dom_db.exists():
                    with _sqlite3_dom.connect(str(_dom_db), timeout=5) as _dom_conn:
                        _dom_conn.row_factory = _sqlite3_dom.Row
                        dom_result = analyze_dom_liquidity(_dom_conn, symbol=symbol, direction=direction, lookback_min=max(5, int(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_LOOKBACK_MIN", 30) or 30)), max_runs=max(2, int(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_MAX_RUNS", 6) or 6)))
                    if bool(dom_result.get("ok")):
                        adverse = dict(dom_result.get("adverse") or {})
                        dom_adverse_score = int(adverse.get("adverse_score", 0) or 0)
                        if dom_adverse_score >= 2:
                            score += 1
                            reasons.append("dom_liquidity_adverse")
                        dom_shift_details = {"dom_adverse_score": dom_adverse_score, "dom_severity": str(adverse.get("severity", "") or ""), "dom_recommendation": str(adverse.get("recommendation", "") or "")}
            except Exception:
                pass

        details = {
            "run_id": str(snapshot.get("run_id") or ""),
            "day_type": day_type,
            "score": int(score),
            "bar_volume_proxy": round(bar_volume_proxy, 4),
            "adverse_delta": round(adverse_delta, 4),
            "adverse_imbalance": round(adverse_imbalance, 4),
            "adverse_drift_pct": round(adverse_drift, 5),
            "rejection_ratio": round(rejection_ratio, 4),
            "reasons": list(reasons),
        }
        if dom_shift_details:
            details["dom_liquidity"] = dom_shift_details
        if order_care_state:
            details["order_care_mode"] = str(order_care_state.get("mode") or "")
        if stop_loss <= 0 or entry <= 0:
            return {"active": False, "reason": "invalid_entry_or_stop", "details": details}
        risk = abs(entry - stop_loss)
        if risk <= 0:
            return {"active": False, "reason": "invalid_risk", "details": details}

        if bool(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_LOSS_CUT_ENABLED", True)) and (r_now is not None):
            loss_cut_r = float(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_LOSS_CUT_R", -0.28) or -0.28)
            loss_cut_min_score = max(1, int(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_LOSS_CUT_MIN_SCORE", 3) or 3))
            if float(r_now) <= loss_cut_r and int(score) >= loss_cut_min_score:
                return {
                    "active": True,
                    "action": "close",
                    "reason": "xau_active_defense_loss_cut",
                    "details": {
                        **details,
                        "loss_cut_r_threshold": round(loss_cut_r, 4),
                        "loss_cut_min_score": loss_cut_min_score,
                    },
                }

        close_score = max(1, int(order_care_overrides.get("close_score", getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_CLOSE_SCORE", 5) or 5) or 5))
        close_max_r = float(order_care_overrides.get("close_max_r", getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_CLOSE_MAX_R", 0.20) or 0.20) or 0.20)
        tighten_score = max(1, int(order_care_overrides.get("tighten_score", getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_TIGHTEN_SCORE", 3) or 3) or 3))
        stop_keep_r = max(0.05, float(order_care_overrides.get("stop_keep_r", getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_TIGHTEN_STOP_KEEP_R", 0.42) or 0.42) or 0.42))
        profit_lock_r = max(0.0, float(order_care_overrides.get("profit_lock_r", getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_PROFIT_LOCK_R", 0.05) or 0.05) or 0.05))
        trim_tp_r = max(0.10, float(order_care_overrides.get("trim_tp_r", getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_TRIM_TP_R", 0.55) or 0.55) or 0.55))
        r_current = float(r_now) if r_now is not None else None
        profit_seek_enabled = bool(getattr(config, "CTRADER_PM_XAU_PROFIT_SEEKING_ENABLED", True))
        profit_seek_min_r = max(0.0, float(getattr(config, "CTRADER_PM_XAU_PROFIT_SEEKING_MIN_R", 0.15) or 0.15))
        profit_seek_active = bool(profit_seek_enabled and r_current is not None and r_current >= profit_seek_min_r)

        if (r_current is not None) and score >= close_score and r_current <= close_max_r:
            if profit_seek_active:
                base_lock_r = max(
                    profit_lock_r,
                    max(0.0, float(getattr(config, "CTRADER_PM_XAU_PROFIT_SEEKING_LOCK_R", 0.08) or 0.08)),
                )
                lock_buffer_r = max(
                    0.0,
                    float(getattr(config, "CTRADER_PM_XAU_PROFIT_SEEKING_LOCK_BUFFER_R", 0.03) or 0.03),
                )
                lock_r = min(base_lock_r, max(0.0, r_current - lock_buffer_r))
                if lock_r > 0.0:
                    new_sl = entry + (risk * lock_r) if direction == "long" else entry - (risk * lock_r)
                    if direction == "long":
                        new_sl = max(stop_loss, new_sl)
                    else:
                        new_sl = min(stop_loss, new_sl)
                    improves = (new_sl > stop_loss) if direction == "long" else (new_sl < stop_loss)
                    if improves and self._stop_valid_for_position(direction, entry, new_sl):
                        return {
                            "active": True,
                            "action": "tighten",
                            "reason": "xau_active_defense_profit_protect",
                            "new_stop_loss": round(new_sl, 4),
                            "new_take_profit": round(target_tp, 4) if self._target_valid_for_position(direction, entry, target_tp) else 0.0,
                            "details": {
                                **details,
                                "close_suppressed": True,
                                "profit_seek_min_r": round(profit_seek_min_r, 4),
                                "profit_lock_r": round(lock_r, 4),
                            },
                        }
                return {
                    "active": False,
                    "reason": "profit_seek_close_suppressed_no_valid_lock",
                    "details": {
                        **details,
                        "close_suppressed": True,
                        "profit_seek_min_r": round(profit_seek_min_r, 4),
                    },
                }
            return {
                "active": True,
                "action": "close",
                "reason": "xau_active_defense_close",
                "details": details,
            }
        if score < tighten_score:
            return {"active": False, "reason": "score_below_tighten", "details": details}

        if (r_now is not None) and float(r_now) > 0:
            new_sl = entry + (risk * profit_lock_r) if direction == "long" else entry - (risk * profit_lock_r)
        else:
            new_sl = entry - (risk * stop_keep_r) if direction == "long" else entry + (risk * stop_keep_r)
        if direction == "long":
            new_sl = max(stop_loss, new_sl)
        else:
            new_sl = min(stop_loss, new_sl)
        new_tp = target_tp
        if self._target_valid_for_position(direction, entry, target_tp):
            trimmed_tp = entry + (risk * trim_tp_r) if direction == "long" else entry - (risk * trim_tp_r)
            if (not profit_seek_active) and self._target_valid_for_position(direction, entry, trimmed_tp):
                if abs(trimmed_tp - entry) < abs(target_tp - entry):
                    new_tp = trimmed_tp
            elif profit_seek_active:
                details["tp_trim_suppressed_profit_seeking"] = True

        breached = (direction == "long" and current_price <= new_sl) or (direction == "short" and current_price >= new_sl)
        if breached:
            return {
                "active": True,
                "action": "close",
                "reason": "xau_active_defense_breached_tightened_stop",
                "new_stop_loss": round(new_sl, 4),
                "new_take_profit": round(new_tp, 4) if self._target_valid_for_position(direction, entry, new_tp) else 0.0,
                "details": details,
            }
        return {
            "active": True,
            "action": "tighten",
            "reason": "xau_active_defense_tighten",
            "new_stop_loss": round(new_sl, 4),
            "new_take_profit": round(new_tp, 4) if self._target_valid_for_position(direction, entry, new_tp) else 0.0,
            "details": details,
        }

    def _xau_momentum_exhaustion_lock(
        self,
        *,
        source: str,
        symbol: str,
        direction: str,
        entry: float,
        stop_loss: float,
        current_price: float,
        confidence: float,
        age_min: float,
        r_now: Optional[float],
    ) -> dict:
        """
        Detect momentum exhaustion and lock profit BEFORE it evaporates.

        Problem: trade reaches +R profit, momentum dies (delta reverses, volume dries,
        drift goes adverse), but system waits for price to hit TP or SL — profit evaporates.

        Solution: When all momentum signals are exhausted AND trade is profitable,
        tighten SL to lock a portion of the profit immediately.

        Triggers when:
          - r_now > 0 (trade in profit)
          - age_min > min_age (not too young)
          - At least 3 of 5 adverse signals fire simultaneously:
              1. Delta reversed against position
              2. Volume dying (bar_volume_proxy < 0.25)
              3. Adverse drift
              4. High rejection ratio (rejections >= 0.25)
              5. Day type switched to range/rotation (non-trending)
        """
        if not self._is_xau_symbol(symbol):
            return {"active": False}
        if r_now is None or r_now <= 0.15:
            return {"active": False, "reason": "not_in_profit"}
        order_care_state = self._xau_order_care_state(symbol=symbol, source=source)
        order_care_overrides = dict(order_care_state.get("overrides") or {}) if order_care_state else {}

        min_age = float(order_care_overrides.get(
            "exhaustion_min_age_min",
            getattr(config, "CTRADER_PM_XAU_EXHAUSTION_MIN_AGE_MIN", 3.0) or 3.0,
        ) or 3.0)
        if age_min < min_age:
            return {"active": False, "reason": "too_young"}

        snapshot = self._latest_capture_snapshot(symbol=symbol, direction=direction, confidence=confidence)
        features = dict(snapshot.get("features") or {})
        if not bool(snapshot.get("ok")) or not features:
            return {"active": False, "reason": str(snapshot.get("status") or "no_capture")}

        day_type = str(features.get("day_type") or "trend").strip().lower() or "trend"
        delta_proxy = _safe_float(features.get("delta_proxy"), 0.0)
        imbalance = _safe_float(features.get("depth_imbalance"), 0.0)
        drift_pct = _safe_float(features.get("mid_drift_pct"), 0.0)
        rejection_ratio = max(0.0, min(1.0, _safe_float(features.get("rejection_ratio"), 0.0)))
        bar_volume_proxy = max(0.0, _safe_float(features.get("bar_volume_proxy"), 0.0))

        if direction == "long":
            adverse_delta = max(0.0, -1.0 * delta_proxy)
            adverse_drift = max(0.0, -1.0 * drift_pct)
        else:
            adverse_delta = max(0.0, delta_proxy)
            adverse_drift = max(0.0, drift_pct)

        # Count exhaustion signals
        exhaustion_signals = 0
        reasons: list[str] = []

        delta_threshold = float(order_care_overrides.get(
            "exhaustion_adverse_delta", getattr(config, "CTRADER_PM_XAU_EXHAUSTION_ADVERSE_DELTA", 0.08) or 0.08,
        ) or 0.08)
        if adverse_delta >= delta_threshold:
            exhaustion_signals += 1
            reasons.append("delta_reversed")

        vol_threshold = float(order_care_overrides.get(
            "exhaustion_max_volume", getattr(config, "CTRADER_PM_XAU_EXHAUSTION_MAX_VOLUME", 0.25) or 0.25,
        ) or 0.25)
        if bar_volume_proxy < vol_threshold:
            exhaustion_signals += 1
            reasons.append("volume_dying")

        drift_threshold = float(order_care_overrides.get(
            "exhaustion_adverse_drift", getattr(config, "CTRADER_PM_XAU_EXHAUSTION_ADVERSE_DRIFT", 0.008) or 0.008,
        ) or 0.008)
        if adverse_drift >= drift_threshold:
            exhaustion_signals += 1
            reasons.append("drift_adverse")

        rejection_threshold = float(order_care_overrides.get(
            "exhaustion_max_rejection", getattr(config, "CTRADER_PM_XAU_EXHAUSTION_MAX_REJECTION", 0.25) or 0.25,
        ) or 0.25)
        if rejection_ratio >= rejection_threshold:
            exhaustion_signals += 1
            reasons.append("high_rejection")

        if day_type in {"range", "rotation", "consolidation"}:
            exhaustion_signals += 1
            reasons.append("day_type_non_trending")

        required_signals = int(order_care_overrides.get(
            "exhaustion_required_signals",
            getattr(config, "CTRADER_PM_XAU_EXHAUSTION_REQUIRED_SIGNALS", 3) or 3,
        ) or 3)
        if exhaustion_signals < required_signals:
            return {"active": False, "reason": "exhaustion_not_confirmed", "details": {
                "exhaustion_signals": exhaustion_signals,
                "required": required_signals,
                "reasons": reasons,
            }}

        # ── Lock profit: tighten SL based on current R-multiple ──────────
        risk = abs(entry - stop_loss)
        if risk <= 0:
            return {"active": False, "reason": "invalid_risk"}

        # Lock percentage scales with R: higher R → lock more
        if r_now >= 1.5:
            lock_pct = 0.70   # 70% of risk locked at 1.5R+
        elif r_now >= 1.0:
            lock_pct = 0.55   # 55% at 1.0R+
        elif r_now >= 0.5:
            lock_pct = 0.35   # 35% at 0.5R+
        else:
            lock_pct = 0.15   # 15% at 0.15R+

        keep_risk = risk * (1.0 - lock_pct)
        if direction == "long":
            new_sl = entry - keep_risk
        else:
            new_sl = entry + keep_risk

        if not self._stop_valid_for_position(direction, entry, new_sl):
            return {"active": False, "reason": "invalid_new_sl"}
        improves = (new_sl > stop_loss) if direction == "long" else (new_sl < stop_loss)
        if not improves:
            return {"active": False, "reason": "sl_not_improving"}

        details = {
            "exhaustion_signals": exhaustion_signals,
            "required": required_signals,
            "reasons": reasons,
            "r_now": round(r_now, 4),
            "lock_pct": lock_pct,
            "day_type": day_type,
            "delta_proxy": round(delta_proxy, 4),
            "bar_volume_proxy": round(bar_volume_proxy, 4),
            "adverse_drift": round(adverse_drift, 5),
            "rejection_ratio": round(rejection_ratio, 4),
        }
        logger.info(
            "[PM:ExhaustionLock] %s %s | r_now=%.2f | signals=%d/%d | lock=%d%% | new_sl=%.2f | reasons=%s",
            symbol, direction, r_now, exhaustion_signals, required_signals,
            int(lock_pct * 100), new_sl, reasons,
        )
        return {
            "active": True,
            "action": "tighten",
            "reason": "xau_momentum_exhaustion_lock",
            "new_stop_loss": round(new_sl, 4),
            "new_take_profit": 0.0,  # keep existing TP
            "details": details,
        }

    def _crypto_dom_defense_plan(
        self,
        *,
        symbol: str,
        direction: str,
        entry: float,
        stop_loss: float,
        current_price: float,
        r_now: Optional[float],
        age_min: float,
    ) -> dict:
        """DOM-only active defense for non-XAU symbols (BTC/ETH).

        Lighter than full XAU active defense — only checks DOM liquidity shift.
        Tightens stop or closes when DOM shows severe adverse liquidity.

        Anti-MM-trap safeguards:
        1. Profit buffer: skip tighten when position is healthy (r_now > 0.5R)
           — if position is working, DOM blip is likely MM noise, not real shift
        2. Require 3+ snapshots: more data points for reliable shift signal
        3. Close only when severe + already losing (r_now <= 0.15)
        4. Never tighten past breakeven from adverse alone
        5. Conservative keep_r (50-65% of risk) — leaves room for normal volatility
        """
        if self._is_xau_symbol(symbol):
            return {"active": False, "reason": "xau_uses_full_active_defense"}
        if not bool(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_ENABLED", True)):
            return {"active": False, "reason": "dom_disabled"}
        min_age = max(1.0, float(getattr(config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_MIN_AGE_MIN", 2.0) or 2.0))
        if age_min < min_age:
            return {"active": False, "reason": "too_young"}
        # Safeguard 1: Profit buffer — position is working well, don't panic on DOM noise
        profit_buffer_r = float(getattr(config, "CRYPTO_DOM_DEFENSE_PROFIT_BUFFER_R", 0.50) or 0.50)
        if (r_now is not None) and float(r_now) > profit_buffer_r:
            return {"active": False, "reason": "position_healthy_skip_dom"}
        if stop_loss <= 0 or entry <= 0:
            return {"active": False, "reason": "invalid_entry_or_stop"}
        risk = abs(entry - stop_loss)
        if risk <= 0:
            return {"active": False, "reason": "invalid_risk"}
        try:
            from analysis.dom_liquidity_shift import analyze_dom_liquidity
            import sqlite3 as _sqlite3_cdom
            _cdom_db = Path(__file__).resolve().parent.parent / "data" / "ctrader_openapi.db"
            if not _cdom_db.exists():
                return {"active": False, "reason": "no_db"}
            with _sqlite3_cdom.connect(str(_cdom_db), timeout=5) as _cdom_conn:
                _cdom_conn.row_factory = _sqlite3_cdom.Row
                dom_result = analyze_dom_liquidity(_cdom_conn, symbol=symbol, direction=direction, lookback_min=max(5, int(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_LOOKBACK_MIN", 30) or 30)), max_runs=max(2, int(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_MAX_RUNS", 6) or 6)))
            if not bool(dom_result.get("ok")):
                return {"active": False, "reason": "dom_no_data"}
            # Safeguard 2: Require 3+ snapshots for reliable shift signal
            snapshots_used = int(dom_result.get("snapshots_used", 0) or 0)
            if snapshots_used < 3:
                return {"active": False, "reason": "insufficient_snapshots", "details": {"snapshots_used": snapshots_used}}
            adverse = dict(dom_result.get("adverse") or {})
            adverse_score = int(adverse.get("adverse_score", 0) or 0)
            severity = str(adverse.get("severity", "none") or "none")
            recommendation = str(adverse.get("recommendation", "hold") or "hold")
            details = {"dom_adverse_score": adverse_score, "dom_severity": severity, "dom_recommendation": recommendation, "symbol": symbol, "snapshots_used": snapshots_used}
            # Safeguard 3: Close only when severe + already in loss/near breakeven
            if severity == "severe" and (r_now is not None) and float(r_now) <= 0.15:
                return {"active": True, "action": "close", "reason": "crypto_dom_defense_close", "details": details}
            # Safeguard 4+5: Conservative tighten — only score 3 (severe), keep 65% risk
            # Score 2 (moderate): keep 65% of risk — leaves plenty of room
            if adverse_score >= 3:
                keep_r = 0.50
            elif adverse_score >= 2:
                keep_r = 0.65
            else:
                return {"active": False, "reason": "dom_not_adverse", "details": details}
            new_sl = entry - (risk * keep_r) if direction == "long" else entry + (risk * keep_r)
            if direction == "long":
                new_sl = max(stop_loss, new_sl)
            else:
                new_sl = min(stop_loss, new_sl)
            return {"active": True, "action": "tighten", "reason": "crypto_dom_defense_tighten", "new_stop_loss": round(new_sl, 4), "new_take_profit": 0.0, "details": details}
        except Exception:
            return {"active": False, "reason": "dom_error"}

    def _crypto_dom_tp_extension_plan(
        self,
        *,
        symbol: str,
        direction: str,
        entry: float,
        stop_loss: float,
        planned_tp: float,
        current_tp: float,
        current_price: float,
        r_now: Optional[float],
        age_min: float,
    ) -> dict:
        """DOM-based TP extension for BTC/ETH when liquidity is favorable.

        When DOM shows favorable conditions (support building for longs,
        resistance building for shorts), extend TP to capture more profit.

        Anti-MM-trap safeguards:
        1. Require 3+ snapshots: avoid spoofing on 1-2 snapshot window
        2. Max extension cap: 3.0R max to prevent chasing unrealistic targets
        3. lock_r always locks profit above entry when extending
        4. Only extend when already in meaningful profit (r_now >= 0.5)
        """
        if self._is_xau_symbol(symbol):
            return {"active": False, "reason": "xau_uses_full_extension"}
        if not bool(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_ENABLED", True)):
            return {"active": False, "reason": "dom_disabled"}
        if stop_loss <= 0 or entry <= 0:
            return {"active": False, "reason": "invalid_entry_or_stop"}
        risk = abs(entry - stop_loss)
        if risk <= 0:
            return {"active": False, "reason": "invalid_risk"}
        if not self._target_valid_for_position(direction, entry, current_tp):
            return {"active": False, "reason": "invalid_current_target"}
        if not self._price_crossed_target(direction, current_price, current_tp):
            return {"active": False, "reason": "target_not_crossed"}
        # Safeguard 4: must be in meaningful profit — don't extend near breakeven
        if (r_now is not None) and float(r_now) < 0.5:
            return {"active": False, "reason": "profit_too_small"}
        if age_min < 1.0:
            return {"active": False, "reason": "too_young"}
        current_target_r = abs(current_tp - entry) / risk
        # Safeguard 2: cap max extension
        max_extension_r = float(getattr(config, "CRYPTO_DOM_TP_MAX_EXTENSION_R", 3.0) or 3.0)
        if current_target_r >= max_extension_r:
            return {"active": False, "reason": "max_extension_reached", "details": {"current_target_r": round(current_target_r, 4), "max_r": max_extension_r}}
        try:
            from analysis.dom_liquidity_shift import analyze_dom_liquidity
            import sqlite3 as _sqlite3_cext
            _cext_db = Path(__file__).resolve().parent.parent / "data" / "ctrader_openapi.db"
            if not _cext_db.exists():
                return {"active": False, "reason": "no_db"}
            with _sqlite3_cext.connect(str(_cext_db), timeout=5) as _cext_conn:
                _cext_conn.row_factory = _sqlite3_cext.Row
                dom_result = analyze_dom_liquidity(_cext_conn, symbol=symbol, direction=direction, lookback_min=max(5, int(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_LOOKBACK_MIN", 30) or 30)), max_runs=max(2, int(getattr(config, "XAU_DOM_LIQUIDITY_SHIFT_MAX_RUNS", 6) or 6)))
            if not bool(dom_result.get("ok")):
                return {"active": False, "reason": "dom_no_data"}
            # Safeguard 1: Require 3+ snapshots for reliable favorable signal
            snapshots_used = int(dom_result.get("snapshots_used", 0) or 0)
            if snapshots_used < 3:
                return {"active": False, "reason": "insufficient_snapshots", "details": {"snapshots_used": snapshots_used}}
            favorable = dict(dom_result.get("favorable") or {})
            fav_score = int(favorable.get("favorable_score", 0) or 0)
            strength = str(favorable.get("strength", "none") or "none")
            if fav_score < 2:
                return {"active": False, "reason": "dom_not_favorable", "details": {"dom_favorable_score": fav_score, "dom_strength": strength}}
            step_r = 0.35 if strength == "strong" else 0.25
            lock_r = 0.15 if strength == "strong" else 0.10
            target_r = min(current_target_r + step_r, max_extension_r)
            target_r = max(target_r, 1.0)
            new_tp = entry + (risk * target_r) if direction == "long" else entry - (risk * target_r)
            if not self._target_valid_for_position(direction, entry, new_tp):
                return {"active": False, "reason": "invalid_extension_target"}
            if not self._target_more_favorable(direction, entry, new_tp, current_tp):
                return {"active": False, "reason": "extension_not_improving"}
            if self._price_crossed_target(direction, current_price, new_tp):
                return {"active": False, "reason": "extension_already_crossed"}
            # Safeguard 3: lock profit above entry
            candidate_sl = entry + (risk * lock_r) if direction == "long" else entry - (risk * lock_r)
            new_sl = max(stop_loss, candidate_sl) if direction == "long" else min(stop_loss, candidate_sl)
            if not self._stop_valid_for_position(direction, entry, new_sl):
                new_sl = stop_loss
            details = {"dom_favorable_score": fav_score, "dom_strength": strength, "dom_reasons": list(favorable.get("reasons") or []), "current_target_r": round(current_target_r, 4), "new_target_r": round(target_r, 4), "max_extension_r": max_extension_r, "snapshots_used": snapshots_used, "symbol": symbol}
            return {"active": True, "action": "extend", "reason": "crypto_dom_tp_extension", "new_stop_loss": round(new_sl, 4), "new_take_profit": round(new_tp, 4), "details": details}
        except Exception:
            return {"active": False, "reason": "dom_error"}

    def _scheduled_canary_rebalanced_stop(
        self,
        *,
        source: str,
        direction: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
    ) -> float:
        if not self._is_scheduled_canary_source(source):
            return _safe_float(stop_loss, 0.0)
        if not bool(getattr(config, "CTRADER_SCHEDULED_CANARY_RR_REBALANCE_ENABLED", True)):
            return _safe_float(stop_loss, 0.0)
        entry = _safe_float(entry_price, 0.0)
        sl = _safe_float(stop_loss, 0.0)
        tp = _safe_float(take_profit, 0.0)
        if entry <= 0 or sl <= 0 or tp <= 0:
            return sl
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        if risk <= 0 or reward <= 0:
            return sl
        min_rr = max(0.25, _safe_float(getattr(config, "CTRADER_SCHEDULED_CANARY_MIN_RR", 0.85), 0.85))
        current_rr = reward / max(risk, 1e-9)
        if current_rr >= min_rr:
            return sl
        keep_ratio = min(0.95, max(0.20, _safe_float(getattr(config, "CTRADER_SCHEDULED_CANARY_MIN_STOP_KEEP_RATIO", 0.58), 0.58)))
        target_risk = max(reward / max(min_rr, 1e-9), risk * keep_ratio)
        target_risk = min(risk, target_risk)
        new_sl = entry - target_risk if str(direction or "").strip().lower() == "long" else entry + target_risk
        return _safe_float(new_sl, sl)

    @staticmethod
    def _position_age_min(pos: dict) -> float:
        first_seen = str((pos or {}).get("first_seen_utc") or (pos or {}).get("last_seen_utc") or "").strip()
        ms = _iso_to_ms(first_seen)
        if ms <= 0:
            return 0.0
        return max(0.0, (time.time() * 1000.0 - float(ms)) / 60000.0)

    def _build_payload(self, signal, source: str) -> tuple[Optional[dict], str]:
        if signal is None:
            return None, "signal_missing"
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        entry = _safe_float(getattr(signal, "entry", 0.0), 0.0)
        stop_loss = _safe_float(getattr(signal, "stop_loss", 0.0), 0.0)
        take_profit = self._take_profit_for_signal(signal)
        if not symbol:
            return None, "symbol_missing"
        if direction not in {"long", "short"}:
            return None, "direction_invalid"
        if entry <= 0 or stop_loss <= 0:
            return None, "entry_or_stop_invalid"
        account_id, account_reason = self._configured_account_id()
        if account_id is None:
            return None, "account_id_missing"
        try:
            raw = dict(getattr(signal, "raw_scores", {}) or {})
        except Exception:
            raw = {}
        trace = self._signal_trace_meta(signal)
        volume_overrides = dict(getattr(config, "get_ctrader_default_volume_symbol_overrides", lambda: {})() or {})
        payload = {
            "account_id": int(account_id),
            "account_reason": str(account_reason or ""),
            "symbol": symbol,
            "market_symbol": str(raw.get("market_symbol", symbol) or symbol).strip().upper(),
            "direction": direction,
            "source": str(source or ""),
            "confidence": round(_safe_float(getattr(signal, "confidence", 0.0), 0.0), 2),
            "entry": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "entry_type": str(getattr(signal, "entry_type", "market") or "market").strip().lower(),
            "pattern": str(getattr(signal, "pattern", "") or ""),
            "timeframe": str(getattr(signal, "timeframe", "") or ""),
            "session": str(getattr(signal, "session", "") or ""),
            "risk_usd": round(
                _safe_float(raw.get("ctrader_risk_usd_override", getattr(config, "CTRADER_RISK_USD_PER_TRADE", 10.0)), 10.0),
                4,
            ),
            "fixed_volume": int(volume_overrides.get(symbol, int(getattr(config, "CTRADER_DEFAULT_VOLUME", 0) or 0))),
            "client_order_id": str(trace.get("run_id") or "")[:64],
            "label": f"dexter:{symbol}:{str(source or '')[:16]}:{int(trace.get('run_no', 0) or 0)}"[:64],
            "comment": f"dexter|{str(source or '')[:24]}|{symbol}"[:128],
            "signal_run_id": str(trace.get("run_id") or ""),
            "signal_run_no": int(trace.get("run_no", 0) or 0),
            "reasons": list(getattr(signal, "reasons", []) or []),
            "warnings": list(getattr(signal, "warnings", []) or []),
            "raw_scores": raw,
            "signal_h1_trend": str(raw.get("signal_h1_trend") or raw.get("scalp_force_trend_h1") or raw.get("trend_h1") or raw.get("h1_trend") or "").strip().lower(),
            "signal_h4_trend": str(raw.get("signal_h4_trend") or raw.get("scalp_force_trend_h4") or raw.get("trend_h4") or raw.get("h4_trend") or "").strip().lower(),
            "xau_mtf_aligned_side": str(((raw.get("xau_multi_tf_snapshot") or {}).get("aligned_side") or "")).strip().lower(),
            "countertrend_confirmed": bool(
                raw.get("countertrend_confirmed")
                or raw.get("xau_mtf_countertrend_confirmed")
                or ((raw.get("scalping_trigger") or {}).get("countertrend_confirmed"))
            ),
        }
        family = self._source_family(source)
        if symbol == "XAUUSD":
            hedge_state = self._xau_hedge_transition_state()
            allowed_families = {str(part or "").strip().lower() for part in list(hedge_state.get("allowed_families") or []) if str(part or "").strip()}
            if str(hedge_state.get("status") or "") == "active" and family and family in allowed_families:
                risk_mult = max(0.10, min(1.0, _safe_float(hedge_state.get("risk_multiplier", getattr(config, "TRADING_MANAGER_XAU_HEDGE_LANE_RISK_MULTIPLIER", 0.65)), getattr(config, "TRADING_MANAGER_XAU_HEDGE_LANE_RISK_MULTIPLIER", 0.65))))
                payload["risk_usd"] = round(max(0.1, _safe_float(payload.get("risk_usd"), 0.0) * risk_mult), 4)
                raw["ctrader_hedge_lane"] = True
                raw["ctrader_hedge_risk_multiplier"] = risk_mult
            bypass_state = self._xau_opportunity_bypass_state()
            bypass_families = {str(part or "").strip().lower() for part in list(bypass_state.get("allowed_families") or []) if str(part or "").strip()}
            if str(bypass_state.get("status") or "") == "active" and family and family in bypass_families:
                risk_mult = max(0.10, min(1.0, _safe_float(bypass_state.get("risk_multiplier", getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_RISK_MULTIPLIER", 0.55)), getattr(config, "TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_RISK_MULTIPLIER", 0.55))))
                payload["risk_usd"] = round(max(0.1, _safe_float(payload.get("risk_usd"), 0.0) * risk_mult), 4)
                raw["ctrader_opportunity_bypass"] = True
                raw["ctrader_opportunity_bypass_risk_multiplier"] = risk_mult
        entry_type = payload["entry_type"]
        if entry_type in {"limit", "patience"}:
            order_type = "limit"
        elif entry_type in {"buy_stop", "sell_stop", "stop"}:
            order_type = "stop"
        else:
            order_type = "market"
        payload["order_type"] = order_type
        return payload, ""

    def _signal_learning_db_path(self) -> Path:
        return self.db_path.parent / "signal_learning.db"

    def _find_journal_match(
        self,
        conn: sqlite3.Connection,
        *,
        order_id: int = 0,
        position_id: int = 0,
        source: str = "",
        symbol: str = "",
        run_no: int = 0,
        entry_price: float = 0.0,
    ) -> Optional[sqlite3.Row]:
        conn.row_factory = sqlite3.Row
        negative_statuses = self._negative_execution_statuses()

        def _row_valid(row: Optional[sqlite3.Row]) -> bool:
            if row is None:
                return False
            row_obj = dict(row)
            row_status = str(row_obj.get("status", "") or "").strip().lower()
            if row_status in negative_statuses:
                return False
            row_source = str(row_obj.get("source", "") or "").strip().lower()
            row_symbol = str(row_obj.get("symbol", "") or "").strip().upper()
            row_run_no = int(row_obj.get("signal_run_no", 0) or 0)
            if source and row_source and row_source != str(source).strip().lower():
                return False
            if symbol and row_symbol and row_symbol != str(symbol).strip().upper():
                return False
            if int(run_no or 0) > 0 and row_run_no > 0 and row_run_no != int(run_no):
                return False
            target_entry = _safe_float(entry_price, 0.0)
            row_entry = _safe_float(row_obj.get("entry"), 0.0)
            if target_entry > 0 and row_entry > 0:
                if abs(row_entry - target_entry) / max(target_entry, 1e-8) > 0.10:
                    return False
            return True

        if int(position_id or 0) > 0:
            row = conn.execute(
                "SELECT * FROM execution_journal WHERE position_id=? ORDER BY id DESC LIMIT 1",
                (int(position_id),),
            ).fetchone()
            if _row_valid(row):
                return row
        if int(order_id or 0) > 0:
            row = conn.execute(
                "SELECT * FROM execution_journal WHERE order_id=? ORDER BY id DESC LIMIT 1",
                (int(order_id),),
            ).fetchone()
            if _row_valid(row):
                return row
        if source and symbol and int(run_no or 0) > 0:
            row = conn.execute(
                """
                SELECT * FROM execution_journal
                 WHERE LOWER(COALESCE(source,''))=?
                   AND UPPER(COALESCE(symbol,''))=?
                   AND signal_run_no=?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(source).lower(), str(symbol).upper(), int(run_no)),
            ).fetchone()
            if _row_valid(row):
                return row
        if source and symbol:
            rows = conn.execute(
                """
                SELECT * FROM execution_journal
                 WHERE LOWER(COALESCE(source,''))=?
                   AND UPPER(COALESCE(symbol,''))=?
                 ORDER BY id DESC
                 LIMIT 5
                """,
                (str(source).lower(), str(symbol).upper()),
            ).fetchall()
            if rows:
                target_entry = _safe_float(entry_price, 0.0)
                for row in rows:
                    if target_entry <= 0:
                        if _row_valid(row):
                            return row
                        continue
                    if _row_valid(row):
                        return row
        return None

    def _source_run_duplicate_guard(self, *, source: str, payload: dict) -> tuple[bool, str, dict]:
        source_token = str(source or payload.get("source") or "").strip().lower()
        symbol = str(payload.get("symbol") or payload.get("market_symbol") or "").strip().upper()
        signal_run_id = str(payload.get("signal_run_id") or payload.get("client_order_id") or "").strip()
        meta = {
            "enabled": bool(source_token and symbol and signal_run_id),
            "source": source_token,
            "symbol": symbol,
            "signal_run_id": signal_run_id,
            "client_order_id": str(payload.get("client_order_id") or "").strip(),
        }
        if not meta["enabled"]:
            return True, "", meta
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            order_row = conn.execute(
                """
                SELECT order_id, order_status, client_order_id, signal_run_id
                  FROM ctrader_orders
                 WHERE is_open=1
                   AND LOWER(COALESCE(source,''))=?
                   AND UPPER(COALESCE(symbol,''))=?
                   AND (
                        COALESCE(signal_run_id,'')=?
                        OR COALESCE(client_order_id,'')=?
                   )
                 ORDER BY order_id DESC
                 LIMIT 1
                """,
                (source_token, symbol, signal_run_id, signal_run_id),
            ).fetchone()
            if order_row is not None:
                meta["duplicate"] = {
                    "kind": "order",
                    "order_id": int(order_row["order_id"] or 0),
                    "status": str(order_row["order_status"] or ""),
                }
                return False, f"duplicate_source_run_order:{symbol}:{int(order_row['order_id'] or 0)}", meta

            position_row = conn.execute(
                """
                SELECT position_id, status, signal_run_id
                  FROM ctrader_positions
                 WHERE is_open=1
                   AND LOWER(COALESCE(source,''))=?
                   AND UPPER(COALESCE(symbol,''))=?
                   AND COALESCE(signal_run_id,'')=?
                 ORDER BY position_id DESC
                 LIMIT 1
                """,
                (source_token, symbol, signal_run_id),
            ).fetchone()
            if position_row is not None:
                meta["duplicate"] = {
                    "kind": "position",
                    "position_id": int(position_row["position_id"] or 0),
                    "status": str(position_row["status"] or ""),
                }
                return False, f"duplicate_source_run_position:{symbol}:{int(position_row['position_id'] or 0)}", meta

            negative_statuses = self._negative_execution_statuses()
            journal_rows = conn.execute(
                """
                SELECT id, status, order_id, position_id, deal_id
                  FROM execution_journal
                 WHERE LOWER(COALESCE(source,''))=?
                   AND UPPER(COALESCE(symbol,''))=?
                   AND COALESCE(signal_run_id,'')=?
                 ORDER BY id DESC
                 LIMIT 8
                """,
                (source_token, symbol, signal_run_id),
            ).fetchall()
            for row in journal_rows:
                status = str(row["status"] or "").strip().lower()
                if status in negative_statuses:
                    continue
                meta["duplicate"] = {
                    "kind": "journal",
                    "journal_id": int(row["id"] or 0),
                    "status": status,
                    "order_id": int(row["order_id"] or 0),
                    "position_id": int(row["position_id"] or 0),
                    "deal_id": int(row["deal_id"] or 0),
                }
                return False, f"duplicate_source_run_journal:{symbol}:{int(row['id'] or 0)}:{status}", meta
        return True, "", meta

    @staticmethod
    def _safe_json_load(raw: str) -> dict:
        if not raw:
            return {}
        try:
            val = json.loads(str(raw or ""))
            return val if isinstance(val, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _merge_signal_event_extra(existing_raw: str, updates: Optional[dict]) -> dict:
        merged = CTraderExecutor._safe_json_load(existing_raw)
        incoming = dict(updates or {})
        for key in ("reasons", "warnings"):
            items = []
            seen = set()
            for raw in list(merged.get(key) or []) + list(incoming.get(key) or []):
                text = str(raw or "").strip()
                if text and text not in seen:
                    seen.add(text)
                    items.append(text)
            if items:
                merged[key] = items
            incoming.pop(key, None)
        raw_scores = dict(merged.get("raw_scores") or {})
        raw_scores.update(dict(incoming.pop("raw_scores", {}) or {}))
        if raw_scores:
            merged["raw_scores"] = raw_scores
        for key, value in incoming.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                next_val = dict(merged.get(key) or {})
                next_val.update(dict(value or {}))
                merged[key] = next_val
            else:
                merged[key] = value
        return merged

    @staticmethod
    def _classify_signal_event_close(
        row: Optional[sqlite3.Row],
        *,
        direction: str,
        pnl_usd: float,
        close_price: float,
    ) -> dict:
        row_obj = dict(row) if row is not None else {}
        entry = _safe_float(row_obj.get("entry"), 0.0)
        stop_loss = _safe_float(row_obj.get("stop_loss"), 0.0)
        tp1 = _safe_float(row_obj.get("take_profit_1"), 0.0)
        tp2 = _safe_float(row_obj.get("take_profit_2"), 0.0)
        tp3 = _safe_float(row_obj.get("take_profit_3"), 0.0)
        dir_long = str(direction or "").strip().lower() == "long"
        risk = abs(entry - stop_loss)
        tol = max(risk * 0.35, abs(entry) * 0.00025, 1e-6) if entry > 0 and stop_loss > 0 else 0.0
        state = "flat"
        if float(pnl_usd) > 0:
            state = "win"
            if close_price > 0:
                for name, level in (("tp3", tp3), ("tp2", tp2), ("tp1", tp1)):
                    if level <= 0:
                        continue
                    if dir_long and close_price >= (level - tol):
                        state = name
                        break
                    if (not dir_long) and close_price <= (level + tol):
                        state = name
                        break
        elif float(pnl_usd) < 0:
            state = "loss"
            if close_price > 0 and stop_loss > 0:
                if dir_long and close_price <= (stop_loss + tol):
                    state = "sl"
                elif (not dir_long) and close_price >= (stop_loss - tol):
                    state = "sl"
        return {
            "state": state,
            "close_price": round(float(close_price), 8) if close_price > 0 else 0.0,
            "risk_distance": round(float(risk), 8) if risk > 0 else 0.0,
            "tolerance": round(float(tol), 8) if tol > 0 else 0.0,
        }

    def _apply_closed_deal_to_journal(
        self,
        conn: sqlite3.Connection,
        *,
        journal_id: Optional[int],
        deal: dict,
        closed_at: str,
    ) -> None:
        if int(journal_id or 0) <= 0:
            return
        row = conn.execute(
            "SELECT id, response_json, execution_meta_json FROM execution_journal WHERE id=? LIMIT 1",
            (int(journal_id),),
        ).fetchone()
        if row is None:
            return
        pnl = _safe_float(deal.get("pnl_usd"), 0.0)
        outcome = "win" if pnl > 0 else ("loss" if pnl < 0 else "flat")
        close_deal_id = int(deal.get("deal_id") or 0) or None
        response_json = self._safe_json_load(str(row["response_json"] or ""))
        response_json["close_deal"] = {
            "deal_id": close_deal_id,
            "position_id": int(deal.get("position_id") or 0) or None,
            "order_id": int(deal.get("order_id") or 0) or None,
            "outcome": outcome,
            "pnl_usd": round(float(pnl), 4),
            "execution_utc": str(closed_at or ""),
        }
        execution_meta = self._safe_json_load(str(row["execution_meta_json"] or ""))
        execution_meta["closed"] = {
            "deal_id": close_deal_id,
            "outcome": outcome,
            "pnl_usd": round(float(pnl), 4),
            "execution_utc": str(closed_at or ""),
            "has_close_detail": bool(deal.get("has_close_detail")),
        }
        conn.execute(
            """
            UPDATE execution_journal
               SET status='closed',
                   message=?,
                   deal_id=COALESCE(?, deal_id),
                   response_json=?,
                   execution_meta_json=?
             WHERE id=?
            """,
            (
                f"ctrader closed {outcome} pnl={pnl:+.2f}$",
                close_deal_id,
                json.dumps(response_json, ensure_ascii=True, separators=(",", ":")),
                json.dumps(execution_meta, ensure_ascii=True, separators=(",", ":")),
                int(journal_id),
            ),
        )

    def _sync_signal_event_open(
        self,
        *,
        source: str,
        symbol: str,
        direction: str,
        position_id: int,
        journal_row: Optional[sqlite3.Row],
        extra: dict,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        run_id: str = "",
        run_no: int = 0,
    ) -> None:
        path = self._signal_learning_db_path()
        if not path.exists():
            return
        source_l = str(source or "").strip().lower()
        symbol_u = str(symbol or "").strip().upper()
        if not source_l or not symbol_u:
            return
        with sqlite3.connect(path) as conn:
            conn.row_factory = sqlite3.Row
            row = None
            if int(position_id or 0) > 0:
                row = conn.execute(
                    "SELECT id, resolved, extra_json FROM signal_events WHERE (position_id=? OR ticket=?) ORDER BY id DESC LIMIT 1",
                    (int(position_id), int(position_id)),
                ).fetchone()
            if row is None and run_no > 0:
                row = conn.execute(
                    """
                    SELECT id, resolved, extra_json FROM signal_events
                     WHERE created_at >= datetime('now','-72 hours')
                       AND LOWER(COALESCE(source,''))=?
                       AND UPPER(COALESCE(signal_symbol,''))=?
                       AND COALESCE(extra_json,'') LIKE ?
                     ORDER BY id DESC LIMIT 1
                    """,
                    (source_l, symbol_u, f'%\"signal_run_no\": {int(run_no)}%'),
                ).fetchone()
            if row is not None:
                merged_extra = self._merge_signal_event_extra(str(row["extra_json"] or ""), extra)
                conn.execute(
                    """
                    UPDATE signal_events
                       SET ticket=?, position_id=?, broker_symbol=?, mt5_status='ctrader_open',
                           mt5_message='ctrader_reconciled_open', extra_json=?
                     WHERE id=?
                    """,
                    (int(position_id), int(position_id), symbol_u, json.dumps(merged_extra, ensure_ascii=True), int(row["id"])),
                )
                conn.commit()
                return
            journal_obj = dict(journal_row) if journal_row is not None else {}
            req = self._safe_json_load(str(journal_obj.get("request_json", "") or ""))
            raw_scores = dict(req.get("raw_scores") or {})
            merged_extra = self._merge_signal_event_extra(
                "",
                {
                    **dict(extra or {}),
                    "raw_scores": raw_scores,
                    "reasons": list(req.get("reasons") or []),
                    "warnings": list(req.get("warnings") or []),
                },
            )
            conn.execute(
                """
                INSERT INTO signal_events (
                    created_at, source, signal_symbol, broker_symbol, direction,
                    confidence, risk_reward, rsi, atr, timeframe, entry, stop_loss,
                    take_profit_1, take_profit_2, take_profit_3, pattern, session,
                    score_long, score_short, score_edge, mt5_status, mt5_message,
                    ticket, position_id, resolved, outcome, pnl, closed_at, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, ?)
                """,
                (
                    _utc_now_iso(),
                    source_l,
                    symbol_u,
                    symbol_u,
                    direction,
                    _safe_float(req.get("confidence"), _safe_float(journal_obj.get("confidence"), 0.0)),
                    0.0,
                    0.0,
                    0.0,
                    str(req.get("timeframe", "") or ""),
                    _safe_float(req.get("entry"), entry_price),
                    _safe_float(req.get("stop_loss"), stop_loss),
                    _safe_float(req.get("take_profit_1"), take_profit),
                    _safe_float(req.get("take_profit_2"), take_profit),
                    _safe_float(req.get("take_profit_3"), take_profit),
                    str(req.get("pattern", "") or ""),
                    str(req.get("session", "") or ""),
                    _safe_float(raw_scores.get("long"), 0.0),
                    _safe_float(raw_scores.get("short"), 0.0),
                    _safe_float(raw_scores.get("edge"), 0.0),
                    "ctrader_open",
                    "ctrader_reconciled_open",
                    int(position_id),
                    int(position_id),
                    json.dumps(merged_extra, ensure_ascii=True),
                ),
            )
            conn.commit()

    def _sync_signal_event_close(
        self,
        *,
        source: str,
        symbol: str,
        direction: str,
        position_id: int,
        pnl_usd: float,
        closed_at: str,
        extra: dict,
    ) -> None:
        path = self._signal_learning_db_path()
        if not path.exists():
            return
        source_l = str(source or "").strip().lower()
        symbol_u = str(symbol or "").strip().upper()
        outcome = 1 if float(pnl_usd) > 0 else 0
        with sqlite3.connect(path) as conn:
            conn.row_factory = sqlite3.Row
            row = None
            if int(position_id or 0) > 0:
                row = conn.execute(
                    """
                    SELECT id, extra_json, entry, stop_loss, take_profit_1, take_profit_2, take_profit_3
                      FROM signal_events
                     WHERE (position_id=? OR ticket=?)
                     ORDER BY id DESC
                     LIMIT 1
                    """,
                    (int(position_id), int(position_id)),
                ).fetchone()
            if row is None and source_l and symbol_u:
                row = conn.execute(
                    """
                    SELECT id, extra_json, entry, stop_loss, take_profit_1, take_profit_2, take_profit_3
                      FROM signal_events
                     WHERE resolved=0
                       AND created_at >= datetime('now','-72 hours')
                       AND LOWER(COALESCE(source,''))=?
                       AND UPPER(COALESCE(signal_symbol,''))=?
                       AND LOWER(COALESCE(direction,''))=?
                     ORDER BY id DESC LIMIT 1
                    """,
                    (source_l, symbol_u, str(direction or "").strip().lower()),
                ).fetchone()
            close_price = _safe_float(dict((extra or {}).get("deal") or {}).get("execution_price"), 0.0)
            close_resolution = self._classify_signal_event_close(
                row,
                direction=str(direction or "").strip().lower(),
                pnl_usd=_safe_float(pnl_usd, 0.0),
                close_price=close_price,
            )
            merged_extra = self._merge_signal_event_extra(
                str(row["extra_json"] or "") if row is not None else "",
                {
                    **dict(extra or {}),
                    "close_resolution": close_resolution,
                },
            )
            if row is None:
                conn.execute(
                    """
                    INSERT INTO signal_events (
                        created_at, source, signal_symbol, broker_symbol, direction, confidence,
                        risk_reward, rsi, atr, timeframe, entry, stop_loss, take_profit_1,
                        take_profit_2, take_profit_3, pattern, session, score_long, score_short,
                        score_edge, mt5_status, mt5_message, ticket, position_id, resolved,
                        outcome, pnl, closed_at, extra_json
                    ) VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0, '', 0, 0, 0, 0, 0, '', '', 0, 0, 0, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                    """,
                    (
                        closed_at or _utc_now_iso(),
                        source_l,
                        symbol_u,
                        symbol_u,
                        str(direction or "").strip().lower(),
                        "ctrader_closed",
                        "ctrader_reconciled_close",
                        int(position_id),
                        int(position_id),
                        int(outcome),
                        float(pnl_usd),
                        closed_at or _utc_now_iso(),
                        json.dumps(merged_extra, ensure_ascii=True),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE signal_events
                       SET ticket=?, position_id=?, broker_symbol=?, resolved=1, outcome=?, pnl=?,
                           closed_at=?, mt5_status='ctrader_closed', mt5_message='ctrader_reconciled_close',
                           extra_json=?
                     WHERE id=?
                    """,
                    (
                        int(position_id),
                        int(position_id),
                        symbol_u,
                        int(outcome),
                        float(pnl_usd),
                        closed_at or _utc_now_iso(),
                        json.dumps(merged_extra, ensure_ascii=True),
                        int(row["id"]),
                    ),
                )
            conn.commit()

    def _unsafe_untracked_position(self, pos: dict, *, journal_id: Optional[int]) -> bool:
        if not bool(getattr(config, "CTRADER_AUTO_CLOSE_UNTRACKED_UNSAFE", True)):
            return False
        if journal_id:
            return False
        ref = self._reference_price(str(pos.get("symbol", "") or ""))
        if ref <= 0:
            return False
        tp = _safe_float(pos.get("take_profit"), 0.0)
        sl = _safe_float(pos.get("stop_loss"), 0.0)
        tp_dev = abs(tp - ref) / max(ref, 1e-8) if tp > 0 else 0.0
        max_tp_dev = float(getattr(config, "CTRADER_AUTO_CLOSE_UNTRACKED_UNSAFE_MAX_TP_DEVIATION_PCT", 0.35) or 0.35)
        return bool(str(pos.get("label", "") or "").lower().startswith("dexter:")) and (sl <= 0 or tp_dev > max_tp_dev)

    def _result_from_dict(self, payload: dict) -> CTraderExecutionResult:
        data = dict(payload or {})
        return CTraderExecutionResult(
            ok=bool(data.get("ok", False)),
            status=str(data.get("status", "") or ""),
            message=str(data.get("message", "") or ""),
            signal_symbol=str(data.get("signal_symbol", "") or ""),
            broker_symbol=str(data.get("broker_symbol", "") or ""),
            dry_run=bool(data.get("dry_run", False)),
            account_id=data.get("account_id"),
            order_id=data.get("order_id"),
            position_id=data.get("position_id"),
            deal_id=data.get("deal_id"),
            volume=_safe_float(data.get("volume"), 0.0) if data.get("volume") is not None else None,
            execution_meta=dict(data.get("execution_meta", {}) or {}),
        )

    @staticmethod
    def _extract_json_line(stdout_text: str) -> dict:
        for line in reversed([str(x).strip() for x in str(stdout_text or "").splitlines()]):
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except Exception:
                continue
            if isinstance(parsed, dict):
                return parsed
        return {}

    def _run_worker(self, *, mode: str, payload: Optional[dict] = None, timeout_sec: int = 20) -> dict:
        if not self.worker_path.exists():
            return {"ok": False, "status": "worker_missing", "message": f"worker not found: {self.worker_path}"}
        cmd = [sys.executable, str(self.worker_path), "--mode", str(mode or "health")]
        tmp_path: Optional[str] = None
        try:
            if payload is not None:
                with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as fh:
                    json.dump(payload, fh, ensure_ascii=True, separators=(",", ":"))
                    tmp_path = fh.name
                cmd.extend(["--payload-file", str(tmp_path)])
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=max(5, int(timeout_sec or 20)),
                cwd=str(Path(__file__).resolve().parent.parent),
                env=env,
            )
            parsed = self._extract_json_line(proc.stdout)
            if parsed:
                parsed.setdefault("worker_returncode", int(proc.returncode))
                if proc.stderr:
                    parsed.setdefault("worker_stderr", proc.stderr[-1500:])
                return parsed
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            return {
                "ok": False,
                "status": "worker_error",
                "message": stderr or stdout or f"worker exited code {proc.returncode}",
                "worker_returncode": int(proc.returncode),
            }
        except subprocess.TimeoutExpired:
            return {"ok": False, "status": "timeout", "message": f"worker timeout after {int(timeout_sec or 0)}s"}
        except Exception as e:
            return {"ok": False, "status": "worker_error", "message": str(e)}
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

    def _store_market_capture(self, payload: dict) -> dict:
        run_id = str(payload.get("run_id", "") or "").strip()
        if not run_id:
            return {"ok": False, "status": "capture_missing_run_id", "message": "run_id missing"}
        spots = list(payload.get("spots") or [])
        depth = list(payload.get("depth") or [])
        created_ts = time.time()
        created_utc = str(payload.get("captured_at") or _utc_now_iso())
        symbols = sorted({str(item.get("symbol", "") or "").strip().upper() for item in (spots + depth) if str(item.get("symbol", "") or "").strip()})
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO ctrader_capture_runs(
                    run_id, created_ts, created_utc, account_id, environment, symbols_json,
                    duration_sec, include_depth, spot_events, depth_events, status, message, raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    float(created_ts),
                    created_utc,
                    _safe_int(payload.get("account_id"), 0) or None,
                    str(payload.get("environment", "") or ""),
                    json.dumps(symbols, ensure_ascii=True, separators=(",", ":")),
                    _safe_int(payload.get("duration_sec"), 0),
                    1 if bool(payload.get("include_depth", True)) else 0,
                    len(spots),
                    len(depth),
                    str(payload.get("status", "") or ""),
                    str(payload.get("message", "") or ""),
                    json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
                ),
            )
            if spots:
                conn.executemany(
                    """
                    INSERT INTO ctrader_spot_ticks(
                        run_id, account_id, symbol_id, symbol, bid, ask, spread, spread_pct, event_utc, event_ts, raw_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    [
                        (
                            run_id,
                            _safe_int(item.get("account_id"), 0) or None,
                            _safe_int(item.get("symbol_id"), 0) or None,
                            str(item.get("symbol", "") or "").strip().upper(),
                            _safe_float(item.get("bid"), 0.0),
                            _safe_float(item.get("ask"), 0.0),
                            _safe_float(item.get("spread"), 0.0),
                            _safe_float(item.get("spread_pct"), 0.0),
                            str(item.get("event_utc", "") or ""),
                            _safe_float(item.get("event_ts"), 0.0),
                            json.dumps(item, ensure_ascii=True, separators=(",", ":")),
                        )
                        for item in spots
                    ],
                )
            if depth:
                conn.executemany(
                    """
                    INSERT INTO ctrader_depth_quotes(
                        run_id, account_id, symbol_id, symbol, quote_id, side, price, size, level_index, event_utc, event_ts, raw_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    [
                        (
                            run_id,
                            _safe_int(item.get("account_id"), 0) or None,
                            _safe_int(item.get("symbol_id"), 0) or None,
                            str(item.get("symbol", "") or "").strip().upper(),
                            _safe_int(item.get("quote_id"), 0) or None,
                            str(item.get("side", "") or "").strip().lower(),
                            _safe_float(item.get("price"), 0.0),
                            _safe_float(item.get("size"), 0.0),
                            _safe_int(item.get("level_index"), 0),
                            str(item.get("event_utc", "") or ""),
                            _safe_float(item.get("event_ts"), 0.0),
                            json.dumps(item, ensure_ascii=True, separators=(",", ":")),
                        )
                        for item in depth
                    ],
                )
        return {
            "ok": True,
            "status": str(payload.get("status", "") or "captured"),
            "run_id": run_id,
            "spots": len(spots),
            "depth": len(depth),
            "symbols": symbols,
        }

    def capture_market_data(
        self,
        *,
        symbols: Optional[list[str]] = None,
        duration_sec: Optional[int] = None,
        include_depth: bool = True,
        max_events: Optional[int] = None,
        max_depth_levels: Optional[int] = None,
    ) -> dict:
        if not self.enabled:
            return {"ok": False, "status": "disabled", "message": "ctrader disabled"}
        if not self.sdk_available:
            return {"ok": False, "status": "unavailable", "message": "ctrader-open-api not installed"}
        capture_symbols = [
            str(sym or "").strip().upper()
            for sym in (symbols or sorted(list(getattr(config, "get_ctrader_market_capture_symbols", lambda: set())() or set())))
            if str(sym or "").strip()
        ]
        if not capture_symbols:
            capture_symbols = ["XAUUSD", "BTCUSD", "ETHUSD"]
        payload = {
            "symbols": capture_symbols,
            "duration_sec": max(3, int(duration_sec if duration_sec is not None else getattr(config, "CTRADER_MARKET_CAPTURE_DURATION_SEC", 12) or 12)),
            "include_depth": bool(include_depth),
            "max_events": max(50, int(max_events if max_events is not None else getattr(config, "CTRADER_MARKET_CAPTURE_MAX_EVENTS", 600) or 600)),
            "max_depth_levels": max(1, int(max_depth_levels if max_depth_levels is not None else getattr(config, "CTRADER_MARKET_CAPTURE_DEPTH_LEVELS", 5) or 5)),
        }
        raw = self._run_worker(
            mode="capture_market",
            payload=payload,
            timeout_sec=max(45, int(payload["duration_sec"]) + 45),
        )
        if bool(raw.get("ok")):
            try:
                stored = self._store_market_capture(raw)
                raw["storage"] = stored
                raw["spots_count"] = int(stored.get("spots", 0) or len(list(raw.get("spots") or [])))
                raw["depth_count"] = int(stored.get("depth", 0) or len(list(raw.get("depth") or [])))
            except Exception as e:
                raw["storage"] = {"ok": False, "status": "store_error", "message": str(e)}
        return raw

    def fetch_trendbars(
        self,
        *,
        symbol: str = "XAUUSD",
        timeframe: str = "5m",
        from_ms: int = 0,
        to_ms: int = 0,
        count: int = 5000,
    ) -> dict:
        """Fetch historical OHLCV bars from cTrader OpenAPI.

        Returns dict with 'ok', 'bars' (list of {ts_ms, ts_utc, open, high, low, close, volume}),
        'bar_count', 'has_more', etc.
        """
        if not self.enabled:
            return {"ok": False, "status": "disabled", "message": "ctrader disabled"}
        if not self.sdk_available:
            return {"ok": False, "status": "unavailable", "message": "ctrader-open-api not installed"}
        if to_ms <= 0:
            to_ms = int(time.time() * 1000)
        payload = {
            "symbol": str(symbol or "XAUUSD").strip().upper(),
            "timeframe": str(timeframe or "5m").strip().lower(),
            "from_ms": int(from_ms),
            "to_ms": int(to_ms),
            "count": max(1, min(int(count), 14000)),
        }
        raw = self._run_worker(mode="get_trendbars", payload=payload, timeout_sec=20)
        return raw

    def _journal(self, signal, result: CTraderExecutionResult, *, source: str, request_payload: Optional[dict] = None, response_payload: Optional[dict] = None) -> int:
        trace = self._signal_trace_meta(signal)
        created_ts = time.time()
        symbol = str(getattr(signal, "symbol", "") or result.signal_symbol or "").strip().upper()
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        confidence = _safe_float(getattr(signal, "confidence", 0.0), 0.0)
        entry = _safe_float(getattr(signal, "entry", 0.0), 0.0)
        stop_loss = _safe_float(getattr(signal, "stop_loss", 0.0), 0.0)
        take_profit = self._take_profit_for_signal(signal)
        lane = self._source_lane(source)
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """
                INSERT INTO execution_journal (
                    created_ts, created_utc, source, lane, symbol, direction, confidence,
                    entry, stop_loss, take_profit, entry_type, dry_run, account_id,
                    broker_symbol, volume, status, message, order_id, position_id, deal_id,
                    signal_run_id, signal_run_no, request_json, response_json, execution_meta_json
                ) VALUES (?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_ts,
                    str(source or ""),
                    lane,
                    symbol,
                    direction,
                    confidence,
                    entry,
                    stop_loss,
                    take_profit,
                    str(getattr(signal, "entry_type", "") or ""),
                    1 if result.dry_run else 0,
                    result.account_id,
                    str(result.broker_symbol or ""),
                    _safe_float(result.volume, 0.0),
                    str(result.status or ""),
                    str(result.message or ""),
                    result.order_id,
                    result.position_id,
                    result.deal_id,
                    str(trace.get("run_id", "") or ""),
                    int(trace.get("run_no", 0) or 0),
                    json.dumps(request_payload or {}, ensure_ascii=True, separators=(",", ":")),
                    json.dumps(response_payload or {}, ensure_ascii=True, separators=(",", ":")),
                    json.dumps(result.execution_meta or {}, ensure_ascii=True, separators=(",", ":")),
                ),
            )
            return int(cur.lastrowid)

    def journal_pre_dispatch_skip(
        self,
        signal,
        *,
        source: str,
        reason: str,
        gate: str,
        request_payload: Optional[dict] = None,
        response_payload: Optional[dict] = None,
        execution_meta: Optional[dict] = None,
        status: str = "filtered",
    ) -> int:
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        meta = dict(execution_meta or {})
        audit_tags = self._normalized_text_list(meta.get("audit_tags") or [])
        for tag in ("pre_dispatch_skip", "xau_pre_dispatch_skip", f"gate:{str(gate or '').strip().lower()}"):
            if tag and tag not in audit_tags:
                audit_tags.append(tag)
        meta["audit_tags"] = audit_tags
        meta["pre_dispatch_audit"] = True
        meta["pre_dispatch_gate"] = str(gate or "")
        meta["pre_dispatch_reason"] = str(reason or "")
        meta["pre_dispatch_recorded_utc"] = _utc_now_iso()
        result = CTraderExecutionResult(
            ok=False,
            status=str(status or "filtered"),
            message=str(reason or ""),
            signal_symbol=symbol,
            broker_symbol=symbol,
            dry_run=self.dry_run,
            execution_meta=meta,
        )
        return self._journal(
            signal,
            result,
            source=source,
            request_payload=request_payload or {},
            response_payload=response_payload or {"pre_dispatch_audit": True},
        )

    def _capture_after_execute(self, result: CTraderExecutionResult, *, symbol: str) -> dict:
        if not bool(getattr(config, "CTRADER_MARKET_CAPTURE_ENABLED", False)):
            return {"ok": False, "status": "capture_disabled"}
        if not bool(getattr(config, "CTRADER_MARKET_CAPTURE_ON_EXECUTE", False)):
            return {"ok": False, "status": "capture_on_execute_disabled"}
        token = str(symbol or result.broker_symbol or result.signal_symbol or "").strip().upper()
        if not token:
            return {"ok": False, "status": "capture_symbol_missing"}
        allowed = set(getattr(config, "get_ctrader_market_capture_symbols", lambda: set())() or set())
        if allowed and token not in allowed:
            return {"ok": False, "status": "capture_symbol_filtered", "symbol": token}
        return self.capture_market_data(
            symbols=[token],
            duration_sec=max(3, int(getattr(config, "CTRADER_MARKET_CAPTURE_ON_EXECUTE_DURATION_SEC", 6) or 6)),
            include_depth=True,
            max_events=max(50, int(getattr(config, "CTRADER_MARKET_CAPTURE_ON_EXECUTE_MAX_EVENTS", 240) or 240)),
            max_depth_levels=max(1, int(getattr(config, "CTRADER_MARKET_CAPTURE_DEPTH_LEVELS", 5) or 5)),
        )

    def _update_journal_execution_meta(self, journal_id: int, execution_meta: dict) -> None:
        row_id = _safe_int(journal_id, 0)
        if row_id <= 0:
            return
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE execution_journal SET execution_meta_json=? WHERE id=?",
                (json.dumps(execution_meta or {}, ensure_ascii=True, separators=(",", ":")), row_id),
            )

    def _persist_position_manager_audit(self, pm_actions: list[dict]) -> int:
        tracked_actions = [
            dict(item or {})
            for item in list(pm_actions or [])
            if str((item or {}).get("action") or "").strip().lower() == "xau_profit_extension"
        ]
        if not tracked_actions:
            return 0
        persisted = 0
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            for action in tracked_actions:
                journal_id = _safe_int(action.get("journal_id"), 0)
                if journal_id <= 0:
                    jrow = self._find_journal_match(
                        conn,
                        position_id=_safe_int(action.get("position_id"), 0),
                        source=str(action.get("source", "") or ""),
                        symbol=str(action.get("symbol", "") or ""),
                    )
                    journal_id = int(jrow["id"]) if jrow is not None else 0
                if journal_id <= 0:
                    continue
                row = conn.execute(
                    "SELECT execution_meta_json FROM execution_journal WHERE id=? LIMIT 1",
                    (journal_id,),
                ).fetchone()
                if row is None:
                    continue
                execution_meta = self._safe_json_load(str(row["execution_meta_json"] or ""))
                action_name = str(action.get("action") or "").strip().lower()
                audit_entry = {
                    "audited_utc": _utc_now_iso(),
                    "action": action_name,
                    "position_id": _safe_int(action.get("position_id"), 0),
                    "source": str(action.get("source", "") or ""),
                    "symbol": str(action.get("symbol", "") or ""),
                    "reference_price": _safe_float(action.get("reference_price"), 0.0),
                    "new_stop_loss": _safe_float(action.get("new_stop_loss"), 0.0),
                    "new_take_profit": _safe_float(action.get("new_take_profit"), 0.0),
                    "r_now": (_safe_float(action.get("r_now"), 0.0) if action.get("r_now") is not None else None),
                    "age_min": (_safe_float(action.get("age_min"), 0.0) if action.get("age_min") is not None else None),
                    "details": dict(action.get("details") or {}),
                }
                audit_tags = [
                    str(tag or "").strip().lower()
                    for tag in list(execution_meta.get("audit_tags") or [])
                    if str(tag or "").strip()
                ]
                if action_name and action_name not in audit_tags:
                    audit_tags.append(action_name)
                if audit_tags:
                    execution_meta["audit_tags"] = audit_tags
                pm_audit = [
                    dict(item or {})
                    for item in list(execution_meta.get("position_manager_audit") or [])
                    if isinstance(item, dict)
                ]
                pm_audit.append(audit_entry)
                execution_meta["position_manager_audit"] = pm_audit[-12:]
                execution_meta["xau_profit_extension"] = audit_entry
                conn.execute(
                    "UPDATE execution_journal SET execution_meta_json=? WHERE id=?",
                    (json.dumps(execution_meta, ensure_ascii=True, separators=(",", ":")), journal_id),
                )
                persisted += 1
            conn.commit()
        return persisted

    def execute_signal(self, signal, *, source: str = "") -> CTraderExecutionResult:
        symbol = str(getattr(signal, "symbol", "") or "").strip().upper()
        pattern = str(getattr(signal, "pattern", "") or "").strip().upper()
        entry = _safe_float(getattr(signal, "entry", 0.0), 0.0)
        stop_loss = _safe_float(getattr(signal, "stop_loss", 0.0), 0.0)
        take_profit = self._take_profit_for_signal(signal)
        trace = self._signal_trace_meta(signal)

        def _minimal_request_payload() -> dict:
            return {
                "source": str(source or ""),
                "symbol": symbol,
                "direction": str(getattr(signal, "direction", "") or ""),
                "entry_type": str(getattr(signal, "entry_type", "") or ""),
                "confidence": _safe_float(getattr(signal, "confidence", 0.0), 0.0),
                "entry": entry,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "pattern": str(getattr(signal, "pattern", "") or ""),
                "session": str(getattr(signal, "session", "") or ""),
                "timeframe": str(getattr(signal, "timeframe", "") or ""),
                "signal_run_id": str(trace.get("run_id", "") or ""),
                "signal_run_no": int(trace.get("run_no", 0) or 0),
                "raw_scores": self._safe_json_load(json.dumps(getattr(signal, "raw_scores", {}) or {}, ensure_ascii=True, default=str)),
            }

        def _early_exit(
            status: str,
            message: str,
            *,
            request_payload: Optional[dict] = None,
            execution_meta: Optional[dict] = None,
        ) -> CTraderExecutionResult:
            result = CTraderExecutionResult(
                False,
                str(status or ""),
                str(message or ""),
                signal_symbol=symbol,
                dry_run=self.dry_run,
                execution_meta={
                    "pre_worker_exit": True,
                    "pre_worker_source": str(source or ""),
                    **dict(execution_meta or {}),
                },
            )
            try:
                self._journal(
                    signal,
                    result,
                    source=source,
                    request_payload=request_payload or _minimal_request_payload(),
                    response_payload={"pre_worker_exit": True, "status": str(status or ""), "message": str(message or "")},
                )
            except Exception:
                pass
            return result

        if not self.enabled:
            return _early_exit("disabled", "ctrader disabled")
        if not self.autotrade_enabled:
            return _early_exit("disabled", "ctrader autotrade disabled")
        if not self.sdk_available:
            return _early_exit("unavailable", "ctrader-open-api not installed")
        if _looks_like_test_pattern(pattern):
            return _early_exit("filtered", f"test_pattern_filtered:{pattern}")
        if _looks_like_fixture_signal(symbol, entry, stop_loss, take_profit):
            return _early_exit("filtered", "fixture_signal_filtered")
        if not self._source_allowed(source):
            return _early_exit("filtered", f"source_not_allowed:{source}")
        if not self._symbol_allowed(symbol):
            return _early_exit("filtered", f"symbol_not_allowed:{symbol}")
        gov_ok, gov_reason, gov_meta = self._source_direction_governance_guard(
            source=source,
            symbol=symbol,
            direction=str(getattr(signal, "direction", "") or ""),
        )
        if not gov_ok:
            return _early_exit(
                "filtered",
                gov_reason,
                execution_meta={"source_direction_governance": dict(gov_meta or {})},
            )
        price_ok, price_reason, _price_meta = self._price_sanity_guard(signal, source=source)
        if not price_ok:
            return _early_exit("filtered", price_reason)
        drift_ok, drift_reason, _drift_meta = self._market_entry_drift_guard(signal, source=source)
        if not drift_ok:
            return _early_exit("filtered", drift_reason)

        if bool(getattr(config, "CTRADER_EXEC_FEATURE_PACK_ENABLED", False)):
            try:
                self._apply_openapi_exec_feature_pack(signal, source=source, symbol=symbol)
            except Exception as exc:
                logger.debug("[CTRADER] exec feature pack skipped: %s", exc)

        payload, reason = self._build_payload(signal, source)
        if payload is None:
            return _early_exit("invalid", reason)
        short_limit_pause = self._xau_short_limit_pause_state(source=source, payload=payload)
        if bool(short_limit_pause.get("active")):
            remain_min = float(short_limit_pause.get("remaining_min", 0.0) or 0.0)
            run_id = str(short_limit_pause.get("trigger_run_id") or "")
            support_state = str(short_limit_pause.get("support_state") or "fss_support")
            return _early_exit(
                "filtered",
                f"xau_short_limit_pause_active:{remain_min:.1f}m:{support_state}:{run_id}",
                request_payload=payload,
                execution_meta={"short_limit_pause": dict(short_limit_pause or {})},
            )
        dup_ok, dup_reason, _dup_meta = self._source_run_duplicate_guard(source=source, payload=payload)
        if not dup_ok:
            return _early_exit("filtered", dup_reason, request_payload=payload)
        pair_cap = self._apply_xau_same_run_pair_risk_cap(source=source, payload=payload)
        if bool(pair_cap.get("active")) and bool(pair_cap.get("blocked")):
            return _early_exit(
                "filtered",
                f"same_run_pair_risk_cap_exhausted:{str(payload.get('signal_run_id') or '')}",
                request_payload=payload,
                execution_meta={"pair_risk_cap": dict(pair_cap or {})},
            )
        pos_ok, pos_reason, _pos_meta = self._position_direction_guard(symbol=symbol, direction=str(getattr(signal, "direction", "") or ""), source=source)
        if not pos_ok:
            return _early_exit("filtered", pos_reason, request_payload=payload)

        if self.dry_run:
            result = CTraderExecutionResult(
                ok=True,
                status="dry_run",
                message="ctrader dry-run planned",
                signal_symbol=str(payload.get("symbol", symbol)),
                broker_symbol=str(payload.get("market_symbol", symbol)),
                dry_run=True,
                account_id=int(payload.get("account_id", 0) or 0) or None,
                volume=float(payload.get("fixed_volume", 0) or 0) or None,
                execution_meta={
                    "mode": "dry_run",
                    "order_type": str(payload.get("order_type", "")),
                    "tp_level": int(getattr(config, "CTRADER_TP_LEVEL", 1) or 1),
                    "risk_usd": _safe_float(payload.get("risk_usd"), 0.0),
                },
            )
            self._journal(signal, result, source=source, request_payload=payload, response_payload={"mode": "dry_run"})
            return result

        raw = self._run_worker(
            mode="execute",
            payload=payload,
            timeout_sec=max(5, int(getattr(config, "CTRADER_EXECUTOR_TIMEOUT_SEC", 25) or 25)),
        )
        result = self._result_from_dict(raw)
        if not result.signal_symbol:
            result.signal_symbol = symbol
        journal_id = self._journal(signal, result, source=source, request_payload=payload, response_payload=raw)
        if bool(result.ok) and not bool(result.dry_run) and str(result.status or "") in {"accepted", "filled"}:
            try:
                protection_repair = self._post_accept_protection_repair(result=result, payload=payload)
                if protection_repair:
                    meta = dict(result.execution_meta or {})
                    meta["protection_repair"] = dict(protection_repair)
                    result.execution_meta = meta
            except Exception as e:
                meta = dict(result.execution_meta or {})
                meta["protection_repair"] = {"attempted": True, "status": "repair_error", "message": str(e)}
                result.execution_meta = meta
            try:
                capture = self._capture_after_execute(result, symbol=str(payload.get("market_symbol", symbol) or symbol))
                if isinstance(capture, dict):
                    meta = dict(result.execution_meta or {})
                    meta["market_capture"] = {
                        "ok": bool(capture.get("ok")),
                        "status": str(capture.get("status", "") or ""),
                        "run_id": str(((capture.get("storage") or {}).get("run_id") or capture.get("run_id") or "")),
                        "spots": _safe_int(capture.get("spots_count", capture.get("spots", 0)), 0),
                        "depth": _safe_int(capture.get("depth_count", capture.get("depth", 0)), 0),
                    }
                    result.execution_meta = meta
            except Exception as e:
                meta = dict(result.execution_meta or {})
                meta["market_capture"] = {"ok": False, "status": "capture_error", "message": str(e)}
                result.execution_meta = meta
            self._update_journal_execution_meta(journal_id, dict(result.execution_meta or {}))
            try:
                from copy_trade.manager import copy_trade_manager
                copy_trade_manager.dispatch_async(
                    master_payload=payload,
                    master_result=raw,
                    source=source,
                )
            except Exception as ct_err:
                logger.debug("[CopyTrade] dispatch skipped: %s", ct_err)
        return result

    def health_check(self, *, live: bool = True) -> dict:
        base = self.status(include_recent=True)
        if not live:
            return base
        if not self.enabled:
            base["live_check"] = {"ok": False, "status": "disabled", "message": "ctrader disabled"}
            return base
        if not self.sdk_available:
            base["live_check"] = {"ok": False, "status": "unavailable", "message": "ctrader-open-api not installed"}
            return base
        payload = {"account_id": self._configured_account_id()[0]}
        raw = self._run_worker(
            mode="health",
            payload=payload,
            timeout_sec=max(5, int(getattr(config, "CTRADER_HEALTHCHECK_TIMEOUT_SEC", 18) or 18)),
        )
        base["live_check"] = raw
        return base

    def list_accounts(self, *, live: bool = True) -> dict:
        rows = list(getattr(config, "get_ctrader_accounts", lambda: [])() or [])
        if rows:
            return {
                "ok": True,
                "status": "configured_accounts",
                "message": f"configured accounts ({len(rows)})",
                "accounts": rows,
                "selected_account_id": self._configured_account_id()[0],
            }
        if not self.enabled:
            return {"ok": False, "status": "disabled", "message": "ctrader disabled"}
        if not self.sdk_available:
            return {"ok": False, "status": "unavailable", "message": "ctrader-open-api not installed"}
        return {"ok": False, "status": "accounts_missing", "message": "no configured ctrader accounts"}

    def get_recent_journal(self, limit: int = 50) -> list[dict]:
        max_rows = max(1, min(int(limit or 50), 200))
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM execution_journal ORDER BY created_ts DESC LIMIT ?",
                (max_rows,),
            ).fetchall()
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            for key in ("request_json", "response_json", "execution_meta_json"):
                try:
                    item[key] = json.loads(item.get(key) or "{}")
                except Exception:
                    item[key] = {}
            out.append(item)
        return out

    def get_open_positions(self) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM ctrader_positions WHERE is_open=1 ORDER BY last_seen_utc DESC, position_id DESC"
            ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            try:
                item["raw_json"] = json.loads(item.get("raw_json") or "{}")
            except Exception:
                item["raw_json"] = {}
            out.append(item)
        return out

    def get_open_orders(self) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM ctrader_orders WHERE is_open=1 ORDER BY last_seen_utc DESC, order_id DESC"
            ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            try:
                item["raw_json"] = json.loads(item.get("raw_json") or "{}")
            except Exception:
                item["raw_json"] = {}
            out.append(item)
        return out

    def get_recent_deals(self, limit: int = 50) -> list[dict]:
        max_rows = max(1, min(int(limit or 50), 200))
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM ctrader_deals ORDER BY execution_utc DESC, deal_id DESC LIMIT ?",
                (max_rows,),
            ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            try:
                item["raw_json"] = json.loads(item.get("raw_json") or "{}")
            except Exception:
                item["raw_json"] = {}
            out.append(item)
        return out

    def close_position(self, *, position_id: int, volume: int = 0) -> CTraderExecutionResult:
        resolved_volume = int(volume or 0)
        if int(position_id or 0) > 0 and resolved_volume <= 0:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT volume FROM ctrader_positions WHERE position_id=? ORDER BY last_seen_utc DESC LIMIT 1",
                    (int(position_id),),
                ).fetchone()
            if row is not None:
                resolved_volume = max(0, int(_safe_float(row[0], 0.0)))
        raw = self._run_worker(
            mode="close",
            payload={"account_id": self._configured_account_id()[0], "position_id": int(position_id or 0), "volume": resolved_volume},
            timeout_sec=max(5, int(getattr(config, "CTRADER_EXECUTOR_TIMEOUT_SEC", 25) or 25)),
        )
        result = self._result_from_dict(raw)
        if bool(result.ok) and int(position_id or 0) > 0:
            try:
                from copy_trade.manager import copy_trade_manager

                copy_trade_manager.enforce_close_follow_async(
                    master_position_id=int(position_id or 0),
                    master_order_id=int(result.order_id or 0),
                    reason="master_close_api",
                    master_close_utc=_utc_now_iso(),
                )
            except Exception as ct_err:
                logger.debug("[CopyTrade] close-follow skipped: %s", ct_err)
        return result

    def amend_position_sltp(
        self,
        *,
        position_id: int,
        stop_loss: float = 0.0,
        take_profit: float = 0.0,
        trailing_stop_loss: bool = False,
    ) -> CTraderExecutionResult:
        raw = self._run_worker(
            mode="amend_position_sltp",
            payload={
                "account_id": self._configured_account_id()[0],
                "position_id": int(position_id or 0),
                "stop_loss": _safe_float(stop_loss, 0.0),
                "take_profit": _safe_float(take_profit, 0.0),
                "trailing_stop_loss": bool(trailing_stop_loss),
            },
            timeout_sec=max(5, int(getattr(config, "CTRADER_EXECUTOR_TIMEOUT_SEC", 25) or 25)),
        )
        result = self._result_from_dict(raw)
        if bool(result.ok) and int(position_id or 0) > 0:
            try:
                from copy_trade.manager import copy_trade_manager

                copy_trade_manager.sync_protection_follow_async(
                    master_position_id=int(position_id or 0),
                    stop_loss=_safe_float(stop_loss, 0.0),
                    take_profit=_safe_float(take_profit, 0.0),
                    reason="master_amend_sltp",
                )
            except Exception as ct_err:
                logger.debug("[CopyTrade] protection-follow skipped: %s", ct_err)
        return result

    def cancel_order(self, *, order_id: int) -> CTraderExecutionResult:
        raw = self._run_worker(
            mode="cancel_order",
            payload={"account_id": self._configured_account_id()[0], "order_id": int(order_id or 0)},
            timeout_sec=max(5, int(getattr(config, "CTRADER_EXECUTOR_TIMEOUT_SEC", 25) or 25)),
        )
        return self._result_from_dict(raw)

    def amend_order(
        self,
        *,
        order_id: int,
        limit_price: float = 0.0,
        stop_price: float = 0.0,
        stop_loss: float = 0.0,
        take_profit: float = 0.0,
        volume: int = 0,
        trailing_stop_loss: bool = False,
    ) -> CTraderExecutionResult:
        raw = self._run_worker(
            mode="amend_order",
            payload={
                "account_id": self._configured_account_id()[0],
                "order_id": int(order_id or 0),
                "limit_price": _safe_float(limit_price, 0.0),
                "stop_price": _safe_float(stop_price, 0.0),
                "stop_loss": _safe_float(stop_loss, 0.0),
                "take_profit": _safe_float(take_profit, 0.0),
                "volume": int(volume or 0),
                "trailing_stop_loss": bool(trailing_stop_loss),
            },
            timeout_sec=max(5, int(getattr(config, "CTRADER_EXECUTOR_TIMEOUT_SEC", 25) or 25)),
        )
        return self._result_from_dict(raw)

    def _post_accept_protection_repair(self, *, result: CTraderExecutionResult, payload: dict) -> dict:
        if not bool(getattr(result, "ok", False)) or bool(getattr(result, "dry_run", False)):
            return {}
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status not in {"accepted", "filled"}:
            return {}
        execution_meta = dict(getattr(result, "execution_meta", {}) or {})
        raw_execution = dict(execution_meta.get("raw_execution") or {})
        planned_sl = _safe_float(payload.get("stop_loss"), 0.0)
        planned_tp = _safe_float(payload.get("take_profit"), 0.0)
        if planned_sl <= 0 and planned_tp <= 0:
            return {}

        order_type = str(payload.get("order_type", "") or "").strip().lower()
        order = dict(raw_execution.get("order") or {})
        if status == "accepted" and order_type in {"limit", "stop"} and int(getattr(result, "order_id", 0) or 0) > 0:
            order_sl = _safe_float(order.get("stopLoss"), 0.0)
            order_tp = _safe_float(order.get("takeProfit"), 0.0)
            missing_sl = planned_sl > 0 and order_sl <= 0
            missing_tp = planned_tp > 0 and order_tp <= 0
            if missing_sl or missing_tp:
                amend_res = self.amend_order(
                    order_id=int(getattr(result, "order_id", 0) or 0),
                    limit_price=_safe_float(order.get("limitPrice"), _safe_float(payload.get("limit_price"), 0.0)),
                    stop_price=_safe_float(order.get("stopPrice"), _safe_float(payload.get("stop_price"), 0.0)),
                    stop_loss=planned_sl if planned_sl > 0 else order_sl,
                    take_profit=planned_tp if planned_tp > 0 else order_tp,
                    trailing_stop_loss=False,
                )
                return {
                    "attempted": True,
                    "action": "repair_order_protection",
                    "order_id": int(getattr(result, "order_id", 0) or 0),
                    "missing_stop_loss": bool(missing_sl),
                    "missing_take_profit": bool(missing_tp),
                    "ok": bool(amend_res.ok),
                    "status": str(amend_res.status or ""),
                    "message": str(amend_res.message or ""),
                }

        execution_type = str(execution_meta.get("execution_type") or "").strip().upper()
        position = dict(raw_execution.get("position") or {})
        if (
            int(getattr(result, "position_id", 0) or 0) > 0
            and (status == "filled" or execution_type in {"ORDER_FILLED", "ORDER_PARTIAL_FILL"})
        ):
            direction = str(payload.get("direction", "") or "").strip().lower()
            symbol = str(payload.get("market_symbol", getattr(result, "signal_symbol", "")) or getattr(result, "signal_symbol", "") or "").strip().upper()
            planned_entry = _safe_float(payload.get("entry"), 0.0)
            pos_sl = _safe_float(position.get("stopLoss", position.get("stop_loss")), 0.0)
            pos_tp = _safe_float(position.get("takeProfit", position.get("take_profit")), 0.0)
            pos_entry = _safe_float(position.get("price", position.get("entryPrice", position.get("entry_price"))), 0.0)
            missing_sl = planned_sl > 0 and pos_sl <= 0
            missing_tp = planned_tp > 0 and pos_tp <= 0
            clamp_plan = self._xau_post_fill_stop_clamp_plan(
                symbol=symbol,
                direction=direction,
                planned_entry=planned_entry,
                planned_stop_loss=planned_sl,
                live_entry=pos_entry,
                live_stop_loss=pos_sl,
                current_price=pos_entry,
            )
            if missing_sl or missing_tp or bool(clamp_plan.get("active")):
                target_sl = planned_sl if missing_sl else pos_sl
                if bool(clamp_plan.get("active")):
                    target_sl = _safe_float(clamp_plan.get("new_stop_loss"), target_sl)
                target_tp = planned_tp if (missing_tp and self._target_valid_for_position(direction, pos_entry or planned_entry, planned_tp)) else pos_tp
                amend_res = self.amend_position_sltp(
                    position_id=int(getattr(result, "position_id", 0) or 0),
                    stop_loss=target_sl,
                    take_profit=target_tp if self._target_valid_for_position(direction, pos_entry or planned_entry, target_tp) else 0.0,
                    trailing_stop_loss=False,
                )
                return {
                    "attempted": True,
                    "action": "clamp_position_stop_after_fill" if bool(clamp_plan.get("active")) else "repair_position_protection",
                    "position_id": int(getattr(result, "position_id", 0) or 0),
                    "missing_stop_loss": bool(missing_sl),
                    "missing_take_profit": bool(missing_tp),
                    "clamped_stop_loss": bool(clamp_plan.get("active")),
                    "new_stop_loss": _safe_float(target_sl, 0.0),
                    "details": dict(clamp_plan.get("details") or {}),
                    "ok": bool(amend_res.ok),
                    "status": str(amend_res.status or ""),
                    "message": str(amend_res.message or ""),
                }
        return {}

    def _pending_order_ttl_min(self, source: str, symbol: str) -> int:
        token = str(source or "").strip().lower()
        symbol_u = str(symbol or "").strip().upper()
        default_ttl = max(1, int(getattr(config, "CTRADER_PENDING_ORDER_TTL_DEFAULT_MIN", 120) or 120))
        if symbol_u == "XAUUSD":
            if ":td:" in token or "tick_depth_filter" in token:
                return max(1, int(getattr(config, "CTRADER_PENDING_ORDER_TTL_XAU_PULLBACK_MIN", 45) or 45))
            if ":bs:" in token or "breakout_stop" in token:
                return max(1, int(getattr(config, "CTRADER_PENDING_ORDER_TTL_XAU_BREAKOUT_MIN", 15) or 15))
            if ":pb:" in token or "pullback_limit" in token:
                return max(1, int(getattr(config, "CTRADER_PENDING_ORDER_TTL_XAU_PULLBACK_MIN", 45) or 45))
            if token.startswith("xauusd_scheduled"):
                return max(1, int(getattr(config, "CTRADER_PENDING_ORDER_TTL_XAU_SCHEDULED_MIN", 240) or 240))
            return max(1, int(getattr(config, "CTRADER_PENDING_ORDER_TTL_XAU_SCALP_MIN", 45) or 45))
        if symbol_u in {"BTCUSD", "ETHUSD"} and ":winner" in token:
            return max(1, int(getattr(config, "CTRADER_PENDING_ORDER_TTL_CRYPTO_WINNER_MIN", 180) or 180))
        return default_ttl

    def _mark_order_canceled(
        self,
        conn: sqlite3.Connection,
        *,
        order_id: int,
        journal_id: Optional[int],
        reason: str,
        cancel_result: Optional[CTraderExecutionResult] = None,
    ) -> None:
        now_iso = _utc_now_iso()
        conn.execute(
            """
            UPDATE ctrader_orders
               SET is_open=0,
                   order_status='canceled',
                   last_seen_utc=?
             WHERE order_id=?
            """,
            (now_iso, int(order_id)),
        )
        if int(journal_id or 0) <= 0:
            row = conn.execute("SELECT journal_id FROM ctrader_orders WHERE order_id=? LIMIT 1", (int(order_id),)).fetchone()
            journal_id = int(row[0]) if row is not None and int(row[0] or 0) > 0 else None
        if int(journal_id or 0) <= 0:
            return
        row = conn.execute(
            "SELECT response_json, execution_meta_json FROM execution_journal WHERE id=? LIMIT 1",
            (int(journal_id),),
        ).fetchone()
        response_json = self._safe_json_load(str((row["response_json"] if row else "") or ""))
        execution_meta = self._safe_json_load(str((row["execution_meta_json"] if row else "") or ""))
        response_json["cancel"] = {
            "order_id": int(order_id),
            "reason": str(reason or ""),
            "canceled_at": now_iso,
            "status": str(getattr(cancel_result, "status", "") or "canceled"),
        }
        execution_meta["canceled"] = {
            "order_id": int(order_id),
            "reason": str(reason or ""),
            "canceled_at": now_iso,
            "status": str(getattr(cancel_result, "status", "") or "canceled"),
        }
        if isinstance(getattr(cancel_result, "execution_meta", None), dict):
            execution_meta["cancel_execution"] = dict(cancel_result.execution_meta or {})
        conn.execute(
            """
            UPDATE execution_journal
               SET status='canceled',
                   message=?,
                   response_json=?,
                   execution_meta_json=?
             WHERE id=?
            """,
            (
                f"ctrader canceled pending order: {reason}",
                json.dumps(response_json, ensure_ascii=True, separators=(",", ":")),
                json.dumps(execution_meta, ensure_ascii=True, separators=(",", ":")),
                int(journal_id),
            ),
        )

    def _mark_order_follow_stop_launch(
        self,
        conn: sqlite3.Connection,
        *,
        journal_id: Optional[int],
        plan: dict,
        follow_source: str,
        follow_result: Optional[CTraderExecutionResult] = None,
    ) -> None:
        if int(journal_id or 0) <= 0:
            return
        row = conn.execute(
            "SELECT response_json, execution_meta_json FROM execution_journal WHERE id=? LIMIT 1",
            (int(journal_id),),
        ).fetchone()
        response_json = self._safe_json_load(str((row["response_json"] if row else "") or ""))
        execution_meta = self._safe_json_load(str((row["execution_meta_json"] if row else "") or ""))
        launch = {
            "launched_at": _utc_now_iso(),
            "source": str(follow_source or ""),
            "reason": str(plan.get("reason") or ""),
            "sample_tier": str(plan.get("sample_tier") or ""),
            "follow_direction": str(plan.get("follow_direction") or ""),
            "entry": round(_safe_float(plan.get("new_entry"), 0.0), 6),
            "stop_loss": round(_safe_float(plan.get("new_stop_loss"), 0.0), 6),
            "take_profit": round(_safe_float(plan.get("new_take_profit"), 0.0), 6),
            "capture_run_id": str(plan.get("capture_run_id") or ""),
            "features": dict(plan.get("features") or {}),
            "gate_reasons": list(plan.get("gate_reasons") or []),
            "result": {
                "ok": bool(getattr(follow_result, "ok", False)),
                "status": str(getattr(follow_result, "status", "") or ""),
                "message": str(getattr(follow_result, "message", "") or ""),
                "order_id": getattr(follow_result, "order_id", None),
                "position_id": getattr(follow_result, "position_id", None),
                "deal_id": getattr(follow_result, "deal_id", None),
            },
        }
        response_json["follow_stop_launch"] = launch
        execution_meta["follow_stop_launch"] = launch
        conn.execute(
            """
            UPDATE execution_journal
               SET response_json=?,
                   execution_meta_json=?
             WHERE id=?
            """,
            (
                json.dumps(response_json, ensure_ascii=True, separators=(",", ":")),
                json.dumps(execution_meta, ensure_ascii=True, separators=(",", ":")),
                int(journal_id),
            ),
        )

    def _latest_capture_snapshot(self, *, symbol: str, direction: str, confidence: float = 0.0) -> dict:
        try:
            if self._autopilot is None:
                from learning.live_profile_autopilot import LiveProfileAutopilot

                self._autopilot = LiveProfileAutopilot()
            return dict(
                self._autopilot.latest_capture_feature_snapshot(
                    symbol=str(symbol or "").strip().upper(),
                    direction=str(direction or "").strip().lower(),
                    confidence=_safe_float(confidence, 0.0),
                )
                or {}
            )
        except Exception as exc:
            return {
                "ok": False,
                "status": "capture_snapshot_error",
                "symbol": str(symbol or "").strip().upper(),
                "error": str(exc),
            }

    @staticmethod
    def _pending_reprice_state(execution_meta: dict) -> tuple[int, str]:
        meta = dict(execution_meta or {})
        rows = list(meta.get("pending_reprices") or [])
        last_ts = ""
        if rows:
            last = dict(rows[-1] or {})
            last_ts = str(last.get("repriced_at") or "")
        count = int(meta.get("pending_reprice_count", len(rows)) or len(rows))
        return max(0, count), last_ts

    def _pending_order_dynamic_max_distance_r(
        self,
        *,
        family: str,
        direction: str,
        confidence: float,
        symbol: str,
    ) -> tuple[float, dict]:
        base_r = max(0.25, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_MAX_DISTANCE_R", 1.45), 1.45))
        min_r = max(0.25, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_MAX_DISTANCE_MIN_R", 0.65), 0.65))
        max_r = max(min_r, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_MAX_DISTANCE_MAX_R", 1.75), 1.75))
        snapshot = self._latest_capture_snapshot(symbol=symbol, direction=direction, confidence=confidence)
        gate = dict(snapshot.get("gate") or {})
        features = dict(snapshot.get("features") or {})
        day_type = str(features.get("day_type") or "").strip().lower() or "trend"
        spread_expansion = max(0.0, _safe_float(features.get("spread_expansion"), 1.0))
        mid_drift_pct = abs(_safe_float(features.get("mid_drift_pct"), 0.0))
        rejection_ratio = max(0.0, min(1.0, _safe_float(features.get("rejection_ratio"), 0.0)))
        bar_volume_proxy = max(0.0, _safe_float(features.get("bar_volume_proxy"), 0.0))
        delta_proxy = _safe_float(features.get("delta_proxy"), 0.0)
        directional_delta = delta_proxy if direction == "short" else (-1.0 * delta_proxy)

        family_mult = 1.0
        if family == "xau_scalp_tick_depth_filter":
            family_mult = 0.92
        elif family == "xau_scalp_pullback_limit":
            family_mult = 1.00

        day_mult = {
            "trend": 0.88,
            "repricing": 0.80,
            "fast_expansion": 0.72,
            "panic_spread": 0.58,
        }.get(day_type, 0.90)

        spread_penalty = max(0.0, (spread_expansion - 1.0) * 0.55)
        drift_penalty = min(0.22, mid_drift_pct * 8.0)
        volume_penalty = min(0.16, max(0.0, bar_volume_proxy - 0.45) * 0.20)
        delta_penalty = min(0.14, max(0.0, directional_delta - 0.05) * 0.55)
        rejection_relief = min(0.16, max(0.0, rejection_ratio - 0.22) * 0.45)

        dynamic_r = base_r * family_mult * day_mult
        dynamic_r *= max(0.45, 1.0 - spread_penalty - drift_penalty - volume_penalty - delta_penalty + rejection_relief)

        if not bool(snapshot.get("ok")):
            dynamic_r = base_r * family_mult
        dynamic_r = min(max_r, max(min_r, dynamic_r))
        return dynamic_r, {
            "ok": bool(snapshot.get("ok")),
            "status": str(snapshot.get("status") or ""),
            "run_id": str(snapshot.get("run_id") or ""),
            "day_type": day_type,
            "spread_expansion": spread_expansion,
            "mid_drift_pct": mid_drift_pct,
            "rejection_ratio": rejection_ratio,
            "bar_volume_proxy": bar_volume_proxy,
            "directional_delta": directional_delta,
            "gate_reasons": list(gate.get("reasons") or []),
            "family_mult": round(family_mult, 4),
            "day_mult": round(day_mult, 4),
        }

    @staticmethod
    def _follow_stop_signal_from_order(order_row: dict, plan: dict):
        source = str(order_row.get("source", "") or "").strip().lower()
        symbol = str(order_row.get("symbol", "") or "").strip().upper()
        old_direction = str(order_row.get("direction", "") or "").strip().lower()
        follow_direction = str(plan.get("follow_direction") or "").strip().lower()
        order_id = int(order_row.get("order_id") or 0)
        confidence = max(70.0, _safe_float(plan.get("confidence"), 72.0))
        signal_run_no = int(order_row.get("signal_run_no", 0) or 0)
        signal_run_id = str(order_row.get("signal_run_id", "") or f"ff-{order_id}-{int(time.time())}")
        entry_type = "buy_stop" if follow_direction == "long" else "sell_stop"
        family_source = "scalp_xauusd:ff:canary"
        pattern = "FAILED_FADE_FOLLOW_STOP"
        reasons = [
            f"failed fade from {source or 'unknown'}",
            str(plan.get("reason") or "opposite_force_significant"),
        ]
        raw_scores = {
            "market_symbol": symbol,
            "signal_run_no": signal_run_no,
            "signal_run_id": signal_run_id,
            "ctrader_risk_usd_override": _safe_float(plan.get("risk_usd"), 0.0),
            "failed_fade_origin_source": source,
            "failed_fade_origin_order_id": order_id,
            "failed_fade_origin_direction": old_direction,
            "follow_stop_reason": str(plan.get("reason") or ""),
            "follow_stop_sample_tier": str(plan.get("sample_tier") or ""),
            "follow_stop_features": dict(plan.get("features") or {}),
            "follow_stop_gate_reasons": list(plan.get("gate_reasons") or []),
        }
        return SimpleNamespace(
            symbol=symbol,
            direction=follow_direction,
            confidence=confidence,
            entry=_safe_float(plan.get("new_entry"), 0.0),
            stop_loss=_safe_float(plan.get("new_stop_loss"), 0.0),
            take_profit_1=_safe_float(plan.get("new_take_profit"), 0.0),
            take_profit_2=_safe_float(plan.get("new_take_profit"), 0.0),
            take_profit_3=_safe_float(plan.get("new_take_profit"), 0.0),
            risk_reward=round(_safe_float(plan.get("tp_r"), 0.0) / max(_safe_float(plan.get("stop_r"), 1.0), 1e-8), 4),
            timeframe="5m+1m",
            session="",
            trend="",
            rsi=0.0,
            atr=0.0,
            pattern=pattern,
            reasons=reasons,
            warnings=["experimental_follow_stop"],
            raw_scores=raw_scores,
            entry_type=entry_type,
        ), family_source

    def _mark_order_repriced(
        self,
        conn: sqlite3.Connection,
        *,
        order_row: dict,
        journal_id: Optional[int],
        new_entry: float,
        new_stop_loss: float,
        new_take_profit: float,
        plan: dict,
        amend_result: Optional[CTraderExecutionResult] = None,
    ) -> None:
        order_id = int(order_row.get("order_id") or 0)
        if order_id <= 0:
            return
        now_iso = _utc_now_iso()
        existing_order = conn.execute(
            "SELECT raw_json, journal_id FROM ctrader_orders WHERE order_id=? LIMIT 1",
            (order_id,),
        ).fetchone()
        raw_json = self._safe_json_load(str((existing_order["raw_json"] if existing_order else "") or ""))
        if int(journal_id or 0) <= 0:
            journal_id = int(existing_order["journal_id"]) if existing_order is not None and int(existing_order["journal_id"] or 0) > 0 else None
        raw_json["_dexter_pending_order"] = {
            "repriced": True,
            "repriced_at": now_iso,
            "reason": str(plan.get("reason") or ""),
            "new_entry": round(_safe_float(new_entry, 0.0), 6),
            "new_stop_loss": round(_safe_float(new_stop_loss, 0.0), 6),
            "new_take_profit": round(_safe_float(new_take_profit, 0.0), 6),
        }
        conn.execute(
            """
            UPDATE ctrader_orders
               SET entry_price=?,
                   stop_loss=?,
                   take_profit=?,
                   last_seen_utc=?,
                   raw_json=?
             WHERE order_id=?
            """,
            (
                _safe_float(new_entry, 0.0),
                _safe_float(new_stop_loss, 0.0),
                _safe_float(new_take_profit, 0.0),
                now_iso,
                json.dumps(raw_json, ensure_ascii=True, separators=(",", ":")),
                order_id,
            ),
        )
        if int(journal_id or 0) <= 0:
            return
        row = conn.execute(
            "SELECT response_json, execution_meta_json FROM execution_journal WHERE id=? LIMIT 1",
            (int(journal_id),),
        ).fetchone()
        response_json = self._safe_json_load(str((row["response_json"] if row else "") or ""))
        execution_meta = self._safe_json_load(str((row["execution_meta_json"] if row else "") or ""))
        history = list(execution_meta.get("pending_reprices") or [])
        event = {
            "order_id": order_id,
            "repriced_at": now_iso,
            "reason": str(plan.get("reason") or ""),
            "old_entry": round(_safe_float(order_row.get("entry_price"), 0.0), 6),
            "new_entry": round(_safe_float(new_entry, 0.0), 6),
            "old_stop_loss": round(_safe_float(order_row.get("stop_loss"), 0.0), 6),
            "new_stop_loss": round(_safe_float(new_stop_loss, 0.0), 6),
            "old_take_profit": round(_safe_float(order_row.get("take_profit"), 0.0), 6),
            "new_take_profit": round(_safe_float(new_take_profit, 0.0), 6),
            "approach_r": round(_safe_float(plan.get("approach_r"), 0.0), 4),
            "features": dict(plan.get("features") or {}),
            "gate_reasons": list(plan.get("gate_reasons") or []),
        }
        history.append(event)
        history = history[-6:]
        execution_meta["pending_reprice"] = dict(event)
        execution_meta["pending_reprices"] = history
        execution_meta["pending_reprice_count"] = len(history)
        if isinstance(getattr(amend_result, "execution_meta", None), dict):
            execution_meta["pending_reprice_execution"] = dict(amend_result.execution_meta or {})
        response_json["pending_reprice"] = {
            "order_id": order_id,
            "repriced_at": now_iso,
            "reason": str(plan.get("reason") or ""),
            "new_entry": round(_safe_float(new_entry, 0.0), 6),
        }
        conn.execute(
            """
            UPDATE execution_journal
               SET entry=?,
                   stop_loss=?,
                   take_profit=?,
                   message=?,
                   response_json=?,
                   execution_meta_json=?
             WHERE id=?
            """,
            (
                _safe_float(new_entry, 0.0),
                _safe_float(new_stop_loss, 0.0),
                _safe_float(new_take_profit, 0.0),
                f"ctrader repriced pending order: {plan.get('reason') or 'retreat'}",
                json.dumps(response_json, ensure_ascii=True, separators=(",", ":")),
                json.dumps(execution_meta, ensure_ascii=True, separators=(",", ":")),
                int(journal_id),
            ),
        )

    def _pending_order_reprice_plan(
        self,
        conn: sqlite3.Connection,
        order_row: dict,
        *,
        now_ts: float,
    ) -> dict:
        if not bool(getattr(config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ENABLED", False)):
            return {}
        source = str(order_row.get("source", "") or "").strip().lower()
        symbol = str(order_row.get("symbol", "") or "").strip().upper()
        direction = str(order_row.get("direction", "") or "").strip().lower()
        order_type = str(order_row.get("order_type", "") or "").strip().lower()
        family = self._source_family(source)
        if symbol != "XAUUSD" or order_type != "limit" or direction not in {"long", "short"}:
            return {}
        allowed_families = set(getattr(config, "get_ctrader_pending_order_dynamic_reprice_families", lambda: set())() or set())
        if allowed_families and family not in allowed_families:
            return {}
        created_ts = _safe_float(order_row.get("created_ts"), 0.0)
        age_sec = max(0.0, float(now_ts) - created_ts) if created_ts > 0 else 0.0
        min_age_sec = max(5, int(getattr(config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_AGE_SEC", 75) or 75))
        if age_sec < float(min_age_sec):
            return {}
        entry = _safe_float(order_row.get("entry_price"), 0.0)
        stop_loss = _safe_float(order_row.get("stop_loss"), 0.0)
        take_profit = _safe_float(order_row.get("take_profit"), 0.0)
        risk = abs(entry - stop_loss)
        if entry <= 0 or risk <= 0 or not self._stop_valid_for_position(direction, entry, stop_loss):
            return {}
        if not self._target_valid_for_position(direction, entry, take_profit):
            return {}
        journal_id = int(order_row.get("journal_id") or 0)
        confidence = 0.0
        execution_meta = {}
        if journal_id > 0:
            journal_row = conn.execute(
                "SELECT confidence, execution_meta_json FROM execution_journal WHERE id=? LIMIT 1",
                (journal_id,),
            ).fetchone()
            if journal_row is not None:
                confidence = _safe_float(journal_row["confidence"], 0.0)
                execution_meta = self._safe_json_load(str(journal_row["execution_meta_json"] or "{}"))
        reprice_count, last_reprice_iso = self._pending_reprice_state(execution_meta)
        cooldown_sec = max(0, int(getattr(config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_COOLDOWN_SEC", 75) or 75))
        if cooldown_sec > 0 and _iso_to_ms(last_reprice_iso) > 0:
            since_reprice = float(now_ts) - (_iso_to_ms(last_reprice_iso) / 1000.0)
            if since_reprice < float(cooldown_sec):
                return {}
        ref = self._reference_price(symbol)
        if ref <= 0:
            return {}
        approach_r = ((ref - entry) / risk) if direction == "short" else ((entry - ref) / risk)
        trigger_r = max(0.0, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_TRIGGER_R", 0.18), 0.18))
        if approach_r < trigger_r:
            return {}
        snapshot = self._latest_capture_snapshot(symbol=symbol, direction=direction, confidence=confidence)
        if bool(getattr(config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_REQUIRE_CAPTURE", True)) and not bool(snapshot.get("ok", False)):
            return {}
        gate = dict(snapshot.get("gate") or {})
        if bool(gate.get("pass", False)):
            return {}
        features = dict(snapshot.get("features") or {})
        day_type = str(features.get("day_type") or "").strip().lower()
        if not day_type:
            try:
                from learning.live_profile_autopilot import classify_xau_day_type

                day_type = str((classify_xau_day_type(features) or {}).get("day_type") or "").strip().lower()
            except Exception:
                day_type = ""
        if not day_type:
            day_type = "trend"
        features["day_type"] = day_type
        mid_drift_pct = _safe_float(features.get("mid_drift_pct"), 0.0)
        min_mid_drift_pct = max(0.0, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_MID_DRIFT_PCT", 0.006), 0.006))
        adverse_continuation = (
            mid_drift_pct >= min_mid_drift_pct if direction == "short" else mid_drift_pct <= (-1.0 * min_mid_drift_pct)
        )
        if not adverse_continuation:
            if direction == "short":
                adverse_continuation = ref >= entry
            else:
                adverse_continuation = ref <= entry
        if not adverse_continuation:
            return {}
        follow_enabled = bool(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENABLED", False))
        follow_allowed = set(getattr(config, "get_ctrader_pending_order_follow_stop_families", lambda: set())() or set())
        if day_type == "panic_spread" and bool(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_PANIC_SPREAD_DISABLE", True)):
            follow_enabled = False
        if follow_enabled and (not follow_allowed or family in follow_allowed):
            follow_trigger_r = max(0.0, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_TRIGGER_R", 0.34), 0.34))
            follow_mid_drift = max(0.0, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_MID_DRIFT_PCT", 0.012), 0.012))
            follow_imbalance = max(0.0, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_IMBALANCE", 0.02), 0.02))
            follow_rejection = max(0.0, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MAX_REJECTION", 0.12), 0.12))
            follow_delta_proxy = max(0.0, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_DELTA_PROXY", 0.12), 0.12))
            follow_bar_volume_proxy = max(0.0, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_BAR_VOLUME_PROXY", 0.35), 0.35))
            follow_sample_enabled = bool(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_ENABLED", True))
            follow_sample_min_conf = max(0.0, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_MIN_CONFIDENCE", 74.0), 74.0))
            follow_sample_trigger_mult = max(0.1, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_TRIGGER_R_MULT", 0.88), 0.88))
            follow_sample_imbalance_mult = max(0.1, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_IMBALANCE_MULT", 0.75), 0.75))
            follow_sample_delta_mult = max(0.1, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_DELTA_MULT", 0.75), 0.75))
            follow_sample_bar_volume_mult = max(0.1, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_BAR_VOLUME_MULT", 0.90), 0.90))
            follow_sample_risk_mult = max(0.1, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_RISK_MULT", 0.70), 0.70))
            follow_secondary_sample_enabled = bool(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_ENABLED", True))
            follow_secondary_sample_min_conf = max(
                0.0,
                _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_MIN_CONFIDENCE", 75.0), 75.0),
            )
            follow_secondary_sample_trigger_mult = max(
                0.1,
                _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_TRIGGER_R_MULT", 0.82), 0.82),
            )
            follow_secondary_sample_imbalance_mult = max(
                0.1,
                _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_IMBALANCE_MULT", 0.65), 0.65),
            )
            follow_secondary_sample_delta_mult = max(
                0.1,
                _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_DELTA_MULT", 0.65), 0.65),
            )
            follow_secondary_sample_bar_volume_mult = max(
                0.1,
                _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_BAR_VOLUME_MULT", 1.05), 1.05),
            )
            follow_secondary_sample_rejection_mult = max(
                0.1,
                _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_REJECTION_MULT", 1.10), 1.10),
            )
            follow_secondary_sample_risk_mult = max(
                0.1,
                _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_RISK_MULT", 0.55), 0.55),
            )
            if day_type in {"repricing", "fast_expansion"} and bool(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_ENABLED", True)):
                follow_sample_min_conf += float(
                    getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_MIN_CONFIDENCE_DELTA", -1.0) or -1.0
                )
                follow_sample_trigger_mult *= max(
                    0.1,
                    _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_TRIGGER_MULT", 0.90), 0.90),
                )
                follow_sample_imbalance_mult *= max(
                    0.1,
                    _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_IMBALANCE_MULT", 0.90), 0.90),
                )
                follow_sample_delta_mult *= max(
                    0.1,
                    _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_DELTA_MULT", 0.90), 0.90),
                )
                follow_sample_bar_volume_mult *= max(
                    0.1,
                    _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_BAR_VOLUME_MULT", 0.95), 0.95),
                )
            imbalance = _safe_float(features.get("depth_imbalance"), 0.0)
            rejection = _safe_float(features.get("rejection_ratio"), 1.0)
            delta_proxy = _safe_float(features.get("delta_proxy"), 0.0)
            bar_volume_proxy = _safe_float(features.get("bar_volume_proxy"), 0.0)
            follow_sample_mode = False
            follow_sample_tier = ""
            if direction == "short":
                opposite_force_significant = (
                    approach_r >= follow_trigger_r
                    and mid_drift_pct >= follow_mid_drift
                    and imbalance >= follow_imbalance
                    and rejection <= follow_rejection
                    and delta_proxy >= follow_delta_proxy
                    and bar_volume_proxy >= follow_bar_volume_proxy
                )
                follow_direction = "long"
            else:
                opposite_force_significant = (
                    approach_r >= follow_trigger_r
                    and mid_drift_pct <= (-1.0 * follow_mid_drift)
                    and imbalance <= (-1.0 * follow_imbalance)
                    and rejection <= follow_rejection
                    and delta_proxy <= (-1.0 * follow_delta_proxy)
                    and bar_volume_proxy >= follow_bar_volume_proxy
                )
                follow_direction = "short"
            if (
                (not opposite_force_significant)
                and follow_sample_enabled
                and confidence >= follow_sample_min_conf
            ):
                if direction == "short":
                    opposite_force_significant = (
                        approach_r >= (follow_trigger_r * follow_sample_trigger_mult)
                        and mid_drift_pct >= follow_mid_drift
                        and imbalance >= (follow_imbalance * follow_sample_imbalance_mult)
                        and rejection <= follow_rejection
                        and delta_proxy >= (follow_delta_proxy * follow_sample_delta_mult)
                        and bar_volume_proxy >= (follow_bar_volume_proxy * follow_sample_bar_volume_mult)
                    )
                else:
                    opposite_force_significant = (
                        approach_r >= (follow_trigger_r * follow_sample_trigger_mult)
                        and mid_drift_pct <= (-1.0 * follow_mid_drift)
                        and imbalance <= (-1.0 * follow_imbalance * follow_sample_imbalance_mult)
                        and rejection <= follow_rejection
                        and delta_proxy <= (-1.0 * follow_delta_proxy * follow_sample_delta_mult)
                        and bar_volume_proxy >= (follow_bar_volume_proxy * follow_sample_bar_volume_mult)
                    )
                follow_sample_mode = bool(opposite_force_significant)
                if follow_sample_mode:
                    follow_sample_tier = "primary"
            if (
                (not opposite_force_significant)
                and follow_secondary_sample_enabled
                and confidence >= follow_secondary_sample_min_conf
            ):
                if direction == "short":
                    opposite_force_significant = (
                        approach_r >= (follow_trigger_r * follow_secondary_sample_trigger_mult)
                        and mid_drift_pct >= follow_mid_drift
                        and imbalance >= (follow_imbalance * follow_secondary_sample_imbalance_mult)
                        and rejection <= (follow_rejection * follow_secondary_sample_rejection_mult)
                        and delta_proxy >= (follow_delta_proxy * follow_secondary_sample_delta_mult)
                        and bar_volume_proxy >= (follow_bar_volume_proxy * follow_secondary_sample_bar_volume_mult)
                    )
                else:
                    opposite_force_significant = (
                        approach_r >= (follow_trigger_r * follow_secondary_sample_trigger_mult)
                        and mid_drift_pct <= (-1.0 * follow_mid_drift)
                        and imbalance <= (-1.0 * follow_imbalance * follow_secondary_sample_imbalance_mult)
                        and rejection <= (follow_rejection * follow_secondary_sample_rejection_mult)
                        and delta_proxy <= (-1.0 * follow_delta_proxy * follow_secondary_sample_delta_mult)
                        and bar_volume_proxy >= (follow_bar_volume_proxy * follow_secondary_sample_bar_volume_mult)
                    )
                if opposite_force_significant:
                    follow_sample_mode = True
                    follow_sample_tier = "secondary"
            if opposite_force_significant:
                entry_buffer_r = max(0.01, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENTRY_BUFFER_R", 0.10), 0.10))
                stop_r = max(0.10, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_STOP_R", 0.58), 0.58))
                tp_r = max(0.10, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_TP_R", 0.88), 0.88))
                risk_usd = max(0.0, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_RISK_USD", 0.50), 0.50))
                if follow_sample_mode:
                    risk_usd *= follow_secondary_sample_risk_mult if follow_sample_tier == "secondary" else follow_sample_risk_mult
                if follow_direction == "long":
                    new_entry = ref + (entry_buffer_r * risk)
                    new_stop_loss = new_entry - (stop_r * risk)
                    new_take_profit = new_entry + (tp_r * risk)
                else:
                    new_entry = ref - (entry_buffer_r * risk)
                    new_stop_loss = new_entry + (stop_r * risk)
                    new_take_profit = new_entry - (tp_r * risk)
                if self._stop_valid_for_position(follow_direction, new_entry, new_stop_loss) and self._target_valid_for_position(follow_direction, new_entry, new_take_profit):
                    return {
                        "action": "follow_stop",
                        "reason": (
                            "failed_fade_opposite_force_secondary_sample"
                            if follow_sample_tier == "secondary"
                            else "failed_fade_opposite_force_sample"
                        ) if follow_sample_mode else "failed_fade_opposite_force_significant",
                        "order_id": int(order_row.get("order_id") or 0),
                        "journal_id": journal_id,
                        "follow_direction": follow_direction,
                        "new_entry": round(new_entry, 6),
                        "new_stop_loss": round(new_stop_loss, 6),
                        "new_take_profit": round(new_take_profit, 6),
                        "stop_r": round(stop_r, 4),
                        "tp_r": round(tp_r, 4),
                        "risk_usd": round(risk_usd, 4),
                        "confidence": max(confidence, 72.0),
                        "sample_mode": bool(follow_sample_mode),
                        "sample_tier": str(follow_sample_tier or ""),
                        "approach_r": round(approach_r, 4),
                        "features": features,
                        "gate_reasons": list(gate.get("reasons") or []),
                        "capture_status": str(snapshot.get("status") or ""),
                        "capture_run_id": str(snapshot.get("run_id") or ""),
                        "day_type": day_type,
                    }
        max_count = max(1, int(getattr(config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MAX_COUNT", 2) or 2))
        if reprice_count >= max_count:
            return {
                "action": "cancel",
                "reason": "max_reprices_no_weakening",
                "approach_r": round(approach_r, 4),
                "features": features,
                "gate_reasons": list(gate.get("reasons") or []),
                "journal_id": journal_id,
                "day_type": day_type,
            }
        step_r = max(0.01, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_STEP_R", 0.22), 0.22))
        buffer_r = max(0.01, _safe_float(getattr(config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_BUFFER_R", 0.16), 0.16))
        if direction == "short":
            target_entry = max(entry + (step_r * risk), ref + (buffer_r * risk))
            delta = max(0.0, target_entry - entry)
            new_entry = entry + delta
            new_stop_loss = stop_loss + delta
            new_take_profit = take_profit + delta
        else:
            target_entry = min(entry - (step_r * risk), ref - (buffer_r * risk))
            delta = max(0.0, entry - target_entry)
            new_entry = entry - delta
            new_stop_loss = stop_loss - delta
            new_take_profit = take_profit - delta
        if delta <= 1e-8:
            return {}
        if not self._stop_valid_for_position(direction, new_entry, new_stop_loss):
            return {}
        if not self._target_valid_for_position(direction, new_entry, new_take_profit):
            return {}
        return {
            "action": "amend",
            "reason": "retreat_no_weakening",
            "order_id": int(order_row.get("order_id") or 0),
            "journal_id": journal_id,
            "new_entry": round(new_entry, 6),
            "new_stop_loss": round(new_stop_loss, 6),
            "new_take_profit": round(new_take_profit, 6),
            "approach_r": round(approach_r, 4),
            "features": features,
            "gate_reasons": list(gate.get("reasons") or []),
            "capture_status": str(snapshot.get("status") or ""),
            "capture_run_id": str(snapshot.get("run_id") or ""),
            "day_type": day_type,
        }

    def _pending_order_cancel_reason(self, order_row: dict, *, now_ts: float) -> str:
        source = str(order_row.get("source", "") or "").strip().lower()
        symbol = str(order_row.get("symbol", "") or "").strip().upper()
        direction = str(order_row.get("direction", "") or "").strip().lower()
        order_type = str(order_row.get("order_type", "") or "").strip().lower()
        family = self._source_family(source)
        if bool(getattr(config, "CTRADER_PENDING_ORDER_CANCEL_DISABLED_SOURCE", True)):
            if source and (not self._source_allowed(source) or (symbol and not self._symbol_allowed(symbol))):
                return "disabled_source_or_symbol"
        if (
            symbol == "XAUUSD"
            and family
            and bool(getattr(config, "CTRADER_PENDING_ORDER_CANCEL_DISABLED_FAMILY", True))
        ):
            runtime_families = self._xau_family_runtime_allowlist()
            if (
                runtime_families
                and family not in runtime_families
                and family != "xau_scalp_microtrend"
                and ":canary" not in source
            ):
                return f"disabled_family:{family}"
        created_ts = _safe_float(order_row.get("created_ts"), 0.0)
        age_min = ((float(now_ts) - created_ts) / 60.0) if created_ts > 0 else 0.0
        ttl_min = float(self._pending_order_ttl_min(source, symbol))
        if age_min >= max(1.0, ttl_min):
            return f"stale_ttl:{int(ttl_min)}m"
        if (
            bool(getattr(config, "CTRADER_PENDING_ORDER_MAX_DISTANCE_ENABLED", True))
            and symbol == "XAUUSD"
            and order_type == "limit"
            and family in set(getattr(config, "get_ctrader_pending_order_dynamic_reprice_families", lambda: set())() or set())
        ):
            min_age_sec = max(10, int(getattr(config, "CTRADER_PENDING_ORDER_MAX_DISTANCE_MIN_AGE_SEC", 120) or 120))
            age_sec = max(0.0, float(now_ts) - created_ts) if created_ts > 0 else 0.0
            if age_sec >= float(min_age_sec):
                entry = _safe_float(order_row.get("entry_price"), 0.0)
                stop_loss = _safe_float(order_row.get("stop_loss"), 0.0)
                risk = abs(entry - stop_loss)
                ref = self._reference_price(symbol)
                if ref > 0 and risk > 0:
                    confidence = 0.0
                    journal_id = int(order_row.get("journal_id") or 0)
                    if journal_id > 0:
                        try:
                            with sqlite3.connect(self.db_path) as conf_conn:
                                conf_row = conf_conn.execute(
                                    "SELECT confidence FROM execution_journal WHERE id=? LIMIT 1",
                                    (journal_id,),
                                ).fetchone()
                            if conf_row is not None:
                                confidence = _safe_float(conf_row[0], 0.0)
                        except Exception:
                            confidence = 0.0
                    distance_r = ((entry - ref) / risk) if direction == "short" else ((ref - entry) / risk)
                    max_distance_r, distance_ctx = self._pending_order_dynamic_max_distance_r(
                        family=family,
                        direction=direction,
                        confidence=confidence,
                        symbol=symbol,
                    )
                    if distance_r >= max_distance_r:
                        return f"far_from_market:{round(distance_r, 2)}r>={round(max_distance_r, 2)}r:{distance_ctx.get('day_type','trend')}"
        order_id = int(order_row.get("order_id") or 0)
        snapshot = self._active_exposure_snapshot(symbol=symbol, exclude_order_id=order_id)
        side_total = int(snapshot["active_long"] if direction == "long" else snapshot["active_short"])
        opp_total = int(snapshot["active_short"] if direction == "long" else snapshot["active_long"])
        if bool(getattr(config, "CTRADER_BLOCK_OPPOSITE_DIRECTION", True)) and direction in {"long", "short"} and opp_total > 0:
            return f"opposite_direction_open:{symbol}:{opp_total}"
        max_symbol = max(1, int(getattr(config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 3) or 3))
        max_direction = max(1, int(getattr(config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2) or 2))
        if int(snapshot["active_total"]) >= max_symbol:
            return f"symbol_position_cap:{symbol}:{int(snapshot['active_total'])}"
        if direction in {"long", "short"} and side_total >= max_direction:
            return f"direction_position_cap:{symbol}:{side_total}"
        return ""

    def _sweep_pending_orders(self, open_orders: list[dict]) -> dict:
        report = {
            "open_orders": len(list(open_orders or [])),
            "canceled_orders": 0,
            "repriced_orders": 0,
            "follow_stop_orders": 0,
            "cancel_actions": [],
            "reprice_actions": [],
            "follow_stop_actions": [],
        }
        if not bool(getattr(config, "CTRADER_PENDING_ORDER_SWEEP_ENABLED", True)):
            return report
        now_ts = time.time()
        grace_min = max(0, int(getattr(config, "CTRADER_PENDING_ORDER_GRACE_MIN", 5) or 5))
        max_per_bucket = max(1, int(getattr(config, "CTRADER_PENDING_ORDER_MAX_PER_SOURCE_SYMBOL", 3) or 3))
        max_per_symbol = max(1, int(getattr(config, "CTRADER_PENDING_ORDER_MAX_PER_SYMBOL", 2) or 2))
        buckets: dict[tuple[str, str], list[dict]] = {}
        symbol_buckets: dict[str, list[dict]] = {}
        for row in list(open_orders or []):
            normalized = dict(row)
            buckets.setdefault(
                (str(normalized.get("source", "") or "").strip().lower(), str(normalized.get("symbol", "") or "").strip().upper()),
                [],
            ).append(normalized)
            symbol_buckets.setdefault(str(normalized.get("symbol", "") or "").strip().upper(), []).append(normalized)
        cancel_candidates: dict[int, str] = {}
        for row in list(open_orders or []):
            order_id = int(row.get("order_id") or 0)
            if order_id <= 0:
                continue
            reason = self._pending_order_cancel_reason(row, now_ts=now_ts)
            if reason:
                cancel_candidates[order_id] = reason
        for bucket_rows in buckets.values():
            bucket_rows = sorted(bucket_rows, key=lambda item: _safe_float(item.get("created_ts"), 0.0), reverse=True)
            if len(bucket_rows) <= max_per_bucket:
                continue
            for extra in bucket_rows[max_per_bucket:]:
                order_id = int(extra.get("order_id") or 0)
                age_min = ((now_ts - _safe_float(extra.get("created_ts"), 0.0)) / 60.0) if _safe_float(extra.get("created_ts"), 0.0) > 0 else 0.0
                if order_id > 0 and age_min >= float(grace_min):
                    cancel_candidates.setdefault(order_id, f"excess_pending:max{max_per_bucket}")
        for bucket_rows in symbol_buckets.values():
            bucket_rows = sorted(bucket_rows, key=lambda item: _safe_float(item.get("created_ts"), 0.0), reverse=True)
            if len(bucket_rows) <= max_per_symbol:
                continue
            for extra in bucket_rows[max_per_symbol:]:
                order_id = int(extra.get("order_id") or 0)
                age_min = ((now_ts - _safe_float(extra.get("created_ts"), 0.0)) / 60.0) if _safe_float(extra.get("created_ts"), 0.0) > 0 else 0.0
                if order_id > 0 and age_min >= float(grace_min):
                    cancel_candidates.setdefault(order_id, f"excess_symbol_pending:max{max_per_symbol}")
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            repriced_ids: set[int] = set()
            for order in list(open_orders or []):
                order_id = int(order.get("order_id") or 0)
                if order_id <= 0 or order_id in cancel_candidates:
                    continue
                plan = self._pending_order_reprice_plan(conn, order, now_ts=now_ts)
                action = str(plan.get("action") or "")
                if action == "amend":
                    amend_result = self.amend_order(
                        order_id=order_id,
                        limit_price=_safe_float(plan.get("new_entry"), 0.0),
                        stop_loss=_safe_float(plan.get("new_stop_loss"), 0.0),
                        take_profit=_safe_float(plan.get("new_take_profit"), 0.0),
                        volume=int(_safe_float(order.get("volume"), 0.0)),
                    )
                    if not bool(amend_result.ok):
                        report["reprice_actions"].append({
                            "order_id": order_id,
                            "source": str(order.get("source", "") or ""),
                            "symbol": str(order.get("symbol", "") or ""),
                            "action": "amend_failed",
                            "reason": str(plan.get("reason") or ""),
                            "status": str(amend_result.status or ""),
                            "message": str(amend_result.message or ""),
                        })
                        continue
                    self._mark_order_repriced(
                        conn,
                        order_row=order,
                        journal_id=int(order.get("journal_id") or 0) or None,
                        new_entry=_safe_float(plan.get("new_entry"), 0.0),
                        new_stop_loss=_safe_float(plan.get("new_stop_loss"), 0.0),
                        new_take_profit=_safe_float(plan.get("new_take_profit"), 0.0),
                        plan=plan,
                        amend_result=amend_result,
                    )
                    repriced_ids.add(order_id)
                    report["repriced_orders"] += 1
                    report["reprice_actions"].append({
                        "order_id": order_id,
                        "source": str(order.get("source", "") or ""),
                        "symbol": str(order.get("symbol", "") or ""),
                        "action": "amended",
                        "reason": str(plan.get("reason") or ""),
                        "new_entry": _safe_float(plan.get("new_entry"), 0.0),
                        "new_stop_loss": _safe_float(plan.get("new_stop_loss"), 0.0),
                        "new_take_profit": _safe_float(plan.get("new_take_profit"), 0.0),
                        "approach_r": _safe_float(plan.get("approach_r"), 0.0),
                    })
                    continue
                if action == "cancel":
                    cancel_candidates.setdefault(order_id, str(plan.get("reason") or "cancel_after_reprice"))
                    continue
                if action == "follow_stop":
                    cancel_reason = f"follow_stop_flip:{str(plan.get('reason') or 'follow_stop')}"
                    cancel_result = self.cancel_order(order_id=order_id)
                    if not bool(cancel_result.ok):
                        report["cancel_actions"].append({
                            "order_id": order_id,
                            "source": str(order.get("source", "") or ""),
                            "symbol": str(order.get("symbol", "") or ""),
                            "reason": cancel_reason,
                            "action": "follow_stop_cancel_failed",
                            "status": str(cancel_result.status or ""),
                            "message": str(cancel_result.message or ""),
                        })
                        continue
                    self._mark_order_canceled(
                        conn,
                        order_id=order_id,
                        journal_id=int(order.get("journal_id") or 0) or None,
                        reason=cancel_reason,
                        cancel_result=cancel_result,
                    )
                    report["canceled_orders"] += 1
                    report["cancel_actions"].append({
                        "order_id": order_id,
                        "source": str(order.get("source", "") or ""),
                        "symbol": str(order.get("symbol", "") or ""),
                        "reason": cancel_reason,
                        "action": "follow_stop_cancelled_origin",
                        "status": str(cancel_result.status or ""),
                    })
                    follow_signal, follow_source = self._follow_stop_signal_from_order(order, plan)
                    follow_result = self.execute_signal(follow_signal, source=follow_source)
                    self._mark_order_follow_stop_launch(
                        conn,
                        journal_id=int(order.get("journal_id") or 0) or None,
                        plan=plan,
                        follow_source=follow_source,
                        follow_result=follow_result,
                    )
                    if bool(follow_result.ok):
                        report["follow_stop_orders"] += 1
                    report["follow_stop_actions"].append({
                        "order_id": order_id,
                        "origin_source": str(order.get("source", "") or ""),
                        "symbol": str(order.get("symbol", "") or ""),
                        "reason": str(plan.get("reason") or ""),
                        "follow_source": follow_source,
                        "follow_direction": str(plan.get("follow_direction") or ""),
                        "entry": _safe_float(plan.get("new_entry"), 0.0),
                        "stop_loss": _safe_float(plan.get("new_stop_loss"), 0.0),
                        "take_profit": _safe_float(plan.get("new_take_profit"), 0.0),
                        "status": str(getattr(follow_result, "status", "") or ""),
                        "ok": bool(getattr(follow_result, "ok", False)),
                        "message": str(getattr(follow_result, "message", "") or ""),
                    })
            for order in list(open_orders or []):
                order_id = int(order.get("order_id") or 0)
                if order_id in repriced_ids:
                    continue
                reason = str(cancel_candidates.get(order_id) or "")
                if order_id <= 0 or not reason:
                    continue
                result = self.cancel_order(order_id=order_id)
                if not bool(result.ok):
                    report["cancel_actions"].append({
                        "order_id": order_id,
                        "source": str(order.get("source", "") or ""),
                        "symbol": str(order.get("symbol", "") or ""),
                        "reason": reason,
                        "status": str(result.status or ""),
                        "message": str(result.message or ""),
                    })
                    continue
                self._mark_order_canceled(
                    conn,
                    order_id=order_id,
                    journal_id=int(order.get("journal_id") or 0) or None,
                    reason=reason,
                    cancel_result=result,
                )
                report["canceled_orders"] += 1
                report["cancel_actions"].append({
                    "order_id": order_id,
                    "source": str(order.get("source", "") or ""),
                    "symbol": str(order.get("symbol", "") or ""),
                    "reason": reason,
                    "status": str(result.status or ""),
                })
            conn.commit()
        return report

    def _manage_open_positions(self, tracked_positions: list[dict]) -> dict:
        report = {
            "managed_positions": 0,
            "amended_positions": 0,
            "closed_profit_positions": 0,
            "pm_actions": [],
        }
        if not bool(getattr(config, "CTRADER_POSITION_MANAGER_ENABLED", True)):
            return report
        if not tracked_positions:
            return report
        price_cache: dict[str, float] = {}
        close_on_planned_target = bool(getattr(config, "CTRADER_PM_CLOSE_AT_PLANNED_TARGET", True))
        invalid_tp_close_r = _safe_float(getattr(config, "CTRADER_PM_INVALID_TP_CLOSE_R", 0.25), 0.25)
        invalid_tp_be_trigger_r = _safe_float(getattr(config, "CTRADER_PM_INVALID_TP_BE_TRIGGER_R", 0.12), 0.12)
        invalid_tp_be_lock_r = _safe_float(getattr(config, "CTRADER_PM_INVALID_TP_BE_LOCK_R", 0.02), 0.02)
        repair_missing_sl = bool(getattr(config, "CTRADER_PM_REPAIR_MISSING_SL_ENABLED", True))
        for item in list(tracked_positions or []):
            pos = dict(item.get("position") or {})
            journal_row = item.get("journal_row")
            journal_obj = dict(journal_row) if journal_row is not None else {}
            journal_id = int(journal_obj.get("id", 0) or 0)
            source = str(item.get("source") or pos.get("source") or "").strip().lower()
            lane = str(item.get("lane", "") or pos.get("lane", "") or "").strip().lower()
            symbol = str(pos.get("symbol", "") or "").strip().upper()
            direction = str(pos.get("direction", "") or "").strip().lower()
            position_id = int(_safe_float(pos.get("position_id"), 0))
            volume = int(_safe_float(pos.get("volume"), 0.0))
            entry = _safe_float(pos.get("entry_price"), 0.0)
            stop_loss = _safe_float(pos.get("stop_loss"), 0.0)
            live_tp = _safe_float(pos.get("take_profit"), 0.0)
            if position_id <= 0 or entry <= 0 or direction not in {"long", "short"}:
                continue
            if source == "untagged_external" or lane == "external":
                logger.warning("[PM] skip external position %s (%s) — source=%s lane=%s — not managed by Dexter", position_id, symbol, source, lane)
                continue
            risk = abs(entry - stop_loss) if stop_loss > 0 else 0.0
            if stop_loss > 0 and risk <= 0:
                continue
            ref = price_cache.get(symbol)
            if ref is None:
                ref = self._reference_price(symbol)
                price_cache[symbol] = ref
            if _safe_float(ref, 0.0) <= 0:
                # When the live reference price is unavailable (e.g. crypto_provider disabled),
                # we must not run any SL-breach close decisions. But we *can* still repair a
                # missing/invalid SL using the planned SL stored in `execution_journal`.
                live_sl_valid = self._stop_valid_for_position(direction, entry, stop_loss)
                if repair_missing_sl and not live_sl_valid:
                    planned_tp = _safe_float(journal_row["take_profit"], 0.0) if journal_row is not None else 0.0
                    planned_sl = _safe_float(journal_row["stop_loss"], 0.0) if journal_row is not None else 0.0
                    planned_rr = self._planned_rr(journal_row)
                    planned_risk = self._planned_risk(journal_row, entry_price=entry, stop_loss=planned_sl or stop_loss)

                    target_sl = planned_sl
                    if not self._stop_valid_for_position(direction, entry, target_sl):
                        # If the planned SL doesn't match the live filled entry, lock a SL that preserves the planned risk.
                        if planned_risk > 0:
                            target_sl = (entry - planned_risk) if direction == "long" else (entry + planned_risk)

                    if self._stop_valid_for_position(direction, entry, target_sl):
                        # cTrader enforces SL outside the current spread.
                        # If we don't have a reference price, capture bid/ask from cTrader and clamp SL accordingly.
                        bid_px: float = 0.0
                        ask_px: float = 0.0
                        try:
                            cap = self.capture_market_data(
                                symbols=[symbol],
                                duration_sec=3,
                                include_depth=False,
                                max_events=20,
                            )
                            spots = list(cap.get("spots") or [])
                            for sp in reversed(spots):
                                sp_sym = str(sp.get("symbol", "") or "").strip().upper()
                                if sp_sym == symbol:
                                    bid_px = _safe_float(sp.get("bid"), 0.0)
                                    ask_px = _safe_float(sp.get("ask"), 0.0)
                                    break
                        except Exception:
                            pass

                        if direction == "short" and ask_px > 0:
                            buffer = max(abs(ask_px) * 0.000001, 0.01)
                            target_sl = max(target_sl, ask_px + buffer)
                        elif direction == "long" and bid_px > 0:
                            buffer = max(abs(bid_px) * 0.000001, 0.01)
                            target_sl = min(target_sl, bid_px - buffer)

                        new_tp = live_tp if self._target_valid_for_position(direction, entry, live_tp) else planned_tp
                        if not self._target_valid_for_position(direction, entry, new_tp):
                            repair_rr = self._repair_rr_for_source(source, planned_rr)
                            new_tp = (
                                entry + (planned_risk * repair_rr)
                                if direction == "long"
                                else entry - (planned_risk * repair_rr)
                            )
                        take_profit_final = (
                            new_tp if self._target_valid_for_position(direction, entry, new_tp) else 0.0
                        )

                        res = self.amend_position_sltp(
                            position_id=position_id,
                            stop_loss=target_sl,
                            take_profit=take_profit_final,
                            trailing_stop_loss=False,
                        )
                        if bool(res.ok):
                            report["amended_positions"] += 1
                            report["pm_actions"].append({
                                "position_id": position_id,
                                "source": source,
                                "symbol": symbol,
                                "action": "repair_missing_sl_no_ref",
                                "reference_price": 0.0,
                                "new_stop_loss": round(target_sl, 4),
                                "new_take_profit": round(take_profit_final, 4),
                            })
                continue
            report["managed_positions"] += 1
            planned_tp = _safe_float(journal_row["take_profit"], 0.0) if journal_row is not None else 0.0
            planned_sl = _safe_float(journal_row["stop_loss"], 0.0) if journal_row is not None else 0.0
            planned_rr = self._planned_rr(journal_row)
            planned_risk = self._planned_risk(journal_row, entry_price=entry, stop_loss=planned_sl or stop_loss)
            r_now = self._r_multiple(direction, entry, stop_loss, ref)
            risk = abs(entry - stop_loss) if self._stop_valid_for_position(direction, entry, stop_loss) else planned_risk
            
            age_min = self._position_age_min(pos)
            # TP for amend paths before `target_tp` is computed later (planned vs live).
            _trail_take_profit = (
                live_tp
                if self._target_valid_for_position(direction, entry, live_tp)
                else (planned_tp if self._target_valid_for_position(direction, entry, planned_tp) else 0.0)
            )
            # --- NEURAL TRAILING BRAIN HOOK (Bridge Mode) ---
            if (r_now is not None) and risk > 0 and direction in {"long", "short"}:
                try:
                    from learning.position_trailing_brain import trailing_brain
                    _sf = getattr(self, "_session_flags", None)
                    _cls_func = getattr(self, "_classify_symbol", lambda x: "other")
                    now_ts = datetime.now(timezone.utc)
                    curr_session_overlap = float(_sf(now_ts.hour).get("session_overlap", 0.0)) if _sf else 0.0
                    
                    snapshot = self._latest_capture_snapshot(symbol=symbol, direction=direction, confidence=0.0)
                    feat = snapshot.get("features", {}) if snapshot else {}
                    
                    state = {
                        "position_id": position_id,
                        "symbol": symbol,
                        "family": _cls_func(symbol),
                        "source_lane": source,
                        "r_now": float(r_now),
                        "time_in_trade_minutes": float(age_min),
                        "vwap_slope_100t": float(feat.get("mid_drift_pct", 0.0)),
                        "tick_velocity": float(feat.get("bar_volume_proxy", 0.0)),
                        "depth_imbalance": float(feat.get("depth_imbalance", 0.0)),
                        "vol_regime_ratio": 1.0,
                        "session_overlap_flag": curr_session_overlap,
                        "active_sl": stop_loss,
                    }
                    decision = trailing_brain.get_trailing_decision(state)
                    
                    if decision and decision.should_move and decision.trail_lock_r > 0:
                        brain_sl = entry + (risk * decision.trail_lock_r) if direction == "long" else entry - (risk * decision.trail_lock_r)
                        brain_improves = (brain_sl > stop_loss) if direction == "long" else (brain_sl < stop_loss)
                        brain_tol = max(abs(entry) * 0.000001, 0.01)
                        if brain_improves and abs(brain_sl - stop_loss) > brain_tol and self._stop_valid_for_position(direction, entry, brain_sl):
                            res = self.amend_position_sltp(
                                position_id=position_id,
                                stop_loss=brain_sl,
                                take_profit=_trail_take_profit,
                                trailing_stop_loss=False,
                            )
                            if bool(res.ok):
                                logger.info(
                                    f"[TRAIL LIVE] symbol={symbol} pos={position_id} | r_now={r_now:.2f} -> proposed_lock={decision.trail_lock_r:.2f} | actual_sl_moved=SUCCESS"
                                )
                                report["amended_positions"] += 1
                                report["pm_actions"].append({
                                    "position_id": position_id,
                                    "source": source,
                                    "symbol": symbol,
                                    "action": f"trailing_brain_{decision.mode}",
                                    "r_now": round(float(r_now), 4),
                                    "trail_lock_r": round(decision.trail_lock_r, 4),
                                    "new_stop_loss": round(brain_sl, 4),
                                    "decision_id": decision.decision_id,
                                })
                                logger.info(
                                    f"[TRAIL DECISION] SL_MOVED | symbol={symbol} | "
                                    f"r_now={float(r_now):.2f} | lock_r={decision.trail_lock_r:.2f} | "
                                    f"old_sl={stop_loss:.4f} | new_sl={brain_sl:.4f}"
                                )
                except Exception as e:
                    logger.error(f"[CTraderExecutor] Trailing brain evaluation crashed: {e}", exc_info=True)
            # ----------------------------------

            # ── Fibo time-based profit lock (runs before active defense) ────
            if "fibo" in source and bool(getattr(config, "FIBO_PM_TIME_LOCK_ENABLED", True)) and r_now is not None and r_now > 0:
                _fibo_be_min = float(getattr(config, "FIBO_PM_BE_AFTER_MIN", 20))
                _fibo_lock_tiers = [
                    (float(getattr(config, "FIBO_PM_LOCK_30_AFTER_MIN", 45)), 0.30),
                    (float(getattr(config, "FIBO_PM_LOCK_50_AFTER_MIN", 90)), 0.50),
                    (float(getattr(config, "FIBO_PM_LOCK_70_AFTER_MIN", 150)), 0.70),
                ]
                _lock_pct = 0.0
                _fibo_tighten = False
                for _tier_min, _tier_pct in reversed(_fibo_lock_tiers):
                    if age_min >= _tier_min:
                        _lock_pct = _tier_pct
                        _fibo_tighten = True
                        break
                if not _fibo_tighten and age_min >= _fibo_be_min:
                    _fibo_tighten = True
                _profit_pts = (ref - entry) if direction == "long" else (entry - ref)
                if _fibo_tighten and _profit_pts > 0:
                    _be_buffer = max(abs(entry) * 0.00005, 0.5)
                    # Tighten SL towards entry from original wide SL:
                    # lock_pct of the distance from original SL to entry is recovered.
                    # E.g. short: SL=4807, entry=4706 → risk=101pts.
                    # lock 50% → new_sl = entry + risk*(1-0.50) = entry + 50.5 = 4756.5
                    _orig_risk = abs(stop_loss - entry)
                    if _lock_pct > 0:
                        _keep_risk = _orig_risk * (1.0 - _lock_pct)
                    else:
                        _keep_risk = _be_buffer  # just entry ± buffer for breakeven
                    if direction == "long":
                        _new_sl = entry - _keep_risk
                    else:
                        _new_sl = entry + _keep_risk
                    _improves = (_new_sl > stop_loss) if direction == "long" else (_new_sl < stop_loss)
                    _tol = max(abs(entry) * 0.000001, 0.01)
                    if _improves and abs(_new_sl - stop_loss) > _tol and self._stop_valid_for_position(direction, entry, _new_sl):
                        _keep_tp = live_tp if self._target_valid_for_position(direction, entry, live_tp) else 0.0
                        res = self.amend_position_sltp(
                            position_id=position_id, stop_loss=_new_sl,
                            take_profit=_keep_tp, trailing_stop_loss=False,
                        )
                        if bool(res.ok):
                            report["amended_positions"] += 1
                            _tier_label = "be" if _lock_pct <= 0 else ("lock_%d" % int(_lock_pct * 100))
                            report["pm_actions"].append({
                                "position_id": position_id, "source": source, "symbol": symbol,
                                "action": "fibo_time_profit_lock_%s" % _tier_label,
                                "reference_price": round(ref, 4), "new_stop_loss": round(_new_sl, 4),
                                "r_now": round(float(r_now), 4), "age_min": round(float(age_min), 1),
                                "lock_pct": _lock_pct, "profit_pts": round(_profit_pts, 2),
                            })
                            logger.info(
                                "[PM:FiboTimeLock] pos=%s %s %s | age=%.0fm | profit=%.1fpts | lock=%d%% | new_sl=%.2f",
                                position_id, symbol, direction, age_min, _profit_pts, int(_lock_pct * 100), _new_sl,
                            )
                        continue

            action_reason = ""
            live_sl_valid = self._stop_valid_for_position(direction, entry, stop_loss)
            if repair_missing_sl and not live_sl_valid:
                target_sl = planned_sl
                if not self._stop_valid_for_position(direction, entry, target_sl):
                    if planned_risk > 0:
                        target_sl = entry - planned_risk if direction == "long" else entry + planned_risk
                if self._stop_valid_for_position(direction, entry, target_sl):
                    breached = (direction == "long" and ref <= target_sl) or (direction == "short" and ref >= target_sl)
                    if breached:
                        res = self.close_position(position_id=position_id, volume=volume)
                        if bool(res.ok):
                            report["pm_actions"].append({
                                "position_id": position_id,
                                "source": source,
                                "symbol": symbol,
                                "action": "close_missing_sl_breached",
                                "reference_price": round(ref, 4),
                                "repair_stop_loss": round(target_sl, 4),
                            })
                        continue
                    new_tp = live_tp if self._target_valid_for_position(direction, entry, live_tp) else planned_tp
                    if not self._target_valid_for_position(direction, entry, new_tp):
                        repair_rr = self._repair_rr_for_source(source, planned_rr)
                        new_tp = entry + (planned_risk * repair_rr) if direction == "long" else entry - (planned_risk * repair_rr)
                    res = self.amend_position_sltp(
                        position_id=position_id,
                        stop_loss=target_sl,
                        take_profit=new_tp if self._target_valid_for_position(direction, entry, new_tp) else 0.0,
                        trailing_stop_loss=False,
                    )
                    if bool(res.ok):
                        report["amended_positions"] += 1
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": "repair_missing_sl",
                            "reference_price": round(ref, 4),
                            "new_stop_loss": round(target_sl, 4),
                            "new_take_profit": round(new_tp, 4) if self._target_valid_for_position(direction, entry, new_tp) else 0.0,
                        })
                    continue
            stop_clamp = self._xau_post_fill_stop_clamp_plan(
                symbol=symbol,
                direction=direction,
                planned_entry=_safe_float(journal_row["entry"], entry) if journal_row is not None else entry,
                planned_stop_loss=planned_sl or stop_loss,
                live_entry=entry,
                live_stop_loss=stop_loss,
                current_price=ref,
            )
            if bool(stop_clamp.get("active")):
                if str(stop_clamp.get("action") or "").strip().lower() == "close":
                    res = self.close_position(position_id=position_id, volume=volume)
                    if bool(res.ok):
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": "xau_post_fill_stop_clamp_close",
                            "reference_price": round(ref, 4),
                            "details": dict(stop_clamp.get("details") or {}),
                        })
                    continue
                new_sl = _safe_float(stop_clamp.get("new_stop_loss"), 0.0)
                if self._stop_valid_for_position(direction, entry, new_sl) and abs(new_sl - stop_loss) > max(abs(entry) * 0.000001, 0.01):
                    res = self.amend_position_sltp(
                        position_id=position_id,
                        stop_loss=new_sl,
                        take_profit=live_tp if self._target_valid_for_position(direction, entry, live_tp) else 0.0,
                        trailing_stop_loss=False,
                    )
                    if bool(res.ok):
                        report["amended_positions"] += 1
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": "xau_post_fill_stop_clamp",
                            "reference_price": round(ref, 4),
                            "new_stop_loss": round(new_sl, 4),
                            "take_profit": round(live_tp, 4) if self._target_valid_for_position(direction, entry, live_tp) else 0.0,
                            "details": dict(stop_clamp.get("details") or {}),
                        })
                        continue
            confidence = _safe_float(journal_row["confidence"], 0.0) if journal_row is not None else 0.0
            age_min = self._position_age_min(pos)
            order_care_state = self._xau_order_care_state(symbol=symbol, source=source)
            order_care_overrides = dict(order_care_state.get("overrides") or {})

            # ── Force-close directive: close all positions in target direction ──
            force_close_dir = str(order_care_state.get("force_close_direction") or "").strip().lower()
            force_close_reason = str(order_care_state.get("force_close_reason") or "order_care_force_close")
            if force_close_dir and direction == force_close_dir:
                res = self.close_position(position_id=position_id, volume=0)
                action_entry = {
                    "journal_id": (journal_id or None),
                    "position_id": position_id,
                    "source": source,
                    "symbol": symbol,
                    "action": "order_care_force_close",
                    "direction": direction,
                    "reason": force_close_reason,
                    "reference_price": round(ref, 4),
                    "entry_price": round(entry, 4),
                    "ok": bool(res.ok),
                }
                logger.info("[PM] force_close_direction=%s pos=%s %s@%s reason=%s ok=%s", force_close_dir, position_id, symbol, round(ref, 4), force_close_reason, res.ok)
                report["pm_actions"].append(action_entry)
                if bool(res.ok):
                    report["closed_profit_positions"] += 1
                continue

            # ── Momentum exhaustion profit lock (before extension) ────────
            # If momentum is dead and trade is profitable, lock profit NOW
            # instead of waiting for TP or letting it evaporate
            if "fibo" in source and bool(getattr(config, "FIBO_PM_EXHAUSTION_LOCK_ENABLED", True)) and r_now is not None and r_now > 0.15:
                try:
                    exhaustion = self._xau_momentum_exhaustion_lock(
                        source=source, symbol=symbol, direction=direction,
                        entry=entry, stop_loss=stop_loss, current_price=ref,
                        confidence=confidence, age_min=age_min, r_now=r_now,
                    )
                    if bool(exhaustion.get("active")):
                        _exh_new_sl = _safe_float(exhaustion.get("new_stop_loss"), 0.0)
                        if self._stop_valid_for_position(direction, entry, _exh_new_sl):
                            _exh_improves = (_exh_new_sl > stop_loss) if direction == "long" else (_exh_new_sl < stop_loss)
                            if _exh_improves:
                                _exh_keep_tp = live_tp if self._target_valid_for_position(direction, entry, live_tp) else 0.0
                                _exh_res = self.amend_position_sltp(
                                    position_id=position_id, stop_loss=_exh_new_sl,
                                    take_profit=_exh_keep_tp, trailing_stop_loss=False,
                                )
                                if bool(_exh_res.ok):
                                    report["amended_positions"] += 1
                                    report["pm_actions"].append({
                                        "journal_id": (journal_id or None),
                                        "position_id": position_id,
                                        "source": source,
                                        "symbol": symbol,
                                        "action": "xau_momentum_exhaustion_lock",
                                        "reference_price": round(ref, 4),
                                        "new_stop_loss": round(_exh_new_sl, 4),
                                        "r_now": round(float(r_now), 4),
                                        "details": dict(exhaustion.get("details") or {}),
                                    })
                                    logger.info(
                                        "[PM:ExhaustionLock] pos=%s %s %s | r_now=%.2f | new_sl=%.2f",
                                        position_id, symbol, direction, r_now, _exh_new_sl,
                                    )
                                    continue
                except Exception as _exh_exc:
                    logger.debug("[PM] momentum_exhaustion_lock error for %s: %s", symbol, _exh_exc)

            planned_tp_valid = self._target_valid_for_position(direction, entry, planned_tp)
            live_tp_valid = self._target_valid_for_position(direction, entry, live_tp)
            live_target_more_favorable = planned_tp_valid and live_tp_valid and self._target_more_favorable(direction, entry, live_tp, planned_tp)
            target_tp = live_tp if live_tp_valid else planned_tp
            if (
                close_on_planned_target
                and planned_tp > 0
                and self._price_crossed_target(direction, ref, planned_tp)
                and (not live_target_more_favorable)
                and ((not live_tp_valid) or planned_tp_valid)
            ):
                extension = self._xau_profit_extension_plan(
                    source=source,
                    symbol=symbol,
                    direction=direction,
                    entry=entry,
                    stop_loss=stop_loss,
                    planned_tp=planned_tp,
                    current_tp=target_tp,
                    current_price=ref,
                    confidence=confidence,
                    age_min=age_min,
                    r_now=r_now,
                )
                # Fallback: crypto DOM TP extension for BTC/ETH
                if not bool(extension.get("active")) and not self._is_xau_symbol(symbol) and symbol.upper() in {"BTCUSD", "ETHUSD"}:
                    try:
                        extension = self._crypto_dom_tp_extension_plan(symbol=symbol, direction=direction, entry=entry, stop_loss=stop_loss, planned_tp=planned_tp, current_tp=target_tp, current_price=ref, r_now=r_now, age_min=age_min)
                    except Exception:
                        logger.debug("[PM] crypto_dom_tp_extension error for %s", symbol, exc_info=True)
                if bool(extension.get("active")):
                    new_sl = _safe_float(extension.get("new_stop_loss"), 0.0)
                    new_tp = _safe_float(extension.get("new_take_profit"), 0.0)
                    res = self.amend_position_sltp(
                        position_id=position_id,
                        stop_loss=new_sl if self._stop_valid_for_position(direction, entry, new_sl) else stop_loss,
                        take_profit=new_tp if self._target_valid_for_position(direction, entry, new_tp) else target_tp,
                        trailing_stop_loss=False,
                    )
                    if bool(res.ok):
                        report["amended_positions"] += 1
                        report["pm_actions"].append({
                            "journal_id": (journal_id or None),
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": str(extension.get("reason") or "xau_profit_extension"),
                            "reference_price": round(ref, 4),
                            "new_stop_loss": round(new_sl, 4) if self._stop_valid_for_position(direction, entry, new_sl) else round(stop_loss, 4),
                            "new_take_profit": round(new_tp, 4) if self._target_valid_for_position(direction, entry, new_tp) else round(target_tp, 4),
                            "r_now": (None if r_now is None else round(float(r_now), 4)),
                            "age_min": round(float(age_min), 4),
                            "details": {
                                **dict(extension.get("details") or {}),
                                "trigger": "planned_target",
                            },
                        })
                        continue
                res = self.close_position(position_id=position_id, volume=volume)
                if bool(res.ok):
                    report["closed_profit_positions"] += 1
                    report["pm_actions"].append({
                        "position_id": position_id,
                        "source": source,
                        "symbol": symbol,
                        "action": "close_at_planned_target",
                        "reference_price": round(ref, 4),
                        "planned_tp": round(planned_tp, 4),
                    })
                continue
            if live_sl_valid and (not live_tp_valid):
                target_tp = planned_tp
                if not self._target_valid_for_position(direction, entry, target_tp) and planned_risk > 0:
                    repair_rr = self._repair_rr_for_source(source, planned_rr)
                    target_tp = entry + (planned_risk * repair_rr) if direction == "long" else entry - (planned_risk * repair_rr)
                if self._target_valid_for_position(direction, entry, target_tp):
                    res = self.amend_position_sltp(
                        position_id=position_id,
                        stop_loss=stop_loss,
                        take_profit=target_tp,
                        trailing_stop_loss=False,
                    )
                    if bool(res.ok):
                        report["amended_positions"] += 1
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": "repair_missing_tp",
                            "reference_price": round(ref, 4),
                            "stop_loss": round(stop_loss, 4),
                            "new_take_profit": round(target_tp, 4),
                        })
                    continue
            if live_tp_valid:
                if self._price_crossed_target(direction, ref, live_tp):
                    extension = self._xau_profit_extension_plan(
                        source=source,
                        symbol=symbol,
                        direction=direction,
                        entry=entry,
                        stop_loss=stop_loss,
                        planned_tp=planned_tp,
                        current_tp=live_tp,
                        current_price=ref,
                        confidence=confidence,
                        age_min=age_min,
                        r_now=r_now,
                    )
                    if bool(extension.get("active")):
                        new_sl = _safe_float(extension.get("new_stop_loss"), 0.0)
                        new_tp = _safe_float(extension.get("new_take_profit"), 0.0)
                        res = self.amend_position_sltp(
                            position_id=position_id,
                            stop_loss=new_sl if self._stop_valid_for_position(direction, entry, new_sl) else stop_loss,
                            take_profit=new_tp if self._target_valid_for_position(direction, entry, new_tp) else live_tp,
                            trailing_stop_loss=False,
                        )
                        if bool(res.ok):
                            report["amended_positions"] += 1
                            report["pm_actions"].append({
                                "journal_id": (journal_id or None),
                                "position_id": position_id,
                                "source": source,
                                "symbol": symbol,
                                "action": str(extension.get("reason") or "xau_profit_extension"),
                                "reference_price": round(ref, 4),
                                "new_stop_loss": round(new_sl, 4) if self._stop_valid_for_position(direction, entry, new_sl) else round(stop_loss, 4),
                                "new_take_profit": round(new_tp, 4) if self._target_valid_for_position(direction, entry, new_tp) else round(live_tp, 4),
                                "r_now": (None if r_now is None else round(float(r_now), 4)),
                                "age_min": round(float(age_min), 4),
                                "details": {
                                    **dict(extension.get("details") or {}),
                                    "trigger": "live_target",
                                },
                            })
                            continue
                    res = self.close_position(position_id=position_id, volume=volume)
                    if bool(res.ok):
                        report["closed_profit_positions"] += 1
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": "close_at_live_target",
                            "reference_price": round(ref, 4),
                            "live_tp": round(live_tp, 4),
                        })
                    continue
            active_defense = self._xau_active_defense_plan(
                source=source,
                symbol=symbol,
                direction=direction,
                entry=entry,
                stop_loss=stop_loss,
                target_tp=target_tp,
                current_price=ref,
                confidence=confidence,
                age_min=age_min,
                r_now=r_now,
            )
            if bool(active_defense.get("active")):
                action = str(active_defense.get("action") or "").strip().lower()
                details = dict(active_defense.get("details") or {})
                if action == "close":
                    res = self.close_position(position_id=position_id, volume=volume)
                    if bool(res.ok):
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": str(active_defense.get("reason") or "xau_active_defense_close"),
                            "reference_price": round(ref, 4),
                            "r_now": (None if r_now is None else round(float(r_now), 4)),
                            "details": details,
                        })
                    continue
                if action == "tighten":
                    new_sl = _safe_float(active_defense.get("new_stop_loss"), 0.0)
                    new_tp = _safe_float(active_defense.get("new_take_profit"), 0.0)
                    stop_tol = max(abs(entry) * 0.000001, 0.01)
                    move_sl = abs(new_sl - stop_loss)
                    move_tp = abs(new_tp - target_tp) if self._target_valid_for_position(direction, entry, new_tp) and self._target_valid_for_position(direction, entry, target_tp) else 0.0
                    if self._stop_valid_for_position(direction, entry, new_sl) and (move_sl > stop_tol or move_tp > stop_tol):
                        res = self.amend_position_sltp(
                            position_id=position_id,
                            stop_loss=new_sl,
                            take_profit=new_tp if self._target_valid_for_position(direction, entry, new_tp) else target_tp,
                            trailing_stop_loss=False,
                        )
                        if bool(res.ok):
                            report["amended_positions"] += 1
                            report["pm_actions"].append({
                                "position_id": position_id,
                                "source": source,
                                "symbol": symbol,
                                "action": str(active_defense.get("reason") or "xau_active_defense_tighten"),
                                "reference_price": round(ref, 4),
                                "new_stop_loss": round(new_sl, 4),
                                "new_take_profit": round(new_tp, 4) if self._target_valid_for_position(direction, entry, new_tp) else round(target_tp, 4),
                                "r_now": (None if r_now is None else round(float(r_now), 4)),
                                "details": details,
                        })
                    continue
            # DOM-only defense for non-XAU symbols (BTC/ETH)
            if not self._is_xau_symbol(symbol) and symbol.upper() in {"BTCUSD", "ETHUSD"}:
                try:
                    crypto_dom = self._crypto_dom_defense_plan(symbol=symbol, direction=direction, entry=entry, stop_loss=stop_loss, current_price=ref, r_now=r_now, age_min=age_min)
                    if bool(crypto_dom.get("active")):
                        c_action = str(crypto_dom.get("action") or "").strip().lower()
                        c_details = dict(crypto_dom.get("details") or {})
                        if c_action == "close":
                            res = self.close_position(position_id=position_id, volume=volume)
                            if bool(res.ok):
                                report["pm_actions"].append({"position_id": position_id, "source": source, "symbol": symbol, "action": str(crypto_dom.get("reason") or "crypto_dom_defense_close"), "reference_price": round(ref, 4), "r_now": (None if r_now is None else round(float(r_now), 4)), "details": c_details})
                            continue
                        if c_action == "tighten":
                            c_new_sl = _safe_float(crypto_dom.get("new_stop_loss"), 0.0)
                            c_stop_tol = max(abs(entry) * 0.000001, 0.01)
                            if self._stop_valid_for_position(direction, entry, c_new_sl) and abs(c_new_sl - stop_loss) > c_stop_tol:
                                res = self.amend_position_sltp(position_id=position_id, stop_loss=c_new_sl, take_profit=target_tp, trailing_stop_loss=False)
                                if bool(res.ok):
                                    report["amended_positions"] += 1
                                    report["pm_actions"].append({"position_id": position_id, "source": source, "symbol": symbol, "action": str(crypto_dom.get("reason") or "crypto_dom_defense_tighten"), "reference_price": round(ref, 4), "new_stop_loss": round(c_new_sl, 4), "r_now": (None if r_now is None else round(float(r_now), 4)), "details": c_details})
                            continue
                except Exception:
                    logger.debug("[PM] crypto_dom_defense error for %s", symbol, exc_info=True)
            retrace_guard = self._profit_retrace_guard_plan(
                source=source,
                symbol=symbol,
                direction=direction,
                position_id=position_id,
                entry=entry,
                stop_loss=stop_loss,
                current_price=ref,
                confidence=confidence,
                age_min=age_min,
                r_now=r_now,
            )
            if bool(retrace_guard.get("active")):
                guard_action = str(retrace_guard.get("action") or "").strip().lower()
                guard_reason = str(retrace_guard.get("reason") or "profit_retrace_guard")
                guard_details = dict(retrace_guard.get("details") or {})
                if guard_action == "close":
                    res = self.close_position(position_id=position_id, volume=volume)
                    if bool(res.ok):
                        report["closed_profit_positions"] += 1
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": guard_reason,
                            "reference_price": round(ref, 4),
                            "r_now": (None if r_now is None else round(float(r_now), 4)),
                            "details": guard_details,
                        })
                    continue
                if guard_action == "tighten":
                    guard_sl = _safe_float(retrace_guard.get("new_stop_loss"), 0.0)
                    guard_tp = live_tp if self._target_valid_for_position(direction, entry, live_tp) else target_tp
                    stop_tol = max(abs(entry) * 0.000001, 0.01)
                    if self._stop_valid_for_management(direction, ref, guard_sl) and abs(guard_sl - stop_loss) > stop_tol:
                        res = self.amend_position_sltp(
                            position_id=position_id,
                            stop_loss=guard_sl,
                            take_profit=guard_tp if self._target_valid_for_position(direction, entry, guard_tp) else 0.0,
                            trailing_stop_loss=False,
                        )
                        if bool(res.ok):
                            report["amended_positions"] += 1
                            report["pm_actions"].append({
                                "position_id": position_id,
                                "source": source,
                                "symbol": symbol,
                                "action": guard_reason,
                                "reference_price": round(ref, 4),
                                "new_stop_loss": round(guard_sl, 4),
                                "new_take_profit": round(guard_tp, 4) if self._target_valid_for_position(direction, entry, guard_tp) else 0.0,
                                "r_now": (None if r_now is None else round(float(r_now), 4)),
                                "details": guard_details,
                            })
                        continue
            if not self._is_scheduled_canary_source(source):
                if order_care_state and self._target_valid_for_position(direction, entry, target_tp):
                    no_follow_age = float(order_care_overrides.get("no_follow_age_min", 0.0) or 0.0)
                    no_follow_max_r = float(order_care_overrides.get("no_follow_max_r", 0.0) or 0.0)
                    be_trigger_r = float(order_care_overrides.get("be_trigger_r", 0.0) or 0.0)
                    be_lock_r = float(order_care_overrides.get("be_lock_r", 0.0) or 0.0)
                    trim_tp_r = float(order_care_overrides.get("trim_tp_r", 0.0) or 0.0)
                    stop_tol = max(abs(entry) * 0.000001, 0.01)
                    profit_seek_active_pm = (
                        bool(getattr(config, "CTRADER_PM_XAU_PROFIT_SEEKING_ENABLED", True))
                        and self._is_xau_symbol(symbol)
                        and (r_now is not None)
                        and float(r_now) >= max(0.0, float(getattr(config, "CTRADER_PM_XAU_PROFIT_SEEKING_MIN_R", 0.15) or 0.15))
                    )
                    if (r_now is not None) and age_min >= no_follow_age and float(r_now) <= no_follow_max_r:
                        res = self.close_position(position_id=position_id, volume=volume)
                        if bool(res.ok):
                            report["closed_profit_positions"] += 1
                            report["pm_actions"].append({
                                "position_id": position_id,
                                "source": source,
                                "symbol": symbol,
                                "action": "xau_order_care_no_follow_close",
                                "reference_price": round(ref, 4),
                                "r_now": round(float(r_now), 4),
                                "age_min": round(float(age_min), 2),
                                "mode": str(order_care_state.get("mode") or ""),
                            })
                        continue
                    if (r_now is not None) and float(r_now) >= be_trigger_r:
                        r_current = float(r_now)
                        trail_lock_r = float(be_lock_r)
                        if r_current >= 1.0:
                            trail_lock_r = max(trail_lock_r, 0.50)
                        if r_current >= 2.0:
                            trail_lock_r = max(trail_lock_r, 1.00)
                        if r_current >= 3.0:
                            trail_lock_r = max(trail_lock_r, 2.00)
                        be_sl = entry + (risk * trail_lock_r) if direction == "long" else entry - (risk * trail_lock_r)
                        improves = (be_sl > stop_loss) if direction == "long" else (be_sl < stop_loss)
                        trimmed_tp = target_tp
                        if trim_tp_r > 0 and not profit_seek_active_pm:
                            candidate_tp = entry + (risk * trim_tp_r) if direction == "long" else entry - (risk * trim_tp_r)
                            if self._target_valid_for_position(direction, entry, candidate_tp) and abs(candidate_tp - entry) < abs(target_tp - entry):
                                trimmed_tp = candidate_tp
                        if improves and abs(be_sl - stop_loss) > stop_tol:
                            res = self.amend_position_sltp(
                                position_id=position_id,
                                stop_loss=be_sl,
                                take_profit=trimmed_tp,
                                trailing_stop_loss=False,
                            )
                            if bool(res.ok):
                                report["amended_positions"] += 1
                                report["pm_actions"].append({
                                    "position_id": position_id,
                                    "source": source,
                                    "symbol": symbol,
                                    "action": "xau_order_care_breakeven",
                                    "reference_price": round(ref, 4),
                                    "new_stop_loss": round(be_sl, 4),
                                    "take_profit": round(trimmed_tp, 4),
                                    "r_now": round(float(r_now), 4),
                                    "mode": str(order_care_state.get("mode") or ""),
                                })
                            continue
                    if ":canary" in source and risk > 0 and self._target_valid_for_position(direction, entry, target_tp):
                        canary_be_trigger_r = float(getattr(config, "CTRADER_PM_CANARY_FAMILY_BE_TRIGGER_R", 0.80) or 0.80)
                        canary_be_lock_r = float(getattr(config, "CTRADER_PM_CANARY_FAMILY_BE_LOCK_R", 0.05) or 0.05)
                        stop_tol_c = max(abs(entry) * 0.000001, 0.01)
                        if (r_now is not None) and float(r_now) >= canary_be_trigger_r:
                            r_current = float(r_now)
                            trail_lock_r = float(canary_be_lock_r)
                            if r_current >= 1.0:
                                trail_lock_r = max(trail_lock_r, 0.50)
                            if r_current >= 2.0:
                                trail_lock_r = max(trail_lock_r, 1.00)
                            if r_current >= 3.0:
                                trail_lock_r = max(trail_lock_r, 2.00)
                            be_sl = entry + (risk * trail_lock_r) if direction == "long" else entry - (risk * trail_lock_r)
                            improves = (be_sl > stop_loss) if direction == "long" else (be_sl < stop_loss)
                            if improves and abs(be_sl - stop_loss) > stop_tol_c:
                                res = self.amend_position_sltp(
                                    position_id=position_id,
                                    stop_loss=be_sl,
                                    take_profit=target_tp,
                                    trailing_stop_loss=False,
                                )
                                if bool(res.ok):
                                    report["amended_positions"] += 1
                                    report["pm_actions"].append({
                                        "position_id": position_id,
                                        "source": source,
                                        "symbol": symbol,
                                        "action": "canary_family_breakeven",
                                        "reference_price": round(ref, 4),
                                        "new_stop_loss": round(be_sl, 4),
                                        "take_profit": round(target_tp, 4),
                                        "r_now": round(float(r_now), 4),
                                    })
                                continue
                    continue
            if self._is_scheduled_canary_source(source) and self._target_valid_for_position(direction, entry, target_tp):
                no_follow_age = max(1, int(getattr(config, "CTRADER_PM_SCHEDULED_CANARY_NO_FOLLOW_MAX_AGE_MIN", 18) or 18))
                no_follow_max_r = float(getattr(config, "CTRADER_PM_SCHEDULED_CANARY_NO_FOLLOW_MAX_R", 0.08) or 0.08)
                be_trigger_r = float(getattr(config, "CTRADER_PM_SCHEDULED_CANARY_BE_TRIGGER_R", 0.22) or 0.22)
                be_lock_r = float(getattr(config, "CTRADER_PM_SCHEDULED_CANARY_BE_LOCK_R", 0.03) or 0.03)
                stop_tol = max(abs(entry) * 0.000001, 0.01)
                if (r_now is not None) and age_min >= float(no_follow_age) and float(r_now) <= float(no_follow_max_r):
                    res = self.close_position(position_id=position_id, volume=volume)
                    if bool(res.ok):
                        report["closed_profit_positions"] += 1
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": "scheduled_canary_no_follow_close",
                            "reference_price": round(ref, 4),
                            "r_now": round(float(r_now), 4),
                            "age_min": round(float(age_min), 2),
                        })
                    continue
                if (r_now is not None) and float(r_now) >= float(be_trigger_r):
                    r_current = float(r_now)
                    trail_lock_r = float(be_lock_r)
                    if r_current >= 1.0:
                        trail_lock_r = max(trail_lock_r, 0.50)
                    if r_current >= 1.5:
                        trail_lock_r = max(trail_lock_r, 1.00)
                    if r_current >= 2.0:
                        trail_lock_r = max(trail_lock_r, 1.50)
                    be_sl = entry + (risk * trail_lock_r) if direction == "long" else entry - (risk * trail_lock_r)
                    move_sl = abs(be_sl - stop_loss)
                    improves = (be_sl > stop_loss) if direction == "long" else (be_sl < stop_loss)
                    if improves and move_sl > stop_tol:
                        res = self.amend_position_sltp(
                            position_id=position_id,
                            stop_loss=be_sl,
                            take_profit=target_tp,
                            trailing_stop_loss=False,
                        )
                        if bool(res.ok):
                            report["amended_positions"] += 1
                            report["pm_actions"].append({
                                "position_id": position_id,
                                "source": source,
                                "symbol": symbol,
                                "action": "scheduled_canary_breakeven",
                                "reference_price": round(ref, 4),
                                "new_stop_loss": round(be_sl, 4),
                                "take_profit": round(target_tp, 4),
                                "r_now": round(float(r_now), 4),
                            })
                        continue
                rebalanced_sl = self._scheduled_canary_rebalanced_stop(
                    source=source,
                    direction=direction,
                    entry_price=entry,
                    stop_loss=stop_loss,
                    take_profit=target_tp,
                )
                move_sl = abs(rebalanced_sl - stop_loss)
                if move_sl > stop_tol:
                    breached = (direction == "long" and ref <= rebalanced_sl) or (direction == "short" and ref >= rebalanced_sl)
                    if breached:
                        res = self.close_position(position_id=position_id, volume=volume)
                        if bool(res.ok):
                            report["pm_actions"].append({
                                "position_id": position_id,
                                "source": source,
                                "symbol": symbol,
                                "action": "scheduled_canary_rr_breached_close",
                                "reference_price": round(ref, 4),
                                "rebalanced_stop_loss": round(rebalanced_sl, 4),
                            })
                        continue
                    res = self.amend_position_sltp(
                        position_id=position_id,
                        stop_loss=rebalanced_sl,
                        take_profit=target_tp,
                        trailing_stop_loss=False,
                    )
                    if bool(res.ok):
                        report["amended_positions"] += 1
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": "scheduled_canary_rr_rebalance",
                            "reference_price": round(ref, 4),
                            "new_stop_loss": round(rebalanced_sl, 4),
                            "take_profit": round(target_tp, 4),
                        })
                    continue
            if not live_tp_valid:
                if (r_now is not None) and (r_now >= invalid_tp_close_r):
                    res = self.close_position(position_id=position_id, volume=volume)
                    if bool(res.ok):
                        report["closed_profit_positions"] += 1
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": "close_invalid_tp_profit",
                            "reference_price": round(ref, 4),
                            "r_now": round(float(r_now), 4),
                        })
                    continue
                repair_rr = self._repair_rr_for_source(source, planned_rr)
                new_tp = entry + (risk * repair_rr) if direction == "long" else entry - (risk * repair_rr)
                new_sl = stop_loss
                if (r_now is not None) and (r_now >= invalid_tp_be_trigger_r):
                    be_sl = entry + (risk * invalid_tp_be_lock_r) if direction == "long" else entry - (risk * invalid_tp_be_lock_r)
                    if direction == "long":
                        new_sl = max(stop_loss, be_sl)
                    else:
                        new_sl = min(stop_loss, be_sl)
                if not self._target_valid_for_position(direction, entry, new_tp):
                    action_reason = "repair_tp_invalid"
                else:
                    move_tp = abs(new_tp - live_tp)
                    move_sl = abs(new_sl - stop_loss)
                    if move_tp <= max(abs(entry) * 0.000001, 0.01) and move_sl <= max(abs(entry) * 0.000001, 0.01):
                        continue
                    res = self.amend_position_sltp(
                        position_id=position_id,
                        stop_loss=new_sl,
                        take_profit=new_tp,
                        trailing_stop_loss=False,
                    )
                    if bool(res.ok):
                        report["amended_positions"] += 1
                        report["pm_actions"].append({
                            "position_id": position_id,
                            "source": source,
                            "symbol": symbol,
                            "action": "repair_invalid_tp",
                            "reference_price": round(ref, 4),
                            "new_stop_loss": round(new_sl, 4),
                            "new_take_profit": round(new_tp, 4),
                            "repair_rr": round(repair_rr, 4),
                            "r_now": (None if r_now is None else round(float(r_now), 4)),
                        })
                    continue
            if action_reason:
                report["pm_actions"].append({
                    "position_id": position_id,
                    "source": source,
                    "symbol": symbol,
                    "action": "skip",
                    "reason": action_reason,
                })
        return report

    def sync_account_state(self, *, lookback_hours: Optional[int] = None, auto_close_unsafe: bool = True) -> dict:
        account_id, _reason = self._configured_account_id()
        payload = {
            "account_id": account_id,
            "lookback_hours": max(1, int(lookback_hours if lookback_hours is not None else getattr(config, "CTRADER_SYNC_DEALS_LOOKBACK_HOURS", 72) or 72)),
            "max_rows": 500,
        }
        raw = self._run_worker(
            mode="reconcile",
            payload=payload,
            timeout_sec=max(8, int(getattr(config, "CTRADER_HEALTHCHECK_TIMEOUT_SEC", 18) or 18) + 6),
        )
        report = {
            "ok": bool(raw.get("ok", False)),
            "status": str(raw.get("status", "") or ""),
            "message": str(raw.get("message", "") or ""),
            "account_id": account_id,
            "positions": 0,
            "orders": 0,
            "deals": 0,
            "matched_journal": 0,
            "reconciled_journal": 0,
            "closed_unsafe": 0,
            "closed_position_ids": [],
            "canceled_orders": 0,
            "managed_positions": 0,
            "amended_positions": 0,
            "closed_profit_positions": 0,
            "pm_actions": [],
            "pm_audited_actions": 0,
            "order_actions": [],
            "error": "",
        }
        if not bool(raw.get("ok", False)):
            report["error"] = str(raw.get("message", "") or raw.get("status", "") or "sync_failed")
            return report
        positions = list(raw.get("positions") or [])
        orders = list(raw.get("orders") or [])
        deals = list(raw.get("deals") or [])
        report["positions"] = len(positions)
        report["orders"] = len(orders)
        report["deals"] = len(deals)
        now_iso = _utc_now_iso()
        pending_close: list[dict] = []
        tracked_positions: list[dict] = []
        tracked_orders: list[dict] = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            seen_positions: set[int] = set()
            seen_orders: set[int] = set()
            negative_statuses = (
                "dry_run",
                "rejected",
                "error",
                "worker_error",
                "auth_failed",
                "account_auth_failed",
                "timeout",
                "invalid",
                "blocked",
                "skipped",
                "filtered",
            )
            for order in orders:
                normalized_order = self._normalize_order(order)
                order_id = int(normalized_order.get("order_id") or 0)
                if order_id <= 0:
                    continue
                seen_orders.add(order_id)
                source = str(normalized_order.get("source", "") or "").strip().lower()
                symbol = str(normalized_order.get("symbol", "") or "").strip().upper()
                lane = str(normalized_order.get("lane", "") or self._source_lane(source))
                journal_row = self._find_journal_match(
                    conn,
                    order_id=order_id,
                    position_id=int(_safe_float(order.get("positionId"), 0)),
                    source=source,
                    symbol=symbol,
                    run_no=int(normalized_order.get("signal_run_no", 0) or 0),
                    entry_price=_safe_float(normalized_order.get("entry_price"), 0.0),
                )
                journal_id = None if journal_row is None else int(journal_row["id"])
                if journal_row is not None:
                    report["matched_journal"] += 1
                    conn.execute(
                        """
                        UPDATE execution_journal
                           SET order_id=?,
                               broker_symbol=CASE WHEN COALESCE(broker_symbol,'')='' THEN ? ELSE broker_symbol END,
                               volume=CASE WHEN COALESCE(volume,0)=0 THEN ? ELSE volume END,
                               status=CASE WHEN LOWER(COALESCE(status,'')) IN ('accepted','pending','submitted','canceled') THEN 'accepted' ELSE status END,
                               message=CASE WHEN LOWER(COALESCE(status,''))='canceled' THEN 'ctrader reconciled pending order'
                                            WHEN LOWER(COALESCE(message,'')) IN ('ctrader accepted','') THEN 'ctrader reconciled pending order'
                                            ELSE message END
                         WHERE id=?
                        """,
                        (order_id, symbol, _safe_float(normalized_order.get("volume"), 0.0), journal_id),
                    )
                elif source and symbol:
                    req = {
                        "symbol": symbol,
                        "source": source,
                        "label": str(normalized_order.get("label", "") or ""),
                        "comment": str(normalized_order.get("comment", "") or ""),
                        "direction": str(normalized_order.get("direction", "") or ""),
                        "entry": _safe_float(normalized_order.get("entry_price"), 0.0),
                        "stop_loss": _safe_float(normalized_order.get("stop_loss"), 0.0),
                        "take_profit": _safe_float(normalized_order.get("take_profit"), 0.0),
                        "entry_type": str(normalized_order.get("order_type", "") or ""),
                        "signal_run_no": int(normalized_order.get("signal_run_no", 0) or 0),
                        "signal_run_id": str(normalized_order.get("signal_run_id", "") or ""),
                        "reconciled_external": True,
                    }
                    cur = conn.execute(
                        """
                        INSERT INTO execution_journal (
                            created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id,
                            broker_symbol, volume, status, message, order_id, position_id, deal_id,
                            signal_run_id, signal_run_no, request_json, response_json, execution_meta_json
                        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, 0, ?, ?, ?, 'accepted',
                                  'ctrader reconciled pending order', ?, NULL, NULL, ?, ?, ?, '{}', ?)
                        """,
                        (
                            _safe_float(normalized_order.get("created_ts"), time.time()),
                            str(normalized_order.get("created_utc", "") or now_iso),
                            source,
                            lane,
                            symbol,
                            str(normalized_order.get("direction", "") or ""),
                            _safe_float(normalized_order.get("entry_price"), 0.0),
                            _safe_float(normalized_order.get("stop_loss"), 0.0),
                            _safe_float(normalized_order.get("take_profit"), 0.0),
                            str(normalized_order.get("order_type", "") or ""),
                            account_id,
                            symbol,
                            _safe_float(normalized_order.get("volume"), 0.0),
                            order_id,
                            str(normalized_order.get("signal_run_id", "") or ""),
                            int(normalized_order.get("signal_run_no", 0) or 0),
                            json.dumps(req, ensure_ascii=True),
                            json.dumps({"reconcile": True, "kind": "pending_order"}, ensure_ascii=True),
                        ),
                    )
                    journal_id = int(cur.lastrowid)
                    report["reconciled_journal"] += 1
                conn.execute(
                    """
                    INSERT INTO ctrader_orders(
                        order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                        entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                        client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                        last_seen_utc, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                    ON CONFLICT(order_id) DO UPDATE SET
                        account_id=excluded.account_id,
                        source=excluded.source,
                        lane=excluded.lane,
                        symbol=excluded.symbol,
                        broker_symbol=excluded.broker_symbol,
                        direction=excluded.direction,
                        volume=excluded.volume,
                        entry_price=excluded.entry_price,
                        stop_loss=excluded.stop_loss,
                        take_profit=excluded.take_profit,
                        order_type=excluded.order_type,
                        order_status=excluded.order_status,
                        label=excluded.label,
                        comment=excluded.comment,
                        client_order_id=excluded.client_order_id,
                        signal_run_id=excluded.signal_run_id,
                        signal_run_no=excluded.signal_run_no,
                        journal_id=excluded.journal_id,
                        is_open=1,
                        last_seen_utc=excluded.last_seen_utc,
                        raw_json=excluded.raw_json
                    """,
                    (
                        order_id,
                        account_id,
                        source,
                        lane,
                        symbol,
                        symbol,
                        str(normalized_order.get("direction", "") or ""),
                        _safe_float(normalized_order.get("volume"), 0.0),
                        _safe_float(normalized_order.get("entry_price"), 0.0),
                        _safe_float(normalized_order.get("stop_loss"), 0.0),
                        _safe_float(normalized_order.get("take_profit"), 0.0),
                        str(normalized_order.get("order_type", "") or ""),
                        str(normalized_order.get("order_status", "") or ""),
                        str(normalized_order.get("label", "") or ""),
                        str(normalized_order.get("comment", "") or ""),
                        str(normalized_order.get("client_order_id", "") or ""),
                        str(normalized_order.get("signal_run_id", "") or ""),
                        int(normalized_order.get("signal_run_no", 0) or 0),
                        journal_id,
                        str(normalized_order.get("created_utc", "") or now_iso),
                        now_iso,
                        str(normalized_order.get("raw_json", "{}") or "{}"),
                    ),
                )
                tracked_orders.append({
                    **normalized_order,
                    "journal_id": journal_id,
                })
            for pos in positions:
                position_id = int(_safe_float(pos.get("position_id"), 0))
                if position_id <= 0:
                    continue
                seen_positions.add(position_id)
                placeholders = ",".join(["?"] * len(negative_statuses))
                conn.execute(
                    f"""
                    UPDATE execution_journal
                       SET position_id=NULL
                     WHERE position_id=?
                       AND LOWER(COALESCE(status,'')) IN ({placeholders})
                    """,
                    (position_id, *negative_statuses),
                )
                meta = self._parse_label_meta(str(pos.get("label", "") or ""), str(pos.get("comment", "") or ""))
                source = str(meta.get("source") or "").strip().lower()
                symbol = str(pos.get("symbol") or meta.get("symbol") or "").strip().upper()
                lane = str(meta.get("lane") or self._source_lane(source))
                if not source and symbol:
                    logger.warning(
                        "[CTrader:Reconcile] position %s (%s) has NO source tag — "
                        "label=%s comment=%s — marking as untagged_external",
                        position_id, symbol,
                        str(pos.get("label", "") or "")[:60],
                        str(pos.get("comment", "") or "")[:60],
                    )
                    source = "untagged_external"
                    lane = "external"
                untracked_unsafe = False
                journal_row = self._find_journal_match(
                    conn,
                    position_id=position_id,
                    source=source,
                    symbol=symbol,
                    run_no=int(meta.get("run_no", 0) or 0),
                    entry_price=_safe_float(pos.get("entry_price"), 0.0),
                )
                journal_id = None if journal_row is None else int(journal_row["id"])
                if journal_row is not None:
                    report["matched_journal"] += 1
                    conn.execute(
                        """
                        UPDATE execution_journal
                           SET position_id=?,
                               broker_symbol=CASE WHEN COALESCE(broker_symbol,'')='' THEN ? ELSE broker_symbol END,
                               volume=CASE WHEN COALESCE(volume,0)=0 THEN ? ELSE volume END,
                               status=CASE WHEN LOWER(COALESCE(status,'')) IN ('accepted','reconciled_open') THEN 'filled' ELSE status END,
                               message=CASE WHEN LOWER(COALESCE(message,'')) IN ('ctrader accepted','') THEN 'ctrader reconciled open position' ELSE message END
                         WHERE id=?
                        """,
                        (position_id, symbol, _safe_float(pos.get("volume"), 0.0), journal_id),
                    )
                elif source and symbol:
                    untracked_unsafe = self._unsafe_untracked_position(pos, journal_id=None)
                    if untracked_unsafe:
                        journal_id = None
                    else:
                        req = {
                            "symbol": symbol,
                            "source": source,
                            "label": str(pos.get("label", "") or ""),
                            "comment": str(pos.get("comment", "") or ""),
                            "direction": str(pos.get("direction", "") or ""),
                            "entry": _safe_float(pos.get("entry_price"), 0.0),
                            "stop_loss": _safe_float(pos.get("stop_loss"), 0.0),
                            "take_profit": _safe_float(pos.get("take_profit"), 0.0),
                            "signal_run_no": int(meta.get("run_no", 0) or 0),
                            "signal_run_id": str(meta.get("run_id", "") or ""),
                            "reconciled_external": True,
                        }
                        cur = conn.execute(
                            """
                            INSERT INTO execution_journal (
                                created_ts, created_utc, source, lane, symbol, direction, confidence,
                                entry, stop_loss, take_profit, entry_type, dry_run, account_id,
                                broker_symbol, volume, status, message, order_id, position_id, deal_id,
                                signal_run_id, signal_run_no, request_json, response_json, execution_meta_json
                            ) VALUES (?, datetime('now'), ?, ?, ?, ?, 0, ?, ?, ?, 'market', 0, ?, ?, ?, 'reconciled_open',
                                      'ctrader reconciled open position', NULL, ?, NULL, ?, ?, ?, '{}', ?)
                            """,
                            (
                                time.time(),
                                source,
                                lane,
                                symbol,
                                str(pos.get("direction", "") or ""),
                                _safe_float(pos.get("entry_price"), 0.0),
                                _safe_float(pos.get("stop_loss"), 0.0),
                                _safe_float(pos.get("take_profit"), 0.0),
                                account_id,
                                symbol,
                                _safe_float(pos.get("volume"), 0.0),
                                position_id,
                                str(meta.get("run_id", "") or ""),
                                int(meta.get("run_no", 0) or 0),
                                json.dumps(req, ensure_ascii=True),
                                json.dumps({"reconcile": True}, ensure_ascii=True),
                            ),
                        )
                        journal_id = int(cur.lastrowid)
                        report["reconciled_journal"] += 1
                existing_position_row = conn.execute(
                    "SELECT first_seen_utc FROM ctrader_positions WHERE position_id=? LIMIT 1",
                    (position_id,),
                ).fetchone()
                first_seen_utc = str((existing_position_row["first_seen_utc"] if existing_position_row is not None else "") or "").strip() or now_iso
                conn.execute(
                    """
                    INSERT INTO ctrader_positions(
                        position_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                        entry_price, stop_loss, take_profit, label, comment, signal_run_id, signal_run_no,
                        journal_id, is_open, status, first_seen_utc, last_seen_utc, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                    ON CONFLICT(position_id) DO UPDATE SET
                        account_id=excluded.account_id,
                        source=excluded.source,
                        lane=excluded.lane,
                        symbol=excluded.symbol,
                        broker_symbol=excluded.broker_symbol,
                        direction=excluded.direction,
                        volume=excluded.volume,
                        entry_price=excluded.entry_price,
                        stop_loss=excluded.stop_loss,
                        take_profit=excluded.take_profit,
                        label=excluded.label,
                        comment=excluded.comment,
                        signal_run_id=excluded.signal_run_id,
                        signal_run_no=excluded.signal_run_no,
                        journal_id=excluded.journal_id,
                        is_open=1,
                        status=excluded.status,
                        last_seen_utc=excluded.last_seen_utc,
                        raw_json=excluded.raw_json
                    """,
                    (
                        position_id,
                        account_id,
                        source,
                        lane,
                        symbol,
                        symbol,
                        str(pos.get("direction", "") or ""),
                        _safe_float(pos.get("volume"), 0.0),
                        _safe_float(pos.get("entry_price"), 0.0),
                        _safe_float(pos.get("stop_loss"), 0.0),
                        _safe_float(pos.get("take_profit"), 0.0),
                        str(pos.get("label", "") or ""),
                        str(pos.get("comment", "") or ""),
                        str(meta.get("run_id", "") or ""),
                        int(meta.get("run_no", 0) or 0),
                        journal_id,
                        str(pos.get("status", "") or ""),
                        first_seen_utc,
                        now_iso,
                        json.dumps(pos, ensure_ascii=True),
                    ),
                )
                if not untracked_unsafe:
                    self._sync_signal_event_open(
                        source=source,
                        symbol=symbol,
                        direction=str(pos.get("direction", "") or ""),
                        position_id=position_id,
                        journal_row=journal_row,
                        extra={
                            "kind": "ctrader_open",
                            "position": pos,
                            "journal_id": journal_id,
                        },
                        entry_price=_safe_float(pos.get("entry_price"), 0.0),
                        stop_loss=_safe_float(pos.get("stop_loss"), 0.0),
                        take_profit=_safe_float(pos.get("take_profit"), 0.0),
                        run_id=str(meta.get("run_id", "") or ""),
                        run_no=int(meta.get("run_no", 0) or 0),
                    )
                    tracked_positions.append({
                        "position": {
                            "position_id": position_id,
                            "symbol": symbol,
                            "direction": str(pos.get("direction", "") or ""),
                            "volume": _safe_float(pos.get("volume"), 0.0),
                            "entry_price": _safe_float(pos.get("entry_price"), 0.0),
                            "stop_loss": _safe_float(pos.get("stop_loss"), 0.0),
                            "take_profit": _safe_float(pos.get("take_profit"), 0.0),
                            "source": source,
                            "first_seen_utc": first_seen_utc,
                            "last_seen_utc": now_iso,
                        },
                        "source": source,
                        "lane": lane,
                        "journal_row": journal_row,
                    })
                if auto_close_unsafe and lane != "external" and source != "untagged_external" and (untracked_unsafe or self._unsafe_untracked_position(pos, journal_id=journal_id)):
                    pending_close.append({"position_id": position_id, "source": source, "symbol": symbol})
            if seen_positions:
                placeholders = ",".join(["?"] * len(seen_positions))
                conn.execute(
                    f"UPDATE ctrader_positions SET is_open=0, last_seen_utc=? WHERE account_id=? AND position_id NOT IN ({placeholders})",
                    (now_iso, account_id, *sorted(seen_positions)),
                )
            else:
                conn.execute(
                    "UPDATE ctrader_positions SET is_open=0, last_seen_utc=? WHERE account_id=?",
                    (now_iso, account_id),
                )
            if seen_orders:
                placeholders = ",".join(["?"] * len(seen_orders))
                conn.execute(
                    f"UPDATE ctrader_orders SET is_open=0, last_seen_utc=? WHERE account_id=? AND order_id NOT IN ({placeholders})",
                    (now_iso, account_id, *sorted(seen_orders)),
                )
            else:
                conn.execute(
                    "UPDATE ctrader_orders SET is_open=0, last_seen_utc=? WHERE account_id=?",
                    (now_iso, account_id),
                )
            if self._position_peak_r:
                alive = set(int(pid) for pid in seen_positions)
                self._position_peak_r = {
                    int(pid): float(peak)
                    for pid, peak in self._position_peak_r.items()
                    if int(pid) in alive
                }
            for deal in deals:
                position_id = int(_safe_float(deal.get("position_id"), 0))
                source = ""
                lane = ""
                symbol = str(deal.get("symbol", "") or "").strip().upper()
                direction = str(deal.get("direction", "") or "").strip().lower()
                prow = conn.execute(
                    "SELECT source, lane, symbol, direction, journal_id, signal_run_id, signal_run_no FROM ctrader_positions WHERE position_id=? LIMIT 1",
                    (position_id,),
                ).fetchone()
                if prow is not None:
                    source = str(prow["source"] or "").strip().lower()
                    lane = str(prow["lane"] or "").strip()
                    symbol = symbol or str(prow["symbol"] or "").strip().upper()
                    direction = direction or str(prow["direction"] or "").strip().lower()
                    journal_id = int(prow["journal_id"] or 0) or None
                    signal_run_id = str(prow["signal_run_id"] or "")
                    signal_run_no = int(prow["signal_run_no"] or 0)
                else:
                    jrow = self._find_journal_match(
                        conn,
                        position_id=position_id,
                        source="",
                        symbol=symbol,
                        run_no=0,
                        entry_price=_safe_float(deal.get("execution_price"), 0.0),
                    )
                    journal_id = int(jrow["id"]) if jrow is not None else None
                    jrow_obj = dict(jrow) if jrow is not None else {}
                    source = str(jrow_obj.get("source", "") or "").strip().lower()
                    lane = self._source_lane(source)
                    signal_run_id = str(jrow_obj.get("signal_run_id", "") or "")
                    signal_run_no = int(jrow_obj.get("signal_run_no", 0) or 0)
                journal_detail_row = None
                if journal_id is not None:
                    journal_detail_row = conn.execute(
                        """
                        SELECT id, source, symbol, direction, confidence, entry_type, request_json, execution_meta_json
                          FROM execution_journal
                         WHERE id=? LIMIT 1
                        """,
                        (int(journal_id),),
                    ).fetchone()
                outcome = None
                if bool(deal.get("has_close_detail")):
                    outcome = 1 if _safe_float(deal.get("pnl_usd"), 0.0) > 0 else 0
                    conn.execute(
                        "UPDATE ctrader_positions SET is_open=0, last_seen_utc=? WHERE position_id=?",
                        (str(deal.get("execution_utc") or now_iso), position_id),
                    )
                    self._apply_closed_deal_to_journal(
                        conn,
                        journal_id=journal_id,
                        deal=deal,
                        closed_at=str(deal.get("execution_utc") or now_iso),
                    )
                    if journal_id is not None:
                        self._sync_signal_event_close(
                            source=source,
                            symbol=symbol,
                            direction=direction,
                            position_id=position_id,
                            pnl_usd=_safe_float(deal.get("pnl_usd"), 0.0),
                            closed_at=str(deal.get("execution_utc") or now_iso),
                            extra={"kind": "ctrader_close", "deal": deal, "journal_id": journal_id},
                        )
                    try:
                        from copy_trade.manager import copy_trade_manager

                        copy_trade_manager.enforce_close_follow_async(
                            master_position_id=position_id,
                            master_order_id=int(deal.get("order_id") or 0),
                            master_deal_id=int(deal.get("deal_id") or 0),
                            reason="master_close_reconcile",
                            master_close_utc=str(deal.get("execution_utc") or now_iso),
                        )
                    except Exception as ct_err:
                        logger.debug("[CopyTrade] reconcile close-follow skipped: %s", ct_err)
                conn.execute(
                    """
                    INSERT INTO ctrader_deals(
                        deal_id, account_id, position_id, order_id, source, lane, symbol, broker_symbol,
                        direction, volume, execution_price, gross_profit_usd, swap_usd, commission_usd,
                        pnl_conversion_fee_usd, pnl_usd, outcome, has_close_detail, signal_run_id, signal_run_no,
                        journal_id, execution_utc, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(deal_id) DO UPDATE SET
                        account_id=excluded.account_id,
                        position_id=excluded.position_id,
                        order_id=excluded.order_id,
                        source=excluded.source,
                        lane=excluded.lane,
                        symbol=excluded.symbol,
                        broker_symbol=excluded.broker_symbol,
                        direction=excluded.direction,
                        volume=excluded.volume,
                        execution_price=excluded.execution_price,
                        gross_profit_usd=excluded.gross_profit_usd,
                        swap_usd=excluded.swap_usd,
                        commission_usd=excluded.commission_usd,
                        pnl_conversion_fee_usd=excluded.pnl_conversion_fee_usd,
                        pnl_usd=excluded.pnl_usd,
                        outcome=excluded.outcome,
                        has_close_detail=excluded.has_close_detail,
                        signal_run_id=excluded.signal_run_id,
                        signal_run_no=excluded.signal_run_no,
                        journal_id=excluded.journal_id,
                        execution_utc=excluded.execution_utc,
                        raw_json=excluded.raw_json
                    """,
                    (
                        int(deal.get("deal_id") or 0),
                        account_id,
                        position_id,
                        int(deal.get("order_id") or 0),
                        source,
                        lane,
                        symbol,
                        symbol,
                        direction,
                        _safe_float(deal.get("volume"), 0.0),
                        _safe_float(deal.get("execution_price"), 0.0),
                        _safe_float(deal.get("gross_profit_usd"), 0.0),
                        _safe_float(deal.get("swap_usd"), 0.0),
                        _safe_float(deal.get("commission_usd"), 0.0),
                        _safe_float(deal.get("pnl_conversion_fee_usd"), 0.0),
                        _safe_float(deal.get("pnl_usd"), 0.0),
                        outcome,
                        1 if bool(deal.get("has_close_detail")) else 0,
                        signal_run_id,
                        signal_run_no,
                        journal_id,
                        str(deal.get("execution_utc") or ""),
                        json.dumps(
                            self._build_deal_attribution_payload(
                                deal=deal,
                                source=source,
                                lane=lane,
                                symbol=symbol,
                                direction=direction,
                                journal_row=journal_detail_row,
                            ),
                            ensure_ascii=True,
                        ),
                    ),
                )
            conn.commit()
        if bool(getattr(config, "CTRADER_PENDING_ORDER_SWEEP_ON_SYNC", True)):
            sweep_report = self._sweep_pending_orders(tracked_orders)
            report["canceled_orders"] = int(sweep_report.get("canceled_orders", 0) or 0)
            report["repriced_orders"] = int(sweep_report.get("repriced_orders", 0) or 0)
            report["follow_stop_orders"] = int(sweep_report.get("follow_stop_orders", 0) or 0)
            report["order_actions"] = list(sweep_report.get("cancel_actions") or [])
            report["order_reprice_actions"] = list(sweep_report.get("reprice_actions") or [])
            report["order_follow_stop_actions"] = list(sweep_report.get("follow_stop_actions") or [])
        pm_report = self._manage_open_positions(tracked_positions)
        report["managed_positions"] = int(pm_report.get("managed_positions", 0) or 0)
        report["amended_positions"] = int(pm_report.get("amended_positions", 0) or 0)
        report["closed_profit_positions"] = int(pm_report.get("closed_profit_positions", 0) or 0)
        report["pm_actions"] = list(pm_report.get("pm_actions") or [])
        try:
            report["pm_audited_actions"] = int(self._persist_position_manager_audit(report["pm_actions"]))
        except Exception as e:
            report["pm_audit_error"] = str(e)
        for row in pending_close:
            res = self.close_position(position_id=int(row.get("position_id") or 0))
            if bool(res.ok):
                report["closed_unsafe"] += 1
                report["closed_position_ids"].append(int(row.get("position_id") or 0))
        return report

    def get_lane_stats(self, *, symbol: str, start_utc: str, end_utc: str) -> dict:
        symbol_u = str(symbol or "").strip().upper()
        source_map = {
            "ETHUSD": {"main": "scalp_ethusd", "winner": "scalp_ethusd:winner"},
            "BTCUSD": {"main": "scalp_btcusd", "winner": "scalp_btcusd:winner"},
            "XAUUSD": {"main": "scalp_xauusd", "winner": "xauusd_scheduled:winner"},
        }
        selected = dict(source_map.get(symbol_u) or {})
        out = {"available": False, "symbol": symbol_u, "lanes": {}}
        if not selected:
            return out
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            lanes = {}
            for lane_name, source in selected.items():
                sent = conn.execute(
                    """
                    SELECT COUNT(*) FROM execution_journal
                     WHERE created_utc >= ? AND created_utc < ?
                       AND LOWER(COALESCE(source,''))=?
                    """,
                    (start_utc, end_utc, str(source).lower()),
                ).fetchone()[0]
                filled = conn.execute(
                    """
                    SELECT COUNT(*) FROM execution_journal
                     WHERE created_utc >= ? AND created_utc < ?
                       AND LOWER(COALESCE(source,''))=?
                       AND (position_id IS NOT NULL OR LOWER(COALESCE(status,'')) IN ('filled','accepted','reconciled_open','closed'))
                    """,
                    (start_utc, end_utc, str(source).lower()),
                ).fetchone()[0]
                open_count = conn.execute(
                    """
                    SELECT COUNT(*) FROM ctrader_positions
                     WHERE is_open=1
                       AND journal_id IS NOT NULL
                       AND LOWER(COALESCE(source,''))=?
                       AND UPPER(COALESCE(symbol,''))=?
                    """,
                    (str(source).lower(), symbol_u),
                ).fetchone()[0]
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS resolved,
                           SUM(CASE WHEN outcome=1 THEN 1 ELSE 0 END) AS wins,
                           SUM(CASE WHEN outcome=0 THEN 1 ELSE 0 END) AS losses,
                           SUM(COALESCE(pnl_usd,0.0)) AS pnl
                      FROM ctrader_deals
                     WHERE execution_utc >= ? AND execution_utc < ?
                       AND has_close_detail=1
                       AND journal_id IS NOT NULL
                       AND LOWER(COALESCE(source,''))=?
                       AND UPPER(COALESCE(symbol,''))=?
                    """,
                    (start_utc, end_utc, str(source).lower(), symbol_u),
                ).fetchone()
                resolved = int((row["resolved"] or 0) if row is not None else 0)
                wins = int((row["wins"] or 0) if row is not None else 0)
                losses = int((row["losses"] or 0) if row is not None else 0)
                pnl = round(float((row["pnl"] or 0.0) if row is not None else 0.0), 2)
                lanes[lane_name] = {
                    "sent": int(sent or 0),
                    "filled": int(filled or 0),
                    "open": int(open_count or 0),
                    "resolved": resolved,
                    "wins": wins,
                    "losses": losses,
                    "pnl": pnl,
                    "fill_rate_pct": round((100.0 * float(filled or 0) / float(sent or 1)), 2) if int(sent or 0) > 0 else 0.0,
                    "win_rate_pct": round((100.0 * float(wins or 0) / float(resolved or 1)), 2) if resolved > 0 else 0.0,
                }
        out["available"] = True
        out["lanes"] = lanes
        return out

    def status(self, *, include_recent: bool = False) -> dict:
        counts = {
            "rows": 0,
            "ok": 0,
            "errors": 0,
            "dry_run": 0,
            "open_positions": 0,
            "open_orders": 0,
            "close_deals": 0,
        }
        latest: dict | None = None
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS rows,
                    SUM(CASE WHEN status IN ('accepted','filled','pending','dry_run','closed') AND COALESCE(message,'') != '' THEN 1 ELSE 0 END) AS ok_rows,
                    SUM(CASE WHEN dry_run = 1 THEN 1 ELSE 0 END) AS dry_rows,
                    SUM(CASE WHEN status IN ('error','worker_error','auth_failed','account_auth_failed','timeout','invalid','rejected') THEN 1 ELSE 0 END) AS err_rows
                FROM execution_journal
                """
            ).fetchone()
            if row:
                counts["rows"] = int(row["rows"] or 0)
                counts["ok"] = int(row["ok_rows"] or 0)
                counts["dry_run"] = int(row["dry_rows"] or 0)
                counts["errors"] = int(row["err_rows"] or 0)
            last_row = conn.execute(
                "SELECT * FROM execution_journal ORDER BY created_ts DESC LIMIT 1"
            ).fetchone()
            if last_row:
                latest = dict(last_row)
            pos_row = conn.execute("SELECT COUNT(*) AS n FROM ctrader_positions WHERE is_open=1").fetchone()
            if pos_row:
                counts["open_positions"] = int(pos_row["n"] or 0)
            ord_row = conn.execute("SELECT COUNT(*) AS n FROM ctrader_orders WHERE is_open=1").fetchone()
            if ord_row:
                counts["open_orders"] = int(ord_row["n"] or 0)
            deal_row = conn.execute("SELECT COUNT(*) AS n FROM ctrader_deals WHERE has_close_detail=1").fetchone()
            if deal_row:
                counts["close_deals"] = int(deal_row["n"] or 0)
        account_id, account_reason = self._configured_account_id()
        report = {
            "enabled": self.enabled,
            "autotrade_enabled": self.autotrade_enabled,
            "dry_run": self.dry_run,
            "sdk_available": self.sdk_available,
            "db_path": str(self.db_path),
            "worker_path": str(self.worker_path),
            "account_id": account_id,
            "account_reason": account_reason,
            "allowed_sources": sorted(list(getattr(config, "get_ctrader_allowed_sources", lambda: set())() or set())),
            "allowed_symbols": sorted(list(getattr(config, "get_ctrader_allowed_symbols", lambda: set())() or set())),
            "counts": counts,
            "tokens_present": {
                "client_id": bool(str(getattr(config, "CTRADER_OPENAPI_CLIENT_ID", "") or "").strip()),
                "client_secret": bool(str(getattr(config, "CTRADER_OPENAPI_CLIENT_SECRET", "") or "").strip()),
                "access_token": bool(str(getattr(config, "CTRADER_OPENAPI_ACCESS_TOKEN", "") or "").strip()),
                "refresh_token": bool(str(getattr(config, "CTRADER_OPENAPI_REFRESH_TOKEN", "") or "").strip()),
            },
            "latest": latest or {},
        }
        if include_recent:
            report["recent"] = self.get_recent_journal(limit=10)
            report["open_positions"] = self.get_open_positions()
            report["open_orders"] = self.get_open_orders()
            report["recent_deals"] = self.get_recent_deals(limit=10)
        return report


ctrader_executor = CTraderExecutor()
