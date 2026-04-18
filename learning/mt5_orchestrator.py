"""
learning/mt5_orchestrator.py
Multi-account-ready policy orchestrator for MT5 execution.

Current runtime supports a single active MT5 bridge, but this module persists
per-account policies/states so future multi-bridge scaling does not require
reworking the risk/sizing layer.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config import config
from execution.mt5_executor import mt5_executor
from learning.mt5_autopilot_core import mt5_autopilot_core
from learning.mt5_walkforward import mt5_walkforward


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


@dataclass
class ExecutionPlan:
    allow: bool
    status: str
    reason: str
    account_key: str = ""
    canary_mode: bool = True
    risk_multiplier: float = 0.25
    gate_snapshot: Optional[dict] = None
    walkforward: Optional[dict] = None
    policy: Optional[dict] = None


class MT5Orchestrator:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        cfg = str(getattr(config, "MT5_ORCHESTRATOR_DB_PATH", "") or "").strip()
        self.db_path = Path(db_path or cfg or (data_dir / "mt5_orchestrator.db"))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=15)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS mt5_orchestrator_accounts (
                        account_key TEXT PRIMARY KEY,
                        account_login INTEGER,
                        account_server TEXT,
                        status TEXT NOT NULL DEFAULT 'active',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        policy_json TEXT,
                        runtime_json TEXT
                    )
                    """
                )
                conn.commit()

    @staticmethod
    def _account_key_from_mt5_status(st: dict) -> str:
        login = _safe_int(st.get("account_login", 0), 0)
        server = str(st.get("account_server", "") or "")
        return f"{server}|{login}" if login and server else ""

    def _default_policy(self) -> dict:
        return {
            "max_risk_multiplier": _safe_float(getattr(config, "MT5_ADAPTIVE_SIZING_MAX_MULT", 1.25), 1.25),
            "min_risk_multiplier": _safe_float(getattr(config, "MT5_ADAPTIVE_SIZING_MIN_MULT", 0.25), 0.25),
            "canary_force": None,  # None|true|false
            "position_manager_enabled": bool(getattr(config, "MT5_POSITION_MANAGER_ENABLED", True)),
            "daily_loss_limit_usd": None,
            "daily_loss_limit_pct": None,
            "max_consecutive_losses": None,
            "loss_cooldown_min": None,
            "max_rejections_1h": None,
            "pm_early_risk_enabled": None,
            "pm_early_risk_trigger_r": None,
            "pm_early_risk_sl_r": None,
            "pm_early_risk_buffer_r": None,
            "pm_spread_spike_protect_enabled": None,
            "pm_spread_spike_pct": None,
            "pm_break_even_r": None,
            "pm_partial_tp_r": None,
            "pm_trail_start_r": None,
            "pm_trail_gap_r": None,
            "pm_time_stop_min": None,
            "pm_time_stop_flat_r": None,
            "notes": "",
        }

    def _symbol_override_candidates(self, signal) -> list[str]:
        candidates: list[str] = []

        def _add(v: str):
            s = str(v or "").strip().upper()
            if s and s not in candidates:
                candidates.append(s)

        try:
            _add(str(getattr(signal, "symbol", "") or ""))
        except Exception:
            pass
        try:
            if candidates:
                _add(str(mt5_executor.resolve_symbol(candidates[0]) or ""))
        except Exception:
            pass
        return candidates

    @staticmethod
    def _lookup_symbol_override(candidates: list[str], overrides: dict) -> tuple[object, str]:
        for idx, c in enumerate(candidates):
            if not c or c not in overrides:
                continue
            val = overrides.get(c)
            if idx == 0:
                return val, f"symbol_override:{c}"
            base_sym = candidates[0] if candidates else ""
            return val, f"symbol_override_mapped:{c}<-{base_sym}"
        return None, ""

    def policy_key_specs(self) -> list[dict]:
        return [
            {
                "key": "canary_force",
                "type": "bool|null",
                "default": None,
                "example": "false | true | auto",
                "desc": "Force canary mode on/off (auto uses walk-forward decision).",
            },
            {
                "key": "min_risk_multiplier",
                "type": "float|null",
                "default": self._default_policy().get("min_risk_multiplier"),
                "example": "0.25",
                "desc": "Lower bound for adaptive sizing multiplier.",
            },
            {
                "key": "max_risk_multiplier",
                "type": "float|null",
                "default": self._default_policy().get("max_risk_multiplier"),
                "example": "1.00",
                "desc": "Upper bound for adaptive sizing multiplier.",
            },
            {
                "key": "position_manager_enabled",
                "type": "bool",
                "default": self._default_policy().get("position_manager_enabled"),
                "example": "true",
                "desc": "Enable/disable position manager actions for this account.",
            },
            {
                "key": "daily_loss_limit_usd",
                "type": "float|null",
                "default": None,
                "example": "0.80",
                "desc": "Override daily realized-loss hard stop in USD.",
            },
            {
                "key": "daily_loss_limit_pct",
                "type": "float|null",
                "default": None,
                "example": "12",
                "desc": "Override daily realized-loss hard stop as % equity.",
            },
            {
                "key": "max_consecutive_losses",
                "type": "int|null",
                "default": None,
                "example": "2",
                "desc": "Override max consecutive losses before cooldown.",
            },
            {
                "key": "loss_cooldown_min",
                "type": "int|null",
                "default": None,
                "example": "45",
                "desc": "Override cooldown minutes after loss-streak trigger.",
            },
            {
                "key": "max_rejections_1h",
                "type": "int|null",
                "default": None,
                "example": "4",
                "desc": "Override broker rejection/error storm limit per hour.",
            },
            {
                "key": "pm_early_risk_enabled",
                "type": "bool|null",
                "default": None,
                "example": "true | false | auto",
                "desc": "Override PM early-risk protector enable flag (auto uses config).",
            },
            {
                "key": "pm_early_risk_trigger_r",
                "type": "float|null",
                "default": None,
                "example": "-0.8",
                "desc": "Trigger early-risk SL tighten when loss reaches this R.",
            },
            {
                "key": "pm_early_risk_sl_r",
                "type": "float|null",
                "default": None,
                "example": "-0.92",
                "desc": "Target SL level in R for early-risk tighten (must stay behind current price).",
            },
            {
                "key": "pm_early_risk_buffer_r",
                "type": "float|null",
                "default": None,
                "example": "0.05",
                "desc": "Minimum R buffer behind current price when tightening early-risk SL.",
            },
            {
                "key": "pm_spread_spike_protect_enabled",
                "type": "bool|null",
                "default": None,
                "example": "true | false | auto",
                "desc": "Enable spread-spike trigger for early-risk protector (auto uses config).",
            },
            {
                "key": "pm_spread_spike_pct",
                "type": "float|null",
                "default": None,
                "example": "0.18",
                "desc": "Spread % threshold to trigger early-risk protection on losing trades.",
            },
            {
                "key": "pm_break_even_r",
                "type": "float|null",
                "default": None,
                "example": "0.75",
                "desc": "Override PM break-even trigger in R.",
            },
            {
                "key": "pm_partial_tp_r",
                "type": "float|null",
                "default": None,
                "example": "0.95",
                "desc": "Override PM partial take-profit trigger in R.",
            },
            {
                "key": "pm_trail_start_r",
                "type": "float|null",
                "default": None,
                "example": "1.30",
                "desc": "Override PM trailing-start trigger in R.",
            },
            {
                "key": "pm_trail_gap_r",
                "type": "float|null",
                "default": None,
                "example": "0.65",
                "desc": "Override PM trailing gap in R.",
            },
            {
                "key": "pm_time_stop_min",
                "type": "int|null",
                "default": None,
                "example": "90",
                "desc": "Override PM time-stop minutes.",
            },
            {
                "key": "pm_time_stop_flat_r",
                "type": "float|null",
                "default": None,
                "example": "0.22",
                "desc": "Override PM time-stop flat threshold in R.",
            },
            {
                "key": "notes",
                "type": "string",
                "default": "",
                "example": "micro-risk profile",
                "desc": "Operator note for this account policy.",
            },
        ]

    def _load_account_row(self, account_key: str) -> Optional[dict]:
        if not account_key:
            return None
        with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    """
                    SELECT account_key, account_login, account_server, status, policy_json, runtime_json, updated_at
                      FROM mt5_orchestrator_accounts
                     WHERE account_key=?
                    """,
                    (account_key,),
                ).fetchone()
        if not row:
            return None
        return {
            "account_key": row[0],
            "account_login": row[1],
            "account_server": row[2],
            "status": row[3],
            "policy": json.loads(row[4] or "{}") if row[4] else {},
            "runtime": json.loads(row[5] or "{}") if row[5] else {},
            "updated_at": row[6],
        }

    def _save_account_policy(self, account_key: str, policy: dict, *, st: Optional[dict] = None) -> dict:
        if not account_key:
            return {"ok": False, "message": "missing account_key"}
        st = dict(st or {})
        login = _safe_int(st.get("account_login", 0), 0)
        server = str(st.get("account_server", "") or "")
        if (not login or not server) and "|" in account_key:
            server, _, login_s = account_key.partition("|")
            login = _safe_int(login_s, 0)
        now = _iso(_utc_now())
        clean = dict(self._default_policy())
        clean.update(dict(policy or {}))
        with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO mt5_orchestrator_accounts(account_key, account_login, account_server, status, created_at, updated_at, policy_json, runtime_json)
                    VALUES (?, ?, ?, 'active', ?, ?, ?, COALESCE((SELECT runtime_json FROM mt5_orchestrator_accounts WHERE account_key=?), '{}'))
                    ON CONFLICT(account_key) DO UPDATE SET
                        account_login=CASE WHEN excluded.account_login>0 THEN excluded.account_login ELSE mt5_orchestrator_accounts.account_login END,
                        account_server=CASE WHEN excluded.account_server<>'' THEN excluded.account_server ELSE mt5_orchestrator_accounts.account_server END,
                        updated_at=excluded.updated_at,
                        policy_json=excluded.policy_json
                    """,
                    (
                        account_key,
                        login,
                        server,
                        now,
                        now,
                        json.dumps(clean, ensure_ascii=True, separators=(",", ":")),
                        account_key,
                    ),
                )
                conn.commit()
        return {"ok": True, "account_key": account_key, "policy": clean}

    @staticmethod
    def _coerce_policy_value(key: str, value):
        k = str(key or "").strip().lower()
        if k in {"canary_force", "pm_early_risk_enabled", "pm_spread_spike_protect_enabled"}:
            if value is None:
                return None
            s = str(value).strip().lower()
            if s in {"none", "auto", "default", ""}:
                return None
            if s in {"1", "true", "yes", "on"}:
                return True
            if s in {"0", "false", "no", "off"}:
                return False
            raise ValueError(f"{k} must be true/false/auto")
        if k in {"position_manager_enabled"}:
            s = str(value).strip().lower()
            if s in {"1", "true", "yes", "on"}:
                return True
            if s in {"0", "false", "no", "off"}:
                return False
            raise ValueError(f"{k} must be true/false")
        if k in {"notes"}:
            return str(value or "")[:300]
        if k in {"max_consecutive_losses", "loss_cooldown_min", "max_rejections_1h", "pm_time_stop_min"}:
            if value is None or str(value).strip().lower() in {"none", "default", "auto", ""}:
                return None
            return max(0, _safe_int(value, 0))
        if k in {"pm_early_risk_trigger_r", "pm_early_risk_sl_r"}:
            if value is None or str(value).strip().lower() in {"none", "default", "auto", ""}:
                return None
            return -abs(_safe_float(value, -0.8))
        if k in {"pm_early_risk_buffer_r", "pm_spread_spike_pct", "pm_break_even_r", "pm_partial_tp_r", "pm_trail_start_r", "pm_trail_gap_r", "pm_time_stop_flat_r"}:
            if value is None or str(value).strip().lower() in {"none", "default", "auto", ""}:
                return None
            return max(0.0, _safe_float(value, 0.0))
        if k in {
            "min_risk_multiplier", "max_risk_multiplier", "daily_loss_limit_usd", "daily_loss_limit_pct",
        }:
            if value is None or str(value).strip().lower() in {"none", "default", "auto", ""}:
                return None
            return max(0.0, _safe_float(value, 0.0))
        raise KeyError(k)

    def policy_presets(self) -> list[dict]:
        return [
            {
                "name": "micro_safe",
                "desc": "Very conservative micro-account preset (strict canary + tighter loss limits).",
                "values": {
                    "canary_force": True,
                    "min_risk_multiplier": 0.20,
                    "max_risk_multiplier": 0.45,
                    "daily_loss_limit_usd": 0.40,
                    "daily_loss_limit_pct": 8.0,
                    "max_consecutive_losses": 1,
                    "loss_cooldown_min": 90,
                    "max_rejections_1h": 3,
                    "position_manager_enabled": True,
                    "pm_early_risk_enabled": True,
                    "pm_early_risk_trigger_r": 0.65,
                    "pm_early_risk_sl_r": 0.88,
                    "pm_early_risk_buffer_r": 0.05,
                    "pm_spread_spike_protect_enabled": True,
                    "pm_spread_spike_pct": 0.12,
                    "notes": "preset:micro_safe",
                },
            },
            {
                "name": "micro_aggressive",
                "desc": "Higher throughput micro-account preset (auto canary, looser limits, still bounded).",
                "values": {
                    "canary_force": None,
                    "min_risk_multiplier": 0.30,
                    "max_risk_multiplier": 1.00,
                    "daily_loss_limit_usd": 0.90,
                    "daily_loss_limit_pct": 15.0,
                    "max_consecutive_losses": 2,
                    "loss_cooldown_min": 45,
                    "max_rejections_1h": 4,
                    "position_manager_enabled": True,
                    "pm_early_risk_enabled": True,
                    "pm_early_risk_trigger_r": 0.80,
                    "pm_early_risk_sl_r": 0.92,
                    "pm_early_risk_buffer_r": 0.05,
                    "pm_spread_spike_protect_enabled": True,
                    "pm_spread_spike_pct": 0.18,
                    "notes": "preset:micro_aggressive",
                },
            },
        ]

    def _normalize_preset_values(self, preset_values: dict) -> dict:
        out = {}
        for k, v in dict(preset_values or {}).items():
            key_norm = str(k or "").strip().lower()
            if key_norm in {"pm_early_risk_trigger_r", "pm_early_risk_sl_r"} and v is not None:
                # Store as negative R internally for PM logic consistency.
                vv = -abs(_safe_float(v, 0.0))
                out[key_norm] = vv
                continue
            out[key_norm] = v
        return out

    def current_account_policy(self) -> dict:
        st = mt5_executor.status()
        account_key = self._account_key_from_mt5_status(st)
        if not account_key:
            return {"ok": False, "message": "mt5 not connected", "account_key": "", "policy": {}}
        row = self._load_account_row(account_key) or {}
        policy = dict(self._default_policy())
        policy.update(dict(row.get("policy", {}) or {}))
        return {"ok": True, "account_key": account_key, "policy": policy}

    def get_account_policy(self, account_key: str) -> dict:
        key = str(account_key or "").strip()
        if not key:
            return {"ok": False, "message": "missing account_key", "account_key": "", "policy": {}}
        row = self._load_account_row(key) or {}
        policy = dict(self._default_policy())
        policy.update(dict(row.get("policy", {}) or {}))
        return {"ok": True, "account_key": key, "policy": policy}

    def set_current_account_policy(self, key: str, value) -> dict:
        st = mt5_executor.status()
        account_key = self._account_key_from_mt5_status(st)
        if not account_key:
            return {"ok": False, "message": "mt5 not connected", "account_key": ""}
        cur = self.current_account_policy()
        if not cur.get("ok"):
            return cur
        policy = dict(cur.get("policy", {}) or {})
        key_norm = str(key or "").strip().lower()
        try:
            coerced = self._coerce_policy_value(key_norm, value)
        except KeyError:
            return {"ok": False, "message": f"unknown policy key: {key_norm}", "account_key": account_key}
        except ValueError as e:
            return {"ok": False, "message": str(e), "account_key": account_key}
        policy[key_norm] = coerced
        # Keep min/max sane.
        min_mult = policy.get("min_risk_multiplier")
        max_mult = policy.get("max_risk_multiplier")
        if (min_mult is not None) and (max_mult is not None):
            if _safe_float(min_mult, 0.0) > _safe_float(max_mult, 0.0):
                policy["max_risk_multiplier"] = _safe_float(min_mult, 0.0)
        return self._save_account_policy(account_key, policy, st=st) | {"updated_key": key_norm, "updated_value": policy.get(key_norm)}

    def reset_current_account_policy(self) -> dict:
        st = mt5_executor.status()
        account_key = self._account_key_from_mt5_status(st)
        if not account_key:
            return {"ok": False, "message": "mt5 not connected", "account_key": ""}
        return self._save_account_policy(account_key, self._default_policy(), st=st) | {"reset": True}

    def apply_current_account_preset(self, preset_name: str) -> dict:
        st = mt5_executor.status()
        account_key = self._account_key_from_mt5_status(st)
        if not account_key:
            return {"ok": False, "message": "mt5 not connected", "account_key": ""}
        name = str(preset_name or "").strip().lower()
        preset = next((p for p in self.policy_presets() if str(p.get("name", "")).lower() == name), None)
        if not preset:
            return {"ok": False, "message": f"unknown preset: {name}", "account_key": account_key}
        cur = self.current_account_policy()
        if not cur.get("ok"):
            return cur
        policy = dict(cur.get("policy", {}) or {})
        policy.update(self._normalize_preset_values(dict(preset.get("values", {}) or {})))
        saved = self._save_account_policy(account_key, policy, st=st)
        return saved | {"preset": name, "preset_desc": str(preset.get("desc", ""))}

    def sync_current_account(self) -> dict:
        st = mt5_executor.status()
        account_key = self._account_key_from_mt5_status(st)
        if not account_key:
            return {"ok": False, "message": "mt5 not connected", "account_key": ""}

        autop = mt5_autopilot_core.status()
        wf = mt5_walkforward.build_report(
            account_key,
            train_days=max(7, _safe_int(getattr(config, "MT5_WF_TRAIN_DAYS", 30), 30)),
            forward_days=max(1, _safe_int(getattr(config, "MT5_WF_FORWARD_DAYS", 7), 7)),
        )
        existing = self._load_account_row(account_key) or {}
        policy = dict(self._default_policy())
        policy.update(dict(existing.get("policy", {}) or {}))
        existing_runtime = dict(existing.get("runtime", {}) or {})
        runtime = {
            "mt5_connected": bool(st.get("connected", False)),
            "balance": st.get("balance"),
            "equity": st.get("equity"),
            "free_margin": st.get("margin_free"),
            "currency": st.get("currency"),
            "micro_mode": st.get("micro_mode"),
            "micro_bucket": st.get("micro_balance_bucket"),
            "autopilot": {
                "risk_gate": (autop.get("risk_gate", {}) or {}),
                "journal": (autop.get("journal", {}) or {}),
                "calibration": (autop.get("calibration", {}) or {}),
            },
            "walkforward": wf,
        }
        if isinstance(existing_runtime.get("policy_draft"), dict):
            runtime["policy_draft"] = dict(existing_runtime.get("policy_draft") or {})
        now = _iso(_utc_now())
        with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO mt5_orchestrator_accounts(account_key, account_login, account_server, status, created_at, updated_at, policy_json, runtime_json)
                    VALUES (?, ?, ?, 'active', ?, ?, ?, ?)
                    ON CONFLICT(account_key) DO UPDATE SET
                        account_login=excluded.account_login,
                        account_server=excluded.account_server,
                        updated_at=excluded.updated_at,
                        policy_json=COALESCE(mt5_orchestrator_accounts.policy_json, excluded.policy_json),
                        runtime_json=excluded.runtime_json
                    """,
                    (
                        account_key,
                        _safe_int(st.get("account_login", 0), 0),
                        str(st.get("account_server", "") or ""),
                        now,
                        now,
                        json.dumps(policy, ensure_ascii=True, separators=(",", ":")),
                        json.dumps(runtime, ensure_ascii=True, separators=(",", ":")),
                    ),
                )
                conn.commit()
        return {"ok": True, "account_key": account_key, "walkforward_ok": bool(wf.get("ok", False))}

    def get_current_account_policy_draft(self) -> dict:
        st = mt5_executor.status()
        account_key = self._account_key_from_mt5_status(st)
        if not account_key:
            return {"ok": False, "message": "mt5 not connected", "account_key": "", "draft": {}}
        row = self._load_account_row(account_key) or {}
        runtime = dict(row.get("runtime", {}) or {})
        draft = dict(runtime.get("policy_draft", {}) or {})
        return {"ok": True, "account_key": account_key, "draft": draft}

    def save_current_account_policy_draft(self, draft: dict, *, source: str = "pm_learning") -> dict:
        st = mt5_executor.status()
        account_key = self._account_key_from_mt5_status(st)
        if not account_key:
            return {"ok": False, "message": "mt5 not connected", "account_key": ""}
        row = self._load_account_row(account_key) or {}
        policy = dict(self._default_policy())
        policy.update(dict(row.get("policy", {}) or {}))
        runtime = dict(row.get("runtime", {}) or {})
        payload = dict(draft or {})
        payload["saved_at"] = _iso(_utc_now())
        payload["source"] = str(source or "pm_learning")
        payload["account_key"] = account_key
        runtime["policy_draft"] = payload
        now = _iso(_utc_now())
        with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO mt5_orchestrator_accounts(account_key, account_login, account_server, status, created_at, updated_at, policy_json, runtime_json)
                    VALUES (?, ?, ?, 'active', ?, ?, ?, ?)
                    ON CONFLICT(account_key) DO UPDATE SET
                        account_login=excluded.account_login,
                        account_server=excluded.account_server,
                        updated_at=excluded.updated_at,
                        policy_json=COALESCE(mt5_orchestrator_accounts.policy_json, excluded.policy_json),
                        runtime_json=excluded.runtime_json
                    """,
                    (
                        account_key,
                        _safe_int(st.get("account_login", 0), 0),
                        str(st.get("account_server", "") or ""),
                        now,
                        now,
                        json.dumps(policy, ensure_ascii=True, separators=(",", ":")),
                        json.dumps(runtime, ensure_ascii=True, separators=(",", ":")),
                    ),
                )
                conn.commit()
        return {
            "ok": True,
            "account_key": account_key,
            "draft": payload,
            "keys": sorted(list(dict(payload.get("global_overrides", {}) or {}).keys())),
            "regimes": [str(x.get("regime") or "") for x in list(payload.get("regime_overrides", []) or []) if str(x.get("regime") or "")],
        }

    def pre_trade_plan(self, signal, source: str = "") -> ExecutionPlan:
        st0 = mt5_executor.status()
        account_key = self._account_key_from_mt5_status(st0)
        row = self._load_account_row(account_key) or {}
        policy = dict(self._default_policy())
        policy.update(dict(row.get("policy", {}) or {}))
        symbol_policy_meta: dict[str, str] = {}
        try:
            candidates = self._symbol_override_candidates(signal)
        except Exception:
            candidates = []
        try:
            canary_val, canary_reason = self._lookup_symbol_override(candidates, config.get_mt5_canary_force_symbol_overrides())
            if canary_reason:
                symbol_policy_meta["canary_force"] = canary_reason
            if isinstance(canary_val, bool) or canary_val is None:
                if canary_reason:
                    policy["canary_force"] = canary_val
        except Exception:
            pass
        try:
            fixed_mult, fixed_reason = self._lookup_symbol_override(candidates, config.get_mt5_risk_multiplier_symbol_overrides())
            if fixed_mult is not None:
                policy["min_risk_multiplier"] = float(fixed_mult)
                policy["max_risk_multiplier"] = float(fixed_mult)
                symbol_policy_meta["risk_multiplier_fixed"] = fixed_reason
        except Exception:
            pass
        try:
            min_mult_ov, min_reason = self._lookup_symbol_override(candidates, config.get_mt5_risk_multiplier_min_symbol_overrides())
            if min_mult_ov is not None:
                policy["min_risk_multiplier"] = float(min_mult_ov)
                symbol_policy_meta["min_risk_multiplier"] = min_reason
        except Exception:
            pass
        try:
            max_mult_ov, max_reason = self._lookup_symbol_override(candidates, config.get_mt5_risk_multiplier_max_symbol_overrides())
            if max_mult_ov is not None:
                policy["max_risk_multiplier"] = float(max_mult_ov)
                symbol_policy_meta["max_risk_multiplier"] = max_reason
        except Exception:
            pass
        gate_overrides = {
            "daily_loss_limit_usd": policy.get("daily_loss_limit_usd"),
            "daily_loss_limit_pct": policy.get("daily_loss_limit_pct"),
            "max_consecutive_losses": policy.get("max_consecutive_losses"),
            "loss_cooldown_min": policy.get("loss_cooldown_min"),
            "max_rejections_1h": policy.get("max_rejections_1h"),
        }
        gate = mt5_autopilot_core.pre_trade_gate(signal, source=source, policy_overrides=gate_overrides)
        if not gate.allow:
            return ExecutionPlan(
                allow=False,
                status=gate.status,
                reason=gate.reason,
                account_key=gate.account_key,
                canary_mode=True,
                risk_multiplier=max(0.1, _safe_float(getattr(config, "MT5_ADAPTIVE_SIZING_CANARY_MULT", 0.35), 0.35)),
                gate_snapshot=gate.snapshot,
                policy=policy,
            )

        # Sync lightweight account runtime snapshot to keep multi-account table fresh.
        try:
            self.sync_current_account()
        except Exception:
            pass

        account_key = str(gate.account_key or account_key or "")
        row = self._load_account_row(account_key) or row
        policy = dict(self._default_policy())
        policy.update(dict(row.get("policy", {}) or {}))
        try:
            canary_val, canary_reason = self._lookup_symbol_override(candidates, config.get_mt5_canary_force_symbol_overrides())
            if canary_reason:
                symbol_policy_meta["canary_force"] = canary_reason
            if isinstance(canary_val, bool) or canary_val is None:
                if canary_reason:
                    policy["canary_force"] = canary_val
        except Exception:
            pass
        try:
            fixed_mult, fixed_reason = self._lookup_symbol_override(candidates, config.get_mt5_risk_multiplier_symbol_overrides())
            if fixed_mult is not None:
                policy["min_risk_multiplier"] = float(fixed_mult)
                policy["max_risk_multiplier"] = float(fixed_mult)
                symbol_policy_meta["risk_multiplier_fixed"] = fixed_reason
        except Exception:
            pass
        try:
            min_mult_ov, min_reason = self._lookup_symbol_override(candidates, config.get_mt5_risk_multiplier_min_symbol_overrides())
            if min_mult_ov is not None:
                policy["min_risk_multiplier"] = float(min_mult_ov)
                symbol_policy_meta["min_risk_multiplier"] = min_reason
        except Exception:
            pass
        try:
            max_mult_ov, max_reason = self._lookup_symbol_override(candidates, config.get_mt5_risk_multiplier_max_symbol_overrides())
            if max_mult_ov is not None:
                policy["max_risk_multiplier"] = float(max_mult_ov)
                symbol_policy_meta["max_risk_multiplier"] = max_reason
        except Exception:
            pass

        wf_dec = mt5_walkforward.decision(
            account_key,
            train_days=max(7, _safe_int(getattr(config, "MT5_WF_TRAIN_DAYS", 30), 30)),
            forward_days=max(1, _safe_int(getattr(config, "MT5_WF_FORWARD_DAYS", 7), 7)),
        )
        canary_force = policy.get("canary_force", None)
        canary_mode = bool(wf_dec.canary_mode)
        if isinstance(canary_force, bool):
            canary_mode = bool(canary_force)
        mult = float(wf_dec.risk_multiplier)

        min_mult = max(0.1, _safe_float(policy.get("min_risk_multiplier", 0.25), 0.25))
        max_mult = max(min_mult, _safe_float(policy.get("max_risk_multiplier", 1.25), 1.25))
        mult = max(min_mult, min(max_mult, mult))

        return ExecutionPlan(
            allow=True,
            status="allowed",
            reason=("canary_mode" if canary_mode else "walkforward_pass"),
            account_key=account_key,
            canary_mode=canary_mode,
            risk_multiplier=mult,
            gate_snapshot=gate.snapshot,
            walkforward={
                "reason": wf_dec.canary_reason,
                "train_trades": wf_dec.train_trades,
                "forward_trades": wf_dec.forward_trades,
                "forward_win_rate": wf_dec.forward_win_rate,
                "forward_mae": wf_dec.forward_mae,
                "symbol_policy_overrides": symbol_policy_meta,
            },
            policy=policy,
        )

    def status(self) -> dict:
        st = mt5_executor.status()
        account_key = self._account_key_from_mt5_status(st)
        current = self._load_account_row(account_key) if account_key else None
        total_accounts = 0
        with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute("SELECT COUNT(*) FROM mt5_orchestrator_accounts").fetchone()
                total_accounts = _safe_int(row[0] if row else 0, 0)
        wf = mt5_walkforward.build_report(
            account_key,
            train_days=max(7, _safe_int(getattr(config, "MT5_WF_TRAIN_DAYS", 30), 30)),
            forward_days=max(1, _safe_int(getattr(config, "MT5_WF_FORWARD_DAYS", 7), 7)),
        ) if account_key else {"ok": False, "error": "mt5 not connected"}
        plan = self.pre_trade_plan(signal=None, source="status") if bool(getattr(config, "MT5_ENABLED", False)) else None
        return {
            "enabled": bool(getattr(config, "MT5_AUTOPILOT_ENABLED", True)),
            "db_path": str(self.db_path),
            "accounts_total": total_accounts,
            "current_account_key": account_key,
            "current_account": current or {},
            "current_policy": ({} if not current else dict(self._default_policy()) | dict((current or {}).get("policy", {}) or {})),
            "walkforward": wf,
            "execution_plan_preview": (None if plan is None else {
                "allow": plan.allow,
                "status": plan.status,
                "reason": plan.reason,
                "canary_mode": plan.canary_mode,
                "risk_multiplier": round(float(plan.risk_multiplier), 4),
                "walkforward": plan.walkforward or {},
            }),
        }


mt5_orchestrator = MT5Orchestrator()
