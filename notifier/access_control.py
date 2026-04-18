"""
notifier/access_control.py
Subscription and entitlement middleware for Telegram bot access control.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from config import config


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


@dataclass
class AccessDecision:
    allowed: bool
    reason: str
    user: dict
    quota_remaining: Optional[int] = None
    quota_limit: Optional[int] = None
    quota_used: Optional[int] = None


class AccessManager:
    def __init__(self):
        db_default = Path(__file__).resolve().parent.parent / "data" / "access_control.db"
        self.db_path = Path(getattr(config, "ACCESS_DB_PATH", "") or db_default)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

        self.trial_days = int(getattr(config, "TRIAL_DAYS", 7))
        self.plan_limits = {
            "trial": int(getattr(config, "PLAN_TRIAL_DAILY_LIMIT", 12)),
            "a": int(getattr(config, "PLAN_A_DAILY_LIMIT", 30)),
            "b": int(getattr(config, "PLAN_B_DAILY_LIMIT", 120)),
            "c": int(getattr(config, "PLAN_C_DAILY_LIMIT", 500)),
        }

        common = {
            "start", "help", "status", "scan_gold", "scan_fx", "markets", "gold_overview",
            "calendar", "macro", "macro_report", "macro_weights", "signal_dashboard", "signal_monitor",
            "monitor_sub", "monitor_unsub", "monitor_status",
            "us_open_guard_status", "tz", "plan", "upgrade",
            "scalping_status", "scalping_scan", "scalping_logic",
        }
        self.plan_features = {
            "trial": set(common) | {"scan_us", "scan_us_open", "scan_vi", "scan_vi_buffett", "scan_vi_turnaround", "scan_thai_vi", "monitor_us", "us_open_report", "us_open_dashboard"},
            "a": set(common) | {"scan_crypto"},
            "b": set(common) | {"scan_crypto", "scan_stocks", "scan_thai", "scan_thai_vi", "scan_us_open", "scan_vi", "scan_vi_buffett", "scan_vi_turnaround", "monitor_us", "us_open_report", "us_open_dashboard", "research"},
            "c": set(common)
            | {
                "scan_crypto",
                "scan_stocks",
                "scan_thai",
                "scan_thai_vi",
                "scan_us_open",
                "scan_vi",
                "scan_vi_buffett",
                "scan_vi_turnaround",
                "monitor_us",
                "us_open_report",
                "us_open_dashboard",
                "research",
                "scan_all",
                "scan_us",
                "mt5_status",
                "mt5_history",
                "mt5_autopilot",
                "mt5_walkforward",
                "mt5_manage",
                "mt5_affordable",
                "mt5_exec_reasons",
                "mt5_pm_learning",
                "mt5_plan",
                "mt5_policy",
                "run",
                "mt5_backtest",
                "mt5_train",
                "scalping_on",
                "scalping_off",
            },
        }

        # Commands that never consume daily quota.
        self.non_metered = {
            "start",
            "help",
            "tz",
            "plan",
            "upgrade",
            "scalping_status",
            "signal_monitor",
            "monitor_sub",
            "monitor_unsub",
            "monitor_status",
            "signal_filter",
            "show_only",
            "show_add",
            "show_clear",
            "show_all",
        }
        self.admin_only = {
            "grant", "revoke", "setplan", "block", "unblock",
            "stock_mt5_filter", "admin_add", "admin_del", "admin_list", "user_list",
            "scalping_on", "scalping_off",
        }
        self.ai_api_commands = {"research"}
        self.mt5_sensitive_commands = {cmd for feats in self.plan_features.values() for cmd in feats if str(cmd).startswith("mt5_")}
        if bool(getattr(config, "TRIAL_NO_AI_ALL", False)):
            self._enable_trial_non_ai_all()

    @staticmethod
    def _symbol_aliases(symbol: str) -> set[str]:
        s = str(symbol or "").strip().upper()
        if not s:
            return set()
        out = {s}
        if "/" in s:
            base = s.split("/", 1)[0]
            out.add(f"{base}/USDT")
            out.add(f"{base}USD")
        elif s.endswith("USDT"):
            base = s[:-4]
            out.add(f"{base}/USDT")
            out.add(f"{base}USD")
        elif s.endswith("USD"):
            base = s[:-3]
            out.add(f"{base}/USDT")
            out.add(f"{base}USD")
        return {x for x in out if x}

    @staticmethod
    def _normalize_signal_symbol_token(symbol: str) -> str:
        s = str(symbol or "").strip().upper().replace(" ", "")
        if not s:
            return ""
        if s in {"GOLD", "XAU"}:
            return "XAUUSD"
        if s.endswith("USDT") and "/" not in s and len(s) > 4:
            return f"{s[:-4]}/USDT"
        return s

    @staticmethod
    def _normalize_monitor_symbol_token(symbol: str) -> str:
        s = AccessManager._normalize_signal_symbol_token(symbol)
        if not s:
            return ""
        if s in {"GOLD", "XAU"}:
            return "XAUUSD"
        if s == "XAUUSD":
            return "XAUUSD"
        if s.endswith("/USDT") and len(s) > 5:
            return f"{s[:-5]}USD"
        if s.endswith("USDT") and len(s) > 4:
            return f"{s[:-4]}USD"
        if s.endswith("USD") and len(s) > 3:
            return s
        return s

    @classmethod
    def _expand_signal_filter_aliases(cls, symbols: list[str] | set[str] | tuple[str, ...]) -> set[str]:
        out: set[str] = set()
        for raw in (symbols or []):
            token = cls._normalize_signal_symbol_token(str(raw or ""))
            if not token:
                continue
            out.add(token)
            out.update(cls._symbol_aliases(token))

            base = ""
            if "/" in token:
                base = token.split("/", 1)[0]
            elif token.endswith("USDT") and len(token) > 4:
                base = token[:-4]
            elif token.endswith("USD") and len(token) > 3:
                base = token[:-3]
            elif token.isalpha() and 2 <= len(token) <= 8:
                base = token
            if base:
                out.add(base)
                out.add(f"{base}/USDT")
                out.add(f"{base}USD")
            if token == "XAUUSD":
                out.update({"XAU", "GOLD"})
        return {x for x in out if x}

    def _enable_trial_non_ai_all(self) -> None:
        """Expand trial plan to all non-AI, non-admin, non-MT5 commands (safe scan/report mode)."""
        universe: set[str] = set()
        for feats in self.plan_features.values():
            universe.update(set(feats or set()))
        excludes = set(self.admin_only) | set(self.ai_api_commands) | set(self.mt5_sensitive_commands)
        self.plan_features["trial"] = set(self.plan_features.get("trial", set())) | {cmd for cmd in universe if cmd not in excludes}

    def _db_admin_ids(self) -> set[int]:
        out: set[int] = set()
        with self._connect() as conn:
            rows = conn.execute("SELECT user_id FROM admin_users WHERE status = 'active'").fetchall()
        for r in rows or []:
            try:
                out.add(int(r[0]))
            except Exception:
                continue
        return out

    def get_admin_ids(self) -> set[int]:
        return set(config.get_admin_ids()) | self._db_admin_ids()

    def is_admin_user(self, user_id: int) -> bool:
        try:
            uid = int(user_id)
        except Exception:
            return False
        return uid in self.get_admin_ids()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path), timeout=10)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id INTEGER PRIMARY KEY,
                    plan TEXT NOT NULL,
                    status TEXT NOT NULL,
                    starts_at TEXT,
                    expires_at TEXT,
                    trial_used INTEGER NOT NULL DEFAULT 0,
                    daily_cmd_limit INTEGER,
                    notes TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_usage (
                    user_id INTEGER NOT NULL,
                    usage_date TEXT NOT NULL,
                    cmd_count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, usage_date)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS payment_events (
                    provider TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    user_id INTEGER,
                    plan TEXT,
                    days INTEGER,
                    amount REAL,
                    currency TEXT,
                    status TEXT NOT NULL,
                    payload TEXT,
                    created_at TEXT NOT NULL,
                    applied_at TEXT,
                    note TEXT DEFAULT '',
                    PRIMARY KEY (provider, event_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_users (
                    user_id INTEGER PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'active',
                    notes TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_preferences (
                    user_id INTEGER PRIMARY KEY,
                    preferred_language TEXT,
                    metadata_json TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_users (
                    user_id INTEGER PRIMARY KEY,
                    chat_id INTEGER,
                    username TEXT DEFAULT '',
                    first_name TEXT DEFAULT '',
                    last_name TEXT DEFAULT '',
                    is_bot INTEGER NOT NULL DEFAULT 0,
                    chat_type TEXT DEFAULT '',
                    last_seen_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_telegram_users_username ON telegram_users(username)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_telegram_users_updated_at ON telegram_users(updated_at)")
            conn.commit()

    def _row_to_user(self, row) -> dict:
        if row is None:
            return {}
        return {
            "user_id": int(row[0]),
            "plan": str(row[1]),
            "status": str(row[2]),
            "starts_at": row[3] or "",
            "expires_at": row[4] or "",
            "trial_used": int(row[5] or 0),
            "daily_cmd_limit": None if row[6] is None else int(row[6]),
            "notes": row[7] or "",
            "created_at": row[8] or "",
            "updated_at": row[9] or "",
        }

    def _get_user(self, user_id: int) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT user_id, plan, status, starts_at, expires_at, trial_used,
                       daily_cmd_limit, notes, created_at, updated_at
                FROM subscriptions WHERE user_id = ?
                """,
                (int(user_id),),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_user(row)

    def _create_trial_user(self, user_id: int) -> dict:
        now = _utc_now()
        start = _iso(now)
        expires = _iso(now + timedelta(days=self.trial_days))
        daily_limit = self.plan_limits["trial"]
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO subscriptions
                (user_id, plan, status, starts_at, expires_at, trial_used, daily_cmd_limit, notes, created_at, updated_at)
                VALUES (?, 'trial', 'active', ?, ?, 1, ?, '', ?, ?)
                """,
                (int(user_id), start, expires, int(daily_limit), start, start),
            )
            conn.commit()
        return self._get_user(user_id) or {}

    def ensure_user(self, user_id: int, is_admin: bool = False) -> dict:
        if is_admin:
            now = _iso(_utc_now())
            return {
                "user_id": int(user_id),
                "plan": "owner",
                "status": "active",
                "starts_at": now,
                "expires_at": "",
                "trial_used": 1,
                "daily_cmd_limit": None,
                "notes": "admin override",
                "created_at": now,
                "updated_at": now,
            }
        user = self._get_user(user_id)
        if user:
            return user
        return self._create_trial_user(user_id)

    def _is_expired(self, user: dict) -> bool:
        if str(user.get("status", "")).lower() not in {"active", "trial"}:
            return True
        plan = str(user.get("plan", "")).lower()
        if plan in {"owner", "admin"}:
            return False
        exp = _parse_iso(user.get("expires_at"))
        if exp is None:
            return False
        return _utc_now() > exp

    def _usage_today(self, user_id: int) -> int:
        day = _utc_now().strftime("%Y-%m-%d")
        with self._connect() as conn:
            row = conn.execute(
                "SELECT cmd_count FROM daily_usage WHERE user_id = ? AND usage_date = ?",
                (int(user_id), day),
            ).fetchone()
        return int(row[0]) if row else 0

    def _increment_usage(self, user_id: int) -> int:
        day = _utc_now().strftime("%Y-%m-%d")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO daily_usage (user_id, usage_date, cmd_count)
                VALUES (?, ?, 1)
                ON CONFLICT(user_id, usage_date) DO UPDATE SET cmd_count = cmd_count + 1
                """,
                (int(user_id), day),
            )
            row = conn.execute(
                "SELECT cmd_count FROM daily_usage WHERE user_id = ? AND usage_date = ?",
                (int(user_id), day),
            ).fetchone()
            conn.commit()
        return int(row[0]) if row else 0

    def _has_feature(self, plan: str, command: str) -> bool:
        p = str(plan or "").lower()
        cmd = str(command or "").lower()
        if p in {"owner", "admin"}:
            return True
        return cmd in self.plan_features.get(p, set())

    def check_and_consume(self, user_id: int, command: str, is_admin: bool = False) -> AccessDecision:
        cmd = str(command or "").lower()
        user = self.ensure_user(user_id, is_admin=is_admin)

        if is_admin:
            return AccessDecision(True, "admin_bypass", user, None, None, None)

        if cmd in self.admin_only:
            return AccessDecision(False, "admin_only", user)

        # Always allow non-metered self-service commands (plan/help/upgrade),
        # even when subscription has expired.
        if cmd in self.non_metered:
            return AccessDecision(True, "ok", user, None, user.get("daily_cmd_limit"), self._usage_today(user_id))

        if self._is_expired(user):
            return AccessDecision(False, "expired", user)

        plan = str(user.get("plan", "")).lower()
        if not self._has_feature(plan, cmd):
            return AccessDecision(False, "feature_locked", user)

        limit = user.get("daily_cmd_limit")
        if limit is None:
            return AccessDecision(True, "ok", user, None, limit, None)

        used = self._usage_today(user_id)
        limit_i = int(limit)
        if used >= limit_i:
            return AccessDecision(False, "daily_limit_reached", user, 0, limit_i, used)

        new_used = self._increment_usage(user_id)
        remaining = max(0, limit_i - new_used)
        return AccessDecision(True, "ok", user, remaining, limit_i, new_used)

    def plan_snapshot(self, user_id: int, is_admin: bool = False) -> dict:
        user = self.ensure_user(user_id, is_admin=is_admin)
        if is_admin:
            return {
                "user": user,
                "used_today": 0,
                "remaining_today": None,
                "is_expired": False,
                "features": sorted(list(self.plan_features.get("c", set()))),
            }
        used = self._usage_today(user_id)
        limit = user.get("daily_cmd_limit")
        remaining = None if limit is None else max(0, int(limit) - int(used))
        plan = str(user.get("plan", "")).lower()
        features = sorted(list(self.plan_features.get(plan, set())))
        return {
            "user": user,
            "used_today": used,
            "remaining_today": remaining,
            "is_expired": self._is_expired(user),
            "features": features,
        }

    def list_entitled_user_ids(
        self,
        command: str,
        signal_symbol: str = "",
        signal_symbols: Optional[list[str]] = None,
    ) -> list[int]:
        """
        List active subscriber IDs entitled to receive push messages for a command.
        This does not consume user quota.
        """
        cmd = str(command or "").strip().lower()
        if not cmd:
            return []

        users: list[dict] = []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT user_id, plan, status, starts_at, expires_at, trial_used,
                       daily_cmd_limit, notes, created_at, updated_at
                FROM subscriptions
                """
            ).fetchall()
            users = [self._row_to_user(r) for r in rows]

        ids: set[int] = set()
        trial_crypto_allowed = config.get_trial_crypto_symbols()
        symbol_pool: set[str] = set()
        symbol_pool.update(self._symbol_aliases(signal_symbol))
        for sym in (signal_symbols or []):
            symbol_pool.update(self._symbol_aliases(sym))
        for u in users:
            try:
                uid = int(u.get("user_id", 0) or 0)
            except Exception:
                uid = 0
            if uid <= 0:
                continue
            if self._is_expired(u):
                continue
            plan = str(u.get("plan", "")).lower()
            entitled = False
            if self._has_feature(plan, cmd):
                entitled = True
            # Trial special lane for selected crypto symbols (e.g., BTC/ETH only).
            elif plan == "trial" and cmd == "scan_crypto" and symbol_pool:
                if symbol_pool.intersection(trial_crypto_allowed):
                    entitled = True
            if not entitled:
                continue
            if not self._user_signal_filter_allows(
                uid,
                signal_symbol=signal_symbol,
                signal_symbols=signal_symbols,
            ):
                continue
            ids.add(uid)

        # Admins receive alerts too, but still respect their personal symbol filter if set.
        for admin_uid in self.get_admin_ids():
            try:
                auid = int(admin_uid)
            except Exception:
                continue
            if not self._user_signal_filter_allows(
                auid,
                signal_symbol=signal_symbol,
                signal_symbols=signal_symbols,
            ):
                continue
            ids.add(auid)
        return sorted(ids)

    def grant_plan(
        self,
        user_id: int,
        plan: str,
        days: int,
        status: str = "active",
        note: str = "",
    ) -> dict:
        p = str(plan or "").strip().lower()
        if p not in {"trial", "a", "b", "c"}:
            raise ValueError("plan must be one of: trial, a, b, c")
        day_count = max(1, int(days))
        now = _utc_now()
        start = _iso(now)
        expires = _iso(now + timedelta(days=day_count))
        limit = self.plan_limits.get(p)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscriptions
                (user_id, plan, status, starts_at, expires_at, trial_used, daily_cmd_limit, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    plan=excluded.plan,
                    status=excluded.status,
                    starts_at=excluded.starts_at,
                    expires_at=excluded.expires_at,
                    trial_used=1,
                    daily_cmd_limit=excluded.daily_cmd_limit,
                    notes=excluded.notes,
                    updated_at=excluded.updated_at
                """,
                (int(user_id), p, status, start, expires, int(limit), note[:240], start, start),
            )
            conn.commit()
        return self._get_user(user_id) or {}

    def apply_payment_upgrade(
        self,
        provider: str,
        event_id: str,
        user_id: int,
        plan: str,
        days: int,
        amount: float = 0.0,
        currency: str = "",
        payload: Optional[dict] = None,
        note: str = "",
    ) -> dict:
        """
        Idempotent paid-upgrade applier. Duplicate provider+event_id will not re-apply.
        Extension rule:
          new_expiry = max(now, current_expiry) + days
        """
        provider_key = str(provider or "").strip().lower()
        eid = str(event_id or "").strip()
        if not provider_key:
            raise ValueError("provider is required")
        if not eid:
            raise ValueError("event_id is required")
        p = str(plan or "").strip().lower()
        if p not in {"trial", "a", "b", "c"}:
            raise ValueError("plan must be one of: trial, a, b, c")
        day_count = max(1, int(days))

        now_dt = _utc_now()
        now_iso = _iso(now_dt)
        payload_text = ""
        if payload is not None:
            try:
                payload_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))[:20000]
            except Exception:
                payload_text = str(payload)[:20000]

        with self._lock:
            with self._connect() as conn:
                try:
                    conn.execute(
                        """
                        INSERT INTO payment_events
                        (provider, event_id, user_id, plan, days, amount, currency, status, payload, created_at, note)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'received', ?, ?, ?)
                        """,
                        (
                            provider_key,
                            eid,
                            int(user_id),
                            p,
                            int(day_count),
                            float(amount or 0.0),
                            str(currency or "").upper()[:12],
                            payload_text,
                            now_iso,
                            str(note or "")[:240],
                        ),
                    )
                except sqlite3.IntegrityError:
                    row = conn.execute(
                        """
                        SELECT status, applied_at
                        FROM payment_events
                        WHERE provider = ? AND event_id = ?
                        """,
                        (provider_key, eid),
                    ).fetchone()
                    user = self._get_user(user_id) or {}
                    return {
                        "ok": True,
                        "duplicate": True,
                        "applied": False,
                        "provider": provider_key,
                        "event_id": eid,
                        "status": (row[0] if row else "duplicate"),
                        "applied_at": (row[1] if row else ""),
                        "user": user,
                    }

                existing = self._get_user(user_id)
                base_dt = now_dt
                starts_at = now_iso
                if existing:
                    starts_at = existing.get("starts_at") or now_iso
                    exp = _parse_iso(existing.get("expires_at"))
                    if exp and exp > now_dt:
                        base_dt = exp
                new_expiry = _iso(base_dt + timedelta(days=day_count))
                limit = int(self.plan_limits.get(p, self.plan_limits["trial"]))

                conn.execute(
                    """
                    INSERT INTO subscriptions
                    (user_id, plan, status, starts_at, expires_at, trial_used, daily_cmd_limit, notes, created_at, updated_at)
                    VALUES (?, ?, 'active', ?, ?, 1, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        plan=excluded.plan,
                        status='active',
                        starts_at=CASE
                            WHEN subscriptions.starts_at IS NULL OR subscriptions.starts_at = '' THEN excluded.starts_at
                            ELSE subscriptions.starts_at
                        END,
                        expires_at=excluded.expires_at,
                        trial_used=1,
                        daily_cmd_limit=excluded.daily_cmd_limit,
                        notes=excluded.notes,
                        updated_at=excluded.updated_at
                    """,
                    (
                        int(user_id),
                        p,
                        starts_at,
                        new_expiry,
                        limit,
                        str(note or "")[:240],
                        now_iso,
                        now_iso,
                    ),
                )

                conn.execute(
                    """
                    UPDATE payment_events
                    SET status = 'applied', applied_at = ?, note = ?
                    WHERE provider = ? AND event_id = ?
                    """,
                    (now_iso, str(note or "")[:240], provider_key, eid),
                )
                conn.commit()

        return {
            "ok": True,
            "duplicate": False,
            "applied": True,
            "provider": provider_key,
            "event_id": eid,
            "status": "applied",
            "applied_at": now_iso,
            "user": self._get_user(user_id) or {},
        }

    def set_status(self, user_id: int, status: str, note: str = "") -> dict:
        now = _iso(_utc_now())
        user = self._get_user(user_id) or self._create_trial_user(user_id)
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE subscriptions
                SET status = ?, notes = ?, updated_at = ?
                WHERE user_id = ?
                """,
                (str(status), note[:240], now, int(user_id)),
            )
            conn.commit()
        return self._get_user(user_id) or user

    def set_admin_role(self, user_id: int, enabled: bool, note: str = "") -> dict:
        uid = int(user_id)
        now = _iso(_utc_now())
        status = "active" if bool(enabled) else "revoked"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO admin_users (user_id, status, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    status=excluded.status,
                    notes=CASE WHEN excluded.notes<>'' THEN excluded.notes ELSE admin_users.notes END,
                    updated_at=excluded.updated_at
                """,
                (uid, status, str(note or ""), now, now),
            )
            conn.commit()
        return self.get_admin_role(uid)

    def get_admin_role(self, user_id: int) -> dict:
        uid = int(user_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT user_id, status, notes, created_at, updated_at FROM admin_users WHERE user_id = ?",
                (uid,),
            ).fetchone()
        if not row:
            return {
                "user_id": uid,
                "status": "active" if uid in config.get_admin_ids() else "none",
                "source": "config" if uid in config.get_admin_ids() else "none",
                "notes": "",
                "created_at": "",
                "updated_at": "",
            }
        return {
            "user_id": int(row[0]),
            "status": str(row[1] or "none"),
            "source": "db",
            "notes": str(row[2] or ""),
            "created_at": str(row[3] or ""),
            "updated_at": str(row[4] or ""),
        }

    def list_admin_roles(self) -> list[dict]:
        rows_out: list[dict] = []
        config_ids = set(config.get_admin_ids())
        for uid in sorted(config_ids):
            rows_out.append({"user_id": int(uid), "status": "active", "source": "config", "notes": "", "created_at": "", "updated_at": ""})
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT user_id, status, notes, created_at, updated_at FROM admin_users ORDER BY updated_at DESC, user_id ASC"
            ).fetchall()
        seen = {int(x["user_id"]) for x in rows_out}
        for r in rows or []:
            uid = int(r[0])
            if uid in seen:
                # Prefer showing config source if static owner/admin; still show DB state in notes if revoked.
                if str(r[1] or "").lower() != "active":
                    rows_out.append({"user_id": uid, "status": str(r[1] or ""), "source": "db", "notes": str(r[2] or ""), "created_at": str(r[3] or ""), "updated_at": str(r[4] or "")})
                continue
            rows_out.append({"user_id": uid, "status": str(r[1] or ""), "source": "db", "notes": str(r[2] or ""), "created_at": str(r[3] or ""), "updated_at": str(r[4] or "")})
        return rows_out

    def record_telegram_user_activity(
        self,
        user_id: int,
        chat_id: Optional[int] = None,
        username: str = "",
        first_name: str = "",
        last_name: str = "",
        is_bot: bool = False,
        chat_type: str = "",
    ) -> None:
        uid = int(user_id)
        cid = None if chat_id is None else int(chat_id)
        now = _iso(_utc_now())
        uname = str(username or "").strip().lstrip("@")[:64]
        fn = str(first_name or "").strip()[:120]
        ln = str(last_name or "").strip()[:120]
        ctype = str(chat_type or "").strip()[:32]
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT created_at FROM telegram_users WHERE user_id = ?",
                    (uid,),
                ).fetchone()
                created_at = str(row[0] or now) if row else now
                conn.execute(
                    """
                    INSERT INTO telegram_users
                    (user_id, chat_id, username, first_name, last_name, is_bot, chat_type, last_seen_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        chat_id=COALESCE(excluded.chat_id, telegram_users.chat_id),
                        username=CASE WHEN excluded.username<>'' THEN excluded.username ELSE telegram_users.username END,
                        first_name=CASE WHEN excluded.first_name<>'' THEN excluded.first_name ELSE telegram_users.first_name END,
                        last_name=CASE WHEN excluded.last_name<>'' THEN excluded.last_name ELSE telegram_users.last_name END,
                        is_bot=excluded.is_bot,
                        chat_type=CASE WHEN excluded.chat_type<>'' THEN excluded.chat_type ELSE telegram_users.chat_type END,
                        last_seen_at=excluded.last_seen_at,
                        updated_at=excluded.updated_at
                    """,
                    (
                        uid,
                        cid,
                        uname,
                        fn,
                        ln,
                        1 if bool(is_bot) else 0,
                        ctype,
                        now,
                        created_at,
                        now,
                    ),
                )
                conn.commit()

    @staticmethod
    def _telegram_user_row_to_dict(row) -> dict:
        if not row:
            return {}
        return {
            "user_id": int(row[0]),
            "chat_id": None if row[1] is None else int(row[1]),
            "username": str(row[2] or ""),
            "first_name": str(row[3] or ""),
            "last_name": str(row[4] or ""),
            "is_bot": bool(int(row[5] or 0)),
            "chat_type": str(row[6] or ""),
            "last_seen_at": str(row[7] or ""),
            "updated_at": str(row[8] or ""),
            "plan": str(row[9] or ""),
            "plan_status": str(row[10] or ""),
            "expires_at": str(row[11] or ""),
        }

    def list_known_telegram_users(self, query: str = "", limit: int = 50, include_bots: bool = False) -> list[dict]:
        q = str(query or "").strip()
        lim = max(1, min(200, int(limit or 50)))
        sql = (
            "SELECT t.user_id, t.chat_id, t.username, t.first_name, t.last_name, t.is_bot, t.chat_type, "
            "t.last_seen_at, t.updated_at, s.plan, s.status, s.expires_at "
            "FROM telegram_users t "
            "LEFT JOIN subscriptions s ON s.user_id = t.user_id "
            "WHERE 1=1 "
        )
        params: list[object] = []
        if not include_bots:
            sql += "AND t.is_bot = 0 "
        # Hide synthetic/test rows that have no user identity fields at all.
        sql += "AND (COALESCE(t.username,'')<>'' OR COALESCE(t.first_name,'')<>'' OR COALESCE(t.last_name,'')<>'' OR COALESCE(t.chat_type,'')<>'') "
        if q:
            q_user = q.lstrip("@")
            q_like = f"%{q_user.lower()}%"
            if q_user.isdigit():
                sql += (
                    "AND (CAST(t.user_id AS TEXT) = ? OR lower(t.username) LIKE ? "
                    "OR lower(COALESCE(t.first_name,'') || ' ' || COALESCE(t.last_name,'')) LIKE ?) "
                )
                params.extend([q_user, q_like, q_like])
            else:
                sql += (
                    "AND (lower(t.username) = ? OR lower(t.username) LIKE ? "
                    "OR lower(COALESCE(t.first_name,'') || ' ' || COALESCE(t.last_name,'')) LIKE ?) "
                )
                params.extend([q_user.lower(), q_like, q_like])
        sql += "ORDER BY t.last_seen_at DESC, t.updated_at DESC, t.user_id ASC LIMIT ?"
        params.append(lim)
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._telegram_user_row_to_dict(r) for r in (rows or [])]

    def resolve_known_telegram_user(self, ref: str) -> Optional[dict]:
        raw = str(ref or "").strip()
        if not raw:
            return None
        token = raw.lstrip("@").strip()
        if not token:
            return None
        if token.lstrip("-").isdigit():
            uid = int(token)
            rows = self.list_known_telegram_users(query=str(uid), limit=5, include_bots=True)
            for r in rows:
                if int(r.get("user_id", 0) or 0) == uid:
                    r["resolved_by"] = "user_id"
                    return r
            return {"user_id": uid, "username": "", "first_name": "", "last_name": "", "chat_id": None, "resolved_by": "user_id"}

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT t.user_id, t.chat_id, t.username, t.first_name, t.last_name, t.is_bot, t.chat_type,
                       t.last_seen_at, t.updated_at, s.plan, s.status, s.expires_at
                FROM telegram_users t
                LEFT JOIN subscriptions s ON s.user_id = t.user_id
                WHERE lower(t.username) = lower(?)
                ORDER BY t.last_seen_at DESC, t.updated_at DESC
                LIMIT 1
                """,
                (token,),
            ).fetchone()
        if not row:
            return None
        out = self._telegram_user_row_to_dict(row)
        out["resolved_by"] = "username"
        return out

    def get_user_language_preference(self, user_id: int) -> Optional[str]:
        uid = int(user_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT preferred_language FROM user_preferences WHERE user_id = ?",
                (uid,),
            ).fetchone()
        if not row:
            return None
        lang = str(row[0] or "").strip().lower()
        if lang not in {"th", "en", "de"}:
            return None
        return lang

    def _get_user_preferences_row(self, user_id: int):
        uid = int(user_id)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT preferred_language, metadata_json, created_at, updated_at
                FROM user_preferences
                WHERE user_id = ?
                """,
                (uid,),
            ).fetchone()
        return row

    @staticmethod
    def _parse_user_pref_metadata(raw: Optional[str]) -> dict:
        txt = str(raw or "").strip()
        if not txt:
            return {}
        try:
            obj = json.loads(txt)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def _upsert_user_preferences_metadata(self, user_id: int, metadata: dict) -> None:
        uid = int(user_id)
        row = self._get_user_preferences_row(uid)
        now = _iso(_utc_now())
        pref_lang = None
        created_at = now
        if row:
            pref_lang = row[0]
            created_at = str(row[2] or now)
        meta_text = json.dumps(metadata or {}, ensure_ascii=False, separators=(",", ":"))[:4000]
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO user_preferences
                    (user_id, preferred_language, metadata_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        preferred_language=COALESCE(user_preferences.preferred_language, excluded.preferred_language),
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (uid, pref_lang, meta_text, created_at, now),
                )
                conn.commit()

    def set_user_language_preference(
        self,
        user_id: int,
        preferred_language: str,
        metadata: Optional[dict] = None,
    ) -> Optional[str]:
        uid = int(user_id)
        lang = str(preferred_language or "").strip().lower()
        if lang not in {"th", "en", "de"}:
            raise ValueError("preferred_language must be one of: th, en, de")
        now = _iso(_utc_now())
        row = self._get_user_preferences_row(uid)
        merged_meta = self._parse_user_pref_metadata(row[1] if row else "")
        if metadata is not None:
            try:
                if isinstance(metadata, dict):
                    merged_meta.update(metadata)
                else:
                    merged_meta["note"] = str(metadata)
            except Exception:
                merged_meta["note"] = str(metadata)
        meta_text = ""
        try:
            meta_text = json.dumps(merged_meta, ensure_ascii=False, separators=(",", ":"))[:2000]
        except Exception:
            meta_text = "{}"

        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO user_preferences
                    (user_id, preferred_language, metadata_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        preferred_language=excluded.preferred_language,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (uid, lang, meta_text, now, now),
                )
                conn.commit()
        return self.get_user_language_preference(uid)

    def get_user_signal_symbol_filter(self, user_id: int) -> list[str]:
        row = self._get_user_preferences_row(user_id)
        if not row:
            return []
        meta = self._parse_user_pref_metadata(row[1] if len(row) > 1 else "")
        raw = meta.get("signal_symbol_filter", [])
        if not isinstance(raw, list):
            return []
        out: list[str] = []
        seen: set[str] = set()
        for item in raw:
            token = self._normalize_signal_symbol_token(str(item or ""))
            if not token or token in seen:
                continue
            seen.add(token)
            out.append(token)
        return out

    def set_user_signal_symbol_filter(self, user_id: int, symbols: Optional[list[str]]) -> list[str]:
        items = list(symbols or [])
        normalized: list[str] = []
        seen: set[str] = set()
        for item in items:
            token = self._normalize_signal_symbol_token(str(item or ""))
            if not token or token in seen:
                continue
            seen.add(token)
            normalized.append(token)
        row = self._get_user_preferences_row(user_id)
        meta = self._parse_user_pref_metadata(row[1] if row else "")
        if normalized:
            meta["signal_symbol_filter"] = normalized
        else:
            meta.pop("signal_symbol_filter", None)
        self._upsert_user_preferences_metadata(user_id, meta)
        return self.get_user_signal_symbol_filter(user_id)

    def _user_signal_filter_allows(
        self,
        user_id: int,
        signal_symbol: str = "",
        signal_symbols: Optional[list[str]] = None,
    ) -> bool:
        user_filter = self.get_user_signal_symbol_filter(user_id)
        if not user_filter:
            return True
        signal_pool: set[str] = set()
        signal_pool.update(self._symbol_aliases(signal_symbol))
        for sym in (signal_symbols or []):
            signal_pool.update(self._symbol_aliases(sym))
        if not signal_pool:
            return True
        filter_aliases = self._expand_signal_filter_aliases(user_filter)
        signal_aliases = self._expand_signal_filter_aliases(list(signal_pool))
        return bool(filter_aliases.intersection(signal_aliases))

    def user_signal_filter_allows(
        self,
        user_id: int,
        signal_symbol: str = "",
        signal_symbols: Optional[list[str]] = None,
    ) -> bool:
        """Public wrapper for notifier layer to check per-user signal visibility filter."""
        try:
            uid = int(user_id)
        except Exception:
            return True
        return self._user_signal_filter_allows(
            uid,
            signal_symbol=signal_symbol,
            signal_symbols=signal_symbols,
        )

    def get_user_macro_risk_filter(self, user_id: int) -> Optional[str]:
        row = self._get_user_preferences_row(user_id)
        if not row:
            return None
        meta = self._parse_user_pref_metadata(row[1] if len(row) > 1 else "")
        raw = str(meta.get("macro_risk_filter", "") or "").strip()
        if raw not in {"*", "**", "***"}:
            return None
        return raw

    def set_user_macro_risk_filter(self, user_id: int, risk_filter: Optional[str]) -> Optional[str]:
        raw = str(risk_filter or "").strip()
        if raw and raw not in {"*", "**", "***"}:
            raise ValueError("risk_filter must be one of: *, **, ***")
        row = self._get_user_preferences_row(user_id)
        meta = self._parse_user_pref_metadata(row[1] if row else "")
        if raw:
            meta["macro_risk_filter"] = raw
        else:
            meta.pop("macro_risk_filter", None)
        self._upsert_user_preferences_metadata(user_id, meta)
        return self.get_user_macro_risk_filter(user_id)

    def get_user_news_utc_offset(self, user_id: int) -> Optional[str]:
        row = self._get_user_preferences_row(user_id)
        if not row:
            return None
        meta = self._parse_user_pref_metadata(row[1] if len(row) > 1 else "")
        raw = str(meta.get("news_utc_offset", "") or "").strip().upper()
        if not raw:
            return None
        if len(raw) == 6 and raw[0] in {"+", "-"} and raw[3] == ":":
            try:
                hh = int(raw[1:3])
                mm = int(raw[4:6])
                if 0 <= hh <= 14 and 0 <= mm < 60:
                    return raw
            except Exception:
                return None
        return None

    def set_user_news_utc_offset(self, user_id: int, utc_offset: Optional[str]) -> Optional[str]:
        raw = str(utc_offset or "").strip().upper()
        if raw:
            if not (len(raw) == 6 and raw[0] in {"+", "-"} and raw[3] == ":"):
                raise ValueError("utc_offset must be in format ±HH:MM")
            try:
                hh = int(raw[1:3])
                mm = int(raw[4:6])
            except Exception as e:
                raise ValueError("utc_offset must be in format ±HH:MM") from e
            if not (0 <= hh <= 14 and 0 <= mm < 60):
                raise ValueError("utc_offset out of supported range")

        row = self._get_user_preferences_row(user_id)
        meta = self._parse_user_pref_metadata(row[1] if row else "")
        if raw:
            meta["news_utc_offset"] = raw
        else:
            meta.pop("news_utc_offset", None)
        self._upsert_user_preferences_metadata(user_id, meta)
        return self.get_user_news_utc_offset(user_id)

    def pricing_text(self) -> str:
        upgrade_url = (getattr(config, "BILLING_UPGRADE_URL", "") or "").strip()
        upgrade_line = f"Upgrade link: {upgrade_url}\n" if upgrade_url else ""
        trial_days = int(getattr(config, "TRIAL_DAYS", 7))
        trial_line = (
            f"TRIAL ({trial_days} days): All non-AI scans/reports (safe mode, no MT5/private commands)\n"
            if bool(getattr(config, "TRIAL_NO_AI_ALL", False))
            else f"TRIAL ({trial_days} days): Gold + US open assistance + BTC/ETH alerts + economic/macro risk + VI scan\n"
        )
        return (
            "Dexter Access Plans\n"
            + trial_line
            + "A: Adds crypto scan\n"
            + "B: Adds stocks + US open monitor + research\n"
            + "C: Adds full scan + MT5 status tools\n\n"
            + upgrade_line
            + "Use /plan to check your current entitlement.\n"
            + "For upgrade/payment, contact admin with your Telegram user ID."
        )


access_manager = AccessManager()
