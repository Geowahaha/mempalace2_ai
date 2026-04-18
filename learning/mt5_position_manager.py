"""
learning/mt5_position_manager.py
Autonomous position manager for MT5:
- Break-even stop
- Partial TP
- Trailing stop (R-based)
- Time-stop exit

Risk-first defaults target micro accounts and avoid repeated modifications via
SQLite state keyed by account + position ticket.
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
from learning.mt5_adaptive_trade_planner import mt5_adaptive_trade_planner


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


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(v)))


@dataclass
class PMActionResult:
    ok: bool
    action: str
    message: str
    ticket: int
    symbol: str


class MT5PositionManager:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        cfg = str(getattr(config, "MT5_POSITION_MANAGER_DB_PATH", "") or "").strip()
        self.db_path = Path(db_path or cfg or (data_dir / "mt5_position_manager.db"))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    @property
    def enabled(self) -> bool:
        return bool(getattr(config, "MT5_POSITION_MANAGER_ENABLED", True)) and bool(getattr(config, "MT5_ENABLED", False))

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
                    CREATE TABLE IF NOT EXISTS mt5_position_mgr_state (
                        account_key TEXT NOT NULL,
                        position_ticket INTEGER NOT NULL,
                        symbol TEXT NOT NULL,
                        state_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (account_key, position_ticket)
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS mt5_position_mgr_actions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account_key TEXT NOT NULL,
                        position_ticket INTEGER NOT NULL,
                        symbol TEXT NOT NULL,
                        action TEXT NOT NULL,
                        action_at TEXT NOT NULL,
                        action_ts INTEGER NOT NULL,
                        r_now REAL,
                        age_min REAL,
                        spread_pct REAL,
                        old_sl REAL,
                        new_sl REAL,
                        requested_close_volume REAL,
                        executed_close_volume REAL,
                        trigger TEXT,
                        adaptive_pm_json TEXT,
                        rules_json TEXT,
                        outcome_label TEXT,
                        outcome_reason TEXT,
                        outcome_pnl REAL,
                        outcome_closed_at TEXT,
                        outcome_close_ts INTEGER,
                        outcome_position_id INTEGER,
                        resolved INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pm_actions_account_open ON mt5_position_mgr_actions(account_key, resolved, action_ts)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pm_actions_symbol ON mt5_position_mgr_actions(account_key, symbol, action_ts)"
                )
                conn.commit()

    @staticmethod
    def _account_key(st: dict) -> str:
        login = _safe_int(st.get("account_login", 0), 0)
        server = str(st.get("account_server", "") or "")
        return f"{server}|{login}" if login and server else ""

    def _get_state(self, account_key: str, ticket: int) -> dict:
        if not account_key or not ticket:
            return {}
        with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    "SELECT state_json FROM mt5_position_mgr_state WHERE account_key=? AND position_ticket=?",
                    (account_key, int(ticket)),
                ).fetchone()
        if not row or not row[0]:
            return {}
        try:
            return dict(json.loads(row[0]) or {})
        except Exception:
            return {}

    def _set_state(self, account_key: str, ticket: int, symbol: str, patch: dict) -> None:
        if not account_key or not ticket:
            return
        now = _iso(_utc_now())
        state = self._get_state(account_key, ticket)
        state.update(dict(patch or {}))
        with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO mt5_position_mgr_state(account_key, position_ticket, symbol, state_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(account_key, position_ticket) DO UPDATE SET
                        symbol=excluded.symbol,
                        state_json=excluded.state_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        account_key,
                        int(ticket),
                        str(symbol or ""),
                        json.dumps(state, ensure_ascii=True, separators=(",", ":")),
                        now,
                        now,
                    ),
                )
                conn.commit()

    def _cleanup_closed_states(self, account_key: str, live_tickets: set[int]) -> int:
        if not account_key:
            return 0
        removed = 0
        with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    "SELECT position_ticket FROM mt5_position_mgr_state WHERE account_key=?",
                    (account_key,),
                ).fetchall()
                to_delete = [int(r[0]) for r in rows if _safe_int(r[0], 0) not in live_tickets]
                for t in to_delete:
                    conn.execute(
                        "DELETE FROM mt5_position_mgr_state WHERE account_key=? AND position_ticket=?",
                        (account_key, int(t)),
                    )
                conn.commit()
                removed = len(to_delete)
        return removed

    def _record_action_learning(self, account_key: str, action_row: dict, pos_rules: Optional[dict] = None) -> None:
        if not bool(getattr(config, "MT5_PM_LEARNING_ENABLED", True)):
            return
        if not account_key:
            return
        row = dict(action_row or {})
        if not bool(row.get("ok", False)):
            return
        ticket = _safe_int(row.get("ticket", 0), 0)
        symbol = str(row.get("symbol", "") or "").upper()
        action = str(row.get("action", "") or "").strip().lower()
        if ticket <= 0 or not symbol or not action:
            return
        now = _utc_now()
        adp = dict(row.get("adaptive_pm") or {})
        rules_keep = {}
        for k in (
            "break_even_r", "partial_tp_r", "trail_start_r", "trail_gap_r",
            "time_stop_min", "time_stop_flat_r", "early_risk_trigger_r", "early_risk_sl_r",
            "spread_spike_pct",
        ):
            if pos_rules and k in pos_rules:
                rules_keep[k] = pos_rules.get(k)
        with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO mt5_position_mgr_actions(
                        account_key, position_ticket, symbol, action, action_at, action_ts,
                        r_now, age_min, spread_pct, old_sl, new_sl,
                        requested_close_volume, executed_close_volume, trigger,
                        adaptive_pm_json, rules_json, resolved
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        account_key,
                        int(ticket),
                        symbol,
                        action,
                        _iso(now),
                        int(now.timestamp()),
                        (_safe_float(row.get("r_now"), 0.0) if row.get("r_now") is not None else None),
                        (_safe_float(row.get("age_min"), 0.0) if row.get("age_min") is not None else None),
                        (_safe_float(row.get("spread_pct"), 0.0) if row.get("spread_pct") is not None else None),
                        (_safe_float(row.get("old_sl"), 0.0) if row.get("old_sl") is not None else None),
                        (_safe_float(row.get("new_sl"), 0.0) if row.get("new_sl") is not None else None),
                        (_safe_float(row.get("requested_close_volume"), 0.0) if row.get("requested_close_volume") is not None else None),
                        (_safe_float(row.get("executed_close_volume"), 0.0) if row.get("executed_close_volume") is not None else None),
                        (str(row.get("trigger") or "")[:200] or None),
                        json.dumps(adp, ensure_ascii=True, separators=(",", ":")),
                        json.dumps(rules_keep, ensure_ascii=True, separators=(",", ":")),
                    ),
                )
                conn.commit()

    @staticmethod
    def _pm_outcome_label(reason: str, pnl: float) -> str:
        r = str(reason or "").upper()
        if r == "TP":
            return "tp"
        if r == "SL":
            return "sl"
        if pnl > 0:
            return "positive"
        if pnl < 0:
            return "negative"
        return "flat"

    def sync_learning_outcomes(self, hours: int = 168) -> dict:
        out = {
            "ok": False,
            "enabled": self.enabled,
            "learning_enabled": bool(getattr(config, "MT5_PM_LEARNING_ENABLED", True)),
            "account_key": "",
            "updated": 0,
            "unresolved_before": 0,
            "still_unresolved": 0,
            "closed_rows_seen": 0,
            "history_query_mode": "",
            "error": "",
        }
        if not self.enabled or not bool(getattr(config, "MT5_PM_LEARNING_ENABLED", True)):
            out["error"] = "disabled"
            return out
        st = mt5_executor.status()
        if not bool(st.get("connected", False)):
            out["error"] = str(st.get("error") or "mt5 not connected")
            return out
        account_key = self._account_key(st)
        out["account_key"] = account_key
        if not account_key:
            out["error"] = "missing account key"
            return out

        lookback_h = max(24, int(hours or getattr(config, "MT5_PM_LEARNING_SYNC_HOURS", 168)))
        snap = mt5_executor.closed_trades_snapshot(
            "",
            hours=lookback_h,
            limit=max(20, int(getattr(config, "MT5_PM_LEARNING_MAX_CLOSED_ROWS", 400))),
        )
        if not bool(snap.get("connected", False)):
            out["error"] = str(snap.get("error") or "closed_trades unavailable")
            return out
        out["history_query_mode"] = str(snap.get("history_query_mode", "") or "")
        closed_rows = list(snap.get("closed_trades", []) or [])
        out["closed_rows_seen"] = len(closed_rows)

        now_ts = int(_utc_now().timestamp())
        since_ts = now_ts - int(lookback_h * 3600)
        with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    """
                    SELECT id, position_ticket, symbol, action_ts
                      FROM mt5_position_mgr_actions
                     WHERE account_key=? AND resolved=0 AND action_ts>=?
                     ORDER BY action_ts DESC
                    """,
                    (account_key, since_ts),
                ).fetchall()
                out["unresolved_before"] = len(rows)
                if not rows:
                    out["ok"] = True
                    return out

                by_pos: dict[int, list[dict]] = {}
                by_symbol: dict[str, list[dict]] = {}
                for rec in closed_rows:
                    try:
                        sym = str(rec.get("symbol", "") or "").upper()
                        cts = _safe_int(rec.get("close_time", 0), 0)
                        pid = _safe_int(rec.get("position_id", 0) or 0, 0)
                        item = {
                            "position_id": (pid if pid > 0 else None),
                            "symbol": sym,
                            "close_time": cts,
                            "reason": str(rec.get("reason", "") or ""),
                            "pnl": _safe_float(rec.get("pnl", 0.0), 0.0),
                            "closed_at_utc": str(rec.get("closed_at_utc", "") or ""),
                        }
                        if pid > 0:
                            by_pos.setdefault(pid, []).append(item)
                        if sym:
                            by_symbol.setdefault(sym, []).append(item)
                    except Exception:
                        continue
                for v in by_pos.values():
                    v.sort(key=lambda x: _safe_int(x.get("close_time", 0), 0))
                for v in by_symbol.values():
                    v.sort(key=lambda x: _safe_int(x.get("close_time", 0), 0))

                max_match_sec = max(3600, int(lookback_h * 3600))
                for rid, pos_ticket, sym, action_ts in rows:
                    rid_i = _safe_int(rid, 0)
                    pt = _safe_int(pos_ticket, 0)
                    sym_u = str(sym or "").upper()
                    ats = _safe_int(action_ts, 0)
                    match = None
                    if pt > 0:
                        for c in by_pos.get(pt, []):
                            cts = _safe_int(c.get("close_time", 0), 0)
                            if cts >= ats and (cts - ats) <= max_match_sec:
                                match = c
                                break
                    if match is None and sym_u:
                        for c in by_symbol.get(sym_u, []):
                            cts = _safe_int(c.get("close_time", 0), 0)
                            if cts >= ats and (cts - ats) <= max_match_sec:
                                match = c
                                break
                    if match is None:
                        continue

                    pnl = _safe_float(match.get("pnl", 0.0), 0.0)
                    reason = str(match.get("reason", "") or "").upper()
                    conn.execute(
                        """
                        UPDATE mt5_position_mgr_actions
                           SET resolved=1,
                               outcome_label=?,
                               outcome_reason=?,
                               outcome_pnl=?,
                               outcome_closed_at=?,
                               outcome_close_ts=?,
                               outcome_position_id=?
                         WHERE id=?
                        """,
                        (
                            self._pm_outcome_label(reason, pnl),
                            reason,
                            pnl,
                            str(match.get("closed_at_utc", "") or ""),
                            _safe_int(match.get("close_time", 0), 0),
                            (_safe_int(match.get("position_id", 0), 0) or None),
                            rid_i,
                        ),
                    )
                    out["updated"] += 1
                conn.commit()
                row = conn.execute(
                    "SELECT COUNT(*) FROM mt5_position_mgr_actions WHERE account_key=? AND resolved=0",
                    (account_key,),
                ).fetchone()
                out["still_unresolved"] = _safe_int(row[0] if row else 0, 0)
        out["ok"] = True
        return out

    def _pm_learning_stats(self, account_key: str, symbol: str, lookback_days: int = 60) -> dict:
        out = {"samples": 0}
        if not bool(getattr(config, "MT5_PM_LEARNING_ENABLED", True)) or not account_key or not symbol:
            return out
        since_ts = int(_utc_now().timestamp()) - max(1, int(lookback_days)) * 86400
        try:
            with self._lock:
                with closing(self._connect()) as conn:
                    rows = conn.execute(
                        """
                        SELECT action, outcome_label, outcome_reason, outcome_pnl
                          FROM mt5_position_mgr_actions
                         WHERE account_key=? AND symbol=? AND resolved=1 AND action_ts>=?
                        """,
                        (account_key, str(symbol or "").upper(), since_ts),
                    ).fetchall()
        except Exception:
            return out
        if not rows:
            return out

        total = 0
        total_pos = 0
        total_neg = 0
        agg: dict[str, dict] = {}
        for action, outcome_label, outcome_reason, outcome_pnl in rows:
            act = str(action or "").strip().lower()
            if not act:
                continue
            total += 1
            pnl = _safe_float(outcome_pnl, 0.0)
            lab = str(outcome_label or "").strip().lower()
            rsn = str(outcome_reason or "").strip().upper()
            is_pos = (lab in {"tp", "positive"}) or (pnl > 0)
            is_neg = (lab in {"sl", "negative"}) or (pnl < 0)
            total_pos += (1 if is_pos else 0)
            total_neg += (1 if is_neg else 0)
            a = agg.setdefault(act, {"samples": 0, "pos": 0, "neg": 0, "tp": 0, "sl": 0})
            a["samples"] += 1
            a["pos"] += (1 if is_pos else 0)
            a["neg"] += (1 if is_neg else 0)
            a["tp"] += (1 if rsn == "TP" else 0)
            a["sl"] += (1 if rsn == "SL" else 0)

        out["samples"] = total
        out["protect_positive_rate"] = round((total_pos / total), 4) if total else None
        out["protect_negative_rate"] = round((total_neg / total), 4) if total else None
        for act in ("breakeven", "trail_sl", "partial_close", "time_stop_close", "early_risk_tighten"):
            a = agg.get(act, {})
            n = _safe_int(a.get("samples", 0), 0)
            out[f"{act}_samples"] = n
            if n > 0:
                out[f"{act}_positive_rate"] = round(_safe_float(a.get("pos", 0), 0) / n, 4)
                out[f"{act}_negative_rate"] = round(_safe_float(a.get("neg", 0), 0) / n, 4)
                out[f"{act}_tp_rate"] = round(_safe_float(a.get("tp", 0), 0) / n, 4)
                out[f"{act}_sl_rate"] = round(_safe_float(a.get("sl", 0), 0) / n, 4)
        return out

    @staticmethod
    def normalize_learning_symbol_filter(symbol: str) -> str:
        s = str(symbol or "").strip().upper()
        if not s:
            return ""
        if s == "GOLD":
            return "XAUUSD"
        if s.endswith("/USDT") and len(s) > 5:
            base = s[:-5]
            if base:
                return f"{base}USD"
        return s

    @staticmethod
    def normalize_learning_action_filter(action: str) -> str:
        a = str(action or "").strip().lower().replace("-", "_")
        if not a:
            return ""
        alias = {
            "be": "breakeven",
            "break_even": "breakeven",
            "break-even": "breakeven",
            "breakeven": "breakeven",
            "partial": "partial_close",
            "partial_tp": "partial_close",
            "partialclose": "partial_close",
            "partial_close": "partial_close",
            "trail": "trail_sl",
            "trailing": "trail_sl",
            "trailsl": "trail_sl",
            "trail_sl": "trail_sl",
            "time_stop": "time_stop_close",
            "timestop": "time_stop_close",
            "time_stop_close": "time_stop_close",
            "time_stop_exit": "time_stop_close",
            "early_risk": "early_risk_tighten",
            "earlyrisk": "early_risk_tighten",
            "risk_tighten": "early_risk_tighten",
            "early_risk_tighten": "early_risk_tighten",
        }
        return alias.get(a, a if a in {"breakeven", "partial_close", "trail_sl", "time_stop_close", "early_risk_tighten"} else "")

    def _build_learning_recommendations(self, raw_rows: list[dict], *, symbol_filter: str = "", action_filter: str = "") -> list[dict]:
        """
        Build bounded threshold recommendations from resolved PM action outcomes.
        Recommendations are data-driven and move current config toward values that
        historically aligned with better outcomes for the filtered sample.
        """
        resolved = [r for r in (raw_rows or []) if bool(r.get("resolved"))]
        if not resolved:
            return []

        # (action, rules_key, config_attr, min_samples, tolerance, max_step, decimals, desc)
        specs = [
            ("breakeven", "break_even_r", "MT5_PM_BREAK_EVEN_R", 6, 0.05, 0.30, 2, "BE trigger (R)"),
            ("partial_close", "partial_tp_r", "MT5_PM_PARTIAL_TP_R", 6, 0.05, 0.30, 2, "Partial TP trigger (R)"),
            ("trail_sl", "trail_start_r", "MT5_PM_TRAIL_START_R", 6, 0.05, 0.40, 2, "Trail start (R)"),
            ("trail_sl", "trail_gap_r", "MT5_PM_TRAIL_GAP_R", 6, 0.03, 0.25, 2, "Trail gap (R)"),
            ("time_stop_close", "time_stop_min", "MT5_PM_TIME_STOP_MIN", 5, 5.0, 30.0, 0, "Time-stop minutes"),
            ("time_stop_close", "time_stop_flat_r", "MT5_PM_TIME_STOP_FLAT_R", 5, 0.03, 0.20, 2, "Time-stop flat threshold (R)"),
            ("early_risk_tighten", "early_risk_trigger_r", "MT5_PM_EARLY_RISK_TRIGGER_R", 5, 0.05, 0.30, 2, "Early-risk trigger (R)"),
            ("early_risk_tighten", "early_risk_sl_r", "MT5_PM_EARLY_RISK_SL_R", 5, 0.05, 0.20, 2, "Early-risk tighten SL (R)"),
        ]

        agg: dict[tuple[str, str], dict] = {}
        for row in resolved:
            act = self.normalize_learning_action_filter(str(row.get("action") or ""))
            if not act:
                continue
            if action_filter and act != action_filter:
                continue
            if symbol_filter and str(row.get("symbol") or "").upper() != symbol_filter:
                continue
            rules = dict(row.get("rules") or {})
            if not rules:
                continue
            pnl = _safe_float(row.get("outcome_pnl", 0.0), 0.0)
            lab = str(row.get("outcome_label") or "").lower()
            is_pos = (lab in {"tp", "positive"}) or (pnl > 0)
            is_neg = (lab in {"sl", "negative"}) or (pnl < 0)
            for spec_act, rules_key, *_rest in specs:
                if act != spec_act or rules_key not in rules:
                    continue
                val = rules.get(rules_key)
                try:
                    v = float(val)
                except Exception:
                    continue
                a = agg.setdefault((act, rules_key), {
                    "samples": 0, "pos_n": 0, "neg_n": 0,
                    "sum_all": 0.0, "sum_pos": 0.0, "sum_neg": 0.0,
                })
                a["samples"] += 1
                a["sum_all"] += v
                if is_pos:
                    a["pos_n"] += 1
                    a["sum_pos"] += v
                if is_neg:
                    a["neg_n"] += 1
                    a["sum_neg"] += v

        recs: list[dict] = []
        for action_name, rules_key, cfg_attr, min_samples, tol, max_step, decimals, label in specs:
            a = agg.get((action_name, rules_key))
            if not a:
                continue
            samples = _safe_int(a.get("samples", 0), 0)
            pos_n = _safe_int(a.get("pos_n", 0), 0)
            neg_n = _safe_int(a.get("neg_n", 0), 0)
            if samples < int(min_samples):
                continue
            if pos_n <= 0 and neg_n <= 0:
                continue
            cur = _safe_float(getattr(config, cfg_attr, 0.0), 0.0)
            avg_all = _safe_float(a.get("sum_all", 0.0), 0.0) / max(1, samples)
            pos_avg = (_safe_float(a.get("sum_pos", 0.0), 0.0) / pos_n) if pos_n > 0 else None
            neg_avg = (_safe_float(a.get("sum_neg", 0.0), 0.0) / neg_n) if neg_n > 0 else None

            target = pos_avg if pos_avg is not None else avg_all
            basis = "positive_avg" if pos_avg is not None else "overall_avg"

            # When mixed outcomes exist, require separation to reduce noisy recommendations.
            if (pos_avg is not None) and (neg_avg is not None):
                sep = abs(float(pos_avg) - float(neg_avg))
                if sep < float(tol):
                    continue
            if abs(float(target) - float(cur)) < float(tol):
                continue

            delta_to_target = float(target) - float(cur)
            suggested = float(cur) + _clamp(delta_to_target * 0.5, -float(max_step), float(max_step))
            if abs(suggested - float(cur)) < float(tol):
                continue

            if decimals <= 0:
                suggested_out = int(round(suggested))
                current_out = int(round(cur))
                target_out = int(round(target))
            else:
                suggested_out = round(suggested, int(decimals))
                current_out = round(cur, int(decimals))
                target_out = round(float(target), int(decimals))

            score = (min(samples, 30) / 30.0)
            if pos_n > 0 and neg_n > 0 and pos_avg is not None and neg_avg is not None:
                score += min(0.4, abs(float(pos_avg) - float(neg_avg)) / max(0.0001, float(tol)) * 0.05)
            confidence = "high" if score >= 1.0 else ("medium" if score >= 0.65 else "low")
            direction = "raise" if suggested > float(cur) else "lower"

            why = f"{label}: move toward {basis} from resolved outcomes"
            if pos_avg is not None and neg_avg is not None:
                why += f" (pos_avg={round(float(pos_avg), int(decimals) if decimals > 0 else 0)}, neg_avg={round(float(neg_avg), int(decimals) if decimals > 0 else 0)})"

            recs.append({
                "action": action_name,
                "key": rules_key,
                "config_attr": cfg_attr,
                "label": label,
                "direction": direction,
                "current": current_out,
                "suggested": suggested_out,
                "target_from_stats": target_out,
                "samples": samples,
                "pos_samples": pos_n,
                "neg_samples": neg_n,
                "confidence": confidence,
                "reason": why,
                "basis": basis,
            })

        recs.sort(
            key=lambda r: (
                {"high": 3, "medium": 2, "low": 1}.get(str(r.get("confidence") or "low"), 1),
                int(r.get("samples", 0)),
                int(r.get("pos_samples", 0)) + int(r.get("neg_samples", 0)),
            ),
            reverse=True,
        )
        return recs

    @staticmethod
    def _sessions_for_timestamp(ts: int) -> list[str]:
        try:
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            hour_min = dt.strftime("%H:%M")
        except Exception:
            return ["unknown"]
        active = []
        sessions_cfg = dict(getattr(config, "SESSIONS", {}) or {})
        for name, times in sessions_cfg.items():
            try:
                start = str((times or {}).get("start") or "")
                end = str((times or {}).get("end") or "")
                if start and end and start <= hour_min <= end:
                    active.append(str(name))
            except Exception:
                continue
        return active or ["off_hours"]

    @classmethod
    def _primary_session_regime(cls, ts: int) -> str:
        sessions = [str(s).lower() for s in cls._sessions_for_timestamp(ts)]
        if "overlap" in sessions:
            return "overlap"
        if "london" in sessions:
            return "london"
        if "new_york" in sessions:
            return "new_york"
        if "asian" in sessions:
            return "asian"
        return sessions[0] if sessions else "off_hours"

    @staticmethod
    def _spread_regime_label(row: dict) -> str:
        spread_pct = _safe_float(row.get("spread_pct", 0.0), 0.0)
        rules = dict(row.get("rules") or {})
        spike = _safe_float(rules.get("spread_spike_pct", getattr(config, "MT5_PM_SPREAD_SPIKE_PCT", 0.18)), 0.18)
        if spread_pct <= 0:
            return "unknown_spread"
        if spread_pct >= max(0.01, spike * 0.85):
            return "high_spread"
        return "normal_spread"

    def _build_regime_recommendations(self, raw_rows: list[dict], *, symbol_filter: str = "", action_filter: str = "") -> list[dict]:
        if not raw_rows:
            return []
        regime_rows: dict[str, list[dict]] = {}
        for row in raw_rows:
            for regime in {str(row.get("session_regime") or ""), str(row.get("spread_regime") or "")}:
                regime = regime.strip().lower()
                if not regime:
                    continue
                regime_rows.setdefault(regime, []).append(row)

        order = {"overlap": 6, "london": 5, "new_york": 4, "asian": 3, "high_spread": 2, "normal_spread": 1}
        out = []
        for regime, rows in regime_rows.items():
            recs = self._build_learning_recommendations(rows, symbol_filter=symbol_filter, action_filter=action_filter)
            resolved_n = sum(1 for r in rows if bool(r.get("resolved")))
            if not recs:
                continue
            out.append({
                "regime": regime,
                "rows": len(rows),
                "resolved_rows": resolved_n,
                "recommendations": recs[:4],
            })
        out.sort(key=lambda x: (order.get(str(x.get("regime")), 0), int(x.get("resolved_rows", 0)), int(x.get("rows", 0))), reverse=True)
        return out

    @staticmethod
    def build_policy_draft_from_learning_report(report: dict) -> dict:
        """
        Build a non-applied per-account policy draft from learning recommendations.
        Includes global overrides and regime-specific advisory sections.
        """
        r = dict(report or {})
        filters = dict(r.get("filters", {}) or {})
        recs = list(r.get("recommendations", []) or [])
        by_regime = list(r.get("recommendations_by_regime", []) or [])
        key_map = {
            "break_even_r": "pm_break_even_r",
            "partial_tp_r": "pm_partial_tp_r",
            "trail_start_r": "pm_trail_start_r",
            "trail_gap_r": "pm_trail_gap_r",
            "time_stop_min": "pm_time_stop_min",
            "time_stop_flat_r": "pm_time_stop_flat_r",
            "early_risk_trigger_r": "pm_early_risk_trigger_r",
            "early_risk_sl_r": "pm_early_risk_sl_r",
            "spread_spike_pct": "pm_spread_spike_pct",
        }

        def _collect(rs: list[dict]) -> dict:
            d = {}
            for rec in rs:
                conf = str(rec.get("confidence") or "low").lower()
                if conf == "low":
                    continue
                pk = key_map.get(str(rec.get("key") or ""))
                if not pk:
                    continue
                d[pk] = rec.get("suggested")
            return d

        global_overrides = _collect(recs)
        regime_overrides = []
        for bucket in by_regime:
            rs = list(bucket.get("recommendations", []) or [])
            ov = _collect(rs)
            if not ov:
                continue
            regime_overrides.append({
                "regime": str(bucket.get("regime") or ""),
                "policy_overrides": ov,
                "rows": int(bucket.get("rows", 0)),
                "resolved_rows": int(bucket.get("resolved_rows", 0)),
            })

        return {
            "kind": "mt5_pm_learning_policy_draft",
            "version": 1,
            "generated_at": _iso(_utc_now()),
            "account_key": str(r.get("account_key") or ""),
            "lookback_days": int(r.get("days", 30) or 30),
            "filters": filters,
            "global_overrides": global_overrides,
            "regime_overrides": regime_overrides,
            "source_summary": {
                "total_actions": int(dict(r.get("summary", {}) or {}).get("total_actions", 0)),
                "resolved_actions": int(dict(r.get("summary", {}) or {}).get("resolved_actions", 0)),
            },
        }

    @staticmethod
    def _position_r_metrics(pos: dict) -> dict:
        ptype = str(pos.get("type", "") or "").lower()
        is_buy = ptype in {"buy", "long"}
        open_px = _safe_float(pos.get("price_open", 0.0), 0.0)
        now_px = _safe_float(pos.get("price_current", 0.0), 0.0)
        sl = _safe_float(pos.get("sl", 0.0), 0.0)
        if open_px <= 0 or now_px <= 0:
            return {"valid": False}
        r_dist = abs(open_px - sl) if sl > 0 else 0.0
        if r_dist <= 0:
            return {"valid": False, "no_sl": True, "open": open_px, "now": now_px, "is_buy": is_buy}
        move = (now_px - open_px) if is_buy else (open_px - now_px)
        r_now = move / r_dist
        return {
            "valid": True,
            "is_buy": is_buy,
            "open": open_px,
            "now": now_px,
            "sl": sl,
            "r_dist": r_dist,
            "r_now": r_now,
        }

    def _eligible_position(self, pos: dict, rules: Optional[dict] = None) -> bool:
        if not bool((rules or {}).get("manage_enabled", getattr(config, "MT5_PM_MANAGE_ENABLED", True))):
            return False
        if bool(getattr(config, "PERSISTENT_CANARY_MT5_SKIP_POSITION_MANAGER", True)):
            base_magic = _safe_int(getattr(config, "MT5_MAGIC", 0), 0)
            canary_magic = base_magic + _safe_int(getattr(config, "PERSISTENT_CANARY_MT5_MAGIC_OFFSET", 700), 700)
            magic = _safe_int(pos.get("magic", 0), 0)
            if canary_magic > 0 and magic == canary_magic:
                return False
        manage_manual = bool((rules or {}).get("manage_manual_positions", getattr(config, "MT5_PM_MANAGE_MANUAL_POSITIONS", True)))
        magic = _safe_int(pos.get("magic", 0), 0)
        if manage_manual:
            return True
        return magic == _safe_int(getattr(config, "MT5_MAGIC", 0), 0)

    def _age_minutes(self, pos: dict) -> float:
        ts = _safe_int(pos.get("time", 0), 0)
        if ts <= 0:
            ts = _safe_int(pos.get("time_msc", 0), 0)
            if ts > 10**12:
                ts = int(ts / 1000)
        if ts <= 0:
            return 0.0
        return max(0.0, (_utc_now().timestamp() - float(ts)) / 60.0)

    def _dynamic_trail_gap_r(
        self,
        *,
        base_gap_r: float,
        trail_start_r: float,
        r_now: float,
        spread_pct: float,
        spread_spike_pct: float,
        age_min: float,
    ) -> tuple[float, dict]:
        info = {
            "enabled": bool(getattr(config, "MT5_PM_TRAIL_DYNAMIC_ENABLED", True)),
            "applied": False,
            "base_gap_r": round(float(base_gap_r), 6),
            "effective_gap_r": round(float(base_gap_r), 6),
            "tighten_pct": 0.0,
            "widen_pct": 0.0,
            "young_widen_pct": 0.0,
            "reason": "disabled",
        }
        gap = max(0.01, float(base_gap_r))
        if not bool(info["enabled"]):
            return gap, info

        step_r = max(0.2, _safe_float(getattr(config, "MT5_PM_TRAIL_DYNAMIC_STEP_R", 0.8), 0.8))
        tighten_per_step = _clamp(
            _safe_float(getattr(config, "MT5_PM_TRAIL_DYNAMIC_TIGHTEN_PCT_PER_STEP", 0.12), 0.12),
            0.0,
            0.60,
        )
        max_tighten = _clamp(
            _safe_float(getattr(config, "MT5_PM_TRAIL_DYNAMIC_MAX_TIGHTEN_PCT", 0.35), 0.35),
            0.0,
            0.80,
        )
        spread_widen_pct = _clamp(
            _safe_float(getattr(config, "MT5_PM_TRAIL_DYNAMIC_SPREAD_WIDEN_PCT", 0.18), 0.18),
            0.0,
            0.80,
        )
        max_widen = _clamp(
            _safe_float(getattr(config, "MT5_PM_TRAIL_DYNAMIC_MAX_WIDEN_PCT", 0.24), 0.24),
            0.0,
            0.80,
        )
        young_age_min = max(0.0, _safe_float(getattr(config, "MT5_PM_TRAIL_DYNAMIC_YOUNG_AGE_MIN", 6.0), 6.0))
        young_widen = _clamp(
            _safe_float(getattr(config, "MT5_PM_TRAIL_DYNAMIC_YOUNG_WIDEN_PCT", 0.10), 0.10),
            0.0,
            0.50,
        )
        min_gap = max(0.05, _safe_float(getattr(config, "MT5_PM_TRAIL_DYNAMIC_MIN_GAP_R", 0.28), 0.28))
        max_gap = max(min_gap + 0.01, _safe_float(getattr(config, "MT5_PM_TRAIL_DYNAMIC_MAX_GAP_R", 1.10), 1.10))

        progressed_r = max(0.0, float(r_now) - float(trail_start_r))
        steps = int(progressed_r / step_r) if step_r > 1e-9 else 0
        tighten_pct = min(max_tighten, max(0.0, float(steps) * float(tighten_per_step)))
        if tighten_pct > 0:
            gap *= (1.0 - tighten_pct)

        widen_pct = 0.0
        if spread_pct > 0 and spread_spike_pct > 1e-9:
            ratio = max(0.0, (float(spread_pct) / float(spread_spike_pct)) - 1.0)
            widen_pct = min(max_widen, ratio * spread_widen_pct)
            if widen_pct > 0:
                gap *= (1.0 + widen_pct)

        young_widen_pct = 0.0
        if float(age_min) <= young_age_min and young_widen > 0:
            young_widen_pct = young_widen
            gap *= (1.0 + young_widen)

        gap = _clamp(gap, min_gap, max_gap)
        info.update(
            {
                "applied": True,
                "effective_gap_r": round(float(gap), 6),
                "tighten_pct": round(float(tighten_pct), 6),
                "widen_pct": round(float(widen_pct), 6),
                "young_widen_pct": round(float(young_widen_pct), 6),
                "steps": int(steps),
                "reason": "dynamic_gap_applied",
            }
        )
        return float(gap), info

    def _rule_params(self, account_key: str = "") -> dict:
        rules = {
            "break_even_r": _safe_float(getattr(config, "MT5_PM_BREAK_EVEN_R", 0.8), 0.8),
            "trail_start_r": _safe_float(getattr(config, "MT5_PM_TRAIL_START_R", 1.2), 1.2),
            "trail_gap_r": _safe_float(getattr(config, "MT5_PM_TRAIL_GAP_R", 0.6), 0.6),
            "partial_tp_r": _safe_float(getattr(config, "MT5_PM_PARTIAL_TP_R", 1.0), 1.0),
            "partial_close_pct": max(0.0, min(1.0, _safe_float(getattr(config, "MT5_PM_PARTIAL_CLOSE_PCT", 0.5), 0.5))),
            "min_partial_volume": max(0.0, _safe_float(getattr(config, "MT5_PM_MIN_PARTIAL_VOLUME", 0.01), 0.01)),
            "time_stop_min": max(0, _safe_int(getattr(config, "MT5_PM_TIME_STOP_MIN", 120), 120)),
            "time_stop_flat_r": _safe_float(getattr(config, "MT5_PM_TIME_STOP_FLAT_R", 0.25), 0.25),
            "max_actions_per_cycle": max(1, _safe_int(getattr(config, "MT5_PM_MAX_ACTIONS_PER_CYCLE", 3), 3)),
            "manage_manual_positions": bool(getattr(config, "MT5_PM_MANAGE_MANUAL_POSITIONS", True)),
            "manage_enabled": bool(getattr(config, "MT5_PM_MANAGE_ENABLED", True)),
            "early_risk_enabled": bool(getattr(config, "MT5_PM_EARLY_RISK_PROTECT_ENABLED", True)),
            "early_risk_trigger_r": -abs(_safe_float(getattr(config, "MT5_PM_EARLY_RISK_TRIGGER_R", -0.8), -0.8)),
            "early_risk_sl_r": -abs(_safe_float(getattr(config, "MT5_PM_EARLY_RISK_SL_R", -0.92), -0.92)),
            "early_risk_buffer_r": max(0.0, _safe_float(getattr(config, "MT5_PM_EARLY_RISK_BUFFER_R", 0.05), 0.05)),
            "spread_spike_protect_enabled": bool(getattr(config, "MT5_PM_SPREAD_SPIKE_PROTECT_ENABLED", True)),
            "spread_spike_pct": max(0.0, _safe_float(getattr(config, "MT5_PM_SPREAD_SPIKE_PCT", 0.18), 0.18)),
        }
        if account_key:
            try:
                from learning.mt5_orchestrator import mt5_orchestrator
                pol_rep = mt5_orchestrator.get_account_policy(account_key)
                pol = dict(pol_rep.get("policy", {}) or {}) if pol_rep.get("ok") else {}
                if "position_manager_enabled" in pol and pol.get("position_manager_enabled") is not None:
                    rules["manage_enabled"] = bool(pol.get("position_manager_enabled"))
                if pol.get("pm_early_risk_enabled") is not None:
                    rules["early_risk_enabled"] = bool(pol.get("pm_early_risk_enabled"))
                if pol.get("pm_spread_spike_protect_enabled") is not None:
                    rules["spread_spike_protect_enabled"] = bool(pol.get("pm_spread_spike_protect_enabled"))
                if pol.get("pm_early_risk_trigger_r") is not None:
                    rules["early_risk_trigger_r"] = -abs(_safe_float(pol.get("pm_early_risk_trigger_r"), rules["early_risk_trigger_r"]))
                if pol.get("pm_early_risk_sl_r") is not None:
                    rules["early_risk_sl_r"] = -abs(_safe_float(pol.get("pm_early_risk_sl_r"), rules["early_risk_sl_r"]))
                if pol.get("pm_early_risk_buffer_r") is not None:
                    rules["early_risk_buffer_r"] = max(0.0, _safe_float(pol.get("pm_early_risk_buffer_r"), rules["early_risk_buffer_r"]))
                if pol.get("pm_spread_spike_pct") is not None:
                    rules["spread_spike_pct"] = max(0.0, _safe_float(pol.get("pm_spread_spike_pct"), rules["spread_spike_pct"]))
                if pol.get("pm_break_even_r") is not None:
                    rules["break_even_r"] = max(0.0, _safe_float(pol.get("pm_break_even_r"), rules["break_even_r"]))
                if pol.get("pm_partial_tp_r") is not None:
                    rules["partial_tp_r"] = max(0.0, _safe_float(pol.get("pm_partial_tp_r"), rules["partial_tp_r"]))
                if pol.get("pm_trail_start_r") is not None:
                    rules["trail_start_r"] = max(0.0, _safe_float(pol.get("pm_trail_start_r"), rules["trail_start_r"]))
                if pol.get("pm_trail_gap_r") is not None:
                    rules["trail_gap_r"] = max(0.0, _safe_float(pol.get("pm_trail_gap_r"), rules["trail_gap_r"]))
                if pol.get("pm_time_stop_min") is not None:
                    rules["time_stop_min"] = max(0, _safe_int(pol.get("pm_time_stop_min"), rules["time_stop_min"]))
                if pol.get("pm_time_stop_flat_r") is not None:
                    rules["time_stop_flat_r"] = max(0.0, _safe_float(pol.get("pm_time_stop_flat_r"), rules["time_stop_flat_r"]))
            except Exception:
                pass
        # keep early-risk target behind trigger
        if rules["early_risk_sl_r"] > rules["early_risk_trigger_r"]:
            rules["early_risk_sl_r"] = rules["early_risk_trigger_r"] - max(0.02, float(rules.get("early_risk_buffer_r", 0.05) or 0.05))
        return rules

    def _adaptive_rules_for_position(
        self,
        account_key: str,
        pos: dict,
        base_rules: dict,
        *,
        metrics: Optional[dict] = None,
        age_min: Optional[float] = None,
    ) -> tuple[dict, dict]:
        """
        Adaptive PM thresholds per position (bounded, explainable).
        Uses symbol behavior stats from MT5 forward-test journal + current regime proxies
        (spread, initial risk %, age/progress).
        """
        rules = dict(base_rules or {})
        info = {
            "enabled": bool(getattr(config, "MT5_PM_ADAPTIVE_ENABLED", True)),
            "applied": False,
            "reason": "disabled",
            "symbol_family": None,
            "samples": 0,
            "factors": {},
        }
        if not bool(info["enabled"]):
            return rules, info

        try:
            symbol = str(pos.get("symbol", "") or "")
            sig_symbol = symbol
            if symbol.endswith("USD") and symbol[: -3] in {"BTC","ETH","SOL","XRP","DOGE","ADA","AVAX","BNB","LTC","BCH","DOT","LINK","TRX","UNI","ATOM","POL","HBAR","PEPE","SHIB","PAXG"}:
                sig_symbol = f"{symbol[:-3]}/USDT"
            fam = mt5_adaptive_trade_planner.symbol_family(sig_symbol, symbol)
            info["symbol_family"] = fam
            m = dict(metrics or {})
            valid = bool(m.get("valid"))
            open_px = _safe_float(m.get("open", 0.0), 0.0) if valid else _safe_float(pos.get("price_open", 0.0), 0.0)
            r_dist = _safe_float(m.get("r_dist", 0.0), 0.0) if valid else 0.0
            r_now = _safe_float(m.get("r_now", 0.0), 0.0) if valid else 0.0
            spread_pct = _safe_float(pos.get("spread_pct", 0.0), 0.0)
            spread_abs = _safe_float(pos.get("spread", 0.0), 0.0)
            if spread_abs <= 0 and open_px > 0 and spread_pct > 0:
                spread_abs = open_px * (spread_pct / 100.0)
            r_dist_pct = (r_dist / open_px * 100.0) if (open_px > 0 and r_dist > 0) else 0.0
            spread_r = (spread_abs / r_dist) if (spread_abs > 0 and r_dist > 0) else 0.0
            age_m = _safe_float(age_min, 0.0)
            lookback_days = max(7, int(getattr(config, "MT5_PM_ADAPTIVE_LOOKBACK_DAYS", 45)))
            stats = mt5_adaptive_trade_planner.symbol_behavior_stats(
                account_key=str(account_key or ""),
                signal_symbol=sig_symbol,
                broker_symbol=symbol,
                lookback_days=lookback_days,
            )
            samples = _safe_int(stats.get("samples", 0), 0)
            info["samples"] = samples

            # Family priors (only small nudges).
            family_spread_warn = {"crypto": 0.10, "metal": 0.06, "fx": 0.03, "index": 0.05, "stock": 0.08}.get(fam, 0.08)
            family_noise = {"crypto": 0.08, "metal": 0.04, "fx": 0.02, "index": 0.03, "stock": 0.03}.get(fam, 0.03)

            # Start from base and adjust in bounded increments.
            be_r = _safe_float(rules.get("break_even_r", 0.8), 0.8)
            partial_r = _safe_float(rules.get("partial_tp_r", 1.0), 1.0)
            trail_start_r = _safe_float(rules.get("trail_start_r", 1.2), 1.2)
            trail_gap_r = _safe_float(rules.get("trail_gap_r", 0.6), 0.6)
            time_stop_min = float(_safe_int(rules.get("time_stop_min", 120), 120))
            time_stop_flat_r = _safe_float(rules.get("time_stop_flat_r", 0.25), 0.25)
            early_trigger_r = _safe_float(rules.get("early_risk_trigger_r", -0.8), -0.8)
            early_sl_r = _safe_float(rules.get("early_risk_sl_r", -0.92), -0.92)
            spread_spike_pct_rule = max(0.0, _safe_float(rules.get("spread_spike_pct", 0.18), 0.18))

            # Regime proxies from live position context.
            regime_shift = 0.0
            regime_shift += _safe_float(family_noise, 0.0)
            if spread_pct > 0:
                regime_shift += _clamp((spread_pct - family_spread_warn) * 2.5, -0.05, 0.18)
            if spread_r > 0:
                regime_shift += _clamp((spread_r - 0.08) * 0.45, -0.03, 0.12)
            if r_dist_pct > 0:
                regime_shift += _clamp((r_dist_pct - 1.0) * 0.04, -0.04, 0.10)  # wider initial stop -> noisier regime

            # If position is aging but not progressing, protect earlier.
            stall_bias = 0.0
            if age_m >= 30.0 and valid:
                if abs(r_now) < 0.20:
                    stall_bias += 0.10
                elif r_now < 0:
                    stall_bias += 0.05

            # Historical behavior per symbol.
            hist_bias = 0.0
            min_samples = max(1, int(getattr(config, "MT5_PM_ADAPTIVE_MIN_SYMBOL_TRADES", 6)))
            win_rate = mae = tp_rate = sl_rate = None
            if samples >= min_samples:
                win_rate = _safe_float(stats.get("win_rate", 0.5), 0.5)
                mae = _safe_float(stats.get("mae", 0.35), 0.35)
                tp_rate = _safe_float(stats.get("tp_rate", win_rate), win_rate)
                sl_rate = _safe_float(stats.get("sl_rate", 1.0 - win_rate), 1.0 - win_rate)
                hist_bias += _clamp((0.52 - win_rate) * 0.35, -0.12, 0.12)   # weak symbol => protect sooner
                hist_bias += _clamp((mae - 0.35) * 0.20, -0.04, 0.10)        # poor calibration => protect sooner
                hist_bias += _clamp((sl_rate - tp_rate) * 0.10, -0.05, 0.05)

            pm_learning = {}
            pm_bias = 0.0
            pm_learning_min = max(1, int(getattr(config, "MT5_PM_LEARNING_MIN_ACTIONS", 8)))
            if bool(getattr(config, "MT5_PM_LEARNING_ENABLED", True)):
                pm_learning = self._pm_learning_stats(
                    account_key=str(account_key or ""),
                    symbol=symbol,
                    lookback_days=max(7, int(getattr(config, "MT5_PM_LEARNING_LOOKBACK_DAYS", 60))),
                )
                pm_samples = _safe_int(pm_learning.get("samples", 0), 0)
                if pm_samples >= pm_learning_min:
                    prot_pos = _safe_float(pm_learning.get("protect_positive_rate", 0.5), 0.5)
                    pm_bias += _clamp((0.52 - prot_pos) * 0.22, -0.06, 0.08)

                    early_n = _safe_int(pm_learning.get("early_risk_tighten_samples", 0), 0)
                    early_neg = _safe_float(pm_learning.get("early_risk_tighten_negative_rate", 0.5), 0.5)
                    if early_n >= 3:
                        if early_neg >= 0.60:
                            early_trigger_r = -_clamp(abs(early_trigger_r) - 0.06, 0.45, 1.20)
                            early_sl_r = -_clamp(abs(early_sl_r) - 0.05, 0.55, 1.35)
                            spread_spike_pct_rule = _clamp(spread_spike_pct_rule * 0.92, 0.06, 0.40)
                            pm_bias += 0.03
                        elif early_neg <= 0.30:
                            early_trigger_r = -_clamp(abs(early_trigger_r) + 0.03, 0.45, 1.20)
                            spread_spike_pct_rule = _clamp(spread_spike_pct_rule * 1.04, 0.06, 0.40)
                            pm_bias -= 0.01

                    trail_n = _safe_int(pm_learning.get("trail_sl_samples", 0), 0)
                    trail_pos = _safe_float(pm_learning.get("trail_sl_positive_rate", prot_pos), prot_pos)
                    if trail_n >= 3:
                        if trail_pos >= 0.60:
                            be_r -= 0.05
                            trail_start_r -= 0.08
                            trail_gap_r -= 0.03
                            pm_bias += 0.02
                        elif trail_pos <= 0.35:
                            trail_start_r += 0.08
                            trail_gap_r += 0.06
                            pm_bias -= 0.02

                    part_n = _safe_int(pm_learning.get("partial_close_samples", 0), 0)
                    part_pos = _safe_float(pm_learning.get("partial_close_positive_rate", prot_pos), prot_pos)
                    if part_n >= 3:
                        if part_pos >= 0.60:
                            partial_r -= 0.05
                        elif part_pos <= 0.30:
                            partial_r += 0.07

                    ts_n = _safe_int(pm_learning.get("time_stop_close_samples", 0), 0)
                    ts_neg = _safe_float(pm_learning.get("time_stop_close_negative_rate", 0.5), 0.5)
                    if ts_n >= 3:
                        if ts_neg >= 0.60:
                            time_stop_min *= 0.90
                            time_stop_flat_r += 0.03
                            pm_bias += 0.02
                        elif ts_neg <= 0.30:
                            time_stop_min *= 1.08
                            time_stop_flat_r -= 0.02
                            pm_bias -= 0.01

            hist_bias += _clamp(pm_bias, -0.08, 0.10)

            protect_bias = _clamp(regime_shift + stall_bias + hist_bias, -0.20, 0.30)
            runner_bias = _clamp(-protect_bias * 0.75, -0.20, 0.15)

            # Earlier protection when protect_bias > 0, looser runner when protect_bias < 0.
            be_r = _clamp(be_r - (protect_bias * 0.35), 0.55, 1.10)
            partial_r = _clamp(partial_r - (protect_bias * 0.40), 0.75, 1.40)
            trail_start_r = _clamp(trail_start_r - (protect_bias * 0.45), 0.90, 1.90)
            trail_gap_r = _clamp(trail_gap_r + (protect_bias * 0.18), 0.35, 1.00)
            time_stop_min = _clamp(time_stop_min * (1.0 - protect_bias * 0.28), 45.0, 300.0)
            time_stop_flat_r = _clamp(time_stop_flat_r + (protect_bias * 0.05), 0.12, 0.45)
            early_trigger_r = -_clamp(abs(early_trigger_r) - (protect_bias * 0.20), 0.45, 1.20)
            early_sl_r = -_clamp(abs(early_sl_r) - (protect_bias * 0.18), 0.55, 1.35)
            spread_spike_pct_rule = _clamp(spread_spike_pct_rule * (1.0 - protect_bias * 0.18), 0.06, 0.40)
            if early_sl_r > early_trigger_r:
                early_sl_r = early_trigger_r - max(0.02, _safe_float(rules.get("early_risk_buffer_r", 0.05), 0.05))

            rules.update({
                "break_even_r": round(be_r, 4),
                "partial_tp_r": round(partial_r, 4),
                "trail_start_r": round(trail_start_r, 4),
                "trail_gap_r": round(trail_gap_r, 4),
                "time_stop_min": int(round(time_stop_min)),
                "time_stop_flat_r": round(time_stop_flat_r, 4),
                "early_risk_trigger_r": round(early_trigger_r, 4),
                "early_risk_sl_r": round(early_sl_r, 4),
                "spread_spike_pct": round(spread_spike_pct_rule, 5),
            })
            info.update({
                "applied": True,
                "reason": "adaptive_pm_applied",
                "factors": {
                    "family": fam,
                    "spread_pct": round(spread_pct, 6),
                    "spread_r": (round(spread_r, 4) if spread_r > 0 else 0.0),
                    "r_dist_pct": (round(r_dist_pct, 4) if r_dist_pct > 0 else 0.0),
                    "age_min": round(age_m, 2),
                    "r_now": (round(r_now, 4) if valid else None),
                    "samples": samples,
                    "win_rate": (round(win_rate, 4) if win_rate is not None else None),
                    "mae": (round(mae, 4) if mae is not None else None),
                    "tp_rate": (round(tp_rate, 4) if tp_rate is not None else None),
                    "sl_rate": (round(sl_rate, 4) if sl_rate is not None else None),
                    "pm_learning_samples": _safe_int(pm_learning.get("samples", 0), 0),
                    "pm_bias": round(pm_bias, 4),
                    "pm_protect_positive_rate": pm_learning.get("protect_positive_rate"),
                    "pm_early_risk_negative_rate": pm_learning.get("early_risk_tighten_negative_rate"),
                    "pm_trail_positive_rate": pm_learning.get("trail_sl_positive_rate"),
                    "pm_partial_positive_rate": pm_learning.get("partial_close_positive_rate"),
                    "pm_time_stop_negative_rate": pm_learning.get("time_stop_close_negative_rate"),
                    "protect_bias": round(protect_bias, 4),
                    "regime_shift": round(regime_shift, 4),
                    "stall_bias": round(stall_bias, 4),
                    "hist_bias": round(hist_bias, 4),
                },
                "rules": {
                    "break_even_r": rules["break_even_r"],
                    "partial_tp_r": rules["partial_tp_r"],
                    "trail_start_r": rules["trail_start_r"],
                    "trail_gap_r": rules["trail_gap_r"],
                    "time_stop_min": rules["time_stop_min"],
                    "time_stop_flat_r": rules["time_stop_flat_r"],
                    "early_risk_trigger_r": rules["early_risk_trigger_r"],
                    "early_risk_sl_r": rules["early_risk_sl_r"],
                    "spread_spike_pct": rules["spread_spike_pct"],
                },
            })
            return rules, info
        except Exception as e:
            info["reason"] = f"adaptive_pm_error:{e}"
            return dict(base_rules or {}), info

    def watch_snapshot(self, signal_symbol: str = "", limit: int = 10) -> dict:
        """
        Read-only snapshot of current PM watch state per open position/ticket.
        Useful for debugging why no action was taken yet.
        """
        out = {
            "ok": False,
            "enabled": self.enabled,
            "account_key": "",
            "requested_symbol": str(signal_symbol or "").upper(),
            "resolved_symbol": "",
            "positions": 0,
            "watched": 0,
            "entries": [],
            "rules": {},
            "error": "",
        }
        if not self.enabled:
            out["error"] = "disabled"
            return out

        st = mt5_executor.status()
        account_key = self._account_key(st)
        out["account_key"] = account_key
        if not account_key or not bool(st.get("connected")):
            out["error"] = "mt5 not connected"
            return out

        out["rules"] = self._rule_params(account_key)
        out["adaptive_pm_enabled"] = bool(getattr(config, "MT5_PM_ADAPTIVE_ENABLED", True))

        snap = mt5_executor.open_positions_snapshot(signal_symbol=str(signal_symbol or ""), limit=max(1, int(limit or 10)))
        if not bool(snap.get("connected")):
            out["error"] = str(snap.get("error") or "open position snapshot failed")
            return out

        positions = list(snap.get("positions", []) or [])
        out["positions"] = len(positions)
        out["resolved_symbol"] = str(snap.get("resolved_symbol", "") or "")
        rules = dict(out.get("rules", {}) or {})
        be_r = _safe_float(rules.get("break_even_r", 0.8), 0.8)
        trail_start_r = _safe_float(rules.get("trail_start_r", 1.2), 1.2)
        partial_r = _safe_float(rules.get("partial_tp_r", 1.0), 1.0)
        time_stop_min = _safe_int(rules.get("time_stop_min", 120), 120)
        time_stop_flat_r = _safe_float(rules.get("time_stop_flat_r", 0.25), 0.25)

        for pos in positions[: max(1, int(limit or 10))]:
            ticket = _safe_int(pos.get("ticket", 0), 0)
            symbol = str(pos.get("symbol", "") or "")
            if ticket <= 0 or not symbol:
                continue
            eligible = self._eligible_position(pos, rules=rules)
            pstate = self._get_state(account_key, ticket)
            metrics = self._position_r_metrics(pos)
            age_min = self._age_minutes(pos)
            eff_rules, adaptive_pm = self._adaptive_rules_for_position(
                account_key,
                pos,
                rules,
                metrics=metrics,
                age_min=age_min,
            )
            valid_metrics = bool(metrics.get("valid"))
            r_now = _safe_float(metrics.get("r_now", 0.0), 0.0) if valid_metrics else None
            no_sl = bool(metrics.get("no_sl", False))
            spread_pct = _safe_float(pos.get("spread_pct", 0.0), 0.0)
            entry = {
                "ticket": ticket,
                "symbol": symbol,
                "type": str(pos.get("type", "") or ""),
                "volume": _safe_float(pos.get("volume", 0.0), 0.0),
                "price_open": _safe_float(pos.get("price_open", 0.0), 0.0),
                "price_current": _safe_float(pos.get("price_current", 0.0), 0.0),
                "sl": _safe_float(pos.get("sl", 0.0), 0.0),
                "tp": _safe_float(pos.get("tp", 0.0), 0.0),
                "profit": _safe_float(pos.get("profit", 0.0), 0.0),
                "spread_pct": round(float(spread_pct), 6) if spread_pct > 0 else 0.0,
                "age_min": round(float(age_min), 2),
                "eligible": bool(eligible),
                "metrics_valid": bool(valid_metrics),
                "no_sl": no_sl,
                "r_now": (None if r_now is None else round(float(r_now), 4)),
                "r_dist": (None if not valid_metrics else round(_safe_float(metrics.get("r_dist", 0.0), 0.0), 6)),
                "state": {
                    "breakeven_done": bool(pstate.get("breakeven_done", False)),
                    "partial_done": bool(pstate.get("partial_done", False)),
                    "time_stop_done": bool(pstate.get("time_stop_done", False)),
                    "early_risk_done": bool(pstate.get("early_risk_done", False)),
                    "last_action": str(pstate.get("last_action", "") or ""),
                    "last_action_at": str(pstate.get("last_action_at", "") or ""),
                    "last_sl": pstate.get("last_sl"),
                    "partial_volume": pstate.get("partial_volume"),
                },
            }
            entry["adaptive_pm"] = adaptive_pm
            if eligible:
                is_buy = bool(metrics.get("is_buy")) if valid_metrics else None
                be_r_eff = _safe_float(eff_rules.get("break_even_r", be_r), be_r)
                partial_r_eff = _safe_float(eff_rules.get("partial_tp_r", partial_r), partial_r)
                trail_start_r_eff = _safe_float(eff_rules.get("trail_start_r", trail_start_r), trail_start_r)
                time_stop_min_eff = _safe_int(eff_rules.get("time_stop_min", time_stop_min), time_stop_min)
                time_stop_flat_r_eff = _safe_float(eff_rules.get("time_stop_flat_r", time_stop_flat_r), time_stop_flat_r)
                be_ready = bool(valid_metrics and (r_now is not None) and (r_now >= be_r_eff) and not bool(pstate.get("breakeven_done", False)))
                partial_ready = bool(valid_metrics and (r_now is not None) and (r_now >= partial_r_eff) and not bool(pstate.get("partial_done", False)))
                trail_ready = bool(valid_metrics and (r_now is not None) and (r_now >= trail_start_r_eff))
                time_stop_ready = bool((time_stop_min_eff > 0) and (age_min >= float(time_stop_min_eff)) and (r_now is not None) and (abs(r_now) < abs(time_stop_flat_r_eff)) and not bool(pstate.get("time_stop_done", False)))
                early_trigger_r = _safe_float(eff_rules.get("early_risk_trigger_r", _safe_float(rules.get("early_risk_trigger_r", -0.8), -0.8)), -0.8)
                early_risk_ready = bool(
                    valid_metrics
                    and bool(eff_rules.get("early_risk_enabled", rules.get("early_risk_enabled", False)))
                    and (r_now is not None)
                    and (r_now <= float(early_trigger_r))
                    and not bool(pstate.get("early_risk_done", False))
                )
                spread_spike_ready = bool(
                    valid_metrics
                    and bool(eff_rules.get("spread_spike_protect_enabled", rules.get("spread_spike_protect_enabled", False)))
                    and (r_now is not None)
                    and (r_now < 0)
                    and (spread_pct > 0)
                    and (spread_pct >= _safe_float(eff_rules.get("spread_spike_pct", rules.get("spread_spike_pct", 0.18)), 0.18))
                    and not bool(pstate.get("early_risk_done", False))
                )
                dist = {}
                if valid_metrics and (r_now is not None):
                    def _fav_price_for_r(target_r: float) -> float:
                        open_px = _safe_float(metrics.get("open", 0.0), 0.0)
                        r_dist = _safe_float(metrics.get("r_dist", 0.0), 0.0)
                        if is_buy:
                            return open_px + (target_r * r_dist)
                        return open_px - (target_r * r_dist)
                    dist["to_be_trigger_r"] = round(max(0.0, float(be_r_eff) - float(r_now)), 4)
                    dist["to_partial_r"] = round(max(0.0, float(partial_r_eff) - float(r_now)), 4)
                    dist["to_trail_r"] = round(max(0.0, float(trail_start_r_eff) - float(r_now)), 4)
                    early_trigger_r = _safe_float(eff_rules.get("early_risk_trigger_r", -0.8), -0.8)
                    dist["to_early_risk_trigger_r"] = round(max(0.0, float(r_now) - float(early_trigger_r)), 4)
                    be_trigger_px = _fav_price_for_r(float(be_r_eff))
                    now_px = _safe_float(entry.get("price_current", 0.0), 0.0)
                    sl_px = _safe_float(entry.get("sl", 0.0), 0.0)
                    tp_px = _safe_float(entry.get("tp", 0.0), 0.0)
                    if now_px > 0:
                        if is_buy:
                            dist["to_be_trigger_price"] = round(be_trigger_px - now_px, 6)
                            dist["to_sl_price"] = round(now_px - sl_px, 6) if sl_px > 0 else None
                            dist["to_tp_price"] = round(tp_px - now_px, 6) if tp_px > 0 else None
                        else:
                            dist["to_be_trigger_price"] = round(now_px - be_trigger_px, 6)
                            dist["to_sl_price"] = round(sl_px - now_px, 6) if sl_px > 0 else None
                            dist["to_tp_price"] = round(now_px - tp_px, 6) if tp_px > 0 else None
                entry["next_checks"] = {
                    "breakeven_ready": be_ready,
                    "partial_ready": partial_ready,
                    "trail_ready": trail_ready,
                    "time_stop_ready": time_stop_ready,
                    "early_risk_ready": early_risk_ready,
                    "spread_spike_ready": spread_spike_ready,
                }
                if dist:
                    entry["distances"] = dist
            out["entries"].append(entry)
        out["watched"] = sum(1 for x in out["entries"] if x.get("eligible"))
        out["ok"] = True
        return out

    def run_cycle(self, source: str = "autopilot") -> dict:
        out = {
            "ok": False,
            "enabled": self.enabled,
            "managed": 0,
            "checked": 0,
            "actions": [],
            "account_key": "",
            "positions": 0,
            "removed_states": 0,
            "error": "",
        }
        if not self.enabled:
            out["error"] = "disabled"
            return out
        st = mt5_executor.status()
        account_key = self._account_key(st)
        out["account_key"] = account_key
        if not account_key or not bool(st.get("connected")):
            out["error"] = "mt5 not connected"
            return out

        snap = mt5_executor.open_positions_snapshot(limit=50)
        if not bool(snap.get("connected")):
            out["error"] = str(snap.get("error") or "open position snapshot failed")
            return out
        positions = list(snap.get("positions", []) or [])
        out["positions"] = len(positions)
        live_tickets = {_safe_int(p.get("ticket", 0), 0) for p in positions if _safe_int(p.get("ticket", 0), 0) > 0}
        out["removed_states"] = self._cleanup_closed_states(account_key, live_tickets)

        rules = self._rule_params(account_key)
        out["rules"] = dict(rules)
        out["adaptive_pm_enabled"] = bool(getattr(config, "MT5_PM_ADAPTIVE_ENABLED", True))
        out["adaptive_pm_applied"] = 0
        be_r = _safe_float(rules.get("break_even_r", 0.8), 0.8)
        trail_start_r = _safe_float(rules.get("trail_start_r", 1.2), 1.2)
        trail_gap_r = _safe_float(rules.get("trail_gap_r", 0.6), 0.6)
        partial_r = _safe_float(rules.get("partial_tp_r", 1.0), 1.0)
        partial_pct = max(0.0, min(1.0, _safe_float(rules.get("partial_close_pct", 0.5), 0.5)))
        min_partial_vol = max(0.0, _safe_float(rules.get("min_partial_volume", 0.01), 0.01))
        time_stop_min = max(0, _safe_int(rules.get("time_stop_min", 120), 120))
        time_stop_flat_r = _safe_float(rules.get("time_stop_flat_r", 0.25), 0.25)
        max_actions = max(1, _safe_int(rules.get("max_actions_per_cycle", 3), 3))

        action_count = 0
        for pos in positions:
            if action_count >= max_actions:
                break
            out["checked"] += 1
            if not self._eligible_position(pos, rules=rules):
                continue
            ticket = _safe_int(pos.get("ticket", 0), 0)
            symbol = str(pos.get("symbol", "") or "")
            if ticket <= 0 or not symbol:
                continue
            pstate = self._get_state(account_key, ticket)
            metrics = self._position_r_metrics(pos)
            age_min = self._age_minutes(pos)
            current_tp = pos.get("tp")
            spread_pct = _safe_float(pos.get("spread_pct", 0.0), 0.0)
            pos_rules, adaptive_pm = self._adaptive_rules_for_position(
                account_key,
                pos,
                rules,
                metrics=metrics,
                age_min=age_min,
            )
            if bool((adaptive_pm or {}).get("applied")):
                out["adaptive_pm_applied"] = _safe_int(out.get("adaptive_pm_applied", 0), 0) + 1

            be_r_pos = _safe_float(pos_rules.get("break_even_r", be_r), be_r)
            trail_start_r_pos = _safe_float(pos_rules.get("trail_start_r", trail_start_r), trail_start_r)
            trail_gap_r_pos = _safe_float(pos_rules.get("trail_gap_r", trail_gap_r), trail_gap_r)
            partial_r_pos = _safe_float(pos_rules.get("partial_tp_r", partial_r), partial_r)
            partial_pct_pos = max(0.0, min(1.0, _safe_float(pos_rules.get("partial_close_pct", partial_pct), partial_pct)))
            min_partial_vol_pos = max(0.0, _safe_float(pos_rules.get("min_partial_volume", min_partial_vol), min_partial_vol))
            time_stop_min_pos = max(0, _safe_int(pos_rules.get("time_stop_min", time_stop_min), time_stop_min))
            time_stop_flat_r_pos = _safe_float(pos_rules.get("time_stop_flat_r", time_stop_flat_r), time_stop_flat_r)

            # Time-stop (close stale position if not making progress).
            if action_count < max_actions and time_stop_min_pos > 0 and age_min >= float(time_stop_min_pos):
                r_now = _safe_float(metrics.get("r_now", 0.0), 0.0) if metrics.get("valid") else 0.0
                time_stop_done = bool(pstate.get("time_stop_done", False))
                if (not time_stop_done) and abs(r_now) < abs(time_stop_flat_r_pos):
                    res = mt5_executor.close_position_partial(
                        broker_symbol=symbol,
                        position_ticket=ticket,
                        position_type=str(pos.get("type", "")),
                        position_volume=_safe_float(pos.get("volume", 0.0), 0.0),
                        close_volume=_safe_float(pos.get("volume", 0.0), 0.0),
                        source=f"{source}:time_stop",
                    )
                    # Suppress market_closed from actions to avoid Telegram spam
                    if getattr(res, "status", "") == "market_closed":
                        continue
                    out["actions"].append({
                        "ticket": ticket, "symbol": symbol, "action": "time_stop_close", "ok": res.ok, "status": res.status, "message": res.message, "retcode": (None if getattr(res, "retcode", None) is None else int(res.retcode)),
                        "position_type": str(pos.get("type", "")),
                        "position_volume": _safe_float(pos.get("volume", 0.0), 0.0),
                        "requested_close_volume": _safe_float(pos.get("volume", 0.0), 0.0),
                        "executed_close_volume": (None if res.volume is None else float(res.volume)),
                        "r_now": float(r_now),
                        "age_min": float(age_min),
                        "price_open": _safe_float(pos.get("price_open", 0.0), 0.0),
                        "price_current": _safe_float(pos.get("price_current", 0.0), 0.0),
                        "adaptive_pm": adaptive_pm,
                    })
                    if res.ok:
                        self._set_state(account_key, ticket, symbol, {"time_stop_done": True, "last_action": "time_stop_close", "last_action_at": _iso(_utc_now())})
                        self._record_action_learning(account_key, out["actions"][-1], pos_rules=pos_rules)
                        action_count += 1
                        out["managed"] += 1
                        # Instantly label neural brain with the real close outcome
                        try:
                            from learning.neural_brain import neural_brain
                            neural_brain.label_from_mt5_close(
                                ticket=int(ticket or 0),
                                close_reason="time_stop",
                                pnl_r=float(r_now),
                                symbol=str(symbol or ""),
                                direction="long" if bool(pos.get("is_buy", True)) else "short",
                            )
                        except Exception:
                            pass
                        continue


            if not metrics.get("valid"):
                continue
            is_buy = bool(metrics["is_buy"])
            open_px = _safe_float(metrics["open"], 0.0)
            sl_px = _safe_float(metrics["sl"], 0.0)
            r_dist = _safe_float(metrics["r_dist"], 0.0)
            r_now = _safe_float(metrics["r_now"], 0.0)
            now_px = _safe_float(metrics["now"], 0.0)

            # Early-risk protector (tighten SL before full -1R hit) and/or spread-spike on losing trade.
            early_enabled = bool(pos_rules.get("early_risk_enabled", rules.get("early_risk_enabled", False)))
            early_done = bool(pstate.get("early_risk_done", False))
            early_trigger_r = _safe_float(pos_rules.get("early_risk_trigger_r", -0.8), -0.8)
            early_sl_r = _safe_float(pos_rules.get("early_risk_sl_r", -0.92), -0.92)
            early_buf_r = max(0.0, _safe_float(pos_rules.get("early_risk_buffer_r", 0.05), 0.05))
            spread_spike_enabled = bool(pos_rules.get("spread_spike_protect_enabled", rules.get("spread_spike_protect_enabled", False)))
            spread_spike_pct = max(0.0, _safe_float(pos_rules.get("spread_spike_pct", 0.18), 0.18))
            early_loss_trigger = bool(r_now <= early_trigger_r)
            early_spread_trigger = bool(spread_spike_enabled and (r_now < 0.0) and (spread_pct > 0.0) and (spread_pct >= spread_spike_pct))
            if action_count < max_actions and early_enabled and (not early_done) and (early_loss_trigger or early_spread_trigger):
                # PM R-axis convention: current adverse loss is negative; SL must remain "behind" current price.
                target_sl_r = min(float(early_sl_r), float(r_now) - float(early_buf_r))
                target_sl_px = open_px + (target_sl_r * r_dist) if is_buy else open_px - (target_sl_r * r_dist)
                # Tighten only, and keep SL on the valid side of market.
                should_move = (target_sl_px > sl_px) if is_buy else (target_sl_px < sl_px)
                valid_side = (target_sl_px < now_px) if is_buy else (target_sl_px > now_px)
                if should_move and valid_side:
                    trigger_bits = []
                    if early_loss_trigger:
                        trigger_bits.append(f"loss<= {early_trigger_r:.2f}R")
                    if early_spread_trigger:
                        trigger_bits.append(f"spread_spike {spread_pct:.4f}%>= {spread_spike_pct:.4f}%")
                    res = mt5_executor.modify_position_sltp(
                        broker_symbol=symbol,
                        position_ticket=ticket,
                        sl=target_sl_px,
                        tp=_safe_float(current_tp, 0.0) or None,
                        source=f"{source}:early_risk",
                    )
                    out["actions"].append({
                        "ticket": ticket, "symbol": symbol, "action": "early_risk_tighten", "ok": res.ok, "status": res.status, "message": res.message, "retcode": (None if getattr(res, "retcode", None) is None else int(res.retcode)),
                        "position_type": str(pos.get("type", "")),
                        "old_sl": float(sl_px),
                        "new_sl": float(target_sl_px),
                        "tp": (_safe_float(current_tp, 0.0) or None),
                        "r_now": float(r_now),
                        "age_min": float(age_min),
                        "spread_pct": float(spread_pct),
                        "trigger": ", ".join(trigger_bits) if trigger_bits else "early_risk",
                        "price_open": float(open_px),
                        "price_current": float(now_px),
                        "adaptive_pm": adaptive_pm,
                    })
                    if res.ok:
                        self._set_state(
                            account_key,
                            ticket,
                            symbol,
                            {
                                "early_risk_done": True,
                                "last_action": "early_risk_tighten",
                                "last_action_at": _iso(_utc_now()),
                                "last_sl": float(target_sl_px),
                                "early_risk_trigger": ", ".join(trigger_bits) if trigger_bits else "early_risk",
                            },
                        )
                        self._record_action_learning(account_key, out["actions"][-1], pos_rules=pos_rules)
                        action_count += 1
                        out["managed"] += 1
                        continue

            # Break-even.
            if action_count < max_actions and r_now >= be_r_pos and (not bool(pstate.get("breakeven_done", False))):
                be_sl = open_px + (0.05 * r_dist if is_buy else -0.05 * r_dist)
                # tighten only
                should_move = (be_sl > sl_px) if is_buy else (be_sl < sl_px)
                if should_move:
                    res = mt5_executor.modify_position_sltp(
                        broker_symbol=symbol,
                        position_ticket=ticket,
                        sl=be_sl,
                        tp=_safe_float(current_tp, 0.0) or None,
                        source=f"{source}:be",
                    )
                    out["actions"].append({
                        "ticket": ticket, "symbol": symbol, "action": "breakeven", "ok": res.ok, "status": res.status, "message": res.message, "retcode": (None if getattr(res, "retcode", None) is None else int(res.retcode)),
                        "position_type": str(pos.get("type", "")),
                        "old_sl": float(sl_px),
                        "new_sl": float(be_sl),
                        "tp": (_safe_float(current_tp, 0.0) or None),
                        "r_now": float(r_now),
                        "age_min": float(age_min),
                        "price_open": float(open_px),
                        "price_current": float(now_px),
                        "adaptive_pm": adaptive_pm,
                    })
                    if res.ok:
                        self._set_state(account_key, ticket, symbol, {"breakeven_done": True, "last_action": "breakeven", "last_action_at": _iso(_utc_now()), "last_sl": float(be_sl)})
                        self._record_action_learning(account_key, out["actions"][-1], pos_rules=pos_rules)
                        action_count += 1
                        out["managed"] += 1
                        # continue; allow one action per position per cycle
                        continue

            # Partial TP at +1R (or configured).
            if action_count < max_actions and r_now >= partial_r_pos and (not bool(pstate.get("partial_done", False))):
                pos_vol = _safe_float(pos.get("volume", 0.0), 0.0)
                close_vol = max(min_partial_vol_pos, pos_vol * partial_pct_pos)
                if pos_vol > 0 and close_vol < pos_vol:
                    res = mt5_executor.close_position_partial(
                        broker_symbol=symbol,
                        position_ticket=ticket,
                        position_type=str(pos.get("type", "")),
                        position_volume=pos_vol,
                        close_volume=close_vol,
                        source=f"{source}:partial",
                    )
                    # Suppress market_closed from actions to avoid Telegram spam
                    if getattr(res, "status", "") == "market_closed":
                        continue
                    out["actions"].append({
                        "ticket": ticket, "symbol": symbol, "action": "partial_close", "ok": res.ok, "status": res.status, "message": res.message, "retcode": (None if getattr(res, "retcode", None) is None else int(res.retcode)),
                        "position_type": str(pos.get("type", "")),
                        "position_volume": float(pos_vol),
                        "requested_close_volume": float(close_vol),
                        "executed_close_volume": (None if res.volume is None else float(res.volume)),
                        "remaining_est_volume": max(0.0, float(pos_vol) - float(res.volume or 0.0)),
                        "r_now": float(r_now),
                        "age_min": float(age_min),
                        "price_open": float(open_px),
                        "price_current": float(now_px),
                        "adaptive_pm": adaptive_pm,
                    })
                    if res.ok:
                        self._set_state(account_key, ticket, symbol, {"partial_done": True, "last_action": "partial_close", "last_action_at": _iso(_utc_now()), "partial_volume": float(close_vol)})
                        self._record_action_learning(account_key, out["actions"][-1], pos_rules=pos_rules)
                        if bool(getattr(config, "MT5_PM_FORCE_BE_AFTER_PARTIAL", True)):
                            buffer_r = max(
                                0.0,
                                _safe_float(getattr(config, "MT5_PM_FORCE_BE_AFTER_PARTIAL_BUFFER_R", 0.05), 0.05),
                            )
                            be_after_partial = open_px + (buffer_r * r_dist if is_buy else -buffer_r * r_dist)
                            should_lock = (be_after_partial > sl_px) if is_buy else (be_after_partial < sl_px)
                            if should_lock:
                                be_res = mt5_executor.modify_position_sltp(
                                    broker_symbol=symbol,
                                    position_ticket=ticket,
                                    sl=be_after_partial,
                                    tp=_safe_float(current_tp, 0.0) or None,
                                    source=f"{source}:partial_be",
                                )
                                out["actions"].append({
                                    "ticket": ticket, "symbol": symbol, "action": "breakeven_after_partial", "ok": be_res.ok, "status": be_res.status, "message": be_res.message, "retcode": (None if getattr(be_res, "retcode", None) is None else int(be_res.retcode)),
                                    "position_type": str(pos.get("type", "")),
                                    "old_sl": float(sl_px),
                                    "new_sl": float(be_after_partial),
                                    "tp": (_safe_float(current_tp, 0.0) or None),
                                    "r_now": float(r_now),
                                    "age_min": float(age_min),
                                    "trigger": f"force_be_after_partial buffer_r={buffer_r:.3f}",
                                    "price_open": float(open_px),
                                    "price_current": float(now_px),
                                    "adaptive_pm": adaptive_pm,
                                })
                                if be_res.ok:
                                    self._set_state(
                                        account_key,
                                        ticket,
                                        symbol,
                                        {
                                            "breakeven_done": True,
                                            "partial_done": True,
                                            "last_action": "breakeven_after_partial",
                                            "last_action_at": _iso(_utc_now()),
                                            "last_sl": float(be_after_partial),
                                            "partial_volume": float(close_vol),
                                        },
                                    )
                                    self._record_action_learning(account_key, out["actions"][-1], pos_rules=pos_rules)
                        action_count += 1
                        out["managed"] += 1
                        continue

            # R-based trailing stop.
            if action_count < max_actions and r_now >= trail_start_r_pos:
                trail_gap_eff, trail_dyn = self._dynamic_trail_gap_r(
                    base_gap_r=trail_gap_r_pos,
                    trail_start_r=trail_start_r_pos,
                    r_now=r_now,
                    spread_pct=spread_pct,
                    spread_spike_pct=spread_spike_pct,
                    age_min=age_min,
                )
                trail_sl = now_px - (trail_gap_eff * r_dist) if is_buy else now_px + (trail_gap_eff * r_dist)
                should_move = (trail_sl > sl_px) if is_buy else (trail_sl < sl_px)
                last_sl = _safe_float(pstate.get("last_sl", sl_px), sl_px)
                if should_move:
                    # Avoid tiny update spam.
                    delta = abs(trail_sl - max(last_sl, 0.0))
                    if delta >= max(1e-6, r_dist * 0.08):
                        res = mt5_executor.modify_position_sltp(
                            broker_symbol=symbol,
                            position_ticket=ticket,
                            sl=trail_sl,
                            tp=_safe_float(current_tp, 0.0) or None,
                            source=f"{source}:trail",
                        )
                        out["actions"].append({
                            "ticket": ticket, "symbol": symbol, "action": "trail_sl", "ok": res.ok, "status": res.status, "message": res.message, "retcode": (None if getattr(res, "retcode", None) is None else int(res.retcode)),
                            "position_type": str(pos.get("type", "")),
                            "old_sl": float(sl_px),
                            "prev_tracked_sl": float(last_sl),
                            "new_sl": float(trail_sl),
                            "trail_gap_r_base": float(trail_gap_r_pos),
                            "trail_gap_r_eff": float(trail_gap_eff),
                            "trail_dynamic": trail_dyn,
                            "tp": (_safe_float(current_tp, 0.0) or None),
                            "r_now": float(r_now),
                            "age_min": float(age_min),
                            "spread_pct": float(spread_pct),
                            "price_open": float(open_px),
                            "price_current": float(now_px),
                            "adaptive_pm": adaptive_pm,
                        })
                        if res.ok:
                            self._set_state(account_key, ticket, symbol, {"last_action": "trail_sl", "last_action_at": _iso(_utc_now()), "last_sl": float(trail_sl)})
                            self._record_action_learning(account_key, out["actions"][-1], pos_rules=pos_rules)
                            action_count += 1
                            out["managed"] += 1
                            continue

        out["ok"] = True
        return out

    def build_learning_report(
        self,
        days: int = 30,
        top: int = 8,
        *,
        sync: bool = True,
        symbol: str = "",
        action: str = "",
        regime_split: bool = True,
    ) -> dict:
        """
        Summarize PM action effectiveness by symbol and action from resolved outcomes.
        """
        out = {
            "ok": False,
            "enabled": self.enabled,
            "learning_enabled": bool(getattr(config, "MT5_PM_LEARNING_ENABLED", True)),
            "account_key": "",
            "days": max(1, int(days or 30)),
            "top": max(1, int(top or 8)),
            "sync": None,
            "summary": {},
            "actions_overall": [],
            "symbols": [],
            "rows": [],
            "filters": {},
            "recommendations": [],
            "recommendations_by_regime": [],
            "error": "",
        }
        if not self.enabled:
            out["error"] = "disabled"
            return out
        st = mt5_executor.status()
        if not bool(st.get("connected", False)):
            out["error"] = str(st.get("error") or "mt5 not connected")
            return out
        account_key = self._account_key(st)
        out["account_key"] = account_key
        if not account_key:
            out["error"] = "missing account key"
            return out

        symbol_filter = self.normalize_learning_symbol_filter(symbol)
        action_filter = self.normalize_learning_action_filter(action)
        out["filters"] = {
            "symbol": symbol_filter,
            "action": action_filter,
        }

        if sync and bool(getattr(config, "MT5_PM_LEARNING_ENABLED", True)):
            try:
                out["sync"] = self.sync_learning_outcomes(hours=max(24, out["days"] * 24))
            except Exception as e:
                out["sync"] = {"ok": False, "error": str(e)}

        since_ts = int(_utc_now().timestamp()) - int(out["days"] * 86400)
        try:
            with self._lock:
                with closing(self._connect()) as conn:
                    rows = conn.execute(
                        """
                        SELECT symbol, action, outcome_label, outcome_reason, outcome_pnl, resolved, action_ts, rules_json, spread_pct, adaptive_pm_json
                          FROM mt5_position_mgr_actions
                         WHERE account_key=? AND action_ts>=?
                         ORDER BY action_ts DESC
                        """,
                        (account_key, since_ts),
                    ).fetchall()
        except Exception as e:
            out["error"] = str(e)
            return out

        raw_rows = []
        for r in rows:
            row_symbol = self.normalize_learning_symbol_filter(str(r[0] or ""))
            row_action = self.normalize_learning_action_filter(str(r[1] or ""))
            if symbol_filter and row_symbol != symbol_filter:
                continue
            if action_filter and row_action != action_filter:
                continue
            try:
                rules = dict(json.loads(r[7] or "{}") or {})
            except Exception:
                rules = {}
            try:
                adaptive_pm = dict(json.loads(r[9] or "{}") or {})
            except Exception:
                adaptive_pm = {}
            spread_pct = (_safe_float(r[8], 0.0) if r[8] is not None else None)
            action_ts = _safe_int(r[6], 0)
            raw_rows.append({
                "symbol": row_symbol,
                "action": row_action,
                "outcome_label": str(r[2] or "").lower(),
                "outcome_reason": str(r[3] or "").upper(),
                "outcome_pnl": _safe_float(r[4], 0.0),
                "resolved": bool(_safe_int(r[5], 0)),
                "action_ts": action_ts,
                "spread_pct": spread_pct,
                "adaptive_pm": adaptive_pm,
                "rules": rules,
                "session_regime": self._primary_session_regime(action_ts),
                "spread_regime": self._spread_regime_label({"spread_pct": spread_pct, "rules": rules}),
            })
        out["rows"] = raw_rows

        total = len(raw_rows)
        resolved_rows = [r for r in raw_rows if bool(r.get("resolved"))]
        unresolved = total - len(resolved_rows)
        out["summary"] = {
            "total_actions": total,
            "resolved_actions": len(resolved_rows),
            "unresolved_actions": unresolved,
        }

        def _bucket():
            return {"samples": 0, "resolved": 0, "positive": 0, "negative": 0, "tp": 0, "sl": 0, "pnl_sum": 0.0}

        overall_actions: dict[str, dict] = {}
        symbol_totals: dict[str, dict] = {}
        symbol_action: dict[tuple[str, str], dict] = {}
        for row in raw_rows:
            sym = str(row.get("symbol") or "")
            act = str(row.get("action") or "")
            if not sym or not act:
                continue
            for dkey, key in ((overall_actions, act), (symbol_totals, sym), (symbol_action, (sym, act))):
                b = dkey.setdefault(key, _bucket())
                b["samples"] += 1
                if bool(row.get("resolved")):
                    b["resolved"] += 1
                    pnl = _safe_float(row.get("outcome_pnl", 0.0), 0.0)
                    lab = str(row.get("outcome_label") or "").lower()
                    rsn = str(row.get("outcome_reason") or "").upper()
                    b["pnl_sum"] += pnl
                    pos = (lab in {"tp", "positive"}) or (pnl > 0)
                    neg = (lab in {"sl", "negative"}) or (pnl < 0)
                    if pos:
                        b["positive"] += 1
                    if neg:
                        b["negative"] += 1
                    if rsn == "TP":
                        b["tp"] += 1
                    if rsn == "SL":
                        b["sl"] += 1

        def _finalize(label, b: dict):
            rb = _safe_int(b.get("resolved", 0), 0)
            return {
                "label": label,
                "samples": _safe_int(b.get("samples", 0), 0),
                "resolved": rb,
                "positive_rate": (None if rb <= 0 else round(_safe_float(b.get("positive", 0), 0) / rb, 4)),
                "negative_rate": (None if rb <= 0 else round(_safe_float(b.get("negative", 0), 0) / rb, 4)),
                "tp_rate": (None if rb <= 0 else round(_safe_float(b.get("tp", 0), 0) / rb, 4)),
                "sl_rate": (None if rb <= 0 else round(_safe_float(b.get("sl", 0), 0) / rb, 4)),
                "avg_pnl": (None if rb <= 0 else round(_safe_float(b.get("pnl_sum", 0.0), 0.0) / rb, 6)),
            }

        out["actions_overall"] = sorted(
            [_finalize(k, v) for k, v in overall_actions.items()],
            key=lambda x: (int(x.get("resolved", 0)), int(x.get("samples", 0))),
            reverse=True,
        )

        symbol_rows = []
        for sym, b in symbol_totals.items():
            row = _finalize(sym, b)
            acts = []
            for (s, a), ab in symbol_action.items():
                if s != sym:
                    continue
                ar = _finalize(a, ab)
                acts.append(ar)
            acts.sort(key=lambda x: (int(x.get("resolved", 0)), int(x.get("samples", 0))), reverse=True)
            row["actions"] = acts
            if acts:
                best = [a for a in acts if a.get("positive_rate") is not None]
                if best:
                    row["best_action"] = sorted(best, key=lambda x: (float(x.get("positive_rate") or 0.0), int(x.get("resolved", 0))), reverse=True)[0]
                    row["weak_action"] = sorted(best, key=lambda x: (float(x.get("negative_rate") or 0.0), -int(x.get("resolved", 0))), reverse=True)[0]
            symbol_rows.append(row)
        symbol_rows.sort(
            key=lambda x: (int(x.get("resolved", 0)), int(x.get("samples", 0)), float(x.get("positive_rate") or 0.0)),
            reverse=True,
        )
        out["symbols"] = symbol_rows[: out["top"]]
        try:
            out["recommendations"] = self._build_learning_recommendations(
                raw_rows,
                symbol_filter=symbol_filter,
                action_filter=action_filter,
            )[:6]
        except Exception:
            out["recommendations"] = []
        if regime_split:
            try:
                out["recommendations_by_regime"] = self._build_regime_recommendations(
                    raw_rows,
                    symbol_filter=symbol_filter,
                    action_filter=action_filter,
                )[:8]
            except Exception:
                out["recommendations_by_regime"] = []
        out["ok"] = True
        return out

    def status(self) -> dict:
        st = mt5_executor.status()
        account_key = self._account_key(st)
        total_states = 0
        with self._lock:
            with closing(self._connect()) as conn:
                if account_key:
                    row = conn.execute(
                        "SELECT COUNT(*) FROM mt5_position_mgr_state WHERE account_key=?",
                        (account_key,),
                    ).fetchone()
                    total_states = _safe_int(row[0] if row else 0, 0)
                total_all = conn.execute("SELECT COUNT(*) FROM mt5_position_mgr_state").fetchone()
                total_all = _safe_int(total_all[0] if total_all else 0, 0)
        return {
            "enabled": self.enabled,
            "db_path": str(self.db_path),
            "account_key": account_key,
            "tracked_positions_current": total_states,
            "tracked_positions_all": total_all,
            "manage_manual_positions": bool(getattr(config, "MT5_PM_MANAGE_MANUAL_POSITIONS", True)),
        }


mt5_position_manager = MT5PositionManager()
