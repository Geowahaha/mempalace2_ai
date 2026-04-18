"""
learning/scalping_runtime.py

Runtime utilities for dedicated scalping flow:
- Time-based fast close manager (scalping-only positions)
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config import config
from execution.mt5_executor import mt5_executor


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    src = dt if isinstance(dt, datetime) else _utc_now()
    if src.tzinfo is None:
        src = src.replace(tzinfo=timezone.utc)
    return src.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(v: str) -> Optional[datetime]:
    raw = str(v or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _safe_int(v, fallback: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(fallback)


def _safe_float(v, fallback: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(fallback)


class ScalpingTimeoutManager:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(__file__).resolve().parent.parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        cfg = str(getattr(config, "MT5_AUTOPILOT_DB_PATH", "") or "").strip()
        self.db_path = Path(db_path or cfg or (data_dir / "mt5_autopilot.db"))
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
                    CREATE TABLE IF NOT EXISTS mt5_scalping_timeout_actions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT NOT NULL,
                        journal_id INTEGER,
                        source TEXT,
                        signal_symbol TEXT,
                        broker_symbol TEXT,
                        ticket INTEGER,
                        age_min REAL,
                        action_status TEXT,
                        action_message TEXT,
                        retcode INTEGER,
                        ok INTEGER
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_scalp_timeout_created ON mt5_scalping_timeout_actions(created_at)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_scalp_timeout_ticket ON mt5_scalping_timeout_actions(ticket)"
                )
                conn.commit()

    @staticmethod
    def _scalping_sources() -> tuple[str, str, str]:
        return ("scalp_xauusd", "scalp_ethusd", "scalp_btcusd")

    def _fetch_open_scalping_journal_rows(self) -> list[dict]:
        out: list[dict] = []
        sources = self._scalping_sources()
        placeholders = ",".join(["?"] * len(sources))
        with self._lock:
            with closing(self._connect()) as conn:
                cur = conn.execute(
                    f"""
                    SELECT id, created_at, source, signal_symbol, broker_symbol, ticket, position_id
                      FROM mt5_execution_journal
                     WHERE resolved=0
                       AND mt5_status='filled'
                       AND source IN ({placeholders})
                     ORDER BY created_at ASC
                     LIMIT 250
                    """,
                    tuple(sources),
                )
                rows = cur.fetchall()
        for rid, created_at, source, signal_symbol, broker_symbol, ticket, position_id in rows:
            pos_ticket = _safe_int(position_id, 0) or _safe_int(ticket, 0)
            out.append(
                {
                    "journal_id": _safe_int(rid, 0),
                    "created_at": str(created_at or ""),
                    "source": str(source or ""),
                    "signal_symbol": str(signal_symbol or ""),
                    "broker_symbol": str(broker_symbol or ""),
                    "ticket": _safe_int(ticket, 0),
                    "position_id": _safe_int(position_id, 0),
                    "position_ticket": _safe_int(pos_ticket, 0),
                }
            )
        return out

    def _record_action(self, row: dict, *, age_min: float, status: str, message: str, ok: bool, retcode: Optional[int]) -> None:
        with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO mt5_scalping_timeout_actions(
                        created_at, journal_id, source, signal_symbol, broker_symbol, ticket,
                        age_min, action_status, action_message, retcode, ok
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _iso(_utc_now()),
                        _safe_int(row.get("journal_id"), 0),
                        str(row.get("source", "") or ""),
                        str(row.get("signal_symbol", "") or ""),
                        str(row.get("broker_symbol", "") or ""),
                        _safe_int(row.get("position_ticket"), 0),
                        round(float(age_min), 3),
                        str(status or ""),
                        str(message or "")[:400],
                        (None if retcode is None else _safe_int(retcode, 0)),
                        1 if bool(ok) else 0,
                    ),
                )
                conn.commit()

    def run_cycle(self, *, timeout_min: Optional[int] = None) -> dict:
        out = {
            "ok": False,
            "enabled": bool(getattr(config, "SCALPING_ENABLED", False)) and bool(getattr(config, "MT5_ENABLED", False)),
            "timeout_min": max(1, int(timeout_min if timeout_min is not None else getattr(config, "SCALPING_CLOSE_TIMEOUT_MIN", 35))),
            "rows": 0,
            "eligible": 0,
            "close_attempted": 0,
            "close_ok": 0,
            "actions": [],
            "error": "",
            "error_detail": "",
        }
        if not out["enabled"]:
            out["error"] = "disabled"
            return out

        status = mt5_executor.status()
        if not bool(status.get("connected", False)):
            out["error"] = "mt5_not_connected"
            out["error_detail"] = str(status.get("error") or "").strip()
            return out

        rows = self._fetch_open_scalping_journal_rows()
        out["rows"] = len(rows)
        if not rows:
            out["ok"] = True
            return out

        snap = mt5_executor.open_positions_snapshot(limit=300)
        positions = list(snap.get("positions", []) or [])
        by_ticket = {}
        for p in positions:
            t = _safe_int(p.get("ticket"), 0)
            if t > 0:
                by_ticket[t] = dict(p)

        now_dt = _utc_now()
        for row in rows:
            created_dt = _parse_iso(row.get("created_at", "")) or now_dt
            age_min = max(0.0, (now_dt - created_dt).total_seconds() / 60.0)
            if age_min < float(out["timeout_min"]):
                continue
            out["eligible"] += 1

            pticket = _safe_int(row.get("position_ticket"), 0)
            pos = by_ticket.get(pticket)
            if not pos:
                continue

            broker_symbol = str(pos.get("symbol") or row.get("broker_symbol") or "")
            position_type = str(pos.get("type") or "")
            position_volume = _safe_float(pos.get("volume"), 0.0)
            if not broker_symbol or position_volume <= 0:
                continue

            out["close_attempted"] += 1
            res = mt5_executor.close_position_partial(
                broker_symbol=broker_symbol,
                position_ticket=pticket,
                position_type=position_type,
                position_volume=position_volume,
                close_volume=position_volume,
                source="scalping_timeout",
            )
            action = {
                "source": str(row.get("source", "")),
                "signal_symbol": str(row.get("signal_symbol", "")),
                "broker_symbol": broker_symbol,
                "ticket": pticket,
                "age_min": round(float(age_min), 2),
                "status": str(getattr(res, "status", "") or ""),
                "ok": bool(getattr(res, "ok", False)),
                "message": str(getattr(res, "message", "") or ""),
                "retcode": getattr(res, "retcode", None),
            }
            if action["ok"]:
                out["close_ok"] += 1
            out["actions"].append(action)
            self._record_action(
                row,
                age_min=float(age_min),
                status=action["status"],
                message=action["message"],
                ok=bool(action["ok"]),
                retcode=action.get("retcode"),
            )

        out["ok"] = True
        return out


scalping_timeout_manager = ScalpingTimeoutManager()
