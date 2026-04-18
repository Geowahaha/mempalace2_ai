"""
execution/stream_reader.py

Read-only interface to the cTrader streaming data.

No Twisted dependency — pure SQLite reads. Safe to import from scheduler,
dashboard, position manager, or any async/sync context.

Usage:
    from execution.stream_reader import StreamReader

    reader = StreamReader()

    # Latest M5 bar for XAUUSD
    bar = reader.get_latest_bar("XAUUSD", "M5")

    # Last N bars for charting
    bars = reader.get_bars("XAUUSD", "M1", limit=100)

    # Margin safety
    alerts = reader.get_margin_alerts(minutes=60)
    status = reader.get_margin_status()

    # Connection health
    connected = reader.is_stream_connected()
    info = reader.get_stream_status()
"""
from __future__ import annotations

import sqlite3
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional


_BASE = Path(__file__).resolve().parent.parent
_DEFAULT_DB = str(_BASE / "data" / "ctrader_openapi.db")


class StreamReader:
    """Read-only SQLite reader for cTrader streaming data."""

    def __init__(self, db_path: Optional[str] = None):
        self._path = db_path or os.getenv("CTRADER_DB_PATH", "") or _DEFAULT_DB

    def _conn(self) -> Optional[sqlite3.Connection]:
        """Open a read-only connection. Returns None if DB doesn't exist."""
        if not os.path.exists(self._path):
            return None
        try:
            conn = sqlite3.connect(self._path, timeout=5)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            return conn
        except Exception:
            return None

    def _has_table(self, conn: sqlite3.Connection, table: str) -> bool:
        """Check if a table exists."""
        try:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            return row is not None
        except Exception:
            return False

    # ── Trendbar queries ─────────────────────────────────────────────────

    def get_latest_bar(self, symbol: str, tf: str) -> Optional[dict]:
        """Get the most recent trendbar for symbol + timeframe.

        Returns dict with keys: symbol, tf, ts_ms, ts_utc, open, high, low,
        close, volume, received_at. Or None if no data.
        """
        conn = self._conn()
        if not conn:
            return None
        try:
            if not self._has_table(conn, "stream_trendbars"):
                return None
            row = conn.execute(
                "SELECT * FROM stream_trendbars "
                "WHERE symbol = ? AND tf = ? "
                "ORDER BY ts_ms DESC LIMIT 1",
                (symbol.upper(), tf.upper()),
            ).fetchone()
            return dict(row) if row else None
        except Exception:
            return None
        finally:
            conn.close()

    def get_bars(self, symbol: str, tf: str, limit: int = 100,
                 since_ms: Optional[int] = None) -> list[dict]:
        """Get recent trendbars, newest first.

        Args:
            symbol: e.g. "XAUUSD"
            tf: e.g. "M5", "M1", "H1"
            limit: max bars to return
            since_ms: only bars with ts_ms >= this value
        """
        conn = self._conn()
        if not conn:
            return []
        try:
            if not self._has_table(conn, "stream_trendbars"):
                return []
            if since_ms:
                rows = conn.execute(
                    "SELECT * FROM stream_trendbars "
                    "WHERE symbol = ? AND tf = ? AND ts_ms >= ? "
                    "ORDER BY ts_ms DESC LIMIT ?",
                    (symbol.upper(), tf.upper(), since_ms, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM stream_trendbars "
                    "WHERE symbol = ? AND tf = ? "
                    "ORDER BY ts_ms DESC LIMIT ?",
                    (symbol.upper(), tf.upper(), limit),
                ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            conn.close()

    def get_bar_coverage(self, symbol: str, tf: str) -> Optional[dict]:
        """Get coverage stats: earliest, latest, count of bars."""
        conn = self._conn()
        if not conn:
            return None
        try:
            if not self._has_table(conn, "stream_trendbars"):
                return None
            row = conn.execute(
                "SELECT MIN(ts_ms) as earliest_ms, MAX(ts_ms) as latest_ms, "
                "COUNT(*) as bar_count "
                "FROM stream_trendbars WHERE symbol = ? AND tf = ?",
                (symbol.upper(), tf.upper()),
            ).fetchone()
            if not row or row["bar_count"] == 0:
                return None
            return {
                "symbol": symbol.upper(),
                "tf": tf.upper(),
                "earliest_ms": row["earliest_ms"],
                "latest_ms": row["latest_ms"],
                "bar_count": row["bar_count"],
                "earliest_utc": _ms_to_iso(row["earliest_ms"]),
                "latest_utc": _ms_to_iso(row["latest_ms"]),
            }
        except Exception:
            return None
        finally:
            conn.close()

    # ── Margin queries ───────────────────────────────────────────────────

    def get_margin_alerts(self, minutes: int = 60) -> list[dict]:
        """Get margin events from the last N minutes."""
        conn = self._conn()
        if not conn:
            return []
        try:
            if not self._has_table(conn, "stream_margin"):
                return []
            cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
            rows = conn.execute(
                "SELECT * FROM stream_margin "
                "WHERE received_at >= ? "
                "ORDER BY received_at DESC",
                (cutoff,),
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            conn.close()

    def get_margin_status(self) -> dict:
        """Get latest margin call events summary."""
        conn = self._conn()
        if not conn:
            return {"has_data": False}
        try:
            if not self._has_table(conn, "stream_margin"):
                return {"has_data": False}
            # Most recent margin call trigger
            trigger = conn.execute(
                "SELECT * FROM stream_margin "
                "WHERE event_type = 'margin_call_trigger' "
                "ORDER BY received_at DESC LIMIT 1",
            ).fetchone()
            # Recent margin changes count
            cutoff_1h = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            change_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM stream_margin "
                "WHERE event_type = 'margin_changed' AND received_at >= ?",
                (cutoff_1h,),
            ).fetchone()

            return {
                "has_data": True,
                "last_trigger": dict(trigger) if trigger else None,
                "margin_changes_1h": change_count["cnt"] if change_count else 0,
            }
        except Exception:
            return {"has_data": False}
        finally:
            conn.close()

    # ── Execution event queries ──────────────────────────────────────────

    def get_recent_executions(self, limit: int = 50) -> list[dict]:
        """Get recent execution events from the stream."""
        conn = self._conn()
        if not conn:
            return []
        try:
            if not self._has_table(conn, "stream_executions"):
                return []
            rows = conn.execute(
                "SELECT * FROM stream_executions "
                "ORDER BY received_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            conn.close()

    # ── Stream status ────────────────────────────────────────────────────

    def is_stream_connected(self) -> bool:
        """Check if the streaming service is currently connected."""
        conn = self._conn()
        if not conn:
            return False
        try:
            if not self._has_table(conn, "stream_status"):
                return False
            row = conn.execute(
                "SELECT connected, updated_at FROM stream_status WHERE id = 1",
            ).fetchone()
            if not row:
                return False
            if not row["connected"]:
                return False
            # Check staleness — if last update > 60s ago, consider disconnected
            try:
                last = datetime.strptime(row["updated_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                age = (datetime.now(timezone.utc) - last).total_seconds()
                return age < 60
            except Exception:
                return bool(row["connected"])
        except Exception:
            return False
        finally:
            conn.close()

    def get_stream_status(self) -> dict:
        """Get full streaming service status."""
        conn = self._conn()
        if not conn:
            return {"running": False, "connected": False, "error": "database not found"}
        try:
            if not self._has_table(conn, "stream_status"):
                return {"running": False, "connected": False, "error": "stream tables not initialized"}
            row = conn.execute(
                "SELECT * FROM stream_status WHERE id = 1",
            ).fetchone()
            if not row:
                return {"running": False, "connected": False}
            status = dict(row)
            # Add staleness check
            try:
                last = datetime.strptime(status["updated_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                age = (datetime.now(timezone.utc) - last).total_seconds()
                status["stale"] = age > 60
                status["last_update_age_sec"] = round(age, 1)
            except Exception:
                status["stale"] = True
                status["last_update_age_sec"] = -1
            status["running"] = bool(status.get("connected") or not status.get("stale", True))
            return status
        except Exception as e:
            return {"running": False, "connected": False, "error": str(e)}
        finally:
            conn.close()

    def get_data_freshness(self) -> dict:
        """Check how fresh the streaming data is for each symbol/tf."""
        conn = self._conn()
        if not conn:
            return {}
        try:
            if not self._has_table(conn, "stream_trendbars"):
                return {}
            rows = conn.execute(
                "SELECT symbol, tf, MAX(ts_ms) as latest_ms, "
                "MAX(received_at) as latest_received, COUNT(*) as total_bars "
                "FROM stream_trendbars GROUP BY symbol, tf",
            ).fetchall()
            result = {}
            now_ms = int(time.time() * 1000)
            for r in rows:
                key = f"{r['symbol']}_{r['tf']}"
                age_sec = (now_ms - r["latest_ms"]) / 1000.0 if r["latest_ms"] else -1
                result[key] = {
                    "symbol": r["symbol"],
                    "tf": r["tf"],
                    "latest_ms": r["latest_ms"],
                    "latest_utc": _ms_to_iso(r["latest_ms"]),
                    "latest_received": r["latest_received"],
                    "total_bars": r["total_bars"],
                    "age_sec": round(age_sec, 1),
                }
            return result
        except Exception:
            return {}
        finally:
            conn.close()


# ── Helpers ──────────────────────────────────────────────────────────────────

import time  # noqa: E402


def _ms_to_iso(ms: Optional[int]) -> str:
    if not ms or ms <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""
