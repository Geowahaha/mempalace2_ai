"""
backtest/results_store.py — Persist backtest run results for comparison.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).parent / "backtest_results.db"


class ResultsStore:
    """SQLite store for backtest run results."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = str(db_path or DEFAULT_DB)
        self._conn: Optional[sqlite3.Connection] = None
        self._ensure_schema()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
        return self._conn

    def _ensure_schema(self) -> None:
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_runs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                run_name        TEXT    NOT NULL,
                strategy        TEXT    NOT NULL DEFAULT '',
                params_json     TEXT    NOT NULL DEFAULT '{}',
                start_date      TEXT,
                end_date        TEXT,
                total_trades    INTEGER NOT NULL DEFAULT 0,
                win_rate        REAL    NOT NULL DEFAULT 0.0,
                total_pnl_r     REAL    NOT NULL DEFAULT 0.0,
                max_drawdown    REAL    NOT NULL DEFAULT 0.0,
                profit_factor   REAL    NOT NULL DEFAULT 0.0,
                report_json     TEXT    NOT NULL DEFAULT '{}',
                created_at      TEXT    NOT NULL
            )
        """)
        conn.commit()

    def save_run(self, report: dict, params: Optional[dict] = None) -> int:
        """Save a backtest run. Returns the row id."""
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "INSERT INTO backtest_runs "
            "(run_name, strategy, params_json, start_date, end_date, "
            "total_trades, win_rate, total_pnl_r, max_drawdown, profit_factor, "
            "report_json, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                str(report.get("run_name", "")),
                str(report.get("strategy", "")),
                json.dumps(params or {}, default=str),
                str(report.get("start_date", "")),
                str(report.get("end_date", "")),
                int(report.get("total_trades", 0)),
                float(report.get("win_rate", 0)),
                float(report.get("total_pnl_r", 0)),
                float(report.get("max_drawdown_r", 0)),
                float(report.get("profit_factor", 0)),
                json.dumps(report, default=str),
                now,
            ),
        )
        conn.commit()
        row_id = cur.lastrowid
        logger.info("[ResultsStore] Saved run #%d: %s", row_id, report.get("run_name", ""))
        return row_id

    def list_runs(self, limit: int = 20) -> List[dict]:
        """List recent backtest runs."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT id, run_name, strategy, total_trades, win_rate, total_pnl_r, "
            "max_drawdown, profit_factor, created_at "
            "FROM backtest_runs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            {
                "id": r[0], "run_name": r[1], "strategy": r[2],
                "total_trades": r[3], "win_rate": r[4], "total_pnl_r": r[5],
                "max_drawdown": r[6], "profit_factor": r[7], "created_at": r[8],
            }
            for r in rows
        ]

    def get_run(self, run_id: int) -> Optional[dict]:
        """Get full report for a specific run."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT report_json FROM backtest_runs WHERE id=?", (run_id,)
        ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
