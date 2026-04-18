import json
import sqlite3
import tempfile
import unittest
import gc
import shutil
from pathlib import Path
from unittest.mock import patch

import execution.ctrader_executor as ctrader_module
from execution.ctrader_executor import CTraderExecutor


def _init_signal_learning_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT,
                source TEXT,
                signal_symbol TEXT,
                broker_symbol TEXT,
                direction TEXT,
                confidence REAL DEFAULT 0,
                risk_reward REAL DEFAULT 0,
                rsi REAL DEFAULT 0,
                atr REAL DEFAULT 0,
                timeframe TEXT DEFAULT '',
                entry REAL DEFAULT 0,
                stop_loss REAL DEFAULT 0,
                take_profit_1 REAL DEFAULT 0,
                take_profit_2 REAL DEFAULT 0,
                take_profit_3 REAL DEFAULT 0,
                pattern TEXT DEFAULT '',
                session TEXT DEFAULT '',
                score_long REAL DEFAULT 0,
                score_short REAL DEFAULT 0,
                score_edge REAL DEFAULT 0,
                mt5_status TEXT DEFAULT '',
                mt5_message TEXT DEFAULT '',
                ticket INTEGER DEFAULT 0,
                position_id INTEGER DEFAULT 0,
                resolved INTEGER DEFAULT 0,
                outcome INTEGER,
                pnl REAL,
                closed_at TEXT,
                extra_json TEXT DEFAULT '{}'
            )
            """
        )


class CTraderLearningContextTests(unittest.TestCase):
    def test_sync_close_preserves_entry_reasons_and_adds_close_resolution(self):
        td = tempfile.mkdtemp()
        try:
            base = Path(td)
            db_path = base / "ctrader_openapi.db"
            learning_db = base / "signal_learning.db"
            executor = None
            try:
                _init_signal_learning_db(learning_db)
                with sqlite3.connect(str(learning_db)) as conn:
                    conn.execute(
                        """
                        INSERT INTO signal_events (
                            created_at, source, signal_symbol, broker_symbol, direction,
                            timeframe, entry, stop_loss, take_profit_1, take_profit_2, take_profit_3,
                            mt5_status, mt5_message, ticket, position_id, resolved, extra_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                        """,
                        (
                            "2026-03-19T00:00:00Z",
                            "scalp_xauusd",
                            "XAUUSD",
                            "XAUUSD",
                            "long",
                            "5m",
                            100.0,
                            99.0,
                            101.0,
                            102.0,
                            103.0,
                            "ctrader_open",
                            "ctrader_reconciled_open",
                            321,
                            321,
                            json.dumps(
                                {
                                    "reasons": ["Fresh sweep rejection"],
                                    "warnings": ["volume_light"],
                                    "raw_scores": {"entry_type": "limit", "gate_reasons": ["volume_light"]},
                                },
                                ensure_ascii=True,
                            ),
                        ),
                    )

                with patch.object(ctrader_module.config, "CTRADER_DB_PATH", str(db_path)):
                    executor = CTraderExecutor()

                executor._sync_signal_event_close(
                    source="scalp_xauusd",
                    symbol="XAUUSD",
                    direction="long",
                    position_id=321,
                    pnl_usd=-4.25,
                    closed_at="2026-03-19T00:10:00Z",
                    extra={
                        "kind": "ctrader_close",
                        "deal": {
                            "execution_price": 99.05,
                            "pnl_usd": -4.25,
                        },
                    },
                )

                with sqlite3.connect(str(learning_db)) as conn:
                    conn.row_factory = sqlite3.Row
                    row = conn.execute(
                        "SELECT resolved, outcome, mt5_status, extra_json FROM signal_events WHERE position_id=321"
                    ).fetchone()

                self.assertEqual(int(row["resolved"]), 1)
                self.assertEqual(int(row["outcome"]), 0)
                self.assertEqual(str(row["mt5_status"]), "ctrader_closed")
                extra = json.loads(str(row["extra_json"]))
                self.assertIn("Fresh sweep rejection", list(extra.get("reasons") or []))
                self.assertEqual(str((extra.get("raw_scores") or {}).get("entry_type") or ""), "limit")
                self.assertEqual(str((extra.get("close_resolution") or {}).get("state") or ""), "sl")
            finally:
                executor = None
                gc.collect()
        finally:
            shutil.rmtree(td, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
