import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from learning.mt5_walkforward import MT5WalkForwardValidator


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class MT5WalkForwardTests(unittest.TestCase):
    def test_build_report_and_canary(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = f"{td}\\auto.db"
            import sqlite3

            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE mt5_execution_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    account_key TEXT NOT NULL,
                    source TEXT,
                    signal_symbol TEXT,
                    broker_symbol TEXT,
                    direction TEXT,
                    confidence REAL,
                    neural_prob REAL,
                    risk_reward REAL,
                    entry REAL,
                    stop_loss REAL,
                    take_profit_2 REAL,
                    order_volume REAL,
                    risk_multiplier REAL,
                    canary_mode INTEGER,
                    mt5_status TEXT NOT NULL,
                    mt5_message TEXT,
                    ticket INTEGER,
                    position_id INTEGER,
                    account_balance REAL,
                    account_equity REAL,
                    account_free_margin REAL,
                    resolved INTEGER NOT NULL DEFAULT 0,
                    outcome INTEGER,
                    pnl REAL,
                    close_reason TEXT,
                    closed_at TEXT,
                    prediction_error REAL,
                    extra_json TEXT
                )
                """
            )
            now = datetime.now(timezone.utc)
            acct = "TEST|1"
            # train window trades (older)
            for i in range(10):
                closed_at = now - timedelta(days=10, hours=i)
                conn.execute(
                    """
                    INSERT INTO mt5_execution_journal(created_at, account_key, mt5_status, resolved, outcome, pnl, closed_at, prediction_error)
                    VALUES (?, ?, 'filled', 1, ?, ?, ?, ?)
                    """,
                    (_iso(closed_at - timedelta(minutes=10)), acct, 1 if i < 6 else 0, 1.0 if i < 6 else -0.5, _iso(closed_at), 0.2),
                )
            # forward window trades (recent)
            for i in range(6):
                closed_at = now - timedelta(days=2, hours=i)
                conn.execute(
                    """
                    INSERT INTO mt5_execution_journal(created_at, account_key, mt5_status, resolved, outcome, pnl, closed_at, prediction_error)
                    VALUES (?, ?, 'filled', 1, ?, ?, ?, ?)
                    """,
                    (_iso(closed_at - timedelta(minutes=10)), acct, 1 if i < 4 else 0, 1.2 if i < 4 else -0.6, _iso(closed_at), 0.18),
                )
            conn.commit()
            conn.close()

            wf = MT5WalkForwardValidator(db_path=db_path)
            rpt = wf.build_report(acct, train_days=30, forward_days=7)
            self.assertTrue(rpt["ok"])
            self.assertEqual(rpt["train"]["trades"], 10)
            self.assertEqual(rpt["forward"]["trades"], 6)
            self.assertIn("risk_multiplier", rpt["canary"])


if __name__ == "__main__":
    unittest.main()

