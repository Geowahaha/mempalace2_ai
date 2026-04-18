import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from analysis.signals import TradeSignal
from learning.mt5_adaptive_trade_planner import MT5AdaptiveTradePlanner


def _iso(dt: datetime) -> str:
    src = dt.astimezone(timezone.utc)
    return src.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_signal(symbol: str = "ETH/USDT", direction: str = "long") -> TradeSignal:
    return TradeSignal(
        symbol=symbol,
        direction=direction,
        confidence=82.0,
        entry=2000.0,
        stop_loss=1950.0 if direction == "long" else 2050.0,
        take_profit_1=2050.0 if direction == "long" else 1950.0,
        take_profit_2=2100.0 if direction == "long" else 1900.0,
        take_profit_3=2150.0 if direction == "long" else 1850.0,
        risk_reward=2.0,
        timeframe="1h",
        session="new_york",
        trend="bullish" if direction == "long" else "bearish",
        rsi=60.0,
        atr=35.0,
        pattern="TEST",
        reasons=[],
        warnings=[],
        raw_scores={},
    )


class MT5AdaptiveTradePlannerTests(unittest.TestCase):
    def _seed_journal(self, db_path: str, *, wins: int, losses: int, symbol: str = "ETHUSD") -> None:
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE mt5_execution_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_key TEXT,
                    resolved INTEGER,
                    closed_at TEXT,
                    broker_symbol TEXT,
                    signal_symbol TEXT,
                    outcome INTEGER,
                    close_reason TEXT,
                    prediction_error REAL,
                    risk_reward REAL,
                    confidence REAL,
                    pnl REAL
                )
                """
            )
            now = datetime.now(timezone.utc)
            rows = []
            for i in range(wins):
                rows.append(
                    (
                        "TEST|123",
                        1,
                        _iso(now - timedelta(hours=i + 1)),
                        symbol,
                        "ETH/USDT",
                        1,
                        "TP",
                        0.10,
                        2.0,
                        82.0,
                        1.2,
                    )
                )
            for i in range(losses):
                rows.append(
                    (
                        "TEST|123",
                        1,
                        _iso(now - timedelta(hours=40 + i)),
                        symbol,
                        "ETH/USDT",
                        0,
                        "SL",
                        0.60,
                        2.0,
                        82.0,
                        -1.0,
                    )
                )
            conn.executemany(
                """
                INSERT INTO mt5_execution_journal(
                    account_key,resolved,closed_at,broker_symbol,signal_symbol,outcome,close_reason,
                    prediction_error,risk_reward,confidence,pnl
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            conn.commit()
        finally:
            conn.close()

    def test_plan_neutral_without_history_still_returns_bounded_values(self):
        with tempfile.TemporaryDirectory() as td:
            planner = MT5AdaptiveTradePlanner(db_path=f"{td}\\missing.db")
            sig = make_signal()
            plan = planner.plan_execution(
                signal=sig,
                account_key="TEST|123",
                broker_symbol="ETHUSD",
                execution_price=2001.0,
                bid=2000.8,
                ask=2001.0,
                point=0.01,
            )
        self.assertTrue(plan.ok)
        self.assertIsNotNone(plan.rr_target)
        self.assertGreaterEqual(float(plan.rr_target), 1.2)
        self.assertLessEqual(float(plan.rr_target), 2.8)
        self.assertGreater(float(plan.size_multiplier), 0.0)

    def test_plan_reduces_size_and_rr_on_poor_symbol_history(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = f"{td}\\ap.db"
            self._seed_journal(db_path, wins=1, losses=8)
            planner = MT5AdaptiveTradePlanner(db_path=db_path)
            sig = make_signal()
            plan = planner.plan_execution(
                signal=sig,
                account_key="TEST|123",
                broker_symbol="ETHUSD",
                execution_price=2001.0,
                bid=2000.7,
                ask=2001.0,
                point=0.01,
            )
        self.assertTrue(plan.ok)
        self.assertGreaterEqual((plan.factors or {}).get("samples", 0), 6)
        self.assertLessEqual(float(plan.size_multiplier), 1.0)
        self.assertLessEqual(float(plan.rr_target), 2.0)


if __name__ == "__main__":
    unittest.main()

