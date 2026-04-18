import tempfile
import unittest
import sqlite3
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import patch

from execution.mt5_executor import MT5ExecutionResult
from learning.mt5_autopilot_core import MT5AutopilotCore


def _signal(symbol="ETH/USDT", direction="long", confidence=78.0):
    return SimpleNamespace(
        symbol=symbol,
        direction=direction,
        confidence=confidence,
        risk_reward=2.0,
        entry=1900.0,
        stop_loss=1880.0,
        take_profit_2=1940.0,
        timeframe="1h",
        session="new_york",
        pattern="OB_BOUNCE",
        raw_scores={"neural_probability": 0.62},
    )


class MT5AutopilotCoreTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.db_path = f"{self._td.name}\\mt5_autopilot_test.db"
        self.core = MT5AutopilotCore(db_path=self.db_path)

    def tearDown(self):
        self._td.cleanup()

    def test_pre_trade_gate_blocks_on_daily_loss_usd(self):
        closed = {
            "connected": True,
            "history_query_mode": "ts_int",
            "closed_trades": [
                {"position_id": 1, "symbol": "ETHUSD", "pnl": -1.2, "close_time": 4102444799, "reason": "SL"}
            ],
        }
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
            "balance": 6.26,
            "equity": 6.26,
            "margin_free": 6.26,
            "currency": "USD",
            "host": "127.0.0.1",
            "port": 18812,
            "leverage": 500,
        }
        open_snap = {"positions": [], "orders": []}
        with patch("learning.mt5_autopilot_core.config.MT5_ENABLED", True), \
             patch("learning.mt5_autopilot_core.config.MT5_AUTOPILOT_ENABLED", True), \
             patch("learning.mt5_autopilot_core.config.MT5_RISK_GOV_LANE_AWARE_ENABLED", False), \
             patch("learning.mt5_autopilot_core.config.MT5_RISK_GOV_DAILY_LOSS_LIMIT_USD", 1.0), \
             patch("learning.mt5_autopilot_core.config.get_mt5_risk_gov_daily_loss_limit_usd_lane_overrides", return_value={}), \
             patch("learning.mt5_autopilot_core.mt5_executor.status", return_value=status), \
             patch("learning.mt5_autopilot_core.mt5_executor.open_positions_snapshot", return_value=open_snap), \
             patch("learning.mt5_autopilot_core.mt5_executor.closed_trades_snapshot", return_value=closed):
            gate = self.core.pre_trade_gate(_signal(), source="test")
        self.assertFalse(gate.allow)
        self.assertEqual(gate.status, "guard_blocked")
        self.assertIn("daily realized loss", gate.reason.lower())

    def test_record_and_sync_outcomes_updates_prediction_error(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
            "balance": 10.0,
            "equity": 10.0,
            "margin_free": 10.0,
            "currency": "USD",
            "host": "127.0.0.1",
            "port": 18812,
            "leverage": 500,
        }
        sig = _signal()
        result = MT5ExecutionResult(
            ok=True,
            status="filled",
            message="order accepted",
            signal_symbol="ETH/USDT",
            broker_symbol="ETHUSD",
            ticket=1001,
            position_id=555,
        )
        with patch("learning.mt5_autopilot_core.config.MT5_ENABLED", True), \
             patch("learning.mt5_autopilot_core.config.MT5_AUTOPILOT_ENABLED", True), \
             patch("learning.mt5_autopilot_core.mt5_executor.status", return_value=status):
            self.core.record_execution(sig, result, source="test")

        closed = {
            "connected": True,
            "history_query_mode": "ts_int",
            "closed_trades": [
                {"position_id": 555, "symbol": "ETHUSD", "pnl": 2.5, "close_time": 4102444799, "reason": "TP"}
            ],
        }
        with patch("learning.mt5_autopilot_core.config.MT5_ENABLED", True), \
             patch("learning.mt5_autopilot_core.config.MT5_AUTOPILOT_ENABLED", True), \
             patch("learning.mt5_autopilot_core.mt5_executor.status", return_value=status), \
             patch("learning.mt5_autopilot_core.mt5_executor.open_positions_snapshot", return_value={"positions": [], "orders": []}), \
             patch("learning.mt5_autopilot_core.mt5_executor.closed_trades_snapshot", return_value=closed):
            sync = self.core.sync_outcomes_from_mt5(hours=72)
            st = self.core.status()

        self.assertTrue(sync.get("ok"))
        self.assertGreaterEqual(int(sync.get("updated", 0) or 0), 1)
        self.assertEqual(st["journal"]["resolved"], 1)
        self.assertEqual(st["calibration"]["labeled_7d"], 1)
        self.assertIsNotNone(st["calibration"]["mae_7d"])

    def test_record_scalping_net_log_accepts_btc_source(self):
        closed_row = {
            "close_time": 4102444799,
            "pnl": 3.5,
            "profit": 4.0,
            "swap": -0.1,
            "commission": -0.4,
        }
        with patch("learning.mt5_autopilot_core.config.SCALPING_NET_LOG_ENABLED", True):
            with closing(sqlite3.connect(self.db_path)) as conn:
                self.core._record_scalping_net_log(
                    conn=conn,
                    journal_id=77,
                    account_key="TEST-MT5|123",
                    source="scalp_btcusd",
                    signal_symbol="BTCUSD",
                    broker_symbol="BTCUSD",
                    position_id=888,
                    ticket=888,
                    opened_at="2099-12-31T23:00:00Z",
                    closed_row=closed_row,
                    close_reason="TP",
                    outcome=1,
                )
                rows = conn.execute(
                    "SELECT source, canonical_symbol, pnl_net_usd FROM mt5_scalping_net_log WHERE journal_id=77"
                ).fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "scalp_btcusd")
        self.assertEqual(rows[0][1], "BTC/USDT")
        self.assertAlmostEqual(float(rows[0][2]), 3.5, places=6)


if __name__ == "__main__":
    unittest.main()
