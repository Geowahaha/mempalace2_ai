import json
import tempfile
import unittest
import time
from unittest.mock import patch

from execution.mt5_executor import MT5ExecutionResult
from learning.mt5_position_manager import MT5PositionManager


class MT5PositionManagerTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.pm = MT5PositionManager(db_path=f"{self._td.name}\\pm.db")

    def tearDown(self):
        self._td.cleanup()

    def test_breakeven_action_on_profitable_position(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
        }
        snap = {
            "connected": True,
            "positions": [
                {
                    "ticket": 1001,
                    "symbol": "ETHUSD",
                    "type": "buy",
                    "volume": 0.01,
                    "price_open": 100.0,
                    "price_current": 104.0,
                    "sl": 95.0,
                    "tp": 120.0,
                    "profit": 0.4,
                    "time": 0,
                    "time_msc": 0,
                    "magic": 0,
                    "comment": "",
                }
            ],
            "orders": [],
        }
        mod_ok = MT5ExecutionResult(ok=True, status="modified", message="ok", broker_symbol="ETHUSD", ticket=1001)
        with patch("learning.mt5_position_manager.config.MT5_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_POSITION_MANAGER_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_MANAGE_ENABLED", True), \
             patch("learning.mt5_position_manager.mt5_executor.status", return_value=status), \
             patch("learning.mt5_position_manager.mt5_executor.open_positions_snapshot", return_value=snap), \
             patch("learning.mt5_position_manager.mt5_executor.modify_position_sltp", return_value=mod_ok) as p_mod, \
             patch("learning.mt5_position_manager.mt5_executor.close_position_partial") as p_close:
            report = self.pm.run_cycle(source="test")
        self.assertTrue(report["ok"])
        self.assertGreaterEqual(report["managed"], 1)
        self.assertTrue(p_mod.called)
        self.assertFalse(p_close.called)

    def test_watch_snapshot_reports_r_and_flags(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
        }
        snap = {
            "connected": True,
            "resolved_symbol": "ETHUSD",
            "positions": [
                {
                    "ticket": 1002,
                    "symbol": "ETHUSD",
                    "type": "buy",
                    "volume": 0.01,
                    "price_open": 100.0,
                    "price_current": 104.0,
                    "sl": 95.0,
                    "tp": 120.0,
                    "profit": 0.4,
                    "time": 0,
                    "time_msc": 0,
                    "magic": 0,
                }
            ],
            "orders": [],
        }
        with patch("learning.mt5_position_manager.config.MT5_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_POSITION_MANAGER_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_MANAGE_ENABLED", True), \
             patch("learning.mt5_position_manager.mt5_executor.status", return_value=status), \
             patch("learning.mt5_position_manager.mt5_executor.open_positions_snapshot", return_value=snap):
            report = self.pm.watch_snapshot(signal_symbol="ETHUSD", limit=5)
        self.assertTrue(report["ok"])
        self.assertEqual(report["resolved_symbol"], "ETHUSD")
        self.assertEqual(report["positions"], 1)
        self.assertEqual(len(report["entries"]), 1)
        row = report["entries"][0]
        self.assertEqual(row["symbol"], "ETHUSD")
        self.assertTrue(row["metrics_valid"])
        self.assertAlmostEqual(float(row["r_now"]), 0.8, places=3)
        self.assertIn("next_checks", row)

    def test_early_risk_tighten_triggers_on_loss_threshold(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
        }
        snap = {
            "connected": True,
            "positions": [
                {
                    "ticket": 1003,
                    "symbol": "ETHUSD",
                    "type": "buy",
                    "volume": 0.01,
                    "price_open": 100.0,
                    "price_current": 96.0,   # -0.8R with SL=95
                    "sl": 95.0,
                    "tp": 120.0,
                    "profit": -0.4,
                    "spread_pct": 0.01,
                    "time": 0,
                    "time_msc": 0,
                    "magic": 0,
                    "comment": "",
                }
            ],
            "orders": [],
        }
        mod_ok = MT5ExecutionResult(ok=True, status="modified", message="ok", broker_symbol="ETHUSD", ticket=1003)
        with patch("learning.mt5_position_manager.config.MT5_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_POSITION_MANAGER_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_MANAGE_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_EARLY_RISK_PROTECT_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_EARLY_RISK_TRIGGER_R", -0.8), \
             patch("learning.mt5_position_manager.config.MT5_PM_EARLY_RISK_SL_R", -0.92), \
             patch("learning.mt5_position_manager.mt5_executor.status", return_value=status), \
             patch("learning.mt5_position_manager.mt5_executor.open_positions_snapshot", return_value=snap), \
             patch("learning.mt5_position_manager.mt5_executor.modify_position_sltp", return_value=mod_ok) as p_mod:
            report = self.pm.run_cycle(source="test")
        self.assertTrue(report["ok"])
        self.assertTrue(p_mod.called)
        self.assertGreaterEqual(report["managed"], 1)
        self.assertEqual(report["actions"][0]["action"], "early_risk_tighten")
        self.assertIn("old_sl", report["actions"][0])
        self.assertIn("new_sl", report["actions"][0])

    def test_adaptive_pm_rules_tighten_for_weak_symbol_history(self):
        base_rules = self.pm._rule_params("TEST-MT5|123")
        pos = {
            "symbol": "ETHUSD",
            "type": "buy",
            "price_open": 100.0,
            "price_current": 98.5,
            "sl": 95.0,
            "spread_pct": 0.20,
        }
        metrics = self.pm._position_r_metrics(pos)
        with patch("learning.mt5_position_manager.config.MT5_PM_ADAPTIVE_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_ADAPTIVE_MIN_SYMBOL_TRADES", 6), \
             patch("learning.mt5_position_manager.mt5_adaptive_trade_planner.symbol_behavior_stats", return_value={
                 "samples": 12,
                 "win_rate": 0.35,
                 "tp_rate": 0.20,
                 "sl_rate": 0.65,
                 "mae": 0.58,
             }):
            rules, info = self.pm._adaptive_rules_for_position(
                "TEST-MT5|123",
                pos,
                base_rules,
                metrics=metrics,
                age_min=45.0,
            )
        self.assertTrue(info["applied"])
        self.assertLessEqual(float(rules["break_even_r"]), float(base_rules["break_even_r"]))
        self.assertLessEqual(float(rules["partial_tp_r"]), float(base_rules["partial_tp_r"]))
        self.assertLessEqual(float(rules["trail_start_r"]), float(base_rules["trail_start_r"]))
        self.assertGreaterEqual(float(rules["trail_gap_r"]), float(base_rules["trail_gap_r"]))
        self.assertGreaterEqual(float(rules["early_risk_trigger_r"]), float(base_rules["early_risk_trigger_r"]))

    def test_pm_learning_sync_and_stats_resolves_actions(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
        }
        self.pm._record_action_learning(
            "TEST-MT5|123",
            {
                "ok": True,
                "ticket": 1005,
                "symbol": "ETHUSD",
                "action": "breakeven",
                "r_now": 0.92,
                "age_min": 22.0,
                "old_sl": 1900.0,
                "new_sl": 1910.0,
            },
            pos_rules={"break_even_r": 0.8, "trail_start_r": 1.2, "spread_spike_pct": 0.18},
        )
        closed = {
            "connected": True,
            "history_query_mode": "ts_int",
            "closed_trades": [
                {
                    "position_id": 1005,
                    "symbol": "ETHUSD",
                    "close_time": int(time.time()) + 60,
                    "reason": "TP",
                    "pnl": 0.42,
                    "closed_at_utc": "2026-02-23 08:00:00 UTC",
                }
            ],
        }
        with patch("learning.mt5_position_manager.config.MT5_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_POSITION_MANAGER_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_LEARNING_ENABLED", True), \
             patch("learning.mt5_position_manager.mt5_executor.status", return_value=status), \
             patch("learning.mt5_position_manager.mt5_executor.closed_trades_snapshot", return_value=closed):
            rep = self.pm.sync_learning_outcomes(hours=48)
            stats = self.pm._pm_learning_stats("TEST-MT5|123", "ETHUSD", lookback_days=30)
        self.assertTrue(rep["ok"])
        self.assertEqual(rep["updated"], 1)
        self.assertEqual(rep["history_query_mode"], "ts_int")
        self.assertGreaterEqual(int(stats.get("samples", 0)), 1)
        self.assertGreaterEqual(float(stats.get("breakeven_positive_rate", 0.0)), 1.0)

    def test_build_learning_report_supports_filters_and_recommendations(self):
        now_ts = int(time.time())
        account_key = "TEST-MT5|123"
        with self.pm._lock:
            conn = self.pm._connect()
            try:
                for i in range(8):
                    # Positive outcomes used with slightly higher trail_start_r than negatives.
                    is_pos = (i % 2 == 0)
                    trail_start_r = 1.4 if is_pos else 1.0
                    rules_json = json.dumps({
                        "trail_start_r": trail_start_r,
                        "trail_gap_r": 0.7 if is_pos else 0.5,
                    })
                    conn.execute(
                        """
                        INSERT INTO mt5_position_mgr_actions(
                            account_key, position_ticket, symbol, action, action_at, action_ts,
                            r_now, age_min, spread_pct, rules_json,
                            outcome_label, outcome_reason, outcome_pnl,
                            outcome_closed_at, outcome_close_ts, outcome_position_id, resolved
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            account_key,
                            2000 + i,
                            "ETHUSD",
                            "trail_sl",
                            "2026-02-23T08:00:00Z",
                            now_ts - (i * 60),
                            1.3,
                            30.0,
                            0.02,
                            rules_json,
                            ("tp" if is_pos else "negative"),
                            ("TP" if is_pos else "MANUAL"),
                            (0.15 if is_pos else -0.10),
                            "2026-02-23T08:30:00Z",
                            now_ts - (i * 60) + 300,
                            2000 + i,
                            1,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
        }
        with patch("learning.mt5_position_manager.config.MT5_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_POSITION_MANAGER_ENABLED", True), \
             patch("learning.mt5_position_manager.mt5_executor.status", return_value=status):
            rpt = self.pm.build_learning_report(days=30, top=5, sync=False, symbol="ETHUSD", action="trail_sl")
        self.assertTrue(rpt["ok"])
        self.assertEqual(rpt["filters"]["symbol"], "ETHUSD")
        self.assertEqual(rpt["filters"]["action"], "trail_sl")
        self.assertGreaterEqual(rpt["summary"]["resolved_actions"], 1)
        self.assertTrue(any(rec.get("key") in {"trail_start_r", "trail_gap_r"} for rec in (rpt.get("recommendations") or [])))
        self.assertTrue(len(list(rpt.get("recommendations_by_regime", []) or [])) >= 1)
        draft = self.pm.build_policy_draft_from_learning_report(rpt)
        self.assertEqual(draft.get("account_key"), "TEST-MT5|123")
        self.assertTrue("global_overrides" in draft)

    def test_dynamic_trail_gap_adjusts_for_progress_spread_and_age(self):
        with patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_STEP_R", 0.8), \
             patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_TIGHTEN_PCT_PER_STEP", 0.12), \
             patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_MAX_TIGHTEN_PCT", 0.35), \
             patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_SPREAD_WIDEN_PCT", 0.18), \
             patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_MAX_WIDEN_PCT", 0.24), \
             patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_YOUNG_AGE_MIN", 6.0), \
             patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_YOUNG_WIDEN_PCT", 0.10), \
             patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_MIN_GAP_R", 0.28), \
             patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_MAX_GAP_R", 1.10):
            gap, info = self.pm._dynamic_trail_gap_r(
                base_gap_r=0.60,
                trail_start_r=1.2,
                r_now=3.0,
                spread_pct=0.36,
                spread_spike_pct=0.18,
                age_min=2.0,
            )
        self.assertTrue(info["applied"])
        self.assertGreater(float(info.get("tighten_pct", 0.0)), 0.0)
        self.assertGreater(float(info.get("widen_pct", 0.0)), 0.0)
        self.assertGreater(float(info.get("young_widen_pct", 0.0)), 0.0)
        self.assertGreaterEqual(float(gap), 0.28)
        self.assertLessEqual(float(gap), 1.10)

    def test_dynamic_trail_gap_returns_base_when_disabled(self):
        with patch("learning.mt5_position_manager.config.MT5_PM_TRAIL_DYNAMIC_ENABLED", False):
            gap, info = self.pm._dynamic_trail_gap_r(
                base_gap_r=0.57,
                trail_start_r=1.2,
                r_now=1.8,
                spread_pct=0.10,
                spread_spike_pct=0.18,
                age_min=20.0,
            )
        self.assertFalse(info["applied"])
        self.assertEqual(float(gap), 0.57)

    def test_partial_close_forces_breakeven_lock_after_success(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
        }
        snap = {
            "connected": True,
            "positions": [
                {
                    "ticket": 1011,
                    "symbol": "XAUUSD",
                    "type": "buy",
                    "volume": 0.10,
                    "price_open": 100.0,
                    "price_current": 104.0,
                    "sl": 95.0,
                    "tp": 112.0,
                    "profit": 0.4,
                    "time": 0,
                    "time_msc": 0,
                    "magic": 0,
                    "comment": "",
                }
            ],
            "orders": [],
        }
        close_ok = MT5ExecutionResult(ok=True, status="partial_closed", message="ok", broker_symbol="XAUUSD", ticket=1011, volume=0.05)
        mod_ok = MT5ExecutionResult(ok=True, status="modified", message="ok", broker_symbol="XAUUSD", ticket=1011)
        with patch("learning.mt5_position_manager.config.MT5_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_POSITION_MANAGER_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_MANAGE_ENABLED", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_BREAK_EVEN_R", 1.5), \
             patch("learning.mt5_position_manager.config.MT5_PM_PARTIAL_TP_R", 0.75), \
             patch("learning.mt5_position_manager.config.MT5_PM_FORCE_BE_AFTER_PARTIAL", True), \
             patch("learning.mt5_position_manager.config.MT5_PM_FORCE_BE_AFTER_PARTIAL_BUFFER_R", 0.05), \
             patch("learning.mt5_position_manager.mt5_executor.status", return_value=status), \
             patch("learning.mt5_position_manager.mt5_executor.open_positions_snapshot", return_value=snap), \
             patch("learning.mt5_position_manager.mt5_executor.close_position_partial", return_value=close_ok) as p_close, \
             patch("learning.mt5_position_manager.mt5_executor.modify_position_sltp", return_value=mod_ok) as p_mod:
            report = self.pm.run_cycle(source="test")

        self.assertTrue(report["ok"])
        self.assertTrue(p_close.called)
        self.assertTrue(p_mod.called)
        actions = list(report.get("actions", []) or [])
        self.assertTrue(any(str(a.get("action")) == "partial_close" for a in actions))
        self.assertTrue(any(str(a.get("action")) == "breakeven_after_partial" for a in actions))


if __name__ == "__main__":
    unittest.main()
