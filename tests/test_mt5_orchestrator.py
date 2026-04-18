import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from learning.mt5_orchestrator import MT5Orchestrator


class MT5OrchestratorTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.orch = MT5Orchestrator(db_path=f"{self._td.name}\\orch.db")

    def tearDown(self):
        self._td.cleanup()

    def test_pre_trade_plan_returns_canary_multiplier(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
            "balance": 10.0,
            "equity": 10.0,
            "margin_free": 10.0,
            "currency": "USD",
        }
        gate = SimpleNamespace(allow=True, status="allowed", reason="ok", account_key="TEST-MT5|123", snapshot={"x": 1})
        wf_dec = SimpleNamespace(
            canary_mode=True,
            canary_reason="insufficient_samples",
            risk_multiplier=0.35,
            train_trades=0,
            forward_trades=0,
            forward_win_rate=0.0,
            forward_mae=None,
        )
        with patch("learning.mt5_orchestrator.mt5_executor.status", return_value=status), \
             patch("learning.mt5_orchestrator.mt5_autopilot_core.pre_trade_gate", return_value=gate), \
             patch("learning.mt5_orchestrator.mt5_walkforward.decision", return_value=wf_dec), \
             patch("learning.mt5_orchestrator.mt5_walkforward.build_report", return_value={"ok": True, "train": {}, "forward": {}, "canary": {}}), \
             patch("learning.mt5_orchestrator.mt5_autopilot_core.status", return_value={"risk_gate": {}, "journal": {}, "calibration": {}}):
            plan = self.orch.pre_trade_plan(signal=SimpleNamespace(raw_scores={}), source="test")
        self.assertTrue(plan.allow)
        self.assertTrue(plan.canary_mode)
        self.assertAlmostEqual(plan.risk_multiplier, 0.35, places=4)

    def test_set_and_show_current_account_policy(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
        }
        with patch("learning.mt5_orchestrator.mt5_executor.status", return_value=status):
            rep = self.orch.set_current_account_policy("canary_force", "false")
            self.assertTrue(rep.get("ok"))
            self.assertEqual(rep.get("updated_key"), "canary_force")
            self.assertIs(rep.get("updated_value"), False)
            show = self.orch.current_account_policy()
            self.assertTrue(show.get("ok"))
            self.assertIs(show["policy"]["canary_force"], False)

    def test_pre_trade_plan_passes_policy_risk_overrides_to_gate(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
            "balance": 10.0,
            "equity": 10.0,
            "margin_free": 10.0,
            "currency": "USD",
        }
        gate = SimpleNamespace(allow=True, status="allowed", reason="ok", account_key="TEST-MT5|123", snapshot={"x": 1})
        wf_dec = SimpleNamespace(
            canary_mode=True,
            canary_reason="insufficient_samples",
            risk_multiplier=0.35,
            train_trades=0,
            forward_trades=0,
            forward_win_rate=0.0,
            forward_mae=None,
        )
        gate_calls = []

        def _gate(*args, **kwargs):
            gate_calls.append(kwargs)
            return gate

        with patch("learning.mt5_orchestrator.mt5_executor.status", return_value=status), \
             patch("learning.mt5_orchestrator.mt5_autopilot_core.pre_trade_gate", side_effect=_gate), \
             patch("learning.mt5_orchestrator.mt5_walkforward.decision", return_value=wf_dec), \
             patch("learning.mt5_orchestrator.mt5_walkforward.build_report", return_value={"ok": True, "train": {}, "forward": {}, "canary": {}}), \
             patch("learning.mt5_orchestrator.mt5_autopilot_core.status", return_value={"risk_gate": {}, "journal": {}, "calibration": {}}):
            self.orch.set_current_account_policy("daily_loss_limit_usd", "0.5")
            plan = self.orch.pre_trade_plan(signal=SimpleNamespace(raw_scores={}), source="test")
        self.assertTrue(plan.allow)
        self.assertGreaterEqual(len(gate_calls), 1)
        overrides = gate_calls[-1].get("policy_overrides", {}) or {}
        self.assertEqual(overrides.get("daily_loss_limit_usd"), 0.5)

    def test_apply_preset_sets_policy_values(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
        }
        with patch("learning.mt5_orchestrator.mt5_executor.status", return_value=status):
            rep = self.orch.apply_current_account_preset("micro_safe")
            self.assertTrue(rep.get("ok"))
            show = self.orch.current_account_policy()
            self.assertTrue(show.get("ok"))
            pol = show.get("policy", {})
            self.assertIs(pol.get("position_manager_enabled"), True)
            self.assertIs(pol.get("canary_force"), True)
            self.assertLess(float(pol.get("pm_early_risk_trigger_r")), 0.0)
            self.assertLess(float(pol.get("pm_early_risk_sl_r")), 0.0)

    def test_pre_trade_plan_applies_symbol_specific_canary_and_risk_overrides(self):
        status = {
            "enabled": True,
            "connected": True,
            "account_login": 123,
            "account_server": "TEST-MT5",
            "balance": 10.0,
            "equity": 10.0,
            "margin_free": 10.0,
            "currency": "USD",
        }
        gate = SimpleNamespace(allow=True, status="allowed", reason="ok", account_key="TEST-MT5|123", snapshot={"x": 1})
        wf_dec = SimpleNamespace(
            canary_mode=True,
            canary_reason="wf_pass",
            risk_multiplier=0.90,
            train_trades=10,
            forward_trades=3,
            forward_win_rate=0.66,
            forward_mae=0.3,
        )
        sig = SimpleNamespace(symbol="ETH/USDT", raw_scores={})

        with patch("learning.mt5_orchestrator.mt5_executor.status", return_value=status), \
             patch("learning.mt5_orchestrator.mt5_executor.resolve_symbol", return_value="ETHUSD"), \
             patch("learning.mt5_orchestrator.mt5_autopilot_core.pre_trade_gate", return_value=gate), \
             patch("learning.mt5_orchestrator.mt5_walkforward.decision", return_value=wf_dec), \
             patch("learning.mt5_orchestrator.mt5_walkforward.build_report", return_value={"ok": True, "train": {}, "forward": {}, "canary": {}}), \
             patch("learning.mt5_orchestrator.mt5_autopilot_core.status", return_value={"risk_gate": {}, "journal": {}, "calibration": {}}), \
             patch("learning.mt5_orchestrator.config.get_mt5_canary_force_symbol_overrides", return_value={"ETHUSD": False}), \
             patch("learning.mt5_orchestrator.config.get_mt5_risk_multiplier_symbol_overrides", return_value={"ETHUSD": 0.42}), \
             patch("learning.mt5_orchestrator.config.get_mt5_risk_multiplier_min_symbol_overrides", return_value={}), \
             patch("learning.mt5_orchestrator.config.get_mt5_risk_multiplier_max_symbol_overrides", return_value={}):
            plan = self.orch.pre_trade_plan(signal=sig, source="crypto")

        self.assertTrue(plan.allow)
        self.assertFalse(plan.canary_mode)
        self.assertAlmostEqual(float(plan.risk_multiplier), 0.42, places=4)
        self.assertIn("symbol_policy_overrides", plan.walkforward)
        self.assertIn("risk_multiplier_fixed", plan.walkforward["symbol_policy_overrides"])


if __name__ == "__main__":
    unittest.main()
