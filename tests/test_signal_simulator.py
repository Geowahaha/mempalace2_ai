import time
import unittest
from unittest.mock import patch

import learning.signal_simulator as simulator_module
from learning.signal_simulator import SignalSimulator


class SignalSimulatorTests(unittest.TestCase):
    def test_calc_pips_distinguishes_fx_vs_crypto(self):
        self.assertAlmostEqual(SignalSimulator._calc_pips("long", 1.1000, 1.1100, "EURUSD"), 100.0, places=2)
        self.assertAlmostEqual(SignalSimulator._calc_pips("long", 2100.0, 2115.0, "ETHUSD"), 15.0, places=2)

    def test_resolve_scalp_uses_symbol_price_for_eth_and_risk_model(self):
        sim = SignalSimulator()
        now_ts = time.time()
        sig = {
            "id": 101,
            "symbol": "ETHUSD",
            "direction": "long",
            "entry": 2000.0,
            "stop_loss": 1980.0,
            "take_profit_1": 2010.0,
            "take_profit_2": 2020.0,
            "take_profit_3": 2030.0,
            "timestamp": now_ts,
        }
        with patch.object(sim, "_get_current_price", return_value=2015.0) as get_px, \
             patch.object(sim, "_check_hit", return_value="tp1_hit"), \
             patch.object(sim, "_calc_pips", return_value=15.0), \
             patch.object(sim, "_calc_r_multiple", return_value=1.2), \
             patch.object(simulator_module.config, "SIM_RISK_USD_PER_SIGNAL", 10.0), \
             patch.object(simulator_module.scalp_store, "update_outcome") as update_call:
            sim._resolve_scalp_signal(sig, now_ts)

        get_px.assert_called_once_with("ETHUSD")
        update_call.assert_called_once()
        args = update_call.call_args.args
        self.assertEqual(args[0], 101)         # signal id
        self.assertEqual(args[1], "tp1_hit")   # outcome
        self.assertAlmostEqual(float(args[4]), 12.0, places=2)  # pnl_usd = 1.2 * 10

    def test_resolve_scalp_xau_keeps_legacy_pip_value_accounting(self):
        sim = SignalSimulator()
        now_ts = time.time()
        sig = {
            "id": 202,
            "symbol": "XAUUSD",
            "direction": "long",
            "entry": 2100.0,
            "stop_loss": 2090.0,
            "take_profit_1": 2110.0,
            "take_profit_2": 2120.0,
            "take_profit_3": 2130.0,
            "timestamp": now_ts,
        }
        with patch.object(sim, "_get_current_price", return_value=2110.0), \
             patch.object(sim, "_check_hit", return_value="tp1_hit"), \
             patch.object(sim, "_calc_pips", return_value=100.0), \
             patch.object(simulator_module.scalp_store, "update_outcome") as update_call:
            sim._resolve_scalp_signal(sig, now_ts)

        update_call.assert_called_once()
        args = update_call.call_args.args
        # 100 pips * 0.1 USD/pip (per 0.01 lot) = 10 USD
        self.assertAlmostEqual(float(args[4]), 10.0, places=2)


if __name__ == "__main__":
    unittest.main()
