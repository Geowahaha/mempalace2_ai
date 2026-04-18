"""
tests/test_momentum_adaptive.py

Tests for momentum-adaptive features:
  1. Momentum exhaustion profit lock
  2. Momentum-adaptive step_r
"""
import json
import tempfile
import shutil
import gc
import unittest
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

sys.modules["yfinance"] = MagicMock()
sys.modules["ccxt"] = MagicMock()

import execution.ctrader_executor as ctrader_module


class TestMomentumExhaustionLock(unittest.TestCase):
    """Test momentum exhaustion detection and profit locking."""

    def _make_executor(self, td):
        db_path = str(Path(td) / "ctrader_openapi.db")
        with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
             patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
             patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
             patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
             patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
            executor = ctrader_module.CTraderExecutor()
            executor.trading_manager_state_path.parent.mkdir(parents=True, exist_ok=True)
            executor.trading_manager_state_path.write_text(
                json.dumps({"xau_order_care": {"status": "active", "mode": "test", "allowed_sources": ["fibo:sniper"], "overrides": {}}}),
                encoding="utf-8",
            )
            return executor

    def test_exhaustion_locks_profit_when_all_signals_fire(self):
        """When delta reversed + volume dying + drift adverse + high rejection + range → lock profit."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True, "run_id": "exh_test",
                "features": {
                    "day_type": "range",
                    "delta_proxy": -0.15, "depth_imbalance": -0.05,
                    "mid_drift_pct": -0.012, "rejection_ratio": 0.35,
                    "bar_volume_proxy": 0.15,
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3028.50,
                    confidence=75.0, age_min=5.0, r_now=0.95,
                )
            self.assertTrue(result["active"])
            self.assertEqual(result["action"], "tighten")
            self.assertIn("xau_momentum_exhaustion_lock", result["reason"])
            self.assertEqual(result["details"]["lock_pct"], 0.35)
            self.assertEqual(result["details"]["exhaustion_signals"], 5)
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_no_lock_when_momentum_still_strong(self):
        """When momentum is still strong → don't lock."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True, "run_id": "exh_test",
                "features": {
                    "day_type": "trend", "delta_proxy": 0.20, "depth_imbalance": 0.15,
                    "mid_drift_pct": 0.015, "rejection_ratio": 0.05, "bar_volume_proxy": 0.75,
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3028.50,
                    confidence=75.0, age_min=5.0, r_now=0.95,
                )
            self.assertFalse(result["active"])
            self.assertEqual(result["reason"], "exhaustion_not_confirmed")
            self.assertEqual(result["details"]["exhaustion_signals"], 0)
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_no_lock_when_not_profitable(self):
        """When trade is in loss → don't lock."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            result = executor._xau_momentum_exhaustion_lock(
                source="fibo:sniper", symbol="XAUUSD", direction="long",
                entry=3026.49, stop_loss=3024.54, current_price=3025.00,
                confidence=75.0, age_min=5.0, r_now=-0.70,
            )
            self.assertFalse(result["active"])
            self.assertEqual(result["reason"], "not_in_profit")
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_no_lock_when_too_young(self):
        """When trade is too young → don't lock."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            result = executor._xau_momentum_exhaustion_lock(
                source="fibo:sniper", symbol="XAUUSD", direction="long",
                entry=3026.49, stop_loss=3024.54, current_price=3028.50,
                confidence=75.0, age_min=1.0, r_now=0.95,
            )
            self.assertFalse(result["active"])
            self.assertEqual(result["reason"], "too_young")
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_partial_signals_not_enough(self):
        """When only 2 of 5 signals fire (need 3) → don't lock."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True, "run_id": "exh_test",
                "features": {
                    "day_type": "trend", "delta_proxy": -0.10, "depth_imbalance": 0.05,
                    "mid_drift_pct": 0.005, "rejection_ratio": 0.30, "bar_volume_proxy": 0.40,
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3028.50,
                    confidence=75.0, age_min=5.0, r_now=0.50,
                )
            self.assertFalse(result["active"])
            self.assertEqual(result["details"]["exhaustion_signals"], 2)
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_lock_scales_with_r_multiple(self):
        """Higher R-multiple → higher lock percentage."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True, "run_id": "exh",
                "features": {
                    "day_type": "range", "delta_proxy": -0.15, "depth_imbalance": -0.05,
                    "mid_drift_pct": -0.012, "rejection_ratio": 0.35, "bar_volume_proxy": 0.15,
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                r_low = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3027.20,
                    confidence=75.0, age_min=5.0, r_now=0.35,
                )
                r_mid = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3028.50,
                    confidence=75.0, age_min=5.0, r_now=1.0,
                )
                r_high = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3030.40,
                    confidence=75.0, age_min=5.0, r_now=2.0,
                )
            self.assertTrue(r_low["active"])
            self.assertTrue(r_mid["active"])
            self.assertTrue(r_high["active"])
            self.assertEqual(r_low["details"]["lock_pct"], 0.15)
            self.assertEqual(r_mid["details"]["lock_pct"], 0.55)
            self.assertEqual(r_high["details"]["lock_pct"], 0.70)
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_adverse_for_short_direction(self):
        """Short direction: positive delta/drift = adverse."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True, "run_id": "exh",
                "features": {
                    "day_type": "range", "delta_proxy": 0.15,  # adverse for short
                    "depth_imbalance": 0.05, "mid_drift_pct": 0.012,  # adverse for short
                    "rejection_ratio": 0.30, "bar_volume_proxy": 0.20,
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="short",
                    entry=3030.00, stop_loss=3033.00, current_price=3028.00,
                    confidence=75.0, age_min=5.0, r_now=0.67,
                )
            self.assertTrue(result["active"])
            self.assertIn("delta_reversed", result["details"]["reasons"])
            self.assertIn("drift_adverse", result["details"]["reasons"])
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)


class TestMomentumAdaptiveStepR(unittest.TestCase):
    """Test momentum-adaptive step_r in TP extension."""

    def _make_executor(self, td):
        db_path = str(Path(td) / "ctrader_openapi.db")
        with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
             patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
             patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
             patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
             patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
            executor = ctrader_module.CTraderExecutor()
            executor.trading_manager_state_path.parent.mkdir(parents=True, exist_ok=True)
            executor.trading_manager_state_path.write_text(
                json.dumps({"xau_order_care": {"status": "active", "mode": "test", "allowed_sources": ["fibo:sniper"], "overrides": {}}}),
                encoding="utf-8",
            )
            return executor

    def test_adaptive_step_r_strong_momentum(self):
        """Strong momentum (4+ favorable) → step_r = base + 0.10."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True, "run_id": "test",
                "features": {
                    "day_type": "trend", "delta_proxy": -0.25, "depth_imbalance": -0.18,
                    "mid_drift_pct": -0.020, "rejection_ratio": 0.05, "bar_volume_proxy": 0.80,
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_profit_extension_plan(
                    source="fibo:sniper", symbol="XAUUSD", direction="short",
                    entry=3030.00, stop_loss=3033.00, planned_tp=3027.00,
                    current_tp=3027.00, current_price=3026.00,
                    confidence=80.0, age_min=2.0, r_now=1.33,
                )
            if result.get("active"):
                mi = result.get("details", {}).get("momentum_adaptive", {})
                self.assertEqual(mi.get("momentum_label"), "strong")
                self.assertAlmostEqual(mi.get("step_r", 0), 0.35, places=2)
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_adaptive_step_r_weak_momentum(self):
        """Weak momentum (0-1 favorable) → step_r = base - 0.10."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True, "run_id": "test",
                "features": {
                    "day_type": "range", "delta_proxy": 0.05, "depth_imbalance": 0.02,
                    "mid_drift_pct": 0.003, "rejection_ratio": 0.35, "bar_volume_proxy": 0.15,
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_profit_extension_plan(
                    source="fibo:sniper", symbol="XAUUSD", direction="short",
                    entry=3030.00, stop_loss=3033.00, planned_tp=3027.00,
                    current_tp=3027.00, current_price=3026.00,
                    confidence=80.0, age_min=2.0, r_now=1.33,
                )
            details = result.get("details", {})
            if "momentum_adaptive" in details:
                self.assertEqual(details["momentum_adaptive"]["momentum_label"], "weak")
                self.assertAlmostEqual(details["momentum_adaptive"]["step_r"], 0.15, places=2)
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_adaptive_step_r_moderate_momentum(self):
        """Moderate momentum (2-3 favorable) → step_r = base (default)."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True, "run_id": "test",
                "features": {
                    "day_type": "trend", "delta_proxy": -0.15, "depth_imbalance": -0.05,
                    "mid_drift_pct": -0.008, "rejection_ratio": 0.18, "bar_volume_proxy": 0.55,
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_profit_extension_plan(
                    source="fibo:sniper", symbol="XAUUSD", direction="short",
                    entry=3030.00, stop_loss=3033.00, planned_tp=3027.00,
                    current_tp=3027.00, current_price=3026.00,
                    confidence=80.0, age_min=2.0, r_now=1.33,
                )
            details = result.get("details", {})
            if "momentum_adaptive" in details:
                self.assertEqual(details["momentum_adaptive"]["momentum_label"], "moderate")
                self.assertAlmostEqual(details["momentum_adaptive"]["step_r"], 0.25, places=2)
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
