import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from analysis.signals import TradeSignal
from scanners.fibo_advance import FiboAdvanceScanner
from scanners.xauusd import XAUUSDScanner


def _make_signal(direction: str = "long", confidence: float = 73.0) -> TradeSignal:
    return TradeSignal(
        symbol="XAUUSD",
        direction=direction,
        confidence=confidence,
        entry=3300.0,
        stop_loss=3298.0 if direction == "long" else 3302.0,
        take_profit_1=3304.0 if direction == "long" else 3296.0,
        take_profit_2=3306.0 if direction == "long" else 3294.0,
        take_profit_3=3308.0 if direction == "long" else 3292.0,
        risk_reward=2.0,
        timeframe="5m+1m",
        session="new_york",
        trend="bullish" if direction == "long" else "bearish",
        rsi=55.0,
        atr=3.0,
        pattern="TEST",
        reasons=[],
        warnings=[],
        raw_scores={},
    )


class ScannerCapturePassthroughTests(unittest.TestCase):
    def test_xau_scanner_does_not_convert_missing_capture_into_low_tick_velocity_block(self):
        scanner = XAUUSDScanner()
        signal = _make_signal("long")
        with patch("scanners.xauusd.autopilot.latest_capture_feature_snapshot", return_value={"ok": False, "status": "capture_missing"}):
            allowed, reason = scanner._check_microstructure_alignment(signal, current_price=3300.0)
        self.assertTrue(allowed)
        self.assertEqual(reason, "micro_capture_unavailable:capture_missing")

    def test_fibo_scanner_does_not_convert_missing_capture_into_low_tick_velocity_block(self):
        scanner = FiboAdvanceScanner()
        allowed, reason = scanner._check_microstructure("long", 73.0, {"ok": False, "status": "capture_missing"})
        self.assertTrue(allowed)
        self.assertEqual(reason, "micro_capture_unavailable:capture_missing")

    def test_xau_scanner_passes_on_insufficient_tick_data(self):
        """ok=True but spots_count=0 depth_count=0 should pass, not reject as low_tick_velocity."""
        scanner = XAUUSDScanner()
        signal = _make_signal("long")
        snapshot = {"ok": True, "status": "ok", "features": {
            "spots_count": 0, "depth_count": 0, "delta_proxy": 0.0,
            "depth_imbalance": 0.0, "bar_volume_proxy": 0.0,
        }}
        with patch("scanners.xauusd.autopilot.latest_capture_feature_snapshot", return_value=snapshot):
            allowed, reason = scanner._check_microstructure_alignment(signal, current_price=3300.0)
        self.assertTrue(allowed)
        self.assertIn("micro_capture_insufficient", reason)

    def test_fibo_scanner_passes_on_insufficient_tick_data(self):
        """ok=True but spots_count=1 depth_count=0 should pass, not reject."""
        scanner = FiboAdvanceScanner()
        snapshot = {"ok": True, "status": "ok", "features": {
            "spots_count": 1, "depth_count": 0, "delta_proxy": 0.0,
            "depth_imbalance": 0.0, "bar_volume_proxy": 0.0,
        }}
        allowed, reason = scanner._check_microstructure("long", 73.0, snapshot)
        self.assertTrue(allowed)
        self.assertIn("micro_capture_insufficient", reason)

    def test_xau_scanner_rejects_low_velocity_with_sufficient_data(self):
        """With enough tick data, low bar_volume_proxy should still reject."""
        scanner = XAUUSDScanner()
        signal = _make_signal("long")
        snapshot = {"ok": True, "status": "ok", "features": {
            "spots_count": 50, "depth_count": 100, "delta_proxy": 0.0,
            "depth_imbalance": 0.0, "bar_volume_proxy": 0.01,
        }}
        with patch("scanners.xauusd.autopilot.latest_capture_feature_snapshot", return_value=snapshot):
            allowed, reason = scanner._check_microstructure_alignment(signal, current_price=3300.0)
        self.assertFalse(allowed)
        self.assertIn("low_tick_velocity", reason)


if __name__ == "__main__":
    unittest.main()
