import sqlite3
import tempfile
import time
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from analysis.signals import TradeSignal
from scanners.scalping_scanner import ScalpingScanner


def _mk_signal(*, direction: str, session: str, confidence: float = 74.0) -> TradeSignal:
    return TradeSignal(
        symbol="XAUUSD",
        direction=direction,
        confidence=confidence,
        entry=5100.0,
        stop_loss=5095.0 if direction == "long" else 5105.0,
        take_profit_1=5102.5 if direction == "long" else 5097.5,
        take_profit_2=5105.0 if direction == "long" else 5095.0,
        take_profit_3=5107.0 if direction == "long" else 5093.0,
        risk_reward=1.0,
        timeframe="5m",
        session=session,
        trend="ranging",
        rsi=52.0,
        atr=6.0,
        pattern="SCALP_FLOW_FORCE",
        reasons=[],
        warnings=[],
        raw_scores={},
    )


class ScalpingScannerWinnerLogicTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self._td.name) / "scalp_signal_history.db"
        self._seed_db(self.db_path)
        self.scanner = ScalpingScanner()

    def tearDown(self):
        self._td.cleanup()

    @staticmethod
    def _seed_db(path: Path) -> None:
        now_ts = time.time()
        with sqlite3.connect(str(path)) as conn:
            conn.execute(
                """
                CREATE TABLE scalp_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    symbol TEXT,
                    direction TEXT,
                    session TEXT,
                    confidence REAL,
                    outcome TEXT,
                    pnl_usd REAL
                )
                """
            )
            # Weak/severe lane: long + london
            for i in range(12):
                outcome = "tp1_hit" if i < 2 else "sl_hit"
                pnl = 18.0 if i < 2 else -27.0
                conn.execute(
                    "INSERT INTO scalp_signals(timestamp,symbol,direction,session,confidence,outcome,pnl_usd) VALUES(?,?,?,?,?,?,?)",
                    (now_ts - (i * 60), "XAUUSD", "long", "london", 74.0, outcome, pnl),
                )
            # Strong lane: short + overlap
            for i in range(14):
                outcome = "tp2_hit" if i < 10 else "sl_hit"
                pnl = 22.0 if i < 10 else -11.0
                conn.execute(
                    "INSERT INTO scalp_signals(timestamp,symbol,direction,session,confidence,outcome,pnl_usd) VALUES(?,?,?,?,?,?,?)",
                    (
                        now_ts - 3600 - (i * 60),
                        "XAUUSD",
                        "short",
                        "london, new_york, overlap",
                        72.0,
                        outcome,
                        pnl,
                    ),
                )
            # ETH: strong only on overlap, severe on plain new_york
            for i in range(6):
                outcome = "tp2_hit" if i < 4 else "sl_hit"
                pnl = 7.0 if i < 4 else -4.0
                conn.execute(
                    "INSERT INTO scalp_signals(timestamp,symbol,direction,session,confidence,outcome,pnl_usd) VALUES(?,?,?,?,?,?,?)",
                    (
                        now_ts - 7200 - (i * 60),
                        "ETHUSD",
                        "long",
                        "london, new_york, overlap",
                        73.0,
                        outcome,
                        pnl,
                    ),
                )
            for i in range(3):
                conn.execute(
                    "INSERT INTO scalp_signals(timestamp,symbol,direction,session,confidence,outcome,pnl_usd) VALUES(?,?,?,?,?,?,?)",
                    (
                        now_ts - 10800 - (i * 60),
                        "ETHUSD",
                        "long",
                        "new_york",
                        76.0,
                        "sl_hit",
                        -18.0,
                    ),
                )
            # BTC: strong new_york + 70-74 band
            for i in range(5):
                outcome = "tp2_hit" if i < 4 else "sl_hit"
                pnl = 16.0 if i < 4 else -9.0
                conn.execute(
                    "INSERT INTO scalp_signals(timestamp,symbol,direction,session,confidence,outcome,pnl_usd) VALUES(?,?,?,?,?,?,?)",
                    (
                        now_ts - 14400 - (i * 60),
                        "BTCUSD",
                        "short",
                        "new_york",
                        74.0,
                        outcome,
                        pnl,
                    ),
                )
            conn.commit()

    def test_winner_logic_penalizes_weak_lane(self):
        sig = _mk_signal(direction="long", session="london", confidence=75.0)
        old_rr = float(sig.risk_reward)
        with patch.object(self.scanner, "_winner_db_path", return_value=self.db_path), patch(
            "scanners.scalping_scanner.config.SCALPING_XAU_WINNER_LOGIC_ENABLED", True
        ), patch("scanners.scalping_scanner.config.SCALPING_XAU_WINNER_MIN_SAMPLES", 8):
            info = self.scanner._apply_xau_winner_logic(sig, apply_confidence=True, apply_exits=True)

        self.assertTrue(info.get("applied"))
        self.assertIn(info.get("regime"), {"weak", "severe"})
        self.assertLess(float(sig.confidence), 75.0)
        self.assertLess(float(sig.risk_reward), old_rr)
        self.assertIn("winner_logic_regime", sig.raw_scores)

    def test_winner_logic_boosts_strong_lane(self):
        sig = _mk_signal(direction="short", session="london, new_york, overlap", confidence=72.0)
        old_conf = float(sig.confidence)
        with patch.object(self.scanner, "_winner_db_path", return_value=self.db_path), patch(
            "scanners.scalping_scanner.config.SCALPING_XAU_WINNER_LOGIC_ENABLED", True
        ), patch("scanners.scalping_scanner.config.SCALPING_XAU_WINNER_MIN_SAMPLES", 8):
            info = self.scanner._apply_xau_winner_logic(sig, apply_confidence=True, apply_exits=False)

        self.assertIn(info.get("regime"), {"strong", "neutral"})
        self.assertGreaterEqual(float(sig.confidence), old_conf)
        self.assertEqual(str(sig.raw_scores.get("winner_logic_scope", ""))[:5], "side_")

    def test_apply_xau_multi_tf_context_stamps_h1_and_h4_bias(self):
        sig = _mk_signal(direction="short", session="london, new_york, overlap", confidence=72.0)
        with patch.object(
            self.scanner,
            "_xau_execution_mtf_snapshot",
            return_value={
                "d1_tf": "1d",
                "h1_tf": "1h",
                "h4_tf": "4h",
                "d1_trend": "bullish",
                "h1_trend": "bullish",
                "h4_trend": "bullish",
                "aligned_side": "long",
                "alignment": "aligned_bullish",
                "strict_aligned_side": "long",
                "strict_alignment": "aligned_bullish",
            },
        ):
            self.scanner._apply_xau_multi_tf_context(sig, trigger={"countertrend_confirmed": False})

        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertEqual(str(raw.get("signal_d1_trend") or ""), "bullish")
        self.assertEqual(str(raw.get("signal_h1_trend") or ""), "bullish")
        self.assertEqual(str(raw.get("signal_h4_trend") or ""), "bullish")
        self.assertEqual(str((raw.get("xau_multi_tf_snapshot") or {}).get("aligned_side") or ""), "long")
        self.assertEqual(str((raw.get("xau_multi_tf_snapshot") or {}).get("strict_aligned_side") or ""), "long")
        self.assertFalse(bool(raw.get("xau_mtf_countertrend_confirmed")))

    def test_maybe_force_xau_result_blocks_forced_short_when_reason_is_no_setup_or_m1_not_confirmed(self):
        sig = _mk_signal(direction="short", session="london, new_york, overlap", confidence=60.0)
        with patch("scanners.scalping_scanner.config.SCALPING_XAU_FORCE_EVERY_SCAN", True), \
             patch("scanners.scalping_scanner.config.SCALPING_XAU_FORCE_BLOCK_REASONS", "no_signal,base_scanner_no_signal,no_direction_passed_threshold,m1_short_not_confirmed"), \
             patch.object(self.scanner, "_build_xau_forced_signal", return_value=sig), \
             patch.object(self.scanner, "_apply_xau_m1_entry_advantage"), \
             patch.object(self.scanner, "_retune_xau_exits"), \
             patch.object(self.scanner, "_apply_xau_winner_logic", return_value={}), \
             patch.object(self.scanner, "_apply_xau_multi_tf_context"), \
             patch.object(self.scanner, "_tag_signal"):
            out = self.scanner._maybe_force_xau_result(
                source="scalp_xauusd",
                blocked_status="no_signal",
                blocked_reason="m1_short_not_confirmed",
                signal=None,
                trigger={"xau_diag_status": "no_setup"},
            )

        self.assertIsNone(out)

    def test_maybe_force_xau_result_blocks_forced_long_without_countertrend_confirmation(self):
        sig = _mk_signal(direction="long", session="london, new_york, overlap", confidence=60.0)
        sig.raw_scores["scalp_force_trend_h1"] = "bearish"
        with patch("scanners.scalping_scanner.config.SCALPING_XAU_FORCE_EVERY_SCAN", True), \
             patch("scanners.scalping_scanner.config.SCALPING_XAU_FORCE_REQUIRE_COUNTERTREND_CONFIRMED_LONG", True), \
             patch.object(self.scanner, "_build_xau_forced_signal", return_value=sig), \
             patch.object(self.scanner, "_apply_xau_m1_entry_advantage"), \
             patch.object(self.scanner, "_retune_xau_exits"), \
             patch.object(self.scanner, "_apply_xau_winner_logic", return_value={}), \
             patch.object(self.scanner, "_apply_xau_multi_tf_context"), \
             patch.object(self.scanner, "_tag_signal"):
            out = self.scanner._maybe_force_xau_result(
                source="scalp_xauusd",
                blocked_status="below_confidence",
                blocked_reason="confidence<70.0",
                signal=None,
                trigger={"countertrend_confirmed": False},
            )

        self.assertIsNone(out)

    def test_crypto_weekend_profile_filters_disallowed_session(self):
        import pandas as pd, numpy as np
        _idx = pd.date_range("2026-03-20", periods=60, freq="5min", tz="UTC")
        _df = pd.DataFrame({"open": np.random.uniform(2090, 2110, 60), "high": np.random.uniform(2095, 2115, 60), "low": np.random.uniform(2085, 2105, 60), "close": np.random.uniform(2090, 2110, 60), "volume": np.random.uniform(100, 1000, 60)}, index=_idx)
        sig = TradeSignal(
            symbol="ETHUSD",
            direction="long",
            confidence=78.0,
            entry=2100.0,
            stop_loss=2088.0,
            take_profit_1=2108.0,
            take_profit_2=2116.0,
            take_profit_3=2124.0,
            risk_reward=1.33,
            timeframe="5m",
            session="off_hours",
            trend="bullish",
            rsi=58.0,
            atr=18.0,
            pattern="TREND_CONTINUATION",
            reasons=[],
            warnings=[],
            raw_scores={},
        )
        with patch("scanners.scalping_scanner.config.SCALPING_ENABLED", True), \
             patch("scanners.scalping_scanner.config.scalping_symbol_enabled", return_value=True), \
             patch.object(self.scanner, "_is_weekend_utc", return_value=True), \
             patch("scanners.scalping_scanner.config.get_scalping_eth_allowed_sessions_weekend", return_value={"asian", "london,new_york,overlap"}), \
             patch.object(self.scanner, "_fetch_ctrader_ohlcv", return_value=_df), \
             patch("analysis.signals.SignalGenerator.score_signal", return_value=sig):
            out = self.scanner.scan_eth()

        self.assertEqual(out.status, "session_filtered")
        self.assertIn("session_not_allowed", out.reason)

    def test_crypto_weekend_profile_raises_min_confidence(self):
        import pandas as pd, numpy as np
        _idx = pd.date_range("2026-03-20", periods=60, freq="5min", tz="UTC")
        _df = pd.DataFrame({"open": np.random.uniform(71500, 72500, 60), "high": np.random.uniform(71600, 72600, 60), "low": np.random.uniform(71400, 72400, 60), "close": np.random.uniform(71500, 72500, 60), "volume": np.random.uniform(100, 1000, 60)}, index=_idx)
        sig = TradeSignal(
            symbol="BTCUSD",
            direction="short",
            confidence=73.0,
            entry=72000.0,
            stop_loss=72450.0,
            take_profit_1=71650.0,
            take_profit_2=71400.0,
            take_profit_3=71100.0,
            risk_reward=1.33,
            timeframe="5m",
            session="london, new_york, overlap",
            trend="bearish",
            rsi=42.0,
            atr=320.0,
            pattern="TREND_CONTINUATION",
            reasons=[],
            warnings=[],
            raw_scores={},
        )
        with patch("scanners.scalping_scanner.config.SCALPING_ENABLED", True), \
             patch("scanners.scalping_scanner.config.scalping_symbol_enabled", return_value=True), \
             patch.object(self.scanner, "_is_weekend_utc", return_value=True), \
             patch("scanners.scalping_scanner.config.SCALPING_CRYPTO_WINNER_LOGIC_ENABLED", False), \
             patch("scanners.scalping_scanner.config.SCALPING_BTC_MIN_CONFIDENCE_WEEKEND", 74.0), \
             patch("scanners.scalping_scanner.config.get_scalping_btc_allowed_sessions_weekend", return_value={"london,new_york,overlap"}), \
             patch.object(self.scanner, "_fetch_ctrader_ohlcv", return_value=_df), \
             patch("analysis.signals.SignalGenerator.score_signal", return_value=sig):
            out = self.scanner.scan_btc()

        self.assertEqual(out.status, "below_confidence")
        self.assertEqual(out.reason, "confidence<74.0")

    def test_crypto_winner_logic_penalizes_eth_severe_session(self):
        sig = TradeSignal(
            symbol="ETHUSD",
            direction="long",
            confidence=76.0,
            entry=2100.0,
            stop_loss=2088.0,
            take_profit_1=2108.0,
            take_profit_2=2116.0,
            take_profit_3=2124.0,
            risk_reward=1.33,
            timeframe="5m",
            session="new_york",
            trend="bullish",
            rsi=58.0,
            atr=18.0,
            pattern="TREND_CONTINUATION",
            reasons=[],
            warnings=[],
            raw_scores={},
        )
        with patch.object(self.scanner, "_winner_db_path", return_value=self.db_path):
            info = self.scanner._apply_crypto_winner_logic(sig, apply_confidence=True)

        self.assertIn(info.get("regime"), {"weak", "severe"})
        self.assertLess(float(sig.confidence), 76.0)
        self.assertEqual(str(sig.raw_scores.get("crypto_winner_logic_symbol", "")), "ETHUSD")

    def test_crypto_winner_logic_boosts_btc_strong_session_band(self):
        sig = TradeSignal(
            symbol="BTCUSD",
            direction="short",
            confidence=73.0,
            entry=72000.0,
            stop_loss=72400.0,
            take_profit_1=71600.0,
            take_profit_2=71200.0,
            take_profit_3=70800.0,
            risk_reward=1.5,
            timeframe="5m",
            session="new_york",
            trend="bearish",
            rsi=42.0,
            atr=350.0,
            pattern="TREND_CONTINUATION",
            reasons=[],
            warnings=[],
            raw_scores={},
        )
        with patch.object(self.scanner, "_winner_db_path", return_value=self.db_path):
            info = self.scanner._apply_crypto_winner_logic(sig, apply_confidence=True)

        self.assertEqual(info.get("regime"), "strong")
        self.assertGreater(float(sig.confidence), 73.0)
        self.assertIn("new_york", str(sig.raw_scores.get("crypto_winner_logic_scope", "")))


if __name__ == "__main__":
    unittest.main()
