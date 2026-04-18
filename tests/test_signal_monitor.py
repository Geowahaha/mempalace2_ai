import os
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from api.scalp_signal_store import ScalpSignalRecord, ScalpSignalStore
from api.signal_store import SignalStore
from notifier.admin_bot import TelegramAdminBot


class SignalMonitorIntentTests(unittest.TestCase):
    def test_parse_signal_monitor_args_normalizes_symbol_and_window(self):
        parsed = TelegramAdminBot._parse_signal_monitor_args("ETHUSDT this week")
        self.assertEqual(parsed.get("symbol"), "ETHUSD")
        self.assertEqual(parsed.get("symbols"), ["ETHUSD"])
        self.assertEqual(parsed.get("window_mode"), "this_week")

    def test_parse_signal_monitor_args_supports_multi_symbol(self):
        parsed = TelegramAdminBot._parse_signal_monitor_args("XAUUSD ETHUSD today")
        self.assertEqual(parsed.get("symbols"), ["XAUUSD", "ETHUSD"])

    def test_resolve_local_intent_signal_monitor_phrase(self):
        bot = TelegramAdminBot()
        intent = bot._resolve_local_intent("signal monitor gold today", lang="en")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("mode"), "run")
        self.assertEqual(intent.get("command"), "signal_monitor")

    def test_format_signal_monitor_text_contains_compact_perf_lines(self):
        bot = TelegramAdminBot()
        payload = {
            "symbol": "XAUUSD",
            "window_mode": "today",
            "days": 1,
            "session": "london",
            "status": "no_signal",
            "tf": "H1/M15/M5",
            "price": 5183.19,
            "unmet": ["base_setup", "behavioral_fallback"],
            "notes": ["signal_generator_returned_none", "fallback:no_direction_passed_threshold"],
            "main_stats": {"completed_signals": 10, "wins": 8, "losses": 2, "total_pnl_usd": 100.0},
            "scalp_stats": {"count": 20, "wins": 19, "losses": 1, "total_usd": 200.0},
            "mt5_exec_stats": {
                "enabled": True,
                "available": True,
                "sent": 12,
                "filled": 4,
                "skipped": 6,
                "guard_blocked": 2,
                "fill_rate_pct": 33.3,
                "top_block_reason": "neural filter",
                "lanes": {
                    "main": {"sent": 8, "filled": 3, "skipped": 4, "guard_blocked": 1},
                    "winner": {"sent": 2, "filled": 1, "skipped": 1, "guard_blocked": 0},
                    "bypass": {"sent": 2, "filled": 0, "skipped": 1, "guard_blocked": 1},
                },
            },
        }
        out = bot._format_signal_monitor_text(payload, lang="en")
        self.assertIn("[$5183.19] [", out)
        self.assertIn("Price: $5183.19", out)
        self.assertIn("Signal closed (model) 10 W8/L2 Profit=100$", out)
        self.assertIn("Scalp closed (model) 20 W19/L1 Profit=200$", out)
        self.assertIn("Track (model): Signal sent", out)
        self.assertIn("MT5 lanes:", out)

    def test_build_signal_monitor_payload_dedupes_duplicate_fallback_note(self):
        bot = TelegramAdminBot()
        diag = {
            "status": "no_signal",
            "unmet": ["base_setup", "behavioral_fallback"],
            "notes": ["signal_generator_returned_none", "fallback:no_direction_passed_threshold"],
            "fallback": {"reason": "no_direction_passed_threshold"},
            "current_price": 2500.0,
        }
        with patch("scanners.xauusd.xauusd_scanner.scan", return_value=None), \
             patch("scanners.xauusd.xauusd_scanner.get_last_scan_diagnostics", return_value=diag), \
             patch("market.data_fetcher.xauusd_provider.get_current_price", return_value=2500.0), \
             patch("market.data_fetcher.session_manager.get_session_info", return_value={"active_sessions": ["asian"]}):
            payload = bot._build_signal_monitor_payload("XAUUSD", window_mode="today", days=1)
        notes = list(payload.get("notes") or [])
        self.assertEqual(notes.count("fallback:no_direction_passed_threshold"), 1)

    @unittest.skip("fx_provider disabled — system uses cTrader OpenAPI only")
    def test_build_signal_monitor_payload_supports_gbpusd_fx(self):
        bot = TelegramAdminBot()
        with patch("scanners.fx_major_scanner.fx_major_scanner.scan", return_value=[]), \
             patch("scanners.fx_major_scanner.fx_major_scanner.get_last_scan_diagnostics", return_value={"reject_reasons": {"no_signal": 1}}), \
             patch("market.data_fetcher.fx_provider.get_current_price", return_value=1.2712), \
             patch("market.data_fetcher.session_manager.get_session_info", return_value={"active_sessions": ["london"]}):
            payload = bot._build_signal_monitor_payload("GBPUSD", window_mode="today", days=1)
        self.assertEqual(payload.get("status"), "no_signal")
        self.assertNotEqual(payload.get("status"), "unsupported_symbol")
        self.assertIn("fx_reject:no_signal", list(payload.get("notes") or []))


class FilteredStatsTests(unittest.TestCase):
    def setUp(self):
        fd1, self.signal_db = tempfile.mkstemp(prefix="dexter_signal_", suffix=".db")
        os.close(fd1)
        fd2, self.scalp_db = tempfile.mkstemp(prefix="dexter_scalp_", suffix=".db")
        os.close(fd2)

    def tearDown(self):
        for path in (self.signal_db, self.scalp_db):
            if not path:
                continue
            for _ in range(3):
                try:
                    if os.path.exists(path):
                        os.remove(path)
                    break
                except PermissionError:
                    time.sleep(0.1)

    def test_signal_store_filtered_stats_by_symbol(self):
        store = SignalStore(db_path=self.signal_db)
        sig_xau = SimpleNamespace(
            symbol="XAUUSD",
            direction="long",
            confidence=75.0,
            entry=2000.0,
            stop_loss=1990.0,
            take_profit_1=2010.0,
            take_profit_2=2020.0,
            take_profit_3=2030.0,
            risk_reward=2.0,
            timeframe="5m",
            session="london",
            pattern="test",
            entry_type="market",
            sl_type="atr",
            tp_type="rr",
            sl_liquidity_mapped=False,
            liquidity_pools_count=0,
        )
        sig_eth = SimpleNamespace(**{**sig_xau.__dict__, "symbol": "ETHUSD", "entry": 3500.0})
        xid = store.store_signal(sig_xau, source="gold")
        eid = store.store_signal(sig_eth, source="crypto")
        store.update_outcome(xid, "tp2_hit", pnl_usd=100.0, pnl_pips=20.0)
        store.update_outcome(eid, "sl_hit", pnl_usd=-25.0, pnl_pips=-10.0)

        stats_xau = store.get_performance_stats_filtered(symbol="XAU")
        self.assertEqual(stats_xau.get("completed_signals"), 1)
        self.assertEqual(stats_xau.get("wins"), 1)
        self.assertEqual(stats_xau.get("losses"), 0)
        self.assertEqual(stats_xau.get("total_pnl_usd"), 100.0)

    def test_scalp_store_filtered_stats_by_symbol_alias(self):
        store = ScalpSignalStore(db_path=self.scalp_db)
        now = time.time()
        rid = store.store(
            ScalpSignalRecord(
                timestamp=now,
                symbol="ETHUSD",
                direction="short",
                confidence=72.0,
                entry=3500.0,
                stop_loss=3510.0,
                take_profit_1=3495.0,
                take_profit_2=3490.0,
                take_profit_3=3480.0,
                risk_reward=2.0,
                session="ny",
            )
        )
        store.update_outcome(rid, "tp1_hit", exit_price=3495.0, pnl_pips=5.0, pnl_usd=20.0)

        stats_eth = store.get_stats_filtered(symbol="ETHUSDT", start_ts=now - 60, end_ts=now + 60, last_n=None)
        self.assertEqual(stats_eth.get("count"), 1)
        self.assertEqual(stats_eth.get("wins"), 1)
        self.assertEqual(stats_eth.get("losses"), 0)
        self.assertEqual(stats_eth.get("total_usd"), 20.0)
        self.assertEqual(stats_eth.get("total_signals"), 1)
        self.assertEqual(stats_eth.get("pending_count"), 0)

    def test_scalp_store_stats_include_pending_count(self):
        store = ScalpSignalStore(db_path=self.scalp_db)
        now = time.time()
        store.store(
            ScalpSignalRecord(
                timestamp=now,
                symbol="XAUUSD",
                direction="long",
                confidence=70.0,
                entry=2100.0,
                stop_loss=2095.0,
                take_profit_1=2105.0,
                take_profit_2=2110.0,
                take_profit_3=2115.0,
                risk_reward=2.0,
                session="london",
            )
        )
        stats = store.get_stats_filtered(symbol="XAUUSD", start_ts=now - 60, end_ts=now + 60, last_n=None)
        self.assertEqual(stats.get("count"), 0)
        self.assertEqual(stats.get("total_signals"), 1)
        self.assertEqual(stats.get("pending_count"), 1)


if __name__ == "__main__":
    unittest.main()
