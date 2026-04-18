import unittest
import sqlite3
import tempfile
import shutil
import json
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import scheduler as scheduler_module
from analysis.signals import TradeSignal
from execution.mt5_executor import MT5ExecutionResult
from market.macro_news import MacroHeadline
from scanners.stock_scanner import StockOpportunity


def make_signal(symbol: str, confidence: float = 72.0) -> TradeSignal:
    return TradeSignal(
        symbol=symbol,
        direction="long",
        confidence=confidence,
        entry=100.0,
        stop_loss=99.0,
        take_profit_1=101.0,
        take_profit_2=102.0,
        take_profit_3=103.0,
        risk_reward=2.0,
        timeframe="1h",
        session="new_york",
        trend="bullish",
        rsi=56.0,
        atr=1.0,
        pattern="TEST",
        reasons=[],
        warnings=[],
        raw_scores={"edge": 20},
    )


def make_opp(symbol: str, vol: float = 0.8, quality_score: int = 1, confidence: float = 72.0) -> StockOpportunity:
    return StockOpportunity(
        signal=make_signal(symbol=symbol, confidence=confidence),
        market="US",
        setup_type="BULLISH_OB_BOUNCE",
        base_setup_type="OB_BOUNCE",
        vol_vs_avg=vol,
        quality_score=quality_score,
        quality_tag="LOW",
    )


class SchedulerWatchlistTests(unittest.TestCase):
    def setUp(self):
        # Prevent test fixture signals (pattern=TEST, entry=100) from leaking into
        # the production execution_journal (data/ctrader_openapi.db).
        # Two write paths: journal_pre_dispatch_skip (pre-dispatch audit) and
        # _journal (called by execute_signal when it filters/dry-runs).
        self._journal_patcher = patch.object(
            scheduler_module.ctrader_executor,
            "journal_pre_dispatch_skip",
            return_value=0,
        )
        self._db_journal_patcher = patch.object(
            scheduler_module.ctrader_executor,
            "_journal",
            return_value=0,
        )
        self._journal_mock = self._journal_patcher.start()
        self._db_journal_mock = self._db_journal_patcher.start()

    def tearDown(self):
        self._db_journal_patcher.stop()
        self._journal_patcher.stop()

    @staticmethod
    def _macro_headline(headline_id: str, score: int, themes: list[str], age_min: int = 15) -> MacroHeadline:
        return MacroHeadline(
            headline_id=headline_id,
            title=f"Headline {headline_id}",
            link=f"https://example.com/{headline_id}",
            source="Reuters",
            published_utc=datetime.now(timezone.utc) - timedelta(minutes=age_min),
            score=score,
            themes=themes,
            impact_hint="Macro-sensitive headline",
        )

    @unittest.skip("stock_scanner disabled — system uses cTrader OpenAPI only")
    def test_scheduler_uses_filtered_watchlist_when_no_quality(self):
        pass

    @unittest.skip("stock_scanner disabled — system uses cTrader OpenAPI only")
    def test_scheduler_logs_quality_and_watchlist_counts(self):
        pass

    def test_xauusd_scheduled_scan_respects_cooldown(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("XAUUSD", confidence=85.0)
        dexter._last_xauusd_alert_ts = 9999999999.0
        dexter._last_xauusd_direction = signal.direction
        dexter._last_xauusd_entry = signal.entry
        dexter._last_xauusd_atr = signal.atr

        with patch.object(scheduler_module.xauusd_scanner, "scan", return_value=signal), \
             patch.object(scheduler_module.session_manager, "get_session_info", return_value={"utc_time": "2026-03-07 01:00 UTC", "active_sessions": ["new_york"], "high_volatility": True, "xauusd_market_open": True}), \
             patch.object(scheduler_module.notifier, "send_signal") as send_signal:
            dexter._run_xauusd_scan(force_alert=False)

        self.assertFalse(send_signal.called)

    def test_xauusd_scheduled_status_suppressed_when_auto_monitor_enabled(self):
        dexter = scheduler_module.DexterScheduler()
        diag = {
            "status": "no_signal",
            "current_price": 2000.0,
            "unmet": ["base_setup"],
            "notes": ["signal_generator_returned_none"],
            "fallback": {"reason": "no_direction_passed_threshold"},
        }
        with patch.object(scheduler_module.config, "SIGNAL_MONITOR_AUTO_PUSH_ENABLED", True), \
             patch.object(scheduler_module.config, "XAUUSD_SCAN_STATUS_NOTIFY_WHEN_AUTO_MONITOR", False), \
             patch.object(scheduler_module.session_manager, "get_session_info", return_value={"utc_time": "2026-03-07 01:00 UTC", "active_sessions": ["new_york"], "high_volatility": True, "xauusd_market_open": True}), \
             patch.object(scheduler_module.xauusd_scanner, "scan", return_value=None), \
             patch.object(scheduler_module.xauusd_scanner, "get_last_scan_diagnostics", return_value=diag), \
             patch.object(scheduler_module.notifier, "send_xauusd_scan_status") as send_status:
            dexter._run_xauusd_scan(force_alert=False, source="scheduled")

        self.assertFalse(send_status.called)

    def test_xauusd_scan_skips_when_market_closed(self):
        dexter = scheduler_module.DexterScheduler()
        session_info = {
            "utc_time": "2026-03-07 01:00 UTC",
            "active_sessions": ["asian"],
            "high_volatility": False,
            "xauusd_market_open": False,
        }
        with patch.object(scheduler_module.config, "SIGNAL_MONITOR_AUTO_PUSH_ENABLED", False), \
             patch.object(scheduler_module.session_manager, "get_session_info", return_value=session_info), \
             patch.object(scheduler_module.xauusd_scanner, "scan") as scan_call, \
             patch.object(scheduler_module.notifier, "send_xauusd_scan_status") as send_status:
            out = dexter._run_xauusd_scan(force_alert=False, source="scheduled")

        self.assertEqual(out.get("status"), "market_closed")
        self.assertEqual(scan_call.call_count, 0)
        self.assertEqual(send_status.call_count, 1)

    def test_xauusd_manual_scan_bypasses_cooldown(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("XAUUSD", confidence=85.0)
        dexter._last_xauusd_alert_ts = 9999999999.0
        dexter._last_xauusd_direction = signal.direction
        dexter._last_xauusd_entry = signal.entry
        dexter._last_xauusd_atr = signal.atr

        with patch.object(scheduler_module.xauusd_scanner, "scan", return_value=signal), \
             patch.object(scheduler_module.session_manager, "get_session_info", return_value={"utc_time": "2026-03-07 01:00 UTC", "active_sessions": ["new_york"], "high_volatility": True, "xauusd_market_open": True}), \
             patch.object(scheduler_module.notifier, "send_signal", return_value=True) as send_signal:
            dexter._run_xauusd_scan(force_alert=True)

        self.assertTrue(send_signal.called)

    def test_xau_guard_transition_watch_alerts_news_freeze_activate_and_clear(self):
        dexter = scheduler_module.DexterScheduler()
        freeze_normal = {"enabled": True, "active": False, "nearest_min": -1, "window_min": 20, "events": []}
        freeze_active = {"enabled": True, "active": True, "nearest_min": 18, "window_min": 20, "events": ["US CPI"]}
        shock_normal = {"enabled": True, "active": False, "kill_switch": False}

        with patch.object(scheduler_module.xauusd_scanner, "_news_freeze_context", side_effect=[freeze_normal, freeze_active, freeze_normal]), \
             patch.object(dexter, "_xau_event_shock_state", return_value=shock_normal), \
             patch.object(dexter, "_publish_xau_guard_transition_alert", side_effect=lambda payload: {"text": "ok", "sent": 1}) as send_alert:
            dexter._run_xau_guard_transition_watch()
            dexter._run_xau_guard_transition_watch()
            dexter._run_xau_guard_transition_watch()

        self.assertEqual(send_alert.call_count, 2)
        first_payload = send_alert.call_args_list[0].args[0]
        second_payload = send_alert.call_args_list[1].args[0]
        self.assertEqual(first_payload["kind"], "news_freeze")
        self.assertEqual(first_payload["action"], "activated")
        self.assertEqual(first_payload["title"], "US CPI")
        self.assertEqual(second_payload["kind"], "news_freeze")
        self.assertEqual(second_payload["action"], "cleared")

    def test_xau_guard_transition_watch_alerts_kill_switch_activate_and_clear(self):
        dexter = scheduler_module.DexterScheduler()
        freeze_normal = {"enabled": True, "active": False, "nearest_min": -1, "window_min": 20, "events": []}
        shock_normal = {"enabled": True, "active": False, "kill_switch": False}
        shock_kill = {
            "enabled": True,
            "active": True,
            "kill_switch": True,
            "title": "Reuters headline",
            "shock_score": 18.4,
            "source": "Reuters",
            "verification": "confirmed",
        }

        with patch.object(scheduler_module.xauusd_scanner, "_news_freeze_context", return_value=freeze_normal), \
             patch.object(dexter, "_xau_event_shock_state", side_effect=[shock_normal, shock_kill, shock_normal]), \
             patch.object(dexter, "_publish_xau_guard_transition_alert", side_effect=lambda payload: {"text": "ok", "sent": 1}) as send_alert:
            dexter._run_xau_guard_transition_watch()
            dexter._run_xau_guard_transition_watch()
            dexter._run_xau_guard_transition_watch()

        self.assertEqual(send_alert.call_count, 2)
        first_payload = send_alert.call_args_list[0].args[0]
        second_payload = send_alert.call_args_list[1].args[0]
        self.assertEqual(first_payload["kind"], "kill_switch")
        self.assertEqual(first_payload["action"], "activated")
        self.assertEqual(first_payload["title"], "Reuters headline")
        self.assertEqual(second_payload["kind"], "kill_switch")
        self.assertEqual(second_payload["action"], "cleared")

    def test_mt5_batch_stops_after_attempt_cap(self):
        dexter = scheduler_module.DexterScheduler()
        signals = [make_signal(f"S{i}") for i in range(6)]
        failed = MT5ExecutionResult(
            ok=False,
            status="skipped",
            message="margin guard",
            signal_symbol="S0",
            broker_symbol="S0USD",
        )

        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_AUTOPILOT_ENABLED", False), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_MICRO_MODE_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_MAX_SIGNALS_PER_SCAN", 1), \
             patch.object(scheduler_module.config, "MT5_MAX_ATTEMPTS_PER_SCAN", 2), \
             patch.object(dexter, "_check_macro_rumor_trade_guard", return_value=(False, "", {})), \
             patch.object(scheduler_module.mt5_executor, "execute_signal", return_value=failed) as exec_call:
            dexter._maybe_execute_mt5_batch(signals, source="crypto")

        self.assertEqual(exec_call.call_count, 2)

    def test_mt5_filled_notifies_execution_update(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("ETH/USDT", confidence=90.0)
        filled = MT5ExecutionResult(
            ok=True,
            status="filled",
            message="order accepted",
            signal_symbol="ETH/USDT",
            broker_symbol="ETHUSD",
            ticket=12345,
            position_id=12345,
        )
        with patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_NOTIFY_EXECUTED", True), \
             patch.object(scheduler_module.notifier, "send_mt5_execution_update") as send_exec:
            dexter._handle_mt5_result(signal, filled, source="crypto")
        self.assertTrue(send_exec.called)

    def test_mt5_execute_assigns_signal_trace(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("XAUUSD", confidence=82.0)
        skipped = MT5ExecutionResult(
            ok=False,
            status="skipped",
            message="test-skip",
            signal_symbol="XAUUSD",
            broker_symbol="XAUUSD",
        )
        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_AUTOPILOT_ENABLED", False), \
             patch.object(scheduler_module.mt5_executor, "execute_signal", return_value=skipped):
            dexter._maybe_execute_mt5_signal(signal, source="xauusd")

        raw = dict(getattr(signal, "raw_scores", {}) or {})
        self.assertGreater(int(raw.get("signal_run_no", 0) or 0), 0)
        self.assertTrue(str(raw.get("signal_run_id", "") or "").strip())
        self.assertTrue(str(raw.get("signal_trace_tag", "") or "").startswith("R"))

    def test_repeat_error_guard_locks_symbol_after_repeated_rejections(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("ETH/USDT", confidence=80.0)
        rej = MT5ExecutionResult(
            ok=False,
            status="rejected",
            message="order rejected retcode=10022",
            signal_symbol="ETH/USDT",
            broker_symbol="ETHUSD",
            retcode=10022,
        )
        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_MAX_HITS", 2), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_LOCK_MIN", 45), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_WINDOW_MIN", 30), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_PERSIST_ENABLED", False):
            dexter._repeat_guard_on_result(signal, rej, source="crypto")
            dexter._repeat_guard_on_result(signal, rej, source="crypto")
            allow, meta = dexter._repeat_guard_allow(signal, source="crypto")
        self.assertFalse(allow)
        self.assertEqual(meta.get("reason"), "repeat_error_lock")
        self.assertGreater(float(meta.get("remaining_sec", 0.0) or 0.0), 0.0)

    def test_repeat_error_guard_blocks_execute_path_when_locked(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("BTC/USDT", confidence=80.0)
        rej = MT5ExecutionResult(
            ok=False,
            status="rejected",
            message="order rejected retcode=10015",
            signal_symbol="BTC/USDT",
            broker_symbol="BTCUSD",
            retcode=10015,
        )
        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_MAX_HITS", 1), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_LOCK_MIN", 45), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_WINDOW_MIN", 30), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_PERSIST_ENABLED", False), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", False), \
             patch.object(scheduler_module.mt5_executor, "execute_signal") as exec_call, \
             patch.object(dexter, "_handle_mt5_result") as handle_result:
            dexter._repeat_guard_on_result(signal, rej, source="crypto")
            dexter._maybe_execute_mt5_signal(signal, source="crypto")
        self.assertFalse(exec_call.called)
        self.assertEqual(handle_result.call_count, 1)

    def test_preclose_flatten_skips_outside_window(self):
        dexter = scheduler_module.DexterScheduler()
        ny_now = datetime(2026, 3, 5, 14, 0, tzinfo=ZoneInfo("America/New_York"))
        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_PRE_CLOSE_FLATTEN_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_PRE_CLOSE_FLATTEN_FRI_ONLY", False), \
             patch.object(scheduler_module.config, "MT5_PRE_CLOSE_FLATTEN_NY_HOUR", 16), \
             patch.object(scheduler_module.config, "MT5_PRE_CLOSE_FLATTEN_NY_MINUTE", 50), \
             patch.object(scheduler_module.config, "MT5_PRE_CLOSE_FLATTEN_WINDOW_MIN", 20):
            rpt = dexter._run_mt5_preclose_flatten(force=False, ny_now=ny_now)
        self.assertTrue(rpt.get("ok"))
        self.assertTrue(rpt.get("skipped"))
        self.assertEqual(rpt.get("reason"), "outside_window")

    def test_preclose_flatten_closes_only_non_excluded_symbols(self):
        dexter = scheduler_module.DexterScheduler()
        ny_now = datetime(2026, 3, 5, 16, 45, tzinfo=ZoneInfo("America/New_York"))
        positions = {
            "positions": [
                {"ticket": 1, "symbol": "ETHUSD", "type": "buy", "volume": 0.10},
                {"ticket": 2, "symbol": "GBPUSD", "type": "sell", "volume": 0.05},
            ]
        }
        ok_res = MT5ExecutionResult(
            ok=True,
            status="partial_closed",
            message="partial close accepted retcode=10009",
            broker_symbol="GBPUSD",
            ticket=2,
            retcode=10009,
        )
        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_PRE_CLOSE_FLATTEN_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_PRE_CLOSE_FLATTEN_FRI_ONLY", False), \
             patch.object(scheduler_module.config, "MT5_PRE_CLOSE_FLATTEN_NY_HOUR", 16), \
             patch.object(scheduler_module.config, "MT5_PRE_CLOSE_FLATTEN_NY_MINUTE", 50), \
             patch.object(scheduler_module.config, "MT5_PRE_CLOSE_FLATTEN_WINDOW_MIN", 20), \
             patch.object(scheduler_module.config, "get_mt5_preclose_flatten_include_symbols", return_value=set()), \
             patch.object(scheduler_module.config, "get_mt5_preclose_flatten_exclude_symbols", return_value={"ETHUSD", "BTCUSD"}), \
             patch.object(scheduler_module.mt5_executor, "open_positions_snapshot", return_value=positions), \
             patch.object(scheduler_module.mt5_executor, "close_position_partial", return_value=ok_res) as close_call:
            rpt = dexter._run_mt5_preclose_flatten(force=True, ny_now=ny_now)
        self.assertTrue(rpt.get("ok"))
        self.assertFalse(rpt.get("skipped"))
        self.assertEqual(int(rpt.get("checked", 0)), 1)
        self.assertEqual(int(rpt.get("closed", 0)), 1)
        self.assertEqual(close_call.call_count, 1)
        kwargs = close_call.call_args.kwargs
        self.assertEqual(kwargs.get("broker_symbol"), "GBPUSD")

    def test_bypass_quick_tp_sends_telegram_on_success(self):
        dexter = scheduler_module.DexterScheduler()
        positions = {
            "positions": [
                {
                    "ticket": 99887766,
                    "symbol": "XAUUSD",
                    "type": "buy",
                    "volume": 0.05,
                    "profit": 12.4,
                    "comment": "DEXTER:BYPASS:XAUUSD:R000123",
                    "magic": 1500,
                }
            ]
        }
        ok_res = MT5ExecutionResult(
            ok=True,
            status="partial_closed",
            message="partial close accepted retcode=10009",
            broker_symbol="XAUUSD",
            ticket=99887766,
            retcode=10009,
        )
        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_BYPASS_TEST_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_BYPASS_TEST_QUICK_TP_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_BYPASS_TEST_QUICK_TP_NOTIFY_TELEGRAM", True), \
             patch.object(scheduler_module.config, "MT5_BYPASS_TEST_QUICK_TP_BALANCE_PCT", 1.0), \
             patch.object(scheduler_module.config, "MT5_BYPASS_TEST_QUICK_TP_MIN_USD", 1.0), \
             patch.object(scheduler_module.config, "MT5_BYPASS_TEST_SOURCE_SUFFIX", "bypass"), \
             patch.object(scheduler_module.config, "MT5_MAGIC", 1000), \
             patch.object(scheduler_module.config, "MT5_BYPASS_TEST_MAGIC_OFFSET", 500), \
             patch.object(scheduler_module.mt5_executor, "status", return_value={"connected": True, "balance": 1000.0}), \
             patch.object(scheduler_module.mt5_executor, "open_positions_snapshot", return_value=positions), \
             patch.object(scheduler_module.mt5_executor, "close_position_partial", return_value=ok_res) as close_call, \
             patch.object(scheduler_module.notifier, "send_mt5_bypass_quick_tp_update", return_value=True) as send_tp:
            rpt = dexter._run_mt5_bypass_quick_tp()

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(int(rpt.get("checked", 0)), 1)
        self.assertEqual(int(rpt.get("closed", 0)), 1)
        self.assertEqual(close_call.call_count, 1)
        self.assertEqual(send_tp.call_count, 1)
        kwargs = send_tp.call_args.kwargs
        self.assertEqual(kwargs.get("symbol"), "XAUUSD")
        self.assertEqual(int(kwargs.get("ticket", 0)), 99887766)
        self.assertAlmostEqual(float(kwargs.get("target_usd", 0.0)), 10.0, places=2)

    def test_neural_soft_adjustment_applies_confidence_without_block(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("XAUUSD", confidence=74.0)

        with patch.object(
            scheduler_module.neural_brain,
            "confidence_adjustment",
            return_value={
                "applied": True,
                "reason": "applied",
                "prob": 0.78,
                "base_confidence": 74.0,
                "adjusted_confidence": 78.0,
                "delta": 4.0,
            },
        ):
            out = dexter._apply_neural_soft_adjustment(signal, source="xauusd")

        self.assertTrue(out.get("applied"))
        self.assertAlmostEqual(signal.confidence, 78.0, places=2)
        self.assertTrue(signal.raw_scores.get("neural_confidence_adjusted"))
        self.assertIn("neural_probability", signal.raw_scores)

    def test_mt5_filter_armed_but_not_ready_does_not_block_execution(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("ETH/USDT", confidence=90.0)
        exec_result = MT5ExecutionResult(
            ok=False,
            status="skipped",
            message="margin guard",
            signal_symbol="ETH/USDT",
            broker_symbol="ETHUSD",
        )

        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_AUTOPILOT_ENABLED", False), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", True), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_EXECUTION_FILTER", True), \
             patch.object(dexter, "_check_macro_rumor_trade_guard", return_value=(False, "", {})), \
             patch.object(scheduler_module.neural_brain, "execution_filter_status", return_value={"ready": False, "reason": "insufficient_samples"}), \
             patch.object(scheduler_module.mt5_executor, "execute_signal", return_value=exec_result) as exec_call, \
             patch.object(dexter, "_handle_mt5_result") as handle_result:
            dexter._maybe_execute_mt5_signal(signal, source="crypto")

        self.assertEqual(exec_call.call_count, 1)
        self.assertEqual(handle_result.call_count, 1)

    def test_mt5_filter_ready_blocks_low_probability(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("ETH/USDT", confidence=90.0)

        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", True), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_EXECUTION_FILTER", True), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_MIN_PROB", 0.60), \
             patch.object(scheduler_module.neural_brain, "execution_filter_status", return_value={"ready": True, "reason": "ready"}), \
             patch.object(dexter, "_get_live_neural_probability", return_value=(0.45, {"mt5_neural_prob_source": "global"})), \
             patch.object(scheduler_module.mt5_executor, "execute_signal") as exec_call, \
             patch.object(dexter, "_handle_mt5_result") as handle_result:
            dexter._maybe_execute_mt5_signal(signal, source="crypto")

        self.assertFalse(exec_call.called)
        self.assertEqual(handle_result.call_count, 1)

    def test_macro_rumor_guard_blocks_mt5_execution(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("XAUUSD", confidence=84.0)
        rumor = self._macro_headline("rumor1", 9, ["geopolitics"], age_min=8)
        rumor.verification = "rumor"
        rumor.source_quality = 0.62
        rumor.source_tier = "low"

        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_AUTOPILOT_ENABLED", False), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", False), \
             patch.object(scheduler_module.config, "MACRO_NEWS_RUMOR_FILTER_ENABLED", True), \
             patch.object(scheduler_module.config, "MACRO_NEWS_RUMOR_BLOCK_MIN_SCORE", 8), \
             patch.object(scheduler_module.config, "MACRO_NEWS_RUMOR_BLOCK_MAX_AGE_MIN", 120), \
             patch.object(scheduler_module.config, "MACRO_NEWS_RUMOR_MAX_SOURCE_QUALITY", 0.75), \
             patch.object(scheduler_module.config, "MACRO_NEWS_RUMOR_BLOCK_MIN_SCORE_XAUUSD", 8), \
             patch.object(scheduler_module.config, "MACRO_NEWS_RUMOR_BLOCK_MAX_AGE_MIN_XAUUSD", 120), \
             patch.object(scheduler_module.config, "MACRO_NEWS_RUMOR_MAX_SOURCE_QUALITY_XAUUSD", 0.75), \
             patch.object(scheduler_module.config, "MACRO_NEWS_LOOKBACK_HOURS", 24), \
             patch.object(scheduler_module.macro_news, "high_impact_headlines", return_value=[rumor]), \
             patch.object(scheduler_module.mt5_executor, "execute_signal") as exec_call, \
             patch.object(dexter, "_handle_mt5_result") as handle_result:
            dexter._maybe_execute_mt5_signal(signal, source="xauusd")

        self.assertFalse(exec_call.called)
        self.assertEqual(handle_result.call_count, 1)
        result = handle_result.call_args.args[1]
        self.assertEqual(str(getattr(result, "status", "")), "guard_blocked")
        self.assertIn("macro rumor guard", str(getattr(result, "message", "")).lower())

    def test_xau_event_shock_mode_reduces_tp_and_size_before_execution(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("XAUUSD", confidence=86.0)
        headline = self._macro_headline("shock1", 10, ["geopolitics"], age_min=5)
        headline.verification = "confirmed"
        headline.source_quality = 0.92
        headline.source_tier = "trusted"
        ok = MT5ExecutionResult(ok=True, status="filled", message="ok", signal_symbol="XAUUSD", broker_symbol="XAUUSD")

        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_AUTOPILOT_ENABLED", False), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", False), \
             patch.object(scheduler_module.config, "MACRO_NEWS_RUMOR_FILTER_ENABLED", False), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_MODE_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_LOOKBACK_HOURS", 6), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_MAX_AGE_MIN", 180), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_MIN_SCORE", 8), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_MIN_SOURCE_QUALITY", 0.75), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_KILL_SWITCH_SCORE", 20), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_KILL_SWITCH_CONFIRMED_ONLY", True), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_SIZE_MULT", 0.5), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_TP1_RR", 0.6), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_TP2_RR", 1.0), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_TP3_RR", 1.4), \
             patch.object(scheduler_module.config, "get_xau_event_shock_kill_switch_themes", return_value={"GEOPOLITICS"}), \
             patch.object(scheduler_module.macro_news, "high_impact_headlines", return_value=[headline]), \
             patch.object(scheduler_module.mt5_executor, "execute_signal", return_value=ok) as exec_call, \
             patch.object(dexter, "_handle_mt5_result") as handle_result:
            dexter._maybe_execute_mt5_signal(signal, source="xauusd")

        self.assertEqual(exec_call.call_count, 1)
        self.assertEqual(handle_result.call_count, 1)
        self.assertAlmostEqual(float(exec_call.call_args.kwargs.get("volume_multiplier")), 0.5, places=3)
        self.assertAlmostEqual(float(signal.take_profit_1), 100.6, places=3)
        self.assertAlmostEqual(float(signal.take_profit_2), 101.0, places=3)
        self.assertAlmostEqual(float(signal.take_profit_3), 101.4, places=3)

    def test_xau_event_shock_kill_switch_blocks_execution(self):
        dexter = scheduler_module.DexterScheduler()
        signal = make_signal("XAUUSD", confidence=86.0)
        headline = self._macro_headline("shock2", 10, ["geopolitics"], age_min=5)
        headline.verification = "confirmed"
        headline.source_quality = 0.95
        headline.source_tier = "trusted"

        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_AUTOPILOT_ENABLED", False), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", False), \
             patch.object(scheduler_module.config, "MACRO_NEWS_RUMOR_FILTER_ENABLED", False), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_MODE_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_LOOKBACK_HOURS", 6), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_MAX_AGE_MIN", 180), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_MIN_SCORE", 8), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_MIN_SOURCE_QUALITY", 0.75), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_KILL_SWITCH_SCORE", 9), \
             patch.object(scheduler_module.config, "XAU_EVENT_SHOCK_KILL_SWITCH_CONFIRMED_ONLY", True), \
             patch.object(scheduler_module.config, "get_xau_event_shock_kill_switch_themes", return_value={"GEOPOLITICS"}), \
             patch.object(scheduler_module.macro_news, "high_impact_headlines", return_value=[headline]), \
             patch.object(scheduler_module.mt5_executor, "execute_signal") as exec_call, \
             patch.object(dexter, "_handle_mt5_result") as handle_result:
            dexter._maybe_execute_mt5_signal(signal, source="xauusd")

        self.assertFalse(exec_call.called)
        self.assertEqual(handle_result.call_count, 1)
        result = handle_result.call_args.args[1]
        self.assertEqual(str(getattr(result, "status", "")), "guard_blocked")
        self.assertIn("kill-switch", str(getattr(result, "message", "")).lower())

    def test_neural_sync_train_uses_feedback_and_bootstrap_min_samples(self):
        dexter = scheduler_module.DexterScheduler()
        with patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", True), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_AUTO_TRAIN", True), \
             patch.object(scheduler_module.config, "SIGNAL_FEEDBACK_ENABLED", True), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_SYNC_DAYS", 120), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_MIN_SAMPLES", 30), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_BOOTSTRAP_MIN_SAMPLES", 10), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_SIGNAL_FEEDBACK_MAX_RECORDS", 400), \
             patch.object(scheduler_module.neural_brain, "sync_outcomes_from_mt5", return_value={"ok": True, "updated": 0, "closed_positions": 0}), \
             patch.object(scheduler_module.neural_brain, "sync_signal_outcomes_from_market", return_value={"ok": True, "reviewed": 20, "resolved": 12, "updated": 20}) as sync_feedback, \
             patch.object(scheduler_module.neural_brain, "model_status", return_value={"available": False}), \
             patch.object(scheduler_module.neural_brain, "train_backprop") as train_call, \
             patch("learning.symbol_neural_brain.symbol_neural_brain.train_all", return_value={}) as sym_train:
            dexter._run_neural_sync_train()

        self.assertEqual(sync_feedback.call_count, 1)
        self.assertEqual(train_call.call_count, 1)
        self.assertEqual(sym_train.call_count, 1)
        self.assertEqual(train_call.call_args.kwargs.get("min_samples"), 10)

    def test_neural_mission_cycle_uses_config_and_aliases(self):
        dexter = scheduler_module.DexterScheduler()
        report = {
            "ok": True,
            "goal_met": False,
            "iterations_done": 1,
            "symbols": ["XAUUSD", "ETHUSD", "BTCUSD", "GBPUSD"],
            "report_path": "data/mission_reports/mission_test.json",
        }
        with patch.object(scheduler_module.config, "NEURAL_MISSION_AUTO_ENABLED", True), \
             patch.object(scheduler_module.config, "NEURAL_MISSION_SYMBOLS", "XAU,ETHUSDT,BTC/USDT,GBPUSD"), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_SYNC_DAYS", 45), \
             patch.object(scheduler_module.config, "NEURAL_MISSION_ITERATIONS_PER_CYCLE", 2), \
             patch.object(scheduler_module.config, "NEURAL_MISSION_TARGET_WIN_RATE", 61.0), \
             patch.object(scheduler_module.config, "NEURAL_MISSION_TARGET_PROFIT_FACTOR", 1.35), \
             patch.object(scheduler_module.config, "NEURAL_MISSION_MIN_TRADES", 15), \
             patch.object(scheduler_module.config, "NEURAL_MISSION_APPLY_POLICY_DRAFT", True), \
             patch("learning.mt5_neural_mission.mt5_neural_mission.run", return_value=report) as run_call:
            out = dexter._run_neural_mission_cycle(source="test")

        self.assertTrue(out.get("ok"))
        self.assertEqual(run_call.call_count, 1)
        kwargs = run_call.call_args.kwargs
        self.assertEqual(kwargs.get("symbols"), "XAUUSD,ETHUSD,BTCUSD,GBPUSD")
        self.assertEqual(kwargs.get("iterations"), 2)
        self.assertEqual(kwargs.get("train_days"), 45)
        self.assertEqual(kwargs.get("backtest_days"), 45)
        self.assertEqual(kwargs.get("sync_days"), 45)
        self.assertEqual(kwargs.get("target_win_rate"), 61.0)
        self.assertEqual(kwargs.get("target_profit_factor"), 1.35)
        self.assertEqual(kwargs.get("min_trades"), 15)
        self.assertTrue(kwargs.get("apply_policy_draft"))

    def test_macro_adaptive_priority_drops_weak_theme_headline(self):
        dexter = scheduler_module.DexterScheduler()
        weak = self._macro_headline("weak1", 8, ["tariff_trade"])
        strong = self._macro_headline("strong1", 8, ["fed_policy"])
        weights = {
            "tariff_trade": {"weight_mult": 0.82, "sample_count": 8, "no_clear_rate": 82.0, "confirmed_rate": 5.0},
            "fed_policy": {"weight_mult": 1.08, "sample_count": 8, "no_clear_rate": 15.0, "confirmed_rate": 55.0},
        }
        with patch.object(scheduler_module.config, "MACRO_ALERT_ADAPTIVE_PRIORITY_ENABLED", True), \
             patch.object(scheduler_module.config, "MACRO_ALERT_ADAPTIVE_MIN_SAMPLES", 3), \
             patch.object(scheduler_module.config, "MACRO_ALERT_ADAPTIVE_MIN_THEME_MULT", 0.90), \
             patch.object(scheduler_module.config, "MACRO_ALERT_ADAPTIVE_SKIP_NO_CLEAR_RATE", 65.0), \
             patch.object(scheduler_module.config, "MACRO_ALERT_ADAPTIVE_ULTRA_SCORE_FLOOR", 10), \
             patch.object(scheduler_module.macro_news, "dynamic_theme_weights_snapshot", return_value=weights):
            ranked, meta = dexter._rank_macro_alert_candidates([weak, strong], now_utc=datetime.now(timezone.utc))

        self.assertEqual([h.headline_id for h in ranked], ["strong1"])
        self.assertEqual(meta["dropped"], 1)

    def test_macro_adaptive_priority_keeps_ultra_score_even_if_theme_weak(self):
        dexter = scheduler_module.DexterScheduler()
        ultra = self._macro_headline("ultra1", 12, ["tariff_trade"])
        weights = {
            "tariff_trade": {"weight_mult": 0.80, "sample_count": 9, "no_clear_rate": 90.0, "confirmed_rate": 5.0},
        }
        with patch.object(scheduler_module.config, "MACRO_ALERT_ADAPTIVE_PRIORITY_ENABLED", True), \
             patch.object(scheduler_module.config, "MACRO_ALERT_ADAPTIVE_MIN_SAMPLES", 3), \
             patch.object(scheduler_module.config, "MACRO_ALERT_ADAPTIVE_MIN_THEME_MULT", 0.90), \
             patch.object(scheduler_module.config, "MACRO_ALERT_ADAPTIVE_SKIP_NO_CLEAR_RATE", 65.0), \
             patch.object(scheduler_module.config, "MACRO_ALERT_ADAPTIVE_ULTRA_SCORE_FLOOR", 10), \
             patch.object(scheduler_module.macro_news, "dynamic_theme_weights_snapshot", return_value=weights):
            ranked, meta = dexter._rank_macro_alert_candidates([ultra], now_utc=datetime.now(timezone.utc))

        self.assertEqual([h.headline_id for h in ranked], ["ultra1"])
        self.assertEqual(meta["dropped"], 0)
        self.assertGreater(float(getattr(ranked[0], "_adaptive_priority", 0.0)), 0.0)

    def test_mt5_micro_mode_does_not_count_margin_guard_skips_against_attempt_cap(self):
        dexter = scheduler_module.DexterScheduler()
        signals = [make_signal(f"S{i}") for i in range(4)]
        skipped = MT5ExecutionResult(ok=False, status="skipped", message="margin guard: required=10 > allowed=1", signal_symbol="S0", broker_symbol="S0")
        filled = MT5ExecutionResult(ok=True, status="filled", message="ok", signal_symbol="S3", broker_symbol="S3")
        seq = [skipped, skipped, skipped, filled]

        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_AUTOPILOT_ENABLED", False), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_MICRO_MODE_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_MAX_SIGNALS_PER_SCAN", 1), \
             patch.object(scheduler_module.config, "MT5_MAX_ATTEMPTS_PER_SCAN", 1), \
             patch.object(dexter, "_check_macro_rumor_trade_guard", return_value=(False, "", {})), \
             patch.object(scheduler_module.mt5_executor, "execute_signal", side_effect=seq) as exec_call:
            dexter._maybe_execute_mt5_batch(signals, source="crypto")

        # Micro-mode margin-guard skips should not consume the single attempt slot.
        self.assertEqual(exec_call.call_count, 4)

    def test_scalping_scan_stores_signal_when_sent(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=79.0)
        sig.raw_scores = {"scalping": True, "scalping_source": "scalp_xauusd"}
        row = SimpleNamespace(
            source="scalp_xauusd",
            symbol="XAUUSD",
            status="ready",
            reason="ok",
            signal=sig,
            trigger={"ok": True},
        )
        with patch.object(scheduler_module.config, "SCALPING_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALPING_NOTIFY_TELEGRAM", True), \
             patch.object(scheduler_module.config, "SCALPING_EXECUTE_MT5", False), \
             patch.object(scheduler_module.config, "SIGNAL_FEEDBACK_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_ENABLED", False), \
             patch.object(scheduler_module.config, "scalping_symbol_enabled", side_effect=lambda s: str(s).upper() == "XAUUSD"), \
             patch.object(scheduler_module.scalping_scanner, "scan_xauusd", return_value=row), \
             patch.object(scheduler_module.session_manager, "is_xauusd_market_open", return_value=True), \
             patch.object(dexter, "_scalping_cooldown_gate", return_value=(True, 0.0)), \
             patch.object(scheduler_module.notifier, "send_signal", return_value=True), \
             patch.object(scheduler_module.scalp_store, "store", return_value=123) as store_call:
            rpt = dexter._run_scalping_scan(force=True)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(store_call.call_count, 1)
        self.assertEqual(rpt["results"][0].get("stored_id"), 123)

    def test_scalping_scan_blocks_xau_when_market_closed(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.3)
        sig.pattern = "SCALP_FLOW_FORCE"
        row = SimpleNamespace(
            source="scalp_xauusd",
            symbol="XAUUSD",
            status="ready",
            reason="ok",
            signal=sig,
            trigger={"forced_mode": True},
        )
        with patch.object(scheduler_module.config, "SCALPING_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALPING_NOTIFY_TELEGRAM", True), \
             patch.object(scheduler_module.config, "SCALPING_EXECUTE_MT5", False), \
             patch.object(scheduler_module.config, "SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED", True), \
             patch.object(scheduler_module.config, "SIGNAL_FEEDBACK_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_ENABLED", False), \
             patch.object(scheduler_module.config, "scalping_symbol_enabled", side_effect=lambda s: str(s).upper() == "XAUUSD"), \
             patch.object(scheduler_module.scalping_scanner, "scan_xauusd", return_value=row), \
             patch.object(scheduler_module.session_manager, "is_xauusd_market_open", return_value=False), \
             patch.object(scheduler_module.notifier, "send_signal", return_value=True) as send_call:
            rpt = dexter._run_scalping_scan(force=True)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(send_call.call_count, 0)
        self.assertEqual(rpt["results"][0].get("status"), "market_closed")
        self.assertEqual(rpt["results"][0].get("reason"), "xauusd_market_closed_weekend_window")

    def test_scalping_scan_suppresses_duplicate_fingerprint(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.3)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores = {
            "scalping": True,
            "scalping_source": "scalp_xauusd",
            "scalp_force_mode": True,
            "scalp_force_last_m5_bar_utc": "2026-03-07T01:25:00+00:00",
            "scalp_force_last_h1_bar_utc": "2026-03-07T01:00:00+00:00",
        }
        row = SimpleNamespace(
            source="scalp_xauusd",
            symbol="XAUUSD",
            status="ready",
            reason="ok",
            signal=sig,
            trigger={"forced_mode": True},
        )
        with patch.object(scheduler_module.config, "SCALPING_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALPING_NOTIFY_TELEGRAM", True), \
             patch.object(scheduler_module.config, "SCALPING_EXECUTE_MT5", False), \
             patch.object(scheduler_module.config, "SCALPING_DUPLICATE_SUPPRESS_SEC", 1800), \
             patch.object(scheduler_module.config, "SCALPING_XAU_MARKET_CLOSED_GUARD_ENABLED", True), \
             patch.object(scheduler_module.config, "SIGNAL_FEEDBACK_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_ENABLED", False), \
             patch.object(scheduler_module.config, "scalping_symbol_enabled", side_effect=lambda s: str(s).upper() == "XAUUSD"), \
             patch.object(scheduler_module.scalping_scanner, "scan_xauusd", return_value=row), \
             patch.object(scheduler_module.session_manager, "is_xauusd_market_open", return_value=True), \
             patch.object(dexter, "_scalping_cooldown_gate", return_value=(True, 0.0)), \
             patch.object(dexter, "_store_scalping_signal", return_value=None), \
             patch.object(scheduler_module.notifier, "send_signal", return_value=True) as send_call:
            first = dexter._run_scalping_scan(force=False)
            second = dexter._run_scalping_scan(force=False)

        self.assertTrue(first.get("ok"))
        self.assertTrue(second.get("ok"))
        self.assertEqual(send_call.call_count, 1)
        self.assertEqual(first["results"][0].get("status"), "ready")
        self.assertEqual(second["results"][0].get("status"), "duplicate_suppressed")
        self.assertIn("duplicate_fingerprint", str(second["results"][0].get("reason", "")))

    def test_scalping_scan_does_not_execute_crypto_when_mt5_crypto_disabled(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("ETHUSD", confidence=77.0)
        sig.raw_scores = {"scalping": True, "scalping_source": "scalp_ethusd"}
        row_eth = SimpleNamespace(
            source="scalp_ethusd",
            symbol="ETHUSD",
            status="ready",
            reason="ok",
            signal=sig,
            trigger={"ok": True},
        )
        row_xau = SimpleNamespace(source="scalp_xauusd", symbol="XAUUSD", status="disabled", reason="symbol_not_enabled", signal=None, trigger={})
        row_btc = SimpleNamespace(source="scalp_btcusd", symbol="BTCUSD", status="disabled", reason="symbol_not_enabled", signal=None, trigger={})
        with patch.object(scheduler_module.config, "SCALPING_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALPING_NOTIFY_TELEGRAM", True), \
             patch.object(scheduler_module.config, "SCALPING_EXECUTE_MT5", True), \
             patch.object(scheduler_module.config, "SIGNAL_FEEDBACK_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_EXECUTE_CRYPTO", False), \
             patch.object(scheduler_module.config, "scalping_symbol_enabled", side_effect=lambda s: str(s).upper() == "ETHUSD"), \
             patch.object(scheduler_module.scalping_scanner, "scan_xauusd", return_value=row_xau), \
             patch.object(scheduler_module.scalping_scanner, "scan_eth", return_value=row_eth), \
             patch.object(scheduler_module.scalping_scanner, "scan_btc", return_value=row_btc), \
             patch.object(dexter, "_scalping_cooldown_gate", return_value=(True, 0.0)), \
             patch.object(dexter, "_store_scalping_signal", return_value=None), \
             patch.object(scheduler_module.notifier, "send_signal", return_value=True), \
             patch.object(dexter, "_maybe_execute_mt5_signal") as exec_call:
            rpt = dexter._run_scalping_scan(force=True)

        eth_row = next(x for x in rpt["results"] if x.get("source") == "scalp_ethusd")
        self.assertTrue(rpt.get("ok"))
        self.assertFalse(bool(eth_row.get("executed_mt5")))
        self.assertEqual(eth_row.get("mt5_live_filter"), "crypto_live_disabled")
        self.assertEqual(exec_call.call_count, 0)

    def test_signal_monitor_auto_push_sends_by_user_language(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {
            "symbol": "XAUUSD",
            "status": "no_signal",
            "main_stats": {"total_signals": 1, "completed_signals": 0},
            "scalp_stats": {"total_signals": 2, "count": 0},
        }
        with patch.object(scheduler_module.config, "SIGNAL_MONITOR_AUTO_PUSH_ENABLED", True), \
             patch.object(scheduler_module.config, "SIGNAL_MONITOR_AUTO_PUSH_DAYS", 1), \
             patch.object(scheduler_module.config, "get_signal_monitor_auto_symbols", return_value=["XAUUSD"]), \
             patch.object(scheduler_module.config, "get_signal_monitor_auto_window_mode", return_value="today"), \
             patch("notifier.admin_bot.admin_bot._build_signal_monitor_payload", return_value=payload), \
             patch("notifier.admin_bot.admin_bot._format_signal_monitor_text", side_effect=lambda p, lang="en", chat_id=None: f"{lang}:{p['symbol']}"), \
             patch.object(scheduler_module.access_manager, "list_entitled_user_ids", return_value=[101, 202]), \
             patch.object(scheduler_module.access_manager, "get_user_language_preference", side_effect=["th", "en"]), \
             patch.object(scheduler_module.notifier, "_send", return_value=True) as send_call:
            rpt = dexter._run_signal_monitor_auto_push(force=True)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(send_call.call_count, 2)
        sent_texts = [c.args[0] for c in send_call.call_args_list]
        self.assertEqual(sent_texts, ["th:XAUUSD", "en:XAUUSD"])

    def test_best_lane_dispatches_for_eligible_signal(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=75.0)
        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_TAG", "winner"), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_MIN_CONFIDENCE", 72.0), \
             patch.object(scheduler_module.config, "get_mt5_best_lane_min_confidence_symbol_overrides", return_value={}), \
             patch.object(scheduler_module.config, "get_mt5_best_lane_sources", return_value={"scalp_xauusd"}), \
             patch.object(scheduler_module.config, "get_mt5_best_lane_symbols", return_value={"XAUUSD"}), \
             patch.object(dexter, "_maybe_execute_mt5_signal") as exec_call:
            ok = dexter._maybe_execute_mt5_best_lane(sig, source="scalp_xauusd")

        self.assertTrue(ok)
        self.assertEqual(exec_call.call_count, 1)
        lane_signal = exec_call.call_args.args[0]
        lane_source = exec_call.call_args.kwargs.get("source", "")
        self.assertNotEqual(id(lane_signal), id(sig))
        self.assertEqual(lane_source, "scalp_xauusd:winner")
        self.assertFalse(bool(getattr(lane_signal, "raw_scores", {}).get("mt5_limit_allow_market_fallback", True)))

    def test_best_lane_skips_when_confidence_below_threshold(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=69.0)
        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_TAG", "winner"), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_MIN_CONFIDENCE", 72.0), \
             patch.object(scheduler_module.config, "get_mt5_best_lane_min_confidence_symbol_overrides", return_value={}), \
             patch.object(scheduler_module.config, "get_mt5_best_lane_sources", return_value={"scalp_xauusd"}), \
             patch.object(scheduler_module.config, "get_mt5_best_lane_symbols", return_value={"XAUUSD"}), \
             patch.object(dexter, "_maybe_execute_mt5_signal") as exec_call:
            ok = dexter._maybe_execute_mt5_best_lane(sig, source="scalp_xauusd")

        self.assertFalse(ok)
        self.assertEqual(exec_call.call_count, 0)

    def test_best_lane_uses_symbol_specific_min_confidence_override(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=74.0)
        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_TAG", "winner"), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_MIN_CONFIDENCE", 78.0), \
             patch.object(scheduler_module.config, "get_mt5_best_lane_min_confidence_symbol_overrides", return_value={"BTCUSD": 74.0}), \
             patch.object(scheduler_module.config, "get_mt5_best_lane_sources", return_value={"scalp_btcusd"}), \
             patch.object(scheduler_module.config, "get_mt5_best_lane_symbols", return_value={"BTCUSD"}), \
             patch.object(dexter, "_maybe_execute_mt5_signal") as exec_call:
            ok = dexter._maybe_execute_mt5_best_lane(sig, source="scalp_btcusd")

        self.assertTrue(ok)
        self.assertEqual(exec_call.call_count, 1)
        self.assertEqual(exec_call.call_args.kwargs.get("source"), "scalp_btcusd:winner")

    def test_xau_scheduled_live_dispatches_canary_lane_for_new_york_h1(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=79.0)
        sig.session = "new_york"
        sig.timeframe = "1h"

        with patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_CANARY_ONLY", True), \
             patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE", 78.0), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_TAG", "winner"), \
             patch.object(scheduler_module.config, "get_mt5_xau_scheduled_live_sessions", return_value={"new_york"}), \
             patch.object(scheduler_module.config, "get_mt5_xau_scheduled_live_timeframes", return_value={"1h"}), \
             patch.object(dexter, "_dispatch_mt5_lane_signal", return_value=True) as dispatch_call:
            ok = dexter._maybe_execute_xau_scheduled_live(sig, scan_source="scheduled")

        self.assertTrue(ok)
        self.assertEqual(dispatch_call.call_count, 1)
        self.assertEqual(dispatch_call.call_args.args[1], "xauusd_scheduled:winner")

    def test_xau_scheduled_live_accepts_overlap_and_1h_plus_5m(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=84.0)
        sig.session = "london, new_york, overlap"
        sig.timeframe = "1h+5m"

        with patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_CANARY_ONLY", True), \
             patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE", 78.0), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_TAG", "winner"), \
             patch.object(scheduler_module.config, "get_mt5_xau_scheduled_live_sessions", return_value={"new_york", "overlap"}), \
             patch.object(scheduler_module.config, "get_mt5_xau_scheduled_live_timeframes", return_value={"1h"}), \
             patch.object(dexter, "_dispatch_mt5_lane_signal", return_value=True) as dispatch_call:
            ok = dexter._maybe_execute_xau_scheduled_live(sig, scan_source="scheduled")

        self.assertTrue(ok)
        self.assertEqual(dispatch_call.call_count, 1)
        self.assertEqual(dispatch_call.call_args.args[1], "xauusd_scheduled:winner")

    def test_xau_scheduled_live_logs_rejected_session_mismatch(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=84.0)
        sig.session = "asian"
        sig.timeframe = "1h+5m"

        with patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_CANARY_ONLY", True), \
             patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_NOTIFY_REJECTED", True), \
             patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE", 78.0), \
             patch.object(scheduler_module.config, "MT5_BEST_LANE_TAG", "winner"), \
             patch.object(scheduler_module.config, "get_mt5_xau_scheduled_live_sessions", return_value={"new_york", "overlap"}), \
             patch.object(scheduler_module.config, "get_mt5_xau_scheduled_live_timeframes", return_value={"1h"}), \
             patch.object(dexter, "_dispatch_mt5_lane_signal") as dispatch_call, \
             patch.object(dexter, "_handle_mt5_result") as handle_result, \
             patch.object(scheduler_module.notifier, "send_mt5_execution_update", return_value=True) as notify_call:
            ok = dexter._maybe_execute_xau_scheduled_live(sig, scan_source="scheduled")

        self.assertTrue(ok)
        self.assertEqual(dispatch_call.call_count, 0)
        self.assertEqual(handle_result.call_count, 1)
        self.assertEqual(notify_call.call_count, 1)
        self.assertEqual(handle_result.call_args.kwargs.get("source"), "xauusd_scheduled:winner")
        result = handle_result.call_args.args[1]
        self.assertEqual(str(getattr(result, "status", "")), "skipped")
        self.assertIn("session mismatch", str(getattr(result, "message", "")).lower())
        self.assertTrue(bool(getattr(sig, "raw_scores", {}).get("mt5_xau_scheduled_live_rejected")))

    def test_ctrader_prefers_crypto_winner_lane_when_allowed(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("ETHUSD", confidence=79.0)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores.update({
            "crypto_winner_logic_enabled": True,
            "crypto_winner_logic_regime": "strong",
        })
        fake_result = SimpleNamespace(status="accepted", signal_symbol="ETHUSD", broker_symbol="ETHUSD", message="ok")

        with patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_ETH_WINNER_DIRECT_ENABLED", True), \
             patch.object(scheduler_module.config, "get_ctrader_allowed_sources", return_value={"scalp_ethusd:winner"}), \
             patch.object(scheduler_module.ctrader_executor, "execute_signal", return_value=fake_result) as exec_call:
            result = dexter._maybe_execute_ctrader_signal(sig, source="scalp_ethusd")

        self.assertIs(result, fake_result)
        self.assertEqual(exec_call.call_count, 1)
        self.assertEqual(exec_call.call_args.kwargs.get("source"), "scalp_ethusd:winner")
        self.assertEqual(getattr(sig, "raw_scores", {}).get("ctrader_dispatch_source"), "scalp_ethusd:winner")

    def test_ctrader_eth_blocks_base_lane_when_winner_not_strong(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("ETHUSD", confidence=79.0)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores.update({
            "crypto_winner_logic_enabled": True,
            "crypto_winner_logic_regime": "weak",
        })

        fake_result = SimpleNamespace(status="accepted", signal_symbol="ETHUSD", broker_symbol="ETHUSD", message="ok")
        with patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_ETH_WINNER_DIRECT_ENABLED", True), \
             patch.object(scheduler_module.config, "get_ctrader_allowed_sources", return_value={"scalp_ethusd", "scalp_ethusd:winner"}), \
             patch.object(scheduler_module.ctrader_executor, "execute_signal", return_value=fake_result) as exec_call:
            dispatch_source, dispatch_meta = dexter._ctrader_pick_dispatch_source(sig, source="scalp_ethusd")
            self.assertEqual(dispatch_source, "")
            self.assertIn("eth_winner_memory_block", str(dispatch_meta.get("winner_reason", "")))
            result = dexter._maybe_execute_ctrader_signal(sig, source="scalp_ethusd")

        self.assertIsNone(result)
        self.assertEqual(exec_call.call_count, 0)

    def test_ctrader_xau_scheduled_routes_winner_lane_when_profile_matches(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=84.0)
        sig.pattern = "Behavioral Sweep-Retest + Liquidity Continuation"
        sig.session = "london, new_york, overlap"
        sig.timeframe = "1h+5m"
        sig.entry_type = "limit"
        fake_result = SimpleNamespace(status="accepted", signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE", 78.0), \
             patch.object(scheduler_module.config, "get_ctrader_allowed_sources", return_value={"xauusd_scheduled:winner"}), \
             patch.object(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_sessions", return_value={"new_york", "overlap"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_timeframes", return_value={"1h"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_entry_types", return_value={"limit"}), \
             patch.object(scheduler_module.config, "get_mt5_xau_scheduled_live_sessions", return_value={"new_york", "overlap"}), \
             patch.object(scheduler_module.config, "get_mt5_xau_scheduled_live_timeframes", return_value={"1h"}), \
             patch.object(scheduler_module.ctrader_executor, "execute_signal", return_value=fake_result) as exec_call:
            result = dexter._maybe_execute_ctrader_signal(sig, source="xauusd_scheduled")

        self.assertIs(result, fake_result)
        self.assertEqual(exec_call.call_args.kwargs.get("source"), "xauusd_scheduled:winner")
        self.assertEqual(getattr(sig, "raw_scores", {}).get("ctrader_dispatch_reason"), "xau_scheduled_live_profile")

    def test_ctrader_falls_back_to_base_lane_when_winner_not_selected(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=75.0)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores.update({
            "crypto_winner_logic_enabled": True,
            "crypto_winner_logic_regime": "weak",
        })
        fake_result = SimpleNamespace(status="accepted", signal_symbol="BTCUSD", broker_symbol="BTCUSD", message="ok")

        with patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_BTC_WINNER_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "CTRADER_BTC_WINNER_MAX_CONFIDENCE", 76.0), \
             patch.object(scheduler_module.config, "get_ctrader_btc_winner_allowed_sessions_weekend", return_value={"new_york"}), \
             patch.object(scheduler_module.config, "get_ctrader_allowed_sources", return_value={"scalp_btcusd", "scalp_btcusd:winner"}), \
             patch.object(scheduler_module.ctrader_executor, "execute_signal", return_value=fake_result) as exec_call:
            result = dexter._maybe_execute_ctrader_signal(sig, source="scalp_btcusd")

        self.assertIs(result, fake_result)
        self.assertEqual(exec_call.call_args.kwargs.get("source"), "scalp_btcusd")

    def test_ctrader_blocks_xau_scalp_outside_safe_live_filter(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=67.5)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.session = "asian"
        fake_result = SimpleNamespace(status="accepted", signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd"}), \
             patch.object(scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_SCALP_XAU_LIVE_FILTER_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_SCALP_XAU_LIVE_CONF_MIN", 72.0), \
             patch.object(scheduler_module.config, "MT5_SCALP_XAU_LIVE_CONF_MAX", 75.0), \
             patch.object(scheduler_module.config, "get_mt5_scalp_xau_live_sessions", return_value={"new_york"}), \
             patch.object(scheduler_module.ctrader_executor, "journal_pre_dispatch_skip", return_value=23) as audit_call, \
             patch.object(scheduler_module.ctrader_executor, "execute_signal", return_value=fake_result) as exec_call:
            result = dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertIsNone(result)
        self.assertEqual(exec_call.call_count, 0)
        self.assertEqual(audit_call.call_count, 1)
        self.assertEqual(audit_call.call_args.kwargs.get("gate"), "xau_live_filter")
        self.assertIn("session_not_allowed", str(audit_call.call_args.kwargs.get("reason", "")))

    def test_persistent_canary_dispatches_separate_mt5_and_ctrader_lanes(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.entry_type = "limit"
        fake_ctr = SimpleNamespace(ok=True, dry_run=False, status="accepted", signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", False), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", False), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_RUN_PARALLEL", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_MT5_VOLUME_MULTIPLIER", 0.2), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_MT5_MAGIC_OFFSET", 700), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_CTRADER_RISK_USD", 2.5), \
             patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_EXECUTE_XAUUSD", True), \
             patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "get_persistent_canary_allowed_sources", return_value={"scalp_xauusd"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_direct_allowed_sources", return_value={"scalp_xauusd"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_allowed_symbols", return_value={"XAUUSD"}), \
             patch.object(dexter, "_maybe_execute_mt5_signal") as mt5_call, \
             patch.object(scheduler_module.ctrader_executor, "execute_signal", return_value=fake_ctr) as ctr_call:
            rpt = dexter._maybe_execute_persistent_canary(sig, source="scalp_xauusd")

        self.assertTrue(rpt.get("enabled"))
        self.assertTrue(rpt.get("mt5"))
        self.assertTrue(rpt.get("ctrader"))
        self.assertEqual(mt5_call.call_count, 1)
        self.assertEqual(ctr_call.call_count, 1)
        mt5_signal = mt5_call.call_args.args[0]
        self.assertEqual(mt5_call.call_args.kwargs.get("source"), "scalp_xauusd:canary")
        self.assertEqual(str(getattr(mt5_signal, "raw_scores", {}).get("persistent_canary_source")), "scalp_xauusd:canary")
        self.assertTrue(bool(getattr(mt5_signal, "raw_scores", {}).get("mt5_canary_mode")))
        self.assertEqual(int(getattr(mt5_signal, "raw_scores", {}).get("mt5_magic_offset", 0)), 700)
        self.assertAlmostEqual(float(getattr(mt5_signal, "raw_scores", {}).get("mt5_extra_volume_multiplier", 0.0)), 0.2, places=3)
        ctr_signal = ctr_call.call_args.args[0]
        self.assertEqual(ctr_call.call_args.kwargs.get("source"), "scalp_xauusd:canary")
        self.assertAlmostEqual(float(getattr(ctr_signal, "raw_scores", {}).get("ctrader_risk_usd_override", 0.0)), 2.5, places=3)

    def test_persistent_canary_dispatches_family_variants_for_xau(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=78.0)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.entry = 5100.0
        sig.stop_loss = 5095.0
        sig.take_profit_1 = 5104.0
        sig.take_profit_2 = 5107.0
        sig.take_profit_3 = 5110.0
        sig.atr = 6.0
        fake_ctr = SimpleNamespace(ok=True, dry_run=False, status="accepted", signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")
        candidates = [
            {"family": "xau_scalp_pullback_limit", "strategy_id": "xau_scalp_pullback_limit_v1", "priority": 3, "execution_ready": True},
            {"family": "xau_scalp_breakout_stop", "strategy_id": "xau_scalp_breakout_stop_v1", "priority": 4, "execution_ready": True},
        ]

        with ExitStack() as stack:
            stack.enter_context(patch.object(scheduler_module.config, "PERSISTENT_CANARY_ENABLED", True))
            stack.enter_context(patch.object(scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", False))
            stack.enter_context(patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", True))
            stack.enter_context(patch.object(scheduler_module.config, "PERSISTENT_CANARY_MT5_ENABLED", True))
            stack.enter_context(patch.object(scheduler_module.config, "PERSISTENT_CANARY_CTRADER_ENABLED", True))
            stack.enter_context(patch.object(scheduler_module.config, "XAU_OPENAPI_ENTRY_ROUTER_ENABLED", False))
            stack.enter_context(patch.object(scheduler_module.config, "PERSISTENT_CANARY_RUN_PARALLEL", True))
            stack.enter_context(patch.object(scheduler_module.config, "PERSISTENT_CANARY_MT5_VOLUME_MULTIPLIER", 0.2))
            stack.enter_context(patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_MT5_VOLUME_MULTIPLIER", 0.1))
            stack.enter_context(patch.object(scheduler_module.config, "PERSISTENT_CANARY_MT5_MAGIC_OFFSET", 700))
            stack.enter_context(patch.object(scheduler_module.config, "PERSISTENT_CANARY_CTRADER_RISK_USD", 2.5))
            stack.enter_context(patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_CTRADER_RISK_USD", 1.25))
            stack.enter_context(patch.object(scheduler_module.config, "XAU_PULLBACK_LIMIT_ENTRY_ATR", 0.12))
            stack.enter_context(patch.object(scheduler_module.config, "XAU_PULLBACK_LIMIT_STOP_PAD_ATR", 0.04))
            stack.enter_context(patch.object(scheduler_module.config, "XAU_BREAKOUT_STOP_TRIGGER_ATR", 0.10))
            stack.enter_context(patch.object(scheduler_module.config, "XAU_BREAKOUT_STOP_STOP_LIFT_RATIO", 0.45))
            stack.enter_context(patch.object(scheduler_module.config, "MT5_ENABLED", True))
            stack.enter_context(patch.object(scheduler_module.config, "MT5_EXECUTE_XAUUSD", True))
            stack.enter_context(patch.object(scheduler_module.config, "CTRADER_ENABLED", True))
            stack.enter_context(patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True))
            stack.enter_context(patch.object(scheduler_module.config, "XAU_PB_NARROW_CONTEXT_ENABLED", False))
            stack.enter_context(patch.object(scheduler_module.config, "get_persistent_canary_allowed_sources", return_value={"scalp_xauusd"}))
            stack.enter_context(patch.object(scheduler_module.config, "get_persistent_canary_direct_allowed_sources", return_value={"scalp_xauusd"}))
            stack.enter_context(patch.object(scheduler_module.config, "get_persistent_canary_allowed_symbols", return_value={"XAUUSD"}))
            stack.enter_context(patch.object(dexter, "_load_strategy_family_candidates", return_value=candidates))
            mt5_call = stack.enter_context(patch.object(dexter, "_maybe_execute_mt5_signal"))
            ctr_call = stack.enter_context(patch.object(scheduler_module.ctrader_executor, "execute_signal", return_value=fake_ctr))
            rpt = dexter._maybe_execute_persistent_canary(sig, source="scalp_xauusd")

        self.assertTrue(rpt.get("enabled"))
        self.assertEqual(mt5_call.call_count, 3)
        self.assertEqual(ctr_call.call_count, 3)
        families = list(rpt.get("family_variants") or [])
        self.assertEqual(len(families), 2)
        pb_signal = mt5_call.call_args_list[1].args[0]
        bs_signal = mt5_call.call_args_list[2].args[0]
        self.assertEqual(mt5_call.call_args_list[1].kwargs.get("source"), "scalp_xauusd:pb:canary")
        self.assertEqual(mt5_call.call_args_list[2].kwargs.get("source"), "scalp_xauusd:bs:canary")
        self.assertEqual(str(getattr(pb_signal, "entry_type", "")), "limit")
        self.assertEqual(str(getattr(bs_signal, "entry_type", "")), "buy_stop")
        self.assertLess(float(getattr(pb_signal, "entry", 0.0)), float(getattr(sig, "entry", 0.0)))
        self.assertGreater(float(getattr(bs_signal, "entry", 0.0)), float(getattr(sig, "entry", 0.0)))
        self.assertTrue(bool(getattr(pb_signal, "raw_scores", {}).get("mt5_canary_mode")))
        self.assertTrue(bool(getattr(bs_signal, "raw_scores", {}).get("mt5_canary_mode")))

    def test_persistent_canary_rebalances_xau_scheduled_canary_rr_before_ctrader_execute(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=76.0)
        sig.entry = 100.0
        sig.stop_loss = 90.0
        sig.take_profit_1 = 105.0
        sig.take_profit_2 = 108.0
        sig.take_profit_3 = 110.0
        sig.entry_type = "market"
        fake_ctr = SimpleNamespace(ok=True, dry_run=False, status="accepted", signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", False), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", False), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_MT5_ENABLED", False), \
             patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_RETEST_ENABLED", False), \
             patch.object(scheduler_module.config, "CTRADER_SCHEDULED_CANARY_RR_REBALANCE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_SCHEDULED_CANARY_MIN_RR", 0.85), \
             patch.object(scheduler_module.config, "CTRADER_SCHEDULED_CANARY_MIN_STOP_KEEP_RATIO", 0.58), \
             patch.object(scheduler_module.config, "get_persistent_canary_allowed_sources", return_value={"xauusd_scheduled"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_direct_allowed_sources", return_value={"xauusd_scheduled"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_allowed_symbols", return_value={"XAUUSD"}), \
             patch.object(dexter, "_load_strategy_family_candidates", return_value=[]), \
             patch.object(scheduler_module.ctrader_executor, "execute_signal", return_value=fake_ctr) as ctr_call:
            rpt = dexter._maybe_execute_persistent_canary(sig, source="xauusd_scheduled")

        self.assertTrue(rpt.get("enabled"))
        self.assertTrue(rpt.get("ctrader"))
        sent = ctr_call.call_args.args[0]
        self.assertEqual(ctr_call.call_args.kwargs.get("source"), "xauusd_scheduled:canary")
        self.assertAlmostEqual(float(getattr(sent, "stop_loss", 0.0)), 94.1176, places=3)
        self.assertTrue(bool(getattr(sent, "raw_scores", {}).get("scheduled_canary_rr_rebalanced")))
        self.assertAlmostEqual(float(getattr(sent, "raw_scores", {}).get("scheduled_canary_rr_after", 0.0)), 0.85, places=2)

    def test_persistent_canary_converts_xau_scheduled_market_entry_to_limit_retest(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=76.0)
        sig.entry = 100.0
        sig.stop_loss = 90.0
        sig.take_profit_1 = 105.0
        sig.take_profit_2 = 108.0
        sig.take_profit_3 = 110.0
        sig.entry_type = "market"
        fake_ctr = SimpleNamespace(ok=True, dry_run=False, status="accepted", signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", False), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", False), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_MT5_ENABLED", False), \
             patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_RETEST_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_PULLBACK_RISK_RATIO", 0.20), \
             patch.object(scheduler_module.config, "CTRADER_SCHEDULED_CANARY_MARKET_TO_LIMIT_MIN_OFFSET_PCT", 0.0), \
             patch.object(scheduler_module.config, "CTRADER_SCHEDULED_CANARY_RR_REBALANCE_ENABLED", False), \
             patch.object(scheduler_module.config, "get_persistent_canary_allowed_sources", return_value={"xauusd_scheduled"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_direct_allowed_sources", return_value={"xauusd_scheduled"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_allowed_symbols", return_value={"XAUUSD"}), \
             patch.object(dexter, "_load_strategy_family_candidates", return_value=[]), \
             patch.object(scheduler_module.ctrader_executor, "execute_signal", return_value=fake_ctr) as ctr_call:
            rpt = dexter._maybe_execute_persistent_canary(sig, source="xauusd_scheduled")

        self.assertTrue(rpt.get("enabled"))
        self.assertTrue(rpt.get("ctrader"))
        sent = ctr_call.call_args.args[0]
        self.assertEqual(ctr_call.call_args.kwargs.get("source"), "xauusd_scheduled:canary")
        self.assertEqual(str(getattr(sent, "entry_type", "")), "limit")
        self.assertAlmostEqual(float(getattr(sent, "entry", 0.0)), 98.0, places=3)
        self.assertTrue(bool(getattr(sent, "raw_scores", {}).get("scheduled_canary_market_to_limit_retest")))

    def test_strategy_family_candidates_include_experimental_xau_td_without_active_family_membership(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_tick_depth_filter",
                    "strategy_id": "xau_scalp_tick_depth_filter_v1",
                    "priority": 12,
                    "execution_ready": True,
                    "experimental": True,
                }
            ]
        }
        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 2), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 1), \
             patch.object(scheduler_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(scheduler_module.Path, "exists", return_value=True), \
             patch.object(scheduler_module.Path, "read_text", return_value=json.dumps(payload)):
            rows = dexter._load_strategy_family_candidates(symbol="XAUUSD", base_source="scalp_xauusd")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["family"], "xau_scalp_tick_depth_filter")
        self.assertTrue(bool(rows[0]["experimental"]))

    def test_strategy_family_candidates_include_experimental_xau_range_repair_fallback(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {"candidates": []}
        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", False), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 2), \
             patch.object(scheduler_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_range_repair"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_range_repair"}), \
             patch.object(scheduler_module.Path, "exists", return_value=True), \
             patch.object(scheduler_module.Path, "read_text", return_value=json.dumps(payload)):
            rows = dexter._load_strategy_family_candidates(symbol="XAUUSD", base_source="scalp_xauusd")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["family"], "xau_scalp_range_repair")
        self.assertEqual(rows[0]["strategy_id"], "xau_scalp_range_repair_v1")
        self.assertTrue(bool(rows[0]["experimental"]))

    def test_strategy_family_candidates_reserve_flow_short_sidecar_when_opportunity_sidecar_active(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_tick_depth_filter",
                    "strategy_id": "xau_scalp_tick_depth_filter_v1",
                    "priority": 5,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_microtrend_follow_up",
                    "strategy_id": "xau_scalp_microtrend_follow_up_v1",
                    "priority": 6,
                    "execution_ready": True,
                    "experimental": True,
                },
            ]
        }
        manager_state = {
            "xau_parallel_families": {
                "status": "active",
                "allowed_families": [
                    "xau_scalp_pullback_limit",
                    "xau_scalp_tick_depth_filter",
                    "xau_scalp_microtrend_follow_up",
                    "xau_scalp_flow_short_sidecar",
                ],
                "max_same_direction_families": 3,
            },
            "xau_opportunity_sidecar": {
                "status": "active",
                "mode": "xau_short_flow_sidecar",
            },
        }
        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 1), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 2), \
             patch.object(scheduler_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up", "xau_scalp_flow_short_sidecar"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(dexter, "_load_trading_manager_runtime_state", return_value=manager_state), \
             patch.object(scheduler_module.Path, "exists", return_value=True), \
             patch.object(scheduler_module.Path, "read_text", return_value=json.dumps(payload)):
            rows = dexter._load_strategy_family_candidates(symbol="XAUUSD", base_source="scalp_xauusd")

        families = [str(row.get("family") or "") for row in rows if bool(row.get("experimental"))]
        self.assertIn("xau_scalp_flow_short_sidecar", families)
        self.assertIn("xau_scalp_tick_depth_filter", families)

    def test_strategy_family_candidates_reorders_from_manager_opportunity_feed(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_tick_depth_filter",
                    "strategy_id": "xau_scalp_tick_depth_filter_v1",
                    "priority": 5,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_microtrend_follow_up",
                    "strategy_id": "xau_scalp_microtrend_follow_up_v1",
                    "priority": 6,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_flow_short_sidecar",
                    "strategy_id": "xau_scalp_flow_short_sidecar_v1",
                    "priority": 50,
                    "execution_ready": True,
                    "experimental": True,
                },
            ]
        }
        manager_state = {
            "opportunity_feed": {
                "status": "active",
                "symbols": {
                    "XAUUSD": {
                        "family_priority_map": {
                            "xau_scalp_flow_short_sidecar": 99.0,
                            "xau_scalp_microtrend_follow_up": 94.0,
                            "xau_scalp_tick_depth_filter": 80.0,
                        }
                    }
                },
            }
        }
        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 1), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 3), \
             patch.object(scheduler_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up", "xau_scalp_flow_short_sidecar"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(dexter, "_load_trading_manager_runtime_state", return_value=manager_state), \
             patch.object(scheduler_module.Path, "exists", return_value=True), \
             patch.object(scheduler_module.Path, "read_text", return_value=json.dumps(payload)):
            rows = dexter._load_strategy_family_candidates(symbol="XAUUSD", base_source="scalp_xauusd")

        exp_families = [str(row.get("family") or "") for row in rows if bool(row.get("experimental"))]
        self.assertGreaterEqual(len(exp_families), 3)
        self.assertEqual(exp_families[:3], [
            "xau_scalp_flow_short_sidecar",
            "xau_scalp_microtrend_follow_up",
            "xau_scalp_tick_depth_filter",
        ])

    def test_strategy_family_candidates_follow_xau_execution_directive_without_hard_family_removal(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_tick_depth_filter",
                    "strategy_id": "xau_scalp_tick_depth_filter_v1",
                    "priority": 5,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_flow_short_sidecar",
                    "strategy_id": "xau_scalp_flow_short_sidecar_v1",
                    "priority": 50,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_microtrend",
                    "strategy_id": "xau_scalp_microtrend_v1",
                    "priority": 3,
                    "execution_ready": True,
                },
            ]
        }
        manager_state = {
            "xau_execution_directive": {
                "status": "active",
                "mode": "family_disagreement_limit_pause",
                "blocked_families": ["xau_scalp_microtrend", "xau_scalp_tick_depth_filter"],
                "preferred_families": ["xau_scalp_flow_short_sidecar"],
                "pause_until_utc": "2099-03-19T23:59:00Z",
            },
            "opportunity_feed": {
                "status": "active",
                "symbols": {
                    "XAUUSD": {
                        "family_priority_map": {
                            "xau_scalp_microtrend": 99.0,
                            "xau_scalp_tick_depth_filter": 95.0,
                            "xau_scalp_flow_short_sidecar": 80.0,
                        }
                    }
                },
            },
        }
        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 2), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 2), \
             patch.object(scheduler_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_microtrend"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_flow_short_sidecar"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_microtrend", "xau_scalp_flow_short_sidecar"}), \
             patch.object(dexter, "_load_trading_manager_runtime_state", return_value=manager_state), \
             patch.object(scheduler_module.Path, "exists", return_value=True), \
            patch.object(scheduler_module.Path, "read_text", return_value=json.dumps(payload)):
            rows = dexter._load_strategy_family_candidates(symbol="XAUUSD", base_source="scalp_xauusd")

        families = [str(row.get("family") or "") for row in rows]
        self.assertIn("xau_scalp_flow_short_sidecar", families)
        self.assertIn("xau_scalp_tick_depth_filter", families)
        self.assertIn("xau_scalp_microtrend", families)
        experimental_families = [str(row.get("family") or "") for row in rows if bool(row.get("experimental"))]
        self.assertTrue(experimental_families)
        self.assertEqual(experimental_families[0], "xau_scalp_flow_short_sidecar")

    def test_allow_ctrader_source_profile_blocks_short_limit_from_manager_execution_directive(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "short"
        sig.entry_type = "limit"
        runtime_state = {
            "xau_execution_directive": {
                "status": "active",
                "mode": "family_disagreement_limit_pause",
                "reason": "confirmation short held while short-limit lanes failed on the same run",
                "blocked_direction": "short",
                "blocked_entry_types": ["limit", "patience"],
                "blocked_families": ["xau_scalp_microtrend", "xau_scalp_tick_depth_filter"],
                "blocked_sources": ["scalp_xauusd:canary", "scalp_xauusd:td:canary"],
                "preferred_families": ["xau_scalp_flow_short_sidecar"],
                "pause_until_utc": "2099-03-19T23:59:00Z",
                "trigger_run_id": "run-disagree-1",
            }
        }
        with patch.object(dexter, "_load_trading_routing_runtime_state", return_value=runtime_state):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, "scalp_xauusd:canary")

        self.assertFalse(allowed)
        self.assertIn("xau_manager_directive_block", reason)
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertTrue(bool(raw.get("xau_manager_directive_block")))
        self.assertEqual(str((raw.get("xau_manager_execution_directive") or {}).get("trigger_run_id") or ""), "run-disagree-1")

    def test_allow_ctrader_source_profile_keeps_fss_confirmation_short_under_manager_execution_directive(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "short"
        sig.entry_type = "sell_stop"
        runtime_state = {
            "xau_execution_directive": {
                "status": "active",
                "mode": "family_disagreement_limit_pause",
                "reason": "confirmation short held while short-limit lanes failed on the same run",
                "blocked_direction": "short",
                "blocked_entry_types": ["limit", "patience"],
                "blocked_families": ["xau_scalp_microtrend", "xau_scalp_tick_depth_filter"],
                "blocked_sources": ["scalp_xauusd:canary", "scalp_xauusd:td:canary"],
                "preferred_families": ["xau_scalp_flow_short_sidecar"],
                "preferred_sources": ["scalp_xauusd:fss:canary"],
                "pause_until_utc": "2099-03-19T23:59:00Z",
                "trigger_run_id": "run-disagree-1",
            }
        }
        with patch.object(dexter, "_load_trading_routing_runtime_state", return_value=runtime_state):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, "scalp_xauusd:fss:canary")

        self.assertTrue(allowed)
        self.assertEqual(reason, "source_profile_pass")
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertEqual(str(raw.get("xau_manager_directive_priority") or ""), "preferred_family")

    def test_allow_ctrader_source_profile_does_not_block_non_matching_long_limit(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "long"
        sig.entry_type = "limit"
        runtime_state = {
            "xau_execution_directive": {
                "status": "active",
                "mode": "family_disagreement_limit_pause",
                "reason": "confirmation short held while short-limit lanes failed on the same run",
                "blocked_direction": "short",
                "blocked_entry_types": ["limit", "patience"],
                "blocked_families": ["xau_scalp_microtrend", "xau_scalp_tick_depth_filter"],
                "blocked_sources": ["scalp_xauusd:canary", "scalp_xauusd:td:canary"],
                "preferred_families": ["xau_scalp_flow_short_sidecar"],
                "pause_until_utc": "2099-03-19T23:59:00Z",
                "trigger_run_id": "run-disagree-1",
            }
        }
        with patch.object(dexter, "_load_trading_routing_runtime_state", return_value=runtime_state):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, "scalp_xauusd:td:canary")

        self.assertTrue(allowed)
        self.assertEqual(reason, "source_profile_pass")
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertFalse(bool(raw.get("xau_manager_directive_block")))

    def test_allow_ctrader_source_profile_blocks_short_limit_from_live_regime_transition(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "short"
        sig.entry_type = "limit"
        runtime_state = {
            "xau_regime_transition": {
                "status": "active",
                "mode": "live_range_transition_limit_pause",
                "reason": "live short continuation degraded to reversal_exhaustion | day=trend rej=0.46 bias=-0.010",
                "support_state": "range_repair_lead",
                "current_side": "short",
                "state_label": "reversal_exhaustion",
                "day_type": "trend",
                "blocked_families": ["xau_scalp_microtrend", "xau_scalp_tick_depth_filter"],
                "preferred_families": ["xau_scalp_range_repair"],
                "hold_until_utc": "2099-03-19T23:59:00Z",
                "snapshot_run_id": "capture-range-1",
            },
            "xau_execution_directive": {
                "status": "active",
                "mode": "live_range_transition_limit_pause",
                "reason": "live short continuation degraded to reversal_exhaustion | day=trend rej=0.46 bias=-0.010",
                "blocked_direction": "short",
                "blocked_entry_types": ["limit", "patience"],
                "blocked_families": ["xau_scalp_microtrend", "xau_scalp_tick_depth_filter"],
                "blocked_sources": ["scalp_xauusd:canary", "scalp_xauusd:td:canary"],
                "preferred_families": ["xau_scalp_range_repair"],
                "preferred_sources": ["scalp_xauusd:rr:canary"],
                "pause_until_utc": "2099-03-19T23:59:00Z",
                "trigger_run_id": "capture-range-1",
            },
        }
        with patch.object(dexter, "_load_trading_routing_runtime_state", return_value=runtime_state):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, "scalp_xauusd:canary")

        self.assertFalse(allowed)
        self.assertIn("xau_manager_directive_block", reason)
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertEqual(str((raw.get("xau_manager_regime_transition") or {}).get("state_label") or ""), "reversal_exhaustion")
        self.assertEqual(str((raw.get("xau_manager_execution_directive") or {}).get("trigger_run_id") or ""), "capture-range-1")

    def test_allow_ctrader_source_profile_keeps_fss_short_confirmation_under_live_regime_transition(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "short"
        sig.entry_type = "sell_stop"
        runtime_state = {
            "xau_regime_transition": {
                "status": "active",
                "mode": "live_range_transition_limit_pause",
                "reason": "live short continuation degraded to reversal_exhaustion | day=trend rej=0.46 bias=-0.010",
                "support_state": "range_repair_lead",
                "current_side": "short",
                "state_label": "reversal_exhaustion",
                "day_type": "trend",
                "blocked_families": ["xau_scalp_microtrend", "xau_scalp_tick_depth_filter"],
                "preferred_families": ["xau_scalp_range_repair"],
                "hold_until_utc": "2099-03-19T23:59:00Z",
                "snapshot_run_id": "capture-range-1",
            },
            "xau_execution_directive": {
                "status": "active",
                "mode": "live_range_transition_limit_pause",
                "reason": "live short continuation degraded to reversal_exhaustion | day=trend rej=0.46 bias=-0.010",
                "blocked_direction": "short",
                "blocked_entry_types": ["limit", "patience"],
                "blocked_families": ["xau_scalp_microtrend", "xau_scalp_tick_depth_filter"],
                "blocked_sources": ["scalp_xauusd:canary", "scalp_xauusd:td:canary"],
                "preferred_families": ["xau_scalp_range_repair"],
                "preferred_sources": ["scalp_xauusd:rr:canary"],
                "pause_until_utc": "2099-03-19T23:59:00Z",
                "trigger_run_id": "capture-range-1",
            },
        }
        with patch.object(dexter, "_load_trading_routing_runtime_state", return_value=runtime_state):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, "scalp_xauusd:fss:canary")

        self.assertTrue(allowed)
        self.assertEqual(reason, "source_profile_pass")
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertEqual(str((raw.get("xau_manager_regime_transition") or {}).get("mode") or ""), "live_range_transition_limit_pause")

    def test_allow_ctrader_source_profile_blocks_forced_short_continuation_on_no_setup_reason(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.5)
        sig.direction = "short"
        sig.entry_type = "limit"
        sig.raw_scores = {
            "scalping_trigger": {
                "forced_mode": True,
                "forced_from_status": "no_signal",
                "forced_from_reason": "m1_short_not_confirmed",
                "xau_diag_status": "no_setup",
            }
        }
        runtime_state = {
            "xau_shock_profile": {
                "status": "active",
                "mode": "shock_protect",
            }
        }
        with patch.object(dexter, "_load_trading_routing_runtime_state", return_value=runtime_state), \
             patch.object(scheduler_module.config, "SCALPING_XAU_FORCE_BLOCK_REASONS", "no_signal,base_scanner_no_signal,no_direction_passed_threshold,m1_short_not_confirmed"):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, "scalp_xauusd:canary")

        self.assertFalse(allowed)
        self.assertEqual(reason, "xau_forced_continuation_block")
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertTrue(bool(raw.get("xau_forced_style_block")))
        self.assertTrue(bool(raw.get("xau_forced_style_block_shock_protect")))
        self.assertEqual(str(raw.get("xau_routing_desk") or ""), "limit_retest")

    def test_allow_ctrader_source_profile_blocks_countertrend_long_without_confirmation(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.5)
        sig.direction = "long"
        sig.entry_type = "limit"
        sig.raw_scores = {
            "xau_multi_tf_snapshot": {
                "aligned_side": "short",
                "strict_aligned_side": "short",
                "countertrend_confirmed": False,
            }
        }
        with patch.object(dexter, "_load_trading_routing_runtime_state", return_value={}), \
             patch.object(scheduler_module.config, "XAU_COUNTERTREND_LONG_REQUIRE_CONFIRMED", True):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, "scalp_xauusd:canary")

        self.assertFalse(allowed)
        self.assertEqual(reason, "xau_countertrend_long_unconfirmed")
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertTrue(bool(raw.get("xau_countertrend_long_block")))

    def test_strategy_family_candidates_returns_all_swarm_families_without_variant_truncation(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_pullback_limit",
                    "strategy_id": "xau_scalp_pullback_limit_v1",
                    "priority": 20,
                    "execution_ready": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_tick_depth_filter",
                    "strategy_id": "xau_scalp_tick_depth_filter_v1",
                    "priority": 21,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_microtrend_follow_up",
                    "strategy_id": "xau_scalp_microtrend_follow_up_v1",
                    "priority": 22,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_flow_short_sidecar",
                    "strategy_id": "xau_scalp_flow_short_sidecar_v1",
                    "priority": 23,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_failed_fade_follow_stop",
                    "strategy_id": "xau_scalp_failed_fade_follow_stop_v1",
                    "priority": 24,
                    "execution_ready": True,
                    "experimental": True,
                },
            ]
        }
        manager_state = {
            "xau_family_routing": {
                "status": "active",
                "mode": "swarm_support_all",
            },
            "xau_parallel_families": {
                "status": "active",
                "allowed_families": [
                    "xau_scalp_pullback_limit",
                    "xau_scalp_tick_depth_filter",
                    "xau_scalp_microtrend_follow_up",
                    "xau_scalp_flow_short_sidecar",
                    "xau_scalp_failed_fade_follow_stop",
                ],
                "max_same_direction_families": 3,
            },
            "opportunity_feed": {
                "status": "active",
                "symbols": {
                    "XAUUSD": {
                        "support_all_families": [
                            "xau_scalp_pullback_limit",
                            "xau_scalp_tick_depth_filter",
                            "xau_scalp_microtrend_follow_up",
                            "xau_scalp_flow_short_sidecar",
                            "xau_scalp_failed_fade_follow_stop",
                        ]
                    }
                },
            },
        }
        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 1), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 1), \
             patch.object(scheduler_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up", "xau_scalp_flow_short_sidecar", "xau_scalp_failed_fade_follow_stop"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(dexter, "_load_trading_manager_runtime_state", return_value=manager_state), \
             patch.object(scheduler_module.Path, "exists", return_value=True), \
             patch.object(scheduler_module.Path, "read_text", return_value=json.dumps(payload)):
            rows = dexter._load_strategy_family_candidates(symbol="XAUUSD", base_source="scalp_xauusd")

        families = [str(row.get("family") or "") for row in rows]
        self.assertEqual(
            set(families),
            {
                "xau_scalp_pullback_limit",
                "xau_scalp_tick_depth_filter",
                "xau_scalp_microtrend_follow_up",
                "xau_scalp_flow_short_sidecar",
                "xau_scalp_failed_fade_follow_stop",
            },
        )
        self.assertEqual(len(families), 5)

    def test_strategy_family_candidates_prefers_trading_team_state_over_manager_state(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_tick_depth_filter",
                    "strategy_id": "xau_scalp_tick_depth_filter_v1",
                    "priority": 5,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_microtrend_follow_up",
                    "strategy_id": "xau_scalp_microtrend_follow_up_v1",
                    "priority": 6,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_flow_short_sidecar",
                    "strategy_id": "xau_scalp_flow_short_sidecar_v1",
                    "priority": 7,
                    "execution_ready": True,
                    "experimental": True,
                },
            ]
        }
        team_state = {
            "xau_family_routing": {
                "status": "active",
                "mode": "team_primary_advisory",
                "primary_family": "xau_scalp_tick_depth_filter",
                "active_families": [
                    "xau_scalp_tick_depth_filter",
                    "xau_scalp_microtrend_follow_up",
                    "xau_scalp_flow_short_sidecar",
                ],
            },
            "opportunity_feed": {
                "status": "active",
                "symbols": {
                    "XAUUSD": {
                        "family_priority_map": {
                            "xau_scalp_tick_depth_filter": 99.0,
                            "xau_scalp_microtrend_follow_up": 89.0,
                            "xau_scalp_flow_short_sidecar": 80.0,
                        }
                    }
                },
            },
        }
        manager_state = {
            "opportunity_feed": {
                "status": "active",
                "symbols": {
                    "XAUUSD": {
                        "family_priority_map": {
                            "xau_scalp_flow_short_sidecar": 120.0,
                            "xau_scalp_microtrend_follow_up": 70.0,
                            "xau_scalp_tick_depth_filter": 60.0,
                        }
                    }
                },
            }
        }
        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 1), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 3), \
             patch.object(scheduler_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up", "xau_scalp_flow_short_sidecar"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(dexter, "_load_trading_team_runtime_state", return_value=team_state), \
             patch.object(dexter, "_load_trading_manager_runtime_state", return_value=manager_state), \
             patch.object(scheduler_module.Path, "exists", return_value=True), \
             patch.object(scheduler_module.Path, "read_text", return_value=json.dumps(payload)):
            rows = dexter._load_strategy_family_candidates(symbol="XAUUSD", base_source="scalp_xauusd")

        exp_families = [str(row.get("family") or "") for row in rows if bool(row.get("experimental"))]
        self.assertGreaterEqual(len(exp_families), 3)
        self.assertEqual(exp_families[:3], [
            "xau_scalp_tick_depth_filter",
            "xau_scalp_microtrend_follow_up",
            "xau_scalp_flow_short_sidecar",
        ])

    def test_build_family_canary_signal_for_tick_depth_filter_uses_capture_gate(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.5)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.entry = 5195.5
        sig.stop_loss = 5191.0
        sig.take_profit_1 = 5198.0
        sig.take_profit_2 = 5200.5
        sig.take_profit_3 = 5203.0
        candidate = {
            "family": "xau_scalp_tick_depth_filter",
            "strategy_id": "xau_scalp_tick_depth_filter_v1",
            "priority": 11,
            "execution_ready": True,
            "experimental": True,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_test",
            "last_event_utc": "2026-03-11T01:00:00Z",
            "gate": {
                "pass": True,
                "score": 4,
                "score_ratio": 0.8,
                "features": {
                    "spread_avg_pct": 0.0016,
                    "depth_imbalance": -0.04,
                    "depth_refill_shift": -0.08,
                },
            },
        }
        with patch.object(scheduler_module.config, "XAU_TICK_DEPTH_FILTER_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_TICK_DEPTH_FILTER_LOOKBACK_SEC", 240), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_CTRADER_RISK_USD", 0.75), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:td:canary")
        self.assertEqual(str(getattr(lane_signal, "entry_type", "")), "limit")
        self.assertEqual(float(getattr(lane_signal, "entry", 0.0)), 5195.5)
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertTrue(bool(raw.get("experimental_family")))
        self.assertEqual(str(raw.get("strategy_family")), "xau_scalp_tick_depth_filter")
        self.assertEqual(str((raw.get("tick_depth_filter_snapshot") or {}).get("run_id")), "ctcap_test")
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0.0)), 0.75, places=3)

    def test_build_family_canary_signal_for_tick_depth_filter_stamps_top_level_chart_state_tags(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.5)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.direction = "short"
        sig.entry_type = "limit"
        sig.entry = 5195.5
        sig.stop_loss = 5200.0
        sig.take_profit_1 = 5192.0
        sig.take_profit_2 = 5190.0
        sig.take_profit_3 = 5188.0
        sig.raw_scores["signal_h1_trend"] = "bearish"
        sig.raw_scores["signal_h4_trend"] = "bearish"
        candidate = {
            "family": "xau_scalp_tick_depth_filter",
            "strategy_id": "xau_scalp_tick_depth_filter_v1",
            "priority": 11,
            "execution_ready": True,
            "experimental": True,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_td_obs",
            "last_event_utc": "2026-03-11T01:00:00Z",
            "gate": {
                "pass": True,
                "features": {
                    "day_type": "repricing",
                    "delta_proxy": -0.16,
                    "bar_volume_proxy": 0.62,
                },
            },
        }
        with patch.object(scheduler_module.config, "XAU_TICK_DEPTH_FILTER_ENABLED", True), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "repricing_transition",
                 "day_type": "repricing",
                 "continuation_bias": -0.19,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:td:canary")
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertEqual(str(raw.get("state_label") or ""), "repricing_transition")
        self.assertEqual(str(raw.get("day_type") or ""), "repricing")
        self.assertEqual(str(raw.get("follow_up_plan") or ""), "")
        self.assertEqual(str(raw.get("xau_routing_desk") or ""), "limit_retest")

    def test_build_family_canary_signal_for_range_repair_uses_isolated_range_desk(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.4)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.direction = "long"
        sig.entry_type = "limit"
        sig.entry = 5195.5
        sig.stop_loss = 5191.0
        sig.take_profit_1 = 5198.5
        sig.take_profit_2 = 5201.5
        sig.take_profit_3 = 5205.0
        candidate = {
            "family": "xau_scalp_range_repair",
            "strategy_id": "xau_scalp_range_repair_v1",
            "priority": 9,
            "execution_ready": True,
            "experimental": True,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_rr",
            "last_event_utc": "2026-03-20T02:00:00Z",
            "features": {
                "rejection_ratio": 0.44,
                "bar_volume_proxy": 0.34,
                "spread_expansion": 1.04,
                "spread_avg_pct": 0.0016,
                "delta_proxy": 0.03,
                "depth_imbalance": 0.01,
            },
        }
        with patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_LOOKBACK_SEC", 300), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_ALLOWED_STATES", "range_probe,reversal_exhaustion"), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_BLOCKED_DAY_TYPES", "fast_expansion,panic_spread"), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MAX_CONTINUATION_BIAS", 0.09), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MIN_REJECTION_RATIO", 0.16), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MIN_BAR_VOLUME_PROXY", 0.18), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MAX_ABS_DELTA_PROXY", 0.11), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MAX_ABS_DEPTH_IMBALANCE", 0.10), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MAX_SPREAD_EXPANSION", 1.10), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MAX_SPREAD_AVG_PCT", 0.0022), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_ENTRY_RISK_RATIO", 0.10), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_ENTRY_ATR_RATIO", 0.045), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_STOP_KEEP_RISK_RATIO", 0.72), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_TP1_RR", 0.50), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_TP2_RR", 0.82), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_TP3_RR", 1.10), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_CTRADER_RISK_USD", 0.35), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "reversal_exhaustion",
                 "day_type": "trend",
                 "continuation_bias": 0.03,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:rr:canary")
        self.assertEqual(str(getattr(lane_signal, "entry_type", "")), "limit")
        self.assertLess(float(getattr(lane_signal, "entry", 0.0) or 0.0), 5195.5)
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertEqual(str(raw.get("strategy_family") or ""), "xau_scalp_range_repair")
        self.assertEqual(str(raw.get("xau_routing_desk") or ""), "range_repair")
        self.assertEqual(str(raw.get("state_label") or ""), "reversal_exhaustion")
        self.assertEqual(str(raw.get("follow_up_plan") or ""), "probe_repair_limit_after_exhaustion")
        self.assertEqual(str((raw.get("range_repair_snapshot") or {}).get("run_id") or ""), "ctcap_rr")
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0.0) or 0.0), 0.35, places=3)

    def test_build_family_canary_signal_for_range_repair_blocks_fast_expansion_day_type(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.4)
        sig.pattern = "SCALP_FLOW_FORCE"
        candidate = {
            "family": "xau_scalp_range_repair",
            "strategy_id": "xau_scalp_range_repair_v1",
            "priority": 9,
            "execution_ready": True,
            "experimental": True,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_rr_block",
            "last_event_utc": "2026-03-20T02:00:00Z",
            "features": {
                "rejection_ratio": 0.44,
                "bar_volume_proxy": 0.34,
                "spread_expansion": 1.04,
                "spread_avg_pct": 0.0016,
                "delta_proxy": 0.03,
                "depth_imbalance": 0.01,
            },
        }
        with patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_ALLOWED_STATES", "range_probe,reversal_exhaustion"), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_BLOCKED_DAY_TYPES", "fast_expansion,panic_spread"), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "range_probe",
                 "day_type": "fast_expansion",
                 "continuation_bias": 0.02,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNone(lane_signal)
        self.assertEqual(lane_source, "")

    def test_build_family_canary_signal_blocks_td_against_bullish_h1_h4_alignment(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.5)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.direction = "short"
        sig.entry = 5195.5
        sig.stop_loss = 5200.0
        sig.raw_scores["signal_h1_trend"] = "bullish"
        sig.raw_scores["signal_h4_trend"] = "bullish"
        candidate = {
            "family": "xau_scalp_tick_depth_filter",
            "strategy_id": "xau_scalp_tick_depth_filter_v1",
            "priority": 11,
            "execution_ready": True,
            "experimental": True,
        }

        lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNone(lane_signal)
        self.assertEqual(lane_source, "")
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertTrue(bool(raw.get("xau_multi_tf_guard_block")))
        self.assertEqual(str((raw.get("xau_multi_tf_guard") or {}).get("aligned_side") or ""), "long")
        self.assertIn("mtf_block:short_vs_long", str(raw.get("xau_multi_tf_guard_reason") or ""))

    def test_build_family_canary_signal_allows_td_countertrend_when_confirmed_even_if_h1_h4_bullish(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.5)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.direction = "short"
        sig.entry = 5195.5
        sig.stop_loss = 5200.0
        sig.raw_scores["signal_h1_trend"] = "bullish"
        sig.raw_scores["signal_h4_trend"] = "bullish"
        sig.raw_scores["countertrend_confirmed"] = True
        candidate = {
            "family": "xau_scalp_tick_depth_filter",
            "strategy_id": "xau_scalp_tick_depth_filter_v1",
            "priority": 11,
            "execution_ready": True,
            "experimental": True,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_test_countertrend",
            "last_event_utc": "2026-03-11T01:00:00Z",
            "gate": {
                "pass": True,
                "score": 4,
                "score_ratio": 0.8,
                "features": {
                    "spread_avg_pct": 0.0016,
                    "depth_imbalance": 0.04,
                    "depth_refill_shift": 0.08,
                },
            },
        }
        with patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:td:canary")
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertEqual(str((raw.get("xau_multi_tf_guard") or {}).get("aligned_side") or ""), "long")
        self.assertTrue(bool((raw.get("xau_multi_tf_guard") or {}).get("countertrend_confirmed")))

    def test_build_family_canary_signal_for_microtrend_follow_up_uses_state_locked_break_stop(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.1)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.direction = "short"
        sig.session = "london, new_york, overlap"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.entry = 5164.2
        sig.stop_loss = 5167.8
        sig.take_profit_1 = 5161.8
        sig.take_profit_2 = 5160.2
        sig.take_profit_3 = 5158.4
        candidate = {
            "family": "xau_scalp_microtrend_follow_up",
            "strategy_id": "xau_scalp_microtrend_follow_up_v1",
            "priority": 6,
            "execution_ready": True,
            "experimental": True,
        }
        follow_ctx = {
            "direction": "short",
            "session": "london,new_york,overlap",
            "timeframe": "5m+1m",
            "confidence_band": "70-74.9",
            "h1_trend": "bearish",
            "day_type": "trend",
            "state_label": "continuation_drive",
            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
            "state_score": 24.0,
            "chart_state": {
                "state_label": "continuation_drive",
                "day_type": "trend",
                "continuation_bias": -0.22,
            },
            "snapshot": {
                "run_id": "ctcap_mfu",
                "last_event_utc": "2026-03-12T07:00:00Z",
                "features": {
                    "delta_proxy": -0.18,
                    "bar_volume_proxy": 0.71,
                },
            },
        }
        with patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.12), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_DELTA_PROXY", 0.10), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.42), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_CTRADER_RISK_USD", 0.65), \
             patch.object(dexter, "_signal_matches_xau_microtrend_follow_up_context", return_value=(True, follow_ctx)):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:mfu:canary")
        self.assertEqual(str(getattr(lane_signal, "entry_type", "")), "sell_stop")
        self.assertLess(float(getattr(lane_signal, "entry", 0.0) or 0.0), 5164.2)
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertEqual(str(raw.get("strategy_family")), "xau_scalp_microtrend_follow_up")
        self.assertEqual(str((raw.get("chart_state_follow_up_snapshot") or {}).get("run_id")), "ctcap_mfu")
        self.assertEqual(str((raw.get("chart_state_follow_up_snapshot") or {}).get("entry_mode")), "break_stop")
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0.0) or 0.0), 0.65, places=3)

    def test_signal_matches_xau_microtrend_follow_up_context_allows_relaxed_h1_and_day_type(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=72.2)
        sig.direction = "short"
        sig.session = "london, new_york, overlap"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores["scalp_force_trend_h1"] = "bearish"
        with patch.object(dexter, "_load_xau_microtrend_follow_up_contexts", return_value=[{
            "direction": "short",
            "session": "london,new_york,overlap",
            "timeframe": "5m+1m",
            "confidence_band": "<70",
            "h1_trend": "bullish",
            "day_type": "trend",
            "state_label": "continuation_drive",
            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
            "state_score": 41.0,
            "resolved": 4,
        }]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value={
                 "ok": True,
                 "run_id": "ctcap_relaxed",
                 "features": {"delta_proxy": -0.14, "bar_volume_proxy": 0.58},
             }), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_ALLOW_ADJACENT_CONFIDENCE", True), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_ALLOW_H1_RELAXED", True), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_ALLOW_COMPATIBLE_DAY_TYPE", True), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_RELAXED_MIN_STATE_SCORE", 28.0), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "repricing",
                 "continuation_bias": -0.17,
             }):
            matched, ctx = dexter._signal_matches_xau_microtrend_follow_up_context(sig)

        self.assertTrue(matched)
        self.assertTrue(bool(ctx.get("relaxed_confidence_band")))
        self.assertTrue(bool(ctx.get("relaxed_h1_trend")))
        self.assertTrue(bool(ctx.get("relaxed_day_type")))

    def test_signal_matches_xau_microtrend_follow_up_context_allows_first_sample_mode(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=72.2)
        sig.direction = "short"
        sig.session = "london, new_york, overlap"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores["scalp_force_trend_h1"] = "bearish"
        with patch.object(dexter, "_load_xau_microtrend_follow_up_contexts", return_value=[{
            "direction": "short",
            "session": "london,new_york,overlap",
            "timeframe": "5m+1m",
            "confidence_band": "<70",
            "h1_trend": "bullish",
            "day_type": "trend",
            "state_label": "continuation_drive",
            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
            "state_score": 49.0,
            "resolved": 4,
        }]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value={
                 "ok": True,
                 "run_id": "ctcap_first_sample",
                 "features": {"delta_proxy": -0.14, "bar_volume_proxy": 0.58},
             }), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_ALLOW_ADJACENT_CONFIDENCE", False), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_ALLOW_H1_RELAXED", False), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_ALLOW_COMPATIBLE_DAY_TYPE", False), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MODE_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MIN_RESOLVED", 3), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MIN_STATE_SCORE", 32.0), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_MAX_RELAXED_BLOCKERS", 2), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_ALLOWED_STATES", "continuation_drive,repricing_transition"), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": -0.17,
             }):
            matched, ctx = dexter._signal_matches_xau_microtrend_follow_up_context(sig)

        self.assertTrue(matched)
        self.assertTrue(bool(ctx.get("first_sample_mode")))
        self.assertEqual(list(ctx.get("first_sample_relaxed_blockers") or []), ["confidence_band", "h1_trend"])

    def test_build_family_canary_signal_for_microtrend_follow_up_reduces_risk_when_relaxed(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=72.2)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.direction = "short"
        sig.session = "london, new_york, overlap"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.entry = 5164.2
        sig.stop_loss = 5167.8
        sig.take_profit_1 = 5161.8
        sig.take_profit_2 = 5160.2
        sig.take_profit_3 = 5158.4
        candidate = {
            "family": "xau_scalp_microtrend_follow_up",
            "strategy_id": "xau_scalp_microtrend_follow_up_v1",
            "priority": 6,
            "execution_ready": True,
            "experimental": True,
        }
        follow_ctx = {
            "direction": "short",
            "session": "london,new_york,overlap",
            "timeframe": "5m+1m",
            "confidence_band": "<70",
            "h1_trend": "bullish",
            "day_type": "trend",
            "state_label": "continuation_drive",
            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
            "state_score": 41.0,
            "relaxed_h1_trend": True,
            "relaxed_day_type": True,
            "requested_h1_trend": "bearish",
            "requested_day_type": "repricing",
            "chart_state": {
                "state_label": "continuation_drive",
                "day_type": "repricing",
                "continuation_bias": -0.22,
            },
            "snapshot": {
                "run_id": "ctcap_mfu_relaxed",
                "last_event_utc": "2026-03-12T07:00:00Z",
                "features": {
                    "delta_proxy": -0.18,
                    "bar_volume_proxy": 0.71,
                },
            },
        }
        with patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.12), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_DELTA_PROXY", 0.10), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.42), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_CTRADER_RISK_USD", 0.65), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_RELAXED_RISK_MULTIPLIER", 0.85), \
             patch.object(dexter, "_signal_matches_xau_microtrend_follow_up_context", return_value=(True, follow_ctx)):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:mfu:canary")
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertTrue(bool((raw.get("chart_state_follow_up") or {}).get("relaxed_h1_trend")))
        self.assertTrue(bool((raw.get("chart_state_follow_up") or {}).get("relaxed_day_type")))
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0.0) or 0.0), 0.5525, places=4)

    def test_build_family_canary_signal_for_microtrend_follow_up_reduces_risk_in_first_sample_mode(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=72.2)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.direction = "short"
        sig.session = "london, new_york, overlap"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.entry = 5164.2
        sig.stop_loss = 5167.8
        sig.take_profit_1 = 5161.8
        sig.take_profit_2 = 5160.2
        sig.take_profit_3 = 5158.4
        candidate = {
            "family": "xau_scalp_microtrend_follow_up",
            "strategy_id": "xau_scalp_microtrend_follow_up_v1",
            "priority": 6,
            "execution_ready": True,
            "experimental": True,
        }
        follow_ctx = {
            "direction": "short",
            "session": "london,new_york,overlap",
            "timeframe": "5m+1m",
            "confidence_band": "<70",
            "h1_trend": "bullish",
            "day_type": "trend",
            "state_label": "continuation_drive",
            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
            "state_score": 49.0,
            "first_sample_mode": True,
            "first_sample_relaxed_blockers": ["confidence_band", "h1_trend"],
            "requested_confidence_band": "70-74.9",
            "requested_h1_trend": "bearish",
            "chart_state": {
                "state_label": "continuation_drive",
                "day_type": "trend",
                "continuation_bias": -0.22,
            },
            "snapshot": {
                "run_id": "ctcap_mfu_first_sample",
                "last_event_utc": "2026-03-12T07:00:00Z",
                "features": {
                    "delta_proxy": -0.18,
                    "bar_volume_proxy": 0.71,
                },
            },
        }
        with patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.12), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_DELTA_PROXY", 0.10), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.42), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_CTRADER_RISK_USD", 0.65), \
             patch.object(scheduler_module.config, "XAU_MICROTREND_FOLLOW_UP_FIRST_SAMPLE_RISK_MULTIPLIER", 0.70), \
             patch.object(dexter, "_signal_matches_xau_microtrend_follow_up_context", return_value=(True, follow_ctx)):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:mfu:canary")
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertTrue(bool((raw.get("chart_state_follow_up") or {}).get("first_sample_mode")))
        self.assertEqual(list((raw.get("chart_state_follow_up") or {}).get("first_sample_relaxed_blockers") or []), ["confidence_band", "h1_trend"])
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0.0) or 0.0), 0.455, places=4)

    def test_build_family_canary_signal_for_flow_short_sidecar_uses_stop_sample_mode(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=72.4)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.direction = "short"
        sig.session = "london, new_york, overlap"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.entry = 5164.2
        sig.stop_loss = 5167.8
        sig.take_profit_1 = 5161.8
        sig.take_profit_2 = 5160.2
        sig.take_profit_3 = 5158.4
        candidate = {
            "family": "xau_scalp_flow_short_sidecar",
            "strategy_id": "xau_scalp_flow_short_sidecar_v1",
            "priority": 7,
            "execution_ready": True,
            "experimental": True,
        }
        ctx = {
            "direction": "short",
            "session": "london,new_york,overlap",
            "timeframe": "5m+1m",
            "confidence_band": "70-74.9",
            "h1_trend": "bearish",
            "day_type": "trend",
            "state_label": "continuation_drive",
            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
            "state_score": 41.0,
            "resolved": 4,
            "best_family": "xau_scalp_microtrend",
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_fss",
            "last_event_utc": "2026-03-12T07:00:00Z",
            "features": {
                "delta_proxy": -0.07,
                "bar_volume_proxy": 0.40,
            },
        }
        with patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FORCE_STOP_ONLY", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_CONFIDENCE", 72.0), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_STATE_SCORE", 32.0), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_CONTINUATION_BIAS_MULT", 0.75), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_DELTA_PROXY_MULT", 0.75), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_BAR_VOLUME_PROXY_MULT", 0.90), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_RISK_MULTIPLIER", 0.70), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_CONTINUATION_BIAS", 0.10), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_DELTA_PROXY", 0.08), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_BREAK_STOP_MIN_BAR_VOLUME_PROXY", 0.38), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_CTRADER_RISK_USD", 0.45), \
             patch.object(dexter, "_load_xau_flow_short_sidecar_contexts", return_value=[ctx]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": -0.09,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:fss:canary")
        self.assertEqual(str(getattr(lane_signal, "entry_type", "")), "sell_stop")
        self.assertLess(float(getattr(lane_signal, "entry", 0.0) or 0.0), 5164.2)
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertEqual(str((raw.get("chart_state_flow_short_snapshot") or {}).get("entry_mode")), "break_stop_sample")
        self.assertTrue(bool((raw.get("chart_state_flow_short_sidecar") or {}).get("sample_mode")))
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0.0) or 0.0), 0.315, places=3)

    def test_build_family_canary_signal_for_flow_short_sidecar_blocks_limit_fallback_when_stop_only(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=71.8)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.direction = "short"
        sig.session = "london, new_york, overlap"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.entry = 5164.2
        sig.stop_loss = 5167.8
        candidate = {
            "family": "xau_scalp_flow_short_sidecar",
            "strategy_id": "xau_scalp_flow_short_sidecar_v1",
            "priority": 7,
            "execution_ready": True,
            "experimental": True,
        }
        ctx = {
            "direction": "short",
            "session": "london,new_york,overlap",
            "timeframe": "5m+1m",
            "confidence_band": "70-74.9",
            "h1_trend": "bearish",
            "day_type": "trend",
            "state_label": "continuation_drive",
            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
            "state_score": 41.0,
            "resolved": 4,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_fss_block",
            "last_event_utc": "2026-03-12T07:00:00Z",
            "features": {
                "delta_proxy": -0.02,
                "bar_volume_proxy": 0.20,
            },
        }
        with patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FORCE_STOP_ONLY", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_ENABLED", True), \
             patch.object(dexter, "_load_xau_flow_short_sidecar_contexts", return_value=[ctx]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": -0.03,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNone(lane_signal)
        self.assertEqual(lane_source, "")

    def test_build_family_canary_signal_for_flow_short_sidecar_uses_live_delta_when_context_bias_missing(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=72.0)
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.direction = "short"
        sig.session = "asian"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.entry = 5108.24
        sig.stop_loss = 5110.70
        candidate = {
            "family": "xau_scalp_flow_short_sidecar",
            "strategy_id": "xau_scalp_flow_short_sidecar_v1",
            "priority": 7,
            "execution_ready": True,
            "experimental": True,
        }
        ctx = {
            "direction": "short",
            "session": "asian",
            "timeframe": "5m+1m",
            "confidence_band": "70-74.9",
            "h1_trend": "bearish",
            "day_type": "trend",
            "state_label": "continuation_drive",
            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
            "state_score": 28.4,
            "resolved": 3,
            "best_family": "xau_scalp_tick_depth_filter",
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_fss_asian",
            "last_event_utc": "2026-03-13T05:13:40Z",
            "features": {
                "delta_proxy": 0.32,
                "bar_volume_proxy": 1.0,
                "depth_imbalance": 0.65,
            },
        }
        with patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FORCE_STOP_ONLY", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_ENABLED", True), \
             patch.object(dexter, "_load_xau_flow_short_sidecar_contexts", return_value=[ctx]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.0,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:fss:canary")
        self.assertEqual(str(getattr(lane_signal, "entry_type", "")), "sell_stop")

    def test_build_family_canary_signal_for_flow_short_sidecar_accepts_behavioral_liquidity_continuation_high_conf_bridge(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=82.0)
        sig.pattern = "Behavioral Sweep-Retest + Liquidity Continuation"
        sig.direction = "short"
        sig.session = "asian"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.entry = 4690.85
        sig.stop_loss = 4704.16
        sig.take_profit_1 = 4678.51
        candidate = {
            "family": "xau_scalp_flow_short_sidecar",
            "strategy_id": "xau_scalp_flow_short_sidecar_v1",
            "priority": 7,
            "execution_ready": True,
            "experimental": True,
        }
        ctx = {
            "direction": "short",
            "session": "asian",
            "timeframe": "5m+1m",
            "confidence_band": "75-79.9",
            "h1_trend": "bearish",
            "day_type": "trend",
            "state_label": "continuation_drive",
            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
            "state_score": 43.2,
            "resolved": 3,
            "best_family": "xau_scalp_flow_short_sidecar",
            "continuation_bias": 0.0,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_fss_behavioral",
            "last_event_utc": "2026-03-20T02:14:54Z",
            "features": {
                "delta_proxy": 0.1391,
                "bar_volume_proxy": 1.0,
                "depth_imbalance": 0.3451,
            },
        }
        with patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FORCE_STOP_ONLY", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_CONFIDENCE", 72.0), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_STATE_SCORE", 32.0), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MODE_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE", 68.0), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_STATE_SCORE", 34.0), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOW_HIGH_CONFIDENCE_BRIDGE", True), \
             patch.object(dexter, "_load_xau_flow_short_sidecar_contexts", return_value=[ctx]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "range_probe",
                 "day_type": "trend",
                 "continuation_bias": 0.0,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:fss:canary")
        self.assertEqual(str(getattr(lane_signal, "entry_type", "")), "sell_stop")
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertTrue(bool((raw.get("chart_state_flow_short_sidecar") or {}).get("relaxed_confidence_band")))
        self.assertFalse(bool((raw.get("chart_state_flow_short_sidecar") or {}).get("first_sample_mode")))
        self.assertFalse(bool((raw.get("chart_state_flow_short_sidecar") or {}).get("high_confidence_bridge")))
        self.assertEqual(str((raw.get("chart_state_flow_short_snapshot") or {}).get("entry_mode")), "break_stop")

    def test_build_family_canary_signal_for_flow_short_sidecar_rejects_70_band_bridge_for_80plus(self):
        """Fix 1 regression: 80+ must NOT bridge to 70-74.9 (two-band jump)."""
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=82.0)
        sig.pattern = "Behavioral Sweep-Retest + Liquidity Continuation"
        sig.direction = "short"
        sig.session = "asian"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.entry = 4690.85
        sig.stop_loss = 4704.16
        sig.take_profit_1 = 4678.51
        candidate = {
            "family": "xau_scalp_flow_short_sidecar",
            "strategy_id": "xau_scalp_flow_short_sidecar_v1",
            "priority": 7,
            "execution_ready": True,
            "experimental": True,
        }
        ctx = {
            "direction": "short",
            "session": "asian",
            "timeframe": "5m+1m",
            "confidence_band": "70-74.9",
            "h1_trend": "bearish",
            "day_type": "trend",
            "state_label": "continuation_drive",
            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
            "state_score": 43.2,
            "resolved": 3,
            "best_family": "xau_scalp_flow_short_sidecar",
            "continuation_bias": 0.0,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_fss_70band",
            "last_event_utc": "2026-03-20T02:14:54Z",
            "features": {
                "delta_proxy": 0.1391,
                "bar_volume_proxy": 1.0,
                "depth_imbalance": 0.3451,
            },
        }
        with patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FORCE_STOP_ONLY", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_CONFIDENCE", 72.0), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_SAMPLE_MIN_STATE_SCORE", 32.0), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MODE_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_CONFIDENCE", 68.0), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_MIN_STATE_SCORE", 34.0), \
             patch.object(scheduler_module.config, "XAU_FLOW_SHORT_SIDECAR_FIRST_SAMPLE_ALLOW_HIGH_CONFIDENCE_BRIDGE", True), \
             patch.object(dexter, "_load_xau_flow_short_sidecar_contexts", return_value=[ctx]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "range_probe",
                 "day_type": "trend",
                 "continuation_bias": 0.0,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNone(lane_signal, "80+ must NOT bridge to 70-74.9 (two-band jump)")

    def test_build_family_canary_signal_blocks_pb_outside_narrow_context(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=75.6)
        sig.session = "london"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.direction = "long"
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores["scalp_force_trend_h1"] = "bullish"
        candidate = {
            "family": "xau_scalp_pullback_limit",
            "strategy_id": "xau_scalp_pullback_limit_v1",
            "priority": 3,
            "execution_ready": True,
        }
        with patch.object(scheduler_module.config, "XAU_PB_NARROW_CONTEXT_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_ENABLED", False), \
             patch.object(dexter, "_load_xau_pb_narrow_contexts", return_value=[{
                 "direction": "short",
                 "session": "new_york",
                 "timeframe": "5m+1m",
                 "entry_type": "limit",
                 "confidence_band": "70-74.9",
                 "h1_trend": "bullish",
                 "memory_score": 31.0,
                 "resolved": 4,
             }]):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNone(lane_signal)
        self.assertEqual(lane_source, "")

    def test_build_family_canary_signal_allows_pb_in_narrow_context(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.3)
        sig.session = "new_york"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.direction = "short"
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores["scalp_force_trend_h1"] = "bullish"
        sig.entry = 5195.4
        sig.stop_loss = 5199.9
        candidate = {
            "family": "xau_scalp_pullback_limit",
            "strategy_id": "xau_scalp_pullback_limit_v1",
            "priority": 3,
            "execution_ready": True,
        }
        with patch.object(scheduler_module.config, "XAU_PB_NARROW_CONTEXT_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_ENABLED", False), \
             patch.object(scheduler_module.config, "XAU_OPENAPI_ENTRY_ROUTER_ENABLED", False), \
             patch.object(dexter, "_load_xau_pb_narrow_contexts", return_value=[{
                 "direction": "short",
                 "session": "new_york",
                 "timeframe": "5m+1m",
                 "entry_type": "limit",
                 "confidence_band": "70-74.9",
                 "h1_trend": "bullish",
                 "memory_score": 31.0,
                 "resolved": 4,
             }]):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:pb:canary")
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertEqual(str((raw.get("pb_narrow_context") or {}).get("session")), "new_york")
        self.assertEqual(str((raw.get("pb_narrow_context") or {}).get("direction")), "short")

    def test_build_family_canary_signal_allows_pb_with_adjacent_confidence_band(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=75.1)
        sig.session = "new_york"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.direction = "short"
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores["scalp_force_trend_h1"] = "bullish"
        sig.entry = 5195.4
        sig.stop_loss = 5199.9
        candidate = {
            "family": "xau_scalp_pullback_limit",
            "strategy_id": "xau_scalp_pullback_limit_v1",
            "priority": 3,
            "execution_ready": True,
        }
        with patch.object(scheduler_module.config, "XAU_PB_NARROW_CONTEXT_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_ENABLED", False), \
             patch.object(scheduler_module.config, "XAU_OPENAPI_ENTRY_ROUTER_ENABLED", False), \
             patch.object(scheduler_module.config, "XAU_PB_NARROW_CONTEXT_ALLOW_ADJACENT_CONFIDENCE", True), \
             patch.object(scheduler_module.config, "XAU_PB_NARROW_CONTEXT_RELAXED_MIN_MEMORY_SCORE", 28.0), \
             patch.object(dexter, "_load_xau_pb_narrow_contexts", return_value=[{
                 "direction": "short",
                 "session": "new_york",
                 "timeframe": "5m+1m",
                 "entry_type": "limit",
                 "confidence_band": "70-74.9",
                 "h1_trend": "bullish",
                 "memory_score": 31.0,
                 "resolved": 4,
             }]):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:pb:canary")
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertTrue(bool((raw.get("pb_narrow_context") or {}).get("relaxed_confidence_band")))
        self.assertEqual(str((raw.get("pb_narrow_context") or {}).get("requested_confidence_band")), "75-79.9")

    def test_build_family_canary_signal_applies_pb_capture_micro_relax(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.3)
        sig.session = "new_york"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.direction = "long"
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores["scalp_force_trend_h1"] = "bullish"
        sig.entry = 5195.4
        sig.stop_loss = 5191.0
        candidate = {
            "family": "xau_scalp_pullback_limit",
            "strategy_id": "xau_scalp_pullback_limit_v1",
            "priority": 3,
            "execution_ready": True,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_pb_relax",
            "last_event_utc": "2026-03-12T06:00:00Z",
            "gate": {
                "pass": False,
                "canary_sample_pass": False,
                "reasons": [
                    "long_imbalance_not_supportive",
                    "long_refill_not_supportive",
                    "long_delta_not_supportive",
                ],
                "features": {
                    "day_type": "trend",
                },
            },
        }
        with patch.object(scheduler_module.config, "XAU_PB_NARROW_CONTEXT_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_PB_CAPTURE_MICRO_RELAX_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_PB_CAPTURE_MICRO_RELAX_RISK_MULT", 0.88), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_CTRADER_RISK_USD", 1.25), \
             patch.object(dexter, "_load_xau_pb_narrow_contexts", return_value=[{
                 "direction": "long",
                 "session": "new_york",
                 "timeframe": "5m+1m",
                 "entry_type": "limit",
                 "confidence_band": "70-74.9",
                 "h1_trend": "bullish",
                 "memory_score": 31.0,
                 "resolved": 4,
             }]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:pb:canary")
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertEqual(str(((raw.get("pb_capture_micro_relax") or {}).get("snapshot") or {}).get("run_id")), "ctcap_pb_relax")
        self.assertEqual(list((raw.get("pb_capture_micro_relax") or {}).get("reasons") or []), [
            "long_imbalance_not_supportive",
            "long_refill_not_supportive",
            "long_delta_not_supportive",
        ])
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0.0)), 1.1, places=4)

    def test_build_family_canary_signal_blocks_pb_on_falling_knife_capture(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=73.6)
        sig.session = "new_york"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.direction = "long"
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores["scalp_force_trend_h1"] = "bullish"
        sig.entry = 5195.4
        sig.stop_loss = 5191.0
        candidate = {
            "family": "xau_scalp_pullback_limit",
            "strategy_id": "xau_scalp_pullback_limit_v1",
            "priority": 3,
            "execution_ready": True,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_pb_block",
            "last_event_utc": "2026-03-12T06:05:00Z",
            "features": {
                "day_type": "repricing",
                "delta_proxy": -0.18,
                "depth_refill_shift": -0.09,
                "rejection_ratio": 0.08,
                "bar_volume_proxy": 0.74,
            },
            "gate": {
                "reasons": [
                    "long_imbalance_not_supportive",
                    "long_refill_not_supportive",
                    "long_delta_not_supportive",
                ]
            },
        }
        with patch.object(scheduler_module.config, "XAU_PB_NARROW_CONTEXT_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_DAY_TYPES", "repricing,fast_expansion,panic_spread"), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_STATE_LABELS", "failed_fade_risk,panic_dislocation"), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_MIN_ADVERSE_DELTA_PROXY", 0.09), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_MIN_ADVERSE_REFILL_SHIFT", 0.04), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_MIN_BAR_VOLUME_PROXY", 0.42), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_MAX_REJECTION_RATIO", 0.18), \
             patch.object(dexter, "_load_xau_pb_narrow_contexts", return_value=[{
                 "direction": "long",
                 "session": "new_york",
                 "timeframe": "5m+1m",
                 "entry_type": "limit",
                 "confidence_band": "70-74.9",
                 "h1_trend": "bullish",
                 "memory_score": 31.0,
                 "resolved": 4,
             }]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "failed_fade_risk",
                 "day_type": "repricing",
                 "continuation_bias": -0.22,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNone(lane_signal)
        self.assertEqual(lane_source, "")
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertTrue(bool(raw.get("pb_falling_knife_block")))
        self.assertIn("state:failed_fade_risk", str(raw.get("pb_falling_knife_block_reason", "")))
        self.assertEqual(str((raw.get("pb_falling_knife_block_snapshot") or {}).get("run_id") or ""), "ctcap_pb_block")
        self.assertEqual(str((raw.get("pb_falling_knife_block_chart_state") or {}).get("day_type") or ""), "repricing")

    def test_build_family_canary_signal_promotes_pb_to_stop_on_strong_openapi_flow(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.4)
        sig.session = "new_york"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.direction = "long"
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.raw_scores["scalp_force_trend_h1"] = "bullish"
        sig.entry = 5195.4
        sig.stop_loss = 5191.0
        candidate = {
            "family": "xau_scalp_pullback_limit",
            "strategy_id": "xau_scalp_pullback_limit_v1",
            "priority": 3,
            "execution_ready": True,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_pb_promote_stop",
            "last_event_utc": "2026-03-19T01:05:00Z",
            "features": {
                "day_type": "trend",
                "spread_avg_pct": 0.0016,
                "spread_expansion": 1.02,
                "delta_proxy": 0.16,
                "depth_imbalance": 0.11,
                "depth_refill_shift": 0.07,
                "rejection_ratio": 0.10,
                "bar_volume_proxy": 0.66,
                "tick_up_ratio": 0.72,
            },
        }
        with patch.object(scheduler_module.config, "XAU_PB_NARROW_CONTEXT_ENABLED", True), \
             patch.object(dexter, "_load_xau_pb_narrow_contexts", return_value=[{
                 "direction": "long",
                 "session": "new_york",
                 "timeframe": "5m+1m",
                 "entry_type": "limit",
                 "confidence_band": "70-74.9",
                 "h1_trend": "bullish",
                 "memory_score": 34.0,
                 "resolved": 6,
             }]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.26,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:pb:canary")
        self.assertEqual(str(getattr(lane_signal, "entry_type", "")), "buy_stop")
        self.assertGreater(float(getattr(lane_signal, "entry", 0.0)), 5195.4)
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertEqual(str((raw.get("xau_openapi_entry_router") or {}).get("mode") or ""), "promote_to_stop")
        self.assertEqual(str((raw.get("xau_openapi_entry_router") or {}).get("selected_entry_type") or ""), "buy_stop")
        self.assertEqual(str(((raw.get("xau_openapi_entry_router") or {}).get("snapshot") or {}).get("run_id") or ""), "ctcap_pb_promote_stop")

    def test_build_family_canary_signal_downgrades_breakout_to_limit_on_absorption_openapi_flow(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.8)
        sig.session = "new_york"
        sig.timeframe = "5m+1m"
        sig.entry_type = "buy_stop"
        sig.direction = "long"
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.entry = 5201.2
        sig.stop_loss = 5196.6
        candidate = {
            "family": "xau_scalp_breakout_stop",
            "strategy_id": "xau_scalp_breakout_stop_v1",
            "priority": 4,
            "execution_ready": True,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_breakout_to_limit",
            "last_event_utc": "2026-03-19T01:09:00Z",
            "features": {
                "day_type": "repricing",
                "spread_avg_pct": 0.0017,
                "spread_expansion": 1.03,
                "delta_proxy": 0.02,
                "depth_imbalance": 0.01,
                "depth_refill_shift": 0.00,
                "rejection_ratio": 0.43,
                "bar_volume_proxy": 0.36,
                "tick_up_ratio": 0.58,
            },
        }
        with patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "pullback_absorption",
                 "day_type": "repricing",
                 "continuation_bias": 0.03,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNotNone(lane_signal)
        self.assertEqual(lane_source, "scalp_xauusd:bs:canary")
        self.assertEqual(str(getattr(lane_signal, "entry_type", "")), "limit")
        self.assertLess(float(getattr(lane_signal, "entry", 0.0)), 5201.2)
        raw = dict(getattr(lane_signal, "raw_scores", {}) or {})
        self.assertEqual(str((raw.get("xau_openapi_entry_router") or {}).get("mode") or ""), "downgrade_to_limit")
        self.assertEqual(str((raw.get("xau_openapi_entry_router") or {}).get("selected_entry_type") or ""), "limit")
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0.0)), 1.1, places=4)

    def test_build_family_canary_signal_blocks_breakout_on_hostile_openapi_flow(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=75.0)
        sig.session = "new_york"
        sig.timeframe = "5m+1m"
        sig.entry_type = "buy_stop"
        sig.direction = "long"
        sig.pattern = "SCALP_FLOW_FORCE"
        sig.entry = 5201.2
        sig.stop_loss = 5196.6
        candidate = {
            "family": "xau_scalp_breakout_stop",
            "strategy_id": "xau_scalp_breakout_stop_v1",
            "priority": 4,
            "execution_ready": True,
        }
        snapshot = {
            "ok": True,
            "run_id": "ctcap_breakout_block",
            "last_event_utc": "2026-03-19T01:11:00Z",
            "features": {
                "day_type": "panic_spread",
                "spread_avg_pct": 0.0028,
                "spread_expansion": 1.19,
                "delta_proxy": -0.17,
                "depth_imbalance": -0.12,
                "depth_refill_shift": -0.08,
                "rejection_ratio": 0.09,
                "bar_volume_proxy": 0.79,
                "tick_up_ratio": 0.18,
            },
        }
        with patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "failed_fade_risk",
                 "day_type": "panic_spread",
                 "continuation_bias": -0.30,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNone(lane_signal)
        self.assertEqual(lane_source, "")
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertTrue(bool(raw.get("xau_openapi_entry_router_block")))
        self.assertIn("state:failed_fade_risk", str(raw.get("xau_openapi_entry_router_block_reason", "")))

    def test_scalp_xau_live_filter_blocks_outside_band(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=78.0)
        sig.session = "new_york"

        with patch.object(scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_SCALP_XAU_LIVE_FILTER_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_SCALP_XAU_LIVE_CONF_MIN", 72.0), \
             patch.object(scheduler_module.config, "MT5_SCALP_XAU_LIVE_CONF_MAX", 75.0), \
             patch.object(scheduler_module.config, "get_mt5_scalp_xau_live_sessions", return_value={"new_york"}):
            allow, reason = dexter._allow_scalp_xau_live_mt5(sig, source="scalp_xauusd")

        self.assertFalse(allow)
        self.assertIn("conf_above_live_band", reason)

    def test_scalp_xau_live_filter_blocks_when_d1_h4_h1_against_trade(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "short"
        sig.raw_scores.update(
            {
                "signal_d1_trend": "bullish",
                "signal_h4_trend": "bullish",
                "signal_h1_trend": "bullish",
                "xau_multi_tf_snapshot": {
                    "d1_trend": "bullish",
                    "h4_trend": "bullish",
                    "h1_trend": "bullish",
                    "strict_aligned_side": "long",
                    "strict_alignment": "aligned_bullish",
                },
            }
        )

        with patch.object(scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_CONF_FILTER_ENABLED", False), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_ALLOW_COUNTERTREND_CONFIRMED", False):
            allow, reason = dexter._allow_scalp_xau_live_mt5(sig, source="scalp_xauusd")

        self.assertFalse(allow)
        self.assertIn("d1_h4_h1_block:short_vs_long", reason)
        guard = dict(getattr(sig, "raw_scores", {}).get("xau_direct_lane_mtf_guard") or {})
        self.assertEqual(str(guard.get("aligned_side") or ""), "long")

    def test_scalp_xau_live_filter_intrabar_3of3_bearish_sets_fss_routing_hint(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=72.0)
        sig.direction = "short"
        sig.raw_scores.update(
            {
                "signal_d1_trend": "bullish",
                "signal_h4_trend": "bullish",
                "signal_h1_trend": "bullish",
                "continuation_bias": -0.22,
                "delta_proxy": -0.17,
                "bar_volume_proxy": 0.62,
                "xau_multi_tf_snapshot": {
                    "d1_open": 4635.0,
                    "d1_last": 4610.0,
                    "h4_open": 4625.0,
                    "h4_last": 4608.0,
                    "h1_open": 4618.0,
                    "h1_last": 4607.0,
                    "strict_alignment": "mixed",
                    "strict_aligned_side": "",
                },
            }
        )
        with patch.object(scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_USE_INTRABAR_COLOR", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_FSS_SELL_ROUTING_ENABLED", True):
            allow, reason = dexter._allow_scalp_xau_live_mt5(sig, source="scalp_xauusd")
        self.assertTrue(allow)
        self.assertEqual(reason, "live_band_pass")
        guard = dict(getattr(sig, "raw_scores", {}).get("xau_direct_lane_mtf_guard") or {})
        self.assertTrue(bool(guard.get("xau_fss_sell_routing_hint")))
        self.assertEqual(str(guard.get("xau_mtf_mode") or ""), "intrabar")
        self.assertTrue(bool((getattr(sig, "raw_scores", {}) or {}).get("xau_fss_sell_routing_hint")))

    def test_scalp_xau_live_filter_intrabar_partial_sell_requires_flow_confirm(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=69.0)
        sig.direction = "short"
        sig.raw_scores.update(
            {
                "continuation_bias": -0.02,
                "delta_proxy": -0.01,
                "bar_volume_proxy": 0.12,
                "xau_multi_tf_snapshot": {
                    "d1_open": 4635.0,
                    "d1_last": 4610.0,   # bearish
                    "h4_open": 4625.0,
                    "h4_last": 4608.0,   # bearish
                    "h1_open": 4618.0,
                    "h1_last": 4618.0,   # neutral (near open)
                    "strict_alignment": "mixed",
                    "strict_aligned_side": "",
                },
            }
        )
        with patch.object(scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_CONF_FILTER_ENABLED", False), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_USE_INTRABAR_COLOR", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_ALLOW_PARTIAL_ALIGN", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_PARTIAL_MIN_CONF", 66.0), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_PARTIAL_FLOW_CONFIRM_ENABLED", True):
            allow, reason = dexter._allow_scalp_xau_live_mt5(sig, source="scalp_xauusd")
        self.assertFalse(allow)
        self.assertIn("partial_align_no_flow_confirm", reason)
        guard = dict(getattr(sig, "raw_scores", {}).get("xau_direct_lane_mtf_guard") or {})
        self.assertEqual(str(guard.get("reason") or ""), "partial_align_no_flow_confirm")
        self.assertEqual(int(guard.get("mtf_support_count") or 0), 2)

    def test_ctrader_direct_winner_lane_still_uses_d1_h4_h1_guard(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "short"
        sig.raw_scores.update(
            {
                "winner_logic_regime": "strong",
                "signal_d1_trend": "bullish",
                "signal_h4_trend": "bullish",
                "signal_h1_trend": "bullish",
                "xau_multi_tf_snapshot": {
                    "d1_trend": "bullish",
                    "h4_trend": "bullish",
                    "h1_trend": "bullish",
                    "strict_aligned_side": "long",
                    "strict_alignment": "aligned_bullish",
                },
            }
        )

        with patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_SOURCE_PROFILE_GATE_ENABLED", False), \
             patch.object(scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_CONF_FILTER_ENABLED", False), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED", True), \
             patch.object(scheduler_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:winner"}), \
             patch.object(scheduler_module.ctrader_executor, "execute_signal") as exec_call:
            out = dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertIsNone(out)
        self.assertEqual(exec_call.call_count, 0)
        guard = dict(getattr(sig, "raw_scores", {}).get("xau_direct_lane_mtf_guard") or {})
        self.assertFalse(bool(guard.get("allowed", True)))
        self.assertEqual(str(guard.get("reason") or ""), "d1_h4_h1_block:short_vs_long")

    def test_ctrader_xau_scheduled_profile_blocks_market_new_york(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=72.0)
        sig.session = "new_york"
        sig.timeframe = "1h"
        sig.entry_type = "market"

        with patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_SOURCE_PROFILE_GATE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "get_ctrader_allowed_sources", return_value={"xauusd_scheduled:winner"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_sessions", return_value={"london", "london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_timeframes", return_value={"1h"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_entry_types", return_value={"limit"}), \
             patch.object(scheduler_module.config, "get_mt5_xau_scheduled_live_sessions", return_value={"new_york"}), \
             patch.object(scheduler_module.config, "get_mt5_xau_scheduled_live_timeframes", return_value={"1h"}), \
             patch.object(scheduler_module.config, "MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.ctrader_executor, "journal_pre_dispatch_skip", return_value=17) as audit_call, \
             patch.object(scheduler_module.ctrader_executor, "execute_signal") as exec_call:
            out = dexter._maybe_execute_ctrader_signal(sig, source="xauusd_scheduled")

        self.assertIsNone(out)
        self.assertEqual(exec_call.call_count, 0)
        self.assertEqual(audit_call.call_count, 1)
        self.assertEqual(audit_call.call_args.kwargs.get("gate"), "source_profile")
        self.assertIn("xau_scheduled_session_not_allowed", str(audit_call.call_args.kwargs.get("reason", "")))
        self.assertTrue(bool(getattr(sig, "raw_scores", {}).get("ctrader_source_profile_blocked")))
        self.assertIn("xau_scheduled_session_not_allowed", str(getattr(sig, "raw_scores", {}).get("ctrader_source_profile_reason", "")))

    def test_ctrader_btc_winner_profile_blocks_high_conf_or_wrong_session(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=77.0)
        sig.session = "new_york"
        sig.timeframe = "5m+1m"
        sig.entry_type = "limit"
        sig.raw_scores["crypto_winner_logic_regime"] = "strong"

        with patch.object(scheduler_module.config, "CTRADER_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_SOURCE_PROFILE_GATE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_BTC_WINNER_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "CTRADER_BTC_WINNER_MAX_CONFIDENCE", 75.0), \
             patch.object(scheduler_module.config, "get_ctrader_allowed_sources", return_value={"scalp_btcusd:winner"}), \
             patch.object(scheduler_module.config, "get_ctrader_btc_winner_allowed_sessions_weekend", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.ctrader_executor, "execute_signal") as exec_call:
            out = dexter._maybe_execute_ctrader_signal(sig, source="scalp_btcusd")

        self.assertIsNone(out)
        self.assertEqual(exec_call.call_count, 0)
        self.assertTrue(bool(getattr(sig, "raw_scores", {}).get("ctrader_source_profile_blocked")))
        self.assertIn("btc_winner_conf_above", str(getattr(sig, "raw_scores", {}).get("ctrader_source_profile_reason", "")))

    def test_btc_weekday_lob_reprices_market_choch_to_limit(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=70.1)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "CHOCH_ENTRY"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "market"
        sig.raw_scores["crypto_winner_logic_regime"] = "strong"
        sig.raw_scores["neural_probability"] = 0.6382

        weekday_dt = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)  # Monday
        with patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_MAX_CONFIDENCE", 74.9), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_REQUIRE_STRONG_WINNER", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_ALLOW_MARKET", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_CHOCH_MARKET_TO_LIMIT_ENABLED", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_CHOCH_MARKET_MAX_CONFIDENCE", 72.2), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_CHOCH_MARKET_MIN_NEURAL_PROB", 0.63), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_CHOCH_LIMIT_PULLBACK_RISK_RATIO", 0.12), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_RELAXED_RISK_MULTIPLIER", 0.7), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_CTRADER_RISK_USD", 0.9), \
             patch.object(scheduler_module.config, "BTC_MRD_ENABLED", False), \
             patch.object(scheduler_module.config, "get_btc_weekday_lob_allowed_sessions", return_value={"new_york", "london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "get_btc_weekday_lob_allowed_patterns", return_value={"ob_bounce", "choch_entry"}), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = weekday_dt
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_weekday_experimental_signal(
                sig,
                base_source="scalp_btcusd",
                candidate={"family": "btc_weekday_lob_momentum", "strategy_id": "btcusd_weekday_lob_momentum_v1", "priority": 3},
            )

        self.assertEqual(lane_source, "scalp_btcusd:bwl:canary")
        self.assertEqual(getattr(shaped, "entry_type"), "limit")
        self.assertLess(float(getattr(shaped, "entry", 0.0) or 0.0), 70000.0)
        self.assertTrue(bool(getattr(shaped, "raw_scores", {}).get("strategy_family_relaxed_gate")))
        self.assertIn("choch_market_to_limit", str(getattr(shaped, "raw_scores", {}).get("strategy_family_relaxed_reason", "")))
        self.assertAlmostEqual(float(getattr(shaped, "raw_scores", {}).get("ctrader_risk_usd_override", 0.0) or 0.0), 0.63, places=2)

    def test_btc_weekday_lob_allows_neutral_ob_bounce_with_reduced_risk(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=73.0)
        sig.timeframe = "5m+1m"
        sig.session = "new_york"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "market"
        sig.raw_scores["crypto_winner_logic_regime"] = "neutral"
        sig.raw_scores["neural_probability"] = 0.6605

        weekday_dt = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)  # Monday
        with patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_MAX_CONFIDENCE", 74.9), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_REQUIRE_STRONG_WINNER", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_ALLOW_MARKET", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_ALLOW_NEUTRAL_OB_BOUNCE", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_CONFIDENCE", 72.8), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_NEURAL_PROB", 0.65), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_RELAXED_RISK_MULTIPLIER", 0.7), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_CTRADER_RISK_USD", 0.9), \
             patch.object(scheduler_module.config, "BTC_MRD_ENABLED", False), \
             patch.object(scheduler_module.config, "get_btc_weekday_lob_allowed_sessions", return_value={"new_york", "london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "get_btc_weekday_lob_allowed_patterns", return_value={"ob_bounce", "choch_entry"}), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = weekday_dt
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_weekday_experimental_signal(
                sig,
                base_source="scalp_btcusd",
                candidate={"family": "btc_weekday_lob_momentum", "strategy_id": "btcusd_weekday_lob_momentum_v1", "priority": 3},
            )

        self.assertEqual(lane_source, "scalp_btcusd:bwl:canary")
        self.assertEqual(getattr(shaped, "entry_type"), "market")
        self.assertTrue(bool(getattr(shaped, "raw_scores", {}).get("strategy_family_relaxed_gate")))
        self.assertIn("neutral_ob_bounce", str(getattr(shaped, "raw_scores", {}).get("strategy_family_relaxed_reason", "")))
        self.assertAlmostEqual(float(getattr(shaped, "raw_scores", {}).get("ctrader_risk_usd_override", 0.0) or 0.0), 0.63, places=2)

    def test_crypto_weekend_toggle_off_blocks_btc(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=72.0)
        sig.timeframe = "5m+1m"
        sig.session = "new_york"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "market"
        sig.raw_scores["crypto_winner_logic_regime"] = "strong"

        weekend_dt = datetime(2026, 3, 21, 12, 0, 0, tzinfo=timezone.utc)  # Saturday
        with patch.object(scheduler_module.config, "CRYPTO_WEEKEND_TRADING_ENABLED", False), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = weekend_dt
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_weekday_experimental_signal(
                sig, base_source="scalp_btcusd",
                candidate={"family": "btc_weekday_lob_momentum", "strategy_id": "test", "priority": 3},
            )

        self.assertIsNone(shaped)
        self.assertEqual(lane_source, "")

    def test_crypto_weekend_btc_strong_winner_fires_with_reduced_risk(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=72.0)
        sig.timeframe = "5m+1m"
        sig.session = "new_york"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "market"
        sig.raw_scores["crypto_winner_logic_regime"] = "strong"

        weekend_dt = datetime(2026, 3, 21, 12, 0, 0, tzinfo=timezone.utc)  # Saturday
        with patch.object(scheduler_module.config, "CRYPTO_WEEKEND_TRADING_ENABLED", True), \
             patch.object(scheduler_module.config, "CRYPTO_WEEKEND_RISK_MULTIPLIER", 0.65), \
             patch.object(scheduler_module.config, "CRYPTO_WEEKEND_BTC_ALLOWED_SESSIONS", "*"), \
             patch.object(scheduler_module.config, "get_crypto_weekend_btc_allowed_sessions", return_value={"*"}), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_MAX_CONFIDENCE", 74.9), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_REQUIRE_STRONG_WINNER", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_ALLOW_MARKET", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_CTRADER_RISK_USD", 0.9), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_RELAXED_RISK_MULTIPLIER", 0.7), \
             patch.object(scheduler_module.config, "BTC_MRD_ENABLED", False), \
             patch.object(scheduler_module.config, "get_btc_weekday_lob_allowed_patterns", return_value={"ob_bounce", "choch_entry"}), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = weekend_dt
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_weekday_experimental_signal(
                sig, base_source="scalp_btcusd",
                candidate={"family": "btc_weekday_lob_momentum", "strategy_id": "test", "priority": 3},
            )

        self.assertIsNotNone(shaped)
        self.assertEqual(lane_source, "scalp_btcusd:bwl:canary")
        raw = getattr(shaped, "raw_scores", {})
        self.assertTrue(raw.get("crypto_weekend_mode"))
        risk = float(raw.get("ctrader_risk_usd_override", 0.0) or 0.0)
        self.assertAlmostEqual(risk, 0.9 * 0.65, places=2)

    def test_crypto_weekend_btc_neutral_winner_fires_with_relaxed_reason(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=72.0)
        sig.timeframe = "5m+1m"
        sig.session = "new_york"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "market"
        sig.raw_scores["crypto_winner_logic_regime"] = "neutral"
        sig.raw_scores["neural_probability"] = 0.70

        weekend_dt = datetime(2026, 3, 21, 12, 0, 0, tzinfo=timezone.utc)  # Saturday
        with patch.object(scheduler_module.config, "CRYPTO_WEEKEND_TRADING_ENABLED", True), \
             patch.object(scheduler_module.config, "CRYPTO_WEEKEND_RISK_MULTIPLIER", 0.65), \
             patch.object(scheduler_module.config, "CRYPTO_WEEKEND_ALLOW_NEUTRAL_WINNER", True), \
             patch.object(scheduler_module.config, "CRYPTO_WEEKEND_BTC_ALLOWED_SESSIONS", "*"), \
             patch.object(scheduler_module.config, "get_crypto_weekend_btc_allowed_sessions", return_value={"*"}), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_MAX_CONFIDENCE", 74.9), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_REQUIRE_STRONG_WINNER", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_ALLOW_MARKET", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_ALLOW_NEUTRAL_OB_BOUNCE", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_CONFIDENCE", 72.8), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_NEUTRAL_OB_MIN_NEURAL_PROB", 0.65), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_CTRADER_RISK_USD", 0.9), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_RELAXED_RISK_MULTIPLIER", 0.7), \
             patch.object(scheduler_module.config, "BTC_MRD_ENABLED", False), \
             patch.object(scheduler_module.config, "get_btc_weekday_lob_allowed_patterns", return_value={"ob_bounce", "choch_entry"}), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = weekend_dt
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_weekday_experimental_signal(
                sig, base_source="scalp_btcusd",
                candidate={"family": "btc_weekday_lob_momentum", "strategy_id": "test", "priority": 3},
            )

        self.assertIsNotNone(shaped)
        raw = getattr(shaped, "raw_scores", {})
        self.assertIn("weekend_neutral_winner", str(raw.get("strategy_family_relaxed_reason", "")))
        self.assertTrue(raw.get("crypto_weekend_mode"))

    def test_crypto_weekend_eth_fires_with_weekend_sessions(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("ETHUSD", confidence=76.0)
        sig.timeframe = "5m+1m"
        sig.session = "asian"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 3500.0
        sig.stop_loss = 3490.0
        sig.entry_type = "market"
        sig.raw_scores["crypto_winner_logic_regime"] = "strong"

        weekend_dt = datetime(2026, 3, 21, 12, 0, 0, tzinfo=timezone.utc)  # Saturday
        with patch.object(scheduler_module.config, "CRYPTO_WEEKEND_TRADING_ENABLED", True), \
             patch.object(scheduler_module.config, "CRYPTO_WEEKEND_RISK_MULTIPLIER", 0.65), \
             patch.object(scheduler_module.config, "CRYPTO_WEEKEND_ETH_ALLOWED_SESSIONS", "*"), \
             patch.object(scheduler_module.config, "get_crypto_weekend_eth_allowed_sessions", return_value={"*"}), \
             patch.object(scheduler_module.config, "ETH_WEEKDAY_PROBE_MIN_CONFIDENCE", 74.0), \
             patch.object(scheduler_module.config, "ETH_WEEKDAY_PROBE_MAX_CONFIDENCE", 79.9), \
             patch.object(scheduler_module.config, "ETH_WEEKDAY_PROBE_REQUIRE_STRONG_WINNER", True), \
             patch.object(scheduler_module.config, "ETH_WEEKDAY_PROBE_ALLOW_MARKET", True), \
             patch.object(scheduler_module.config, "ETH_WEEKDAY_PROBE_CTRADER_RISK_USD", 0.35), \
             patch.object(scheduler_module.config, "get_eth_weekday_probe_allowed_patterns", return_value={"ob_bounce"}), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = weekend_dt
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_weekday_experimental_signal(
                sig, base_source="scalp_ethusd",
                candidate={"family": "eth_weekday_overlap_probe", "strategy_id": "test", "priority": 3},
            )

        self.assertIsNotNone(shaped)
        self.assertEqual(lane_source, "scalp_ethusd:ewp:canary")
        raw = getattr(shaped, "raw_scores", {})
        self.assertTrue(raw.get("crypto_weekend_mode"))
        risk = float(raw.get("ctrader_risk_usd_override", 0.0) or 0.0)
        self.assertAlmostEqual(risk, 0.35 * 0.65, places=2)

    def test_crypto_weekday_toggle_on_unchanged_behavior(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=72.0)
        sig.timeframe = "5m+1m"
        sig.session = "new_york"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "market"
        sig.raw_scores["crypto_winner_logic_regime"] = "strong"

        weekday_dt = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)  # Monday
        with patch.object(scheduler_module.config, "CRYPTO_WEEKEND_TRADING_ENABLED", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_MAX_CONFIDENCE", 74.9), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_REQUIRE_STRONG_WINNER", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_ALLOW_MARKET", True), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_CTRADER_RISK_USD", 0.9), \
             patch.object(scheduler_module.config, "BTC_WEEKDAY_LOB_RELAXED_RISK_MULTIPLIER", 0.7), \
             patch.object(scheduler_module.config, "BTC_MRD_ENABLED", False), \
             patch.object(scheduler_module.config, "get_btc_weekday_lob_allowed_sessions", return_value={"new_york", "london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "get_btc_weekday_lob_allowed_patterns", return_value={"ob_bounce", "choch_entry"}), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = weekday_dt
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_weekday_experimental_signal(
                sig, base_source="scalp_btcusd",
                candidate={"family": "btc_weekday_lob_momentum", "strategy_id": "test", "priority": 3},
            )

        self.assertIsNotNone(shaped)
        raw = getattr(shaped, "raw_scores", {})
        self.assertFalse(raw.get("crypto_weekend_mode", False))
        risk = float(raw.get("ctrader_risk_usd_override", 0.0) or 0.0)
        self.assertAlmostEqual(risk, 0.9, places=2)

    def test_mt5_lane_scorecard_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        tmpdir = tempfile.mkdtemp()
        try:
            db_path = f"{tmpdir}\\mt5_autopilot.db"
            conn = sqlite3.connect(db_path)
            now = datetime.now(timezone.utc)
            conn.execute(
                """
                CREATE TABLE mt5_execution_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    source TEXT,
                    signal_symbol TEXT,
                    mt5_status TEXT NOT NULL,
                    mt5_message TEXT,
                    resolved INTEGER NOT NULL DEFAULT 0,
                    outcome INTEGER,
                    pnl REAL,
                    canary_mode INTEGER
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO mt5_execution_journal(created_at, source, signal_symbol, mt5_status, mt5_message, resolved, outcome, pnl, canary_mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ((now - timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_xauusd", "XAUUSD", "filled", "ok", 1, 1, 12.5, 0),
                    ((now - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"), "xauusd_scheduled:winner", "XAUUSD", "filled", "ok", 1, 1, 25.0, 1),
                    ((now - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_xauusd", "XAUUSD", "skipped", "neural filter", 0, None, 0.0, 0),
                ],
            )
            conn.commit()
            conn.close()

            with patch.object(scheduler_module.mt5_autopilot_core, "db_path", db_path), \
                 patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
                rpt = dexter._run_mt5_lane_scorecard(force=False)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(((rpt.get("lanes") or {}).get("main") or {}).get("wins"), 1)
        self.assertEqual(((rpt.get("lanes") or {}).get("winner") or {}).get("wins"), 1)
        self.assertEqual(save_call.call_count, 1)

    def test_crypto_weekend_scorecard_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {
            "ok": True,
            "model_rows": 5,
            "symbols": [
                {"symbol": "BTCUSD", "weekend": {"resolved": 4, "pnl_usd": 28.0}},
                {"symbol": "ETHUSD", "weekend": {"resolved": 0, "pnl_usd": 0.0}},
            ],
        }
        with patch.object(scheduler_module.config, "CRYPTO_WEEKEND_SCORECARD_LOOKBACK_DAYS", 14), \
             patch.object(scheduler_module.scalping_forward_analyzer, "build_crypto_weekend_scorecard", return_value=fake_report) as build_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
            rpt = dexter._run_crypto_weekend_scorecard(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(save_call.call_count, 1)

    def test_winner_mission_report_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {
            "ok": True,
            "symbols": [
                {"symbol": "BTCUSD", "recommended_live_mode": "winner_only"},
                {"symbol": "XAUUSD", "recommended_live_mode": "scheduled_winner_plus_safe_scalp"},
            ],
        }
        with patch.object(scheduler_module.config, "WINNER_MISSION_REPORT_LOOKBACK_DAYS", 14), \
             patch.object(scheduler_module.scalping_forward_analyzer, "build_winner_mission_report", return_value=fake_report) as build_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
            rpt = dexter._run_winner_mission_report(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(save_call.call_count, 1)

    def test_missed_opportunity_audit_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {
            "ok": True,
            "summary": {"missed_rows": 12, "missed_positive_groups": 1},
            "recommendations": [{"symbol": "XAUUSD", "action": "canary_reinstate"}],
        }
        with patch.object(scheduler_module.config, "MISSED_OPPORTUNITY_AUDIT_LOOKBACK_DAYS", 14), \
             patch.object(scheduler_module.live_profile_autopilot, "build_missed_opportunity_audit_report", return_value=fake_report) as build_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
            rpt = dexter._run_missed_opportunity_audit(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(save_call.call_count, 1)

    def test_auto_apply_live_profile_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {
            "ok": True,
            "status": "applied",
            "candidate_changes": {"NEURAL_GATE_CANARY_MIN_CONFIDENCE": "70"},
        }
        with patch.object(scheduler_module.live_profile_autopilot, "auto_apply_live_profile", return_value=fake_report) as build_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
            rpt = dexter._run_auto_apply_live_profile(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(save_call.call_count, 1)

    def test_xau_direct_lane_report_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {
            "ok": True,
            "hours": 24,
            "summary": {"sent": 4, "resolved": 2, "win_rate": 0.5, "pnl_usd": 1.2},
            "sources": {"main": {"sent": 2}, "winner": {"sent": 2}},
        }
        with patch.object(scheduler_module.config, "XAU_DIRECT_LANE_REPORT_LOOKBACK_HOURS", 24), \
             patch.object(scheduler_module.live_profile_autopilot, "build_xau_direct_lane_report", return_value=fake_report) as build_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
            rpt = dexter._run_xau_direct_lane_report(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(save_call.call_count, 1)

    def test_ctrader_data_integrity_report_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {
            "ok": True,
            "summary": {
                "journal_rows": 12,
                "deal_rows": 20,
                "deal_rows_repaired": 7,
                "deal_rows_remaining_missing": 1,
            },
        }
        with patch.object(scheduler_module.config, "CTRADER_DATA_INTEGRITY_REPORT_LOOKBACK_DAYS", 180), \
             patch.object(scheduler_module.config, "CTRADER_DATA_INTEGRITY_REPORT_REPAIR_ON_RUN", True), \
             patch.object(scheduler_module.live_profile_autopilot, "build_ctrader_data_integrity_report", return_value=fake_report) as build_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
            rpt = dexter._run_ctrader_data_integrity_report(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(save_call.call_count, 1)

    def test_xau_direct_lane_auto_tune_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {
            "ok": True,
            "status": "tightened",
            "changes": {"MT5_SCALP_XAU_LIVE_CONF_MIN": "72.50"},
        }
        with patch.object(scheduler_module.live_profile_autopilot, "auto_tune_xau_direct_lane", return_value=fake_report) as build_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
            rpt = dexter._run_xau_direct_lane_auto_tune(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(save_call.call_count, 1)

    def test_canary_post_trade_audit_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {
            "ok": True,
            "status": "review_ready",
            "summary": {"total_canary_closed": 4, "groups": 2},
            "rows": [{"symbol": "XAUUSD"}],
        }
        with patch.object(scheduler_module.config, "CANARY_POST_TRADE_AUDIT_LOOKBACK_DAYS", 14), \
             patch.object(scheduler_module.config, "AUTO_APPLY_LIVE_PROFILE_ENABLED", True), \
             patch.object(scheduler_module.config, "STRATEGY_LAB_REPORT_ENABLED", True), \
             patch.object(scheduler_module.config, "MISSION_PROGRESS_REPORT_ENABLED", True), \
             patch.object(scheduler_module.live_profile_autopilot, "build_canary_post_trade_audit_report", return_value=fake_report) as build_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call, \
             patch.object(dexter, "_run_auto_apply_live_profile", return_value={"ok": True}) as auto_call, \
             patch.object(dexter, "_run_strategy_lab_report", return_value={"ok": True}) as lab_call, \
             patch.object(dexter, "_run_mission_progress_report", return_value={"ok": True}) as progress_call:
            rpt = dexter._run_canary_post_trade_audit(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(save_call.call_count, 1)
        self.assertEqual(auto_call.call_count, 1)
        self.assertEqual(lab_call.call_count, 1)
        self.assertEqual(progress_call.call_count, 1)

    def test_strategy_lab_report_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {"ok": True, "candidates": [{"symbol": "XAUUSD", "strategy_id": "xau_scheduled_trend_v_next"}]}
        fake_lab_team = {"ok": True, "summary": {"promotion_count": 1, "live_shadow_count": 1}, "symbols": {"XAUUSD": {}}}
        fake_trading_team = {"ok": True, "symbols": {"XAUUSD": {"execution_desk": {"primary_family": "xau_scalp_tick_depth_filter"}}}}
        with patch.object(scheduler_module.live_profile_autopilot, "build_strategy_lab_report", return_value=fake_report) as build_call, \
             patch.object(scheduler_module.strategy_lab_team_agent, "build_report", return_value=fake_lab_team) as lab_team_call, \
             patch.object(scheduler_module.trading_team_agent, "build_report", return_value=fake_trading_team) as trading_team_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
            rpt = dexter._run_strategy_lab_report(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(lab_team_call.call_count, 1)
        self.assertEqual(trading_team_call.call_count, 1)
        self.assertEqual(save_call.call_count, 3)

    def test_strategy_family_candidates_skip_shadow_and_blocked_lab_models(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_tick_depth_filter",
                    "strategy_id": "xau_td_live_shadow_v1",
                    "priority": 4,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_microtrend_follow_up",
                    "strategy_id": "xau_mfu_shadow_v1",
                    "priority": 3,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_flow_short_sidecar",
                    "strategy_id": "xau_fss_promote_v1",
                    "priority": 2,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_failed_fade_follow_stop",
                    "strategy_id": "xau_ff_blocked_v1",
                    "priority": 1,
                    "execution_ready": True,
                    "experimental": True,
                },
            ]
        }
        manager_state = {
            "xau_family_routing": {
                "status": "active",
                "mode": "team_primary_advisory",
                "primary_family": "xau_scalp_flow_short_sidecar",
                "active_families": [
                    "xau_scalp_tick_depth_filter",
                    "xau_scalp_microtrend_follow_up",
                    "xau_scalp_flow_short_sidecar",
                    "xau_scalp_failed_fade_follow_stop",
                ],
            }
        }
        lab_state = {
            "status": "active",
            "symbols": {
                "XAUUSD": {
                    "strategy_states": {
                        "xau_td_live_shadow_v1": "live_shadow",
                        "xau_mfu_shadow_v1": "shadow",
                        "xau_fss_promote_v1": "promotable",
                        "xau_ff_blocked_v1": "blocked",
                    },
                    "family_states": {
                        "xau_scalp_tick_depth_filter": "live_shadow",
                        "xau_scalp_microtrend_follow_up": "shadow",
                        "xau_scalp_flow_short_sidecar": "promotable",
                        "xau_scalp_failed_fade_follow_stop": "blocked",
                    },
                    "execution_family_priority_map": {
                        "xau_scalp_flow_short_sidecar": 44.0,
                        "xau_scalp_tick_depth_filter": 33.0,
                    },
                }
            },
        }
        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 1), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 4), \
             patch.object(scheduler_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up", "xau_scalp_flow_short_sidecar", "xau_scalp_failed_fade_follow_stop"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(dexter, "_load_trading_manager_runtime_state", return_value=manager_state), \
             patch.object(dexter, "_load_trading_team_runtime_state", return_value=manager_state), \
             patch.object(dexter, "_load_strategy_lab_team_runtime_state", return_value=lab_state), \
             patch.object(scheduler_module.Path, "exists", return_value=True), \
             patch.object(scheduler_module.Path, "read_text", return_value=json.dumps(payload)):
            rows = dexter._load_strategy_family_candidates(symbol="XAUUSD", base_source="scalp_xauusd")

        families = [str(row.get("family") or "") for row in rows]
        self.assertEqual(families, ["xau_scalp_flow_short_sidecar", "xau_scalp_tick_depth_filter"])

    def test_strategy_family_candidates_prioritize_production_budget_before_sampling(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_tick_depth_filter",
                    "strategy_id": "xau_scalp_tick_depth_filter_v1",
                    "priority": 4,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_flow_short_sidecar",
                    "strategy_id": "xau_scalp_flow_short_sidecar_v1",
                    "priority": 3,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_microtrend_follow_up",
                    "strategy_id": "xau_scalp_microtrend_follow_up_v1",
                    "priority": 2,
                    "execution_ready": True,
                    "experimental": True,
                },
            ]
        }
        team_state = {
            "status": "active",
            "xau_family_routing": {
                "status": "active",
                "mode": "team_primary_advisory",
                "primary_family": "xau_scalp_tick_depth_filter",
                "active_families": [
                    "xau_scalp_tick_depth_filter",
                    "xau_scalp_flow_short_sidecar",
                    "xau_scalp_microtrend_follow_up",
                ],
            },
            "xau_family_budget": {
                "status": "active",
                "mode": "institutional_live_edge_allocator",
                "production_families": ["xau_scalp_tick_depth_filter"],
                "sampling_families": ["xau_scalp_flow_short_sidecar"],
                "sampling_parallel_limit": 1,
                "family_live_edge_map": {
                    "xau_scalp_tick_depth_filter": {"live_edge_score": 8.4, "comparison_bonus": 3.5},
                    "xau_scalp_flow_short_sidecar": {"live_edge_score": -2.1, "comparison_bonus": 0.0},
                    "xau_scalp_microtrend_follow_up": {"live_edge_score": 6.0, "comparison_bonus": 2.5},
                },
            },
        }
        lab_state = {
            "status": "active",
            "symbols": {
                "XAUUSD": {
                    "family_states": {
                        "xau_scalp_tick_depth_filter": "live_shadow",
                        "xau_scalp_flow_short_sidecar": "live_shadow",
                        "xau_scalp_microtrend_follow_up": "live_shadow",
                    },
                    "execution_family_priority_map": {
                        "xau_scalp_tick_depth_filter": 44.0,
                        "xau_scalp_flow_short_sidecar": 20.0,
                        "xau_scalp_microtrend_follow_up": 33.0,
                    },
                }
            },
        }
        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 1), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 4), \
             patch.object(scheduler_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up", "xau_scalp_flow_short_sidecar"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(dexter, "_load_trading_manager_runtime_state", return_value=team_state), \
             patch.object(dexter, "_load_trading_team_runtime_state", return_value=team_state), \
             patch.object(dexter, "_load_strategy_lab_team_runtime_state", return_value=lab_state), \
             patch.object(scheduler_module.Path, "exists", return_value=True), \
             patch.object(scheduler_module.Path, "read_text", return_value=json.dumps(payload)):
            rows = dexter._load_strategy_family_candidates(symbol="XAUUSD", base_source="scalp_xauusd")

        families = [str(row.get("family") or "") for row in rows]
        self.assertEqual(families, ["xau_scalp_tick_depth_filter", "xau_scalp_flow_short_sidecar"])

    def test_strategy_family_candidates_allow_recovery_family_when_lab_has_no_live_shadow(self):
        dexter = scheduler_module.DexterScheduler()
        payload = {
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_tick_depth_filter",
                    "strategy_id": "xau_td_recovery_v1",
                    "priority": 4,
                    "execution_ready": True,
                    "experimental": True,
                },
                {
                    "symbol": "XAUUSD",
                    "family": "xau_scalp_microtrend_follow_up",
                    "strategy_id": "xau_mfu_shadow_v1",
                    "priority": 3,
                    "execution_ready": True,
                    "experimental": True,
                },
            ]
        }
        manager_state = {
            "xau_family_routing": {
                "status": "active",
                "mode": "team_primary_advisory",
                "primary_family": "xau_scalp_tick_depth_filter",
                "active_families": [
                    "xau_scalp_tick_depth_filter",
                    "xau_scalp_microtrend_follow_up",
                ],
            }
        }
        lab_state = {
            "status": "active",
            "symbols": {
                "XAUUSD": {
                    "strategy_states": {
                        "xau_td_recovery_v1": "shadow",
                        "xau_mfu_shadow_v1": "shadow",
                    },
                    "family_states": {
                        "xau_scalp_tick_depth_filter": "shadow",
                        "xau_scalp_microtrend_follow_up": "shadow",
                    },
                    "recovery_strategy_ids": ["xau_td_recovery_v1"],
                    "recovery_families": ["xau_scalp_tick_depth_filter"],
                    "execution_family_priority_map": {
                        "xau_scalp_tick_depth_filter": 15.0,
                    },
                }
            },
        }
        with patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_FAMILY_MAX_VARIANTS", 1), \
             patch.object(scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_MAX_VARIANTS", 3), \
             patch.object(scheduler_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(scheduler_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(dexter, "_load_trading_manager_runtime_state", return_value=manager_state), \
             patch.object(dexter, "_load_trading_team_runtime_state", return_value=manager_state), \
             patch.object(dexter, "_load_strategy_lab_team_runtime_state", return_value=lab_state), \
             patch.object(scheduler_module.Path, "exists", return_value=True), \
             patch.object(scheduler_module.Path, "read_text", return_value=json.dumps(payload)):
            rows = dexter._load_strategy_family_candidates(symbol="XAUUSD", base_source="scalp_xauusd")

        families = [str(row.get("family") or "") for row in rows]
        modes = [str(row.get("strategy_lab_mode") or "") for row in rows]
        self.assertEqual(families, ["xau_scalp_tick_depth_filter"])
        self.assertEqual(modes, ["recovery"])

    def test_mission_progress_report_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {"ok": True, "summary": {"active_bundle_status": "active", "strategy_candidates": 3}}
        with patch.object(scheduler_module.live_profile_autopilot, "build_mission_progress_report", return_value=fake_report) as build_call, \
             patch.object(dexter, "_run_ct_only_watch_report", return_value={"ok": True}) as watch_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
            rpt = dexter._run_mission_progress_report(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(save_call.call_count, 1)
        self.assertEqual(watch_call.call_count, 1)

    def test_ct_only_watch_report_builds_and_saves_report(self):
        dexter = scheduler_module.DexterScheduler()
        fake_report = {"ok": True, "summary": {"td_first_execution_detected": True}}
        with patch.object(scheduler_module.live_profile_autopilot, "build_ct_only_watch_report", return_value=fake_report) as build_call, \
             patch.object(scheduler_module.report_store, "save_report", return_value=True) as save_call:
            rpt = dexter._run_ct_only_watch_report(force=False)

        self.assertTrue(rpt.get("ok"))
        self.assertEqual(build_call.call_count, 1)
        self.assertEqual(save_call.call_count, 1)

    def test_format_mission_progress_report_includes_asian_long_memory(self):
        text = scheduler_module.DexterScheduler._format_mission_progress_report_text(
            {
                "ok": True,
                "summary": {"active_bundle_status": "active", "canary_closed_total": 10, "strategy_candidates": 3, "promotable_candidates": 1},
                "symbols": [
                    {
                        "symbol": "XAUUSD",
                        "selected_family": "xau_scalp_microtrend",
                        "selected_regime": "continuation",
                        "wr_gap_to_target": -0.02,
                        "sample_gap_to_target": 0,
                        "canary_total": {"wins": 5, "resolved": 8},
                        "asian_long_memory": {
                            "family": "xau_scalp_microtrend",
                            "confidence_band": "70-74.9",
                            "wins": 5,
                            "resolved": 5,
                            "pnl_usd": 22.41,
                        },
                        "winner_memory_library": {
                            "family": "xau_scalp_pullback_limit",
                            "session": "asian",
                            "direction": "long",
                            "stats": {"wins": 6, "resolved": 6, "pnl_usd": 26.46},
                        },
                    }
                ],
            }
        )
        self.assertIn("memory asian-long", text)
        self.assertIn("library beat-market", text)
        self.assertIn("xau_scalp_pullback_limit", text)
        self.assertIn("xau_scalp_microtrend", text)
        self.assertIn("22.41", text)

    def test_main_scalp_limit_lane_enforces_strict_limit_fallback_off(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=78.0)
        sig.entry_type = "limit"
        skipped = MT5ExecutionResult(
            ok=False,
            status="skipped",
            message="test skip",
            signal_symbol="XAUUSD",
            broker_symbol="XAUUSD",
        )
        with patch.object(scheduler_module.config, "MT5_ENABLED", True), \
             patch.object(scheduler_module.config, "MT5_REPEAT_ERROR_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "NEURAL_BRAIN_ENABLED", False), \
             patch.object(scheduler_module.config, "MT5_AUTOPILOT_ENABLED", False), \
             patch.object(dexter, "_check_macro_rumor_trade_guard", return_value=(False, "", {})), \
             patch.object(dexter, "_neural_execution_filter_ready", return_value=(False, {})), \
             patch.object(dexter, "_apply_xau_event_shock_trade_controls", return_value=(None, None)), \
             patch.object(scheduler_module.mt5_executor, "execute_signal", return_value=skipped) as exec_call:
            dexter._maybe_execute_mt5_signal(sig, source="scalp_xauusd")

        self.assertEqual(exec_call.call_count, 1)
        forwarded = exec_call.call_args.args[0]
        self.assertFalse(bool(getattr(forwarded, "raw_scores", {}).get("mt5_limit_allow_market_fallback", True)))
        self.assertEqual(
            str(getattr(forwarded, "raw_scores", {}).get("mt5_limit_policy_reason", "")),
            "main_scalp_strict_limit",
        )


    # ── Crypto Smart Families (CFS / CFB / CWC / CBR) ──────────────────────

    def test_cfs_btc_sell_stop_fires(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=72.0)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 70200.0
        sig.entry_type = "sell_stop"
        sig.direction = "short"
        sig.raw_scores["crypto_winner_logic_regime"] = "neutral"
        sig.raw_scores["short"] = 75.0
        sig.raw_scores["edge"] = 50.0
        sig.raw_scores["scalping_trigger"] = {"rsi14": 40.0}
        with patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_ENABLED", True), \
             patch.object(scheduler_module.config, "get_crypto_flow_short_allowed_symbols", return_value={"BTCUSD", "ETHUSD"}), \
             patch.object(scheduler_module.config, "get_crypto_flow_short_allowed_sessions", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_MIN_CONFIDENCE", 68.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_MAX_CONFIDENCE", 85.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_MIN_SHORT_SCORE", 70.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_MIN_EDGE", 30.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_RSI_MAX", 45.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_BLOCK_SEVERE_WINNER", True), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_BREAK_STOP_TRIGGER_RISK_RATIO", 0.10), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_BREAK_STOP_STOP_LIFT_RATIO", 0.30), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_BTC_CTRADER_RISK_USD", 0.45), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_flow_short_signal(sig, base_source="scalp_btcusd", candidate={"family": "crypto_flow_short", "strategy_id": "test", "priority": 199})
        self.assertIsNotNone(shaped)
        self.assertEqual(lane_source, "scalp_btcusd:cfs:canary")
        self.assertEqual(str(getattr(shaped, "entry_type", "")), "sell_stop")
        raw = getattr(shaped, "raw_scores", {})
        self.assertEqual(raw.get("strategy_family"), "crypto_flow_short")
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0)), 0.45, places=2)

    def test_cfs_blocks_long_direction(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=72.0)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "limit"
        sig.direction = "long"
        sig.raw_scores["short"] = 75.0
        sig.raw_scores["edge"] = 50.0
        sig.raw_scores["scalping_trigger"] = {"rsi14": 40.0}
        with patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_ENABLED", True), \
             patch.object(scheduler_module.config, "get_crypto_flow_short_allowed_symbols", return_value={"BTCUSD"}), \
             patch.object(scheduler_module.config, "get_crypto_flow_short_allowed_sessions", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_MIN_CONFIDENCE", 68.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_MAX_CONFIDENCE", 85.0), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, _ = dexter._build_crypto_flow_short_signal(sig, base_source="scalp_btcusd", candidate={"family": "crypto_flow_short", "strategy_id": "test", "priority": 199})
        self.assertIsNone(shaped)

    def test_cfs_blocks_high_rsi(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=72.0)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 70200.0
        sig.entry_type = "sell_stop"
        sig.direction = "short"
        sig.raw_scores["short"] = 75.0
        sig.raw_scores["edge"] = 50.0
        sig.raw_scores["scalping_trigger"] = {"rsi14": 55.0}
        with patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_ENABLED", True), \
             patch.object(scheduler_module.config, "get_crypto_flow_short_allowed_symbols", return_value={"BTCUSD"}), \
             patch.object(scheduler_module.config, "get_crypto_flow_short_allowed_sessions", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_MIN_CONFIDENCE", 68.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_MAX_CONFIDENCE", 85.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_MIN_SHORT_SCORE", 70.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_MIN_EDGE", 30.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_SHORT_RSI_MAX", 45.0), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, _ = dexter._build_crypto_flow_short_signal(sig, base_source="scalp_btcusd", candidate={"family": "crypto_flow_short", "strategy_id": "test", "priority": 199})
        self.assertIsNone(shaped)

    def test_cfb_btc_buy_stop_fires(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=73.0)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "limit"
        sig.direction = "long"
        sig.raw_scores["crypto_winner_logic_regime"] = "strong"
        sig.raw_scores["long"] = 90.0
        sig.raw_scores["edge"] = 60.0
        sig.raw_scores["scalping_trigger"] = {"rsi14": 60.0}
        with patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_ENABLED", True), \
             patch.object(scheduler_module.config, "get_crypto_flow_buy_allowed_symbols", return_value={"BTCUSD", "ETHUSD"}), \
             patch.object(scheduler_module.config, "get_crypto_flow_buy_allowed_sessions", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_MIN_CONFIDENCE", 68.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_MAX_CONFIDENCE", 80.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_MIN_LONG_SCORE", 85.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_MIN_EDGE", 40.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_RSI_MIN", 55.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_RSI_MAX", 70.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_REQUIRE_STRONG_WINNER", True), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_ALLOW_NEUTRAL_WINNER", True), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_BREAK_STOP_TRIGGER_RISK_RATIO", 0.10), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_BREAK_STOP_STOP_LIFT_RATIO", 0.30), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_BTC_CTRADER_RISK_USD", 0.65), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_flow_buy_signal(sig, base_source="scalp_btcusd", candidate={"family": "crypto_flow_buy", "strategy_id": "test", "priority": 199})
        self.assertIsNotNone(shaped)
        self.assertEqual(lane_source, "scalp_btcusd:cfb:canary")
        self.assertEqual(str(getattr(shaped, "entry_type", "")), "buy_stop")
        raw = getattr(shaped, "raw_scores", {})
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0)), 0.65, places=2)

    def test_cfb_blocks_short_direction(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=73.0)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 70200.0
        sig.entry_type = "sell_stop"
        sig.direction = "short"
        sig.raw_scores["long"] = 90.0
        sig.raw_scores["edge"] = 60.0
        sig.raw_scores["scalping_trigger"] = {"rsi14": 60.0}
        with patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_ENABLED", True), \
             patch.object(scheduler_module.config, "get_crypto_flow_buy_allowed_symbols", return_value={"BTCUSD"}), \
             patch.object(scheduler_module.config, "get_crypto_flow_buy_allowed_sessions", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_MIN_CONFIDENCE", 68.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_MAX_CONFIDENCE", 80.0), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, _ = dexter._build_crypto_flow_buy_signal(sig, base_source="scalp_btcusd", candidate={"family": "crypto_flow_buy", "strategy_id": "test", "priority": 199})
        self.assertIsNone(shaped)

    def test_cfb_blocks_overbought_rsi(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=73.0)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "limit"
        sig.direction = "long"
        sig.raw_scores["crypto_winner_logic_regime"] = "strong"
        sig.raw_scores["long"] = 90.0
        sig.raw_scores["edge"] = 60.0
        sig.raw_scores["scalping_trigger"] = {"rsi14": 72.0}
        with patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_ENABLED", True), \
             patch.object(scheduler_module.config, "get_crypto_flow_buy_allowed_symbols", return_value={"BTCUSD"}), \
             patch.object(scheduler_module.config, "get_crypto_flow_buy_allowed_sessions", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_MIN_CONFIDENCE", 68.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_MAX_CONFIDENCE", 80.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_MIN_LONG_SCORE", 85.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_MIN_EDGE", 40.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_RSI_MIN", 55.0), \
             patch.object(scheduler_module.config, "CRYPTO_FLOW_BUY_RSI_MAX", 70.0), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, _ = dexter._build_crypto_flow_buy_signal(sig, base_source="scalp_btcusd", candidate={"family": "crypto_flow_buy", "strategy_id": "test", "priority": 199})
        self.assertIsNone(shaped)

    def test_cwc_fires_strong_winner(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=74.0)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "limit"
        sig.direction = "long"
        sig.raw_scores["crypto_winner_logic_regime"] = "strong"
        sig.raw_scores["crypto_winner_logic_win_rate"] = 0.65
        sig.raw_scores["edge"] = 65.0
        sig.raw_scores["neural_probability"] = 0.68
        with patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_ENABLED", True), \
             patch.object(scheduler_module.config, "get_crypto_winner_confirmed_allowed_symbols", return_value={"BTCUSD"}), \
             patch.object(scheduler_module.config, "get_crypto_winner_confirmed_allowed_sessions", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_MAX_CONFIDENCE", 80.0), \
             patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_MIN_WIN_RATE", 0.62), \
             patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_MIN_EDGE", 60.0), \
             patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_MIN_NEURAL_PROB", 0.62), \
             patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_CTRADER_RISK_USD", 0.90), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_winner_confirmed_signal(sig, base_source="scalp_btcusd", candidate={"family": "crypto_winner_confirmed", "strategy_id": "test", "priority": 199})
        self.assertIsNotNone(shaped)
        self.assertEqual(lane_source, "scalp_btcusd:cwc:canary")
        raw = getattr(shaped, "raw_scores", {})
        self.assertAlmostEqual(float(raw.get("ctrader_risk_usd_override", 0)), 0.90, places=2)

    def test_cwc_blocks_neutral_winner(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=74.0)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "limit"
        sig.direction = "long"
        sig.raw_scores["crypto_winner_logic_regime"] = "neutral"
        sig.raw_scores["crypto_winner_logic_win_rate"] = 0.55
        sig.raw_scores["edge"] = 65.0
        sig.raw_scores["neural_probability"] = 0.68
        with patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_ENABLED", True), \
             patch.object(scheduler_module.config, "get_crypto_winner_confirmed_allowed_symbols", return_value={"BTCUSD"}), \
             patch.object(scheduler_module.config, "get_crypto_winner_confirmed_allowed_sessions", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_MAX_CONFIDENCE", 80.0), \
             patch.object(scheduler_module.config, "CRYPTO_WINNER_CONFIRMED_MIN_WIN_RATE", 0.62), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, _ = dexter._build_crypto_winner_confirmed_signal(sig, base_source="scalp_btcusd", candidate={"family": "crypto_winner_confirmed", "strategy_id": "test", "priority": 199})
        self.assertIsNone(shaped)

    def test_cbr_market_to_limit_conversion(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=75.0)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "CHOCH_ENTRY"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "market"
        sig.direction = "long"
        sig.raw_scores["crypto_winner_logic_regime"] = "neutral"
        sig.raw_scores["neural_probability"] = 0.70
        with patch.object(scheduler_module.config, "CRYPTO_BEHAVIORAL_RETEST_ENABLED", True), \
             patch.object(scheduler_module.config, "get_crypto_behavioral_retest_allowed_symbols", return_value={"BTCUSD", "ETHUSD"}), \
             patch.object(scheduler_module.config, "get_crypto_behavioral_retest_allowed_sessions", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "get_crypto_behavioral_retest_allowed_patterns", return_value={"choch_entry"}), \
             patch.object(scheduler_module.config, "CRYPTO_BEHAVIORAL_RETEST_MIN_CONFIDENCE", 72.0), \
             patch.object(scheduler_module.config, "CRYPTO_BEHAVIORAL_RETEST_MAX_CONFIDENCE", 82.0), \
             patch.object(scheduler_module.config, "CRYPTO_BEHAVIORAL_RETEST_MIN_NEURAL_PROB", 0.65), \
             patch.object(scheduler_module.config, "CRYPTO_BEHAVIORAL_RETEST_BLOCK_SEVERE_WINNER", True), \
             patch.object(scheduler_module.config, "CRYPTO_BEHAVIORAL_RETEST_PULLBACK_RISK_RATIO", 0.15), \
             patch.object(scheduler_module.config, "CRYPTO_BEHAVIORAL_RETEST_BTC_CTRADER_RISK_USD", 0.45), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, lane_source = dexter._build_crypto_behavioral_retest_signal(sig, base_source="scalp_btcusd", candidate={"family": "crypto_behavioral_retest", "strategy_id": "test", "priority": 199})
        self.assertIsNotNone(shaped)
        self.assertEqual(lane_source, "scalp_btcusd:cbr:canary")
        self.assertEqual(str(getattr(shaped, "entry_type", "")), "limit")
        raw = getattr(shaped, "raw_scores", {})
        self.assertTrue(raw.get("crypto_behavioral_retest_market_to_limit"))

    def test_cbr_blocks_wrong_pattern(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("BTCUSD", confidence=75.0)
        sig.timeframe = "5m+1m"
        sig.session = "london, new_york, overlap"
        sig.pattern = "OB_BOUNCE"
        sig.entry = 70000.0
        sig.stop_loss = 69800.0
        sig.entry_type = "limit"
        sig.direction = "long"
        sig.raw_scores["neural_probability"] = 0.70
        with patch.object(scheduler_module.config, "CRYPTO_BEHAVIORAL_RETEST_ENABLED", True), \
             patch.object(scheduler_module.config, "get_crypto_behavioral_retest_allowed_symbols", return_value={"BTCUSD"}), \
             patch.object(scheduler_module.config, "get_crypto_behavioral_retest_allowed_sessions", return_value={"london,new_york,overlap"}), \
             patch.object(scheduler_module.config, "get_crypto_behavioral_retest_allowed_patterns", return_value={"choch_entry"}), \
             patch.object(scheduler_module.config, "CRYPTO_BEHAVIORAL_RETEST_MIN_CONFIDENCE", 72.0), \
             patch.object(scheduler_module.config, "CRYPTO_BEHAVIORAL_RETEST_MAX_CONFIDENCE", 82.0), \
             patch("scheduler.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            shaped, _ = dexter._build_crypto_behavioral_retest_signal(sig, base_source="scalp_btcusd", candidate={"family": "crypto_behavioral_retest", "strategy_id": "test", "priority": 199})
        self.assertIsNone(shaped)

    def test_rsi_ceiling_blocks_overbought_long(self):
        from scanners.scalping_scanner import ScalpingScanner
        import pandas as pd
        scanner = ScalpingScanner.__new__(ScalpingScanner)
        scanner._as_float = lambda v, d=0.0: float(v) if v is not None else d
        close_prices = [70000 + i * 10 for i in range(130)]
        high_prices = [p + 20 for p in close_prices]
        low_prices = [p - 20 for p in close_prices]
        open_prices = [p - 5 for p in close_prices]
        df = pd.DataFrame({"open": open_prices, "high": high_prices, "low": low_prices, "close": close_prices})
        with patch.object(scheduler_module.config, "SCALPING_M1_TRIGGER_RSI_LONG_MIN", 52.0), \
             patch.object(scheduler_module.config, "SCALPING_M1_TRIGGER_RSI_LONG_MAX", 70.0), \
             patch.object(scheduler_module.config, "SCALPING_M1_TRIGGER_REFHIGH_BUFFER_MULT_LONG", 1.0):
            ok, info = scanner._m1_trigger(df, direction="long")
        checks = info.get("checks", {})
        if checks.get("rsi_ceiling") is not None:
            rsi_val = info.get("rsi14", 0)
            if rsi_val > 70:
                self.assertFalse(checks.get("rsi_ceiling"), f"RSI {rsi_val} should be blocked by ceiling 70")

    def test_crypto_severe_winner_hard_block_default_on(self):
        self.assertTrue(bool(getattr(scheduler_module.config, "SCALPING_CRYPTO_WINNER_HARD_BLOCK_SEVERE", False)), "SCALPING_CRYPTO_WINNER_HARD_BLOCK_SEVERE should default to True")

    # --- Scheduled canary MTF direction guard tests ---

    def test_xau_scheduled_mtf_guard_blocks_short_when_bullish_aligned(self):
        """Scheduled canary SHORT must be blocked when D1/H4/H1 are all bullish."""
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "short"
        sig.session = "london"
        sig.timeframe = "1h"
        sig.entry_type = "limit"
        sig.raw_scores.update({
            "signal_d1_trend": "bullish",
            "signal_h4_trend": "bullish",
            "signal_h1_trend": "bullish",
            "xau_multi_tf_snapshot": {
                "d1_trend": "bullish",
                "h4_trend": "bullish",
                "h1_trend": "bullish",
                "strict_aligned_side": "long",
                "strict_alignment": "aligned_bullish",
            },
        })

        with patch.object(scheduler_module.config, "CTRADER_SOURCE_PROFILE_GATE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MTF_GUARD_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_REQUIRE_D1_H4_H1_ALIGN", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_ALLOW_COUNTERTREND_CONFIRMED", False), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_sessions", return_value={"london"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_timeframes", return_value={"1h"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_entry_types", return_value={"limit"}):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, source="xauusd_scheduled")

        self.assertFalse(allowed)
        self.assertIn("xau_scheduled_mtf_block", reason)
        self.assertIn("d1_h4_h1_block:short_vs_long", reason)

    def test_xau_scheduled_mtf_guard_blocks_long_when_bearish_aligned(self):
        """Scheduled canary LONG blocked by MTF guard (bypassing earlier style guard)."""
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "long"
        sig.session = "london"
        sig.timeframe = "1h"
        sig.entry_type = "limit"
        sig.raw_scores.update({
            "signal_d1_trend": "bearish",
            "signal_h4_trend": "bearish",
            "signal_h1_trend": "bearish",
            "xau_multi_tf_snapshot": {
                "d1_trend": "bearish",
                "h4_trend": "bearish",
                "h1_trend": "bearish",
                "strict_aligned_side": "short",
                "strict_alignment": "aligned_bearish",
            },
        })

        with patch.object(scheduler_module.config, "CTRADER_SOURCE_PROFILE_GATE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MTF_GUARD_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_REQUIRE_D1_H4_H1_ALIGN", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_ALLOW_COUNTERTREND_CONFIRMED", False), \
             patch.object(scheduler_module.config, "XAU_COUNTERTREND_LONG_REQUIRE_CONFIRMED", False), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_sessions", return_value={"london"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_timeframes", return_value={"1h"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_entry_types", return_value={"limit"}):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, source="xauusd_scheduled")

        self.assertFalse(allowed)
        self.assertIn("xau_scheduled_mtf_block", reason)

    def test_xau_scheduled_mtf_guard_allows_short_when_bearish_aligned(self):
        """Scheduled canary SHORT should pass when D1/H4/H1 are all bearish."""
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "short"
        sig.session = "london"
        sig.timeframe = "1h"
        sig.entry_type = "limit"
        sig.raw_scores.update({
            "signal_d1_trend": "bearish",
            "signal_h4_trend": "bearish",
            "signal_h1_trend": "bearish",
            "xau_multi_tf_snapshot": {
                "d1_trend": "bearish",
                "h4_trend": "bearish",
                "h1_trend": "bearish",
                "strict_aligned_side": "short",
                "strict_alignment": "aligned_bearish",
            },
        })

        with patch.object(scheduler_module.config, "CTRADER_SOURCE_PROFILE_GATE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MTF_GUARD_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_REQUIRE_D1_H4_H1_ALIGN", True), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_ALLOW_COUNTERTREND_CONFIRMED", False), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_sessions", return_value={"london"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_timeframes", return_value={"1h"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_entry_types", return_value={"limit"}):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, source="xauusd_scheduled")

        self.assertTrue(allowed)
        self.assertEqual(reason, "xau_scheduled_profile_pass")

    def test_xau_scheduled_mtf_guard_disabled_allows_counter_trend(self):
        """When MTF guard is disabled, counter-trend scheduled canary SHORT should pass."""
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD", confidence=74.0)
        sig.direction = "short"
        sig.session = "london"
        sig.timeframe = "1h"
        sig.entry_type = "limit"
        sig.raw_scores.update({
            "signal_d1_trend": "bullish",
            "signal_h4_trend": "bullish",
            "signal_h1_trend": "bullish",
            "xau_multi_tf_snapshot": {
                "d1_trend": "bullish",
                "h4_trend": "bullish",
                "h1_trend": "bullish",
                "strict_aligned_side": "long",
                "strict_alignment": "aligned_bullish",
            },
        })

        with patch.object(scheduler_module.config, "CTRADER_SOURCE_PROFILE_GATE_ENABLED", True), \
             patch.object(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MIN_CONFIDENCE", 70.0), \
             patch.object(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MTF_GUARD_ENABLED", False), \
             patch.object(scheduler_module.config, "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED", True), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_sessions", return_value={"london"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_timeframes", return_value={"1h"}), \
             patch.object(scheduler_module.config, "get_ctrader_xau_scheduled_allowed_entry_types", return_value={"limit"}):
            allowed, reason = dexter._allow_ctrader_source_profile(sig, source="xauusd_scheduled")

        self.assertTrue(allowed)
        self.assertEqual(reason, "xau_scheduled_profile_pass")

    # --- Canary family BE config defaults ---

    def test_canary_family_be_config_defaults(self):
        """Verify canary family BE config keys exist with correct defaults."""
        self.assertAlmostEqual(float(getattr(scheduler_module.config, "CTRADER_PM_CANARY_FAMILY_BE_TRIGGER_R", 0)), 0.80, places=2)
        self.assertAlmostEqual(float(getattr(scheduler_module.config, "CTRADER_PM_CANARY_FAMILY_BE_LOCK_R", 0)), 0.05, places=2)

    def test_xau_scheduled_mtf_guard_config_default(self):
        """Verify scheduled canary MTF guard is enabled by default."""
        self.assertTrue(bool(getattr(scheduler_module.config, "CTRADER_XAU_SCHEDULED_MTF_GUARD_ENABLED", False)))

    def test_classify_family_canary_build_miss_stamp_and_mtf(self):
        dexter = scheduler_module.DexterScheduler()
        cand = {"family": "xau_scalp_flow_short_sidecar", "strategy_id": "xau_scalp_flow_short_sidecar_v1"}
        sig = make_signal("XAUUSD")
        st, r = dexter._classify_family_canary_build_miss(sig, cand)
        self.assertEqual(st, "family_builder")
        self.assertIn("unstamped", r)

        dexter._stamp_family_canary_skip(
            sig, family="xau_scalp_flow_short_sidecar", stage="pattern_gate", reason="fss_pattern_token_not_allowed"
        )
        st, r = dexter._classify_family_canary_build_miss(sig, cand)
        self.assertEqual(st, "pattern_gate")
        self.assertEqual(r, "fss_pattern_token_not_allowed")

        sig2 = make_signal("XAUUSD")
        rs = dict(sig2.raw_scores or {})
        rs["xau_multi_tf_guard_block"] = True
        rs["xau_multi_tf_guard_reason"] = "mtf_unit_test"
        sig2.raw_scores = rs
        st, r = dexter._classify_family_canary_build_miss(sig2, cand)
        self.assertEqual(st, "multi_tf_guard")
        self.assertEqual(r, "mtf_unit_test")

    def test_family_canary_ff_stamps_executor_only_reason(self):
        dexter = scheduler_module.DexterScheduler()
        sig = make_signal("XAUUSD")
        cand = {"family": "xau_scalp_failed_fade_follow_stop", "strategy_id": "xau_scalp_failed_fade_follow_stop_v1"}
        with patch.object(dexter, "_xau_multi_tf_entry_guard", return_value={"blocked": False, "allowed": True}):
            lane, _src = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=cand)
        self.assertIsNone(lane)
        skip = dict(getattr(sig, "raw_scores", {}) or {}).get("family_canary_skip") or {}
        self.assertEqual(skip.get("family"), "xau_scalp_failed_fade_follow_stop")
        self.assertEqual(skip.get("stage"), "family_builder")
        self.assertIn("executor_spawned", skip.get("reason", ""))


# =============================================================================
# LIVE DISPATCH INTEGRATION TESTS
#
# These tests simulate the FULL live signal path:
#   Scanner → _maybe_execute_ctrader_signal → gate stack → executor.execute_signal
#
# The regular unit tests and BT both bypass _allow_scalp_xau_live_mt5() and
# _allow_ctrader_source_profile() entirely, so bugs there cause live trading to
# stop while BT and unit tests still show green.  These tests exist to close
# that gap.  Each test exercises ONE gate in the dispatch stack, leaving all
# other gates disabled so the failure reason is unambiguous.
# =============================================================================

class LiveDispatchIntegrationTests(unittest.TestCase):
    """Full live dispatch path tests: scanner signal → cTrader executor."""

    def setUp(self):
        self._journal_patcher = patch.object(
            scheduler_module.ctrader_executor, "journal_pre_dispatch_skip", return_value=0
        )
        self._db_journal_patcher = patch.object(
            scheduler_module.ctrader_executor, "_journal", return_value=0
        )
        self._journal_mock = self._journal_patcher.start()
        self._db_journal_mock = self._db_journal_patcher.start()

    def tearDown(self):
        self._db_journal_patcher.stop()
        self._journal_patcher.stop()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _xau_signal(session="new_york", confidence=73.0, direction="long"):
        sig = make_signal("XAUUSD", confidence=confidence)
        sig.session = session
        sig.direction = direction
        sig.timeframe = "5m"
        sig.entry = 3100.0
        sig.stop_loss = 3090.0
        sig.take_profit_1 = 3110.0
        sig.take_profit_2 = 3120.0
        sig.take_profit_3 = 3130.0
        return sig

    @staticmethod
    def _base_dispatch_patches(extra=None):
        """Minimal patches to let a signal reach executor with all gates open."""
        patches = {
            "CTRADER_ENABLED": True,
            "CTRADER_AUTOTRADE_ENABLED": True,
            "CTRADER_SOURCE_PROFILE_GATE_ENABLED": False,
            "XAU_HOLIDAY_GUARD_ENABLED": False,
            "MT5_SCALP_XAU_LIVE_FILTER_ENABLED": False,
            "SCALP_XAU_DIRECT_CONF_FILTER_ENABLED": False,
            "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED": False,
        }
        if extra:
            patches.update(extra)
        return patches

    # ------------------------------------------------------------------
    # GROUP 1 — SESSION GATE (regression for the bug that stopped XAUUSD)
    # ------------------------------------------------------------------

    def test_live_xauusd_asian_london_overlap_reaches_executor(self):
        """
        REGRESSION: asian,london overlap session must NOT be blocked.
        Before fix: 'asian,london' not in {'asian','london'} → True → blocked.
        After fix:  _session_signature_matches allows token-subset match → passes.
        """
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="asian,london", confidence=73.0)
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({
                "MT5_SCALP_XAU_LIVE_FILTER_ENABLED": True,
                "MT5_SCALP_XAU_LIVE_CONF_MIN": 70.0,
                "MT5_SCALP_XAU_LIVE_CONF_MAX": 80.0,
            }).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_mt5_scalp_xau_live_sessions",
                return_value={"asian", "london", "new_york"}))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertEqual(exec_mock.call_count, 1,
            "executor.execute_signal must be called for asian,london overlap — was blocked before fix")

    def test_live_xauusd_pure_asian_session_reaches_executor(self):
        """Pure 'asian' session should reach executor when asian is in allowed list."""
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="asian", confidence=73.0)
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({
                "MT5_SCALP_XAU_LIVE_FILTER_ENABLED": True,
                "MT5_SCALP_XAU_LIVE_CONF_MIN": 70.0,
                "MT5_SCALP_XAU_LIVE_CONF_MAX": 80.0,
            }).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_mt5_scalp_xau_live_sessions",
                return_value={"asian", "london", "new_york"}))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertEqual(exec_mock.call_count, 1)

    def test_live_xauusd_off_hours_session_is_blocked_by_filter(self):
        """Session NOT in allowed list must be blocked and executor must NOT be called."""
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="off_hours", confidence=73.0)
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({
                "MT5_SCALP_XAU_LIVE_FILTER_ENABLED": True,
                "MT5_SCALP_XAU_LIVE_CONF_MIN": 70.0,
                "MT5_SCALP_XAU_LIVE_CONF_MAX": 80.0,
            }).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_mt5_scalp_xau_live_sessions",
                return_value={"asian", "london", "new_york"}))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            result = dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertIsNone(result)
        self.assertEqual(exec_mock.call_count, 0)
        audit_kwargs = self._journal_mock.call_args.kwargs if self._journal_mock.call_count else {}
        self.assertIn("session_not_allowed", str(audit_kwargs.get("reason", "")))

    # ------------------------------------------------------------------
    # GROUP 2 — HOLIDAY GATE
    # ------------------------------------------------------------------

    def test_live_xauusd_good_friday_blocks_dispatch(self):
        """Good Friday (2026-04-03) must block all XAUUSD orders."""
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="new_york")
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({"XAU_HOLIDAY_GUARD_ENABLED": True}).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            stack.enter_context(patch.object(
                scheduler_module.session_manager, "is_xauusd_holiday", return_value=True))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            result = dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertIsNone(result)
        self.assertEqual(exec_mock.call_count, 0)
        audit_kwargs = self._journal_mock.call_args.kwargs if self._journal_mock.call_count else {}
        self.assertIn("holiday", str(audit_kwargs.get("reason", "")))

    def test_live_xauusd_christmas_blocks_dispatch(self):
        """Christmas Day (Dec 25) must block XAUUSD dispatch."""
        from market.data_fetcher import SessionManager
        from datetime import date
        christmas = date(2026, 12, 25)
        self.assertIn(christmas, SessionManager.xauusd_market_holidays(2026))

        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="london")
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({"XAU_HOLIDAY_GUARD_ENABLED": True}).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            stack.enter_context(patch.object(
                scheduler_module.session_manager, "is_xauusd_holiday", return_value=True))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            result = dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertIsNone(result)
        self.assertEqual(exec_mock.call_count, 0)

    def test_live_xauusd_new_years_day_in_holiday_set(self):
        """New Year's Day must be in the xauusd_market_holidays set for 2026 and 2027."""
        from market.data_fetcher import SessionManager
        from datetime import date
        self.assertIn(date(2026, 1, 1), SessionManager.xauusd_market_holidays(2026))
        self.assertIn(date(2027, 1, 1), SessionManager.xauusd_market_holidays(2027))

    def test_live_xauusd_easter_algorithm_correct_2026(self):
        """Butcher's Easter algorithm must return 2026-04-05 for 2026."""
        from market.data_fetcher import SessionManager
        from datetime import date
        self.assertEqual(SessionManager._easter_sunday(2026), date(2026, 4, 5))
        good_friday_2026 = date(2026, 4, 3)
        self.assertIn(good_friday_2026, SessionManager.xauusd_market_holidays(2026))

    def test_live_xauusd_normal_weekday_not_blocked_by_holiday_guard(self):
        """A regular Tuesday must NOT be blocked by the holiday guard."""
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="london")
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({"XAU_HOLIDAY_GUARD_ENABLED": True}).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            # Explicitly: not a holiday, not weekend
            stack.enter_context(patch.object(
                scheduler_module.session_manager, "is_xauusd_holiday", return_value=False))
            stack.enter_context(patch.object(
                scheduler_module.session_manager, "is_xauusd_market_open", return_value=True))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertEqual(exec_mock.call_count, 1,
            "Normal weekday must reach executor — holiday guard must not over-block")

    # ------------------------------------------------------------------
    # GROUP 3 — WEEKEND GATE
    # ------------------------------------------------------------------

    def test_live_xauusd_weekend_blocks_dispatch(self):
        """Saturday must be blocked by market-closed check (not holiday guard)."""
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="off_hours")
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({"XAU_HOLIDAY_GUARD_ENABLED": True}).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            stack.enter_context(patch.object(
                scheduler_module.session_manager, "is_xauusd_holiday", return_value=False))
            stack.enter_context(patch.object(
                scheduler_module.session_manager, "is_xauusd_market_open", return_value=False))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            result = dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertIsNone(result)
        self.assertEqual(exec_mock.call_count, 0)

    # ------------------------------------------------------------------
    # GROUP 4 — MTF GUARD (in dispatch path)
    # ------------------------------------------------------------------

    def test_live_xauusd_all_unknown_mtf_allows_dispatch(self):
        """
        REGRESSION: when D1/H4/H1 all return 'unknown' (no provider data),
        signal must still reach executor.  Before fix: blocked as d1_h4_h1_not_aligned.
        """
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="london", confidence=73.0)
        # No trend data → all methods return "unknown"
        sig.raw_scores["xau_multi_tf_snapshot"] = {
            "strict_aligned_side": "", "strict_alignment": "unknown"
        }
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({
                "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED": True,
                "SCALP_XAU_DIRECT_MTF_USE_INTRABAR_COLOR": False,
            }).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        mtf = dict((sig.raw_scores or {}).get("xau_direct_lane_mtf_guard") or {})
        self.assertEqual(mtf.get("reason"), "missing_mtf_trends_allow",
            "All-unknown MTF must return missing_mtf_trends_allow and allow dispatch")
        self.assertEqual(exec_mock.call_count, 1)

    def test_live_xauusd_counter_trend_mtf_blocks_dispatch(self):
        """Bullish D1+H4+H1 with short direction must block dispatch."""
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="london", confidence=73.0, direction="short")
        sig.raw_scores.update({
            "signal_d1_trend": "bullish", "signal_h4_trend": "bullish",
            "signal_h1_trend": "bullish",
            "xau_multi_tf_snapshot": {
                "d1_trend": "bullish", "h4_trend": "bullish", "h1_trend": "bullish",
                "strict_aligned_side": "long", "strict_alignment": "aligned_bullish",
            },
        })
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({
                "SCALP_XAU_DIRECT_MTF_STRICT_ENABLED": True,
                "SCALP_XAU_DIRECT_CONF_FILTER_ENABLED": False,
                "SCALP_XAU_DIRECT_MTF_USE_INTRABAR_COLOR": False,
            }).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            result = dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertIsNone(result)
        self.assertEqual(exec_mock.call_count, 0)
        mtf = dict((sig.raw_scores or {}).get("xau_direct_lane_mtf_guard") or {})
        self.assertIn("d1_h4_h1_block", str(mtf.get("reason", "")))

    # ------------------------------------------------------------------
    # GROUP 5 — FULL END-TO-END (all gates open → executor called)
    # ------------------------------------------------------------------

    def test_live_xauusd_full_path_executor_called_with_correct_source(self):
        """
        All gates disabled → executor.execute_signal must be called exactly once
        with source='scalp_xauusd' (or winner lane).
        This is the end-to-end smoke test for the dispatch stack.
        """
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="london", confidence=73.0)
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches().items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertEqual(exec_mock.call_count, 1, "executor must be called exactly once")
        called_source = exec_mock.call_args.kwargs.get("source") or exec_mock.call_args[1].get("source", "")
        self.assertIn("scalp_xauusd", str(called_source))

    # ------------------------------------------------------------------
    # GROUP 6 — CANARY DISPATCH PATH
    # ------------------------------------------------------------------

    def test_live_canary_holiday_blocks_persistent_canary(self):
        """
        Holiday guard must also block _maybe_execute_persistent_canary.
        The canary used to bypass _allow_ctrader_source_profile entirely
        (fixed 2026-03-31), but the holiday guard is newer — verify it holds.
        """
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="london")
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        profile = {
            "enabled": True,
            "direct_enabled": True,
            "ctrader_enabled": True,
            "mt5_enabled": False,
            "source": "scalp_xauusd:behavioral_v2:canary",
            "base_source": "scalp_xauusd",
            "symbol": "XAUUSD",
            "run_parallel": False,
        }

        with ExitStack() as stack:
            stack.enter_context(patch.object(
                scheduler_module.config, "XAU_HOLIDAY_GUARD_ENABLED", True))
            stack.enter_context(patch.object(
                scheduler_module.session_manager, "is_xauusd_holiday", return_value=True))
            stack.enter_context(patch.object(
                dexter, "_persistent_canary_profile", return_value=profile))
            stack.enter_context(patch.object(
                scheduler_module.config, "PERSISTENT_CANARY_ENABLED", True))
            stack.enter_context(patch.object(
                scheduler_module.config, "PERSISTENT_CANARY_CTRADER_ENABLED", True))
            stack.enter_context(patch.object(
                scheduler_module.config, "PERSISTENT_CANARY_FAMILY_EXECUTOR_ENABLED", False))
            stack.enter_context(patch.object(
                scheduler_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", False))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            report = dexter._maybe_execute_persistent_canary(sig, source="scalp_xauusd")

        self.assertFalse(bool(report.get("ctrader")),
            "Holiday must block canary ctrader execution")
        self.assertEqual(exec_mock.call_count, 0,
            "executor.execute_signal must NOT be called on a holiday")

    def test_live_xauusd_confidence_below_band_blocks_dispatch(self):
        """
        Confidence below MT5_SCALP_XAU_LIVE_CONF_MIN must block dispatch.
        This gate fires after the holiday check and before the MTF guard.
        """
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="london", confidence=65.0)  # below 72 floor
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({
                "XAU_HOLIDAY_GUARD_ENABLED": False,
                "SCALP_XAU_DIRECT_CONF_FILTER_ENABLED": True,
                "MT5_SCALP_XAU_LIVE_CONF_MIN": 72.0,
                "MT5_SCALP_XAU_LIVE_CONF_MAX": 80.0,
            }).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            result = dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertIsNone(result)
        self.assertEqual(exec_mock.call_count, 0)
        audit_kwargs = self._journal_mock.call_args.kwargs if self._journal_mock.call_count else {}
        self.assertIn("conf_below_live_band", str(audit_kwargs.get("reason", "")))

    def test_live_xauusd_confidence_within_band_reaches_executor(self):
        """Confidence within [conf_min, conf_max) must pass the confidence gate."""
        dexter = scheduler_module.DexterScheduler()
        sig = self._xau_signal(session="london", confidence=73.5)
        fake = SimpleNamespace(ok=True, dry_run=False, status="accepted",
                               signal_symbol="XAUUSD", broker_symbol="XAUUSD", message="ok")

        with ExitStack() as stack:
            for k, v in self._base_dispatch_patches({
                "XAU_HOLIDAY_GUARD_ENABLED": False,
                "SCALP_XAU_DIRECT_CONF_FILTER_ENABLED": True,
                "MT5_SCALP_XAU_LIVE_CONF_MIN": 72.0,
                "MT5_SCALP_XAU_LIVE_CONF_MAX": 80.0,
            }).items():
                stack.enter_context(patch.object(scheduler_module.config, k, v))
            stack.enter_context(patch.object(
                scheduler_module.config, "get_ctrader_allowed_sources",
                return_value={"scalp_xauusd"}))
            exec_mock = stack.enter_context(patch.object(
                scheduler_module.ctrader_executor, "execute_signal", return_value=fake))
            dexter._maybe_execute_ctrader_signal(sig, source="scalp_xauusd")

        self.assertEqual(exec_mock.call_count, 1)


if __name__ == "__main__":
    unittest.main()
