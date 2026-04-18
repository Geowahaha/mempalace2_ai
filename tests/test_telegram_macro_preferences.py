import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from config import config
from market.economic_calendar import EconomicEvent
from market.macro_news import MacroHeadline
from notifier.telegram_bot import TelegramNotifier
from scanners.stock_scanner import StockOpportunity
from analysis.signals import TradeSignal
from execution.mt5_executor import MT5ExecutionResult


class TelegramMacroPreferenceTests(unittest.TestCase):
    def test_send_xau_guard_transition_alert_for_news_freeze(self):
        n = TelegramNotifier()
        payload = {
            "kind": "news_freeze",
            "action": "activated",
            "checked_utc": "2026-03-19T14:30:00Z",
            "title": "US CPI",
            "nearest_min": 18,
            "window_min": 20,
        }
        with patch.object(n, "_send", return_value=True) as send_call:
            ok = n.send_xau_guard_transition_alert(payload, chat_id=999)
        self.assertTrue(ok)
        text = send_call.call_args.args[0]
        self.assertIn("NEWS FREEZE ACTIVE", text)
        self.assertIn("blocked because CPI", text)
        self.assertEqual(send_call.call_args.kwargs["feature"], "calendar")
        self.assertEqual(send_call.call_args.kwargs["signal_symbol"], "XAUUSD")
        self.assertIsNone(send_call.call_args.kwargs["parse_mode"])

    def test_send_xau_guard_transition_alert_for_kill_switch(self):
        n = TelegramNotifier()
        payload = {
            "kind": "kill_switch",
            "action": "cleared",
            "checked_utc": "2026-03-19T14:31:00Z",
            "title": "Reuters headline",
            "shock_score": 18.4,
            "source": "Reuters",
            "verification": "confirmed",
        }
        with patch.object(n, "_send", return_value=True) as send_call:
            ok = n.send_xau_guard_transition_alert(payload, chat_id=999)
        self.assertTrue(ok)
        text = send_call.call_args.args[0]
        self.assertIn("KILL SWITCH CLEARED", text)
        self.assertIn("Reuters headline", text)
        self.assertEqual(send_call.call_args.kwargs["feature"], "macro")

    def test_send_signal_marks_scalping_signal_type(self):
        n = TelegramNotifier()
        sig = TradeSignal(
            symbol="XAUUSD",
            direction="short",
            confidence=74.0,
            entry=2145.0,
            stop_loss=2151.0,
            take_profit_1=2140.0,
            take_profit_2=2135.0,
            take_profit_3=2130.0,
            risk_reward=2.0,
            timeframe="5m+1m",
            session="new_york",
            trend="bearish",
            rsi=44.0,
            atr=3.2,
            pattern="SCALP_TEST",
            reasons=[],
            warnings=[],
            raw_scores={"scalping": True, "scalping_source": "scalp_xauusd"},
        )
        with patch.object(n, "_send", return_value=True) as send_call:
            ok = n.send_signal(sig, chat_id=999)
        self.assertTrue(ok)
        text = send_call.call_args.args[0]
        self.assertIn("Signal Type:* `SCALPING`", text)

    def test_send_signal_includes_run_trace_tag(self):
        n = TelegramNotifier()
        sig = TradeSignal(
            symbol="XAUUSD",
            direction="long",
            confidence=81.2,
            entry=2101.1,
            stop_loss=2098.8,
            take_profit_1=2102.4,
            take_profit_2=2103.7,
            take_profit_3=2105.0,
            risk_reward=1.4,
            timeframe="5m+1m",
            session="asian",
            trend="bullish",
            rsi=58.0,
            atr=3.1,
            pattern="TRACE_TEST",
            reasons=[],
            warnings=[],
            raw_scores={"signal_run_no": 42, "signal_trace_tag": "R000042", "signal_run_id": "20260306010101-000042"},
        )
        with patch.object(n, "_send", return_value=True) as send_call:
            ok = n.send_signal(sig, chat_id=999)
        self.assertTrue(ok)
        text = send_call.call_args.args[0]
        self.assertIn("`#R000042`", text)

    def test_mt5_execution_update_includes_run_trace(self):
        n = TelegramNotifier()
        sig = TradeSignal(
            symbol="XAUUSD",
            direction="long",
            confidence=79.0,
            entry=2099.5,
            stop_loss=2097.0,
            take_profit_1=2100.5,
            take_profit_2=2101.5,
            take_profit_3=2102.5,
            risk_reward=1.2,
            timeframe="5m+1m",
            session="asian",
            trend="bullish",
            rsi=55.0,
            atr=2.8,
            pattern="TRACE_EXEC",
            reasons=[],
            warnings=[],
            raw_scores={"signal_run_no": 8, "signal_trace_tag": "R000008", "signal_run_id": "20260306010202-000008"},
        )
        res = MT5ExecutionResult(
            ok=True,
            status="filled",
            message="order accepted retcode=10009",
            signal_symbol="XAUUSD",
            broker_symbol="XAUUSD",
            ticket=123,
            position_id=123,
        )
        with patch.object(n, "_send", return_value=True) as send_call:
            ok = n.send_mt5_execution_update(sig, res, source="scalp_xauusd")
        self.assertTrue(ok)
        text = send_call.call_args.args[0]
        self.assertIn("*Run:* `#R000008`", text)
        self.assertIn("*ID:* `20260306010202\\-000008`", text)

    def test_mt5_execution_update_shows_planned_vs_fill(self):
        n = TelegramNotifier()
        sig = TradeSignal(
            symbol="XAUUSD",
            direction="short",
            confidence=83.0,
            entry=5114.98,
            stop_loss=5118.36,
            take_profit_1=5111.85,
            take_profit_2=5110.80,
            take_profit_3=5109.41,
            risk_reward=1.2,
            timeframe="5m+1m",
            session="london",
            trend="bearish",
            rsi=38.0,
            atr=6.1,
            pattern="TRACE_EXEC",
            reasons=[],
            warnings=[],
            raw_scores={
                "signal_run_no": 9,
                "signal_trace_tag": "R000009",
                "signal_run_id": "20260306010303-000009",
                "mt5_planned_entry_price": 5114.98,
                "mt5_request_price": 5110.35,
                "mt5_actual_fill_price": 5110.35,
                "mt5_limit_fallback_market": True,
                "mt5_limit_fallback_reason": "ok",
            },
        )
        res = MT5ExecutionResult(
            ok=True,
            status="filled",
            message="order accepted retcode=10009",
            signal_symbol="XAUUSD",
            broker_symbol="XAUUSD",
            ticket=456,
            position_id=456,
        )
        with patch.object(n, "_send", return_value=True) as send_call:
            ok = n.send_mt5_execution_update(sig, res, source="scalp_xauusd:bypass")
        self.assertTrue(ok)
        text = send_call.call_args.args[0]
        self.assertIn("Planned/Fill", text)
        self.assertIn("Limit Fallback", text)

    def test_bypass_quick_tp_update_is_short_and_plain(self):
        n = TelegramNotifier()
        with patch.object(n, "_send", return_value=True) as send_call:
            ok = n.send_mt5_bypass_quick_tp_update(
                symbol="XAUUSD",
                ticket=1517262012,
                profit_usd=8.33,
                target_usd=7.20,
                balance_usd=721.76,
            )
        self.assertTrue(ok)
        text = send_call.call_args.args[0]
        self.assertIn("BYPASS QUICK-TP CLOSED", text)
        self.assertIn("XAUUSD", text)
        self.assertIn("1517262012", text)
        self.assertIn("+8.33$", text)
        self.assertEqual(send_call.call_args.kwargs.get("parse_mode"), None)

    def test_resolve_targets_applies_symbol_filter_to_owner_chat(self):
        n = TelegramNotifier()
        n.broadcast_enabled = False
        old_owner = getattr(config, "TELEGRAM_CHAT_ID", "")
        try:
            config.TELEGRAM_CHAT_ID = "123456789"
            with patch("notifier.telegram_bot.access_manager.user_signal_filter_allows", return_value=False):
                ids = n._resolve_target_chat_ids(
                    chat_id=None,
                    feature="scan_stocks",
                    signal_symbol="MBG.DE",
                    signal_symbols=["MBG.DE", "RWE.DE"],
                )
            self.assertEqual(ids, [])
        finally:
            config.TELEGRAM_CHAT_ID = old_owner

    def test_resolve_targets_applies_symbol_filter_to_direct_chat(self):
        n = TelegramNotifier()
        n.broadcast_enabled = False
        with patch("notifier.telegram_bot.access_manager.user_signal_filter_allows", return_value=False):
            ids = n._resolve_target_chat_ids(
                chat_id=777,
                feature="scan_stocks",
                signal_symbol="MBG.DE",
                signal_symbols=["MBG.DE"],
            )
        self.assertEqual(ids, [])

    def test_vi_stock_summary_includes_profile_reasons_and_escapes_plus(self):
        n = TelegramNotifier()
        sig = TradeSignal(
            symbol="AAPL",
            direction="long",
            confidence=78.0,
            entry=190.0,
            stop_loss=184.0,
            take_profit_1=196.0,
            take_profit_2=202.0,
            take_profit_3=208.0,
            risk_reward=2.0,
            timeframe="1h",
            session="new_york",
            trend="bullish",
            rsi=59.0,
            atr=3.0,
            pattern="BULLISH_OB_BOUNCE",
            raw_scores={
                "vi_total_score": 82.4,
                "vi_value_score": 74.0,
                "vi_trend_score": 88.0,
                "vi_compounder_score": 86.0,
                "vi_turnaround_score": 40.0,
                "vi_primary_score": 86.0,
                "vi_primary_profile": "BUFFETT",
                "vi_market_cap_bucket": "mega",
                "vi_range_position_pct": 35.0,
                "vi_metric_price_to_book": 3.2,
                "vi_metric_revenue_growth": 0.11,
                "vi_metric_earnings_growth": 0.14,
                "vi_reasons_detailed": [
                    "Buffett-inspired: quality business + reasonable valuation + trend confirmation",
                    "Quality metrics: ROE 22.0%, OpMargin 28.0%, ProfitMargin 22.0%",
                    "Execution context: LONG bullish | vol 1.30x | $vol 40,000,000 | setup 60.0%",
                ],
            },
        )
        opp = StockOpportunity(
            signal=sig,
            market="US",
            setup_type="BULLISH_OB_BOUNCE",
            base_setup_type="OB_BOUNCE",
            vol_vs_avg=1.3,
            dollar_volume=40_000_000,
            setup_win_rate=0.60,
            quality_score=3,
            quality_tag="HIGH",
            pe_ratio=18.0,
        )
        with patch.object(n, "_send", return_value=True) as send_call:
            ok = n.send_vi_stock_summary([opp], chat_id=777)
        self.assertTrue(ok)
        text = send_call.call_args.args[0]
        self.assertIn("Buffett", text)
        self.assertIn("Reasons:", text)
        self.assertIn("quality/value \\+ turnaround", text)
        self.assertNotIn("quality/value + turnaround", text)

    def test_macro_alert_respects_per_user_saved_filter(self):
        now = datetime.now(timezone.utc)
        high = MacroHeadline(
            headline_id="h1",
            title="High risk tariff headline",
            link="https://example.com/h1",
            source="Reuters",
            published_utc=now,
            score=9,  # ***
            themes=["tariff_trade"],
            impact_hint="Policy/trade shock risk",
        )
        medium = MacroHeadline(
            headline_id="h2",
            title="Medium Fed headline",
            link="https://example.com/h2",
            source="Reuters",
            published_utc=now,
            score=5,  # **
            themes=["fed_policy"],
            impact_hint="Fed sensitivity",
        )

        n = TelegramNotifier()
        with patch.object(n, "_resolve_target_chat_ids", return_value=[111, 222]), \
             patch("notifier.telegram_bot.access_manager.get_user_macro_risk_filter", side_effect=["***", "**"]), \
             patch.object(n, "_send", return_value=True) as send_call:
            ok = n.send_macro_news_alert([high, medium], chat_id=None)

        self.assertTrue(ok)
        self.assertEqual(send_call.call_count, 2)

        # User 111 (***): should only receive the high-risk headline.
        text_111 = send_call.call_args_list[0].args[0]
        kwargs_111 = send_call.call_args_list[0].kwargs
        self.assertEqual(kwargs_111.get("chat_id"), 111)
        self.assertIn("High risk tariff headline", text_111)
        self.assertNotIn("Medium Fed headline", text_111)

        # User 222 (**): should receive both headlines.
        text_222 = send_call.call_args_list[1].args[0]
        kwargs_222 = send_call.call_args_list[1].kwargs
        self.assertEqual(kwargs_222.get("chat_id"), 222)
        self.assertIn("High risk tariff headline", text_222)
        self.assertIn("Medium Fed headline", text_222)

    def test_macro_messages_do_not_emit_unescaped_greater_than_symbol(self):
        now = datetime.now(timezone.utc)
        h = MacroHeadline(
            headline_id="h3",
            title="Tariff headline",
            link="https://example.com/h3",
            source="Reuters",
            published_utc=now,
            score=9,
            themes=["tariff_trade"],
            impact_hint="Policy/trade shock risk",
        )
        n = TelegramNotifier()

        with patch.object(n, "_resolve_target_chat_ids", return_value=[111]), \
             patch("notifier.telegram_bot.access_manager.get_user_macro_risk_filter", return_value="***"), \
             patch.object(n, "_send", return_value=True) as send_call:
            n.send_macro_news_alert([h], chat_id=None)
        alert_text = send_call.call_args.args[0]
        self.assertNotIn(">=", alert_text)
        self.assertIn("Min risk", alert_text)

        with patch.object(n, "_send", return_value=True) as send_call2:
            n.send_macro_news_snapshot([h], lookback_hours=24, chat_id=111, min_risk_stars="***")
        snapshot_text = send_call2.call_args.args[0]
        self.assertNotIn(">=", snapshot_text)
        self.assertIn("Min risk", snapshot_text)

    def test_macro_and_calendar_times_render_in_user_timezone(self):
        n = TelegramNotifier()
        headline = MacroHeadline(
            headline_id="h4",
            title="Fed headline",
            link="https://example.com/h4",
            source="Reuters",
            published_utc=datetime(2026, 2, 22, 12, 30, tzinfo=timezone.utc),
            score=9,
            themes=["fed_policy"],
            impact_hint="Fed-policy sensitivity",
        )
        event = EconomicEvent(
            event_id="e1",
            title="FOMC",
            currency="USD",
            impact="high",
            forecast="",
            previous="",
            actual="",
            source_url="https://example.com/e1",
            time_utc=datetime(2026, 2, 22, 13, 0, tzinfo=timezone.utc),
        )

        with patch.object(n, "_resolve_target_chat_ids", return_value=[777]), \
             patch("notifier.telegram_bot.access_manager.get_user_macro_risk_filter", return_value="***"), \
             patch("notifier.telegram_bot.access_manager.get_user_news_utc_offset", return_value="+07:00"), \
             patch.object(n, "_send", return_value=True) as send_call:
            n.send_macro_news_alert([headline], chat_id=None)
        macro_text = send_call.call_args.args[0]
        self.assertIn("UTC\\+07:00", macro_text)
        self.assertIn("19:30", macro_text)  # 12:30 UTC -> 19:30 UTC+7

        with patch.object(n, "_resolve_target_chat_ids", return_value=[777]), \
             patch("notifier.telegram_bot.access_manager.get_user_news_utc_offset", return_value="+07:00"), \
             patch.object(n, "_send", return_value=True) as send_call2:
            n.send_economic_calendar_snapshot([event], lookahead_hours=24, chat_id=None)
        cal_text = send_call2.call_args.args[0]
        self.assertIn("UTC\\+07:00", cal_text)
        self.assertIn("20:00", cal_text)  # 13:00 UTC -> 20:00 UTC+7

    def test_calendar_countdown_and_macro_age_use_hms(self):
        n = TelegramNotifier()
        now = datetime.now(timezone.utc)
        headline = MacroHeadline(
            headline_id="h5",
            title="Tariff headline",
            link="https://example.com/h5",
            source="Reuters",
            published_utc=now,
            score=9,
            themes=["tariff_trade"],
            impact_hint="Policy/trade shock risk",
        )
        event = EconomicEvent(
            event_id="e2",
            title="CPI",
            currency="USD",
            impact="high",
            forecast="",
            previous="",
            actual="",
            source_url="https://example.com/e2",
            time_utc=now,
        )

        with patch.object(n, "_resolve_target_chat_ids", return_value=[777]), \
             patch("notifier.telegram_bot.access_manager.get_user_news_utc_offset", return_value="+07:00"), \
             patch.object(n, "_send", return_value=True) as send_call:
            n.send_economic_calendar_snapshot([event], lookahead_hours=24, chat_id=None)
        cal_text = send_call.call_args.args[0]
        self.assertIn("T\\-", cal_text)
        self.assertRegex(cal_text, r"\d{2}:\d{2}:\d{2}")

        with patch.object(n, "_send", return_value=True) as send_call2:
            n.send_macro_news_snapshot([headline], lookback_hours=24, chat_id=777, min_risk_stars="***")
        macro_text = send_call2.call_args.args[0]
        self.assertRegex(macro_text, r"Age: `\d{2}:\d{2}:\d{2}`")
        self.assertTrue(any(x in macro_text for x in ("IMMEDIATE", "DEVELOPING", "ACTIVE", "FADING")))

    def test_mt5_position_manager_update_formats_actions(self):
        n = TelegramNotifier()
        rpt = {
            "account_key": "TEST|123",
            "positions": 1,
            "checked": 1,
            "managed": 1,
            "actions": [
                {
                    "symbol": "ETHUSD",
                    "ticket": 12345,
                    "action": "trail_sl",
                    "status": "ok",
                    "message": "SL moved > breakeven",
                    "old_sl": 100.1,
                    "new_sl": 101.2,
                    "position_volume": 0.01,
                    "requested_close_volume": 0.005,
                    "executed_close_volume": 0.005,
                    "r_now": 1.42,
                    "age_min": 23.5,
                },
            ],
        }
        with patch.object(n, "_send", return_value=True) as send_call:
            ok = n.send_mt5_position_manager_update(rpt, source="scheduler", chat_id=777)
        self.assertTrue(ok)
        text = send_call.call_args.args[0]
        self.assertIn("MT5 POSITION MANAGER", text)
        self.assertIn("ETHUSD", text)
        self.assertIn("\\>", text)  # escaped in MarkdownV2 output
        self.assertIn("SL:", text)
        self.assertIn("Volume:", text)
        self.assertIn("Context:", text)
        self.assertEqual(send_call.call_args.kwargs.get("feature"), "mt5_manage")


if __name__ == "__main__":
    unittest.main()
