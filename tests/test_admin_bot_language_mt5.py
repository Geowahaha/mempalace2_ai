import unittest
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import notifier.admin_bot as admin_bot_module


class AdminBotLanguageMt5Tests(unittest.TestCase):
    def setUp(self):
        self.bot = admin_bot_module.TelegramAdminBot()
        self.bot._intent_phrase_memory = {}

    def test_parse_run_trace_args_accepts_short_tag(self):
        parsed = self.bot._parse_run_trace_args("R123")
        self.assertTrue(parsed.get("valid"))
        self.assertEqual(parsed.get("run_no"), 123)
        self.assertEqual(parsed.get("run_tag"), "R000123")

    def test_run_command_uses_trace_lookup_and_formatter(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_report = {"ok": True, "query": {"run_tag": "R000123", "valid": True}, "signal_rows": [], "journal_rows": []}
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(self.bot, "_lookup_run_trace", return_value=fake_report) as lookup, \
             patch.object(self.bot, "_format_run_trace_report", return_value="Run Trace\nquery=R000123") as fmt, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(
                chat_id=1001,
                user_id=2002,
                command="run",
                args="R000123",
                is_admin=True,
                lang="en",
            )

        self.assertEqual(lookup.call_count, 1)
        self.assertEqual(fmt.call_count, 1)
        self.assertEqual(send_text.call_count, 2)  # progress + final
        self.assertIn("R000123", send_text.call_args.args[1])

    def test_detect_language_thai_and_german(self):
        self.assertEqual(self.bot._detect_language("ช่วยเช็คสถานะ mt5 ให้หน่อย"), "th")
        self.assertEqual(self.bot._detect_language("Bitte prüfe meine offene Position"), "de")

    def test_extract_symbol_mt5_style_ethusd(self):
        symbol, side = self.bot._extract_symbol_and_side_hint(
            "I opened ETHUSD order. Can you check MT5 position?",
            mt5_hint=True,
        )
        self.assertEqual(symbol, "ETHUSD")
        self.assertIsNone(side)

    def test_natural_language_order_query_routes_to_mt5_status_with_symbol(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd:
            self.bot._handle_natural_language(
                chat_id=1001,
                user_id=2002,
                text="I opened ETHUSD order, please check my position",
                is_admin=True,
                lang="en",
            )

        self.assertEqual(handle_cmd.call_count, 1)
        _, kwargs = handle_cmd.call_args
        self.assertEqual(kwargs["lang"], "en")
        # positional args: chat_id, user_id, command, args, is_admin
        pos_args = handle_cmd.call_args.args
        self.assertEqual(pos_args[2], "mt5_status")
        self.assertEqual(pos_args[3], "ETHUSD")

    def test_natural_language_mt5_status_takes_priority_over_generic_status(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd:
            self.bot._handle_natural_language(
                chat_id=1101,
                user_id=2102,
                text="mt5 status",
                is_admin=True,
                lang="en",
            )
        self.assertEqual(handle_cmd.call_count, 1)
        pos_args = handle_cmd.call_args.args
        self.assertEqual(pos_args[2], "mt5_status")

    def test_natural_language_maps_thai_stock_scan_phrase_to_scan_thai(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_record_intent_event"), \
             patch.object(self.bot, "_remember_intent_phrase"):
            self.bot._handle_natural_language(
                chat_id=1201,
                user_id=2202,
                text="หาหุ้นไทย อันดับแรก scan th stock",
                is_admin=True,
                lang="th",
            )
        self.assertEqual(handle_cmd.call_count, 1)
        self.assertEqual(handle_cmd.call_args.args[2], "scan_thai")

    def test_natural_language_maps_show_only_gold_to_signal_filter(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_record_intent_event"), \
             patch.object(self.bot, "_remember_intent_phrase"):
            self.bot._handle_natural_language(
                chat_id=1202,
                user_id=2203,
                text="แสดงแค่ทองคำ",
                is_admin=True,
                lang="th",
            )
        self.assertEqual(handle_cmd.call_count, 1)
        pos = handle_cmd.call_args.args
        self.assertEqual(pos[2], "show_only")
        self.assertEqual(pos[3], "XAUUSD")

    def test_natural_language_ambiguous_stock_scan_requests_confirmation(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_send_text") as send_text, \
             patch.object(self.bot, "_record_intent_event"):
            self.bot._handle_natural_language(
                chat_id=1203,
                user_id=2204,
                text="scan stock now",
                is_admin=True,
                lang="en",
            )
        self.assertEqual(handle_cmd.call_count, 0)
        rec = self.bot._pending_intent_confirm(1203)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.get("command"), "scan_stocks")
        self.assertEqual(send_text.call_count, 1)
        self.assertIn("confirmation", send_text.call_args.args[1].lower())

    def test_pending_intent_confirmation_yes_executes_and_learns_phrase(self):
        self.bot._set_pending_intent_confirm(
            chat_id=1204,
            command="scan_stocks",
            args="",
            source_text="scan stock now",
        )
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_record_intent_event"), \
             patch.object(self.bot, "_save_intent_phrase_memory"):
            ok = self.bot._try_handle_pending_intent_confirm(
                chat_id=1204,
                user_id=2205,
                text="yes",
                is_admin=True,
                lang="en",
            )
        self.assertTrue(ok)
        self.assertEqual(handle_cmd.call_count, 1)
        self.assertEqual(handle_cmd.call_args.args[2], "scan_stocks")
        learned = self.bot._lookup_learned_intent("scan stock now")
        self.assertIsNotNone(learned)
        self.assertEqual(learned[0], "scan_stocks")

    def test_mt5_status_includes_open_positions_snapshot(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_status = {
            "enabled": True,
            "dry_run": False,
            "connected": True,
            "host": "127.0.0.1",
            "port": 18812,
            "symbols": 2397,
            "account_login": 5074117,
            "account_server": "ICMarketsSC-MT5",
            "error": "",
        }
        fake_snapshot = {
            "connected": True,
            "positions": [
                {
                    "symbol": "ETHUSD",
                    "type": "buy",
                    "volume": 0.01,
                    "price_open": 2500.0,
                    "price_current": 2520.0,
                    "profit": 0.2,
                }
            ],
            "orders": [],
        }

        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("execution.mt5_executor.mt5_executor.status", return_value=fake_status), \
             patch("execution.mt5_executor.mt5_executor.open_positions_snapshot", return_value=fake_snapshot), \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(
                chat_id=1001,
                user_id=2002,
                command="mt5_status",
                args="",
                is_admin=True,
                lang="en",
            )

        self.assertEqual(send_text.call_count, 1)
        text = send_text.call_args.args[1]
        self.assertIn("MT5 Bridge Status", text)
        self.assertIn("Open positions=1", text)
        self.assertIn("ETHUSD BUY", text)

    def test_mt5_followup_manage_trade_uses_previous_context_not_research(self):
        self.bot._chat_mt5_context[9001] = {
            "ts": time.time(),
            "symbol": "ETHUSD",
            "positions_count": 1,
            "orders_count": 0,
            "kind": "position",
            "sample": {"symbol": "ETHUSD"},
            "ambiguous": False,
        }
        fake_snap = {
            "enabled": True,
            "connected": True,
            "resolved_symbol": "ETHUSD",
            "positions": [
                {
                    "symbol": "ETHUSD",
                    "type": "sell",
                    "volume": 0.1,
                    "ticket": 123,
                    "price_open": 1948.42,
                    "price_current": 1952.01,
                    "profit": -0.36,
                    "sl": 0.0,
                    "tp": 0.0,
                }
            ],
            "orders": [],
        }
        with patch("execution.mt5_executor.mt5_executor.open_positions_snapshot", return_value=fake_snap) as snap_call, \
             patch.object(self.bot, "_send_text_localized") as send_loc, \
             patch.object(self.bot, "_send_text") as send_text:
            handled = self.bot._handle_mt5_trade_followup(
                chat_id=9001,
                user_id=9901,
                text="ชวย monitor and TP or SL this trade please take over",
                is_admin=True,
                lang="th",
            )
        self.assertTrue(handled)
        snap_call.assert_called_once()
        self.assertEqual(snap_call.call_args.kwargs.get("signal_symbol"), "ETHUSD")
        self.assertGreaterEqual(send_loc.call_count, 1)
        self.assertGreaterEqual(send_text.call_count, 1)
        reply = send_text.call_args.args[1]
        self.assertIn("ETHUSD SELL", reply)

    def test_mixed_language_usage_prompts_preference(self):
        with patch.object(self.bot, "_handle_natural_language"), \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_message({
                "text": "สวัสดีครับ",
                "chat": {"id": 3001},
                "from": {"id": 4001},
            })
            self.bot._handle_message({
                "text": "hello",
                "chat": {"id": 3001},
                "from": {"id": 4001},
            })
            self.bot._handle_message({
                "text": "check status please",
                "chat": {"id": 3001},
                "from": {"id": 4001},
            })

        self.assertTrue(self.bot._chat_lang_prompt_pending.get(3001))
        self.assertEqual(send_text.call_count, 1)
        self.assertIn("multiple languages", send_text.call_args.args[1].lower())

    def test_preference_reply_sets_default_language_for_next_command(self):
        self.bot._chat_lang_prompt_pending[3002] = True
        with patch.object(self.bot, "_send_text") as send_text, \
             patch.object(self.bot, "_handle_natural_language") as handle_nl:
            self.bot._handle_message({
                "text": "ไทย",
                "chat": {"id": 3002},
                "from": {"id": 4002},
            })

        self.assertEqual(handle_nl.call_count, 0)
        self.assertEqual(self.bot._chat_lang_pref.get(3002), "th")
        self.assertEqual(send_text.call_count, 1)
        self.assertIn("ภาษาไทย", send_text.call_args.args[1])

        with patch.object(self.bot, "_handle_admin_command") as handle_cmd:
            self.bot._handle_message({
                "text": "/status",
                "chat": {"id": 3002},
                "from": {"id": 4002},
            })
        self.assertEqual(handle_cmd.call_count, 1)
        self.assertEqual(handle_cmd.call_args.kwargs.get("lang"), "th")

    def test_persisted_language_preference_loaded_on_new_message(self):
        bot2 = admin_bot_module.TelegramAdminBot()
        with patch.object(admin_bot_module.access_manager, "get_user_language_preference", return_value="de"), \
             patch.object(bot2, "_handle_admin_command") as handle_cmd:
            bot2._handle_message({
                "text": "/status",
                "chat": {"id": 7001},
                "from": {"id": 8001},
            })

        self.assertEqual(handle_cmd.call_count, 1)
        self.assertEqual(bot2._chat_lang_pref.get(7001), "de")
        self.assertEqual(handle_cmd.call_args.kwargs.get("lang"), "de")

    def test_trial_open_ended_message_skips_ai_parser_and_research(self):
        with patch.object(self.bot, "_ai_api_allowed", return_value=False), \
             patch.object(self.bot, "_infer_command_ai") as infer_ai, \
             patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_send_text_localized") as send_loc:
            self.bot._handle_natural_language(
                chat_id=7101,
                user_id=8101,
                text="What should I do with the market now?",
                is_admin=False,
                lang="en",
            )
        infer_ai.assert_not_called()
        handle_cmd.assert_not_called()
        self.assertEqual(send_loc.call_count, 1)
        self.assertEqual(send_loc.call_args.args[1], "ai_api_locked_trial")

    def test_paid_open_ended_message_does_not_trigger_ai_fallback(self):
        with patch.object(self.bot, "_ai_api_allowed", return_value=True), \
             patch.object(self.bot, "_infer_command_ai") as infer_ai, \
             patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_send_text") as send_text, \
             patch.object(self.bot, "_record_intent_event"):
            self.bot._handle_natural_language(
                chat_id=7102,
                user_id=8102,
                text="Can you think deeply and decide everything for me?",
                is_admin=True,
                lang="en",
            )
        infer_ai.assert_not_called()
        handle_cmd.assert_not_called()
        self.assertEqual(send_text.call_count, 1)
        self.assertIn("rephrase", send_text.call_args.args[1].lower())

    def test_macro_command_accepts_star_filter(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_head = SimpleNamespace(
            headline_id="h1",
            title="Fed headline",
            link="https://example.com",
            source="Reuters",
            published_utc=datetime.now(timezone.utc),
            score=9,
            themes=["fed_policy"],
            impact_hint="Fed-policy sensitivity",
        )
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(admin_bot_module.access_manager, "set_user_macro_risk_filter", return_value="***") as set_pref, \
             patch("market.macro_news.macro_news.high_impact_headlines", return_value=[fake_head]) as hi_call, \
             patch.object(admin_bot_module.notifier, "send_macro_news_snapshot", return_value=True) as send_snap, \
             patch.object(self.bot, "_send_text_localized"):
            self.bot._handle_admin_command(
                chat_id=9201,
                user_id=9301,
                command="macro",
                args="***",
                is_admin=True,
                lang="th",
            )
        self.assertEqual(hi_call.call_count, 1)
        self.assertEqual(int(hi_call.call_args.kwargs.get("min_score")), 8)
        set_pref.assert_called_once_with(9301, "***")
        self.assertEqual(send_snap.call_count, 1)
        self.assertEqual(send_snap.call_args.kwargs.get("min_risk_stars"), "***")

    def test_macro_command_uses_saved_filter_when_no_args(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(admin_bot_module.access_manager, "get_user_macro_risk_filter", return_value="**"), \
             patch("market.macro_news.macro_news.high_impact_headlines", return_value=[]) as hi_call, \
             patch.object(admin_bot_module.notifier, "send_macro_news_snapshot", return_value=True) as send_snap, \
             patch.object(self.bot, "_send_text_localized"):
            self.bot._handle_admin_command(
                chat_id=9202,
                user_id=9302,
                command="macro",
                args="",
                is_admin=True,
                lang="en",
            )
        self.assertEqual(int(hi_call.call_args.kwargs.get("min_score")), 5)
        self.assertEqual(send_snap.call_args.kwargs.get("min_risk_stars"), "**")

    def test_macro_command_reset_clears_saved_filter(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(admin_bot_module.access_manager, "set_user_macro_risk_filter", return_value=None) as set_pref, \
             patch("market.macro_news.macro_news.high_impact_headlines", return_value=[]) as hi_call, \
             patch.object(admin_bot_module.notifier, "send_macro_news_snapshot", return_value=True), \
             patch.object(self.bot, "_send_text_localized"):
            self.bot._handle_admin_command(
                chat_id=9203,
                user_id=9303,
                command="macro",
                args="reset",
                is_admin=True,
                lang="th",
            )
        set_pref.assert_called_once_with(9303, None)
        self.assertGreaterEqual(int(hi_call.call_args.kwargs.get("min_score")), 1)

    def test_tz_command_sets_bangkok_offset(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(admin_bot_module.access_manager, "set_user_news_utc_offset", return_value="+07:00") as set_tz, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(
                chat_id=9401,
                user_id=9501,
                command="tz",
                args="bangkok",
                is_admin=False,
                lang="th",
            )
        set_tz.assert_called_once_with(9501, "+07:00")
        self.assertEqual(send_text.call_count, 1)
        self.assertIn("+07:00", send_text.call_args.args[1])

    def test_tz_command_show_current(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(admin_bot_module.access_manager, "get_user_news_utc_offset", return_value="+07:00"), \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(
                chat_id=9402,
                user_id=9502,
                command="tz",
                args="",
                is_admin=False,
                lang="en",
            )
        self.assertEqual(send_text.call_count, 1)
        self.assertIn("Current: +07:00", send_text.call_args.args[1])

    def test_tz_parser_accepts_gmt_plus_7(self):
        self.assertEqual(self.bot._normalize_utc_offset_input("gmt+7"), "+07:00")
        self.assertEqual(self.bot._normalize_utc_offset_input("UTC+07"), "+07:00")
        self.assertEqual(self.bot._normalize_utc_offset_input("+7"), "+07:00")

    def test_show_only_command_sets_signal_filter_symbols(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(admin_bot_module.access_manager, "get_user_signal_symbol_filter", return_value=[]), \
             patch.object(admin_bot_module.access_manager, "set_user_signal_symbol_filter", return_value=["XAUUSD", "BTC"]) as set_filter, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(
                chat_id=9901,
                user_id=8801,
                command="show_only",
                args="gold btc",
                is_admin=False,
                lang="en",
            )
        set_filter.assert_called_once_with(8801, ["XAUUSD", "BTC"])
        self.assertEqual(send_text.call_count, 1)
        self.assertIn("XAUUSD", send_text.call_args.args[1])

    def test_signal_filter_status_reports_current_symbols(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(admin_bot_module.access_manager, "get_user_signal_symbol_filter", return_value=["BTC", "ETH"]), \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(
                chat_id=9902,
                user_id=8802,
                command="signal_filter",
                args="status",
                is_admin=False,
                lang="en",
            )
        self.assertEqual(send_text.call_count, 1)
        self.assertIn("BTC, ETH", send_text.call_args.args[1])

    def test_scan_ethusd_starts_targeted_symbol_scan_not_us_open(self):
        fake_thread = SimpleNamespace(start=lambda: None)
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_send_text_localized") as send_loc, \
             patch("notifier.admin_bot.threading.Thread", return_value=fake_thread) as thread_ctor:
            self.bot._handle_natural_language(
                chat_id=9601,
                user_id=9701,
                text="scan ETHUSD",
                is_admin=True,
                lang="en",
            )
        handle_cmd.assert_not_called()
        self.assertEqual(send_loc.call_count, 1)
        self.assertEqual(send_loc.call_args.args[1], "running_symbol_scan")
        self.assertEqual(thread_ctor.call_count, 1)

    def test_scan_th_stock_now_routes_to_scan_thai_not_symbol_now(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_send_text_localized") as send_loc, \
             patch("notifier.admin_bot.threading.Thread") as thread_ctor:
            self.bot._handle_natural_language(
                chat_id=9602,
                user_id=9702,
                text="Scan Th stock now",
                is_admin=True,
                lang="th",
            )
        send_loc.assert_not_called()
        thread_ctor.assert_not_called()
        handle_cmd.assert_called_once()
        self.assertEqual(handle_cmd.call_args.args[2], "scan_thai")

    def test_scan_us_stock_now_routes_to_scan_us_open_not_symbol_now(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_send_text_localized") as send_loc, \
             patch("notifier.admin_bot.threading.Thread") as thread_ctor:
            self.bot._handle_natural_language(
                chat_id=9603,
                user_id=9703,
                text="US scan stock now",
                is_admin=True,
                lang="en",
            )
        send_loc.assert_not_called()
        thread_ctor.assert_not_called()
        handle_cmd.assert_called_once()
        self.assertEqual(handle_cmd.call_args.args[2], "scan_us_open")

    def test_mt5_history_phrase_without_symbol_prompts_slot_fill(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_send_text_localized") as send_loc:
            self.bot._handle_natural_language(
                chat_id=9801,
                user_id=9901,
                text="check last night mt5 history it was TP or SL",
                is_admin=True,
                lang="en",
            )
        handle_cmd.assert_not_called()
        self.assertEqual(send_loc.call_count, 1)
        self.assertEqual(send_loc.call_args.args[1], "mt5_history_need_symbol")
        self.assertEqual(self.bot._chat_pending_slots[9801]["kind"], "mt5_history_symbol")

    def test_mt5_history_phrase_with_context_routes_to_mt5_history(self):
        self.bot._chat_mt5_context[9802] = {"ts": time.time(), "symbol": "ETHUSD", "requested_symbol": "ETHUSD"}
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd:
            self.bot._handle_natural_language(
                chat_id=9802,
                user_id=9902,
                text="check last night mt5 history it was TP or SL",
                is_admin=True,
                lang="en",
            )
        self.assertEqual(handle_cmd.call_count, 1)
        self.assertEqual(handle_cmd.call_args.args[2], "mt5_history")
        self.assertIn("ETHUSD", handle_cmd.call_args.args[3])

    def test_typo_mt5_status_command_is_autocorrected(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_status = {
            "enabled": True,
            "dry_run": False,
            "connected": True,
            "host": "127.0.0.1",
            "port": 18812,
            "symbols": 10,
            "account_login": 123,
            "account_server": "TEST",
            "error": "",
        }
        fake_snapshot = {"connected": True, "positions": [], "orders": []}
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("execution.mt5_executor.mt5_executor.status", return_value=fake_status), \
             patch("execution.mt5_executor.mt5_executor.open_positions_snapshot", return_value=fake_snapshot), \
             patch.object(self.bot, "_send_text_localized") as send_loc, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(
                chat_id=9910,
                user_id=9920,
                command="mt5_ststus",
                args="",
                is_admin=True,
                lang="th",
            )
        self.assertGreaterEqual(send_loc.call_count, 1)
        self.assertEqual(send_loc.call_args_list[0].args[1], "command_autocorrected")
        self.assertGreaterEqual(send_text.call_count, 1)
        final_text = send_text.call_args.args[1]
        self.assertIn("MT5", final_text)

    def test_natural_language_mt5_histroy_typo_prompts_symbol_slot(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_send_text_localized") as send_loc:
            self.bot._handle_natural_language(
                chat_id=9951,
                user_id=9952,
                text="chek mt5 histroy last night tp or sl?",
                is_admin=True,
                lang="en",
            )
        handle_cmd.assert_not_called()
        self.assertEqual(send_loc.call_count, 1)
        self.assertEqual(send_loc.call_args.args[1], "mt5_history_need_symbol")
        self.assertEqual(self.bot._chat_pending_slots[9951]["kind"], "mt5_history_symbol")

    def test_pending_slot_mt5_history_resolves_on_next_symbol_message(self):
        self.bot._chat_pending_slots[9961] = {"kind": "mt5_history_symbol", "payload": {"hours": 24}, "ts": time.time()}
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_maybe_offer_language_preference"):
            self.bot._handle_message({
                "text": "ETHUSD",
                "chat": {"id": 9961},
                "from": {"id": 9962},
            })
        self.assertEqual(handle_cmd.call_count, 1)
        args = handle_cmd.call_args.args
        self.assertEqual(args[2], "mt5_history")
        self.assertIn("ETHUSD", args[3])
        self.assertIn("24h", args[3])
        self.assertNotIn(9961, self.bot._chat_pending_slots)

    def test_macro_report_command_uses_saved_macro_filter(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_report = {"entries": [], "hours": 24, "min_risk_stars": "***"}
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(admin_bot_module.access_manager, "get_user_macro_risk_filter", return_value="***"), \
             patch("market.macro_impact_tracker.macro_impact_tracker.sync", return_value={"ok": True}), \
             patch("market.macro_impact_tracker.macro_impact_tracker.build_report", return_value=fake_report) as build_report, \
             patch.object(admin_bot_module.notifier, "send_macro_impact_report", return_value=True) as send_report, \
             patch.object(self.bot, "_send_text_localized"):
            self.bot._handle_admin_command(
                chat_id=9971,
                user_id=9972,
                command="macro_report",
                args="",
                is_admin=True,
                lang="en",
            )
        self.assertEqual(build_report.call_count, 1)
        self.assertEqual(build_report.call_args.kwargs.get("min_risk_stars"), "***")
        self.assertEqual(send_report.call_count, 1)

    def test_macro_weights_command_refreshes_and_sends_report(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_report = {"ok": True, "rows": [], "top_n": 6}
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("market.macro_impact_tracker.macro_impact_tracker.refresh_adaptive_weights", return_value={"ok": True, "status": "ok", "updated": 4}) as refresh_weights, \
             patch("market.macro_impact_tracker.macro_impact_tracker.build_weights_report", return_value=fake_report) as build_weights, \
             patch.object(admin_bot_module.notifier, "send_macro_weights_report", return_value=True) as send_weights, \
             patch.object(self.bot, "_send_text_localized") as send_loc:
            self.bot._handle_admin_command(
                chat_id=9981,
                user_id=9982,
                command="macro_weights",
                args="refresh top6",
                is_admin=True,
                lang="en",
            )

        self.assertEqual(send_loc.call_count, 1)
        self.assertEqual(send_loc.call_args.args[1], "checking_macro_weights")
        refresh_weights.assert_called_once()
        build_weights.assert_called_once()
        self.assertEqual(build_weights.call_args.kwargs.get("limit"), 6)
        send_weights.assert_called_once()
        sent_report = send_weights.call_args.args[0]
        self.assertIn("refresh_result", sent_report)
        self.assertEqual(send_weights.call_args.kwargs.get("chat_id"), 9981)

    def test_mt5_autopilot_command_sends_summary(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_status = {
            "enabled": True,
            "account_key": "TEST|123",
            "risk_gate": {"allow": True, "status": "allowed", "reason": "ok"},
            "risk_snapshot": {
                "daily_realized_pnl": 1.23,
                "daily_loss_abs": 0.0,
                "consecutive_losses": 0,
                "recent_rejections_1h": 0,
                "open_positions": 1,
                "pending_orders": 0,
            },
            "journal": {"total": 10, "resolved": 5, "open_forward_tests": 2},
            "calibration": {"labeled_7d": 7, "win_rate_7d": 0.5, "mae_7d": 0.12},
        }
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("learning.mt5_autopilot_core.mt5_autopilot_core.status", return_value=fake_status), \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(
                chat_id=91001,
                user_id=91002,
                command="mt5_autopilot",
                args="",
                is_admin=True,
                lang="en",
            )
        self.assertEqual(send_text.call_count, 1)
        out = send_text.call_args.args[1]
        self.assertIn("MT5 Autopilot Core", out)
        self.assertIn("risk_gate", out)
        self.assertIn("journal:", out)

    def test_mt5_policy_command_show_and_set(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_show = {"ok": True, "account_key": "TEST|123", "policy": {"canary_force": None, "daily_loss_limit_usd": 0.8}}
        fake_set = {"ok": True, "account_key": "TEST|123", "updated_key": "canary_force", "updated_value": False}
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("learning.mt5_orchestrator.mt5_orchestrator.current_account_policy", return_value=fake_show), \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_policy", "show", True, lang="en")
        self.assertIn("MT5 Policy", send_text.call_args.args[1])

        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("learning.mt5_orchestrator.mt5_orchestrator.set_current_account_policy", return_value=fake_set) as set_pol, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_policy", "set canary_force false", True, lang="en")
        set_pol.assert_called_once_with("canary_force", "false")
        self.assertIn("updated", send_text.call_args.args[1].lower())

    def test_mt5_policy_keys_command_returns_schema(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        specs = [{"key": "canary_force", "type": "bool|null", "default": None, "example": "false", "desc": "Force canary"}]
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("learning.mt5_orchestrator.mt5_orchestrator.policy_key_specs", return_value=specs), \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_policy", "keys", True, lang="en")
        self.assertEqual(send_text.call_count, 1)
        self.assertIn("MT5 Policy Keys", send_text.call_args.args[1])
        self.assertIn("canary_force", send_text.call_args.args[1])

    def test_mt5_manage_command_with_actions_uses_notifier_report(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_report = {
            "ok": True,
            "account_key": "TEST|123",
            "positions": 1,
            "checked": 1,
            "managed": 1,
            "actions": [{"ticket": 1, "symbol": "ETHUSD", "action": "breakeven", "status": "ok", "message": "moved"}],
        }
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("learning.mt5_position_manager.mt5_position_manager.run_cycle", return_value=fake_report) as run_cycle, \
             patch.object(admin_bot_module.notifier, "send_mt5_position_manager_update", return_value=True) as send_pm, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_manage", "", True, lang="en")
        run_cycle.assert_called_once()
        send_pm.assert_called_once()
        self.assertGreaterEqual(send_text.call_count, 1)

    def test_mt5_manage_watch_command_uses_watch_snapshot(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_watch = {"enabled": True, "ok": True, "account_key": "TEST|123", "positions": 1, "watched": 1, "entries": []}
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("learning.mt5_position_manager.mt5_position_manager.watch_snapshot", return_value=fake_watch) as watch_snap, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_manage", "watch ETHUSD", True, lang="en")
        watch_snap.assert_called_once()
        self.assertEqual(watch_snap.call_args.kwargs.get("signal_symbol"), "ETHUSD")
        self.assertIn("MT5 Position Manager Watch", send_text.call_args.args[1])

    def test_mt5_policy_preset_command_applies_preset(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_rep = {"ok": True, "account_key": "TEST|123", "preset": "micro_safe", "preset_desc": "safe"}
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("learning.mt5_orchestrator.mt5_orchestrator.apply_current_account_preset", return_value=fake_rep) as apply_preset, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_policy", "preset micro_safe", True, lang="en")
        apply_preset.assert_called_once_with("micro_safe")
        self.assertIn("preset applied", send_text.call_args.args[1].lower())

    def test_mt5_plan_command_builds_preview(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_signal = SimpleNamespace(
            symbol="ETH/USDT",
            direction="long",
            confidence=82.0,
            pattern="OB_BOUNCE",
            entry=2000.0,
            stop_loss=1900.0,
            take_profit_1=2050.0,
            take_profit_2=2100.0,
            take_profit_3=2150.0,
            risk_reward=2.0,
        )
        fake_exec_preview = {
            "ok": True,
            "status": "ok",
            "broker_symbol": "ETHUSD",
            "base": {"entry": 2000.0, "stop_loss": 1900.0, "take_profit_2": 2100.0, "risk_reward": 2.0},
            "adaptive": {
                "applied": True,
                "reason": "adaptive_applied",
                "rr_base": 2.0,
                "rr_target": 1.6,
                "stop_scale": 0.9,
                "size_multiplier": 0.8,
                "factors": {"family": "crypto", "samples": 9, "win_rate": 0.56, "mae": 0.31, "spread_pct": 0.01, "atr_pct": 1.2, "session": "london"},
            },
            "execution": {"entry": 2000.0, "stop_loss": 1910.0, "take_profit_1": 2090.0, "take_profit_2": 2144.0, "take_profit_3": 2234.0, "risk_reward": 1.6, "volume_multiplier_input": 0.35, "volume_multiplier_final": 0.28, "desired_volume": 0.03, "fitted_volume": 0.02},
            "margin": {"free_margin": 6.26, "required": 0.19, "fit_reason": "ok"},
            "market": {"bid": 1999.9, "ask": 2000.0, "spread": 0.1, "spread_pct": 0.005},
            "account": {"account_key": "TEST|123", "balance": 6.26, "equity": 6.26, "free_margin": 6.26},
        }
        fake_wf = SimpleNamespace(allow=True, reason="ok", canary_mode=True, risk_multiplier=0.35)
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(self.bot, "_load_live_signal_for_symbol", return_value=(fake_signal, {"kind": "crypto"})) as load_sig, \
             patch("learning.mt5_orchestrator.mt5_orchestrator.pre_trade_plan", return_value=fake_wf) as preplan, \
             patch("execution.mt5_executor.mt5_executor.preview_adaptive_execution", return_value=fake_exec_preview) as preview, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_plan", "ETHUSD", True, lang="en")
        load_sig.assert_called_once()
        preplan.assert_called_once()
        preview.assert_called_once()
        self.assertGreaterEqual(send_text.call_count, 2)  # progress + result
        self.assertIn("Adaptive Plan Preview", send_text.call_args.args[1])
        self.assertIn("ETHUSD", send_text.call_args.args[1])

    def test_mt5_plan_whatif_builds_three_scenarios(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_signal = SimpleNamespace(
            symbol="ETH/USDT",
            direction="long",
            confidence=82.0,
            pattern="OB_BOUNCE",
            entry=2000.0,
            stop_loss=1900.0,
            take_profit_1=2050.0,
            take_profit_2=2100.0,
            take_profit_3=2150.0,
            risk_reward=2.0,
        )
        def _preview_for(scen):
            return {
                "ok": True,
                "status": "ok",
                "scenario": scen,
                "broker_symbol": "ETHUSD",
                "adaptive": {"reason": f"adaptive_{scen}", "size_multiplier": 0.8, "stop_scale": 1.0, "factors": {"samples": 5, "atr_pct": 1.2}},
                "execution": {"risk_reward": 1.8, "fitted_volume": 0.02, "stop_loss": 1910.0, "take_profit_2": 2144.0},
                "margin": {"required": 0.2, "fit_reason": "ok"},
                "market": {"spread_pct": 0.01},
            }
        fake_wf = SimpleNamespace(allow=True, reason="ok", canary_mode=True, risk_multiplier=0.35)
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch.object(self.bot, "_load_live_signal_for_symbol", return_value=(fake_signal, {"kind": "crypto"})), \
             patch("learning.mt5_orchestrator.mt5_orchestrator.pre_trade_plan", return_value=fake_wf), \
             patch("execution.mt5_executor.mt5_executor.preview_adaptive_execution", side_effect=[
                 _preview_for("conservative"), _preview_for("balanced"), _preview_for("aggressive")
             ]) as preview, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_plan", "ETHUSD --whatif", True, lang="en")
        self.assertEqual(preview.call_count, 3)
        scenarios = [c.kwargs.get("scenario") for c in preview.call_args_list]
        self.assertEqual(scenarios, ["conservative", "balanced", "aggressive"])
        self.assertIn("What-If", send_text.call_args.args[1])
        self.assertIn("[CONSERVATIVE]", send_text.call_args.args[1])
        self.assertIn("[AGGRESSIVE]", send_text.call_args.args[1])

    def test_mt5_pm_learning_command_builds_report(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_report = {
            "enabled": True,
            "ok": True,
            "account_key": "TEST|123",
            "days": 30,
            "filters": {"symbol": "ETHUSD", "action": "trail_sl"},
            "sync": {"ok": True, "updated": 1, "still_unresolved": 2, "closed_rows_seen": 5, "history_query_mode": "ts_int"},
            "summary": {"total_actions": 4, "resolved_actions": 2, "unresolved_actions": 2},
            "actions_overall": [{"label": "breakeven", "samples": 2, "resolved": 2, "positive_rate": 0.5, "negative_rate": 0.5, "tp_rate": 0.5, "avg_pnl": 0.1}],
            "symbols": [{"label": "ETHUSD", "samples": 2, "resolved": 2, "positive_rate": 0.5, "negative_rate": 0.5, "avg_pnl": 0.1, "actions": []}],
            "recommendations": [
                {"key": "trail_start_r", "action": "trail_sl", "current": 1.2, "suggested": 1.4, "direction": "raise", "confidence": "medium", "samples": 8, "reason": "Trail start (R): move toward positive_avg"}
            ],
        }
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("learning.mt5_position_manager.mt5_position_manager.build_learning_report", return_value=fake_report) as build, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_pm_learning", "30d top5 ETHUSD action trail_sl", True, lang="en")
        build.assert_called_once()
        self.assertEqual(build.call_args.kwargs.get("days"), 30)
        self.assertEqual(build.call_args.kwargs.get("top"), 5)
        self.assertTrue(build.call_args.kwargs.get("sync"))
        self.assertEqual(build.call_args.kwargs.get("symbol"), "ETHUSD")
        self.assertEqual(build.call_args.kwargs.get("action"), "trail_sl")
        self.assertIn("MT5 PM Learning Report", send_text.call_args.args[1])
        self.assertIn("filter: symbol=ETHUSD | action=trail_sl", send_text.call_args.args[1])
        self.assertIn("recommendations:", send_text.call_args.args[1])

    def test_mt5_pm_learning_args_parser_supports_aliases(self):
        parsed = self.bot._parse_mt5_pm_learning_args("ETHUSD action trail 14d top3 nosync recommend draft")
        self.assertEqual(parsed["symbol"], "ETHUSD")
        self.assertEqual(parsed["action"], "trail_sl")
        self.assertEqual(parsed["days"], 14)
        self.assertEqual(parsed["top"], 3)
        self.assertFalse(parsed["sync"])
        self.assertTrue(parsed["save_draft"])

    def test_mt5_affordable_command_parses_category_and_top(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake = {
            "enabled": True,
            "connected": True,
            "category": "crypto",
            "account_server": "TEST-MT5",
            "account_login": 123,
            "balance": 5.0,
            "equity": 5.0,
            "free_margin": 5.0,
            "currency": "USD",
            "margin_budget_pct": 20.0,
            "allowed_margin": 1.0,
            "min_free_margin_after_trade": 1.0,
            "micro_max_spread_pct": 0.15,
            "symbol_policy": {"allowlist_active": False, "allow_count": 0, "block_count": 0},
            "summary": {"ok_now": 1, "market_ok": 2, "margin_ok": 2, "spread_ok": 1, "checked": 5},
            "rows": [{"symbol": "ETHUSD", "category": "crypto", "status": "ok", "margin_min_lot": 0.1, "spread_pct": 0.12, "vol_min": 0.01, "margin_ok": True, "spread_ok": True, "policy_ok": True}],
        }
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("execution.mt5_executor.mt5_executor.affordable_symbols_snapshot", return_value=fake) as affordable, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_affordable", "crypto top5", True, lang="en")
        affordable.assert_called_once_with(category="crypto", limit=5, only_ok=False)
        self.assertIn("MT5 Affordable Symbols", send_text.call_args.args[1])

    def test_mt5_affordable_command_ok_filter(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake = {
            "enabled": True,
            "connected": True,
            "category": "all",
            "only_ok": True,
            "account_server": "TEST-MT5",
            "account_login": 123,
            "balance": 5.0,
            "equity": 5.0,
            "free_margin": 5.0,
            "currency": "USD",
            "margin_budget_pct": 20.0,
            "allowed_margin": 1.0,
            "min_free_margin_after_trade": 1.0,
            "micro_max_spread_pct": 0.15,
            "symbol_policy": {"allowlist_active": False, "allow_count": 0, "block_count": 0},
            "summary": {"ok_now": 1, "market_ok": 2, "margin_ok": 2, "spread_ok": 1, "checked": 5},
            "rows": [{"symbol": "NZDUSD", "category": "fx", "status": "ok", "margin_min_lot": 1.19, "spread_pct": 0.002, "vol_min": 0.01, "margin_ok": True, "spread_ok": True, "policy_ok": True}],
        }
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("execution.mt5_executor.mt5_executor.affordable_symbols_snapshot", return_value=fake) as affordable, \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(100, 200, "mt5_affordable", "ok top5", True, lang="en")
        affordable.assert_called_once_with(category="all", limit=5, only_ok=True)
        self.assertIn("filter=ok_only", send_text.call_args.args[1])


    def test_vi_phrase_thai_routes_to_scan_thai_vi(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd:
            self.bot._handle_natural_language(
                chat_id=9901,
                user_id=9901,
                text="ค้นหาหุ้นไทย vi",
                is_admin=True,
                lang="th",
            )
        handle_cmd.assert_called_once()
        self.assertEqual(handle_cmd.call_args.args[2], "scan_thai_vi")

    def test_vi_phrase_us_thai_colloquial_routes_to_scan_vi(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd:
            self.bot._handle_natural_language(
                chat_id=9902,
                user_id=9902,
                text="ค้นหาหุ้นเมกา vi",
                is_admin=True,
                lang="th",
            )
        handle_cmd.assert_called_once()
        self.assertEqual(handle_cmd.call_args.args[2], "scan_vi")

    def test_scan_th_vi_routes_to_scan_thai_vi(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd:
            self.bot._handle_natural_language(
                chat_id=9903,
                user_id=9903,
                text="scan th vi",
                is_admin=True,
                lang="en",
            )
        handle_cmd.assert_called_once()
        self.assertEqual(handle_cmd.call_args.args[2], "scan_thai_vi")

    def test_scan_us_vi_routes_to_scan_vi(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd:
            self.bot._handle_natural_language(
                chat_id=9904,
                user_id=9904,
                text="scan us vi",
                is_admin=True,
                lang="en",
            )
        handle_cmd.assert_called_once()
        self.assertEqual(handle_cmd.call_args.args[2], "scan_vi")

    def test_natural_language_scalping_on_routes_to_scalping_on(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_record_intent_event"), \
             patch.object(self.bot, "_remember_intent_phrase"):
            self.bot._handle_natural_language(
                chat_id=9905,
                user_id=9905,
                text="เปิดโหมด scalping สำหรับ btc กับ eth",
                is_admin=True,
                lang="th",
            )
        handle_cmd.assert_called_once()
        pos = handle_cmd.call_args.args
        self.assertEqual(pos[2], "scalping_on")
        self.assertIn("BTCUSD", pos[3])
        self.assertIn("ETHUSD", pos[3])

    def test_natural_language_logic_trade_btc_routes_to_scalping_logic(self):
        with patch.object(self.bot, "_handle_admin_command") as handle_cmd, \
             patch.object(self.bot, "_record_intent_event"), \
             patch.object(self.bot, "_remember_intent_phrase"):
            self.bot._handle_natural_language(
                chat_id=9906,
                user_id=9906,
                text="find out the logic trade for BTC",
                is_admin=True,
                lang="en",
            )
        handle_cmd.assert_called_once()
        pos = handle_cmd.call_args.args
        self.assertEqual(pos[2], "scalping_logic")
        self.assertEqual(pos[3], "BTCUSD")

    def test_scalping_logic_command_formats_btc_signal(self):
        decision = SimpleNamespace(allowed=True, reason="ok")
        fake_signal = SimpleNamespace(
            direction="long",
            confidence=77.5,
            entry=60123.0,
            stop_loss=59888.0,
            take_profit_1=60456.0,
            take_profit_2=60720.0,
            reasons=["trend aligned", "m1 confirmed"],
        )
        fake_row = SimpleNamespace(
            source="scalp_btcusd",
            symbol="BTCUSD",
            status="ready",
            reason="ok",
            trigger={"ok": True, "reason": "m1_long_confirmed", "rsi14": 55.2, "ema9": 60110.0, "ema21": 60090.0},
            signal=fake_signal,
        )
        with patch.object(admin_bot_module.access_manager, "check_and_consume", return_value=decision), \
             patch("scanners.scalping_scanner.scalping_scanner.scan_btc", return_value=fake_row), \
             patch.object(self.bot, "_send_text") as send_text:
            self.bot._handle_admin_command(
                chat_id=1007,
                user_id=2007,
                command="scalping_logic",
                args="btc",
                is_admin=True,
                lang="en",
            )
        send_text.assert_called_once()
        msg = send_text.call_args.args[1]
        self.assertIn("Scalping Logic (BTCUSD)", msg)
        self.assertIn("status=ready", msg)
        self.assertIn("signal=LONG", msg)
        self.assertIn("m1_trigger=", msg)


if __name__ == "__main__":
    unittest.main()
