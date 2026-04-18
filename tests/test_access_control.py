import tempfile
import unittest
from pathlib import Path
import gc

from config import config
from notifier.access_control import AccessManager


class AccessControlTests(unittest.TestCase):
    def setUp(self):
        self._old = {
            "ACCESS_DB_PATH": getattr(config, "ACCESS_DB_PATH", ""),
            "TRIAL_DAYS": getattr(config, "TRIAL_DAYS", 7),
            "PLAN_TRIAL_DAILY_LIMIT": getattr(config, "PLAN_TRIAL_DAILY_LIMIT", 12),
            "TRIAL_CRYPTO_SYMBOLS": getattr(config, "TRIAL_CRYPTO_SYMBOLS", ""),
            "PLAN_A_DAILY_LIMIT": getattr(config, "PLAN_A_DAILY_LIMIT", 30),
            "PLAN_B_DAILY_LIMIT": getattr(config, "PLAN_B_DAILY_LIMIT", 120),
            "PLAN_C_DAILY_LIMIT": getattr(config, "PLAN_C_DAILY_LIMIT", 500),
            "TELEGRAM_CHAT_ID": getattr(config, "TELEGRAM_CHAT_ID", ""),
            "TELEGRAM_ADMIN_IDS": getattr(config, "TELEGRAM_ADMIN_IDS", ""),
            "TRIAL_NO_AI_ALL": getattr(config, "TRIAL_NO_AI_ALL", False),
        }
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        config.ACCESS_DB_PATH = str(Path(self.tmp.name) / "access_test.db")
        config.TRIAL_DAYS = 7
        config.PLAN_TRIAL_DAILY_LIMIT = 2
        config.TRIAL_CRYPTO_SYMBOLS = "BTC/USDT,ETH/USDT,BTCUSD,ETHUSD"
        config.PLAN_A_DAILY_LIMIT = 3
        config.PLAN_B_DAILY_LIMIT = 5
        config.PLAN_C_DAILY_LIMIT = 10
        config.TELEGRAM_CHAT_ID = ""
        config.TELEGRAM_ADMIN_IDS = ""
        config.TRIAL_NO_AI_ALL = False
        self.manager = AccessManager()
        self.user_id = 101001

    def tearDown(self):
        del self.manager
        gc.collect()
        try:
            self.tmp.cleanup()
        except PermissionError:
            pass
        for key, value in self._old.items():
            setattr(config, key, value)

    def test_new_user_auto_gets_trial(self):
        decision = self.manager.check_and_consume(self.user_id, "scan_gold", is_admin=False)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.user.get("plan"), "trial")
        self.assertEqual(decision.reason, "ok")

    def test_trial_feature_lock_blocks_stocks(self):
        decision = self.manager.check_and_consume(self.user_id, "scan_stocks", is_admin=False)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "feature_locked")

    def test_trial_allows_us_open_assistance(self):
        d1 = self.manager.check_and_consume(self.user_id, "scan_us_open", is_admin=False)
        d2 = self.manager.check_and_consume(self.user_id, "monitor_us", is_admin=False)
        self.assertTrue(d1.allowed)
        self.assertTrue(d2.allowed)

    def test_trial_allows_vi_and_calendar(self):
        d1 = self.manager.check_and_consume(self.user_id, "scan_vi", is_admin=False)
        d2 = self.manager.check_and_consume(self.user_id + 1, "calendar", is_admin=False)
        d3 = self.manager.check_and_consume(self.user_id + 2, "macro", is_admin=False)
        self.assertTrue(d1.allowed)
        self.assertTrue(d2.allowed)
        self.assertTrue(d3.allowed)

    def test_trial_daily_limit_enforced(self):
        d1 = self.manager.check_and_consume(self.user_id, "scan_gold", is_admin=False)
        d2 = self.manager.check_and_consume(self.user_id, "scan_gold", is_admin=False)
        d3 = self.manager.check_and_consume(self.user_id, "scan_gold", is_admin=False)
        self.assertTrue(d1.allowed)
        self.assertTrue(d2.allowed)
        self.assertFalse(d3.allowed)
        self.assertEqual(d3.reason, "daily_limit_reached")

    def test_admin_can_grant_plan_b(self):
        granted = self.manager.grant_plan(self.user_id, plan="b", days=30)
        self.assertEqual(granted.get("plan"), "b")
        decision = self.manager.check_and_consume(self.user_id, "scan_stocks", is_admin=False)
        self.assertTrue(decision.allowed)

    def test_admin_bypass(self):
        decision = self.manager.check_and_consume(9999, "scan_all", is_admin=True)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reason, "admin_bypass")

    def test_expired_user_can_still_access_upgrade_command(self):
        self.manager.grant_plan(self.user_id, plan="a", days=1, status="expired")
        decision = self.manager.check_and_consume(self.user_id, "upgrade", is_admin=False)
        self.assertTrue(decision.allowed)

    def test_apply_payment_upgrade_is_idempotent(self):
        first = self.manager.apply_payment_upgrade(
            provider="stripe",
            event_id="evt_001",
            user_id=self.user_id,
            plan="b",
            days=30,
            amount=49.0,
            currency="USD",
            payload={"id": "evt_001"},
        )
        second = self.manager.apply_payment_upgrade(
            provider="stripe",
            event_id="evt_001",
            user_id=self.user_id,
            plan="b",
            days=30,
            amount=49.0,
            currency="USD",
            payload={"id": "evt_001"},
        )
        self.assertTrue(first["applied"])
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertFalse(second["applied"])

    def test_apply_payment_upgrade_extends_existing_expiry(self):
        first = self.manager.apply_payment_upgrade(
            provider="stripe",
            event_id="evt_100",
            user_id=self.user_id,
            plan="a",
            days=10,
            amount=10.0,
            currency="USD",
            payload={"id": "evt_100"},
        )
        exp1 = first["user"].get("expires_at")
        second = self.manager.apply_payment_upgrade(
            provider="promptpay",
            event_id="pp_101",
            user_id=self.user_id,
            plan="a",
            days=5,
            amount=100.0,
            currency="THB",
            payload={"id": "pp_101"},
        )
        exp2 = second["user"].get("expires_at")
        self.assertTrue(bool(exp1))
        self.assertTrue(bool(exp2))
        self.assertGreater(exp2, exp1)

    def test_list_entitled_user_ids_filters_by_feature_and_status(self):
        trial_user = 111
        b_user = 222
        expired_b_user = 333
        self.manager.check_and_consume(trial_user, "scan_gold", is_admin=False)
        self.manager.grant_plan(b_user, plan="b", days=30)
        self.manager.grant_plan(expired_b_user, plan="b", days=1, status="expired")

        gold_ids = self.manager.list_entitled_user_ids("scan_gold")
        stocks_ids = self.manager.list_entitled_user_ids("scan_stocks")

        self.assertIn(trial_user, gold_ids)
        self.assertIn(b_user, gold_ids)
        self.assertIn(b_user, stocks_ids)
        self.assertNotIn(trial_user, stocks_ids)
        self.assertNotIn(expired_b_user, gold_ids)
        self.assertNotIn(expired_b_user, stocks_ids)

    def test_trial_crypto_special_symbols_only(self):
        trial_user = 111
        self.manager.check_and_consume(trial_user, "scan_gold", is_admin=False)

        ids_btc = self.manager.list_entitled_user_ids("scan_crypto", signal_symbol="BTC/USDT")
        ids_eth = self.manager.list_entitled_user_ids("scan_crypto", signal_symbol="ETHUSD")
        ids_sol = self.manager.list_entitled_user_ids("scan_crypto", signal_symbol="SOL/USDT")

        self.assertIn(trial_user, ids_btc)
        self.assertIn(trial_user, ids_eth)
        self.assertNotIn(trial_user, ids_sol)

    def test_user_language_preference_persists_between_instances(self):
        saved = self.manager.set_user_language_preference(self.user_id, "th", metadata={"source": "test"})
        self.assertEqual(saved, "th")
        self.assertEqual(self.manager.get_user_language_preference(self.user_id), "th")

        manager2 = AccessManager()
        try:
            self.assertEqual(manager2.get_user_language_preference(self.user_id), "th")
            manager2.set_user_language_preference(self.user_id, "de")
            self.assertEqual(manager2.get_user_language_preference(self.user_id), "de")
        finally:
            del manager2
            gc.collect()

    def test_user_language_preference_rejects_invalid(self):
        with self.assertRaises(ValueError):
            self.manager.set_user_language_preference(self.user_id, "jp")

    def test_user_macro_risk_filter_persists_and_coexists_with_language_pref(self):
        self.assertIsNone(self.manager.get_user_macro_risk_filter(self.user_id))
        saved = self.manager.set_user_macro_risk_filter(self.user_id, "***")
        self.assertEqual(saved, "***")
        self.assertEqual(self.manager.get_user_macro_risk_filter(self.user_id), "***")

        self.manager.set_user_language_preference(self.user_id, "th", metadata={"source": "test"})
        self.assertEqual(self.manager.get_user_language_preference(self.user_id), "th")
        self.assertEqual(self.manager.get_user_macro_risk_filter(self.user_id), "***")

        self.manager.set_user_macro_risk_filter(self.user_id, None)
        self.assertIsNone(self.manager.get_user_macro_risk_filter(self.user_id))

    def test_user_macro_risk_filter_rejects_invalid(self):
        with self.assertRaises(ValueError):
            self.manager.set_user_macro_risk_filter(self.user_id, "****")

    def test_user_signal_symbol_filter_persists_and_filters_entitled_delivery(self):
        u_gold = 1101
        u_crypto = 2202
        self.manager.grant_plan(u_gold, plan="b", days=30)
        self.manager.grant_plan(u_crypto, plan="b", days=30)

        self.assertEqual(self.manager.set_user_signal_symbol_filter(u_gold, ["gold"]), ["XAUUSD"])
        self.assertEqual(self.manager.set_user_signal_symbol_filter(u_crypto, ["btc", "eth"]), ["BTC", "ETH"])

        ids_gold = self.manager.list_entitled_user_ids("scan_gold", signal_symbol="XAUUSD")
        ids_btc = self.manager.list_entitled_user_ids("scan_crypto", signal_symbol="BTC/USDT")

        self.assertIn(u_gold, ids_gold)
        self.assertNotIn(u_crypto, ids_gold)
        self.assertIn(u_crypto, ids_btc)
        self.assertNotIn(u_gold, ids_btc)

        self.assertEqual(self.manager.set_user_signal_symbol_filter(u_gold, []), [])
        ids_btc_after_clear = self.manager.list_entitled_user_ids("scan_crypto", signal_symbol="BTCUSD")
        self.assertIn(u_gold, ids_btc_after_clear)

    def test_user_signal_symbol_filter_coexists_with_other_preferences(self):
        self.manager.set_user_language_preference(self.user_id, "de", metadata={"source": "test"})
        self.manager.set_user_macro_risk_filter(self.user_id, "**")
        saved_filter = self.manager.set_user_signal_symbol_filter(self.user_id, ["xauusd", "ethusdt"])
        self.assertEqual(saved_filter, ["XAUUSD", "ETH/USDT"])

        self.assertEqual(self.manager.get_user_language_preference(self.user_id), "de")
        self.assertEqual(self.manager.get_user_macro_risk_filter(self.user_id), "**")
        self.assertEqual(self.manager.get_user_signal_symbol_filter(self.user_id), ["XAUUSD", "ETH/USDT"])

    def test_user_news_timezone_persists_and_coexists_with_other_preferences(self):
        self.assertIsNone(self.manager.get_user_news_utc_offset(self.user_id))
        self.assertEqual(self.manager.set_user_news_utc_offset(self.user_id, "+07:00"), "+07:00")
        self.assertEqual(self.manager.get_user_news_utc_offset(self.user_id), "+07:00")

        self.manager.set_user_language_preference(self.user_id, "de", metadata={"source": "test"})
        self.manager.set_user_macro_risk_filter(self.user_id, "**")
        self.assertEqual(self.manager.get_user_language_preference(self.user_id), "de")
        self.assertEqual(self.manager.get_user_macro_risk_filter(self.user_id), "**")
        self.assertEqual(self.manager.get_user_news_utc_offset(self.user_id), "+07:00")

        manager2 = AccessManager()
        try:
            self.assertEqual(manager2.get_user_news_utc_offset(self.user_id), "+07:00")
            manager2.set_user_news_utc_offset(self.user_id, None)
            self.assertIsNone(manager2.get_user_news_utc_offset(self.user_id))
        finally:
            del manager2
            gc.collect()

    def test_user_news_timezone_rejects_invalid(self):
        with self.assertRaises(ValueError):
            self.manager.set_user_news_utc_offset(self.user_id, "bangkok")
        with self.assertRaises(ValueError):
            self.manager.set_user_news_utc_offset(self.user_id, "+99:99")


    def test_trial_no_ai_all_switch_enables_non_ai_scans_but_not_mt5_or_research(self):
        config.TRIAL_NO_AI_ALL = True
        mgr = AccessManager()
        try:
            d_stocks = mgr.check_and_consume(self.user_id + 500, "scan_stocks", is_admin=False)
            d_thai = mgr.check_and_consume(self.user_id + 501, "scan_thai", is_admin=False)
            d_research = mgr.check_and_consume(self.user_id + 502, "research", is_admin=False)
            d_mt5 = mgr.check_and_consume(self.user_id + 503, "mt5_status", is_admin=False)
            self.assertTrue(d_stocks.allowed)
            self.assertTrue(d_thai.allowed)
            self.assertFalse(d_research.allowed)
            self.assertIn(d_research.reason, {"feature_locked", "expired"})
            self.assertFalse(d_mt5.allowed)
            self.assertEqual(d_mt5.reason, "feature_locked")
        finally:
            del mgr
            gc.collect()


if __name__ == "__main__":
    unittest.main()
