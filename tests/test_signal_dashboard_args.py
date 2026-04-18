import unittest

from notifier.admin_bot import TelegramAdminBot


class SignalDashboardArgParseTests(unittest.TestCase):
    def test_gold_this_week_maps_to_xauusd_symbol_filter(self):
        parsed = TelegramAdminBot._parse_signal_dashboard_args("gold this week top 7")
        self.assertEqual(parsed.get("window_mode"), "this_week")
        self.assertEqual(parsed.get("top"), 7)
        self.assertEqual(parsed.get("market_filter"), "gold")
        self.assertEqual(parsed.get("symbol_filter"), "XAUUSD")

    def test_ethusd_yesterday_maps_symbol_and_window(self):
        parsed = TelegramAdminBot._parse_signal_dashboard_args("ETHUSD yesterday")
        self.assertEqual(parsed.get("window_mode"), "yesterday")
        self.assertEqual(parsed.get("symbol_filter"), "ETHUSD")
        self.assertEqual(parsed.get("market_filter"), "crypto")

    def test_this_month_us_market_filter(self):
        parsed = TelegramAdminBot._parse_signal_dashboard_args("us this month")
        self.assertEqual(parsed.get("window_mode"), "this_month")
        self.assertEqual(parsed.get("market_filter"), "us")

    def test_compare_keeps_left_right_filters(self):
        parsed = TelegramAdminBot._parse_signal_dashboard_args("compare us vs thai this week")
        self.assertTrue(parsed.get("compare"))
        self.assertEqual(parsed.get("left"), "us")
        self.assertEqual(parsed.get("right"), "thai")
        self.assertEqual(parsed.get("window_mode"), "this_week")

    def test_natural_language_resolver_maps_signal_dashboard_phrase(self):
        bot = TelegramAdminBot()
        intent = bot._resolve_local_intent("signal dashboard gold this week", lang="en")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("mode"), "run")
        self.assertEqual(intent.get("command"), "signal_dashboard")
        self.assertIn("gold", str(intent.get("args", "")).lower())

    def test_window_and_market_labels_localized(self):
        self.assertEqual(
            TelegramAdminBot._signal_dashboard_window_label("this_week", 7, lang="th"),
            "สัปดาห์นี้",
        )
        self.assertEqual(
            TelegramAdminBot._signal_dashboard_market_label("gold", lang="th"),
            "ทอง (Gold)",
        )


if __name__ == "__main__":
    unittest.main()
