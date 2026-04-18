import unittest
from unittest.mock import patch

from notifier.telegram_bot import TelegramNotifier
from notifier.admin_bot import TelegramAdminBot


class SignalDashboardRenderTests(unittest.TestCase):
    def setUp(self):
        self.notifier = TelegramNotifier()

    def test_no_data_dashboard_renders_thai_when_lang_th(self):
        report = {
            "status": "no_data",
            "message": "ไม่พบข้อมูล",
            "window_mode": "today",
            "window": {"start_local": "2026-03-04 00:00", "end_local": "2026-03-05 00:00"},
        }
        with patch.object(self.notifier, "_send", return_value=True) as send_mock:
            ok = self.notifier.send_signal_trader_dashboard(report, chat_id=123, lang="th")
        self.assertTrue(ok)
        text = send_mock.call_args.args[0]
        self.assertIn("แดชบอร์ดสัญญาณ", text)
        self.assertIn("ช่วงเวลา", text)

    def test_no_data_dashboard_renders_english_when_lang_en(self):
        report = {
            "status": "no_data",
            "message": "No data",
            "window_mode": "today",
            "window": {"start_local": "2026-03-04 00:00", "end_local": "2026-03-05 00:00"},
        }
        with patch.object(self.notifier, "_send", return_value=True) as send_mock:
            ok = self.notifier.send_signal_trader_dashboard(report, chat_id=123, lang="en")
        self.assertTrue(ok)
        text = send_mock.call_args.args[0]
        self.assertIn("SIGNAL DASHBOARD", text)
        self.assertIn("Period", text)

    def test_dashboard_uses_saved_language_when_lang_missing(self):
        report = {
            "status": "no_data",
            "message": "empty",
            "window_mode": "today",
            "window": {"start_local": "2026-03-04 00:00", "end_local": "2026-03-05 00:00"},
        }
        with patch("notifier.telegram_bot.access_manager.get_user_language_preference", return_value="th"), \
             patch.object(self.notifier, "_send", return_value=True) as send_mock:
            ok = self.notifier.send_signal_trader_dashboard(report, chat_id=123, lang=None)
        self.assertTrue(ok)
        text = send_mock.call_args.args[0]
        self.assertIn("แดชบอร์ดสัญญาณ", text)


class SignalDashboardCompareRenderTests(unittest.TestCase):
    def test_compare_render_localizes_thai(self):
        bot = TelegramAdminBot()
        a = {
            "days": 7,
            "window_mode": "this_week",
            "market_filter": "gold",
            "summary": {"sent": 2, "resolved": 1, "pending": 1, "wins": 1, "losses": 0, "win_rate": 100.0, "net_r": 1.2, "pending_mark_r": 0.3},
            "simulation": {"marked_balance": 1015.0},
            "best_symbols": [{"symbol": "XAUUSD", "session_r": 1.5}],
        }
        b = {
            "days": 7,
            "window_mode": "this_week",
            "market_filter": "crypto",
            "summary": {"sent": 3, "resolved": 2, "pending": 1, "wins": 1, "losses": 1, "win_rate": 50.0, "net_r": 0.2, "pending_mark_r": -0.1},
            "simulation": {"marked_balance": 1001.0},
            "best_symbols": [{"symbol": "ETHUSD", "session_r": 0.1}],
        }
        out = bot._format_signal_dashboard_compare(a, b, lang="th")
        self.assertIn("ช่วงเวลา", out)
        self.assertIn("ตัวชี้วัด", out)


if __name__ == "__main__":
    unittest.main()
