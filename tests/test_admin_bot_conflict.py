import unittest
from unittest.mock import patch

import notifier.admin_bot as admin_bot_module


class _FakeResp:
    def __init__(self, payload: dict):
        self._payload = payload

    def json(self):
        return self._payload


class AdminBotConflictTests(unittest.TestCase):
    def test_detect_polling_conflict_409(self):
        with patch.object(admin_bot_module.config, "TELEGRAM_BOT_TOKEN", "token"), \
             patch.object(admin_bot_module.config, "TELEGRAM_CHAT_ID", "12345"):
            bot = admin_bot_module.TelegramAdminBot()

        payload = {
            "ok": False,
            "error_code": 409,
            "description": "Conflict: terminated by other getUpdates request",
        }
        with patch.object(admin_bot_module.requests, "get", return_value=_FakeResp(payload)):
            conflict, detail = bot._detect_polling_conflict()

        self.assertTrue(conflict)
        self.assertIn("Conflict", detail)

    def test_start_blocks_when_duplicate_instance_detected(self):
        with patch.object(admin_bot_module.config, "TELEGRAM_BOT_TOKEN", "token"), \
             patch.object(admin_bot_module.config, "TELEGRAM_CHAT_ID", "12345"):
            bot = admin_bot_module.TelegramAdminBot()

        with patch.object(bot, "_refresh_identity"), \
             patch.object(bot, "_detect_polling_conflict", return_value=(True, "Conflict 409")), \
             patch.object(bot, "_skip_historical_updates") as skip_hist, \
             patch.object(bot, "_send_text") as send_text:
            bot.start()

        self.assertFalse(bot.running)
        self.assertEqual(skip_hist.call_count, 0)
        self.assertEqual(send_text.call_count, 1)


if __name__ == "__main__":
    unittest.main()

