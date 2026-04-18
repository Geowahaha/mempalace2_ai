import hashlib
import hmac
import json
import time
import unittest

from notifier.billing_webhook import BillingWebhookServer


class BillingWebhookTests(unittest.TestCase):
    def setUp(self):
        self.server = BillingWebhookServer()

    def test_verify_stripe_signature_ok(self):
        secret = "whsec_test_123"
        payload = b'{"id":"evt_test","type":"checkout.session.completed"}'
        ts = int(time.time())
        signed = f"{ts}.".encode("utf-8") + payload
        sig = hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
        header = f"t={ts},v1={sig}"
        ok, why = BillingWebhookServer.verify_stripe_signature(payload, header, secret, tolerance_sec=300)
        self.assertTrue(ok, why)

    def test_verify_promptpay_signature_ok(self):
        secret = "pp_secret_123"
        payload = b'{"event_id":"pp_1","status":"paid"}'
        sig = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
        ok, why = BillingWebhookServer.verify_promptpay_signature(payload, sig, secret)
        self.assertTrue(ok, why)

    def test_resolve_upgrade_context(self):
        metadata = {"telegram_user_id": "1585019324", "plan": "b", "days": "30"}
        user_id, plan, days = self.server._resolve_upgrade_context(metadata)
        self.assertEqual(user_id, 1585019324)
        self.assertEqual(plan, "b")
        self.assertEqual(days, 30)

    def test_parse_json_bytes(self):
        raw = json.dumps({"ok": True}).encode("utf-8")
        parsed = BillingWebhookServer._parse_json_bytes(raw)
        self.assertEqual(parsed, {"ok": True})


if __name__ == "__main__":
    unittest.main()

